#!/usr/bin/env python3
"""
GLM API 代理 v4.5.1 — codex-relay + Python 路由层

架构：
    Codex CLI → 本代理(:9999) → codex-relay(:4444/:4445) → 上游 /chat/completions
                             ↘ 其他路径直接透传上游

功能：
    - codex-relay 负责 Responses API ↔ Chat Completions 翻译（社区维护）
    - Python 层负责：模型覆盖、密钥注入、多上游回退
    - /v1/models 返回静态模型列表
    - 日志和错误请求保存到 logs/ 目录

依赖：
    pip install codex-relay（启动时自动安装/升级）
"""
import json, traceback, threading, time, socket, os, signal, sys, logging, shutil, subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from datetime import datetime

# ── IPv4 优先（本机 IPv6 出站不通：AAAA 先试会卡 SYN-SENT 直到 300s 超时才回退 IPv4）──
# urllib 无 Happy Eyeballs（curl 有），getaddrinfo 稳定排序把 IPv4 提前即可；
# 域名只有 AAAA 时仍按原序尝试（行为不劣化）。曾致 official "4.5 分钟才回 429"、
# cmoyan 直通"挂死 9 分钟"——实为 IPv6 SYN 卡满超时。
_orig_getaddrinfo = socket.getaddrinfo
def _ipv4_first_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    res = _orig_getaddrinfo(host, port, family, type, proto, flags)
    res.sort(key=lambda ai: ai[0] != socket.AF_INET)  # 稳定排序：IPv4 提前，同族保持原序
    return res
socket.getaddrinfo = _ipv4_first_getaddrinfo

# ── 渠道封锁标志（429 限额时封锁 official，直到重置时间）──
# 1313 公平使用限频不封锁，只回退下一上游
_channel_blocked_until = {}  # {"official": timestamp}

def _block_channel_on_429(err_body, upstream_name, req_id=0):
    """检测 429 错误体，解析重置时间并封锁 official 渠道。"""
    import re
    try:
        err_text = err_body.decode("utf-8", errors="replace") if isinstance(err_body, bytes) else str(err_body)
        dbg("[#%d]     [429] _block_channel_on_429 called: upstream=%s err_body_len=%d", req_id, upstream_name, len(err_text))
        # 先尝试 json.loads 提取 message 字段（解决 unicode 转义 \uXXXX 的问题）
        try:
            parsed = json.loads(err_text)
            if isinstance(parsed, dict):
                err_obj = parsed.get("error", parsed)
                if isinstance(err_obj, dict):
                    err_text = err_obj.get("message", err_text)
                    dbg("[#%d]     [429] parsed message: %s", req_id, err_text[:100])
        except Exception:
            pass
        match = re.search(r"限额将在 (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) 重置", err_text)
        dbg("[#%d]     [429] regex match=%s upstream=%s", req_id, bool(match), upstream_name)
        if match and upstream_name == "official":
            reset_str = match.group(1)
            reset_dt = datetime.strptime(reset_str, "%Y-%m-%d %H:%M:%S")
            reset_ts = reset_dt.timestamp()
            _channel_blocked_until["official"] = reset_ts
            log.warning("[#%d]     !!! %s rate limit blocked until %s", req_id, upstream_name, reset_str)
        else:
            dbg("[#%d]     [429] no match or not official, no block set", req_id)
    except Exception as ex:
        log.warning("[#%d]     !!! %s 429 block failed: %s", req_id, upstream_name, ex)


# ── 配置 ──────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_PATH = os.path.join(PROJECT_ROOT, "config.json")
_CHATGPT_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"


def _cg_fresh_access(up):
    """ChatGPT 订阅渠道：从 tokens 文件取 access_token，exp<6h 自动刷新并写回。
    返回 (access_token, account_id)；失败返回 (None, None)。"""
    tf = up.get("tokens_file")
    if not tf:
        return None, None
    path = os.path.join(os.path.dirname(_CONFIG_PATH), tf)
    try:
        d = json.load(open(path, encoding="utf-8"))
        tok = d.get("tokens", d)
        acc = tok.get("access_token") or ""
        exp = 0
        try:
            import base64 as _b64
            mid = acc.split(".")[1]
            exp = json.loads(_b64.urlsafe_b64decode(mid + "=" * (-len(mid) % 4))).get("exp", 0)
        except Exception:
            pass
        rt = tok.get("refresh_token")
        if (not acc or (exp and exp - time.time() < 6 * 3600)) and rt:
            rbody = json.dumps({"grant_type": "refresh_token", "refresh_token": rt,
                                "client_id": d.get("client_id", _CHATGPT_CLIENT_ID),
                                "scope": "openid profile email offline_access"}).encode()
            rreq = Request("https://auth.openai.com/oauth/token", data=rbody,
                           headers={"Content-Type": "application/json",
                                    "User-Agent": "codex_cli_rs/0.45.0"})
            try:
                with urlopen(rreq, timeout=30) as r:
                    nd = json.loads(r.read())
                tok["access_token"] = nd["access_token"]
                if nd.get("id_token"):
                    tok["id_token"] = nd["id_token"]
                if nd.get("refresh_token"):
                    tok["refresh_token"] = nd["refresh_token"]
                d["tokens"] = tok
                d["last_refresh"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                json.dump(d, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
                log.info("[chatgpt] access_token 已自动刷新")
                acc = tok["access_token"]
            except Exception as re_:
                log.warning("[chatgpt] token 刷新失败（用旧token继续）: %s", str(re_)[:100])
        return acc, tok.get("account_id") or ""
    except Exception as e:
        log.warning("[chatgpt] tokens 文件读取失败: %s", str(e)[:100])
        return None, None

def _load_config():
    """从 config.json 加载配置（密钥等敏感信息）。不存在则用示例。"""
    if not os.path.exists(_CONFIG_PATH):
        print("config.json 不存在，请复制 config.example.json 并填入密钥", file=sys.stderr)
        return {"upstreams": []}
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

_cfg = _load_config()

LISTEN = ("0.0.0.0", 9999)
REQUEST_TIMEOUT = 300
# GPT 原生 Responses 偶发接受请求后不返回任何有效 SSE。Codex 不把 SSE comment
# 心跳视为 completion 进度，因此必须在客户端 idle watchdog 前主动回退。
GPT_FIRST_OUTPUT_TIMEOUT = 45
# probe-before-commit 兜底：messages 路径上游首内容前 hold 住响应头（便于检测超限改返 400），
# 但探测期客户端收不到任何字节。超过此秒数仍无首内容（上游首 token 慢/深度推理）→ 强制 commit + keepalive，
# 防客户端流式空闲超时（"Stream idle timeout - no chunks received"，约 60s）。15s 远小于该阈值，
# 且超限/空收尾信号通常几秒内返回，足够探测到。
PROBE_TIMEOUT = 15
# 部分 upstream 套 Cloudflare bot 防护（如 hybgzs），Python-urllib 默认 UA 会被 1010 拦截。
# 统一伪装浏览器 UA，对其他 upstream 无副作用。
_UPSTREAM_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

UPSTREAMS = _cfg.get("upstreams", [])

# v3.0.1: responses_direct（GPT 直通）渠道不进列表——GPT 模型窗口 Codex 原生认识，
# 代理替它报 context_window 反而会用默认值覆盖成错误窗口（chatgpt 渠道无该字段→128000）。
_CHAIN_UPS = [up for up in UPSTREAMS if not up.get("responses_direct")]
STATIC_MODELS = json.dumps({
    "object": "list",
    "data": list({up["model"]: {"id": up["model"], "slug": up["model"], "object": "model",
                                "display_name": up["model"],
                                "owned_by": up.get("owned_by", "zhipu"),
                                "context_window": up.get("max_context_tokens", 128000),
                                "max_context_window": up.get("max_context_tokens", 128000)}
                  for up in _CHAIN_UPS}.values()),
    # v2.9.87: Codex models_manager 期望 "models" 字段（非标准 OpenAI "data"），
    # 缺失会每 3 分钟报 "failed to decode models response: missing field models"
    # v2.9.92: Codex 新版还要每个 model 对象含 "slug" 字段，缺失报 "missing field `slug`"
    # v4.5.1: Codex 0.147 还要 "display_name"，缺失报 missing field `display_name`
    # 且模型列表刷新失败会导致新建会话超时（models_manager 卡住）
    "models": list({up["model"]: {"id": up["model"], "slug": up["model"], "object": "model",
                                 "display_name": up["model"],
                                 "owned_by": up.get("owned_by", "zhipu"),
                                 "context_window": up.get("max_context_tokens", 128000),
                                 "max_context_window": up.get("max_context_tokens", 128000)}
                   for up in _CHAIN_UPS}.values()),
}, ensure_ascii=False).encode()

# ── 日志 ──────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)

def _setup_logger():
    log = logging.getLogger("proxy")
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    log.addHandler(ch)
    # 进程内直接写文件（不依赖启动命令的 stderr 重定向，Windows 也有日志）
    try:
        fh = logging.FileHandler(os.path.join(LOG_DIR, "proxy.log"), encoding="utf-8")
        fh.setFormatter(fmt)
        log.addHandler(fh)
    except Exception as e:
        print(f"[FileHandler failed: {e}]", file=sys.stderr)
    return log

log = _setup_logger()

# ── 调试开关：排查问题时设 GLM_PROXY_DEBUG=1 启用排查日志/请求落盘 ──
DEBUG = os.environ.get("GLM_PROXY_DEBUG", "").strip().lower() not in ("", "0", "false")


def dbg(fmt, *args):
    """仅 DEBUG 开启时输出的排查日志"""
    if DEBUG:
        log.info(fmt, *args)

# 请求序号（多会话日志关联用）
import itertools
_req_counter = itertools.count(1)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """每个请求使用独立线程处理的 HTTP 服务。"""
    daemon_threads = True
