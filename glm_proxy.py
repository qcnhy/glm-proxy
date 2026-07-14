#!/usr/bin/env python3
"""
GLM API 代理 v2.9.35 — codex-relay + Python 路由层

架构：
    Codex CLI → 本代理(:9999) → codex-relay(:4444/:4445) → 上游 /chat/completions
                             ↘ 其他路径直接透传上游

功能：
    - codex-relay 负责 Responses API ↔ Chat Completions 翻译（社区维护）
    - Python 层负责：模型覆盖、密钥注入、多上游回退、健康检查
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

# ── 渠道封锁标志（429 限额时封锁 external+official，直到重置时间）──
_channel_blocked_until = {}  # {"external": timestamp, "official": timestamp}

def _block_channel_on_429(err_body, upstream_name, req_id=0):
    """检测 429 错误体，解析重置时间，封锁 official+external 渠道。
    external 渠道 429 不封锁（直接 fallback 到 official）；official 封锁时联动 external。"""
    import re
    try:
        err_text = err_body.decode("utf-8", errors="replace") if isinstance(err_body, bytes) else str(err_body)
        log.info("[#%d]     [429] _block_channel_on_429 called: upstream=%s err_body_len=%d", req_id, upstream_name, len(err_text))
        # 先尝试 json.loads 提取 message 字段（解决 unicode 转义 \uXXXX 的问题）
        try:
            parsed = json.loads(err_text)
            if isinstance(parsed, dict):
                err_obj = parsed.get("error", parsed)
                if isinstance(err_obj, dict):
                    err_text = err_obj.get("message", err_text)
                    log.info("[#%d]     [429] parsed message: %s", req_id, err_text[:100])
        except Exception:
            pass
        match = re.search(r"限额将在 (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) 重置", err_text)
        log.info("[#%d]     [429] regex match=%s upstream=%s", req_id, bool(match), upstream_name)
        if match and upstream_name == "official":
            reset_str = match.group(1)
            reset_dt = datetime.strptime(reset_str, "%Y-%m-%d %H:%M:%S")
            reset_ts = reset_dt.timestamp()
            _channel_blocked_until["official"] = reset_ts
            _channel_blocked_until["external"] = reset_ts  # official 封锁时联动 external
            log.warning("[#%d]     !!! %s rate limit blocked until %s (official+external)", req_id, upstream_name, reset_str)
        elif upstream_name.startswith("external"):
            log.info("[#%d]     [429] external 429, not blocking (will fallback to official)", req_id)
        else:
            log.info("[#%d]     [429] no match or not official, no block set", req_id)
    except Exception as ex:
        log.warning("[#%d]     !!! %s 429 block failed: %s", req_id, upstream_name, ex)


# ── 配置 ──────────────────────────────────────────────
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

def _load_config():
    """从 config.json 加载配置（密钥等敏感信息）。不存在则用示例。"""
    if not os.path.exists(_CONFIG_PATH):
        log.warning("config.json 不存在，请复制 config.example.json 并填入密钥")
        return {"upstreams": []}
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

_cfg = _load_config()

LISTEN = ("0.0.0.0", 9999)
REQUEST_TIMEOUT = 300
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

# Responses API 路由路径（config.json 的 responses_use_completions 控制）：
#   True  → codex-relay（Responses→Chat Completions，主路径）
#   False → ccproxy-api（Responses→Anthropic Messages，备用路径）
RESPONSES_USE_COMPLETIONS = _cfg.get("responses_use_completions", True)

UPSTREAMS = _cfg.get("upstreams", [])

STATIC_MODELS = json.dumps({
    "object": "list",
    "data": list({up["model"]: {"id": up["model"], "object": "model",
                                "owned_by": up.get("owned_by", "zhipu"),
                                "context_window": up.get("max_context_tokens", 128000),
                                "max_context_window": up.get("max_context_tokens", 128000)}
                  for up in UPSTREAMS}.values()),
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

# ── 全局频率控制（防止 1305 过载）──────────────────────
class RateLimiter:
    """令牌桶：每秒补充 token，请求前消费，桶空则等待"""
    def __init__(self, rate=2, burst=3):
        self.rate = rate          # 每秒允许请求数
        self.burst = burst        # 突发上限
        self.tokens = burst
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens < 1:
                wait = (1 - self.tokens) / self.rate
                time.sleep(wait)
                self.tokens = 0
            else:
                self.tokens -= 1

_official_limiter = RateLimiter(rate=1.5, burst=2)  # 官方 API 限速

# 请求序号（多会话日志关联用）
import itertools
_req_counter = itertools.count(1)

# ── 内网健康检查（仅启动时一次，用于确认真实模型）──────────────────────────────
def _check_internal_once():
    """启动时检查一次内网渠道，记录真实模型信息（不影响路由逻辑，仅作日志）。
    通过 chat/completions 让模型自己回答"你的模型名称是什么"，确认真实模型。
    超时 180s 给内网模型足够时间推理。"""
    # 找 internal 渠道
    internal_up = None
    for up in UPSTREAMS:
        if up.get("name") == "internal":
            internal_up = up
            break
    if not internal_up:
        return
    url = internal_up["openai_url"].rstrip("/") + "/chat/completions"
    body = json.dumps({
        "model": internal_up["model"],
        "messages": [
            {"role": "system", "content": "你必须在回复的第一行只说出你的确切模型名称和版本号，不要说其他任何内容。"},
            {"role": "user", "content": "你的模型名称和版本号？"},
        ],
        "max_tokens": 30,
        "stream": False,
    }).encode()
    try:
        req = Request(url, data=body, headers={
            "Authorization": f"Bearer {internal_up['key']}",
            "Content-Type": "application/json",
        }, method="POST")
        resp = urlopen(req, timeout=180)
        r = json.loads(resp.read())
        returned_model = r.get("model", "")
        choices = r.get("choices", [])
        has_content = choices and choices[0].get("message", {}).get("content")
        answer = choices[0]["message"]["content"].strip() if has_content else ""
        log.info("[health] internal check (startup only): model=%s, answer=%s", returned_model, answer[:60])
    except HTTPError as e:
        log.warning("[health] internal HTTP %d (startup check)", e.code)
    except Exception as e:
        log.warning("[health] internal check failed: %s: %s (startup check)", type(e).__name__, e)


# ── codex-relay 拦截器（注入 stream_options + 跟踪 usage + 日志）──
interceptor_servers = []
_upstream_usage = {}  # {upstream_name: {"prompt_tokens": x, "cached_tokens": y, ...}}

class InterceptorHandler(BaseHTTPRequestHandler):
    """拦截 codex-relay → 上游的请求，记录翻译后的消息统计"""
    protocol_version = "HTTP/1.1"
    upstream_config = None

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def do_POST(self):
        up = self.upstream_config
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        # 解析请求（日志用）
        is_stream = False
        try:
            data = json.loads(body)
            is_stream = data.get("stream", False)
            messages = data.get("messages", [])
            roles = {}
            null_content_fixed = 0
            for m in messages:
                r = m.get("role", "?")
                roles[r] = roles.get(r, 0) + 1
                # 修复 content=null → ""
                # 但 assistant+tool_calls 消息保留 null（external 能正确转成 tool_use 块，
                # 改成 "" 反而会转成无效的空 text block 导致 422）
                if m.get("content") is None and not m.get("tool_calls"):
                    m["content"] = ""
                    null_content_fixed += 1
            if null_content_fixed:
                log.info("[relay] %s fixed %d null content → \"\"", up["name"], null_content_fixed)
                body = json.dumps(data).encode("utf-8")
            log.info("[relay] %s translated: %d msgs %dKB | roles=%s | stream=%s",
                     up["name"], len(messages), len(body) // 1024, roles, is_stream)
            # [DEBUG] 保存翻译后的请求用于排查（大请求才存，避免占磁盘）
            if len(messages) > 15:
                ts = time.strftime("%Y%m%d_%H%M%S")
                path = os.path.join(LOG_DIR, f"debug_relay_{ts}.json")
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                log.info("[relay] %s [DEBUG] saved translated request to %s", up["name"], path)
            if is_stream:
                body = json.dumps(data).encode("utf-8")
        except Exception:
            log.info("[relay] %s translated: %dKB (parse failed)", up["name"], len(body) // 1024)

        # 转发到真实上游（usage 由 codex-relay v0.2.1 自行处理）
        url = up["openai_url"].rstrip("/") + self.path
        headers = {
            "Content-Type": self.headers.get("Content-Type", "application/json"),
            "Authorization": f"Bearer {up['key']}",
        }
        try:
            req = Request(url, data=body, headers=headers, method="POST")
            resp = urlopen(req, timeout=REQUEST_TIMEOUT)

            if is_stream:
                self.send_response(200)
                for h in ["Content-Type", "Cache-Control"]:
                    v = resp.headers.get(h)
                    if v:
                        self.send_header(h, v)
                self.send_header("Connection", "close")
                self.end_headers()
                resp_size = 0
                while True:
                    chunk = resp.read(4096)
                    if not chunk:
                        break
                    resp_size += len(chunk)
                    # 提取上游 usage（GLM 流式返回 prompt_tokens:0，需用 cached_tokens 修正）
                    try:
                        text = chunk.decode("utf-8", errors="replace")
                        for line in text.split("\n"):
                            if line.startswith("data: ") and '"usage"' in line:
                                u = json.loads(line[6:]).get("usage")
                                if u and u.get("total_tokens", 0) > 0:
                                    _upstream_usage[up["name"]] = u
                    except Exception:
                        pass
                    self.wfile.write(chunk)
                    self.wfile.flush()
                self.close_connection = True
                log.info("[relay] %s upstream response: %dKB", up["name"], resp_size // 1024)
            else:
                result = resp.read()
                try:
                    r = json.loads(result)
                    u = r.get("usage")
                    if u and u.get("total_tokens", 0) > 0:
                        _upstream_usage[up["name"]] = u
                except Exception:
                    pass
                self.send_response(resp.status)
                self.send_header("Content-Type", resp.headers.get("Content-Type", "application/json"))
                self.send_header("Content-Length", str(len(result)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(result)
                self.close_connection = True
        except HTTPError as e:
            err_body = e.read()
            self.send_response(e.code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(err_body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(err_body)
            self.close_connection = True
        except Exception as e:
            err = json.dumps({"error": str(e)}).encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(err)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(err)
            self.close_connection = True

    def do_GET(self):
        """转发 GET（如 /v1/models）到上游，带 API key。
        codex-relay 启动时 GET /models 探测模型类型，据此决定是否注入 thinking。"""
        up = self.upstream_config
        try:
            url = up["openai_url"].rstrip("/") + self.path
            req = Request(url, headers={
                "Authorization": f"Bearer {up['key']}",
                "Connection": "close",
            }, method="GET")
            resp = urlopen(req, timeout=30)
            result = resp.read()
            self.send_response(resp.status)
            self.send_header("Content-Type", resp.headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(result)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(result)
            self.close_connection = True
        except Exception as e:
            err = json.dumps({"error": str(e)}).encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(err)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(err)
            self.close_connection = True

    def log_message(self, *a):
        pass


def _start_interceptors():
    for up in UPSTREAMS:
        if "interceptor_port" not in up:
            continue  # 仅 Messages 的渠道不需要拦截器
        handler = type("InterceptorHandler_%s" % up["name"],
                       (InterceptorHandler,),
                       {"upstream_config": up})
        server = ThreadedHTTPServer(("127.0.0.1", up["interceptor_port"]), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        interceptor_servers.append(server)
        log.info("interceptor :%d → %s", up["interceptor_port"], up["openai_url"])

# ── ccproxy-api 自动安装 ────────────────────────────
def _ensure_ccproxy():
    """确保 ccproxy-api 已安装（Responses↔Messages 格式转换依赖）"""
    try:
        from importlib.metadata import version as _pkg_ver
        _pkg_ver("ccproxy-api")
        log.info("ccproxy-api %s OK", _pkg_ver("ccproxy-api"))
    except Exception:
        log.info("ccproxy-api not found, installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-U",
                               "ccproxy-api", "--break-system-packages", "--quiet"])

_ensure_ccproxy()

# ccproxy-api 转换模块（Responses ↔ Messages 格式互转）
try:
    from ccproxy.llms.formatters.anthropic_to_openai.streams import AnthropicToOpenAIResponsesStreamAdapter
    from ccproxy.llms.formatters.anthropic_to_openai.responses import convert__anthropic_message_to_openai_responses__response
    from ccproxy.llms.models.anthropic import MessageResponse
    HAS_CCPROXY = True
except ImportError:
    HAS_CCPROXY = False

# ── codex-relay 子进程管理 ────────────────────────────
relay_procs = []

def _find_relay_binary():
    """跨平台查找 codex-relay 二进制"""
    name = "codex-relay.exe" if sys.platform == "win32" else "codex-relay"
    # 1) PATH 查找
    path = shutil.which(name) or shutil.which("codex-relay")
    if path:
        return path
    # 2) 环境变量
    env_path = os.environ.get("CODEX_RELAY_BIN")
    if env_path and os.path.isfile(env_path):
        return env_path
    # 3) Python Scripts 目录（Windows 常见）
    scripts_dir = os.path.dirname(sys.executable)
    if sys.platform == "win32" and not scripts_dir.endswith("Scripts"):
        scripts_dir = os.path.join(scripts_dir, "Scripts")
    candidate = os.path.join(scripts_dir, name)
    if os.path.isfile(candidate):
        return candidate
    # 4) codex_relay 包内 _bin 目录
    try:
        import codex_relay
        candidate = os.path.join(os.path.dirname(codex_relay.__file__), "_bin", name)
        if os.path.isfile(candidate):
            return candidate
    except ImportError:
        pass
    return None

def _start_relays():
    _RELAY_MIN = (0, 5, 2)
    need_install = False
    try:
        from importlib.metadata import version as _pkg_ver
        installed = tuple(int(x) for x in _pkg_ver("codex-relay").split(".")[:3])
        if installed < _RELAY_MIN:
            need_install = True
            log.info("codex-relay %s < %s, upgrading...",
                     ".".join(map(str, installed)), ".".join(map(str, _RELAY_MIN)))
    except Exception:
        need_install = True
        log.info("codex-relay not found, installing...")
    if need_install:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-U", "codex-relay",
                               "--break-system-packages", "--quiet"])
    binary = _find_relay_binary()
    if not binary:
        log.error("codex-relay binary not found! Run: pip install codex-relay")
        log.error("Or set CODEX_RELAY_BIN=/path/to/codex-relay")
        sys.exit(1)
    for up in UPSTREAMS:
        if "relay_port" not in up:
            continue  # 仅 Messages 的渠道（如 external-claude）不需要 codex-relay
        env = os.environ.copy()
        env["CODEX_RELAY_API_KEY"] = up["key"]
        # codex-relay → interceptor → 真实上游
        interceptor_url = f"http://127.0.0.1:{up['interceptor_port']}"
        proc = subprocess.Popen(
            [binary, "--port", str(up["relay_port"]), "--upstream", interceptor_url],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        relay_procs.append(proc)
        log.info("codex-relay :%d → interceptor :%d → %s (pid=%d)",
                 up["relay_port"], up["interceptor_port"], up["openai_url"], proc.pid)

def _stop_relays():
    for proc in relay_procs:
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except:
            try:
                proc.kill()
            except:
                pass





def _request_has_images(body):
    """检测 Responses 请求是否包含图片（input_image）"""
    input_items = body.get("input", [])
    if not isinstance(input_items, list):
        return False
    for item in input_items:
        if not isinstance(item, dict):
            continue
        for field in ("content", "output"):
            content = item.get(field)
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "input_image":
                        return True
    return False


def _to_anthropic_content(content):
    """将 Responses API 的 content（str/list）转成 Anthropic Messages 的 content 块列表。
    关键：input_image(data:image/...;base64,XXX) → Anthropic image 块，
    否则 GLM 会把 base64 当文本数 token（一张图变几十万 token）。"""
    if isinstance(content, str):
        return [{"type": "text", "text": content}] if content else []
    if not isinstance(content, list):
        return [{"type": "text", "text": str(content)}] if content else []
    blocks = []
    for item in content:
        if not isinstance(item, dict):
            blocks.append({"type": "text", "text": str(item)})
            continue
        t = item.get("type", "")
        if t in ("input_text", "output_text", "text"):
            txt = item.get("text", "")
            if txt:
                blocks.append({"type": "text", "text": txt})
        elif t == "input_image":
            url = item.get("image_url", "") or item.get("url", "")
            if url.startswith("data:"):
                header, _, data = url.partition(",")
                media = "image/png"
                for m in ("image/jpeg", "image/jpg", "image/gif", "image/webp"):
                    if m in header:
                        media = m
                        break
                if data:
                    blocks.append({"type": "image",
                                   "source": {"type": "base64", "media_type": media, "data": data}})
            elif url:
                blocks.append({"type": "image", "source": {"type": "url", "url": url}})
    return blocks


# ── Responses API → Messages API 请求转换 ──────────────────────────

def _safe_parse_json(s):
    """安全解析 JSON 字符串，失败返回 {}"""
    if isinstance(s, dict):
        return s
    if not isinstance(s, str):
        return {}
    try:
        r = json.loads(s)
        # Responses API 的 arguments 有时是 list，取第一个 dict
        if isinstance(r, list) and r and isinstance(r[0], dict):
            return r[0]
        return r if isinstance(r, dict) else {}
    except (json.JSONDecodeError, ValueError):
        return {}


def _extract_text_from_content(content):
    """从 Responses API 的 content 数组提取纯文本"""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return str(content) if content else ""
    parts = []
    for part in content:
        if isinstance(part, dict) and part.get("type") in ("input_text", "output_text", "text"):
            t = part.get("text", "")
            if t:
                parts.append(t)
    return " ".join(parts)


def _append_tool_use(messages, call_item):
    """将 function_call 输入项追加为 assistant 消息中的 tool_use 块"""
    tool_block = {
        "type": "tool_use",
        "id": call_item.get("call_id", f"call_{len(messages)}"),
        "name": call_item.get("name", ""),
        "input": _safe_parse_json(call_item.get("arguments", "{}")),
    }
    # 如果最后一条是 assistant 且 content 是 list，追加
    if messages and messages[-1].get("role") == "assistant":
        c = messages[-1].get("content")
        if isinstance(c, list):
            c.append(tool_block)
            return
        elif isinstance(c, str) and c:
            messages[-1]["content"] = [{"type": "text", "text": c}, tool_block]
            return
    # 否则新建 assistant 消息
    messages.append({"role": "assistant", "content": [tool_block]})


def _append_tool_result(messages, output_item):
    """将 function_call_output 追加为 user 消息中的 tool_result 块"""
    raw_output = output_item.get("output", "")
    # output 可能是 str 或 list(含 input_image)；转成 Anthropic 块，避免 base64 当文本
    if isinstance(raw_output, list):
        content = _to_anthropic_content(raw_output)
    else:
        content = raw_output
    tool_result = {
        "type": "tool_result",
        "tool_use_id": output_item.get("call_id", ""),
        "content": content,
    }
    # 如果最后一条是 user 且 content 是 list，追加
    if messages and messages[-1].get("role") == "user":
        c = messages[-1].get("content")
        if isinstance(c, list):
            c.append(tool_result)
            return
    messages.append({"role": "user", "content": [tool_result]})


# apply_patch 的 patch 格式规则（GLM 不原生支持 FREEFORM 工具，靠描述教它）


def _convert_responses_to_messages(body):
    """将 OpenAI Responses API 请求转换为 Anthropic Messages API 请求。
    ccproxy-api 的转换器不处理 function_call/function_call_output/custom_tool_call，
    所以这里自己实现完整的转换（用于 Messages 路径）。"""
    messages = []
    system_parts = []

    # 提取 system prompt
    instructions = body.get("instructions")
    if isinstance(instructions, str) and instructions:
        system_parts.append(instructions)

    # 转换 input 数组
    input_items = body.get("input", [])
    if isinstance(input_items, str):
        messages.append({"role": "user", "content": input_items})
    elif isinstance(input_items, list):
        for item in input_items:
            if not isinstance(item, dict):
                continue
            item_type = item.get("type", "")
            if item_type == "message":
                role = item.get("role", "user")
                content = item.get("content", [])
                text = _extract_text_from_content(content)
                if role in ("system", "developer"):
                    if text:
                        system_parts.append(text)
                elif role == "user":
                    # 保留图片：转成 Anthropic image 块（无图时退化为纯文本）
                    blocks = _to_anthropic_content(content)
                    messages.append({"role": role, "content": blocks if blocks else text})
                elif role == "assistant":
                    messages.append({"role": role, "content": text})
            elif item_type == "function_call":
                _append_tool_use(messages, item)
            elif item_type == "function_call_output":
                _append_tool_result(messages, item)
            elif item_type == "custom_tool_call":
                # custom_tool_call → function_call（apply_patch 等）
                _append_tool_use(messages, {**item, "type": "function_call",
                                            "arguments": item.get("input", "")})
            elif item_type == "custom_tool_call_output":
                # custom_tool_call_output → function_call_output
                _append_tool_result(messages, item)

    # 构建输出
    result = {"model": body.get("model", "glm-5")}
    if system_parts:
        result["system"] = "\n\n".join(system_parts)
    result["messages"] = messages

    # 转换 tools
    tools_out = []
    for tool in body.get("tools", []):
        if not isinstance(tool, dict):
            continue
        t = tool.get("type", "")
        if t == "function":
            tools_out.append({
                "type": "custom",
                "name": tool.get("name", ""),
                "input_schema": tool.get("parameters", tool.get("input_schema", {})),
            })
        elif t == "custom":
            # apply_patch 等 custom/grammar 工具 → 转 function 让 GLM 能调用
            tools_out.append({
                "type": "custom",
                "name": tool.get("name", ""),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "patch": {"type": "string",
                                  "description": tool.get("description", "")},
                    },
                    "required": ["patch"],
                },
            })
        elif t == "namespace":
            # 展平 namespace 子工具
            ns_name = tool.get("name", "")
            for sub in tool.get("tools", []):
                sub_name = sub.get("name", "")
                tools_out.append({
                    "type": "custom",
                    "name": f"{ns_name}.{sub_name}" if ns_name else sub_name,
                    "input_schema": sub.get("parameters", sub.get("input_schema", {})),
                })
    if tools_out:
        result["tools"] = tools_out

    # 字段映射
    max_tokens = body.get("max_output_tokens")
    result["max_tokens"] = max_tokens if max_tokens else 16384
    if body.get("stream"):
        result["stream"] = True
    if body.get("temperature") is not None:
        result["temperature"] = body["temperature"]

    return result


def _convert_messages_error_to_responses(err_body):
    """将 Anthropic Messages API 错误格式转为 Responses API 错误格式"""
    try:
        r = json.loads(err_body)
        if r.get("type") == "error" and "error" in r:
            e = r["error"]
            return json.dumps({
                "error": {
                    "message": e.get("message", "unknown error"),
                    "type": e.get("type", "api_error"),
                    "code": str(e.get("type", "")),
                }
            }).encode()
    except Exception:
        pass
    return err_body



# ── apply_patch 转换 ──────────────────────────────────
def _is_retriable_conv(code):
    """converted 路径早期错误是否值得退避重试（1305 过载 / 5xx / 429）。"""
    if code is None:
        return False
    s = str(code)
    return s in {"1305", "overloaded", "429", "500", "502", "503", "529"} or "overload" in s.lower()


def _est_tokens(body):
    """估算请求 token 数：剔除 base64 图片数据（图片按分辨率约 1-3K token，不按 base64 字节长度算）。
    避免 converted 路径图片请求的 base64 把字节估算撑到虚高（~840K/张）导致误判上下文超限、
    挡住 failover。"""
    import re
    try:
        s = json.dumps(body, ensure_ascii=False)
    except Exception:
        return 0
    n_images = s.count("data:image/") + s.count('"type":"image"')
    # 剔除 data URL 中的 base64
    s = re.sub(r'data:image/[^;]*;base64,[A-Za-z0-9+/=\s]+', 'data:img', s)
    # 剔除 Anthropic image block 中的 data 字段（长 base64 串）
    s = re.sub(r'"data":"[A-Za-z0-9+/=]{80,}"', '"data":""', s)
    return len(s.encode()) / 3.5 + n_images * 1500


# ── HTTP 处理 ─────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def do_GET(self):
        try:
            self._route("GET")
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            log.warning("client disconnected")

    def do_POST(self):
        try:
            self._route("POST")
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            log.warning("client disconnected")
        except Exception as e:
            log.error("unexpected error: %s", e)
            traceback.print_exc()

    # ── 统一路由 ─────────────────────────────────────
    def _ms(self):
        """本次请求耗时（毫秒）"""
        return int((time.monotonic() - getattr(self, '_req_start', time.monotonic())) * 1000)

    def _route(self, method):
        self._req_id = next(_req_counter)  # 请求序号（日志关联用）
        self._req_start = time.monotonic()  # 请求开始时间（耗时统计用）
        # 0) 拦截错误的客户端配置（base URL 多了 /v1 导致路径重复）
        if self.path.startswith("/v1/v1/"):
            log.warning(">>> malformed path %s (double /v1), rejecting", self.path)
            err = json.dumps({"error": {"message": "Bad path: base URL should not include /v1. "
                                     "Remove /v1 from your API base URL setting.",
                                     "type": "invalid_request_error"}}).encode()
            self._send_raw(400, err, "application/json")
            return

        # 1) GET /v1/models → 静态返回（忽略查询参数，如 ?client_version=）
        path_only = self.path.split("?")[0].rstrip("/")
        if method == "GET" and path_only == "/v1/models":
            self._send_raw(200, STATIC_MODELS, "application/json")
            return

        # 2) 读取请求体
        payload = None
        is_stream = False
        is_responses = method == "POST" and "/v1/responses" in self.path
        is_messages = method == "POST" and "/v1/messages" in self.path
        body = None

        if method == "POST":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            body = json.loads(raw)
            is_stream = body.get("stream", False)
            self._debug_req_body = body

        # 3) 日志 + 计算 payload 大小
        raw_len = len(raw) if method == "POST" else 0
        client_ip = (self.headers.get("X-Forwarded-For", "").split(",")[0].strip()
                     or self.headers.get("X-Real-IP", "")
                     or self.client_address[0])
        if is_responses:
            log.info("[#%d] >>> [%s] POST %s stream=%s tools=%d input=%d",
                     self._req_id, client_ip, self.path, is_stream, len(body.get("tools", [])),
                     len(body.get("input", [])))
        else:
            log.info("[#%d] >>> [%s] %s %s", self._req_id, client_ip, method, self.path)

        # 4) 遍历上游，自动回退（不截断——客户端会自动压缩，错误都是实际错误）
        last_err = None
        body_saved = False  # 调试body只保存一次（首次失败时）

        # 客户端发特定 key（"3"）强制走 official
        client_key = self.headers.get("Authorization", "").replace("Bearer ", "").strip()
        # key 路由：1=内网千问, 2=外部中转(codex→external-openai, claude→external-claude), 3=官方
        force_channel = None
        if client_key == "1":
            force_channel = "internal"
        elif client_key == "2":
            force_channel = "external"
        elif client_key == "3":
            force_channel = "official"

        # 模型名路由（优先于 key）：模型名严格匹配 internal 渠道配置的 model → 走内网千问
        if isinstance(body, dict):
            req_model = body.get("model") or ""
            internal_model = next((up["model"] for up in UPSTREAMS if up.get("name") == "internal"), None)
            if internal_model and req_model == internal_model:
                force_channel = "internal"

        hour = datetime.now().hour
        weekday = datetime.now().weekday()  # 0=Mon, 6=Sun
        is_worktime = weekday < 5 and 9 <= hour < 18  # 工作日 9:00-18:00

        # 检测图片：有图片强制走 Messages（Completions 不支持图片，Messages 支持）
        has_images = is_responses and isinstance(body, dict) and _request_has_images(body)
        use_completions = RESPONSES_USE_COMPLETIONS and not has_images

        # 调试日志：路由决策完整信息（必须在 use_completions 定义之后）
        needs_completions = is_responses and use_completions
        needs_messages = is_messages or (is_responses and not use_completions)
        blocked_active = {k: datetime.fromtimestamp(v).strftime("%H:%M") for k, v in _channel_blocked_until.items() if v > time.time()}
        log.info("[#%d] ROUTE: key=%s model=%s force=%s worktime=%s blocked=%s needs_comp=%s needs_msg=%s",
                 self._req_id, client_key[:20] or "(empty)", req_model or "?",
                 force_channel, is_worktime, blocked_active or "{}",
                 needs_completions, needs_messages)
        if has_images:
            log.info("    [image] 含图片 → 走 Messages 路径")

        for up in UPSTREAMS:
            if up.get("disabled"):
                continue
            # key 路由：强制指定渠道时只走对应渠道
            if force_channel == "internal" and up["name"] != "internal":
                continue
            if force_channel == "official" and up["name"] != "official":
                continue
            if force_channel == "external":
                # codex(/v1/responses→completions)走 external-openai, claude(/v1/messages)走 external-claude
                if needs_completions and up["name"] != "external-openai":
                    continue
                if needs_messages and up["name"] != "external-claude":
                    continue
            if not force_channel and up.get("worktime_only") and not is_worktime:
                continue
            # 渠道封锁检查：429 限额封锁期内跳过 external 和 official
            if not force_channel:
                block_key = "external" if up["name"].startswith("external") else up["name"]
                if _channel_blocked_until.get(block_key, 0) > time.time():
                    log.info("[#%d]     [skip] %s blocked until %s",
                             self._req_id, up["name"],
                             datetime.fromtimestamp(_channel_blocked_until[block_key]).strftime("%H:%M"))
                    continue
            # 能力检查：渠道必须支持本次请求需要的端点类型
            if needs_completions and "openai_url" not in up:
                continue
            if needs_messages and "anthropic_url" not in up:
                continue

            # 构建目标 URL 和请求头（每个上游只需一次）
            is_responses_converted = False
            if is_responses and not use_completions and "anthropic_url" in up and HAS_CCPROXY:
                # Responses API → Anthropic Messages 直连（ccproxy 路径）
                url = up["anthropic_url"].rstrip("/")
                mkey = up["key"]  # 统一用渠道 key（external-claude 拆分后单一 key）
                auth = up.get("anthropic_auth", "x-api-key")
                if auth == "bearer":
                    auth_header = {"Authorization": f"Bearer {mkey}"}
                else:
                    auth_header = {"x-api-key": mkey, "anthropic-version": "2023-06-01"}
                up_headers = {
                    "Content-Type": "application/json",
                    "Connection": "close",
                    **auth_header,
                }
                is_responses_converted = True
            elif is_responses:
                # Responses API → codex-relay（Chat Completions 路径，OpenAI 原生）
                url = f"http://127.0.0.1:{up['relay_port']}{self.path}"
                up_headers = {
                    "Content-Type": "application/json",
                    "Authorization": self.headers.get("Authorization", ""),
                }
            elif is_messages and "anthropic_url" in up:
                # Messages API → Anthropic 端点
                url = up["anthropic_url"].rstrip("/")
                mkey = up["key"]  # 统一用渠道 key（external-claude 拆分后单一 key）
                auth = up.get("anthropic_auth", "x-api-key")
                if auth == "bearer":
                    auth_header = {"Authorization": f"Bearer {mkey}"}
                else:
                    auth_header = {"x-api-key": mkey, "anthropic-version": "2023-06-01"}
                up_headers = {
                    "Content-Type": "application/json",
                    "Connection": "close",
                    **auth_header,
                }
            else:
                api_path = self.path
                if api_path.startswith("/v1/"):
                    api_path = api_path[3:]
                url = up["openai_url"].rstrip("/") + api_path
                up_headers = {
                    "Authorization": f"Bearer {up['key']}",
                    "Connection": "close",
                }

            # 修复 tool_use input 为 list 的非标准格式（大 payload 时触发上游 500/502）
            if body and is_messages:
                fixed = 0
                for m in body.get("messages", []):
                    c = m.get("content", "")
                    if isinstance(c, list):
                        for block in c:
                            if block.get("type") == "tool_use" and isinstance(block.get("input"), list):
                                old = block["input"]
                                block["input"] = old[0] if old and isinstance(old[0], dict) else {}
                                fixed += 1
                if fixed:
                    log.info("    fixed %d tool_use inputs (list→dict)", fixed)


            # 生成 payload（每个 upstream 只尝试一次，不截断重试——客户端会自动压缩）
            payload = None
            if body is not None:
                body["model"] = up["model"]
                if is_responses and body.get("previous_response_id"):
                    pid_len = len(json.dumps(body, ensure_ascii=False))
                    if pid_len > 200000:
                        log.warning("    payload %dKB, stripping previous_response_id", pid_len // 1024)
                        del body["previous_response_id"]
                if is_responses_converted:
                    converted = _convert_responses_to_messages(body)
                    converted["model"] = up.get("messages_model", up["model"])
                    payload = json.dumps(converted).encode()
                    log.info("    [converted] %d msgs, %d tools, %dKB, max_tokens=%d (client max_output=%s)",
                             len(converted.get("messages", [])),
                             len(converted.get("tools", [])),
                             len(payload) // 1024,
                             converted.get("max_tokens", 0),
                             body.get("max_output_tokens"))
                else:
                    # codex-relay 路径：codex-relay 0.5.1+ 原生处理 custom tool 转换，无需预处理
                    payload = json.dumps(body).encode()
                if "Content-Type" not in up_headers and payload is not None:
                    up_headers["Content-Type"] = "application/json"

            log.info("[#%d]     -> %s", self._req_id, up["name"])

            try:
                # 官方 API 限速
                if up["name"] == "official":
                    _official_limiter.acquire()
                req = Request(url, data=payload, headers=up_headers, method=method)
                resp = urlopen(req, timeout=REQUEST_TIMEOUT)

                if is_stream:
                    if is_responses_converted:
                        # Responses→Messages 转换路径：增量流式 + 早期错误退避重试 + 错误转发
                        cres = self._converted_stream_with_retry(resp, up, url, up_headers, payload, method)
                        if cres.get("done"):
                            return
                        # 真空输出（无错误）→ 尝试下一个上游
                    elif is_responses:
                        # relay 路径：立即发头+keepalive（idle安全），早期 1305/过载退避重试
                        events, has_output, stream_error = self._relay_stream_with_retry(
                            resp, up, url, up_headers, payload, method)
                        if stream_error:
                            log.warning("[#%d]     !!! %s upstream error, forwarding to client: %s",
                                        self._req_id, up["name"], stream_error)
                        return  # 已发响应头，总是 done
                    elif is_messages:
                        # 增量流式：已发送响应头并边收边发，直接返回（不再回退/不发400）
                        self._messages_stream(resp, up["name"])
                        return
                    else:
                        self._pipe_stream(resp, up["name"])
                        return
                    # 空输出：不截断，尝试下一个上游
                    if not body_saved:
                        self._save_debug_body(body)
                        body_saved = True
                    log.warning("[#%d]     !!! %s empty output, trying next upstream", self._req_id, up["name"])
                    continue
                else:
                    result = resp.read()
                    # Responses→Messages 转换路径：非流式响应转换
                    if is_responses_converted:
                        try:
                            anthropic_resp = json.loads(result)
                            # 检查是否是 Anthropic 错误响应
                            if anthropic_resp.get("type") == "error":
                                result = _convert_messages_error_to_responses(result)
                                self._send_raw(resp.status, result, "application/json")
                                return
                            # 用 ccproxy 转换
                            msg_resp = MessageResponse(**anthropic_resp)
                            openai_resp = convert__anthropic_message_to_openai_responses__response(msg_resp)
                            result = json.dumps(openai_resp.model_dump(exclude_none=True, mode='json'),
                                                ensure_ascii=False).encode()
                            output = openai_resp.output if hasattr(openai_resp, 'output') else []
                            if not output:
                                if not body_saved:
                                    self._save_debug_body(body)
                                    body_saved = True
                                log.warning("[#%d]     !!! %s empty output, trying next upstream", self._req_id, up["name"])
                                continue
                            log.info("[#%d]     <<< %s [converted] OK (%dms)", self._req_id, up["name"], self._ms())
                        except Exception as ce:
                            log.error("[#%d]     !!! [converted] response error: %s", self._req_id, ce)
                        self._send_raw(200, result, "application/json")
                        return
                    # 检测空输出
                    empty = False
                    try:
                        r = json.loads(result)
                        output = r.get("output", [])
                        if is_responses and not output and body:
                            empty = True
                        # 记录上游 token 用量
                        usage = r.get("usage", {})
                        if usage:
                            inp = usage.get("input_tokens", 0)
                            out = usage.get("output_tokens", 0)
                            log.info("    usage: input=%d output=%d total=%d",
                                     inp, out, inp + out)
                    except Exception:
                        pass
                    if empty:
                        if not body_saved:
                            self._save_debug_body(body)
                            body_saved = True
                        log.warning("[#%d]     !!! %s empty output, trying next upstream", self._req_id, up["name"])
                        continue  # 尝试下一个上游
                    try:
                        self._send_raw(resp.status, result,
                                       resp.headers.get("Content-Type", "application/json"))
                    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                        log.warning("client disconnected during response")
                        return
                    log.info("[#%d]     <<< %s OK (%dms)", self._req_id, up["name"], self._ms())
                    return
            except HTTPError as e:
                err_body = e.read()
                last_err = (e.code, err_body)
                log.error("[#%d]     !!! %s HTTP %d: %s", self._req_id, up["name"], e.code, err_body[:300].decode(errors="replace"))
                # 429 rate_limit → 解析重置时间，封锁 official+external（HTTP 错误，所有路径都经过此处）
                if e.code == 429:
                    log.info("[#%d]     [429] HTTPError 429 from %s, calling _block_channel_on_429", self._req_id, up["name"])
                    _block_channel_on_429(err_body, up["name"], self._req_id)
                    log.info("[#%d]     [429] after block: %s", self._req_id,
                             {k: datetime.fromtimestamp(v).strftime("%H:%M") for k, v in _channel_blocked_until.items() if v > time.time()})
                # 502/500 可能是上下文超限，返回标准错误触发客户端压缩
                if e.code in (500, 502) and body and (is_responses or is_messages):
                    max_ctx = up.get("max_context_tokens", 200000)
                    est_tokens = _est_tokens(body)  # 剔除 base64 图片，避免虚高
                    if est_tokens > max_ctx * 0.9:
                        self._send_context_exceeded(body, up)
                        return
                continue  # 尝试下一个上游（不截断重试）
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
                log.error("[#%d]     !!! %s connection reset: %s", self._req_id, up["name"], e)
                last_err = e
                continue
            except Exception as e:
                last_err = e
                log.error("[#%d]     !!! %s error: %s", self._req_id, up["name"], e)
                continue

        # 所有上游都失败
        if body and (is_responses or is_messages) and not body_saved:
            self._save_debug_body(body)
        self._send_last_error(last_err)

    # ── 流式透传（非 Responses 路径）──
    def _pipe_stream(self, resp, upstream_name):
        """简单透传，不解析 SSE"""
        try:
            self.send_response(200)
            for h in ["Content-Type", "Cache-Control"]:
                v = resp.headers.get(h)
                if v:
                    self.send_header(h, v)
            self.send_header("Connection", "close")
            self.end_headers()
            total = 0
            while True:
                chunk = resp.read(4096)
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
                total += len(chunk)
            self.close_connection = True
            size = f"{total // 1024}KB" if total >= 1024 else f"{total}B"
            log.info("[#%d]     <<< %s STREAM OK (%s, %dms)", self._req_id, upstream_name, size, self._ms())
        except (ConnectionResetError, BrokenPipeError):
            log.warning("[#%d]     <<< %s STREAM interrupted", self._req_id, upstream_name)

    # ── Messages API 流式处理（缓冲 + 空输出检测 + usage）──
    def _messages_stream(self, resp, upstream_name):
        """增量流式转发 Messages API SSE（边收边发，避免客户端超时）。
        遇 error 块就地合成正常结束。返回 (has_output, usage_dict, context_exceeded, stream_error)"""
        has_output = False
        context_exceeded = False
        stream_error = False
        saw_message_stop = False
        last_usage = {}
        open_indices = []
        skip_indices = set()  # 客户端不支持的 server_tool_use 等内容块 index，转发时跳过
        total_bytes = 0
        block_count = 0
        size = 0
        try:
            # 立即发送响应头，开始增量流式（避免客户端长时间等数据超时断开）
            self.send_response(200)
            for h in ["Content-Type", "Cache-Control"]:
                v = resp.headers.get(h)
                if v:
                    self.send_header(h, v)
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            buf = b""
            while True:
                chunk = resp.read(4096)
                if not chunk:
                    break
                total_bytes += len(chunk)
                buf += chunk
                # SSE 分隔符兼容 \n\n 和 \r\n\r\n
                while b"\n\n" in buf or b"\r\n\r\n" in buf:
                    if b"\r\n\r\n" in buf:
                        block, buf = buf.split(b"\r\n\r\n", 1)
                    else:
                        block, buf = buf.split(b"\n\n", 1)
                    block = block.replace(b"\r\n", b"\n")
                    block_count += 1

                    # 检测 error 事件（如 1234 overloaded）
                    if b"event: error" in block:
                        try:
                            text = block.decode("utf-8", errors="replace")
                            err_code, err_msg, data_raw = None, None, None
                            for line in text.split("\n"):
                                if line.startswith("data: "):
                                    data_raw = line[6:]
                                    p = json.loads(data_raw)
                                    err = p.get("error", p)  # 兼容 {error:{}} 和顶层
                                    err_code = err.get("code") if isinstance(err, dict) else None
                                    err_msg = err.get("message", "") if isinstance(err, dict) else str(err)
                            log.warning("[#%d] [messages] %s stream error: code=%s msg=%s | has_output=%s bytes=%d",
                                        self._req_id, upstream_name, err_code, (err_msg or "")[:150],
                                        has_output, total_bytes)
                            log.warning("[#%d] [messages] raw error block: %s",
                                        self._req_id, repr(text[:500]))
                        except Exception as pe:
                            log.warning("[#%d] [messages] %s stream error (parse failed: %s), raw: %s",
                                        self._req_id, upstream_name, pe,
                                        repr(block.decode("utf-8", errors="replace")[:500]))
                        stream_error = True
                        break  # 错误后由收尾逻辑合成正常结束

                    # 校验 data JSON 合法性，避免转发畸形块导致客户端解码失败
                    if (b"\ndata: " in block or block.startswith(b"data: ")) and b"event: ping" not in block:
                        malformed = False
                        for line in block.decode("utf-8", errors="replace").split("\n"):
                            if line.startswith("data: "):
                                try:
                                    json.loads(line[6:])
                                except Exception:
                                    malformed = True
                                    break
                        if malformed:
                            log.warning("[#%d] [messages] %s dropping malformed block: %s",
                                        self._req_id, upstream_name, repr(block[:200]))
                            continue

                    # 过滤客户端不支持的 server_tool_use 等服务端工具内容块
                    # （GLM 偶发产出，Claude Code 不认 → "Unsupported content type"）
                    _SERVER_TOOL_TYPES = {
                        "server_tool_use", "web_search_tool_result",
                        "code_execution_tool_use", "code_execution_tool_result",
                        "computer_tool_use", "computer_tool_result",
                        "bash_tool_use", "bash_tool_result",
                        "text_editor_tool_use", "text_editor_tool_result",
                    }
                    try:
                        _elines = block.decode("utf-8", errors="replace").split("\n")
                        _etype = next((l[7:].strip() for l in _elines if l.startswith("event: ")), None)
                        _dj = None
                        for _l in _elines:
                            if _l.startswith("data: "):
                                _dj = json.loads(_l[6:]); break
                        if _etype == "content_block_start" and isinstance(_dj, dict):
                            _cbt = (_dj.get("content_block") or {}).get("type", "")
                            if _cbt in _SERVER_TOOL_TYPES:
                                skip_indices.add(_dj.get("index", 0))
                                log.warning("[#%d] [messages] %s dropping unsupported content_block type=%s idx=%s",
                                            self._req_id, upstream_name, _cbt, _dj.get("index"))
                                continue
                        elif _etype in ("content_block_delta", "content_block_stop") and isinstance(_dj, dict):
                            if _dj.get("index", 0) in skip_indices:
                                if _etype == "content_block_stop":
                                    skip_indices.discard(_dj.get("index", 0))
                                continue
                    except Exception:
                        pass

                    # 增量转发该块
                    out = block + b"\n\n"
                    self.wfile.write(out); self.wfile.flush()
                    size += len(out)
                    if b"content_block_delta" in block:
                        has_output = True
                    if b'"content_block_start"' in block:
                        try:
                            for line in block.decode("utf-8", errors="replace").split("\n"):
                                if line.startswith("data: "):
                                    open_indices.append(json.loads(line[6:]).get("index", 0))
                        except: pass
                    elif b'"content_block_stop"' in block:
                        try:
                            for line in block.decode("utf-8", errors="replace").split("\n"):
                                if line.startswith("data: "):
                                    idx = json.loads(line[6:]).get("index", -1)
                                    if idx in open_indices: open_indices.remove(idx)
                        except: pass
                    elif b"message_stop" in block:
                        saw_message_stop = True
                    elif b"message_delta" in block:
                        try:
                            for line in block.decode("utf-8", errors="replace").split("\n"):
                                if line.startswith("data: "):
                                    p = json.loads(line[6:])
                                    if p.get("usage"): last_usage = p["usage"]
                                    if p.get("delta", {}).get("stop_reason") == "model_context_window_exceeded":
                                        context_exceeded = True
                        except: pass
                if stream_error:
                    break  # 错误后退出外层 chunk 循环
            # 上游流不完整（EOF 但未见 message_stop，如中转中途断流）→ 合成正常收尾，
            # 否则客户端会 "stream disconnected / error decoding response body"
            if not saw_message_stop:
                log.warning("[#%d] [messages] %s upstream incomplete (no message_stop), synthesizing close (open=%d, err=%s, %dKB)",
                            self._req_id, upstream_name, len(open_indices), stream_error, total_bytes // 1024)
                for idx in open_indices:
                    self.wfile.write(("event: content_block_stop\ndata: {\"type\": \"content_block_stop\", \"index\": " + str(idx) + "}\n\n").encode())
                self.wfile.write(b'event: message_delta\ndata: {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 0}}\n\nevent: message_stop\ndata: {"type": "message_stop"}\n\n')
                self.wfile.flush()
            size_str = (str(size // 1024) + "KB") if size >= 1024 else (str(size) + "B")
            if last_usage:
                log.info("[#%d]     <<< %s STREAM OK (%s, %dms) usage: input=%d output=%d total=%d",
                         self._req_id, upstream_name, size_str, self._ms(), last_usage.get("input_tokens", 0),
                         last_usage.get("output_tokens", 0),
                         last_usage.get("input_tokens", 0) + last_usage.get("output_tokens", 0))
            else:
                log.info("[#%d]     <<< %s STREAM OK (%s, %dms)", self._req_id, upstream_name, size_str, self._ms())
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            log.warning("[#%d]     <<< %s STREAM interrupted", self._req_id, upstream_name)
        except Exception as e:
            # 任何其他异常：合成干净收尾，避免客户端解码失败
            log.error("[#%d] [messages] %s stream exception: %s — synthesizing clean close",
                      self._req_id, upstream_name, e)
            try:
                for idx in open_indices:
                    self.wfile.write(("event: content_block_stop\ndata: {\"type\": \"content_block_stop\", \"index\": " + str(idx) + "}\n\n").encode())
                if not saw_message_stop:
                    self.wfile.write(b'event: message_delta\ndata: {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 0}}\n\nevent: message_stop\ndata: {"type": "message_stop"}\n\n')
                self.wfile.flush()
            except Exception:
                pass

        return has_output, last_usage, context_exceeded, stream_error

    # ── Responses→Messages 转换流式处理（旧：整段缓冲，留作兜底）──
    def _converted_stream_sync(self, resp, upstream_name, _write, _emit, stop_ka):
        """同步翻译 Anthropic Messages SSE → OpenAI Responses SSE，边收边发。
        纯同步、无 worker 线程/async loop，不可能挂起；收到 message_stop 就地发 response.completed。
        返回 (done, early_err)，early_err=(code,msg) 为真实输出前的上游错误（供上层重试）。"""
        seq = [0]
        def nseq():
            seq[0] += 1
            return seq[0]
        rid = [None]
        model = [None]
        created_at = [int(time.time())]
        usage = [{}]
        blocks = {}        # anthropic content_block index -> 状态 dict
        output_items = []  # response.completed 的 output 数组
        created_sent = [False]
        has_output = [False]

        def _resp_obj(status):
            # Responses usage 必须含 total_tokens（Codex 严格解析，缺失会 "failed to parse ResponseCompleted"）
            u = dict(usage[0])
            inp = u.get("input_tokens", 0) or 0
            out = u.get("output_tokens", 0) or 0
            u["input_tokens"] = inp
            u["output_tokens"] = out
            u["total_tokens"] = inp + out
            return {"id": rid[0] or "resp_syn", "object": "response", "created_at": created_at[0],
                    "status": status, "model": model[0] or "glm-5.2", "output": output_items, "usage": u}

        def _emit_created():
            if created_sent[0]:
                return
            ro = _resp_obj("in_progress")
            _emit({"sequence_number": nseq(), "type": "response.created", "response": ro})
            _emit({"sequence_number": nseq(), "type": "response.in_progress", "response": ro})
            created_sent[0] = True

        def _on_event(et, d):
            # 返回 "done" / ("error",code,msg) / None
            if et == "message_start":
                msg = d.get("message", {}) or {}
                rid[0] = msg.get("id")
                model[0] = msg.get("model")
                u = msg.get("usage", {}) or {}
                if u:
                    usage[0].update(u)
                _emit_created()
            elif et == "content_block_start":
                _emit_created()
                idx = d.get("index", 0)
                cb = d.get("content_block", {}) or {}
                btype = cb.get("type")
                oi = idx
                if btype == "text":
                    iid = "msg_" + (rid[0] or "x")
                    blocks[idx] = {"type": "text", "iid": iid, "oi": oi, "text": ""}
                    _emit({"sequence_number": nseq(), "type": "response.output_item.added", "output_index": oi,
                           "item": {"id": iid, "type": "message", "status": "in_progress", "role": "assistant", "content": []}})
                    _emit({"sequence_number": nseq(), "type": "response.content_part.added", "item_id": iid,
                           "output_index": oi, "content_index": 0, "part": {"type": "output_text", "text": "", "annotations": []}})
                elif btype == "thinking":
                    iid = "rs_" + (rid[0] or "x")
                    blocks[idx] = {"type": "thinking", "iid": iid, "oi": oi, "text": ""}
                    _emit({"sequence_number": nseq(), "type": "response.output_item.added", "output_index": oi,
                           "item": {"id": iid, "type": "reasoning", "status": "in_progress", "summary": []}})
                    _emit({"sequence_number": nseq(), "type": "response.reasoning_summary_part.added", "item_id": iid,
                           "output_index": oi, "summary_index": 0, "part": {"type": "summary_text", "text": ""}})
                elif btype == "tool_use":
                    call_id = cb.get("id", "call_x")
                    name = cb.get("name", "")
                    iid = "fc_" + call_id
                    blocks[idx] = {"type": "tool_use", "iid": iid, "oi": oi, "call_id": call_id, "name": name, "args": ""}
                    # apply_patch 缓冲到 stop 时合成 custom_tool_call，此处不发 function_call 事件
                    if name != "apply_patch":
                        _emit({"sequence_number": nseq(), "type": "response.output_item.added", "output_index": oi,
                               "item": {"id": iid, "type": "function_call", "status": "in_progress", "call_id": call_id, "name": name}})
            elif et == "content_block_delta":
                idx = d.get("index", 0)
                b = blocks.get(idx)
                if not b:
                    return None
                delta = d.get("delta", {}) or {}
                dt = delta.get("type")
                if dt == "text_delta":
                    t = delta.get("text", "")
                    b["text"] += t
                    _emit({"sequence_number": nseq(), "type": "response.output_text.delta", "item_id": b["iid"],
                           "output_index": b["oi"], "content_index": 0, "delta": t})
                    has_output[0] = True; stop_ka.set()
                elif dt == "thinking_delta":
                    t = delta.get("thinking", "")
                    b["text"] += t
                    _emit({"sequence_number": nseq(), "type": "response.reasoning_summary_text.delta", "item_id": b["iid"],
                           "output_index": b["oi"], "summary_index": 0, "delta": t})
                    has_output[0] = True; stop_ka.set()
                elif dt == "input_json_delta":
                    t = delta.get("partial_json", "")
                    b["args"] += t
                    # apply_patch 的 args 缓冲（不发 delta，stop 时一次性合成 custom_tool_call）
                    if b.get("name") != "apply_patch":
                        _emit({"sequence_number": nseq(), "type": "response.function_call_arguments.delta", "item_id": b["iid"],
                               "output_index": b["oi"], "delta": t})
                        has_output[0] = True; stop_ka.set()
            elif et == "content_block_stop":
                idx = d.get("index", 0)
                b = blocks.get(idx)
                if not b:
                    return None
                if b["type"] == "text":
                    _emit({"sequence_number": nseq(), "type": "response.output_text.done", "item_id": b["iid"],
                           "output_index": b["oi"], "content_index": 0, "text": b["text"]})
                    _emit({"sequence_number": nseq(), "type": "response.content_part.done", "item_id": b["iid"],
                           "output_index": b["oi"], "content_index": 0, "part": {"type": "output_text", "text": b["text"], "annotations": []}})
                    item = {"id": b["iid"], "type": "message", "status": "completed", "role": "assistant",
                            "content": [{"type": "output_text", "text": b["text"], "annotations": []}]}
                    _emit({"sequence_number": nseq(), "type": "response.output_item.done", "output_index": b["oi"], "item": item})
                    output_items.append(item)
                elif b["type"] == "thinking":
                    _emit({"sequence_number": nseq(), "type": "response.reasoning_summary_part.done", "item_id": b["iid"],
                           "output_index": b["oi"], "summary_index": 0, "part": {"type": "summary_text", "text": b["text"]}})
                    item = {"id": b["iid"], "type": "reasoning", "status": "completed",
                            "summary": [{"type": "summary_text", "text": b["text"]}]}
                    _emit({"sequence_number": nseq(), "type": "response.output_item.done", "output_index": b["oi"], "item": item})
                    output_items.append(item)
                elif b["type"] == "tool_use":
                    if b.get("name") == "apply_patch":
                        raw_args = b["args"]
                        try:
                            parsed = json.loads(raw_args)
                            patch_text = parsed.get("patch", raw_args) if isinstance(parsed, dict) else raw_args
                        except Exception:
                            patch_text = raw_args
                        if not patch_text.startswith("*** Begin Patch"):
                            patch_text = "*** Begin Patch\n" + patch_text
                        if not patch_text.rstrip().endswith("*** End Patch"):
                            patch_text = patch_text.rstrip() + "\n*** End Patch"
                        ctc_id = "ctc_" + b["call_id"]
                        s = nseq()
                        _emit({"sequence_number": s, "type": "response.output_item.added", "output_index": b["oi"],
                               "item": {"id": ctc_id, "type": "custom_tool_call", "status": "in_progress", "call_id": b["call_id"], "name": "apply_patch"}})
                        for ck in [patch_text[i:i+20] for i in range(0, len(patch_text), 20)]:
                            s += 1
                            _emit({"sequence_number": s, "type": "response.custom_tool_call_input.delta",
                                   "delta": ck, "item_id": ctc_id, "output_index": b["oi"]})
                        s += 1
                        _emit({"sequence_number": s, "type": "response.custom_tool_call_input.done",
                               "input": patch_text, "item_id": ctc_id, "output_index": b["oi"]})
                        s += 1
                        item = {"id": ctc_id, "type": "custom_tool_call", "status": "completed",
                                "call_id": b["call_id"], "name": "apply_patch", "input": patch_text}
                        _emit({"sequence_number": s, "type": "response.output_item.done", "output_index": b["oi"], "item": item})
                        output_items.append(item)
                        has_output[0] = True
                    else:
                        _emit({"sequence_number": nseq(), "type": "response.function_call_arguments.done",
                               "item_id": b["iid"], "output_index": b["oi"], "arguments": b["args"]})
                        item = {"id": b["iid"], "type": "function_call", "status": "completed",
                                "call_id": b["call_id"], "name": b["name"], "arguments": b["args"]}
                        _emit({"sequence_number": nseq(), "type": "response.output_item.done", "output_index": b["oi"], "item": item})
                        output_items.append(item)
            elif et == "message_delta":
                u = d.get("usage", {}) or {}
                if u:
                    usage[0].update(u)
            elif et == "message_stop":
                _emit_created()
                _emit({"sequence_number": nseq(), "type": "response.completed", "response": _resp_obj("completed")})
                return "done"
            elif et == "error":
                err = d.get("error", d)
                code = err.get("code") if isinstance(err, dict) else None
                msg = (err.get("message", "") if isinstance(err, dict) else str(err))
                return ("error", code, msg)
            return None

        buf = b""
        done = False
        early_err = None
        try:
            while not done and early_err is None:
                chunk = resp.read(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n\n" in buf or b"\r\n\r\n" in buf:
                    if b"\r\n\r\n" in buf:
                        block, buf = buf.split(b"\r\n\r\n", 1)
                    else:
                        block, buf = buf.split(b"\n\n", 1)
                    block = block.replace(b"\r\n", b"\n")
                    et, dj = None, None
                    for line in block.decode("utf-8", errors="replace").split("\n"):
                        if line.startswith("event: "):
                            et = line[7:].strip()
                        elif line.startswith("data: "):
                            try:
                                dj = json.loads(line[6:])
                            except Exception:
                                pass
                    if not et or not dj:
                        continue
                    r = _on_event(et, dj)
                    if r == "done":
                        done = True
                        break
                    if isinstance(r, tuple) and r[0] == "error":
                        if not has_output[0]:
                            early_err = (r[1], r[2])
                        else:
                            _emit({"sequence_number": nseq(), "type": "response.failed",
                                   "response": {"error": {"message": (r[2] or "upstream error"), "code": r[1], "type": "upstream_error"}}})
                            done = True
                        break
            # 上游 EOF 但未发 message_stop：合成完成（避免客户端干等，这是治"响应结束仍等待"的关键）
            if not done and early_err is None:
                log.warning("[#%d] [converted] %s upstream EOF without message_stop, synthesizing completed",
                            self._req_id, upstream_name)
                _emit_created()
                _emit({"sequence_number": nseq(), "type": "response.completed", "response": _resp_obj("completed")})
                done = True
            log.info("[#%d]     <<< %s [converted] STREAM OK (sync, %dms, out=%d)",
                     self._req_id, upstream_name, self._ms(), len(output_items))
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            log.warning("[#%d]     <<< %s [converted] STREAM interrupted", self._req_id, upstream_name)
        return done, early_err

    def _converted_stream_with_retry(self, first_resp, up, url, up_headers, payload, method):
        """converted 路径：立即发响应头 + keepalive（防 TTFT 期 idle 超时），同步翻译
        Anthropic→Responses（无 async，不可能挂），早期错误退避重试，用尽则流内转发。
        返回 {done: bool}（已发响应头，总是 done）。"""
        import threading
        upstream_name = up["name"]
        MAX = 2
        wlock = threading.Lock()
        stop_ka = threading.Event()
        def _write(data):
            with wlock:
                self.wfile.write(data); self.wfile.flush()
        def _emit(ed):
            _write((f"event: {ed.get('type', '')}\ndata: {json.dumps(ed, ensure_ascii=False)}\n\n").encode())
        def _keepalive():
            while not stop_ka.wait(5):
                try:
                    _write(b": keepalive\n\n")
                except Exception:
                    break
        # 立即发响应头进入 SSE 模式 + 启动 keepalive
        self.send_response(200)
        for h in ["Content-Type", "Cache-Control"]:
            v = first_resp.headers.get(h)
            if v:
                self.send_header(h, v)
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        kat = threading.Thread(target=_keepalive, daemon=True)
        kat.start()
        resp = first_resp
        try:
            for attempt in range(MAX + 1):
                done, early_err = self._converted_stream_sync(resp, upstream_name, _write, _emit, stop_ka)
                if done:
                    return {"done": True}
                if early_err:
                    code, msg = early_err
                    if attempt < MAX and _is_retriable_conv(code):
                        wait = min(2 ** attempt, 4)
                        log.warning("[#%d] [converted] %s early error code=%s, retry %d/%d in %ds",
                                    self._req_id, upstream_name, code, attempt + 1, MAX, wait)
                        try:
                            resp.close()
                        except Exception:
                            pass
                        time.sleep(wait)  # 期间 keepalive 持续保活
                        try:
                            if upstream_name == "official":
                                _official_limiter.acquire()
                            resp = urlopen(Request(url, data=payload, headers=up_headers, method=method),
                                           timeout=REQUEST_TIMEOUT)
                        except Exception as oe:
                            log.error("[#%d] [converted] reopen failed: %s", self._req_id, oe)
                            _emit({"sequence_number": 0, "type": "response.failed",
                                   "response": {"error": {"message": str(oe), "type": "upstream_error"}}})
                            return {"done": True}
                        continue
                    _emit({"sequence_number": 0, "type": "response.failed",
                           "response": {"error": {"message": msg or "upstream error", "code": code, "type": "upstream_error"}}})
                    return {"done": True}
                return {"done": True}
        finally:
            stop_ka.set()
        return {"done": True}


    # ── 流式转发 ─────────────────────────────────────
    def _relay_stream_with_retry(self, first_resp, up, url, up_headers, payload, method):
        """增量流式转发 codex-relay 的 Responses SSE：立即发响应头 + 后台 keepalive 线程
        防 idle 超时，边收边发；apply_patch 项缓冲后合成 custom_tool_call。
        早期（真实输出前）遇 response.failed 且为可重试错误（1305/过载/5xx/429）→ 退避重试，
        复用同一客户端连接（期间 keepalive 持续保活，客户端只感知到稍慢开始）；
        真实输出开始后不再重试，中途错误直接转发。
        返回 (events, has_output, stream_error)。已发响应头即视为完成（不回退下一上游）。"""
        import threading
        upstream_name = up["name"]
        MAX = 2
        events = 0
        last_usage = {}
        has_output = False
        stream_error = None
        held_completed = None
        _apply_patch_oi = set()  # output_index 集合：apply_patch function_call 需转 custom_tool_call
        _apply_patch_args = {}  # output_index -> 累积的 JSON arguments 字符串
        raw_blocks = []
        wlock = threading.Lock()
        stop_ka = threading.Event()

        def _write(data):
            with wlock:
                self.wfile.write(data)
                self.wfile.flush()

        def _emit(ed):
            _write((f"event: {ed.get('type', '')}\ndata: {json.dumps(ed, ensure_ascii=False)}\n\n").encode())

        def _keepalive():
            while not stop_ka.wait(5):
                try:
                    _write(b": keepalive\n\n")
                except Exception:
                    break

        # 立即发响应头进入 SSE 模式（避免客户端等待响应头/首字节超时）
        self.send_response(200)
        for h in ["Content-Type", "Cache-Control"]:
            v = first_resp.headers.get(h)
            if v:
                self.send_header(h, v)
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        kat = threading.Thread(target=_keepalive, daemon=True)
        kat.start()

        resp = first_resp
        try:
            for attempt in range(MAX + 1):
                early_err = None  # (code, err, out_bytes) 真实输出前的 response.failed，候选重试
                buf = b""
                done = False
                while not done:
                    chunk = resp.read(4096)
                    if not chunk:
                        done = True
                        break
                    buf += chunk
                    while b"\n\n" in buf or b"\r\n\r\n" in buf:
                        if b"\r\n\r\n" in buf:
                            block, buf = buf.split(b"\r\n\r\n", 1)
                        else:
                            block, buf = buf.split(b"\n\n", 1)
                        block = block.replace(b"\r\n", b"\n")
                        raw_blocks.append(block)
                        out, usage, _ = self._process_sse_block(block, upstream_name)
                        if usage:
                            last_usage = usage
                        events += 1
                        etype, dstr = "", ""
                        for l in out.decode("utf-8", errors="replace").split("\n"):
                            if l.startswith("event: "):
                                etype = l[7:].strip()
                            elif l.startswith("data: "):
                                dstr = l[6:]
                        p = None
                        if dstr:
                            try:
                                p = json.loads(dstr)
                            except Exception:
                                p = None
                        t = etype or (p.get("type", "") if p else "")
                        oi = p.get("output_index") if p else None
                        item = (p.get("item") or {}) if p else {}
                        # 上游错误
                        if t == "response.failed":
                            err = (p or {}).get("response", {}).get("error")
                            code = err.get("code") if isinstance(err, dict) else None
                            if not has_output:
                                early_err = (code, err, out)  # 真实输出前 → 候选重试（暂不转发）
                                done = True
                                break
                            stream_error = err
                            log.warning("[#%d]     !!! %s response.failed (mid-stream): %s", self._req_id, upstream_name, err)
                            _write(out)
                            continue
                        if t == "response.completed":
                            held_completed = p
                            continue
                        # apply_patch fallback: 缓冲 JSON arguments，结束时解包成 custom_tool_call
                        if t == "response.output_item.added" and item.get("name") == "apply_patch" and item.get("type") == "function_call":
                            item["type"] = "custom_tool_call"
                            _apply_patch_oi.add(oi)
                            _apply_patch_args[oi] = ""  # 开始缓冲
                            out = (f"event: {t}\ndata: {json.dumps(p, ensure_ascii=False)}\n\n").encode()
                        elif oi in _apply_patch_oi:
                            if t == "response.function_call_arguments.delta":
                                _apply_patch_args[oi] += p.get("delta", "")
                                continue  # 缓冲，不发
                            if t == "response.function_call_arguments.done":
                                _apply_patch_args[oi] += p.get("arguments", "")
                                continue  # 缓冲，不发
                            if t == "response.output_item.done":
                                # 解包 JSON {"patch":"..."} → 原始 patch 文本
                                raw_args = _apply_patch_args.pop(oi, "")
                                try:
                                    parsed = json.loads(raw_args)
                                    patch_text = parsed.get("patch", raw_args) if isinstance(parsed, dict) else raw_args
                                except Exception:
                                    patch_text = raw_args
                                if not patch_text.startswith("*** Begin Patch"):
                                    patch_text = "*** Begin Patch\n" + patch_text
                                if not patch_text.rstrip().endswith("*** End Patch"):
                                    patch_text = patch_text.rstrip() + "\n*** End Patch"
                                seq = p.get("sequence_number", events + 1)
                                for ck in [patch_text[i:i+20] for i in range(0, len(patch_text), 20)]:
                                    seq += 1
                                    _emit({"type": "response.custom_tool_call_input.delta", "sequence_number": seq,
                                           "delta": ck, "item_id": item.get("id",""), "output_index": oi})
                                seq += 1
                                _emit({"type": "response.custom_tool_call_input.done", "sequence_number": seq,
                                       "input": patch_text, "item_id": item.get("id",""), "output_index": oi})
                                item["type"] = "custom_tool_call"
                                item["input"] = patch_text
                                item.pop("arguments", None)
                                out = (f"event: response.output_item.done\ndata: {json.dumps(p, ensure_ascii=False)}\n\n").encode()
                                has_output = True
                                stop_ka.set()
                        if b"output_text.delta" in out or b"custom_tool_call" in out:
                            has_output = True
                            stop_ka.set()
                        _write(out)
                    if early_err:
                        break
                # 尾部残余（当前 resp 正常结束；重试时跳过）
                if not early_err and buf.strip():
                    buf2 = buf.replace(b"\r\n", b"\n")
                    raw_blocks.append(buf2)
                    out, usage, _ = self._process_sse_block(buf2, upstream_name)
                    if usage:
                        last_usage = usage
                    events += 1
                    _write(out)

                # 早期 response.failed：决定重试 or 转发
                if early_err and not has_output:
                    code, err, out_bytes = early_err
                    if attempt < MAX and _is_retriable_conv(code):
                        wait = min(2 ** attempt, 4)
                        log.warning("[#%d]     !!! %s early error code=%s, retry %d/%d in %ds",
                                    self._req_id, upstream_name, code, attempt + 1, MAX, wait)
                        try:
                            resp.close()
                        except Exception:
                            pass
                        time.sleep(wait)  # 期间 keepalive 持续保活
                        try:
                            if upstream_name == "official":
                                _official_limiter.acquire()
                            resp = urlopen(Request(url, data=payload, headers=up_headers, method=method),
                                           timeout=REQUEST_TIMEOUT)
                        except Exception as oe:
                            log.error("[#%d]     !!! %s reopen failed: %s", self._req_id, upstream_name, oe)
                            _emit({"type": "response.failed", "sequence_number": events + 1,
                                   "response": {"error": {"message": str(oe), "type": "upstream_error"}}})
                            stream_error = str(oe)
                            break
                        continue  # 用新 resp 重试
                    # 不可重试或重试用尽 → 检查 429 并封锁，然后转发 response.failed
                    if isinstance(err, dict):
                        ec = str(err.get("code", ""))
                        if ec == "429" or err.get("type") == "rate_limit_error":
                            log.info("[#%d]     [429] SSE response.failed code=%s from %s, calling _block_channel_on_429",
                                     self._req_id, ec, upstream_name)
                            _block_channel_on_429(json.dumps(err).encode(), upstream_name, self._req_id)
                            log.info("[#%d]     [429] after block: %s", self._req_id,
                                     {k: datetime.fromtimestamp(v).strftime("%H:%M") for k, v in _channel_blocked_until.items() if v > time.time()})
                    log.warning("[#%d]     !!! %s forwarding response.failed: %s", self._req_id, upstream_name, err)
                    _write(out_bytes)
                    stream_error = err
                    break
                # 正常完成或已真实输出（含中途错误已转发）→ 退出重试循环
                break

            # response.completed（apply_patch 项改写 function_call→custom_tool_call）
            if held_completed is not None:
                if _apply_patch_oi:
                    for it in held_completed.get("response", {}).get("output", []) or []:
                        if it.get("name") == "apply_patch" and it.get("type") == "function_call":
                            it["type"] = "custom_tool_call"
                            if "arguments" in it:
                                import json as _json
                                try:
                                    args = _json.loads(it["arguments"])
                                    it["input"] = args.get("patch", it["arguments"])
                                except Exception:
                                    it["input"] = it["arguments"]
                                it.pop("arguments", None)
                _emit(held_completed)
            elif not stream_error:
                # 上游未发 response.completed（不完整流）→ 合成收尾，避免客户端"响应结束但一直转"
                log.warning("[#%d]     !!! %s no response.completed, synthesizing (has_output=%s, events=%d)",
                            self._req_id, upstream_name, has_output, events)
                _emit({"type": "response.completed", "sequence_number": events + 1,
                       "response": {"id": "resp_syn", "object": "response",
                                    "status": "completed", "output": [], "usage": last_usage or {}}})

            # 保存 exchange 用于排查
            resp_text = b"".join(raw_blocks).decode("utf-8", errors="replace")
            self._save_exchange(getattr(self, '_debug_req_body', {}), resp_text, upstream_name, "relay")

            log.info("[#%d]     <<< %s STREAM OK (%d events, %dms)", self._req_id, upstream_name, events, self._ms())
            if last_usage:
                inp = last_usage.get("input_tokens", 0)
                out_ = last_usage.get("output_tokens", 0)
                log.info("    usage: input=%d output=%d total=%d", inp, out_, inp + out_)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            log.warning("[#%d]     <<< %s STREAM interrupted", self._req_id, upstream_name)
        finally:
            stop_ka.set()

        return events, True, stream_error  # 已发响应头，总是 done（不回退下一上游）

    @staticmethod
    def _process_sse_block(block, upstream_name=None):
        """透传 SSE block，修正 GLM 的 prompt_tokens:0"""
        lines = block.decode("utf-8", errors="replace").strip().split("\n")
        etype, dstr = "", ""
        for l in lines:
            if l.startswith("event: "):
                etype = l[7:]
            elif l.startswith("data: "):
                dstr = l[6:]
        if not dstr:
            return block + b"\n\n", None, False
        try:
            p = json.loads(dstr)
        except Exception:
            return block + b"\n\n", None, False
        pt = p.get("type", "")
        # 提取 usage 用于日志
        usage = None
        if pt == "response.completed":
            usage = p.get("response", {}).get("usage")
        elif pt == "message_delta":
            delta_usage = p.get("usage")
            if delta_usage:
                usage = delta_usage
        out = f"event: {etype}\ndata: {json.dumps(p, ensure_ascii=False)}\n\n"
        return out.encode(), usage, False

    # ── 辅助方法 ─────────────────────────────────────
    def _send_raw(self, code, data, content_type):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def _send_json(self, code, obj):
        self._send_raw(code, json.dumps(obj).encode(), "application/json")

    def _send_last_error(self, last_err):
        if isinstance(last_err, tuple):
            code, err_body = last_err
            try:
                self._send_raw(code, err_body or b'{"error":"all upstreams failed"}',
                               "application/json")
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                log.warning("client disconnected before error response")
        else:
            try:
                self._send_json(502, {"error": str(last_err)})
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                log.warning("client disconnected before error response")

    def _send_context_exceeded(self, body, up):
        """上下文超限时返回 400 invalid_request_error，触发客户端自动压缩。"""
        max_ctx = up.get("max_context_tokens", 200000)
        est_tokens = _est_tokens(body)  # 剔除 base64 图片，避免虚高
        log.warning("[#%d]     !!! context overflow (~%dK > %dK), returning 400 to client",
                     getattr(self, '_req_id', 0), int(est_tokens // 1000), max_ctx // 1000)
        err_resp = json.dumps({
            "type": "error",
            "error": {
                "type": "invalid_request_error",
                "message": f"prompt is too long: ~{int(est_tokens)} tokens > {max_ctx} maximum context window"
            }
        }).encode()
        self._send_raw(400, err_resp, "application/json")

    def _save_debug(self, body):
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = os.path.join(LOG_DIR, f"debug_err_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.write(json.dumps(body, ensure_ascii=False, indent=2))
        log.info("    !!! saved to %s", path)

    def _save_debug_body(self, body):
        """保存大请求体用于排查上下文问题"""
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = os.path.join(LOG_DIR, f"debug_req_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.write(json.dumps(body, ensure_ascii=False, indent=2))
        log.info("    saved request body to %s", path)

    def _save_exchange(self, body, response_data, upstream_name, note=""):
        """保存请求+响应用于排查"""
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = os.path.join(LOG_DIR, f"exchange_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "timestamp": ts, "upstream": upstream_name, "note": note,
                "request": body,
                "response": response_data[:50000] if isinstance(response_data, str) else response_data,
            }, f, ensure_ascii=False, indent=2)
        log.info("    saved exchange to %s (%s)", path, note)

    def log_message(self, *a):
        pass


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


# ── 入口 ─────────────────────────────────────────────
if __name__ == "__main__":
    log.info("GLM Proxy v2.9.35 :%d", LISTEN[1])
    for up in UPSTREAMS:
        ctx = f"{up['max_context_tokens'] // 1000}K" if up.get("max_context_tokens") else "?"
        if "relay_port" in up:
            log.info("  %s: relay :%d → interceptor :%d → %s | model=%s ctx=%s",
                     up["name"], up["relay_port"], up["interceptor_port"], up["openai_url"], up["model"], ctx)
        else:
            log.info("  %s: messages → %s | model=%s ctx=%s",
                     up["name"], up.get("anthropic_url", "?"), up["model"], ctx)

    # 启动拦截器（必须在 codex-relay 之前）
    _start_interceptors()

    # 启动 codex-relay 子进程
    _start_relays()
    time.sleep(1)

    # 内网健康检查（仅启动时一次，确认真实模型）
    threading.Thread(target=_check_internal_once, daemon=True).start()

    # 优雅退出
    def _shutdown(sig, frame):
        log.info("Shutting down...")
        _stop_relays()
        sys.exit(0)
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        ThreadedHTTPServer(LISTEN, Handler).serve_forever()
    except KeyboardInterrupt:
        _shutdown(None, None)
