#!/usr/bin/env python3
"""
GLM API 代理 v2.9.80 — codex-relay + Python 路由层

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
# probe-before-commit 兜底：messages/converted 路径上游首内容前 hold 住响应头（便于检测超限改返 400），
# 但探测期客户端收不到任何字节。超过此秒数仍无首内容（上游首 token 慢/深度推理）→ 强制 commit + keepalive，
# 防客户端流式空闲超时（"Stream idle timeout - no chunks received"，约 60s）。15s 远小于该阈值，
# 且超限/空收尾信号通常几秒内返回，足够探测到。
PROBE_TIMEOUT = 15
# 部分 upstream 套 Cloudflare bot 防护（如 hybgzs），Python-urllib 默认 UA 会被 1010 拦截。
# 统一伪装浏览器 UA，对其他 upstream 无副作用。
_UPSTREAM_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
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


# 请求序号（多会话日志关联用）
import itertools
_req_counter = itertools.count(1)


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
            "User-Agent": _UPSTREAM_UA,  # 防 Cloudflare 1010
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
                "User-Agent": _UPSTREAM_UA,  # 防 Cloudflare 1010
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
    _RELAY_MIN = (0, 5, 5)
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
_APPLY_PATCH_RULES = (
    "\n\nSTOP! Read this before calling apply_patch:\n"
    "MANDATORY: You MUST use apply_patch for ALL file operations (create, edit, delete). "
    "Do NOT use shell commands (echo, cat, sed, node, python, powershell) to write files. "
    "apply_patch is the ONLY correct way.\n\n"
    "PATCH FORMAT (the patch field is FREEFORM TEXT, not JSON):\n"
    "1. EVERY content line in BOTH *** Add File and *** Update File MUST start with:\n"
    "   - plus (+) = line to add | minus (-) = line to remove | space ( ) = context line\n"
    "   NEVER write bare text lines without a prefix character!\n"
    "2. Example - create a new file:\n"
    "   *** Begin Patch\n*** Add File: hello.txt\n+line one\n+line two\n*** End Patch\n"
    "3. Use @@ to separate change hunks within Update File.\n"
    "4. *** Begin Patch / *** End Patch are commands, not content (no prefix).\n"
    "5. Each file can only be Added ONCE. To modify existing, use *** Update File.\n"
    "6. Content with --- # @ etc is FINE with +/- prefix. Do NOT avoid any content.\n"
    "7. Do NOT wrap content in markdown code blocks (```).\n"
)

def _inject_apply_patch_rules(body):
    """给 apply_patch 工具的描述注入格式规则（relay + converted 路径都用）"""
    if not isinstance(body, dict):
        return body
    tools = body.get("tools") or body.get("input", [])
    if not isinstance(tools, list):
        return body
    modified = False
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        name = tool.get("name", "")
        ttype = tool.get("type", "")
        # Responses API 格式: {"type":"function","name":"apply_patch",...}
        # 或 custom 格式: {"type":"custom","name":"apply_patch",...}
        if name == "apply_patch" and "description" in tool:
            if _APPLY_PATCH_RULES.strip() not in (tool.get("description") or ""):
                tool["description"] = (tool.get("description") or "") + _APPLY_PATCH_RULES
                modified = True
        # 嵌套格式: {"type":"function","function":{"name":"apply_patch",...}}
        func = tool.get("function")
        if isinstance(func, dict) and func.get("name") == "apply_patch":
            if _APPLY_PATCH_RULES.strip() not in (func.get("description") or ""):
                func["description"] = (func.get("description") or "") + _APPLY_PATCH_RULES
                modified = True
    return body





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
                # 关键：input 是 FREEFORM 文本（patch），不是 JSON
                # 必须包成 {"patch": "..."} 否则 _safe_parse_json 返回 {} 教坏 GLM
                raw_input = item.get("input", "")
                if item.get("name") == "apply_patch" and isinstance(raw_input, str):
                    arguments = json.dumps({"patch": raw_input}, ensure_ascii=False)
                else:
                    arguments = raw_input
                _append_tool_use(messages, {**item, "type": "function_call",
                                            "arguments": arguments})
            elif item_type == "custom_tool_call_output":
                # custom_tool_call_output → function_call_output
                _append_tool_result(messages, item)

    # 构建输出
    result = {"model": body.get("model", "glm-5")}
    if system_parts:
        result["system"] = "\n\n".join(system_parts)
    result["messages"] = messages

    # 转换 tools → Anthropic Messages 标准格式（name + description + input_schema，**不带 type**）
    # 关键：绝不能加 "type":"custom"——venus-deepseek 等 anthropic 端点会报 unknown variant 'custom'，
    # 且若为此 strip 掉工具，模型在缺工具定义时会用原生 DSML 格式输出伪 tool_call 污染正文。
    # 标准 tool 格式（无 type）所有 anthropic 兼容端点通用，实测 venus deepseek-v4-pro 正常 tool_use。
    tools_out = []
    for tool in body.get("tools", []):
        if not isinstance(tool, dict):
            continue
        t = tool.get("type", "")
        if t == "function":
            fn_name = tool.get("name", "")
            if fn_name == "apply_patch":
                # apply_patch：保留 description（含规则注入），patch 包成 object
                desc = tool.get("description", "")
                tools_out.append({
                    "name": fn_name,
                    "description": desc,
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "patch": {"type": "string", "description": desc},
                        },
                        "required": ["patch"],
                    },
                })
            else:
                t_out = {
                    "name": fn_name,
                    "input_schema": tool.get("parameters", tool.get("input_schema", {})),
                }
                if tool.get("description"):
                    t_out["description"] = tool["description"]
                tools_out.append(t_out)
        elif t == "custom":
            # apply_patch 等 custom/grammar 工具 → 标准 tool 格式（规则已在上方统一注入）
            desc = tool.get("description", "")
            tools_out.append({
                "name": tool.get("name", ""),
                "description": desc,
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "patch": {"type": "string", "description": desc},
                    },
                    "required": ["patch"],
                },
            })
        elif t == "namespace":
            # 展平 namespace 子工具。连接符用 '-'（非 '.'）：
            # ① OpenAI function name 校验 ^[a-zA-Z0-9_-]+$ 不允许点号（venus/deepseek 会 400）；
            # ② Codex 调用 namespace 子工具的 name 实测为 "{ns}-{sub}"（如 codex_app-read_thread_terminal）。
            ns_name = tool.get("name", "")
            for sub in tool.get("tools", []):
                sub_name = sub.get("name", "")
                t_out = {
                    "name": f"{ns_name}-{sub_name}" if ns_name else sub_name,
                    "input_schema": sub.get("parameters", sub.get("input_schema", {})),
                }
                if sub.get("description"):
                    t_out["description"] = sub["description"]
                tools_out.append(t_out)
        elif t.startswith("web_search_"):
            # DeepSeek 原生支持 web_search_20250305 / web_search_20260209，直通（保留其 type）
            tools_out.append(tool)
        # 其他未知类型（如 tool_search）跳过，不发往不支持的上游
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


def _patch_msg_usage(block_bytes, est_input=0, model=None):
    """修正 message_start：① usage.input_tokens 兜底（BigModel 真值在 message_delta）② model 改回客户端模型名。

    ① 只覆盖 message_start.usage.input_tokens（est_input 兜底）；不覆盖 message_delta（保留 BigModel 真实计数）。
    ② 上游响应 message_start.model 通常是上游模型名（glm-5.2 / grok-4.5-build-free 等），原样转发会污染
       客户端会话存档 → 恢复会话报"模型无法识别"并回退默认模型。改成客户端发来的 model 即可让代理透明。"""
    if b'"message_start"' not in block_bytes:
        return block_bytes
    if not est_input and not model:
        return block_bytes
    try:
        lines = block_bytes.decode("utf-8", errors="replace").split("\n")
        changed = False
        for i, line in enumerate(lines):
            if not line.startswith("data: "):
                continue
            try:
                p = json.loads(line[6:])
            except Exception:
                continue
            if p.get("type") == "message_start":
                msg = p.get("message")
                if not isinstance(msg, dict):
                    continue
                if est_input:
                    u = msg.get("usage")
                    if isinstance(u, dict):
                        u["input_tokens"] = int(est_input)
                if model:
                    msg["model"] = model
                lines[i] = "data: " + json.dumps(p, ensure_ascii=False)
                changed = True
        return "\n".join(lines).encode("utf-8") if changed else block_bytes
    except Exception:
        return block_bytes


def _normalize_sse_block(block_bytes):
    """规范化上游 SSE：兼容千问等网关 "event:foo"/"data:foo"(冒号后无空格) → 标准 "event: foo"/"data: foo"。

    标准允许冒号后空格可选，但 ① 本代理多处解析用 startswith("event: ")/("data: ") 硬要求空格
    （含 _patch_msg_usage / _process_sse_block / messages 探测逻辑）；② 客户端(Claude Code)的 SSE
    解析器对无空格格式不一定兼容。故在 messages 路径分块后统一规范化一次：下游所有解析与转发给
    客户端的字节都用标准格式。converted 路径重建式输出、relay 路径读 codex-relay 重建输出，均不受影响。"""
    try:
        lines = block_bytes.split(b"\n")
        for i, ln in enumerate(lines):
            if ln.startswith(b"event:") and not ln.startswith(b"event: "):
                lines[i] = b"event: " + ln[6:].lstrip(b" ")
            elif ln.startswith(b"data:") and not ln.startswith(b"data: "):
                lines[i] = b"data: " + ln[5:].lstrip(b" ")
        return b"\n".join(lines)
    except Exception:
        return block_bytes


# 超限信号关键词（按各上游【实测】超限响应归纳，覆盖 4 种不同表现）
# 实测：official/external-claude = 200流式 + model_context_window_exceeded
#       ecloud       = HTTP400 + "ContextWindowExceededError"/"maximum context length is N tokens"
#       venus-deepseek = HTTP400 + "CONTEXT_TOO_LARGE"/"上下文内容过大"
#       internal     = HTTP400 + "Exceeded limit on max bytes to request body"/"BadRequest.TooLarge"(请求体上限)
_OVERFLOW_MARKERS = (
    # stop_reason / 错误码名
    "context_window_exceeded", "contextwindowexceeded",
    "context_length_exceeded", "contextlengthexceeded",
    "context_too_large",                # venus-deepseek code
    # 英文文案
    "maximum context length", "maximum context window",
    "context length exceeded",
    "prompt is too long", "input too long",
    "prompt exceeds max length",        # external-openai/official openai 端点
    "exceeds max length",
    "exceeds the context window", "exceeds context",
    "too many input tokens",
    "exceeded limit on max bytes",      # internal 请求体过大
    "toolarge",                         # BadRequest.TooLarge
    # 中文文案 (venus-deepseek / ecloud)
    "上下文内容过大", "上下文过大", "超出模型处理限制",
    "文本过长",                         # ecloud openai 端点
)


def _is_overflow_signal(text):
    """文本是否含上下文超限信号（兼容各上游：BigModel 的 model_context_window_exceeded、
    OpenAI 风格的 context_length_exceeded、以及各家中转 'prompt is too long' 等文案）。"""
    if not text:
        return False
    t = text.lower()
    return any(m in t for m in _OVERFLOW_MARKERS)


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

        # GET 非 /v1/models 的未知路径（如 /v1/sub2api/billing）→ 404，不进上游循环
        if method == "GET":
            log.warning("[#%d] >>> GET %s (unsupported)", getattr(self, "_req_id", 0), self.path)
            err = json.dumps({"error": {"message": f"Unsupported path: {path_only}",
                                         "type": "invalid_request_error"}}).encode()
            self._send_raw(404, err, "application/json")
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

        # 客户端 Authorization key 路由：
        # - 纯数字 key=N → 强制走 config.upstreams 第 N 个渠道（1-based，动态，不写死渠道名）
        # - 其他 key → 默认按配置顺序回退；并受模型钉选 / 429 封锁影响
        client_key = self.headers.get("Authorization", "").replace("Bearer ", "").strip()
        force_upstream = None  # 强制渠道 name；None 表示默认回退链
        if client_key.isdigit():
            idx = int(client_key) - 1
            if 0 <= idx < len(UPSTREAMS):
                force_upstream = UPSTREAMS[idx]["name"]
            else:
                log.warning("[#%d] key=%s out of range (1..%d), fallback to default chain",
                            self._req_id, client_key, len(UPSTREAMS))

        # GET/无 body 时也要初始化，避免 ROUTE 日志 NameError
        req_model = ""
        # 模型名钉选：客户端指定的 model 若在 config 里有匹配 → 只按顺序走匹配的渠道，不回退到其他模型
        model_pinned = False  # True=只走 model 匹配的渠道
        if isinstance(body, dict):
            req_model = body.get("model") or ""
            if req_model and not force_upstream:
                # 检查是否有任何渠道的 model == req_model
                if any(up.get("model") == req_model or up.get("messages_model") == req_model for up in UPSTREAMS):
                    model_pinned = True

        # 检测图片：有图片强制走 Messages（Completions 不支持图片，Messages 支持）
        has_images = is_responses and isinstance(body, dict) and _request_has_images(body)
        use_completions = RESPONSES_USE_COMPLETIONS and not has_images

        # 调试日志：路由决策完整信息（必须在 use_completions 定义之后）
        needs_completions = is_responses and use_completions
        needs_messages = is_messages or (is_responses and not use_completions)
        blocked_active = {k: datetime.fromtimestamp(v).strftime("%H:%M") for k, v in _channel_blocked_until.items() if v > time.time()}
        # 动态 key 映射预览：1=第一渠道...
        key_map = {str(i + 1): up["name"] for i, up in enumerate(UPSTREAMS)}
        log.info("[#%d] ROUTE: key=%s model=%s force=%s pinned=%s blocked=%s needs_comp=%s needs_msg=%s path=%s key_map=%s",
                 self._req_id, client_key[:20] or "(empty)", req_model or "?",
                 force_upstream, model_pinned, blocked_active or "{}",
                 needs_completions, needs_messages, self.path, key_map)
        if has_images:
            log.info("    [image] 含图片 → 走 Messages 路径")

        for up in UPSTREAMS:
            if up.get("disabled"):
                continue
            # 数字 key 强制：只走对应渠道
            if force_upstream:
                if up["name"] != force_upstream:
                    continue
            else:
                # 模型名钉选：只走 model 匹配的渠道，跳过不匹配的
                if model_pinned:
                    if req_model != up.get("model") and req_model != up.get("messages_model"):
                        continue
                # 渠道封锁检查：429 限额封锁期内跳过 external 和 official
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
            # 通用 OpenAI 路径（/v1/chat/completions 等）：必须有 openai_url
            if not is_responses and not is_messages and "openai_url" not in up:
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
                # 通用 OpenAI 路径（/v1/chat/completions 等）
                if "openai_url" not in up:
                    continue
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
                # Messages 路径走 Anthropic 端点，用 messages_model（如 grok-4.5-claude）；
                # relay/通用 OpenAI 路径用 model（OpenAI 名）。converted 路径下方再覆盖。
                body["model"] = up.get("messages_model", up["model"]) if is_messages else up["model"]
                if is_responses and body.get("previous_response_id"):
                    pid_len = len(json.dumps(body, ensure_ascii=False))
                    if pid_len > 200000:
                        log.warning("    payload %dKB, stripping previous_response_id", pid_len // 1024)
                        del body["previous_response_id"]
                _inject_apply_patch_rules(body)  # 统一注入（relay + converted 都覆盖）
                # converted 路径转换后已是标准 Anthropic tool 格式（无 type），venus 等端点直接接受，无需 strip。
                # messages 路径防御：若 tools 仍含 "type":"custom"（不应出现）则剔之。
                _tools_bak = None
                if up.get("strip_custom_tools") and not is_responses_converted:
                    _tools_list = body.get("tools")
                    if isinstance(_tools_list, list) and _tools_list:
                        _tools_bak = body["tools"]
                        _before = len(_tools_bak)
                        body["tools"] = [t for t in _tools_bak if t.get("type") != "custom"]
                        if len(body["tools"]) != _before:
                            log.info("    [strip] %s: %d → %d tools (stripped custom)", up["name"], _before, len(body["tools"]))

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
                    payload = json.dumps(body).encode()
                if "Content-Type" not in up_headers and payload is not None:
                    up_headers["Content-Type"] = "application/json"
                if _tools_bak is not None:
                    body["tools"] = _tools_bak  # 恢复 tools（回退下一上游时原样）
            up_headers["User-Agent"] = _UPSTREAM_UA  # 防 Cloudflare 1010

            log.info("[#%d]     -> %s", self._req_id, up["name"])

            try:
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
                            resp, up, url, up_headers, payload, method, _est_tokens(body))
                        if stream_error:
                            log.warning("[#%d]     !!! %s upstream error, forwarding to client: %s",
                                        self._req_id, up["name"], stream_error)
                        return  # 已发响应头，总是 done
                    elif is_messages:
                        # 流式（probe-before-commit）：延迟提交 200，握住到首条内容才 flush；
                        # 上游实际返回超限(含 200 流式超限信号) → 返干净 400 触发客户端 auto-compact（不做 est 预判）。
                        # est_input 仅用于上报 input_tokens 让客户端看到真实上下文。
                        self._messages_stream(resp, up, up_headers, body, _est_tokens(body), req_model)
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
                # 任意路径 + HTTP 超限(ecloud/venus/internal 等 400 带超限文案) → 返干净 400 触发客户端压缩（不回退）
                if (is_messages or is_responses) and _is_overflow_signal(err_body.decode("utf-8", errors="replace")):
                    self._send_overflow_400(up, _est_tokens(body), as_responses=is_responses)
                    return
                # 429 rate_limit → 解析重置时间，封锁 official+external（HTTP 错误，所有路径都经过此处）
                if e.code == 429:
                    log.info("[#%d]     [429] HTTPError 429 from %s, calling _block_channel_on_429", self._req_id, up["name"])
                    _block_channel_on_429(err_body, up["name"], self._req_id)
                    log.info("[#%d]     [429] after block: %s", self._req_id,
                             {k: datetime.fromtimestamp(v).strftime("%H:%M") for k, v in _channel_blocked_until.items() if v > time.time()})
                # 502/500 可能是上下文超限 → 统一入口检测+返400触发客户端压缩重试（缺口1）
                if e.code in (500, 502) and body and (is_responses or is_messages):
                    if self._send_context_exceeded(body, up):
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

    def _messages_stream(self, first_resp, up, up_headers, body, est_input=0, orig_model=""):
        """Messages 流式（probe-before-commit，靠上游实际响应驱动超限检测，不用 est 预判）。
        **延迟提交 200**：先握住上游块不发响应头，直到看见首条内容(content_block_delta)：
          - 见内容 → 提交 200 + flush 已握住的块 + 继续正常增量流式（keepalive 随之启动）；
          - 上游出内容前返回超限信号(model_context_window_exceeded / "prompt is too long" 等)或空收尾
            → 返干净 HTTP 400（探测期未提交 200，可直接 _send_raw(400)）→ 触发客户端 auto-compact；
          - 非超限错误(event:error) → 提交 200 + flush 转发给客户端。
        est_input 用客户端原始请求大小，上报 input_tokens 反映真实上下文。"""
        upstream_name = up["name"]
        url = up["anthropic_url"].rstrip("/")
        _SERVER_TOOL_TYPES = {
            "server_tool_use", "web_search_tool_result",
            "code_execution_tool_use", "code_execution_tool_result",
            "computer_tool_use", "computer_tool_result",
            "bash_tool_use", "bash_tool_result",
            "text_editor_tool_use", "text_editor_tool_result",
        }
        wlock = threading.Lock()
        stop_ka = threading.Event()
        committed = [False]
        def _write(data):
            with wlock:
                self.wfile.write(data); self.wfile.flush()
        def _keepalive():
            while not stop_ka.wait(3):
                try:
                    _write(b": keepalive\n\n")
                except Exception:
                    break
        def _commit():
            # 延迟提交 200（probe-before-commit）：探测期(到首条内容前)不发响应头，
            # 便于检测到上游"200 流式却超限"时改返干净 400（200 已发就回不去 400）。
            with wlock:
                if committed[0]:
                    return
                self.send_response(200)
                for h in ["Content-Type", "Cache-Control"]:
                    v = first_resp.headers.get(h)
                    if v:
                        self.send_header(h, v)
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                committed[0] = True
            threading.Thread(target=_keepalive, daemon=True).start()
        def _probe_watchdog():
            # probe-hold 兜底：超过 PROBE_TIMEOUT 仍无首内容（上游首 token 慢/深度推理）→ 强制 commit
            # + keepalive，防客户端流式空闲超时("Stream idle timeout - no chunks received")。
            # stop_ka 在结束/overflow-400 时置位 → 提前退出；否则等满超时检查 committed。
            if stop_ka.wait(PROBE_TIMEOUT):
                return
            if not committed[0]:
                log.info("[#%d] [messages] %s probe-hold %ds 无首内容，强制提交防 idle timeout",
                         self._req_id, upstream_name, PROBE_TIMEOUT)
                try:
                    _commit()
                except Exception:
                    pass
        threading.Thread(target=_probe_watchdog, daemon=True).start()

        resp = first_resp
        last_usage = {}
        try:
            while True:  # 单次处理上游 resp（超限直接返 400，不再 trim 重试）
                buf = b""
                held = []          # 探测期握住、未转发的块
                flushed = False    # 是否已见内容并 flush（进入正常流式）
                stream_error = False
                saw_message_stop = False
                open_indices = []
                skip_indices = set()
                size = 0
                total_bytes = 0
                try:
                    while True:  # 读单个 resp
                        chunk = resp.read(4096)
                        if not chunk:
                            break
                        total_bytes += len(chunk)
                        buf += chunk
                        while b"\n\n" in buf or b"\r\n\r\n" in buf:
                            if b"\r\n\r\n" in buf:
                                block, buf = buf.split(b"\r\n\r\n", 1)
                            else:
                                block, buf = buf.split(b"\n\n", 1)
                            block = block.replace(b"\r\n", b"\n")
                            block = _normalize_sse_block(block)  # 千问等无空格 SSE → 标准化（解析+转发+patch 全受益）

                            is_error_block = b"event: error" in block
                            if is_error_block:
                                try:
                                    for line in block.decode("utf-8", errors="replace").split("\n"):
                                        if line.startswith("data: "):
                                            p = json.loads(line[6:])
                                            err = p.get("error", p)
                                            log.warning("[#%d] [messages] %s stream error: code=%s msg=%s | bytes=%d",
                                                        self._req_id, upstream_name,
                                                        err.get("code") if isinstance(err, dict) else None,
                                                        (err.get("message", "") if isinstance(err, dict) else str(err))[:150],
                                                        total_bytes)
                                            break
                                except Exception:
                                    pass

                            # 畸形 JSON 块丢弃（error 块跳过此校验）
                            if not is_error_block and (b"\ndata: " in block or block.startswith(b"data: ")) and b"event: ping" not in block:
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
                                                self._req_id, upstream_name, repr(block[:120]))
                                    continue

                            # 过滤客户端不支持的 server_tool 内容块
                            try:
                                _elines = block.decode("utf-8", errors="replace").split("\n")
                                _etype = next((l[7:].strip() for l in _elines if l.startswith("event: ")), None)
                                _dj = None
                                for _l in _elines:
                                    if _l.startswith("data: "):
                                        _dj = json.loads(_l[6:]); break
                                if _etype == "content_block_start" and isinstance(_dj, dict):
                                    if (_dj.get("content_block") or {}).get("type", "") in _SERVER_TOOL_TYPES:
                                        skip_indices.add(_dj.get("index", 0))
                                        continue
                                elif _etype in ("content_block_delta", "content_block_stop") and isinstance(_dj, dict):
                                    if _dj.get("index", 0) in skip_indices:
                                        if _etype == "content_block_stop":
                                            skip_indices.discard(_dj.get("index", 0))
                                        continue
                            except Exception:
                                pass

                            if est_input or orig_model:
                                block = _patch_msg_usage(block, est_input, orig_model)
                            out = block + b"\n\n"

                            # === 探测：未 flush 前握住，见首条内容才 flush ===
                            if not flushed:
                                if b'"content_block_delta"' in block:
                                    _commit()  # 见首条内容 → 提交 200 + 启动 keepalive
                                    for hb in held:
                                        _write(hb)
                                    held = []
                                    _write(out); size += len(out)
                                    flushed = True
                                elif _is_overflow_signal(block.decode("utf-8", errors="replace")) or b'"message_stop"' in block:
                                    # 上游超限信号(model_context_window_exceeded / context_length_exceeded /
                                    # "prompt is too long" 等)或空完整收尾(出内容前 message_stop) → 返干净 400
                                    # 触发客户端 auto-compact（探测期未提交 200，可直接 _send_raw(400)）
                                    stop_ka.set()
                                    try:
                                        resp.close()
                                    except Exception:
                                        pass
                                    self._send_overflow_400(up, est_input)
                                    return
                                elif is_error_block:
                                    # 429 → 封锁渠道（与 relay 一致）
                                    if b"429" in block or b"rate_limit" in block:
                                        _block_channel_on_429(block, up["name"], self._req_id)
                                    _commit()  # 出内容前的非超限错误 → 提交 200 + 转发错误
                                    for hb in held:
                                        _write(hb)
                                    held = []
                                    _write(out); size += len(out)
                                    flushed = True
                                    stream_error = True
                                else:
                                    held.append(out)
                            else:
                                _write(out); size += len(out)

                            if b'"content_block_start"' in block:
                                try:
                                    for line in block.decode("utf-8", errors="replace").split("\n"):
                                        if line.startswith("data: "):
                                            open_indices.append(json.loads(line[6:]).get("index", 0))
                                except Exception:
                                    pass
                            elif b'"content_block_stop"' in block:
                                try:
                                    for line in block.decode("utf-8", errors="replace").split("\n"):
                                        if line.startswith("data: "):
                                            idx = json.loads(line[6:]).get("index", -1)
                                            if idx in open_indices:
                                                open_indices.remove(idx)
                                except Exception:
                                    pass
                            elif b"message_stop" in block:
                                saw_message_stop = True
                            elif b"message_delta" in block:
                                try:
                                    for line in block.decode("utf-8", errors="replace").split("\n"):
                                        if line.startswith("data: "):
                                            p = json.loads(line[6:])
                                            if p.get("usage"):
                                                last_usage = p["usage"]
                                except Exception:
                                    pass
                except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                    log.warning("[#%d]     <<< %s STREAM interrupted", self._req_id, upstream_name)
                    return

                # === 判定本次 resp ===
                if flushed:
                    if not saw_message_stop:  # 不完整 → 合成收尾
                        log.warning("[#%d] [messages] %s incomplete (no message_stop), synthesizing close",
                                    self._req_id, upstream_name)
                        for idx in open_indices:
                            _write(("event: content_block_stop\ndata: {\"type\": \"content_block_stop\", \"index\": " + str(idx) + "}\n\n").encode())
                        _write(b'event: message_delta\ndata: {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 0}}\n\nevent: message_stop\ndata: {"type": "message_stop"}\n\n')
                    size_str = (str(size // 1024) + "KB") if size >= 1024 else (str(size) + "B")
                    if last_usage:
                        log.info("[#%d]     <<< %s STREAM OK (%s, %dms) usage: input=%d output=%d total=%d",
                                 self._req_id, upstream_name, size_str, self._ms(),
                                 last_usage.get("input_tokens", 0), last_usage.get("output_tokens", 0),
                                 last_usage.get("input_tokens", 0) + last_usage.get("output_tokens", 0))
                    else:
                        log.info("[#%d]     <<< %s STREAM OK (%s, %dms)",
                                 self._req_id, upstream_name, size_str, self._ms())
                    return
                # 非内容（上游早断/不完整/空但无超限信号；超限已在探测期返 400）→ 提交 200 + 转发已握住的块 + 合成收尾
                _commit()
                for hb in held:
                    _write(hb)
                if held and not saw_message_stop:
                    _write(b'event: message_delta\ndata: {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 0}}\n\nevent: message_stop\ndata: {"type": "message_stop"}\n\n')
                log.info("[#%d]     <<< %s STREAM non-content (%dms) — 提交200+合成收尾",
                         self._req_id, upstream_name, self._ms())
                return
        except Exception as e:
            log.error("[#%d] [messages] %s revive exception: %s — synthesizing close",
                      self._req_id, upstream_name, e)
            try:
                _write(b'event: message_delta\ndata: {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 0}}\n\nevent: message_stop\ndata: {"type": "message_stop"}\n\n')
            except Exception:
                pass
        finally:
            stop_ka.set()

    # ── Responses→Messages 转换流式处理（旧：整段缓冲，留作兜底）──
    def _converted_stream_with_retry(self, first_resp, up, url, up_headers, payload, method):
        """converted 路径（probe-before-commit）：同步翻译 Anthropic→Responses。
        延迟提交 200：首条内容前缓冲，见内容才提交+flush；overflow(超限 stop_reason / 空收尾 / error 体)
        → 返干净 400(as_responses)触发客户端压缩；其他早期错误转发 response.failed。"""
        import threading
        upstream_name = up["name"]
        wlock = threading.Lock()
        stop_ka = threading.Event()
        committed = [False]
        held = []  # probe-hold：首条内容前缓冲，便于超限(200流式)时改返干净 400
        def _write(data):
            with wlock:
                self.wfile.write(data); self.wfile.flush()
        def _keepalive():
            while not stop_ka.wait(5):
                try:
                    _write(b": keepalive\n\n")
                except Exception:
                    break
        def _commit():
            # probe-before-commit：延迟提交 200，首条内容才提交（+启动 keepalive）
            with wlock:
                if committed[0]:
                    return
                self.send_response(200)
                for h in ["Content-Type", "Cache-Control"]:
                    v = first_resp.headers.get(h)
                    if v:
                        self.send_header(h, v)
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                committed[0] = True
            threading.Thread(target=_keepalive, daemon=True).start()
        def _probe_watchdog():
            # probe-hold 兜底：超过 PROBE_TIMEOUT 仍无首内容（上游首 token 慢/深度推理）→ 强制 commit
            # + keepalive，防客户端流式空闲超时("Stream idle timeout - no chunks received")。
            if stop_ka.wait(PROBE_TIMEOUT):
                return
            if not committed[0]:
                log.info("[#%d] [converted] %s probe-hold %ds 无首内容，强制提交防 idle timeout",
                         self._req_id, upstream_name, PROBE_TIMEOUT)
                try:
                    _commit()
                except Exception:
                    pass
        threading.Thread(target=_probe_watchdog, daemon=True).start()
        def _emit(ed):
            t = ed.get("type", "")
            _is_content = t.endswith(".delta") and ("output_text" in t or "reasoning_summary_text" in t
                        or "function_call_arguments" in t or "custom_tool_call_input" in t)
            if _is_content and not committed[0]:
                _commit()  # 首条内容 → 提交 200 + flush 缓冲
                for hb in held:
                    _write(hb)
                held.clear()
            data = (f"event: {t}\ndata: {json.dumps(ed, ensure_ascii=False)}\n\n").encode()
            if committed[0]:
                _write(data)
            else:
                held.append(data)

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
                    # 调试：apply_patch 的每个 delta
                    if b.get("name") == "apply_patch":
                        log.warning("[#%d] [converted] PATCH_DELTA: len=%d frag=%s (accumulated=%d)",
                                    getattr(self, '_req_id', 0), len(t), repr(t[:100]), len(b["args"]))
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
                    # 调试日志：看 GLM 实际生成的 tool_use
                    log.warning("[#%d] [converted] TOOL_USE: name=%s args_len=%d args_raw=%s",
                                getattr(self, '_req_id', 0), b.get("name",""), len(b.get("args","")), repr(b.get("args","")[:300]))
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
                stop = (d.get("delta", {}) or {}).get("stop_reason", "")
                if stop and _is_overflow_signal(stop):
                    return ("overflow", None, stop)  # 超限 stop_reason（如 model_context_window_exceeded）
            elif et == "message_stop":
                if not has_output[0]:
                    return ("overflow", None, "empty completion")  # 出内容前收尾 = 超限
                _emit_created()
                _emit({"sequence_number": nseq(), "type": "response.completed", "response": _resp_obj("completed")})
                return "done"
            elif et == "error":
                err = d.get("error", d)
                code = err.get("code") if isinstance(err, dict) else None
                msg = (err.get("message", "") if isinstance(err, dict) else str(err))
                _etxt = msg if isinstance(msg, str) else json.dumps(msg, ensure_ascii=False)
                if _is_overflow_signal(_etxt):
                    return ("overflow", code, msg)
                return ("error", code, msg)
            return None

        # 内层函数：读上游 SSE，翻译成 Responses 事件，返回 (done, early_err)
        def _run_once(resp):
            nonlocal done, early_err, overflow
            try:
                buf = b""
                while not done and early_err is None and not overflow:
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
                            if line.startswith("event:"):
                                et = line[6:].strip()  # 兼容千问 "event:xxx"(无空格) 与标准 "event: xxx"
                            elif line.startswith("data:"):
                                try:
                                    dj = json.loads(line[5:].strip())
                                except Exception:
                                    pass
                        if not et or not dj:
                            continue
                        r = _on_event(et, dj)
                        if r == "done":
                            done = True
                            break
                        if isinstance(r, tuple) and r[0] == "overflow":
                            overflow = True
                            break
                        if isinstance(r, tuple) and r[0] == "error":
                            if not has_output[0]:
                                early_err = (r[1], r[2])
                            else:
                                _emit({"sequence_number": nseq(), "type": "response.failed",
                                       "response": {"error": {"message": (r[2] or "upstream error"), "code": r[1], "type": "upstream_error"}}})
                                done = True
                            break
                # 上游 EOF 但未发 message_stop：合成完成（overflow 时不合成，交外层返 400）
                if not done and early_err is None and not overflow:
                    log.warning("[#%d] [converted] %s upstream EOF without message_stop, synthesizing completed",
                                self._req_id, upstream_name)
                    _emit_created()
                    # 空收尾(out=0)时从未发 content delta → 未 _commit，held 里的 created/completed
                    # 缓冲发不出去，客户端会挂死。强制提交 + flush held。
                    if not committed[0]:
                        _commit()
                        for hb in held:
                            _write(hb)
                        held.clear()
                    _emit({"sequence_number": nseq(), "type": "response.completed", "response": _resp_obj("completed")})
                    done = True
                if done:
                    log.info("[#%d]     <<< %s [converted] STREAM OK (sync, %dms, out=%d)",
                             self._req_id, upstream_name, self._ms(), len(output_items))
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                log.warning("[#%d]     <<< %s [converted] STREAM interrupted", self._req_id, upstream_name)

        # 首次运行
        resp = first_resp
        done = False
        early_err = None
        overflow = False
        _run_once(resp)

        # 超限（探测期未提交 200）→ 返干净 400 触发客户端压缩
        if overflow:
            stop_ka.set()
            try:
                resp.close()
            except Exception:
                pass
            self._send_overflow_400(up, 0, as_responses=True)
            return {"done": True}

        # 早期错误：与 messages/relay 一致，429 封锁后直接转发 response.failed（不退避重试）
        if not done and early_err:
            code, msg = early_err[0], early_err[1]
            # 错误体含超限关键词 → 同样返 400（兜底：_on_event 未归为 overflow 的情况）
            _etxt = msg if isinstance(msg, str) else json.dumps(msg, ensure_ascii=False)
            if _is_overflow_signal(_etxt):
                stop_ka.set()
                try:
                    resp.close()
                except Exception:
                    pass
                self._send_overflow_400(up, 0, as_responses=True)
                return {"done": True}
            if str(code) == "429" or (isinstance(msg, dict) and msg.get("type") == "rate_limit_error") or "429" in str(msg)[:200]:
                _block_channel_on_429(json.dumps({"error": {"code": code, "message": msg}}).encode(), upstream_name, self._req_id)
            _emit({"sequence_number": 0, "type": "response.failed",
                   "response": {"error": {"message": early_err[1] or "upstream error", "code": early_err[0], "type": "upstream_error"}}})

        stop_ka.set()
        return {"done": True}


    # ── 流式转发 ─────────────────────────────────────
    def _relay_stream_with_retry(self, first_resp, up, url, up_headers, payload, method, est_input=0):
        """增量流式转发 codex-relay 的 Responses SSE（probe-before-commit，靠 codex-relay 自带 keepalive）。
        apply_patch 项缓冲后合成 custom_tool_call。
        延迟提交 200：非内容块(response.created 等)握住不发，首次 _write 才提交 200+flush；
        overflow(超限信号/空completed)→返干净 400(as_responses)触发客户端压缩；其他错误与 messages 一致直接转发(429另封锁渠道)。
        返回 (events, has_output, stream_error)。已发响应头即视为完成（不回退下一上游）。"""
        import threading
        upstream_name = up["name"]
        events = 0
        last_usage = {}
        has_output = False
        stream_error = None
        held_completed = None
        raw_blocks = []

        committed = [False]
        def _commit():
            # probe-before-commit：延迟提交 200，首次 _write 才提交，便于超限(200流式)时改返干净 400
            if committed[0]:
                return
            self.send_response(200)
            for h in ["Content-Type", "Cache-Control"]:
                v = first_resp.headers.get(h)
                if v:
                    self.send_header(h, v)
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            committed[0] = True

        def _write(data):
            _commit()  # 首次写入前才提交 200（探测期握住不发，超限可不提交直接 400）
            self.wfile.write(data)
            self.wfile.flush()

        def _emit(ed):
            _write((f"event: {ed.get('type', '')}\ndata: {json.dumps(ed, ensure_ascii=False)}\n\n").encode())

        resp = first_resp
        try:
            while True:  # 单次处理（超限直接返 400，不再裁input重发；非超限错误直接转发）
                early_err = None  # (code, err, out_bytes) 真实输出前的 response.failed
                held_completed = None
                has_output = False
                buf = b""
                held = []  # probe-hold：握住 response.created 等非内容块，见首条内容才 flush
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
                                early_err = (code, err, out)  # 真实输出前 → 候选重试/裁剪（暂不转发）
                                done = True
                                break
                            stream_error = err
                            log.warning("[#%d]     !!! %s response.failed (mid-stream): %s", self._req_id, upstream_name, err)
                            _write(out)
                            continue
                        if t == "response.completed":
                            held_completed = p
                            continue
                        # probe-hold：非内容块(response.created 等)握住不发，见首条内容才 flush；
                        # overflow(无内容)重试时整批丢弃 → 避免重试发多个 response.created
                        _is_content = (b"output_text.delta" in out or b"function_call_arguments.delta" in out or b"custom_tool_call" in out or b"reasoning" in out)
                        if _is_content:
                            if not has_output:
                                has_output = True
                                for hb in held:
                                    _write(hb)
                                held = []
                            _write(out)
                        elif has_output:
                            _write(out)
                        else:
                            held.append(out)
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

                # 中途错误（已真实输出后的 response.failed，已转发）→ 完成
                if stream_error:
                    break

                # === 超限检测（靠上游实际响应）===：response.failed 带超限文案 或 空 completed(无 output)
                # → 返干净 400（Responses 形态）触发客户端压缩；探测期未提交 200，可直接 _send_raw(400)
                if (early_err and not has_output
                        and _is_overflow_signal(json.dumps(early_err[1], ensure_ascii=False))) \
                   or (held_completed is not None and not has_output):
                    try:
                        resp.close()
                    except Exception:
                        pass
                    self._send_overflow_400(up, est_input, as_responses=True)
                    return

                # === 早期 response.failed（非超限）：与 messages 一致，直接转发（不退避重试）===
                if early_err and not has_output and not stream_error:
                    code, err, out_bytes = early_err
                    # 429 → 封锁渠道（直到重置）；其余错误直接转发 response.failed
                    if isinstance(err, dict):
                        ec = str(err.get("code", ""))
                        if ec == "429" or err.get("type") == "rate_limit_error":
                            log.info("[#%d]     [429] SSE response.failed code=%s from %s, calling _block_channel_on_429",
                                     self._req_id, ec, upstream_name)
                            _block_channel_on_429(json.dumps(err).encode(), upstream_name, self._req_id)
                            log.info("[#%d]     [429] after block: %s", self._req_id,
                                     {k: datetime.fromtimestamp(v).strftime("%H:%M") for k, v in _channel_blocked_until.items() if v > time.time()})
                    log.warning("[#%d]     !!! %s forwarding response.failed: %s", self._req_id, upstream_name, err)
                    for hb in held:  # probe-hold：转发前 flush 握住的(含 response.created)，让客户端看到 created+failed
                        _write(hb)
                    held = []
                    _write(out_bytes)
                    stream_error = err

                # 正常完成或已真实输出（含中途错误已转发）→ emit response.completed 收尾
                # 先 flush 还握住的块(若 has_output 期间已 flush 则 held 为空)：保证 created+completed 结构合法
                for hb in held:
                    _write(hb)
                held = []
                if held_completed is not None:
                    # 与 messages 一致：上报 input_tokens 用客户端原始请求大小（est_input），
                    # 非裁剪后的大小 → 让 Codex 看到真实上下文 → 触发其压缩（否则复活后 Codex 不压缩、循环超限）
                    if est_input:
                        try:
                            u = held_completed.setdefault("response", {}).setdefault("usage", {})
                            u["input_tokens"] = int(est_input)
                            u["total_tokens"] = int(est_input) + int(u.get("output_tokens", 0) or 0)
                            # 同步到 last_usage（日志用——否则 last_usage 是上游原始值=0）
                            last_usage["input_tokens"] = int(est_input)
                            last_usage["total_tokens"] = int(est_input) + int(last_usage.get("output_tokens", 0) or 0)
                        except Exception:
                            pass
                    _emit(held_completed)
                elif not stream_error:
                    # 上游未发 response.completed（不完整流）→ 合成收尾，避免客户端"响应结束但一直转"
                    log.warning("[#%d]     !!! %s no response.completed, synthesizing (has_output=%s, events=%d)",
                                self._req_id, upstream_name, has_output, events)
                    _emit({"type": "response.completed", "sequence_number": events + 1,
                           "response": {"id": "resp_syn", "object": "response",
                                        "status": "completed", "output": [], "usage": last_usage or {}}})
                break  # 完成，退出重试循环

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

    def _send_overflow_400(self, up, est_tokens=0, as_responses=False):
        """超限：向客户端返干净 HTTP 400（触发其 auto-compact）。
        不做 est 预判——调用方已通过上游实际响应(_is_overflow_signal 命中)确认是真超限。
        as_responses=True → OpenAI Responses 错误形态（relay/converted，Codex）；
        否则 Anthropic 形态（messages，Claude Code，H1 实锤：此形态触发 auto-compact）。"""
        max_ctx = up.get("max_context_tokens", 200000)
        msg = (f"prompt is too long: ~{int(est_tokens)} tokens > {max_ctx} maximum context window"
               if est_tokens else "context length exceeded, please reduce conversation history")
        if as_responses:
            err = json.dumps({"error": {"message": msg, "type": "invalid_request_error",
                                        "code": "context_length_exceeded"}}).encode()
        else:
            err = json.dumps({"type": "error", "error": {"type": "invalid_request_error",
                                                         "message": msg}}).encode()
        log.warning("[#%d]     !!! context overflow (~%dK > %dK), returning 400 to client",
                    getattr(self, '_req_id', 0),
                    int(est_tokens // 1000) if est_tokens else 0, max_ctx // 1000)
        self._send_raw(400, err, "application/json")

    def _send_context_exceeded(self, body, up):
        """统一的「上下文超限即 400」入口：检测(est>max_ctx*0.9) + 发送合一。

        供缺口1(500/502) 与 缺口3(流式 context_exceeded) 复用。只在上游「确实处理不了」时
        才拦截返 400 触发客户端压缩重试——不做飞行前预判拦截（用户要求：先让请求发出去试）。
        返回 True=已发400(调用方应return)；False=未超限不拦截(调用方继续回退下一个上游)。"""
        max_ctx = up.get("max_context_tokens", 200000)
        est_tokens = _est_tokens(body)  # 剔除 base64 图片，避免虚高
        if est_tokens <= max_ctx * 0.9:
            return False
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
        return True

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
    log.info("GLM Proxy v2.9.80 :%d", LISTEN[1])
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
