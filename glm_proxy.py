#!/usr/bin/env python3
"""
GLM API 代理 v2.9.2 — codex-relay + Python 路由层

架构：
    Codex CLI → 本代理(:9999) → codex-relay(:4444/:4445) → 上游 /chat/completions
                             ↘ 其他路径直接透传上游

功能：
    - codex-relay 负责 Responses API ↔ Chat Completions 翻译（社区维护）
    - Python 层负责：模型覆盖、密钥注入、多上游回退、健康检查、飞书通知
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

# ── 配置 ──────────────────────────────────────────────
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

def _load_config():
    """从 config.json 加载配置（密钥等敏感信息）。不存在则用示例。"""
    if not os.path.exists(_CONFIG_PATH):
        log.warning("config.json 不存在，请复制 config.example.json 并填入密钥")
        return {"upstreams": [], "feishu_webhook": ""}
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

FEISHU_WEBHOOK = _cfg.get("feishu_webhook", "")

UPSTREAMS = _cfg.get("upstreams", [])

STATIC_MODELS = json.dumps({
    "object": "list",
    "data": [{"id": up["model"], "object": "model", "owned_by": up.get("owned_by", "zhipu")} for up in UPSTREAMS],
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

# ── 飞书通知 ──────────────────────────────────────────
def _send_feishu(msg):
    try:
        payload = json.dumps({"msg_type": "text", "content": {"text": msg}}).encode()
        req = Request(FEISHU_WEBHOOK, data=payload, headers={"Content-Type": "application/json"})
        urlopen(req, timeout=5)
    except:
        pass

# ── 内网健康检查（模型验证）──────────────────────────────
internal_alive = False

def _check_internal():
    global internal_alive
    up = UPSTREAMS[0]
    url = up["openai_url"].rstrip("/") + "/chat/completions"
    body = json.dumps({
        "model": up["model"],
        "messages": [
            {"role": "system", "content": "你必须在回复的第一行说出你的确切模型名称和版本号。不要说其他内容。"},
            {"role": "user", "content": "你的模型名称和版本号？"},
        ],
        "max_tokens": 50,
        "stream": False,
    }).encode()
    expected = up["model"].lower().replace("-", "").replace(" ", "")
    first = True
    while True:
        prev = internal_alive
        reason = ""
        try:
            req = Request(url, data=body, headers={
                "Authorization": f"Bearer {up['key']}",
                "Content-Type": "application/json",
            }, method="POST")
            resp = urlopen(req, timeout=60)
            r = json.loads(resp.read())
            returned_model = r.get("model", "")
            choices = r.get("choices", [])
            has_content = choices and choices[0].get("message", {}).get("content")
            model_match = returned_model and (returned_model == up["model"] or up["model"].startswith(returned_model))
            identity_ok = False
            if has_content:
                answer = choices[0]["message"]["content"].strip().split("\n")[0].lower().replace("-", "").replace(" ", "")
                identity_ok = expected in answer
            if has_content and model_match and identity_ok:
                internal_alive = True
            else:
                internal_alive = False
                if not model_match:
                    reason = f"model 字段不匹配 (expected={up['model']}, got={returned_model})"
                    log.warning("[health] internal model field mismatch: expected=%s, got=%s", up["model"], returned_model)
                elif not identity_ok:
                    answer_raw = choices[0]["message"]["content"].strip().split("\n")[0] if has_content else ""
                    reason = f"模型自称不匹配 (got={answer_raw})"
                    log.warning("[health] internal identity mismatch: %s", answer_raw)
                else:
                    reason = "空响应"
                    log.warning("[health] internal empty response")
        except HTTPError as e:
            internal_alive = False
            reason = f"HTTP {e.code}"
            log.warning("[health] internal HTTP %d", e.code)
        except Exception as e:
            internal_alive = False
            reason = f"连接失败 ({type(e).__name__})"
            log.warning("[health] internal check failed: %s: %s", type(e).__name__, e)
        if first or internal_alive != prev:
            log.info("[health] internal %s (identity=%s)", "UP" if internal_alive else "DOWN",
                     choices[0]["message"]["content"].strip().split("\n")[0] if internal_alive and choices else reason)
            if internal_alive == False and prev == True:
                _send_feishu("GLM proxy DOWN: " + reason + " " + up["openai_url"])
            elif internal_alive == True and prev == False:
                _send_feishu("GLM proxy UP: " + up["openai_url"])
        first = False
        time.sleep(300)



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

        # 解析请求，注入 stream_options 使上游返回 usage
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
            # 修复 max_tokens=None（智谱 Chat Completions 会报 1210 参数错误）
            if data.get("max_tokens") is None:
                data["max_tokens"] = 16384
            if is_stream:
                data.setdefault("stream_options", {})["include_usage"] = True
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
    _RELAY_MIN = (0, 3, 0)
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


# ── think 标签清理 ──
def _strip_think_tags(text):
    """剥离推理标签"""
    import re
    # 移除完整的 <think...</think> 块
    text = re.sub(r'<think.*?</think>', '', text, flags=re.DOTALL)
    # 移除闭合标签 </think> 及后面的空白
    text = re.sub(r'</think>' + r'\s*', '', text)
    # 移除开始标签 <think> 及后面的空白
    text = re.sub(r'<think>' + r'\s*', '', text)
    # 移除不完整的开始标签 <think（无闭合）
    text = re.sub(r'<think\b', '', text)
    return text.strip()




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
    tool_result = {
        "type": "tool_result",
        "tool_use_id": output_item.get("call_id", ""),
        "content": output_item.get("output", ""),
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
    "\n\nSTOP! Your previous apply_patch calls were ALL WRONG (empty {} or invalid format). "
    "IGNORE all previous attempts. Follow ONLY the rules below:\n"
    "MANDATORY: You MUST use apply_patch for ALL file operations — creating, editing, "
    "or deleting files. Do NOT use shell commands (echo, sed, powershell Set-Content, etc.) "
    "to write or modify files. apply_patch is the ONLY correct way to change files.\n\n"
    "CRITICAL RULES for patch format:\n"
    "1. In *** Update File sections, EVERY content line MUST start with one of:\n"
    "   - space ( ) = context line (unchanged, shown for reference)\n"
    "   - minus (-) = line to REMOVE from the file\n"
    "   - plus (+) = line to ADD to the file\n"
    "2. NEVER write bare text lines without a prefix character!\n"
    "3. Start each change section with @@ (just @@ alone, NO curly braces or anything after it)\n"
    "4. Do NOT use --- separator, it is NOT valid\n"
    "5. *** Begin Patch and *** End Patch are COMMANDS, NOT content. "
    "Do NOT add +/-/space prefix to them!\n"
    "6. You CANNOT append to a file with multiple Add File operations. "
    "Each file can only be Added ONCE. If you need a large file, "
    "write ALL content in a SINGLE *** Add File operation. "
    "To modify an existing file, use *** Update File instead.\n\n"
    "To change line 2 from 'old' to 'new' in a file:\n"
    "*** Begin Patch\n"
    "*** Update File: path/to/file.txt\n"
    "@@\n"
    " line 1\n"
    "-old\n"
    "+new\n"
    " line 3\n"
    "*** End Patch\n\n"
    "To create a new file (NO @@ in Add File):\n"
    "*** Begin Patch\n"
    "*** Add File: path/new.txt\n"
    "+line 1\n"
    "+line 2\n"
    "*** End Patch\n\n"
    "To delete a file:\n"
    "*** Begin Patch\n"
    "*** Delete File: path/old.txt\n"
    "*** End Patch"
)


def _make_apply_patch_tool_description(original_desc):
    """给 apply_patch 工具描述追加 patch 格式规则"""
    return (original_desc or "") + _APPLY_PATCH_RULES


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
                elif role in ("user", "assistant"):
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
                                  "description": _make_apply_patch_tool_description(tool.get("description", ""))},
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


def _convert_custom_tools_for_completions(body):
    """codex-relay 路径（Responses→Chat Completions）的工具转换：
    把 custom/grammar 工具（如 apply_patch）转成 OpenAI function 格式，
    否则 codex-relay/GLM 不认识，GLM 会改用 shell_command。"""
    tools = body.get("tools")
    if not isinstance(tools, list):
        return body
    new_tools = []
    for tool in tools:
        if not isinstance(tool, dict):
            new_tools.append(tool)
            continue
        t = tool.get("type", "")
        if t == "custom":
            name = tool.get("name", "")
            new_tools.append({
                "type": "function",
                "name": name,
                "description": _make_apply_patch_tool_description(tool.get("description", "")),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "patch": {"type": "string", "description": "The patch content in V4A format"},
                    },
                    "required": ["patch"],
                },
            })
        else:
            new_tools.append(tool)
    body["tools"] = new_tools

    # 转换 input 里的 custom_tool_call / custom_tool_call_output（往返结果）
    # codex-relay 不认识 custom 类型，需转成标准 function_call / function_call_output
    inp = body.get("input")
    if isinstance(inp, list):
        new_input = []
        for item in inp:
            if isinstance(item, dict):
                t = item.get("type", "")
                if t == "custom_tool_call":
                    new_item = dict(item)
                    new_item["type"] = "function_call"
                    if "input" in new_item and "arguments" not in new_item:
                        # custom_tool_call.input 是原始文本（apply_patch 的 patch），
                        # 但 function_call.arguments 必须是 JSON，包成 {"patch": ...}
                        raw_input = new_item.pop("input")
                        try:
                            json.loads(raw_input)  # 已是合法JSON则原样用
                            new_item["arguments"] = raw_input
                        except Exception:
                            new_item["arguments"] = json.dumps({"patch": raw_input}, ensure_ascii=False)
                    new_input.append(new_item)
                elif t == "custom_tool_call_output":
                    new_item = dict(item)
                    new_item["type"] = "function_call_output"
                    new_input.append(new_item)
                else:
                    new_input.append(item)
            else:
                new_input.append(item)
        body["input"] = new_input
    return body
    """将 OpenAI Responses API 请求转换为 Anthropic Messages API 请求。
    ccproxy-api 的转换器不处理 function_call/function_call_output，
    所以这里自己实现完整的转换。"""
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
            item_type = item.get("type", "")
            if item_type == "message":
                role = item.get("role", "user")
                content = item.get("content", [])
                text = _extract_text_from_content(content)
                if role in ("system", "developer"):
                    if text:
                        system_parts.append(text)
                elif role in ("user", "assistant"):
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
        t = tool.get("type", "")
        if t == "function":
            tools_out.append({
                "type": "custom",
                "name": tool.get("name", ""),
                "input_schema": tool.get("parameters", tool.get("input_schema", {})),
            })
        elif t == "custom":
            # apply_patch 等 custom/grammar 工具 → 转为 function 格式让 GLM 能调用
            name = tool.get("name", "")
            desc = _make_apply_patch_tool_description(tool.get("description", ""))
            tools_out.append({
                "type": "custom",
                "name": name,
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "patch": {"type": "string", "description": desc},
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



    """构建 codex-relay namespace 工具名修正映射。
    codex-relay 把 namespace 工具的子工具名拼接到 namespace 后面作为 function_call name，
    且丢弃了 namespace 字段。正确格式：name="js", namespace="mcp__node_repl"。
    codex-relay 格式：name="mcp__node_repljs", 无 namespace 字段。
    返回 {mangled_name: {"name": sub_name, "namespace": ns_name}} 映射。"""
    fix_map = {}
    for tool in body.get("tools", []):
        if tool.get("type") == "namespace":
            ns_name = tool.get("name", "")
            for sub_tool in tool.get("tools", []):
                sub_name = sub_tool.get("name", "")
                if ns_name and sub_name:
                    mangled = ns_name + sub_name
                    fix_map[mangled] = {"name": sub_name, "namespace": ns_name}
    return fix_map



# ── apply_patch 转换 ──────────────────────────────────
def _parse_patch_to_operation(patch_text):
    """将 patch 文本解析为 Codex 的 operation 对象。
    Codex 期望: {type: create_file|update_file|delete_file, path, diff}"""
    lines = patch_text.split("\n")
    path = ""
    diff_lines = []
    op_type = None
    in_content = False

    for line in lines:
        if line.startswith("*** Add File: "):
            op_type = "create_file"
            path = line[len("*** Add File: "):].strip()
            in_content = True
            continue
        elif line.startswith("*** Update File: "):
            op_type = "update_file"
            path = line[len("*** Update File: "):].strip()
            in_content = True
            continue
        elif line.startswith("*** Delete File: "):
            return {"type": "delete_file", "path": line[len("*** Delete File: "):].strip()}
        elif line.strip() in ("*** Begin Patch", "*** End Patch", ""):
            continue
        elif in_content:
            if line.startswith("+") or line.startswith("-") or line.startswith(" "):
                diff_lines.append(line)
            else:
                diff_lines.append("+" + line if line else "")

    if not op_type or not path:
        return None

    diff = "\n".join(diff_lines)
    result = {"type": op_type, "path": path}
    if diff.strip():
        result["diff"] = diff
    return result


def _fix_apply_patch_args(output_blocks):
    """将 function_call(apply_patch) 改写为 custom_tool_call 格式。
    GPT 返回的格式: type=custom_tool_call, name=apply_patch, input=原始patch文本"""
    # 1. 找到 apply_patch 的 call_id 和完整 arguments
    patch_calls = []
    for block in output_blocks:
        if not block:
            continue
        text = block.decode("utf-8", errors="replace")
        if '"apply_patch"' not in text:
            continue
        for line in text.split("\n"):
            if not line.startswith("data: "):
                continue
            try:
                p = json.loads(line[6:])
                item = p.get("item", {})
                if item.get("name") == "apply_patch" and item.get("type") == "function_call" and item.get("arguments"):
                    patch_calls.append({
                        "call_id": item.get("call_id", item.get("id", "")),
                        "ids": {x for x in (item.get("id"), item.get("call_id")) if x},
                        "arguments": item["arguments"],
                    })
            except:
                pass

    if not patch_calls:
        return

    for pc in patch_calls:
        call_id = pc["call_id"]
        ids = pc["ids"]
        args = pc["arguments"]
        if not args:
            continue

        # 2. 解包 JSON {"patch":"..."} → 原始 patch 文本
        try:
            parsed = json.loads(args)
            patch_text = parsed.get("patch", args) if isinstance(parsed, dict) else args
        except:
            patch_text = args

        # 如果 patch 内容无效（只有 {} 等），跳过转换
        cleaned = patch_text.replace("*** Begin Patch", "").replace("*** End Patch", "").strip()
        if not cleaned or cleaned in ("{}",):
            log.warning("    [apply_patch] invalid patch content: %s", repr(patch_text[:100]))
            # 不跳过，仍然转成 custom_tool_call 让 Codex 返回错误给模型

        # 自动修复：确保 patch 格式正确
        if not patch_text.startswith("*** Begin Patch"):
            patch_text = "*** Begin Patch\n" + patch_text
        # 清理被模型加了 +/-/空格前缀的 *** End Patch 行
        import re
        patch_text = re.sub(r'^[+\- ]\*\*\* End Patch$', '*** End Patch', patch_text, flags=re.MULTILINE)
        if not patch_text.rstrip().endswith("*** End Patch"):
            patch_text = patch_text.rstrip() + "\n*** End Patch"

        log.info("    [apply_patch] unwrapped: %s", repr(patch_text[:100]))

        # 3. 改写所有相关的 block（匹配 id 或 call_id，因为 delta 用 item_id 而非 call_id）
        id_bytes = [i.encode() for i in ids]
        new_blocks = []
        for block in output_blocks:
            if not block or not any(ib in block for ib in id_bytes):
                new_blocks.append(block)
                continue

            text = block.decode("utf-8", errors="replace")
            evt_type = ""
            for ln in text.split("\n"):
                if ln.startswith("event: "):
                    evt_type = ln[7:].strip()
                    break

            # 跳过 argument delta
            if "function_call_arguments.delta" in evt_type:
                continue

            # 改写 output_item.added → custom_tool_call
            if "output_item.added" in evt_type and "apply_patch" in text:
                for ln in text.split("\n"):
                    if ln.startswith("data: "):
                        try:
                            p = json.loads(ln[6:])
                            seq = p.get("sequence_number", 0)
                            item_id = f"ctc_{call_id}"
                            # 1. output_item.added
                            p["item"] = {"id": item_id, "type": "custom_tool_call",
                                         "status": "in_progress", "call_id": call_id, "name": "apply_patch"}
                            new_blocks.append(f"event: response.output_item.added\ndata: {json.dumps(p, ensure_ascii=False)}\n\n".encode())
                            # 2. custom_tool_call_input.delta（分块流式）
                            for chunk in [patch_text[i:i+20] for i in range(0, len(patch_text), 20)]:
                                seq += 1
                                d = {"type": "response.custom_tool_call_input.delta",
                                     "sequence_number": seq, "delta": chunk, "item_id": item_id,
                                     "output_index": p.get("output_index", 0)}
                                new_blocks.append(f"event: response.custom_tool_call_input.delta\ndata: {json.dumps(d, ensure_ascii=False)}\n\n".encode())
                            # 3. custom_tool_call_input.done
                            seq += 1
                            dn = {"type": "response.custom_tool_call_input.done",
                                  "sequence_number": seq, "input": patch_text, "item_id": item_id,
                                  "output_index": p.get("output_index", 0)}
                            new_blocks.append(f"event: response.custom_tool_call_input.done\ndata: {json.dumps(dn, ensure_ascii=False)}\n\n".encode())
                            log.info("    [apply_patch] emitted custom_tool_call stream (delta+done)")
                        except:
                            new_blocks.append(block)
                        break
                continue

            # 跳过 arguments.done（custom_tool_call 用 input 字段，不用 arguments）
            if "function_call_arguments.done" in evt_type:
                continue

            # 改写 output_item.done → custom_tool_call + input
            if "output_item.done" in evt_type and call_id in text:
                for ln in text.split("\n"):
                    if ln.startswith("data: "):
                        try:
                            p = json.loads(ln[6:])
                            p["item"] = {"id": f"ctc_{call_id}", "type": "custom_tool_call",
                                         "status": "completed", "call_id": call_id,
                                         "name": "apply_patch", "input": patch_text}
                            new_blocks.append(f"event: response.output_item.done\ndata: {json.dumps(p, ensure_ascii=False)}\n\n".encode())
                            log.info("    [apply_patch] rewrote output_item.done as custom_tool_call")
                        except:
                            new_blocks.append(block)
                        break
                continue

            # 改写 response.completed
            if "response.completed" in evt_type:
                for ln in text.split("\n"):
                    if ln.startswith("data: "):
                        try:
                            p = json.loads(ln[6:])
                            for out_item in p.get("response", {}).get("output", []):
                                if out_item.get("call_id") == call_id:
                                    out_item["type"] = "custom_tool_call"
                                    out_item["id"] = f"ctc_{call_id}"
                                    out_item["name"] = "apply_patch"
                                    out_item["input"] = patch_text
                                    out_item.pop("arguments", None)
                            new_blocks.append(f"event: response.completed\ndata: {json.dumps(p, ensure_ascii=False)}\n\n".encode())
                            log.info("    [apply_patch] rewrote response.completed as custom_tool_call")
                        except:
                            new_blocks.append(block)
                        break
                continue

            new_blocks.append(block)

        output_blocks[:] = new_blocks


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

        # 1) GET /v1/models → 静态返回
        if method == "GET" and self.path.rstrip("/") == "/v1/models":
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
            # 智谱 count_tokens 不支持 max_tokens=None（Python 崩溃）
            if "max_tokens" not in body or body.get("max_tokens") is None:
                body["max_tokens"] = 16384
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
        force_official = (client_key == "3")

        hour = datetime.now().hour
        weekday = datetime.now().weekday()  # 0=Mon, 6=Sun
        is_worktime = weekday < 5 and 9 <= hour < 18  # 工作日 9:00-18:00

        # 检测图片：有图片强制走 Messages（Completions 不支持图片，Messages 支持）
        has_images = is_responses and isinstance(body, dict) and _request_has_images(body)
        use_completions = RESPONSES_USE_COMPLETIONS and not has_images
        if has_images:
            log.info("    [image] 含图片 → 走 Messages 路径")

        # 确定本次请求需要的能力
        needs_completions = is_responses and use_completions
        needs_messages = is_messages or (is_responses and not use_completions)

        for up in UPSTREAMS:
            if up.get("disabled"):
                continue
            if force_official and up["name"] != "official":
                continue
            if up.get("require_health") and not internal_alive:
                continue
            if up.get("worktime_only") and not is_worktime:
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
                    # codex-relay 路径：转换 custom 工具（apply_patch 等）为 function 格式
                    _convert_custom_tools_for_completions(body)
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
                        # Responses→Messages 转换路径的流式处理
                        events, has_output = self._converted_stream(resp, up["name"])
                        if has_output:
                            return
                    elif is_responses:
                        events, has_output, stream_error = self._relay_stream(resp, up["name"])
                        # 上游返回 response.failed → 直接转发错误
                        if stream_error:
                            log.warning("[#%d]     !!! %s upstream error, forwarding to client: %s",
                                        self._req_id, up["name"], stream_error)
                            return
                        if has_output:
                            return
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
                log.error("[#%d]     !!! %s %d: %s", self._req_id, up["name"], e.code, err_body[:200].decode(errors="replace"))
                # 502/500 可能是上下文超限，返回标准错误触发客户端压缩
                if e.code in (500, 502) and body and (is_responses or is_messages):
                    max_ctx = up.get("max_context_tokens", 200000)
                    est_tokens = len(json.dumps(body).encode()) / 3.5
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
        last_usage = {}
        open_indices = []
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
                        # 就地合成正常结束（关闭未关闭的 content_block）
                        for idx in open_indices:
                            self.wfile.write(("event: content_block_stop\ndata: {\"type\": \"content_block_stop\", \"index\": " + str(idx) + "}\n\n").encode())
                        self.wfile.write(b'event: message_delta\ndata: {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 0}}\n\nevent: message_stop\ndata: {"type": "message_stop"}\n\n')
                        self.wfile.flush()
                        log.info("[#%d] [messages] %s stream error intercepted, synthesized end (closed %d blocks)", self._req_id, upstream_name, len(open_indices))
                        break  # 错误后结束流

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
            size_str = (str(size // 1024) + "KB") if size >= 1024 else (str(size) + "B")
            if last_usage:
                log.info("[#%d]     <<< %s STREAM OK (%s, %dms) usage: input=%d output=%d total=%d",
                         self._req_id, upstream_name, size_str, self._ms(), last_usage.get("input_tokens", 0),
                         last_usage.get("output_tokens", 0),
                         last_usage.get("input_tokens", 0) + last_usage.get("output_tokens", 0))
            else:
                log.info("[#%d]     <<< %s STREAM OK (%s, %dms)", self._req_id, upstream_name, size_str, self._ms())
        except (ConnectionResetError, BrokenPipeError):
            log.warning("[#%d]     <<< %s STREAM interrupted", self._req_id, upstream_name)

        return has_output, last_usage, context_exceeded, stream_error

    # ── Responses→Messages 转换流式处理 ──────────────
    def _converted_stream(self, resp, upstream_name):
        """缓冲 Anthropic Messages SSE → 逐块转换为 Responses SSE → 发给客户端"""
        import asyncio
        converter = AnthropicToOpenAIResponsesStreamAdapter()
        output_blocks = []
        has_output = False
        total_bytes = 0
        raw_blocks = []  # Anthropic 原始 SSE blocks

        try:
            buf = b""
            while True:
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
                    raw_blocks.append(block)

            if buf.strip():
                buf = buf.replace(b"\r\n", b"\n")
                raw_blocks.append(buf)

            # 将 Anthropic SSE 事件解析为 dict 列表
            events = []
            event_type_counts = {}
            for block in raw_blocks:
                event_type = None
                data_json = None
                for line in block.decode("utf-8", errors="replace").split("\n"):
                    if line.startswith("event: "):
                        event_type = line[7:].strip()
                    elif line.startswith("data: "):
                        try:
                            data_json = json.loads(line[6:])
                        except Exception:
                            pass
                if data_json and event_type:
                    # 用 dict 而非 Pydantic 模型（避免 Python 3.12 兼容问题）
                    data_json["_event_type"] = event_type
                    events.append(data_json)
                    event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
                    # 记录上游错误事件详情
                    if event_type == "error":
                        log.warning("    [converted] upstream error event: %s", data_json)
                    # 提取 message_delta 中的 stop_reason
                    if event_type == "message_delta":
                        stop = data_json.get("delta", {}).get("stop_reason")
                        if stop:
                            log.info("    [converted] upstream stop_reason=%s", stop)

            log.info("    [converted] upstream events: %s", event_type_counts)

            # 修复不完整的 Anthropic SSE 流（部分上游不发送关闭事件）
            has_message_stop = event_type_counts.get("message_stop", 0) > 0
            has_block_stop = event_type_counts.get("content_block_stop", 0) > 0
            has_error = event_type_counts.get("error", 0) > 0
            if not has_message_stop:
                log.warning("    [converted] upstream stream incomplete, synthesizing closing events")
                # 找到最后一个 content_block_start 的 index
                last_block_idx = 0
                for i, evt in enumerate(events):
                    if evt.get("_event_type") == "content_block_start":
                        last_block_idx = evt.get("index", 0)
                if not has_block_stop:
                    events.append({"_event_type": "content_block_stop", "type": "content_block_stop", "index": last_block_idx})
                # 内容审查错误 → end_turn（让 CLI 正常接受已完成部分）
                # 其他截断 → max_tokens（让 CLI 自动续写）
                stop_reason = "end_turn" if has_error else "max_tokens"
                events.append({"_event_type": "message_delta", "type": "message_delta",
                               "delta": {"stop_reason": stop_reason}, "usage": {"output_tokens": 0}})
                events.append({"_event_type": "message_stop", "type": "message_stop"})

            # 用 ccproxy 的转换器处理（同步包装异步）
            async def _convert():
                nonlocal has_output
                converted_blocks = []

                async def _gen():
                    for evt in events:
                        yield evt  # dict with _event_type key

                gen = converter.run(_gen())
                try:
                    async for out_event in gen:
                        # out_event 是 openai_models.StreamEventType (dict-like)
                        evt_dict = out_event if isinstance(out_event, dict) else out_event.model_dump(exclude_none=True, mode="json")
                        evt_type = evt_dict.get("type", "")
                        sse_block = f"event: {evt_type}\ndata: {json.dumps(evt_dict, ensure_ascii=False)}\n\n".encode()
                        converted_blocks.append(sse_block)
                        if b"output_text.delta" in sse_block or b"function_call_arguments.delta" in sse_block:
                            has_output = True
                finally:
                    try:
                        await gen.aclose()
                    except Exception:
                        pass
                return converted_blocks

            try:
                loop = asyncio.new_event_loop()
                output_blocks = loop.run_until_complete(_convert())
                loop.close()
            except Exception as e:
                log.error("    [converted] stream conversion error: %s", e)

            log.info("    [converted] raw=%d events → converted=%d blocks, has_output=%s",
                     len(events), len(output_blocks), has_output)

            # 修复 apply_patch：function_call → apply_patch_call
            _fix_apply_patch_args(output_blocks)

            # 从 response.completed 事件中提取 status 和 usage
            for block in output_blocks:
                try:
                    text = block.decode("utf-8", errors="replace")
                    if "response.completed" in text:
                        for line in text.split("\n"):
                            if line.startswith("data: "):
                                p = json.loads(line[6:])
                                resp_obj = p.get("response", {})
                                status = resp_obj.get("status", "?")
                                usage = resp_obj.get("usage", {})
                                log.info("    [converted] status=%s usage=%s", status,
                                         {k: v for k, v in usage.items() if v})
                except Exception:
                    pass

            if has_output or output_blocks:
                self.send_response(200)
                for h in ["Content-Type", "Cache-Control"]:
                    v = resp.headers.get(h)
                    if v:
                        self.send_header(h, v)
                self.send_header("Connection", "close")
                self.end_headers()
                for block in output_blocks:
                    self.wfile.write(block)
                    self.wfile.flush()
                self.close_connection = True
                log.info("[#%d]     <<< %s [converted] STREAM OK (%d events, %dKB, %dms)",
                         self._req_id, upstream_name, len(output_blocks), total_bytes // 1024, self._ms())
                # 保存 exchange
                resp_text = b"".join(output_blocks).decode("utf-8", errors="replace")
                self._save_exchange(getattr(self, '_debug_req_body', {}), resp_text, upstream_name, "converted")

        except (ConnectionResetError, BrokenPipeError):
            log.warning("[#%d]     <<< %s [converted] STREAM interrupted", self._req_id, upstream_name)

        return len(output_blocks), has_output

    # ── 流式转发 ─────────────────────────────────────
    def _relay_stream(self, resp, upstream_name):
        """读取并缓冲完整 SSE 流，检测空输出。返回 (events, has_output, stream_error)"""
        events = 0
        last_usage = {}
        has_output = False
        stream_error = None  # response.failed 中的错误
        saw_think = False  # 是否检测到 think 标签
        output_blocks = []
        raw_blocks = []  # 保存原始 block 用于 debug
        try:
            buf = b""
            while True:
                chunk = resp.read(4096)
                if not chunk:
                    break
                buf += chunk
                # SSE 分隔符兼容 \n\n 和 \r\n\r\n
                while b"\n\n" in buf or b"\r\n\r\n" in buf:
                    if b"\r\n\r\n" in buf:
                        block, buf = buf.split(b"\r\n\r\n", 1)
                    else:
                        block, buf = buf.split(b"\n\n", 1)
                    # 移除 block 中的 \r\n（保留内容）
                    block = block.replace(b"\r\n", b"\n")
                    raw_blocks.append(block)  # 记录原始
                    # 广泛检测：原始 block 中包含任何 think 相关内容
                    if b"<think" in block or b"</think" in block or b"&lt;think" in block or b"&lt;/think" in block:
                        saw_think = True
                        log.warning("    [think] RAW block: %s", repr(block[:300]))
                    out, usage, block_has_think = self._process_sse_block(block, upstream_name)
                    if block_has_think:
                        saw_think = True
                    if usage:
                        last_usage = usage
                    events += 1
                    output_blocks.append(out)
                    if b"output_text.delta" in block or b"function_call_arguments.delta" in block:
                        has_output = True
                    # 检测 response.failed（上游错误，不应截断重试）
                    if b"response.failed" in block:
                        try:
                            text = block.decode("utf-8", errors="replace")
                            for line in text.split("\n"):
                                if line.startswith("data: "):
                                    p = json.loads(line[6:])
                                    err = p.get("response", {}).get("error")
                                    if err:
                                        stream_error = err
                                        log.warning("[#%d]     !!! %s response.failed: %s", self._req_id, upstream_name, err)
                        except Exception:
                            pass
            if buf.strip():
                buf = buf.replace(b"\r\n", b"\n")
                raw_blocks.append(buf)
                out, usage, block_has_think = self._process_sse_block(buf, upstream_name)
                if block_has_think:
                    saw_think = True
                if usage:
                    last_usage = usage
                events += 1
                output_blocks.append(out)

            # 检测到 think 标签时保存原始 SSE 到 debug 文件
            if saw_think:
                ts = time.strftime("%Y%m%d_%H%M%S")
                path = os.path.join(LOG_DIR, f"debug_think_{ts}.txt")
                with open(path, "w", encoding="utf-8") as f:
                    for rb in raw_blocks:
                        f.write(rb.decode("utf-8", errors="replace") + "\n\n")
                log.warning("    [think] saved raw SSE to %s", path)

            if not has_output:
                for block in output_blocks:
                    try:
                        text = block.decode("utf-8", errors="replace")
                        for line in text.split("\n"):
                            if line.startswith("data: "):
                                p = json.loads(line[6:])
                                if p.get("type") == "response.completed":
                                    if p.get("response", {}).get("output"):
                                        has_output = True
                    except Exception:
                        pass

            # 修复 apply_patch：function_call → custom_tool_call（与 Messages 路径一致）
            _fix_apply_patch_args(output_blocks)

            # 保存 exchange 用于排查
            resp_text = b"".join(output_blocks).decode("utf-8", errors="replace")
            self._save_exchange(getattr(self, '_debug_req_body', {}), resp_text, upstream_name, "relay")

            if has_output:
                # 发送前检测处理后输出是否包含 think 标签
                for ob in output_blocks:
                    if b"<think" in ob or b"</think" in ob or b"&lt;think" in ob:
                        saw_think = True
                        log.warning("    [think] OUTPUT block (after process): %s", repr(ob[:300]))
                if saw_think:
                    # 保存完整处理后的输出
                    ts = time.strftime("%Y%m%d_%H%M%S")
                    path = os.path.join(LOG_DIR, f"debug_output_{ts}.txt")
                    with open(path, "w", encoding="utf-8") as f:
                        for ob in output_blocks:
                            f.write(ob.decode("utf-8", errors="replace"))
                    log.warning("    [think] saved OUTPUT to %s", path)
                self.send_response(200)
                for h in ["Content-Type", "Cache-Control"]:
                    v = resp.headers.get(h)
                    if v:
                        self.send_header(h, v)
                self.send_header("Connection", "close")
                self.end_headers()
                for block in output_blocks:
                    self.wfile.write(block)
                    self.wfile.flush()
                self.close_connection = True
                log.info("[#%d]     <<< %s STREAM OK (%d events, %dms)", self._req_id, upstream_name, events, self._ms())
                if last_usage:
                    inp = last_usage.get("input_tokens", 0)
                    out = last_usage.get("output_tokens", 0)
                    log.info("    usage: input=%d output=%d total=%d", inp, out, inp + out)
            else:
                # 空输出时检查是否有 response.failed 错误
                if not stream_error:
                    for block in output_blocks:
                        try:
                            text = block.decode("utf-8", errors="replace")
                            for line in text.split("\n"):
                                if line.startswith("data: "):
                                    p = json.loads(line[6:])
                                    if p.get("type") == "response.failed":
                                        stream_error = p.get("response", {}).get("error")
                        except Exception:
                            pass
                if stream_error:
                    # 上游明确报错 → 直接转发 response.failed 给客户端，不截断重试
                    log.warning("[#%d]     !!! %s upstream error, forwarding response.failed: %s",
                                self._req_id, upstream_name, stream_error)
                    self.send_response(200)
                    for h in ["Content-Type", "Cache-Control"]:
                        v = resp.headers.get(h)
                        if v:
                            self.send_header(h, v)
                    self.send_header("Connection", "close")
                    self.end_headers()
                    for block in output_blocks:
                        self.wfile.write(block)
                        self.wfile.flush()
                    self.close_connection = True
                    has_output = True  # 标记已处理，阻止外层截断重试
                else:
                    log.warning("[#%d]     !!! %s STREAM empty output (%d events)", self._req_id, upstream_name, events)

        except (ConnectionResetError, BrokenPipeError):
            log.warning("[#%d]     <<< %s STREAM interrupted", self._req_id, upstream_name)

        return events, has_output, stream_error

    @staticmethod
    def _process_sse_block(block, upstream_name=None):
        """透传 SSE block，修正 GLM 的 prompt_tokens:0，剥离推理标签"""
        has_think = False
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
        # 剥离推理标签（带日志）- 同时检测开始和闭合标签
        if pt == "response.output_text.delta":
            delta = p.get("delta", "")
            if "<think" in delta or "</think" in delta:
                has_think = True
                log.warning("    [think] delta BEFORE: %s", repr(delta[:100]))
                stripped = _strip_think_tags(delta)
                log.warning("    [think] delta AFTER: %s", repr(stripped[:100]))
                p["delta"] = stripped
        elif pt == "response.output_item.done":
            item = p.get("item", {})
            content = item.get("content", [])
            for c in content:
                if c.get("type") == "output_text":
                    text = c.get("text", "")
                    if "<think" in text or "</think" in text:
                        has_think = True
                        log.warning("    [think] output_item.done BEFORE: %s", repr(text[:200]))
                        stripped = _strip_think_tags(text)
                        log.warning("    [think] output_item.done AFTER: %s", repr(stripped[:200]))
                        c["text"] = stripped
        elif pt == "response.completed":
            for item in p.get("response", {}).get("output", []):
                content = item.get("content", [])
                for c in content:
                    if c.get("type") == "output_text":
                        text = c.get("text", "")
                        if "<think" in text or "</think" in text:
                            has_think = True
                            log.warning("    [think] response.completed BEFORE: %s", repr(text[:200]))
                            stripped = _strip_think_tags(text)
                            log.warning("    [think] response.completed AFTER: %s", repr(stripped[:200]))
                            c["text"] = stripped
        # 修正 response.completed 中的 usage（GLM 流式 prompt_tokens=0，需加 cached_tokens）
        if pt == "response.completed" and upstream_name:
            raw_usage = p.get("response", {}).get("usage")
            upstream_u = _upstream_usage.get(upstream_name)
            if upstream_u and raw_usage:
                prompt_tokens = raw_usage.get("input_tokens", 0)
                if prompt_tokens == 0:
                    cached = 0
                    details = upstream_u.get("prompt_tokens_details")
                    if details:
                        cached = details.get("cached_tokens", 0)
                    real_prompt = upstream_u.get("prompt_tokens", 0)
                    completion = raw_usage.get("output_tokens", 0)
                    corrected = real_prompt + cached
                    raw_usage["input_tokens"] = corrected
                    raw_usage["total_tokens"] = corrected + completion
                    if cached > 0:
                        raw_usage.setdefault("input_tokens_details", {})["cached_tokens"] = cached
                    log.info("    [response.completed] corrected usage: input=%d(+%d cached) output=%d",
                             real_prompt, cached, completion)
        # 提取 usage 用于日志
        usage = None
        if pt == "response.completed":
            usage = p.get("response", {}).get("usage")
        elif pt == "message_delta":
            delta_usage = p.get("usage")
            if delta_usage:
                usage = delta_usage
        out = f"event: {etype}\ndata: {json.dumps(p, ensure_ascii=False)}\n\n"
        return out.encode(), usage, has_think

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
        payload_bytes = len(json.dumps(body).encode())
        est_tokens = payload_bytes / 3.5
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
    log.info("GLM Proxy v2.9.2 :%d", LISTEN[1])
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

    # 健康检查线程
    threading.Thread(target=_check_internal, daemon=True).start()

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
