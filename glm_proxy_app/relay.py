"""codex-relay 拦截器与子进程生命周期管理。"""
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .common import (
    DEBUG, LOG_DIR, REQUEST_TIMEOUT, UPSTREAMS, ThreadedHTTPServer,
    _UPSTREAM_UA, dbg, log,
)

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
                # 但 assistant+tool_calls 消息保留 null（上游能正确转成 tool_use 块，
                # 改成 "" 反而会转成无效的空 text block 导致 422）
                if m.get("content") is None and not m.get("tool_calls"):
                    m["content"] = ""
                    null_content_fixed += 1
            if null_content_fixed:
                dbg("[relay] %s fixed %d null content → \"\"", up["name"], null_content_fixed)
                body = json.dumps(data).encode("utf-8")
            dbg("[relay] %s translated: %d msgs %dKB | roles=%s | stream=%s",
                     up["name"], len(messages), len(body) // 1024, roles, is_stream)
            # [DEBUG] 保存翻译后的请求用于排查（大请求才存，避免占磁盘）
            if DEBUG and len(messages) > 15:
                ts = time.strftime("%Y%m%d_%H%M%S")
                path = os.path.join(LOG_DIR, f"debug_relay_{ts}.json")
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                dbg("[relay] %s [DEBUG] saved translated request to %s", up["name"], path)
            if is_stream:
                body = json.dumps(data).encode("utf-8")
        except Exception:
            dbg("[relay] %s translated: %dKB (parse failed)", up["name"], len(body) // 1024)

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
                dbg("[relay] %s upstream response: %dKB", up["name"], resp_size // 1024)
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


def start_interceptors():
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

def start_relays():
    # 每次启动无条件做一次升级检查：已最新则 pip 无操作，有新版本则自动跟上
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-U", "codex-relay",
                               "--break-system-packages", "--quiet"])
    except Exception as e:
        log.warning("codex-relay upgrade check failed: %s (keep using installed)", e)
    binary = _find_relay_binary()
    if not binary:
        log.error("codex-relay binary not found! Run: pip install codex-relay")
        log.error("Or set CODEX_RELAY_BIN=/path/to/codex-relay")
        sys.exit(1)
    for up in UPSTREAMS:
        if "relay_port" not in up:
            continue  # 仅 Messages 的渠道不需要 codex-relay
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

def stop_relays():
    for proc in relay_procs:
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except:
            try:
                proc.kill()
            except:
                pass
