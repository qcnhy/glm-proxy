"""面向客户端的 HTTP Handler、路由和流式响应处理。"""
import itertools
import json
import os
import threading
import time
import traceback
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .common import (
    LOG_DIR, PROBE_TIMEOUT, REQUEST_TIMEOUT, STATIC_MODELS, UPSTREAMS,
    DEBUG, _UPSTREAM_UA, _block_channel_on_429, _cg_fresh_access, dbg,
    _channel_blocked_until, _req_counter, log,
)
from .transforms import (
    _est_tokens, _extract_additional_tools, _fix_tool_result_roles,
    _flatten_agent_messages, _inject_tool_rules, _is_overflow_signal,
    _merge_duplicate_tool_outputs, _normalize_sse_block,
    _patch_msg_usage, _route_mode, _strip_gpt_state,
)

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
            client_ip = (self.headers.get("X-Forwarded-For", "").split(",")[0].strip()
                         or self.headers.get("X-Real-IP", "")
                         or self.client_address[0])
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            try:
                body = json.loads(raw)
            except json.JSONDecodeError as je:
                # 客户端发的 JSON 非法或 Content-Length 与实际不符（截断/编码错乱）→
                # 存证 + 干净 400（异常冒到外层会让客户端挂着收不到响应）
                log.warning("[#%d] >>> [%s] POST %s BAD JSON: %s (declared_len=%d read=%d)",
                            self._req_id, client_ip, self.path, je, length, len(raw))
                log.warning("[#%d]     raw[:300]=%r", self._req_id, raw[:300])
                try:
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    path = os.path.join(LOG_DIR, f"debug_badjson_{ts}.json")
                    with open(path, "wb") as f:
                        f.write(raw)
                    log.warning("[#%d]     saved raw body to %s", self._req_id, path)
                except Exception:
                    pass
                err = json.dumps({"error": {"message": f"request body is not valid JSON: {je}",
                                            "type": "invalid_request_error"}}).encode()
                self._send_raw(400, err, "application/json")
                return
            is_stream = body.get("stream", False)
            self._debug_req_body = body
            # v2.9.102: 合并同 call_id 的重复 tool 输出（Codex 新版拆两条：空壳头+正文）
            if is_responses:
                n_merged = _merge_duplicate_tool_outputs(body)
                if n_merged:
                    dbg("[#%d]     [merge-out] 合并 %d 条重复 call_id 的 tool 输出", self._req_id, n_merged)

        # 3) 日志 + 计算 payload 大小
        raw_len = len(raw) if method == "POST" else 0
        if is_responses:
            log.info("[#%d] >>> [%s] POST %s stream=%s tools=%d input=%d",
                     self._req_id, client_ip, self.path, is_stream, len(body.get("tools", [])),
                     len(body.get("input", [])))
        else:
            log.info("[#%d] >>> [%s] %s %s", self._req_id, client_ip, method, self.path)

        # 4) 遍历上游，自动回退（不截断——客户端会自动压缩，错误都是实际错误）
        last_err = None
        body_saved = False  # 调试body只保存一次（首次失败时）
        route_sse_committed = False  # GPT 已先发心跳时，后续 GLM 复用同一 SSE，不重复发 HTTP 头

        # 客户端 Authorization key 路由：
        # - key=0 → 第一个未 disabled 的 chain_exclude 直通渠道；没有时保持空缺
        # - key=N (N>=1) → 第 N 个未 disabled 的普通链内渠道（1-based）
        # - 其他 key → 默认按配置顺序回退（不含 chain_exclude 渠道）；受模型钉选 / 429 封锁影响
        client_key = self.headers.get("Authorization", "").replace("Bearer ", "").strip()
        force_upstream = None  # 强制渠道 name；None 表示默认回退链
        # 普通 key 按当前启用渠道动态编号；禁用渠道后，后续渠道顺序前移。
        chain_upstreams = [u for u in UPSTREAMS if not u.get("chain_exclude") and not u.get("disabled")]
        exclude_upstreams = [u for u in UPSTREAMS if u.get("chain_exclude")]
        enabled_excludes = [u for u in exclude_upstreams if not u.get("disabled")]
        if client_key == "0" and enabled_excludes:
            force_upstream = enabled_excludes[0]["name"]
        elif client_key.isdigit() and int(client_key) >= 1:
            idx = int(client_key) - 1
            if idx < len(chain_upstreams):
                force_upstream = chain_upstreams[idx]["name"]
            else:
                log.warning("[#%d] key=%s out of range (1..%d), fallback to default chain",
                            self._req_id, client_key, len(chain_upstreams))
        elif client_key == "0":
            log.warning("[#%d] key=0 has no enabled direct upstream, fallback to default chain", self._req_id)

        # 普通回退链不能解析其他上游的服务端 item 引用。进入链前删除所有纯
        # item_reference、rs_*/reasoning 和 previous_response_id，保留完整消息及工具记录。
        # 钉选/数字 key 指向链内渠道时同样需要清理（GPT 会话切渠道不能带 rs_*）；
        # 唯一例外：目标是无状态 responses_direct 渠道（循环内自行 strip）。
        _force_up = next((u for u in UPSTREAMS if u.get("name") == force_upstream), None) if force_upstream else None
        _force_is_stateless_gpt = bool(_force_up and _force_up.get("responses_direct")
                                       and _force_up.get("store_responses") is False)
        if is_responses and isinstance(body, dict) and not _force_is_stateless_gpt:
            _n_gpt, _had_prev = _strip_gpt_state(body)
            if _n_gpt or _had_prev:
                dbg("[#%d]     [gpt-state] stripped %d GPT item(s), previous_response_id=%s",
                    self._req_id, _n_gpt, _had_prev)

        # GET/无 body 时也要初始化，避免 ROUTE 日志 NameError
        req_model = ""
        # 模型名钉选：客户端指定的 model 若在 config 里有匹配 → 只按顺序走匹配的渠道，不回退到其他模型
        model_pinned = False  # True=只走 model 匹配的渠道
        if isinstance(body, dict):
            req_model = body.get("model") or ""
            if req_model and not force_upstream:
                # 检查是否有任何渠道的 model == req_model（responses_direct 渠道不参与钉选，只走默认回退）
                if any(not up.get("disabled") and not up.get("responses_direct") and
                       (up.get("model") == req_model or up.get("messages_model") == req_model)
                       for up in UPSTREAMS):
                    model_pinned = True
                # 注意：responses_direct（GPT 直通）渠道不参与模型钉选，只能通过 key=0 直达。

        # Responses 不按内容类型分叉；图片与文本走同一条协议路径。
        blocked_active = {k: datetime.fromtimestamp(v).strftime("%H:%M") for k, v in _channel_blocked_until.items() if v > time.time()}
        log.info("[#%d] ROUTE: key=%s model=%s force=%s pinned=%s blocked=%s path=%s",
                 self._req_id, client_key[:20] or "(empty)", req_model or "?",
                 force_upstream, model_pinned, blocked_active or "{}",
                 self.path)
        for up in UPSTREAMS:
            if up.get("disabled"):
                continue
            # chain_exclude 渠道永不参与自动回退链，仅数字 key 可手动直达。
            if up.get("chain_exclude") and up["name"] != force_upstream:
                continue
            # v2.9.103: 超大载荷跳过 cf_gate 渠道（如 cmoyan）——大上下文会话源站处理超
            # Cloudflare 100s 上限必 524，白等约两分钟才回退。≥1MB 直接跳过走下一渠道。
            # v2.9.108: 改按 cf_gate 标志判定——chatgpt 官方 backend 早发 response.created
            # 不触发百秒闸，不受此限。
            if not force_upstream and up.get("cf_gate") and len(raw) >= 1024 * 1024:
                log.info("[#%d]     [skip] %s: 载荷 %dKB ≥1MB，Cloudflare 必 524，跳过",
                         self._req_id, up["name"], len(raw) // 1024)
                continue
            # 数字 key 强制：只走对应渠道（跳过封锁限制）
            if force_upstream:
                if up["name"] != force_upstream:
                    continue
            else:
                # 模型名钉选：只走 model 匹配的渠道，跳过不匹配的
                if model_pinned:
                    if req_model != up.get("model") and req_model != up.get("messages_model"):
                        continue
                # 渠道封锁检查：429 限额封锁期内跳过对应渠道
                block_key = up["name"]
                if _channel_blocked_until.get(block_key, 0) > time.time():
                    log.info("[#%d]     [skip] %s blocked until %s",
                             self._req_id, up["name"],
                             datetime.fromtimestamp(_channel_blocked_until[block_key]).strftime("%H:%M"))
                    continue
            route_mode = _route_mode(up, is_responses, is_messages)
            if route_mode is None:
                continue

            # 构建目标 URL 和请求头（每个上游只需一次）
            if route_mode == "responses_direct":
                # 原生 Responses 端点直通
                api_path = self.path
                if api_path.startswith("/v1/"):
                    api_path = api_path[3:]
                url = up["openai_url"].rstrip("/") + api_path
                up_headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {up.get('key', '')}",
                    "Connection": "close",
                }
                if up.get("tokens_file"):
                    # ChatGPT 订阅 OAuth：fresh access_token + 账号头 + beta 头（实测必需组合）
                    _acc, _acct = _cg_fresh_access(up)
                    if _acc:
                        up_headers["Authorization"] = f"Bearer {_acc}"
                        if _acct:
                            up_headers["chatgpt-account-id"] = _acct
                        up_headers["OpenAI-Beta"] = "responses=experimental"
                        up_headers["originator"] = "codex_cli_rs"
            elif route_mode == "responses_relay":
                # Responses API → codex-relay（Chat Completions 路径，OpenAI 原生）
                url = f"http://127.0.0.1:{up['relay_port']}{self.path}"
                up_headers = {
                    "Content-Type": "application/json",
                    "Authorization": self.headers.get("Authorization", ""),
                }
            elif route_mode == "messages":
                # Messages API → Anthropic 端点
                url = up["anthropic_url"].rstrip("/")
                mkey = up["key"]
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
            else:  # route_mode == "openai"
                # 通用 OpenAI 路径（/v1/chat/completions 等）
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
                    dbg("    fixed %d tool_use inputs (list→dict)", fixed)


            # 生成 payload（每个 upstream 只尝试一次，不截断重试——客户端会自动压缩）
            payload = None
            if body is not None:
                # 无状态 GPT 渠道同样不能解析上一轮未持久化或其他上游的纯 item_reference。
                if route_mode == "responses_direct" and up.get("store_responses") is False:
                    _n_gpt, _had_prev = _strip_gpt_state(body)
                    if _n_gpt or _had_prev:
                        dbg("[#%d]     [gpt-state] %s store=false stripped %d item(s), previous_response_id=%s",
                            self._req_id, up["name"], _n_gpt, _had_prev)
                # Messages 路径走 Anthropic 端点，用 messages_model（如 grok-4.5-claude）；
                # relay/通用 OpenAI 路径用 model（OpenAI 名）。
                # responses_direct 渠道透传客户端原始 model（GPT 原生名，上游按名路由）；
                # 用 req_model 而非 body["model"]——回退链前面的渠道可能已把 body["model"] 改写成自己的 model
                if up.get("responses_direct"):
                    body["model"] = req_model
                else:
                    body["model"] = up.get("messages_model", up["model"]) if is_messages else up["model"]
                if is_responses and body.get("previous_response_id"):
                    pid_len = len(json.dumps(body, ensure_ascii=False))
                    if pid_len > 200000:
                        log.warning("    payload %dKB, stripping previous_response_id", pid_len // 1024)
                        del body["previous_response_id"]
                _extract_additional_tools(body)  # Codex additional_tools(input 内) → 顶层 tools
                _flatten_agent_messages(body)  # Codex 多智能体 agent_message/encrypted_content → message/input_text
                # namespace 展平已移除（v2.9.93）：codex-relay 0.5.8 (#62) 原生处理 namespace，
                # custom 子工具裸名/function 子工具 namespaced 编码由 relay 自己正确 round-trip。
                _inject_tool_rules(body)  # 注入 exec 沙箱规则（relay 路径覆盖）
                # messages 路径防御：若 tools 仍含 "type":"custom"（不应出现）则剔之。
                _tools_bak = None
                if up.get("strip_custom_tools"):
                    _tools_list = body.get("tools")
                    if isinstance(_tools_list, list) and _tools_list:
                        _tools_bak = body["tools"]
                        _before = len(_tools_bak)
                        body["tools"] = [t for t in _tools_bak if t.get("type") != "custom"]
                        if len(body["tools"]) != _before:
                            log.info("    [strip] %s: %d → %d tools (stripped custom)", up["name"], _before, len(body["tools"]))

                if is_messages:
                    _fix_tool_result_roles(body)
                # ChatGPT backend 拒绝 max_output_tokens；store 由渠道显式配置。
                _scg = up.get("strip_max_output")
                _mox = body.pop("max_output_tokens", None) if _scg else None
                _force_store = up.get("store_responses")
                _store_bak = body.get("store", "__absent__") if _force_store is not None else "__skip__"
                if _force_store is not None:
                    body["store"] = bool(_force_store)
                payload = json.dumps(body).encode()
                if _mox is not None:
                    body["max_output_tokens"] = _mox  # 恢复（回退下一上游时原样）
                if _store_bak == "__absent__":
                    body.pop("store", None)
                elif _store_bak != "__skip__":
                    body["store"] = _store_bak
                if "Content-Type" not in up_headers and payload is not None:
                    up_headers["Content-Type"] = "application/json"
                if _tools_bak is not None:
                    body["tools"] = _tools_bak  # 恢复 tools（回退下一上游时原样）
            if up.get("tokens_file"):
                up_headers["User-Agent"] = "codex_cli_rs/0.45.0"  # ChatGPT backend 认 codex UA（实测组合）
            else:
                up_headers["User-Agent"] = _UPSTREAM_UA  # 防 Cloudflare 1010

            log.info("[#%d]     -> %s", self._req_id, up["name"])

            # GPT 直通为 key=0 独占渠道：连接上游等待响应头期间先向客户端提交 SSE
            # 并发送心跳，避免长首包触发 Codex idle timeout。
            _connect_ka_stop = None
            if is_stream and is_responses and route_mode == "responses_direct":
                _connect_ka_stop = threading.Event()
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                self.wfile.write(b": connected\n\n")
                self.wfile.flush()
                route_sse_committed = True

                def _connect_keepalive():
                    while not _connect_ka_stop.wait(3):
                        try:
                            self.wfile.write(b": keepalive\n\n")
                            self.wfile.flush()
                        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                            break
                threading.Thread(target=_connect_keepalive, daemon=True).start()

            try:
                req = Request(url, data=payload, headers=up_headers, method=method)
                resp = urlopen(req, timeout=REQUEST_TIMEOUT)
                if _connect_ka_stop is not None:
                    _connect_ka_stop.set()

                if is_stream:
                    if is_responses:
                        # relay 路径：立即发头+keepalive（idle安全），早期 1305/过载退避重试
                        events, has_output, stream_error, fallback = self._relay_stream_with_retry(
                            resp, up, url, up_headers, payload, method, _est_tokens(body),
                            precommitted=route_sse_committed)
                        if fallback:
                            # 未发头即失败（429限流/502不可达/DNS等），回退下一上游；
                            # 记住错误供链尾 _send_last_error 呈现（否则全链失败时客户端只看到 None）
                            fcode, ferr = fallback
                            try:
                                last_err = (int(fcode), json.dumps({"error": ferr}).encode())
                            except (TypeError, ValueError):
                                last_err = (502, b'{"error":"upstream failed before output"}')
                            if route_mode == "responses_direct":
                                # 链外 GPT 失败后解除强制选择；后续与普通链使用同一回退规则。
                                force_upstream = None
                                _strip_gpt_state(body)
                            log.warning("[#%d]     !!! %s failed before output, trying next upstream",
                                        self._req_id, up["name"])
                            continue
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
                else:
                    result = resp.read()
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
                            dbg("    usage: input=%d output=%d total=%d",
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
                if _connect_ka_stop is not None:
                    _connect_ka_stop.set()
                    last_err = (e.code, err_body)
                    force_upstream = None
                    _strip_gpt_state(body)
                    log.warning("[#%d]     !!! %s HTTP %d before output, fallback to GLM chain",
                                self._req_id, up["name"], e.code)
                    continue
                last_err = (e.code, err_body)
                log.error("[#%d]     !!! %s HTTP %d: %s", self._req_id, up["name"], e.code, err_body[:300].decode(errors="replace"))
                # 任意路径 + HTTP 超限(ecloud/venus/internal 等 400 带超限文案) → 返干净 400 触发客户端压缩（不回退）
                _ovtxt = err_body.decode("utf-8", errors="replace")
                if (is_messages or is_responses) and _is_overflow_signal(_ovtxt):
                    self._send_overflow_400(up, _est_tokens(body), as_responses=is_responses, upstream_text=_ovtxt)
                    return
                # 429 rate_limit → 解析重置时间并封锁 official（HTTP 错误，所有路径都经过此处）
                if e.code == 429:
                    dbg("[#%d]     [429] HTTPError 429 from %s, calling _block_channel_on_429", self._req_id, up["name"])
                    _block_channel_on_429(err_body, up["name"], self._req_id)
                    dbg("[#%d]     [429] after block: %s", self._req_id,
                        {k: datetime.fromtimestamp(v).strftime("%H:%M") for k, v in _channel_blocked_until.items() if v > time.time()})
                # 502/500 可能是上下文超限 → 统一入口检测+返400触发客户端压缩重试（缺口1）
                if e.code in (500, 502) and body and (is_responses or is_messages):
                    if self._send_context_exceeded(body, up):
                        return
                continue  # 尝试下一个上游（不截断重试）
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
                if _connect_ka_stop is not None:
                    _connect_ka_stop.set()
                    last_err = e
                    force_upstream = None
                    _strip_gpt_state(body)
                    log.warning("[#%d]     !!! %s connection reset before output, fallback to GLM chain: %s",
                                self._req_id, up["name"], e)
                    continue
                log.error("[#%d]     !!! %s connection reset: %s", self._req_id, up["name"], e)
                last_err = e
                continue
            except Exception as e:
                if _connect_ka_stop is not None:
                    _connect_ka_stop.set()
                    last_err = e
                    force_upstream = None
                    _strip_gpt_state(body)
                    log.warning("[#%d]     !!! %s error before output, fallback to GLM chain: %s",
                                self._req_id, up["name"], e)
                    continue
                last_err = e
                log.error("[#%d]     !!! %s error: %s", self._req_id, up["name"], e)
                continue

        # 所有上游都失败
        if body and (is_responses or is_messages) and not body_saved:
            self._save_debug_body(body)
        if route_sse_committed:
            if isinstance(last_err, tuple):
                code, raw_err = last_err
                try:
                    err_obj = json.loads(raw_err).get("error")
                except Exception:
                    err_obj = {"code": str(code), "message": raw_err.decode("utf-8", errors="replace")}
            else:
                err_obj = {"code": "all_upstreams_failed", "message": str(last_err or "all upstreams failed")}
            failed = {"type": "response.failed", "response": {"status": "failed", "error": err_obj}}
            self.wfile.write(("event: response.failed\ndata: " + json.dumps(failed, ensure_ascii=False) + "\n\n").encode())
            self.wfile.flush()
            return
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
                                elif _is_overflow_signal(_btxt := block.decode("utf-8", errors="replace")) or b'"message_stop"' in block:
                                    # 上游超限信号(model_context_window_exceeded / context_length_exceeded /
                                    # "prompt is too long" 等)或空完整收尾(出内容前 message_stop) → 返干净 400
                                    # 触发客户端 auto-compact（探测期未提交 200，可直接 _send_raw(400)）
                                    stop_ka.set()
                                    try:
                                        resp.close()
                                    except Exception:
                                        pass
                                    self._send_overflow_400(up, est_input, upstream_text=_btxt)
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
                    if not saw_message_stop:  # 不完整 → 明确报错，不能伪装正常 end_turn
                        log.warning("[#%d] [messages] %s incomplete (no message_stop), forwarding stream error",
                                    self._req_id, upstream_name)
                        for idx in open_indices:
                            _write(("event: content_block_stop\ndata: {\"type\": \"content_block_stop\", \"index\": " + str(idx) + "}\n\n").encode())
                        _write(b'event: error\ndata: {"type":"error","error":{"type":"api_error","message":"upstream stream ended before message_stop"}}\n\n')
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
                # 非内容（上游早断/不完整/空但无超限信号；超限已在探测期返 400）→ 明确报错
                _commit()
                for hb in held:
                    _write(hb)
                if held and not saw_message_stop:
                    _write(b'event: error\ndata: {"type":"error","error":{"type":"api_error","message":"upstream stream ended before message_stop"}}\n\n')
                log.warning("[#%d]     <<< %s STREAM non-content/incomplete (%dms)",
                            self._req_id, upstream_name, self._ms())
                return
        except Exception as e:
            log.error("[#%d] [messages] %s stream exception: %s",
                      self._req_id, upstream_name, e)
            try:
                _commit()
                _write(b'event: error\ndata: {"type":"error","error":{"type":"api_error","message":"upstream stream interrupted"}}\n\n')
            except Exception:
                pass
        finally:
            stop_ka.set()

    # ── 流式转发 ─────────────────────────────────────
    def _relay_stream_with_retry(self, first_resp, up, url, up_headers, payload, method,
                                 est_input=0, precommitted=False):
        """增量流式转发 codex-relay 的 Responses SSE（probe-before-commit，靠 codex-relay 自带 keepalive）。
        延迟提交 200：非内容块(response.created 等)握住不发，首次 _write 才提交 200+flush；
        overflow(超限信号/空completed)→返干净 400(as_responses)触发客户端压缩；其他错误与 messages 一致直接转发(429另封锁渠道)。
        返回 (events, has_output, stream_error, fallback)。fallback=(code, err) 表示尚无可交付输出，
        无论 SSE 是否已提交都应尝试下一渠道；已有正文/合法工具调用后失败才在当前流内结束。"""
        import threading
        upstream_name = up["name"]
        events = 0
        last_usage = {}
        has_output = False
        stream_error = None
        stream_incomplete = False
        held_completed = None
        raw_blocks = []

        committed = [precommitted]
        wlock = threading.Lock()
        stop_ka = threading.Event()

        def _keepalive():
            while not stop_ka.wait(3):
                try:
                    with wlock:
                        if committed[0]:
                            self.wfile.write(b": keepalive\n\n")
                            self.wfile.flush()
                except Exception:
                    break

        if precommitted:
            threading.Thread(target=_keepalive, daemon=True).start()

        def _commit_locked():
            # 调用方须持 wlock。probe-before-commit：延迟提交 200，首次 _write 才提交，
            # 便于超限(200流式)时改返干净 400。commit 后启动 keepalive——
            # responses_direct 直通渠道无 codex-relay keepalive，GPT 大上下文首内容可达数分钟，
            # 不发心跳客户端会 "Stream idle timeout" 断开（与 messages 路径 v2.9.80 同型问题）
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

        def _commit():
            with wlock:
                _commit_locked()

        def _write(data):
            with wlock:
                _commit_locked()  # 首次写入前才提交 200（探测期握住不发，超限可不提交直接 400）
                self.wfile.write(data)
                self.wfile.flush()

        def _emit(ed):
            _write((f"event: {ed.get('type', '')}\ndata: {json.dumps(ed, ensure_ascii=False)}\n\n").encode())

        def _probe_watchdog():
            # probe-hold 兜底（与 messages 路径一致）：PROBE_TIMEOUT 仍无首内容 → 强制 commit + keepalive
            if stop_ka.wait(PROBE_TIMEOUT):
                return
            if not committed[0]:
                log.info("[#%d] [relay] %s probe-hold %ds 无首内容，强制提交防 idle timeout",
                         self._req_id, upstream_name, PROBE_TIMEOUT)
                try:
                    _commit()
                except Exception:
                    pass
        threading.Thread(target=_probe_watchdog, daemon=True).start()

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
                        # reasoning 只是模型内部推理，不能证明最终正文/工具调用有效。
                        # 若把 reasoning 当首内容提交 200，随后 invalid_tool_call 等失败就无法回退。
                        # 只有可交付正文或已通过 relay 校验的工具调用才提交响应头。
                        _is_content = (b"output_text.delta" in out
                                       or b"function_call_arguments.delta" in out
                                       or b"custom_tool_call" in out)
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
                if not committed[0] and (
                        (early_err and not has_output
                         and _is_overflow_signal(json.dumps(early_err[1], ensure_ascii=False)))
                        or (held_completed is not None and not has_output)):
                    # 仅探测期（未发 200 头）可改返 400；watchdog 已强制 commit 则走正常收尾，防二次 send_response
                    try:
                        resp.close()
                    except Exception:
                        pass
                    self._send_overflow_400(up, est_input, as_responses=True,
                                            upstream_text=json.dumps(early_err[1], ensure_ascii=False) if early_err else "")
                    return events, has_output, None, False

                # 已预提交 SSE（GPT 心跳/探测 watchdog）时无法改返 HTTP 400；空 completed
                # 仍属于“无可交付输出”，统一交给外层回退下一渠道。
                if committed[0] and held_completed is not None and not has_output:
                    try:
                        resp.close()
                    except Exception:
                        pass
                    return events, False, None, ("empty_output", {
                        "code": "empty_output", "message": "upstream completed without output"})

                # === 早期 response.failed（非超限）：未发头一律回退下一上游（429 先封锁渠道）；已发头才转发 ===
                if early_err and not has_output and not stream_error:
                    code, err, out_bytes = early_err
                    # 429/rate_limit → 封锁渠道（直到重置）
                    if isinstance(err, dict):
                        ec = str(err.get("code", ""))
                        if ec == "429" or err.get("type") == "rate_limit_error":
                            dbg("[#%d]     [429] SSE response.failed code=%s from %s, calling _block_channel_on_429",
                                self._req_id, ec, upstream_name)
                            _block_channel_on_429(json.dumps(err).encode(), upstream_name, self._req_id)
                            dbg("[#%d]     [429] after block: %s", self._req_id,
                                {k: datetime.fromtimestamp(v).strftime("%H:%M") for k, v in _channel_blocked_until.items() if v > time.time()})
                    # 统一规则：是否已提交 SSE 不影响回退；只要尚无正文/合法工具调用，
                    # 就丢弃该渠道握住的 created/reasoning/error，交由外层尝试下一渠道。
                    log.warning("[#%d]     !!! %s failed before output (code=%s), trying next upstream",
                                self._req_id, upstream_name, code)
                    try:
                        resp.close()
                    except Exception:
                        pass
                    return events, has_output, None, (code, err)

                # 上游 EOF 但没有 response.completed：无输出时允许统一回退；已有输出时
                # 不能跨模型续写，也不能伪造 completed，明确告诉客户端本次响应不完整。
                if held_completed is None and not stream_error:
                    incomplete_err = {
                        "code": "incomplete_stream",
                        "message": "upstream stream ended before response.completed",
                    }
                    if not has_output:
                        log.warning("[#%d]     !!! %s stream ended before response.completed without output, trying next upstream",
                                    self._req_id, upstream_name)
                        try:
                            resp.close()
                        except Exception:
                            pass
                        return events, False, None, ("incomplete_stream", incomplete_err)
                    stream_incomplete = True
                    stream_error = incomplete_err
                    log.warning("[#%d]     !!! %s stream ended before response.completed after output",
                                self._req_id, upstream_name)

                # 正常完成或已真实输出后的失败：先 flush 还握住的块，保证事件结构合法。
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
                            u.setdefault("output_tokens", 0)  # Codex required 字段，上游缺失时补
                            u["input_tokens"] = int(est_input)
                            u["total_tokens"] = int(est_input) + int(u.get("output_tokens", 0) or 0)
                            # 同步到 last_usage（日志用——否则 last_usage 是上游原始值=0）
                            last_usage["input_tokens"] = int(est_input)
                            last_usage["total_tokens"] = int(est_input) + int(last_usage.get("output_tokens", 0) or 0)
                        except Exception:
                            pass
                    _emit(held_completed)
                elif stream_incomplete:
                    _emit({"type": "response.failed", "sequence_number": events + 1,
                           "response": {"id": "resp_incomplete", "object": "response",
                                        "status": "failed", "error": stream_error}})
                break  # 完成，退出重试循环

            # 保存 exchange 用于排查
            resp_text = b"".join(raw_blocks).decode("utf-8", errors="replace")
            self._save_exchange(getattr(self, '_debug_req_body', {}), resp_text, upstream_name, "relay")

            if stream_error:
                log.warning("[#%d]     <<< %s STREAM FAILED (%d events, %dms): %s",
                            self._req_id, upstream_name, events, self._ms(), stream_error)
            else:
                log.info("[#%d]     <<< %s STREAM OK (%d events, %dms)", self._req_id, upstream_name, events, self._ms())
            if last_usage:
                inp = last_usage.get("input_tokens", 0)
                out_ = last_usage.get("output_tokens", 0)
                dbg("    usage: input=%d output=%d total=%d", inp, out_, inp + out_)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            log.warning("[#%d]     <<< %s STREAM interrupted", self._req_id, upstream_name)
        finally:
            stop_ka.set()  # 停 keepalive / watchdog

        return events, True, stream_error, False  # 已有可交付输出，禁止跨模型回退

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

    def _send_overflow_400(self, up, est_tokens=0, as_responses=False, upstream_text=""):
        """超限：向客户端返干净 HTTP 400（触发其 auto-compact）。
        不做 est 预判——调用方已通过上游实际响应(_is_overflow_signal 命中)确认是真超限。
        as_responses=True → OpenAI Responses 错误形态（relay，Codex）；
        否则 Anthropic 形态（messages，Claude Code，H1 实锤：此形态触发 auto-compact）。
        upstream_text：上游原始报错——从中提取真实上限数字（est 低估/配置 max 虚高时避免矛盾文案）"""
        import re as _re
        max_ctx = up.get("max_context_tokens", 200000)
        shown_max = max_ctx
        if upstream_text:
            m = _re.search(r"maximum context length is (\d+)|context length of (\d+)", upstream_text)
            if m:
                shown_max = int(m.group(1) or m.group(2))
        msg = (f"prompt is too long: ~{int(est_tokens)} tokens > {shown_max} maximum context window"
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
        # responses_direct（GPT 直通）不走 est 启发式：GPT 超限会返回结构化 400（带真实上限），
        # 由 _is_overflow_signal 路径处理；est>0.9*兜底值(200K) 的猜测对 GPT 只会误伤。
        if up.get("responses_direct"):
            return False
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
        if not DEBUG:
            return
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = os.path.join(LOG_DIR, f"debug_err_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.write(json.dumps(body, ensure_ascii=False, indent=2))
        log.info("    !!! saved to %s", path)

    def _save_debug_body(self, body):
        """保存大请求体用于排查上下文问题"""
        if not DEBUG:
            return
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = os.path.join(LOG_DIR, f"debug_req_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.write(json.dumps(body, ensure_ascii=False, indent=2))
        log.info("    saved request body to %s", path)

    def _save_exchange(self, body, response_data, upstream_name, note=""):
        """保存请求+响应用于排查"""
        if not DEBUG:
            return
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
