"""请求、工具调用与 SSE 数据的无状态规范化函数。"""
import json
import re

from .common import REQUEST_TIMEOUT, dbg





def _concat_tool_output(a, b):
    """合并两条 tool 输出（保持首条形态：str+str→str，其余→块列表）。"""
    if a is None:
        return b
    if b is None:
        return a
    if isinstance(a, str) and isinstance(b, str):
        # 空壳头以 "Output:\n" 结尾，正文直接续上；其余用换行分隔
        return a if a.endswith("\n") else a + "\n" + b

    def _blocks(x):
        if isinstance(x, list):
            return [y for y in x if isinstance(y, dict)]
        if isinstance(x, str):
            return [{"type": "input_text", "text": x}] if x else []
        return []

    return _blocks(a) + _blocks(b)


def _merge_duplicate_tool_outputs(body):
    """Codex 新版 exec 会把同一次调用的输出拆成多条 input 记录（先"Script completed...Output:\\n"
    空壳头，正文 text()/notify() 载荷在后续条目，call_id 相同）。真实 Responses 后端能容忍重复
    call_id，但 relay 转换层每 call_id 只认一条 → 模型只看到空壳头（"工具输出全是空"）。
    这里按出现顺序合并同 call_id 的输出，恢复"头+正文"单条形态（12:03 健康时期的 wire 格式）。
    返回合并掉的条目数。"""
    items = body.get("input")
    if not isinstance(items, list):
        return 0
    merged = []
    last_out_idx = {}  # call_id → 在 merged 中的下标
    n_merged = 0
    for it in items:
        if isinstance(it, dict) and it.get("type") in ("custom_tool_call_output", "function_call_output"):
            cid = it.get("call_id")
            idx = last_out_idx.get(cid) if cid is not None else None
            if idx is not None:
                prev = merged[idx]
                prev["output"] = _concat_tool_output(prev.get("output"), it.get("output"))
                n_merged += 1
                continue
            if cid is not None:
                last_out_idx[cid] = len(merged)
        merged.append(it)
    if n_merged:
        body["input"] = merged
    return n_merged


def _strip_gpt_state(body):
    """删除只能由原 GPT Responses 上游解析的服务端状态引用。

    所有纯 item_reference 都不可跨上游重放，无论 ID 前缀；reasoning/rs_* 同样删除。
    带完整内容的普通消息、函数调用及工具输出保持不变，使请求可以进入无状态 GPT 或 GLM 回退链。
    返回删除的 input item 数量以及是否删除 previous_response_id。
    """
    if not isinstance(body, dict):
        return 0, False
    removed_previous = body.pop("previous_response_id", None) is not None
    items = body.get("input")
    if not isinstance(items, list):
        return 0, removed_previous
    kept = []
    removed = 0
    for item in items:
        item_id = item.get("id", "") if isinstance(item, dict) else ""
        item_type = item.get("type", "") if isinstance(item, dict) else ""
        if (item_type in ("reasoning", "item_reference")
                or (isinstance(item_id, str) and item_id.startswith("rs_"))):
            removed += 1
            continue
        kept.append(item)
    if removed:
        body["input"] = kept
    return removed, removed_previous


def _route_mode(up, is_responses, is_messages):
    """根据请求需求和渠道能力选择统一路由模式；None 表示不兼容。

    路由只依赖能力字段，不依赖渠道名称或配置位置。
    """
    if is_responses:
        # v5.1.0: 统一 responses_url — 渠道配置完整 Responses 端点 URL 直通
        # （官方 /api/v1、venusgroup /ai/zhipu/responses、GPT 后端等）。
        if up.get("responses_url"):
            return "responses_url"
        # v5.0.0: codex-relay 退役。无原生 Responses 能力且无 relay 的渠道不再承接
        # responses 请求（跳过走回退链）。
        return None
    if is_messages:
        return "messages" if up.get("anthropic_url") else None
    # v5.2.5: /v1/chat/completions 等通用 OpenAI 路径仍需直通（completion_url 为专用
    # completions 端点，如智谱 coding plan 的 /api/coding/paas/v4）；responses_url
    # 只声明 Responses 端点能力，不覆盖 completions。有 completion_url 即可承接。
    if up.get("completion_url"):
        return "openai"
    return None


def _select_upstream_model(up, requested_model, is_messages=False):
    """按配置中固化的模型清单选择模型，不在运行时访问上游。

    客户端模型匹配 available_models（v4.8.1 起大小写不敏感）时透传（用清单内的
    规范写法，避免上游大小写敏感导致 MODEL_NOT_SUPPORTED）；不匹配时使用该路径的
    配置默认值。available_models 未配置时保持旧行为，仅使用默认值。
    """
    default_model = up.get("messages_model", up["model"]) if is_messages else up["model"]
    available = up.get("available_models")
    if requested_model and isinstance(available, list):
        for m in available:
            if isinstance(m, str) and m.casefold() == requested_model.casefold():
                return m
    return default_model


def _stream_timeout_error(has_output, first_output_timeout):
    """将 Responses 读超时分类为可回退的首输出超时或不可续写的中途截断。"""
    limit = first_output_timeout or REQUEST_TIMEOUT
    if has_output:
        return {
            "code": "incomplete_stream",
            "message": "upstream stream timed out after output",
        }
    return {
        "code": "first_output_timeout",
        "message": "upstream produced no deliverable output within %ss" % limit,
    }



# apply_patch 独立工具规则已移除：Codex 新版不再声明 apply_patch 为独立工具，
# 改用 exec（JS 编排器）架构，apply_patch 降为 exec 的嵌套工具（tools.apply_patch()）。
# 教模型用法的规则在下方 _EXEC_FILE_EDIT_RULES（注入到 exec 描述）。

# exec 工具（Codex 新版 JS 编排器）的文件操作规则注入。
# glm-5.3 等模型常把 exec 的纯 V8 沙箱当 shell/Node 用（require('fs')、PowerShell
# $var/foreach），导致文件操作必然在沙箱里 THROW。这里强制教它用嵌套
# tools.apply_patch() 编辑文件、tools.shell_command() 执行命令。
_EXEC_FILE_EDIT_RULES = (
    "\n\nSTOP! READ THIS BEFORE WRITING JS — CRITICAL SANDBOX RULES:\n"
    "This `exec` tool runs PURE V8 JavaScript only. The following DO NOT EXIST here and will THROW:\n"
    "  - require() / import (NO Node.js). require('fs'), require('child_process') → THROW.\n"
    "  - Node globals: fs, process, Buffer, __dirname, path → DO NOT EXIST.\n"
    "  - Shell / PowerShell: `$var`, `foreach`, `cp`, `cat`, `>`, pipes → NOT JS syntax here.\n"
    "You CANNOT touch the filesystem directly from this sandbox. To do ANYTHING outside pure\n"
    "computation, call the nested tools on the global `tools` object:\n"
    "  • Edit / create / delete a file → await tools.apply_patch(\"*** Begin Patch\\n*** Update File: path\\n@@\\n context\\n-old\\n+new\\n*** End Patch\")\n"
    "    apply_patch is the ONLY allowed way to EDIT files — NO EXCEPTIONS. Every other edit path\n"
    "    is FORBIDDEN: sed -i / awk / tee / `>` / printf / cp, AND python (open().write / pathlib /\n"
    "    subprocess / `python -c`), heredocs (cat > f <<EOF), perl -i. This INCLUDES running them\n"
    "    via tools.shell_command — shell_command is READ-ONLY (ls / cat / head / grep / php -l)\n"
    "    ONLY, NEVER for writing. Writing a script to edit a file is EVASION. The ONLY edit call\n"
    "    is `await tools.apply_patch(...)`. NOTE: creating a script file (.py/.sh/.js/.pl/ANY) via\n"
    "    apply_patch then running it (tools.shell_command) to edit ANOTHER file is the SAME\n"
    "    evasion — a script must never write/edit files; only apply_patch edits.\n"
    "  • Run a shell command (non-editing) → await tools.shell_command({ command: \"ls -la\" })\n"
    "  • Read a file → await tools.shell_command({ command: \"cat -n path\" })\n"
    ">>> apply_patch 'no match' / 'line too long' / 'invalid hunk' = your patch is malformed or\n"
    "    the `-` line differs from the file. STAY in apply_patch (do NOT switch to python / Node /\n"
    "    sed — python & Node CANNOT edit here). FIX in order: ① EACH `*** Update File:` block MUST\n"
    "    start with its OWN `@@` line — TWO `*** Update File:` lines ⇒ TWO `@@` lines (the top\n"
    "    cause of 'invalid hunk: expected @@' is a missing @@ on the 2nd block); ② read\n"
    "    EXACT bytes with `await tools.shell_command({command:\"cat -n path\"})`, copy verbatim into\n"
    "    the `-` line. For YAML / Python (indent-sensitive files), the `+new` AND context lines\n"
    "    MUST copy the file's EXACT leading spaces — count them from `cat -n` output, never guess\n"
    "    (one wrong space breaks YAML). ③ for an unmatchable long line, rebuild via *** Delete + *** Add. Do NOT\n"
    "    switch to Node/fs/require/PowerShell (they DO NOT EXIST here, always THROW) and do NOT\n"
    "    edit via sed/awk. 'python / Node / 直接改 is more reliable' is WRONG (no Node; python\n"
    "    editing is FORBIDDEN — apply_patch is the only edit call).\n"
    "tools.apply_patch takes ONE argument: the RAW PATCH TEXT (a plain string), NOT JSON.\n"
    "PATCH FORMAT (file paths must be RELATIVE, never absolute):\n"
    "1. EACH `*** Update File:` block MUST start with a `@@` line right after it — 'invalid hunk:\n"
    "   expected @@' means you forgot it. CRITICAL: a 2nd `*** Update File:` needs its OWN new `@@`,\n"
    "   never share one `@@` across two blocks. `@@` may carry an anchor, e.g. `@@ def greet():`.\n"
    "   Lines with NO prefix are ONLY: `*** Begin/End Patch`, `*** Add/Update/Delete File: <path>`,\n"
    "   and `@@`. Every other line MUST start with `+`|`-`|` ` (space). NEVER bare `**`/`***` in hunk.\n"
    "2. Create: *** Begin Patch\\n*** Add File: hello.txt\\n+line one\\n+line two\\n*** End Patch\n"
    "3. Update: *** Begin Patch\\n*** Update File: src/app.py\\n@@ def main():\\n print('hi')\\n-old\\n+new\\n*** End Patch\n"
    "4. Update 2 files — EACH `*** Update File:` gets its OWN `@@`:\n"
    "   *** Begin Patch\\n*** Update File: a.py\\n@@ def a():\\n x\\n-y\\n+z\\n*** Update File: b.py\\n@@ def b():\\n m\\n-n\\n+o\\n*** End Patch\n"
    "5. Do NOT wrap the patch in markdown code blocks (```).\n"
)

def _extract_additional_tools(body):
    """Codex 新版把工具声明放在 input 数组的 additional_tools item
    （{type:"additional_tools", role:"developer", tools:[...]}），而非顶层 tools。
    本代理各路径（relay/messages）和 codex-relay 都只认顶层 tools，
    不处理 additional_tools → 工具定义全丢 → 模型看不到工具、无法 tool_call、
    输出一句意图就结束。

    这里在转发前把 additional_tools 的工具提取合并到顶层 body["tools"]（按 name 去重），
    并从 input 移除该 item（避免被 codex-relay 当 developer 消息污染上下文）。
    幂等：已提取后再次调用 input 里无 additional_tools，直接 no-op。"""
    if not isinstance(body, dict):
        return body
    inp = body.get("input")
    if not isinstance(inp, list):
        return body
    extra = []
    kept = []
    for item in inp:
        if isinstance(item, dict) and item.get("type") == "additional_tools":
            ts = item.get("tools")
            if isinstance(ts, list):
                extra.extend(t for t in ts if isinstance(t, dict))
        else:
            kept.append(item)
    if extra:
        body["input"] = kept
        existing = body.get("tools")
        if not isinstance(existing, list):
            existing = []
        seen = {t.get("name") for t in existing if isinstance(t, dict)}
        added = []
        for t in extra:
            nm = t.get("name")
            if nm not in seen:
                existing.append(t)
                seen.add(nm)
                added.append("%s:%s" % (t.get("type", "?"), nm))
        body["tools"] = existing
        if added:
            dbg("    [additional_tools] extracted %d → top-level tools: %s",
                     len(added), ", ".join(added))
            # 诊断：展开 namespace 子工具名（exec 是否藏在 namespace 内）
            for t in extra:
                if isinstance(t, dict) and t.get("type") == "namespace":
                    subs = [s.get("name", "?") for s in (t.get("tools") or []) if isinstance(s, dict)]
                    if subs:
                        dbg("    [additional_tools] namespace %s subs: %s", t.get("name"), ", ".join(subs))
    return body
def _inject_tool_rules(body):
    """注入工具描述规则：Codex 新版 exec（JS 编排器）→ V8 沙箱约束 + tools.apply_patch()/shell_command() 用法。
    apply_patch 在新版已降为 exec 的嵌套工具（非独立工具），靠 exec 描述教模型调 tools.apply_patch()。
    relay 路径覆盖。"""
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
        # Codex 新版 exec 工具（custom/function/嵌套均匹配）：注入 V8 沙箱约束 +
        # tools.apply_patch()/shell_command() 用法。glm-5.3 会把 exec 当 shell/Node 用 → 必 THROW。
        # 展平 namespace 后名为 functions-exec（endwith("-exec") 兜底匹配）。
        if (name == "exec" or name.endswith("-exec")) and "description" in tool:
            if _EXEC_FILE_EDIT_RULES.strip() not in (tool.get("description") or ""):
                tool["description"] = (tool.get("description") or "") + _EXEC_FILE_EDIT_RULES
                modified = True
        func = tool.get("function")
        if isinstance(func, dict) and (func.get("name") == "exec" or (func.get("name") or "").endswith("-exec")):
            if _EXEC_FILE_EDIT_RULES.strip() not in (func.get("description") or ""):
                func["description"] = (func.get("description") or "") + _EXEC_FILE_EDIT_RULES
                modified = True
    return body

# ── token 估算 ──────────────────────────────────────

def _est_tokens(body):
    """估算请求 token 数：剔除 base64 图片数据（图片按分辨率约 1-3K token，不按 base64 字节长度算）。
    避免图片请求的 base64 把字节估算撑到虚高（~840K/张）导致误判上下文超限、
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
    ② 上游响应 message_start.model 通常是上游模型名（glm-5.3 / grok-4.5-build-free 等），原样转发会污染
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
    客户端的字节都用标准格式。relay 路径读 codex-relay 重建输出，不受影响。"""
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
# 实测：部分 Messages 上游会返回 200 流式 + model_context_window_exceeded
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
    "prompt exceeds max length",        # OpenAI 兼容端点
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
