# Project Memory

## Current state (v5.4.8, 2026-10-29)

- Architecture: single Python proxy, stdlib only. common.py (config/logger/server/rotating-log), server.py (routing/streams/fallback), transforms.py (request transforms), main.py (startup+probe), __init__.py. codex-relay fully retired since v5.0.0.
- Upstream layout (config.json, not git-tracked): internal (enabled, probe:true, only_models=flash/luna/haiku substring match) -> official (bigmodel, enabled) -> chatgpt/cmoyan/venus-deepseek (disabled, kept as cold spares per user decision).
- Three explicit endpoints per channel: responses_url / completion_url (full URL incl /chat/completions) / anthropic_url. Empty string = capability off. No runtime path concatenation.

## Zhipu endpoint capability matrix (probe-verified 2026-10-29)

| Endpoint | Responses | Completions | Billing |
|---|---|---|---|
| https://open.bigmodel.cn/api/v1/responses | YES | via /api/v1/chat/completions | pay-as-you-go |
| https://open.bigmodel.cn/api/coding/paas/v4 | NO (404 on /responses) | YES | Coding Plan subscription |
| https://open.bigmodel.cn/api/anthropic/v1/messages | n/a | Messages protocol | subscription |

Do NOT merge official's three URLs into one - v5.2.5 incident: merge dropped completion_url and broke /v1/chat/completions passthrough (Codex aux calls: ~100/day traffic on 100 host).

## Toolchain transforms (probe-verified, keep them)

- _extract_additional_tools: REQUIRED. Zhipu ignores tools inside input additional_tools item; must lift to top-level tools.
- _inject_tool_rules: REQUIRED for glm models. Without it glm-5.3 emits require('fs') in Codex V8 sandbox; with it emits await tools.shell_command(...).
- _fix_tool_result_roles: REMOVED v5.2.3 (both endpoints accept mixed tool_result).

## Hard-won bugfixes (do not regress)

- SSE precommit guard (route_sse_committed): fallback re-send_response mixes 2nd HTTP header into SSE -> Codex stuck until restart. v5.2.1+.
- First-output timeout is ACTIVITY window (v5.3.2): every upstream SSE block re-arms the 45s deadline. Fixed deadline killed GLM long-reasoning streams (thousands of reasoning_text.delta before first output_text; observed 45115ms/2116-events false timeout on 100). Reasoning still does NOT set has_output (fallback semantics preserved).
- Fallback last_err keeps real string codes (v5.4.3): empty_output/incomplete_stream must reach client, not the generic "upstream failed before output" (old int(fcode) ValueError trap).
- /v1/models empty list: filter by disabled only (v5.2.4).
- v5.2.0 lesson: native-passthrough probe validated protocol only, NOT tool-calling. Any transforms/server change needs real function_call smoke (tests/test_transforms.py, 8 tests, locks toolchain).
- Probe channel selection is config flag "probe": true (v5.4.2), NOT domain substring (venusgroup hardcode removed).

## Open items (parked by user decision)

- Empty-output fallback re-bills tokens (same request re-sent to next channel). Low frequency; revisit on real pain.
- disabled channels keep their config blocks (chatgpt tokens_file/store_responses etc.) as cold spares.

## create_thread 工具缺失事件 (2026-10-29, 已解决)

- 现象: 桌面端(26.1023) SSH 远程会话无 create_thread/thread 工具组。**自愈于桌面端完全重启**(客户端 apps MCP 协商状态损坏)。重启连接/重启 app-server 无效。
- 误判教训: session-flags ignored 警告(code_mode_tool_search 等)是干扰项,与本案无关; tools=0 旧日志口径误导排查(真实工具在 additional_tools)。
- v5.4.8 日志: >>> 行改 tools_raw/addl, 提取后打 [tools-final] 清单。排查口径: addl=0→客户端没发; addl>0 但 final 缺项→proxy 丢。
- 已部署 100 (2026-10-29 14:39 重启生效)。注意 addl=N 提取后可能少于 N(namespace 去重合并)。

## Verification

- Unit: .venv/bin/python -m unittest discover -s tests -q (8 tests)
- Import smoke: .venv/bin/python -c 'import glm_proxy_app.server'
- Real smoke on :9999 - responses (function_call>0), messages, chat/completions.

## Deployment

- Dev 201: run/restart freely. Production 100 (Windows, Desktop dir): scp files, USER restarts. Deploy package files INTO Desktop/glm_proxy_app/, entry glm_proxy.py at Desktop root (v5.4.0 incident: flat scp littered Desktop root; cleaned + bak folder since removed).
- 100 log: RotatingFileHandler 10MB x3 (v5.4.3). Old 41MB unbounded log deleted.
- internal reachable from 201 only via SOCKS 192.168.10.100:1080 (probe only; proxy runs on 100 where gateway DNS resolves - never add proxy client code).
- 100 restart verified 2026-10-29 01:46: v5.4.3, probe internal OK, official streaming traffic OK.

