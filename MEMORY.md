# Project Memory

## Current state (v5.2.5, 2026-10-29)

- Architecture: single Python proxy, 5 files. common.py (config/logger/server), server.py (routing/streams), transforms.py (request transforms), main.py (startup), __init__.py. codex-relay and relay.py fully retired since v5.0.0.
- Upstream layout (config.json, not git-tracked): internal (venusgroup, enabled, only_models=luna/haiku/Flash) -> official (bigmodel, enabled; glm-native+official merged in v5.2.x) -> cmoyan/venus-deepseek/chatgpt (disabled, models renamed to gpt-6-sol).

## Zhipu endpoint capability matrix (probe-verified 2026-10-29)

| Endpoint | Responses | Completions | Billing |
|---|---|---|---|
| https://open.bigmodel.cn/api/v1/responses | YES (SSE + non-stream, current Codex path) | via /api/v1/chat/completions | pay-as-you-go |
| https://open.bigmodel.cn/api/coding/paas/v4 | NO (404 on /responses) | YES (current completions passthrough) | Coding Plan subscription quota |
| https://open.bigmodel.cn/api/anthropic/v1/messages | n/a | n/a (Messages protocol) | subscription |

official channel uses THREE URLs: responses_url=/api/v1/responses, completion_url=/api/coding/paas/v4 (subscription quota for completions clients), anthropic_url=/api/anthropic/v1/messages. Do NOT "simplify" by merging these - v5.2.5 incident: channel merge dropped completion_url handling and broke /v1/chat/completions passthrough.

## Toolchain transforms (probe-verified, keep them)

- _extract_additional_tools: REQUIRED. Zhipu ignores tools declared in input additional_tools item (A/B: 0 vs 1 function_calls). Moving to top-level tools is mandatory.
- _inject_tool_rules: REQUIRED for glm models. Without it glm-5.3 generates require('fs') inside Codex pure-V8 exec sandbox (always throws); with it generates await tools.shell_command(...).
- _fix_tool_result_roles: REMOVED in v5.2.3. Both zhipu endpoints now accept tool_result mixed in assistant messages (HTTP 200 both).

## Hard-won bugfixes (do not regress)

- SSE precommit guard (not route_sse_committed): fallback chain re-calling send_response mixed a 2nd HTTP header into the SSE stream -> Codex parse error -> client stuck until restart. v5.2.1 introduced, re-applied in v5.2.2.
- /v1/models empty list: _CHAIN_UPS filter excluded responses_url channels; after v5.0 all channels had responses_url -> zero models broadcast. Fixed v5.2.4 (filter by disabled only).
- v5.2.0 lesson: "probe says upstream native" validated protocol passthrough only, NOT tool-calling behavior. Any change touching transforms/server requires a real function_call smoke test (tests/test_transforms.py locks this).

## Verification

- Unit: .venv/bin/python -m unittest discover -s tests -q (8 tests)
- Import smoke: .venv/bin/python -c 'import glm_proxy_app.server'
- Real smoke on :9999 - responses w/ additional_tools (expect function_call>0), messages, chat/completions via official.

## Deployment

- Dev 201: run/restart freely. Production 100 (Windows, Desktop dir): deploy files via scp, USER restarts. Clean stray files after deploy (test files must not land in glm_proxy_app/; clear __pycache__).
- 100 backup of pre-v5.2 state: Desktop/backup_v5.1.1_20261029/
- internal channel reachable from 201 only via SOCKS 192.168.10.100:1080 (probe only; do not add proxy code - proxy itself runs on 100 where venus DNS resolves).
