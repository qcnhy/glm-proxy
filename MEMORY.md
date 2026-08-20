# Project Memory

## Current architecture

- Version: 4.4.2.
- `glm_proxy.py` is a compatibility entry point only.
- `glm_proxy_app/common.py` owns configuration, logging, shared state, and the threaded server type.
- `glm_proxy_app/relay.py` owns interceptor servers and codex-relay child processes.
- `glm_proxy_app/transforms.py` owns stateless request/tool/SSE normalization.
- `glm_proxy_app/server.py` owns client HTTP routing and stream handling.
- `glm_proxy_app/main.py` assembles and starts the application.
- Keep dependencies one-way: common -> transforms/relay -> server -> main.
- GPT direct channels use `store_responses: false`. Before a stateless GPT request or GLM fallback, strip every pure `item_reference` regardless of ID prefix, plus `rs_*`/reasoning items and `previous_response_id`; preserve full messages and tool traffic.
- External OpenAI/Claude channels and all worktime routing were removed. The fixed automatic chain is official -> venus-deepseek -> internal for both Responses and Messages.

## Verification

- Compile: `python3 -m compileall -q glm_proxy.py glm_proxy_app`
- Tests: `python3 -m unittest discover -v`
- Import smoke test should verify config and logs still resolve at the repository root.
