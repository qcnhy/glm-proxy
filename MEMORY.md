# Project Memory

## Current architecture

- Version: 4.4.5.
- `glm_proxy.py` is a compatibility entry point only.
- `glm_proxy_app/common.py` owns configuration, logging, shared state, and the threaded server type.
- `glm_proxy_app/relay.py` owns interceptor servers and codex-relay child processes.
- `glm_proxy_app/transforms.py` owns stateless request/tool/SSE normalization.
- `glm_proxy_app/server.py` owns client HTTP routing and stream handling.
- `glm_proxy_app/main.py` assembles and starts the application.
- Keep dependencies one-way: common -> transforms/relay -> server -> main.
- GPT direct channels use `store_responses: false`. Before a stateless GPT request or GLM fallback, strip every pure `item_reference` regardless of ID prefix, plus `rs_*`/reasoning items and `previous_response_id`; preserve full messages and tool traffic.
- External OpenAI/Claude channels and all worktime routing were removed. The fixed automatic chain is official -> venus-deepseek -> internal for both Responses and Messages.
- Responses never convert to Anthropic Messages: GLM Responses always use codex-relay -> Chat Completions, while `anthropic_url` only serves direct `/v1/messages` clients. Image stripping is intentionally deferred until a real upstream incompatibility is observed.
- GPT direct Responses use a 45-second first-deliverable-output timeout. Before text or a valid tool call, timeout enters the normal GLM fallback chain; after deliverable output begins, the read timeout returns to 300 seconds and any later timeout becomes `response.failed` without cross-model continuation.
- `stream disconnected before completion` (mid-stream) = GLM official upstream itself drops the SSE connection after partial output; codex-relay translates it to `response.failed(stream_incomplete)` and the proxy forwards it. By design there is no cross-model retry after output. Rare (once per evening as of 2026-08-20); if frequency rises, plan is same-channel silent retry when almost no deltas were emitted, plus per-channel drop-rate demotion similar to the 429 block.

## Verification

- Compile: `python3 -m compileall -q glm_proxy.py glm_proxy_app`
- Tests: `python3 -m unittest discover -v`
- Import smoke test should verify config and logs still resolve at the repository root.
