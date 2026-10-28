# GLM Proxy

让 [Codex CLI](https://developers.openai.com/codex/cli/) / Claude Code 直接使用 GLM、DeepSeek 等模型的本地多上游代理。

Codex CLI 原生只支持 OpenAI Responses API。本代理按渠道能力把请求直通到三种端点（Responses / Chat Completions / Anthropic Messages），并处理 Codex 与各上游之间的工具格式差异（特别是 `apply_patch`）。

## 架构

```
客户端（Codex CLI / Claude Code / 其他 OpenAI 兼容工具）
   │
   ▼
GLM Proxy (:9999)
   │  按 URL 能力字段路由，多上游按配置顺序回退
   ├─ /v1/responses       ──► responses_url   （Responses API 直通）
   ├─ /v1/chat/completions ──► completion_url （Chat Completions 直通）
   └─ /v1/messages        ──► anthropic_url   （Anthropic Messages 直通）
```

### 代码结构

`glm_proxy.py` 是兼容启动入口，具体职责拆在 `glm_proxy_app/`：

| 模块 | 职责 |
|------|------|
| `common.py` | 配置加载、日志、共享状态、HTTP Server 基础设施 |
| `transforms.py` | 请求/工具调用/SSE 的无状态规范化与路由模式判定 |
| `server.py` | 客户端 HTTP Handler、上游路由、流式响应与回退 |
| `main.py` | 应用启动、内网网关探针线程、优雅退出 |

模块依赖单向：`common → transforms → server → main`。协议修复优先放 `transforms.py`。

## 路由规则

- **能力由 URL 字段决定**：有 `responses_url` 才承接 `/v1/responses`；有 `completion_url` 才承接 `/v1/chat/completions`；有 `anthropic_url` 才承接 `/v1/messages`。端点 URL 全部在配置里写完整，代码不拼接路径。
- **only_models**：可选子串白名单（大小写不敏感）。配置项是请求模型名的子串即命中（如 `flash` 命中 `GLM-5.3-Flash`、`glm-5.3-flash`）。未配置则该渠道对任意模型开放。
- **available_models**：静态模型清单。请求模型精确匹配（大小写不敏感）时透传该清单内的规范写法，否则使用渠道 `model`（Messages 路径用 `messages_model`）。运行时不访问上游 `/models`。
- **回退链**：按 `upstreams` 配置顺序尝试；单渠道失败/限流/无输出自动切下一个。`disabled: true` 的渠道完全跳过。
- **模型改写**：发往上游的模型名 = `available_models` 精确命中项或渠道默认 `model`。 `only_models` 只做渠道筛选，不参与改写。

## 功能

- **三端点直通**：Responses / Chat Completions / Anthropic Messages，按渠道配置各端独立可用
- **多上游回退**：DNS 失败、HTTP 4xx/5xx、流式无输出、限流 429（自动按 Retry-After 封锁渠道）逐级切换
- **SSE 活动窗口超时**：首输出 45s 判据按上游 SSE 块滚动续期——模型长时间只推 reasoning 不会被误杀；真正静默 45s 才回退
- **probe-before-commit**：非内容事件先握住，见首条可交付内容（正文/工具调用）才提交 200；上游超限改返干净 400 触发客户端自动压缩
- **GPT 无状态处理**：`store_responses: false` 渠道自动剥 `previous_response_id`、`rs_*`/reasoning 引用，回退 GLM 链时同样清理
- **工具链修复**：`additional_tools` 提取到顶层、exec 沙箱规则注入、tool_use input 规范化——删过一次导致无法执行命令，现有单测锁定
- **上下文超限**：识别上游超限信号（含 Venus/智谱各家文案），返回结构化 400 让客户端压缩后重试

## 配置

```bash
cp config.example.json config.json
# 编辑 config.json 填入真实密钥
```

`config.json` 已 gitignore。字段说明：

| 字段 | 适用端点 | 说明 |
|------|---------|------|
| `name` | - | 渠道标识，用于日志与限流封锁 |
| `responses_url` | Responses | 完整 URL。留空则该渠道不承接 `/v1/responses` |
| `completion_url` | Completions | 完整 URL（含 `/chat/completions`）。留空则不承接该路径 |
| `anthropic_url` | Messages | 完整 URL。留空则不承接 `/v1/messages` |
| `key` | 全部 | API 密钥（`tokens_file` 渠道可留空） |
| `anthropic_auth` | Messages | `x-api-key`（默认，智谱官方）或 `bearer`（中转站） |
| `model` | 全部 | Responses/Completions 路径默认模型名 |
| `messages_model` | Messages | Messages 路径模型名，缺省回退 `model` |
| `available_models` | 全部 | 精确匹配透传清单（规范写法），见路由规则 |
| `only_models` | 全部 | 子串白名单，见路由规则 |
| `max_context_tokens` | - | 上报 `/v1/models` 供客户端判断压缩时机 |
| `tokens_file` | Responses | ChatGPT 订阅 OAuth token 文件 |
| `store_responses` | Responses | `false` 时启用无状态剥离 |
| `strip_max_output` | Responses | 删除 max_output_tokens（上游不认该字段时） |
| `cf_gate` | 全部 | Cloudflare 百秒闸渠道标记，≥1MB 载荷跳过 |
| `probe` | Responses | `true` 启用周期探针（每 5 分钟向 responses_url 发 1-token 请求保活/记录可用性） |
| `disabled` | - | `true` 完全禁用该渠道 |

## 使用

```bash
python3 glm_proxy.py   # 默认 0.0.0.0:9999，无需额外依赖（纯标准库）
```

客户端（Codex CLI 等）API 端点指向 `http://<代理机器IP>:9999`。

验证：

```bash
curl http://127.0.0.1:9999/v1/models
curl http://127.0.0.1:9999/v1/chat/completions -H 'Content-Type: application/json' \
  -d '{"model":"glm-5.3","messages":[{"role":"user","content":"ping"}]}'
```

回归测试：

```bash
python3 -m unittest discover -v
```

## 日志

- 控制台实时输出；`nohup` 场景完整日志落 `logs/proxy.log`
- `logs/exchange_*.json`：请求+响应对（DEBUG 开启时）
- `logs/debug_req_*.json`：异常请求体存证

## 已知上游行为备忘

- 智谱官方：`/api/v1/responses` 支持 Responses；`/api/coding/paas/v4/chat/completions` 走订阅额度；`/api/coding/paas/v4/responses` 不存在
- GLM 长思考场景会先推数千条 `reasoning_text.delta` 再出正文——首输出超时必须按 SSE 活动滚动，固定 45s 会误杀（v5.3.2 修复）
- GLM 在 `max_output_tokens` 截断时发 `response.incomplete`（非 completed），按合法终态处理

## 许可

私有项目。
