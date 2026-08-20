# GLM Proxy

让 [Codex CLI](https://developers.openai.com/codex/cli/) 直接使用智谱 GLM 系列模型（glm-5 / glm-5.1 / glm-5.3）的本地代理。

Codex CLI 原生只支持 OpenAI Responses API，而智谱 GLM 提供 Chat Completions 和 Anthropic Messages 两种端点。本代理通过 codex-relay 做 Responses ↔ Chat Completions 协议翻译，并处理 Codex 与 GLM 之间的工具格式差异（特别是 `apply_patch`）。

## 架构

```
Codex CLI ──► GLM Proxy (:9999)
                │
                 ├─► codex-relay ──► GLM Chat Completions 端点
                              (Responses→Chat Completions)
                 │
                 └─► key=0 ──► GPT 原生 Responses 直通（支持图片）
```

### 核心组件

| 组件 | 作用 |
|------|------|
| **GLM Proxy**（本仓库） | 主入口 `:9999`。多上游回退、密钥注入、限速、`apply_patch` 工具格式修复、流式错误拦截 |
| **codex-relay** | Responses API ↔ Chat Completions 翻译（Responses 主路径）。启动时自动 pip 安装 |

### 代码结构

`glm_proxy.py` 只保留兼容启动入口，具体职责拆在 `glm_proxy_app/`：

| 模块 | 职责 |
|------|------|
| `common.py` | 配置、日志、共享状态和 HTTP Server 基础设施 |
| `relay.py` | 上游拦截器及 codex-relay 子进程生命周期 |
| `transforms.py` | 请求、工具调用和 SSE 的无状态规范化 |
| `server.py` | 客户端 HTTP Handler、上游路由和流式响应处理 |
| `main.py` | 应用装配、启动和优雅退出 |

模块依赖保持单向：`common → transforms/relay → server → main`。新增协议修复优先放在
`transforms.py`，不要继续塞进 HTTP Handler。

## 功能

- **协议翻译**：codex-relay 把 Codex 的 Responses API 请求转成 GLM 能理解的 Chat Completions 格式；`/v1/messages`（Claude Code 等）直连 Anthropic Messages 端点
- **多上游回退**：配置多个上游（内网网关 / 中转站 / 智谱官方），按优先级和健康状态自动切换
- **`apply_patch` 支持**：GLM 不原生支持 Codex 的 FREEFORM 工具，代理做双向转换
  - 请求侧：`custom`/`grammar` 工具定义 → GLM 可调用的 function 格式
  - 响应侧：GLM 的 `function_call` → Codex 的 `custom_tool_call`（含完整流式 delta 事件）
  - 请求侧：Codex 的 `custom_tool_call_output` → GLM 的 `function_call_output`
- **流式错误拦截**：GLM 流式返回 1234（上下文超限）/1305（限速）错误时，合成正常结束保留已生成内容
- **官方限速**：令牌桶控制官方 API 请求频率，防止 1305
- **`max_tokens` 兜底**：自动补全默认值，避免智谱 `count_tokens` 在 `None` 时崩溃
- **健康检查**：定时验证内网网关模型身份，异常时飞书告警
- **namespace 工具名修正**：处理 codex-relay 对 MCP namespace 工具的命名拼接

## 配置

密钥等敏感信息存在 `config.json`（已 gitignore，不会提交）。首次使用：

```bash
cp config.example.json config.json
# 编辑 config.json 填入真实密钥
```

配置结构见 [config.example.json](config.example.json)：

```json
{
  "feishu_webhook": "https://...",            // 健康检查告警 webhook（可选）
  "upstreams": [
    {
      "name": "official",                      // 上游名称
      "openai_url": "...",                     // Chat Completions 端点（Responses 走 codex-relay 到这里）
      "anthropic_url": "...",                  // Anthropic Messages 端点（/v1/messages 直连）
      "anthropic_auth": "x-api-key",           // 认证方式：x-api-key / bearer
      "key": "...",                            // API 密钥
      "relay_port": 4446,                      // codex-relay 端口
      "interceptor_port": 14446,               // 拦截器端口
      "model": "glm-5.3",                      // Chat Completions 模型名
      "messages_model": "glm-5.3",             // Messages 端点模型名（不填则回退到 model）
      "max_context_tokens": 202745             // 上下文窗口
    }
  ]
}
```

**上游字段说明：**

| 字段 | 必填 | 说明 |
|------|------|------|
| `name` | 是 | 上游标识，用于路由和日志 |
| `key` | 是 | API 密钥 |
| `model` | 是 | Chat Completions 路径用的模型名 |
| `relay_port` / `interceptor_port` | 是 | codex-relay 子进程端口，各上游不能重复 |
| `anthropic_url` | 否 | 配置则可服务 `/v1/messages` 直连请求（Claude Code 等） |
| `anthropic_auth` | 否 | `x-api-key`（智谱官方）或 `bearer`（中转站） |
| `messages_model` | 否 | Messages 端点模型名，不填回退到 `model` |
| `max_context_tokens` | GLM 渠道必填 | 上下文窗口，上报 `/v1/models` 供 Codex 判断压缩时机；`responses_direct`（GPT 直通）渠道无需配置（Codex 原生认识 GPT 模型窗口） |

### GPT 原生 Responses 直通

GPT 渠道放在 `upstreams` 最前面，配置 `responses_direct: true` 和 `chain_exclude: true`。这些渠道不参与默认回退链，也不接受 `/v1/messages` 或 Chat Completions；只能用数字 key 手动直达。

Responses 服务端 item ID 只存在于生成它的上游。当前 GPT 渠道使用 `store_responses: false`：代理会在请求 GPT 前删除所有纯 `item_reference`、`rs_*`/`reasoning` 状态和 `previous_response_id`；当请求进入 GLM 回退链时执行相同清理，同时保留带完整内容的普通消息、函数调用和工具输出。

多个 GPT 渠道中应只启用一个：将要用的渠道设为 `"disabled": false`，其余设为 `true`，然后重启代理。启用的链外直通渠道使用 key 0；如果全部 disabled，key 0 保持空缺。普通链内渠道始终从 key 1 开始编号，不受 GPT 启停影响。

GPT 模型不出现在 `/v1/models` 列表中（Codex 对 GPT 模型名使用原生窗口知识），也不参与本地上下文超限启发式——GPT 超限会返回结构化错误，由错误处理层直接转发。

Responses 不按是否带 `input_image` 分叉；图片和文本统一走 codex-relay 路径处理。如果上游明确返回图片不兼容错误，再由错误处理层执行后续降级。

## 使用

### 1. 安装依赖

```bash
pip install codex-relay
```

（代理启动时会自动检查并安装）

### 2. 启动代理

```bash
python3 glm_proxy.py
```

默认监听 `0.0.0.0:9999`。

### 3. 配置 Codex CLI

将 Codex 的 API 端点指向本代理：

```
http://<代理机器IP>:9999
```

### 4. 验证

```bash
curl http://127.0.0.1:9999/v1/models
```

运行本地回归测试：

```bash
python3 -m unittest discover -v
```

## 上游路由规则

未指定上游时，按以下规则自动选择：

自动链不区分工作时间，始终按配置顺序回退：

1. **官方（official）**
2. **Venus DeepSeek（venus-deepseek）**
3. **内网千问（internal）**

## 日志

- 控制台实时输出
- `logs/` 目录保存调试数据：
  - `exchange_*.json`：请求+响应对（排查工具调用问题）
  - `debug_req_*.json`：大请求体（排查上下文问题）
  - `proxy.log`：nohup 后台运行时的完整日志

## apply_patch 工作原理

Codex 的 `apply_patch` 是 FREEFORM 工具（grammar 格式），但：

1. **GLM 不支持 FREEFORM 工具** → 代理把 grammar 工具定义转成普通 function，并在描述里教 GLM 生成 V4A patch 格式
2. **GLM 返回的是 `function_call`** → 代理转成 Codex 期望的 `custom_tool_call`，并补全流式 `custom_tool_call_input.delta` + `.done` 事件（否则长 patch 会被 Codex 丢弃）
3. **Codex 返回 `custom_tool_call_output`** → 代理转回 GLM 的 `function_call_output`

代理还会自动修复 GLM 常见的 patch 格式错误：补全 `*** Begin/End Patch`、清理带前缀的命令行。

## 限速与错误处理

- **1234（上下文超限）**：流式拦截，保留已生成内容并合成正常结束
- **1305（限速）**：令牌桶控制官方 API 频率（默认 1.5 req/s，突发 2）
- **上游失败**：自动回退到下一个可用上游

## 许可

私有项目。
