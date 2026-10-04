# 环境与显式配置

## 默认行为

`Agent(model="deployment-name")` 默认使用 OpenAI provider 和 Responses API；`Agent()` 从 `AGENT_MODEL` 读取模型名。没有配置模型名时在启动前报错。

`RuntimeConfig.from_env()` 读取进程环境，`Models` 在构造时保存配置快照，不在各租户请求中修改 `os.environ`。`Models(env={})` 禁用环境继承；`Models(env=mapping)` 使用调用方给定的配置来源。

`.env.template` 列出全部默认读取的环境变量。SDK 不自动遍历目录寻找 `.env`，可以由 `uv run --env-file .env ...`、部署平台或调用方的配置加载器注入。

## 环境变量

| 变量 | 默认值 | 用途 |
| --- | --- | --- |
| `AGENT_PROVIDER` | openai | 默认 provider |
| `AGENT_MODEL` | 无 | 模型／部署名，调用时也可显式传入 |
| `AGENT_API` | provider 默认协议 | openai 默认 openai-responses；anthropic 默认 anthropic-messages |
| `OPENAI_API_KEY` | 无 | OpenAI / openai-chat 凭据 |
| `OPENAI_BASE_URL` | https://api.openai.com/v1 | 包含 /v1 的 API 根路径 |
| `ANTHROPIC_API_KEY` | 无 | Anthropic 凭据 |
| `ANTHROPIC_BASE_URL` | https://api.anthropic.com/v1 | Anthropic API 根路径 |
| `AGENT_TIMEOUT_SECONDS` | 120 | HTTP 每项网络操作的超时，不是整轮运行总时限 |
| `AGENT_MAX_TOKENS` | 已知模型上限；未知模型 4096 | 请求输出预算，仍会受模型元数据约束 |
| `AGENT_TEMPERATURE` | 不发送 | 温度参数，仍经过兼容过滤 |
| `AGENT_REASONING` | provider 默认值 | 统一 reasoning 等级 |

未知模型的 4096 是 SDK 的初始输出预算，不是推测的模型能力；需要更大预算时注册准确的 `Model.max_tokens`。空字符串按未设置处理。配置数值或 URL 格式错误时直接报错，不静默使用意外地址。

模板显式设置了 `AGENT_API=openai-responses`。切换到 Anthropic 时应同时修改或清空该项；显式 API 配置不会根据 provider 自动改写。

## 优先级

- API Key：每次请求／Agent 显式 key（或动态 `get_api_key`）> `Models(api_keys=...)` > provider 配置／环境。
- 模型名、provider：构造参数 > 环境默认值。
- API、Base URL：显式调用参数 > 显式注册模型 > 显式注册 provider > 环境／内置默认值。
- 生成参数：显式请求参数 > 环境默认参数；合并后统一过滤和重写。

直接传入 `Model` 对象视为显式模型配置，它的 Base URL 不再被环境变量覆盖。模型名字从注册表解析时，内置模型的官方地址可以被环境中的 Base URL 覆盖。

## SaaS 调用示例

```python
from agent_runtime import Agent, Models, ProviderConfig

models = Models(env={}, providers=[ProviderConfig(
    id="tenant-a", base_url="https://gateway.example.com/v1", api_key="runtime-injected-key",
)])
agent = Agent(models=models, provider="tenant-a", model="deployment-name")
```

自定义 provider 不会继承 `OPENAI_API_KEY`。通过 `get_api_key(provider)` 可在每次模型请求前刷新短期凭据。共享连接池可以显式传入 `httpx.AsyncClient`；该 client 由调用方关闭。未传入时，SDK 每次请求创建并关闭自己的 client。

不自动加载 ChatGPT OAuth、系统凭据仓库或其他项目的 `.env`。API Key 字段不会出现在配置对象的默认 repr 中。

## Session 存储位置

`Agent()` 默认使用文件支持的 `LocalSession`，位于当前工作目录的 `.agent-runtime/sessions`。
通过 `Agent(session=LocalSession(session_id="...", directory="/data/sessions"), ...)` 指定目录和身份。
Session 不读取新的环境变量，存储路径由宿主显式配置；`directory=None` 为该组件内的纯内存模式。
已存在的 Session 优先恢复其模型配置，API Key 和 transport headers 仍由当前进程重新注入。
详见 [Session 生命周期与恢复](sessions.md)。
