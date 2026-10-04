# agent-runtime

以 [pi Agent](https://github.com/earendil-works/pi) 为主参考的异步 Python SDK。
当前为初始核心移植，面向 Python 3.11+，不是完整 pi-ai、coding-agent 或 SaaS 平台的替代实现。

## 安装与运行

```bash
uv sync
uv run python examples/offline.py
uv run python -m unittest discover -s tests -v
```

也可以使用 `python -m pip install -e .`。凭据由参数、实例级配置或对应环境变量注入，仓库中不保存真实凭据。

使用 Responses API：复制 [.env.template](.env.template) 为 `.env`，填写 `AGENT_MODEL`、`OPENAI_API_KEY`，按需修改 `OPENAI_BASE_URL`，然后执行：

```bash
uv run --env-file .env python examples/openai_responses.py
```

SDK 默认读取进程环境变量，不自动搜索 `.env`。`uv --env-file` 负责将文件加载到进程环境；生产环境直接注入环境变量即可。`Agent()` 默认 provider 为 `openai`、协议为 `openai-responses`，模型名来自 `AGENT_MODEL`；未配置模型名会提示错误，不替用户选择模型。

```python
import asyncio
from agent_runtime import Agent

async def main():
    agent = Agent(
        system_prompt="你是一个助手。",
        parameters={"max_tokens": 2048},
    )  # 从环境读取模型、Base URL 和 API Key；不自动使用 ChatGPT 登录态

    def on_event(event, signal):
        if event["type"] == "message_update":
            delta = event["assistantMessageEvent"]
            if delta["type"] == "text_delta":
                print(delta["delta"], end="", flush=True)

    agent.subscribe(on_event)
    await agent.prompt("解释 Agent loop 的工作方式。")
    if agent.state.error_message:
        raise RuntimeError(agent.state.error_message)

asyncio.run(main())
```

## 当前范围

- `Agent`：状态、订阅、取消、继续执行、steering/follow-up 队列。
- 内置 `LocalSession`：瞬时内存状态、按会话保存的 JSONL 文件、可靠边界恢复。
- `agent_loop` / `run_agent_loop`：流式或可等待事件 sink 的低层入口。
- 工具 JSON Schema 校验、参数准备、执行前后钩子、串行／并行执行。
- `finish_turn`、`prepare_request`、`prepare_next_turn`、上下文转换与动态 API key。
- 三种 HTTP SSE 协议：OpenAI Responses、OpenAI Chat Completions、Anthropic Messages。
- provider/model 注册、API 实现注入、参数过滤与模型限制处理。
- pi `/api/stream` 代理协议客户端；没有内置代理服务器。

Python 方法使用 snake_case；消息和事件字典保留 pi 的 camelCase 字段，例如 `toolCallId`、`stopReason` 和 `toolResults`。`continue()` 在 Python 中叫 `continue_()`。

## 模型与参数

```python
from agent_runtime import Models, Model, ParameterPolicy, normalize_parameters

models = Models()
# 示例元数据，不代表任何真实模型的限制。
model = Model(
    id="internal-model", provider="internal", api="openai-responses",
    base_url="https://gateway.example.com/v1", max_tokens=4096,
    parameter_policy=ParameterPolicy(
        unsupported=frozenset({"top_p"}),
        fixed_values={"temperature": 1},
    ),
)
report = normalize_parameters(model, {
    "temperature": 0.2, "top_p": 0.8, "max_tokens": 2048, "unknown": True,
})
print(report.parameters)  # 转换后的 provider 参数
print(report.dropped)     # top_p、unknown 及原因
print(report.adjusted)    # temperature: 0.2 → 1
models.register_model(model)
```

参数先经过 API 允许列表，再应用 provider/model 元数据，最后转换成协议字段。未知字段丢弃；不会将用户提供的 `model`、`input`、`tools` 等任意字段透传覆盖 Runtime 构造的请求。

provider 已知时可以直接传任意模型／部署名；没有元数据的模型仅应用协议级规则，不能自动发现服务端所有限制。需要准确的模型级行为时，注册 `Model` 和 `ParameterPolicy`。限制规则不依赖硬编码的模型名称判断，初版不维护完整的模型能力目录。

显式请求配置优先于注册的模型／provider 配置，再优先于环境默认值。`Models(env={})` 可禁用环境继承，适合多租户调用方明确注入配置；实例间不共享凭据和注册项。详见 [环境配置](docs/configuration.md)。

完整规则与限制见 [参数兼容](docs/parameters.md)。在 `parameters` 中设置 `on_parameters(report, model)` 可以观察每次请求的过滤结果；`on_payload(payload, model)` 仅观察请求副本，不能绕过过滤。

## 工具

```python
from agent_runtime import AgentTool, AgentToolResult

async def add(call_id, arguments, signal, on_update):
    signal.throw_if_aborted()
    value = arguments["a"] + arguments["b"]
    return AgentToolResult(content=[{"type": "text", "text": str(value)}], details={"value": value})

tool = AgentTool(
    name="add", description="Add two numbers",
    parameters={"type": "object", "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                "required": ["a", "b"], "additionalProperties": False},
    execute=add,
)
# Agent(..., tools=[tool])
```

`execute` 建议使用异步函数；同步函数不能执行阻塞 I/O，否则会阻塞事件循环。
`on_update(partial_result)` 是同步调度回调，结算时会等待已发送更新；工具返回后再调用它不会继续发事件。
工具错误成为 `isError` 结果传给模型；被 token 上限截断的工具调用不会执行。

## Session 与恢复

`Agent()` 默认使用 `LocalSession`，自动分配 session ID，记录写入 `.agent-runtime/sessions/<id>/events.jsonl`。
内存能力合并在同一个组件中；需要纯内存运行时使用 `LocalSession(directory=None)`。
DB Session 仅预留文件，尚无数据库实现。

```python
from agent_runtime import Agent, LocalSession

session = LocalSession("conversation-123", directory="./sessions")
agent = Agent(session=session, model="your-model", tools=tools)
await agent.prompt("处理这个任务")

# 新进程中，使用原目录和 ID；重新注入工具、凭据及原来的自定义钩子。
session = LocalSession("conversation-123", directory="./sessions")
agent = Agent(session=session, tools=tools)
if session.resumable:
    await agent.resume()
```

模型完整响应、工具执行意图、原始结果和最终结果由 Runtime 直接交给 Session 保存，可靠提交后再推进。
高频 token 和工具进度仅在内存中展示。已完成工作复用；未完成模型请求可重做。
已开始但结果未知的工具默认暂停核实，只有 `replay="safe"` 才自动重试。

不需要网络和 API Key 的两进程示例：

```bash
uv run python examples/local_session.py start --session-id demo
uv run python examples/local_session.py resume --session-id demo
```

第一条保存工具计划并停止，第二条从文件恢复并完成工具与模型循环。
使用方式、文件格式、工具恢复决策和限制详见 [Session 生命周期](docs/sessions.md)。

## 外层集成边界

`subscribe` 回调按注册顺序等待，`agent_end` 的回调也完成后才进入 idle。
Session 保存和恢复由 Runtime 直接调用，不依赖事件订阅。`message_update` 可用于向独立 UI 缓冲区投递增量，完整事件可用于额外审计。UI 投递失败应由调用方隔离处理；Session 保存失败会抛出 `SessionError` 并停止推进。

低层 `run_agent_loop(..., emit=...)` 直接等待 sink，并向调用方传播 sink 失败；流式 `agent_loop` 使用内存队列，不能视为持久化队列，也不适合永远不消费的订阅。

前端断开不应取消后台 `agent.prompt()` 所属任务。用户停止时调用 `agent.abort()`；模型网络请求会被中断，工具通过 signal 协作退出。Python 任务本身被取消时，取消会继续向上传播。

`continue_()` 保留 pi 的限制：用于最后一条是 user/toolResult 的历史，不能直接恢复尚未执行完的 assistant 工具计划。`resume()` 可恢复 LocalSession 保存的工具计划和部分完成批次。工具外部幂等、DB Session、Redis 接管、SSE 服务和多租户调度仍由后续实现或宿主负责。

## 来源与差异

上游固定提交：`83692682f095528f8b71652ddacff7075e36e893`。
对应文件、保留语义和未移植范围见 [移植映射](docs/porting.md)。保留上游 MIT 许可与版权声明，见 [LICENSE](LICENSE) 和 [NOTICE](NOTICE)。
