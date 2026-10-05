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

使用 Responses API：复制 [.env.template](.env.template) 为 `.env`，填写 `OPENAI_API_KEY`，默认模型为 `gpt-6-luna`，按需修改 `OPENAI_BASE_URL`，然后执行：

```bash
uv run --env-file .env python examples/openai_responses.py
```

SDK 默认读取进程环境变量，不自动搜索 `.env`。`uv --env-file` 负责将文件加载到进程环境；生产环境直接注入环境变量即可。`Agent()` 默认 provider 为 `openai`、协议为 `openai-responses`、模型为 `gpt-6-luna`。显式模型参数或非空 `AGENT_MODEL` 可以覆盖默认值；调用失败不会自动切换模型。

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

真实 OpenAI 集成测试单独运行，不包含在普通离线测试中，会产生 API 用量。该入口只允许官方 Responses API 和 `gpt-6-luna`：

```bash
uv run --env-file .env python tests/live_openai.py --live
```

测试覆盖文本增量、工具循环、保存工具计划后的恢复、文本流中断后的重新请求；恢复阶段使用独立进程读取同一 Session 文件。详见 [在线测试说明](docs/live-testing.md)。

## 当前范围

- `Agent`：状态、订阅、取消、继续执行、steering/follow-up 队列。
- 内置 `LocalSession`：瞬时内存状态、按会话保存的 JSONL 文件、可靠边界恢复。
- 单份 Session 日志推导上下文；可选摘要压缩、事件节点查询及分叉到独立新会话。
- `agent_loop` / `run_agent_loop`：流式或可等待事件 sink 的低层入口。
- 工具 JSON Schema 校验、参数准备、执行前后钩子、串行／并行执行。
- 显式装配 MCP 工具：HTTP/stdio 调用、动态请求头、已有工具审批和恢复流程。
- 专用 `load_skill` / `load_skill_reference` 工具、可替换 SkillStore 和本地目录发现。
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

SDK 随包提供 pi 的四个核心工具，放在独立的 `agent_runtime.tools` 模块中。`Agent()` 默认不启用任何工具，需要在组装时显式声明：

```python
from agent_runtime import Agent
from agent_runtime.tools import create_coding_tools

agent = Agent(
    tools=create_coding_tools(cwd="/path/to/workspace"),
    system_prompt="先读取文件，再执行修改。",
)
await agent.prompt("检查并修改这个工作目录中的代码。")
```

`create_coding_tools` 返回 `read`、`bash`、`edit`、`write`。也可以分别使用 `create_read_tool` 等工厂，只启用需要的工具。工具参数、替换执行后端、审批与恢复说明见 [内置工具](docs/tools.md)，完整示例见 [coding_tools.py](examples/coding_tools.py)。

MCP 工具由独立的 `agent_runtime.mcp` 模块提供。平台传入已发现、已选择的 schema，`assemble_tools` 根据 `type: builtin | mcp` 组装；MCP 业务名称为 `server.tool`。支持 Streamable HTTP、stdio、静态和动态请求头，沿用现有审批及 Session 恢复。完整用法及边界见 [MCP 调用](docs/mcp.md)，无需 API Key 的真实 stdio 示例：

```bash
uv run python examples/mcp_tools.py
```

自定义工具仍沿用原有接口：

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
普通工具错误成为 `isError` 结果传给模型；MCP 调用结果未知时暂停核实，不自动重试。被 token 上限截断的工具调用不会执行。

## Skill

`agent_runtime.skills` 提供专用 Skill 加载工具，默认通过 `AGENT_SKILLS_DIR` 指定的本地目录读取。目录发现返回名称和描述，正文及参考内容在调用工具时加载；后续可继承 `SkillStore` 替换为 DB/S3 存储。

```python
from agent_runtime.skills import LocalSkillStore, create_skill_tools

store = LocalSkillStore()  # .env 中配置 AGENT_SKILLS_DIR，由宿主加载到进程环境
catalog = await store.discover()  # 外层筛选候选目录并交给模型
prompt = "可用 Skill，执行任务前按需加载：\n" + "\n".join(
    f"- {item.name}: {item.description}" for item in catalog
)
agent = Agent(tools=create_skill_tools(store=store), system_prompt=prompt)
```

完整装配、两个工具的调用参数和恢复行为见 [Skill 加载](docs/skills.md)。离线示例：

```bash
AGENT_SKILLS_DIR=examples/skill_catalog uv run python examples/skills.py
```

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

## 上下文压缩与历史节点分叉

每个会话仍只有一份 `events.jsonl`。摘要和保留边界追加到同一日志，`Agent.state.messages` 与
`session.build_context()` 是从日志推导的内存视图，压缩前的消息继续保留在原记录中。

```python
from agent_runtime import Agent, CompactionSettings, LocalSession

session = LocalSession("conversation-123", directory="./sessions")
agent = Agent(
    session=session,
    tools=tools,
    compaction=CompactionSettings(reserve_tokens=16384, keep_recent_tokens=20000),
)
await agent.prompt("处理任务")

# 可选：空闲时主动生成摘要，沿用 Agent 当前的模型和 provider。
# await agent.compact("保留用户约束、已完成的修改和下一步")

# 浏览完整历史，每个持久化事件有稳定 id。
records = session.read_records()
event_id = records[-1]["id"]
context_at_node = session.build_context(event_id)

# 复制该节点及之前的全部记录，创建新的 Session ID 和独立日志。
child = await session.fork(event_id, session_id="conversation-branch")
child_agent = Agent(session=child, tools=tools)
if child.resumable:
    await child_agent.resume()
else:
    await child_agent.prompt("从这里尝试另一种做法")
```

自动压缩需要显式传入 `compaction`；手动压缩也支持宿主提供摘要，不必调用模型。
从未完成步骤分叉会保留恢复状态，结果未知的副作用工具仍需核实。
完整语义见 [压缩、投影与分叉](docs/context.md)。离线示例：

```bash
uv run python examples/session_projection.py
```

## 外层集成边界

同一 Session 同时只由一个 Agent 执行，实例限于同一事件循环使用；平台负责执行者调度、接管和失效控制。主执行协程统一修改状态并提交 Session，工具任务只交回进度和结果。切换 Worker 时重新打开 Session。

`steer()`、`follow_up()` 和 `await enqueue()` 只进入内存队列，主流程在安全边界保存；入队返回不是持久化确认。需要可靠接收时由平台先保存输入。直接使用模型层时，先 `stream = await models.stream_simple(model, context, options)`，再消费事件。

`subscribe` 回调按注册顺序等待，`agent_end` 的回调也完成后才进入 idle。
Session 保存和恢复由 Runtime 直接调用，不依赖事件订阅。`message_update` 可用于向独立 UI 缓冲区投递增量，完整事件可用于额外审计。UI 投递失败应由调用方隔离处理；Session 保存失败会抛出 `SessionError` 并停止推进。

低层 `run_agent_loop(..., emit=...)` 直接等待 sink，并向调用方传播 sink 失败。流式 `agent_loop`、内置模型 adapter 和 proxy 使用有容量限制的内存队列，消费落后时暂停生产，保留事件顺序。它不是持久化队列。

需要事件和最终结果时，先 `async for event in stream`，再 `await stream.result()`；只需要结果时直接 `await stream.result()`，会丢弃尚未消费的中间事件。不要一边迭代一边提前调用 `result()`。持续生产事件的自定义 adapter 使用 `await stream.send(event)`；同步 `push()` 只适用于已经限制大小的批次。

队列默认阈值为 64 个事件；模型／proxy 在读取每个协议事件后、解析前等待容量，单次解析及终态事件可以短暂超过阈值。前端 SSE 应由外层独立任务消费自己的缓冲区，`subscribe` 只做快速投递并隔离连接错误。缓冲区满时可按此前约定放弃打字机增量、等待完整结果；不要让 SDK 回调等待断开的前端。

前端断开不应取消后台 `agent.prompt()` 所属任务。用户停止时调用 `agent.abort()`；模型网络请求会被中断，工具通过 signal 协作退出。Python 任务本身被取消时，取消会继续向上传播。

`continue_()` 保留 pi 的限制：用于最后一条是 user/toolResult 的历史，不能直接恢复尚未执行完的 assistant 工具计划。`resume()` 可恢复 LocalSession 保存的工具计划和部分完成批次。工具外部幂等、DB Session、Redis 接管、SSE 服务和多租户调度仍由后续实现或宿主负责。

## 来源与差异

上游固定提交：`83692682f095528f8b71652ddacff7075e36e893`。
对应文件、保留语义和未移植范围见 [移植映射](docs/porting.md)。保留上游 MIT 许可与版权声明，见 [LICENSE](LICENSE) 和 [NOTICE](NOTICE)。
