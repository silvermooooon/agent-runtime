# LocalSession：瞬时内存状态与文件恢复记录

本版把内存能力合并进 `LocalSession`，不提供独立的 `MemorySession`。
`Session` 基类定义状态转换和恢复语义；`LocalSession` 通过继承实现文件读写。
可选 `DatabaseSession` 提供 PostgreSQL 存储、审查与 S3 归档，见 [数据库组件](database.md)。

## 创建和使用

```python
from agent_runtime import Agent, LocalSession

session = LocalSession(session_id="conversation-123", directory="./sessions")
agent = Agent(session=session, model="your-model", tools=tools)
await agent.prompt("处理这个任务")
```

`Agent()` 默认创建一个自动生成 ID 的 `LocalSession`，文件目录为 `.agent-runtime/sessions`。
通过 `agent.session.session_id` 获取并保存这个 ID。目录和文件在首次需要写入时创建，不在 import 时创建。
调用方对一次会话复用同一个 ID，恢复时使用同一目录；不同 ID 的记录互相独立。

如果只需要内存运行，可以使用同一个组件的 `LocalSession(directory=None)`。
此模式没有文件，`session.durable` 为 False，进程退出后不能恢复。

## 保存内容

内存中的 `session.live` 只保留最新的模型输出和各个运行中工具的进度，不累积 token 事件列表。
运行结束时清空。历史投影也缓存在内存中供 Runtime 使用，其持久依据是文件记录。

每个文件 Session 对应：

```text
sessions/
└── conversation-123/
    └── events.jsonl
```

`events.jsonl` 为追加写入的结构化记录。每条有格式版本、session 身份、连续序号和 SHA-256 校验；
逻辑记录有稳定 `id`、`type`、`seq`、`time`、`run_id`、`data`。`seq` 用于会话内顺序，
`id` 用于历史节点查询和分叉；旧格式没有 id 时，读取器从原 session ID 和 seq 推导稳定 ID，不重写文件。

| 记录 | 内容与作用 |
| --- | --- |
| run_started | 本轮输入、模型元数据、参数、工具定义和版本、调度方式、所需钩子名称 |
| messages_added / queues | 注入上下文、安全边界处的排队输入及消费进度 |
| model_request | 当前模型配置；上下文或转换后的输入发生变化时记录相应内容 |
| provider_parameters | SDK 内置 HTTP adapter 最终过滤／重写后的参数及处理报告 |
| model_completed | 完整结构化模型消息，保留必要的 Responses 原始 output／reasoning 数据 |
| model_attempt | 已结算的失败或取消生成，供审计；不作为恢复后的可执行模型历史 |
| tool_started | 工具实际执行前的调用身份与最终参数 |
| tool_returned | 执行器已经返回的原始结果，即使后处理尚未完成也能保留 |
| tool_completed | 最终工具结果；与原始结果相同时只记录引用，避免重复保存正文 |
| tools_completed / turn_completed | 批次结束、工具结果的模型顺序、finish_turn 决策 |
| run_completed / run_interrupted / run_resumed | 正常结束、停止／中断及恢复尝试 |
| tool_retry_authorized / history_reset | 外部明确决定重试未知工具结果或放弃当前工作 |
| compaction_started / compaction_failed | 摘要生成尝试、范围、配置和失败原因 |
| compaction | 完整摘要、保留消息边界、估算 token 数及生成详情 |
| context_replaced | 自定义请求准备改变上下文时，记录新的选择结果 |
| session_forked | 新会话来源的 session ID 和 event ID；之前已复制完整前缀 |

常规执行不在每个边界重复存储整份历史；完整历史由记录推导。
自定义上下文替换或转换改变实际模型输入时，需要保留相应快照。
高频 token 增量和工具进度不会写入文件。进程突然退出时，尚未结算的生成可以全部丢失并重做。

不保存 API Key、认证 headers、Python 函数或连接对象。消息、工具结果、details 和持久参数必须能表示为 JSON，
不允许 NaN／Infinity。提供者配置和自定义 stream_fn 的可执行实现由宿主重新注入。

## 可靠边界与直接调用

Runtime 在关键位置直接等待 Session 的保存方法，不通过 `subscribe` 驱动持久化。
文件提交在 `flush` 和 `fsync` 完成后才返回，然后推进 Session 的已确认状态。

- 模型完整响应可靠保存后，才开始工具处理。
- 工具执行意图可靠保存后，才调用执行器。
- 主流程逐个接收工具结果，先保存原始结果，再运行 after_tool_call；不等待整批工具结束。
- 最终工具结果可靠保存后，才发出完成事件并供后续模型使用。
- 文件写入失败会抛出 `SessionError`，停止推进，不转换成可忽略的工具错误。
- 取消发生在文件提交过程中时，等待该次提交结算、更新投影后再传播取消，避免后台文件写入尚未结束就报告运行结束。
- 持久化失败后重新打开 Session；当前实例不继续提交。

外部事件订阅仍用于 SSE、UI 或额外审计。订阅异常可能停止执行，但不能替代 Session 恢复协议。
外部已经保存的业务动作不会因为后续回调失败而被撤销。

## 从另一个进程恢复

```python
session = LocalSession(session_id="conversation-123", directory="./sessions")
agent = Agent(session=session, tools=original_tools, before_tool_call=original_approval_hook)

if session.resumable:
    await agent.resume()
else:
    print(agent.state.messages)
```

已有模型配置从文件还原，凭据从新的进程配置重新注入。
原来使用了审批、上下文转换、finish_turn 等钩子时，重新注入对应钩子；缺失时拒绝恢复。
钩子代码本身不会序列化。工具还需要相同的名称、schema、description、version 和 replay 声明。

`Agent.resume()` 与 pi 的 `continue_()` 不同：前者可以恢复已保存的 assistant 工具计划及部分完成的工具批次。
后者保留原来的历史尾部限制。存在未完成运行时，`prompt()` 不会自动覆盖它。

| 可靠前缀结束位置 | resume 行为 |
| --- | --- |
| 输入或模型请求 | 再做该模型请求；已保存的转换输入和内置 adapter 参数直接复用 |
| 模型响应 | 直接处理工具或结束这一轮，不重做已完成的模型请求 |
| 部分工具结果 | 复用已保存结果，只处理剩余调用；模型结果顺序不变 |
| 原始工具结果 | 只继续结果后处理，不重新调用工具执行器 |
| turn_completed | 复用已经保存的 finish_turn 决策 |
| run_completed | 展示结果，resume 会提示没有未完成工作 |

内存快照不需要持久化。恢复读取完整有效日志前缀并重建投影，包括已提交的上下文压缩记录。
尚无存储日志的物理压缩、历史分页索引或定期快照加速。
业务查询可使用 `session.snapshot`、`session.revision` 和 `session.read_records(after_seq=...)`。
`read_records(after_seq=...)` 每次主动读取文件；向前轮询时只校验、解析尚未读取的尾部，使用一个内存游标，不创建额外索引文件。约定已提交前缀只追加、不原地修改；文件替换、缩短或同长度修改会触发重新校验，完整历史查询始终重新校验。
`snapshot`、`revision` 和上下文投影不随其他进程的写入自动更新；需要新的投影时重新打开 LocalSession。打开时仍会完整重放日志，没有消除超长会话的首次加载成本。

提交时只复制状态的顶层容器，避免每条事件递归复制全部历史。内部 reducer 必须替换嵌套值，不能原地修改旧消息／工具结果；对外返回的快照与上下文仍是独立副本。

历史查询使用 `snapshot_at(event_id)` / `build_context(event_id)`；`fork(event_id)` 将前缀复制为独立的新会话，
不在原日志建立多分支。单份日志、压缩及分叉细节见 [上下文设计](context.md)。

## 工具结果未知

`tool_started` 已保存但没有 `tool_returned`／`tool_completed`，意味着外部结果未知。
默认 `AgentTool.replay="never"`，恢复会抛出 `ToolRecoveryRequired`，包含 `call_ids`。

```python
# 已核实外部操作成功：保存真实结果后再恢复。
await session.resolve_tool_call(call_id, verified_result)
await agent.resume()

# 调用方明确决定重试未知调用：保持原调用身份。
await session.authorize_tool_retry(call_id)
await agent.resume()
```

只有可安全重试的工具才声明 `replay="safe"`，此时 Runtime 可自动重试未结算的调用。
`session.tool_operation_id(call_id)` 提供按 session/run/step/call 区分的幂等身份，恢复时保持不变；
工具执行器需要把它传给支持幂等的外部服务。SDK 不自动让任意外部操作具备幂等性。

`AgentTool.version` 用于显式区分工具实现版本。名称和 schema 一样但业务实现改变时也应升级它。
审批前或后处理钩子若在自己的完成记录写入前中断，可能重新运行，应避免不可重复的外部副作用。
当前 before_tool_call 仍是等待式钩子，尚无独立的持久化审批单／pending 决策协议。

## 执行约定

一个 Session 同时只交给一个 Agent 执行，Agent 与 Session 实例限于同一事件循环使用。平台负责调度、Worker 接管，以及阻止失效 Worker 的外部写入。切换执行者时重新打开 Session，不复用旧执行者的内存投影。SSE 重连只恢复展示，不启动新的执行者。

主执行协程顺序调用 `Session.commit()`；Session 子类只需实现 `_persist(seq, record)`、`read_records(after_seq)`，支持分叉时实现 `_import_records(records)`。

- 工具任务只执行工具并交回进度和结果。主流程保存调用意图、原始结果、后处理结果，并更新 Agent / Session 的状态。
- 工具可以并行执行；结果按完成顺序逐个提交，交给模型的 toolResult 消息仍按原工具计划排序。
- 内置 `Models.stream_simple()` 是异步准备入口，先由调用协程保存有效请求参数，再启动 HTTP 流；直接使用时写 `stream = await models.stream_simple(model, context, options)`。
- 工具实现、事件订阅和后台 provider 任务不直接提交 Session。扩展 provider 的参数准备必须在返回事件流之前被主流程等待。
- 直接调用 Session 的压缩、分叉、结果核实等修改接口时，调用方应保证 Agent 已停止；查询记录不启动运行。修改上下文后重新创建 Agent，或直接使用会同步内存状态的 `Agent.compact()`；结果核实后使用 `Agent.resume()` 读取恢复状态。

## 停止、排队输入和重置

- `agent.abort()`：协作停止模型和工具；已保存的未完成工作保留，可显式 resume。
- Python Task 取消：向上继续传播，已提交记录保留；瞬时 UI 状态清空。
- `steer()`、`follow_up()`、`await agent.enqueue(text, follow_up=False)`：仅将消息放入内存队列，不在请求处理协程中写入 Session。
- 主流程在运行开始、检查新输入的安全边界及正常结束前保存队列；工具批次结束后再注入 steering，follow-up 在没有工具和 steering 时处理。
- 入队返回不表示已经持久化。平台若要求接收请求返回时输入就可恢复，应先保存输入，再投递给 Agent。强制退出会丢失尚未到达保存边界的内存输入。
- `clear_steering_queue()`、`clear_follow_up_queue()`、`clear_all_queues()`：修改内存队列，由主流程在下一保存边界记录。
- `await agent.reset_session()`：空闲时明确放弃未完成工作，记录 reset，不删除之前的审计日志。
- 同步 `reset()` 不允许绕过尚未完成的持久运行。

## 本地日志恢复

打开时读取完整有效记录前缀，只忽略没有换行的最后一个未完整片段；下一次追加前截断该片段。纯查询不会修改文件。中间损坏、完整记录校验失败、序号不连续、未知必需事件或不支持的格式均报错，不能静默丢弃。

文件持久化及目录同步按 POSIX 本地文件系统验证。部署方应配置保留数据的存储位置，并保证恢复时仍可访问日志；容器临时盘不能保证 Pod 删除后的恢复。

## 验证与参考

测试覆盖独立进程 os._exit、模型和工具边界中断、原始结果后处理失败、未知副作用、显式重试、
停止与反复取消、磁盘错误、主流程统一提交、完成结果即时保存、截断尾部、损坏日志、凭据排除、队列边界保存和参数重放。

事件事实来源与独立持久化边界参考 DeepSeek Harness 的
[Session](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/subsystems/session.md) 和
[Persistence](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/subsystems/persistence.md) 设计。
Agent loop 继续以本项目固定的 pi 版本为主参考；LocalSession 是本 SDK 的扩展，不宣称兼容其他项目的日志格式。

## PostgreSQL 后端

可选 DatabaseSession、DDL、审查查询及 S3 冷归档见 [数据库组件](database.md)。
