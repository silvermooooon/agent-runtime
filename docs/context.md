# 单份日志、上下文推导与新会话分叉

每个 Session 只有一份完整的可靠事件日志。当前上下文、消息位置和执行阶段均由日志重放得出；
恢复状态随压缩事件保存，不建立第二套上下文存储。完整历史通过 `read_records()` 查询
（DB 使用 `await aread_records()`），模型可用上下文通过 `build_context()` 查询。
高频 token 增量和工具进度继续使用瞬时内存事件，不写入日志。

## 压缩记录如何生效

```text
完整日志：消息 1～100 → 压缩记录 C → 消息 101、102……
C：摘要 + 保留边界 + 压缩完成后的恢复状态 checkpoint
有效上下文：系统指令和工具声明 + C 的摘要 + 保留消息 + 后续消息
```

checkpoint 包含有效消息及 ID、系统指令、模型和工具配置、运行阶段、已保存队列、运行输出及子 Agent 关联。
这些字段足以独立恢复，不引用 checkpoint 之前的日志；因此会重复保存保留消息等仍然有效的数据。
状态带格式版本，不包含工具实现、连接、凭据或回调函数，这些由宿主重新装配。
恢复从最近 checkpoint 开始重放后续事件；没有 checkpoint 的旧日志仍从头重放。
压缩前的原文、此前的摘要、工具结果都继续留在日志里。重复压缩时，以前一份摘要加上新选中的消息生成新摘要。
当前 Agent 状态与新进程重放得到的上下文一致。

## 装配策略

```python
from agent_runtime import Agent, CompactionSettings, LocalSession

agent = Agent(
    session=LocalSession("chat", "./sessions"),
    compaction=CompactionSettings(
        enabled=True,
        reserve_tokens=16384,
        keep_recent_tokens=20000,
    ),
)
```

未传 `compaction` 时不自动生成摘要。传入后，SDK 在新的模型请求前检查大小，包括本轮工具批次完成之后。
触发条件参考 pi：估算上下文 token 数大于模型窗口减去 `reserve_tokens`。未知窗口不自动触发。
当前使用消息大小估算，避免压缩前的 usage 被误用为新上下文大小；不是精确 tokenizer。

保留约 `keep_recent_tokens` 的近期消息，切点不能从 toolResult 开始。如果一次用户请求跨度很大，
会生成历史摘要和该请求前缀的衔接摘要。摘要生成沿用 Agent 当前模型、provider 和凭据入口；不切换模型。
一次压缩通常调用一次模型，分割用户请求时最多两次。工具输出在摘要输入中截断到 2000 字符，原日志内容不变。

生成策略可通过继承 `Compactor`，覆写 `prepare` / `generate` / `should_compact` 后传给 `Agent(compaction=...)`。
新进程需要重新装配相同的自动压缩策略。已经提交的摘要无需策略或原摘要模型即可重放。

## 手动压缩

Agent 空闲且没有未完成运行时：

```python
event = await agent.compact("保留约束、关键路径和下一步")
print(event["id"])
```

如果当前消息还没有超过保留预算，会提示没有可压缩内容。可以调整 `keep_recent_tokens`，
或由宿主直接提供摘要和保留边界，避免摘要模型调用：

```python
entries = agent.session.context_entries()
keep_from = entries[-2]["message_id"]
event = await agent.compact(
    summary="旧消息的摘要……",
    first_kept_message_id=keep_from,
)
```

提供摘要且边界为 `None` 时，所有既有对话消息被摘要替代，系统指令和工具声明仍保留。
`message_id` 从来源 event ID、字段和位置确定：一个持久化事件可能引入多条消息，因此消息边界比 event ID 更精细。
外部提供摘要时，由宿主确保摘要覆盖所选范围；SDK 校验边界存在、摘要非空和工具结果配对。

## 生命周期与故障

先追加 `compaction_started`，生成成功后把摘要和 checkpoint 作为同一条 `compaction` 提交，再切换内存投影。
Local Session 的 `checkpoint.json` 仅保存最新 checkpoint 的事件 ID、序号和文件偏移；丢失或过期可从日志重建。
DB 的定位索引与事件在同一事务写入，冷化时保留索引。
生成失败、空摘要、输出截断、生成工具调用都会拒绝提交摘要；普通失败追加 `compaction_failed`。
外部订阅者收到 `compaction_start` 和 `compaction_end`，结束事件含结果记录或错误。

生成期间崩溃，重放仍得到旧上下文，可重新尝试压缩。已经提交的摘要会直接复用。
取消发生在文件提交过程中时，等待提交结算；成功提交的摘要不会因为取消而被撤销或记作失败。
存储失败抛出 `SessionError`。任意故障下都不保证订阅者一定收到结束事件，宿主仍需处理异常。

手动压缩不丢弃未完成模型／工具步骤；应先恢复。恢复已经保存的模型请求时，直接复用该请求输入，
不会在重试前插入一次新的压缩。暂未移植 pi 的模型上下文溢出后自动 compact-and-retry，以及摘要请求的自动网络重试。

## 任意日志节点与独立会话

```python
records = session.read_records()
event_id = records[-1]["id"]  # 也可以选择任意一个历史记录
old_state = session.snapshot_at(event_id)
old_context = session.build_context(event_id)

child = await session.fork(event_id, session_id="new-chat")
agent = Agent(session=child, tools=original_tools)
if child.resumable:
    await agent.resume()
else:
    await agent.prompt("开始新的尝试")
```

DB 历史状态查询使用 `old_state = await session.asnapshot_at(event_id)`，上下文为 `old_state["messages"]`。

分叉复制从第一条到选定事件（含该事件）的全部记录，再追加 `session_forked` 来源记录。
新会话有自己的 ID、顺序和完整文件，不依赖父日志继续存在，也不改变父会话。
继承的 event ID 保留以维持摘要边界引用；新追加的事件生成新 ID，因此跨会话查询时使用 `(session_id, event_id)` 定位。
已有目标会话会被拒绝，文件后端通过临时文件和原子替换一次发布完整前缀。

历史查询只使用选定节点及此前的日志，之后的摘要不会污染更早节点。
内存模式同样支持独立分叉。存储无关入口为 `Session.fork_into(event_id, target_session)`；后端需要实现原子 `_import_records`。

分叉会继承选定节点处的未完成执行状态。已保存结果复用；只看到 `tool_started` 的未知副作用仍抛出
`ToolRecoveryRequired`。选到工具执行之前的节点意味着新分支可能再次执行它；Session 不回滚文件系统或外部服务。

## 自定义上下文转换

内置压缩在完成事件中保存恢复状态，正常后续请求无需重复保存整个压缩后上下文。
原有 `prepare_request` / `transform_context` / `convert_to_llm` 仍可用：外部任意替换上下文或实际请求输入时，
日志需要记录变化结果，以便精确恢复。它们位于同一份日志中，未引入独立的上下文存储。
`build_context()` 返回标准消息投影；特定请求经过转换的输入记录在 `model_request` 中。
需要长期生效的摘要应使用正式压缩接口，而非每次在 `transform_context` 中重新生成摘要。

## 参考与范围

算法和模块分工参考 pi 的 `AgentSession`、`compaction.ts`、`buildSessionProjection()`。
本实现每个 Session 保持单条事件序列，分叉创建新 Session，不实现 pi 的同文件多分支树。
压缩边界采用稳定的消息位置，兼容本 SDK 一条事件含多条消息的执行日志。
DB、S3、归档、检索索引和跨 Pod 协调不在此模块实现。
