# 后台子 Agent

`agent_runtime.subagents` 是显式装配的组合模块。核心 loop 仍只处理普通工具。
每个子 Agent 有独立 Session、上下文和输入队列；多个子 Agent 可以并行运行。
第一版是同进程 asyncio 执行，不提供集群调度或平级 Agent 通信。

## 装配

```python
from agent_runtime import Agent, LocalSession
from agent_runtime.subagents import SubagentManager, create_subagent_tools

parent = Agent(session=LocalSession("main"), model="gpt-6-luna")
manager = SubagentManager(
    parent,
    agent_factories={
        "researcher": lambda session: Agent(
            session=session,
            model="gpt-6-luna",
            system_prompt="根据收到的任务和材料进行研究，并给出结论。",
            tools=[],  # 由业务显式配置；不自动继承父 Agent 的权限
        )
    },
    session_factory=lambda key: LocalSession(key),
)
parent.state.tools.extend(create_subagent_tools(manager))
try:
    await parent.prompt("启动两个研究任务，然后汇总结果")
    # parent.prompt 返回后，后台任务仍可运行；不要在每一轮后关闭 manager。
finally:
    await manager.close()  # 服务/会话执行范围结束时收尾
```

工厂是同步函数。Agent 工厂每次创建新实例，必须使用收到的 Session；恢复时保持原配置及工具版本。
Session 工厂按 ID 打开同一个日志。使用 `directory=None` 时，业务需要按 ID 复用内存 Session。
子 Agent 不自动继承父上下文；任务和材料统一通过 `task` 显式传入。
默认不给子 Agent 装配委派工具。共享文件的写入范围由业务分配。

## 工具与 Python 接口

| 工具 | Python 接口 | 语义 |
|---|---|---|
| `spawn_agent` | `await manager.spawn(name, task, operation_id=...)` | 持久化创建信息后返回 ID，不等待模型完成 |
| `get_agent` | `manager.get(task_id=None)` | 查询单个或列出关联任务 |
| `send_agent_message` | `await manager.send(task_id, message, follow_up=False)` | 运行中排队，正常完成后开始下一轮 |
| `wait_agent` | `await manager.wait(task_id, timeout=None)` | 等待本地执行；超时或取消等待不取消子任务 |
| `stop_agent` | `await manager.stop(task_id)` | 取消当前执行并等待收尾 |

状态包括 `running`、`completed`、`failed`、`needs_recovery`；还返回原始 `session_status`、错误和已完成的最后一条 assistant 消息。
子任务失败不会自动终止兄弟任务。恢复失败通过状态和错误返回，未知工具结果仍由 Session 的原有恢复机制处理。

工具适配器自动使用父 Session 的工具操作标识。直接调用 Python `spawn` 时，业务提供稳定且唯一的 `operation_id`。
同一标识表示同一次委派，不得换任务复用。直接创建的任务在进程重启后由业务保存的 ID 找回；工具创建的任务可从父日志推导并列出。

创建、查询、等待允许安全重放。停止和发送消息不可自动重放，避免误停新工作或重复输入。
同一管理器的生命周期操作由调用者协调，不要并发地对同一个子任务执行恢复、停止或开启新一轮。

## 单份日志与恢复

子 Session 的 `subagent_created` 记录包含父 Session ID、父操作标识、配置名称和初始任务，不进入模型上下文。
它由子任务执行协程先保存，随后同一个协程运行 Agent。父工具协程不写子 Session。

恢复区分三个节点：

1. 只有创建记录：显式恢复时从保存的初始任务启动。
2. 已有 `run_started` 且未完成：调用现有 `Agent.resume()`，不重复提交初始任务。
3. 已经完成：直接读取结果，不再次执行。

父工具结果尚未保存时发生中断，相同操作标识会找回原子任务。重放 `spawn_agent` 不会自动重启已存在的任务。
进程重启后，查询和等待均不执行恢复。业务先保证旧执行者已退出，再重新装配管理器并调用：

```python
await manager.resume(task_id)
state = await manager.wait(task_id)
```

若状态为 `waiting_recovery`，按 [Session 恢复接口](sessions.md) 核实未知工具结果。
在执行者停止后打开子 Session，调用 `resolve_tool_call` 或 `authorize_tool_retry`，再使用重新装配的管理器恢复，以读取最新状态。
不要在管理器背后修改其已打开的 Session。分叉或重置子会话应作为新的业务会话处理，不复用原委派身份。

运行中追加消息只表示内存入队，安全节点才保存；可靠接收由平台先保存输入。
子 Agent 正在启动或收尾时发送消息会提示稍后重试；失败或中断状态必须先恢复。

## 取消、事件和 SaaS 边界

主 Agent 正常完成不取消子任务。用户停止整个工作时，从外部业务协程调用 `await manager.stop_all()`；它停止父 Agent 和当前管理器的活动子任务。
`await manager.close()` 同时禁止后续启动。不要从父/子 Agent 自己的工具或事件回调调用这两个收尾接口。
原有 `parent.abort()` 仍只停止父 Agent；Python 取消子任务不能撤销已经发生的外部副作用。

`manager.subscribe(listener)` 转发 `{"task_id": ..., "event": ...}`，回调签名为 `(event, signal)`，返回取消订阅函数。
回调由子执行协程按顺序等待，外部 UI 缓冲、慢消费者和断连应在业务层隔离。
结果不会自动注入父输入，也不会自动开启新的模型调用。

每个 Session 仍只有一个执行协程写入，每个父 Agent 只绑定一个活动管理器。
平台负责租户身份、工作目录、并发额度、Worker 所有权及跨进程接管。
运行句柄和观察信息只保存在内存中；没有额外持久化任务状态表。

## 参考与验证

参考 PI `8369268` 的 subagent 扩展与 `pi-durable` 前台/后台示例：复用工具适配、独立会话、稳定操作身份和不可重放的停止操作。
不移植子进程 CLI、Anchor、Reporter 或持久任务图。

离线示例：`uv run python examples/subagents.py`。通过模拟模型展示两个后台任务、父 Agent 继续工作和显式等待汇总，不访问真实 provider。
