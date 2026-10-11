# 用户 Memory

Memory 位于独立的 `agent_runtime.memory` 模块，复用普通工具、Agent 和 Session。
核心 loop 不感知 Memory。Session 保存执行历史；Memory 保存跨会话使用的用户记忆。
两者不互相替代，压缩 Session 上下文也不会自动产生长期记忆。

## 启用与归属

```python
from agent_runtime import Agent, LocalSession
from agent_runtime.memory import LocalMemoryStore, create_memory_tools

session = LocalSession(tenant_id="company-a", user_id="alice")
store = LocalMemoryStore(session.info)  # AGENT_MEMORY_DIR，默认 .agent-runtime/memory
agent = Agent(
    session=session,
    tools=create_memory_tools(session=session, store=store),
    system_prompt="按需读取用户记忆；只有明确、长期有效的信息才保存。",
)
await agent.prompt("记住，我更喜欢简短的回答。")
```

仅导入模块、提供用户身份或设置环境变量，不会扫描记忆、创建仓库或启动后台任务。
只有宿主显式装配工具后，模型才可调用它们。只读场景使用 `read_only=True`。
后台整理还需宿主显式调用 `run_memory_dream`；SDK 没有定时器或用户注册表。
平台应让工具装配和后台任务遵循同一项用户 Memory 启用配置。

Memory 要求 `session.info` 同时有 `tenant_id` 和 `user_id`，缺少任意一项即拒绝装配。
Store 绑定这两个标识，可复用于同一用户的其他 Session；工具仍逐次检查归属。
身份来自宿主，不提供让模型传入或切换用户的工具参数。
认证、授权、用户与 Git 仓库映射由 SaaS 平台负责；SDK 不把身份字段当作授权凭据。

## 工具与存储接口

| 工具 | 参数 | 行为 |
| --- | --- | --- |
| `memory_read` | `key` | 返回完整内容、版本和来源；不存在时返回 missing |
| `memory_search` | `query`, 可选 `limit` | 关键词匹配，返回摘要、版本和来源；limit 默认 10，上限 50 |
| `memory_write` | `key`, `content`, `expected_version` | 创建或替换内容；创建不存在的键时版本传 null，更新前先读取 |
| `memory_delete` | `key`, `expected_version` | 按已读版本删除 |

键是 `MEMORY`、`people/alice` 等不带后端扩展名的逻辑名称，不是任意本地路径。
可以在内容中使用 `[[people/alice]]` 作为导航约定；工具不解析 Markdown、不自动遍历链接。
搜索采用不区分大小写、所有关键词均需命中的文本匹配，不包含向量或语义检索。

`MemoryStore` 定义异步 `read/search/write/delete`，后续 DB/S3 实现继承它即可。
`MemoryEntry` 只包含 key、content、version、sources、updated_at，内容是普通文本。
`operation_id` 由 Session 的稳定工具操作标识传入，供后端记录或核实；它本身不是幂等保证。
没有另外增加 Memory 专用的 Agent 状态机、锁、消息总线或会话日志。

## 来源、上下文与恢复

前台写入默认关联当前 Session 最近一条用户消息的持久化 event ID。
压缩产生的摘要不会被当成用户原话；上下文中仅剩摘要时，宿主需要提供原始来源或等待新输入。
宿主也可向 `create_memory_tools(..., sources=[MemorySource(...)])` 提供已核实的来源。
更新会保留并去重已有来源；来源是条目级线索，不意味着模型生成的每个事实均已验证。
历史引用使用 `MemorySource(session_id, event_id)`，由宿主按当前用户权限读取原始日志。

读取结果、搜索结果、写入参数及结果都通过现有工具流程写入 Session 日志。
恢复已完成的工具使用记录下来的结果，不重新读取最新 Memory 替换过去的上下文。
尚未完成的读操作标记 `replay="safe"`，恢复时可能读到更新后的内容；写入和删除标记
`replay="never"`，已开始而没有可靠结果时使用现有 `ToolRecoveryRequired` 暂停核实。
核实后通过 `session.resolve_tool_call()` 提供真实结果，再继续前台 Session。
Memory 与 Session 分属两个存储，不承诺跨存储的原子提交或恰好一次执行。
Session 分叉默认继承用户身份，也就继续访问该用户当前的 Memory；它不会回滚共享记忆。
需要隔离试验时，由宿主显式提供另一个存储目录或仓库。

所有 Memory 不会自动塞入 prompt。模型可以先读 `MEMORY` 索引，再搜索和读取需要的条目。
如果希望新会话预加载一份小索引，可以显式调用：

```python
from agent_runtime.memory import load_memory_index

index = await load_memory_index(session=session, store=store)  # 仅用于尚未开始的新 Session
agent = Agent(
    session=session,
    messages=[index] if index else [],
    tools=create_memory_tools(session=session, store=store),
)
await agent.prompt("开始新任务")
```

返回内容标记为参考资料，通过普通输入日志保存。重开已有 Session 时直接恢复日志，
不要重新注入索引；需要最新记忆时调用工具。Memory 文本应作为数据使用。

## 默认 LocalMemoryStore

每个 `(tenant_id, user_id)` 映射到根目录下一个哈希子目录，每个键保存为一个 JSON 文件。
明文保存 content 和少量元数据；没有 Git、Markdown 解析器或数据库依赖。
显式目录优先于 `AGENT_MEMORY_DIR`，构造时不创建文件。
单文件写入使用临时文件、fsync 和原子替换，版本为随机标识；删除前也检查已读版本。

**宿主必须串行化同一用户的全部写入，包括多个会话和后台整理。**
版本比较用于发现过期内容，不是跨进程的原子 CAS；SDK 不添加文件锁或分布式锁。
本地 Store 仅保存最新内容，执行历史留在 Session；它不提供 Git 历史或多文件事务。
后台整理中已经成功的本地写入不会因后续失败回滚。
生产环境文件持久卷、备份、访问权限和用户写入调度由宿主配置。

## 可选 GitMemoryStore

```python
from agent_runtime.memory import GitMemoryStore, create_memory_tools

store = GitMemoryStore(
    session.info,
    remote=user_memory_repository,  # 宿主按 tenant_id/user_id 选择，不能由模型指定
    branch="main",
    timeout=60,  # 每条 Git 命令的秒数上限
    publish_attempts=3,  # 仅无冲突的并发发布允许有限重试
)
agent = Agent(session=session, tools=create_memory_tools(session=session, store=store))
```

需要宿主安装 Git CLI、配置凭据，并准备已有提交和指定分支的仓库。
SDK 不创建 GitHub 仓库，不管理凭据、不 force-push，也不共享可写 checkout。
约定每个用户一个仓库、一条权威分支；仓库远端与本地文件系统必须由宿主管理访问权限。

每次普通操作在临时目录 clone，用完删除，避免共享工作目录的并发状态。
这是简单实现，网络延迟和仓库历史体积会影响每次操作成本，尚未引入 clone 缓存。
逻辑键 `MEMORY` 对应 `MEMORY.md`；新写入的文件首行 HTML 注释保存 sources 和 updated_at。
已有纯文本 Markdown 文件可读取，此时来源为空。无需 Markdown 解析依赖。

版本是读取时的 commit SHA。发布时确认它属于当前分支的祖先历史，并比较涉及文件的 blob：

- 其他文件变化：将当前改动重新应用到最新 HEAD，普通 push；保留他人的提交。
- 同一文件变化：返回 `MemoryConflict`，包含当前内容和版本，调用方重新读取后决定如何合并。
- push 竞争：重新 fetch，再按同一规则检查，最多尝试配置次数。
- push 应答丢失：fetch 后发现本次提交已在分支历史中，视为成功，不再次提交。
- 无法确认发布结果：直接 Store API 抛出 `MemoryPublishUnknown`；工具适配为
  `ToolRecoveryRequired`，暂停 Agent，不让模型把结果未知当作普通失败重试。

提交信息包含 `Memory-Operation`，可结合 Session 的 `tool_operation_id` 核实远端提交。
存储层不做同文件文本自动合并或语义合并。Git 提供历史和普通推送冲突检测，
不会替宿主解决权限或跨系统事务。

## 一次性后台整理（Dream）

```python
from agent_runtime import LocalSession
from agent_runtime.memory import MemorySource, run_memory_dream

dream_session = LocalSession(tenant_id="company-a", user_id="alice")
result = await run_memory_dream(
    session=dream_session,
    store=store,
    experiences=authorized_conversation_text,
    sources=[MemorySource(source_session_id, source_event_id)],
    source_reader=read_authorized_source,  # 可选 async 函数，接收 MemorySource
    model="gpt-6-luna",
    parameters={"max_tokens": 4096},
)
```

宿主选择同一用户尚未处理的经历，提供来源和一个全新独立 Session。
其余关键字参数直接传给普通 `Agent`，可以配置 provider、models、API key、审批钩子、
stream_fn 或预算相关钩子；session、tools、system_prompt 由整理流程提供，不重复传入。
`source_reader` 必须验证当前用户是否有权读取该来源，可以从 DB 或 S3 找回证据。
没有 reader 时，只能使用宿主提供的经历和已有记忆，模型不应凭空补全来源。

Local 后端使用普通工具即时写入。Git 后端使用一个临时 draft，所有读写都在同一快照和
内存改动集上进行，模型最终调用 `memory_publish`，以一个提交发布整个批次。
发布涉及的任一文件冲突则不发布本次改动，下一次任务从最新远端创建新 draft。
未调用 publish、取消或失败时不自动发布；清理临时目录。
已发布的 Git 提交不会因随后 Session 落盘失败而回滚。

Git draft 是临时态，**不支持在新进程中从旧 Dream Session 的工具中间点继续执行**。
中断后保留旧 Session 作审计，创建新 Session 开始新的整理尝试。若发布结果未知，先核实
`Memory-Operation`；确认结果后再决定是否需要新尝试，不直接重新运行旧发布操作。
Local 后端失败时可能已有部分写入，新尝试也应读取最新内容再整理。
宿主负责触发频率、费用／执行时限、失败重试和处理游标，不在异常时推进游标。
成功返回表示 Agent 流程结束（Git 时还确认了发布），不代表模型提炼事实必然正确。

离线端到端示例：`uv run python examples/memory.py`。测试使用模拟模型驱动真实文件和
本地 Git 仓库，包含并发 push、同文件冲突、应答丢失及工具恢复边界。
