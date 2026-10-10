# PostgreSQL Session、审查与 S3 归档

这是可选组件，不改变 LocalSession 的默认行为，不提供 Worker 所有权或后台归档调度。
安装 `agent-runtime[postgres,s3]`；只用 PostgreSQL 时安装 `[postgres]` 即可。
Python 3.11–3.13，DDL 使用 PostgreSQL 14+ 支持的语法；CI 验证 PostgreSQL 16。

## DDL 与部署

版本化 DDL 随包发布在 `src/agent_runtime/storage/sql/`。
新部署依次执行 `001_sessions.sql`、`002_checkpoints.sql`；已安装 001 的部署只执行 002。
例如 `psql "$AGENT_DB_URL" -f .../002_checkpoints.sql`。
也可以显式调用 `await store.install_schema()`。正常连接和打开 Session 不会执行 DDL。
数据库账号需有目标 schema 的权限；schema 通过 PostgreSQL 连接串 options/search_path 配置。
DDL 不使用 `IF NOT EXISTS` 掩盖结构不匹配，已安装时重复执行会失败。

七张表：

| 表 | 内容 | 事件冷化后 |
|---|---|---|
| agent_sessions | 租户、会话、创建/更新时间、next_seq、metadata | 保留 |
| agent_session_events | seq、event_id、完整 record、metadata | 已归档区间删除 |
| agent_session_archives | 序号区间、S3 URI、版本、SHA-256、格式版本、metadata | 保留 |
| agent_session_checkpoints | checkpoint 的 seq、event_id；不保存状态正文 | 保留 |
| agent_runs | 用户运行身份、主任务归属、来源、状态与时间 | 保留 |
| agent_audit_events | 实际执行者、访问对象、行为、结果与时间 | 保留 |
| agent_model_usage | 每次模型调用尝试、用户归属、token 与已知/未知状态 | 保留 |

事件 ID 仅在会话内唯一，分叉复制的历史保留原 ID。
正文使用 `json`，支持转义后的 NUL 等模型/工具文本；metadata 使用 `jsonb`，遵守 PostgreSQL jsonb 的字符限制。
事件归档不会删除审查表。审查保留策略由业务独立管理，本组件不自动清理审查记录。

## 装配

```python
from agent_runtime import Agent
from agent_runtime.sessions import DatabaseSession
from agent_runtime.storage import ArchiveConfig, DatabaseConfig
from agent_runtime.storage.audit import AuditContext
from agent_runtime.storage.postgres import PostgresStore
from agent_runtime.storage.s3 import S3Archive

archive_config = ArchiveConfig.from_env()
archive = S3Archive(archive_config) if archive_config.enabled else None
store = await PostgresStore.connect(DatabaseConfig.from_env(), archive=archive)
try:
    session = await DatabaseSession.open(
        store, tenant_id="company-a", session_id="conversation-123",
        audit_context=AuditContext(user_id="user-456"),
    )
    agent = Agent(session=session, model="gpt-6-luna")
    if session.resumable:
        await agent.resume()
    else:
        await agent.prompt("继续分析这个问题")
finally:
    await store.close()
```

租户和用户身份由已认证的业务服务注入，不能直接信任模型或客户端提交的身份。
所有存储查询按 tenant_id 限定；本组件不替代业务鉴权，也不默认创建 PostgreSQL RLS 策略。
一个服务进程共享一个连接池，不为每个 Session 创建连接池。

`DatabaseSession(...)` 构造器仅用于全新会话，不执行网络 I/O；已有会话必须 `await open()`。
同步 `build_context()`、`snapshot` 读取当前执行状态。历史记录使用 `await session.aread_records(after_seq)`，
历史状态使用 `await session.asnapshot_at(event_id)`；DB Session 的同步历史接口会提示使用异步接口。
观察其他 Worker 的新事件也可调用 `await session.read_latest_records(after_seq)`，不会修改当前执行状态。
接管时重新 `open()`，不要把观察到的新事件直接灌入活动 Agent。

打开时查找最近的压缩 checkpoint，只读取该事件到日志末尾，推算后释放事件列表。
内存保留上下文、运行阶段、队列、运行输出和子 Agent 关联等恢复状态，不保留完整日志缓存。
没有 checkpoint 的旧会话从头重放；主动或自动压缩成功后自动生成 checkpoint，无额外配置。
状态正文放在压缩事件的 `checkpoint` 字段中，定位表和事件在同一事务提交。
定位表在冷化后保留，恢复只下载与所需区间相交的 S3 对象；gzip 对象内部仍需整块解压。

历史状态查询选择目标节点及其之前最近的 checkpoint。普通事件冷化后，仅凭 event ID 定位需读取历史，
因为没有为每条冷事件维护额外索引；热事件和 checkpoint 可直接定位。完整历史查询和独立分叉仍按需加载历史。
checkpoint 不删除旧数据，也不执行模型或工具。工具、回调和策略由宿主重新装配。
当前恢复仍一次性加载 checkpoint 后的区间，未实现分页重放；长时间不压缩或单次运行输出很大时，内存仍可能较大。
所有 PostgreSQL I/O 为异步，S3 SDK 调用在线程中执行。

## 审查与统计

`run_started` / `run_resumed` / 终态事件投影到 agent_runs，和事件本身在同一事务提交。
恢复同一运行不新增用户运行次数；分叉导入历史不产生新的历史访问或用量。
默认 root_run_id 使用 `session_id/run_id`，避免独立分叉保留旧 run_id 时错误合并主任务统计。
分叉后恢复执行才记录新运行事实与新模型调用。

DB Session 通过 Agent 的 stream 包装接口记录模型调用尝试：

- 每次 adapter 调用前生成独立 attempt_id，失败或恢复重试不会共用 ID。
- `model_call_started` / `model_call_finished` 也是完整日志的一部分，其投影不改变模型上下文。
- 正常生成正文仍由 model_completed/model_attempt 保存，不在用量事件里复制一遍。
- 上下文压缩使用同一包装入口，保存压缩模型的请求和结果并单独计量。
- 子 Agent 来源标为 subagent，从父运行继承用户与 root_run_id；主 Agent 收取结果不重复计量。
- 未拿到完整 usage 时，各 token 列为 NULL，usage_status 为 unknown，不把未知当作零。
- 崩溃时只有开始记录的调用保留 unknown，可能已经在 provider 产生费用。

内置 provider 标记 `usageStatus="known"`。自定义 stream adapter 必须显式标记并返回完整的
input/output/cacheRead/cacheWrite/totalTokens，否则审查保守地视为未知。
统计单位是 adapter 调用；自定义 adapter 若内部自行重试多个收费请求，应在外层拆开或自行提供逐次审查。
恢复时重新装配 Agent，以保留 Session 的包装；不要直接覆盖已装配 Agent 的 stream_function。
低层 loop 若绕过 Agent，需显式使用 `session.wrap_stream_fn(stream_fn)`。

网页浏览、输入接收、下载、工具访问的具体资源由业务服务/工具适配器主动记录：

```python
await store.audit(
    tenant_id="company-a", audit_id="request-unique-id",
    actor_id="user-456", actor_type="user", user_id="user-456",
    session_id="conversation-123", action="input_submitted",
    resource_type="session", resource_id="conversation-123", outcome="success",
    metadata={"department_id": "sales"},
)
```

同一行为重试使用相同 audit_id；不要给不同的行为复用该 ID。
资源访问应提供实际资源 ID/版本；SDK 不凭工具参数猜测访问对象。审查默认不复制正文，
若需要证明当时看到的内容，应将正文或不可变版本保存在工具结果/业务归档中。
输入接收审查由入口服务记录，不依赖尚未落盘的 steering 内存队列。

查询方法均要求 tenant/user 和 UTC 的 `[start, end)` 时间区间：

- `daily_usage(..., timezone_name="Asia/Shanghai")`：每日已知 token、未知尝试数、调用数。
- `daily_runs(...)`：按开始日统计发起，按完成日统计完成；排除 subagent/system。
- `daily_activity(...)`：用户实际行为及 input_submitted 次数；后台 Agent 不算用户活跃。
- `audit_history(...)`：按时间读取访问详情，默认 limit=1000。

返回的 token 总量在全为未知时为 NULL。完整调用用量按结束时间归属日期，尚无结束时间的未知调用按开始时间。
统计不依赖冷事件和 S3，也不与 provider 账单承诺完全一致。需要分页或更丰富报表时直接基于公开 DDL 查询。

## 冷热转换

环境变量完整清单见 `.env.template`。配置只读取进程环境，不隐式加载 .env。
默认归档关闭；开启需 S3 URI、AWS_REGION 和同区域的 KMS key ARN。AWS 凭据使用标准凭据链，生产建议 IAM Role。
上传显式指定 SSE-KMS 和 Key ARN，验证加密响应并读回校验 SHA-256 后才允许删除 DB 热事件。
需要 S3 Put/Get/Head 与对应 KMS GenerateDataKey/Decrypt 权限；SSE-KMS 是服务端加密，不是客户端加密。

```python
candidates = await store.archive_candidates("company-a")
# 业务调度器排除活动执行者、禁止目标会话启动，再调用；候选不等于已获得所有权。
await store.archive_session("company-a", "conversation-123")
```

默认按 30 天无新增事件判冷；查询不更新 updated_at。不是每个 SDK 实例启动一个扫描服务。
每个 JSONL gzip 对象最多按 10000 条或约 32 MiB 未压缩内容分块，单个事件不拆分。
超大单事件超出 S3 单次上传能力会失败并保留 DB 数据，不自动切换上传策略。
对象 URI 带内容摘要，条件写入禁止覆盖；存在对象时验证已有内容。
版本化 Bucket 的 VersionId 记录在目录里；读取使用已保存 URI，不依赖后来修改的上传配置。
关闭新归档后仍需读取冷数据时，可显式给 store 传入 `S3Archive(config)`（enabled=False），
此时只允许读取，上传与归档接口会拒绝执行。
旧 KMS Key 和对象必须保留可读，不能通过 Bucket 生命周期直接删除仍被目录引用的对象。

上传完成后，一个短事务发布目录并删除对应事件。失败只可能留下未引用对象，不会先删热数据。
上传不持有数据库事务。业务保证归档期间没有写入者或第二个归档者；代码没有分布式锁。
读取用同一个 REPEATABLE READ 快照获取目录和热数据，再下载归档、校验、按序拼接。
冷数据不自动写回 DB，新事件直接从 next_seq 继续追加。

未知格式、缺失对象、序号缺口或校验失败会停止读取，不能跳过继续执行。
不默认使用需要等待取回的 Glacier 档位。归档上传中断产生的未引用对象由运维独立清理。

## Subagent 装配与恢复

子 Agent 的 Session 工厂保持同步：新任务可返回新的 DatabaseSession；已有任务需要业务先异步 open，
放进以 task_id 为键的实例字典，工厂只返回已打开对象。任务 ID 可从父日志的 spawn_agent 结果中取得；
父工具尚未保存结果时，使用同一父工具操作标识调用 manager.task_id() 计算，再异步 open。
未存在的会话 open 返回空 Session，不写 DB。每个子 Session 只允许一个活动执行者。

源会话可以 `await session.fork(event_id)`，也可通过现有 fork_into 导入 LocalSession。
DB 分叉在一个事务中发布完整历史，不把原用量再次计费。对冷历史跳转需要读取对应日志，当前不保留逐冷事件索引。

## 测试

`AGENT_TEST_DB_URL` 指向测试数据库时运行 `python -m unittest discover -s tests -v`。
每个数据库测试创建并清理独立 schema，测试账号需 CREATE SCHEMA 权限；不在生产环境运行。
CI 使用真实 PostgreSQL 16，模型使用模拟 adapter，S3 使用可控客户端验证协议参数与故障行为。
真实 AWS IAM/KMS、网络和 Bucket 策略需要在部署环境另做连通性验证。
