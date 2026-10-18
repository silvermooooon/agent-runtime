# Durable local and deployed execution

`DatabaseSession` remains the only canonical execution journal and reducer. SQLite and PostgreSQL use the same seven session, journal, archive, checkpoint, run, audit and model-usage tables. The additional `agent_commands` and `agent_session_leases` tables are SDK-owned acceptance/ownership control, not another run or output journal.

Complete semantic outputs are durable before their corresponding completion events are emitted. Text/thinking/tool-call deltas and tool progress are ephemeral; reopening does not replay token animation. A previously committed model/tool result remains readable even while the enclosing run is unfinished.

## Local SQLite

No optional database dependency is required. Use a local filesystem. SQLite operations run in worker threads, with foreign keys, WAL, full synchronous durability and serialized transactional writes. Network-share/multi-host deployment should use PostgreSQL.

```python
from agent_runtime import Agent, DatabaseSession
from agent_runtime.execution import DurableExecutor
from agent_runtime.storage.sqlite import SQLiteStore

store = await SQLiteStore.connect("runtime.sqlite3")
await store.install_schema()   # Explicit fresh install, or verify known v2; no implicit upgrade.
await store.check_schema()

executor = DurableExecutor(
    store,
    lambda session: Agent(session=session, model="your-configured-model"),
)
result = await executor.execute(
    "tenant", "session", user_id="authenticated-owner",
    command_id="stable-business-request-id", message="Analyze this request",
)
# result: command_id, session_id, run_id, status, snapshot, optional error.
await store.close()
```

The directory must exist before connecting. `connect` creates/opens the file but never installs a schema. `install_schema` explicitly creates an empty schema; a matching v2 installation is checked and left intact. Unknown/partial structures fail closed. `check_schema` is a structural check, not a substitute for deployment migration review or a full integrity audit.

Early local v1 files can be upgraded explicitly with `await store.migrate_schema()`. Stop writers and back up first. The transaction adds missing control columns/indexes, backfills the child-parent projection from accepted input, normalizes timestamp precision and advances to v2. It never deletes journal, business, command or audit rows. Normal connect/open/install does not silently migrate v1.

SQLite's archive manifest/reader contract is compatible with existing `DatabaseSession` recovery; this adapter does not implement local archive upload/rotation. SQLite and PostgreSQL schemas are separate backend DDLs, not portable files that can be copied directly between engines.

## PostgreSQL deployment

```python
from agent_runtime.storage import DatabaseConfig
from agent_runtime.storage.postgres import PostgresStore

store = await PostgresStore.connect(DatabaseConfig.from_env())
# Fresh deployment only: await store.install_schema()
# Existing installation through 003: await store.install_coordination_schema()
await store.check_schema()  # requires canonical tables plus SDK coordination
```

The existing connect/append/recovery/audit/archive API remains supported. Fresh explicit install applies 001 through 004. Existing deployments apply `004_coordination.sql` once, after 001–003; repeat install is intentionally an error. Legacy journal deployments can use `check_schema(require_coordination=False)` and continue unfenced legacy append until they explicitly install/use coordination. Once a session has had a lease, all future writes require a current lease, including after release or expiration. A released lease row is retained so old unbound writers cannot bypass fencing.

PostgreSQL was not available for this implementation exercise. Its deployment SQL and driver APIs were reviewed/imported, not executed against a database. Do not treat the local SQLite checks as PostgreSQL certification.

## Acceptance, FIFO and recovery

- `execute` commits the stable prompt command before execution. Reusing a command ID with different immutable input, session or explicit run ID is rejected. The prompt's run ID is allocated once and attached atomically to canonical `run_started`.
- `execute` only executes its named command. It returns `busy` if another worker holds the lease or an older prompt is ahead in the session FIFO. It never runs an unrelated request through the current factory closure.
- `execute_command` runs a previously accepted named prompt. `run_next` is for hosts whose factory is valid for every pending prompt in that session. Only `kind="prompt"` is executable; notification commands never block prompt FIFO.
- The executor owns lease acquisition, heartbeat, local commit fencing, pre-dispatch ownership/cancel checks, and release. A monotonic fence rejects an expired or replaced writer. Hosts do not need to reproduce that machinery.
- Repeating the same command after crash interruption opens the canonical state and resumes its original run. A pending run belonging to a different command returns `recovery_required`; it is never hijacked by new input.
- Failed, stopped and unknown-tool work is not automatically retried for a new command. `resume` is an explicit retry of the original command and still enforces tool recovery rules and original model/tools/hooks. Cancelled work requires explicit abandonment and a new prompt.
- `request_cancel(tenant_id, command_id)` durably records cross-worker abort intent. The heartbeat and pre-dispatch boundaries observe it. A canceled unstarted command is rejected durably without starting a model; active cancellation is cooperative. `cancel` is only the immediate local-process shortcut.
- `abandon(..., command_id=...)` explicitly clears that command's pending execution, retaining canonical history and earlier run facts. A currently interrupted/running run gets a stopped journal fact first. A failed run remains failed in the run projection. Structurally unfinished assistant/tool batches are excluded from the next model context, while their complete committed records remain in history. Abandoning never undoes or reconciles real-world effects.

Factories may be synchronous or asynchronous and must return a fresh Agent using exactly the supplied session. Re-inject credentials, tools, policy hooks, provider configuration and any external references on recovery. Construct agents promptly; do not perform external effects in the factory. No global/background scheduler is installed by constructing an executor.

## Read APIs

Both stores provide:

- `list_sessions(tenant, user_id=None, limit=100, after=None)`: descending `(created_at, session_id)` cursor.
- `list_runs(tenant, session_id=None, user_id=None, root_run_id=None, status=None, start=None, end=None, limit=100, after=None)`: descending `(started_at, session_id, run_id)` cursor; UTC-aware `[start,end)` filters.
- `read_records(tenant, session_id, after_seq=-1)` and `read_run_records(tenant, session_id, run_id, after_seq=-1, limit=1000)`: canonical semantic records, including archived history when an archive reader is configured.
- `read_recovery_records`, `DatabaseSession.open`, `asnapshot_at(event_id)` and `fork(event_id)` reuse the existing reducer/checkpoint implementation. Reading/forking does not execute tools or reverse past side effects; forked historical imports do not re-charge usage or re-project old runs.
- Existing `daily_usage`, `daily_runs`, `daily_activity`, `audit_history` and `audit` APIs. Unknown usage remains unknown/NULL, never zero. Actual host access audit still requires explicit instrumentation.

Use these canonical facts for activity/detail projections. The SDK does not authenticate tenant/user parameters or implement business ACLs. Verify access before calling it and before dereferencing any tool result or attachment.

## Durable child agents and result handoff

```python
from agent_runtime.subagents import DurableSubagentManager, create_subagent_tools

manager = DurableSubagentManager(
    parent_agent,
    agent_factories={"research": child_agent_factory}, # async factories supported
)
parent_agent.state.tools.extend(create_subagent_tools(manager))

async def child_finished(notification):
    await business_fifo.accept_child_result(notification) # stable notification ID dedupe
    await manager.ack_notification(notification["command_id"])

manager.subscribe_completion(child_finished)
await manager.recover(resume=True)  # discover/reconcile/recover at service startup
```

`create_subagent_tools` is compatible with the durable manager; install/merge tools using the host's ordinary Agent configuration API. The manager owns deterministic child session IDs, durable initial prompt/creation intent, child leases and execution, original name/task/parent/root ancestry, and idempotent terminal notification acceptance. Initial metadata includes direct `parent_run_id`, not only the root. `spawn`, `aget`, `wait`, `send`, `resume`, `stop`, `stop_all`, `close` are async. `get` reads cached children only. `send` accepts a durable next prompt in FIFO order, including when the current child is active; it does not mutate the active run's input. Reading/reopening a child alone does not execute it.

Terminal notifications are `kind="child_result"` SDK inbox entries on the parent session. They contain child session/run/source-command/root/direct-parent/completion-event references, not a duplicate output journal. `store.list_notifications(tenant, parent_session, state="accepted", after_seq=0, limit=100)` supports restart polling. A callback runs only after notification acceptance is durable. After the product durably accepts its own reply request, acknowledge via `manager.ack_notification` or `store.ack_notification`. Acknowledgement changes notification control state only. Repeated delivery before acknowledgement must be deduplicated by the business request ID.

Child completion and parent notification acceptance are separate transactions. `recover` reconstructs missing notifications from committed child terminal commands and resends unacknowledged notifications, closing that crash gap by retry rather than claiming an impossible cross-transaction atomic acknowledgement. Keep the manager alive after the parent's foreground run ends, and invoke recovery after service restart. `recover(resume=True)` only auto-restarts accepted/crash-interrupted work; failed, stopped and unknown-effect work requires explicit handling. A busy child stays in an SDK-owned lease-aware wait/retry loop until another owner completes it or its lease can be acquired, rather than requiring a later unrelated request. Safe notification acceptance/callback failures retry with capped backoff in tracked delivery tasks. Callbacks must deduplicate by notification ID; a successful business acceptance followed by a failed acknowledgement can be delivered again. `close` cancels and settles execution, lease waiters and notification retries for shutdown, leaving durable work recoverable. Explicit stop also terminates that child's ownership waiter.

The original `SubagentManager` remains available for local scheduling. Its session and agent factories now also accept async factories; use `await aget()` for DB-backed discovery.

## Guarantees and boundaries

Fencing protects local commits and checks ownership immediately before SDK-managed model/tool dispatch. It cannot revoke a request already received by a provider. Exactly-once external effects are not promised. Existing `tool_started`/raw `tool_returned`/final `tool_completed` boundaries remain authoritative. Unknown outcomes require provider lookup, idempotency support or explicit human reconciliation; timeouts are not proof that an effect did not happen. `run_tool_call` nested calls remain a low-level host adapter boundary, not a separate durable operation ledger. Provider-specific operation idempotency/reconciliation and external action adapters remain host integrations.

No token replay, distributed autonomous scheduler, authentication/RLS, blob retention policy, automatic backup, PITR, or production RPO/RTO commitment is implied. Run deployment restore drills and PostgreSQL integration checks before production use.

## Bounded verification performed

`PYTHONPATH=src .venv/bin/python tests/test_durable_sqlite.py -v` exercises only these new local control contracts with a controlled offline model adapter. It covers idempotency, NUL payload preservation, reopen/snapshot/fork, same-run interruption recovery, stale/unbound fencing, durable pre-start and cross-worker in-flight cancellation, explicit additive migration, notification/FIFO isolation, completion-before-notification recovery, stale-child-lease takeover, safe notification retry and waiter shutdown. The checks also cover direct and wrapped ownership-loss errors: they propagate as `ExecutionLeaseError`, without turning the logical command into a failed result. This is not a live-model run or a broad regression suite.

## Optional durable approvals

`agent_runtime.execution.DurableApprovals(store)` reuses immutable `agent_commands`
facts for requests, decisions and single-operation bindings; it adds no tables or
background tasks. Call `request(tenant, session, approval_id=..., user_id=...,
run_id=..., action={"tool": ..., "arguments": ...}, context=...)` with a stable ID.
The exact action is frozen and hashed. `decide(tenant, approval_id,
actor_id=..., decision="approved" | "rejected")` records the owner's immutable
choice. Repeating the same choice succeeds; a conflicting choice is rejected.

The host polls `get(tenant, approval_id)` and accepts a new continuation into its
business FIFO, deduplicating by `decision_id`. It does not keep the original run
or business lane open while waiting. Immediately before the action, call
`authorize(tenant, approval_id, user_id=..., action=..., operation_id=...)`.
An approved action binds to one stable logical effect operation. Its replay is
allowed; different action arguments or operation IDs are rejected. The host still
checks current access and uses its normal idempotent/durable tool path. Approval
alone never means an effect occurred.

`tests/test_approvals.py` verifies owner matching, pending rejection, durable
reopen, exact-action binding, operation replay and conflicting decisions. This
helper is optional; the Agent loop and LocalSession do not depend on it.
