# Durable local and deployed execution

`DatabaseSession` remains the only canonical execution journal and reducer. PostgreSQL uses the same seven session, journal, archive, checkpoint, run, audit and model-usage tables. The additional `agent_commands` and `agent_session_leases` tables are SDK-owned acceptance/ownership control, not another run or output journal.

Complete semantic outputs are durable before their corresponding completion events are emitted. Text/thinking/tool-call deltas and tool progress are ephemeral; reopening does not replay token animation. A previously committed model/tool result remains readable even while the enclosing run is unfinished.

## PostgreSQL deployment

```python
from agent_runtime.storage import DatabaseConfig
from agent_runtime.storage.postgres import PostgresStore

store = await PostgresStore.connect(DatabaseConfig.from_env())
# Fresh deployment only: await store.install_schema()
# Existing installation through 003: await store.install_coordination_schema()
# Explicit deployment upgrade (repeatable, including 005 indexes/run routing):
# await store.upgrade_schema()
await store.check_schema()  # requires canonical tables plus SDK coordination
```

Fresh explicit install applies 001 through 005. Existing deployments call `upgrade_schema()` explicitly during serialized deployment; repeating it is safe. The upgrade backfills run routing keys in bounded pages in Python because semantic JSON may contain escaped NUL. `connect` and `check_schema` never migrate. Once a session has had a lease, all future writes require a current lease, including after release or expiration. A released lease row is retained so old unbound writers cannot bypass fencing.

Local integration is exercised against a real PostgreSQL server. Tests create disposable schemas and run the packaged DDL; application tables are not reset.

## Acceptance, FIFO and recovery

- `execute` commits the stable prompt command before execution. Reusing a command ID with different immutable input, session or explicit run ID is rejected. The prompt's run ID is allocated once and attached atomically to canonical `run_started`.
- `execute` only executes its named command. It returns `busy` if another worker holds the lease or an older prompt is ahead in the session FIFO. It never runs an unrelated request through the current factory closure.
- `execute_command` runs a previously accepted named prompt. `run_next` is for hosts whose factory is valid for every pending prompt in that session. Only `kind="prompt"` is executable; notification commands never block prompt FIFO.
- The executor owns lease acquisition, local commit fencing, pre-dispatch ownership/cancel checks, and release. A shared `ExecutionHost` renews every active lease in one batch, by default with a 30-second TTL and 10-second renewal interval. It also reads cancellation intents in a batch; no per-command one-second heartbeat is created.
- Repeating the same command after crash interruption opens the canonical state and resumes its original run. A pending run belonging to a different command returns `recovery_required`; it is never hijacked by new input.
- Failed, stopped and unknown-tool work is not automatically retried for a new command. `resume` is an explicit retry of the original command and still enforces tool recovery rules and original model/tools/hooks. Cancelled work requires explicit abandonment and a new prompt.
- `request_cancel(tenant_id, command_id)` durably records cross-worker abort intent. The heartbeat and pre-dispatch boundaries observe it. A canceled unstarted command is rejected durably without starting a model; active cancellation is cooperative. `cancel` is only the immediate local-process shortcut.
- `abandon(..., command_id=...)` explicitly clears that command's pending execution, retaining canonical history and earlier run facts. A currently interrupted/running run gets a stopped journal fact first. A failed run remains failed in the run projection. Structurally unfinished assistant/tool batches are excluded from the next model context, while their complete committed records remain in history. Abandoning never undoes or reconciles real-world effects.

Factories may be synchronous or asynchronous and must return a fresh Agent using exactly the supplied session. Re-inject credentials, tools, policy hooks, provider configuration and any external references on recovery. Construct agents promptly; do not perform external effects in the factory. No global/background scheduler is installed by constructing an executor.

## Shared worker host and notifications

```python
from agent_runtime.execution import DurableExecutor, ExecutionHost

host = ExecutionHost(store, max_active=8, max_active_children=8, publish=publish_hint)
executor = DurableExecutor(store, agent_factory, host=host)
await executor.execute_command(tenant, session_id, user_id=user, command_id=request_id)
# A Redis/other transport subscriber calls this synchronous local hint:
host.notify(tenant, session_id)
# This persists the intent before publishing the hint:
await host.request_cancel(tenant, request_id)
```

`publish_hint(tenant, session_id)` is an optional async host callback. Redis is not an SDK dependency and holds no authoritative command/lease state. Duplicate or lost hints cannot grant ownership. The host uses a batch control read on renewal as a fallback for cancellation; model/tool dispatch boundaries also read the durable intent. A notification wakes local cancellation and child waiters immediately. Transport failures do not retract a command already committed to PostgreSQL.

Root and child capacity are separate and bounded, so waiting roots do not consume every child execution slot. A durable child cannot spawn grandchildren: the supported maximum depth is one. Managers cap submitted children per parent run at 32 by default, using the indexed parent run lineage. Completing a child does not restore that run's submission budget; a new run in the same session receives a fresh budget. Historical children do not reduce it. Keep one shared host per worker capacity class; creating a host per request defeats batch renewal and process limits. Call `await host.close()` on shutdown after settling service execution tasks.

The application owns wake routing and centralized compensation. Once per recovery interval, page `host.recovery_candidates(limit=100, after=cursor)` and resolve the immutable command's original factory/configuration before execution. Its ascending cursor is `(updated_at, tenant_id, command_id)`. It returns only unowned/expired pending prompt hints, including children after their parent ends, and excludes runs paused for unknown effects. It does not acquire leases or execute work. Use `child=True` for recovered child execution. No worker should scan every session or replay journal history to find work. Acceptance into an application business outbox is a separate durable receipt; only idempotent SDK acceptance means the runtime command exists.

## Read APIs

Both stores provide:

- `list_sessions(tenant, user_id=None, limit=100, after=None)`: descending `(created_at, session_id)` cursor.
- `list_runs(tenant, session_id=None, user_id=None, root_run_id=None, status=None, start=None, end=None, limit=100, after=None)`: descending `(started_at, session_id, run_id)` cursor; UTC-aware `[start,end)` filters.
- `read_records(tenant, session_id, after_seq=-1)` and `read_run_records(tenant, session_id, run_id, after_seq=-1, limit=1000)`: canonical semantic records, including archived history when an archive reader is configured. The run API uses a PostgreSQL run/sequence index and SQL limit. New archive manifests identify their contained runs; legacy manifests may require reading older chunks to preserve complete history.
- `read_recovery_records`, `DatabaseSession.open`, `asnapshot_at(event_id)` and `fork(event_id)` reuse the existing reducer/checkpoint implementation. Reading/forking does not execute tools or reverse past side effects; forked historical imports do not re-charge usage or re-project old runs.
- Existing `daily_usage`, `daily_runs`, `daily_activity`, `audit_history` and `audit` APIs. Unknown usage remains unknown/NULL, never zero. Actual host access audit still requires explicit instrumentation.

Use these canonical facts for activity/detail projections. The SDK does not authenticate tenant/user parameters or implement business ACLs. Verify access before calling it and before dereferencing any tool result or attachment.

## Durable child agents and result handoff

```python
from agent_runtime.subagents import DurableSubagentManager, create_subagent_tools

manager = DurableSubagentManager(
    parent_agent,
    agent_factories={"research": child_agent_factory}, # async factories supported
    host=host,
)
parent_agent.state.tools.extend(create_subagent_tools(manager))

async def child_finished(notification):
    await business_fifo.accept_child_result(notification) # stable notification ID dedupe
    await manager.ack_notification(notification["command_id"])

manager.subscribe_completion(child_finished) # optional immediate delivery
# The centralized dispatcher also handles pending results after process death:
for notice in await store.pending_child_results(limit=100):
    await business_fifo.accept_child_result(notice) # dedupe by notice["command_id"]
    await store.ack_notification(notice["tenant_id"], notice["session_id"], notice["command_id"])
```

`create_subagent_tools` is compatible with the durable manager; install/merge tools using the host's ordinary Agent configuration API. The manager owns deterministic child session IDs, durable initial prompt/creation intent, child leases and execution, original name/task/parent/root ancestry, and idempotent terminal notification acceptance. Initial metadata includes direct `parent_run_id`, not only the root. `spawn`, `aget`, `wait`, `send`, `resume`, `stop`, `stop_all`, `close` are async. `get` reads cached children only. `send` accepts a durable next prompt in FIFO order, including when the current child is active; it does not mutate the active run's input. Reading/reopening a child alone does not execute it.

Terminal notifications are `kind="child_result"` SDK inbox entries on the parent session, accepted in the same PostgreSQL transaction as the child's terminal event and command projection. They contain child session/run/source-command/root/direct-parent/completion-event references. `store.pending_child_results(limit=100, after=cursor)` reads a global page with ascending `(accepted_at, tenant_id, command_id)` cursor, without opening parents or children. Per-parent `list_notifications` remains available. After durably accepting the business continuation, acknowledge via `manager.ack_notification` or `store.ack_notification`. Acknowledgement changes notification control state only. Repeated delivery must be deduplicated by notification command ID.

There is no child-completion-to-parent-inbox transaction gap. Business acceptance and SDK acknowledgement remain separate, so business consumers must deduplicate. Optional callbacks are attempted once; failures leave the accepted notification for the centralized dispatcher. No per-child delivery retry task is installed. `recover` remains an explicit per-parent inspection/recovery API; production startup can use the indexed global candidate and notification APIs instead. `recover(resume=True)` only starts accepted/crash-interrupted work, preserving unknown-effect recovery rules. A busy child waits for a notification or the PostgreSQL lease expiry deadline, without a one-second ownership poll. `close` settles local execution/wait tasks and leaves durable work recoverable.

The original `SubagentManager` remains available for local scheduling. Its session and agent factories now also accept async factories; use `await aget()` for DB-backed discovery.

## Guarantees and boundaries

Fencing protects local commits and checks ownership immediately before SDK-managed model/tool dispatch. It cannot revoke a request already received by a provider. Exactly-once external effects are not promised. Existing `tool_started`/raw `tool_returned`/final `tool_completed` boundaries remain authoritative. Unknown outcomes require provider lookup, idempotency support or explicit human reconciliation; timeouts are not proof that an effect did not happen. `run_tool_call` nested calls remain a low-level host adapter boundary, not a separate durable operation ledger. Provider-specific operation idempotency/reconciliation and external action adapters remain host integrations.

No token replay, distributed autonomous scheduler, authentication/RLS, blob retention policy, automatic backup, PITR, or production RPO/RTO commitment is implied. Run deployment restore drills and PostgreSQL integration checks before production use.

## Verification

Install `agent-runtime[postgres,s3]` and set `AGENT_TEST_DB_URL` to a PostgreSQL
connection URL. Run `python -m unittest discover -s tests -p 'test_*.py' -v`.
`test_database.py`, `test_durable_postgres.py` and `test_approvals.py` use real
PostgreSQL with disposable schemas. They cover journal/archive recovery,
checkpoints, snapshots/forks, same-run resume, stale-writer fencing, prompt FIFO,
cooperative cancellation, durable child handoff, and exact-action approval.
Model adapters and S3 clients in these checks are controlled test implementations.

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
