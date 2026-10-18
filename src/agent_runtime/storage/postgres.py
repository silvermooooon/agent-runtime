"""PostgreSQL journal, retained facts, and optional durable execution control."""

import json
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from types import SimpleNamespace
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Json, Jsonb
from psycopg_pool import AsyncConnectionPool

from ..sessions.base import SessionError
from .audit import project
from .coordination import (
    COMMAND_STATES,
    CommandConflictError,
    CoordinationError,
    ExecutionLease,
    ExecutionLeaseError,
    canonical_payload,
    command_transition,
    validate_identifier,
    validate_limit,
    validate_ttl,
)


async def _coordination_installed(conn):
    row = await (
        await conn.execute("SELECT to_regclass('agent_session_leases') AS relation")
    ).fetchone()
    return row["relation"] is not None


async def _assert_execution_row(conn, lease):
    row = await (
        await conn.execute(
            "SELECT *,expires_at>clock_timestamp() AS live FROM agent_session_leases "
            "WHERE tenant_id=%s AND session_id=%s FOR UPDATE",
            (lease.tenant_id, lease.session_id),
        )
    ).fetchone()
    if (
        row is None
        or not row["live"]
        or row["holder"] != lease.holder
        or row["fence"] != lease.fence
    ):
        raise ExecutionLeaseError("Execution lease expired, was released, or was superseded")
    return row


async def guard_session_write(conn, session):
    """Serialize against takeover and fence every coordinated session append.

    The session row is the common lock even before a lease row exists. A
    retained, expired lease still requires fencing: expiry never authorizes an
    unbound old writer. Deployments without 004 retain legacy append behavior.
    """
    tenant, sid = session.info.tenant_id, session.session_id
    identity = await (
        await conn.execute(
            "SELECT user_id FROM agent_sessions WHERE tenant_id=%s AND session_id=%s FOR UPDATE",
            (tenant, sid),
        )
    ).fetchone()
    if (
        identity is not None
        and identity["user_id"] is not None
        and session.info.user_id is not None
        and identity["user_id"] != session.info.user_id
    ):
        raise CoordinationError("Session user does not match its durable ownership")
    lease = getattr(session, "execution_lease", None)
    if not await _coordination_installed(conn):
        if lease is not None:
            raise ExecutionLeaseError(
                "Install the explicit coordination schema before binding a lease"
            )
        return
    if lease is not None:
        if not isinstance(lease, ExecutionLease) or (lease.tenant_id, lease.session_id) != (
            tenant,
            sid,
        ):
            raise ExecutionLeaseError("Execution lease does not belong to this session")
        await _assert_execution_row(conn, lease)
    else:
        row = await (
            await conn.execute(
                "SELECT fence FROM agent_session_leases WHERE tenant_id=%s AND session_id=%s",
                (tenant, sid),
            )
        ).fetchone()
        if row is not None:
            raise ExecutionLeaseError("This session requires a valid execution lease")


async def coordination_project(conn, session, record):
    """Project command ownership in the SAME transaction as its canonical event."""
    transition = command_transition(record)
    if transition is None:
        return
    command_id, state, run_id = transition
    if not await _coordination_installed(conn):
        if command_id:
            raise CoordinationError(
                "Install the explicit coordination schema before applying commands"
            )
        return
    tenant, sid = session.info.tenant_id, session.session_id
    if command_id:
        command = await (
            await conn.execute(
                "SELECT * FROM agent_commands WHERE tenant_id=%s AND command_id=%s FOR UPDATE",
                (tenant, command_id),
            )
        ).fetchone()
        if command is None or command["session_id"] != sid:
            raise CoordinationError("Command does not belong to this session")
    elif run_id:
        command = await (
            await conn.execute(
                "SELECT * FROM agent_commands WHERE tenant_id=%s AND session_id=%s "
                "AND run_id=%s FOR UPDATE",
                (tenant, sid, run_id),
            )
        ).fetchone()
        if command is None:
            return
        command_id = command["command_id"]
    else:
        return
    if getattr(session, "execution_lease", None) is None:
        raise ExecutionLeaseError("Applying durable commands requires an execution lease")
    if run_id is not None and command["run_id"] != run_id:
        raise CommandConflictError("Command is bound to a different canonical run")
    kind = record["type"]
    if kind == "run_started":
        if command["kind"] != "prompt" or command["state"] != "accepted":
            raise CoordinationError("Only an accepted prompt command can start a new run")
        if command["cancel_requested_at"] is not None:
            raise CoordinationError("Cancelled command must be rejected before another run starts")
        first = await (
            await conn.execute(
                "SELECT command_id FROM agent_commands WHERE tenant_id=%s AND session_id=%s "
                "AND kind='prompt' AND state IN ('accepted','applied') ORDER BY command_seq "
                "LIMIT 1",
                (tenant, sid),
            )
        ).fetchone()
        if first is None or first["command_id"] != command_id:
            raise CoordinationError("An earlier command must finish before this command starts")
    elif kind == "run_resumed":
        if command["state"] not in ("applied", "failed", "stopped"):
            raise CoordinationError("This command's run cannot be resumed")
        active = await (
            await conn.execute(
                "SELECT command_id FROM agent_commands WHERE tenant_id=%s AND session_id=%s "
                "AND state='applied' AND command_id<>%s LIMIT 1",
                (tenant, sid, command_id),
            )
        ).fetchone()
        if active is not None:
            raise CoordinationError("Another command already owns the active run")
    elif kind == "command_rejected":
        if command["state"] != "accepted":
            raise CoordinationError("Only an unapplied command can be rejected")
    elif kind == "history_reset":
        if command["state"] not in ("accepted", "applied", "failed", "stopped"):
            raise CoordinationError("This command cannot be abandoned")
    elif command["state"] != "applied":
        raise CoordinationError("A terminal event requires its applied command")
    if state == "applied":
        await conn.execute(
            "UPDATE agent_commands SET state='applied',applied_journal_seq="
            "COALESCE(applied_journal_seq,%s),applied_event_id=COALESCE(applied_event_id,%s),"
            "completed_journal_seq=NULL,completed_event_id=NULL,updated_at=clock_timestamp() "
            "WHERE tenant_id=%s AND command_id=%s",
            (record["seq"], record["id"], tenant, command_id),
        )
    else:
        await conn.execute(
            "UPDATE agent_commands SET state=%s,completed_journal_seq=%s,completed_event_id=%s,"
            "updated_at=clock_timestamp() WHERE tenant_id=%s AND command_id=%s",
            (state, record["seq"], record["id"], tenant, command_id),
        )


class PostgresStore:
    def __init__(self, pool, *, archive=None):
        self.pool = pool
        self.archive = archive

    @classmethod
    async def connect(cls, config, *, archive=None):
        pool = AsyncConnectionPool(
            config.url,
            min_size=config.pool_min_size,
            max_size=config.pool_max_size,
            open=False,
            kwargs={
                "row_factory": dict_row,
                "connect_timeout": config.connect_timeout_seconds,
                "options": f"-c statement_timeout={config.statement_timeout_seconds * 1000}",
            },
        )
        try:
            await pool.open(wait=True, timeout=config.connect_timeout_seconds)
        except BaseException:
            await pool.close()
            raise
        return cls(pool, archive=archive)

    async def close(self):
        await self.pool.close()

    async def install_schema(self):
        """Explicit deployment action; not called by connect/open. Fails if already installed."""
        async with self.pool.connection() as conn:
            for name in (
                "001_sessions.sql",
                "002_checkpoints.sql",
                "003_session_identity.sql",
                "004_coordination.sql",
            ):
                ddl = files("agent_runtime.storage").joinpath(f"sql/{name}").read_text()
                await conn.execute(ddl)

    async def install_coordination_schema(self):
        """Explicit, additive one-time upgrade of an existing version-3 schema."""
        ddl = files("agent_runtime.storage").joinpath("sql/004_coordination.sql").read_text()
        async with self.pool.connection() as conn:
            await conn.execute(ddl)

    async def check_schema(self, *, require_coordination=True):
        """Verify expected structure without installing or changing anything."""
        expected = {
            "agent_sessions": {
                "tenant_id",
                "session_id",
                "user_id",
                "created_at",
                "updated_at",
                "next_seq",
                "metadata",
            },
            "agent_session_events": {
                "tenant_id",
                "session_id",
                "seq",
                "event_id",
                "record",
                "metadata",
            },
            "agent_session_archives": {
                "tenant_id",
                "session_id",
                "start_seq",
                "end_seq",
                "object_uri",
                "version_id",
                "sha256",
                "format_version",
                "created_at",
                "metadata",
            },
            "agent_runs": {
                "tenant_id",
                "session_id",
                "run_id",
                "user_id",
                "root_run_id",
                "source",
                "started_at",
                "finished_at",
                "status",
                "metadata",
            },
            "agent_audit_events": {
                "tenant_id",
                "audit_id",
                "actor_id",
                "actor_type",
                "user_id",
                "session_id",
                "run_id",
                "occurred_at",
                "action",
                "resource_type",
                "resource_id",
                "outcome",
                "metadata",
            },
            "agent_model_usage": {
                "tenant_id",
                "attempt_id",
                "session_id",
                "run_id",
                "root_run_id",
                "user_id",
                "provider",
                "model",
                "purpose",
                "started_at",
                "finished_at",
                "status",
                "input_tokens",
                "output_tokens",
                "cache_read_tokens",
                "cache_write_tokens",
                "total_tokens",
                "usage_status",
                "metadata",
            },
            "agent_session_checkpoints": {"tenant_id", "session_id", "seq", "event_id"},
        }
        if require_coordination:
            expected.update(
                {
                    "agent_commands": {
                        "tenant_id",
                        "command_id",
                        "session_id",
                        "command_seq",
                        "kind",
                        "payload",
                        "payload_hash",
                        "run_id",
                        "state",
                        "accepted_at",
                        "applied_journal_seq",
                        "applied_event_id",
                        "completed_journal_seq",
                        "completed_event_id",
                        "updated_at",
                        "cancel_requested_at",
                        "parent_session_id",
                    },
                    "agent_session_leases": {
                        "tenant_id",
                        "session_id",
                        "holder",
                        "fence",
                        "expires_at",
                    },
                }
            )
        async with self.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT c.relname AS table_name,a.attname AS column_name FROM pg_class c "
                    "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
                    "WHERE c.oid=ANY(ARRAY(SELECT to_regclass(name) FROM unnest(%s::text[]) name))",
                    (list(expected),),
                )
            ).fetchall()
        actual = {}
        for row in rows:
            actual.setdefault(row["table_name"], set()).add(row["column_name"])
        missing = [
            f"{name}({','.join(sorted(columns - actual.get(name, set())))})"
            for name, columns in expected.items()
            if columns - actual.get(name, set())
        ]
        if missing:
            raise SessionError(
                "Runtime schema is missing required structure: "
                + ", ".join(missing)
                + ". Apply the explicit SDK SQL scripts through 004_coordination.sql"
                + (
                    "."
                    if require_coordination
                    else " (001 through 003 suffice for legacy journals)."
                )
            )
        return {
            "ready": True,
            "version": 4 if require_coordination else 3,
            "backend": "postgres",
            "coordination": require_coordination,
        }

    async def _ensure(self, conn, info):
        await conn.execute(
            "INSERT INTO agent_sessions (tenant_id,session_id,user_id) VALUES (%s,%s,%s) "
            "ON CONFLICT (tenant_id,session_id) DO UPDATE SET user_id=EXCLUDED.user_id "
            "WHERE agent_sessions.user_id IS NULL AND EXCLUDED.user_id IS NOT NULL",
            (info.tenant_id, info.session_id, info.user_id),
        )

    async def append(self, session, record):
        async with self.pool.connection() as conn:
            await self._ensure(conn, session.info)
            await guard_session_write(conn, session)
            await self._insert_event(conn, session.info.tenant_id, session.session_id, record)
            updated = await conn.execute(
                "UPDATE agent_sessions SET next_seq=%s,updated_at=clock_timestamp() "
                "WHERE tenant_id=%s AND session_id=%s AND next_seq=%s",
                (record["seq"] + 1, session.info.tenant_id, session.session_id, record["seq"]),
            )
            if updated.rowcount != 1:
                raise SessionError("Stale session; reopen after the previous writer exits")
            await project(conn, session, record)
            await coordination_project(conn, session, record)
            lease = getattr(session, "execution_lease", None)
            if lease is not None:
                # A long projection must not commit after its lease elapsed.
                await _assert_execution_row(conn, lease)

    async def accept_command(
        self, tenant_id, session_id, *, command_id, kind, payload, user_id=None, run_id=None
    ):
        """Commit an idempotent command before acknowledging acceptance.

        The session lock assigns commit-ordered FIFO sequence numbers. Unknown
        command kinds can be retained but are never automatically executed.
        A prompt's canonical run ID is allocated exactly once at acceptance.
        """
        for name, value in (
            ("tenant_id", tenant_id),
            ("session_id", session_id),
            ("command_id", command_id),
            ("kind", kind),
        ):
            validate_identifier(value, name)
        if user_id is not None:
            validate_identifier(user_id, "user_id")
        if run_id is not None:
            validate_identifier(run_id, "run_id")
        normalized, digest = canonical_payload(payload)
        parent_session_id = None
        subagent = normalized.get("subagent")
        if subagent is not None:
            if not isinstance(subagent, dict):
                raise ValueError("subagent metadata must be a JSON object")
            parent_session_id = subagent.get("parent_session_id")
            if parent_session_id is not None:
                validate_identifier(parent_session_id, "subagent.parent_session_id")
        proposed_run_id = run_id or (uuid4().hex if kind == "prompt" else None)
        async with self.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO agent_sessions (tenant_id,session_id,user_id) VALUES (%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (tenant_id, session_id, user_id),
            )
            identity = await (
                await conn.execute(
                    "SELECT user_id FROM agent_sessions WHERE tenant_id=%s AND session_id=%s "
                    "FOR UPDATE",
                    (tenant_id, session_id),
                )
            ).fetchone()
            if user_id is not None and identity["user_id"] not in (None, user_id):
                raise CommandConflictError("Command user does not own this session")
            if user_id is not None and identity["user_id"] is None:
                await conn.execute(
                    "UPDATE agent_sessions SET user_id=%s WHERE tenant_id=%s AND session_id=%s",
                    (user_id, tenant_id, session_id),
                )
            command = await (
                await conn.execute(
                    "SELECT * FROM agent_commands WHERE tenant_id=%s AND command_id=%s",
                    (tenant_id, command_id),
                )
            ).fetchone()
            if command is None:
                if proposed_run_id is not None:
                    bound = await (
                        await conn.execute(
                            "SELECT command_id FROM agent_commands WHERE tenant_id=%s AND "
                            "session_id=%s "
                            "AND run_id=%s",
                            (tenant_id, session_id, proposed_run_id),
                        )
                    ).fetchone()
                    if bound is not None:
                        raise CommandConflictError(
                            "Canonical run is already bound to another command"
                        )
                    existing_run = await (
                        await conn.execute(
                            "SELECT run_id FROM agent_runs WHERE tenant_id=%s AND session_id=%s "
                            "AND run_id=%s",
                            (tenant_id, session_id, proposed_run_id),
                        )
                    ).fetchone()
                    if existing_run is not None:
                        raise CommandConflictError(
                            "Canonical run ID already exists; resume its original command"
                        )
                command = await (
                    await conn.execute(
                        "INSERT INTO agent_commands "
                        "(tenant_id,command_id,session_id,command_seq,kind,payload,payload_hash,"
                        "parent_session_id,run_id) "
                        "SELECT %s,%s,%s,COALESCE(MAX(command_seq),0)+1,%s,%s,%s,%s,%s "
                        "FROM agent_commands WHERE tenant_id=%s AND session_id=%s "
                        "ON CONFLICT (tenant_id,command_id) DO NOTHING RETURNING *",
                        (
                            tenant_id,
                            command_id,
                            session_id,
                            kind,
                            Json(normalized),
                            digest,
                            parent_session_id,
                            proposed_run_id,
                            tenant_id,
                            session_id,
                        ),
                    )
                ).fetchone()
                if command is None:
                    command = await (
                        await conn.execute(
                            "SELECT * FROM agent_commands WHERE tenant_id=%s AND command_id=%s",
                            (tenant_id, command_id),
                        )
                    ).fetchone()
            if (
                command["session_id"] != session_id
                or command["kind"] != kind
                or command["payload_hash"] != digest
                or (run_id is not None and command["run_id"] != run_id)
            ):
                raise CommandConflictError("command_id was already accepted with different content")
            return dict(command)

    async def list_commands(
        self, tenant_id, session_id, *, state=None, kind=None, after_seq=0, limit=100
    ):
        validate_limit(limit)
        if state is not None and state not in COMMAND_STATES:
            raise ValueError("Unknown command state")
        if isinstance(after_seq, bool) or not isinstance(after_seq, int) or after_seq < 0:
            raise ValueError("after_seq must be a non-negative integer")
        sql = "SELECT * FROM agent_commands WHERE tenant_id=%s AND session_id=%s AND command_seq>%s"
        params = [tenant_id, session_id, after_seq]
        if state is not None:
            sql += " AND state=%s"
            params.append(state)
        if kind is not None:
            validate_identifier(kind, "kind")
            sql += " AND kind=%s"
            params.append(kind)
        sql += " ORDER BY command_seq LIMIT %s"
        params.append(limit)
        async with self.pool.connection() as conn:
            return await (await conn.execute(sql, params)).fetchall()

    async def list_notifications(
        self, tenant_id, session_id, *, state=None, after_seq=0, limit=100
    ):
        """Read durable child-result references without claiming or injecting them."""
        return await self.list_commands(
            tenant_id,
            session_id,
            kind="child_result",
            state=state,
            after_seq=after_seq,
            limit=limit,
        )

    async def ack_notification(self, tenant_id, session_id, command_id):
        """Acknowledge product acceptance of a child-result notification.

        This changes only inbox control state. It does not create a run or
        invent a canonical journal event/result or completion pointer.
        """
        validate_identifier(tenant_id, "tenant_id")
        validate_identifier(session_id, "session_id")
        validate_identifier(command_id, "command_id")
        async with self.pool.connection() as conn:
            await conn.execute(
                "SELECT session_id FROM agent_sessions WHERE tenant_id=%s AND session_id=%s "
                "FOR UPDATE",
                (tenant_id, session_id),
            )
            command = await (
                await conn.execute(
                    "SELECT * FROM agent_commands WHERE tenant_id=%s AND session_id=%s "
                    "AND command_id=%s FOR UPDATE",
                    (tenant_id, session_id, command_id),
                )
            ).fetchone()
            if command is None or command["kind"] != "child_result":
                raise CommandConflictError("Notification does not belong to this session or kind")
            if command["state"] == "completed":
                return dict(command)
            if command["state"] != "accepted":
                raise CoordinationError(
                    "Only an accepted child-result notification can be acknowledged"
                )
            return await (
                await conn.execute(
                    "UPDATE agent_commands SET state='completed',updated_at=clock_timestamp() "
                    "WHERE tenant_id=%s AND session_id=%s AND command_id=%s RETURNING *",
                    (tenant_id, session_id, command_id),
                )
            ).fetchone()

    async def list_child_commands(self, tenant_id, parent_session_id, *, limit=1000, after=None):
        """Discover child creation intents through their validated control projection."""
        validate_identifier(tenant_id, "tenant_id")
        validate_identifier(parent_session_id, "parent_session_id")
        validate_limit(limit)
        if after is not None:
            if not isinstance(after, (tuple, list)) or len(after) != 2:
                raise ValueError("after must be (session_id, command_seq)")
        sql = (
            "SELECT * FROM agent_commands WHERE tenant_id=%s AND kind='prompt' "
            "AND parent_session_id=%s"
        )
        params = [tenant_id, parent_session_id]
        if after is not None:
            sql += " AND (session_id,command_seq)>(%s,%s)"
            params.extend(after)
        sql += " ORDER BY session_id,command_seq LIMIT %s"
        params.append(limit)
        async with self.pool.connection() as conn:
            return await (await conn.execute(sql, params)).fetchall()

    async def get_command(self, tenant_id, command_id):
        """Look up a stable tenant-scoped command without scanning its session."""
        validate_identifier(tenant_id, "tenant_id")
        validate_identifier(command_id, "command_id")
        async with self.pool.connection() as conn:
            return await (
                await conn.execute(
                    "SELECT * FROM agent_commands WHERE tenant_id=%s AND command_id=%s",
                    (tenant_id, command_id),
                )
            ).fetchone()

    async def request_cancel(self, tenant_id, command_id):
        """Durably request cancellation for the executing worker to observe.

        This is a control intent, not proof that execution stopped. Existing
        terminal facts remain unchanged. An unknown command returns None.
        """
        validate_identifier(tenant_id, "tenant_id")
        validate_identifier(command_id, "command_id")
        async with self.pool.connection() as conn:
            command = await (
                await conn.execute(
                    "SELECT session_id FROM agent_commands WHERE tenant_id=%s AND command_id=%s",
                    (tenant_id, command_id),
                )
            ).fetchone()
            if command is None:
                return None
            await conn.execute(
                "SELECT session_id FROM agent_sessions WHERE tenant_id=%s AND session_id=%s "
                "FOR UPDATE",
                (tenant_id, command["session_id"]),
            )
            await conn.execute(
                "UPDATE agent_commands SET cancel_requested_at=clock_timestamp(),"
                "updated_at=clock_timestamp() WHERE tenant_id=%s AND command_id=%s "
                "AND state IN ('accepted','applied') AND cancel_requested_at IS NULL",
                (tenant_id, command_id),
            )
            return await (
                await conn.execute(
                    "SELECT * FROM agent_commands WHERE tenant_id=%s AND command_id=%s",
                    (tenant_id, command_id),
                )
            ).fetchone()

    async def get_execution(self, tenant_id, session_id):
        """Read current ownership/expiry without acquiring or extending it."""
        validate_identifier(tenant_id, "tenant_id")
        validate_identifier(session_id, "session_id")
        async with self.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM agent_session_leases WHERE tenant_id=%s AND session_id=%s",
                    (tenant_id, session_id),
                )
            ).fetchone()
            return self._lease(row) if row is not None else None

    async def acquire_execution(self, tenant_id, session_id, *, holder, ttl_seconds=30):
        """Acquire an expired/unowned session, advancing its fencing generation."""
        validate_identifier(tenant_id, "tenant_id")
        validate_identifier(session_id, "session_id")
        validate_identifier(holder, "holder")
        ttl = validate_ttl(ttl_seconds)
        async with self.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO agent_sessions (tenant_id,session_id) VALUES (%s,%s) ON CONFLICT "
                "DO NOTHING",
                (tenant_id, session_id),
            )
            await conn.execute(
                "SELECT session_id FROM agent_sessions WHERE tenant_id=%s AND session_id=%s "
                "FOR UPDATE",
                (tenant_id, session_id),
            )
            row = await (
                await conn.execute(
                    "INSERT INTO agent_session_leases "
                    "(tenant_id,session_id,holder,fence,expires_at) "
                    "VALUES (%s,%s,%s,1,clock_timestamp()+(%s * interval '1 second')) "
                    "ON CONFLICT (tenant_id,session_id) DO UPDATE SET holder=EXCLUDED.holder,"
                    "fence=agent_session_leases.fence+1,expires_at=EXCLUDED.expires_at "
                    "WHERE agent_session_leases.expires_at<=clock_timestamp() RETURNING *",
                    (tenant_id, session_id, holder, ttl),
                )
            ).fetchone()
            return self._lease(row) if row is not None else None

    @staticmethod
    def _lease(row):
        return ExecutionLease(
            row["tenant_id"], row["session_id"], row["holder"], row["fence"], row["expires_at"]
        )

    async def assert_execution(self, lease):
        if not isinstance(lease, ExecutionLease):
            raise ExecutionLeaseError("A valid ExecutionLease is required")
        async with self.pool.connection() as conn:
            await conn.execute(
                "SELECT session_id FROM agent_sessions WHERE tenant_id=%s AND session_id=%s "
                "FOR UPDATE",
                (lease.tenant_id, lease.session_id),
            )
            await _assert_execution_row(conn, lease)

    async def assert_session_execution(self, session):
        """Recheck ownership immediately before model/tool dispatch, including legacy writers."""
        async with self.pool.connection() as conn:
            await guard_session_write(conn, session)

    async def renew_execution(self, lease, *, ttl_seconds=30):
        ttl = validate_ttl(ttl_seconds)
        if not isinstance(lease, ExecutionLease):
            raise ExecutionLeaseError("A valid ExecutionLease is required")
        async with self.pool.connection() as conn:
            await conn.execute(
                "SELECT session_id FROM agent_sessions WHERE tenant_id=%s AND session_id=%s "
                "FOR UPDATE",
                (lease.tenant_id, lease.session_id),
            )
            await _assert_execution_row(conn, lease)
            row = await (
                await conn.execute(
                    "UPDATE agent_session_leases SET expires_at=clock_timestamp()+(%s * "
                    "interval '1 second') "
                    "WHERE tenant_id=%s AND session_id=%s AND holder=%s AND fence=%s "
                    "AND expires_at>clock_timestamp() RETURNING *",
                    (ttl, lease.tenant_id, lease.session_id, lease.holder, lease.fence),
                )
            ).fetchone()
            if row is None:
                raise ExecutionLeaseError("Execution lease expired before renewal")
            return self._lease(row)

    async def release_execution(self, lease):
        if not isinstance(lease, ExecutionLease):
            raise ExecutionLeaseError("A valid ExecutionLease is required")
        async with self.pool.connection() as conn:
            await conn.execute(
                "SELECT session_id FROM agent_sessions WHERE tenant_id=%s AND session_id=%s "
                "FOR UPDATE",
                (lease.tenant_id, lease.session_id),
            )
            # Keep the generation forever, preventing unfenced writes and ABA on reacquire.
            changed = await conn.execute(
                "UPDATE agent_session_leases SET expires_at=clock_timestamp() "
                "WHERE tenant_id=%s AND session_id=%s AND holder=%s AND fence=%s "
                "AND expires_at>clock_timestamp()",
                (lease.tenant_id, lease.session_id, lease.holder, lease.fence),
            )
            return changed.rowcount == 1

    async def _insert_event(self, conn, tenant, sid, record):
        await conn.execute(
            "INSERT INTO agent_session_events (tenant_id,session_id,seq,event_id,record) "
            "VALUES (%s,%s,%s,%s,%s)",
            (tenant, sid, record["seq"], record["id"], Json(record)),
        )
        if record["type"] == "compaction" and "checkpoint" in record:
            await conn.execute(
                "INSERT INTO agent_session_checkpoints (tenant_id,session_id,seq,event_id) "
                "VALUES (%s,%s,%s,%s)",
                (tenant, sid, record["seq"], record["id"]),
            )

    async def import_records(self, info, records):
        tenant, sid = info.tenant_id, info.session_id
        # Historical copies are not new access or usage and never project audit facts.
        async with self.pool.connection() as conn:
            await self._ensure(conn, info)
            await guard_session_write(
                conn, SimpleNamespace(info=info, session_id=sid, execution_lease=None)
            )
            updated = await conn.execute(
                "UPDATE agent_sessions SET next_seq=%s,updated_at=clock_timestamp() "
                "WHERE tenant_id=%s AND session_id=%s AND next_seq=0",
                (len(records), tenant, sid),
            )
            if updated.rowcount != 1:
                raise SessionError("Fork target must be empty")
            for record in records:
                await self._insert_event(conn, tenant, sid, record)

    async def list_sessions(self, tenant, *, user_id=None, limit=100, after=None):
        """Read canonical session projections with descending keyset pagination."""
        validate_limit(limit)
        sql, params = "SELECT * FROM agent_sessions WHERE tenant_id=%s", [tenant]
        if user_id is not None:
            sql += " AND user_id=%s"
            params.append(user_id)
        if after is not None:
            if not isinstance(after, (tuple, list)) or len(after) != 2:
                raise ValueError("after must be (created_at, session_id)")
            sql += " AND (created_at,session_id)<(%s,%s)"
            params.extend(after)
        sql += " ORDER BY created_at DESC,session_id DESC LIMIT %s"
        params.append(limit)
        async with self.pool.connection() as conn:
            return await (await conn.execute(sql, params)).fetchall()

    async def list_runs(
        self,
        tenant,
        *,
        session_id=None,
        user_id=None,
        root_run_id=None,
        status=None,
        start=None,
        end=None,
        limit=100,
        after=None,
    ):
        """Read existing run facts, bounded by [start, end) on started_at."""
        validate_limit(limit)
        sql, params = "SELECT * FROM agent_runs WHERE tenant_id=%s", [tenant]
        for column, value in (
            ("session_id", session_id),
            ("user_id", user_id),
            ("root_run_id", root_run_id),
            ("status", status),
        ):
            if value is not None:
                sql += f" AND {column}=%s"
                params.append(value)
        if start is not None:
            sql += " AND started_at>=%s"
            params.append(start)
        if end is not None:
            sql += " AND started_at<%s"
            params.append(end)
        if after is not None:
            if not isinstance(after, (tuple, list)) or len(after) != 3:
                raise ValueError("after must be (started_at, session_id, run_id)")
            sql += " AND (started_at,session_id,run_id)<(%s,%s,%s)"
            params.extend(after)
        sql += " ORDER BY started_at DESC,session_id DESC,run_id DESC LIMIT %s"
        params.append(limit)
        async with self.pool.connection() as conn:
            return await (await conn.execute(sql, params)).fetchall()

    async def read_run_records(self, tenant, sid, run_id, *, after_seq=-1, limit=1000):
        """Read canonical semantic run records, including any archived prefix."""
        validate_limit(limit)
        records = await self.read_records(tenant, sid, after_seq)
        return [record for record in records if record.get("run_id") == run_id][:limit]

    async def read_records(self, tenant, sid, after_seq=-1):
        return await self._read_records(tenant, sid, after_seq=after_seq)

    async def read_recovery_records(self, tenant, sid, *, event_id=None):
        """Read the nearest checkpoint and suffix, or legacy history without a checkpoint."""
        return await self._read_records(tenant, sid, recovery=True, event_id=event_id)

    async def _read_records(self, tenant, sid, *, after_seq=-1, recovery=False, event_id=None):
        async with self.pool.connection() as conn:
            await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            row = await (
                await conn.execute(
                    "SELECT next_seq FROM agent_sessions WHERE tenant_id=%s AND session_id=%s",
                    (tenant, sid),
                )
            ).fetchone()
            if row is None:
                if event_id is not None:
                    raise SessionError(f"Unknown event ID: {event_id}")
                return []
            end_seq = row["next_seq"] - 1
            if event_id is not None:
                target = await (
                    await conn.execute(
                        "SELECT seq FROM agent_session_events "
                        "WHERE tenant_id=%s AND session_id=%s AND event_id=%s "
                        "UNION ALL SELECT seq FROM agent_session_checkpoints "
                        "WHERE tenant_id=%s AND session_id=%s AND event_id=%s LIMIT 1",
                        (tenant, sid, event_id, tenant, sid, event_id),
                    )
                ).fetchone()
                if target is None:
                    # Ordinary archived event IDs have no retained per-event index.
                    # Resolve them by explicitly reading history after releasing this connection.
                    end_seq = None
                else:
                    end_seq = target["seq"]
            if end_seq is not None:
                if recovery:
                    checkpoint = await (
                        await conn.execute(
                            "SELECT seq FROM agent_session_checkpoints "
                            "WHERE tenant_id=%s AND session_id=%s AND seq<=%s "
                            "ORDER BY seq DESC LIMIT 1",
                            (tenant, sid, end_seq),
                        )
                    ).fetchone()
                    after_seq = checkpoint["seq"] - 1 if checkpoint else -1
                archives = await (
                    await conn.execute(
                        "SELECT * FROM agent_session_archives WHERE tenant_id=%s AND session_id=%s "
                        "AND end_seq>%s AND start_seq<=%s ORDER BY start_seq",
                        (tenant, sid, after_seq, end_seq),
                    )
                ).fetchall()
                hot = await (
                    await conn.execute(
                        "SELECT record FROM agent_session_events "
                        "WHERE tenant_id=%s AND session_id=%s AND seq>%s AND seq<=%s ORDER BY seq",
                        (tenant, sid, after_seq, end_seq),
                    )
                ).fetchall()
        if end_seq is None:
            from ..sessions.base import Session

            prefix = Session._select_prefix(await self.read_records(tenant, sid), event_id)
            start = next(
                (i for i in range(len(prefix) - 1, -1, -1) if "checkpoint" in prefix[i]), 0
            )
            return prefix[start:]
        records = []
        for manifest in archives:
            if self.archive is None:
                raise SessionError("S3 archive reader is required for this session")
            records.extend(
                r for r in await self.archive.get(manifest) if after_seq < r["seq"] <= end_seq
            )
        records.extend(r["record"] for r in hot)
        records.sort(key=lambda r: r["seq"])
        if [r["seq"] for r in records] != list(range(after_seq + 1, end_seq + 1)):
            raise SessionError("Missing or overlapping journal events")
        return records

    async def archive_candidates(self, tenant, *, limit=100):
        if self.archive is None or not self.archive.config.enabled:
            raise ValueError("Archive uploads are not enabled")
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.archive.config.after_days)
        async with self.pool.connection() as conn:
            return await (
                await conn.execute(
                    "SELECT session_id,updated_at FROM agent_sessions s "
                    "WHERE tenant_id=%s AND updated_at<%s AND EXISTS "
                    "(SELECT 1 FROM agent_session_events e WHERE e.tenant_id=s.tenant_id "
                    "AND e.session_id=s.session_id) ORDER BY updated_at LIMIT %s",
                    (tenant, cutoff, limit),
                )
            ).fetchall()

    async def archive_session(self, tenant, sid):
        """Host must exclude writers and other archivers for this session until return."""
        if self.archive is None or not self.archive.config.enabled:
            raise ValueError("Archive uploads are not enabled")
        config = self.archive.config
        cutoff = datetime.now(timezone.utc) - timedelta(days=config.after_days)
        count = 0
        while True:
            async with self.pool.connection() as conn:
                row = await (
                    await conn.execute(
                        "SELECT next_seq,updated_at FROM agent_sessions "
                        "WHERE tenant_id=%s AND session_id=%s",
                        (tenant, sid),
                    )
                ).fetchone()
                if not row or row["updated_at"] >= cutoff:
                    return count
                rows = await (
                    await conn.execute(
                        "SELECT record FROM agent_session_events "
                        "WHERE tenant_id=%s AND session_id=%s "
                        "ORDER BY seq LIMIT %s",
                        (tenant, sid, config.max_events),
                    )
                ).fetchall()
            if not rows:
                return count
            records, size = [], 0
            for item in rows:
                record = item["record"]
                length = len(json.dumps(record, ensure_ascii=True).encode()) + 1
                if records and size + length > config.target_bytes:
                    break
                records.append(record)
                size += length
            manifest = await self.archive.put(tenant, sid, records)
            async with self.pool.connection() as conn:
                current = await (
                    await conn.execute(
                        "SELECT next_seq,updated_at FROM agent_sessions WHERE tenant_id=%s "
                        "AND session_id=%s",
                        (tenant, sid),
                    )
                ).fetchone()
                if current != row:
                    raise SessionError("Session changed during archive; host must exclude writers")
                await conn.execute(
                    """INSERT INTO agent_session_archives
                    (tenant_id,session_id,start_seq,end_seq,object_uri,version_id,sha256,
                     format_version,metadata) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        tenant,
                        sid,
                        manifest["start_seq"],
                        manifest["end_seq"],
                        manifest["object_uri"],
                        manifest.get("version_id"),
                        manifest["sha256"],
                        manifest["format_version"],
                        Jsonb(manifest["metadata"]),
                    ),
                )
                deleted = await conn.execute(
                    "DELETE FROM agent_session_events WHERE tenant_id=%s AND session_id=%s "
                    "AND seq BETWEEN %s AND %s",
                    (tenant, sid, manifest["start_seq"], manifest["end_seq"]),
                )
                if deleted.rowcount != len(records):
                    raise SessionError("Archive range changed")
            count += len(records)

    async def audit(
        self,
        *,
        tenant_id,
        audit_id,
        actor_id,
        actor_type,
        user_id,
        action,
        resource_type,
        resource_id,
        outcome,
        session_id=None,
        run_id=None,
        occurred_at=None,
        metadata=None,
    ):
        """Host/tool adapter records actual access; stable audit_id makes retries idempotent."""
        async with self.pool.connection() as conn:
            await conn.execute(
                """INSERT INTO agent_audit_events
                (tenant_id,audit_id,actor_id,actor_type,user_id,session_id,run_id,occurred_at,
                 action,resource_type,resource_id,outcome,metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (
                    tenant_id,
                    audit_id,
                    actor_id,
                    actor_type,
                    user_id,
                    session_id,
                    run_id,
                    occurred_at or datetime.now(timezone.utc),
                    action,
                    resource_type,
                    resource_id,
                    outcome,
                    Jsonb(metadata or {}),
                ),
            )

    async def daily_usage(self, tenant, user, start, end, *, timezone_name="UTC"):
        async with self.pool.connection() as conn:
            return await (
                await conn.execute(
                    """SELECT (COALESCE(finished_at,started_at) AT TIME ZONE %s)::date AS day,
                sum(total_tokens) FILTER (WHERE usage_status='known') AS total_tokens,
                count(*) FILTER (WHERE usage_status='unknown') AS unknown_attempts,
                count(*) AS attempts FROM agent_model_usage
                WHERE tenant_id=%s AND user_id=%s
                AND COALESCE(finished_at,started_at)>=%s
                AND COALESCE(finished_at,started_at)<%s GROUP BY 1 ORDER BY 1""",
                    (timezone_name, tenant, user, start, end),
                )
            ).fetchall()

    async def audit_history(self, tenant, user, start, end, *, limit=1000):
        async with self.pool.connection() as conn:
            return await (
                await conn.execute(
                    "SELECT * FROM agent_audit_events WHERE tenant_id=%s AND user_id=%s "
                    "AND occurred_at>=%s AND occurred_at<%s ORDER BY occurred_at,audit_id LIMIT %s",
                    (tenant, user, start, end, limit),
                )
            ).fetchall()

    async def daily_activity(self, tenant, user, start, end, *, timezone_name="UTC"):
        async with self.pool.connection() as conn:
            return await (
                await conn.execute(
                    """SELECT (occurred_at AT TIME ZONE %s)::date AS day, count(*) AS actions,
                count(*) FILTER (WHERE action='input_submitted') AS inputs
                FROM agent_audit_events WHERE tenant_id=%s AND user_id=%s AND actor_type='user'
                AND occurred_at>=%s AND occurred_at<%s GROUP BY 1 ORDER BY 1""",
                    (timezone_name, tenant, user, start, end),
                )
            ).fetchall()

    async def daily_runs(self, tenant, user, start, end, *, timezone_name="UTC"):
        async with self.pool.connection() as conn:
            return await (
                await conn.execute(
                    """WITH facts AS (
                    SELECT started_at AS at, 1 AS started, 0 AS completed
                    FROM agent_runs WHERE tenant_id=%s AND user_id=%s AND source='user'
                    UNION ALL
                    SELECT finished_at, 0, 1 FROM agent_runs
                    WHERE tenant_id=%s AND user_id=%s AND source='user' AND status='completed'
                ) SELECT (at AT TIME ZONE %s)::date AS day,
                    sum(started) AS started, sum(completed) AS completed
                FROM facts WHERE at>=%s AND at<%s GROUP BY 1 ORDER BY 1""",
                    (tenant, user, tenant, user, timezone_name, start, end),
                )
            ).fetchall()
