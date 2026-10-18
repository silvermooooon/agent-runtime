"""Local SQLite storage for the same canonical DatabaseSession journal as PostgreSQL.

No optional dependency, implicit schema install, or token-delta persistence. SQLite I/O
runs off the event loop; each store serializes access to its connection. Use a local
filesystem, not a network share, and PostgreSQL for multi-host deployment.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from uuid import uuid4
from zoneinfo import ZoneInfo

from ..sessions.base import Session, SessionError
from .coordination import (
    COMMAND_STATES,
    CommandConflictError,
    ExecutionLease,
    ExecutionLeaseError,
    canonical_payload,
    validate_identifier,
    validate_ttl,
)


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _now():
    return datetime.now(timezone.utc)


def _time(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Expected a timezone-aware datetime")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _row(value):
    if value is None:
        return None
    result = dict(value)
    for key in ("metadata", "payload", "record"):
        if key in result and isinstance(result[key], str):
            result[key] = json.loads(result[key])
    for key in (
        "created_at",
        "updated_at",
        "started_at",
        "finished_at",
        "occurred_at",
        "accepted_at",
        "expires_at",
        "cancel_requested_at",
    ):
        if result.get(key) is not None:
            result[key] = datetime.fromisoformat(result[key])
    return result


class SQLiteStore:
    def __init__(self, connection, *, archive=None):
        self.connection = connection
        self.archive = archive
        self._lock = threading.RLock()
        self._closed = False

    @classmethod
    async def connect(cls, path, *, timeout_seconds=10, archive=None):
        """Open/create a local file. Its parent directory must already exist."""
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        def open_connection():
            connection = sqlite3.connect(
                os.fspath(path),
                timeout=timeout_seconds,
                check_same_thread=False,
                isolation_level=None,
            )
            try:
                connection.row_factory = sqlite3.Row
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                return connection
            except BaseException:
                connection.close()
                raise

        return cls(await asyncio.to_thread(open_connection), archive=archive)

    async def _call(self, operation, *, write=False):
        def run():
            with self._lock:
                if self._closed:
                    raise SessionError("SQLiteStore is closed")
                self.connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                try:
                    result = operation(self.connection)
                    self.connection.commit()
                    return result
                except BaseException:
                    self.connection.rollback()
                    raise

        return await asyncio.to_thread(run)

    async def close(self):
        def close():
            with self._lock:
                if not self._closed:
                    self.connection.close()
                    self._closed = True

        await asyncio.to_thread(close)

    async def install_schema(self):
        """Explicit install, or verify an already installed known version. Never migrates."""
        version = await self._call(lambda conn: conn.execute("PRAGMA user_version").fetchone()[0])
        if version == 2:
            return await self.check_schema()
        if version != 0:
            raise SessionError(
                "Existing SQLite schema requires explicit await store.migrate_schema(); no "
                "migration ran"
            )
        ddl = files("agent_runtime.storage").joinpath("sqlite/001_schema.sql").read_text()

        def install():
            with self._lock:
                if self._closed:
                    raise SessionError("SQLiteStore is closed")
                try:
                    self.connection.executescript("BEGIN IMMEDIATE;\n" + ddl + "\nCOMMIT;")
                except BaseException:
                    self.connection.rollback()
                    raise

        await asyncio.to_thread(install)
        return await self.check_schema()

    @staticmethod
    def _ensure(conn, tenant, sid, user_id=None):
        conn.execute(
            "INSERT INTO agent_sessions(tenant_id,session_id,user_id,created_at,updated_at) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT DO NOTHING",
            (tenant, sid, user_id, _time(_now()), _time(_now())),
        )
        row = conn.execute(
            "SELECT user_id FROM agent_sessions WHERE tenant_id=? AND session_id=?", (tenant, sid)
        ).fetchone()
        if user_id is not None:
            if row["user_id"] is None:
                conn.execute(
                    "UPDATE agent_sessions SET user_id=? WHERE tenant_id=? AND session_id=?",
                    (user_id, tenant, sid),
                )
            elif row["user_id"] != user_id:
                raise SessionError("Session user identity does not match storage")

    @staticmethod
    def _insert_event(conn, tenant, sid, record):
        conn.execute(
            "INSERT INTO agent_session_events(tenant_id,session_id,seq,event_id,record) "
            "VALUES(?,?,?,?,?)",
            (tenant, sid, record["seq"], record["id"], _json(record)),
        )
        if record["type"] == "compaction" and "checkpoint" in record:
            conn.execute(
                "INSERT INTO agent_session_checkpoints(tenant_id,session_id,seq,event_id) "
                "VALUES(?,?,?,?)",
                (tenant, sid, record["seq"], record["id"]),
            )

    @staticmethod
    def _assert_lease(conn, lease):
        if not isinstance(lease, ExecutionLease):
            raise ExecutionLeaseError("A valid ExecutionLease is required")
        row = conn.execute(
            "SELECT * FROM agent_session_leases WHERE tenant_id=? AND session_id=?",
            (lease.tenant_id, lease.session_id),
        ).fetchone()
        if (
            not row
            or row["holder"] != lease.holder
            or row["fence"] != lease.fence
            or row["expires_at"] <= _time(_now())
        ):
            raise ExecutionLeaseError("Execution ownership expired or was replaced")

    @classmethod
    def _guard(cls, conn, session):
        lease = getattr(session, "execution_lease", None)
        if lease is not None:
            if (lease.tenant_id, lease.session_id) != (session.info.tenant_id, session.session_id):
                raise ExecutionLeaseError("Execution lease belongs to another session")
            cls._assert_lease(conn, lease)
        elif conn.execute(
            "SELECT 1 FROM agent_session_leases WHERE tenant_id=? AND session_id=?",
            (session.info.tenant_id, session.session_id),
        ).fetchone():
            raise ExecutionLeaseError("Coordinated session requires a live execution lease")

    async def append(self, session, record):
        def append(conn):
            tenant, sid = session.info.tenant_id, session.session_id
            self._ensure(conn, tenant, sid, session.info.user_id)
            self._guard(conn, session)
            self._insert_event(conn, tenant, sid, record)
            updated = conn.execute(
                "UPDATE agent_sessions SET next_seq=?,updated_at=? "
                "WHERE tenant_id=? AND session_id=? AND next_seq=?",
                (record["seq"] + 1, _time(_now()), tenant, sid, record["seq"]),
            )
            if updated.rowcount != 1:
                raise SessionError("Stale session; reopen after the previous writer exits")
            self._project(conn, session, record)
            self._project_command(conn, session, record)

        await self._call(append, write=True)

    async def import_records(self, info, records):
        def publish(conn):
            self._ensure(conn, info.tenant_id, info.session_id, info.user_id)
            if [r["seq"] for r in records] != list(range(len(records))):
                raise SessionError("Fork records must be a complete contiguous prefix")
            if conn.execute(
                "SELECT 1 FROM agent_session_leases WHERE tenant_id=? AND session_id=?",
                (info.tenant_id, info.session_id),
            ).fetchone():
                raise ExecutionLeaseError("Fork target must be a new uncoordinated session")
            updated = conn.execute(
                "UPDATE agent_sessions SET next_seq=?,updated_at=? WHERE tenant_id=? AND "
                "session_id=? AND next_seq=0",
                (len(records), _time(_now()), info.tenant_id, info.session_id),
            )
            if updated.rowcount != 1:
                raise SessionError("Fork target must be empty")
            for record in records:
                self._insert_event(conn, info.tenant_id, info.session_id, record)

        await self._call(publish, write=True)

    async def read_records(self, tenant, sid, after_seq=-1):
        return await self._read_records(tenant, sid, after_seq=after_seq)

    async def read_recovery_records(self, tenant, sid, *, event_id=None):
        return await self._read_records(tenant, sid, recovery=True, event_id=event_id)

    async def _read_records(self, tenant, sid, *, after_seq=-1, recovery=False, event_id=None):
        def select(conn):
            row = conn.execute(
                "SELECT next_seq FROM agent_sessions WHERE tenant_id=? AND session_id=?",
                (tenant, sid),
            ).fetchone()
            if row is None:
                if event_id is not None:
                    raise SessionError(f"Unknown event ID: {event_id}")
                return -1, after_seq, [], []
            end = row["next_seq"] - 1
            if event_id is not None:
                target = conn.execute(
                    "SELECT seq FROM agent_session_events WHERE tenant_id=? AND session_id=? "
                    "AND event_id=? "
                    "UNION ALL SELECT seq FROM agent_session_checkpoints WHERE tenant_id=? "
                    "AND session_id=? AND event_id=? LIMIT 1",
                    (tenant, sid, event_id, tenant, sid, event_id),
                ).fetchone()
                if target is None:
                    return None, after_seq, [], []
                end = target["seq"]
            start = after_seq
            if recovery:
                checkpoint = conn.execute(
                    "SELECT seq FROM agent_session_checkpoints WHERE tenant_id=? AND "
                    "session_id=? AND seq<=? ORDER BY seq DESC LIMIT 1",
                    (tenant, sid, end),
                ).fetchone()
                start = checkpoint["seq"] - 1 if checkpoint else -1
            archives = [
                _row(r)
                for r in conn.execute(
                    "SELECT * FROM agent_session_archives WHERE tenant_id=? AND session_id=? "
                    "AND end_seq>? AND start_seq<=? ORDER BY start_seq",
                    (tenant, sid, start, end),
                )
            ]
            hot = [
                json.loads(r["record"])
                for r in conn.execute(
                    "SELECT record FROM agent_session_events WHERE tenant_id=? AND "
                    "session_id=? AND seq>? AND seq<=? ORDER BY seq",
                    (tenant, sid, start, end),
                )
            ]
            return end, start, archives, hot

        end, start, archives, records = await self._call(select)
        if end is None:
            prefix = Session._select_prefix(await self.read_records(tenant, sid), event_id)
            checkpoint = next(
                (i for i in range(len(prefix) - 1, -1, -1) if "checkpoint" in prefix[i]), 0
            )
            return prefix[checkpoint:] if recovery else prefix
        for manifest in archives:
            if self.archive is None:
                raise SessionError("Archive reader is required for this session")
            records.extend(r for r in await self.archive.get(manifest) if start < r["seq"] <= end)
        records.sort(key=lambda r: r["seq"])
        if [r["seq"] for r in records] != list(range(start + 1, end + 1)):
            raise SessionError("Missing or overlapping journal events")
        return records

    @staticmethod
    def _project(conn, session, record):
        tenant, sid, user = session.info.tenant_id, session.session_id, session.info.user_id
        kind, data, run = record["type"], record["data"], record.get("run_id")
        at = _time(datetime.fromtimestamp(record["time"] / 1000, timezone.utc))
        if kind in ("run_started", "run_resumed"):
            source = data["source"] if kind == "run_started" else session.run_source
            root = data["root_run_id"] if kind == "run_started" else session.root_run_id
            conn.execute(
                "INSERT INTO "
                "agent_runs(tenant_id,session_id,run_id,user_id,root_run_id,source,started_at,"
                "status) "
                "VALUES(?,?,?,?,?,?,?,'running') ON CONFLICT(tenant_id,session_id,run_id) DO "
                "UPDATE SET status='running',finished_at=NULL",
                (tenant, sid, run, user, root, source, at),
            )
        elif kind in ("run_completed", "run_interrupted"):
            conn.execute(
                "UPDATE agent_runs SET status=?,finished_at=? WHERE tenant_id=? AND "
                "session_id=? AND run_id=?",
                ("completed" if kind == "run_completed" else data["status"], at, tenant, sid, run),
            )
        elif kind == "model_call_started":
            conn.execute(
                "INSERT INTO "
                "agent_model_usage(tenant_id,attempt_id,session_id,run_id,root_run_id,user_id,"
                "provider,model,purpose,started_at,status,usage_status) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,'unknown','unknown')",
                (
                    tenant,
                    data["attempt_id"],
                    sid,
                    run,
                    session.root_run_id,
                    user,
                    data["provider"],
                    data["model"],
                    data["purpose"],
                    at,
                ),
            )
        elif kind == "model_call_finished":
            message = data.get("message") or {}
            usage = message.get("usage", {})
            keys = ("input", "output", "cacheRead", "cacheWrite", "totalTokens")
            known = message.get("usageStatus") == "known" and all(
                type(usage.get(k)) is int and usage[k] >= 0 for k in keys
            )
            conn.execute(
                "UPDATE agent_model_usage SET "
                "finished_at=?,status=?,input_tokens=?,output_tokens=?,cache_read_tokens=?,cac"
                "he_write_tokens=?,total_tokens=?,usage_status=?,metadata=? WHERE tenant_id=? "
                "AND attempt_id=?",
                (
                    at,
                    data["status"],
                    *(usage.get(k) if known else None for k in keys),
                    "known" if known else "unknown",
                    _json(
                        {
                            "usage": usage,
                            "provider_response_id": message.get("responseId"),
                            "event_id": record["id"],
                        }
                    ),
                    tenant,
                    data["attempt_id"],
                ),
            )

    async def list_sessions(self, tenant, *, user_id=None, limit=100, after=None):
        clauses, args = ["tenant_id=?"], [tenant]
        if user_id is not None:
            clauses.append("user_id=?")
            args.append(user_id)
        if after is not None:
            clauses.append("(created_at,session_id)<(?,?)")
            args.extend((_time(after[0]), after[1]))
        return await self._query(
            "SELECT * FROM agent_sessions WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at DESC,session_id DESC LIMIT ?",
            (*args, self._limit(limit)),
        )

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
        clauses, args = ["tenant_id=?"], [tenant]
        for key, value in (
            ("session_id", session_id),
            ("user_id", user_id),
            ("root_run_id", root_run_id),
            ("status", status),
        ):
            if value is not None:
                clauses.append(key + "=?")
                args.append(value)
        for operator, value in ((">=", start), ("<", end)):
            if value is not None:
                clauses.append("started_at" + operator + "?")
                args.append(_time(value))
        if after is not None:
            clauses.append("(started_at,session_id,run_id)<(?,?,?)")
            args.extend((_time(after[0]), after[1], after[2]))
        return await self._query(
            "SELECT * FROM agent_runs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY started_at DESC,session_id DESC,run_id DESC LIMIT ?",
            (*args, self._limit(limit)),
        )

    async def read_run_records(self, tenant, sid, run_id, *, after_seq=-1, limit=1000):
        limit = self._limit(limit)
        return [
            r for r in await self.read_records(tenant, sid, after_seq) if r.get("run_id") == run_id
        ][:limit]

    @staticmethod
    def _limit(value):
        if type(value) is not int or not 1 <= value <= 10000:
            raise ValueError("limit must be between 1 and 10000")
        return value

    async def _query(self, sql, args=()):
        return await self._call(lambda conn: [_row(r) for r in conn.execute(sql, args)])

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
        values = (
            tenant_id,
            audit_id,
            actor_id,
            actor_type,
            user_id,
            session_id,
            run_id,
            _time(occurred_at or _now()),
            action,
            resource_type,
            resource_id,
            outcome,
            _json(metadata or {}),
        )
        await self._call(
            lambda conn: (
                conn.execute(
                    "INSERT INTO "
                    "agent_audit_events(tenant_id,audit_id,actor_id,actor_type,user_id,session"
                    "_id,run_id,occurred_at,action,resource_type,resource_id,outcome,metadata)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    values,
                ).rowcount
            ),
            write=True,
        )

    async def audit_history(self, tenant, user, start, end, *, limit=1000):
        return await self._query(
            "SELECT * FROM agent_audit_events WHERE tenant_id=? AND user_id=? AND "
            "occurred_at>=? AND occurred_at<? ORDER BY occurred_at,audit_id LIMIT ?",
            (tenant, user, _time(start), _time(end), self._limit(limit)),
        )

    async def daily_usage(self, tenant, user, start, end, *, timezone_name="UTC"):
        zone = ZoneInfo(timezone_name)
        rows = await self._query(
            "SELECT * FROM agent_model_usage WHERE tenant_id=? AND user_id=? AND "
            "COALESCE(finished_at,started_at)>=? AND COALESCE(finished_at,started_at)<?",
            (tenant, user, _time(start), _time(end)),
        )
        days = {}
        for row in rows:
            day = (row["finished_at"] or row["started_at"]).astimezone(zone).date()
            item = days.setdefault(
                day, dict(day=day, total_tokens=None, unknown_attempts=0, attempts=0)
            )
            item["attempts"] += 1
            if row["usage_status"] == "known":
                item["total_tokens"] = (item["total_tokens"] or 0) + row["total_tokens"]
            else:
                item["unknown_attempts"] += 1
        return [days[day] for day in sorted(days)]

    async def daily_activity(self, tenant, user, start, end, *, timezone_name="UTC"):
        zone, days = ZoneInfo(timezone_name), {}
        rows = await self._query(
            "SELECT * FROM agent_audit_events WHERE tenant_id=? AND user_id=? AND "
            "actor_type='user' AND occurred_at>=? AND occurred_at<?",
            (tenant, user, _time(start), _time(end)),
        )
        for row in rows:
            day = row["occurred_at"].astimezone(zone).date()
            item = days.setdefault(day, dict(day=day, actions=0, inputs=0))
            item["actions"] += 1
            item["inputs"] += row["action"] == "input_submitted"
        return [days[day] for day in sorted(days)]

    async def daily_runs(self, tenant, user, start, end, *, timezone_name="UTC"):
        zone, days = ZoneInfo(timezone_name), {}
        rows = await self._query(
            "SELECT * FROM agent_runs WHERE tenant_id=? AND user_id=? AND source='user'",
            (tenant, user),
        )
        start_text, end_text = _time(start), _time(end)
        for row in rows:
            for key, at in (
                ("started", row["started_at"]),
                ("completed", row["finished_at"] if row["status"] == "completed" else None),
            ):
                if at is not None and start_text <= _time(at) < end_text:
                    day = at.astimezone(zone).date()
                    item = days.setdefault(day, dict(day=day, started=0, completed=0))
                    item[key] += 1
        return [days[day] for day in sorted(days)]

    async def accept_command(
        self, tenant_id, session_id, *, command_id, kind, payload, user_id=None, run_id=None
    ):
        """Acknowledge only after a durable inbox commit; retry the same stable ID."""
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
        subagent = normalized.get("subagent")
        parent_session_id = (
            subagent.get("parent_session_id") if isinstance(subagent, dict) else None
        )
        if parent_session_id is not None:
            validate_identifier(parent_session_id, "subagent.parent_session_id")
        explicit_run_id = run_id
        run_id = run_id or (uuid4().hex if kind == "prompt" else None)

        def accept(conn):
            existing = _row(
                conn.execute(
                    "SELECT * FROM agent_commands WHERE tenant_id=? AND command_id=?",
                    (tenant_id, command_id),
                ).fetchone()
            )
            if existing:
                if (
                    existing["session_id"] != session_id
                    or existing["kind"] != kind
                    or existing["payload_hash"] != digest
                    or (explicit_run_id is not None and existing["run_id"] != explicit_run_id)
                ):
                    raise CommandConflictError("Command ID already identifies different input")
                self._ensure(conn, tenant_id, session_id, user_id)
                return existing
            self._ensure(conn, tenant_id, session_id, user_id)
            if (
                run_id is not None
                and conn.execute(
                    "SELECT 1 FROM agent_commands WHERE tenant_id=? AND session_id=? AND run_id=?",
                    (tenant_id, session_id, run_id),
                ).fetchone()
            ):
                raise CommandConflictError("Canonical run is already bound to another command")
            if (
                run_id is not None
                and conn.execute(
                    "SELECT 1 FROM agent_runs WHERE tenant_id=? AND session_id=? AND run_id=?",
                    (tenant_id, session_id, run_id),
                ).fetchone()
            ):
                raise CommandConflictError("Canonical run already exists; use its original command")
            seq = conn.execute(
                "SELECT COALESCE(MAX(command_seq),0)+1 AS n FROM agent_commands WHERE "
                "tenant_id=? AND session_id=?",
                (tenant_id, session_id),
            ).fetchone()["n"]
            now = _time(_now())
            conn.execute(
                "INSERT INTO "
                "agent_commands(tenant_id,command_id,session_id,parent_session_id,command_seq,"
                "kind,payload,payload_hash,run_id,state,accepted_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,'accepted',?,?)",
                (
                    tenant_id,
                    command_id,
                    session_id,
                    parent_session_id,
                    seq,
                    kind,
                    _json(normalized),
                    digest,
                    run_id,
                    now,
                    now,
                ),
            )
            return _row(
                conn.execute(
                    "SELECT * FROM agent_commands WHERE tenant_id=? AND command_id=?",
                    (tenant_id, command_id),
                ).fetchone()
            )

        return await self._call(accept, write=True)

    async def list_commands(
        self, tenant_id, session_id, *, state=None, kind=None, after_seq=0, limit=100
    ):
        if state is not None and state not in COMMAND_STATES:
            raise ValueError("Unknown command state")
        if type(after_seq) is not int or after_seq < 0:
            raise ValueError("after_seq must be a non-negative integer")
        clauses, args = (
            ["tenant_id=?", "session_id=?", "command_seq>?"],
            [tenant_id, session_id, after_seq],
        )
        if state is not None:
            clauses.append("state=?")
            args.append(state)
        if kind is not None:
            clauses.append("kind=?")
            args.append(kind)
        return await self._query(
            "SELECT * FROM agent_commands WHERE "
            + " AND ".join(clauses)
            + " ORDER BY command_seq LIMIT ?",
            (*args, self._limit(limit)),
        )

    async def list_notifications(
        self, tenant_id, session_id, *, state=None, after_seq=0, limit=100
    ):
        return await self.list_commands(
            tenant_id,
            session_id,
            kind="child_result",
            state=state,
            after_seq=after_seq,
            limit=limit,
        )

    async def ack_notification(self, tenant_id, session_id, command_id):
        def acknowledge(conn):
            row = conn.execute(
                "SELECT * FROM agent_commands WHERE tenant_id=? AND command_id=?",
                (tenant_id, command_id),
            ).fetchone()
            if (
                row is None
                or row["session_id"] != session_id
                or row["kind"] != "child_result"
                or row["state"] not in ("accepted", "completed")
            ):
                raise CommandConflictError(
                    "Notification does not belong to this session or is not acknowledgeable"
                )
            conn.execute(
                "UPDATE agent_commands SET state='completed',updated_at=? WHERE tenant_id=? "
                "AND command_id=?",
                (_time(_now()), tenant_id, command_id),
            )
            return _row(
                conn.execute(
                    "SELECT * FROM agent_commands WHERE tenant_id=? AND command_id=?",
                    (tenant_id, command_id),
                ).fetchone()
            )

        return await self._call(acknowledge, write=True)

    async def list_child_commands(self, tenant_id, parent_session_id, *, limit=1000, after=None):
        sql = (
            "SELECT * FROM agent_commands WHERE tenant_id=? AND kind='prompt' AND "
            "parent_session_id=?"
        )
        args = [tenant_id, parent_session_id]
        if after is not None:
            sql += " AND (session_id,command_seq)>(?,?)"
            args.extend(after)
        return await self._query(
            sql + " ORDER BY session_id,command_seq LIMIT ?", (*args, self._limit(limit))
        )

    async def get_command(self, tenant_id, command_id):
        rows = await self._query(
            "SELECT * FROM agent_commands WHERE tenant_id=? AND command_id=?",
            (tenant_id, command_id),
        )
        return rows[0] if rows else None

    async def request_cancel(self, tenant_id, command_id):
        def cancel(conn):
            conn.execute(
                "UPDATE agent_commands SET "
                "cancel_requested_at=COALESCE(cancel_requested_at,?),updated_at=? WHERE "
                "tenant_id=? AND command_id=? AND state IN ('accepted','applied')",
                (_time(_now()), _time(_now()), tenant_id, command_id),
            )
            return _row(
                conn.execute(
                    "SELECT * FROM agent_commands WHERE tenant_id=? AND command_id=?",
                    (tenant_id, command_id),
                ).fetchone()
            )

        return await self._call(cancel, write=True)

    async def get_execution(self, tenant_id, session_id):
        """Read current ownership/expiry without acquiring or extending it."""
        rows = await self._query(
            "SELECT * FROM agent_session_leases WHERE tenant_id=? AND session_id=?",
            (tenant_id, session_id),
        )
        if not rows:
            return None
        row = rows[0]
        return ExecutionLease(
            row["tenant_id"], row["session_id"], row["holder"], row["fence"], row["expires_at"]
        )

    async def acquire_execution(self, tenant_id, session_id, *, holder, ttl_seconds=30):
        validate_ttl(ttl_seconds)
        for name, value in (
            ("tenant_id", tenant_id),
            ("session_id", session_id),
            ("holder", holder),
        ):
            validate_identifier(value, name)

        def acquire(conn):
            self._ensure(conn, tenant_id, session_id)
            current = conn.execute(
                "SELECT * FROM agent_session_leases WHERE tenant_id=? AND session_id=?",
                (tenant_id, session_id),
            ).fetchone()
            now = _now()
            if current and current["expires_at"] > _time(now):
                return None
            fence = current["fence"] + 1 if current else 1
            expires = now + timedelta(seconds=ttl_seconds)
            conn.execute(
                "INSERT INTO "
                "agent_session_leases(tenant_id,session_id,holder,fence,expires_at) "
                "VALUES(?,?,?,?,?) ON CONFLICT(tenant_id,session_id) DO UPDATE SET "
                "holder=excluded.holder,fence=excluded.fence,expires_at=excluded.expires_at",
                (tenant_id, session_id, holder, fence, _time(expires)),
            )
            return ExecutionLease(tenant_id, session_id, holder, fence, expires)

        return await self._call(acquire, write=True)

    async def renew_execution(self, lease, *, ttl_seconds=30):
        validate_ttl(ttl_seconds)

        def renew(conn):
            self._assert_lease(conn, lease)
            expires = _now() + timedelta(seconds=ttl_seconds)
            conn.execute(
                "UPDATE agent_session_leases SET expires_at=? WHERE tenant_id=? AND "
                "session_id=? AND holder=? AND fence=?",
                (_time(expires), lease.tenant_id, lease.session_id, lease.holder, lease.fence),
            )
            return ExecutionLease(
                lease.tenant_id, lease.session_id, lease.holder, lease.fence, expires
            )

        return await self._call(renew, write=True)

    async def release_execution(self, lease):
        def release(conn):
            updated = conn.execute(
                "UPDATE agent_session_leases SET expires_at=? WHERE tenant_id=? AND "
                "session_id=? AND holder=? AND fence=? AND expires_at>?",
                (
                    _time(_now()),
                    lease.tenant_id,
                    lease.session_id,
                    lease.holder,
                    lease.fence,
                    _time(_now()),
                ),
            )
            return updated.rowcount == 1

        return await self._call(release, write=True)

    async def assert_execution(self, lease):
        await self._call(lambda conn: self._assert_lease(conn, lease))

    async def assert_session_execution(self, session):
        await self._call(lambda conn: self._guard(conn, session))

    @staticmethod
    def _project_command(conn, session, record):
        from .coordination import command_transition

        transition = command_transition(record)
        if transition is None:
            return
        command_id, state, run_id = transition
        tenant, sid = session.info.tenant_id, session.session_id
        if command_id is not None:
            command = conn.execute(
                "SELECT * FROM agent_commands WHERE tenant_id=? AND command_id=?",
                (tenant, command_id),
            ).fetchone()
            if command is None or command["session_id"] != sid:
                raise CommandConflictError("Command does not belong to this session")
        elif run_id is not None:
            command = conn.execute(
                "SELECT * FROM agent_commands WHERE tenant_id=? AND session_id=? AND run_id=?",
                (tenant, sid, run_id),
            ).fetchone()
            if command is None:
                return
        else:
            return
        if getattr(session, "execution_lease", None) is None:
            raise ExecutionLeaseError("Applying durable commands requires an execution lease")
        if run_id is not None and command["run_id"] != run_id:
            raise CommandConflictError("Command run ID does not match the journal")
        if state == "applied":
            if record["type"] == "run_started":
                first = conn.execute(
                    "SELECT command_id FROM agent_commands WHERE tenant_id=? AND session_id=? "
                    "AND kind='prompt' AND state IN ('accepted','applied') ORDER BY "
                    "command_seq LIMIT 1",
                    (tenant, sid),
                ).fetchone()
                if command["kind"] != "prompt" or command["cancel_requested_at"] is not None:
                    raise CommandConflictError("Only an uncanceled accepted prompt can start")
                if (
                    command["state"] != "accepted"
                    or first is None
                    or first["command_id"] != command["command_id"]
                ):
                    raise CommandConflictError("Commands must start once in accepted FIFO order")
            elif command["state"] not in ("applied", "stopped", "failed"):
                raise CommandConflictError("Command is not resumable")
            if (
                record["type"] == "run_resumed"
                and conn.execute(
                    "SELECT 1 FROM agent_commands WHERE tenant_id=? AND session_id=? AND "
                    "state='applied' AND command_id<>?",
                    (tenant, sid, command["command_id"]),
                ).fetchone()
            ):
                raise CommandConflictError("Another command owns the active run")
            conn.execute(
                "UPDATE agent_commands SET "
                "completed_journal_seq=NULL,completed_event_id=NULL,state='applied',run_id=COA"
                "LESCE(run_id,?),applied_journal_seq=COALESCE(applied_journal_seq,?),applied_e"
                "vent_id=COALESCE(applied_event_id,?),updated_at=? WHERE tenant_id=? AND "
                "command_id=?",
                (run_id, record["seq"], record["id"], _time(_now()), tenant, command["command_id"]),
            )
        else:
            allowed = (
                ("accepted",)
                if record["type"] == "command_rejected"
                else ("applied", "failed", "stopped")
            )
            if command["state"] not in allowed:
                raise CommandConflictError("Command terminal transition is invalid")
            conn.execute(
                "UPDATE agent_commands SET "
                "state=?,completed_journal_seq=?,completed_event_id=?,updated_at=? WHERE "
                "tenant_id=? AND command_id=?",
                (state, record["seq"], record["id"], _time(_now()), tenant, command["command_id"]),
            )

    async def migrate_schema(self, *, target_version=2):
        """Explicit additive upgrade of the early local v1 schema; never drops data.

        Stop writers and back up the file before a deployment migration. Unknown
        versions and malformed legacy values fail closed and roll back.
        """
        if target_version != 2:
            raise ValueError("Only SQLite schema version 2 is supported")

        def migrate(conn):
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version == 2:
                return
            if version != 1:
                raise SessionError(
                    "Only an installed SQLite v1 schema can be upgraded; use install_schema "
                    "for a new empty file"
                )
            columns = {r["name"] for r in conn.execute("PRAGMA table_info(agent_commands)")}
            required = {
                "tenant_id",
                "command_id",
                "session_id",
                "payload",
                "run_id",
                "state",
                "accepted_at",
                "applied_journal_seq",
                "completed_journal_seq",
                "updated_at",
            }
            if not required <= columns:
                raise SessionError("Unrecognized SQLite v1 command schema; migrate manually")
            if "cancel_requested_at" not in columns:
                conn.execute("ALTER TABLE agent_commands ADD COLUMN cancel_requested_at TEXT")
            if "parent_session_id" not in columns:
                conn.execute("ALTER TABLE agent_commands ADD COLUMN parent_session_id TEXT")
            for row in conn.execute(
                "SELECT tenant_id,command_id,payload FROM agent_commands"
            ).fetchall():
                payload = json.loads(row["payload"])
                creation = payload.get("subagent")
                parent = creation.get("parent_session_id") if isinstance(creation, dict) else None
                if parent is not None:
                    validate_identifier(parent, "subagent.parent_session_id")
                    conn.execute(
                        "UPDATE agent_commands SET parent_session_id=? WHERE tenant_id=? AND "
                        "command_id=?",
                        (parent, row["tenant_id"], row["command_id"]),
                    )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS agent_commands_parent ON "
                "agent_commands(tenant_id,parent_session_id,session_id,command_seq)"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS agent_commands_run ON "
                "agent_commands(tenant_id,session_id,run_id)"
            )
            # Early v1 defaults used millisecond timestamps; normalize for stable keyset cursors.
            for table, timestamps in {
                "agent_sessions": ("created_at", "updated_at"),
                "agent_session_archives": ("created_at",),
                "agent_runs": ("started_at", "finished_at"),
                "agent_model_usage": ("started_at", "finished_at"),
                "agent_audit_events": ("occurred_at",),
                "agent_commands": ("accepted_at", "updated_at", "cancel_requested_at"),
                "agent_session_leases": ("expires_at",),
            }.items():
                for row in conn.execute(
                    "SELECT rowid AS internal_rowid,* FROM " + table
                ).fetchall():
                    for field in timestamps:
                        if row[field] is not None:
                            normalized = _time(datetime.fromisoformat(row[field]))
                            conn.execute(
                                "UPDATE " + table + " SET " + field + "=? WHERE rowid=?",
                                (normalized, row["internal_rowid"]),
                            )
            conn.execute("PRAGMA user_version=2")

        await self._call(migrate, write=True)
        return await self.check_schema()

    async def check_schema(self):
        """Verify the explicitly installed local schema; never changes it."""

        def check(conn):
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            expected = {
                "agent_sessions": {"tenant_id", "session_id", "user_id", "next_seq"},
                "agent_session_events": {"tenant_id", "session_id", "seq", "event_id", "record"},
                "agent_session_archives": {
                    "tenant_id",
                    "session_id",
                    "start_seq",
                    "end_seq",
                    "object_uri",
                    "sha256",
                },
                "agent_session_checkpoints": {"tenant_id", "session_id", "seq", "event_id"},
                "agent_runs": {
                    "tenant_id",
                    "session_id",
                    "run_id",
                    "user_id",
                    "root_run_id",
                    "status",
                },
                "agent_model_usage": {"tenant_id", "attempt_id", "usage_status", "total_tokens"},
                "agent_audit_events": {"tenant_id", "audit_id", "occurred_at", "user_id"},
                "agent_commands": {
                    "tenant_id",
                    "command_id",
                    "session_id",
                    "command_seq",
                    "payload_hash",
                    "state",
                    "run_id",
                    "applied_journal_seq",
                    "completed_journal_seq",
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
            if version != 2:
                raise SessionError(
                    "SQLite schema is not installed or has an unsupported version; explicitly "
                    "install a fresh schema or migrate offline"
                )
            for table, names in expected.items():
                actual = {r["name"] for r in conn.execute("PRAGMA table_info(" + table + ")")}
                if not names <= actual:
                    raise SessionError(
                        "SQLite schema mismatch in " + table + "; explicit migration is required"
                    )
            return {"ready": True, "version": 2, "backend": "sqlite", "coordination": True}

        return await self._call(check)
