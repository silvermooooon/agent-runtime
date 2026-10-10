"""PostgreSQL journal and retained audit facts. Scheduling remains the host's job."""

import json
from datetime import datetime, timedelta, timezone
from importlib.resources import files

from psycopg.rows import dict_row
from psycopg.types.json import Json, Jsonb
from psycopg_pool import AsyncConnectionPool

from ..sessions.base import SessionError
from .audit import project


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
            for name in ("001_sessions.sql", "002_checkpoints.sql"):
                ddl = files("agent_runtime.storage").joinpath(f"sql/{name}").read_text()
                await conn.execute(ddl)

    async def _ensure(self, conn, tenant, sid):
        await conn.execute(
            "INSERT INTO agent_sessions (tenant_id,session_id) VALUES (%s,%s) "
            "ON CONFLICT DO NOTHING",
            (tenant, sid),
        )

    async def append(self, session, record):
        async with self.pool.connection() as conn:
            await self._ensure(conn, session.tenant_id, session.session_id)
            await self._insert_event(conn, session.tenant_id, session.session_id, record)
            updated = await conn.execute(
                "UPDATE agent_sessions SET next_seq=%s,updated_at=clock_timestamp() "
                "WHERE tenant_id=%s AND session_id=%s AND next_seq=%s",
                (record["seq"] + 1, session.tenant_id, session.session_id, record["seq"]),
            )
            if updated.rowcount != 1:
                raise SessionError("Stale session; reopen after the previous writer exits")
            await project(conn, session, record)

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

    async def import_records(self, tenant, sid, records):
        # Historical copies are not new access or usage and never project audit facts.
        async with self.pool.connection() as conn:
            await self._ensure(conn, tenant, sid)
            updated = await conn.execute(
                "UPDATE agent_sessions SET next_seq=%s,updated_at=clock_timestamp() "
                "WHERE tenant_id=%s AND session_id=%s AND next_seq=0",
                (len(records), tenant, sid),
            )
            if updated.rowcount != 1:
                raise SessionError("Fork target must be empty")
            for record in records:
                await self._insert_event(conn, tenant, sid, record)

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
