"""Real PostgreSQL integration; each test owns a disposable schema, never public tables."""

import asyncio
import importlib.util
import io
import os
import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from test_loop import MODEL, FakeProvider, answer

from agent_runtime import Agent, SessionError
from agent_runtime.sessions import DatabaseSession
from agent_runtime.storage.config import ArchiveConfig, DatabaseConfig
from agent_runtime.storage.s3 import S3Archive

HAS_DB = importlib.util.find_spec("psycopg") is not None
if HAS_DB:
    from psycopg import AsyncConnection, sql

    from agent_runtime.storage.audit import AuditContext
    from agent_runtime.storage.postgres import PostgresStore


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]
        return {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": kwargs["SSEKMSKeyId"]}

    def get_object(self, Bucket, Key, **kwargs):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


class ArchiveTests(unittest.IsolatedAsyncioTestCase):
    def config(self):
        return ArchiveConfig(
            enabled=True,
            after_days=0,
            max_events=3,
            s3_uri="s3://test/history",
            region="us-east-1",
            kms_key_arn="arn:aws:kms:us-east-1:123456789012:key/test",
        )

    async def test_encrypted_roundtrip_and_corruption(self):
        client = FakeS3()
        archive = S3Archive(self.config(), client=client)
        records = [{"seq": 0, "id": "e", "data": {"text": "\x00你好"}}]
        manifest = await archive.put("tenant", "session", records)
        self.assertEqual(await archive.get(manifest), records)
        self.assertEqual(client.puts[0]["IfNoneMatch"], "*")
        self.assertEqual(client.puts[0]["ServerSideEncryption"], "aws:kms")
        key = next(iter(client.objects))
        client.objects[key] = b"broken"
        with self.assertRaisesRegex(SessionError, "checksum"):
            await archive.get(manifest)

    def test_config(self):
        with self.assertRaises(ValueError):
            ArchiveConfig.from_env({"AGENT_ARCHIVE_ENABLED": "true"})
        with self.assertRaises(ValueError):
            DatabaseConfig("", pool_max_size=0)
        self.assertNotIn("secret", repr(DatabaseConfig("postgresql://user:secret@host/db")))
        self.assertEqual(ArchiveConfig.from_env({}).after_days, 30)


@unittest.skipUnless(HAS_DB and os.environ.get("AGENT_TEST_DB_URL"), "Set AGENT_TEST_DB_URL")
class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.schema = "test_" + uuid4().hex
        self.dsn = os.environ["AGENT_TEST_DB_URL"]
        self.admin = await AsyncConnection.connect(self.dsn, autocommit=True)
        await self.admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.addAsyncCleanup(self.cleanup_database)
        from psycopg.rows import dict_row
        from psycopg_pool import AsyncConnectionPool

        pool = AsyncConnectionPool(
            self.dsn,
            open=False,
            min_size=1,
            max_size=2,
            kwargs={"options": f"-c search_path={self.schema}", "row_factory": dict_row},
        )
        await pool.open(wait=True)
        self.s3 = FakeS3()
        self.store = PostgresStore(pool, archive=S3Archive(ArchiveTests().config(), client=self.s3))
        await self.store.install_schema()
        self.ctx = AuditContext("alice")
        self.session = await self.open("main")

    async def cleanup_database(self):
        if hasattr(self, "store"):
            await self.store.close()
        await self.admin.execute(
            sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
        )
        await self.admin.close()

    async def open(self, sid, *, tenant="org"):
        return await DatabaseSession.open(
            self.store, tenant_id=tenant, audit_context=self.ctx, session_id=sid
        )

    async def rows(self, table):
        async with self.store.pool.connection() as conn:
            return await (
                await conn.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
            ).fetchall()

    def provider(self, *messages):
        if not messages:
            message = answer("done\x00你好")
            message["usage"] = dict(input=10, output=4, cacheRead=2, cacheWrite=0, totalTokens=16)
            message["usageStatus"] = "known"
            messages = [message]
        return FakeProvider(*messages)

    async def run_agent(self, session=None):
        agent = Agent(model=MODEL, session=session or self.session, stream_fn=self.provider())
        await agent.prompt("hello\x00world")
        return agent

    async def test_roundtrip_usage_and_tenant_isolation(self):
        await self.run_agent()
        reopened = await self.open("main")
        self.assertEqual(reopened.snapshot, self.session.snapshot)
        self.assertEqual((await reopened.aread_records()), (await self.session.aread_records()))
        self.assertEqual((await self.open("main", tenant="other")).revision, 0)
        usage = await self.rows("agent_model_usage")
        self.assertEqual(len(usage), 1)
        self.assertEqual(usage[0]["total_tokens"], 16)
        self.assertEqual(usage[0]["usage_status"], "known")
        self.assertEqual((await self.rows("agent_runs"))[0]["status"], "completed")

    async def test_archive_restore_then_append_and_fork_no_double_usage(self):
        await self.run_agent()
        records = await self.session.aread_records()
        archived = await self.store.archive_session("org", "main")
        self.assertEqual(archived, len(records))
        self.assertEqual(await self.rows("agent_session_events"), [])
        reopened = await self.open("main")
        self.assertEqual((await reopened.aread_records()), records)
        child = await reopened.fork(records[-1]["id"], session_id="fork")
        self.assertEqual(len(await self.rows("agent_model_usage")), 1)
        self.assertEqual(child.snapshot["messages"], reopened.snapshot["messages"])
        await self.run_agent(reopened)
        self.assertEqual(len(await self.rows("agent_model_usage")), 2)
        mixed = await self.open("main")
        self.assertEqual(mixed.snapshot, reopened.snapshot)
        self.assertEqual(
            await self.store.read_records("org", "main", len(records) - 1),
            await reopened.aread_records(len(records) - 1),
        )

    async def test_checkpoint_restores_without_old_archives_or_history_cache(self):
        from unittest.mock import patch

        agent = await self.run_agent()
        before = self.session.snapshot
        old_event = (await self.session.aread_records())[-1]["id"]
        # Archive pre-checkpoint history into separate objects, then commit a hot checkpoint.
        await self.store.archive_session("org", "main")
        checkpoint = await agent.compact(summary="preserve the user's goal")
        await self.run_agent()
        expected = self.session.snapshot
        with patch.object(
            self.store.archive, "get", side_effect=AssertionError("old archive read")
        ):
            reopened = await self.open("main")
            self.assertEqual(reopened.snapshot, expected)
            self.assertEqual(reopened.revision, self.session.revision)
            self.assertFalse(hasattr(reopened, "_records"))
            at_checkpoint = await reopened.asnapshot_at(checkpoint["id"])
            self.assertEqual(at_checkpoint, checkpoint["checkpoint"]["state"])
        self.assertEqual(await reopened.asnapshot_at(old_event), before)
        branch = await reopened.fork(checkpoint["id"], session_id="checkpoint-branch")
        self.assertEqual(branch.build_context(), at_checkpoint["messages"])
        self.assertEqual((await self.open("checkpoint-branch")).snapshot, branch.snapshot)
        with self.assertRaisesRegex(SessionError, "aread_records"):
            reopened.read_records()

    async def test_cold_checkpoint_reads_only_overlapping_archives(self):
        from unittest.mock import patch

        agent = await self.run_agent()
        await self.store.archive_session("org", "main")
        checkpoint = await agent.compact(summary="checkpoint in S3")
        await self.run_agent()
        await self.store.archive_session("org", "main")
        get = self.store.archive.get
        fetched = []

        async def tracked(manifest):
            self.assertGreaterEqual(manifest["end_seq"], checkpoint["seq"])
            fetched.append(manifest)
            return await get(manifest)

        with patch.object(self.store.archive, "get", side_effect=tracked):
            restored = await self.open("main")
            self.assertEqual(restored.snapshot, self.session.snapshot)
        self.assertTrue(fetched)
        self.assertEqual(len(await self.rows("agent_session_checkpoints")), 1)
        self.assertEqual(len(await self.rows("agent_model_usage")), 2)

    async def test_checkpoint_and_index_rollback_together(self):
        from unittest.mock import patch

        await self.run_agent()
        old_revision, old_state = self.session.revision, self.session.snapshot
        with patch("agent_runtime.storage.postgres.project", side_effect=RuntimeError("rollback")):
            with self.assertRaises(SessionError):
                await self.session.compact("failed publication", None)
        self.assertEqual(await self.rows("agent_session_checkpoints"), [])
        reopened = await self.open("main")
        self.assertEqual(reopened.revision, old_revision)
        self.assertEqual(reopened.snapshot, old_state)

    async def test_audit_idempotence_and_stats_survive_archive(self):
        await self.run_agent()
        event = dict(
            tenant_id="org",
            audit_id="request-1",
            actor_id="alice",
            actor_type="user",
            user_id="alice",
            action="input_submitted",
            resource_type="session",
            resource_id="main",
            outcome="success",
        )
        await self.store.audit(**event)
        await self.store.audit(**event)
        await self.store.archive_session("org", "main")
        start, end = (
            datetime.now(timezone.utc) - timedelta(days=1),
            datetime.now(timezone.utc) + timedelta(days=1),
        )
        usage = await self.store.daily_usage(
            "org", "alice", start, end, timezone_name="Asia/Shanghai"
        )
        self.assertEqual(usage[0]["total_tokens"], 16)
        self.assertEqual(
            (await self.store.daily_activity("org", "alice", start, end))[0]["inputs"], 1
        )
        self.assertEqual(
            (await self.store.daily_runs("org", "alice", start, end))[0]["completed"], 1
        )
        self.assertEqual(len(await self.store.audit_history("org", "alice", start, end)), 1)

    async def test_projection_failure_rolls_back_event_and_revision(self):
        original = self.ctx
        # Constraint failure in the retained fact must also roll back the source event.
        object.__setattr__(original, "source", "invalid")
        with self.assertRaises(SessionError):
            await self.run_agent()
        self.assertEqual(await self.rows("agent_session_events"), [])
        self.assertEqual(await self.rows("agent_runs"), [])

    async def test_upload_failure_keeps_hot_events(self):
        await self.run_agent()

        async def fail(*args):
            raise OSError("S3 unavailable")

        self.store.archive.put = fail
        with self.assertRaises(OSError):
            await self.store.archive_session("org", "main")
        self.assertEqual(len(await self.rows("agent_session_events")), self.session.revision)
        self.assertEqual(await self.rows("agent_session_archives"), [])

    async def test_unknown_usage_is_not_zero(self):
        await Agent(model=MODEL, session=self.session, stream_fn=self.provider(answer())).prompt(
            "hi"
        )
        usage = (await self.rows("agent_model_usage"))[0]
        self.assertIsNone(usage["total_tokens"])
        self.assertEqual(usage["usage_status"], "unknown")

    async def test_compaction_is_counted_once(self):
        agent = await self.run_agent()
        agent.stream_function = self.session.wrap_stream_fn(self.provider())
        from agent_runtime import CompactionSettings, Compactor

        agent.compactor = Compactor(CompactionSettings(keep_recent_tokens=0))
        await agent.compact()
        rows = await self.rows("agent_model_usage")
        self.assertEqual([r["purpose"] for r in rows], ["generation", "compaction"])

    async def test_retry_creates_new_attempt_not_new_run(self):
        entered = asyncio.Event()

        async def hanging(*args):
            entered.set()
            await asyncio.Event().wait()

        agent = Agent(model=MODEL, session=self.session, stream_fn=hanging)
        task = asyncio.create_task(agent.prompt("hi"))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        reopened = await self.open("main")
        await Agent(model=MODEL, session=reopened, stream_fn=self.provider()).resume()
        self.assertEqual(len(await self.rows("agent_runs")), 1)
        rows = await self.rows("agent_model_usage")
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(r["total_tokens"] or 0 for r in rows), 16)

    async def test_archive_publish_failure_retains_hot_rows(self):
        await self.run_agent()
        async with self.store.pool.connection() as conn:
            await conn.execute("ALTER TABLE agent_session_archives ADD CHECK (start_seq < 0)")
        with self.assertRaises(Exception):
            await self.store.archive_session("org", "main")
        self.assertEqual(len(await self.rows("agent_session_events")), self.session.revision)
        self.assertEqual(await self.rows("agent_session_archives"), [])
        self.assertTrue(self.s3.objects)  # Unreferenced uploaded object is harmless.

    async def test_missing_archive_fails_closed(self):
        await self.run_agent()
        await self.store.archive_session("org", "main")
        self.s3.objects.clear()
        with self.assertRaises(KeyError):
            await self.open("main")

    async def test_subagent_usage_attributed_to_root_without_double_count(self):
        from test_loop import call

        from agent_runtime.subagents import SubagentManager, create_subagent_tools

        children = {}

        def session_factory(key):
            if key not in children:
                children[key] = DatabaseSession(
                    self.store,
                    tenant_id="org",
                    audit_context=AuditContext("worker-service", source="system"),
                    session_id=key,
                )
            return children[key]

        parent = Agent(
            model=MODEL,
            session=self.session,
            stream_fn=self.provider(
                answer(calls=[call("spawn_agent", args={"name": "worker", "task": "research"})]),
                answer("done"),
            ),
        )
        manager = SubagentManager(
            parent,
            agent_factories={
                "worker": lambda session: Agent(
                    model=MODEL, session=session, stream_fn=self.provider()
                )
            },
            session_factory=session_factory,
        )
        parent.state.tools = create_subagent_tools(manager)
        try:
            await parent.prompt("delegate")
            child = manager.get()[0]
            await manager.wait(child["task_id"])
        finally:
            await manager.close()
        rows = await self.rows("agent_model_usage")
        self.assertEqual(len(rows), 3)
        self.assertEqual({r["user_id"] for r in rows}, {"alice"})
        self.assertEqual(len({r["root_run_id"] for r in rows}), 1)
        runs = await self.rows("agent_runs")
        self.assertEqual({r["source"] for r in runs}, {"user", "subagent"})

    async def test_fork_preserves_event_ids_and_future_resume_counts_only_new_calls(self):
        await self.run_agent()
        request = next(
            r for r in (await self.session.aread_records()) if r["type"] == "model_request"
        )
        child = await self.session.fork(request["id"], session_id="branch")
        self.assertEqual(
            (await child.aread_records())[0]["id"], (await self.session.aread_records())[0]["id"]
        )
        await Agent(model=MODEL, session=child, stream_fn=self.provider()).resume()
        self.assertEqual(len(await self.rows("agent_model_usage")), 2)

    async def test_database_failure_cannot_publish_partial_fork(self):
        await self.run_agent()
        target = await self.open("branch")
        await self.run_agent(target)
        before = await target.aread_records()
        with self.assertRaises(SessionError):
            await self.session.fork_into((await self.session.aread_records())[-1]["id"], target)
        self.assertEqual(await (await self.open("branch")).aread_records(), before)

    async def test_model_failure_with_reported_usage_is_retained(self):
        message = answer(reason="error")
        message["usage"] = dict(input=3, output=0, cacheRead=0, cacheWrite=0, totalTokens=3)
        message["usageStatus"] = "known"
        await Agent(model=MODEL, session=self.session, stream_fn=self.provider(message)).prompt(
            "hi"
        )
        usage = (await self.rows("agent_model_usage"))[0]
        self.assertEqual(usage["status"], "failed")
        self.assertEqual(usage["total_tokens"], 3)

    async def test_readonly_archive_reader_works_after_uploads_disabled(self):
        await self.run_agent()
        await self.store.archive_session("org", "main")
        self.store.archive = S3Archive(ArchiveConfig(), client=self.s3)
        reopened = await self.open("main")
        self.assertEqual(reopened.snapshot, self.session.snapshot)
        with self.assertRaises(ValueError):
            await self.store.archive_session("org", "main")


@unittest.skipUnless(importlib.util.find_spec("boto3"), "Install the s3 extra")
class AWSRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_boto3_request_shape_without_network(self):
        import hashlib

        import boto3
        from botocore.response import StreamingBody
        from botocore.stub import ANY, Stubber

        from agent_runtime.storage.s3 import encode_records

        config = ArchiveTests().config()
        client = boto3.client(
            "s3", region_name=config.region, aws_access_key_id="test", aws_secret_access_key="test"
        )
        records = [{"seq": 0, "id": "event", "data": {}}]
        body = encode_records(records)
        digest = hashlib.sha256(body).hexdigest()
        key = f"history/org/main/0-0-{digest}.jsonl.gz"
        with Stubber(client) as stub:
            stub.add_response(
                "put_object",
                {
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": config.kms_key_arn,
                    "VersionId": "v1",
                },
                {
                    "Bucket": "test",
                    "Key": key,
                    "Body": ANY,
                    "IfNoneMatch": "*",
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": config.kms_key_arn,
                    "ContentType": "application/gzip",
                    "Metadata": {"sha256": digest},
                },
            )
            stub.add_response(
                "get_object",
                {"Body": StreamingBody(io.BytesIO(body), len(body))},
                {"Bucket": "test", "Key": key, "VersionId": "v1"},
            )
            manifest = await S3Archive(config, client=client).put("org", "main", records)
            self.assertEqual(manifest["version_id"], "v1")
            stub.assert_no_pending_responses()
        client.close()
