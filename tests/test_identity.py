"""Conversation ownership is shared across backends, replay and composition."""

import json
import tempfile
import unittest
from dataclasses import FrozenInstanceError

from test_loop import MODEL, FakeProvider, answer
from test_sessions import ProcessLost

from agent_runtime import Agent, LocalSession, SessionError, SessionInfo
from agent_runtime.subagents import SubagentManager


class IdentityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def session(self, name="main", **kwargs):
        return LocalSession(name, self.directory.name, **kwargs)

    async def test_identity_roundtrip_checkpoint_reset_and_not_model_input(self):
        session = self.session(tenant_id="tenant-secret", user_id="user-secret")
        provider = FakeProvider(answer())
        agent = Agent(model=MODEL, session=session, stream_fn=provider)
        self.assertEqual(session.info, SessionInfo("main", "tenant-secret", "user-secret"))
        with self.assertRaises(FrozenInstanceError):
            session.info.user_id = "changed"
        with self.assertRaises(AttributeError):
            session.session_id = "changed"
        await agent.prompt("hello")
        self.assertNotIn("user-secret", json.dumps(provider.contexts))
        self.assertEqual(session.read_records()[0]["type"], "session_created")
        await agent.compact(summary="remember hello")
        self.assertEqual(self.session().info, session.info)
        await agent.reset_session()
        self.assertEqual(self.session().info, session.info)
        with self.assertRaisesRegex(SessionError, "identity"):
            self.session(user_id="another-user")
        with self.assertRaisesRegex(SessionError, "identity"):
            self.session(tenant_id="another-tenant")
        with self.assertRaises(SessionError):
            await session.commit("session_created", info={"session_id": "main", "user_id": "other"})

    async def test_fork_and_subagent_inherit_owner(self):
        parent = Agent(
            model=MODEL,
            session=self.session(tenant_id="org", user_id="alice"),
            stream_fn=FakeProvider(answer()),
        )
        await parent.prompt("hi")
        node = parent.session.read_records()[-1]["id"]
        fork = await parent.session.fork(node, session_id="branch")
        self.assertEqual(fork.info, SessionInfo("branch", "org", "alice"))
        self.assertEqual(self.session("branch").info, fork.info)
        self.assertNotEqual(fork.root_run_id, parent.session.root_run_id)
        manager = SubagentManager(
            parent,
            agent_factories={
                "worker": lambda s: Agent(model=MODEL, session=s, stream_fn=FakeProvider(answer()))
            },
            session_factory=self.session,
        )
        try:
            child = await manager.spawn("worker", "work", operation_id="one")
            await manager.wait(child["task_id"])
            restored = self.session(child["task_id"])
            self.assertEqual(restored.info.user_id, "alice")
            self.assertEqual(restored.info.tenant_id, "org")
            self.assertEqual(restored.root_run_id, parent.session.root_run_id)
            self.assertEqual(restored.run_source, "subagent")
        finally:
            await manager.close()
        wrong = self.session("wrong", tenant_id="org", user_id="bob")
        with self.assertRaisesRegex(SessionError, "ownership"):
            await parent.session.fork_into(node, wrong)
        self.assertEqual(wrong.revision, 0)

    async def test_creation_survives_interruption_before_first_run(self):
        class Interrupted(LocalSession):
            async def _persist(self, seq, record):
                if record["type"] == "run_started":
                    raise ProcessLost()
                await super()._persist(seq, record)

        session = Interrupted("main", self.directory.name, tenant_id="org", user_id="alice")
        with self.assertRaises(ProcessLost):
            await Agent(model=MODEL, session=session, stream_fn=FakeProvider()).prompt("hi")
        restored = self.session()
        self.assertEqual(restored.info, session.info)
        self.assertEqual(restored.revision, 1)
        self.assertFalse(restored.resumable)

    async def test_anonymous_local_and_invalid_first_commit(self):
        session = self.session()
        original = session.snapshot
        with self.assertRaises(SessionError):
            await session.commit("unknown")
        self.assertEqual(session.revision, 0)
        self.assertEqual(session.snapshot, original)
        await Agent(model=MODEL, session=session, stream_fn=FakeProvider(answer())).prompt("hello")
        self.assertIsNone(self.session().info.user_id)
        self.assertEqual(self.session().run_source, "system")
