"""Background composition and crash boundaries, without live model calls."""

import asyncio
import tempfile
import unittest

from test_loop import MODEL, FakeProvider, answer, call

from agent_runtime import Agent, LocalSession, SessionError
from agent_runtime.subagents import SubagentManager, create_subagent_tools


class SubagentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.sessions = lambda key: LocalSession(key, self.directory.name)
        self.parent = Agent(model=MODEL, session=self.sessions("parent"), stream_fn=FakeProvider())
        self.providers = []
        self.gate = asyncio.Event()
        self.gate.set()

        def factory(session):
            provider = FakeProvider(answer("child answer"), answer("next answer"))
            self.providers.append(provider)

            async def stream(*args):
                await self.gate.wait()
                return provider(*args)

            return Agent(model=MODEL, session=session, stream_fn=stream)

        self.factories = {"worker": factory}
        self.manager = self.make_manager()
        self.addAsyncCleanup(self.manager.close)

    def make_manager(self):
        return SubagentManager(
            self.parent, agent_factories=self.factories, session_factory=self.sessions
        )

    async def test_background_parallel_wait_and_isolation(self):
        self.gate.clear()
        first = await self.manager.spawn("worker", "one", operation_id="a")
        second = await self.manager.spawn("worker", "two", operation_id="b")
        self.assertNotEqual(first["task_id"], second["task_id"])
        self.assertEqual(
            (await self.manager.wait(first["task_id"], timeout=0))["status"], "running"
        )
        self.gate.set()
        for item in (first, second):
            self.assertEqual((await self.manager.wait(item["task_id"]))["status"], "completed")
        self.assertEqual(self.parent.session.revision, 0)
        self.assertEqual(len(self.providers), 2)
        self.assertEqual(self.providers[0].contexts[0][0]["content"][0]["text"], "one")
        self.assertEqual(self.providers[1].contexts[0][0]["content"][0]["text"], "two")

    async def test_creation_only_recovery_and_duplicate_spawn(self):
        key = self.manager.task_id("a")
        session = self.sessions(key)
        await session.commit(
            "subagent_created",
            parent_session_id="parent",
            operation_id="a",
            name="worker",
            task="initial",
        )
        item = await self.manager.spawn("worker", "initial", operation_id="a")
        self.assertEqual(item["status"], "needs_recovery")
        self.assertEqual(self.providers, [])
        await self.manager.resume(key)
        await self.manager.wait(key)
        await self.manager.spawn("worker", "initial", operation_id="a")
        self.assertEqual(len(self.providers), 1)
        self.assertEqual(
            sum(r["type"] == "run_started" for r in self.sessions(key).read_records()), 1
        )

    async def test_interrupted_run_does_not_repeat_initial_input(self):
        self.gate.clear()
        item = await self.manager.spawn("worker", "initial", operation_id="a")
        key = item["task_id"]
        while not self.manager._child(key).session.resumable:
            await asyncio.sleep(0)
        await self.manager.stop(key)
        reopened = self.make_manager()
        self.addAsyncCleanup(reopened.close)
        self.assertEqual(reopened.get(key)["status"], "needs_recovery")
        await reopened.wait(key, timeout=0)
        self.assertEqual(len(self.providers), 1)
        self.gate.set()
        await reopened.resume(key)
        self.assertEqual((await reopened.wait(key))["status"], "completed")
        records = self.sessions(key).read_records()
        self.assertEqual(sum(r["type"] == "run_started" for r in records), 1)
        self.assertEqual(sum(m["role"] == "user" for m in self.sessions(key).build_context()), 1)

    async def test_completed_child_reopen_and_next_message(self):
        item = await self.manager.spawn("worker", "initial", operation_id="a")
        key = item["task_id"]
        await self.manager.wait(key)
        reopened = self.make_manager()
        self.addAsyncCleanup(reopened.close)
        self.assertEqual(reopened.get(key)["status"], "completed")
        await reopened.send(key, "next")
        self.assertEqual((await reopened.wait(key))["status"], "completed")
        self.assertEqual(sum(m["role"] == "user" for m in self.sessions(key).build_context()), 2)

    async def test_tool_integration_and_parent_discovery(self):
        self.gate.clear()
        self.parent.stream_function = FakeProvider(
            answer(calls=[call("spawn_agent", args={"name": "worker", "task": "research"})]),
            answer("parent finished"),
        )
        self.parent.state.tools = create_subagent_tools(self.manager)
        await self.parent.prompt("delegate")
        item = self.manager.get()[0]
        self.assertEqual(item["status"], "running")
        self.gate.set()
        await self.manager.wait(item["task_id"])
        reopened = self.make_manager()
        self.assertEqual(reopened.get()[0]["status"], "completed")
        self.assertEqual(reopened.get()[0]["task_id"], item["task_id"])
        policies = {t.name: t.replay for t in create_subagent_tools(self.manager)}
        self.assertEqual(policies["stop_agent"], "never")
        self.assertEqual(policies["send_agent_message"], "never")

    async def test_cancel_wait_does_not_cancel_child_and_stop_all(self):
        self.gate.clear()
        item = await self.manager.spawn("worker", "initial", operation_id="a")
        key = item["task_id"]
        waiter = asyncio.create_task(self.manager.wait(key))
        await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(self.manager.get(key)["status"], "running")
        await self.manager.stop_all()
        self.assertNotEqual(self.manager.get(key)["status"], "running")
        with self.assertRaises(SessionError):
            await self.manager.send(key, "new")

    async def test_creation_failure_surfaces_and_no_agent_starts(self):
        class Broken(LocalSession):
            async def _persist(self, seq, record):
                raise OSError("disk full")

        manager = SubagentManager(
            self.parent,
            agent_factories=self.factories,
            session_factory=lambda key: Broken(key, directory=None),
        )
        self.addAsyncCleanup(manager.close)
        with self.assertRaisesRegex(SessionError, "disk full"):
            await manager.spawn("worker", "initial", operation_id="a")
        self.assertEqual(self.providers, [])

    async def test_running_input_is_consumed_at_safe_boundary(self):
        self.gate.clear()
        item = await self.manager.spawn("worker", "initial", operation_id="a")
        key = item["task_id"]
        await self.manager.send(key, "additional", follow_up=True)
        self.gate.set()
        self.assertEqual((await self.manager.wait(key))["status"], "completed")
        users = [m for m in self.sessions(key).build_context() if m["role"] == "user"]
        self.assertEqual(len(users), 2)
        self.assertEqual(users[-1]["content"][0]["text"], "additional")

    async def test_parent_resume_before_spawn_result_does_not_duplicate(self):
        self.parent.stream_function = FakeProvider(
            answer(calls=[call("spawn_agent", args={"name": "worker", "task": "research"})])
        )
        self.parent.state.tools = create_subagent_tools(self.manager)
        original = self.parent.session.tool_returned

        async def interrupt(*args):
            raise asyncio.CancelledError

        self.parent.session.tool_returned = interrupt
        with self.assertRaises(asyncio.CancelledError):
            await self.parent.prompt("delegate")
        item = self.manager.get()[0]
        await self.manager.wait(item["task_id"])
        self.parent.session.tool_returned = original
        self.parent.stream_function = FakeProvider(answer("resumed"))
        await self.parent.resume()
        self.assertEqual(len(self.manager.get()), 1)
        self.assertEqual(len(self.providers), 1)
        records = self.sessions(item["task_id"]).read_records()
        self.assertEqual(sum(r["type"] == "run_started" for r in records), 1)

    async def test_unknown_tool_outcome_is_not_replayed(self):
        from agent_runtime import AgentTool, AgentToolResult

        invoked = asyncio.Event()
        executions = []

        async def effect(call_id, args, signal, update):
            executions.append(call_id)
            invoked.set()
            await asyncio.Event().wait()
            return AgentToolResult()

        def factory(session):
            return Agent(
                model=MODEL,
                session=session,
                tools=[AgentTool("effect", "External effect", {"type": "object"}, effect)],
                stream_fn=FakeProvider(answer(calls=[call("effect", args={})])),
            )

        self.manager.agent_factories["worker"] = factory
        item = await self.manager.spawn("worker", "act", operation_id="a")
        await invoked.wait()
        await self.manager.stop(item["task_id"])
        await self.manager.resume(item["task_id"])
        state = await self.manager.wait(item["task_id"])
        self.assertEqual(state["session_status"], "waiting_recovery")
        self.assertEqual(executions, ["call-1"])
        self.assertIn("unknown", state["error"])

    async def test_child_failure_does_not_stop_sibling(self):
        def failing(session):
            return Agent(
                model=MODEL, session=session, stream_fn=FakeProvider(answer(reason="error"))
            )

        self.manager.agent_factories["broken"] = failing
        bad = await self.manager.spawn("broken", "fail", operation_id="bad")
        good = await self.manager.spawn("worker", "work", operation_id="good")
        self.assertEqual((await self.manager.wait(bad["task_id"]))["status"], "failed")
        self.assertEqual((await self.manager.wait(good["task_id"]))["status"], "completed")

    async def test_stop_all_interrupts_parent_waiting_on_child(self):
        self.gate.clear()
        item = await self.manager.spawn("worker", "initial", operation_id="a")
        self.parent.state.tools = create_subagent_tools(self.manager)
        self.parent.stream_function = FakeProvider(
            answer(calls=[call("wait_agent", args={"task_id": item["task_id"]})])
        )
        waiting = asyncio.Event()

        def observe(event, signal):
            if event["type"] == "tool_execution_start":
                waiting.set()

        self.parent.subscribe(observe)
        parent_task = asyncio.create_task(self.parent.prompt("wait"))
        await asyncio.wait_for(waiting.wait(), 2)
        await asyncio.wait_for(self.manager.stop_all(), 2)
        await parent_task
        self.assertFalse(self.parent.state.is_streaming)
        self.assertNotEqual(self.manager.get(item["task_id"])["status"], "running")

    async def test_events_are_tagged_and_do_not_write_parent(self):
        events = []
        unsubscribe = self.manager.subscribe(lambda event, signal: events.append(event))
        item = await self.manager.spawn("worker", "initial", operation_id="a")
        await self.manager.wait(item["task_id"])
        unsubscribe()
        self.assertTrue(events)
        self.assertTrue(all(e["task_id"] == item["task_id"] for e in events))
        self.assertIn("agent_end", [e["event"]["type"] for e in events])
        self.assertEqual(self.parent.session.revision, 0)
