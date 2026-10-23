"""Offline nested scheduling checks using real agents, sessions and executors.

The small test ledger supplies only the existing store interface; PostgreSQL
transaction/fencing guarantees remain covered by the PostgreSQL integration suite.
"""

import asyncio
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from agent_runtime import (
    Agent,
    AgentTool,
    AgentToolResult,
    AssistantMessageEventStream,
    DatabaseSession,
    Model,
    SessionError,
    assistant_message,
)
from agent_runtime.execution import DurableExecutor, ExecutionHost
from agent_runtime.storage.coordination import (
    CommandConflictError,
    ExecutionLease,
    ExecutionLeaseError,
    command_transition,
)
from agent_runtime.subagents import DurableSubagentManager, create_subagent_tools
from agent_runtime.types import maybe_await

MODEL = Model("controlled-offline", "local", "nested-test", "")


def response(model):
    stream = AssistantMessageEventStream()
    stream.push(
        {
            "type": "done",
            "message": assistant_message(
                model, content=[{"type": "text", "text": "done"}], stopReason="stop"
            ),
        }
    )
    return stream


class TestLedger:
    """Isolated journal/control fixture, never a production storage backend."""

    def __init__(self):
        self.commands = {}
        self.records = {}
        self.leases = {}
        self.owners = {}
        self.fences = {}

    async def accept_command(self, tenant, sid, *, user_id, command_id, kind, payload, run_id=None):
        owner = self.owners.setdefault((tenant, sid), user_id)
        if owner != user_id:
            raise CommandConflictError("Session owner changed")
        key = tenant, command_id
        if key in self.commands:
            prior = self.commands[key]
            if (prior["session_id"], prior["kind"], prior["payload"]) != (sid, kind, payload):
                raise CommandConflictError("Command input changed")
            return deepcopy(prior)
        command = {
            "tenant_id": tenant,
            "session_id": sid,
            "command_id": command_id,
            "command_seq": 1 + len(await self.list_commands(tenant, sid)),
            "kind": kind,
            "payload": deepcopy(payload),
            "run_id": run_id or (uuid4().hex if kind == "prompt" else None),
            "state": "accepted",
            "completed_event_id": None,
        }
        self.commands[key] = command
        return deepcopy(command)

    async def get_command(self, tenant, command_id):
        return deepcopy(self.commands.get((tenant, command_id)))

    async def get_commands(self, keys):
        return {key: deepcopy(self.commands.get(key)) for key in keys}

    async def list_commands(self, tenant, sid, *, after_seq=0, limit=100, **filters):
        return deepcopy(
            sorted(
                (
                    c
                    for c in self.commands.values()
                    if c["tenant_id"] == tenant
                    and c["session_id"] == sid
                    and c["command_seq"] > after_seq
                    and all(c.get(k) == v for k, v in filters.items())
                ),
                key=lambda c: c["command_seq"],
            )[:limit]
        )

    async def list_child_commands(self, tenant, parent, *, limit=100, after=None):
        children = [
            c
            for c in self.commands.values()
            if c["tenant_id"] == tenant
            and c["kind"] == "prompt"
            and c["payload"].get("subagent", {}).get("parent_session_id") == parent
            and (after is None or (c["session_id"], c["command_seq"]) > after)
        ]
        return deepcopy(sorted(children, key=lambda c: (c["session_id"], c["command_seq"]))[:limit])

    async def count_run_children(self, tenant, parent, run_id):
        return len(
            {
                c["session_id"]
                for c in await self.list_child_commands(tenant, parent, limit=1000)
                if c["payload"]["subagent"].get("parent_run_id") == run_id
            }
        )

    async def read_recovery_records(self, tenant, sid):
        return deepcopy(self.records.get((tenant, sid), []))

    async def append(self, session, record):
        await self.assert_session_execution(session)
        key = session.info.tenant_id, session.session_id
        records = self.records.setdefault(key, [])
        if record["seq"] != len(records):
            raise AssertionError("Concurrent journal writer")
        records.append(deepcopy(record))
        transition = command_transition(record)
        if transition:
            command_id, state, run_id = transition
            command = next(
                c
                for c in self.commands.values()
                if c["tenant_id"] == key[0]
                and c["session_id"] == key[1]
                and (c["command_id"] == command_id if command_id else c["run_id"] == run_id)
            )
            command["state"] = state
            if state in ("completed", "failed", "stopped"):
                command["completed_event_id"] = record["id"]

    async def acquire_execution(self, tenant, sid, *, holder, ttl_seconds):
        key = tenant, sid
        if key in self.leases:
            return None
        fence = self.fences.get(key, 0) + 1
        self.fences[key] = fence
        lease = ExecutionLease(
            tenant,
            sid,
            holder,
            fence,
            datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
        )
        self.leases[key] = lease
        return lease

    async def assert_execution(self, lease):
        if self.leases.get((lease.tenant_id, lease.session_id)) != lease:
            raise ExecutionLeaseError("Stale test lease")

    async def assert_session_execution(self, session):
        if session.execution_lease:
            await self.assert_execution(session.execution_lease)

    async def release_execution(self, lease):
        await self.assert_execution(lease)
        self.leases.pop((lease.tenant_id, lease.session_id))

    async def request_cancel(self, tenant, command_id):
        self.commands[tenant, command_id]["cancel_requested_at"] = datetime.now(timezone.utc)
        return await self.get_command(tenant, command_id)


class NestedDurableChecks(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = TestLedger()
        self.host = ExecutionHost(self.store, max_active_children=8)
        self.addAsyncCleanup(self.host.close)
        self.managers = {}
        self.agents = {}
        self.provider = lambda session, model, context, options: response(model)

        async def capability(call_id, args, signal, update):
            return AgentToolResult()

        self.capability = AgentTool("capability", "Host capability", {"type": "object"}, capability)
        await DurableExecutor(self.store, self.factory, host=self.host).execute(
            "tenant", "root", user_id="owner", command_id="root", message="root-only context"
        )
        self.root = self.managers["root"]

    def factory(self, session, **limits):
        async def provider(model, context, options):
            return await maybe_await(self.provider(session, model, context, options))

        agent = Agent(model=MODEL, session=session, tools=[self.capability], stream_fn=provider)
        manager = DurableSubagentManager(
            agent, agent_factories={"worker": self.factory}, host=self.host, **limits
        )
        agent.state.tools.extend(create_subagent_tools(manager))
        self.managers[session.session_id] = manager
        self.agents[session.session_id] = agent
        self.addAsyncCleanup(manager.close)
        return agent

    async def completed(self, manager, task_id):
        result = await manager.wait(task_id, timeout=2)
        self.assertEqual(result["status"], "completed", result)
        return result

    async def test_nested_tools_ownership_context_and_direct_parent_scope(self):
        parent = self.root
        root_run_id = parent.parent.session.root_run_id
        for depth in range(1, 4):
            child = await parent.spawn("worker", f"depth {depth}", operation_id=f"depth-{depth}")
            await self.completed(parent, child["task_id"])
            agent = self.agents[child["task_id"]]
            origin = agent.session.subagent_creation
            self.assertEqual(origin["depth"], depth)
            self.assertEqual(origin["parent_session_id"], parent.parent.session.session_id)
            self.assertEqual(origin["parent_run_id"], parent.parent.session.snapshot["run_id"])
            self.assertEqual(origin["root_run_id"], root_run_id)
            self.assertEqual(
                (agent.session.info.tenant_id, agent.session.info.user_id), ("tenant", "owner")
            )
            self.assertIs(agent.state.tools[0], self.capability)
            self.assertEqual(
                agent.session.snapshot["tools"], self.agents["root"].session.snapshot["tools"]
            )
            self.assertNotIn("root-only context", str(agent.session.build_context()))
            self.assertEqual([c["task_id"] for c in await parent.aget()], [child["task_id"]])
            if depth > 1:
                with self.assertRaisesRegex(ValueError, "Unknown child"):
                    await self.root.aget(child["task_id"])
            notices = await self.store.list_commands(
                "tenant", parent.parent.session.session_id, kind="child_result"
            )
            self.assertEqual(notices[0]["payload"]["child_session_id"], child["task_id"])
            parent = self.managers[child["task_id"]]
        with self.assertRaisesRegex(SessionError, "depth"):
            await parent.spawn("worker", "too deep", operation_id="depth-4")
        parent.max_depth = 4
        child = await parent.spawn("worker", "configured depth", operation_id="depth-4")
        await self.completed(parent, child["task_id"])
        self.assertEqual(self.agents[child["task_id"]].session.subagent_creation["depth"], 4)

    async def test_limits_are_per_parent_and_accepted_replay_survives_lowered_depth(self):
        self.root.max_children = 1
        first = await self.root.spawn("worker", "first", operation_id="first")
        await self.completed(self.root, first["task_id"])
        with self.assertRaisesRegex(SessionError, "parent run"):
            await self.root.spawn("worker", "second", operation_id="second")
        nested = self.managers[first["task_id"]]
        grandchild = await nested.spawn("worker", "nested", operation_id="nested")
        await self.completed(nested, grandchild["task_id"])
        nested.max_children = 1
        nested.max_depth = 1
        replay = await nested.spawn("worker", "nested", operation_id="nested")
        self.assertEqual(replay["command_id"], grandchild["command_id"])
        with self.assertRaises(CommandConflictError):
            await nested.spawn("worker", "changed", operation_id="nested")
        with self.assertRaisesRegex(SessionError, "depth"):
            await nested.spawn("worker", "new", operation_id="new")

    async def test_child_budget_resets_for_a_new_run_in_the_same_parent_session(self):
        self.root.max_children = 1
        child = await self.root.spawn("worker", "first run", operation_id="first")
        await self.completed(self.root, child["task_id"])
        await DurableExecutor(self.store, self.factory, host=self.host).execute(
            "tenant", "root", user_id="owner", command_id="root-second", message="second run"
        )
        current = self.managers["root"]
        current.max_children = 1
        child = await current.spawn("worker", "second run", operation_id="second")
        await self.completed(current, child["task_id"])
        with self.assertRaisesRegex(SessionError, "parent run"):
            await current.spawn("worker", "excess", operation_id="excess")

    async def test_subagent_continuation_prompts_do_not_consume_new_child_allowances(self):
        self.root.max_children = 2
        child = await self.root.spawn("worker", "first child", operation_id="first")
        await self.completed(self.root, child["task_id"])
        origin = self.agents[child["task_id"]].session.subagent_creation
        executor = DurableExecutor(self.store, self.factory, host=self.host, child=True)
        for index in range(3):
            command_id = f"parent-continuation-{index}"
            await self.store.accept_command(
                "tenant",
                child["task_id"],
                user_id="owner",
                command_id=command_id,
                kind="prompt",
                payload={"message": f"child result {index}", "subagent": origin},
            )
            result = await executor.execute_command(
                "tenant", child["task_id"], user_id="owner", command_id=command_id
            )
            self.assertEqual(result["status"], "completed")
        run_id = self.root.parent.session.snapshot["run_id"]
        self.assertEqual(await self.store.count_run_children("tenant", "root", run_id), 1)
        second = await self.root.spawn("worker", "second child", operation_id="second")
        await self.completed(self.root, second["task_id"])
        self.assertEqual(await self.store.count_run_children("tenant", "root", run_id), 2)
        with self.assertRaisesRegex(SessionError, "parent run"):
            await self.root.spawn("worker", "excess", operation_id="excess")

    async def test_depth_configuration_rejects_invalid_limits(self):
        for invalid in (0, -1, True, 1.5, None):
            with self.subTest(max_depth=invalid), self.assertRaisesRegex(ValueError, "positive"):
                DurableSubagentManager(
                    self.root.parent,
                    agent_factories={"worker": self.factory},
                    host=self.host,
                    max_depth=invalid,
                )

    async def test_legacy_origin_is_depth_one_and_legacy_replay_keeps_payload(self):
        legacy = DatabaseSession(self.store, tenant_id="tenant", user_id="owner", session_id="old")
        await legacy.commit("subagent_created", parent_session_id="root", name="worker")
        await DurableExecutor(self.store, self.factory, host=self.host, child=True).execute(
            "tenant", "old", user_id="owner", command_id="old", message="legacy"
        )
        manager = self.managers["old"]
        child = await manager.spawn("worker", "new", operation_id="new")
        await self.completed(manager, child["task_id"])
        self.assertEqual(self.agents[child["task_id"]].session.subagent_creation["depth"], 2)
        task_id = manager.task_id("legacy-acceptance")
        origin = {
            "parent_session_id": "old",
            "parent_run_id": manager.parent.session.snapshot["run_id"],
            "root_run_id": manager.parent.session.root_run_id,
            "name": "worker",
            "task": "accepted before upgrade",
            "operation_id": "legacy-acceptance",
        }
        accepted = await self.store.accept_command(
            "tenant",
            task_id,
            user_id="owner",
            command_id=f"subagent-start:{task_id}",
            kind="prompt",
            payload={"message": origin["task"], "subagent": origin},
        )
        manager.max_depth = 1
        replay = await manager.spawn("worker", origin["task"], operation_id="legacy-acceptance")
        self.assertEqual(replay["command_id"], accepted["command_id"])
        self.assertIsNone(manager._children[task_id].task)
        self.assertNotIn(
            "depth",
            (await self.store.get_command("tenant", replay["command_id"]))["payload"]["subagent"],
        )

    async def test_full_child_pool_accepts_nested_spawns_then_runs_them_after_parents_yield(self):
        entered, spawned, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        active, grandchildren = [], []

        async def provider(session, model, context, options):
            if session.subagent_creation["depth"] == 1:
                active.append(session.session_id)
                if len(active) == 8:
                    entered.set()
                await entered.wait()
                manager = self.managers[session.session_id]
                grandchild = await manager.spawn("worker", "grandchild", operation_id="nested")
                self.assertEqual(grandchild["status"], "queued")
                grandchildren.append((manager, grandchild))
                if len(grandchildren) == 8:
                    spawned.set()
                await release.wait()
            return response(model)

        self.provider = provider
        children = [
            await self.root.spawn("worker", f"child {i}", operation_id=f"child-{i}")
            for i in range(8)
        ]
        await asyncio.wait_for(spawned.wait(), 2)
        self.assertEqual(len(active), 8)
        self.assertEqual(len(self.host._active), 8)
        for manager, child in grandchildren:
            self.assertEqual((await manager.aget(child["task_id"]))["status"], "queued")
            self.assertNotIn(child["task_id"], self.agents)
        release.set()
        for child in children:
            await self.completed(self.root, child["task_id"])
        for manager, child in grandchildren:
            await self.completed(manager, child["task_id"])

    async def test_accepted_queued_work_recovers_with_same_command_run_and_origin(self):
        # Occupying the pool prevents even the child's factory from running.
        for _ in range(8):
            await self.host.slot(child=True).acquire()
        child = await self.root.spawn("worker", "recover queued", operation_id="recover")
        self.assertEqual(child["status"], "queued")
        await self.root.close()
        accepted = await self.store.get_command("tenant", child["command_id"])
        self.assertEqual(accepted["state"], "accepted")
        self.assertNotIn(child["task_id"], self.agents)
        parent = await DatabaseSession.open(self.store, tenant_id="tenant", session_id="root")
        self.factory(parent)
        recovered = self.managers["root"]
        await recovered.recover(resume=True)
        self.assertEqual((await recovered.aget(child["task_id"]))["status"], "queued")
        for _ in range(8):
            self.host.slot(child=True).release()
        finished = await self.completed(recovered, child["task_id"])
        self.assertEqual(
            (finished["command_id"], finished["run_id"]), (child["command_id"], child["run_id"])
        )
        self.assertEqual(
            self.agents[child["task_id"]].session.subagent_creation, accepted["payload"]["subagent"]
        )

    async def test_queued_factory_and_follow_up_do_not_report_previous_run_as_running(self):
        factory_entered, open_factory = asyncio.Event(), asyncio.Event()
        calls = []

        async def blocked_factory(session):
            factory_entered.set()
            await open_factory.wait()
            return self.factory(session)

        self.root.agent_factories["worker"] = blocked_factory
        child = await self.root.spawn("worker", "first", operation_id="slow-factory")
        await asyncio.wait_for(factory_entered.wait(), 2)
        self.assertEqual((await self.root.aget(child["task_id"]))["status"], "queued")
        open_factory.set()
        await self.completed(self.root, child["task_id"])
        open_factory.clear()
        factory_entered.clear()
        queued = await self.root.send(child["task_id"], "second")
        await asyncio.wait_for(factory_entered.wait(), 2)
        state = await self.root.aget(child["task_id"])
        self.assertEqual(state["command_id"], queued["queued_command_id"])
        self.assertEqual(state["status"], "queued")
        running = asyncio.Event()
        finish = asyncio.Event()

        async def provider(session, model, context, options):
            calls.append(session.snapshot["run_id"])
            running.set()
            await finish.wait()
            return response(model)

        self.provider = provider
        open_factory.set()
        await asyncio.wait_for(running.wait(), 2)
        self.assertEqual((await self.root.aget(child["task_id"]))["status"], "running")
        finish.set()
        await self.completed(self.root, child["task_id"])
        self.assertEqual(calls, [state["run_id"]])

    async def test_stop_queued_child_does_not_wait_for_a_free_pool_slot(self):
        for _ in range(8):
            await self.host.slot(child=True).acquire()
        try:
            child = await self.root.spawn("worker", "stop queued", operation_id="stop")
            # Allow the driver to begin waiting on its semaphore.
            await asyncio.sleep(0)
            stopped = await asyncio.wait_for(self.root.stop(child["task_id"]), 1)
            self.assertEqual(stopped["status"], "stopped")
            self.assertNotIn(child["task_id"], self.agents)
            command = await self.store.get_command("tenant", child["command_id"])
            self.assertEqual(command["state"], "rejected")
        finally:
            for _ in range(8):
                self.host.slot(child=True).release()


if __name__ == "__main__":
    unittest.main()
