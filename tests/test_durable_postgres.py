"""PostgreSQL durable execution, recovery and child delivery with an offline model."""

import asyncio

from postgres_fixture import PostgresCase

from agent_runtime import (
    Agent,
    AssistantMessageEventStream,
    DatabaseSession,
    Model,
    SessionError,
    assistant_message,
)
from agent_runtime.execution import DurableExecutor
from agent_runtime.storage.coordination import CommandConflictError, ExecutionLeaseError
from agent_runtime.subagents import DurableSubagentManager

MODEL = Model("controlled-offline", "local", "fake", "")


def response(model, text="durable result"):
    stream = AssistantMessageEventStream()
    stream.push({"type": "start", "partial": assistant_message(model)})
    stream.push(
        {
            "type": "done",
            "message": assistant_message(
                model, stopReason="stop", content=[{"type": "text", "text": text}]
            ),
        }
    )
    return stream


def factory(session):
    return Agent(
        model=MODEL, session=session, stream_fn=lambda model, context, options: response(model)
    )


class DurablePostgresChecks(PostgresCase):
    async def test_idempotent_acceptance_reopen_snapshot_and_fork(self):
        await self.store.check_schema()
        executor = DurableExecutor(self.store, factory)
        first = await executor.execute(
            "t", "s", user_id="u", command_id="c", message="hello\x00world"
        )
        duplicate = await executor.execute(
            "t", "s", user_id="u", command_id="c", message="hello\x00world"
        )
        self.assertEqual((first["status"], duplicate["status"]), ("completed", "completed"))
        self.assertEqual(len(await self.store.list_runs("t", session_id="s")), 1)
        with self.assertRaises(CommandConflictError):
            await executor.execute("t", "s", user_id="u", command_id="c", message="different")
        session = await DatabaseSession.open(self.store, tenant_id="t", session_id="s")
        events = await session.aread_records()
        self.assertEqual((await session.asnapshot_at(events[-1]["id"]))["status"], "completed")
        branch = await session.fork(events[-1]["id"], session_id="branch")
        self.assertEqual(branch.snapshot["messages"], session.snapshot["messages"])
        other = await self.open_store()
        try:
            reopened = await DatabaseSession.open(other, tenant_id="t", session_id="s")
            self.assertEqual(reopened.snapshot["status"], "completed")
        finally:
            await other.close()

    async def test_interrupted_command_resumes_same_run(self):
        entered = asyncio.Event()

        async def blocked(model, context, options):
            entered.set()
            await asyncio.Event().wait()

        executor = DurableExecutor(
            self.store, lambda session: Agent(model=MODEL, session=session, stream_fn=blocked)
        )
        work = asyncio.create_task(
            executor.execute("t", "s", user_id="u", command_id="c", message="hello")
        )
        await asyncio.wait_for(entered.wait(), 2)
        work.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await work
        accepted = await self.store.get_command("t", "c")
        recovered = await DurableExecutor(self.store, factory).execute(
            "t", "s", user_id="u", command_id="c", message="hello"
        )
        self.assertEqual(recovered["status"], "completed")
        self.assertEqual(recovered["run_id"], accepted["run_id"])
        self.assertEqual(len(await self.store.list_runs("t", session_id="s")), 1)

    async def test_lost_ownership_is_never_a_terminal_command_result(self):
        for wrapped in (False, True):
            with self.subTest(wrapped=wrapped):
                sid = f"lost-{wrapped}"

                def losing_factory(session):
                    agent = factory(session)

                    async def lose_ownership(message):
                        await self.store.release_execution(session.execution_lease)
                        try:
                            await self.store.assert_execution(session.execution_lease)
                        except ExecutionLeaseError as error:
                            if wrapped:
                                raise SessionError("Parallel tool batch interrupted") from error
                            raise

                    agent.prompt = lose_ownership
                    return agent

                executor = DurableExecutor(self.store, losing_factory)
                with self.assertRaises(ExecutionLeaseError):
                    await executor.execute("t", sid, user_id="u", command_id=sid, message="hi")
                command = await self.store.get_command("t", sid)
                self.assertEqual(command["state"], "accepted")
                recovered = await DurableExecutor(self.store, factory).execute_command(
                    "t", sid, user_id="u", command_id=sid
                )
                self.assertEqual(recovered["status"], "completed")

    async def test_fencing_blocks_old_and_unbound_writers(self):
        session = DatabaseSession(self.store, tenant_id="t", user_id="u", session_id="s")
        first = await self.store.acquire_execution("t", "s", holder="first")
        session.execution_lease = first
        await session.commit("context_replaced", messages=[])
        self.assertIsNone(await self.store.acquire_execution("t", "s", holder="second"))
        await self.store.release_execution(first)
        second = await self.store.acquire_execution("t", "s", holder="second")
        self.assertGreater(second.fence, first.fence)
        with self.assertRaises(ExecutionLeaseError):
            await self.store.assert_execution(first)
        stale = await DatabaseSession.open(self.store, tenant_id="t", session_id="s")
        with self.assertRaises(ExecutionLeaseError):
            await stale.assert_execution()
        with self.assertRaises(SessionError):
            await session.commit("context_replaced", messages=[])
        await self.store.release_execution(second)

    async def test_accepted_cancel_is_durable_and_does_not_start_model(self):
        await self.store.accept_command(
            "t", "s", user_id="u", command_id="c", kind="prompt", payload={"message": "hello"}
        )
        await self.store.request_cancel("t", "c")
        executor = DurableExecutor(
            self.store, lambda session: self.fail("Canceled command created an agent")
        )
        result = await executor.execute("t", "s", user_id="u", command_id="c", message="hello")
        self.assertEqual(result["status"], "stopped")
        self.assertEqual(await self.store.list_runs("t", session_id="s"), [])
        self.assertEqual((await self.store.get_command("t", "c"))["state"], "rejected")

    async def test_notification_does_not_block_prompt_and_can_be_acknowledged(self):
        await self.store.accept_command(
            "t",
            "s",
            user_id="u",
            command_id="notice",
            kind="child_result",
            payload={"child_session_id": "child"},
        )
        result = await DurableExecutor(self.store, factory).execute(
            "t", "s", user_id="u", command_id="prompt", message="hello"
        )
        self.assertEqual(result["status"], "completed")
        await self.store.ack_notification("t", "s", "notice")
        self.assertEqual((await self.store.list_notifications("t", "s"))[0]["state"], "completed")

    async def test_cross_worker_cancel_stops_inflight_adapter(self):
        entered = asyncio.Event()

        async def blocked(model, context, options):
            entered.set()
            await options["signal"].run(asyncio.Event().wait())

        executor = DurableExecutor(
            self.store,
            lambda session: Agent(model=MODEL, session=session, stream_fn=blocked),
            lease_seconds=0.3,
        )
        work = asyncio.create_task(
            executor.execute("t", "s", user_id="u", command_id="active", message="hello")
        )
        await asyncio.wait_for(entered.wait(), 2)
        other_worker = DurableExecutor(self.store, factory)
        await other_worker.request_cancel("t", "active")
        result = await asyncio.wait_for(work, 2)
        self.assertEqual(result["status"], "stopped")
        self.assertIsNotNone((await self.store.get_command("t", "active"))["cancel_requested_at"])
        await executor.abandon("t", "s", user_id="u", command_id="active")
        next_run = await other_worker.execute(
            "t", "s", user_id="u", command_id="next", message="next"
        )
        self.assertEqual(next_run["status"], "completed")

    async def test_child_recovery_waits_for_crashed_worker_lease_expiry(self):
        await DurableExecutor(self.store, factory).execute(
            "t", "parent", user_id="u", command_id="parent-prompt", message="hello"
        )
        parent_session = await DatabaseSession.open(self.store, tenant_id="t", session_id="parent")
        calls = []

        def counted_factory(session):
            calls.append(session.session_id)
            return factory(session)

        manager = DurableSubagentManager(
            factory(parent_session), agent_factories={"worker": counted_factory}, lease_seconds=0.2
        )
        self.addAsyncCleanup(manager.close)
        task_id = manager.task_id("crashed-owner")
        origin = {
            "parent_session_id": "parent",
            "parent_run_id": parent_session.snapshot["run_id"],
            "operation_id": "crashed-owner",
            "root_run_id": parent_session.root_run_id,
            "name": "worker",
            "task": "accepted before crash",
        }
        await self.store.accept_command(
            "t",
            task_id,
            user_id="u",
            command_id="leased-child",
            kind="prompt",
            payload={"message": "accepted before crash", "subagent": origin},
        )
        old_lease = await self.store.acquire_execution(
            "t", task_id, holder="crashed-pod", ttl_seconds=0.15
        )
        await manager.recover(resume=True)
        result = await manager.wait(task_id, timeout=2)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(await self.store.list_runs("t", session_id=task_id)), 1)
        self.assertEqual(len(await self.store.list_notifications("t", "parent")), 1)
        with self.assertRaises(ExecutionLeaseError):
            await self.store.assert_execution(old_lease)

    async def test_child_notification_survives_delivery_failure_for_central_dispatch(self):
        await DurableExecutor(self.store, factory).execute(
            "t", "parent", user_id="u", command_id="parent-prompt", message="hello"
        )
        parent_session = await DatabaseSession.open(self.store, tenant_id="t", session_id="parent")
        manager = DurableSubagentManager(
            factory(parent_session), agent_factories={"worker": factory}
        )
        self.addAsyncCleanup(manager.close)
        original_accept = self.store.accept_command
        acceptance_attempts = []

        async def fail_first_notification(*args, **kwargs):
            if kwargs.get("kind") == "child_result":
                acceptance_attempts.append(1)
                if len(acceptance_attempts) == 1:
                    raise RuntimeError("controlled transient notification DB failure")
            return await original_accept(*args, **kwargs)

        self.store.accept_command = fail_first_notification
        delivered = asyncio.Event()
        callbacks = []

        async def transient_receiver(notification):
            callbacks.append(notification["command_id"])
            if len(callbacks) == 1:
                raise RuntimeError("controlled transient product FIFO failure")
            await manager.ack_notification(notification["command_id"])
            delivered.set()

        manager.subscribe_completion(transient_receiver)
        child = await manager.spawn("worker", "work", operation_id="notification-retry")
        await manager.wait(child["task_id"], timeout=2)
        # Acceptance no longer depends on the callback: the child terminal event
        # and this parent inbox entry are one PG transaction, even though the
        # manager's optional delivery attempt above failed.
        pending = await self.store.pending_child_results(limit=100)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["state"], "accepted")
        with self.assertRaisesRegex(RuntimeError, "product FIFO"):
            await transient_receiver(pending[0])
        await transient_receiver((await self.store.pending_child_results())[0])
        await asyncio.wait_for(delivered.wait(), 2)
        notices = await self.store.list_notifications("t", "parent")
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0]["state"], "completed")
        self.assertEqual(len(acceptance_attempts), 1)
        self.assertEqual(len(callbacks), 2)
        self.assertEqual(len(await self.store.list_runs("t", session_id=child["task_id"])), 1)

    async def test_closing_manager_settles_lease_wait_without_canceling_accepted_work(self):
        await DurableExecutor(self.store, factory).execute(
            "t", "parent", user_id="u", command_id="parent-prompt", message="hello"
        )
        parent_session = await DatabaseSession.open(self.store, tenant_id="t", session_id="parent")
        manager = DurableSubagentManager(
            factory(parent_session), agent_factories={"worker": factory}
        )
        task_id = manager.task_id("still-owned")
        origin = {
            "parent_session_id": "parent",
            "parent_run_id": parent_session.snapshot["run_id"],
            "operation_id": "still-owned",
            "root_run_id": parent_session.root_run_id,
            "name": "worker",
            "task": "wait",
        }
        await self.store.accept_command(
            "t",
            task_id,
            user_id="u",
            command_id="waiting-child",
            kind="prompt",
            payload={"message": "wait", "subagent": origin},
        )
        lease = await self.store.acquire_execution(
            "t", task_id, holder="live-other-pod", ttl_seconds=30
        )
        await manager.recover(resume=True)
        await asyncio.wait_for(manager.close(), 1)
        self.assertTrue(manager._children[task_id].task.done())
        self.assertEqual((await self.store.get_command("t", "waiting-child"))["state"], "accepted")
        await self.store.assert_execution(lease)

    async def test_child_completion_commits_notification_before_callback(self):
        await DurableExecutor(self.store, factory).execute(
            "t", "parent", user_id="u", command_id="parent-prompt", message="hello"
        )
        parent_session = await DatabaseSession.open(self.store, tenant_id="t", session_id="parent")
        manager = DurableSubagentManager(
            factory(parent_session), agent_factories={"worker": factory}, store=self.store
        )
        self.addAsyncCleanup(manager.close)

        async def lose_notification(task_id, command):
            raise RuntimeError("controlled completion-before-notification crash")

        manager._notify = lose_notification
        spawned = await manager.spawn("worker", "child task", operation_id="stable-operation")
        await manager.wait(spawned["task_id"])
        self.assertEqual(len(await self.store.pending_child_results()), 1)
        recovered = DurableSubagentManager(
            factory(parent_session), agent_factories={"worker": factory}, store=self.store
        )
        self.addAsyncCleanup(recovered.close)
        graph = await recovered.recover()
        self.assertEqual(graph[0]["task_id"], spawned["task_id"])
        notices = await self.store.list_notifications("t", "parent")
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0]["payload"]["status"], "completed")
        self.assertEqual(notices[0]["payload"]["parent_run_id"], parent_session.snapshot["run_id"])
        await recovered.ack_notification(notices[0]["command_id"])
        await recovered.recover()
        self.assertEqual(len(await self.store.list_notifications("t", "parent")), 1)
        child_runs = await self.store.list_runs("t", session_id=spawned["task_id"])
        self.assertEqual(child_runs[0]["source"], "subagent")
        self.assertEqual(child_runs[0]["root_run_id"], parent_session.root_run_id)


if __name__ == "__main__":
    import unittest

    unittest.main()
