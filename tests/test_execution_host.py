"""Shared execution host against real PG; no external model calls."""

import asyncio
from unittest.mock import AsyncMock

from postgres_fixture import PostgresCase
from test_durable_postgres import MODEL, factory, response

from agent_runtime import Agent, DatabaseSession, SessionError
from agent_runtime.execution import DurableExecutor, ExecutionHost
from agent_runtime.subagents import DurableSubagentManager


class HostChecks(PostgresCase):
    async def host(self, **options):
        host = ExecutionHost(self.store, **options)
        self.addAsyncCleanup(host.close)
        return host

    async def test_shared_batch_renewal_and_notification_cancel(self):
        host = await self.host(lease_seconds=1, renew_interval=0.1)
        entered = {name: asyncio.Event() for name in ("a", "b")}
        batches = []
        renew = self.store.renew_executions

        async def batch(leases, **options):
            batches.append(len(leases))
            return await renew(leases, **options)

        self.store.renew_executions = batch
        self.store.renew_execution = AsyncMock(side_effect=AssertionError("per-run heartbeat"))

        def blocked_factory(session):
            async def stream(model, context, options):
                entered[session.session_id].set()
                await options["signal"].run(asyncio.Event().wait())

            return Agent(model=MODEL, session=session, stream_fn=stream)

        tasks = [
            asyncio.create_task(
                DurableExecutor(self.store, blocked_factory, host=host).execute(
                    "t", name, user_id="u", command_id=name, message=name
                )
            )
            for name in entered
        ]
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in entered.values())), 2)
        await asyncio.sleep(0.15)
        await host.request_cancel("t", "a")
        await host.request_cancel("t", "b")
        results = await asyncio.wait_for(asyncio.gather(*tasks), 2)
        self.assertEqual([result["status"] for result in results], ["stopped", "stopped"])
        self.assertIn(2, batches)
        self.store.renew_execution.assert_not_called()

    async def test_child_can_finish_while_only_root_slot_is_occupied(self):
        host = await self.host(max_active=1, max_active_children=1)

        def parent_factory(session):
            async def stream(model, context, options):
                result = await DurableExecutor(self.store, factory, host=host, child=True).execute(
                    "t", "child", user_id="u", command_id="child", message="child"
                )
                self.assertEqual(result["status"], "completed")
                return response(model)

            return Agent(model=MODEL, session=session, stream_fn=stream)

        result = await asyncio.wait_for(
            DurableExecutor(self.store, parent_factory, host=host).execute(
                "t", "parent", user_id="u", command_id="parent", message="parent"
            ),
            2,
        )
        self.assertEqual(result["status"], "completed")

    async def test_upgrade_is_explicit_repeatable_and_run_read_is_scoped(self):
        await self.store.upgrade_schema()
        await self.store.upgrade_schema()
        result = await DurableExecutor(self.store, factory).execute(
            "t", "s", user_id="u", command_id="first", message="hello\x00world"
        )
        await self.store.upgrade_schema()
        self.store.read_records = AsyncMock(side_effect=AssertionError("full journal read"))
        records = await self.store.read_run_records("t", "s", result["run_id"], limit=2)
        self.assertEqual(len(records), 2)
        self.assertTrue(all(record["run_id"] == result["run_id"] for record in records))
        suffix = await self.store.read_run_records(
            "t", "s", result["run_id"], after_seq=records[-1]["seq"], limit=2
        )
        self.assertGreater(suffix[0]["seq"], records[-1]["seq"])

    async def test_recovery_candidates_are_bounded_and_skip_waiting_recovery(self):
        for name in ("a", "b"):
            await self.store.accept_command(
                "t",
                name,
                command_id=name,
                kind="prompt",
                user_id="u",
                payload={"message": name},
            )
        host = await self.host()
        first = await host.recovery_candidates(limit=1)
        self.assertEqual(len(first), 1)
        cursor = (first[0]["updated_at"], first[0]["tenant_id"], first[0]["command_id"])
        second = await host.recovery_candidates(limit=1, after=cursor)
        self.assertNotEqual(first[0]["command_id"], second[0]["command_id"])
        lease = await self.store.acquire_execution("t", "a", holder="active")
        self.assertEqual([row["session_id"] for row in await host.recovery_candidates()], ["b"])
        await self.store.release_execution(lease)

    async def test_batch_does_not_resurrect_an_expired_fence(self):
        old = await self.store.acquire_execution("t", "s", holder="old")
        await self.store.release_execution(old)
        current = await self.store.acquire_execution("t", "s", holder="current")
        self.assertEqual(await self.store.renew_executions([old]), [])
        await self.store.assert_execution(current)

    async def test_child_budget_resets_for_new_run_in_the_same_session(self):
        host = await self.host()
        executor = DurableExecutor(self.store, factory, host=host)
        first = await executor.execute(
            "t", "parent", user_id="u", command_id="run-1", message="first run"
        )

        async def manager():
            session = await DatabaseSession.open(self.store, tenant_id="t", session_id="parent")
            result = DurableSubagentManager(
                factory(session),
                agent_factories={"worker": factory},
                host=host,
                max_children=1,
            )
            self.addAsyncCleanup(result.close)
            return result

        prior = await manager()
        child = await prior.spawn("worker", "first child", operation_id="first-child")
        await prior.wait(child["task_id"], timeout=2)
        # A completed child still counts in this run's finite submission budget.
        with self.assertRaisesRegex(SessionError, "parent run"):
            await prior.spawn("worker", "excess", operation_id="excess-child")
        duplicate = await prior.spawn("worker", "first child", operation_id="first-child")
        self.assertEqual(duplicate["task_id"], child["task_id"])

        second = await executor.execute(
            "t", "parent", user_id="u", command_id="run-2", message="second run"
        )
        current = await manager()
        next_child = await current.spawn("worker", "new child", operation_id="new-child")
        self.assertEqual(
            (await current.wait(next_child["task_id"], timeout=2))["status"], "completed"
        )
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertEqual(await self.store.count_run_children("t", "parent", first["run_id"]), 1)
        self.assertEqual(await self.store.count_run_children("t", "parent", second["run_id"]), 1)

    async def test_factory_ownership_loss_never_starts_prompt(self):
        host = await self.host(lease_seconds=0.3, renew_interval=0.03)
        agent = None

        async def slow_factory(session):
            nonlocal agent
            await self.store.release_execution(session.execution_lease)
            await asyncio.sleep(0.08)
            agent = factory(session)
            agent.prompt = AsyncMock(side_effect=AssertionError("lost holder dispatched"))
            return agent

        from agent_runtime.storage.coordination import ExecutionLeaseError

        with self.assertRaises(ExecutionLeaseError):
            await DurableExecutor(self.store, slow_factory, host=host).execute(
                "t", "s", user_id="u", command_id="c", message="input"
            )
        agent.prompt.assert_not_called()
        self.assertEqual((await self.store.get_command("t", "c"))["state"], "accepted")
        with self.assertRaises(FileNotFoundError):
            await DatabaseSession.open(self.store, tenant_id="t", session_id="s")
