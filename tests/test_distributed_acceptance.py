"""Real process death and fencing against an isolated PostgreSQL schema.

No model network calls. Child processes inherit the test DSN through their
environment; stdout contains synchronization facts only, never credentials.
"""

import asyncio
import json
import os
import signal
import sys
import tempfile
from pathlib import Path

from postgres_fixture import PostgresCase

from agent_runtime import (
    Agent,
    AgentTool,
    AgentToolResult,
    AssistantMessageEventStream,
    DatabaseSession,
    Model,
    assistant_message,
)
from agent_runtime.execution import DurableExecutor, ExecutionHost
from agent_runtime.storage import DatabaseConfig
from agent_runtime.storage.coordination import ExecutionLeaseError
from agent_runtime.storage.postgres import PostgresStore
from agent_runtime.subagents import DurableSubagentManager

MODEL = Model("controlled-offline", "local", "process-acceptance", "")


def response(model, *, effect=False):
    content = (
        [{"type": "toolCall", "id": "effect-1", "name": "effect", "arguments": {}}]
        if effect
        else [{"type": "text", "text": "Recovered committed result"}]
    )
    stream = AssistantMessageEventStream()
    stream.push(
        {
            "type": "done",
            "message": assistant_message(
                model, content=content, stopReason="toolUse" if effect else "stop"
            ),
        }
    )
    return stream


def announce(value):
    print(json.dumps(value), flush=True)


async def worker(role):
    store = await PostgresStore.connect(DatabaseConfig(os.environ["PROCESS_TEST_DSN"]))
    try:
        if role == "remote_complete":
            host = ExecutionHost(
                store,
                publish=lambda tenant, sid: announce(
                    {"notification": True, "tenant": tenant, "session": sid}
                ),
            )
            announce({"ready": True})
            await asyncio.to_thread(sys.stdin.readline)
            await DurableExecutor(
                store,
                lambda session: Agent(
                    model=MODEL,
                    session=session,
                    stream_fn=lambda model, context, options: response(model),
                ),
                host=host,
            ).execute("t", "remote", user_id="u", command_id="remote", message="complete remotely")
            await host.close()
            return
        if role == "stale_writer":
            session = DatabaseSession(store, tenant_id="t", user_id="u", session_id="fenced")
            lease = await store.acquire_execution(
                "t", "fenced", holder="old-process", ttl_seconds=1
            )
            session.execution_lease = lease
            await session.commit("context_replaced", messages=[])
            announce({"ready": True, "fence": lease.fence})
            await asyncio.to_thread(sys.stdin.readline)
            try:
                await session.commit("context_replaced", messages=[])
            except Exception as error:
                announce({"stale_commit": "rejected", "error_type": type(error).__name__})
            else:
                announce({"stale_commit": "accepted"})
            return

        async def blocked(model, context, options):
            announce({"ready": True, "boundary": "model_request_committed"})
            await asyncio.Event().wait()

        async def effect(call_id, arguments, abort, update):
            with Path(os.environ["PROCESS_EFFECT_FILE"]).open("a") as target:
                target.write(call_id + "\n")
            announce({"ready": True, "boundary": "effect_succeeded_return_unknown"})
            await asyncio.Event().wait()
            return AgentToolResult()

        def factory(session):
            if role == "unknown_effect":
                return Agent(
                    model=MODEL,
                    session=session,
                    tools=[
                        AgentTool("effect", "Unknown external effect", {"type": "object"}, effect)
                    ],
                    stream_fn=lambda model, context, options: response(model, effect=True),
                )
            return Agent(model=MODEL, session=session, stream_fn=blocked)

        await DurableExecutor(store, factory, lease_seconds=1).execute(
            "t", role, user_id="u", command_id=role, message="bounded acceptance"
        )
    finally:
        await store.close()


class DistributedProcessChecks(PostgresCase):
    async def test_parent_waits_for_child_with_single_root_slot_without_deadlock(self):
        host = ExecutionHost(self.store, max_active=1, max_active_children=1)
        self.addAsyncCleanup(host.close)
        managers = []

        def parent_factory(session):
            async def provider(model, context, options):
                return response(
                    model,
                    effect=not any(message["role"] == "toolResult" for message in context.messages),
                )

            async def delegate(call_id, arguments, abort, update):
                child = await manager.spawn("worker", "bounded child", operation_id=call_id)
                state = await manager.wait(child["task_id"], timeout=2)
                self.assertEqual(state["status"], "completed")
                return AgentToolResult(content=[{"type": "text", "text": "child completed"}])

            parent = Agent(
                model=MODEL,
                session=session,
                stream_fn=provider,
                tools=[AgentTool("effect", "Wait for child", {"type": "object"}, delegate)],
            )
            manager = DurableSubagentManager(
                parent,
                host=host,
                agent_factories={
                    "worker": lambda child_session: Agent(
                        model=MODEL,
                        session=child_session,
                        stream_fn=lambda model, context, options: response(model),
                    )
                },
            )
            managers.append(manager)
            return parent

        try:
            result = await asyncio.wait_for(
                DurableExecutor(self.store, parent_factory, host=host).execute(
                    "t", "parent", user_id="u", command_id="parent", message="delegate then wait"
                ),
                5,
            )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(len(await self.store.list_notifications("t", "parent")), 1)
        finally:
            for manager in managers:
                await manager.close()

    async def test_cancel_completion_race_has_one_terminal_run_and_no_reexecution(self):
        for iteration in range(4):
            entered, finish = asyncio.Event(), asyncio.Event()
            calls = []

            async def provider(model, context, options):
                calls.append(True)
                entered.set()
                await options["signal"].run(finish.wait())
                return response(model)

            host = ExecutionHost(self.store)
            self.addAsyncCleanup(host.close)
            executor = DurableExecutor(
                self.store,
                lambda session: Agent(model=MODEL, session=session, stream_fn=provider),
                host=host,
            )
            key = "race-" + str(iteration)
            active = asyncio.create_task(
                executor.execute("t", key, user_id="u", command_id=key, message="race")
            )
            await asyncio.wait_for(entered.wait(), 2)
            cancel = asyncio.create_task(host.request_cancel("t", key))
            finish.set()
            result, _ = await asyncio.wait_for(asyncio.gather(active, cancel), 3)
            self.assertIn(result["status"], ("completed", "stopped"))
            command = await self.store.get_command("t", key)
            self.assertIn(command["state"], ("completed", "stopped"))
            replay = await executor.execute_command("t", key, user_id="u", command_id=key)
            self.assertEqual(replay["status"], result["status"])
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(await self.store.list_runs("t", session_id=key)), 1)

    async def test_ipc_completion_wakes_waiter_in_another_process(self):
        process, _ = await self.spawn("remote_complete")
        host = ExecutionHost(self.store)
        self.addAsyncCleanup(host.close)
        waiting = asyncio.create_task(host.wait_for_change("t", "remote", timeout=3))
        await asyncio.sleep(0.05)
        self.assertFalse(waiting.done())
        process.stdin.write(b"finish remotely\n")
        await process.stdin.drain()
        notification = json.loads(await asyncio.wait_for(process.stdout.readline(), 3))
        self.assertTrue(notification["notification"])
        host.notify(notification["tenant"], notification["session"])
        self.assertTrue(await waiting)
        self.assertEqual((await self.store.get_command("t", "remote"))["state"], "completed")
        self.assertEqual(len(await self.store.list_runs("t", session_id="remote")), 1)

    async def test_waiter_timeout_and_cancel_do_not_steal_another_waiters_signal(self):
        host = ExecutionHost(self.store)
        self.addAsyncCleanup(host.close)
        short = asyncio.create_task(host.wait_for_change("t", "s", timeout=0.02))
        long = asyncio.create_task(host.wait_for_change("t", "s", timeout=1))
        self.assertFalse(await short)
        host.notify("t", "s")
        self.assertTrue(await long)
        canceled = asyncio.create_task(host.wait_for_change("t", "s", timeout=1))
        survivor = asyncio.create_task(host.wait_for_change("t", "s", timeout=1))
        await asyncio.sleep(0)
        canceled.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await canceled
        host.notify("t", "s")
        self.assertTrue(await survivor)

    async def test_lease_loss_during_factory_or_open_never_starts_agent(self):
        for boundary in ("factory", "open"):
            with self.subTest(boundary=boundary):
                host = ExecutionHost(self.store, lease_seconds=1, renew_interval=0.03)
                self.addAsyncCleanup(host.close)
                entered, release = asyncio.Event(), asyncio.Event()
                starts = []

                class ObservedAgent(Agent):
                    async def prompt(self, *args, **kwargs):
                        starts.append(True)
                        return await super().prompt(*args, **kwargs)

                async def factory(session):
                    if boundary == "factory":
                        entered.set()
                        await release.wait()
                    return ObservedAgent(
                        model=MODEL,
                        session=session,
                        stream_fn=lambda model, context, options: response(model),
                    )

                executor = DurableExecutor(self.store, factory, host=host)
                original_open = executor._open

                async def delayed_open(*args, **kwargs):
                    if boundary == "open":
                        entered.set()
                        await release.wait()
                    return await original_open(*args, **kwargs)

                executor._open = delayed_open
                running = asyncio.create_task(
                    executor.execute(
                        "t",
                        boundary,
                        user_id="u",
                        command_id=boundary,
                        message="do not start after loss",
                    )
                )
                await asyncio.wait_for(entered.wait(), 2)
                lease = await self.store.get_execution("t", boundary)
                await self.store.release_execution(lease)
                await asyncio.sleep(0.08)
                release.set()
                with self.assertRaises(ExecutionLeaseError):
                    await asyncio.wait_for(running, 2)
                self.assertEqual(starts, [])
                self.assertEqual((await self.store.get_command("t", boundary))["state"], "accepted")

    async def spawn(self, role, *, effect_file=None):
        env = {**os.environ, "PROCESS_TEST_DSN": self.dsn}
        env["OPENAI_API_KEY"] = "explicit-process-test-no-network"
        if effect_file:
            env["PROCESS_EFFECT_FILE"] = str(effect_file)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-u",
            str(Path(__file__).resolve()),
            role,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        self.addAsyncCleanup(self.finish_process, process)
        line = await asyncio.wait_for(process.stdout.readline(), 10)
        self.assertTrue(line, "Acceptance child exited before its synchronization boundary")
        fact = json.loads(line)
        self.assertTrue(fact["ready"])
        return process, fact

    async def finish_process(self, process):
        if process.returncode is None:
            process.send_signal(signal.SIGCONT)
            process.kill()
        await asyncio.wait_for(process.wait(), 5)

    async def wait_expiry(self, sid):
        from datetime import datetime, timezone

        lease = await self.store.get_execution("t", sid)
        if lease:
            await asyncio.sleep(
                max(0, (lease.expires_at - datetime.now(timezone.utc)).total_seconds()) + 0.1
            )

    async def test_stopped_old_process_cannot_write_after_takeover(self):
        process, first = await self.spawn("stale_writer")
        original_records = await self.store.read_records("t", "fenced")
        self.assertIsNone(await self.store.acquire_execution("t", "fenced", holder="competitor"))
        process.send_signal(signal.SIGSTOP)
        await self.wait_expiry("fenced")
        lease = await self.store.acquire_execution("t", "fenced", holder="new-process")
        self.assertGreater(lease.fence, first["fence"])
        process.send_signal(signal.SIGCONT)
        process.stdin.write(b"attempt stale write\n")
        await process.stdin.drain()
        fact = json.loads(await asyncio.wait_for(process.stdout.readline(), 5))
        self.assertEqual(fact["stale_commit"], "rejected")
        self.assertEqual(await self.store.read_records("t", "fenced"), original_records)
        await self.store.assert_execution(lease)
        await self.store.release_execution(lease)

    async def test_sigkill_recovers_same_run_without_new_user_input(self):
        process, _ = await self.spawn("interrupted_model")
        accepted = await self.store.get_command("t", "interrupted_model")
        process.kill()
        await process.wait()
        await self.wait_expiry("interrupted_model")
        result = await DurableExecutor(
            self.store,
            lambda session: Agent(
                model=MODEL,
                session=session,
                stream_fn=lambda model, context, options: response(model),
            ),
        ).execute_command("t", "interrupted_model", user_id="u", command_id="interrupted_model")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["run_id"], accepted["run_id"])
        self.assertEqual(len(await self.store.list_runs("t", session_id="interrupted_model")), 1)

    async def test_sigkill_unknown_effect_requires_reconciliation(self):
        with tempfile.TemporaryDirectory(prefix="runtime-effect-check-") as directory:
            effect_file = Path(directory) / "effect.txt"
            process, fact = await self.spawn("unknown_effect", effect_file=effect_file)
            self.assertEqual(fact["boundary"], "effect_succeeded_return_unknown")
            process.kill()
            await process.wait()
            await self.wait_expiry("unknown_effect")

            async def forbidden_replay(*args):
                self.fail("Unknown external effect was automatically replayed")

            def factory(session):
                return Agent(
                    model=MODEL,
                    session=session,
                    tools=[
                        AgentTool(
                            "effect",
                            "Unknown external effect",
                            {"type": "object"},
                            forbidden_replay,
                        )
                    ],
                    stream_fn=lambda model, context, options: response(model),
                )

            result = await DurableExecutor(self.store, factory).execute_command(
                "t", "unknown_effect", user_id="u", command_id="unknown_effect"
            )
            self.assertEqual(result["status"], "recovery_required")
            self.assertEqual(result["snapshot"]["status"], "waiting_recovery")
            self.assertIn("unknown", result["error"].lower())
            self.assertEqual(effect_file.read_text().splitlines(), ["effect-1"])
            reopened = await DatabaseSession.open(
                self.store, tenant_id="t", session_id="unknown_effect"
            )
            self.assertEqual(reopened.snapshot["status"], "waiting_recovery")
            records = await reopened.aread_records()
            self.assertTrue(any(record["type"] == "tool_started" for record in records))
            self.assertTrue(any(record["type"] == "model_completed" for record in records))


if __name__ == "__main__":
    asyncio.run(worker(sys.argv[1]))
