"""Regressions for writer release, stream backpressure, tail reads and retained outputs."""

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fixtures import models_for_tests
from test_loop import MODEL, FakeProvider, answer, call
from test_providers import response_events, sse

from agent_runtime import (
    AbortSignal,
    Agent,
    AgentContext,
    EventStream,
    LocalSession,
    SessionBusyError,
    SessionError,
    stream_proxy,
    user_message,
)
from agent_runtime.tools import create_bash_tool


class RuntimeBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()

    async def test_run_end_keeps_writer_until_waiting_enqueue_is_durable(self):
        at_end, finish_end, at_enqueue, finish_enqueue = (asyncio.Event() for _ in range(4))

        class GatedSession(LocalSession):
            async def _persist(self, seq, record):
                if record["type"] == "run_completed":
                    at_end.set()
                    await finish_end.wait()
                if record["type"] == "input_queued":
                    at_enqueue.set()
                    await finish_enqueue.wait()
                await super()._persist(seq, record)

        session = GatedSession("race", self.directory)
        agent = Agent(session=session, model=MODEL, stream_fn=FakeProvider(answer()))
        run = asyncio.create_task(agent.prompt("first"))
        await at_end.wait()
        enqueue = asyncio.create_task(agent.enqueue("second"))
        await asyncio.sleep(0)
        finish_end.set()
        await at_enqueue.wait()
        with self.assertRaises(SessionBusyError):
            await LocalSession("race", self.directory).acquire()
        self.assertFalse(run.done())
        finish_enqueue.set()
        await asyncio.gather(run, enqueue)
        reopened = LocalSession("race", self.directory)
        self.assertEqual(reopened.snapshot["steering"][0]["content"][0]["text"], "second")
        await reopened.acquire()
        await reopened.commit("input_queued", queue="steering", message=user_message("third"))
        await reopened.release()
        self.assertEqual(len(LocalSession("race", self.directory).snapshot["steering"]), 2)

    async def test_late_commit_rechecks_ownership_after_release(self):
        entered, finish = asyncio.Event(), asyncio.Event()

        class SlowRelease(LocalSession):
            async def _release(self):
                entered.set()
                await finish.wait()
                await super()._release()

        session = SlowRelease(directory=None)
        await session.acquire()
        release = asyncio.create_task(session.release())
        await entered.wait()
        commit = asyncio.create_task(
            session.commit("input_queued", queue="steering", message=user_message("late"))
        )
        await asyncio.sleep(0)
        finish.set()
        await release
        with self.assertRaises(SessionError):
            await commit
        self.assertEqual(session.revision, 0)

    async def test_waiting_commit_does_not_write_after_previous_persistence_failure(self):
        entered, finish = asyncio.Event(), asyncio.Event()
        writes = []

        class FailingSession(LocalSession):
            async def _persist(self, seq, record):
                writes.append(record)
                entered.set()
                await finish.wait()
                raise OSError("disk failure")

        session = FailingSession(directory=None)
        await session.acquire()
        first = asyncio.create_task(
            session.commit("input_queued", queue="steering", message=user_message("first"))
        )
        await entered.wait()
        second = asyncio.create_task(
            session.commit("input_queued", queue="steering", message=user_message("second"))
        )
        await asyncio.sleep(0)
        finish.set()
        errors = await asyncio.gather(first, second, return_exceptions=True)
        self.assertTrue(all(isinstance(e, SessionError) for e in errors))
        self.assertEqual(len(writes), 1)
        self.assertEqual(session.snapshot["steering"], [])
        await session.release()

    async def test_repeated_cancellation_during_release_still_releases_writer(self):
        entered, finish = asyncio.Event(), asyncio.Event()

        class SlowRelease(LocalSession):
            async def _release(self):
                entered.set()
                await finish.wait()
                await super()._release()

        session = SlowRelease("cancel", self.directory)
        agent = Agent(session=session, model=MODEL, stream_fn=FakeProvider(answer()))
        run = asyncio.create_task(agent.prompt("go"))
        await entered.wait()
        run.cancel()
        await asyncio.sleep(0)
        run.cancel()
        with self.assertRaises(SessionBusyError):
            await LocalSession("cancel", self.directory).acquire()
        finish.set()
        with self.assertRaises(asyncio.CancelledError):
            await run
        await asyncio.wait_for(agent.wait_for_idle(), 1)
        reopened = LocalSession("cancel", self.directory)
        await reopened.acquire()
        await reopened.release()

    async def test_failed_transition_does_not_mutate_queues_or_message_history(self):
        session = LocalSession(directory=None)
        await session.acquire()
        await session.commit("input_queued", queue="steering", message=user_message("pending"))
        original = session.snapshot
        # This transition modifies queues before encountering invalid input data.
        with self.assertRaises(SessionError):
            await session.commit("run_started", queues={"steering": [user_message("new")]})
        self.assertEqual(session.snapshot, original)
        self.assertEqual(session.revision, 1)
        await session.release()

    async def test_tail_poll_reads_only_new_records_and_full_reads_revalidate(self):
        writer = LocalSession("tail", self.directory)
        await writer.acquire()
        for i in range(20):
            await writer.commit("input_queued", queue="steering", message=user_message(str(i)))
        reader = LocalSession("tail", self.directory)
        with patch("agent_runtime.sessions.local.json.loads", wraps=json.loads) as loads:
            self.assertEqual(reader.read_records(after_seq=19), [])
            self.assertEqual(loads.call_count, 0)
        await writer.commit("input_queued", queue="steering", message=user_message("new"))
        with patch("agent_runtime.sessions.local.json.loads", wraps=json.loads) as loads:
            self.assertEqual([r["seq"] for r in reader.read_records(after_seq=19)], [20])
            self.assertEqual(loads.call_count, 1)
        self.assertEqual(len(reader.read_records(after_seq=3)), 17)
        self.assertEqual(len(reader.read_records()), 21)
        await writer.release()

    async def test_tail_poll_handles_partial_tail_repair_and_replacement(self):
        writer = LocalSession("tail", self.directory)
        await writer.acquire()
        await writer.commit("input_queued", queue="steering", message=user_message("first"))
        await writer.release()
        path = writer.path
        original = path.read_bytes()
        reader = LocalSession("tail", self.directory)
        with path.open("ab") as file:
            file.write(b'{"incomplete":')
        self.assertEqual(reader.read_records(after_seq=0), [])
        await writer.acquire()  # Repairs the uncommitted suffix.
        await writer.commit("input_queued", queue="steering", message=user_message("second"))
        await writer.release()
        self.assertEqual([r["seq"] for r in reader.read_records(after_seq=0)], [1])
        replacement = path.with_suffix(".replacement")
        replacement.write_bytes(original)
        replacement.replace(path)
        self.assertEqual(reader.read_records(after_seq=1), [])
        await writer.acquire()
        await writer.commit("input_queued", queue="steering", message=user_message("replacement"))
        await writer.release()
        records = reader.read_records(after_seq=0)
        self.assertEqual(records[0]["data"]["message"]["content"][0]["text"], "replacement")

    async def test_tail_poll_detects_modified_same_size_record(self):
        session = LocalSession("corrupt", self.directory)
        await session.acquire()
        await session.commit("input_queued", queue="steering", message=user_message("old"))
        await session.release()
        reader = LocalSession("corrupt", self.directory)
        stamp = session.path.stat().st_mtime_ns
        session.path.write_bytes(session.path.read_bytes().replace(b'"old"', b'"bad"'))
        os.utime(session.path, ns=(stamp + 1_000_000, stamp + 1_000_000))
        with self.assertRaises(SessionError):
            reader.read_records(after_seq=0)

    async def test_split_record_is_not_skipped_by_cursor(self):
        session = LocalSession("split", self.directory)
        await session.acquire()
        await session.commit("input_queued", queue="steering", message=user_message("first"))
        before = session.path.stat().st_size
        await session.commit("input_queued", queue="steering", message=user_message("second"))
        await session.release()
        full = session.path.read_bytes()
        session.path.write_bytes(full[: before + 20])
        reader = LocalSession("split", self.directory)
        self.assertEqual(reader.read_records(after_seq=0), [])
        with session.path.open("ab") as file:
            file.write(full[before + 20 :])
        self.assertEqual([r["seq"] for r in reader.read_records(after_seq=0)], [1])

    async def test_bounded_stream_preserves_order_and_result_only_never_stalls(self):
        stream = EventStream(lambda e: e == 100, lambda e: e, max_pending=4)

        async def produce():
            for i in range(101):
                await stream.send(i)

        task = asyncio.create_task(produce())
        await asyncio.sleep(0)
        self.assertEqual(len(stream._queue), 4)
        self.assertFalse(task.done())
        self.assertEqual([event async for event in stream], list(range(101)))
        self.assertEqual(await stream.result(), 100)
        await task
        final_only = EventStream(lambda e: e == 100, lambda e: e, max_pending=4)

        async def send_all():
            for i in range(101):
                await final_only.send(i)

        task = asyncio.create_task(send_all())
        await asyncio.sleep(0)
        self.assertEqual(await asyncio.wait_for(final_only.result(), 1), 100)
        await task
        self.assertFalse(final_only._queue)

    async def test_provider_backpressure_and_cancellation_when_consumer_is_stalled(self):
        events = response_events(text="x" * 1000)
        delta = events[1]
        events[1:2] = [{**delta, "delta": "x"} for _ in range(1000)]
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=sse(events)))
        ) as client:
            models = models_for_tests(client=client, api_keys={"openai": "test-only"})
            model = models.get_model("openai", "test-reasoner")
            stream = models.stream_simple(model, AgentContext())
            await asyncio.sleep(0.02)
            self.assertLessEqual(len(stream._queue), stream.max_pending)
            self.assertFalse(stream.task.done())
            deltas = [e["delta"] async for e in stream if e["type"] == "text_delta"]
            self.assertEqual("".join(deltas), "x" * 1000)
            self.assertEqual((await stream.result())["stopReason"], "stop")
            signal = AbortSignal()
            stopped = models.stream_simple(model, AgentContext(), {"signal": signal})
            await asyncio.sleep(0.02)
            signal.abort()
            await asyncio.wait_for(stopped.task, 1)
            self.assertEqual((await stopped.result())["stopReason"], "aborted")
            final_only = models.stream_simple(model, AgentContext())
            self.assertEqual(
                (await asyncio.wait_for(final_only.result(), 2))["content"][0]["text"], "x" * 1000
            )

    async def test_proxy_backpressure_preserves_all_deltas(self):
        events = [{"type": "start"}, {"type": "text_start", "contentIndex": 0}]
        events += [{"type": "text_delta", "contentIndex": 0, "delta": "x"} for _ in range(200)]
        events += [{"type": "text_end", "contentIndex": 0}, {"type": "done", "reason": "stop"}]
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=sse(events)))
        ) as client:
            stream = stream_proxy(
                MODEL,
                AgentContext(),
                {
                    "auth_token": "test",
                    "proxy_url": "https://example.invalid",
                },
                client=client,
            )
            await asyncio.sleep(0.02)
            self.assertLessEqual(len(stream._queue), stream.max_pending)
            deltas = [e["delta"] async for e in stream if e["type"] == "text_delta"]
            self.assertEqual("".join(deltas), "x" * 200)
            self.assertEqual((await stream.result())["content"][0]["text"], "x" * 200)

    async def test_large_output_retained_next_to_session_and_readable_after_reopen(self):
        data = b"a" * 1048576 + b"middle-marker" + b"b" * 1048576

        class OutputBackend:
            async def exec(self, command, cwd, *, on_data, signal, timeout, env):
                on_data(data)
                return 0

        session = LocalSession("output", self.directory)
        tool = create_bash_tool(
            self.directory, operations=OutputBackend(), output_dir=session.directory / "outputs"
        )
        agent = Agent(
            session=session,
            model=MODEL,
            tools=[tool],
            stream_fn=FakeProvider(
                answer(calls=[call("bash", args={"command": "synthetic"})]),
                answer(),
            ),
        )
        await agent.prompt("go")
        reopened = LocalSession("output", self.directory)
        returned = next(r for r in reopened.read_records() if r["type"] == "tool_returned")
        result = returned["data"]["result"]
        path = Path(result["details"]["fullOutputPath"])
        self.assertEqual(path.parent, session.directory / "outputs")
        self.assertEqual(path.read_bytes(), data)
        self.assertTrue(result["structuredContent"]["truncated"])
        # Serialized history contains the durable reference, not another copy of the blob.
        self.assertIn(str(path), json.dumps(result))


if __name__ == "__main__":
    unittest.main()
