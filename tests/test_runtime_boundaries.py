"""Regressions for ordered commits, stream backpressure, tail reads and retained outputs."""

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fixtures import models_for_tests
from test_loop import MODEL, FakeProvider, add_tool, answer, call
from test_providers import response_events, sse
from test_sessions import ProcessLost

from agent_runtime import (
    AbortSignal,
    Agent,
    AgentContext,
    AgentToolResult,
    EventStream,
    LocalSession,
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

    async def test_parallel_tools_and_external_input_share_only_the_loop_commit_coroutine(self):
        owner = asyncio.current_task()
        slow_started, fast_saved, queued = (asyncio.Event() for _ in range(3))
        commits, completions = [], []
        case = self

        class TrackingSession(LocalSession):
            async def commit(self, kind, **data):
                case.assertIs(asyncio.current_task(), owner)
                commits.append(kind)
                await asyncio.sleep(0)
                record = await super().commit(kind, **data)
                if kind == "tool_returned" and data["call_id"] == "fast":
                    fast_saved.set()
                return record

            def observe(self, event):
                case.assertIs(asyncio.current_task(), owner)
                super().observe(event)

        session = TrackingSession("single", self.directory)

        async def execute(call_id, args, signal, update):
            self.assertIsNot(asyncio.current_task(), owner)
            self.assertIn(call_id, session.snapshot["started"])
            if call_id == "slow":
                slow_started.set()
                await asyncio.wait_for(fast_saved.wait(), 2)
            else:
                await slow_started.wait()
                await queued.wait()
            update(AgentToolResult([{"type": "text", "text": call_id}]))
            return AgentToolResult([{"type": "text", "text": call_id}])

        agent = Agent(
            session=session,
            model=MODEL,
            tools=[add_tool(execute)],
            stream_fn=FakeProvider(answer(calls=[call(id="slow"), call(id="fast")]), answer()),
        )
        agent.subscribe(
            lambda event, signal: (
                completions.append(event["toolCallId"])
                if event["type"] == "tool_execution_end"
                else None
            )
        )

        async def input_request():
            await slow_started.wait()
            before = session.revision
            await agent.enqueue("additional input")
            self.assertEqual(session.revision, before)
            queued.set()

        request = asyncio.create_task(input_request())
        await agent.prompt("go")
        await request
        self.assertEqual(completions, ["fast", "slow"])
        self.assertEqual(commits.count("tool_returned"), 2)
        history = LocalSession("single", self.directory).build_context()
        self.assertEqual(
            [m["toolCallId"] for m in history if m["role"] == "toolResult"], ["slow", "fast"]
        )
        self.assertIn("additional input", json.dumps(history))
        self.assertEqual({p.name for p in session.directory.iterdir()}, {"events.jsonl"})

    async def test_input_during_final_commit_is_saved_when_run_completes(self):
        entered, finish = asyncio.Event(), asyncio.Event()
        committing = []

        class SlowSession(LocalSession):
            async def commit(self, kind, **data):
                committing.append(asyncio.current_task())
                return await super().commit(kind, **data)

            async def _persist(self, seq, record):
                if record["type"] == "run_completed":
                    entered.set()
                    await finish.wait()
                await super()._persist(seq, record)

        session = SlowSession("end", self.directory)
        agent = Agent(session=session, model=MODEL, stream_fn=FakeProvider(answer()))
        run = asyncio.create_task(agent.prompt("first"))
        await entered.wait()
        before = session.revision
        await agent.enqueue("next")
        self.assertEqual(session.revision, before)
        finish.set()
        await run
        self.assertEqual(set(committing), {run})
        reopened = LocalSession("end", self.directory)
        self.assertEqual(reopened.snapshot["steering"][0]["content"][0]["text"], "next")

    async def test_selected_input_remains_recoverable_if_next_turn_preparation_fails(self):
        failed = False

        def prepare_next_turn(turn):
            nonlocal failed
            if not failed:
                failed = True
                raise ProcessLost()

        session = LocalSession("pending", self.directory)
        agent = Agent(
            session=session,
            model=MODEL,
            tools=[add_tool()],
            prepare_next_turn=prepare_next_turn,
            stream_fn=FakeProvider(answer(calls=[call()]), answer(), answer()),
        )

        def enqueue(event, signal):
            if event["type"] == "tool_execution_end":
                agent.steer("selected input")

        unsubscribe = agent.subscribe(enqueue)
        with self.assertRaises(ProcessLost):
            await agent.prompt("go")
        unsubscribe()
        stored = LocalSession("pending", self.directory).snapshot
        self.assertEqual(stored["steering"][0]["content"][0]["text"], "selected input")
        await agent.enqueue("new input")
        await agent.resume()
        history = LocalSession("pending", self.directory).build_context()
        texts = [m["content"][0]["text"] for m in history if m["role"] == "user"]
        self.assertEqual(texts, ["go", "selected input", "new input"])

    async def test_fork_into_empty_partial_file_keeps_imported_prefix_on_append(self):
        source = LocalSession("source", self.directory)
        await source.commit("queues", steering=[user_message("keep")], follow_up=[])
        target = LocalSession("child", self.directory)
        target.directory.mkdir(parents=True)
        target.path.write_bytes(b'{"partial":')
        target = LocalSession("child", self.directory)
        await source.fork_into(source.read_records()[-1]["id"], target)
        prefix = target.path.read_bytes()
        await target.commit("queues", steering=[], follow_up=[])
        self.assertTrue(target.path.read_bytes().startswith(prefix))
        self.assertEqual(LocalSession("child", self.directory).revision, 3)

    async def test_provider_preparation_is_committed_by_caller_before_http(self):
        owner = asyncio.current_task()
        commits, requests = [], []
        case = self

        class TrackingSession(LocalSession):
            async def commit(self, kind, **data):
                case.assertIs(asyncio.current_task(), owner)
                await asyncio.sleep(0)
                record = await super().commit(kind, **data)
                commits.append(kind)
                return record

        session = TrackingSession("parameters", self.directory)

        def respond(request):
            self.assertIn("provider_parameters", commits)
            requests.append(request)
            return httpx.Response(200, content=sse(response_events()))

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            models = models_for_tests(client=client, api_keys={"openai": "test-only"})
            agent = Agent(
                session=session, models=models, model=models.get_model("openai", "test-reasoner")
            )
            await agent.prompt("go")
        self.assertEqual(len(requests), 1)
        self.assertEqual(session.snapshot["status"], "completed")

    async def test_provider_checkpoint_failure_prevents_http_request(self):
        requests = []

        class FailingSession(LocalSession):
            async def _persist(self, seq, record):
                if record["type"] == "provider_parameters":
                    raise OSError("disk full")
                await super()._persist(seq, record)

        def respond(request):
            requests.append(request)
            return httpx.Response(200, content=sse(response_events()))

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            models = models_for_tests(client=client, api_keys={"openai": "test-only"})
            agent = Agent(
                session=FailingSession("failure", self.directory),
                models=models,
                model=models.get_model("openai", "test-reasoner"),
            )
            with self.assertRaises(SessionError):
                await agent.prompt("go")
        self.assertFalse(requests)
        self.assertFalse(agent.state.is_streaming)
        self.assertEqual(LocalSession("failure", self.directory).snapshot["phase"], "model")

    async def test_failed_storage_requires_reopening_before_further_commits(self):
        writes = []

        class FailingSession(LocalSession):
            async def _persist(self, seq, record):
                writes.append(record)
                raise OSError("disk failure")

        session = FailingSession(directory=None)
        for _ in range(2):
            with self.assertRaises(SessionError):
                await session.commit("queues", steering=[user_message("pending")], follow_up=[])
        self.assertEqual(len(writes), 1)
        self.assertEqual(session.revision, 0)
        self.assertFalse(session.snapshot["steering"])

    async def test_failed_transition_does_not_mutate_queues_or_message_history(self):
        session = LocalSession(directory=None)
        await session.commit("input_queued", queue="steering", message=user_message("pending"))
        original = session.snapshot
        # This transition modifies queues before encountering invalid input data.
        with self.assertRaises(SessionError):
            await session.commit("run_started", queues={"steering": [user_message("new")]})
        self.assertEqual(session.snapshot, original)
        self.assertEqual(session.revision, 1)

    async def test_tail_poll_reads_only_new_records_and_full_reads_revalidate(self):
        writer = LocalSession("tail", self.directory)
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

    async def test_tail_poll_handles_partial_tail_repair_and_replacement(self):
        writer = LocalSession("tail", self.directory)
        await writer.commit("input_queued", queue="steering", message=user_message("first"))
        path = writer.path
        original = path.read_bytes()
        reader = LocalSession("tail", self.directory)
        with path.open("ab") as file:
            file.write(b'{"incomplete":')
        self.assertEqual(reader.read_records(after_seq=0), [])
        writer = LocalSession("tail", self.directory)
        await writer.commit("input_queued", queue="steering", message=user_message("second"))
        self.assertEqual([r["seq"] for r in reader.read_records(after_seq=0)], [1])
        replacement = path.with_suffix(".replacement")
        replacement.write_bytes(original)
        replacement.replace(path)
        self.assertEqual(reader.read_records(after_seq=1), [])
        writer = LocalSession("tail", self.directory)
        await writer.commit("input_queued", queue="steering", message=user_message("replacement"))
        records = reader.read_records(after_seq=0)
        self.assertEqual(records[0]["data"]["message"]["content"][0]["text"], "replacement")

    async def test_tail_poll_detects_modified_same_size_record(self):
        session = LocalSession("corrupt", self.directory)
        await session.commit("input_queued", queue="steering", message=user_message("old"))
        reader = LocalSession("corrupt", self.directory)
        stamp = session.path.stat().st_mtime_ns
        session.path.write_bytes(session.path.read_bytes().replace(b'"old"', b'"bad"'))
        os.utime(session.path, ns=(stamp + 1_000_000, stamp + 1_000_000))
        with self.assertRaises(SessionError):
            reader.read_records(after_seq=0)

    async def test_split_record_is_not_skipped_by_cursor(self):
        session = LocalSession("split", self.directory)
        await session.commit("input_queued", queue="steering", message=user_message("first"))
        before = session.path.stat().st_size
        await session.commit("input_queued", queue="steering", message=user_message("second"))
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
            stream = await models.stream_simple(model, AgentContext())
            await asyncio.sleep(0.02)
            self.assertLessEqual(len(stream._queue), stream.max_pending)
            self.assertFalse(stream.task.done())
            deltas = [e["delta"] async for e in stream if e["type"] == "text_delta"]
            self.assertEqual("".join(deltas), "x" * 1000)
            self.assertEqual((await stream.result())["stopReason"], "stop")
            signal = AbortSignal()
            stopped = await models.stream_simple(model, AgentContext(), {"signal": signal})
            await asyncio.sleep(0.02)
            signal.abort()
            await asyncio.wait_for(stopped.task, 1)
            self.assertEqual((await stopped.result())["stopReason"], "aborted")
            final_only = await models.stream_simple(model, AgentContext())
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
