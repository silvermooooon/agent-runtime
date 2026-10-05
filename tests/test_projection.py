"""Journal-only context, compaction and independent forks, with real file persistence."""

import asyncio
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from test_loop import MODEL, FakeProvider, add_tool, answer, call
from test_sessions import ProcessLost

from agent_runtime import (
    Agent,
    AgentToolResult,
    CompactionSettings,
    Compactor,
    LocalSession,
    SessionError,
    ToolRecoveryRequired,
)
from agent_runtime.sessions.local import encode
from agent_runtime.transcript import current_tools


class ProjectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def session(self, name="source"):
        return LocalSession(name, self.directory)

    async def history(self, **kwargs):
        session = kwargs.pop("session", self.session())
        agent = Agent(
            session=session,
            model=MODEL,
            stream_fn=FakeProvider(answer("answer one"), answer("answer two")),
            **kwargs,
        )
        await agent.prompt("old request")
        await agent.prompt("recent request")
        return agent

    async def test_one_log_compaction_projection_and_reopen(self):
        agent = await self.history(system_prompt="rules", tools=[add_tool()])
        session = agent.session
        before = session.path.read_bytes()
        keep = session.context_entries()[-2]["message_id"]
        event = await agent.compact(summary="old work summarized", first_kept_message_id=keep)
        self.assertTrue(session.path.read_bytes().startswith(before))
        context = session.build_context()
        self.assertEqual([m["role"] for m in context], ["system", "user", "user", "assistant"])
        self.assertIn("old work summarized", context[1]["content"][0]["text"])
        self.assertNotIn("old request", json.dumps(context))
        self.assertIn("old request", json.dumps(session.read_records()))
        self.assertEqual(current_tools(context)[0]["name"], "add")
        reopened = self.session()
        self.assertEqual(reopened.build_context(), agent.state.messages)
        self.assertEqual(reopened.snapshot["compaction"]["event_id"], event["id"])
        self.assertNotIn("messages", event["data"])
        self.assertEqual({p.name for p in session.directory.iterdir()}, {"events.jsonl"})
        provider = FakeProvider(answer("next"))
        next_agent = Agent(session=reopened, tools=[add_tool()], stream_fn=provider)
        await next_agent.prompt("next request")
        self.assertNotIn("old request", json.dumps(provider.contexts[0]))
        self.assertIn("old work summarized", json.dumps(provider.contexts[0]))
        request = [r for r in reopened.read_records() if r["type"] == "model_request"][-1]
        self.assertNotIn("llm_messages", request["data"])

    async def test_history_node_uses_only_its_prefix(self):
        agent = await self.history()
        old = agent.session.read_records()[4]
        before = agent.session.snapshot_at(old["id"])
        await agent.compact(summary="future summary")
        self.assertEqual(agent.session.snapshot_at(old["id"]), before)
        self.assertNotIn("future summary", json.dumps(agent.session.build_context(old["id"])))
        with self.assertRaises(SessionError):
            agent.session.snapshot_at("missing")

    async def test_fork_is_complete_independent_journal(self):
        agent = await self.history()
        records = agent.session.read_records()
        end = next(r for r in records if r["type"] == "run_completed")
        source_bytes = agent.session.path.read_bytes()
        child = await agent.session.fork(end["id"], session_id="child")
        self.assertEqual(child.read_records()[:-1], records[: end["seq"] + 1])
        self.assertEqual(child.snapshot["origin"]["parent_event_id"], end["id"])
        self.assertFalse(child.resumable)
        fork_agent = Agent(session=child, stream_fn=FakeProvider(answer("child result")))
        await fork_agent.prompt("child request")
        self.assertEqual(agent.session.path.read_bytes(), source_bytes)
        self.assertNotIn("recent request", json.dumps(child.build_context()))
        agent.session.path.unlink()  # The child has no lazy dependency on its parent.
        self.assertIn("old request", json.dumps(self.session("child").build_context()))

    async def test_fork_before_and_after_compaction(self):
        agent = await self.history()
        end = agent.session.read_records()[-1]["id"]
        compacted = await agent.compact(summary="checkpoint")
        old = await agent.session.fork(end, session_id="before")
        new = await agent.session.fork(compacted["id"], session_id="after")
        self.assertIn("old request", json.dumps(old.build_context()))
        self.assertNotIn("checkpoint", json.dumps(old.build_context()))
        self.assertNotIn("old request", json.dumps(new.build_context()))
        self.assertIn("old request", json.dumps(new.read_records()))
        self.assertIn("checkpoint", json.dumps(new.build_context()))

    async def test_fork_never_overwrites_existing_target(self):
        agent = await self.history()
        node = agent.session.read_records()[-1]["id"]
        child = await agent.session.fork(node, session_id="child")
        original = child.path.read_bytes()
        with self.assertRaises(SessionError):
            await agent.session.fork(node, session_id="child")
        with self.assertRaises(SessionError):
            await agent.session.fork(node, session_id="source")
        self.assertEqual(child.path.read_bytes(), original)

    async def test_memory_fork(self):
        agent = await self.history(session=LocalSession(directory=None))
        child = await agent.session.fork(agent.session.read_records()[-1]["id"])
        self.assertFalse(child.durable)
        self.assertEqual(child.build_context(), agent.session.build_context())

    async def test_legacy_records_gain_stable_ids_without_rewriting(self):
        agent = await self.history()
        lines = []
        for line in agent.session.path.read_bytes().splitlines():
            envelope = json.loads(line)
            envelope.pop("checksum")
            envelope["record"].pop("id")
            envelope["checksum"] = hashlib.sha256(encode(envelope)).hexdigest()
            lines.append(encode(envelope) + b"\n")
        legacy = b"".join(lines)
        agent.session.path.write_bytes(legacy)
        first, second = self.session(), self.session()
        self.assertEqual(first.read_records(), second.read_records())
        self.assertEqual(first.path.read_bytes(), legacy)
        child = await first.fork(first.read_records()[-1]["id"], session_id="legacy-child")
        self.assertEqual(child.build_context(), first.build_context())
        current = Agent(session=first, stream_fn=FakeProvider(answer("ok")))
        await current.compact(summary="legacy summary")
        self.assertTrue(first.path.read_bytes().startswith(legacy))

    async def test_repeated_compaction_replaces_summary_and_preserves_all_history(self):
        agent = await self.history()
        await agent.compact(
            summary="summary one",
            first_kept_message_id=agent.session.context_entries()[-2]["message_id"],
        )
        agent.compactor = Compactor(CompactionSettings(keep_recent_tokens=0))
        provider = FakeProvider(answer("summary two"))
        agent.stream_function = provider
        await agent.compact()
        prompt = json.dumps(provider.contexts[0])
        self.assertIn("summary one", prompt)
        self.assertIn("recent request", prompt)
        self.assertNotIn("old request", prompt)
        self.assertNotIn("summary one", json.dumps(agent.session.build_context()))
        self.assertIn("summary two", json.dumps(self.session().build_context()))
        log = json.dumps(agent.session.read_records())
        self.assertIn("summary one", log)
        self.assertIn("old request", log)

    async def test_generated_compaction_and_split_prefix(self):
        agent = await self.history()
        agent.compactor = Compactor(CompactionSettings(reserve_tokens=200, keep_recent_tokens=1))
        provider = FakeProvider(answer("history summary"), answer("prefix summary"))
        agent.stream_function = provider
        observed = []
        agent.subscribe(lambda event, signal: observed.append(event["type"]))
        result = await agent.compact("preserve constraints")
        self.assertEqual(len(provider.contexts), 2)
        self.assertIn("history summary", result["data"]["summary"])
        self.assertIn("prefix summary", result["data"]["summary"])
        self.assertEqual(observed, ["compaction_start", "compaction_end"])
        self.assertEqual(len(result["data"]["details"]["usage"]), 2)

    async def test_failed_summary_does_not_change_context(self):
        for response in (
            answer(reason="length"),
            answer(reason="error"),
            answer(""),
            answer(calls=[call()]),
        ):
            session = LocalSession(directory=None)
            agent = await self.history(session=session)
            before = session.build_context()
            agent.compactor = Compactor(CompactionSettings(keep_recent_tokens=0))
            agent.stream_function = FakeProvider(response)
            with self.assertRaises(ValueError):
                await agent.compact()
            self.assertEqual(session.build_context(), before)
            self.assertEqual(session.read_records()[-1]["type"], "compaction_failed")
            self.assertFalse(agent.state.is_streaming)

    async def test_compaction_write_failure_keeps_previous_projection(self):
        class Failing(LocalSession):
            async def _persist(self, seq, record):
                if record["type"] == "compaction":
                    raise OSError("disk failed")
                await super()._persist(seq, record)

        agent = await self.history(session=Failing("source", self.directory))
        before = agent.session.build_context()
        with self.assertRaises(SessionError):
            await agent.compact(summary="not committed")
        self.assertEqual(self.session().build_context(), before)
        self.assertEqual(agent.state.messages, before)

    async def test_cancel_during_compaction_commit_preserves_success(self):
        started, finish = asyncio.Event(), asyncio.Event()

        class Slow(LocalSession):
            async def _persist(self, seq, record):
                if record["type"] == "compaction":
                    started.set()
                    await finish.wait()
                await super()._persist(seq, record)

        agent = await self.history(session=Slow("source", self.directory))
        task = asyncio.create_task(agent.compact(summary="committed"))
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        self.assertTrue(agent.state.is_streaming)
        finish.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.session().read_records()[-1]["type"], "compaction")
        self.assertIn("committed", json.dumps(agent.state.messages))

    async def test_auto_compaction_uses_new_projection_before_next_request(self):
        agent = await self.history()
        model = replace(MODEL, context_window=200)
        agent.state.model = model
        agent.compactor = Compactor(CompactionSettings(reserve_tokens=195, keep_recent_tokens=0))
        provider = FakeProvider(answer("automatic summary"), answer("new answer"))
        agent.stream_function = provider
        await agent.prompt("new request")
        self.assertIsNone(agent.state.error_message)
        self.assertEqual(len(provider.contexts), 2)
        self.assertIn("automatic summary", json.dumps(provider.contexts[1]))
        self.assertNotIn("old request", json.dumps(provider.contexts[1]))
        self.assertEqual(self.session().build_context(), agent.state.messages)

    async def test_compacted_failed_request_reuses_exact_input_on_resume(self):
        agent = await self.history()
        await agent.compact(summary="saved summary")

        def crash(*args):
            raise ProcessLost()

        agent.stream_function = crash
        with self.assertRaises(ProcessLost):
            await agent.prompt("next")
        provider = FakeProvider(answer("recovered"))
        recovered = Agent(
            session=self.session(),
            stream_fn=provider,
            compaction=CompactionSettings(reserve_tokens=999999, keep_recent_tokens=0),
        )
        await recovered.resume()
        self.assertEqual(len(provider.contexts), 1)
        self.assertIn("saved summary", json.dumps(provider.contexts[0]))
        self.assertNotIn("old request", json.dumps(provider.contexts[0]))

    async def test_unknown_tool_outcome_remains_unknown_in_fork(self):
        async def crash(*args):
            raise ProcessLost()

        tool = add_tool(crash)
        agent = Agent(
            session=self.session(),
            model=MODEL,
            tools=[tool],
            stream_fn=FakeProvider(answer(calls=[call()])),
        )
        with self.assertRaises(ProcessLost):
            await agent.prompt("work")
        node = agent.session.read_records()[-1]["id"]
        child = await agent.session.fork(node, session_id="pending")
        resumed = Agent(session=child, tools=[tool], stream_fn=FakeProvider(answer()))
        with self.assertRaises(ToolRecoveryRequired):
            await resumed.resume()
        await child.resolve_tool_call("call-1", AgentToolResult([{"type": "text", "text": "5"}]))
        await resumed.resume()
        self.assertFalse(child.resumable)

    async def test_fork_reuses_returned_tool_result_without_reexecution(self):
        executions = []

        async def execute(*args):
            executions.append(1)
            return AgentToolResult([{"type": "text", "text": "5"}])

        tool = add_tool(execute)
        agent = Agent(
            session=self.session(),
            model=MODEL,
            tools=[tool],
            stream_fn=FakeProvider(answer(calls=[call()]), answer()),
        )
        await agent.prompt("work")
        returned = next(r for r in agent.session.read_records() if r["type"] == "tool_returned")
        child = await agent.session.fork(returned["id"], session_id="returned")
        resumed = Agent(session=child, tools=[tool], stream_fn=FakeProvider(answer()))
        await resumed.resume()
        self.assertEqual(executions, [1])

    async def test_reject_cut_between_tool_call_and_result(self):
        agent = Agent(
            session=self.session(),
            model=MODEL,
            tools=[add_tool()],
            stream_fn=FakeProvider(answer(calls=[call()]), answer()),
        )
        await agent.prompt("work")
        result_entry = next(
            e for e in agent.session.context_entries() if e["message"]["role"] == "toolResult"
        )
        with self.assertRaisesRegex(ValueError, "tool results"):
            await agent.compact(summary="bad cut", first_kept_message_id=result_entry["message_id"])
        self.assertIsNone(agent.session.snapshot["compaction"])

    async def test_compact_cannot_discard_pending_work(self):
        agent = Agent(
            session=self.session(), model=MODEL, stream_fn=FakeProvider(answer(reason="error"))
        )
        await agent.prompt("work")
        with self.assertRaises(SessionError):
            await agent.compact(summary="skip")
        self.assertTrue(agent.session.resumable)

    async def test_manual_compaction_uses_current_thinking_level(self):
        agent = await self.history(thinking_level="low")
        agent.compactor = Compactor(CompactionSettings(keep_recent_tokens=0))
        provider = FakeProvider(answer("summary"))
        agent.stream_function = provider
        await agent.compact()
        self.assertEqual(provider.options[0]["reasoning"], "low")

    async def test_system_message_cannot_hide_orphaned_tool_result(self):
        agent = Agent(
            session=self.session(),
            model=MODEL,
            tools=[add_tool()],
            stream_fn=FakeProvider(answer(calls=[call()]), answer()),
        )
        await agent.prompt("work")
        messages = agent.session.build_context()
        index = next(i for i, m in enumerate(messages) if m["role"] == "toolResult")
        messages.insert(index, {"role": "system", "content": "extra rules", "timestamp": 0})
        await agent.session.sync_context(messages)
        reopened = Agent(session=self.session(), stream_fn=FakeProvider())
        boundary = reopened.session.context_entries()[index]["message_id"]
        with self.assertRaisesRegex(ValueError, "tool results"):
            await reopened.compact(summary="bad cut", first_kept_message_id=boundary)
        self.assertIsNone(reopened.session.snapshot["compaction"])

    async def test_auto_compaction_after_tool_batch_keeps_call_result_pair(self):
        async def large_result(*args):
            return AgentToolResult([{"type": "text", "text": "x" * 3000}])

        provider = FakeProvider(answer(calls=[call()]), answer("request summary"), answer("final"))
        agent = Agent(
            session=self.session(),
            model=replace(MODEL, context_window=600),
            tools=[add_tool(large_result)],
            stream_fn=provider,
            compaction=CompactionSettings(reserve_tokens=100, keep_recent_tokens=10),
        )
        await agent.prompt("calculate")
        self.assertIsNone(agent.state.error_message)
        types = [r["type"] for r in agent.session.read_records()]
        self.assertLess(types.index("tools_completed"), types.index("compaction"))
        self.assertEqual(len(provider.contexts), 3)
        final_context = provider.contexts[-1]
        self.assertEqual(
            [m["role"] for m in final_context], ["system", "user", "assistant", "toolResult"]
        )
        self.assertEqual(final_context[-1]["toolCallId"], final_context[-2]["content"][0]["id"])
        self.assertEqual(self.session().build_context(), agent.state.messages)

    async def test_fork_after_in_run_compaction_resumes_without_reexecuting_tools(self):
        executions = []

        async def large_result(*args):
            executions.append(1)
            return AgentToolResult([{"type": "text", "text": "x" * 3000}])

        tool = add_tool(large_result)
        agent = Agent(
            session=self.session(),
            model=replace(MODEL, context_window=600),
            tools=[tool],
            stream_fn=FakeProvider(answer(calls=[call()]), answer("summary"), answer()),
            compaction=CompactionSettings(reserve_tokens=100, keep_recent_tokens=10),
        )
        await agent.prompt("calculate")
        node = next(r["id"] for r in agent.session.read_records() if r["type"] == "compaction")
        child = await agent.session.fork(node, session_id="during-run")
        self.assertTrue(child.resumable)
        provider = FakeProvider(answer("child final"))
        resumed = Agent(session=child, tools=[tool], stream_fn=provider)
        await resumed.resume()
        self.assertEqual(executions, [1])
        self.assertIn("summary", json.dumps(provider.contexts[0]))
        self.assertEqual(child.build_context(), resumed.state.messages)

    async def test_abort_before_summary_retains_old_context(self):
        agent = await self.history()
        before = agent.session.build_context()
        agent.compactor = Compactor(CompactionSettings(keep_recent_tokens=0))

        def observe(event, signal):
            if event["type"] == "compaction_start":
                agent.abort()

        agent.subscribe(observe)
        with self.assertRaises(Exception):
            await agent.compact()
        self.assertEqual(self.session().build_context(), before)
        self.assertEqual(agent.session.read_records()[-1]["type"], "compaction_failed")

    async def test_crash_during_summary_can_repeat_from_original_journal(self):
        agent = await self.history()
        before = agent.session.build_context()

        def crash(*args):
            raise ProcessLost()

        agent.stream_function = crash
        agent.compactor = Compactor(CompactionSettings(keep_recent_tokens=0))
        with self.assertRaises(ProcessLost):
            await agent.compact()
        reopened = self.session()
        self.assertEqual(reopened.build_context(), before)
        self.assertEqual(reopened.read_records()[-1]["type"], "compaction_started")
        recovered = Agent(
            session=reopened,
            stream_fn=FakeProvider(answer("retry summary")),
            compaction=CompactionSettings(keep_recent_tokens=0),
        )
        await recovered.compact()
        self.assertIn("retry summary", json.dumps(self.session().build_context()))

    async def test_fork_publication_failure_never_exposes_a_partial_log(self):
        agent = await self.history()
        original = agent.session.path.read_bytes()
        with patch(
            "agent_runtime.sessions.local.os.replace", side_effect=OSError("publish failed")
        ):
            with self.assertRaises(SessionError):
                await agent.session.fork(
                    agent.session.read_records()[-1]["id"], session_id="failed"
                )
        child = self.session("failed")
        self.assertEqual(child.revision, 0)
        self.assertFalse(child.path.exists())
        self.assertEqual(agent.session.path.read_bytes(), original)

    async def test_replay_and_fork_never_mutate_prior_request_events(self):
        fake = FakeProvider(answer())

        async def provider(model, context, options):
            await options["_session"].commit(
                "provider_parameters",
                parameters={"temperature": 1},
                dropped={},
                adjusted={},
                timeout=20,
            )
            return fake(model, context, options)

        agent = Agent(session=self.session(), model=MODEL, stream_fn=provider)
        await agent.prompt("test")
        original = agent.session.read_records()
        self.assertNotIn(
            "provider_parameters", next(r["data"] for r in original if r["type"] == "model_request")
        )
        child = await agent.session.fork(original[-1]["id"], session_id="parameters")
        self.assertEqual(child.read_records()[:-1], original)


if __name__ == "__main__":
    unittest.main()
