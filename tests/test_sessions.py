"""Recovery tests use real JSONL files and injected faults at execution boundaries."""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from test_loop import MODEL, FakeProvider, add_tool, answer, call

from agent_runtime import (
    Agent,
    AgentToolResult,
    LocalSession,
    SessionBusyError,
    SessionError,
    ToolRecoveryRequired,
)


class ProcessLost(BaseException):
    """Simulate an abrupt exit without the ordinary error-finalization path."""


class SessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def session(self, name="test"):
        return LocalSession(name, self.directory)

    def reopen(self, **kwargs):
        return Agent(session=self.session(), **kwargs)

    async def test_file_layout_projection_and_live_events_are_not_durable(self):
        session = self.session()
        agent = Agent(session=session, model=MODEL, stream_fn=FakeProvider(answer("final")))
        live = []
        agent.subscribe(
            lambda e, s: live.append(session.live.copy()) if e["type"] == "message_update" else None
        )
        await agent.prompt("hello")
        records = session.read_records()
        self.assertTrue(live)
        self.assertEqual(session.live, {})
        self.assertEqual(session.path, self.directory / "test" / "events.jsonl")
        self.assertEqual([r["type"] for r in records].count("model_completed"), 1)
        self.assertNotIn("message_update", [r["type"] for r in records])
        restored = self.session()
        self.assertEqual(restored.snapshot["messages"], agent.state.messages)
        self.assertEqual(restored.snapshot["status"], "completed")
        self.assertEqual(restored.revision, session.revision)
        self.assertFalse(restored.resumable)

    async def test_memory_mode_lives_inside_local_session(self):
        session = LocalSession("memory", directory=None)
        agent = Agent(session=session, model=MODEL, stream_fn=FakeProvider(answer()))
        await agent.prompt("go")
        self.assertFalse(session.durable)
        self.assertIsNone(session.path)
        self.assertTrue(session.read_records())
        self.assertEqual(LocalSession("memory", directory=None).snapshot["messages"], [])

    async def test_two_runs_append_without_duplicate_history(self):
        first = Agent(session=self.session(), model=MODEL, stream_fn=FakeProvider(answer("one")))
        await first.prompt("first")
        second = self.reopen(stream_fn=FakeProvider(answer("two")))
        await second.prompt("second")
        messages = self.session().snapshot["messages"]
        self.assertEqual([m["role"] for m in messages], ["user", "assistant", "user", "assistant"])
        starts = [r for r in self.session().read_records() if r["type"] == "run_started"]
        self.assertNotIn("context", starts[-1]["data"])
        with self.assertRaises(SessionError):
            await second.resume()

    async def test_unfinished_model_request_restores_input_config_and_transform(self):
        transforms = []

        def transform(messages, signal):
            transforms.append(True)
            return [{"role": "user", "content": "transformed", "timestamp": 0}]

        def crash(model, context, options):
            self.assertEqual(context.messages[0]["content"], "transformed")
            raise ProcessLost()

        first = Agent(
            session=self.session(),
            model=MODEL,
            stream_fn=crash,
            parameters={"temperature": 0.4, "max_tokens": 123},
            transform_context=transform,
        )
        with self.assertRaises(ProcessLost):
            await first.prompt("original")
        provider = FakeProvider(answer())
        second = self.reopen(stream_fn=provider, transform_context=transform)
        await second.resume()
        self.assertEqual(len(transforms), 1)
        self.assertEqual(provider.contexts[0][0]["content"], "transformed")
        self.assertEqual(provider.options[0]["temperature"], 0.4)
        self.assertEqual(provider.options[0]["max_tokens"], 123)
        self.assertEqual(second.state.model.id, MODEL.id)
        self.assertEqual(len([m for m in second.state.messages if m["role"] == "user"]), 1)

    async def test_crash_after_model_response_reuses_plan_without_model_reexecution(self):
        first = Agent(
            session=self.session(),
            model=MODEL,
            tools=[add_tool()],
            stream_fn=FakeProvider(answer(calls=[call()])),
        )

        def crash(event, signal):
            if event["type"] == "message_end" and event["message"]["role"] == "assistant":
                raise ProcessLost()

        first.subscribe(crash)
        with self.assertRaises(ProcessLost):
            await first.prompt("go")
        self.assertEqual(self.session().snapshot["phase"], "tools")
        provider = FakeProvider(answer("finished"))
        second = self.reopen(stream_fn=provider, tools=[add_tool()])
        await second.resume()
        self.assertEqual(len(provider.contexts), 1)
        self.assertEqual(provider.contexts[0][-1]["role"], "toolResult")
        self.assertEqual(self.session().snapshot["messages"], second.state.messages)

    async def test_each_completed_tool_survives_without_reexecuting_earlier_tools(self):
        executed = []
        fail = True

        async def execute(id, args, signal, update):
            nonlocal fail
            executed.append(id)
            if id == "second" and fail:
                raise ProcessLost()
            return AgentToolResult([{"type": "text", "text": id}])

        tool = add_tool(execute, replay="safe")
        first = Agent(
            session=self.session(),
            model=MODEL,
            tools=[tool],
            tool_execution="sequential",
            stream_fn=FakeProvider(answer(calls=[call(id="first"), call(id="second")])),
        )
        with self.assertRaises(ProcessLost):
            await first.prompt("go")
        state = self.session().snapshot
        self.assertIn("first", state["results"])
        self.assertIn("second", state["started"])
        fail = False
        provider = FakeProvider(answer("done"))
        second = self.reopen(stream_fn=provider, tools=[tool])
        self.assertEqual(second.tool_execution, "sequential")
        await second.resume()
        self.assertEqual(executed, ["first", "second", "second"])
        results = [m for m in second.state.messages if m["role"] == "toolResult"]
        self.assertEqual([m["toolCallId"] for m in results], ["first", "second"])
        self.assertEqual(self.session().snapshot["messages"], second.state.messages)

    async def test_unknown_unsafe_tool_is_not_silently_retried_and_can_be_reconciled(self):
        executions = []

        async def execute(id, *args):
            executions.append(id)
            raise ProcessLost()

        tool = add_tool(execute)
        first = Agent(
            session=self.session(),
            model=MODEL,
            tools=[tool],
            stream_fn=FakeProvider(answer(calls=[call()])),
        )
        with self.assertRaises(BaseException):
            await first.prompt("go")
        second = self.reopen(stream_fn=FakeProvider(answer()), tools=[tool])
        with self.assertRaises(ToolRecoveryRequired) as caught:
            await second.resume()
        self.assertEqual(caught.exception.call_ids, ["call-1"])
        self.assertEqual(executions, ["call-1"])
        self.assertEqual(second.session.snapshot["status"], "waiting_recovery")
        await second.session.resolve_tool_call(
            "call-1",
            AgentToolResult([{"type": "text", "text": "external operation already succeeded"}]),
        )
        await second.resume()
        self.assertEqual(executions, ["call-1"])
        self.assertIn("already succeeded", second.state.messages[-2]["content"][0]["text"])

    async def test_explicit_authorization_retains_call_id_for_retry(self):
        attempts = []

        async def execute(id, *args):
            attempts.append(id)
            if len(attempts) == 1:
                raise ProcessLost()
            return AgentToolResult()

        tool = add_tool(execute, execution_mode="sequential")
        first = Agent(
            session=self.session(),
            model=MODEL,
            tools=[tool],
            stream_fn=FakeProvider(answer(calls=[call()])),
        )
        with self.assertRaises(ProcessLost):
            await first.prompt("go")
        second = self.reopen(stream_fn=FakeProvider(answer()), tools=[tool])
        await second.session.authorize_tool_retry("call-1")
        await second.resume()
        self.assertEqual(attempts, ["call-1", "call-1"])

    async def test_saved_turn_decision_is_not_recomputed_after_crash(self):
        decisions = []

        def finish(ctx, signal):
            decisions.append(True)
            return {"action": "end"}

        first = Agent(
            session=self.session(),
            model=MODEL,
            finish_turn=finish,
            stream_fn=FakeProvider(answer()),
        )
        first.subscribe(
            lambda e, s: (_ for _ in ()).throw(ProcessLost()) if e["type"] == "turn_end" else None
        )
        with self.assertRaises(ProcessLost):
            await first.prompt("go")
        second = self.reopen(stream_fn=FakeProvider(), finish_turn=finish)
        await second.resume()
        self.assertEqual(decisions, [True])
        self.assertEqual(second.session.snapshot["status"], "completed")

    async def test_cancellation_and_model_error_keep_last_safe_request(self):
        for name, mode in (("cancelled", "cancel"), ("failed", "error")):
            with self.subTest(mode=mode):
                entered = asyncio.Event()

                async def blocked(model, context, options):
                    entered.set()
                    await asyncio.Event().wait()

                first = Agent(
                    session=self.session(name),
                    model=MODEL,
                    stream_fn=blocked if mode == "cancel" else FakeProvider(answer(reason="error")),
                )
                if mode == "cancel":
                    task = asyncio.create_task(first.prompt("go"))
                    await entered.wait()
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                else:
                    await first.prompt("go")
                session = self.session(name)
                self.assertTrue(session.resumable)
                self.assertEqual(session.snapshot["phase"], "model")
                second = Agent(session=session, stream_fn=FakeProvider(answer()))
                await second.resume()
                self.assertEqual([m["role"] for m in second.state.messages], ["user", "assistant"])

    async def test_torn_tail_is_ignored_then_repaired_under_writer_ownership(self):
        first = Agent(session=self.session(), model=MODEL, stream_fn=FakeProvider(answer()))
        await first.prompt("one")
        prefix = first.session.path.read_bytes()
        with first.session.path.open("ab") as file:
            file.write(b'{"format":1,"seq":')
        reopened = self.session()
        self.assertEqual(reopened.snapshot["status"], "completed")
        second = Agent(session=reopened, stream_fn=FakeProvider(answer()))
        await second.prompt("two")
        self.assertTrue(reopened.path.read_bytes().startswith(prefix))
        self.assertEqual(self.session().snapshot["status"], "completed")

    async def test_committed_corruption_is_rejected_not_silently_discarded(self):
        first = Agent(session=self.session(), model=MODEL, stream_fn=FakeProvider(answer()))
        await first.prompt("one")
        lines = first.session.path.read_bytes().splitlines(keepends=True)
        item = json.loads(lines[0])
        item["checksum"] = "bad"
        lines[0] = json.dumps(item).encode() + b"\n"
        first.session.path.write_bytes(b"".join(lines))
        with self.assertRaises(SessionError):
            self.session()

    async def test_writer_guard_prevents_two_local_writers(self):
        first, second = self.session(), self.session()
        await first.acquire()
        try:
            with self.assertRaises(SessionBusyError):
                await second.acquire()
        finally:
            await first.release()
        await second.acquire()
        await second.release()

    async def test_storage_failure_stops_before_tool_and_does_not_fake_a_result(self):
        class FailingSession(LocalSession):
            async def _persist(self, seq, record):
                if record["type"] == "model_completed":
                    raise OSError("disk full")
                await super()._persist(seq, record)

        executed = []
        session = FailingSession("test", self.directory)
        agent = Agent(
            session=session,
            model=MODEL,
            tools=[add_tool(lambda *a: executed.append(a))],
            stream_fn=FakeProvider(answer(calls=[call()])),
        )
        with self.assertRaises(SessionError):
            await agent.prompt("go")
        self.assertFalse(executed)
        self.assertTrue(self.session().resumable)
        self.assertEqual(self.session().snapshot["phase"], "model")
        self.assertFalse(agent.state.is_streaming)

    async def test_configuration_excludes_credentials_and_callbacks(self):
        model = MODEL.__class__(**{**MODEL.__dict__, "headers": {"Authorization": "secret-header"}})
        first = Agent(
            session=self.session(),
            model=model,
            api_key="secret-key",
            parameters={
                "headers": {"x-api-key": "secret-option"},
                "on_parameters": lambda *args: None,
            },
            stream_fn=FakeProvider(answer()),
        )
        await first.prompt("go")
        log = first.session.path.read_text()
        for secret in ("secret-header", "secret-key", "secret-option", "on_parameters"):
            self.assertNotIn(secret, log)

    async def test_restored_tools_must_match_saved_definition_and_version(self):
        first = Agent(
            session=self.session(),
            model=MODEL,
            tools=[add_tool(version="v1")],
            stream_fn=FakeProvider(answer(calls=[call()])),
        )
        first.subscribe(
            lambda e, s: (
                (_ for _ in ()).throw(ProcessLost())
                if e["type"] == "message_end" and e["message"]["role"] == "assistant"
                else None
            )
        )
        with self.assertRaises(ProcessLost):
            await first.prompt("go")
        second = self.reopen(stream_fn=FakeProvider(answer()), tools=[add_tool(version="v2")])
        with self.assertRaises(SessionError):
            await second.resume()

    async def test_queue_consumption_is_not_repeated_after_reopen(self):
        first = Agent(session=self.session(), model=MODEL, stream_fn=FakeProvider(answer()))
        first.steer("queued")
        await first.prompt("first")
        self.assertEqual(self.session().snapshot["steering"], [])
        second = self.reopen(stream_fn=FakeProvider(answer()))
        await second.prompt("second")
        texts = [m["content"][0]["text"] for m in second.state.messages if m["role"] == "user"]
        self.assertEqual(texts.count("queued"), 1)

    async def test_durable_enqueue_survives_before_start(self):
        first = Agent(session=self.session(), model=MODEL, stream_fn=FakeProvider(answer()))
        await first.enqueue("queued")
        second = self.reopen(model=MODEL, stream_fn=FakeProvider(answer()))
        await second.prompt("start")
        texts = [m["content"][0]["text"] for m in second.state.messages if m["role"] == "user"]
        self.assertEqual(texts, ["start", "queued"])

    async def test_reset_abandons_pending_work_without_erasing_audit(self):
        def crash(*args):
            raise ProcessLost()

        first = Agent(session=self.session(), model=MODEL, stream_fn=crash)
        with self.assertRaises(ProcessLost):
            await first.prompt("abandoned")
        second = self.reopen(stream_fn=FakeProvider(answer()))
        with self.assertRaises(SessionError):
            await second.prompt("new")
        await second.reset_session()
        await second.prompt("new")
        self.assertEqual(second.state.messages[0]["content"][0]["text"], "new")
        self.assertIn("abandoned", second.session.path.read_text())

    def test_session_id_cannot_escape_directory(self):
        for value in ("../other", "", "a/b"):
            with self.assertRaises(ValueError):
                LocalSession(value, self.directory)

    def test_database_file_is_reserved_without_implementation(self):
        from agent_runtime.sessions import database

        self.assertFalse(hasattr(database, "DatabaseSession"))

    async def test_raw_tool_result_survives_crash_in_after_hook(self):
        executions, decisions = [], []
        fail = True

        async def execute(id, *args):
            executions.append(id)
            return AgentToolResult([{"type": "text", "text": "raw"}])

        def after(ctx, signal):
            decisions.append(True)
            if fail:
                raise ProcessLost()
            return {"content": [{"type": "text", "text": "processed"}]}

        tool = add_tool(execute, execution_mode="sequential")
        first = Agent(
            session=self.session(),
            model=MODEL,
            tools=[tool],
            after_tool_call=after,
            stream_fn=FakeProvider(answer(calls=[call()])),
        )
        with self.assertRaises(ProcessLost):
            await first.prompt("go")
        self.assertIn("call-1", self.session().snapshot["returned"])
        fail = False
        second = self.reopen(stream_fn=FakeProvider(answer()), tools=[tool], after_tool_call=after)
        await second.resume()
        self.assertEqual(executions, ["call-1"])
        self.assertEqual(decisions, [True, True])
        self.assertEqual(second.state.messages[-2]["content"][0]["text"], "processed")

    async def test_missing_approval_hook_is_rejected_on_resume(self):
        first = Agent(
            session=self.session(),
            model=MODEL,
            tools=[add_tool()],
            before_tool_call=lambda ctx, sig: None,
            stream_fn=FakeProvider(answer(calls=[call()])),
        )
        first.subscribe(
            lambda e, s: (
                (_ for _ in ()).throw(ProcessLost())
                if e["type"] == "message_end" and e["message"]["role"] == "assistant"
                else None
            )
        )
        with self.assertRaises(ProcessLost):
            await first.prompt("go")
        second = self.reopen(stream_fn=FakeProvider(answer()), tools=[add_tool()])
        with self.assertRaisesRegex(SessionError, "required runtime hooks"):
            await second.resume()

    async def test_stale_agent_cannot_overwrite_newer_history(self):
        first = Agent(session=self.session(), model=MODEL, stream_fn=FakeProvider(answer()))
        stale = Agent(session=self.session(), model=MODEL, stream_fn=FakeProvider(answer()))
        await first.prompt("first")
        with self.assertRaisesRegex(SessionError, "another writer"):
            await stale.prompt("second")
        self.assertEqual(self.session().snapshot["messages"][0]["content"][0]["text"], "first")

    async def test_repeated_cancel_waits_for_commit_before_releasing_writer(self):
        entered, finish = asyncio.Event(), asyncio.Event()

        class SlowSession(LocalSession):
            async def _persist(self, seq, record):
                if record["type"] == "run_started":
                    entered.set()
                    await finish.wait()
                await super()._persist(seq, record)

        session = SlowSession("test", self.directory)
        agent = Agent(session=session, model=MODEL, stream_fn=FakeProvider(answer()))
        task = asyncio.create_task(agent.prompt("go"))
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(SessionBusyError):
            await self.session().acquire()
        finish.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        restored = self.session()
        self.assertTrue(restored.resumable)
        self.assertEqual(restored.snapshot["messages"][0]["content"][0]["text"], "go")
        await restored.acquire()
        await restored.release()

    async def test_abort_before_tool_preserves_unexecuted_plan(self):
        first = Agent(
            session=self.session(),
            model=MODEL,
            tools=[add_tool()],
            stream_fn=FakeProvider(answer(calls=[call()])),
        )
        first.subscribe(
            lambda e, s: (
                first.abort()
                if e["type"] == "message_end" and e["message"]["role"] == "assistant"
                else None
            )
        )
        await first.prompt("go")
        self.assertEqual(self.session().snapshot["results"], {})
        second = self.reopen(stream_fn=FakeProvider(answer()), tools=[add_tool()])
        await second.resume()
        self.assertEqual(second.state.messages[-2]["content"][0]["text"], "5")

    async def test_idempotency_identity_is_stable_across_recovery(self):
        keys = []
        session = self.session()

        async def execute(id, *args):
            keys.append(session.tool_operation_id(id))
            if len(keys) == 1:
                raise ProcessLost()
            return AgentToolResult()

        tool = add_tool(execute, replay="safe", execution_mode="sequential")
        first = Agent(
            session=session,
            model=MODEL,
            tools=[tool],
            stream_fn=FakeProvider(answer(calls=[call()])),
        )
        with self.assertRaises(ProcessLost):
            await first.prompt("go")
        session = self.session()
        second = Agent(session=session, tools=[tool], stream_fn=FakeProvider(answer()))
        await second.resume()
        self.assertEqual(keys[0], keys[1])

    async def test_actual_process_exit_keeps_journal_and_releases_writer(self):
        import sys

        from agent_runtime import AgentTool

        script = """
import asyncio, os, sys
from agent_runtime import Agent, AgentTool, LocalSession, Model, AssistantMessageEventStream
from agent_runtime import assistant_message
model = Model("mock", "mock", "mock", "https://example.invalid")
async def execute(*args):
    os._exit(23)
tool = AgentTool("work", "Work", {"type":"object"}, execute, replay="safe")
def stream(model, context, options):
    result = AssistantMessageEventStream()
    result.push({"type":"done", "message":assistant_message(model, stopReason="toolUse",
        content=[{"type":"toolCall","id":"work-1","name":"work","arguments":{}}])})
    return result
asyncio.run(Agent(session=LocalSession("test", sys.argv[1]), model=model,
    tools=[tool], stream_fn=stream).prompt("go"))
"""
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", script, str(self.directory)
        )
        self.assertEqual(await process.wait(), 23)
        session = self.session()
        self.assertIn("work-1", session.snapshot["started"])
        executed = []

        async def work(*args):
            executed.append(True)
            return AgentToolResult()

        second = Agent(
            session=session,
            tools=[AgentTool("work", "Work", {"type": "object"}, work, replay="safe")],
            stream_fn=FakeProvider(answer()),
        )
        await second.resume()
        self.assertEqual(executed, [True])
        self.assertEqual(session.snapshot["status"], "completed")

    async def test_responses_recovery_uses_saved_effective_provider_parameters(self):
        import httpx
        from test_providers import response_events, sse

        from agent_runtime import Models, ParameterPolicy, ProviderConfig

        requested = []

        def success(request):
            requested.append(json.loads(request.content))
            return httpx.Response(200, content=sse(response_events()))

        model = MODEL.__class__(
            "deployment", "tenant", "openai-responses", "https://example.invalid"
        )
        provider = ProviderConfig(
            "tenant",
            api_key="test-only",
            parameter_policy=ParameterPolicy(fixed_values={"temperature": 1}),
        )

        # Use a normal transport error: producer tasks intentionally finalize Exceptions.
        def network_error(request):
            requested.append(json.loads(request.content))
            raise httpx.ConnectError("lost", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(network_error)) as client:
            first = Agent(
                session=self.session(),
                model=model,
                models=Models(env={}, client=client, providers=[provider]),
                parameters={"temperature": 0.2},
            )
            await first.prompt("go")
        provider.parameter_policy = ParameterPolicy()  # Changed policy after restart.
        async with httpx.AsyncClient(transport=httpx.MockTransport(success)) as client:
            second = self.reopen(models=Models(env={}, client=client, providers=[provider]))
            await second.resume()
        self.assertEqual([body["temperature"] for body in requested], [1, 1])

    async def test_initial_resume_preserves_prepare_request_updates(self):
        requested = []

        def prepare(ctx, signal):
            return {"model": MODEL.__class__("prepared", "mock", "mock", "https://example.invalid")}

        def stream(model, context, options):
            requested.append(model.id)
            return FakeProvider(answer())(model, context, options)

        first = Agent(
            session=self.session(), model=MODEL, prepare_request=prepare, stream_fn=stream
        )
        first.subscribe(
            lambda e, s: (_ for _ in ()).throw(ProcessLost()) if e["type"] == "turn_start" else None
        )
        with self.assertRaises(ProcessLost):
            await first.prompt("go")
        second = self.reopen(prepare_request=prepare, stream_fn=stream)
        await second.resume()
        self.assertEqual(requested, ["prepared"])

    async def test_queued_input_is_not_lost_when_continue_crashes_before_request(self):
        first = Agent(session=self.session(), model=MODEL, stream_fn=FakeProvider(answer()))
        await first.prompt("first")
        await first.enqueue("queued")
        first.subscribe(
            lambda e, s: (_ for _ in ()).throw(ProcessLost()) if e["type"] == "turn_start" else None
        )
        with self.assertRaises(ProcessLost):
            await first.continue_()
        second = self.reopen(stream_fn=FakeProvider(answer()))
        await second.resume()
        texts = [m["content"][0]["text"] for m in second.state.messages if m["role"] == "user"]
        self.assertEqual(texts, ["first", "queued"])

    async def test_durable_clear_and_transformed_system_queue_consumption(self):
        first = Agent(session=self.session(), model=MODEL, stream_fn=FakeProvider(answer()))
        await first.enqueue("remove me")
        await first.clear_queued_inputs()
        await first.enqueue(
            {
                "role": "system",
                "content": "instruction",
                "timestamp": 0,
                "toolsAdded": [{"name": "old"}],
            }
        )
        await first.prompt("go")
        self.assertEqual(self.session().snapshot["steering"], [])
        self.assertNotIn("remove me", json.dumps(self.session().snapshot["messages"]))

    async def test_parallel_storage_failure_propagates_and_leaves_a_recoverable_prefix(self):
        class FailingSession(LocalSession):
            async def _persist(self, seq, record):
                if record["type"] == "tool_returned":
                    raise OSError("disk full")
                await super()._persist(seq, record)

        agent = Agent(
            session=FailingSession("test", self.directory),
            model=MODEL,
            tools=[add_tool()],
            stream_fn=FakeProvider(answer(calls=[call(id="a"), call(id="b")])),
        )
        with self.assertRaises(SessionError):
            await agent.prompt("go")
        self.assertTrue(self.session().resumable)
        self.assertEqual(self.session().snapshot["phase"], "tools")
        self.assertFalse(agent.state.is_streaming)
