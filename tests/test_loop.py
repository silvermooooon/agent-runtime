"""Behavior cases adapted from pi packages/agent/test/{agent,agent-loop}.test.ts."""

import asyncio
import unittest
from copy import deepcopy

from agent_runtime import (
    Agent,
    AgentContext,
    AgentLoopConfig,
    AgentTool,
    AgentToolResult,
    AssistantMessageEventStream,
    Model,
    agent_loop,
    assistant_message,
    run_agent_loop,
    run_agent_loop_continue,
    run_tool_call,
    user_message,
)
from agent_runtime.transcript import current_tools

MODEL = Model("mock", "mock", "mock", "https://example.invalid")


def answer(text="done", *, calls=None, reason=None):
    content = calls or [{"type": "text", "text": text}]
    return assistant_message(
        MODEL, content=content, stopReason=reason or ("toolUse" if calls else "stop")
    )


def call(name="add", id="call-1", args=None):
    return {
        "type": "toolCall",
        "id": id,
        "name": name,
        "arguments": {"a": 2, "b": 3} if args is None else args,
    }


class FakeProvider:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.contexts = []
        self.options = []

    def __call__(self, model, context, options):
        self.contexts.append(deepcopy(context.messages))
        self.options.append(options)
        message = deepcopy(self.responses.pop(0))
        stream = AssistantMessageEventStream()
        stream.push({"type": "start", "partial": assistant_message(model)})
        stream.push({"type": "text_delta", "delta": "preview", "partial": message})
        if message["stopReason"] in ("error", "aborted"):
            stream.push({"type": "error", "error": message})
        else:
            stream.push({"type": "done", "message": message})
        return stream


def add_tool(execute=None, **kwargs):
    async def add(id, args, signal, update):
        return AgentToolResult([{"type": "text", "text": str(args["a"] + args["b"])}])

    return AgentTool(
        "add",
        "Add",
        {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
            "additionalProperties": False,
        },
        execute or add,
        **kwargs,
    )


class LoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_basic_event_order_and_state(self):
        fake = FakeProvider(answer())
        agent = Agent(model=MODEL, stream_fn=fake)
        events = []
        agent.subscribe(lambda e, s: events.append(e["type"]))
        await agent.prompt("hello")
        self.assertEqual(
            events,
            [
                "agent_start",
                "turn_start",
                "message_start",
                "message_end",
                "message_start",
                "message_update",
                "message_end",
                "turn_end",
                "agent_end",
            ],
        )
        self.assertEqual([m["role"] for m in agent.state.messages], ["user", "assistant"])
        self.assertFalse(agent.state.is_streaming)

    async def test_tools_coercion_hooks_and_second_request(self):
        fake = FakeProvider(answer(calls=[call(args={"a": "2", "b": 3})]), answer("5"))
        seen = []

        async def before(ctx, signal):
            seen.append(ctx["args"])

        agent = Agent(model=MODEL, stream_fn=fake, tools=[add_tool()], before_tool_call=before)
        await agent.prompt("sum")
        self.assertEqual(seen, [{"a": 2, "b": 3}])
        self.assertEqual(len(fake.contexts), 2)
        self.assertEqual(fake.contexts[-1][-1]["role"], "toolResult")
        self.assertEqual(fake.contexts[-1][-1]["content"][0]["text"], "5.0")

    async def test_unknown_and_invalid_tools_become_error_results(self):
        fake = FakeProvider(
            answer(calls=[call("missing"), call(id="2", args={"a": "wrong"})]), answer()
        )
        agent = Agent(model=MODEL, stream_fn=fake, tools=[add_tool()])
        await agent.prompt("go")
        results = [m for m in agent.state.messages if m["role"] == "toolResult"]
        self.assertEqual(len(results), 2)
        self.assertTrue(all(m["isError"] for m in results))

    async def test_truncated_arguments_never_execute(self):
        called = []
        fake = FakeProvider(answer(calls=[call()], reason="length"), answer())
        agent = Agent(
            model=MODEL, stream_fn=fake, tools=[add_tool(lambda *args: called.append(args))]
        )
        await agent.prompt("go")
        self.assertFalse(called)
        self.assertIn("token limit", fake.contexts[-1][-1]["content"][0]["text"])

    async def test_parallel_completion_order_and_source_order(self):
        second_started = asyncio.Event()

        async def execute(id, args, signal, update):
            if id == "first":
                await second_started.wait()
            else:
                second_started.set()
            return AgentToolResult([{"type": "text", "text": id}])

        fake = FakeProvider(answer(calls=[call(id="first"), call(id="second")]), answer())
        agent = Agent(model=MODEL, stream_fn=fake, tools=[add_tool(execute)])
        completions = []
        agent.subscribe(
            lambda e, s: (
                completions.append(e["toolCallId"]) if e["type"] == "tool_execution_end" else None
            )
        )
        await asyncio.wait_for(agent.prompt("go"), 2)
        self.assertEqual(completions, ["second", "first"])
        self.assertEqual(
            [m["toolCallId"] for m in agent.state.messages if m["role"] == "toolResult"],
            ["first", "second"],
        )

    async def test_parallel_preflight_finishes_before_execution(self):
        trace = []

        async def before(ctx, signal):
            trace.append("prepare-" + ctx["toolCall"]["id"])

        async def execute(id, args, signal, update):
            trace.append("execute-" + id)
            return AgentToolResult()

        fake = FakeProvider(answer(calls=[call(id="1"), call(id="2")]), answer())
        agent = Agent(
            model=MODEL, stream_fn=fake, tools=[add_tool(execute)], before_tool_call=before
        )
        await agent.prompt("go")
        self.assertEqual(trace[:2], ["prepare-1", "prepare-2"])

    async def test_per_tool_sequential_override(self):
        trace = []

        async def execute(id, args, signal, update):
            trace.append("start-" + id)
            await asyncio.sleep(0)
            trace.append("end-" + id)
            return AgentToolResult()

        fake = FakeProvider(answer(calls=[call(id="1"), call(id="2")]), answer())
        agent = Agent(
            model=MODEL, stream_fn=fake, tools=[add_tool(execute, execution_mode="sequential")]
        )
        await agent.prompt("go")
        self.assertEqual(trace, ["start-1", "end-1", "start-2", "end-2"])

    async def test_block_and_terminate_skip_execution_and_next_model(self):
        fake = FakeProvider(answer(calls=[call()]))
        called = []
        agent = Agent(
            model=MODEL,
            stream_fn=fake,
            tools=[add_tool(lambda *a: called.append(a))],
            before_tool_call=lambda ctx, sig: {"block": True, "terminate": True},
        )
        await agent.prompt("go")
        self.assertFalse(called)
        self.assertEqual(len(fake.contexts), 1)

    async def test_after_hook_content_replacement_drops_structured_content(self):
        async def execute(*args):
            return AgentToolResult(
                [{"type": "text", "text": "before"}], structured_content={"x": 1}
            )

        tool = add_tool(execute)
        result = await run_tool_call(
            call(),
            tools=[tool],
            assistant_message=answer(calls=[call()]),
            context=AgentContext(tools=[tool]),
            after_tool_call=lambda c, s: {"content": [], "isError": True},
        )
        self.assertNotIn("structuredContent", result["result"])
        self.assertTrue(result["isError"])

    async def test_finish_turn_order_and_explicit_continuation(self):
        fake = FakeProvider(answer("first"), answer("second"))
        trace = []

        def finish(ctx, signal):
            trace.append("finish")
            if len(fake.contexts) == 1:
                return {"action": "continue"}

        agent = Agent(model=MODEL, stream_fn=fake, finish_turn=finish)
        agent.subscribe(lambda e, s: trace.append(e["type"]))
        await agent.prompt("go")
        self.assertEqual(len(fake.contexts), 2)
        self.assertLess(trace.index("finish"), trace.index("turn_end"))

    async def test_finish_end_preserves_queues(self):
        fake = FakeProvider(answer())
        agent = Agent(model=MODEL, stream_fn=fake, finish_turn=lambda c, s: {"action": "end"})
        agent.follow_up("later")
        await agent.prompt("go")
        self.assertTrue(agent.has_queued_messages())
        self.assertEqual(len(fake.contexts), 1)

    async def test_steering_then_follow_up(self):
        fake = FakeProvider(answer(), answer(), answer())
        agent = Agent(model=MODEL, stream_fn=fake)
        agent.steer("s1")
        agent.steer("s2")
        agent.follow_up("follow")
        await agent.prompt("go")
        self.assertEqual(
            [m["content"][0]["text"] for m in agent.state.messages if m["role"] == "user"],
            ["go", "s1", "s2", "follow"],
        )

    async def test_transform_before_convert_and_refresh_key_per_call(self):
        trace = []

        def transform(messages, signal):
            trace.append("transform")
            return messages

        def convert(messages):
            trace.append("convert")
            return messages

        fake = FakeProvider(answer(calls=[call()]), answer())
        keys = iter(["first", "second"])
        agent = Agent(
            model=MODEL,
            stream_fn=fake,
            tools=[add_tool()],
            transform_context=transform,
            convert_to_llm=convert,
            get_api_key=lambda provider: next(keys),
        )
        await agent.prompt("go")
        self.assertEqual(trace, ["transform", "convert", "transform", "convert"])
        self.assertEqual([o["api_key"] for o in fake.options], ["first", "second"])

    async def test_prepare_request_replaces_model_and_context(self):
        fake = FakeProvider(answer())
        replacement = Model("new", "other", "mock", "")
        agent = Agent(
            model=MODEL,
            stream_fn=fake,
            prepare_request=lambda c, s: {
                "model": replacement,
                "context": AgentContext([user_message("replacement")]),
                "thinking_level": "high",
            },
        )
        await agent.prompt("go")
        self.assertEqual(fake.contexts[0][0]["content"][0]["text"], "replacement")
        self.assertEqual(fake.options[0]["reasoning"], "high")

    async def test_tool_changes_are_in_transcript_and_reset_keeps_baseline(self):
        fake = FakeProvider(answer(), answer())
        agent = Agent(model=MODEL, stream_fn=fake, tools=[add_tool()], system_prompt="base")
        await agent.prompt("one")
        agent.state.tools = []
        await agent.prompt("two")
        self.assertEqual(current_tools(fake.contexts[1]), [])
        agent.reset()
        self.assertEqual(len(agent.state.messages), 1)
        self.assertEqual(agent.state.system_prompt, "base")

    async def test_continue_does_not_reemit_old_input(self):
        fake = FakeProvider(answer())
        events = []
        result = await run_agent_loop_continue(
            AgentContext([user_message("old")]),
            AgentLoopConfig(MODEL),
            events.append,
            stream_fn=fake,
        )
        self.assertEqual(len(result), 1)
        self.assertFalse(any(e.get("message", {}).get("role") == "user" for e in events))
        with self.assertRaises(ValueError):
            await run_agent_loop_continue(
                AgentContext([answer()]), AgentLoopConfig(MODEL), events.append, stream_fn=fake
            )

    async def test_continue_assistant_uses_one_steering_message(self):
        fake = FakeProvider(answer(), answer())
        agent = Agent(model=MODEL, stream_fn=fake, messages=[user_message("old"), answer()])
        agent.steer("one")
        agent.steer("two")
        await agent.continue_()
        self.assertEqual(fake.contexts[0][-1]["content"][0]["text"], "one")
        self.assertEqual(fake.contexts[1][-1]["content"][0]["text"], "two")

    async def test_idle_waits_for_agent_end_listener_and_rejects_reentry(self):
        entered, release = asyncio.Event(), asyncio.Event()
        agent = Agent(model=MODEL, stream_fn=FakeProvider(answer()))

        async def listener(event, signal):
            if event["type"] == "agent_end":
                entered.set()
                await release.wait()

        agent.subscribe(listener)
        task = asyncio.create_task(agent.prompt("go"))
        await entered.wait()
        idle = asyncio.create_task(agent.wait_for_idle())
        await asyncio.sleep(0)
        self.assertFalse(idle.done())
        with self.assertRaises(RuntimeError):
            await agent.prompt("again")
        with self.assertRaises(RuntimeError):
            agent.reset()
        release.set()
        await task
        await idle

    async def test_late_tool_updates_are_ignored(self):
        updates, captured = [], []

        async def execute(id, args, signal, update):
            captured.append(update)
            update(AgentToolResult([{"type": "text", "text": "early"}]))
            return AgentToolResult()

        agent = Agent(
            model=MODEL,
            stream_fn=FakeProvider(answer(calls=[call()]), answer()),
            tools=[add_tool(execute)],
        )
        agent.subscribe(
            lambda e, s: updates.append(e) if e["type"] == "tool_execution_update" else None
        )
        await agent.prompt("go")
        captured[0](AgentToolResult())
        await asyncio.sleep(0)
        self.assertEqual(len(updates), 1)

    async def test_sink_failure_stops_before_tool_execution(self):
        called = []

        async def emit(event):
            if event["type"] == "message_end" and event["message"]["role"] == "assistant":
                raise RuntimeError("storage unavailable")

        with self.assertRaisesRegex(RuntimeError, "storage"):
            await run_agent_loop(
                [user_message("go")],
                AgentContext(tools=[add_tool(lambda *a: called.append(a))]),
                AgentLoopConfig(MODEL),
                emit,
                stream_fn=FakeProvider(answer(calls=[call()])),
            )
        self.assertFalse(called)

    async def test_low_level_stream_failure_settles_result(self):
        def broken(*args):
            raise RuntimeError("broken provider contract")

        stream = agent_loop(
            [user_message("go")], AgentContext(), AgentLoopConfig(MODEL), stream_fn=broken
        )
        with self.assertRaisesRegex(RuntimeError, "broken"):
            await asyncio.wait_for(stream.result(), 1)

    async def test_failing_stream_listener_closes_background_producer(self):
        closed = asyncio.Event()

        def streaming(model, context, options):
            stream = AssistantMessageEventStream()

            async def produce():
                try:
                    stream.push({"type": "start", "partial": assistant_message(model)})
                    await asyncio.Event().wait()
                finally:
                    closed.set()

            stream.task = asyncio.create_task(produce())
            return stream

        def emit(event):
            if event["type"] == "message_start" and event["message"]["role"] == "assistant":
                raise RuntimeError("sink failed")

        with self.assertRaisesRegex(RuntimeError, "sink failed"):
            await run_agent_loop(
                [user_message("go")],
                AgentContext(),
                AgentLoopConfig(MODEL),
                emit,
                stream_fn=streaming,
            )
        self.assertTrue(closed.is_set())

    async def test_error_response_is_hard_exit_even_when_finish_requests_continue(self):
        fake = FakeProvider(answer(reason="error"))
        agent = Agent(model=MODEL, stream_fn=fake, finish_turn=lambda c, s: {"action": "continue"})
        agent.follow_up("later")
        await agent.prompt("go")
        self.assertEqual(len(fake.contexts), 1)
        self.assertTrue(agent.has_queued_messages())
