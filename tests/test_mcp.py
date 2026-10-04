"""MCP execution through real local HTTP/stdio transports and the Agent journal."""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path

import httpx
from fixtures import models_for_tests
from mcp_fixture import HttpFixture
from test_loop import MODEL, FakeProvider, add_tool, answer, call
from test_providers import response_events, sse

from agent_runtime import AbortSignal, Agent, AgentToolResult, LocalSession, ToolRecoveryRequired
from agent_runtime.ai.messages import build_payload
from agent_runtime.mcp import (
    McpCaller,
    McpCallError,
    McpHeaderProvider,
    McpHttpServer,
    McpStdioServer,
    assemble_tools,
    create_mcp_tool,
)
from agent_runtime.mcp.adapter import mcp_result
from agent_runtime.types import AgentContext, Model

SCHEMA = {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]}
STDIO_FIXTURE = str(Path(__file__).with_name("mcp_fixture.py"))


class Headers(McpHeaderProvider):
    def __init__(self):
        self.seen = []

    async def get_headers(self, context):
        self.seen.append(context)
        return {
            "Authorization": f"Bearer test-{context.values['tenant']}",
            "X-Tenant": context.values["tenant"],
            "X-Call": context.tool_call_id,
            "X-Refresh": str(len(self.seen)),
        }


class McpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.http = HttpFixture()
        self.addAsyncCleanup(asyncio.to_thread, self.http.close)

    def caller(self, **kwargs):
        return McpCaller({"demo": McpHttpServer(self.http.url, timeout=3, **kwargs)})

    def tool(self, name="demo.echo", caller=None, **kwargs):
        return create_mcp_tool(
            name, "Echo a value", SCHEMA, caller=caller or self.caller(), **kwargs
        )

    def requests(self, method):
        return [body for _, _, body in self.http.requests if body and body["method"] == method]

    async def test_http_headers_on_handshake_call_get_and_close_without_discovery(self):
        headers = Headers()
        updates = []
        raw = await self.caller(
            headers={"X-App": "runtime", "Authorization": "overridden"}, header_provider=headers
        ).call(
            "demo.echo",
            {"value": "hello"},
            call_id="c1",
            context={"tenant": "a"},
            on_update=lambda update: updates.append(update),
        )
        self.assertEqual(raw["structuredContent"], {"value": "hello"})
        self.assertEqual(updates[0].details, {"progress": 1, "total": 2})
        self.assertEqual(len(self.requests("tools/call")), 1)
        self.assertFalse(self.requests("tools/list"))
        self.assertTrue({"POST", "GET", "DELETE"} <= {r[0] for r in self.http.requests})
        for _, received, _ in self.http.requests:
            lower = {k.lower(): v for k, v in received.items()}
            self.assertEqual(lower["authorization"], "Bearer test-a")
            self.assertEqual(lower["x-app"], "runtime")
            self.assertEqual(lower["x-call"], "c1")
        self.assertGreater(len(headers.seen), 2)

    async def test_parallel_contexts_never_share_headers(self):
        caller = self.caller(header_provider=Headers())
        await asyncio.gather(
            *(
                caller.call(
                    "demo.echo", {"value": str(i)}, call_id=str(i), context={"tenant": str(i)}
                )
                for i in range(8)
            )
        )
        for _, headers, body in self.http.requests:
            if body and body["method"] == "tools/call":
                value = body["params"]["arguments"]["value"]
                headers = {k.lower(): v for k, v in headers.items()}
                self.assertEqual(headers["authorization"], f"Bearer test-{value}")
                self.assertEqual(headers["x-tenant"], value)
                self.assertEqual(headers["x-call"], value)

    async def test_stdio_transport_progress_environment_and_process_cleanup(self):
        log, pid = self.directory / "calls", self.directory / "pid"
        caller = McpCaller(
            {
                "demo": McpStdioServer(
                    sys.executable,
                    (STDIO_FIXTURE,),
                    env={"MCP_TEST_LOG": str(log), "MCP_TEST_PID": str(pid)},
                    timeout=3,
                )
            }
        )
        updates = []
        raw = await caller.call(
            "demo.echo", {"value": "stdio"}, call_id="s1", on_update=updates.append
        )
        self.assertEqual(raw["content"][0]["text"], "stdio")
        self.assertEqual(updates[0].content[0]["text"], "working")
        self.assertNotIn('"tools/list"', log.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(int(pid.read_text()), 0)

    async def test_assembly_selects_only_configured_tools(self):
        builtin = add_tool()
        tools = assemble_tools(
            [
                {"type": "builtin", "name": "add"},
                {"type": "mcp", "name": "demo.echo", "description": "Echo", "input_schema": SCHEMA},
            ],
            builtins={"add": builtin, "unused": add_tool()},
            mcp_caller=self.caller(),
        )
        self.assertIs(tools[0], builtin)
        self.assertEqual([t.name for t in tools], ["add", "demo.echo"])
        self.assertFalse(self.http.requests)  # Assembly performs no networking.

    async def test_validation_and_approval_precede_any_connection(self):
        for args in ({}, {"value": "blocked"}):
            agent = Agent(
                session=LocalSession(directory=None),
                model=MODEL,
                tools=[self.tool()],
                stream_fn=FakeProvider(answer(calls=[call("demo.echo", args=args)]), answer()),
                before_tool_call=lambda *_: {"block": True, "reason": "denied"},
            )
            await agent.prompt("go")
            self.assertTrue(agent.state.messages[-2]["isError"])
        self.assertFalse(self.http.requests)

    async def test_result_raw_data_and_progress_follow_existing_journal(self):
        session = LocalSession("http", self.directory)
        agent = Agent(
            session=session,
            model=MODEL,
            tools=[self.tool(context={"tenant": "private"})],
            stream_fn=FakeProvider(
                answer(calls=[call("demo.echo", args={"value": "saved"})]), answer()
            ),
        )
        events = []
        agent.subscribe(lambda e, _: events.append(e))
        await agent.prompt("go")
        records = session.read_records()
        returned = next(r for r in records if r["type"] == "tool_returned")
        self.assertEqual(
            returned["data"]["result"]["details"]["mcp"]["structuredContent"], {"value": "saved"}
        )
        self.assertTrue(any(e["type"] == "tool_execution_update" for e in events))
        self.assertNotIn("private", session.path.read_text())
        child = await session.fork(returned["id"], session_id="recovered")
        await Agent(session=child, tools=[self.tool()], stream_fn=FakeProvider(answer())).resume()
        self.assertEqual(len(self.requests("tools/call")), 1)

    async def test_explicit_error_result_is_saved(self):
        for name in ("demo.error", "demo.reject"):
            tool = self.tool(name)
            result = await tool.execute("err", {"value": "bad"}, AbortSignal(), None)
            self.assertTrue(result.is_error)

    async def test_dropped_call_requires_reconciliation_and_does_not_retry(self):
        session = LocalSession("unknown", self.directory)
        tool = self.tool("demo.drop")
        agent = Agent(
            session=session,
            model=MODEL,
            tools=[tool],
            stream_fn=FakeProvider(answer(calls=[call(tool.name, args={"value": "side effect"})])),
        )
        with self.assertRaises(ToolRecoveryRequired):
            await agent.prompt("go")
        self.assertEqual(session.snapshot["status"], "waiting_recovery")
        self.assertEqual(len(self.requests("tools/call")), 1)
        self.assertFalse(any(r["type"] == "tool_returned" for r in session.read_records()))
        resumed = Agent(
            session=LocalSession("unknown", self.directory),
            tools=[tool],
            stream_fn=FakeProvider(answer()),
        )
        with self.assertRaises(ToolRecoveryRequired):
            await resumed.resume()
        self.assertEqual(len(self.requests("tools/call")), 1)
        await resumed.session.resolve_tool_call(
            "call-1", AgentToolResult([{"type": "text", "text": "confirmed"}])
        )
        await resumed.resume()
        self.assertFalse(resumed.session.resumable)

    async def test_timeout_leaves_outcome_unknown(self):
        caller = McpCaller({"demo": McpHttpServer(self.http.url, timeout=0.5)})
        with self.assertRaises(ToolRecoveryRequired):
            await caller.call("demo.wait", {"value": "late"}, call_id="timeout")
        self.assertEqual(len(self.requests("tools/call")), 1)

    async def test_abort_after_dispatch_leaves_outcome_unknown(self):
        signal = AbortSignal()
        task = asyncio.create_task(
            self.caller().call("demo.wait", {}, call_id="abort", signal=signal)
        )
        self.assertTrue(await asyncio.to_thread(self.http.called.wait, 2))
        signal.abort()
        with self.assertRaises(ToolRecoveryRequired):
            await task
        self.assertEqual(len(self.requests("tools/call")), 1)

    async def test_task_cancellation_propagates(self):
        task = asyncio.create_task(self.caller().call("demo.wait", {}, call_id="cancel"))
        self.assertTrue(await asyncio.to_thread(self.http.called.wait, 2))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_stdio_timeout_terminates_process_without_retry(self):
        log, pid = self.directory / "calls", self.directory / "pid"
        caller = McpCaller(
            {
                "demo": McpStdioServer(
                    sys.executable,
                    (STDIO_FIXTURE,),
                    env={"MCP_TEST_LOG": str(log), "MCP_TEST_PID": str(pid)},
                    timeout=0.5,
                )
            }
        )
        with self.assertRaises(ToolRecoveryRequired):
            await caller.call("demo.wait", {}, call_id="stdio-timeout")
        self.assertEqual(log.read_text().count('"tools/call"'), 1)
        with self.assertRaises(ProcessLookupError):
            os.kill(int(pid.read_text()), 0)

    async def test_agent_task_cancel_preserves_pending_call_on_reopen(self):
        session = LocalSession("cancelled", self.directory)
        tool = self.tool("demo.wait")
        agent = Agent(
            session=session,
            model=MODEL,
            tools=[tool],
            stream_fn=FakeProvider(answer(calls=[call(tool.name, args={"value": "x"})])),
        )
        task = asyncio.create_task(agent.prompt("go"))
        self.assertTrue(await asyncio.to_thread(self.http.called.wait, 2))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        resumed = Agent(
            session=LocalSession("cancelled", self.directory),
            tools=[tool],
            stream_fn=FakeProvider(answer()),
        )
        with self.assertRaises(ToolRecoveryRequired):
            await resumed.resume()
        self.assertEqual(len(self.requests("tools/call")), 1)

    async def test_parallel_failure_keeps_acknowledged_sibling_result(self):
        session = LocalSession("parallel", self.directory)
        tool = self.tool("demo.drop_after")
        agent = Agent(
            session=session,
            model=MODEL,
            tools=[tool, add_tool()],
            stream_fn=FakeProvider(
                answer(
                    calls=[
                        call("add", "known", {"a": 1, "b": 2}),
                        call(tool.name, "unknown", {"value": "x"}),
                    ]
                )
            ),
        )

        def observe(event, signal):
            if event["type"] == "tool_execution_end" and event["toolCallId"] == "known":
                self.http.release.set()

        agent.subscribe(observe)
        with self.assertRaises(ToolRecoveryRequired) as error:
            await agent.prompt("go")
        self.assertEqual(error.exception.call_ids, ["unknown"])
        self.assertIsNotNone(session.returned_result("known"))
        self.assertIsNone(session.returned_result("unknown"))
        self.assertEqual(session.snapshot["status"], "waiting_recovery")

    async def test_failure_before_dispatch_does_not_leak_credentials(self):
        class BrokenHeaders(McpHeaderProvider):
            async def get_headers(self, context):
                raise RuntimeError("secret-token-value")

        with self.assertRaises(McpCallError) as error:
            await self.caller(header_provider=BrokenHeaders()).call(
                "demo.echo", {}, call_id="no-call"
            )
        self.assertNotIn("secret-token", str(error.exception))
        self.assertFalse(self.requests("tools/call"))

    async def test_cleanup_failure_keeps_received_result(self):
        class Caller(McpCaller):
            @asynccontextmanager
            async def _connect(self, *args):
                async with super()._connect(*args) as session:
                    yield session
                raise RuntimeError("close failed")

        caller = Caller({"demo": McpHttpServer(self.http.url)})
        result = await caller.call("demo.echo", {"value": "acknowledged"}, call_id="close")
        self.assertEqual(result["content"][0]["text"], "acknowledged")

    def test_pi_content_projection_preserves_full_raw_result(self):
        raw = {
            "content": [
                {"type": "text", "text": "text", "_meta": {"private": True}},
                {"type": "image", "data": "AA==", "mimeType": "image/png"},
                {"type": "audio", "data": "AA==", "mimeType": "audio/wav"},
                {"type": "resource", "resource": {"uri": "test://text", "text": "embedded"}},
                {
                    "type": "resource",
                    "resource": {"uri": "test://image", "blob": "AA==", "mimeType": "image/png"},
                },
                {"type": "resource_link", "uri": "test://link", "name": "link"},
            ]
        }
        result = mcp_result(raw)
        self.assertEqual(result.details["mcp"], raw)
        self.assertNotIn("_meta", result.content[0])
        self.assertEqual(result.content[3]["text"], "embedded")
        self.assertEqual(result.content[4]["type"], "image")
        structured = mcp_result({"content": [], "structuredContent": {"ok": True}})
        self.assertEqual(json.loads(structured.content[0]["text"]), {"ok": True})

    async def test_responses_wire_names_roundtrip_and_replay(self):
        requests, observed = [], []

        def handle(request):
            requests.append(json.loads(request.content))
            events = response_events(tool=len(requests) == 1)
            if len(requests) == 1:
                events = json.loads(
                    json.dumps(events)
                    .replace('"add"', '"demo__echo"')
                    .replace('{\\"a\\":2,\\"b\\":3}', '{\\"value\\":\\"wire\\"}')
                )
            return httpx.Response(200, content=sse(events))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            models = models_for_tests(client=http, api_keys={"openai": "test-only"})
            agent = Agent(
                session=LocalSession(directory=None),
                models=models,
                model="test-reasoner",
                tools=[self.tool()],
                parameters={"tool_choice": {"type": "function", "name": "demo.echo"}},
            )
            agent.subscribe(lambda event, _: observed.append(event))
            await agent.prompt("echo")
        self.assertIsNone(agent.state.error_message)
        self.assertEqual(requests[0]["tools"][0]["name"], "demo__echo")
        self.assertEqual(requests[0]["tool_choice"]["name"], "demo__echo")
        self.assertEqual(len(self.requests("tools/call")), 1)
        assistant = next(m for m in agent.state.messages if m["role"] == "assistant")
        self.assertEqual(assistant["content"][0]["name"], "demo.echo")
        self.assertEqual(assistant["responseOutput"][0]["name"], "demo__echo")
        replayed = next(i for i in requests[1]["input"] if i.get("type") == "function_call")
        self.assertEqual(replayed["name"], "demo__echo")
        for event in observed:
            update = event.get("assistantMessageEvent", {})
            if update.get("type") == "toolcall_end":
                self.assertEqual(update["toolCall"]["name"], "demo.echo")
            for block in event.get("message", {}).get("content", []):
                if block["type"] == "toolCall":
                    self.assertEqual(block["name"], "demo.echo")

    def test_other_protocols_encode_tool_names_without_changing_history(self):
        messages = [
            {
                "role": "system",
                "content": "",
                "toolsAdded": [{"name": "demo.echo", "description": "echo", "parameters": SCHEMA}],
            },
            answer(calls=[call("demo.echo", args={"value": "x"})]),
        ]
        original = deepcopy(messages)
        for api in ("openai-completions", "anthropic-messages"):
            payload = build_payload(Model("test", "test", api, ""), AgentContext(messages), {})
            self.assertNotIn("demo.echo", json.dumps(payload))
            self.assertIn("demo__echo", json.dumps(payload))
        self.assertEqual(messages, original)


if __name__ == "__main__":
    unittest.main()
