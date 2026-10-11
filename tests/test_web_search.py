"""Native search uses real SDK request/stream/journal paths with synthetic HTTP SSE."""

import json
import tempfile
import unittest
from copy import deepcopy

import httpx
from fixtures import models_for_tests as Models
from test_loop import add_tool
from test_providers import response_events, sse
from test_sessions import ProcessLost

from agent_runtime import Agent, AgentContext, LocalSession, Model, SessionError, user_message
from agent_runtime.ai.messages import build_payload
from agent_runtime.tools import create_web_search_tool
from agent_runtime.transcript import current_tools, tool_declaration


def search_events(*, function_call=False, terminal_only=False):
    events = response_events(tool=function_call, text="An answer with a source.")
    item = {
        "type": "web_search_call",
        "id": "ws_1",
        "status": "completed",
        "action": {"type": "search", "queries": ["SDK documentation"]},
    }
    annotation = {
        "type": "url_citation",
        "start_index": 0,
        "end_index": 9,
        "url": "https://example.com/docs",
        "title": "SDK documentation",
    }
    for event in events[:-1]:
        event["output_index"] += 1
    final = events[-1]["response"]
    final["output"].insert(0, item)
    if not function_call:
        final["output"][1]["content"][0]["annotations"] = [annotation]
    if terminal_only:
        return events[-1:]
    return [
        {
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"type": "web_search_call", "id": "ws_1", "status": "in_progress"},
        },
        *[
            {
                "type": f"response.web_search_call.{status}",
                "output_index": 0,
                "item_id": "ws_1",
            }
            for status in ("in_progress", "searching", "completed")
        ],
        {"type": "response.output_item.done", "output_index": 0, "item": item},
        *events,
    ]


class WebSearchTests(unittest.IsolatedAsyncioTestCase):
    def test_explicit_assembly_preserves_provider_defaults_and_options(self):
        model = Model("test", "openai", "openai-responses", "https://example.invalid/v1")
        self.assertNotIn("tools", build_payload(model, AgentContext([user_message("hi")]), {}))
        tool = create_web_search_tool()
        context = AgentContext([{"role": "system", "toolsAdded": [tool_declaration(tool)]}])
        self.assertEqual(build_payload(model, context, {})["tools"], [{"type": "web_search"}])
        filters = {"allowed_domains": ["example.com"]}
        tool = create_web_search_tool(
            search_context_size="low",
            external_web_access=False,
            filters=filters,
            user_location={"type": "approximate", "country": "CN"},
        )
        filters["allowed_domains"].append("other.com")
        context.messages[0]["toolsAdded"] = [tool_declaration(tool)]
        options = build_payload(model, context, {})["tools"][0]
        self.assertEqual(options["filters"]["allowed_domains"], ["example.com"])
        self.assertEqual(options["search_context_size"], "low")
        self.assertFalse(options["external_web_access"])
        self.assertEqual(options["user_location"]["country"], "CN")
        with self.assertRaises(ValueError):
            create_web_search_tool(search_context_size="invalid")

    def test_other_protocols_reject_native_search(self):
        context = AgentContext(
            [{"role": "system", "toolsAdded": [tool_declaration(create_web_search_tool())]}]
        )
        for api in ("openai-completions", "anthropic-messages"):
            with self.subTest(api=api), self.assertRaisesRegex(ValueError, "requires API"):
                build_payload(Model("test", "test", api, "https://example.invalid"), context, {})

    async def test_search_stream_citations_journal_and_next_request(self):
        requests, events, approvals = [], [], []

        def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, content=sse(search_events()))

        with tempfile.TemporaryDirectory() as directory:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                models = Models(client=client, api_keys={"openai": "test-only"})
                agent = Agent(
                    models=models,
                    model="test-reasoner",
                    session=LocalSession("search", directory),
                    tools=[create_web_search_tool()],
                    before_tool_call=lambda *args: approvals.append(args),
                )
                agent.subscribe(lambda event, signal: events.append(event))
                await agent.prompt("Search the web.")
                self.assertIsNone(agent.state.error_message)
                self.assertEqual(len(requests), 1)
                self.assertEqual(requests[0]["tools"], [{"type": "web_search"}])
                final = deepcopy(agent.state.messages[-1])
                self.assertEqual(final["stopReason"], "stop")
                self.assertEqual(final["content"][0]["type"], "webSearchCall")
                self.assertEqual(
                    final["content"][1]["annotations"][0]["title"], "SDK documentation"
                )
                updates = [
                    e["assistantMessageEvent"] for e in events if e["type"] == "message_update"
                ]
                self.assertEqual(sum(e["type"] == "web_search_start" for e in updates), 1)
                self.assertEqual(sum(e["type"] == "web_search_end" for e in updates), 1)
                self.assertEqual(
                    [e["item"]["status"] for e in updates if e["type"] == "web_search_update"],
                    ["in_progress", "searching", "completed"],
                )
                self.assertTrue(any(e["type"] == "text_delta" for e in updates))
                self.assertFalse(approvals)
                self.assertFalse(any(e["type"].startswith("tool_execution") for e in events))
                restored = LocalSession("search", directory)
                self.assertEqual(restored.snapshot["messages"][-1], final)
                self.assertEqual(
                    [
                        r["data"]["message"]
                        for r in restored.read_records()
                        if r["type"] == "model_completed"
                    ],
                    [final],
                )
                second = Agent(models=models, session=restored, tools=[create_web_search_tool()])
                await second.prompt("Continue.")
                self.assertIsNone(second.state.error_message)
                for item in final["responseOutput"]:
                    self.assertEqual(requests[1]["input"].count(item), 1)
                await second.compact(summary="Search complete.")
                third = Agent(
                    models=models,
                    session=LocalSession("search", directory),
                    tools=[create_web_search_tool()],
                )
                await third.prompt("A new question.")
                self.assertEqual(requests[2]["tools"], [{"type": "web_search"}])
                self.assertFalse(
                    any(i.get("type") == "web_search_call" for i in requests[2]["input"])
                )
                # Omitting the tool on a subsequent turn removes its declaration.
                fourth = Agent(models=models, session=LocalSession("search", directory))
                await fourth.prompt("Answer without searching.")
                self.assertNotIn("tools", requests[3])
                self.assertEqual(current_tools(fourth.state.messages), [])

    async def test_hosted_search_and_local_function_share_one_loop(self):
        requests = []
        tool = add_tool()
        tool.name = "math.add"

        def handler(request):
            requests.append(json.loads(request.content))
            events = search_events(function_call=True) if len(requests) == 1 else response_events()
            if len(requests) == 1:
                for event in events:
                    if (event.get("item") or {}).get("type") == "function_call":
                        event["item"]["name"] = "math__add"
                    if event["type"] == "response.completed":
                        event["response"]["output"][1]["name"] = "math__add"
            return httpx.Response(200, content=sse(events))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            agent = Agent(
                models=Models(client=client, api_keys={"openai": "test-only"}),
                model="test-reasoner",
                session=LocalSession(directory=None),
                tools=[create_web_search_tool(), tool],
            )
            await agent.prompt("Search and add.")
            self.assertIsNone(agent.state.error_message)
            self.assertEqual(len(requests), 2)
            self.assertEqual([t["type"] for t in requests[0]["tools"]], ["web_search", "function"])
            self.assertEqual(requests[0]["tools"][1]["name"], "math__add")
            outputs = [i for i in requests[1]["input"] if i.get("type") == "function_call_output"]
            self.assertEqual(len(outputs), 1)
            self.assertEqual(outputs[0]["output"], "5")
            self.assertEqual(agent.state.messages[-2]["toolName"], "math.add")

    async def test_pending_request_resumes_with_same_native_definition(self):
        def crash(*args):
            raise ProcessLost()

        requests = []

        def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, content=sse(search_events(terminal_only=True)))

        with tempfile.TemporaryDirectory() as directory:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                models = Models(client=client, api_keys={"openai": "test-only"})
                tool = create_web_search_tool(filters={"allowed_domains": ["example.com"]})
                first = Agent(
                    models=models,
                    model="test-reasoner",
                    session=LocalSession("pending", directory),
                    tools=[tool],
                    stream_fn=crash,
                )
                with self.assertRaises(ProcessLost):
                    await first.prompt("Search.")
                wrong = Agent(models=models, session=LocalSession("pending", directory))
                with self.assertRaisesRegex(SessionError, "original tools"):
                    await wrong.resume()
                second = Agent(
                    models=models, session=LocalSession("pending", directory), tools=[tool]
                )
                await second.resume()
                self.assertIsNone(second.state.error_message)
                self.assertEqual(len(requests), 1)
                self.assertEqual(requests[0]["tools"], [tool.options])
                self.assertEqual(second.state.messages[-1]["content"][0]["type"], "webSearchCall")
                self.assertTrue(second.state.messages[-1]["content"][1]["annotations"])

    async def test_multiple_text_parts_keep_citation_offsets(self):
        events = search_events(terminal_only=True)
        parts = events[0]["response"]["output"][1]["content"]
        parts.insert(0, {"type": "output_text", "text": "Prefix. ", "annotations": []})
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=sse(events)))
        ) as client:
            agent = Agent(
                models=Models(client=client, api_keys={"openai": "test-only"}),
                model="test-reasoner",
                session=LocalSession(directory=None),
                tools=[create_web_search_tool()],
            )
            await agent.prompt("Search.")
            self.assertIsNone(agent.state.error_message)
            block = agent.state.messages[-1]["content"][1]
            self.assertTrue(block["text"].startswith("Prefix. "))
            self.assertEqual(block["annotations"][0]["start_index"], 8)
            raw = agent.state.messages[-1]["responseOutput"][1]["content"][1]
            self.assertEqual(raw["annotations"][0]["start_index"], 0)
