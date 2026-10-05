import asyncio
import json
import unittest
from copy import deepcopy

import httpx
from fixtures import models_for_tests as Models
from test_loop import add_tool

from agent_runtime import (
    AbortSignal,
    Agent,
    AgentContext,
    Model,
    stream_proxy,
    user_message,
)
from agent_runtime.ai.messages import build_payload


def sse(events, *, final_newline=True):
    text = "\n\n".join("data: " + (e if isinstance(e, str) else json.dumps(e)) for e in events)
    return (text + ("\n\n" if final_newline else "")).encode()


def response_events(*, tool=False, text="hello", reasoning=False):
    items = []
    if reasoning:
        items.append(
            {"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "opaque"}
        )
    if tool:
        item = {
            "type": "function_call",
            "id": "fc_1",
            "call_id": "call_1",
            "name": "add",
            "arguments": '{"a":2,"b":3}',
            "status": "completed",
        }
    else:
        item = {
            "type": "message",
            "id": "msg_1",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": text, "annotations": []}],
        }
    index = len(items)
    items.append(item)
    start = deepcopy(item)
    if tool:
        start["arguments"] = ""
        delta = {
            "type": "response.function_call_arguments.delta",
            "output_index": index,
            "delta": item["arguments"],
        }
    else:
        start["content"] = []
        delta = {"type": "response.output_text.delta", "output_index": index, "delta": text}
    events = [
        {"type": "response.output_item.added", "output_index": index, "item": start},
        delta,
        {"type": "response.output_item.done", "output_index": index, "item": item},
        {
            "type": "response.completed",
            "response": {
                "id": "resp_1",
                "status": "completed",
                "output": items,
                "usage": {
                    "input_tokens": 20,
                    "output_tokens": 5,
                    "input_tokens_details": {"cached_tokens": 3},
                },
            },
        },
    ]
    return events


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    def test_tool_images_survive_provider_conversion_and_preserve_batch_order(self):
        image = {"type": "image", "mimeType": "image/png", "data": "aW1hZ2U="}
        results = [
            {
                "role": "toolResult",
                "toolCallId": f"call_{index}",
                "content": [{"type": "text", "text": f"result {index}"}, image],
            }
            for index in range(2)
        ]
        for api in ("openai-responses", "openai-completions", "anthropic-messages"):
            with self.subTest(api=api):
                model = Model("vision", "test", api, "https://example.invalid/v1")
                payload = build_payload(model, AgentContext(results), {})
                if api == "openai-responses":
                    items = payload["input"]
                    self.assertEqual([i["call_id"] for i in items], ["call_0", "call_1"])
                    self.assertEqual(
                        items[0]["output"][1]["image_url"], "data:image/png;base64,aW1hZ2U="
                    )
                elif api == "openai-completions":
                    items = payload["messages"]
                    self.assertEqual([i["role"] for i in items], ["tool", "tool", "user"])
                    self.assertEqual(len(items[-1]["content"]), 3)
                    self.assertEqual(
                        items[-1]["content"][1]["image_url"]["url"],
                        "data:image/png;base64,aW1hZ2U=",
                    )
                else:
                    items = payload["messages"][0]["content"]
                    self.assertEqual([i["tool_use_id"] for i in items], ["call_0", "call_1"])
                    self.assertEqual(items[0]["content"][1]["source"]["data"], "aW1hZ2U=")

    async def test_responses_full_tool_loop_and_final_output_no_duplication(self):
        requests, raw, reports = [], [], []

        def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, content=sse(response_events(tool=len(requests) == 1)))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            models = Models(api_keys={"openai": "test-only"}, client=client)
            agent = Agent(
                provider="openai",
                model="test-reasoner",
                models=models,
                tools=[add_tool()],
                parameters={
                    "temperature": 0.3,
                    "unsupported": True,
                    "on_provider_stream_event": lambda e, m: raw.append(e),
                    "on_parameters": lambda r, m: reports.append(r),
                },
            )
            events = []
            agent.subscribe(lambda e, s: events.append(e))
            await agent.prompt("2 + 3")
        self.assertIsNone(agent.state.error_message)
        self.assertEqual(len(requests), 2)
        self.assertNotIn("temperature", requests[0])
        self.assertNotIn("unsupported", requests[0])
        self.assertIn("temperature", reports[0].dropped)
        output = agent.state.messages[-1]
        self.assertEqual([b["text"] for b in output["content"] if b["type"] == "text"], ["hello"])
        self.assertEqual(output["usage"]["input"], 17)
        self.assertEqual(output["usage"]["cacheRead"], 3)
        self.assertTrue(
            any(
                i.get("type") == "function_call_output" and i["call_id"] == "call_1"
                for i in requests[1]["input"]
            )
        )
        self.assertTrue(any(e["type"] == "message_update" for e in events))
        self.assertTrue(raw)

    async def test_reasoning_terminal_signature_survives_stateless_replay(self):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, content=sse(response_events(reasoning=True)))
            )
        ) as client:
            models = Models(api_keys={"openai": "test-only"}, client=client)
            model = models.get_model("openai", "test-reasoner")
            result = await models.complete_simple(model, AgentContext([user_message("go")]))
        payload = build_payload(model, AgentContext([user_message("go"), result]), {})
        self.assertTrue(any(i.get("encrypted_content") == "opaque" for i in payload["input"]))
        self.assertEqual(result["stopReason"], "stop")

    async def test_clean_eof_without_terminal_is_error(self):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, content=sse(response_events()[:-1]))
            )
        ) as client:
            models = Models(api_keys={"openai": "test-only"}, client=client)
            result = await models.complete_simple(
                models.get_model("openai", "test-reasoner"), AgentContext()
            )
        self.assertEqual(result["stopReason"], "error")
        self.assertIn("terminal", result["errorMessage"])

    async def test_invalid_final_tool_arguments_are_not_executable(self):
        events = response_events(tool=True)
        events[2]["item"]["arguments"] = '{"a":'
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=sse(events)))
        ) as client:
            models = Models(api_keys={"openai": "test-only"}, client=client)
            executed = []
            agent = Agent(
                provider="openai",
                model="test-reasoner",
                models=models,
                tools=[add_tool(lambda *a: executed.append(a))],
            )
            await agent.prompt("go")
        self.assertFalse(executed)
        self.assertIsNotNone(agent.state.error_message)

    async def test_incomplete_response_is_length_not_success(self):
        events = response_events()
        events[-1]["type"] = "response.incomplete"
        events[-1]["response"]["status"] = "incomplete"
        events[-1]["response"]["incomplete_details"] = {"reason": "max_output_tokens"}
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=sse(events)))
        ) as client:
            models = Models(api_keys={"openai": "test-only"}, client=client)
            result = await models.complete_simple(
                models.get_model("openai", "test-reasoner"), AgentContext()
            )
        self.assertEqual(result["stopReason"], "length")

    async def test_http_error_is_protocol_error_and_settles(self):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(429, json={"error": "rate limit"})
            )
        ) as client:
            models = Models(api_keys={"openai": "test-only"}, client=client)
            stream = await models.stream_simple(
                models.get_model("openai", "test-reasoner"), AgentContext()
            )
            result = await asyncio.wait_for(stream.result(), 1)
        self.assertEqual(result["stopReason"], "error")
        self.assertIn("429", result["errorMessage"])

    async def test_abort_interrupts_stalled_transport_and_closes_stream(self):
        entered, closed = asyncio.Event(), asyncio.Event()

        class BlockingStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                entered.set()
                await asyncio.Event().wait()
                yield b""

            async def aclose(self):
                closed.set()

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=BlockingStream()))
        ) as client:
            models = Models(api_keys={"openai": "test-only"}, client=client)
            agent = Agent(provider="openai", model="test-reasoner", models=models)
            task = asyncio.create_task(agent.prompt("go"))
            await entered.wait()
            agent.abort()
            await asyncio.wait_for(task, 1)
            self.assertEqual(agent.state.messages[-1]["stopReason"], "aborted")
            self.assertTrue(closed.is_set())
            self.assertFalse(agent.state.is_streaming)

    async def test_preaborted_request_never_opens_network(self):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(200)

        signal = AbortSignal()
        signal.abort()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            models = Models(api_keys={"openai": "test-only"}, client=client)
            result = await models.complete_simple(
                models.get_model("openai", "test-reasoner"), AgentContext(), {"signal": signal}
            )
        self.assertFalse(calls)
        self.assertEqual(result["stopReason"], "aborted")

    async def test_completions_fragmented_arguments_and_usage(self):
        events = [
            {
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "c1",
                                    "function": {"name": "add", "arguments": '{"a":'},
                                }
                            ]
                        },
                    }
                ]
            },
            {
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [{"index": 0, "function": {"arguments": '2,"b":3}'}}]
                        },
                        "finish_reason": "tool_calls",
                    }
                ]
            },
            {"choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 2}},
            "[DONE]",
        ]
        requests = []

        def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, content=sse(events))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            models = Models(api_keys={"openai-chat": "test-only"}, client=client)
            result = await models.complete_simple(
                models.get_model("openai-chat", "gpt-4.1"),
                AgentContext([user_message("go")]),
                {"max_tokens": 100},
            )
        self.assertEqual(result["stopReason"], "toolUse")
        self.assertEqual(result["content"][0]["arguments"], {"a": 2, "b": 3})
        self.assertEqual(result["usage"]["totalTokens"], 12)
        self.assertEqual(requests[0]["max_completion_tokens"], 100)

    async def test_anthropic_tool_stream_and_parameter_mapping(self):
        events = [
            {"type": "message_start", "message": {"usage": {"input_tokens": 10}}},
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "tool_use", "id": "toolu_1", "name": "add", "input": {}},
            },
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": '{"a":2,"b":3}'},
            },
            {"type": "content_block_stop", "index": 0},
            {
                "type": "message_delta",
                "delta": {"stop_reason": "tool_use"},
                "usage": {"output_tokens": 5},
            },
            {"type": "message_stop"},
        ]
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(200, content=sse(events))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            models = Models(api_keys={"anthropic": "test-only"}, client=client)
            result = await models.complete_simple(
                models.get_model("anthropic", "claude-sonnet-4-20250514"),
                AgentContext([user_message("go")]),
                {"reasoning": "low", "temperature": 0.2},
            )
        self.assertEqual(result["content"][0]["arguments"], {"a": 2, "b": 3})
        self.assertEqual(result["stopReason"], "toolUse")
        self.assertNotIn("temperature", json.loads(requests[0].content))
        self.assertEqual(requests[0].headers["x-api-key"], "test-only")

    async def test_proxy_reconstructs_deltas_and_flushes_unterminated_last_line(self):
        events = [
            {"type": "start"},
            {"type": "text_start", "contentIndex": 0},
            {"type": "text_delta", "contentIndex": 0, "delta": "hi"},
            {"type": "text_end", "contentIndex": 0},
            {"type": "done", "reason": "stop", "usage": {}},
        ]
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, content=sse(events, final_newline=False))
            )
        ) as client:
            stream = stream_proxy(
                Model("m", "p", "a", ""),
                AgentContext(),
                {"auth_token": "test-only", "proxy_url": "https://example.invalid"},
                client=client,
            )
            result = await asyncio.wait_for(stream.result(), 1)
        self.assertEqual(result["content"][0]["text"], "hi")

    async def test_proxy_eof_is_error(self):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, content=sse([{"type": "start"}]))
            )
        ) as client:
            stream = stream_proxy(
                Model("m", "p", "a", ""),
                AgentContext(),
                {"auth_token": "test-only", "proxy_url": "https://example.invalid"},
                client=client,
            )
            result = await asyncio.wait_for(stream.result(), 1)
        self.assertEqual(result["stopReason"], "error")

    async def test_custom_provider_uses_explicit_credentials_not_openai_environment(self):
        from unittest.mock import patch

        model = Model("internal", "tenant", "openai-completions", "https://example.invalid")
        with patch.dict("os.environ", {"OPENAI_API_KEY": "must-not-leak"}):
            result = await Models().complete_simple(model, AgentContext())
        self.assertEqual(result["stopReason"], "error")
        self.assertIn("No API key", result["errorMessage"])
