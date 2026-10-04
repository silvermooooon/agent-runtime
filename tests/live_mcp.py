"""Opt-in real Responses -> local HTTP MCP -> Responses test, only gpt-6-luna."""

import argparse
import asyncio
import json
import os
import sys
import tempfile

from live_openai import BASE_URL, MODEL, require, validate_environment
from mcp_fixture import HttpFixture

from agent_runtime import Agent, LocalSession, Models
from agent_runtime.mcp import McpCaller, McpHeaderProvider, McpHttpServer, assemble_tools


class Headers(McpHeaderProvider):
    async def get_headers(self, context):
        return {"Authorization": "Bearer local-test-only", "X-Tenant": context.values["tenant"]}


async def run():
    validate_environment()
    fixture = HttpFixture()
    requests, responses = [], []

    def payload(body, model):
        require(body["model"] == MODEL and model.base_url == BASE_URL, "Unexpected model/endpoint")
        requests.append(body)
        require(len(requests) <= 3, "Exceeded request budget")

    def response(info, model):
        responses.append(
            {"status": info["status"], "request_id": info["headers"].get("x-request-id")}
        )

    try:
        with tempfile.TemporaryDirectory(prefix="agent-runtime-mcp-live-") as directory:
            tools = assemble_tools(
                [
                    {
                        "type": "mcp",
                        "name": "demo.echo",
                        "description": "Echo the supplied value",
                        "input_schema": {
                            "type": "object",
                            "properties": {"value": {"type": "string"}},
                            "required": ["value"],
                            "additionalProperties": False,
                        },
                    },
                ],
                mcp_caller=McpCaller(
                    {
                        "demo": McpHttpServer(fixture.url, header_provider=Headers(), timeout=5),
                    }
                ),
                context={"tenant": "test-tenant"},
            )
            session = LocalSession("live-mcp", directory)
            agent = Agent(
                session=session,
                tools=tools,
                models=Models(
                    env={
                        "OPENAI_API_KEY": os.environ["OPENAI_API_KEY"],
                        "OPENAI_BASE_URL": BASE_URL,
                        "AGENT_MODEL": MODEL,
                    }
                ),
                system_prompt=(
                    "Use the echo tool exactly once. After its result, reply with the value only."
                ),
                parameters={
                    "reasoning": "none",
                    "max_tokens": 512,
                    "timeout": 20,
                    "on_payload": payload,
                    "on_response": response,
                },
            )
            async with asyncio.timeout(90):
                await agent.prompt("Use the echo tool with value mcp-runtime-ok.")
            require(not agent.state.error_message, "Agent reported a model or tool error")
            calls = [b for _, _, b in fixture.requests if b and b["method"] == "tools/call"]
            require(
                len(calls) == 1 and len(requests) == 2,
                "Expected model -> MCP -> model exactly once",
            )
            require(
                not any(b and b["method"] == "tools/list" for _, _, b in fixture.requests),
                "Unexpected discovery",
            )
            for _, headers, _ in fixture.requests:
                lower = {k.lower(): v for k, v in headers.items()}
                require(
                    lower.get("authorization") == "Bearer local-test-only",
                    "Missing MCP authorization",
                )
                require(lower.get("x-tenant") == "test-tenant", "Missing tenant header")
            records = session.read_records()
            returned = next(r for r in records if r["type"] == "tool_returned")
            raw = returned["data"]["result"]["details"]["mcp"]
            require(raw["structuredContent"] == {"value": "mcp-runtime-ok"}, "MCP result not saved")
            require(
                "local-test-only" not in session.path.read_text(), "MCP headers leaked to journal"
            )
            assistant = next(m for m in agent.state.messages if m["role"] == "assistant")
            call = next(b for b in assistant["content"] if b["type"] == "toolCall")
            require(call["name"] == "demo.echo", "Logical name was not restored")
            require(requests[0]["tools"][0]["name"] == "demo__echo", "Wire name was not encoded")
            final = agent.state.messages[-1]
            text = "".join(b.get("text", "") for b in final["content"])
            require(
                text.strip() == "mcp-runtime-ok" and not session.resumable, "Run did not complete"
            )
            print(
                json.dumps(
                    {
                        "result": "passed",
                        "model": MODEL,
                        "requests": len(requests),
                        "responses": responses,
                        "mcp_calls": len(calls),
                        "session_status": session.snapshot["status"],
                    }
                )
            )
    finally:
        await asyncio.to_thread(fixture.close)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="Allow billed OpenAI requests")
    if not parser.parse_args().live:
        parser.error("Pass --live to enable real API calls")
    try:
        asyncio.run(run())
    except Exception as error:
        # Never print an arbitrary exception or request body containing credentials.
        print(json.dumps({"result": "failed", "error_type": type(error).__name__}), file=sys.stderr)
        sys.exit(1)
