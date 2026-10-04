"""Offline: uv run python examples/mcp_tools.py

Real model: uv run --env-file .env python examples/mcp_tools.py --agent
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from agent_runtime import Agent
from agent_runtime.mcp import McpCaller, McpStdioServer, assemble_tools


def serve():
    from mcp.server import MCPServer

    server = MCPServer("calculator")

    @server.tool()
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    server.run(transport="stdio")


async def main(use_agent):
    caller = McpCaller(
        {
            "calculator": McpStdioServer(
                command=sys.executable,
                args=(str(Path(__file__).resolve()), "--server"),
            )
        }
    )
    # These definitions come from the platform's independent discovery/selection step.
    tools = assemble_tools(
        [
            {
                "type": "mcp",
                "name": "calculator.add",
                "description": "Add two integers",
                "input_schema": {
                    "type": "object",
                    "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                    "required": ["a", "b"],
                    "additionalProperties": False,
                },
            }
        ],
        mcp_caller=caller,
    )
    if not use_agent:
        # Real MCP subprocess + protocol, no model request or API key needed.
        result = await caller.call("calculator.add", {"a": 17, "b": 25}, call_id="demo")
        print(json.dumps(result, ensure_ascii=False))
        return

    agent = Agent(
        tools=tools,
        model="gpt-6-luna",
        system_prompt="Use calculator.add once, then reply with only its result.",
        parameters={"max_tokens": 512, "timeout": 20},
    )
    await agent.prompt("Calculate 17 + 25 using the tool.")
    if agent.state.error_message:
        raise RuntimeError(agent.state.error_message)
    print("".join(b.get("text", "") for b in agent.state.messages[-1]["content"]))
    print(f"Session: {agent.session.session_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent", action="store_true", help="Call a real model (billed)")
    parser.add_argument("--server", action="store_true", help=argparse.SUPPRESS)
    options = parser.parse_args()
    if options.server:
        serve()
    else:
        asyncio.run(main(options.agent))
