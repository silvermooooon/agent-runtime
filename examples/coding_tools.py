"""Run: uv run --env-file .env python examples/coding_tools.py /path/to/workspace '任务'."""

import argparse
import asyncio

from agent_runtime import Agent
from agent_runtime.tools import create_coding_tools


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cwd", help="Working directory for the four local tools")
    parser.add_argument("prompt", help="Task to perform in this directory")
    args = parser.parse_args()

    agent = Agent(
        tools=create_coding_tools(cwd=args.cwd),
        system_prompt="Read files before editing. Verify changes with an appropriate command.",
    )

    def observe(event, signal):
        if event["type"] == "message_update":
            update = event["assistantMessageEvent"]
            if update["type"] == "text_delta":
                print(update["delta"], end="", flush=True)
        elif event["type"] == "tool_execution_start":
            print(f"\n[{event['toolName']}]")

    agent.subscribe(observe)
    await agent.prompt(args.prompt)
    print(f"\nSession: {agent.session.session_id}")
    if agent.state.error_message:
        raise RuntimeError(agent.state.error_message)


if __name__ == "__main__":
    asyncio.run(main())
