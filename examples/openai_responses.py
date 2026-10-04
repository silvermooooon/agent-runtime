"""Run with: uv run --env-file .env python examples/openai_responses.py"""

import asyncio

from agent_runtime import Agent


async def main():
    def observe(event, signal):
        if event["type"] == "message_update":
            update = event["assistantMessageEvent"]
            if update["type"] == "text_delta":
                print(update["delta"], end="", flush=True)

    agent = Agent(system_prompt="You are a concise assistant.")
    agent.subscribe(observe)
    await agent.prompt("Explain an agent loop in two sentences.")
    print()
    if agent.state.error_message:
        raise RuntimeError(agent.state.error_message)


if __name__ == "__main__":
    asyncio.run(main())
