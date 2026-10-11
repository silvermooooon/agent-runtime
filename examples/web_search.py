"""Run: uv run --env-file .env python examples/web_search.py"""

import asyncio

from agent_runtime import Agent
from agent_runtime.tools import create_web_search_tool


async def main():
    agent = Agent(tools=[create_web_search_tool()])

    def observe(event, signal):
        if event["type"] == "message_update":
            update = event["assistantMessageEvent"]
            if update["type"] == "text_delta":
                print(update["delta"], end="", flush=True)

    agent.subscribe(observe)
    await agent.prompt("搜索 OpenAI 官方 Responses API 的网页搜索说明，简要回答并引用来源。")
    print()
    if agent.state.error_message:
        raise RuntimeError(agent.state.error_message)
    for block in agent.state.messages[-1]["content"]:
        for citation in block.get("annotations", []):
            if citation["type"] == "url_citation":
                print(f"{citation['title']}: {citation['url']}")


if __name__ == "__main__":
    asyncio.run(main())
