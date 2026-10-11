"""Opt-in official Responses search test; never run by unittest discovery."""

import argparse
import asyncio
import json
import os
import tempfile

from live_openai import BASE_URL, MODEL, require, validate_environment

from agent_runtime import Agent, LocalSession, Models
from agent_runtime.tools import create_web_search_tool


async def main():
    validate_environment()
    requests, responses, updates = [], [], []

    def payload(body, model):
        requests.append(body)
        require(len(requests) == 1, "Exceeded the one-request test budget")
        require(body["model"] == MODEL and model.base_url == BASE_URL, "Unexpected model/endpoint")
        require(body["tools"] == [{"type": "web_search"}], "Incorrect native search declaration")

    with tempfile.TemporaryDirectory() as directory:
        agent = Agent(
            models=Models(env={"OPENAI_API_KEY": os.environ["OPENAI_API_KEY"]}),
            provider="openai",
            model=MODEL,
            api="openai-responses",
            base_url=BASE_URL,
            session=LocalSession("search", directory),
            tools=[create_web_search_tool()],
            parameters={
                "reasoning": "low",
                "max_tokens": 2048,
                "timeout": 45,
                "tool_choice": {"type": "web_search"},
                "on_payload": payload,
                "on_response": lambda info, model: responses.append(
                    {"status": info["status"], "request_id": info["headers"].get("x-request-id")}
                ),
            },
        )
        agent.subscribe(
            lambda event, signal: (
                updates.append(event["assistantMessageEvent"]["type"])
                if event["type"] == "message_update"
                else None
            )
        )
        await asyncio.wait_for(
            agent.prompt(
                "Search the official OpenAI website for Responses API web search documentation. "
                "Give one short sentence describing the web_search tool and cite its documentation."
            ),
            timeout=90,
        )
        require(not agent.state.error_message, agent.state.error_message)
        final = agent.state.messages[-1]
        require(final["stopReason"] == "stop", "Response did not finish normally")
        searches = [i for i in final["responseOutput"] if i["type"] == "web_search_call"]
        citations = [
            a
            for b in final["content"]
            for a in b.get("annotations", [])
            if a["type"] == "url_citation"
        ]
        require(bool(searches), "No server-side search was executed")
        require(bool(citations), "No citation returned")
        require("web_search_end" in updates and "text_delta" in updates, "Missing stream events")
        reopened = LocalSession("search", directory)
        require(reopened.snapshot["messages"][-1] == final, "Journal round-trip lost raw output")
        print(
            json.dumps(
                {
                    "model": MODEL,
                    "responses": responses,
                    "searches": len(searches),
                    "citations": len(citations),
                    "journal_restored": True,
                }
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", required=True)
    parser.parse_args()
    asyncio.run(main())
