"""Offline: AGENT_SKILLS_DIR=examples/skill_catalog uv run python examples/skills.py

Real model: set AGENT_SKILLS_DIR in .env, then run
uv run --env-file .env python examples/skills.py --agent --prompt 'your task'
"""

import argparse
import asyncio
import json
from dataclasses import asdict

from agent_runtime import Agent, AssistantMessageEventStream, LocalSession, Model, assistant_message
from agent_runtime.skills import LocalSkillStore, create_skill_tools


def offline_stream(skill_name):
    def stream(model, context, options):
        results = [m for m in context.messages if m["role"] == "toolResult"]
        if not results:
            name, arguments = "load_skill", {"name": skill_name}
        elif len(results) == 1 and results[0]["details"]["skill"]["references"]:
            name = "load_skill_reference"
            arguments = {
                "name": skill_name,
                "reference": results[0]["details"]["skill"]["references"][0],
            }
        else:
            output = AssistantMessageEventStream()
            output.push(
                {
                    "type": "done",
                    "message": assistant_message(
                        model,
                        stopReason="stop",
                        content=[
                            {
                                "type": "text",
                                "text": "Skill content loaded through dedicated tools.",
                            }
                        ],
                    ),
                }
            )
            return output
        message = assistant_message(
            model,
            stopReason="toolUse",
            content=[
                {
                    "type": "toolCall",
                    "id": f"load-{len(results)}",
                    "name": name,
                    "arguments": arguments,
                }
            ],
        )
        output = AssistantMessageEventStream()
        output.push({"type": "done", "message": message})
        return output

    return stream


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", help="Override AGENT_SKILLS_DIR")
    parser.add_argument("--agent", action="store_true", help="Call the real model (billed)")
    parser.add_argument(
        "--prompt", default="Draft an update: the SDK tests passed; next, review the code."
    )
    args = parser.parse_args()
    store = LocalSkillStore(args.directory)
    catalog = await store.discover()
    if not catalog:
        raise RuntimeError("No skills found in the configured directory")
    print("Available skills:", ", ".join(info.name for info in catalog))
    options = (
        {}
        if args.agent
        else {
            "model": Model("offline", "local", "offline", ""),
            "stream_fn": offline_stream(catalog[0].name),
        }
    )
    agent = Agent(
        tools=create_skill_tools(store=store),
        session=LocalSession(),
        system_prompt=(
            "Load an appropriate skill before answering. Load its references when needed. "
            "Available skills: "
            + json.dumps([asdict(info) for info in catalog], ensure_ascii=False)
        ),
        **options,
    )
    agent.subscribe(
        lambda event, _: (
            print(event["toolName"]) if event["type"] == "tool_execution_start" else None
        )
    )
    await agent.prompt(args.prompt)
    if agent.state.error_message:
        raise RuntimeError(agent.state.error_message)
    print("".join(b.get("text", "") for b in agent.state.messages[-1]["content"]))
    print(f"Session: {agent.session.session_id}")


if __name__ == "__main__":
    asyncio.run(main())
