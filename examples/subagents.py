"""Two background children, parent work, then collection. Offline; no API credentials."""

import asyncio
import json
import tempfile

from agent_runtime import Agent, AssistantMessageEventStream, LocalSession, Model, assistant_message
from agent_runtime.subagents import SubagentManager, create_subagent_tools

MODEL = Model("offline", "local", "fake", "")


def response(content, *, tools=False):
    stream = AssistantMessageEventStream()
    stream.push(
        {
            "type": "done",
            "message": assistant_message(
                MODEL, content=content, stopReason="toolUse" if tools else "stop"
            ),
        }
    )
    return stream


def text(value):
    return {"type": "text", "text": value}


def call(tool_name, call_id, **args):
    return {"type": "toolCall", "id": call_id, "name": tool_name, "arguments": args}


def parent_stream(model, context, options):
    results = [m for m in context.messages if m["role"] == "toolResult"]
    if not results:
        return response(
            [
                call("spawn_agent", "a", name="researcher", task="Research storage"),
                call("spawn_agent", "b", name="researcher", task="Research cancellation"),
            ],
            tools=True,
        )
    if len(results) == 2:
        ids = [json.loads(m["content"][0]["text"])["task_id"] for m in results]
        return response(
            [
                text("While the children work, I have prepared the comparison criteria."),
                *(call("wait_agent", f"wait-{i}", task_id=key) for i, key in enumerate(ids)),
            ],
            tools=True,
        )
    answers = [
        json.loads(m["content"][0]["text"])["result"]["content"][0]["text"]
        for m in results
        if m["toolName"] == "wait_agent"
    ]
    return response([text("Combined results: " + "; ".join(answers))])


async def child_stream(model, context, options):
    await asyncio.sleep(0.05)
    task = next(m for m in context.messages if m["role"] == "user")
    return response([text("Completed: " + task["content"][0]["text"])])


async def main():
    with tempfile.TemporaryDirectory() as directory:
        parent = Agent(
            model=MODEL, session=LocalSession("parent", directory), stream_fn=parent_stream
        )
        manager = SubagentManager(
            parent,
            agent_factories={
                "researcher": lambda session: Agent(
                    model=MODEL, session=session, stream_fn=child_stream
                )
            },
            session_factory=lambda key: LocalSession(key, directory),
        )
        parent.state.tools = create_subagent_tools(manager)
        try:
            await parent.prompt("Compare storage and cancellation designs")
            print(parent.state.messages[-1]["content"][0]["text"])
            print("Child states:", [item["status"] for item in manager.get()])
        finally:
            await manager.close()


if __name__ == "__main__":
    asyncio.run(main())
