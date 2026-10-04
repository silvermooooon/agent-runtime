"""Run a full model → tool → model loop without network access or credentials."""

import asyncio

from agent_runtime import (
    Agent,
    AgentTool,
    AgentToolResult,
    AssistantMessageEventStream,
    Model,
    assistant_message,
)


async def add(call_id, args, signal, on_update):
    signal.throw_if_aborted()
    return AgentToolResult([{"type": "text", "text": str(args["a"] + args["b"])}])


def fake_stream(model, context, options):
    stream = AssistantMessageEventStream()
    if context.messages[-1]["role"] != "toolResult":
        message = assistant_message(
            model,
            stopReason="toolUse",
            content=[
                {"type": "toolCall", "id": "call-1", "name": "add", "arguments": {"a": 2, "b": 3}}
            ],
        )
    else:
        result = context.messages[-1]["content"][0]["text"]
        message = assistant_message(
            model, stopReason="stop", content=[{"type": "text", "text": f"2 + 3 = {result}"}]
        )
    stream.push({"type": "start", "partial": assistant_message(model)})
    stream.push({"type": "done", "message": message})
    return stream


async def main():
    tool = AgentTool(
        "add",
        "Add two numbers",
        {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
            "additionalProperties": False,
        },
        add,
    )
    agent = Agent(model=Model("offline", "local", "fake", ""), stream_fn=fake_stream, tools=[tool])
    agent.subscribe(lambda event, signal: print(event["type"]))
    await agent.prompt("What is 2 + 3?")
    print(agent.state.messages[-1]["content"][0]["text"])


if __name__ == "__main__":
    asyncio.run(main())
