"""Run start and resume in two separate processes; no network or API key needed."""

import argparse
import asyncio

from offline import add, fake_stream

from agent_runtime import Agent, AgentTool, LocalSession, Model


def tool():
    return AgentTool(
        "add",
        "Add two numbers",
        {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
            "additionalProperties": False,
        },
        add,
        replay="safe",
        version="1",
    )


async def main(args):
    session = LocalSession(args.session_id, args.directory)
    agent = Agent(
        session=session,
        model=Model("offline", "local", "fake", ""),
        tools=[tool()],
        stream_fn=fake_stream,
    )
    if args.action == "start":

        def stop_before_tools(event, signal):
            if (
                event["type"] == "message_end"
                and event["message"]["role"] == "assistant"
                and any(b["type"] == "toolCall" for b in event["message"]["content"])
            ):
                agent.abort()

        agent.subscribe(stop_before_tools)
        await agent.prompt("What is 2 + 3?")
    else:
        await agent.resume()
    print(f"session={session.session_id} status={session.snapshot['status']}")
    print(f"journal={session.path}")
    if session.resumable:
        print("The tool plan is saved; run the resume command in a new process.")
    else:
        print(agent.state.messages[-1]["content"][0]["text"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "resume"))
    parser.add_argument("--session-id", default="demo")
    parser.add_argument("--directory", default=".agent-runtime/sessions")
    asyncio.run(main(parser.parse_args()))
