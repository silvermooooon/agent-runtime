"""Offline user memory across two sessions; no API key, Git or model request required.

Run: uv run python examples/memory.py
For real calls, construct Agent with the same session/tools and omit stream_fn and model below.
"""

import asyncio
import json
import tempfile
from pathlib import Path

from agent_runtime import Agent, AssistantMessageEventStream, LocalSession, Model, assistant_message
from agent_runtime.memory import LocalMemoryStore, create_memory_tools


def offline_stream(operation, arguments):
    def stream(model, context, options):
        results = [m for m in context.messages if m["role"] == "toolResult"]
        if results:
            content = [{"type": "text", "text": "Memory operation completed."}]
        else:
            content = [
                {
                    "type": "toolCall",
                    "id": "memory-1",
                    "name": operation,
                    "arguments": arguments,
                }
            ]
        output = AssistantMessageEventStream()
        output.push(
            {
                "type": "done",
                "message": assistant_message(
                    model, content=content, stopReason="stop" if results else "toolUse"
                ),
            }
        )
        return output

    return stream


async def main():
    with tempfile.TemporaryDirectory(prefix="memory-example-") as directory:
        root = Path(directory)
        first = LocalSession("first", root / "sessions", tenant_id="demo", user_id="alice")
        store = LocalMemoryStore(first.info, root / "memory")
        # This model is a label only: the injected stream never calls any provider.
        model = Model("gpt-6-luna", "openai", "openai-responses", "")
        writer = Agent(
            session=first,
            model=model,
            tools=create_memory_tools(session=first, store=store),
            stream_fn=offline_stream(
                "memory_write",
                {
                    "key": "MEMORY",
                    "content": "Alice prefers concise answers.",
                    "expected_version": None,
                },
            ),
        )
        await writer.prompt("Remember that I prefer concise answers.")

        second = LocalSession("second", root / "sessions", tenant_id="demo", user_id="alice")
        reader = Agent(
            session=second,
            model=model,
            tools=create_memory_tools(session=second, store=store, read_only=True),
            stream_fn=offline_stream("memory_read", {"key": "MEMORY"}),
        )
        await reader.prompt("What preferences do you remember?")
        for agent in (writer, reader):
            if agent.state.error_message:
                raise RuntimeError(agent.state.error_message)
        result = next(m for m in reader.state.messages if m["role"] == "toolResult")
        if result.get("isError"):
            raise RuntimeError(result["content"])
        entry = result["details"]["memory"]["entry"]
        print(json.dumps(entry, indent=2, ensure_ascii=False))
        assert entry["sources"][0]["session_id"] == first.session_id
        print("Second session read the first session's memory; both calls are journaled.")


if __name__ == "__main__":
    asyncio.run(main())
