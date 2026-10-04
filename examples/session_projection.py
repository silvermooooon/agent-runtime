"""One journal, derived context and an independent fork; no network or credentials."""

import asyncio
import tempfile

from agent_runtime import (
    Agent,
    AssistantMessageEventStream,
    LocalSession,
    Model,
    assistant_message,
)


def fake_stream(model, context, options):
    stream = AssistantMessageEventStream()
    stream.push(
        {
            "type": "done",
            "message": assistant_message(
                model, stopReason="stop", content=[{"type": "text", "text": "Recorded."}]
            ),
        }
    )
    return stream


async def main():
    with tempfile.TemporaryDirectory() as directory:
        session = LocalSession("original", directory)
        agent = Agent(
            session=session,
            model=Model("offline", "local", "fake", ""),
            stream_fn=fake_stream,
        )
        await agent.prompt("The project uses Python.")
        first_turn = session.read_records()[-1]["id"]
        await agent.prompt("Next, implement persistence.")

        # Supply a summary locally; agent.compact() can also generate one with the model.
        keep_from = session.context_entries()[-2]["message_id"]
        compacted = await agent.compact(
            summary="The project uses Python.", first_kept_message_id=keep_from
        )
        reopened = LocalSession("original", directory)
        assert reopened.build_context() == agent.state.messages
        print("Compaction event:", compacted["id"])
        print("Complete journal records:", len(reopened.read_records()))
        print("Current context messages:", len(reopened.build_context()))

        # Choose a node before compaction. The child contains its complete history prefix.
        child = await reopened.fork(first_turn, session_id="alternative")
        alternative = Agent(session=child, stream_fn=fake_stream)
        await alternative.prompt("Instead, start with tools.")
        print("Independent session:", child.session_id)
        print("Original status:", reopened.snapshot["status"])
        print("Child status:", child.snapshot["status"])


if __name__ == "__main__":
    asyncio.run(main())
