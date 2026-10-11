"""Offline Agent + real shell + encrypted demo credential; no model requests.

Run: uv run --extra secrets python examples/execution.py
The key and fake credential below exist only in a disposable demonstration directory.
Production hosts must supply a stable master key and accept credentials outside the conversation.
"""

import asyncio
import tempfile
from pathlib import Path

from cryptography.fernet import Fernet

from agent_runtime import Agent, AssistantMessageEventStream, LocalSession, Model, assistant_message
from agent_runtime.execution import LocalExecutionBackend, create_execution_tools
from agent_runtime.secrets import LocalSecretStore


def offline_stream(model, context, options):
    results = [message for message in context.messages if message["role"] == "toolResult"]
    if results:
        content = [{"type": "text", "text": "The command used the configured credential."}]
    else:
        content = [
            {
                "type": "toolCall",
                "id": "run-1",
                "name": "bash",
                "arguments": {
                    "command": 'test -n "$PROJECT_API_TOKEN" && printf "credential available\\n"',
                    "credentials": ["project-api"],
                    "timeout": 10,
                },
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


async def main():
    with tempfile.TemporaryDirectory(prefix="execution-example-") as directory:
        root = Path(directory)
        session = LocalSession("demo", root / "sessions", tenant_id="demo", user_id="alice")
        store = LocalSecretStore(session.info, root / "secrets", key=Fernet.generate_key())
        await store.put(
            "project-api", {"PROJECT_API_TOKEN": "fake-demo-value"}, description="Project API"
        )
        backend = LocalExecutionBackend(session.info, root / "workspaces")
        tools = await create_execution_tools(
            session=session, backend=backend, secrets=store, credential_refs=["project-api"]
        )
        agent = Agent(
            session=session,
            tools=tools,
            model=Model("gpt-6-luna", "openai", "openai-responses", ""),
            stream_fn=offline_stream,
        )
        await agent.prompt("Check whether the configured project credential is available.")
        if agent.state.error_message:
            raise RuntimeError(agent.state.error_message)
        result = next(
            message for message in agent.state.messages if message["role"] == "toolResult"
        )
        if result.get("isError"):
            raise RuntimeError(result["content"])
        print(result["content"][0]["text"].strip())
        print("Audit metadata:", result["details"]["execution"])
        assert "fake-demo-value" not in session.path.read_text()
        print("Session records the reference and variable name, without the credential value.")


if __name__ == "__main__":
    asyncio.run(main())
