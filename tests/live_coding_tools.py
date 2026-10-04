"""Opt-in live coding tools test; run with uv run --env-file .env ... --live."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

from live_openai import BASE_URL, MODEL, require, validate_environment

from agent_runtime import Agent, LocalSession, Models
from agent_runtime.tools import create_coding_tools


async def run(directory):
    validate_environment()
    workspace = Path(directory) / "workspace"
    workspace.mkdir()
    responses, requests, completed = [], [], []
    order = ["write", "read", "edit", "bash"]

    def payload(body, model):
        require(body["model"] == MODEL and model.base_url == BASE_URL, "Unexpected model/endpoint")
        requests.append(True)
        require(len(requests) <= 6, "Exceeded live test request budget")

    def response(info, model):
        responses.append(
            {"status": info["status"], "request_id": info["headers"].get("x-request-id")}
        )

    def approve(context, signal):
        name, args = context["toolCall"]["name"], context["args"]
        allowed = len(completed) < len(order) and name == order[len(completed)]
        if name == "bash":
            allowed = allowed and args == {"command": "wc -c < hello.txt", "timeout": 5}
        else:
            allowed = allowed and args.get("path") == "hello.txt"
            if name == "write":
                allowed = allowed and args.get("content") == "hello pi\n"
            elif name == "edit":
                allowed = allowed and args.get("edits") == [{"oldText": "pi", "newText": "runtime"}]
        if not allowed:
            return {"block": True, "terminate": True, "reason": "Outside fixed live test plan"}

    def observe(event, signal):
        if event["type"] == "tool_execution_end":
            require(not event["isError"], f"Tool failed: {event['toolName']}")
            completed.append(event["toolName"])
            if event["toolName"] == "read":
                require(event["result"]["content"][0]["text"] == "hello pi\n", "Read mismatch")
            if event["toolName"] == "bash":
                structured = event["result"]["structuredContent"]
                require(structured["exit_code"] == 0, "Bash failed")
                require(structured["output"].strip() == "14", "Bash output mismatch")

    models = Models(
        env={"OPENAI_API_KEY": os.environ["OPENAI_API_KEY"], "OPENAI_BASE_URL": BASE_URL}
    )
    session = LocalSession("coding", Path(directory) / "sessions")
    agent = Agent(
        models=models,
        model=MODEL,
        session=session,
        tools=create_coding_tools(
            workspace,
            bash_options={"spawn_hook": lambda ctx: {**ctx, "env": {"PATH": "/usr/bin:/bin"}}},
        ),
        before_tool_call=approve,
        system_prompt="Follow the fixed tool plan exactly. Call one tool per turn, in order.",
        parameters={
            "reasoning": "none",
            "max_tokens": 1024,
            "timeout": 20,
            "on_payload": payload,
            "on_response": response,
        },
    )
    agent.subscribe(observe)
    await asyncio.wait_for(
        agent.prompt(
            '1. Use write with path="hello.txt", content="hello pi\\n" (newline at end). '
            '2. Use read with path="hello.txt". '
            '3. Use edit with path="hello.txt", edits=[{"oldText":"pi","newText":"runtime"}]. '
            '4. Use bash with command="wc -c < hello.txt", timeout=5. '
            '5. Reply exactly "coding-tools-ok". Do not call any other commands or tools.'
        ),
        timeout=120,
    )
    require(not agent.state.error_message, agent.state.error_message or "Agent failed")
    require(completed == order, "Not all four tools executed in order")
    require((workspace / "hello.txt").read_bytes() == b"hello runtime\n", "Final file mismatch")
    final = agent.state.messages[-1]
    text = "".join(b.get("text", "") for b in final["content"])
    require(text.strip() == "coding-tools-ok" and final["stopReason"] == "stop", "Reply mismatch")
    require(session.snapshot["status"] == "completed", "Session incomplete")
    require(responses and all(r["status"] == 200 for r in responses), "HTTP response failed")
    return {"result": "passed", "model": MODEL, "tools": completed, "responses": responses}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    if not args.live:
        parser.error("Pass --live to enable billed OpenAI requests")
    with tempfile.TemporaryDirectory(prefix="agent-runtime-coding-live-") as directory:
        print(json.dumps(asyncio.run(run(directory))))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        message = str(error)
        key = os.environ.get("OPENAI_API_KEY", "")
        if key:
            message = message.replace(key, "[REDACTED]")
        print(json.dumps({"result": "failed", "error": message}), file=sys.stderr)
        sys.exit(1)
