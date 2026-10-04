"""Opt-in real Responses tests. Run: uv run --env-file .env python tests/live_openai.py --live.

Never discovered by unittest's test*.py pattern; no mocks or fallback models are used here.
Each stage runs in a fresh process. Credentials are inherited through the environment only.
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from agent_runtime import Agent, AgentTool, AgentToolResult, LocalSession, Models

MODEL = "gpt-6-luna"
BASE_URL = "https://api.openai.com/v1"
STAGES = ("text", "tools", "plan-start", "plan-resume", "stream-start", "stream-resume")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def validate_environment():
    require(bool(os.environ.get("OPENAI_API_KEY", "").strip()), "OPENAI_API_KEY is missing")
    require(
        os.environ.get("OPENAI_BASE_URL", BASE_URL).strip().rstrip("/") in ("", BASE_URL),
        "Live tests require the official OpenAI endpoint",
    )
    require(
        os.environ.get("AGENT_MODEL", MODEL).strip() in ("", MODEL),
        "Live tests only allow gpt-6-luna",
    )


async def run_stage(stage, directory):
    validate_environment()
    kind = stage.split("-", 1)[0]
    session = LocalSession(kind, directory)
    executions, deltas, responses, requests = [], [], [], []

    async def add(call_id, args, signal, on_update):
        signal.throw_if_aborted()
        executions.append(call_id)
        return AgentToolResult([{"type": "text", "text": str(args["a"] + args["b"])}])

    tool = AgentTool(
        "add",
        "Add two integers",
        {
            "type": "object",
            "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
            "required": ["a", "b"],
            "additionalProperties": False,
        },
        add,
        replay="safe",
        version="1",
    )

    def payload(body, model):
        require(body["model"] == MODEL, "Unexpected outbound model")
        require(model.base_url == BASE_URL, "Unexpected outbound endpoint")
        requests.append(body)
        require(len(requests) <= 3, "Exceeded the per-stage request budget")

    def response(info, model):
        responses.append(
            {"status": info["status"], "request_id": info["headers"].get("x-request-id")}
        )

    tools = [tool] if kind in ("tools", "plan") else []
    models = Models(
        env={
            "OPENAI_API_KEY": os.environ["OPENAI_API_KEY"],
            "OPENAI_BASE_URL": BASE_URL,
            "AGENT_MODEL": MODEL,
        }
    )
    agent = Agent(
        models=models,
        session=session,
        tools=tools,
        system_prompt=(
            "Use add exactly once when asked to add. After receiving its result, "
            "reply with the number only and never call add again."
            if tools
            else "Follow the requested output format."
        ),
        parameters={
            "reasoning": "none",
            "max_tokens": 512,
            "timeout": 20,
            "on_payload": payload,
            "on_response": response,
        },
    )
    require(agent.state.model.id == MODEL, "Unexpected runtime model")
    saved_calls = [
        b["id"]
        for b in (session.snapshot.get("assistant") or {}).get("content", [])
        if b["type"] == "toolCall"
    ]

    def observe(event, signal):
        if event["type"] == "message_update":
            update = event["assistantMessageEvent"]
            if update["type"] == "text_delta":
                deltas.append(update["delta"])
                if stage == "stream-start":
                    agent.abort()
        if (
            stage == "plan-start"
            and event["type"] == "message_end"
            and event["message"]["role"] == "assistant"
            and any(b["type"] == "toolCall" for b in event["message"]["content"])
        ):
            agent.abort()

    agent.subscribe(observe)
    if stage.endswith("resume"):
        require(session.resumable, "Missing interrupted session")
        await agent.resume()
    else:
        prompt = (
            "Use add to calculate 17 + 25."
            if tools
            else "Output the integers from 1 through 100, separated by commas, without explanation."
            if kind == "stream"
            else "Reply with exactly: runtime-ok"
        )
        await agent.prompt(prompt)

    if agent.state.error_message and not stage.endswith("start"):
        raise RuntimeError(agent.state.error_message)
    require(responses and all(r["status"] == 200 for r in responses), "No successful API response")
    final = agent.state.messages[-1]
    text = "".join(b.get("text", "") for b in final.get("content", []))
    if stage == "text":
        require(deltas and text.strip() == "runtime-ok", "Text stream verification failed")
    elif stage == "tools":
        require(len(executions) == 1 and text.strip() == "42", "Tool loop verification failed")
        require(len(requests) == 2, "Expected model -> tool -> model")
    elif stage == "plan-start":
        require(session.resumable and not executions, "Tool plan was not saved before execution")
        require(session.snapshot["phase"] == "tools", "Unexpected recovery boundary")
    elif stage == "plan-resume":
        require(saved_calls == executions, "Recovered tool call identity changed")
        require(len(requests) == 1 and text.strip() == "42", "Saved tool plan was not reused")
    elif stage == "stream-start":
        require(deltas and session.resumable, "Stream did not stop at an unfinished request")
        require(session.snapshot["phase"] == "model", "Unexpected model recovery boundary")
    else:
        expected = ",".join(str(i) for i in range(1, 101))
        require(
            deltas and "".join(text.split()) == expected,
            "Interrupted model request did not resume with the full output",
        )
    if not stage.endswith("start"):
        require(not session.resumable, "Run is still incomplete")
        require(final.get("stopReason") == "stop", "Model response did not complete normally")
    records = session.read_records()
    require(
        not any(r["type"] in ("text_delta", "message_update") for r in records),
        "Ephemeral deltas were stored as durable events",
    )
    return {
        "stage": stage,
        "result": "passed",
        "model": MODEL,
        "requests": len(requests),
        "responses": responses,
        "text_delta_count": len(deltas),
        "tool_execution_count": len(executions),
        "session_status": session.snapshot["status"],
        "usage": final.get("usage"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="Allow billed requests to OpenAI")
    parser.add_argument("--stage", choices=STAGES, help=argparse.SUPPRESS)
    parser.add_argument("--directory", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.live:
        parser.error("Pass --live to explicitly enable real API calls")
    validate_environment()
    if args.stage:
        require(bool(args.directory), "Stage needs a session directory")
        print(json.dumps(asyncio.run(run_stage(args.stage, args.directory))))
        return
    with tempfile.TemporaryDirectory(prefix="agent-runtime-live-") as directory:
        for stage in STAGES:
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--live",
                    "--stage",
                    stage,
                    "--directory",
                    directory,
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
            print(result.stdout.strip(), flush=True)
            require(result.returncode == 0, f"Stage {stage} failed: {result.stderr.strip()}")
    print(json.dumps({"result": "passed", "model": MODEL, "stages": len(STAGES)}))


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
