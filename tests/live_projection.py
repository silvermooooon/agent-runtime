"""Opt-in compaction/fork test.

Run: uv run --env-file .env python tests/live_projection.py --live.
"""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

from live_openai import BASE_URL, MODEL, require, validate_environment

from agent_runtime import Agent, CompactionSettings, LocalSession, Models


async def run(directory):
    validate_environment()
    responses, requests = [], []

    def payload(body, model):
        require(body["model"] == MODEL and model.base_url == BASE_URL, "Unexpected model/endpoint")
        requests.append(True)
        require(len(requests) <= 3, "Exceeded live test request budget")

    def response(info, model):
        responses.append(
            {"status": info["status"], "request_id": info["headers"].get("x-request-id")}
        )

    models = Models(
        env={"OPENAI_API_KEY": os.environ["OPENAI_API_KEY"], "OPENAI_BASE_URL": BASE_URL}
    )
    options = {
        "reasoning": "none",
        "max_tokens": 512,
        "timeout": 20,
        "on_payload": payload,
        "on_response": response,
    }
    session = LocalSession("parent", directory)
    agent = Agent(
        session=session,
        models=models,
        model=MODEL,
        parameters=options,
        compaction=CompactionSettings(enabled=False, reserve_tokens=1280, keep_recent_tokens=0),
    )
    await agent.prompt("Remember this project code exactly: ORBIT-482. Reply with exactly: noted")
    require(not agent.state.error_message, agent.state.error_message or "Initial request failed")
    original = session.path.read_bytes()
    event = await agent.compact("Preserve the exact project code for future questions.")
    require("ORBIT-482" in event["data"]["summary"], "Summary lost the project code")
    require(session.path.read_bytes().startswith(original), "Compaction rewrote the original log")
    before = session.build_context()
    child = await session.fork(event["id"], session_id="child")
    require(child.build_context() == before, "Fork projection mismatch")
    session.path.unlink()  # Recovery must work from the child's own complete journal.
    reopened = LocalSession("child", directory)
    require(reopened.build_context() == before, "Reopened child lost compaction state")
    followup = Agent(session=reopened, models=models, parameters=options)
    await followup.prompt("What is the project code? Reply with the code only.")
    require(not followup.state.error_message, followup.state.error_message or "Follow-up failed")
    final = followup.state.messages[-1]
    text = "".join(block.get("text", "") for block in final["content"]).strip()
    require(text == "ORBIT-482", "Compacted context did not preserve the answer")
    require(not reopened.resumable, "Child run incomplete")
    require(
        len(requests) == 3 and all(r["status"] == 200 for r in responses),
        "HTTP verification failed",
    )
    return {
        "result": "passed",
        "model": MODEL,
        "requests": len(requests),
        "responses": responses,
        "compaction_event_id": event["id"],
        "fork_session": child.session_id,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    if not args.live:
        parser.error("Pass --live to enable billed OpenAI requests")
    with tempfile.TemporaryDirectory(prefix="agent-runtime-projection-live-") as directory:
        print(json.dumps(asyncio.run(asyncio.wait_for(run(Path(directory)), 120))))


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
