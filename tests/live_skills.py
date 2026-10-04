"""Opt-in real Responses -> load_skill -> load_skill_reference -> final answer test."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

from live_openai import BASE_URL, MODEL, require, validate_environment

from agent_runtime import Agent, LocalSession, Models
from agent_runtime.skills import LocalSkillStore, create_skill_tools


async def run():
    validate_environment()
    requests, responses, calls = [], [], []

    def payload(body, model):
        require(body["model"] == MODEL and model.base_url == BASE_URL, "Unexpected model/endpoint")
        requests.append(body)
        require(len(requests) <= 3, "Exceeded request budget")

    def response(info, model):
        responses.append(
            {"status": info["status"], "request_id": info["headers"].get("x-request-id")}
        )

    with tempfile.TemporaryDirectory(prefix="agent-runtime-skill-live-") as directory:
        root = Path(directory) / "skills"
        reference_dir = root / "verify" / "references"
        reference_dir.mkdir(parents=True)
        (root / "verify" / "SKILL.md").write_text(
            "---\nname: verify\ndescription: Return a verification phrase from a reference.\n---\n"
            "Load references/result.txt with load_skill_reference, name verify. "
            "Then reply with exactly the reference content. Do not load this skill again.\n"
        )
        (reference_dir / "result.txt").write_text("skill-runtime-ok", encoding="utf-8")
        store = LocalSkillStore(env={"AGENT_SKILLS_DIR": str(root)})
        catalog = await store.discover()
        session = LocalSession("skills-live", Path(directory) / "sessions")
        agent = Agent(
            models=Models(
                env={
                    "OPENAI_API_KEY": os.environ["OPENAI_API_KEY"],
                    "OPENAI_BASE_URL": BASE_URL,
                    "AGENT_MODEL": MODEL,
                }
            ),
            session=session,
            tools=create_skill_tools(store=store),
            system_prompt=(
                "Load the verify skill exactly once, then follow its instructions. "
                "Available skills: " + catalog[0].name + ": " + catalog[0].description
            ),
            parameters={
                "reasoning": "none",
                "max_tokens": 512,
                "timeout": 20,
                "on_payload": payload,
                "on_response": response,
            },
        )

        def observe(event, signal):
            if event["type"] == "tool_execution_start":
                calls.append(event["toolName"])

        agent.subscribe(observe)
        async with asyncio.timeout(90):
            await agent.prompt("Return the verification phrase using the verify skill.")
        require(not agent.state.error_message, "Agent reported an error")
        require(calls == ["load_skill", "load_skill_reference"], "Unexpected tool sequence")
        require(len(requests) == 3, "Expected model -> skill -> model -> reference -> model")
        final = "".join(b.get("text", "") for b in agent.state.messages[-1]["content"])
        require(final.strip() == "skill-runtime-ok", "Reference instructions were not followed")
        require(not session.resumable, "Session is incomplete")
        saved = [r for r in session.read_records() if r["type"] == "tool_returned"]
        require(len(saved) == 2, "Both tool results must be durable")
        require(
            saved[1]["data"]["result"]["content"][0]["text"] == "skill-runtime-ok",
            "Reference not saved",
        )
        print(
            json.dumps(
                {
                    "result": "passed",
                    "model": MODEL,
                    "requests": len(requests),
                    "responses": responses,
                    "tools": calls,
                    "session_status": session.snapshot["status"],
                }
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="Allow billed OpenAI requests")
    if not parser.parse_args().live:
        parser.error("Pass --live to enable real API calls")
    try:
        asyncio.run(run())
    except Exception as error:
        print(json.dumps({"result": "failed", "error_type": type(error).__name__}), file=sys.stderr)
        sys.exit(1)
