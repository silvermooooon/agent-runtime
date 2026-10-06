"""Ordinary tool adapters; no special handling in the agent loop."""

import json

from ..types import AgentTool, AgentToolResult


def create_subagent_tools(manager):
    """Explicitly install these tools on manager.parent, not on its children."""
    text = {"type": "string"}
    definitions = [
        (
            "spawn_agent",
            "Start a background agent with explicit task materials. Configurations: "
            + ", ".join(manager.agent_factories),
            {"name": {"type": "string", "enum": list(manager.agent_factories)}, "task": text},
            ["name", "task"],
            "safe",
        ),
        (
            "get_agent",
            "Read child status and completed result; omit task_id to list children.",
            {"task_id": text},
            [],
            "safe",
        ),
        (
            "send_agent_message",
            "Queue input to a child; delivery becomes durable at a safe boundary.",
            {"task_id": text, "message": text, "follow_up": {"type": "boolean"}},
            ["task_id", "message"],
            "never",
        ),
        (
            "wait_agent",
            "Wait for a child result. Timeout does not stop the child.",
            {"task_id": text, "timeout": {"type": "number", "minimum": 0}},
            ["task_id"],
            "safe",
        ),
        (
            "stop_agent",
            "Stop a child's current execution. Never automatically replay this action.",
            {"task_id": text},
            ["task_id"],
            "never",
        ),
    ]

    def make_execute(name):
        async def execute(call_id, args, signal, on_update):
            signal.throw_if_aborted()
            if name == "spawn_agent":
                result = await manager.spawn(
                    **args, operation_id=manager.parent.session.tool_operation_id(call_id)
                )
            elif name == "get_agent":
                result = manager.get(**args)
            elif name == "send_agent_message":
                result = await manager.send(**args)
            elif name == "wait_agent":
                result = await signal.run(manager.wait(**args))
            else:
                result = await manager.stop(**args)
            return AgentToolResult(
                content=[{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
                details=result,
            )

        return execute

    return [
        AgentTool(
            name=name,
            description=description,
            parameters={
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": False,
            },
            execute=make_execute(name),
            replay=replay,
        )
        for name, description, properties, required, replay in definitions
    ]
