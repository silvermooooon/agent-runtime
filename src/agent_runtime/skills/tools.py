"""Dedicated skill tools, independent of the storage backend and generic read tool."""

from ..types import AbortSignal, AgentTool, AgentToolResult
from .local import LocalSkillStore
from .store import SkillStore


def create_load_skill_tool(store: SkillStore) -> AgentTool:
    async def execute(call_id, args, signal=None, on_update=None):
        signal = signal or AbortSignal()
        signal.throw_if_aborted()
        skill = await signal.run(store.load(args["name"]))
        references = "\n".join(f"- {name}" for name in skill.references) or "(none)"
        return AgentToolResult(
            [
                {
                    "type": "text",
                    "text": (
                        f"Skill: {skill.name}\n\n{skill.content}\n\n"
                        f"Available references:\n{references}\n\n"
                        "Use load_skill_reference with this skill name and a reference name "
                        "when its additional instructions are needed."
                    ),
                }
            ],
            details={
                "skill": {
                    "name": skill.name,
                    "description": skill.description,
                    "references": list(skill.references),
                }
            },
        )

    return AgentTool(
        "load_skill",
        "Load the complete instructions of a skill from the available skill catalog. "
        "Use its name, not a filesystem path. The result lists reference names that "
        "can be loaded separately with load_skill_reference.",
        {
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1, "description": "Skill name"},
            },
            "required": ["name"],
            "additionalProperties": False,
        },
        execute,
        replay="safe",
        version="skills-1",
    )


def create_load_skill_reference_tool(store: SkillStore) -> AgentTool:
    async def execute(call_id, args, signal=None, on_update=None):
        signal = signal or AbortSignal()
        signal.throw_if_aborted()
        content = await signal.run(store.load_reference(args["name"], args["reference"]))
        return AgentToolResult(
            [{"type": "text", "text": content}],
            details={"skill": {"name": args["name"], "reference": args["reference"]}},
        )

    return AgentTool(
        "load_skill_reference",
        "Load the complete text of a named skill reference. Use the skill name and "
        "reference name returned by load_skill, e.g. references/style.md. "
        "References are resolved by the skill store; do not pass an absolute path.",
        {
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1, "description": "Skill name"},
                "reference": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Reference name within this skill",
                },
            },
            "required": ["name", "reference"],
            "additionalProperties": False,
        },
        execute,
        replay="safe",
        version="skills-1",
    )


def create_skill_tools(directory=None, *, store=None, env=None):
    """Explicitly enable both tools. An injected store takes precedence over local config."""
    store = store if store is not None else LocalSkillStore(directory, env=env)
    return [create_load_skill_tool(store), create_load_skill_reference_tool(store)]
