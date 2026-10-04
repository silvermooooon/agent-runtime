"""Port of pi's system-message/tool declaration replay helpers."""

from copy import deepcopy

from .types import AgentContext, AgentTool, Message, timestamp


def content_text(content) -> str:
    if isinstance(content, str):
        return content
    return "\n".join(
        block.get("text", "") for block in content or [] if block.get("type") == "text"
    )


def tool_declaration(tool: AgentTool) -> dict:
    return {
        "name": tool.name,
        "description": tool.description,
        "parameters": deepcopy(tool.parameters),
    }


def current_tools(messages: list[Message]) -> list[dict]:
    tools = {}
    for message in messages:
        if message.get("role") != "system":
            continue
        for tool in message.get("toolsRemoved", []):
            tools.pop(tool["name"], None)
        for tool in message.get("toolsAdded", []):
            tools[tool["name"]] = tool
    return list(tools.values())


def current_system_message(messages: list[Message]) -> Message | None:
    systems = [m for m in messages if m.get("role") == "system"]
    if not systems:
        return None
    parts, sections = [], {}
    for message in systems:
        text = content_text(message.get("content"))
        if text:
            parts.append(text)
        for key, value in message.get("sections", {}).items():
            if value is None:
                sections.pop(key, None)
            else:
                sections[key] = value
    result = {
        "role": "system",
        "content": "\n\n".join(parts),
        "timestamp": systems[0].get("timestamp", 0),
    }
    if sections:
        result["sections"] = sections
    tools = current_tools(messages)
    if tools:
        result["toolsAdded"] = tools
    return result


def system_prompt(messages: list[Message]) -> str:
    message = current_system_message(messages)
    if not message:
        return ""
    parts = [message["content"], *message.get("sections", {}).values()]
    return "\n\n".join(p for p in parts if p)


def declare_tool_changes(context: AgentContext, pending: list[Message]) -> list[Message]:
    pending = list(pending)
    system_indices = [i for i, m in enumerate(pending) if m.get("role") == "system"]
    index = system_indices[-1] if system_indices else None
    if index is not None:
        pending[index] = {
            k: v for k, v in pending[index].items() if k not in ("toolsAdded", "toolsRemoved")
        }
    previous = {t["name"]: t for t in current_tools(context.messages + pending)}
    current = {t.name: tool_declaration(t) for t in context.tools}
    added = [t for name, t in current.items() if previous.get(name) != t]
    removed = [{"name": name} for name, t in previous.items() if current.get(name) != t]
    if not added and not removed:
        return pending
    if index is None:
        index = next((i for i, m in enumerate(pending) if m["role"] != "system"), len(pending))
        pending.insert(index, {"role": "system", "content": "", "timestamp": timestamp()})
    if added:
        pending[index]["toolsAdded"] = added
    if removed:
        pending[index]["toolsRemoved"] = removed
    return pending
