"""Wire spelling for dotted platform names; no registry or collision policy.

Like pi's mcp__server__tool spelling, double underscores work with function APIs.
The host owns name uniqueness, including this spelling. Journals keep logical names.
"""

from copy import deepcopy

from ..transcript import current_tools


def wire_name(name):
    return name.replace(".", "__")


def encode_tool_names(payload):
    def rename(item):
        if "name" in item:
            item["name"] = wire_name(item["name"])

    for tool in payload.get("tools", []):
        rename(tool.get("function", tool))
    for item in payload.get("input", []):
        if item.get("type") == "function_call":
            rename(item)
    for message in payload.get("messages", []):
        for call in message.get("tool_calls", []):
            rename(call["function"])
        content = message.get("content")
        for block in content if isinstance(content, list) else []:
            if block.get("type") == "tool_use":
                rename(block)
    choice = payload.get("tool_choice")
    if isinstance(choice, dict):
        rename(choice.get("function", choice))
    return payload


class ToolNameStream:
    def __init__(self, stream, context):
        self.stream = stream
        names = [t["name"] for t in current_tools(context.messages)]
        for message in context.messages:
            content = message.get("content", [])
            names.extend(
                b["name"] for b in content if isinstance(b, dict) and b.get("type") == "toolCall"
            )
        self.names = {wire_name(name): name for name in names if "." in name}

    def push(self, event):
        if self.names:
            event = deepcopy(event)
            if "toolCall" in event:
                block = event["toolCall"]
                block["name"] = self.names.get(block["name"], block["name"])
            for key in ("partial", "message", "error"):
                message = event.get(key)
                if isinstance(message, dict):
                    for block in message.get("content", []):
                        if block.get("type") == "toolCall":
                            block["name"] = self.names.get(block["name"], block["name"])
        self.stream.push(event)
