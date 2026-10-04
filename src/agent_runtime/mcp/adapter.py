"""Turn host-selected MCP definitions into ordinary AgentTool instances."""

import json
from copy import deepcopy

from ..types import AgentTool, AgentToolResult


def mcp_result(raw):
    """Pi-style model projection, keeping the complete MCP result in durable details."""
    content = []
    for block in raw.get("content", []):
        kind = block["type"]
        if kind == "text":
            content.append({"type": "text", "text": block["text"]})
        elif kind == "image":
            content.append({"type": "image", "data": block["data"], "mimeType": block["mimeType"]})
        elif kind == "audio":
            content.append({"type": "text", "text": f"[audio {block['mimeType']} omitted]"})
        elif kind == "resource" and "text" in block["resource"]:
            content.append({"type": "text", "text": block["resource"]["text"]})
        elif kind == "resource" and block["resource"].get("mimeType", "").startswith("image/"):
            resource = block["resource"]
            content.append(
                {"type": "image", "data": resource["blob"], "mimeType": resource["mimeType"]}
            )
        elif kind == "resource_link":
            content.append({"type": "text", "text": f"{block['name']}: {block['uri']}"})
        elif kind == "resource":
            resource = block["resource"]
            content.append(
                {
                    "type": "text",
                    "text": f"[binary resource {resource['uri']} "
                    f"({resource.get('mimeType', 'unknown type')}) omitted]",
                }
            )
        else:
            content.append({"type": "text", "text": f"[unsupported MCP content {kind}]"})
    if not content and "structuredContent" in raw:
        content.append(
            {
                "type": "text",
                "text": json.dumps(raw["structuredContent"], ensure_ascii=False, indent=2),
            }
        )
    return AgentToolResult(
        content=content,
        details={"mcp": deepcopy(raw)},
        structured_content=deepcopy(raw.get("structuredContent")),
        is_error=raw.get("isError", False),
    )


def create_mcp_tool(name, description, input_schema, *, caller, context=None, version=""):
    """Names/schemas are business-owned; no discovery or identity registry is introduced."""
    bound_context = deepcopy(context or {})

    async def execute(call_id, arguments, signal, on_update):
        raw = await caller.call(
            name,
            arguments,
            call_id=call_id,
            signal=signal,
            on_update=on_update,
            context=bound_context,
        )
        return mcp_result(raw)

    return AgentTool(name, description, deepcopy(input_schema), execute, version=version)


def assemble_tools(configs, *, builtins=None, mcp_caller=None, context=None):
    """Select local instances or adapt configured MCP definitions, in configuration order.

    builtin: {type: builtin, name: read}
    mcp: {type: mcp, name: crm.query, description: ..., input_schema: ..., version?: ...}
    """
    builtins = builtins or {}
    tools = []
    for config in configs:
        if config["type"] == "builtin":
            tools.append(builtins[config["name"]])
        elif config["type"] == "mcp":
            if mcp_caller is None:
                raise ValueError("MCP tools require mcp_caller")
            tools.append(
                create_mcp_tool(
                    config["name"],
                    config["description"],
                    config["input_schema"],
                    caller=mcp_caller,
                    context=context,
                    version=config.get("version", ""),
                )
            )
        else:
            raise ValueError(f"Unknown tool type: {config['type']}")
    return tools
