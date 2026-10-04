"""Provider message conversion adapted from pi's AI API adapters."""

import json
from copy import deepcopy

from ..transcript import content_text, current_tools, system_prompt


def _blocks(message):
    content = message.get("content", [])
    return [{"type": "text", "text": content}] if isinstance(content, str) else content


def _call_id(value):
    return value.split("|", 1)[0]


def _image_url(block):
    return block.get("url") or f"data:{block['mimeType']};base64,{block['data']}"


def _anthropic_image(block):
    source = (
        {"type": "url", "url": block["url"]}
        if "url" in block
        else {"type": "base64", "media_type": block["mimeType"], "data": block["data"]}
    )
    return {"type": "image", "source": source}


def _tool_output(message, api):
    blocks = _blocks(message)
    text = content_text(blocks)
    images = [b for b in blocks if b["type"] == "image"]
    if not images:
        return text or "(no tool output)"
    if api == "openai-responses":
        return ([{"type": "input_text", "text": text}] if text else []) + [
            {"type": "input_image", "detail": "auto", "image_url": _image_url(b)} for b in images
        ]
    if api == "anthropic-messages":
        return [{"type": "text", "text": text or "(see attached image)"}] + [
            _anthropic_image(b) for b in images
        ]
    return text or "(see attached image)"


def build_payload(model, context, parameters):
    if model.api == "openai-responses":
        return _responses(model, context, parameters)
    if model.api == "openai-completions":
        return _completions(model, context, parameters)
    if model.api == "anthropic-messages":
        return _anthropic(model, context, parameters)
    raise ValueError(f"Unsupported API: {model.api}")


def _responses(model, context, parameters):
    items = []
    prompt = system_prompt(context.messages)
    if prompt:
        items.append(
            {
                "role": "developer"
                if model.compat.get("supports_developer_role", True)
                else "system",
                "content": prompt,
            }
        )
    for message in context.messages:
        role = message["role"]
        if role == "user":
            content = []
            for block in _blocks(message):
                if block["type"] == "text":
                    content.append({"type": "input_text", "text": block["text"]})
                elif block["type"] == "image":
                    content.append({"type": "input_image", "image_url": _image_url(block)})
            items.append({"role": "user", "content": content})
        elif role == "toolResult":
            items.append(
                {
                    "type": "function_call_output",
                    "call_id": _call_id(message["toolCallId"]),
                    "output": _tool_output(message, model.api),
                }
            )
        elif role == "assistant":
            if message.get("stopReason") in ("error", "aborted"):
                continue
            same_model = (
                message.get("api") == model.api
                and message.get("provider") == model.provider
                and message.get("model") == model.id
            )
            if same_model and message.get("responseOutput"):
                items.extend(deepcopy(message["responseOutput"]))
                continue
            for block in _blocks(message):
                if block["type"] == "text":
                    items.append({"role": "assistant", "content": block["text"]})
                elif block["type"] == "toolCall":
                    items.append(
                        {
                            "type": "function_call",
                            "call_id": _call_id(block["id"]),
                            "name": block["name"],
                            "arguments": json.dumps(block["arguments"]),
                        }
                    )
    result = {"model": model.id, "stream": True, "store": False, "input": items, **parameters}
    tools = current_tools(context.messages)
    if tools:
        result["tools"] = [{"type": "function", **t, "strict": False} for t in tools]
    return result


def _completions(model, context, parameters):
    messages = []
    tool_images = []

    def flush_tool_images():
        if tool_images:
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Attached image(s) from tool result:"},
                        *tool_images,
                    ],
                }
            )
            tool_images.clear()

    prompt = system_prompt(context.messages)
    if prompt:
        messages.append(
            {
                "role": "developer"
                if model.reasoning and model.compat.get("supports_developer_role", True)
                else "system",
                "content": prompt,
            }
        )
    for message in context.messages:
        role = message["role"]
        if role != "toolResult":
            flush_tool_images()
        if role == "user":
            content = []
            for block in _blocks(message):
                if block["type"] == "text":
                    content.append({"type": "text", "text": block["text"]})
                elif block["type"] == "image":
                    content.append({"type": "image_url", "image_url": {"url": _image_url(block)}})
            messages.append({"role": "user", "content": content})
        elif role == "toolResult":
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": _call_id(message["toolCallId"]),
                    "content": _tool_output(message, model.api),
                }
            )
            tool_images.extend(
                {"type": "image_url", "image_url": {"url": _image_url(b)}}
                for b in _blocks(message)
                if b["type"] == "image"
            )
        elif role == "assistant" and message.get("stopReason") not in ("error", "aborted"):
            converted = {
                "role": "assistant",
                "content": content_text(message.get("content")) or None,
            }
            calls = [
                {
                    "id": _call_id(b["id"]),
                    "type": "function",
                    "function": {"name": b["name"], "arguments": json.dumps(b["arguments"])},
                }
                for b in _blocks(message)
                if b["type"] == "toolCall"
            ]
            if calls:
                converted["tool_calls"] = calls
            messages.append(converted)
    flush_tool_images()
    result = {"model": model.id, "messages": messages, "stream": True, **parameters}
    if model.compat.get("supports_usage_in_streaming", True):
        result["stream_options"] = {"include_usage": True}
    if model.compat.get("supports_store", model.provider.startswith("openai")):
        result["store"] = False
    tools = current_tools(context.messages)
    if tools:
        result["tools"] = [{"type": "function", "function": {**t, "strict": False}} for t in tools]
    return result


def _anthropic(model, context, parameters):
    messages = []
    for message in context.messages:
        role = message["role"]
        if role == "system" or (
            role == "assistant" and message.get("stopReason") in ("error", "aborted")
        ):
            continue
        content = []
        if role == "toolResult":
            content = [
                {
                    "type": "tool_result",
                    "tool_use_id": _call_id(message["toolCallId"]),
                    "content": _tool_output(message, model.api),
                    "is_error": message.get("isError", False),
                }
            ]
            role = "user"
        else:
            for block in _blocks(message):
                kind = block["type"]
                if kind == "text":
                    content.append({"type": "text", "text": block["text"]})
                elif kind == "image":
                    content.append(_anthropic_image(block))
                elif kind == "toolCall":
                    content.append(
                        {
                            "type": "tool_use",
                            "id": _call_id(block["id"]),
                            "name": block["name"],
                            "input": block["arguments"],
                        }
                    )
                elif kind == "thinking" and block.get("thinkingSignature"):
                    if (
                        message.get("provider") == model.provider
                        and message.get("model") == model.id
                    ):
                        content.append(
                            {
                                "type": "thinking",
                                "thinking": block["thinking"],
                                "signature": block["thinkingSignature"],
                            }
                        )
        if content:
            if messages and messages[-1]["role"] == role:
                messages[-1]["content"].extend(content)
            else:
                messages.append({"role": role, "content": content})
    result = {"model": model.id, "messages": messages, "stream": True, **parameters}
    prompt = system_prompt(context.messages)
    if prompt:
        result["system"] = prompt
    tools = current_tools(context.messages)
    if tools:
        result["tools"] = [
            {"name": t["name"], "description": t["description"], "input_schema": t["parameters"]}
            for t in tools
        ]
    return result
