"""pi's inexpensive context estimate: reuse applicable usage, otherwise estimate chars / 4."""

import json
import math

from ..transcript import content_text


def _json_tokens(value):
    return math.ceil(len(json.dumps(value, ensure_ascii=False, separators=(",", ":"))) / 4)


def estimate_message_tokens(message):
    content = message.get("content", [])
    if message["role"] == "system":
        text = "\n\n".join(
            filter(
                None,
                [
                    content_text(content),
                    *[v for v in message.get("sections", {}).values() if v is not None],
                ],
            )
        )
        return math.ceil(len(text) / 4) + sum(
            _json_tokens(message[key]) for key in ("toolsAdded", "toolsRemoved") if message.get(key)
        )
    if isinstance(content, str):
        return math.ceil(len(content) / 4)
    chars = 0
    for block in content:
        if block["type"] == "text":
            chars += len(block["text"])
        elif block["type"] == "thinking":
            chars += len(block["thinking"])
        elif block["type"] == "image":
            chars += 4800
        elif block["type"] == "toolCall":
            chars += len(block["name"]) + len(json.dumps(block["arguments"], separators=(",", ":")))
    return math.ceil(chars / 4)


def estimate_context_tokens(context):
    usage_tokens, last_index, prefix_timestamp = 0, -1, float("-inf")
    for index, message in enumerate(context.messages):
        stamp = message.get("timestamp", 0)
        usage = message.get("usage") or {}
        tokens = usage.get("totalTokens") or sum(
            usage.get(k, 0) for k in ("input", "output", "cacheRead", "cacheWrite")
        )
        if (
            message["role"] == "assistant"
            and stamp >= prefix_timestamp
            and message.get("stopReason") not in ("aborted", "error")
            and tokens > 0
        ):
            usage_tokens, last_index = tokens, index
        prefix_timestamp = max(prefix_timestamp, stamp)
    return usage_tokens + sum(
        estimate_message_tokens(m) for m in context.messages[last_index + 1 :]
    )


def clamp_max_tokens_to_context(model, context, maximum):
    if model.context_window <= 0:
        return max(1, maximum)
    available = model.context_window - estimate_context_tokens(context) - 4096
    return min(maximum, max(1, available))
