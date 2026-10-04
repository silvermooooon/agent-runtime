"""Port of pi's /api/stream proxy client; this is not a SaaS execution server."""

import asyncio
import json
from contextlib import aclosing
from copy import deepcopy

from .ai.transport import stream_http
from .event_stream import AssistantMessageEventStream
from .types import AbortSignal, assistant_message


def process_proxy_event(event, partial, buffers):
    kind = event["type"]
    if kind == "start":
        return {"type": "start", "partial": deepcopy(partial)}
    if kind in ("done", "error"):
        partial["stopReason"] = event["reason"]
        partial["usage"] = event.get("usage", partial["usage"])
        for name in ("providerThinkingLevel", "errorMessage"):
            if name in event:
                partial[name] = event[name]
        return {
            "type": kind,
            "reason": event["reason"],
            "message" if kind == "done" else "error": deepcopy(partial),
        }
    index = event.get("contentIndex")
    if not isinstance(index, int) or index < 0 or index > len(partial["content"]):
        raise ValueError("Invalid proxy contentIndex")
    prefix = kind.split("_")[0]
    block_type = {"text": "text", "thinking": "thinking", "toolcall": "toolCall"}.get(prefix)
    if block_type is None:
        raise ValueError(f"Unsupported proxy event: {kind}")
    if kind.endswith("_start"):
        block = (
            {"type": "toolCall", "id": event["id"], "name": event["toolName"], "arguments": {}}
            if prefix == "toolcall"
            else {"type": prefix, prefix: ""}
        )
        if index == len(partial["content"]):
            partial["content"].append(block)
        else:
            partial["content"][index] = block
        buffers[index] = ""
    else:
        if index >= len(partial["content"]) or partial["content"][index]["type"] != block_type:
            raise ValueError("Proxy delta/end does not match content type")
        block = partial["content"][index]
        if kind.endswith("_delta"):
            if prefix == "toolcall":
                buffers[index] += event["delta"]
                try:
                    block["arguments"] = json.loads(buffers[index])
                except ValueError:
                    pass
            else:
                block[prefix] += event["delta"]
        elif kind.endswith("_end"):
            if prefix == "toolcall":
                block.update(event["toolCall"])
                buffers.pop(index, None)
            elif "contentSignature" in event:
                block[prefix + "Signature"] = event["contentSignature"]
        else:
            raise ValueError(f"Unsupported proxy event: {kind}")
    return {**event, "partial": deepcopy(partial)}


def stream_proxy(model, context, options, *, client=None):
    stream = AssistantMessageEventStream()
    partial = assistant_message(model)
    signal = options.get("signal") or AbortSignal()

    async def produce():
        async def request():
            fields = {
                "temperature": "temperature",
                "max_tokens": "maxTokens",
                "reasoning": "reasoning",
                "session_id": "sessionId",
                "thinking_budgets": "thinkingBudgets",
                "metadata": "metadata",
            }
            payload = {
                "model": {
                    "id": model.id,
                    "name": model.id,
                    "provider": model.provider,
                    "api": model.api,
                    "baseUrl": model.base_url,
                    "reasoning": model.reasoning,
                    "maxTokens": model.max_tokens,
                    "contextWindow": model.context_window,
                    "thinkingLevelMap": model.thinking_level_map,
                },
                "context": {"messages": context.messages},
                "options": {wire: options[key] for key, wire in fields.items() if key in options},
            }
            buffers = {}
            events = stream_http(
                options["proxy_url"].rstrip("/") + "/api/stream",
                payload,
                {"Authorization": "Bearer " + options["auth_token"]},
                client=client,
                timeout=options.get("timeout", 120),
            )
            async with aclosing(events):
                async for event in events:
                    await stream.wait_for_capacity()
                    converted = process_proxy_event(event, partial, buffers)
                    stream.push(converted)
                    if converted["type"] in ("done", "error"):
                        return
            raise RuntimeError("Connection closed by proxy before the response completed")

        try:
            await signal.run(request())
        except (Exception, asyncio.CancelledError) as error:
            partial["stopReason"] = "aborted" if signal.aborted else "error"
            partial["errorMessage"] = str(error)
            stream.push({"type": "error", "reason": partial["stopReason"], "error": partial})

    stream.task = asyncio.create_task(produce())
    return stream
