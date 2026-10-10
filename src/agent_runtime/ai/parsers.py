"""SSE reducers adapted from pi's Responses, Completions and Anthropic adapters."""

import json
from copy import deepcopy


def _push(stream, output, kind, index=None, **fields):
    event = {"type": kind, "partial": deepcopy(output), **fields}
    if index is not None:
        event["contentIndex"] = index
    stream.push(event)


def _arguments(value):
    result = json.loads(value or "{}")
    if not isinstance(result, dict):
        raise ValueError("Tool arguments must be a JSON object")
    return result


def _usage(usage, *, anthropic=False):
    if anthropic:
        cached = usage.get("cache_read_input_tokens", 0)
        write = usage.get("cache_creation_input_tokens", 0)
        inp, out = usage.get("input_tokens", 0), usage.get("output_tokens", 0)
    else:
        cached = (
            usage.get("input_tokens_details") or usage.get("prompt_tokens_details") or {}
        ).get("cached_tokens", 0)
        write = 0
        inp = usage.get("input_tokens", usage.get("prompt_tokens", 0)) - cached
        out = usage.get("output_tokens", usage.get("completion_tokens", 0))
    return {
        "input": inp,
        "output": out,
        "cacheRead": cached,
        "cacheWrite": write,
        "totalTokens": inp + out + cached + write,
    }


class ResponsesParser:
    def __init__(self, output, stream):
        self.output, self.stream = output, stream
        self.slots = {}
        self.terminal = False

    def _slot(self, index, item):
        if index in self.slots:
            return self.slots[index]
        kind = item["type"]
        if kind == "message":
            block = {"type": "text", "text": ""}
            event = "text_start"
        elif kind == "reasoning":
            block = {"type": "thinking", "thinking": ""}
            event = "thinking_start"
        elif kind == "function_call":
            block = {
                "type": "toolCall",
                "id": item["call_id"] + "|" + item.get("id", ""),
                "name": item["name"],
                "arguments": {},
            }
            event = "toolcall_start"
        else:
            raise ValueError(f"Responses output type not implemented: {kind}")
        position = len(self.output["content"])
        self.output["content"].append(block)
        self.slots[index] = {
            "index": position,
            "block": block,
            "json": item.get("arguments", ""),
            "done": False,
        }
        _push(self.stream, self.output, event, position)
        return self.slots[index]

    def _finalize_item(self, index, item, *, emit=True, truncated=False):
        slot = self._slot(index, item)
        was_done = slot["done"]
        block = slot["block"]
        if item["type"] == "message":
            block["text"] = "".join(
                c.get("text", c.get("refusal", "")) for c in item.get("content", [])
            )
            block["textSignature"] = json.dumps(
                {
                    "v": 1,
                    "id": item.get("id"),
                    **({"phase": item["phase"]} if item.get("phase") else {}),
                }
            )
            event, fields = "text_end", {"content": block["text"]}
        elif item["type"] == "reasoning":
            block["thinking"] = "\n\n".join(c.get("text", "") for c in item.get("summary", []))
            block["thinkingSignature"] = json.dumps(item)
            event, fields = "thinking_end", {"content": block["thinking"]}
        else:
            raw = item.get("arguments", slot["json"])
            try:
                block["arguments"] = _arguments(raw)
            except (ValueError, TypeError):
                if not truncated:
                    raise
                block["arguments"] = {}
            event, fields = "toolcall_end", {"toolCall": deepcopy(block)}
        slot["done"] = True
        if emit and not was_done:
            _push(self.stream, self.output, event, slot["index"], **fields)

    def feed(self, event):
        usage = (event.get("response") or {}).get("usage")
        if usage:
            self.output["usage"] = _usage(usage)
            self.output["usageStatus"] = "known"
        kind = event.get("type", "")
        if kind == "response.output_item.added":
            self._slot(event["output_index"], event["item"])
        elif kind in (
            "response.output_text.delta",
            "response.refusal.delta",
            "response.reasoning_summary_text.delta",
            "response.function_call_arguments.delta",
        ):
            slot = self.slots.get(event["output_index"])
            if slot is None:
                raise ValueError("Delta without matching Responses output item")
            block, delta = slot["block"], event["delta"]
            if kind == "response.function_call_arguments.delta":
                slot["json"] += delta
                try:
                    block["arguments"] = _arguments(slot["json"])
                except ValueError:
                    pass  # Partial JSON is display-only, never executable.
                event_type = "toolcall_delta"
            elif kind == "response.reasoning_summary_text.delta":
                block["thinking"] += delta
                event_type = "thinking_delta"
            else:
                block["text"] += delta
                event_type = "text_delta"
            _push(self.stream, self.output, event_type, slot["index"], delta=delta)
        elif kind == "response.output_item.done":
            self._finalize_item(
                event["output_index"],
                event["item"],
                truncated=event["item"].get("status") == "incomplete",
            )
        elif kind in ("response.completed", "response.incomplete"):
            response = event["response"]
            incomplete = kind == "response.incomplete" or response.get("status") == "incomplete"
            reason = (response.get("incomplete_details") or {}).get("reason")
            if incomplete and reason != "max_output_tokens":
                raise ValueError(f"Response incomplete: {reason or 'unknown reason'}")
            for index, item in enumerate(response.get("output", [])):
                self._finalize_item(index, item, truncated=incomplete)
            if any(not slot["done"] for slot in self.slots.values()) and not incomplete:
                raise ValueError("Response completed with an unfinished output item")
            self.output["responseOutput"] = deepcopy(response.get("output", []))
            # Terminal backfill may arrive in a different order from streamed items.
            self.output["content"] = [self.slots[index]["block"] for index in sorted(self.slots)]
            self.output["responseId"] = response.get("id")
            self.output["usage"] = _usage(response.get("usage") or {})
            self.output["usageStatus"] = "known" if response.get("usage") else "unknown"
            tool_use = any(b["type"] == "toolCall" for b in self.output["content"])
            self.output["stopReason"] = (
                "length" if incomplete else "toolUse" if tool_use else "stop"
            )
            self.terminal = True
        elif kind in ("response.failed", "error"):
            error = event.get("response", {}).get("error") or event.get("error") or event
            raise RuntimeError(str(error.get("message", error)))


class CompletionsParser:
    def __init__(self, output, stream):
        self.output, self.stream = output, stream
        self.text_index = None
        self.thinking_index = None
        self.calls = {}
        self.finish_reason = None
        self.terminal = False

    def feed(self, event):
        if "error" in event:
            raise RuntimeError(str(event["error"]))
        if event.get("usage"):
            self.output["usage"] = _usage(event["usage"])
            self.output["usageStatus"] = "known"
        for choice in event.get("choices", []):
            if choice.get("index", 0) != 0:
                continue
            delta = choice.get("delta") or {}
            for field, index_name, block_type, content_field in (
                ("content", "text_index", "text", "text"),
                ("reasoning_content", "thinking_index", "thinking", "thinking"),
            ):
                if delta.get(field):
                    index = getattr(self, index_name)
                    if index is None:
                        index = len(self.output["content"])
                        setattr(self, index_name, index)
                        self.output["content"].append({"type": block_type, content_field: ""})
                        _push(self.stream, self.output, f"{block_type}_start", index)
                    self.output["content"][index][content_field] += delta[field]
                    _push(
                        self.stream, self.output, f"{block_type}_delta", index, delta=delta[field]
                    )
            for call in delta.get("tool_calls", []):
                position = call["index"]
                function = call.get("function", {})
                if position not in self.calls:
                    index = len(self.output["content"])
                    block = {
                        "type": "toolCall",
                        "id": call.get("id", ""),
                        "name": function.get("name", ""),
                        "arguments": {},
                    }
                    self.output["content"].append(block)
                    self.calls[position] = {"index": index, "block": block, "json": ""}
                    _push(self.stream, self.output, "toolcall_start", index)
                slot = self.calls[position]
                if call.get("id"):
                    slot["block"]["id"] = call["id"]
                if function.get("name"):
                    slot["block"]["name"] = function["name"]
                if function.get("arguments"):
                    slot["json"] += function["arguments"]
                    _push(
                        self.stream,
                        self.output,
                        "toolcall_delta",
                        slot["index"],
                        delta=function["arguments"],
                    )
            if choice.get("finish_reason"):
                self.finish_reason = choice["finish_reason"]
        if event.get("type") == "_done":
            self.finish()

    def finish(self):
        if self.terminal:
            return
        reasons = {"stop": "stop", "length": "length", "tool_calls": "toolUse"}
        if self.finish_reason not in reasons:
            raise ValueError(f"Missing or unsupported finish_reason: {self.finish_reason}")
        self.output["stopReason"] = reasons[self.finish_reason]
        for slot in self.calls.values():
            if not slot["block"]["id"] or not slot["block"]["name"]:
                raise ValueError("Incomplete tool call identity")
            try:
                slot["block"]["arguments"] = _arguments(slot["json"])
            except ValueError:
                if self.finish_reason != "length":
                    raise
            _push(
                self.stream,
                self.output,
                "toolcall_end",
                slot["index"],
                toolCall=deepcopy(slot["block"]),
            )
        for index, kind, key in (
            (self.text_index, "text", "text"),
            (self.thinking_index, "thinking", "thinking"),
        ):
            if index is not None:
                _push(
                    self.stream,
                    self.output,
                    f"{kind}_end",
                    index,
                    content=self.output["content"][index][key],
                )
        self.terminal = True


class AnthropicParser:
    def __init__(self, output, stream):
        self.output, self.stream = output, stream
        self.slots = {}
        self.usage = {}
        self.reason = None
        self.terminal = False

    def feed(self, event):
        kind = event.get("type")
        if kind == "message_start":
            self.usage.update(event.get("message", {}).get("usage", {}))
        elif kind == "content_block_start":
            item = event["content_block"]
            content_type = item["type"]
            if content_type == "text":
                block, prefix = {"type": "text", "text": item.get("text", "")}, "text"
            elif content_type == "thinking":
                block, prefix = (
                    {"type": "thinking", "thinking": item.get("thinking", "")},
                    "thinking",
                )
            elif content_type == "tool_use":
                block, prefix = (
                    {
                        "type": "toolCall",
                        "id": item["id"],
                        "name": item["name"],
                        "arguments": item.get("input", {}),
                    },
                    "toolcall",
                )
            else:
                raise ValueError(f"Anthropic content type not implemented: {content_type}")
            index = len(self.output["content"])
            self.output["content"].append(block)
            self.slots[event["index"]] = {
                "block": block,
                "index": index,
                "json": "",
                "prefix": prefix,
                "done": False,
            }
            _push(self.stream, self.output, prefix + "_start", index)
        elif kind == "content_block_delta":
            slot = self.slots[event["index"]]
            block, delta = slot["block"], event["delta"]
            if delta["type"] == "signature_delta":
                block["thinkingSignature"] = block.get("thinkingSignature", "") + delta["signature"]
                return
            if delta["type"] == "input_json_delta":
                text = delta["partial_json"]
                slot["json"] += text
            elif delta["type"] in ("text_delta", "thinking_delta"):
                key = "text" if delta["type"] == "text_delta" else "thinking"
                text = delta[key]
                block[key] += text
            else:
                raise ValueError(f"Unsupported Anthropic delta: {delta['type']}")
            _push(self.stream, self.output, slot["prefix"] + "_delta", slot["index"], delta=text)
        elif kind == "content_block_stop":
            slot = self.slots[event["index"]]
            slot["done"] = True
            if slot["prefix"] == "toolcall":
                # Delay JSON validation until stop_reason is known (max_tokens is not executable).
                fields = {"toolCall": deepcopy(slot["block"])}
                if slot["json"]:
                    try:
                        slot["block"]["arguments"] = _arguments(slot["json"])
                        fields["toolCall"] = deepcopy(slot["block"])
                    except ValueError:
                        slot["invalid_json"] = True
            else:
                fields = {"content": slot["block"][slot["prefix"]]}
            _push(self.stream, self.output, slot["prefix"] + "_end", slot["index"], **fields)
        elif kind == "message_delta":
            self.usage.update(event.get("usage", {}))
            self.reason = event.get("delta", {}).get("stop_reason", self.reason)
        elif kind == "message_stop":
            reasons = {
                "end_turn": "stop",
                "stop_sequence": "stop",
                "tool_use": "toolUse",
                "max_tokens": "length",
                "pause_turn": "stop",
            }
            if self.reason not in reasons:
                raise ValueError(f"Missing or unsupported stop_reason: {self.reason}")
            if self.reason != "max_tokens" and any(
                not s["done"] or s.get("invalid_json") for s in self.slots.values()
            ):
                raise ValueError("Anthropic stream contains incomplete content")
            self.output["stopReason"] = reasons[self.reason]
            self.output["usage"] = _usage(self.usage, anthropic=True)
            self.output["usageStatus"] = "known" if self.usage else "unknown"
            self.terminal = True
        elif kind == "error":
            raise RuntimeError(str(event.get("error", event)))
