"""Storage-independent session transitions. Runtime calls these directly, not via listeners."""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import asdict
from uuid import uuid4

from ..transcript import tool_declaration
from ..types import AgentToolResult, Model, ParameterPolicy, default_convert_to_llm, timestamp
from .projection import append_messages, apply_compaction, replace_messages


class SessionError(RuntimeError):
    """Persistence, format or recovery contract failure; never a tool result."""


class ToolRecoveryRequired(SessionError):
    def __init__(self, call_ids):
        self.call_ids = list(call_ids)
        super().__init__(f"Tool outcomes are unknown; reconcile before retrying: {self.call_ids}")


def model_record(model):
    value = asdict(model)
    value.pop("headers")  # Credentials and transport headers are injected on each process.
    policy = value["parameter_policy"]
    policy["unsupported"] = sorted(policy["unsupported"])
    policy["omit_when_reasoning"] = sorted(policy["omit_when_reasoning"])
    policy["value_maps"] = {k: list(v.items()) for k, v in policy["value_maps"].items()}
    return value


def restore_model(value):
    value = deepcopy(value)
    policy = value.pop("parameter_policy")
    policy["unsupported"] = frozenset(policy["unsupported"])
    policy["omit_when_reasoning"] = frozenset(policy["omit_when_reasoning"])
    policy["value_maps"] = {k: dict(v) for k, v in policy["value_maps"].items()}
    return Model(**value, parameter_policy=ParameterPolicy(**policy))


def saved_options(options):
    # Only the published model options and non-secret operational settings are restorable.
    names = {
        "temperature",
        "max_tokens",
        "max_output_tokens",
        "max_completion_tokens",
        "reasoning",
        "reasoning_effort",
        "reasoning_summary",
        "top_p",
        "top_k",
        "tool_choice",
        "service_tier",
        "metadata",
        "frequency_penalty",
        "presence_penalty",
        "stop",
        "stop_sequences",
        "seed",
        "thinking_budgets",
        "timeout",
        "base_url",
    }
    return {k: deepcopy(v) for k, v in options.items() if k in names}


def tool_records(tools):
    return [{**tool_declaration(t), "replay": t.replay, "version": t.version} for t in tools]


def empty_state():
    return {
        "messages": [],
        "message_ids": [],
        "compaction": None,
        "compaction_attempt": None,
        "origin": None,
        "status": "idle",
        "phase": "idle",
        "run_id": None,
        "model": None,
        "options": {},
        "tools": [],
        "assistant": None,
        "results": {},
        "returned": {},
        "started": {},
        "decision": None,
        "last_turn": None,
        "request": None,
        "steering": [],
        "follow_up": [],
        "error": None,
        "tool_execution": "parallel",
        "queue_modes": {"steering": "one-at-a-time", "follow_up": "one-at-a-time"},
        "required_hooks": [],
        "run_start": 0,
        "run_messages": [],
        "step_id": None,
    }


def consume_queues(state, messages):
    for key in ("steering", "follow_up"):
        for message in messages:
            for queued in state[key]:
                if queued == message or (
                    message.get("queueId") and queued.get("queueId") == message["queueId"]
                ):
                    state[key].remove(queued)
                    break


def tool_message(call, result, time):
    return {
        "role": "toolResult",
        "toolCallId": call["id"],
        "toolName": call["name"],
        "content": result.get("content") or [],
        "details": result.get("details"),
        "isError": result.get("isError", False),
        "timestamp": time,
        **({"usage": result["usage"]} if "usage" in result else {}),
    }


def reduce_record(state, record):
    kind, data = record["type"], record["data"]
    if kind == "subagent_created":
        pass  # Composition metadata; never enters model context.
    elif kind == "run_started":
        for key, incoming in data.get("queues", {}).items():
            for message in incoming:
                if message not in state[key]:
                    state[key].append(message)
        if "context" in data:
            replace_messages(state, data["context"], record, "context")
        state["run_start"] = len(state["messages"])
        append_messages(state, data["inputs"], record, "inputs")
        state["run_messages"] = list(data["inputs"])
        consume_queues(state, data["inputs"])
        state.update(
            status="running",
            phase="model",
            run_id=data["run_id"],
            model=data["model"],
            options=data["options"],
            tools=data["tools"],
            assistant=None,
            results={},
            returned={},
            started={},
            decision=None,
            last_turn=None,
            request=None,
            error=None,
            tool_execution=data["tool_execution"],
            queue_modes=data["queue_modes"],
            required_hooks=data["required_hooks"],
            step_id=None,
        )
    elif kind == "messages_added":
        append_messages(state, data["messages"], record, "messages")
        state["run_messages"].extend(data["messages"])
        consume_queues(state, data["messages"])
    elif kind == "queues":
        state["steering"], state["follow_up"] = data["steering"], data["follow_up"]
    elif kind == "input_queued":
        state[data["queue"]].append(data["message"])
    elif kind == "model_request":
        if "context" in data:
            replace_messages(state, data["context"], record, "context")
        state.update(
            model=data["model"],
            options=data["options"],
            tools=data["tools"],
            phase="model",
            request=data,
            step_id=data["step_id"],
            assistant=None,
            results={},
            returned={},
            started={},
        )
    elif kind == "model_completed":
        ids = [b["id"] for b in data["message"]["content"] if b["type"] == "toolCall"]
        if len(ids) != len(set(ids)):
            raise SessionError("Tool call IDs must be unique within one model response")
        append_messages(state, [data["message"]], record, "message")
        state["run_messages"].append(data["message"])
        state.update(
            assistant=data["message"],
            results={},
            returned={},
            started={},
            decision=None,
            phase="tools"
            if any(b["type"] == "toolCall" for b in data["message"]["content"])
            else "finish_turn",
        )
    elif kind == "provider_parameters":
        state["request"]["provider_parameters"] = data
    elif kind == "model_attempt":
        # Partial/error output is audit evidence; it must not become executable history.
        state["error"] = data["message"].get("errorMessage")
    elif kind == "tool_started":
        state["started"][data["call_id"]] = data
    elif kind == "tool_returned":
        state["returned"][data["call_id"]] = data
    elif kind == "tool_completed":
        if data.get("use_returned"):
            data = {**data, "result": state["returned"][data["call_id"]]["result"]}
        state["results"][data["call_id"]] = data
    elif kind == "tool_retry_authorized":
        state["started"].pop(data["call_id"], None)
    elif kind == "tools_completed":
        for index, call in enumerate(state["assistant"]["content"]):
            if call["type"] == "toolCall":
                item = state["results"][call["id"]]
                message = tool_message(call, item["result"], item["time"])
                append_messages(state, [message], record, f"tool-{index}")
                state["run_messages"].append(message)
        state["phase"] = "finish_turn"
    elif kind == "turn_completed":
        state["decision"] = data["decision"]
        state["last_turn"] = {
            "message": state["assistant"],
            "toolResults": [
                m for m in state["messages"][-len(state["results"]) :] if m["role"] == "toolResult"
            ]
            if state["results"]
            else [],
        }
        state["phase"] = "after_turn"
    elif kind == "run_completed":
        state.update(status="completed", phase="idle", request=None, error=None)
    elif kind == "run_interrupted":
        state.update(status=data["status"], error=data.get("reason"))
    elif kind == "run_resumed":
        state.update(status="running", error=None)
    elif kind == "history_reset":
        state.clear()
        state.update(empty_state())
        replace_messages(state, data["messages"], record, "messages")
    elif kind == "compaction":
        apply_compaction(state, record)
        state["compaction_attempt"] = None
    elif kind == "compaction_started":
        state["compaction_attempt"] = deepcopy(data)
    elif kind == "compaction_failed":
        state["compaction_attempt"] = None
    elif kind == "context_replaced":
        replace_messages(state, data["messages"], record, "messages")
        state["compaction"] = None
    elif kind == "session_forked":
        state["origin"] = deepcopy(data)
    else:
        raise SessionError(f"Unknown required session record: {kind}")


class Session(ABC):
    """Ordered session transitions. The caller supplies a single execution coroutine."""

    durable = False

    def __init__(self, session_id=None):
        self.session_id = uuid4().hex if session_id is None else session_id
        self._state = empty_state()
        self._seq = 0
        self._failed = False
        self.live = {}  # Bounded latest-value snapshots, never a token event queue.

    @property
    def snapshot(self):
        return deepcopy(self._state)

    @property
    def revision(self):
        return self._seq

    @property
    def resumable(self):
        return self._state["phase"] != "idle"

    def restored_model(self):
        return restore_model(self._state["model"]) if self._state["model"] else None

    def build_context(self, event_id=None):
        """Derive model-visible messages at the current or a historical journal boundary."""
        if event_id is None:
            return deepcopy(self._state["messages"])
        return self.snapshot_at(event_id)["messages"]

    def context_entries(self):
        return [
            {"message_id": key, "event_id": key.split("/", 1)[0], "message": deepcopy(message)}
            for key, message in zip(self._state["message_ids"], self._state["messages"])
        ]

    def _prefix(self, event_id):
        records = self.read_records()
        for index, record in enumerate(records):
            if record["id"] == event_id:
                return records[: index + 1]
        raise SessionError(f"Unknown event ID: {event_id}")

    def snapshot_at(self, event_id):
        return replay_records(self._prefix(event_id))

    async def fork_into(self, event_id, target):
        """Copy a complete prefix into an empty independent Session, preserving checkpoints."""
        if target is self or target.session_id == self.session_id:
            raise SessionError("Fork needs a different session ID")
        records = self._prefix(event_id)
        records.append(
            {
                "id": uuid4().hex,
                "seq": len(records),
                "type": "session_forked",
                "time": timestamp(),
                "run_id": records[-1].get("run_id"),
                "data": {"parent_session_id": self.session_id, "parent_event_id": event_id},
            }
        )
        state = replay_records(records)
        try:
            if target.revision:
                raise SessionError("Fork target must be empty")
            task = asyncio.create_task(target._import_records(records))
            cancelled = False
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    cancelled = True
            task.result()
            target._state, target._seq = state, len(records)
            if cancelled:
                raise asyncio.CancelledError
        except Exception as error:
            if isinstance(error, SessionError):
                raise
            target._failed = True
            raise SessionError(f"Fork journal publication failed: {error}") from error
        return target

    async def commit(self, kind, **data):
        if self._failed:
            raise SessionError("Session persistence failed; reopen the session before continuing")
        try:
            record = json.loads(
                json.dumps(
                    {
                        "id": uuid4().hex,
                        "type": kind,
                        "seq": self._seq,
                        "data": data,
                        "time": timestamp(),
                        "run_id": data.get("run_id", self._state["run_id"]),
                    },
                    ensure_ascii=False,
                    allow_nan=False,
                )
            )
            # Reducers replace nested values, and only mutate top-level containers.
            # Copy those containers without copying the full message/result history.
            candidate = {
                key: value.copy() if isinstance(value, (list, dict)) else value
                for key, value in self._state.items()
            }
            reduce_record(candidate, record)
        except (TypeError, ValueError, KeyError) as error:
            raise SessionError(f"Invalid session record {kind}: {error}") from error
        task = asyncio.create_task(self._persist(self._seq, record))
        cancelled = False
        try:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    # Settle the write before publishing state or propagating cancellation.
                    cancelled = True
            task.result()
        except Exception as error:
            self._failed = True
            raise SessionError(f"Session persistence failed: {error}") from error
        self._state, self._seq = candidate, self._seq + 1
        if cancelled:
            raise asyncio.CancelledError
        return deepcopy(record)

    async def sync_context(self, messages):
        if messages != self._state["messages"]:
            await self.commit("context_replaced", messages=messages)

    async def sync_queues(self, steering, follow_up):
        if steering != self._state["steering"] or follow_up != self._state["follow_up"]:
            await self.commit("queues", steering=steering, follow_up=follow_up)

    async def compact(self, summary, first_kept_message_id, *, tokens_before=0, details=None):
        """Append a compaction; old records remain untouched."""
        return await self.commit(
            "compaction",
            summary=summary,
            first_kept_message_id=first_kept_message_id,
            tokens_before=tokens_before,
            details=details,
        )

    async def begin(self, context, inputs, config):
        if self.resumable:
            raise SessionError("Session has unfinished work; call Agent.resume() first")
        data = dict(
            run_id=uuid4().hex,
            inputs=inputs,
            model=model_record(config.model),
            options=saved_options(config.options),
            tools=tool_records(context.tools),
            tool_execution=config.tool_execution,
            queue_modes=config.queue_modes,
            queues=config.queue_state,
            required_hooks=[
                name
                for name in (
                    "before_tool_call",
                    "after_tool_call",
                    "finish_turn",
                    "prepare_request",
                    "prepare_next_turn",
                    "transform_context",
                    "convert_to_llm",
                )
                if getattr(config, name) is not None
                and getattr(config, name) is not default_convert_to_llm
            ],
        )
        if context.messages != self._state["messages"]:
            data["context"] = context.messages
        await self.commit("run_started", **data)

    async def request(self, context, model, options, llm_messages):
        data = dict(
            model=model_record(model),
            options=saved_options(options),
            tools=tool_records(context.tools),
            step_id=uuid4().hex,
        )
        if context.messages != self._state["messages"]:
            data["context"] = context.messages
        if llm_messages != context.messages:
            data["llm_messages"] = llm_messages
        await self.commit("model_request", **data)

    async def model_finished(self, message):
        await self.commit(
            "model_attempt" if message["stopReason"] in ("error", "aborted") else "model_completed",
            message=message,
        )

    def check_recovery_tools(self, calls, tools):
        previous = {t["name"]: t for t in self._state["tools"]}
        current = {t.name: t for t in tools}
        unknown = []
        for call in calls:
            if call["id"] in self._state["results"]:
                continue
            tool = current.get(call["name"])
            if not tool or tool_records([tool])[0] != previous.get(call["name"]):
                raise SessionError(f"Restore the original tool definition/version: {call['name']}")
            if (
                call["id"] in self._state["started"]
                and call["id"] not in self._state["returned"]
                and tool.replay != "safe"
            ):
                unknown.append(call["id"])
        if unknown:
            raise ToolRecoveryRequired(unknown)

    def tool_result(self, call_id):
        item = self._state["results"].get(call_id)
        return deepcopy(item["result"]) if item else None

    def tool_operation_id(self, call_id):
        """Stable across recovery, distinct across model steps even if call IDs are reused."""
        if not self._state["run_id"] or not self._state["step_id"]:
            raise SessionError("No active model step")
        return f"{self.session_id}/{self._state['run_id']}/{self._state['step_id']}/{call_id}"

    def result_message(self, call):
        item = self._state["results"][call["id"]]
        return deepcopy(tool_message(call, item["result"], item["time"]))

    async def tool_started(self, call, args):
        await self.commit("tool_started", call_id=call["id"], args=args, time=timestamp())

    def started_call(self, call_id):
        return deepcopy(self._state["started"].get(call_id))

    def returned_result(self, call_id):
        item = self._state["returned"].get(call_id)
        return deepcopy(item["result"]) if item else None

    async def tool_returned(self, call, result):
        await self.commit("tool_returned", call_id=call["id"], result=result, time=timestamp())

    async def tool_finished(self, call, result):
        if self.tool_result(call["id"]) is None:
            data = (
                {"use_returned": True}
                if result == self.returned_result(call["id"])
                else {
                    "result": result,
                }
            )
            await self.commit("tool_completed", call_id=call["id"], time=timestamp(), **data)

    async def resolve_tool_call(self, call_id, result):
        """Record an externally reconciled outcome; never claims an unknown call failed."""
        await self._resolve(call_id, result=result)

    async def authorize_tool_retry(self, call_id):
        """Explicit caller decision to retry an uncertain operation, retaining its call ID."""
        await self._resolve(call_id)

    async def _resolve(self, call_id, result=None):
        if (
            call_id not in self._state["started"]
            or call_id in self._state["results"]
            or call_id in self._state["returned"]
        ):
            raise SessionError("Call is not an unresolved started tool")
        if result is None:
            await self.commit("tool_retry_authorized", call_id=call_id)
        else:
            result = result.to_dict() if isinstance(result, AgentToolResult) else result
            await self.commit("tool_completed", call_id=call_id, result=result, time=timestamp())

    def observe(self, event):
        kind = event["type"]
        if kind in ("message_start", "message_update"):
            self.live["message"] = deepcopy(event["message"])
        elif kind == "message_end":
            self.live.pop("message", None)
        elif kind == "tool_execution_update":
            self.live.setdefault("tools", {})[event["toolCallId"]] = deepcopy(
                event["partialResult"]
            )
        elif kind == "tool_execution_end":
            self.live.get("tools", {}).pop(event["toolCallId"], None)
        elif kind == "agent_end":
            self.live.clear()

    @abstractmethod
    async def _persist(self, seq, record): ...

    @abstractmethod
    def read_records(self, after_seq=-1): ...

    async def _import_records(self, records):
        raise NotImplementedError("This backend does not implement atomic journal import")


def replay_records(records):
    state, seen = empty_state(), set()
    for seq, record in enumerate(records):
        if record["seq"] != seq or record["id"] in seen:
            raise SessionError("Invalid event sequence or duplicate event ID")
        seen.add(record["id"])
        try:
            reduce_record(state, deepcopy(record))
        except (ValueError, KeyError, TypeError) as error:
            raise SessionError(f"Invalid journal event {record['id']}: {error}") from error
    return state
