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


class SessionError(RuntimeError):
    """Persistence, format, ownership or recovery contract failure; never a tool result."""


class SessionBusyError(SessionError):
    pass


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
    if kind == "run_started":
        for key, incoming in data.get("queues", {}).items():
            for message in incoming:
                if message not in state[key]:
                    state[key].append(message)
        if "context" in data:
            state["messages"] = data["context"]
        state["run_start"] = len(state["messages"])
        state["messages"].extend(data["inputs"])
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
        state["messages"].extend(data["messages"])
        state["run_messages"].extend(data["messages"])
        consume_queues(state, data["messages"])
    elif kind == "queues":
        state["steering"], state["follow_up"] = data["steering"], data["follow_up"]
    elif kind == "input_queued":
        state[data["queue"]].append(data["message"])
    elif kind == "model_request":
        if "context" in data:
            state["messages"] = data["context"]
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
        state["messages"].append(data["message"])
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
        for call in state["assistant"]["content"]:
            if call["type"] == "toolCall":
                item = state["results"][call["id"]]
                message = tool_message(call, item["result"], item["time"])
                state["messages"].append(message)
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
        state.update(empty_state(), messages=data["messages"])
    else:
        raise SessionError(f"Unknown required session record: {kind}")


class Session(ABC):
    """Shared execution semantics. Subclasses implement ordered storage and writer ownership."""

    durable = False

    def __init__(self, session_id=None):
        self.session_id = uuid4().hex if session_id is None else session_id
        self._state = empty_state()
        self._seq = 0
        self._busy = False
        self._failed = False
        self._commit_lock = asyncio.Lock()
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

    async def acquire(self):
        if self._busy:
            raise SessionBusyError("Session already has an active writer")
        self._busy = True
        try:
            await self._acquire()
            self._failed = False
        except BaseException:
            self._busy = False
            raise

    async def release(self):
        try:
            await self._release()
        finally:
            self._busy = False
            self.live.clear()

    async def commit(self, kind, **data):
        if not self._busy or self._failed:
            raise SessionError("Session needs an active, healthy writer")
        async with self._commit_lock:
            try:
                record = json.loads(
                    json.dumps(
                        {
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
                candidate = deepcopy(self._state)
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
                        # Repeated cancellation must not release an in-flight file writer.
                        cancelled = True
                task.result()
            except Exception as error:
                self._failed = True
                raise SessionError(f"Session persistence failed: {error}") from error
            self._state, self._seq = candidate, self._seq + 1
            if cancelled:
                raise asyncio.CancelledError

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
        await self.acquire()
        try:
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
                await self.commit(
                    "tool_completed", call_id=call_id, result=result, time=timestamp()
                )
        finally:
            await self.release()

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
    async def _acquire(self): ...

    @abstractmethod
    async def _release(self): ...
