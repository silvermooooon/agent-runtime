"""Python equivalents of pi packages/agent/src/types.ts. Wire dictionaries keep pi keys."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal, TypeAlias, TypeVar

Message: TypeAlias = dict[str, Any]
AgentEvent: TypeAlias = dict[str, Any]
Hook: TypeAlias = Callable[..., Any]
QueueMode = Literal["all", "one-at-a-time"]
ToolExecutionMode = Literal["parallel", "sequential"]
T = TypeVar("T")


async def maybe_await(value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await value
    return value


def timestamp() -> int:
    return int(time.time() * 1000)


class AbortSignal:
    """Cooperative cancellation passed to providers, hooks and tools."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    @property
    def aborted(self) -> bool:
        return self._event.is_set()

    def abort(self) -> None:
        self._event.set()

    async def wait(self) -> None:
        await self._event.wait()

    def throw_if_aborted(self) -> None:
        if self.aborted:
            raise OperationAborted("Operation aborted")

    async def run(self, awaitable: Awaitable[T]) -> T:
        """Interrupt an in-flight async transport operation, including a stalled read."""
        task = asyncio.ensure_future(awaitable)
        watcher = asyncio.create_task(self.wait())
        try:
            await asyncio.wait((task, watcher), return_when=asyncio.FIRST_COMPLETED)
            self.throw_if_aborted()
            return await task
        finally:
            for pending in (task, watcher):
                if not pending.done():
                    pending.cancel()
            await asyncio.gather(task, watcher, return_exceptions=True)


class OperationAborted(Exception):
    pass


@dataclass
class ParameterPolicy:
    """Canonical constraints: model rewrites override provider rewrites; omissions accumulate."""

    unsupported: frozenset[str] = frozenset()
    fixed_values: dict[str, Any] = field(default_factory=dict)
    value_maps: dict[str, dict[Any, Any]] = field(default_factory=dict)
    ranges: dict[str, tuple[float, float]] = field(default_factory=dict)
    omit_when_reasoning: frozenset[str] = frozenset()


@dataclass
class Model:
    id: str
    provider: str
    api: str
    base_url: str
    reasoning: bool | None = None
    max_tokens: int = 4096
    context_window: int = 0
    thinking_level_map: dict[str, str | None] = field(default_factory=dict)
    compat: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    parameter_policy: ParameterPolicy = field(default_factory=ParameterPolicy)


@dataclass
class AgentToolResult:
    content: list[dict[str, Any]] = field(default_factory=list)
    details: Any = None
    is_error: bool = False
    terminate: bool = False
    structured_content: Any = None
    usage: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        value = {
            "content": self.content,
            "details": self.details,
            "isError": self.is_error,
            "terminate": self.terminate,
        }
        if self.structured_content is not None:
            value["structuredContent"] = self.structured_content
        if self.usage is not None:
            value["usage"] = self.usage
        return value


@dataclass
class AgentTool:
    name: str
    description: str
    parameters: dict[str, Any]
    execute: Hook
    label: str = ""
    prepare_arguments: Hook | None = None
    execution_mode: ToolExecutionMode | None = None
    replay: Literal["never", "safe"] = "never"
    version: str = ""


@dataclass
class AgentContext:
    messages: list[Message] = field(default_factory=list)
    tools: list[AgentTool] = field(default_factory=list)


def default_convert_to_llm(messages: list[Message]) -> list[Message]:
    return [m for m in messages if m.get("role") in ("system", "user", "assistant", "toolResult")]


@dataclass
class AgentLoopConfig:
    model: Model
    options: dict[str, Any] = field(default_factory=dict)
    convert_to_llm: Hook = default_convert_to_llm
    transform_context: Hook | None = None
    get_api_key: Hook | None = None
    get_steering_messages: Hook | None = None
    get_follow_up_messages: Hook | None = None
    before_tool_call: Hook | None = None
    after_tool_call: Hook | None = None
    finish_turn: Hook | None = None
    prepare_request: Hook | None = None
    prepare_next_turn: Hook | None = None
    tool_execution: ToolExecutionMode = "parallel"
    session: Any = None
    queue_modes: dict = field(
        default_factory=lambda: {
            "steering": "one-at-a-time",
            "follow_up": "one-at-a-time",
        }
    )
    queue_state: dict = field(default_factory=dict)


def assistant_message(model: Model, **fields: Any) -> Message:
    return {
        "role": "assistant",
        "content": [],
        "api": model.api,
        "provider": model.provider,
        "model": model.id,
        "stopReason": "pending",
        "timestamp": timestamp(),
        "usage": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 0},
        **fields,
    }


def user_message(text: str) -> Message:
    return {"role": "user", "content": [{"type": "text", "text": text}], "timestamp": timestamp()}
