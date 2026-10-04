"""pi EventStream: async iteration plus independently awaitable final result."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from typing import Generic, TypeVar

E = TypeVar("E")
R = TypeVar("R")


class EventStream(Generic[E, R]):
    def __init__(self, is_complete: Callable[[E], bool], extract_result: Callable[[E], R]):
        self._queue: deque[E] = deque()
        self._wake = asyncio.Event()
        self._done = False
        self._failure: BaseException | None = None
        self._result: R | None = None
        self._complete = asyncio.Event()
        self._is_complete = is_complete
        self._extract_result = extract_result
        self.task: asyncio.Task | None = None

    def push(self, event: E) -> None:
        if self._done:
            return
        self._queue.append(event)
        if self._is_complete(event):
            self.end(self._extract_result(event))
        self._wake.set()

    def end(self, result: R | None = None) -> None:
        if not self._done:
            self._result = result
        self._done = True
        self._complete.set()
        self._wake.set()

    def fail(self, error: BaseException) -> None:
        self._failure = error
        self.end()

    def __aiter__(self):
        return self

    async def __anext__(self) -> E:
        while True:
            if self._queue:
                return self._queue.popleft()
            if self._done:
                if self._failure:
                    raise self._failure
                raise StopAsyncIteration
            self._wake.clear()
            await self._wake.wait()

    async def result(self) -> R:
        await self._complete.wait()
        if self._failure:
            raise self._failure
        return self._result  # type: ignore[return-value]


class AssistantMessageEventStream(EventStream[dict, dict]):
    def __init__(self):
        super().__init__(
            lambda e: e["type"] in ("done", "error"),
            lambda e: e["message"] if e["type"] == "done" else e["error"],
        )
