"""pi EventStream: async iteration plus independently awaitable final result."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from typing import Generic, TypeVar

E = TypeVar("E")
R = TypeVar("R")


class EventStream(Generic[E, R]):
    def __init__(
        self, is_complete: Callable[[E], bool], extract_result: Callable[[E], R], *, max_pending=64
    ):
        if type(max_pending) is not int or max_pending <= 0:
            raise ValueError("max_pending must be a positive integer")
        self.max_pending = max_pending
        self._queue: deque[E] = deque()
        self._wake = asyncio.Event()
        self._space = asyncio.Event()
        self._space.set()
        self._result_only = False
        self._done = False
        self._failure: BaseException | None = None
        self._result: R | None = None
        self._complete = asyncio.Event()
        self._is_complete = is_complete
        self._extract_result = extract_result
        self.task: asyncio.Task | None = None

    def push(self, event: E) -> None:
        """Synchronous insertion for a finite parser batch; async producers use send()."""
        if self._done:
            return
        if not self._result_only:
            self._queue.append(event)
            if len(self._queue) >= self.max_pending:
                self._space.clear()
        if self._is_complete(event):
            self.end(self._extract_result(event))
        self._wake.set()

    async def wait_for_capacity(self):
        while not self._done and not self._result_only and len(self._queue) >= self.max_pending:
            await self._space.wait()

    async def send(self, event: E) -> None:
        await self.wait_for_capacity()
        self.push(event)

    def end(self, result: R | None = None) -> None:
        if not self._done:
            self._result = result
        self._done = True
        self._complete.set()
        self._wake.set()
        self._space.set()

    def fail(self, error: BaseException) -> None:
        self._failure = error
        self.end()

    def __aiter__(self):
        return self

    async def __anext__(self) -> E:
        while True:
            if self._queue:
                event = self._queue.popleft()
                if len(self._queue) < self.max_pending:
                    self._space.set()
                return event
            if self._done:
                if self._failure:
                    raise self._failure
                raise StopAsyncIteration
            self._wake.clear()
            await self._wake.wait()

    async def result(self) -> R:
        """Wait for the final result, discarding any unconsumed intermediate events.

        To observe both, first iterate the stream and then await result(). This also
        lets result-only callers complete without filling a queue nobody consumes.
        """
        self._result_only = True
        self._queue.clear()
        self._space.set()
        await self._complete.wait()
        if self._failure:
            raise self._failure
        return self._result  # type: ignore[return-value]


class AssistantMessageEventStream(EventStream[dict, dict]):
    def __init__(self, *, max_pending=64):
        super().__init__(
            lambda e: e["type"] in ("done", "error"),
            lambda e: e["message"] if e["type"] == "done" else e["error"],
            max_pending=max_pending,
        )
