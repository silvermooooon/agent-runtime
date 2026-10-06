"""Local background agents. The host owns scheduling and session exclusivity."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from ..agent import Agent
from ..sessions import Session, SessionError
from ..types import maybe_await


@dataclass
class _Child:
    session: Session
    agent: Agent | None = None
    task: asyncio.Task | None = None
    error: str | None = None


class SubagentManager:
    """One manager per parent, on its event loop. Factories must return fresh agents.

    agent_factories maps configuration names to synchronous factory(session) functions.
    session_factory(id) opens that exact session. Keep configurations stable on recovery.
    """

    def __init__(self, parent, *, agent_factories, session_factory):
        self.parent = parent
        self.agent_factories = dict(agent_factories)
        self.session_factory = session_factory
        self._children = {}
        self._listeners = []
        self._closed = False
        self._stopping = False

    def task_id(self, operation_id):
        return uuid5(NAMESPACE_URL, f"subagent:{self.parent.session.session_id}:{operation_id}").hex

    def subscribe(self, listener):
        """Await listener(event, signal) inside the child; keep observers fast and reliable."""
        self._listeners.append(listener)

        def unsubscribe():
            self._listeners.remove(listener)

        return unsubscribe

    def _check_open(self):
        if self._closed or self._stopping:
            raise RuntimeError("Subagent manager is closed or stopping")

    def _creation(self, session):
        return next(
            (r["data"] for r in session.read_records() if r["type"] == "subagent_created"),
            None,
        )

    def _child(self, task_id):
        if task_id not in self._children:
            session = self.session_factory(task_id)
            creation = self._creation(session)
            if not creation or creation["parent_session_id"] != self.parent.session.session_id:
                raise ValueError(f"Unknown child task: {task_id}")
            self._children[task_id] = _Child(session)
        return self._children[task_id]

    def _known_ids(self):
        ids = dict.fromkeys(self._children)
        # A spawn can have committed its child before the parent saved its tool result.
        step = None
        for record in self.parent.session.read_records():
            if record["type"] == "model_request":
                step = record["data"]["step_id"]
            if record["type"] != "model_completed":
                continue
            for call in record["data"]["message"]["content"]:
                if call["type"] == "toolCall" and call["name"] == "spawn_agent":
                    # The step is the most recent model_request preceding this response.
                    operation = (
                        f"{self.parent.session.session_id}/{record['run_id']}/{step}/{call['id']}"
                    )
                    task_id = self.task_id(operation)
                    if self._creation(self.session_factory(task_id)):
                        ids[task_id] = None
        return ids

    async def spawn(self, name, task, *, operation_id):
        """Create once. Repeating an operation finds its child but never restarts it."""
        self._check_open()
        if name not in self.agent_factories:
            raise ValueError(f"Unknown agent configuration: {name}")
        task_id = self.task_id(operation_id)
        if task_id in self._children:
            return self.get(task_id)
        session = self.session_factory(task_id)
        if self._creation(session):
            return self.get(task_id)
        child = _Child(session)
        self._children[task_id] = child

        ready = asyncio.Event()

        async def start():
            await session.commit(
                "subagent_created",
                parent_session_id=self.parent.session.session_id,
                operation_id=operation_id,
                name=name,
                task=task,
            )
            ready.set()
            await self._execute(task_id)

        # The same coroutine owns creation and all subsequent child commits.
        self._launch(child, start)
        # Return only after the creation record is durable, without waiting for the model.
        child.task.add_done_callback(lambda _: ready.set())
        await ready.wait()
        if self._creation(session) is None:
            raise SessionError(child.error or "Child creation interrupted")
        return self.get(task_id)

    def _launch(self, child, operation):
        child.error = None

        async def run():
            try:
                await operation()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                child.error = str(error)

        child.task = asyncio.create_task(run())

    async def _execute(self, task_id, message=None):
        child = self._child(task_id)
        creation = self._creation(child.session)
        agent = self.agent_factories[creation["name"]](child.session)
        child.agent = agent

        async def emit(event, signal):
            for listener in tuple(self._listeners):
                await maybe_await(listener({"task_id": task_id, "event": deepcopy(event)}, signal))

        unsubscribe = agent.subscribe(emit)
        try:
            if message is not None:
                await agent.prompt(message)
            elif child.session.resumable:
                await agent.resume()
            elif not any(r["type"] == "run_started" for r in child.session.read_records()):
                await agent.prompt(creation["task"])
        finally:
            unsubscribe()

    def get(self, task_id=None):
        """Read only; opening an interrupted child does not execute it."""
        if task_id is None:
            return [self.get(key) for key in self._known_ids()]
        child = self._child(task_id)
        state = child.session.snapshot
        active = child.task is not None and not child.task.done()
        status = (
            "running"
            if active
            else (
                "failed"
                if child.error or state["status"] == "failed"
                else "completed"
                if state["status"] == "completed"
                else "needs_recovery"
            )
        )
        result = (
            next((m for m in reversed(state["messages"]) if m["role"] == "assistant"), None)
            if status == "completed"
            else None
        )
        return {
            "task_id": task_id,
            "status": status,
            "result": result,
            "error": child.error or state["error"],
            "session_status": state["status"],
        }

    async def wait(self, task_id, *, timeout=None):
        child = self._child(task_id)
        if child.task and not child.task.done():
            # asyncio.wait never cancels the child when this waiter times out or is cancelled.
            await asyncio.wait([child.task], timeout=timeout)
        return self.get(task_id)

    async def send(self, task_id, message, *, follow_up=False):
        self._check_open()
        child = self._child(task_id)
        if child.task and not child.task.done():
            if child.agent is None or not child.agent.state.is_streaming:
                raise RuntimeError("Child is starting or settling; retry after checking status")
            await child.agent.enqueue(message, follow_up=follow_up)
        else:
            if child.session.snapshot["status"] != "completed" or child.error:
                raise SessionError("Recover the interrupted child before sending new input")
            self._launch(child, lambda: self._execute(task_id, message))
        return self.get(task_id)

    async def resume(self, task_id):
        """Explicit host recovery; unknown tool outcomes retain existing recovery rules."""
        self._check_open()
        child = self._child(task_id)
        if not child.task or child.task.done():
            if self.get(task_id)["status"] != "completed":
                self._launch(child, lambda: self._execute(task_id))
        return self.get(task_id)

    async def stop(self, task_id):
        child = self._child(task_id)
        if child.task and not child.task.done():
            if child.agent:
                child.agent.abort()
            child.task.cancel()
            await asyncio.gather(child.task, return_exceptions=True)
        return self.get(task_id)

    async def stop_all(self):
        """Business stop entry: stop the parent, then settle every local child."""
        self._stopping = True
        try:
            self.parent.abort()
            for task_id in list(self._children):
                await self.stop(task_id)
            await self.parent.wait_for_idle()
        finally:
            self._stopping = False

    async def close(self):
        self._closed = True
        await self.stop_all()
