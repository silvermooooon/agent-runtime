"""Durable SDK-managed child agents and restart-reconcilable parent notifications."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid4, uuid5

from ..execution import DurableExecutor, ExecutionHost
from ..sessions import DatabaseSession, SessionError
from ..storage.coordination import validate_limit
from ..types import maybe_await


@dataclass
class _DurableChild:
    command: dict
    origin: dict
    executor: DurableExecutor | None = None
    task: asyncio.Task | None = None
    session: DatabaseSession | None = None
    error: str | None = None
    notification_error: str | None = None
    stopping: bool = False


class DurableSubagentManager:
    """Database-backed counterpart of SubagentManager.

    Child execution, acceptance, leases and result notification are SDK-owned.
    agent_factories[name](session) can be async. Parent notifications contain
    canonical output references, not a second output journal. Call recover() on
    startup to discover children, reconcile terminal notifications, and optionally
    restart interrupted work. Unknown tool effects retain normal recovery rules.
    """

    def __init__(
        self,
        parent,
        *,
        agent_factories,
        store=None,
        holder=None,
        lease_seconds=30,
        host=None,
        max_children=32,
        max_active_children=8,
    ):
        if not isinstance(parent.session, DatabaseSession):
            raise ValueError("DurableSubagentManager requires a DatabaseSession parent")
        self.parent = parent
        self.store = store or parent.session.store
        if self.store is not parent.session.store:
            raise ValueError("Parent and children must share their canonical store")
        self.agent_factories = dict(agent_factories)
        self.host = (
            host
            or getattr(parent.session, "execution_host", None)
            or ExecutionHost(
                self.store,
                holder=holder,
                lease_seconds=lease_seconds,
                max_active_children=max_active_children,
            )
        )
        if self.host.store is not self.store:
            raise ValueError("Subagents and execution host must share their store")
        self.holder = self.host.holder
        self.lease_seconds = self.host.lease_seconds
        self.max_children = validate_limit(max_children)
        self._spawn_lock = asyncio.Lock()
        self._children = {}
        self._listeners = []
        self._completion_listeners = []
        self._closed = False

    def task_id(self, operation_id):
        return uuid5(NAMESPACE_URL, f"subagent:{self.parent.session.session_id}:{operation_id}").hex

    def subscribe(self, listener):
        self._listeners.append(listener)
        return lambda: self._listeners.remove(listener)

    def subscribe_completion(self, listener):
        """Observe an already durable notification; replay via store.list_notifications."""
        self._completion_listeners.append(listener)
        return lambda: self._completion_listeners.remove(listener)

    @property
    def _tenant(self):
        return self.parent.session.info.tenant_id

    @property
    def _user(self):
        return self.parent.session.info.user_id

    async def _commands(self, task_id):
        result, after = [], 0
        while True:
            page = await self.store.list_commands(
                self._tenant, task_id, kind="prompt", after_seq=after, limit=1000
            )
            result.extend(page)
            if len(page) < 1000:
                return result
            after = page[-1]["command_seq"]

    async def _discover(self):
        after = None
        while True:
            page = await self.store.list_child_commands(
                self._tenant, self.parent.session.session_id, limit=1000, after=after
            )
            for command in page:
                task_id = command["session_id"]
                if task_id not in self._children:
                    self._children[task_id] = _DurableChild(command, command["payload"]["subagent"])
            if len(page) < 1000:
                return
            after = (page[-1]["session_id"], page[-1]["command_seq"])

    async def _load(self, task_id):
        if task_id not in self._children:
            await self._discover()
        child = self._children.get(task_id)
        if child is None:
            raise ValueError(f"Unknown child task: {task_id}")
        commands = await self._commands(task_id)
        active = child.task is not None and not child.task.done()
        if not active:
            try:
                child.session = await DatabaseSession.open(
                    self.store, tenant_id=self._tenant, session_id=task_id
                )
            except FileNotFoundError:
                child.session = None
            if commands:
                pending = next((c for c in commands if c["state"] in ("accepted", "applied")), None)
                current_run = (
                    child.session.snapshot["run_id"]
                    if child.session and child.session.resumable
                    else None
                )
                current = (
                    next((c for c in commands if c["run_id"] == current_run), None)
                    if current_run
                    else None
                )
                child.command = current or pending or commands[-1]
        return child

    def get(self, task_id=None):
        """Cached status only. Use await aget() to discover or refresh DB children."""
        if task_id is None:
            return [self.get(key) for key in self._children]
        if task_id not in self._children:
            raise SessionError("Unknown cached child; use await manager.aget(task_id)")
        child = self._children[task_id]
        state = child.session.snapshot if child.session else None
        active = child.task is not None and not child.task.done()
        status = (
            "stop_requested"
            if child.stopping and child.command["state"] in ("accepted", "applied")
            else "running"
            if active
            else (
                "completed"
                if child.command["state"] == "completed"
                else "failed"
                if child.error or child.command["state"] == "failed"
                else "stopped"
                if child.command["state"] in ("stopped", "rejected")
                else "needs_recovery"
            )
        )
        result = None
        if status == "completed" and state and state.get("run_id") == child.command["run_id"]:
            result = next(
                (m for m in reversed(state["messages"]) if m["role"] == "assistant"), None
            )
        return {
            "task_id": task_id,
            "status": status,
            "result": result,
            "error": child.error or (state or {}).get("error"),
            "session_status": (state or {}).get("status", "idle"),
            "run_id": child.command["run_id"],
            "command_id": child.command["command_id"],
            "notification_error": child.notification_error,
        }

    async def aget(self, task_id=None):
        if task_id is not None:
            await self._load(task_id)
            return self.get(task_id)
        await self._discover()
        for key in self._children:
            await self._load(key)
        return self.get()

    async def spawn(self, name, task, *, operation_id):
        # Only one child level is supported by this bounded host. Children cannot
        # recursively occupy the same child pool while waiting on grandchildren.
        if self.parent.session.subagent_creation is not None:
            raise SessionError("Nested durable subagents are not supported")
        async with self._spawn_lock:
            return await self._spawn(name, task, operation_id=operation_id)

    async def _spawn(self, name, task, *, operation_id):
        if self._closed:
            raise RuntimeError("Subagent manager is closed")
        if name not in self.agent_factories:
            raise ValueError(f"Unknown agent configuration: {name}")
        parent_run_id = self.parent.session.snapshot["run_id"]
        if not parent_run_id:
            raise SessionError("A durable child requires a parent run")
        task_id = self.task_id(operation_id)
        command_id = f"subagent-start:{task_id}"
        prior = await self.store.get_command(self._tenant, command_id)
        if prior is None:
            count = await self.store.count_run_children(
                self._tenant,
                self.parent.session.session_id,
                parent_run_id,
            )
            if count >= self.max_children:
                raise SessionError("This parent run has reached its durable child limit")
        origin = {
            "parent_session_id": self.parent.session.session_id,
            "operation_id": operation_id,
            "root_run_id": self.parent.session.root_run_id,
            "parent_run_id": parent_run_id,
            "name": name,
            "task": task,
        }
        command = await self.store.accept_command(
            self._tenant,
            task_id,
            user_id=self._user,
            command_id=command_id,
            kind="prompt",
            payload={"message": task, "subagent": origin},
        )
        if task_id not in self._children:
            self._children[task_id] = _DurableChild(command, origin)
        if prior is None:
            ready = asyncio.Event()
            self._launch(task_id, command_id, ready=ready)
            await ready.wait()
        return await self.aget(task_id)

    def _launch(self, task_id, command_id, *, ready=None, explicit_resume=False):
        child = self._children[task_id]
        if child.task is not None and not child.task.done():
            return
        child.error = None
        child.stopping = False

        async def run():
            try:
                await self._drive(task_id, command_id, ready=ready, explicit_resume=explicit_resume)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                child.error = str(error)
            finally:
                if ready is not None:
                    ready.set()

        child.task = asyncio.create_task(run())
        child.task.add_done_callback(lambda _: asyncio.create_task(self._continue_queued(task_id)))

    async def _continue_queued(self, task_id):
        # A send may commit after the driver's last empty read but before task.done().
        # Rechecking after settlement closes that acceptance/idle scheduling race.
        if self._closed:
            return
        child = self._children[task_id]
        if child.stopping or child.error or child.command["state"] != "completed":
            return
        pending = await self.store.list_commands(
            self._tenant, task_id, kind="prompt", state="accepted", limit=1
        )
        if pending and not self._closed:
            self._launch(task_id, pending[0]["command_id"])

    async def _drive(self, task_id, command_id, *, ready=None, explicit_resume=False):
        child = self._children[task_id]
        origin = child.origin

        async def factory(session):
            child.session = session
            if session.subagent_creation is None:
                await session.commit("subagent_created", **origin)
            else:
                creation = session.subagent_creation
                if any(
                    creation.get(k) != origin.get(k)
                    for k in ("parent_session_id", "operation_id", "name")
                ):
                    raise SessionError("Stored child ancestry/configuration does not match")
            if ready is not None:
                ready.set()
            agent = await maybe_await(self.agent_factories[origin["name"]](session))

            async def emit(event, signal):
                for listener in tuple(self._listeners):
                    await maybe_await(listener({"task_id": task_id, "event": event}, signal))

            agent.subscribe(emit)
            return agent

        child.executor = DurableExecutor(self.store, factory, host=self.host, child=True)
        while not self._closed and not child.stopping:
            if explicit_resume:
                result = await child.executor.resume(
                    self._tenant, task_id, user_id=self._user, command_id=command_id
                )
            else:
                result = await child.executor.execute_command(
                    self._tenant, task_id, user_id=self._user, command_id=command_id
                )
            child.command = await self.store.get_command(self._tenant, command_id)
            if ready is not None:
                ready.set()
            if result["status"] == "busy":
                # A previous worker may still own an unexpired lease after a crash.
                # Stay attached to this accepted child until claim/terminal/stop.
                # Retain explicit_resume across busy ownership waits.
                await self._wait_for_owner(task_id)
                continue
            explicit_resume = False
            if result["status"] in ("completed", "failed", "stopped"):
                await self._deliver(task_id, child.command)
            if result["status"] != "completed":
                if result.get("error"):
                    child.error = result["error"]
                return
            pending = await self.store.list_commands(
                self._tenant, task_id, kind="prompt", state="accepted", limit=1
            )
            if not pending:
                return
            command_id = pending[0]["command_id"]

    async def _wait_for_owner(self, task_id):
        # Arm before the PG read. An explicit notification wakes this immediately;
        # the PG expiry deadline supplies recovery when a notification is lost.
        event = self.host.change_event(self._tenant, task_id)
        lease = await self.store.get_execution(self._tenant, task_id)
        remaining = (lease.expires_at - datetime.now(timezone.utc)).total_seconds() if lease else 0
        if remaining > 0:
            try:
                await asyncio.wait_for(event.wait(), remaining)
            except TimeoutError:
                pass
        else:
            await asyncio.sleep(0)

    async def _deliver(self, task_id, command):
        if command["state"] not in ("completed", "failed", "stopped"):
            return None
        child = self._children[task_id]
        try:
            notification = await self._notify(task_id, command)
            child.notification_error = None
            return notification
        except asyncio.CancelledError:
            raise
        except Exception as error:
            child.notification_error = str(error)
        # The notification is already committed with the child's terminal fact.
        # A central dispatcher reads pending_child_results; there is no per-child
        # retry task or need to rebuild parent/child sessions to deliver it.
        return None

    async def _notify(self, task_id, command):
        if command["state"] not in ("completed", "failed", "stopped"):
            return None
        origin = self._children[task_id].origin
        notification_id = (
            f"subagent-result:{task_id}:{command['run_id']}:{command['completed_event_id']}"
        )
        notification = await self.store.accept_command(
            self._tenant,
            self.parent.session.session_id,
            user_id=self._user,
            command_id=notification_id,
            kind="child_result",
            payload={
                "parent_session_id": self.parent.session.session_id,
                "child_session_id": task_id,
                "run_id": command["run_id"],
                "root_run_id": origin.get("root_run_id"),
                "source_command_id": command["command_id"],
                "status": command["state"],
                "parent_run_id": origin.get("parent_run_id"),
                "completed_event_id": command["completed_event_id"],
            },
        )
        if notification["state"] != "completed":
            await self.host.announce(self._tenant, self.parent.session.session_id)
            for listener in tuple(self._completion_listeners):
                await maybe_await(listener(notification))
        return notification

    async def ack_notification(self, command_id):
        """Call after the product durably accepts this result into its FIFO."""
        return await self.store.ack_notification(
            self._tenant, self.parent.session.session_id, command_id
        )

    async def recover(self, *, resume=False):
        """Rebuild child graph and retry the completion→parent-inbox crash gap.

        Reconciliation is idempotent. resume=True also restarts accepted or
        crash-interrupted child commands, but never failed/stopped/unknown work.
        """
        await self._discover()
        for task_id in self._children:
            child = await self._load(task_id)
            for command in await self._commands(task_id):
                await self._deliver(task_id, command)
            if resume and child.command["state"] in ("accepted", "applied"):
                if child.session is None or child.session.snapshot["status"] not in (
                    "failed",
                    "stopped",
                    "waiting_recovery",
                ):
                    self._launch(task_id, child.command["command_id"])
        return self.get()

    async def wait(self, task_id, *, timeout=None):
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        while True:
            event = self.host.change_event(self._tenant, task_id)
            child = await self._load(task_id)
            if child.command["state"] not in ("accepted", "applied") and (
                child.task is None or child.task.done()
            ):
                return self.get(task_id)
            remaining = None if deadline is None else deadline - asyncio.get_running_loop().time()
            if remaining is not None and remaining <= 0:
                return self.get(task_id)
            tasks = [asyncio.create_task(event.wait())]
            if child.task is not None and not child.task.done():
                tasks.append(child.task)
            try:
                done, _ = await asyncio.wait(
                    tasks, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
                )
                if not done:
                    return await self.aget(task_id)
                if child.task is not None and child.task.done():
                    return await self.aget(task_id)
            finally:
                tasks[0].cancel()
                await asyncio.gather(tasks[0], return_exceptions=True)

    async def send(self, task_id, message, *, follow_up=False):
        """Durably accept a FIFO next prompt; it doesn't mutate an active run."""
        if self._closed:
            raise RuntimeError("Subagent manager is closed")
        child = await self._load(task_id)
        command_id = f"subagent-message:{uuid4().hex}"
        command = await self.store.accept_command(
            self._tenant,
            task_id,
            user_id=self._user,
            command_id=command_id,
            kind="prompt",
            payload={"message": message},
        )
        if child.task is None or child.task.done():
            self._launch(task_id, command_id)
        result = self.get(task_id)
        result["queued_command_id"] = command["command_id"]
        return result

    async def resume(self, task_id):
        child = await self._load(task_id)
        self._launch(task_id, child.command["command_id"], explicit_resume=True)
        return self.get(task_id)

    async def stop(self, task_id):
        child = await self._load(task_id)
        command = child.command
        child.stopping = True
        await self.host.request_cancel(self._tenant, command["command_id"])
        self.host.notify(self._tenant, task_id)
        if child.executor is not None:
            child.executor.cancel(self._tenant, command["command_id"])
        if child.task is not None and not child.task.done():
            await asyncio.gather(child.task, return_exceptions=True)
        executor = child.executor or DurableExecutor(
            self.store, lambda session: None, holder=self.holder, lease_seconds=self.lease_seconds
        )
        await executor.abandon(
            self._tenant,
            task_id,
            user_id=self._user,
            command_id=command["command_id"],
            reason="Child stop requested",
        )
        return await self.aget(task_id)

    async def stop_all(self):
        self.parent.abort()
        await self._discover()
        for task_id in self._children:
            await self.stop(task_id)

    async def close(self):
        """Settle local tasks for shutdown; durable work remains recoverable."""
        self._closed = True
        tasks = [
            c.task for c in self._children.values() if c.task is not None and not c.task.done()
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
