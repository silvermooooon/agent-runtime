"""Bounded process host; PostgreSQL remains the sole execution authority.

Notifications are hints. A transport (for example Redis) calls notify after a
durable change. One task renews all active leases and checks cancellation in
batches; no task state or lease is kept in the transport.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from uuid import uuid4
from weakref import WeakValueDictionary

from ..storage.coordination import ExecutionLeaseError, validate_limit, validate_ttl
from ..types import maybe_await


@dataclass
class _Active:
    lease: object
    command_id: str
    on_cancel: object
    on_lost: object
    on_renew: object


class ExecutionHost:
    def __init__(
        self,
        store,
        *,
        holder=None,
        lease_seconds=30,
        renew_interval=10,
        max_active=8,
        max_active_children=8,
        publish=None,
    ):
        self.store = store
        self.holder = holder or uuid4().hex
        self.lease_seconds = validate_ttl(lease_seconds)
        self.renew_interval = min(validate_ttl(renew_interval), self.lease_seconds / 3)
        self._root_slots = asyncio.Semaphore(validate_limit(max_active))
        self._child_slots = asyncio.Semaphore(validate_limit(max_active_children))
        self.publish = publish
        self._active = {}
        self._signals = WeakValueDictionary()
        self._changed = set()
        self._wake = asyncio.Event()
        self._task = None
        self._closed = False

    def slot(self, *, child=False):
        if self._closed:
            raise RuntimeError("Execution host is closed")
        return self._child_slots if child else self._root_slots

    def notify(self, tenant_id, session_id):
        """Wake local waiters. Safe to repeat; never authorizes execution."""
        key = (tenant_id, session_id)
        event = self._signals.pop(key, None)
        if event is not None:
            event.set()
        if key in self._active:
            self._changed.add(key)
            self._wake.set()

    async def announce(self, tenant_id, session_id):
        """Notify locally and optionally invoke the host's transport publisher."""
        self.notify(tenant_id, session_id)
        if self.publish is not None:
            await maybe_await(self.publish(tenant_id, session_id))

    def change_event(self, tenant_id, session_id):
        """Arm before reading PG, then await to close the read/wait race."""
        return self._signals.setdefault((tenant_id, session_id), asyncio.Event())

    async def wait_for_change(self, tenant_id, session_id, *, timeout=None):
        event = self.change_event(tenant_id, session_id)
        try:
            await asyncio.wait_for(event.wait(), timeout)
            return True
        except TimeoutError:
            return False

    def register(self, lease, command_id, *, on_cancel, on_lost, on_renew):
        if self._closed:
            raise RuntimeError("Execution host is closed")
        key = (lease.tenant_id, lease.session_id)
        if key in self._active:
            raise ExecutionLeaseError("This host already has an active session owner")
        self._active[key] = _Active(lease, command_id, on_cancel, on_lost, on_renew)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._maintain())
        self._wake.set()

    def unregister(self, lease):
        key = (lease.tenant_id, lease.session_id)
        active = self._active.get(key)
        if active is not None and active.lease.fence == lease.fence:
            self._active.pop(key)
        self._wake.set()

    async def _controls(self, active):
        if not active:
            return
        commands = await self.store.get_commands(
            [(item.lease.tenant_id, item.command_id) for item in active]
        )
        for item in active:
            command = commands.get((item.lease.tenant_id, item.command_id))
            if command and command.get("cancel_requested_at"):
                await maybe_await(item.on_cancel())

    async def _maintain(self):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.renew_interval
        while self._active and not self._closed:
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), max(0, deadline - loop.time()))
            except TimeoutError:
                pass
            active = list(self._active.values())
            if not active:
                return
            try:
                if loop.time() >= deadline:
                    renewed = await self.store.renew_executions(
                        [item.lease for item in active], ttl_seconds=self.lease_seconds
                    )
                    by_key = {(lease.tenant_id, lease.session_id): lease for lease in renewed}
                    valid = []
                    for item in active:
                        lease = by_key.get((item.lease.tenant_id, item.lease.session_id))
                        if lease is None:
                            await maybe_await(
                                item.on_lost(
                                    ExecutionLeaseError("Execution lease expired or was superseded")
                                )
                            )
                        else:
                            item.lease = lease
                            await maybe_await(item.on_renew(lease))
                            valid.append(item)
                    await self._controls(valid)
                    deadline = loop.time() + self.renew_interval
                elif self._changed:
                    changed, self._changed = self._changed, set()
                    await self._controls(
                        [
                            item
                            for item in active
                            if (item.lease.tenant_id, item.lease.session_id) in changed
                        ]
                    )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # A failed renewal is loss of confidence in ownership. Do not
                # dispatch more effects while waiting for connectivity to return.
                for item in active:
                    await maybe_await(item.on_lost(error))
                return

    async def execute_command(
        self,
        tenant_id,
        session_id,
        *,
        user_id,
        command_id,
        agent_factory,
        child=False,
    ):
        from .durable import DurableExecutor

        return await DurableExecutor(
            self.store, agent_factory, host=self, child=child
        ).execute_command(tenant_id, session_id, user_id=user_id, command_id=command_id)

    async def resume(
        self,
        tenant_id,
        session_id,
        *,
        user_id,
        command_id,
        agent_factory,
        child=False,
    ):
        from .durable import DurableExecutor

        return await DurableExecutor(self.store, agent_factory, host=self, child=child).resume(
            tenant_id, session_id, user_id=user_id, command_id=command_id
        )

    async def request_cancel(self, tenant_id, command_id):
        command = await self.store.request_cancel(tenant_id, command_id)
        if command is not None:
            await self.announce(tenant_id, command["session_id"])
        return command

    async def recovery_candidates(self, *, limit=100, after=None):
        """Bounded indexed PG read for one centralized recovery dispatcher."""
        return await self.store.execution_candidates(limit=limit, after=after)

    async def close(self):
        self._closed = True
        for item in list(self._active.values()):
            await maybe_await(item.on_lost(ExecutionLeaseError("Execution host closed")))
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        for event in list(self._signals.values()):
            event.set()
        self._signals.clear()
