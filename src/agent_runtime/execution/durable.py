"""SDK-owned durable prompt acceptance, fenced execution, and explicit recovery.

A caller must re-supply the original agent configuration on recovery. The durable
inbox stores immutable input, never credentials or arbitrary Python callbacks.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from uuid import uuid4

from ..sessions import DatabaseSession, SessionError, ToolRecoveryRequired
from ..storage.coordination import CommandConflictError, ExecutionLeaseError, validate_ttl
from ..types import maybe_await
from .host import ExecutionHost


class DurableExecutor:
    """Execute one accepted prompt with SDK-owned leases and heartbeat.

    agent_factory(session) may be async. execute only runs its named command and
    returns busy if an older accepted command owns the FIFO. It never executes a
    different request through that factory. No background scheduler is installed.
    """

    def __init__(
        self, store, agent_factory, *, holder=None, lease_seconds=30, host=None, child=False
    ):
        self.store = store
        self.agent_factory = agent_factory
        self.host = host or ExecutionHost(
            store, holder=holder or uuid4().hex, lease_seconds=lease_seconds
        )
        if self.host.store is not store:
            raise ValueError("Executor and execution host must share the store")
        self.holder = self.host.holder
        self.lease_seconds = validate_ttl(self.host.lease_seconds)
        self.child = child
        self._active = {}

    async def execute(self, tenant_id, session_id, *, user_id, command_id, message):
        command = await self.store.accept_command(
            tenant_id,
            session_id,
            command_id=command_id,
            kind="prompt",
            payload={"message": message},
            user_id=user_id,
        )
        return await self._execute(command, user_id=user_id)

    async def execute_command(self, tenant_id, session_id, *, user_id, command_id):
        """Execute an already accepted named prompt, preserving its immutable payload."""
        command = await self._command(tenant_id, session_id, command_id)
        return await self._execute(command, user_id=user_id)

    async def run_next(self, tenant_id, session_id, *, user_id):
        """Use only with a factory valid for every queued command in this session."""
        command = await self._first_pending(tenant_id, session_id)
        if command is None:
            return {
                "session_id": session_id,
                "command_id": None,
                "run_id": None,
                "status": "idle",
                "snapshot": None,
            }
        return await self._execute(command, user_id=user_id)

    async def resume(self, tenant_id, session_id, *, user_id, command_id):
        """Explicitly resume this original command, including failed/stopped work."""
        command = await self._command(tenant_id, session_id, command_id)
        return await self._execute(command, user_id=user_id, explicit_resume=True)

    async def _first_pending(self, tenant, sid):
        candidates = []
        for state in ("accepted", "applied"):
            candidates.extend(
                await self.store.list_commands(tenant, sid, state=state, kind="prompt", limit=1)
            )
        return min(candidates, key=lambda c: c["command_seq"]) if candidates else None

    async def _command(self, tenant, sid, command_id):
        command = await self.store.get_command(tenant, command_id)
        if command is None or command["session_id"] != sid:
            raise CommandConflictError("Command does not belong to this session")
        return command

    @staticmethod
    def _result(command, status=None, snapshot=None, error=None):
        if snapshot is not None and snapshot.get("run_id") != command["run_id"]:
            snapshot = None
        result = {
            "command_id": command["command_id"],
            "session_id": command["session_id"],
            "run_id": command["run_id"],
            "status": status
            or (
                "stopped"
                if command["state"] == "rejected" and command.get("cancel_requested_at")
                else command["state"]
            ),
            "snapshot": snapshot,
        }
        if error is not None:
            result["error"] = str(error)
        return result

    async def _open(self, command, user_id, lease):
        try:
            session = await DatabaseSession.open(
                self.store, tenant_id=command["tenant_id"], session_id=command["session_id"]
            )
            if session.info.user_id != user_id:
                raise SessionError("Execution user does not own the stored session")
        except FileNotFoundError:
            session = DatabaseSession(
                self.store,
                tenant_id=command["tenant_id"],
                user_id=user_id,
                session_id=command["session_id"],
            )
        session.execution_lease = lease
        return session

    async def _execute(self, command, *, user_id, explicit_resume=False):
        async with self.host.slot(child=self.child):
            return await self._execute_owned(
                command, user_id=user_id, explicit_resume=explicit_resume
            )

    async def _execute_owned(self, command, *, user_id, explicit_resume=False):
        tenant, sid = command["tenant_id"], command["session_id"]
        if command["state"] in ("completed", "rejected") or (
            command["state"] in ("failed", "stopped") and not explicit_resume
        ):
            try:
                snapshot = (
                    await DatabaseSession.open(self.store, tenant_id=tenant, session_id=sid)
                ).snapshot
                if snapshot.get("run_id") != command["run_id"]:
                    snapshot = None
            except FileNotFoundError:
                snapshot = None
            return self._result(command, snapshot=snapshot)
        lease = await self.store.acquire_execution(
            tenant, sid, holder=self.holder, ttl_seconds=self.lease_seconds
        )
        if lease is None:
            return self._result(command, "busy")
        session = None
        work = agent = None
        heartbeat_error = []

        def abort():
            if agent is not None:
                agent.abort()

        def lost(error):
            heartbeat_error.append(error)
            abort()
            if work is not None:
                work.cancel()

        def renewed(current):
            nonlocal lease
            lease = current
            if session is not None:
                session.execution_lease = current

        self.host.register(
            lease, command["command_id"], on_cancel=abort, on_lost=lost, on_renew=renewed
        )
        try:
            command = await self._command(tenant, sid, command["command_id"])
            session = await self._open(command, user_id, lease)
            if command["state"] in ("completed", "rejected") or (
                command["state"] in ("failed", "stopped") and not explicit_resume
            ):
                return self._result(
                    command,
                    snapshot=session.snapshot
                    if session.snapshot.get("run_id") == command["run_id"]
                    else None,
                )
            if explicit_resume and command.get("cancel_requested_at"):
                return self._result(
                    command,
                    "recovery_required",
                    session.snapshot,
                    "Cancelled work requires explicit abandon and a new prompt",
                )
            if command["state"] == "accepted" and command.get("cancel_requested_at"):
                await session.commit(
                    "command_rejected",
                    command_id=command["command_id"],
                    run_id=command["run_id"],
                    reason="Cancelled before execution",
                )
                command = await self._command(tenant, sid, command["command_id"])
                return self._result(command, snapshot=session.snapshot)
            first = await self._first_pending(tenant, sid)
            if command["state"] == "accepted" and (
                first is None or first["command_id"] != command["command_id"]
            ):
                return self._result(command, "busy", session.snapshot)
            if session.resumable:
                if session.snapshot["run_id"] != command["run_id"]:
                    return self._result(
                        command,
                        "recovery_required",
                        session.snapshot,
                        "Earlier unfinished run must be recovered or explicitly abandoned",
                    )
                if (
                    session.snapshot["status"] in ("failed", "stopped", "waiting_recovery")
                    and not explicit_resume
                ):
                    return self._result(
                        command,
                        "recovery_required",
                        session.snapshot,
                        session.snapshot.get("error"),
                    )
            elif command["state"] == "applied":
                raise SessionError(
                    "Applied command has no matching unfinished run; inspect journal"
                )
            elif command["state"] in ("failed", "stopped"):
                return self._result(
                    command,
                    "recovery_required",
                    session.snapshot,
                    "Command has already been abandoned; submit new input",
                )
            if command["kind"] != "prompt":
                return self._result(
                    command,
                    "unsupported",
                    session.snapshot,
                    "DurableExecutor executes prompt commands only",
                )
            session.accepted_command = command
            session.execution_host = self.host
            agent = await maybe_await(self.agent_factory(session))
            if agent.session is not session:
                raise SessionError("Agent factory must use the supplied DatabaseSession")
            self._active[(tenant, command["command_id"])] = agent

            async def run():
                if session.resumable:
                    return await agent.resume()
                return await agent.prompt(command["payload"]["message"])

            if heartbeat_error:
                agent.abort()
                raise ExecutionLeaseError(
                    "Execution ownership lost while opening the agent"
                ) from heartbeat_error[0]
            work = asyncio.create_task(run())
            try:
                await work
            except asyncio.CancelledError:
                agent.abort()
                work.cancel()
                await asyncio.gather(work, return_exceptions=True)
                if heartbeat_error:
                    raise ExecutionLeaseError(
                        "Execution ownership lost; reopen before recovery"
                    ) from heartbeat_error[0]
                raise
            except ToolRecoveryRequired as error:
                if not session._failed and session.snapshot["status"] != "waiting_recovery":
                    await session.commit(
                        "run_interrupted", status="waiting_recovery", reason=str(error)
                    )
                current = await self._command(tenant, sid, command["command_id"])
                return self._result(current, "recovery_required", session.snapshot, error)
            except ExecutionLeaseError:
                # Ownership loss is not a terminal result of the logical command.
                # The next holder must reconcile it from the canonical journal.
                raise
            except SessionError as error:
                # Parallel tools may wrap ownership errors in SessionError. Check
                # the actual lease before classifying any error as recovery work.
                await self.store.assert_execution(lease)
                current = await self._command(tenant, sid, command["command_id"])
                if current["state"] == "accepted" and current.get("cancel_requested_at"):
                    session = await self._open(current, user_id, lease)
                    await session.commit(
                        "command_rejected",
                        command_id=current["command_id"],
                        run_id=current["run_id"],
                        reason="Cancelled before execution",
                    )
                    current = await self._command(tenant, sid, command["command_id"])
                    return self._result(current, snapshot=session.snapshot)
                return self._result(current, "recovery_required", session.snapshot, error)
            current = await self._command(tenant, sid, command["command_id"])
            return self._result(
                current, snapshot=session.snapshot, error=session.snapshot.get("error")
            )
        finally:
            self._active.pop((tenant, command["command_id"]), None)
            self.host.unregister(lease)
            # Lease expiration is safe if shutdown prevents a successful release.
            with suppress(Exception):
                await self.store.release_execution(lease)
            with suppress(Exception):
                await self.host.announce(tenant, sid)

    @staticmethod
    def _settled_context(messages):
        """Exclude an unfinished assistant/tool batch from the next provider context."""
        for index, message in enumerate(messages):
            calls = (
                {
                    block["id"]
                    for block in message.get("content", [])
                    if isinstance(block, dict) and block.get("type") == "toolCall"
                }
                if message.get("role") == "assistant"
                else set()
            )
            if not calls:
                continue
            returned = set()
            for following in messages[index + 1 :]:
                if following.get("role") != "toolResult":
                    break
                returned.add(following.get("toolCallId"))
            if not calls <= returned:
                return messages[:index]
        return messages

    async def abandon(
        self, tenant_id, session_id, *, user_id, command_id, reason="Explicitly abandoned"
    ):
        """Abandon pending execution while preserving committed messages and audit.

        This does not undo tool side effects or reconcile their outcome. Cancelling
        an active executor must settle before this operation acquires its lease.
        """
        command = await self._command(tenant_id, session_id, command_id)
        lease = await self.store.acquire_execution(
            tenant_id, session_id, holder=self.holder, ttl_seconds=self.lease_seconds
        )
        if lease is None:
            return self._result(command, "busy")
        try:
            session = await self._open(command, user_id, lease)
            if command["state"] == "accepted":
                await session.commit(
                    "command_rejected",
                    command_id=command_id,
                    run_id=command["run_id"],
                    reason=reason,
                )
                current = await self._command(tenant_id, session_id, command_id)
                return self._result(current, "stopped", session.snapshot)
            if not session.resumable or session.snapshot["run_id"] != command["run_id"]:
                return self._result(command, snapshot=session.snapshot)
            if session.snapshot["status"] not in ("failed", "stopped"):
                await session.commit("run_interrupted", status="stopped", reason=reason)
            await session.commit(
                "history_reset",
                command_id=command_id,
                reason=reason,
                messages=self._settled_context(session.build_context()),
            )
            current = await self._command(tenant_id, session_id, command_id)
            return self._result(current, snapshot=session.snapshot)
        finally:
            with suppress(Exception):
                await self.store.release_execution(lease)

    async def request_cancel(self, tenant_id, command_id):
        """Persist cross-worker abort intent; active heartbeat observes it."""
        command = await self.host.request_cancel(tenant_id, command_id)
        self.cancel(tenant_id, command_id)
        return None if command is None else self._result(command)

    def cancel(self, tenant_id, command_id):
        """Request abort of a command executing in this executor instance."""
        agent = self._active.get((tenant_id, command_id))
        if agent is None:
            return False
        agent.abort()
        return True
