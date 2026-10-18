"""Optional durable action consent, using the existing immutable command store.

Requests, decisions and single-operation bindings are distinct facts. They never
hold a session execution lease or run an action themselves. The host authenticates
the decision maker and dispatches the approved action through its ordinary durable
tool path, with current permission checks and a stable effect operation ID.
"""

from ..storage.coordination import CommandConflictError, canonical_payload


class ApprovalRequired(PermissionError):
    """The exact action has no matching approval or operation binding."""


class DurableApprovals:
    def __init__(self, store):
        self.store = store

    @staticmethod
    def _key(approval_id, fact):
        return f"approval:{approval_id}:{fact}"

    async def request(
        self, tenant_id, session_id, *, approval_id, user_id, run_id, action, context=None
    ):
        action, action_digest = canonical_payload(action)
        await self.store.accept_command(
            tenant_id,
            session_id,
            command_id=self._key(approval_id, "request"),
            kind="approval_request",
            user_id=user_id,
            payload={
                "approval_id": approval_id,
                "approver_id": user_id,
                "requesting_run_id": run_id,
                "action": action,
                "action_digest": action_digest,
                "context": context or {},
            },
        )
        return await self.get(tenant_id, approval_id)

    async def get(self, tenant_id, approval_id):
        request = await self.store.get_command(tenant_id, self._key(approval_id, "request"))
        if request is None or request["kind"] != "approval_request":
            raise LookupError("Approval not found")
        decision = await self.store.get_command(tenant_id, self._key(approval_id, "decision"))
        return {
            **request["payload"],
            "session_id": request["session_id"],
            "status": decision["payload"]["decision"] if decision else "pending",
            "decision_id": decision["command_id"] if decision else None,
        }

    async def decide(self, tenant_id, approval_id, *, actor_id, decision):
        approval = await self.get(tenant_id, approval_id)
        if actor_id != approval["approver_id"]:
            raise PermissionError("Only the designated approver may decide")
        if decision not in {"approved", "rejected"}:
            raise ValueError("Decision must be approved or rejected")
        # Immutable same-key acceptance makes conflicting concurrent decisions fail.
        # This decision is also the durable notification. The host polls/reconciles
        # it into its own FIFO, deduplicating by decision_id before acknowledgement.
        await self.store.accept_command(
            tenant_id,
            approval["session_id"],
            command_id=self._key(approval_id, "decision"),
            kind="approval_decision",
            user_id=actor_id,
            payload={
                "approval_id": approval_id,
                "actor_id": actor_id,
                "decision": decision,
                "action_digest": approval["action_digest"],
            },
        )
        return await self.get(tenant_id, approval_id)

    async def authorize(self, tenant_id, approval_id, *, user_id, action, operation_id):
        """Bind one exact approved action to one stable logical tool operation.

        Replaying that operation is allowed; a different operation or action is not.
        This is consent, not proof of execution or an external exactly-once guarantee.
        """
        approval = await self.get(tenant_id, approval_id)
        _, action_digest = canonical_payload(action)
        if (
            approval["status"] != "approved"
            or user_id != approval["approver_id"]
            or action_digest != approval["action_digest"]
        ):
            raise ApprovalRequired("The exact action requires this owner's approval")
        try:
            await self.store.accept_command(
                tenant_id,
                approval["session_id"],
                command_id=self._key(approval_id, "operation"),
                kind="approval_operation",
                user_id=user_id,
                payload={
                    "approval_id": approval_id,
                    "operation_id": operation_id,
                    "action_digest": action_digest,
                },
            )
        except CommandConflictError as exc:
            raise ApprovalRequired("Approval is already bound to another operation") from exc
        return approval["action"]
