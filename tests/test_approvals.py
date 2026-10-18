import tempfile
import unittest
from pathlib import Path

from agent_runtime.execution import ApprovalRequired, DurableApprovals
from agent_runtime.storage.coordination import CommandConflictError
from agent_runtime.storage.sqlite import SQLiteStore


class ApprovalTests(unittest.IsolatedAsyncioTestCase):
    async def test_durable_exact_action_owner_and_single_operation_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = await SQLiteStore.connect(path)
            await store.install_schema()
            approvals = DurableApprovals(store)
            action = {"tool": "send", "arguments": {"target": "alice", "amount": 3}}
            await approvals.request(
                "tenant", "session", approval_id="a", user_id="owner", run_id="run", action=action
            )
            with self.assertRaises(ApprovalRequired):
                await approvals.authorize(
                    "tenant", "a", user_id="owner", action=action, operation_id="effect"
                )
            with self.assertRaises(PermissionError):
                await approvals.decide("tenant", "a", actor_id="someone", decision="approved")
            await approvals.decide("tenant", "a", actor_id="owner", decision="approved")
            await store.close()
            store = await SQLiteStore.connect(path)
            self.addAsyncCleanup(store.close)
            approvals = DurableApprovals(store)
            self.assertEqual((await approvals.get("tenant", "a"))["status"], "approved")
            for _ in range(2):
                self.assertEqual(
                    await approvals.authorize(
                        "tenant", "a", user_id="owner", action=action, operation_id="effect"
                    ),
                    action,
                )
            with self.assertRaises(ApprovalRequired):
                await approvals.authorize(
                    "tenant", "a", user_id="owner", action=action, operation_id="another-effect"
                )
            with self.assertRaises(ApprovalRequired):
                await approvals.authorize(
                    "tenant",
                    "a",
                    user_id="owner",
                    action={"tool": "send", "arguments": {"target": "bob", "amount": 3}},
                    operation_id="effect",
                )
            with self.assertRaises(CommandConflictError):
                await approvals.decide("tenant", "a", actor_id="owner", decision="rejected")


if __name__ == "__main__":
    unittest.main()
