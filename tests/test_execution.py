"""Real local execution and encrypted credentials; no external services or model calls."""

import asyncio
import json
import os
import shlex
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_loop import MODEL, FakeProvider, answer, call
from test_sessions import ProcessLost

from agent_runtime import AbortSignal, Agent, LocalSession, SessionInfo, ToolRecoveryRequired
from agent_runtime.execution import LocalExecutionBackend, create_execution_tools
from agent_runtime.execution._redact import Redactor
from agent_runtime.secrets import LocalSecretStore, SecretError
from agent_runtime.tools import create_bash_tool

try:
    from cryptography.fernet import Fernet
except ImportError:
    Fernet = None


@unittest.skipIf(Fernet is None, "Install agent-runtime[secrets]")
class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.session = LocalSession(
            "chat", self.root / "sessions", tenant_id="org", user_id="alice"
        )
        self.key = Fernet.generate_key()
        self.store = LocalSecretStore(self.session.info, self.root / "secrets", key=self.key)
        self.backend = LocalExecutionBackend(self.session.info, self.root / "workspaces")
        self.value = "demo-secret-你好-12345"

    async def tools(self, **kwargs):
        return await create_execution_tools(
            session=self.session, backend=self.backend, secrets=self.store, **kwargs
        )

    async def bash(self, **kwargs):
        return next(tool for tool in await self.tools(**kwargs) if tool.name == "bash")

    async def test_encrypted_storage_reopen_rotation_delete_and_metadata(self):
        info = await self.store.put("api", {"API_TOKEN": self.value}, description="Project API")
        self.assertEqual(info.env_names, ("API_TOKEN",))
        self.assertNotIn(self.value, repr(info))
        path = next(self.store.directory.glob("*.secret"))
        self.assertNotIn(self.value.encode(), path.read_bytes())
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        reopened = LocalSecretStore(self.session.info, self.root / "secrets", key=self.key)
        self.assertEqual(await reopened.resolve("api"), {"API_TOKEN": self.value})
        self.assertEqual(await reopened.list(), [info])
        await reopened.put("api", {"API_TOKEN": "rotated"})
        self.assertEqual(await self.store.resolve("api"), {"API_TOKEN": "rotated"})
        await reopened.delete("api")
        self.assertEqual(await self.store.list(), [])
        with self.assertRaises(SecretError):
            await self.store.resolve("api")

    async def test_missing_key_tampering_and_cross_user_ciphertext_rejected(self):
        with self.assertRaises(ValueError):
            LocalSecretStore(self.session.info, env={})
        await self.store.put("api", {"API_TOKEN": self.value})
        wrong_key = LocalSecretStore(
            self.session.info, self.root / "secrets", key=Fernet.generate_key()
        )
        with self.assertRaises(SecretError):
            await wrong_key.resolve("api")
        bob = LocalSecretStore(SessionInfo("b", "org", "bob"), self.root / "secrets", key=self.key)
        await bob.put("api", {"API_TOKEN": "bob-value"})
        alice_file = next(self.store.directory.glob("*.secret"))
        next(bob.directory.glob("*.secret")).write_bytes(alice_file.read_bytes())
        with self.assertRaises(SecretError):
            await bob.resolve("api")
        alice_file.write_bytes(b"broken ciphertext")
        with self.assertRaises(SecretError):
            await self.store.resolve("api")

    async def test_identity_and_explicit_credential_selection(self):
        await self.store.put("api", {"API_TOKEN": self.value})
        with self.assertRaises(ValueError):
            LocalExecutionBackend(SessionInfo("anonymous"))
        other = LocalSession(directory=None, tenant_id="org", user_id="bob")
        with self.assertRaises(ValueError):
            await create_execution_tools(session=other, backend=self.backend)
        with self.assertRaises(ValueError):
            await create_execution_tools(
                session=other,
                backend=LocalExecutionBackend(other.info, self.root),
                secrets=self.store,
            )
        tool = await self.bash()
        self.assertNotIn("API_TOKEN", tool.description)
        with self.assertRaises(PermissionError):
            await tool.execute("x", {"command": "true", "credentials": ["api"]})
        allowed = await self.bash(credential_refs=["api"])
        self.assertIn("API_TOKEN", allowed.description)
        self.assertNotIn(self.value, allowed.description)
        await self.store.delete("api")
        with self.assertRaises(SecretError):
            await allowed.execute("x", {"command": "true", "credentials": ["api"]})

    async def test_plain_bash_no_longer_inherits_service_environment(self):
        with patch.dict(
            os.environ, {"OPENAI_API_KEY": "host-private", "BASH_ENV": "/missing/profile"}
        ):
            tool = create_bash_tool(self.root)
            result = await tool.execute(
                "x", {"command": 'printf "%s|%s" "${OPENAI_API_KEY-unset}" "${BASH_ENV-unset}"'}
            )
        self.assertEqual(result.structured_content["output"], "unset|unset")

    async def test_concurrent_environments_home_and_temp_cleanup(self):
        await self.store.put("api", {"API_TOKEN": self.value})
        bash = await self.bash(credential_refs=["api"])
        script = (
            "import os,time,json; time.sleep(.05); "
            "print(json.dumps({k:os.getenv(k) "
            "for k in ['API_TOKEN','HOME','TMPDIR','OPENAI_API_KEY']}))"
        )
        command = f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"
        before = dict(os.environ)
        with patch.dict(os.environ, {"OPENAI_API_KEY": "host-private"}):
            one, two = await asyncio.gather(
                bash.execute("one", {"command": command, "credentials": ["api"]}),
                bash.execute("two", {"command": command}),
            )
        first = json.loads(one.structured_content["output"])
        second = json.loads(two.structured_content["output"])
        self.assertEqual(first["API_TOKEN"], "[REDACTED]")
        self.assertIsNone(second["API_TOKEN"])
        self.assertIsNone(first["OPENAI_API_KEY"])
        self.assertEqual(first["HOME"], self.backend.home)
        self.assertNotEqual(first["TMPDIR"], second["TMPDIR"])
        self.assertFalse(Path(first["TMPDIR"]).exists())
        self.assertFalse(Path(second["TMPDIR"]).exists())
        self.assertEqual(dict(os.environ), before)

    async def test_all_four_tools_share_backend_and_home(self):
        tools = {tool.name: tool for tool in await self.tools()}
        self.assertEqual(set(tools), {"read", "write", "edit", "bash"})
        await tools["write"].execute("w", {"path": "~/a", "content": "one"})
        await tools["edit"].execute(
            "e", {"path": "~/a", "edits": [{"oldText": "one", "newText": "two"}]}
        )
        self.assertEqual(
            (await tools["read"].execute("r", {"path": "~/a"})).content[0]["text"], "two"
        )
        self.assertEqual(
            (await tools["bash"].execute("b", {"command": 'cat "$HOME/a"'})).structured_content[
                "output"
            ],
            "two",
        )
        await tools["write"].execute("w", {"path": "relative", "content": "work"})
        self.assertEqual((Path(self.backend.cwd) / "relative").read_text(), "work")

    async def test_reserved_bindings_collisions_and_changed_names_fail_before_execution(self):
        await self.store.put("bad", {"BASH_ENV": "not-a-token"})
        with self.assertRaises(ValueError):
            await self.bash(credential_refs=["bad"])
        await self.store.put("a", {"API_TOKEN": self.value})
        await self.store.put("b", {"API_TOKEN": "other"})
        tool = await self.bash(credential_refs=["a", "b"])
        marker = Path(self.backend.cwd) / "should-not-run"
        with self.assertRaises(ValueError):
            await tool.execute("x", {"command": "touch should-not-run", "credentials": ["a", "b"]})
        await self.store.put("a", {"NEW_TOKEN": self.value})
        with self.assertRaises(PermissionError):
            await tool.execute("x", {"command": "touch should-not-run", "credentials": ["a"]})
        self.assertFalse(marker.exists())

    async def test_redaction_before_live_updates_spool_and_session(self):
        await self.store.put("api", {"API_TOKEN": self.value})
        tools = await self.tools(credential_refs=["api"])
        script = (
            "import os,sys,time; v=os.environ['API_TOKEN'].encode(); "
            "sys.stdout.buffer.write(v[:8]); sys.stdout.flush(); time.sleep(.15); "
            "sys.stdout.buffer.write(v[8:]+b'\\n'+b'x'*60000+v)"
        )
        command = f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"
        events = []
        agent = Agent(
            session=self.session,
            model=MODEL,
            tools=tools,
            stream_fn=FakeProvider(
                answer(calls=[call("bash", args={"command": command, "credentials": ["api"]})]),
                answer(),
            ),
        )
        agent.subscribe(lambda event, _: events.append(event))
        await agent.prompt("Use the configured credential")
        self.assertIsNone(agent.state.error_message)
        serialized = json.dumps(events, ensure_ascii=False) + json.dumps(
            self.session.read_records(), ensure_ascii=False
        )
        self.assertNotIn(self.value, serialized)
        self.assertIn("[REDACTED]", serialized)
        files = list(self.backend.output_dir.glob("*.log"))
        self.assertEqual(len(files), 1)
        self.assertNotIn(self.value, files[0].read_text())
        self.assertIn("[REDACTED]", files[0].read_text())
        restored = LocalSession("chat", self.root / "sessions")
        self.assertNotIn(self.value, json.dumps(restored.snapshot, ensure_ascii=False))

    async def test_approval_denies_before_resolving_values(self):
        await self.store.put("api", {"API_TOKEN": self.value})
        tools = await self.tools(credential_refs=["api"])
        agent = Agent(
            session=self.session,
            model=MODEL,
            tools=tools,
            before_tool_call=lambda context, signal: {"block": True, "reason": "Denied"},
            stream_fn=FakeProvider(
                answer(calls=[call("bash", args={"command": "true", "credentials": ["api"]})]),
                answer(),
            ),
        )
        with patch.object(self.store, "resolve", side_effect=AssertionError("Must not resolve")):
            await agent.prompt("go")
        self.assertIn("Denied", json.dumps(self.session.read_records()))

    async def test_backend_exception_is_redacted(self):
        await self.store.put("api", {"API_TOKEN": self.value})
        tool = await self.bash(credential_refs=["api"])

        async def fail(*args, **kwargs):
            raise RuntimeError("Backend echoed " + kwargs["env"]["API_TOKEN"])

        with patch.object(self.backend, "exec", side_effect=fail):
            with self.assertRaises(RuntimeError) as caught:
                await tool.execute("x", {"command": "true", "credentials": ["api"]})
        self.assertNotIn(self.value, str(caught.exception))
        self.assertIn("[REDACTED]", str(caught.exception))

    async def test_cancel_and_normal_exit_cleanup_child_process_group_and_temp(self):
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                marker = Path(self.backend.cwd) / "late"
                tool = await self.bash()
                command = "(sleep .5; touch late) & printf ready; " + ("wait" if cancel else "true")
                ready = asyncio.Event()

                def update(result):
                    if result.content and "ready" in result.content[0]["text"]:
                        ready.set()

                task = asyncio.create_task(
                    tool.execute("x", {"command": command}, on_update=update)
                )
                await asyncio.wait_for(ready.wait(), 3)
                if cancel:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                else:
                    await task
                await asyncio.sleep(0.6)
                self.assertFalse(marker.exists())
                self.assertEqual(list((self.backend.directory / "tmp").iterdir()), [])

    async def test_started_bash_is_not_replayed_after_lost_result(self):
        await self.store.put("api", {"API_TOKEN": self.value})
        tools = await self.tools(credential_refs=["api"])
        execute = self.backend.exec

        async def lost(*args, **kwargs):
            await execute(*args, **kwargs)
            raise ProcessLost()

        agent = Agent(
            session=self.session,
            model=MODEL,
            tools=tools,
            stream_fn=FakeProvider(
                answer(
                    calls=[
                        call(
                            "bash", args={"command": "printf once >> count", "credentials": ["api"]}
                        )
                    ]
                )
            ),
        )
        with patch.object(self.backend, "exec", side_effect=lost):
            with self.assertRaises(ProcessLost):
                await agent.prompt("run")
        restored = LocalSession("chat", self.root / "sessions")
        agent = Agent(
            session=restored,
            model=MODEL,
            stream_fn=FakeProvider(answer()),
            tools=await create_execution_tools(
                session=restored, backend=self.backend, secrets=self.store, credential_refs=["api"]
            ),
        )
        with self.assertRaises(ToolRecoveryRequired):
            await agent.resume()
        self.assertEqual((Path(self.backend.cwd) / "count").read_text(), "once")

    def test_redactor_every_chunk_boundary_and_overlapping_values(self):
        text = b"before abcdef abc after abcdef"
        for size in range(1, len(text) + 1):
            redactor = Redactor(["abc", "abcdef"])
            output = b"".join(redactor.feed(text[i : i + size]) for i in range(0, len(text), size))
            output += redactor.feed(b"", final=True)
            self.assertEqual(output, b"before [REDACTED] [REDACTED] after [REDACTED]")

    async def test_abort_signal_cleans_temporary_directory(self):
        signal = AbortSignal()
        tool = await self.bash()
        ready = asyncio.Event()

        def update(result):
            if result.content and "ready" in result.content[0]["text"]:
                ready.set()

        task = asyncio.create_task(
            tool.execute("x", {"command": "printf ready; sleep 10"}, signal, update)
        )
        await asyncio.wait_for(ready.wait(), 3)
        signal.abort()
        with self.assertRaisesRegex(RuntimeError, "Command aborted"):
            await task
        self.assertEqual(list((self.backend.directory / "tmp").iterdir()), [])

    async def test_two_users_have_distinct_workspaces_and_environment(self):
        bob_session = LocalSession(directory=None, tenant_id="org", user_id="bob")
        bob = LocalExecutionBackend(bob_session.info, self.root / "workspaces")
        bob_tools = {
            t.name: t for t in await create_execution_tools(session=bob_session, backend=bob)
        }
        alice_tools = {t.name: t for t in await self.tools()}
        await asyncio.gather(
            alice_tools["write"].execute("a", {"path": "file", "content": "alice"}),
            bob_tools["write"].execute("b", {"path": "file", "content": "bob"}),
        )
        self.assertNotEqual(bob.cwd, self.backend.cwd)
        self.assertEqual((Path(self.backend.cwd) / "file").read_text(), "alice")
        self.assertEqual((Path(bob.cwd) / "file").read_text(), "bob")

    async def test_backend_can_be_replaced_without_changing_tool_interfaces(self):
        class Remote:
            identity = self.session.info
            cwd, home, output_dir = "/remote/work", "/remote/home", self.root / "output"

            def __init__(self):
                self.files = {}

            async def prepare(self):
                pass

            async def access(self, path):
                if path not in self.files:
                    raise FileNotFoundError(path)

            async def read_file(self, path):
                return self.files[path]

            async def mkdir(self, path):
                pass

            async def write_file(self, path, content):
                self.files[path] = content.encode()

            async def exec(self, command, cwd, *, on_data, signal, timeout, env):
                if command == "uncertain":
                    raise ToolRecoveryRequired(["call"])
                on_data(self.files["/remote/home/a"])
                return 0

        tools = {
            t.name: t for t in await create_execution_tools(session=self.session, backend=Remote())
        }
        await tools["write"].execute("w", {"path": "~/a", "content": "old"})
        await tools["edit"].execute(
            "e", {"path": "~/a", "edits": [{"oldText": "old", "newText": "new"}]}
        )
        self.assertEqual(
            (await tools["read"].execute("r", {"path": "~/a"})).content[0]["text"], "new"
        )
        self.assertEqual(
            (await tools["bash"].execute("b", {"command": "run"})).structured_content["output"],
            "new",
        )
        with self.assertRaises(ToolRecoveryRequired):
            await tools["bash"].execute("b", {"command": "uncertain"})


if __name__ == "__main__":
    unittest.main()
