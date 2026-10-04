"""Real filesystem/process tests for the four explicitly assembled pi coding tools."""

import asyncio
import base64
import json
import os
import shlex
import signal as process_signal
import sys
import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from PIL import Image
from test_loop import MODEL, FakeProvider, answer, call
from test_sessions import ProcessLost

from agent_runtime import (
    AbortSignal,
    Agent,
    AgentContext,
    LocalSession,
    ToolRecoveryRequired,
    run_tool_call,
)
from agent_runtime.tools import (
    ImageResizeOptions,
    LocalFileOperations,
    create_bash_tool,
    create_coding_tools,
    create_edit_tool,
    create_read_tool,
    create_write_tool,
)
from agent_runtime.tools._output import OutputAccumulator
from agent_runtime.tools._paths import resolve_path
from agent_runtime.tools._truncate import DEFAULT_MAX_BYTES, truncate


class CodingToolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.tools = create_coding_tools(self.directory)

    async def invoke(self, name, args, **kwargs):
        return (
            await run_tool_call(
                call(name, args=args),
                tools=self.tools,
                assistant_message=answer(),
                context=AgentContext(),
                **kwargs,
            )
        )["result"]

    def test_explicit_assembly_and_replay_policy(self):
        agent = Agent(env={}, session=LocalSession(directory=None))
        self.assertEqual(agent.state.tools, [])
        self.assertEqual([t.name for t in self.tools], ["read", "bash", "edit", "write"])
        agent = Agent(tools=self.tools, env={}, session=LocalSession(directory=None))
        self.assertEqual(agent.state.tools, self.tools)
        self.assertEqual([t.replay for t in self.tools], ["safe", "never", "never", "never"])

    async def test_write_creates_parents_and_overwrites_utf8(self):
        await self.invoke("write", {"path": "nested/a.txt", "content": "你好\r\n"})
        self.assertEqual((self.directory / "nested/a.txt").read_bytes(), "你好\r\n".encode())
        await self.invoke("write", {"path": "nested/a.txt", "content": "short"})
        self.assertEqual((self.directory / "nested/a.txt").read_text(), "short")

    async def test_read_pagination_and_out_of_range(self):
        (self.directory / "a").write_text("a\nb\nc\nd")
        result = await self.invoke("read", {"path": "a", "offset": 2, "limit": 2})
        self.assertEqual(
            result["content"][0]["text"],
            "b\nc\n\n[1 more lines in file. Use offset=4 to continue.]",
        )
        self.assertTrue((await self.invoke("read", {"path": "a", "offset": 5}))["isError"])
        self.assertTrue((await self.invoke("read", {"path": "a", "limit": -1}))["isError"])

    async def test_read_line_and_byte_truncation(self):
        path = self.directory / "a"
        path.write_text("\n".join(str(i) for i in range(2100)))
        result = await self.invoke("read", {"path": "a"})
        self.assertEqual(result["details"]["truncation"]["outputLines"], 2000)
        self.assertIn("offset=2001", result["content"][0]["text"])
        path.write_text("x" * 60000)
        result = await self.invoke("read", {"path": "a"})
        self.assertTrue(result["details"]["truncation"]["firstLineExceedsLimit"])
        self.assertIn("Use bash", result["content"][0]["text"])

    async def test_empty_and_invalid_utf8_read(self):
        path = self.directory / "a"
        path.write_bytes(b"")
        self.assertEqual((await self.invoke("read", {"path": "a"}))["content"][0]["text"], "")
        path.write_bytes(b"abc\xff")
        self.assertIn("\ufffd", (await self.invoke("read", {"path": "a"}))["content"][0]["text"])

    async def test_image_magic_resize_and_bmp_conversion(self):
        path = self.directory / "not-an-image-extension"
        Image.new("RGB", (100, 50), "red").save(path, format="PNG")
        tool = create_read_tool(
            self.directory, resize_options=ImageResizeOptions(max_width=20, max_height=20)
        )
        result = await tool.execute("r", {"path": path.name})
        image = result.content[1]
        with Image.open(BytesIO(base64.b64decode(image["data"]))) as decoded:
            self.assertEqual(decoded.size, (20, 10))
        self.assertIn("100x50", result.content[0]["text"])
        Image.new("RGB", (3, 4)).save(path, format="BMP")
        result = await self.invoke("read", {"path": path.name})
        self.assertEqual(result["content"][1]["mimeType"], "image/png")

    async def test_image_resize_can_be_disabled(self):
        path = self.directory / "a.png"
        Image.new("RGB", (20, 30)).save(path)
        tool = create_read_tool(self.directory, auto_resize_images=False)
        result = await tool.execute("r", {"path": "a.png"})
        self.assertEqual(base64.b64decode(result.content[1]["data"]), path.read_bytes())

    def test_path_aliases(self):
        self.assertEqual(
            resolve_path("@a\u00a0b", str(self.directory)), str(self.directory / "a b")
        )
        self.assertEqual(resolve_path("~/a", str(self.directory)), str(Path.home() / "a"))
        self.assertEqual(
            resolve_path((self.directory / "a b").as_uri(), "/"), str(self.directory / "a b")
        )

    async def test_read_mac_filename_fallback(self):
        (self.directory / "Screenshot 1\u202fPM.png").write_text("content")
        result = await self.invoke("read", {"path": "Screenshot 1 PM.png"})
        self.assertEqual(result["content"][0]["text"], "content")

    async def test_edit_multiple_regions_uses_original_and_preserves_bom_crlf(self):
        path = self.directory / "a"
        path.write_bytes(b"\xef\xbb\xbfalpha\r\nbeta\r\ngamma\r\n")
        result = await self.invoke(
            "edit",
            {
                "path": "a",
                "edits": [
                    {"oldText": "alpha", "newText": "beta"},
                    {"oldText": "beta", "newText": "new"},
                ],
            },
        )
        self.assertFalse(result["isError"])
        self.assertEqual(path.read_bytes(), b"\xef\xbb\xbfbeta\r\nnew\r\ngamma\r\n")
        self.assertIn("--- a", result["details"]["patch"])
        self.assertEqual(result["details"]["firstChangedLine"], 1)

    async def test_edit_failures_never_partially_write(self):
        path = self.directory / "a"
        original = "alpha\nbeta\nalpha\n"
        for edits in (
            [{"oldText": "alpha", "newText": "x"}],
            [{"oldText": "missing", "newText": "x"}],
            [{"oldText": "beta", "newText": "beta"}],
            [{"oldText": "", "newText": "x"}],
            [{"oldText": "beta", "newText": "x"}, {"oldText": "missing", "newText": "y"}],
            [{"oldText": "beta\nalpha", "newText": "x"}, {"oldText": "beta", "newText": "y"}],
        ):
            with self.subTest(edits=edits):
                path.write_text(original)
                result = await self.invoke("edit", {"path": "a", "edits": edits})
                self.assertTrue(result["isError"])
                self.assertEqual(path.read_text(), original)

    async def test_edit_fuzzy_keeps_untouched_lines_exact(self):
        path = self.directory / "a"
        path.write_text("keep ‘curly’   \nreplace “this”  \nkeep — end  \n")
        result = await self.invoke(
            "edit",
            {
                "path": "a",
                "edits": [
                    {"oldText": 'replace "this"', "newText": "done"},
                ],
            },
        )
        self.assertFalse(result["isError"])
        self.assertEqual(path.read_text(), "keep ‘curly’   \ndone\nkeep — end  \n")

    async def test_edit_legacy_and_stringified_forms(self):
        path = self.directory / "a"
        for form in (
            {"oldText": "before", "newText": "after"},
            {"edits": {"oldText": "before", "newText": "after"}},
            {"edits": json.dumps([{"oldText": "before", "newText": "after"}])},
        ):
            path.write_text("before")
            result = await self.invoke("edit", {"path": "a", **form})
            self.assertFalse(result["isError"])
            self.assertEqual(path.read_text(), "after")

    async def test_parallel_edits_and_symlink_share_mutation_queue(self):
        (self.directory / "a").write_text("one two")
        (self.directory / "link").symlink_to(self.directory / "a")
        results = await asyncio.gather(
            self.invoke("edit", {"path": "a", "edits": [{"oldText": "one", "newText": "ONE"}]}),
            self.invoke("edit", {"path": "link", "edits": [{"oldText": "two", "newText": "TWO"}]}),
        )
        self.assertFalse(any(r["isError"] for r in results))
        self.assertEqual((self.directory / "a").read_text(), "ONE TWO")

    async def test_cancelled_mutation_holds_queue_until_io_settles(self):
        started, finish = asyncio.Event(), asyncio.Event()
        writes = []

        class SlowOperations(LocalFileOperations):
            async def write_file(self, path, content):
                if content == "first":
                    started.set()
                    await finish.wait()
                writes.append(content)
                await super().write_file(path, content)

        ops = SlowOperations()
        tool = create_write_tool(self.directory, operations=ops)
        first = asyncio.create_task(tool.execute("1", {"path": "a", "content": "first"}))
        await started.wait()
        first.cancel()
        second = asyncio.create_task(tool.execute("2", {"path": "a", "content": "second"}))
        await asyncio.sleep(0.01)
        first.cancel()
        self.assertEqual(writes, [])
        finish.set()
        with self.assertRaises(asyncio.CancelledError):
            await first
        await second
        self.assertEqual(writes, ["first", "second"])
        self.assertEqual((self.directory / "a").read_text(), "second")

    async def test_approval_can_block_actual_write(self):
        seen = []

        def deny(context, signal):
            seen.append(context["args"])
            return {"block": True, "reason": "Denied"}

        result = await self.invoke("write", {"path": "a", "content": "no"}, before_tool_call=deny)
        self.assertTrue(result["isError"])
        self.assertEqual(len(seen), 1)
        self.assertFalse((self.directory / "a").exists())

    async def test_custom_operations(self):
        class MemoryOperations:
            def __init__(self):
                self.files = {}

            async def access(self, path):
                if path not in self.files:
                    raise FileNotFoundError(path)

            async def read_file(self, path):
                return self.files[path]

            async def mkdir(self, path):
                pass

            async def write_file(self, path, content):
                self.files[path] = content.encode()

        ops = MemoryOperations()
        await create_write_tool("/remote", operations=ops).execute(
            "w", {"path": "a", "content": "one"}
        )
        await create_edit_tool("/remote", operations=ops).execute(
            "e", {"path": "a", "edits": [{"oldText": "one", "newText": "two"}]}
        )
        result = await create_read_tool("/remote", operations=ops).execute("r", {"path": "a"})
        self.assertEqual(result.content[0]["text"], "two")

    async def test_bash_cwd_output_exit_code_and_updates(self):
        updates = []
        result = await self.invoke(
            "bash", {"command": "pwd; printf out; printf err >&2; exit 7"}, on_update=updates.append
        )
        self.assertTrue(result["isError"])
        self.assertIn(str(self.directory.resolve()), result["structuredContent"]["output"])
        self.assertIn("out", result["content"][0]["text"])
        self.assertIn("err", result["content"][0]["text"])
        self.assertEqual(result["structuredContent"]["exit_code"], 7)
        self.assertTrue(updates)

    async def test_bash_truncation_and_full_output(self):
        script = "import sys; sys.stdout.write('你好'*400000)"
        result = await self.invoke(
            "bash", {"command": f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"}
        )
        self.assertFalse(result["isError"])
        detail = result["details"]
        path = Path(detail["fullOutputPath"])
        self.addCleanup(path.unlink)
        self.assertEqual(path.read_text(), "你好" * 400000)
        self.assertTrue(detail["truncation"]["lastLinePartial"])
        self.assertLessEqual(detail["truncation"]["outputBytes"], DEFAULT_MAX_BYTES)
        self.assertTrue(result["structuredContent"]["truncated"])
        self.assertNotIn("\ufffd", result["content"][0]["text"])

    async def test_bash_timeout_preserves_output_and_kills_group(self):
        marker = self.directory / "late"
        command = f"printf ready; (sleep 0.4; touch {shlex.quote(str(marker))}) & wait"
        result = await self.invoke("bash", {"command": command, "timeout": 0.1})
        self.assertTrue(result["isError"])
        self.assertIn("ready", result["content"][0]["text"])
        self.assertIn("timed out", result["content"][0]["text"])
        await asyncio.sleep(0.45)
        self.assertFalse(marker.exists())

    async def test_bash_abort_and_task_cancel(self):
        for cancel_task in (False, True):
            ready = asyncio.Event()
            signal = AbortSignal()

            def update(result):
                if result.content and "ready" in result.content[0]["text"]:
                    ready.set()

            task = asyncio.create_task(
                create_bash_tool(self.directory).execute(
                    "b",
                    {"command": "printf ready; sleep 30"},
                    signal,
                    update,
                )
            )
            await asyncio.wait_for(ready.wait(), 3)
            if cancel_task:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 3)
            else:
                signal.abort()
                with self.assertRaisesRegex(RuntimeError, "Command aborted"):
                    await asyncio.wait_for(task, 3)

    async def test_bash_signal_termination_is_not_success(self):
        result = await self.invoke("bash", {"command": "kill -TERM $$"})
        self.assertTrue(result["isError"])
        self.assertEqual(result["structuredContent"]["exit_code"], 128 + process_signal.SIGTERM)

    async def test_background_inherited_pipes_do_not_hang(self):
        pidfile = self.directory / "child.pid"
        result = await asyncio.wait_for(
            self.invoke(
                "bash",
                {
                    "command": f"sleep 30 & echo $! > {shlex.quote(str(pidfile))}; printf done",
                },
            ),
            3,
        )
        try:
            self.assertFalse(result["isError"])
            self.assertIn("done", result["content"][0]["text"])
        finally:
            if pidfile.exists():
                try:
                    os.kill(int(pidfile.read_text()), process_signal.SIGKILL)
                except ProcessLookupError:
                    pass

    async def test_bash_custom_backend_prefix_and_spawn_hook(self):
        seen = []

        class Remote:
            async def exec(self, command, cwd, **options):
                seen.append((command, cwd, options["env"]))
                options["on_data"](b"remote")
                return 0

        tool = create_bash_tool(
            "/remote",
            operations=Remote(),
            command_prefix="setup",
            spawn_hook=lambda ctx: {**ctx, "env": {"TENANT": "one"}},
        )
        result = await tool.execute("b", {"command": "run"})
        self.assertEqual(seen, [("setup\nrun", "/remote", {"TENANT": "one"})])
        self.assertEqual(result.content[0]["text"], "remote")

    async def test_bash_delayed_update_failure_is_propagated(self):
        class Backend:
            async def exec(self, command, cwd, **options):
                options["on_data"](b"one")
                options["on_data"](b"two")
                await asyncio.sleep(0.12)
                return 0

        def update(result):
            if result.content and result.content[0]["text"] == "onetwo":
                raise ValueError("observer failed")

        tool = create_bash_tool(self.directory, operations=Backend())
        with self.assertRaisesRegex(ValueError, "observer failed"):
            await tool.execute("b", {"command": "run"}, on_update=update)

    async def test_real_tools_through_agent_and_local_session(self):
        fake = FakeProvider(
            answer(calls=[call("write", "w", {"path": "a", "content": "one"})]),
            answer(calls=[call("read", "r", {"path": "a"})]),
            answer(
                calls=[
                    call(
                        "edit", "e", {"path": "a", "edits": [{"oldText": "one", "newText": "two"}]}
                    )
                ]
            ),
            answer(calls=[call("bash", "b", {"command": "test -f a"})]),
            answer("done"),
        )
        session = LocalSession("tools", self.directory / "sessions")
        agent = Agent(model=MODEL, stream_fn=fake, tools=self.tools, session=session)
        await agent.prompt("work")
        self.assertIsNone(agent.state.error_message)
        self.assertEqual((self.directory / "a").read_text(), "two")
        results = [r for r in agent.state.messages if r["role"] == "toolResult"]
        self.assertEqual(len(results), 4)
        self.assertFalse(any(r["isError"] for r in results))
        self.assertEqual(
            LocalSession("tools", self.directory / "sessions").snapshot["status"], "completed"
        )

    async def test_unknown_write_outcome_requires_reconciliation(self):
        class CrashOperations(LocalFileOperations):
            async def write_file(self, path, content):
                await super().write_file(path, content)
                raise ProcessLost()

        session_dir = self.directory / "sessions"
        agent = Agent(
            model=MODEL,
            stream_fn=FakeProvider(
                answer(calls=[call("write", args={"path": "a", "content": "saved"})])
            ),
            session=LocalSession("lost", session_dir),
            tools=[create_write_tool(self.directory, operations=CrashOperations())],
        )
        with self.assertRaises(BaseExceptionGroup):
            await agent.prompt("write")
        restored = Agent(
            session=LocalSession("lost", session_dir),
            stream_fn=FakeProvider(answer()),
            tools=[create_write_tool(self.directory)],
        )
        with self.assertRaises(ToolRecoveryRequired):
            await restored.resume()
        self.assertEqual((self.directory / "a").read_text(), "saved")


class TruncationTests(unittest.TestCase):
    def test_line_counts_and_multibyte_boundaries(self):
        self.assertEqual(truncate("")["totalLines"], 0)
        self.assertEqual(truncate("a\n")["totalLines"], 1)
        self.assertEqual(truncate("a\nb\nc", max_lines=2)["content"], "a\nb")
        self.assertEqual(truncate("a\nb\nc", tail=True, max_lines=2)["content"], "b\nc")
        self.assertEqual(truncate("你好", tail=True, max_bytes=4)["content"], "好")

    def test_accumulator_handles_split_utf8_and_keeps_bounded_memory(self):
        output = OutputAccumulator()
        for chunk in (b"\xe4", b"\xbd", b"\xa0", b"\n"):
            output.append(chunk)
        self.assertEqual(output.snapshot()["content"], "你\n")
        for _ in range(50):
            output.append(b"x" * 10000)
        output.finish()
        snapshot = output.snapshot()
        output.close()
        self.addCleanup(Path(output.path).unlink)
        self.assertLess(len(output.tail.encode()), DEFAULT_MAX_BYTES * 4 + 1)
        self.assertEqual(snapshot["totalLines"], 2)
        self.assertEqual(snapshot["totalBytes"], 500004)


if __name__ == "__main__":
    unittest.main()
