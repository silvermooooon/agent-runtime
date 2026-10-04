"""Skill storage and dedicated tools, exercised through the existing Agent lifecycle."""

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_loop import MODEL, FakeProvider, answer, call

from agent_runtime import AbortSignal, Agent, AgentContext, LocalSession, run_tool_call
from agent_runtime.skills import (
    LocalSkillStore,
    Skill,
    SkillInfo,
    SkillStore,
    create_skill_tools,
)
from agent_runtime.types import OperationAborted


class RemoteStore(SkillStore):
    """In-memory stand-in for a future DB/S3 implementation, with no local paths."""

    def __init__(self):
        self.calls = []

    async def discover(self):
        return [SkillInfo("remote", "Remote skill")]

    async def load(self, name):
        self.calls.append(("load", name))
        return Skill(name, "Remote skill", "Remote instructions", ("style-guide",))

    async def load_reference(self, name, reference):
        self.calls.append(("reference", name, reference))
        return "Remote reference"


class SkillTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.root = self.base / "skills"
        self.skill_dir = self.root / "report"
        (self.skill_dir / "references").mkdir(parents=True)
        self.instructions = (
            "---\nname: report\ndescription: >-\n  Write a report.\n  Follow the style guide.\n"
            "---\n# 写报告\nRead references/style.md when needed.\n"
        )
        (self.skill_dir / "SKILL.md").write_text(self.instructions, encoding="utf-8")
        (self.skill_dir / "references/style.md").write_text("用简短的段落。\n", encoding="utf-8")
        self.store = LocalSkillStore(self.root)
        self.tools = create_skill_tools(store=self.store)

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

    async def test_discovery_returns_only_metadata_in_stable_order(self):
        other = self.root / "alpha"
        other.mkdir()
        (other / "SKILL.md").write_text("# No front matter\nInstructions\n")
        (self.root / "not-a-skill").mkdir()
        (self.skill_dir / "references/binary.bin").write_bytes(b"\xff")
        items = await self.store.discover()
        self.assertEqual(
            items,
            [
                SkillInfo("alpha", ""),
                SkillInfo("report", "Write a report. Follow the style guide."),
            ],
        )
        self.assertFalse(hasattr(items[1], "content"))
        self.assertFalse(hasattr(items[1], "references"))

    async def test_load_full_instructions_then_reference(self):
        loaded = await self.invoke("load_skill", {"name": "report"})
        self.assertIn(self.instructions, loaded["content"][0]["text"])
        self.assertNotIn("用简短的段落", loaded["content"][0]["text"])
        self.assertEqual(loaded["details"]["skill"]["references"], ["references/style.md"])
        reference = await self.invoke(
            "load_skill_reference",
            {
                "name": "report",
                "reference": "references/style.md",
            },
        )
        self.assertEqual(reference["content"][0]["text"], "用简短的段落。\n")
        self.assertNotIn(str(self.root), str(loaded))
        self.assertNotIn(str(self.root), str(reference))

    async def test_reference_content_is_not_truncated_or_executed(self):
        text = "print('never execute this')\n" * 4000
        (self.skill_dir / "sample.py").write_text(text)
        self.assertEqual(await self.store.load_reference("report", "sample.py"), text)

    def test_environment_and_explicit_precedence(self):
        with patch.dict("os.environ", {"AGENT_SKILLS_DIR": str(self.root)}):
            self.assertEqual(LocalSkillStore().directory, self.root)
            self.assertEqual(len(create_skill_tools()), 2)
            with self.assertRaises(ValueError):
                LocalSkillStore(env={})
        self.assertEqual(
            LocalSkillStore(env={"AGENT_SKILLS_DIR": str(self.root)}).directory, self.root
        )
        self.assertEqual(
            LocalSkillStore(self.root, env={"AGENT_SKILLS_DIR": "/missing"}).directory, self.root
        )
        with self.assertRaises(ValueError):
            LocalSkillStore(env={"AGENT_SKILLS_DIR": " "})
        with self.assertRaises(NotADirectoryError):
            LocalSkillStore(self.base / "missing")

    def test_explicit_assembly_and_safe_replay(self):
        agent = Agent(env={}, session=LocalSession(directory=None))
        self.assertEqual(agent.state.tools, [])
        self.assertEqual([t.name for t in self.tools], ["load_skill", "load_skill_reference"])
        self.assertTrue(all(t.replay == "safe" for t in self.tools))
        self.assertNotIn(str(self.root), str([t.parameters for t in self.tools]))

    async def test_backend_is_replaceable_without_local_config(self):
        store = RemoteStore()
        tools = create_skill_tools(store=store, env={})
        loaded = await tools[0].execute("skill", {"name": "remote"})
        reference = await tools[1].execute("ref", {"name": "remote", "reference": "style-guide"})
        self.assertIn("Remote instructions", loaded.content[0]["text"])
        self.assertEqual(reference.content[0]["text"], "Remote reference")
        self.assertEqual(store.calls, [("load", "remote"), ("reference", "remote", "style-guide")])

    async def test_validation_and_approval_precede_backend_access(self):
        store = RemoteStore()
        self.tools = create_skill_tools(store=store)
        for args in ({}, {"name": "remote", "extra": True}):
            result = await self.invoke("load_skill", args)
            self.assertTrue(result["isError"])
        blocked = await self.invoke(
            "load_skill",
            {"name": "remote"},
            before_tool_call=lambda *_: {"block": True},
        )
        self.assertTrue(blocked["isError"])
        self.assertFalse(store.calls)

    async def test_paths_cannot_escape_skill_scope(self):
        (self.base / "secret").write_text("outside")
        for name, args in (
            ("load_skill", {"name": "../secret"}),
            ("load_skill", {"name": str(self.skill_dir)}),
            ("load_skill_reference", {"name": "report", "reference": "../../secret"}),
            ("load_skill_reference", {"name": "report", "reference": str(self.base / "secret")}),
        ):
            self.assertTrue((await self.invoke(name, args))["isError"])
        (self.skill_dir / "references/escape").symlink_to(self.base / "secret")
        self.assertTrue(
            (
                await self.invoke(
                    "load_skill_reference",
                    {
                        "name": "report",
                        "reference": "references/escape",
                    },
                )
            )["isError"]
        )
        self.assertNotIn("references/escape", (await self.store.load("report")).references)
        (self.root / "external").symlink_to(self.base)
        self.assertEqual([i.name for i in await self.store.discover()], ["report"])

    async def test_missing_skill_reference_and_binary_become_tool_errors(self):
        (self.skill_dir / "binary").write_bytes(b"\xff\x00")
        for name, args in (
            ("load_skill", {"name": "missing"}),
            ("load_skill_reference", {"name": "report", "reference": "missing"}),
            ("load_skill_reference", {"name": "report", "reference": "binary"}),
        ):
            self.assertTrue((await self.invoke(name, args))["isError"])

    async def test_malformed_metadata_reports_an_error(self):
        for content in (
            "---\nunclosed",
            "---\n[broken\n---",
            "---\n- list\n---",
            "---\ndescription: 123\n---",
        ):
            (self.skill_dir / "SKILL.md").write_text(content)
            with self.assertRaises(ValueError):
                await self.store.discover()
            self.assertTrue((await self.invoke("load_skill", {"name": "report"}))["isError"])

    async def test_abort_cancels_backend_wait(self):
        entered, cancelled = asyncio.Event(), asyncio.Event()

        class SlowStore(RemoteStore):
            async def load(self, name):
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        signal = AbortSignal()
        tool = create_skill_tools(store=SlowStore())[0]
        task = asyncio.create_task(tool.execute("load", {"name": "remote"}, signal))
        await asyncio.wait_for(entered.wait(), 2)
        signal.abort()
        with self.assertRaises(OperationAborted):
            await task
        self.assertTrue(cancelled.is_set())

    async def test_parallel_stores_keep_contents_separate(self):
        second = self.base / "second"
        (second / "report").mkdir(parents=True)
        (second / "report/SKILL.md").write_text("Other tenant instructions")
        first_tool = self.tools[0]
        second_tool = create_skill_tools(second)[0]
        results = await asyncio.gather(
            first_tool.execute("first", {"name": "report"}),
            second_tool.execute("second", {"name": "report"}),
        )
        self.assertIn(self.instructions, results[0].content[0]["text"])
        self.assertNotIn("Other tenant", results[0].content[0]["text"])
        self.assertIn("Other tenant instructions", results[1].content[0]["text"])

    async def test_loaded_content_is_journaled_and_reused_after_source_changes(self):
        session = LocalSession("original", self.base / "sessions")
        fake = FakeProvider(
            answer(calls=[call("load_skill", "skill", {"name": "report"})]),
            answer(
                calls=[
                    call(
                        "load_skill_reference",
                        "ref",
                        {
                            "name": "report",
                            "reference": "references/style.md",
                        },
                    )
                ]
            ),
            answer("done"),
        )
        agent = Agent(session=session, model=MODEL, tools=self.tools, stream_fn=fake)
        await agent.prompt("Write a report")
        self.assertIsNone(agent.state.error_message)
        self.assertIn(self.instructions, str(fake.contexts[-1]).replace("\\n", "\n"))
        records = session.read_records()
        returned = [r for r in records if r["type"] == "tool_returned"]
        self.assertEqual(len(returned), 2)
        self.assertIn(self.instructions, returned[0]["data"]["result"]["content"][0]["text"])
        self.assertEqual(returned[1]["data"]["result"]["content"][0]["text"], "用简短的段落。\n")
        (self.skill_dir / "references/style.md").unlink()
        (self.skill_dir / "SKILL.md").write_text("Changed instructions")
        child = await session.fork(returned[1]["id"], session_id="child")
        resumed_model = FakeProvider(answer("resumed"))
        await Agent(session=child, tools=self.tools, stream_fn=resumed_model).resume()
        results = [m for m in resumed_model.contexts[0] if m["role"] == "toolResult"]
        self.assertIn(self.instructions, results[0]["content"][0]["text"])
        self.assertEqual(results[1]["content"][0]["text"], "用简短的段落。\n")


if __name__ == "__main__":
    unittest.main()
