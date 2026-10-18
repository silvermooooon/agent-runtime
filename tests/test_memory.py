"""User memory integration with real files/Git and offline model responses."""

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_loop import MODEL, FakeProvider, answer, call
from test_sessions import ProcessLost

from agent_runtime import Agent, LocalSession, SessionInfo, ToolRecoveryRequired
from agent_runtime.memory import (
    GitMemoryError,
    GitMemoryStore,
    LocalMemoryStore,
    MemoryConflict,
    MemorySource,
    create_memory_tools,
    load_memory_index,
    run_memory_dream,
)


class MemoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.session = LocalSession(
            "chat", self.root / "sessions", tenant_id="org", user_id="alice"
        )
        self.store = LocalMemoryStore(self.session.info, self.root / "memory")
        self.sources = (MemorySource("history", "event-1"),)

    async def test_opt_in_scope_and_no_git_dependency(self):
        self.assertFalse((self.root / "memory").exists())
        with self.assertRaises(ValueError):
            create_memory_tools(session=LocalSession(directory=None))
        with self.assertRaises(ValueError):
            create_memory_tools(
                session=LocalSession(directory=None, tenant_id="org", user_id="bob"),
                store=self.store,
            )
        with patch("subprocess.run", side_effect=AssertionError("Git should not run")):
            tools = create_memory_tools(session=self.session, directory=self.root / "memory")
            self.assertEqual(len(tools), 4)
            self.assertFalse((self.root / "memory").exists())
            await self.store.write(
                "MEMORY", "Concise answers", sources=self.sources, expected_version=None
            )
        other = LocalMemoryStore(SessionInfo("other", "other-org", "alice"), self.root / "memory")
        self.assertIsNone(await other.read("MEMORY"))
        self.assertEqual(
            len(create_memory_tools(session=self.session, store=self.store, read_only=True)), 2
        )

    async def test_local_versions_sources_search_and_delete(self):
        entry = await self.store.write(
            "people/alice", "Alice owns pricing", sources=self.sources, expected_version=None
        )
        self.assertEqual((await self.store.read(entry.key)).sources, self.sources)
        self.assertEqual((await self.store.search("alice pricing"))[0]["key"], entry.key)
        with self.assertRaises(MemoryConflict):
            await self.store.write(
                entry.key, "overwrite", sources=self.sources, expected_version=None
            )
        changed = await self.store.write(
            entry.key, "Alice leads product", sources=self.sources, expected_version=entry.version
        )
        with self.assertRaises(MemoryConflict):
            await self.store.delete(entry.key, expected_version=entry.version)
        await self.store.delete(entry.key, expected_version=changed.version)
        self.assertIsNone(await self.store.read(entry.key))
        for key in ("../escape", "/absolute", ".git/config", "a/../b", "a//b"):
            with self.assertRaises(ValueError):
                await self.store.read(key)

    async def test_list_entries_pages_only_selected_bodies(self):
        for key in ("a", "b", "c", "d"):
            await self.store.write(key, key, sources=self.sources, expected_version=None)
        with patch.object(self.store, "_read", wraps=self.store._read) as read:
            page = await self.store.list_entries(limit=2)
            self.assertEqual([entry.key for entry in page["items"]], ["a", "b"])
            self.assertEqual(page["next_cursor"], "b")
            self.assertEqual(read.call_count, 2)
        page = await self.store.list_entries(limit=2, after="b")
        self.assertEqual([entry.key for entry in page["items"]], ["c", "d"])
        self.assertIsNone(page["next_cursor"])
        for invalid in (0, 101, True):
            with self.assertRaises(ValueError):
                await self.store.list_entries(limit=invalid)

    async def test_tools_record_actual_content_and_current_user_source(self):
        provider = FakeProvider(
            answer(
                calls=[
                    call(
                        "memory_write",
                        args={
                            "key": "MEMORY",
                            "content": "Short answers",
                            "expected_version": None,
                        },
                    )
                ]
            ),
            answer(calls=[call("memory_read", args={"key": "MEMORY"})]),
            answer(),
        )
        agent = Agent(
            model=MODEL,
            session=self.session,
            tools=create_memory_tools(session=self.session, store=self.store),
            stream_fn=provider,
        )
        await agent.prompt("Remember I prefer short answers")
        entry = await self.store.read("MEMORY")
        self.assertEqual(entry.sources[0].session_id, "chat")
        self.assertIn(entry.sources[0].event_id, {r["id"] for r in self.session.read_records()})
        self.assertIn(entry.version, json.dumps(self.session.read_records()))
        fresh = LocalSession("new", self.root / "sessions", tenant_id="org", user_id="alice")
        index = await load_memory_index(session=fresh, store=self.store)
        await Agent(
            model=MODEL, session=fresh, messages=[index], stream_fn=FakeProvider(answer())
        ).prompt("hello")
        self.assertIn(entry.version, json.dumps(fresh.read_records()))
        with self.assertRaises(ValueError):
            await load_memory_index(session=fresh, store=self.store)

    async def test_dream_accepts_only_identity_event_before_first_run(self):
        from dataclasses import asdict

        dream = LocalSession(
            "identity-only", self.root / "sessions", tenant_id="org", user_id="alice"
        )
        await dream.commit("session_created", info=asdict(dream.info))
        self.assertEqual(dream.revision, 1)
        self.assertEqual(dream.context_entries(), [])
        provider = FakeProvider(answer("No durable change"))
        await run_memory_dream(
            session=dream,
            store=self.store,
            experiences="Authorized experience",
            sources=self.sources,
            model=MODEL,
            stream_fn=provider,
        )
        with self.assertRaises(ValueError):
            await run_memory_dream(
                session=dream,
                store=self.store,
                experiences="repeat",
                sources=self.sources,
                model=MODEL,
                stream_fn=provider,
            )

    async def test_local_dream_is_explicit_and_uses_supplied_sources(self):
        dream = LocalSession("dream", self.root / "sessions", tenant_id="org", user_id="alice")
        provider = FakeProvider(
            answer(
                calls=[
                    call(
                        "memory_write",
                        args={"key": "MEMORY", "content": "Consolidated", "expected_version": None},
                    )
                ]
            ),
            answer(),
        )
        await run_memory_dream(
            session=dream,
            store=self.store,
            experiences="Alice prefers concise answers",
            sources=self.sources,
            model=MODEL,
            stream_fn=provider,
        )
        self.assertEqual((await self.store.read("MEMORY")).sources, self.sources)
        self.assertEqual(dream.snapshot["status"], "completed")
        with self.assertRaises(ValueError):
            await run_memory_dream(
                session=dream,
                store=self.store,
                experiences="again",
                sources=self.sources,
                model=MODEL,
                stream_fn=provider,
            )

    async def test_write_survives_lost_result_without_automatic_replay(self):
        write = self.store.write

        async def lost_result(*args, **kwargs):
            await write(*args, **kwargs)
            raise ProcessLost()

        agent = Agent(
            session=self.session,
            model=MODEL,
            tools=create_memory_tools(session=self.session, store=self.store),
            stream_fn=FakeProvider(
                answer(
                    calls=[
                        call(
                            "memory_write",
                            args={
                                "key": "MEMORY",
                                "content": "Persisted",
                                "expected_version": None,
                            },
                        )
                    ]
                )
            ),
        )
        with patch.object(self.store, "write", side_effect=lost_result):
            with self.assertRaises(ProcessLost):
                await agent.prompt("remember")
        persisted = await self.store.read("MEMORY")
        restored = LocalSession("chat", self.root / "sessions")
        agent = Agent(
            session=restored,
            model=MODEL,
            tools=create_memory_tools(session=restored, store=self.store),
            stream_fn=FakeProvider(answer()),
        )
        with self.assertRaises(ToolRecoveryRequired):
            await agent.resume()
        self.assertEqual(await self.store.read("MEMORY"), persisted)

    async def test_environment_sources_and_symlink_boundary(self):
        store = LocalMemoryStore(self.session.info, env={"AGENT_MEMORY_DIR": str(self.root)})
        self.assertEqual(store.directory.parent, self.root.resolve())
        first = await store.write("a", "One", sources=self.sources, expected_version=None)
        next_source = MemorySource("next", "event-2")
        second = await store.write(
            "a", "Two", sources=[next_source], expected_version=first.version
        )
        self.assertEqual(second.sources, self.sources + (next_source,))
        outside = self.root / "outside"
        outside.mkdir()
        (store.directory / "escape").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(ValueError):
            await store.write("escape/file", "No", sources=self.sources, expected_version=None)
        self.assertFalse((outside / "file.json").exists())

    async def test_dream_reads_authorized_source_and_journals_evidence(self):
        dream = LocalSession("evidence", self.root / "sessions", tenant_id="org", user_id="alice")
        requested = []

        async def read_source(source):
            requested.append(source)
            self.assertEqual(source, self.sources[0])
            return "Original evidence"

        await run_memory_dream(
            session=dream,
            store=self.store,
            experiences="Review source",
            sources=self.sources,
            source_reader=read_source,
            model=MODEL,
            stream_fn=FakeProvider(
                answer(
                    calls=[
                        call(
                            "read_source",
                            args={
                                "session_id": "history",
                                "event_id": "event-1",
                            },
                        )
                    ]
                ),
                answer(),
            ),
        )
        self.assertEqual(requested, list(self.sources))
        self.assertIn("Original evidence", json.dumps(dream.read_records()))

    async def test_reader_tolerates_file_deleted_after_search_listing(self):
        await self.store.write("a", "Find me", sources=self.sources, expected_version=None)
        original = self.store._read

        def removed(key):
            self.store._path(key).unlink()
            return original(key)

        with patch.object(self.store, "_read", side_effect=removed):
            self.assertEqual(await self.store.search("Find"), [])

    async def test_compaction_summary_is_not_mistaken_for_a_user_source(self):
        agent = Agent(
            session=self.session,
            model=MODEL,
            tools=create_memory_tools(session=self.session, store=self.store),
            stream_fn=FakeProvider(
                answer(),
                answer(
                    calls=[
                        call(
                            "memory_write",
                            args={
                                "key": "MEMORY",
                                "content": "Unverified",
                                "expected_version": None,
                            },
                        )
                    ]
                ),
                answer(),
            ),
        )
        await agent.prompt("Earlier conversation")
        await agent.compact(summary="Unverified summary", first_kept_message_id=None)
        await agent.continue_()
        self.assertIsNone(await self.store.read("MEMORY"))
        result = next(m for m in agent.state.messages if m["role"] == "toolResult")
        self.assertTrue(result["isError"])


@unittest.skipUnless(shutil.which("git"), "Git CLI required")
class GitMemoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        MemoryTests.setUp(self)
        self.remote = self.root / "remote.git"
        self.seed = self.root / "seed"
        self.git(None, "init", "--bare", str(self.remote))
        self.git(None, "init", "-b", "main", str(self.seed))
        (self.seed / "MEMORY.md").write_text("Initial index", encoding="utf-8")
        self.git(self.seed, "add", ".")
        self.git(self.seed, "commit", "-m", "Initialize")
        self.git(self.seed, "remote", "add", "origin", str(self.remote))
        self.git(self.seed, "push", "origin", "main")
        self.git(None, "--git-dir", str(self.remote), "symbolic-ref", "HEAD", "refs/heads/main")
        self.store = GitMemoryStore(self.session.info, str(self.remote))

    def git(self, directory, *args):
        return (
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    *args,
                ],
                cwd=directory,
                check=True,
                capture_output=True,
            )
            .stdout.decode()
            .strip()
        )

    async def test_git_and_draft_list_entries(self):
        await self.store.write(
            "people/alice", "Pricing", sources=self.sources, expected_version=None
        )
        page = await self.store.list_entries(limit=1)
        self.assertEqual([entry.key for entry in page["items"]], ["MEMORY"])
        self.assertEqual(page["next_cursor"], "MEMORY")
        following = await self.store.list_entries(limit=1, after=page["next_cursor"])
        self.assertEqual([entry.key for entry in following["items"]], ["people/alice"])
        self.assertIsNone(following["next_cursor"])
        async with self.store.draft() as draft:
            page = await draft.list_entries(limit=1)
            self.assertEqual(page["next_cursor"], "MEMORY")
            self.assertEqual(
                (await draft.list_entries(after="MEMORY"))["items"][0].key, "people/alice"
            )

    async def test_different_files_merge_same_file_conflicts_and_no_force(self):
        initial = await self.store.read("MEMORY")
        await self.store.write(
            "people/alice", "Pricing", sources=self.sources, expected_version=None
        )
        updated = await self.store.write(
            "MEMORY",
            "Index [[people/alice]]",
            sources=self.sources,
            expected_version=initial.version,
        )
        self.assertIsNotNone(await self.store.read("people/alice"))
        with self.assertRaises(MemoryConflict):
            await self.store.write(
                "MEMORY", "Stale index", sources=self.sources, expected_version=initial.version
            )
        self.assertEqual((await self.store.read("MEMORY")).content, updated.content)
        self.assertEqual(
            len(self.git(None, "--git-dir", str(self.remote), "rev-list", "main").splitlines()), 3
        )

    async def test_draft_publish_is_atomic_and_conflict_leaves_all_files_unchanged(self):
        async with self.store.draft() as draft:
            old = await draft.read("MEMORY")
            await draft.write(
                "MEMORY", "Dream index", sources=self.sources, expected_version=old.version
            )
            await draft.write("new", "New fact", sources=self.sources, expected_version=None)
            self.assertIsNone(await self.store.read("new"))
            await self.store.write(
                "MEMORY", "Foreground", sources=self.sources, expected_version=old.version
            )
            with self.assertRaises(MemoryConflict):
                await draft.publish("dream-operation")
        self.assertIsNone(await self.store.read("new"))
        async with self.store.draft() as draft:
            old = await draft.read("MEMORY")
            await draft.write(
                "MEMORY", "Merged", sources=self.sources, expected_version=old.version
            )
            await draft.write("new", "New fact", sources=self.sources, expected_version=None)
            await draft.publish("dream-operation-2")
        self.assertIsNotNone(await self.store.read("new"))
        self.assertEqual(
            self.git(
                None, "--git-dir", str(self.remote), "show", "--format=", "--name-only", "main"
            ).splitlines(),
            ["MEMORY.md", "new.md"],
        )

    async def test_git_dream_publishes_through_journaled_tool(self):
        dream = LocalSession("git-dream", self.root / "sessions", tenant_id="org", user_id="alice")
        provider = FakeProvider(
            answer(
                calls=[
                    call(
                        "memory_write",
                        args={"key": "new", "content": "Dream fact", "expected_version": None},
                    )
                ]
            ),
            answer(calls=[call("memory_publish", args={})]),
        )
        outcome = await run_memory_dream(
            session=dream,
            store=self.store,
            experiences="new facts",
            sources=self.sources,
            model=MODEL,
            stream_fn=provider,
        )
        self.assertEqual((await self.store.read("new")).version, outcome["version"])
        self.assertIn(outcome["version"], json.dumps(dream.read_records()))

    async def test_concurrent_push_retries_only_for_unrelated_files(self):
        for key in ("unrelated", "MEMORY"):
            with self.subTest(key=key):
                old = await self.store.read("MEMORY")
                original = self.store._git
                pushes = []

                def racing_git(directory, *args, **kwargs):
                    if args[0] == "push":
                        self.assertNotIn("--force", args)
                        if not pushes:
                            self.git(self.seed, "pull", "--ff-only", "origin", "main")
                            (self.seed / f"{key}.md").write_text("Concurrent writer")
                            self.git(self.seed, "add", ".")
                            self.git(self.seed, "commit", "-m", "Race")
                            self.git(self.seed, "push", "origin", "main")
                        pushes.append(True)
                    return original(directory, *args, **kwargs)

                with patch.object(self.store, "_git", side_effect=racing_git):
                    if key == "MEMORY":
                        with self.assertRaises(MemoryConflict):
                            await self.store.write(
                                "MEMORY",
                                "Rejected",
                                sources=self.sources,
                                expected_version=old.version,
                            )
                    else:
                        await self.store.write(
                            "MEMORY", "Accepted", sources=self.sources, expected_version=old.version
                        )
                self.assertEqual(len(pushes), 1 if key == "MEMORY" else 2)
                self.assertEqual((await self.store.read(key)).content, "Concurrent writer")

    async def test_push_acknowledgement_loss_is_confirmed_without_duplicate_commit(self):
        original = self.store._git

        def lost_ack(directory, *args, **kwargs):
            result = original(directory, *args, **kwargs)
            if args[0] == "push":
                return subprocess.CompletedProcess(args, 1, b"", b"connection lost")
            return result

        with patch.object(self.store, "_git", side_effect=lost_ack):
            entry = await self.store.write(
                "new",
                "Once",
                sources=self.sources,
                expected_version=None,
                operation_id="one-operation",
            )
        self.assertEqual((await self.store.read("new")).version, entry.version)
        commits = self.git(None, "--git-dir", str(self.remote), "rev-list", "main").splitlines()
        self.assertEqual(len(commits), 2)

    async def test_unconfirmed_push_pauses_agent_and_never_blindly_retries(self):
        original = self.store._git
        pushes = []

        def lost_network(directory, *args, **kwargs):
            if args[0] == "fetch":
                raise GitMemoryError("Unavailable")
            result = original(directory, *args, **kwargs)
            if args[0] == "push":
                pushes.append(True)
                return subprocess.CompletedProcess(args, 1, b"", b"lost")
            return result

        agent = Agent(
            session=self.session,
            model=MODEL,
            tools=create_memory_tools(session=self.session, store=self.store),
            stream_fn=FakeProvider(
                answer(
                    calls=[
                        call(
                            "memory_write",
                            args={
                                "key": "new",
                                "content": "Maybe",
                                "expected_version": None,
                            },
                        )
                    ]
                )
            ),
        )
        with patch.object(self.store, "_git", side_effect=lost_network):
            with self.assertRaises(ToolRecoveryRequired):
                await agent.prompt("remember")
        self.assertEqual(len(pushes), 1)
        self.assertEqual((await self.store.read("new")).content, "Maybe")
        with self.assertRaises(ToolRecoveryRequired):
            await agent.resume()

    async def test_unpublished_dream_discards_changes_and_timeout_redacts_remote(self):
        dream = LocalSession("discard", self.root / "sessions", tenant_id="org", user_id="alice")
        provider = FakeProvider(
            answer(
                calls=[
                    call(
                        "memory_write",
                        args={
                            "key": "discard",
                            "content": "Draft only",
                            "expected_version": None,
                        },
                    )
                ]
            ),
            answer(),
        )
        with self.assertRaisesRegex(RuntimeError, "not published"):
            await run_memory_dream(
                session=dream,
                store=self.store,
                experiences="facts",
                sources=self.sources,
                model=MODEL,
                stream_fn=provider,
            )
        self.assertIsNone(await self.store.read("discard"))
        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired("git clone https://secret@example.invalid", 1),
        ):
            with self.assertRaises(GitMemoryError) as caught:
                await self.store.read("MEMORY")
        self.assertNotIn("secret", str(caught.exception))

    async def test_bad_versions_symlinks_and_absent_delete(self):
        with self.assertRaises(MemoryConflict):
            await self.store.write("MEMORY", "No", sources=self.sources, expected_version="--help")
        await self.store.delete("absent", expected_version=None)
        (self.seed / "link.md").symlink_to("MEMORY.md")
        self.git(self.seed, "add", ".")
        self.git(self.seed, "commit", "-m", "Symlink")
        self.git(self.seed, "push", "origin", "main")
        with self.assertRaises(GitMemoryError):
            await self.store.read("link")
