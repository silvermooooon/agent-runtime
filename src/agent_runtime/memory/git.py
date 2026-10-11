"""Optional Git CLI backend. Each operation uses an isolated clone; never force-pushes."""

import asyncio
import json
import os
import re
import subprocess
import tempfile
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from pathlib import Path

from ..tools.operations import settle
from ..types import timestamp
from .store import (
    MemoryConflict,
    MemoryEntry,
    MemorySource,
    MemoryStore,
    search_entries,
    validate_key,
)

HEADER = "<!-- agent-runtime-memory "


class GitMemoryError(RuntimeError):
    pass


class MemoryPublishUnknown(GitMemoryError):
    """Publication could not be confirmed; reconcile the operation before retrying."""


class GitMemoryStore(MemoryStore):
    def __init__(self, identity, remote, *, branch="main", timeout=60, publish_attempts=3):
        super().__init__(identity)
        remote = str(remote)
        if not remote or remote.startswith("-") or not branch or branch.startswith("-"):
            raise ValueError("Git memory requires a remote and branch")
        if timeout <= 0 or publish_attempts < 1:
            raise ValueError("Git timeout and publish_attempts must be positive")
        self.remote, self.branch = str(remote), branch
        self.timeout, self.publish_attempts = timeout, publish_attempts

    def _git(self, directory, *args, check=True):
        # No shell, no model-controlled remote, and no repository hooks on commits.
        try:
            result = subprocess.run(
                [
                    "git",
                    "--literal-pathspecs",
                    "-c",
                    "core.hooksPath=/dev/null",
                    "-c",
                    "commit.gpgsign=false",
                    "-c",
                    "protocol.ext.allow=never",
                    "-c",
                    "user.name=Agent Memory",
                    "-c",
                    "user.email=agent-memory@localhost",
                    *args,
                ],
                cwd=directory,
                env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
                capture_output=True,
                timeout=self.timeout,
            )
        except subprocess.TimeoutExpired:
            raise GitMemoryError(f"Git memory {args[0]} timed out") from None
        if check and result.returncode:
            # stderr may include remote credentials. Keep it out of tool results/session logs.
            raise GitMemoryError(f"Git memory {args[0]} failed (exit {result.returncode})")
        return result

    def _clone(self, directory):
        self._git(
            None,
            "clone",
            "--quiet",
            "--no-checkout",
            "--single-branch",
            "--branch",
            self.branch,
            "--",
            self.remote,
            str(directory),
        )
        return self._head(directory)

    def _head(self, directory):
        return (
            self._git(directory, "rev-parse", f"refs/remotes/origin/{self.branch}")
            .stdout.decode()
            .strip()
        )

    @contextmanager
    def _checkout(self):
        with tempfile.TemporaryDirectory(prefix="agent-memory-") as temporary:
            directory = Path(temporary) / "repo"
            yield directory, self._clone(directory)

    @staticmethod
    def _name(key):
        return validate_key(key) + ".md"

    def _blob(self, directory, revision, key):
        path = self._name(key)
        result = self._git(directory, "ls-tree", "-z", revision, "--", path)
        if not result.stdout:
            return None
        mode, kind, oid = result.stdout.split(b"\t", 1)[0].split()
        if mode not in (b"100644", b"100755") or kind != b"blob":
            raise GitMemoryError("Memory must be a regular text file")
        return oid.decode()

    def _read(self, directory, revision, key):
        blob = self._blob(directory, revision, key)
        if blob is None:
            return None
        text = self._git(directory, "cat-file", "blob", blob).stdout.decode("utf-8")
        sources, updated_at = (), None
        if text.startswith(HEADER):
            first, _, text = text.partition("\n")
            if not first.endswith(" -->"):
                raise GitMemoryError("Invalid memory metadata")
            data = json.loads(first[len(HEADER) : -4])
            sources = tuple(MemorySource(**s) for s in data["sources"])
            updated_at = data["updated_at"]
        return MemoryEntry(key, text, revision, sources, updated_at)

    def _entries(self, directory, revision):
        paths = self._git(directory, "ls-tree", "-r", "--name-only", "-z", revision).stdout.split(
            b"\0"
        )
        return {
            key: self._read(directory, revision, key)
            for path in paths
            if path.endswith(b".md")
            for key in [path.decode("utf-8")[:-3]]
            if not any(p.startswith(".") for p in key.split("/"))
        }

    async def read(self, key):
        def read():
            with self._checkout() as (directory, head):
                return self._read(directory, head, key)

        return await settle(asyncio.to_thread(read))

    async def search(self, query, limit=10):
        def search():
            with self._checkout() as (directory, head):
                return search_entries(self._entries(directory, head).values(), query, limit)

        return await settle(asyncio.to_thread(search))

    def _publish(self, directory, changes, expected, operation_id):
        """Compare touched files against their read commits; publish one descendant commit."""
        if not changes:
            return self._head(directory)
        for _ in range(self.publish_attempts):
            head = self._head(directory)
            for key in changes:
                previous = expected[key]
                if previous is not None:
                    if not re.fullmatch(r"[0-9a-f]{40,64}", previous):
                        raise MemoryConflict(key, self._read(directory, head, key))
                    # The read commit must belong to this repo's authoritative history.
                    result = self._git(
                        directory, "merge-base", "--is-ancestor", previous, head, check=False
                    )
                    if result.returncode:
                        raise MemoryConflict(key, self._read(directory, head, key))
                old = self._blob(directory, previous, key) if previous is not None else None
                if self._blob(directory, head, key) != old:
                    raise MemoryConflict(key, self._read(directory, head, key))
            self._git(directory, "checkout", "--quiet", "--detach", head)
            for key, entry in changes.items():
                path = directory / self._name(key)
                if not path.resolve().is_relative_to(directory.resolve()):
                    raise GitMemoryError("Memory path escapes repository")
                if entry is None:
                    if not path.exists():
                        continue
                    path.unlink()
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    data = entry.to_dict()
                    metadata = json.dumps(
                        {"sources": data["sources"], "updated_at": entry.updated_at},
                        ensure_ascii=True,
                    )
                    path.write_text(HEADER + metadata + " -->\n" + entry.content, encoding="utf-8")
                self._git(directory, "add", "-A", "--", self._name(key))
            if self._git(directory, "diff", "--cached", "--quiet", check=False).returncode == 0:
                return head
            message = "Update agent memory"
            if operation_id:
                message += "\n\nMemory-Operation: " + operation_id
            self._git(directory, "commit", "--quiet", "-m", message)
            commit = self._git(directory, "rev-parse", "HEAD").stdout.decode().strip()
            try:
                pushed = self._git(
                    directory,
                    "push",
                    "--quiet",
                    "origin",
                    f"HEAD:refs/heads/{self.branch}",
                    check=False,
                )
            except GitMemoryError:
                pushed = None
            if pushed is not None and pushed.returncode == 0:
                return commit
            try:
                self._git(directory, "fetch", "--quiet", "origin", self.branch)
                latest = self._head(directory)
                if (
                    self._git(
                        directory, "merge-base", "--is-ancestor", commit, latest, check=False
                    ).returncode
                    == 0
                ):
                    return commit  # Push succeeded but its acknowledgement was lost.
            except GitMemoryError:
                raise MemoryPublishUnknown(
                    "Cannot confirm memory publication; reconcile before retry"
                ) from None
            if latest == head:
                raise GitMemoryError("Memory push rejected; authoritative branch was not updated")
            self._git(directory, "reset", "--hard", latest)
        raise GitMemoryError("Memory publication remained busy; retry from a fresh read")

    async def write(self, key, content, *, sources, expected_version, operation_id=None):
        validate_key(key)
        if not isinstance(content, str):
            raise ValueError("Memory content must be text")
        entry = MemoryEntry(key, content, "", tuple(sources), timestamp())

        def write():
            with self._checkout() as (directory, head):
                current = self._read(directory, head, key)
                refs = tuple(dict.fromkeys((current.sources if current else ()) + entry.sources))
                if not refs:
                    raise ValueError("Memory writes require sources")
                value = replace(entry, sources=refs)
                version = self._publish(
                    directory, {key: value}, {key: expected_version}, operation_id
                )
                return replace(value, version=version)

        return await settle(asyncio.to_thread(write))

    async def delete(self, key, *, expected_version, operation_id=None):
        def delete():
            with self._checkout() as (directory, _):
                self._publish(directory, {key: None}, {key: expected_version}, operation_id)

        await settle(asyncio.to_thread(delete))

    @asynccontextmanager
    async def draft(self):
        """A disposable dream workspace. Publication is explicit, never on context exit."""
        with tempfile.TemporaryDirectory(prefix="agent-memory-dream-") as temporary:
            directory = Path(temporary) / "repo"
            head = await settle(asyncio.to_thread(self._clone, directory))
            entries = await settle(asyncio.to_thread(self._entries, directory, head))
            yield GitMemoryDraft(self, directory, head, entries)


class GitMemoryDraft(MemoryStore):
    staged = True

    def __init__(self, store, directory, head, entries):
        super().__init__(store.identity)
        self.store, self.directory, self.head = store, directory, head
        self.entries, self.changes = entries, {}
        self.expected = {key: entry.version for key, entry in entries.items()}
        self.published_version = None

    async def read(self, key):
        return self.entries.get(validate_key(key))

    async def search(self, query, limit=10):
        return search_entries(self.entries.values(), query, limit)

    async def write(self, key, content, *, sources, expected_version, operation_id=None):
        from .store import check_version

        if self.published_version:
            raise GitMemoryError("Draft already published")
        validate_key(key)
        if not isinstance(content, str):
            raise ValueError("Memory content must be text")
        check_version(key, self.entries.get(key), expected_version)
        current = self.entries.get(key)
        refs = tuple(dict.fromkeys((current.sources if current else ()) + tuple(sources)))
        if not refs:
            raise ValueError("Memory writes require sources")
        entry = MemoryEntry(key, content, self.head, refs, timestamp())
        self.expected.setdefault(key, None)
        self.entries[key] = self.changes[key] = entry
        return entry

    async def delete(self, key, *, expected_version, operation_id=None):
        from .store import check_version

        if self.published_version:
            raise GitMemoryError("Draft already published")
        validate_key(key)
        check_version(key, self.entries.get(key), expected_version)
        self.expected.setdefault(key, None)
        self.changes[key] = None
        self.entries.pop(key, None)

    async def publish(self, operation_id):
        if self.published_version:
            return self.published_version
        # Refresh before comparing: the foreground may have written during the dream.
        await settle(
            asyncio.to_thread(
                self.store._git, self.directory, "fetch", "--quiet", "origin", self.store.branch
            )
        )
        self.published_version = await settle(
            asyncio.to_thread(
                self.store._publish, self.directory, self.changes, self.expected, operation_id
            )
        )
        return self.published_version
