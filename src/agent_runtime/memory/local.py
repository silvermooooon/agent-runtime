"""Minimal JSON/text memory. The host serializes writes per user, including dreams."""

import asyncio
import hashlib
import json
import os
import tempfile
from pathlib import Path
from uuid import uuid4

from ..tools.operations import settle
from ..types import timestamp
from .store import (
    MemoryEntry,
    MemorySource,
    MemoryStore,
    check_version,
    search_entries,
    validate_key,
)


class LocalMemoryStore(MemoryStore):
    def __init__(self, identity, directory=None, *, env=None):
        super().__init__(identity)
        env = os.environ if env is None else env
        root = directory or env.get("AGENT_MEMORY_DIR") or ".agent-runtime/memory"
        scope = json.dumps([identity.tenant_id, identity.user_id], ensure_ascii=True)
        self.directory = (
            Path(root).expanduser().resolve() / hashlib.sha256(scope.encode()).hexdigest()
        )

    def _path(self, key):
        path = self.directory / (validate_key(key) + ".json")
        if not path.resolve().is_relative_to(self.directory):
            raise ValueError("Memory path escapes its user directory")
        return path

    def _read(self, key):
        path = self._path(key)
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        return MemoryEntry(
            key,
            value["content"],
            value["version"],
            tuple(MemorySource(**s) for s in value["sources"]),
            value["updated_at"],
        )

    async def read(self, key):
        return await settle(asyncio.to_thread(self._read, key))

    async def search(self, query, limit=10):
        def search():
            entries = []
            for path in sorted(self.directory.rglob("*.json")):
                key = path.relative_to(self.directory).as_posix()[:-5]
                entry = self._read(key)
                if entry is not None:
                    entries.append(entry)
            return search_entries(entries, query, limit)

        return await settle(asyncio.to_thread(search))

    async def write(self, key, content, *, sources, expected_version, operation_id=None):
        def write():
            current = self._read(key)
            check_version(key, current, expected_version)
            if not isinstance(content, str):
                raise ValueError("Memory content must be text")
            refs = tuple(dict.fromkeys((current.sources if current else ()) + tuple(sources)))
            if not refs:
                raise ValueError("Memory writes require sources")
            entry = MemoryEntry(key, content, uuid4().hex, refs, timestamp())
            path = self._path(key)
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as file:
                    temporary = Path(file.name)
                    file.write(json.dumps(entry.to_dict(), ensure_ascii=True).encode())
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(temporary, path)
                self._sync_directory(path.parent)
            finally:
                if temporary:
                    temporary.unlink(missing_ok=True)
            return entry

        return await settle(asyncio.to_thread(write))

    @staticmethod
    def _sync_directory(directory):
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    async def delete(self, key, *, expected_version, operation_id=None):
        def delete():
            check_version(key, self._read(key), expected_version)
            path = self._path(key)
            if path.exists():
                path.unlink()
                self._sync_directory(path.parent)

        await settle(asyncio.to_thread(delete))
