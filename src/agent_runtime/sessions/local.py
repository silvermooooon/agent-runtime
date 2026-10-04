"""Per-session JSONL journal plus ephemeral in-memory display state."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import tempfile
from copy import deepcopy
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from .base import Session, SessionBusyError, SessionError, replay_records

try:
    import fcntl
except ImportError:  # Package remains importable; file-backed ownership needs POSIX.
    fcntl = None


def encode(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


class LocalSession(Session):
    """directory=None keeps everything in-process; the default directory enables persistence.

    File-backed sessions have one nonblocking OS writer guard per session, released after each
    Agent run and automatically on process death. This is local-filesystem ownership, not a
    cluster lease; network filesystems and multiple hosts require a future backend/coordinator.
    """

    def __init__(self, session_id=None, directory=".agent-runtime/sessions"):
        super().__init__(session_id)
        if not isinstance(self.session_id, str) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,128}", self.session_id
        ):
            raise ValueError(
                "session_id must be 1–128 ASCII letters, digits, underscores or hyphens"
            )
        self.directory = Path(directory) / self.session_id if directory is not None else None
        self.path = self.directory / "events.jsonl" if self.directory is not None else None
        self.durable = self.path is not None
        self._writer = None
        self._records = []
        self._reload()

    def _read_file(self):
        raw = self.path.read_bytes() if self.path and self.path.exists() else b""
        boundary = raw.rfind(b"\n") + 1
        records = []
        for seq, line in enumerate(raw[:boundary].splitlines()):
            try:
                envelope = json.loads(line)
                checksum = envelope.pop("checksum")
                if envelope["format"] != 1 or envelope["session_id"] != self.session_id:
                    raise ValueError("Unsupported format or session identity")
                if envelope["seq"] != seq:
                    raise ValueError("Non-contiguous event sequence")
                if hashlib.sha256(encode(envelope)).hexdigest() != checksum:
                    raise ValueError("Checksum mismatch")
                record = envelope["record"]
                if record["seq"] != seq:
                    raise ValueError("Record/envelope sequence mismatch")
                # Existing logs gain stable IDs without changing any stored bytes/checksums.
                record.setdefault(
                    "id", uuid5(NAMESPACE_URL, f"agent-runtime:{self.session_id}:{seq}").hex
                )
                if not isinstance(record["id"], str) or not record["id"] or "/" in record["id"]:
                    raise ValueError("Invalid event ID")
                records.append(record)
            except (TypeError, ValueError, KeyError) as error:
                raise SessionError(f"Corrupt session record {seq}: {error}") from error
        return records, raw, boundary

    def _reload(self, repair=False):
        if self.path is None:
            return
        records, raw, boundary = self._read_file()
        state = replay_records(records)
        if repair and boundary != len(raw):
            with self.path.open("r+b") as file:
                file.truncate(boundary)
                file.flush()
                os.fsync(file.fileno())
        self._state, self._seq = state, len(records)
        # File-backed sessions retain the projection, not a duplicate full event log in RAM.
        self._records = []

    def read_records(self, after_seq=-1):
        """Read durable logical events. Live deltas are only available through `live`."""
        if self.path is None:
            return json.loads(json.dumps(self._records[after_seq + 1 :]))
        records, _, _ = self._read_file()
        return records[after_seq + 1 :]

    async def fork(self, event_id, *, session_id=None):
        """Create an independent session with the complete journal through event_id."""
        target = LocalSession(
            session_id, self.directory.parent if self.directory is not None else None
        )
        return await self.fork_into(event_id, target)

    async def _import_records(self, records):
        if self.path is None:
            self._records = deepcopy(records)
            return

        def write():
            # Publish the whole prefix at once. A crash cannot expose half a fork.
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=self.directory, delete=False) as file:
                    temporary = Path(file.name)
                    for seq, record in enumerate(records):
                        envelope = {
                            "format": 1,
                            "session_id": self.session_id,
                            "seq": seq,
                            "record": record,
                        }
                        envelope["checksum"] = hashlib.sha256(encode(envelope)).hexdigest()
                        file.write(encode(envelope) + b"\n")
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(temporary, self.path)
                for directory in (self.directory, self.directory.parent):
                    descriptor = os.open(directory, os.O_RDONLY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)

        await asyncio.to_thread(write)

    async def _acquire(self):
        if self.directory is None:
            return
        if fcntl is None:
            raise SessionError("File-backed LocalSession currently requires POSIX flock")
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        guard = self.directory / "writer.lock"
        file = os.fdopen(os.open(guard, os.O_CREAT | os.O_RDWR, 0o600), "rb+")
        try:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._reload(repair=True)
        except BlockingIOError:
            file.close()
            raise SessionBusyError("Another process is writing this local session") from None
        except BaseException:
            file.close()
            raise
        self._writer = file

    async def _release(self):
        if self._writer:
            self._writer.close()
            self._writer = None

    async def _persist(self, seq, record):
        if self.path is None:
            self._records.append(record)
            return
        envelope = {"format": 1, "session_id": self.session_id, "seq": seq, "record": record}
        envelope["checksum"] = hashlib.sha256(encode(envelope)).hexdigest()
        line = encode(envelope) + b"\n"

        def write():
            new_file = not self.path.exists()
            descriptor = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "ab") as file:
                file.write(line)
                file.flush()
                os.fsync(file.fileno())
            if new_file:
                for directory in (self.directory, self.directory.parent):
                    descriptor = os.open(directory, os.O_RDONLY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)

        await asyncio.to_thread(write)
