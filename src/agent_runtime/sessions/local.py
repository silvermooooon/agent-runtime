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

from .base import Session, SessionError, replay_records


def encode(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


class LocalSession(Session):
    """directory=None keeps everything in-process; the default directory enables persistence.

    The host guarantees one execution per session and reopens it on worker handoff.
    Journal reads do not change the file; the next append repairs an incomplete tail.
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
        self._tail_boundary = None
        self._records = []
        self._read_cursor = None
        self._reload()

    def _read_file(self, after_seq=-1):
        if self.path is None or not self.path.exists():
            self._read_cursor = None
            return [], 0, 0
        records, seq, boundary = [], 0, 0
        with self.path.open("rb") as file:
            stat = os.fstat(file.fileno())
            identity = (stat.st_dev, stat.st_ino)
            cursor = self._read_cursor
            # Journals are append-only. A cursor accelerates forward polling; full
            # history, reopen, replacement and truncation still validate the full log.
            if cursor and after_seq >= 0:
                previous_id, size, modified, count, offset = cursor
                if (
                    identity == previous_id
                    and after_seq >= count - 1
                    and (
                        stat.st_size > size
                        or (stat.st_size == size and stat.st_mtime_ns == modified)
                    )
                ):
                    seq, boundary = count, offset
                    file.seek(offset)
            for line in file:
                if not line.endswith(b"\n"):
                    break  # An incomplete tail will be reread on the next poll.
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
                    record.setdefault(
                        "id", uuid5(NAMESPACE_URL, f"agent-runtime:{self.session_id}:{seq}").hex
                    )
                    if not isinstance(record["id"], str) or not record["id"] or "/" in record["id"]:
                        raise ValueError("Invalid event ID")
                    if seq > after_seq:
                        records.append(record)
                except (TypeError, ValueError, KeyError) as error:
                    raise SessionError(f"Corrupt session record {seq}: {error}") from error
                seq += 1
                boundary = file.tell()
            self._read_cursor = (identity, stat.st_size, stat.st_mtime_ns, seq, boundary)
        return records, stat.st_size, boundary

    def _reload(self):
        if self.path is None:
            return
        records, size, boundary = self._read_file()
        state = replay_records(records)
        self._tail_boundary = boundary if boundary != size else None
        self._state, self._seq = state, len(records)
        # File-backed sessions retain the projection, not a duplicate full event log in RAM.
        self._records = []

    def read_records(self, after_seq=-1):
        """Read durable logical events. Live deltas are only available through `live`."""
        if self.path is None:
            return json.loads(json.dumps(self._records[after_seq + 1 :]))
        records, _, _ = self._read_file(after_seq)
        return records

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
            self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
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
        self._tail_boundary = None

    async def _persist(self, seq, record):
        if self.path is None:
            self._records.append(record)
            return
        envelope = {"format": 1, "session_id": self.session_id, "seq": seq, "record": record}
        envelope["checksum"] = hashlib.sha256(encode(envelope)).hexdigest()
        line = encode(envelope) + b"\n"

        def write():
            self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            if self._tail_boundary is not None:
                with self.path.open("r+b") as file:
                    file.truncate(self._tail_boundary)
                self._tail_boundary = None
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
