"""Bounded streaming UTF-8 tail and lazy full-output spool, ported from pi."""

import codecs
import tempfile
from pathlib import Path

from ._truncate import DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES, truncate


class OutputAccumulator:
    def __init__(self):
        self.decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self.raw = bytearray()
        self.tail = ""
        self.boundary = True
        self.raw_bytes = self.total_bytes = self.newlines = self.total_lines = 0
        self.last_line_bytes = 0
        self.finished = False
        self.file = None
        self.path = None

    def _decoded(self, text):
        self.total_bytes += len(text.encode("utf-8"))
        self.tail += text
        if "\n" in text:
            self.newlines += text.count("\n")
            self.last_line_bytes = len(text.rsplit("\n", 1)[-1].encode("utf-8"))
        else:
            self.last_line_bytes += len(text.encode("utf-8"))
        self.total_lines = self.newlines + bool(self.last_line_bytes)
        data = self.tail.encode("utf-8")
        if len(data) > DEFAULT_MAX_BYTES * 4:
            start = len(data) - DEFAULT_MAX_BYTES * 2
            while start < len(data) and data[start] & 0xC0 == 0x80:
                start += 1
            self.boundary = data[start - 1] == 10
            self.tail = data[start:].decode("utf-8")

    def _needs_file(self):
        return (
            self.raw_bytes > DEFAULT_MAX_BYTES
            or self.total_bytes > DEFAULT_MAX_BYTES
            or self.total_lines > DEFAULT_MAX_LINES
        )

    def _spool(self):
        if self.path is not None:
            return
        self.file = tempfile.NamedTemporaryFile(
            prefix="agent-runtime-bash-", suffix=".log", delete=False
        )
        self.path = self.file.name
        self.file.write(self.raw)
        self.raw.clear()

    def append(self, data):
        if self.finished:
            return
        self.raw_bytes += len(data)
        self._decoded(self.decoder.decode(data))
        if self.file is not None or self._needs_file():
            self._spool()
            self.file.write(data)
        else:
            self.raw.extend(data)

    def snapshot(self):
        text = self.tail
        if not self.boundary and "\n" in text:
            text = text.split("\n", 1)[1]
        result = truncate(text, tail=True)
        truncated = self.total_lines > DEFAULT_MAX_LINES or self.total_bytes > DEFAULT_MAX_BYTES
        result.update(
            truncated=truncated,
            truncatedBy=(
                result["truncatedBy"]
                or ("bytes" if self.total_bytes > DEFAULT_MAX_BYTES else "lines")
            )
            if truncated
            else None,
            totalBytes=self.total_bytes,
            totalLines=self.total_lines,
        )
        if truncated:
            self._spool()
        if self.file is not None:
            self.file.flush()
        return result

    def finish(self):
        if not self.finished:
            self.finished = True
            self._decoded(self.decoder.decode(b"", final=True))
            if self._needs_file():
                self._spool()

    def close(self):
        if self.file is not None:
            self.file.close()
            self.file = None

    def full_output(self, max_bytes=1024 * 1024):
        if self.path is None:
            return self.raw.decode("utf-8", errors="replace"), False
        with Path(self.path).open("rb") as file:
            size = file.seek(0, 2)
            file.seek(0)
            if size <= max_bytes:
                return file.read().decode("utf-8", errors="replace"), False
            head_size = max_bytes // 2
            head = file.read(head_size).decode("utf-8", errors="ignore")
            file.seek(size - (max_bytes - head_size))
            tail = file.read().decode("utf-8", errors="ignore")
            return f"{head}\n\n[... {size - max_bytes} bytes omitted ...]\n\n{tail}", True
