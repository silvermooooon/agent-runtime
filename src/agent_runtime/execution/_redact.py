"""Literal UTF-8 credential masking before output buffering. Not an exfiltration boundary."""

import json
import re


class Redactor:
    def __init__(self, values):
        forms = {
            form.encode()
            for value in values
            if value
            for form in (
                value,
                json.dumps(value)[1:-1],
                json.dumps(value, ensure_ascii=False)[1:-1],
            )
        }
        secrets = sorted(forms, key=len, reverse=True)
        self.pattern = (
            re.compile(b"|".join(re.escape(value) for value in secrets)) if secrets else None
        )
        self.keep = max(map(len, secrets), default=1) - 1
        self.pending = b""

    def feed(self, data, *, final=False):
        if self.pattern is None:
            return data
        self.pending += data
        limit = len(self.pending) if final else max(0, len(self.pending) - self.keep)
        result, end = [], 0
        for match in self.pattern.finditer(self.pending):
            if match.start() >= limit:
                break
            result.extend((self.pending[end : match.start()], b"[REDACTED]"))
            end = match.end()
        end_limit = max(end, limit)
        result.append(self.pending[end:end_limit])
        self.pending = self.pending[end_limit:]
        return b"".join(result)

    def text(self, value):
        if self.pattern is None:
            return value
        return self.pattern.sub(b"[REDACTED]", value.encode()).decode()
