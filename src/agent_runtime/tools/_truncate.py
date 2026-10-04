"""pi's line/UTF-8 byte limits, keeping a head for read and a tail for bash."""

DEFAULT_MAX_LINES = 2000
DEFAULT_MAX_BYTES = 50 * 1024


def format_size(size):
    if size < 1024:
        return f"{size}B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f}KB"
    return f"{size / (1024 * 1024):.1f}MB"


def truncate(content, *, tail=False, max_lines=DEFAULT_MAX_LINES, max_bytes=DEFAULT_MAX_BYTES):
    lines = content.split("\n") if content else []
    if content.endswith("\n"):
        lines.pop()
    result = {
        "content": content,
        "truncated": False,
        "truncatedBy": None,
        "totalLines": len(lines),
        "totalBytes": len(content.encode("utf-8")),
        "outputLines": len(lines),
        "outputBytes": len(content.encode("utf-8")),
        "lastLinePartial": False,
        "firstLineExceedsLimit": False,
        "maxLines": max_lines,
        "maxBytes": max_bytes,
    }
    if len(lines) <= max_lines and result["totalBytes"] <= max_bytes:
        return result
    selected, size, reason = [], 0, "lines"
    if not tail and len(lines[0].encode("utf-8")) > max_bytes:
        reason = "bytes"
        result["firstLineExceedsLimit"] = True
    else:
        for line in reversed(lines) if tail else lines:
            if len(selected) >= max_lines:
                break
            encoded = line.encode("utf-8")
            count = len(encoded) + bool(selected)
            if size + count > max_bytes:
                reason = "bytes"
                if tail and not selected:
                    selected.append(encoded[-max_bytes:].decode("utf-8", errors="ignore"))
                    result["lastLinePartial"] = True
                break
            selected.append(line)
            size += count
        if len(selected) >= max_lines and size <= max_bytes:
            reason = "lines"
    text = "\n".join(reversed(selected) if tail else selected)
    result.update(
        content=text,
        truncated=True,
        truncatedBy=reason,
        outputLines=len(selected),
        outputBytes=len(text.encode("utf-8")),
    )
    return result
