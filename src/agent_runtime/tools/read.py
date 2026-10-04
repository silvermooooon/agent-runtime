"""Python port of pi coding-agent's text/image read tool."""

import asyncio
import base64
import shlex
from pathlib import Path

from ..types import AbortSignal, AgentTool, AgentToolResult
from ._images import ImageResizeOptions, detect_image, process_image
from ._paths import resolve_path, resolve_read_path
from ._truncate import DEFAULT_MAX_BYTES, format_size, truncate
from .operations import LocalFileOperations, ReadOperations


def create_read_tool(
    cwd: str | Path,
    *,
    operations: ReadOperations | None = None,
    auto_resize_images: bool = True,
    resize_options: ImageResizeOptions | None = None,
) -> AgentTool:
    cwd = str(Path(cwd).expanduser().absolute())
    ops = operations if operations is not None else LocalFileOperations()

    async def execute(call_id, args, signal=None, on_update=None):
        signal = signal or AbortSignal()
        signal.throw_if_aborted()
        path = (resolve_read_path if operations is None else resolve_path)(args["path"], cwd)
        await ops.access(path)
        signal.throw_if_aborted()
        data = await ops.read_file(path)
        signal.throw_if_aborted()
        mime = detect_image(data[:4100])
        if mime:
            processed = await asyncio.to_thread(
                process_image,
                data,
                mime,
                auto_resize=auto_resize_images,
                options=resize_options,
            )
            signal.throw_if_aborted()
            if processed is None:
                return AgentToolResult(
                    [
                        {
                            "type": "text",
                            "text": f"Read image file [{mime}]\n"
                            "[Image omitted: could not be converted or resized "
                            "below the inline image limit.]",
                        }
                    ]
                )
            image, mime, hints = processed
            return AgentToolResult(
                [
                    {"type": "text", "text": "\n".join([f"Read image file [{mime}]", *hints])},
                    {
                        "type": "image",
                        "mimeType": mime,
                        "data": base64.b64encode(image).decode("ascii"),
                    },
                ]
            )
        lines = data.decode("utf-8", errors="replace").split("\n")
        start = max(0, int(args.get("offset", 1)) - 1)
        if start >= len(lines):
            raise ValueError(
                f"Offset {args.get('offset')} is beyond end of file ({len(lines)} lines total)"
            )
        end = min(len(lines), start + int(args["limit"])) if "limit" in args else len(lines)
        result = truncate("\n".join(lines[start:end]))
        output, details = result["content"], None
        if result["firstLineExceedsLimit"]:
            size = format_size(len(lines[start].encode("utf-8")))
            output = (
                f"[Line {start + 1} is {size}, exceeds {format_size(DEFAULT_MAX_BYTES)} limit. "
                f"Use bash: sed -n '{start + 1}p' {shlex.quote(args['path'])} "
                f"| head -c {DEFAULT_MAX_BYTES}]"
            )
            details = {"truncation": result}
        elif result["truncated"]:
            last = start + result["outputLines"]
            size = (
                f" ({format_size(DEFAULT_MAX_BYTES)} limit)"
                if result["truncatedBy"] == "bytes"
                else ""
            )
            output += (
                f"\n\n[Showing lines {start + 1}-{last} of {len(lines)}{size}. "
                f"Use offset={last + 1} to continue.]"
            )
            details = {"truncation": result}
        elif end < len(lines):
            output += (
                f"\n\n[{len(lines) - end} more lines in file. Use offset={end + 1} to continue.]"
            )
        return AgentToolResult([{"type": "text", "text": output}], details=details)

    return AgentTool(
        "read",
        "Read text files or images (jpg, png, gif, webp, bmp). Images are attachments. "
        "Text output is truncated to 2000 lines or 50KB. Use offset/limit and continue with "
        "offset until complete. Use read to examine files instead of cat or sed.",
        {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "File path, relative or absolute"},
                "offset": {"type": "integer", "minimum": 1, "description": "First line, 1-indexed"},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum number of lines",
                },
            },
            "required": ["path"],
        },
        execute,
        label="read",
        replay="safe",
        version="pi-tools-1",
    )
