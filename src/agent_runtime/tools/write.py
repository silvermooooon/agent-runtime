"""Python port of pi coding-agent's write tool."""

from pathlib import Path

from ..types import AbortSignal, AgentTool, AgentToolResult
from ._paths import resolve_path
from .operations import LocalFileOperations, WriteOperations, file_mutation, settle


def create_write_tool(cwd: str | Path, *, operations: WriteOperations | None = None) -> AgentTool:
    cwd = str(Path(cwd).expanduser().absolute())
    ops = operations if operations is not None else LocalFileOperations()

    async def execute(call_id, args, signal=None, on_update=None):
        signal = signal or AbortSignal()
        path = resolve_path(args["path"], cwd)
        async with file_mutation(path, operations):
            signal.throw_if_aborted()
            await settle(ops.mkdir(str(Path(path).parent)))
            signal.throw_if_aborted()
            await settle(ops.write_file(path, args["content"]))
            signal.throw_if_aborted()
        return AgentToolResult([{"type": "text", "text": f"Successfully wrote to {args['path']}"}])

    return AgentTool(
        "write",
        "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. "
        "Automatically creates parent directories. Use for new files or complete rewrites.",
        {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "File path, relative or absolute"},
                "content": {"type": "string", "description": "Content to write"},
            },
            "required": ["path", "content"],
        },
        execute,
        label="write",
        replay="never",
        version="pi-tools-1",
    )
