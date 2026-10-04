"""Python port of pi coding-agent's multi-replacement edit tool."""

import json
from copy import deepcopy
from pathlib import Path

from ..types import AbortSignal, AgentTool, AgentToolResult
from ._edit_diff import apply_edits, generate_diff, normalize_lf
from ._paths import resolve_path
from .operations import EditOperations, LocalFileOperations, file_mutation, settle


def prepare_edit_arguments(args):
    if not isinstance(args, dict):
        return args
    args = deepcopy(args)
    edits = args.get("edits")
    if isinstance(edits, str):
        try:
            edits = json.loads(edits)
        except ValueError:
            pass
    if isinstance(edits, dict) and all(
        isinstance(edits.get(k), str) for k in ("oldText", "newText")
    ):
        edits = [edits]
    if isinstance(edits, list):
        args["edits"] = edits
    if all(isinstance(args.get(k), str) for k in ("oldText", "newText")):
        args["edits"] = (edits if isinstance(edits, list) else []) + [
            {
                "oldText": args.pop("oldText"),
                "newText": args.pop("newText"),
            }
        ]
    return args


def create_edit_tool(cwd: str | Path, *, operations: EditOperations | None = None) -> AgentTool:
    cwd = str(Path(cwd).expanduser().absolute())
    ops = operations if operations is not None else LocalFileOperations()

    async def execute(call_id, args, signal=None, on_update=None):
        signal = signal or AbortSignal()
        path = resolve_path(args["path"], cwd)
        async with file_mutation(path, operations):
            signal.throw_if_aborted()
            await settle(ops.access(path))
            signal.throw_if_aborted()
            data = await settle(ops.read_file(path))
            signal.throw_if_aborted()
            raw = data.decode("utf-8", errors="replace")
            bom = "\ufeff" if raw.startswith("\ufeff") else ""
            raw = raw.removeprefix(bom) if bom else raw
            ending = "\r\n" if "\r\n" in raw and raw.index("\r\n") < raw.index("\n") else "\n"
            old = normalize_lf(raw)
            new = apply_edits(old, args["edits"], args["path"])
            signal.throw_if_aborted()
            await settle(
                ops.write_file(path, bom + (new.replace("\n", ending) if ending != "\n" else new))
            )
            signal.throw_if_aborted()
        return AgentToolResult(
            [
                {
                    "type": "text",
                    "text": (
                        f"Successfully replaced {len(args['edits'])} block(s) in {args['path']}."
                    ),
                }
            ],
            details=generate_diff(args["path"], old, new),
        )

    return AgentTool(
        "edit",
        "Edit a single file using exact text replacement. Each edits[].oldText must "
        "match a unique, non-overlapping region of the original file. All replacements match "
        "the original file, not the results of earlier edits. Merge overlapping changes.",
        {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "File path, relative or absolute"},
                "edits": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "properties": {
                            "oldText": {"type": "string", "minLength": 1},
                            "newText": {"type": "string"},
                        },
                        "required": ["oldText", "newText"],
                    },
                },
            },
            "required": ["path", "edits"],
        },
        execute,
        label="edit",
        prepare_arguments=prepare_edit_arguments,
        replay="never",
        version="pi-tools-1",
    )
