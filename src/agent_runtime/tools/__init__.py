"""Bundled pi coding tools. Importing this module does not enable any tools on an Agent."""

from pathlib import Path

from ._images import ImageResizeOptions
from .bash import BashOperations, LocalBashOperations, create_bash_tool
from .edit import create_edit_tool
from .operations import EditOperations, LocalFileOperations, ReadOperations, WriteOperations
from .read import create_read_tool
from .write import create_write_tool


def create_coding_tools(
    cwd: str | Path,
    *,
    read_options=None,
    bash_options=None,
    edit_options=None,
    write_options=None,
):
    """pi's read, bash, edit, write in that order; explicitly pass the list to Agent(tools=...)."""
    return [
        create_read_tool(cwd, **(read_options or {})),
        create_bash_tool(cwd, **(bash_options or {})),
        create_edit_tool(cwd, **(edit_options or {})),
        create_write_tool(cwd, **(write_options or {})),
    ]


__all__ = [
    "create_coding_tools",
    "create_read_tool",
    "create_bash_tool",
    "create_edit_tool",
    "create_write_tool",
    "ImageResizeOptions",
    "ReadOperations",
    "WriteOperations",
    "EditOperations",
    "BashOperations",
    "LocalFileOperations",
    "LocalBashOperations",
]
