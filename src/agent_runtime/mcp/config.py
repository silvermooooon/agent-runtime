"""Execution configuration only. Tool discovery and naming belong to the host."""

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class McpRequestContext:
    server_name: str
    tool_name: str
    tool_call_id: str
    values: Mapping[str, Any] = field(default_factory=dict, repr=False)
    http_method: str = ""
    url: str = ""


class McpHeaderProvider:
    """Override to resolve credentials and platform headers before each HTTP request."""

    async def get_headers(self, context: McpRequestContext) -> Mapping[str, str]:
        return {}


@dataclass(frozen=True)
class McpHttpServer:
    url: str
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)
    header_provider: McpHeaderProvider | None = field(default=None, repr=False)
    timeout: float = 60
    proxy: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class McpStdioServer:
    command: str
    args: tuple[str, ...] = ()
    env: Mapping[str, str] | None = field(default=None, repr=False)
    cwd: str | None = None
    timeout: float = 60
