"""One MCP call per connection lifecycle, using the official protocol client.

No discovery, connection pool, retry policy, credentials store or tenant registry.
Subclass call() to integrate a host-managed MCP gateway/connection lifecycle.
"""

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import MappingProxyType

import httpx2
from mcp import types
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import MCPError

from ..sessions import ToolRecoveryRequired
from ..types import AbortSignal, AgentToolResult, OperationAborted, maybe_await
from .config import McpHttpServer, McpRequestContext

# These describe the transport, rather than platform identity.
_PROTOCOL_HEADERS = {"accept", "content-type", "mcp-session-id", "mcp-protocol-version"}


class McpCallError(RuntimeError):
    """The remote tool was not dispatched (e.g. initialization or credentials failed)."""


class McpCaller:
    def __init__(self, servers):
        self.servers = dict(servers)

    @asynccontextmanager
    async def _connect(self, server, context):
        async with AsyncExitStack() as stack:
            if isinstance(server, McpHttpServer):

                async def headers(request):
                    try:
                        values = dict(server.headers)
                        if server.header_provider:
                            values.update(
                                await server.header_provider.get_headers(
                                    replace(
                                        context, http_method=request.method, url=str(request.url)
                                    )
                                )
                            )
                        for key, value in values.items():
                            if key.lower() not in _PROTOCOL_HEADERS:
                                request.headers[key] = value
                    except Exception:
                        # The transport may log callback exceptions, including during close.
                        raise McpCallError("MCP request header preparation failed") from None

                http = await stack.enter_async_context(
                    httpx2.AsyncClient(
                        timeout=server.timeout,
                        proxy=server.proxy,
                        event_hooks={"request": [headers]},
                    )
                )
                transport = streamable_http_client(server.url, http_client=http)
            else:
                # A subprocess's stderr may contain credentials. It is not model output.
                stderr = stack.enter_context(open(os.devnull, "w"))
                transport = stdio_client(
                    StdioServerParameters(
                        command=server.command,
                        args=list(server.args),
                        env=dict(server.env) if server.env is not None else None,
                        cwd=server.cwd,
                    ),
                    errlog=stderr,
                )
            read, write = await stack.enter_async_context(transport)
            session = await stack.enter_async_context(
                ClientSession(read, write, read_timeout_seconds=server.timeout)
            )
            await session.initialize()
            yield session

    async def call(self, name, arguments, *, call_id, signal=None, on_update=None, context=None):
        """Return a raw MCP result dictionary; never list tools or repeat tools/call.

        The host owns names, scopes and server configuration. Each call carries its own
        platform context, so parallel calls do not mutate shared HTTP headers.
        """
        signal = signal or AbortSignal()
        signal.throw_if_aborted()
        server_name, tool_name = name.split(".", 1)
        server = self.servers[server_name]
        request_context = McpRequestContext(
            server_name, tool_name, call_id, MappingProxyType(deepcopy(context or {}))
        )
        dispatched = False
        result = None

        async def progress(value, total=None, message=None):
            if on_update:
                await maybe_await(
                    on_update(
                        AgentToolResult(
                            content=[{"type": "text", "text": message}] if message else [],
                            details={"progress": value, "total": total},
                        )
                    )
                )

        async def execute():
            nonlocal dispatched, result
            async with self._connect(server, request_context) as session:
                signal.throw_if_aborted()
                dispatched = True
                try:
                    # The high-level call_tool() also discovers output schemas. The host
                    # already supplied definitions, so use the public low-level request API.
                    response = await session.send_request(
                        types.CallToolRequest(
                            params=types.CallToolRequestParams(name=tool_name, arguments=arguments)
                        ),
                        types.CallToolResult,
                        request_read_timeout_seconds=server.timeout,
                        progress_callback=progress,
                    )
                    result = response.model_dump(mode="json", by_alias=True, exclude_none=True)
                except MCPError as error:
                    # Explicit protocol rejection before tool execution. Other codes may be
                    # local connection errors, or failures after a side effect occurred.
                    if error.code not in (-32600, -32601, -32602):
                        raise
                    result = {
                        "content": [
                            {"type": "text", "text": f"MCP request rejected ({error.code})"}
                        ],
                        "isError": True,
                    }
            return result

        try:
            async with asyncio.timeout(server.timeout):
                return await signal.run(execute())
        except asyncio.CancelledError:
            # The runtime leaves tool_started pending and propagates task cancellation.
            raise
        except Exception as error:
            if result is not None:
                # A failed connection close must not discard an acknowledged tool result.
                return result
            if dispatched:
                raise ToolRecoveryRequired([call_id]) from None
            if isinstance(error, OperationAborted):
                raise
            # Never copy transport/header-provider error text into the session journal.
            raise McpCallError(f"MCP connection failed before calling {name}") from None
