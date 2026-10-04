# MCP 工具调用

`agent_runtime.mcp` 随 SDK 提供，只有显式装配的工具才会交给 Agent。平台负责发现、选择工具及提供 JSON Schema；SDK 负责连接、调用和结果适配。装配时不联网，调用前不运行 `tools/list` 或额外健康检查。

## 装配方式

```python
from agent_runtime import Agent
from agent_runtime.mcp import (
    McpCaller, McpHttpServer, McpHeaderProvider, assemble_tools,
)
from agent_runtime.tools import create_read_tool

class PlatformHeaders(McpHeaderProvider):
    async def get_headers(self, context):
        return {
            "X-Tenant-ID": context.values["tenant_id"],
            "X-Request-ID": context.values["request_id"],
            "Authorization": f"Bearer {context.values['jwt']}",
        }

caller = McpCaller({
    "crm": McpHttpServer(
        url="https://mcp.example.com/mcp",
        headers={"X-Application": "agent-platform"},
        header_provider=PlatformHeaders(),
        timeout=60,
    ),
})

# 来自业务层圈选后的工具列表，可同时配置本地工具和 MCP 工具。
selected = [
    {"type": "builtin", "name": "read"},
    {
        "type": "mcp",
        "name": "crm.query",
        "description": "查询客户资料",
        "input_schema": {
            "type": "object",
            "properties": {"customer_id": {"type": "string"}},
            "required": ["customer_id"],
            "additionalProperties": False,
        },
        "version": "1",  # 可选；沿用已有工具版本及恢复检查
    },
]
tools = assemble_tools(
    selected,
    builtins={"read": create_read_tool(cwd="/workspace")},
    mcp_caller=caller,
    context={"tenant_id": "tenant-a", "request_id": "request-123", "jwt": user_jwt},
)
agent = Agent(tools=tools)  # 使用已配置的模型及 Session
await agent.prompt("查询客户 123 的资料")
```

`user_jwt` 来自宿主的认证上下文。同一个 caller 可供多个 Agent 使用，每个 Agent 分别装配自己的工具和调用上下文；不要把用户 JWT 写进共享的静态 headers。`context` 在装配、调用时复制，只交给请求头提供者，不放进模型工具定义或 Session。恢复会话时须重新注入 caller、工具及有效凭据。

如果已经使用 `AgentTool` 工厂组装，可以直接调用 `create_mcp_tool(name, description, input_schema, caller=..., context=..., version=...)`，不必使用配置列表。

自定义凭据提供者需要支持宿主的并发调用方式；同一个正在运行的 Agent/Session 仍由单一执行者使用，MCP 适配不会改变这项约束。

名称按业务约定为 `server_name.tool_name`，以第一个点分隔路由。没有独立 tool ID、注册中心或冲突检查。模型 API 的函数名称有格式限制，默认 AI 层把点转换为双下划线，例如 `crm.query` → `crm__query`；输出事件和普通消息还原为业务名称。原始 `responseOutput` 保留协议名称，便于重放。业务负责名称长度、合法性，以及转换后的唯一性，例如不要同时配置 `crm.query` 和 `crm__query`。自定义 `stream_fn` 沿用 SDK 的业务名称协议。

## 请求头和连接

- `headers`：服务级静态请求头。
- `header_provider.get_headers(context)`：每次 HTTP 请求前调用；同名请求头覆盖静态值。需要动态刷新 JWT 时，在此方法内调用业务凭据服务。
- `McpRequestContext` 包含 `server_name`、`tool_name`、`tool_call_id`、业务 `values`、本次 `http_method` 和 `url`。
- 透传覆盖初始化、通知、工具 POST、服务端事件 GET，以及连接关闭时的 DELETE。协议需要的 `Accept`、`Content-Type`、`Mcp-Session-Id`、`Mcp-Protocol-Version` 由官方客户端管理，忽略业务对这些字段的覆盖。
- `timeout` 是一次调用的总时限，也用作网络和协议等待超时；取消时仍需等待客户端及子进程清理。`proxy` 可显式配置 HTTP 代理，否则沿用客户端的环境代理行为。

默认使用官方 Python MCP 客户端的 Streamable HTTP 或 stdio 传输，每次调用建立并关闭独立连接。HTTP 并发调用不共享会话及请求头；stdio 每次调用启动独立子进程并清理。代价是每次初始化的开销，远端 MCP 会话也不会跨工具调用保留。宿主需要复用连接时，可替换 caller；核心不维护连接池、后台探活、schema 缓存或 OAuth 存储。

当前使用带 `initialize` 握手的 `ClientSession`，不支持仅提供旧版独立 SSE 端点的服务，也未适配 2026 新式无初始化协议、交互式 elicitation 或 sampling。

stdio 配置：

```python
from agent_runtime.mcp import McpCaller, McpStdioServer

caller = McpCaller({
    "local": McpStdioServer(
        command="/path/to/python",
        args=("/path/to/mcp_server.py",),
        env={"SERVICE_TOKEN": service_token},
        cwd="/workspace",
        timeout=60,
    ),
})
```

stdio 环境采用官方客户端的基础环境白名单，加上显式 `env`，不会默认继承宿主全部凭据。HTTP 请求头不适用于 stdio。默认丢弃子进程 stderr，避免把它作为模型输出或泄漏到运行日志；需要服务端诊断日志时，由宿主管理子进程日志。

## 结果、审批与恢复

适配后仍是普通 `AgentTool`，沿用 JSON Schema 校验、`before_tool_call` 审批、执行进度、`after_tool_call` 和 Session 保存顺序。校验失败或审批拒绝时不会建立 MCP 连接。

MCP 进度通知转换为 `tool_execution_update`，仅用于瞬时展示。文本、图片、嵌入资源及其他内容的模型投影直接参考 Pi 的 `toLlmContent`：支持的文本和图片传入模型；音频、二进制资源等使用说明文本；空 content 有 structuredContent 时转换为 JSON。完整协议结果保留在 `AgentToolResult.details["mcp"]`，随 `tool_returned` 落入现有单份 Session 日志。不会另建 MCP 日志。后处理失败时仍可复用已经保存的原始结果。

| 情况 | 行为 |
| --- | --- |
| 配置/初始化失败，尚未发送工具调用 | 作为工具错误交给模型；连接错误文案不包含凭据提供者或传输异常原文 |
| 明确的 `isError` 或无效请求/未知方法/无效参数拒绝 | 保存工具错误结果 |
| 发出调用后断线、超时，或主动 abort | 抛出 `ToolRecoveryRequired`，保持结果未知，Agent 进入 `waiting_recovery` |
| Python task 被取消 | 传播 `CancelledError`，保留已开始的工具记录；恢复时核实未知结果 |
| 已收到完整结果，但关闭连接失败 | 保留已知结果 |

默认工具保持 `replay="never"`，caller 不重发 `tools/call`。连接中断或取消不能证明远端副作用已停止。再次 `resume()` 会要求宿主核实：确认实际结果后调用已有 `session.resolve_tool_call(call_id, result)` 再继续，其他显式重试决策见 [Session 恢复](sessions.md)。协议客户端可能恢复读取 SSE，但这不是重新执行工具。

## 扩展入口与验证

继承 `McpHeaderProvider` 定制凭据；继承 `McpCaller` 并覆盖异步 `call(name, arguments, *, call_id, signal, on_update, context)` 接入平台网关或由宿主管理的连接。返回 MCP result 字典，参数中有取消信号和进度回调。替换实现也应遵守未知结果抛出 `ToolRecoveryRequired([call_id])` 的约定，不能把无法确认的调用伪装成已完成错误。发现模块可以独立运行，调用器不依赖它。

离线示例使用官方 MCPServer 启动真实 stdio 子进程：

```bash
uv run python examples/mcp_tools.py
uv run --env-file .env python examples/mcp_tools.py --agent  # 会调用真实模型
```

本地 HTTP/stdio 集成测试不使用外部凭据，涵盖协议交互、请求头隔离、审批、并发、取消、日志及恢复，并主动拒绝 `tools/list`。真实模型联调单独启用：

```bash
uv run --env-file .env python tests/live_mcp.py --live
```

该测试只允许官方 Responses API 和 `gpt-6-luna`，MCP 为本地测试服务。移植出处见 [Pi 映射](porting.md)，在线验证记录见 [测试说明](live-testing.md)。
