# Responses 原生网页搜索

`agent_runtime.tools.create_web_search_tool` 提供 OpenAI Responses API 的 `web_search`。
调用方把它加入 `Agent(tools=...)` 才会启用，不传就不启用；没有全局默认启用配置或关闭开关。
不传搜索选项时直接发送 `{"type": "web_search"}`，使用 OpenAI 自身的默认行为。

```python
from agent_runtime import Agent
from agent_runtime.tools import create_web_search_tool

agent = Agent(tools=[create_web_search_tool()])
await agent.prompt("搜索 OpenAI 官方 Responses API 的网页搜索说明，并引用来源。")
```

模型默认沿用 SDK 的 `gpt-6-luna`。是否实际搜索由模型决定；需要强制搜索时，
可使用现有参数入口 `parameters={"tool_choice": {"type": "web_search"}}`。
可以和 `create_coding_tools()`、MCP 工具及普通 `AgentTool` 放在同一个工具列表中。

可选的搜索参数直接对应官方协议：

```python
tool = create_web_search_tool(
    search_context_size="low",  # low / medium / high
    filters={"allowed_domains": ["developers.openai.com"]},
    user_location={"type": "approximate", "country": "CN"},
    external_web_access=False,
)
agent = Agent(tools=[tool])
```

`external_web_access=False` 表示仅使用缓存或索引结果，仍然允许搜索。
工具仅支持 `openai-responses` 协议；配置在其他协议上会明确报错，不会静默转为普通函数。
兼容服务或模型是否实现原生搜索由服务端决定，SDK 不进行模型名称猜测或自动换模型。

## 执行、事件和恢复

工厂返回轻量的 `ProviderTool` 声明。它和本地执行的 `AgentTool` 共用 `tools` 装配入口，
但搜索由 OpenAI 在本次请求内部执行。不会产生本地 `tool_execution_*` 事件、
不会发送 `function_call_output`，也不会经过 `before_tool_call` / `after_tool_call`。
需要限制搜索的调用方应在请求前决定是否装配该工具。

搜索进度通过现有 `message_update` 中的 `assistantMessageEvent` 传递：

- `web_search_start`：开始一个搜索输出项。
- `web_search_update`：`item.status` 为 `in_progress`、`searching` 或 `completed`。
- `web_search_end`：完整搜索输出项，`item.action` 保留查询、打开页面或页内查找等动作。

这些事件的 `partial.content` 包含 `type: "webSearchCall"` 块。回答仍通过 `text_delta`
输出；完整文本块的 `annotations` 保留引用链接、标题和位置，多段文本合并时同步调整位置。
展示网页搜索结果时，界面应将引用呈现为清晰可点击的链接，见
[OpenAI 官方说明](https://developers.openai.com/api/docs/guides/tools-web-search)。

完整原始 `web_search_call` 和引用保存在助手消息的 `responseOutput` 中，
沿用现有 `model_completed`、Session 投影和 checkpoint；不新增日志表或第二份事实来源。
瞬时进度沿用现有策略，不逐条落盘。恢复未完成的模型请求时，需要重新传入相同工具声明；
已完成响应会原样进入同 provider/model 的后续上下文，压缩后保留完整历史但只加载压缩后的上下文。

如果模型响应中途失败，恢复会重新发起该次模型请求，搜索可能再次执行和计费；
SDK 不尝试单独接管 OpenAI 服务端内部的搜索步骤。

运行示例：`uv run --env-file .env python examples/web_search.py`。
离线测试：`uv run python -m unittest discover -s tests -p test_web_search.py -v`。
真实接口测试：`uv run --env-file .env python tests/live_web_search.py --live`，会产生 API 用量。
