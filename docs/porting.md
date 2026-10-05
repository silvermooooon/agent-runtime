# pi 移植映射与边界

## 固定参考版本

- 上游：<https://github.com/earendil-works/pi>，原 `badlogic/pi-mono` 重定向到此仓库。
- 提交：[`83692682f095528f8b71652ddacff7075e36e893`](https://github.com/earendil-works/pi/tree/83692682f095528f8b71652ddacff7075e36e893)。
- 本项目是 Python 移植，不向上游 TypeScript 仓库提交修改。
- 上游许可为 MIT；随项目保留原始 LICENSE 和 NOTICE。

## 文件对应

| 上游文件 | Python 文件 | 保留的核心行为 |
| --- | --- | --- |
| packages/agent/src/agent.ts | src/agent_runtime/agent.py | 状态、订阅、队列、取消、生命周期 |
| packages/agent/src/agent-loop.ts | src/agent_runtime/agent_loop.py | 内外层 loop、工具执行、turn 钩子 |
| packages/agent/src/types.ts | src/agent_runtime/types.py | Model、context、工具和运行配置 |
| packages/agent/src/stream-fn.ts | src/agent_runtime/stream_fn.py | 可注入默认流函数 |
| packages/agent/src/proxy.ts | src/agent_runtime/proxy.py | /api/stream 的增量重建与终态检测 |
| packages/agent/src/index.ts | src/agent_runtime/__init__.py | 公共导出 |
| packages/ai/src/utils/event-stream.ts | event_stream.py | 异步迭代与最终 result |
| packages/ai/src/utils/transcript.ts | transcript.py | system/tool 状态重放和工具声明差异 |
| packages/ai/src/utils/validation.ts | validation.py | 参数复制、可选 null 清理、转换和校验 |
| packages/ai/src/api/simple-options.ts | ai/options.py | 输出上限、thinking 预算 |
| packages/ai/src/models.ts | ai/models.py、ai/__init__.py | 元数据、reasoning 等级裁剪、注册入口 |
| packages/ai/src/utils/estimate.ts | ai/estimate.py | 基于 usage 和字符数的上下文估算 |
| packages/ai/src/api/openai-responses*.ts | ai/messages.py、ai/parsers.py | Responses 请求映射、增量与完整结果 |
| packages/ai/src/api/openai-completions.ts | ai/messages.py、ai/parsers.py | 基本 Chat Completions 流与工具调用 |
| packages/ai/src/api/anthropic-messages.ts | ai/options.py、ai/messages.py、ai/parsers.py | 基本 Messages 协议与 thinking 预算 |
| packages/coding-agent/src/core/compaction/compaction.ts、utils.ts | compaction.py | 保留近期消息、工具边界、长轮次分割摘要、文件跟踪 |
| packages/coding-agent/src/core/session-manager.ts | sessions/projection.py、sessions/base.py、sessions/local.py | 日志推导摘要上下文、复制前缀创建独立会话 |
| packages/coding-agent/src/core/agent-session.ts | agent.py | 模型请求前自动压缩、手动压缩、生命周期事件 |
| packages/coding-agent/src/core/tools/index.ts | tools/__init__.py | 显式创建 read、bash、edit、write 工具集 |
| packages/coding-agent/src/core/tools/read.ts | tools/read.py | 文本分页、头部截断、图片附件 |
| packages/coding-agent/src/core/tools/bash.ts | tools/bash.py | 流式输出、超时／取消、退出码、可替换执行后端 |
| packages/coding-agent/src/core/tools/edit.ts、edit-diff.ts | tools/edit.py、tools/_edit_diff.py | 原文多块匹配、模糊匹配、冲突校验、diff |
| packages/coding-agent/src/core/tools/write.ts、file-mutation-queue.ts | tools/write.py、tools/operations.py | 创建目录、写入、同文件修改队列 |
| packages/coding-agent/src/core/tools/path-utils.ts、truncate.ts、output-accumulator.ts | tools/_paths.py、tools/_truncate.py、tools/_output.py | 路径兼容、输出截断、完整输出临时文件 |
| packages/coding-agent/src/utils/image-*.ts、mime.ts | tools/_images.py | 图片识别、缩放、BMP 转换、附件大小限制 |
| packages/mcp/src/protocol/content.ts | mcp/adapter.py | toLlmContent 文本、图片、嵌入资源和结构化结果投影 |
| packages/coding-agent/src/extensions/mcp/tools.ts | mcp/adapter.py、mcp/caller.py | MCP 结果适配普通工具、错误标志及进度通知的接口思路 |

## 保留的调度语义

- 一轮是一次模型响应与它产生的全部工具调用／结果。
- 工具批次执行完后才注入 steering；不会因此跳过本轮剩余工具。
- 没有工具和 steering 时才检查 follow-up。
- 并行工具先顺序完成前置校验，再并行执行；完成事件按完成顺序，toolResult 消息按模型原始顺序。
- 任一工具要求 sequential 时，整个批次顺序执行。
- 所有工具结果都要求 terminate，才提前结束工具循环。
- 输出被 token 上限截断时，不执行其中的工具调用。
- finish_turn 在 turn_end 前运行；error/aborted 仍强制退出。
- async 订阅回调被等待，最后的 agent_end 回调完成后才进入 idle。
- continue_ 保留 pi 的 transcript 尾部限制，并非通用的持久化恢复引擎。

## 有意的 Python／需求扩展

- 方法和配置字段使用 snake_case；消息／事件字典保留 pi 的 camelCase。
- 用 asyncio、httpx 和 jsonschema 替换 TS 的 Promise、厂商 SDK 和 TypeBox。
- 工具任务仅交回进度和原始结果，主流程统一保存并发布事件、串行执行后处理。失败或取消时等待任务结束；已发生的外部副作用仍不因此撤销。
- 增加 provider/model 参数策略、过滤报告、环境配置与默认 Responses 入口。
- 增加基于 Session 继承接口的本地事件记录、直接保存边界和 resume 入口；内存瞬时态合并在 LocalSession 中。该恢复协议为 Python SDK 扩展，见 [Session 设计](sessions.md)。
- Agent 默认创建 LocalSession。开启文件存储时，持久内容必须能序列化为 JSON；原始工具结果在后处理前保存。
- 压缩只追加摘要与保留边界，原消息不删除；内存上下文从单份完整日志重放。按 event ID 分叉时复制完整前缀到新 Session，不实现上游同文件分支树。见 [上下文与分叉](context.md)。
- 自动压缩需要显式装配 `CompactionSettings`；摘要使用当前 Agent 的模型。上下文大小采用消息估算，未移植上游上下文溢出后的自动压缩重试。
- 未指定 reasoning 时保留 provider 默认，而不主动发送 off。
- `on_payload` 是观察副本的回调，不像 pi 那样允许任意覆盖整个请求体，避免绕过参数过滤。
- Responses 完整原始 output 保存在 `responseOutput`，用于同 provider/model 的无状态 reasoning 重放。流式工具参数不完整时不交给执行器。
- 未知模型允许使用已注册 provider 的默认协议；模型能力保留未知状态，不按名字猜测。
- 四个内置工具的默认参数和主要行为参考上述固定版本；工具必须显式装配。图片后端使用 Pillow，diff 使用 difflib，因此编码结果和 diff 分块不保证逐字节等同于上游。
- 文件修改队列使用事件循环内的 asyncio.Lock，等待正在执行的 I/O 结束后释放；不是数据库锁，也不是跨进程锁。工具恢复沿用 SDK 的 replay 协议。
- 未移植 coding-agent 的 TUI renderer、提示词元数据注入和模型专用图片配置；常用使用说明放入工具 description，图片限制可通过工厂参数显式配置。本地 bash 的进程组取消测试覆盖 macOS/POSIX；Windows 未验证，Windows 默认后端只终止直接子进程。
- MCP 协议收发复用官方 Python `mcp` 2.3 客户端，使用 `ClientSession.send_request` 避免高层 `call_tool` 隐式发现 schema。默认按次连接，平台提供已选 schema 和身份上下文；未移植 Pi 的发现、schema 缓存、OAuth、codemode、资源读取及截断管理。
- MCP 的模型内容投影直接翻译上述 `toLlmContent`；完整结果另外保留在现有工具结果的 details 内。业务名称使用约定的 `server.tool`，在默认 AI 协议层转为 `server__tool` 后还原，没有移植 Pi 的 hash 命名和冲突处理。
- Skill 按本项目需求使用 `load_skill` / `load_skill_reference` 专用工具及可继承的 `SkillStore`，没有移植 Pi 通过通用 read 工具读取 Skill 的方式。本地后端支持目录发现和 YAML 元数据，宿主负责提供候选目录，详见 [Skill 加载](skills.md)。

## 执行与持久化边界

平台保证同一 Session 的执行者唯一性，并负责跨 Worker 的接管和失效控制；实例只在所属事件循环中使用。SDK 主流程顺序修改状态和提交日志。追加消息只进入内存队列，入队成功不承诺落盘；可靠接收由平台先保存输入来实现。

并行工具保留 pi 的执行和消息排序语义，持久化结果由主流程按完成顺序保存，后处理钩子也在主流程运行。内置模型 adapter 的准备入口 `Models.stream_simple()` 需要 await，使请求参数能在启动后台 HTTP 流之前由调用协程保存。Session、恢复点和日志投影是本 SDK 的扩展，具体契约见 [Session](sessions.md)。

## 初版未移植范围

- pi-ai 完整生成模型目录、价格计算、OAuth 和订阅登录。
- Google、Bedrock、Azure 特殊认证、厂商自定义协议和路由参数。
- WebSocket、自动网络重试、prompt cache 会话亲和与完整 cache 配置。
- 原生 hosted tools、tool search、grammar/custom tools、语音等所有输出类型。遇到未实现的输出块会报错，不假装执行成功。
- 完整跨模型历史修复、mid-conversation 原生工具变更协议；本版在请求边界折叠 system/tool 声明。
- TypeBox 专用 symbol 语义与精确 tokenizer。
- DB Session、Redis 租约、任务接管、权限后端、MCP 工具发现与 SaaS 服务。

这些边界不会隐藏在“与 pi 完全等价”的表述下。扩展其他 provider 时应继续对照固定或明确升级的 pi 版本。

## 验证方式

核心测试场景参考上游 `packages/agent/test/agent.test.ts` 与 `agent-loop.test.ts`，覆盖事件顺序、队列、工具校验、串并行、取消和钩子。
HTTP 测试使用 httpx MockTransport 注入 Responses、Completions、Anthropic、proxy SSE，验证实际请求体及流解析。参数测试使用合成模型元数据，不绑定特定新模型名称。
Python 流增加有限队列和异步 `send`，内置 adapter 在协议事件之间等待容量；`result()` 明确采用只保留最终结果的消费方式。Session 由主流程顺序提交，尾部查询只维护内存游标；bash 的 `output_dir` 复用现有完整输出文件机制。对应回归测试见 `tests/test_runtime_boundaries.py`。

内置工具测试使用真实临时文件和本地 shell，覆盖分页、图片、批量编辑、并发修改、输出截断、超时、取消、审批及 Session 恢复；模型调度部分使用 FakeProvider。

MCP 测试使用真实本地 HTTP/stdio 传输，验证不触发发现、逐请求头透传、并发身份隔离、审批、进度、完整结果存储、超时及未知结果恢复。`examples/mcp_tools.py` 使用官方 MCPServer 验证 stdio 互操作，`tests/live_mcp.py` 单独验证真实模型到本地 MCP 的工具循环。

真实 provider 测试与离线测试分开运行，已有官方 Responses API 与 gpt-6-luna 的实测记录，见 [在线测试说明](live-testing.md)。这不表示所有厂商兼容端点都已通过验证。
