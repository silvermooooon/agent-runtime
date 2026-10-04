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
- Python TaskGroup 在事件 sink 失败时取消并行兄弟任务；避免异常后遗留后台执行。工具外部副作用仍不因此撤销。
- 增加 provider/model 参数策略、过滤报告、环境配置与默认 Responses 入口。
- 增加基于 Session 继承接口的本地事件记录、直接保存边界和 resume 入口；内存瞬时态合并在 LocalSession 中。该恢复协议为 Python SDK 扩展，见 [Session 设计](sessions.md)。
- Agent 默认创建 LocalSession。开启文件存储时，持久内容必须能序列化为 JSON；原始工具结果在后处理前保存。
- 未指定 reasoning 时保留 provider 默认，而不主动发送 off。
- `on_payload` 是观察副本的回调，不像 pi 那样允许任意覆盖整个请求体，避免绕过参数过滤。
- Responses 完整原始 output 保存在 `responseOutput`，用于同 provider/model 的无状态 reasoning 重放。流式工具参数不完整时不交给执行器。
- 未知模型允许使用已注册 provider 的默认协议；模型能力保留未知状态，不按名字猜测。

## 初版未移植范围

- pi-ai 完整生成模型目录、价格计算、OAuth 和订阅登录。
- Google、Bedrock、Azure 特殊认证、厂商自定义协议和路由参数。
- WebSocket、自动网络重试、prompt cache 会话亲和与完整 cache 配置。
- 原生 hosted tools、tool search、grammar/custom tools、语音等所有输出类型。遇到未实现的输出块会报错，不假装执行成功。
- 完整跨模型历史修复、mid-conversation 原生工具变更协议；本版在请求边界折叠 system/tool 声明。
- TypeBox 专用 symbol 语义与精确 tokenizer。
- DB Session、Redis 租约、任务接管、权限后端、MCP 客户端与 SaaS 服务。

这些边界不会隐藏在“与 pi 完全等价”的表述下。扩展其他 provider 时应继续对照固定或明确升级的 pi 版本。

## 验证方式

核心测试场景参考上游 `packages/agent/test/agent.test.ts` 与 `agent-loop.test.ts`，覆盖事件顺序、队列、工具校验、串并行、取消和钩子。
HTTP 测试使用 httpx MockTransport 注入 Responses、Completions、Anthropic、proxy SSE，验证实际请求体及流解析。参数测试使用合成模型元数据，不绑定特定新模型名称。

这些都是离线测试；没有使用用户凭据进行真实 provider 联调，也不表示所有厂商兼容端点都已通过验证。
