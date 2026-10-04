# 参数丢弃与重写

## 处理顺序

以 pi 的 `simple-options.ts`、`Model.compat`、`thinkingLevelMap` 和 API 专用 `buildParams` 为主参考。pi 固定快照不包含一套完整的未知参数过滤框架；本项目增加的白名单和 `ParameterPolicy` 是用户明确要求的扩展。

1. 合并环境默认值与本次调用参数；显式值优先，显式 `None` 可以清除可选默认值。
2. 分离 API Key、signal、timeout 等运行控制参数；它们不进入模型请求体。
3. 将 `max_output_tokens` / `max_completion_tokens` 归一为 `max_tokens`，`reasoning_effort` 归一为 `reasoning`。
4. 按 API 允许列表丢弃未实现或不支持的字段。
5. 合并 provider/model 策略：禁用项累加；相同字段的模型重写覆盖 provider 重写。禁用优先于重写。
6. 对已传参数应用值映射、固定值、范围限制，并验证基础类型与边界。
7. 按模型 reasoning 元数据处理级别、采样冲突及 token 预算。
8. 转换为实际 API 字段，构造请求体。

不根据一次服务端错误进行无限尝试或偷偷切换模型。没有模型元数据时不能声称了解它的全部限制。

## 元数据接口

```python
from agent_runtime import Model, Models, ParameterPolicy, ProviderConfig

models = Models(env={})
models.register_provider(ProviderConfig(
    id="gateway",
    base_url="https://gateway.example.com/v1",
    parameter_policy=ParameterPolicy(unsupported=frozenset({"service_tier"})),
))
models.register_model(Model(
    id="example-reasoner", provider="gateway", api="openai-responses",
    base_url="https://gateway.example.com/v1", reasoning=True, max_tokens=8192,
    thinking_level_map={"off": "none", "minimal": None, "xhigh": None, "max": None},
    parameter_policy=ParameterPolicy(
        fixed_values={"temperature": 1},
        value_maps={"reasoning": {"fast": "low", "careful": "high"}},
        ranges={"top_p": (0.1, 0.9)},
        omit_when_reasoning=frozenset({"temperature", "top_p"}),
    ),
))
```

这是假想网关模型的配置示例，不代表任何真实模型。模型级策略是数据，与运行循环分离。

| 配置 | 行为 |
| --- | --- |
| `unsupported` | 丢弃指定统一参数 |
| `fixed_values` | 调用者传了该参数时重写为固定值，不主动注入未传参数 |
| `value_maps` | 将统一参数的值映射到受支持的值 |
| `ranges` | 对有限数值进行范围裁剪；错误类型仍会被 API 校验丢弃 |
| `omit_when_reasoning` | 有效 reasoning 不是 off/none 时丢弃这些参数 |
| `thinking_level_map` | 复用 pi 的级别能力映射；`None` 表示该级别不可用 |
| `compat` | API 特性，如 `max_tokens_field`、`supports_store`、`supports_developer_role` |

`reasoning=None`（模型元数据字段）表示能力未知；`False` 表示明确不支持。请求未指定 reasoning 时保留 provider 默认行为；需要判定默认 reasoning 冲突时，通过 `compat.default_reasoning` 给出可信元数据。

`compat.fixed_temperature` 保留为固定温度的便捷写法；新配置建议统一使用 `ParameterPolicy.fixed_values`。

## 当前协议转换

| 统一参数 | Responses | Chat Completions | Anthropic Messages |
| --- | --- | --- | --- |
| `max_tokens` | `max_output_tokens` | `max_completion_tokens`，可由 compat 改成 `max_tokens` | `max_tokens` |
| `reasoning` | `reasoning.effort` | `reasoning_effort` | budget 或 adaptive thinking |
| `tool_choice` | 直接发送 | 直接发送 | 字符串转 `type`，required 转 any |
| `metadata` | 直接发送 | 初版不支持，丢弃 | 仅保留 `user_id` |

模型级最大输出与上下文余量裁剪参考 pi：优先复用适用的 usage，否则按字符数估算，预留 4096 token。估算不是精确 tokenizer；Responses 的最小输出上限按 pi 设为 16。

Anthropic budget thinking 使用 pi 的默认预算 1024/2048/8192/16384，xhigh/max 回退 high；支持自定义 `thinking_budgets`。开启 thinking 时不发送冲突采样参数。

`on_parameters(report, model)` 可查看 `parameters`、`dropped` 和 `adjusted`。回调收到副本，不改变请求。报告不含 API Key 等运行凭据，但仍应由调用方决定是否记录业务参数。

自定义 `register_api` 适配器自行实现参数处理契约；内置过滤器只覆盖已实现的三个 API。完整支持范围以代码和测试为准，不将 OpenAI 兼容协议等同于所有厂商特性兼容。
