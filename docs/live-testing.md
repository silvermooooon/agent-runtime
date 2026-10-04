# 真实 OpenAI 测试

普通的 `unittest discover` 对模型使用 FakeProvider 和 MockTransport；MCP 集成测试使用本地 HTTP/stdio 服务，均不消耗模型 API 用量。真实模型测试独立运行：

```bash
uv run --env-file .env python tests/live_openai.py --live
```

前提是 `.env` 中存在 `OPENAI_API_KEY`。测试固定使用官方 `https://api.openai.com/v1/responses` 和 `gpt-6-luna`，拒绝其他配置，不会自动切换模型。

测试在六个独立进程中依次验证四类场景：

1. 收到真实文本增量并组装完整回复。
2. 模型发起 `add` 调用，Python 执行工具，再将结果交回模型。
3. 模型的完整工具计划落盘后停止，在新进程恢复，复用原工具调用身份，只发起获取最终回复的请求。
4. 首个文本增量后停止，在新进程从保存的模型请求重新执行；不要求恢复同一次服务端生成。

每次请求限制为 512 个输出 token、20 秒网络操作超时，每个阶段最多 3 次请求，子进程最多运行 60 秒。正常完整执行预计 7 次请求；任一阶段失败立即停止，不自动重试。恢复测试是主动取消后跨进程重新启动；强制进程退出由离线 Session 测试覆盖。

测试输出阶段结果、模型、HTTP 状态、request ID、事件计数和 token 用量，不输出 API key 或请求头。Session 存在临时目录，测试后自动清理。运行前需由宿主加载 `.env`；SDK 本身不会搜索文件。

2026-10-04 首次在线尝试：已检查本地 key 配置存在，模型已指定为 `gpt-6-luna`。当前执行环境解析 `api.openai.com` 失败，请求未到达 OpenAI；尚未验证 key 有效性或真实接口兼容性。此记录不是在线测试通过报告。

## 2026-10-04 实测通过

调整当前任务的执行权限后，通过项目 `.env` 中的 `HTTPS_PROXY=http://127.0.0.1:7890` 重新运行上述命令。没有修改系统代理设置。六个阶段全部通过，共 7 次真实请求，均为官方 Responses API、`gpt-6-luna`、HTTP 200；未使用 MockTransport、FakeProvider 或其他模型。

| 阶段 | 请求数 | 文本增量数 | 工具执行数 | Session 状态 |
| --- | --- | --- | --- | --- |
| text | 1 | 2 | 0 | completed |
| tools | 2 | 1 | 1 | completed |
| plan-start | 1 | 0 | 0 | stopped |
| plan-resume | 1 | 1 | 1 | completed |
| stream-start | 1 | 2 | 0 | stopped |
| stream-resume | 1 | 199 | 0 | completed |

文本场景完整返回 `runtime-ok`。工具场景执行 `17 + 25` 并返回 `42`。工具计划在执行前停止后，新进程复用原 tool call ID，执行工具并仅请求一次最终回复。文本流在首个增量处发出停止信号后，新进程重新请求并完整返回 `1` 至 `100`，不要求恢复原服务端流；停止信号发出后仍可能收到少量已排队的增量。

以下 request ID 可用于核对真实请求（按阶段顺序排列）：

```text
text:          req_ee3c739b32ca4576977b3e9210afe563
tools:         req_432e2e5175c044bca93771d175618dc1
               req_66007280f4f94486a2bf746f54ea51a8
plan-start:    req_385726348df34386b673dca1148ff28a
plan-resume:   req_fc3d2951d10546e7856039107ccbafba
stream-start:  req_5a99a86c349445bf803ed663d2a1cef0
stream-resume: req_2529ca9b80f84794b2cdc27fbe593368
```

此次测试参数为 `reasoning=none`、每次请求 `max_tokens=512`。它确认这些具体场景的真实连通性和恢复行为，不覆盖所有参数组合、模型能力或并发故障。被取消的流未收到终态 usage，不能通过 SDK 返回的 usage 汇总得到完整计费量。

## 2026-10-05 生命周期与流式修复后回归

再次运行 `tests/live_openai.py --live`，六个阶段全部通过，共 7 次官方 Responses 请求，固定 `gpt-6-luna`，全部 HTTP 200。覆盖文本、工具循环、工具计划停止后跨进程恢复，以及文本流停止后重新请求。停止后的工具计划只执行一次工具；流恢复后完整输出 1 至 100。

```text
text:          req_a04ac9d600524e7a8902dca3b911e83c
tools:         req_48d4c4c7221a49d5a20ad19cf473ebfc
               req_eb029cf3356d4899a021ce77bbd8d7ac
plan-start:    req_80702c35c5094fea94519a3ca07f916e
plan-resume:   req_90c2ad9b41b042288fbab7610bcce9a5
stream-start:  req_1cd4eb85eade44c1971801336ce27e44
stream-resume: req_27f9e2cd249146108dfeb7804949d05d
```

同版本离线测试 201 项通过，其中新增 13 项覆盖写入权释放竞争、持久化失败、重复取消、流式背压、增量日志读取和完整输出文件保留。慢速消费及文件故障由可控的离线测试验证，不以本次在线成功替代故障覆盖。Ruff 检查及 wheel／sdist 构建通过。

## 2026-10-04 内置四工具实测通过

独立入口：

```bash
uv run --env-file .env python tests/live_coding_tools.py --live
```

固定官方 Responses API 和 `gpt-6-luna`，最多 6 次请求，每次最多 1024 输出 token、20 秒网络操作超时，整个任务上限 120 秒。工具通过 `create_coding_tools` 显式装配。审批钩子限制到固定的临时文件和命令，子进程只接收测试所需的 PATH；临时工作目录及 Session 在测试结束后清理。

本次 5 次真实请求全部 HTTP 200，模型依次完成：

1. `write`：写入 `hello.txt`，内容为 `hello pi\n`。
2. `read`：读取并验证完整内容。
3. `edit`：将 `pi` 替换为 `runtime`。
4. `bash`：运行 `wc -c < hello.txt`，返回 14 字节、退出码 0。
5. 返回 `coding-tools-ok`，LocalSession 状态为 `completed`。

最终文件逐字节验证为 `hello runtime\n`。没有使用模拟模型或模拟文件系统。图片、截断、超时／取消等边界仍由离线测试覆盖，此次在线验证仅证明上述工具循环。

```text
req_3340b6818d08427596d0f6032da9d88d
req_dff90e22b5c245639d245deb9f977d3e
req_3e91f2e6c31642e3a40d545a329136e4
req_cddaa2652fe840a2b5a164d53cb8dbfa
req_185d1be6a15f4f72a563410af2743d06
```

## 2026-10-04 压缩与独立分叉实测通过

独立入口：

```bash
uv run --env-file .env python tests/live_projection.py --live
```

固定官方 Responses API 和 `gpt-6-luna`，最多 3 次请求，单次网络操作超时 20 秒，整个任务上限 120 秒。
这次 3 次真实请求全部 HTTP 200，依次验证：记住测试代码 `ORBIT-482`、生成压缩摘要、从分叉后的会话继续回答该代码。
压缩前的日志字节完整保留；分叉后删除父会话日志，再重新打开子会话，仍可推导相同上下文并正确回答。
Session 文件使用临时目录，测试结束后清理。此次实测不覆盖自动阈值、并发崩溃等全部边界，这些由离线测试覆盖。

```text
req_f0fe40cd70ee4705bb38435bfe603aeb
req_549dc49a14f343a4aaf8526dc0fc5425
req_5337614e9f3a4cee8548261730b76f5c
```

## 2026-10-05 MCP 调用实测通过

独立入口：

```bash
uv run --env-file .env python tests/live_mcp.py --live
```

固定官方 Responses API 和 `gpt-6-luna`，最多 3 次请求、每次最多 512 输出 token、20 秒网络操作超时，整个 Agent 运行上限 90 秒。使用项目 `.env` 的凭据和代理配置，没有修改系统配置。

本次 2 次真实请求均为 HTTP 200：模型选择 `demo.echo`，SDK 调用本地 HTTP MCP 测试服务一次，再把工具结果发给模型，最终返回 `mcp-runtime-ok`，Session 为 `completed`。模型未使用 MockTransport 或 FakeProvider；MCP 是真实协议交互的本地测试服务，不是外部生产 MCP 服务。

同时验证：未发起 `tools/list`；初始化、工具调用和关闭等 HTTP 请求带上指定测试身份头；业务工具名和模型协议工具名正确转换；完整 MCP 结果进入 `tool_returned`，请求头没有进入 Session 日志。超时、取消、并发隔离和未知结果恢复由离线测试覆盖，此次实测仅确认成功链路。

```text
req_3aa14a64f226462fa23203e57ed786ed
req_eb0f025d39b742bf91577932b131fde8
```

## 2026-10-05 Skill 加载实测通过

独立入口：

```bash
uv run --env-file .env python tests/live_skills.py --live
```

固定官方 Responses API 和 `gpt-6-luna`，最多 3 次请求，每次最多 512 输出 token、20 秒网络操作超时，Agent 运行上限 90 秒。使用临时 Skill 目录和 Session，Skill 存储通过 `AGENT_SKILLS_DIR` 配置接口注入该目录；模型凭据和代理从项目 `.env` 加载。

本次 3 次真实请求均为 HTTP 200。模型依次调用 `load_skill`、`load_skill_reference`，然后按参考文本返回 `skill-runtime-ok`。两个完整工具返回值均已写入 Session，最终状态为 `completed`；没有使用模拟模型或通用 read 工具。目录配置、替换后端、审批、并发、取消、路径边界和源文件变更后的恢复由离线测试覆盖。

```text
req_848e8bc9cbea47f9a254cae595c44080
req_0379820137d64622b2f840fa0e3643d0
req_296741cefca94a2ebb92bc5be8a858af
```
