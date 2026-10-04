# 内置 coding tools

四个工具随 SDK 安装，但不会由 `Agent()` 自动启用。独立模块为 `agent_runtime.tools`，参考 pi 固定提交 `83692682f095528f8b71652ddacff7075e36e893` 的 coding-agent 工具实现。

## 显式装配

```python
from agent_runtime import Agent
from agent_runtime.tools import create_coding_tools

agent = Agent(tools=create_coding_tools(cwd="/workspace/project"))
await agent.prompt("读取 README.md，并修正其中的拼写错误。")
```

或者逐个声明，只启用部分工具，也可以混合业务工具：

```python
from agent_runtime.tools import (
    create_read_tool, create_bash_tool, create_edit_tool, create_write_tool,
)

tools = [
    create_read_tool("/workspace/project"),
    create_bash_tool("/workspace/project"),
    create_edit_tool("/workspace/project"),
    create_write_tool("/workspace/project"),
]
agent = Agent(tools=tools)
```

工厂返回普通 `AgentTool`，沿用现有 JSON Schema 校验、审批钩子、工具事件和 Session 记录，无需另一套 Runtime。API key、provider、模型等仍通过 `Agent` 原有入口和环境变量配置。

完整命令行示例：

```bash
uv run --env-file .env python examples/coding_tools.py /workspace/project '检查 README.md'
```

## 参数与行为

| 名称 | 模型可传参数 | 默认行为 |
| --- | --- | --- |
| `read` | `path`、可选 `offset` / `limit` | 文本从第 1 行开始，最多返回头部 2000 行或 50 KiB；附下一页提示。图片返回附件。 |
| `bash` | `command`、可选 `timeout`（秒） | 在指定目录执行 shell，合并 stdout/stderr，实时更新；保留尾部 2000 行或 50 KiB，截断时另存完整输出临时文件。默认无超时。 |
| `edit` | `path`、`edits: [{oldText, newText}]` | 所有替换都匹配修改前的原文；匹配不唯一、重叠、找不到或整体没有变化时失败，不写入部分修改。 |
| `write` | `path`、`content` | 创建缺失父目录，写入 UTF-8，覆盖已有文件。 |

`edit` 优先精确匹配，必要时使用上游的 Unicode／空白归一化匹配；保留未涉及的原始行、UTF-8 BOM 和文件换行风格。结果的 `details` 包含 `diff`、`patch`、`firstChangedLine`。兼容旧的 `{path, oldText, newText}` 调用以及被字符串化的 `edits`，在 schema 校验前统一转换。

`read` 识别 JPEG、PNG、GIF、WebP、BMP。默认图片上限为 2000×2000，base64 上限为 4.5 MiB；超限时缩放／重新编码，BMP 转 PNG。使用 Pillow，不能保证编码结果与 pi 的图片库完全相同。无法处理的图片返回说明，不把二进制内容当文本塞给模型。

`bash` 非零退出码会成为 `isError` 工具结果。`structuredContent` 包含退出码、耗时、最多 1 MiB 的头尾输出。超时和取消时，本地 POSIX 后端终止进程组；Windows 默认后端仅终止直接子进程，尚未验证 Windows 行为。命令退出后仍被后台子进程占用的输出管道有 100ms 空闲收尾窗口。完整输出文件不会自动删除，宿主负责按保留策略清理 `fullOutputPath`。

完整输出默认放在系统临时目录。需要与会话一起保留时，显式指定 `output_dir`：

```python
from agent_runtime import Agent, LocalSession
from agent_runtime.tools import create_coding_tools

session = LocalSession("conversation-123", directory="./sessions")
tools = create_coding_tools(
    cwd="/workspace/project",
    bash_options={"output_dir": session.directory / "outputs"},
)
agent = Agent(session=session, tools=tools)
```

指定目录后，截断产生的完整输出文件会在工具返回前执行 `flush` / `fsync`，并同步目录项；这一保证面向 POSIX 本地文件系统。Session 记录仍保存有界输出及完整文件路径，超大内容不会重复塞入日志。宿主需保留这些文件并保证恢复时路径可访问；会话分叉复制日志，不复制引用的输出文件，因此清理父会话时需保留仍被引用的文件。尚未完成的命令输出不作为可恢复的工具结果。

## 配置和替换后端

工厂选项由宿主传入，不交给模型修改：

```python
from agent_runtime.tools import ImageResizeOptions, create_coding_tools

tools = create_coding_tools(
    cwd="/workspace/project",
    read_options={
        "resize_options": ImageResizeOptions(max_width=1600, max_height=1600),
        "auto_resize_images": True,
    },
    bash_options={
        "shell_path": "/bin/bash",
        "command_prefix": "set -o pipefail",
    },
)
```

`read_options`、`bash_options`、`edit_options`、`write_options` 分别透传到对应工厂，均支持 `operations` 替换执行后端。接口与方法签名见 [`operations.py`](../src/agent_runtime/tools/operations.py) 和 [`bash.py`](../src/agent_runtime/tools/bash.py)：

- `ReadOperations`：异步 `access(path)`、`read_file(path) -> bytes`。
- `WriteOperations`：异步 `mkdir(path)`、`write_file(path, content: str)`。
- `EditOperations`：异步 `access`、`read_file`、`write_file`。
- `BashOperations`：异步 `exec(command, cwd, *, on_data, signal, timeout, env) -> int | None`。使用同步 `on_data(bytes)` 提交输出，在返回前结束输出，并负责落实超时和取消。

可继承 `LocalFileOperations` / `LocalBashOperations`，也可实现相同接口，将 I/O 接到容器或远程环境。文件操作失败应抛出异常。自定义文件后端供 `edit` 和 `write` 共用同一个实例时，才能共用对应的修改队列。

`bash` 还支持同步或异步 `spawn_hook(context)`，接收并返回包含 `command`、`cwd`、`env` 的字典。默认继承当前进程环境；可在此限制传给子进程的环境：

```python
tools = create_coding_tools(
    "/workspace/project",
    bash_options={
        "spawn_hook": lambda context: {
            **context,
            "env": {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "en_US.UTF-8"},
        },
    },
)
```

## 审批、恢复与边界

使用现有 `before_tool_call` 控制执行。例如暂时禁止命令执行：

```python
def approve(context, signal):
    if context["toolCall"]["name"] == "bash":
        return {"block": True, "reason": "当前会话未授权命令执行"}

agent = Agent(tools=tools, before_tool_call=approve)
```

异步审批可以在该钩子等待外部决定。直接调用 `tool.execute()` 会绕过 Runtime 的 schema 校验、审批和 Session，应由服务通过 `Agent` 正常调用。

`read` 标记为 `replay="safe"`，允许恢复时重新读取；文件可能已经变化，因此不保证读到之前时刻的字节。`bash`、`edit`、`write` 标记为 `replay="never"`：已保存结果直接复用，若已开始却没有可靠结果，恢复时抛出 `ToolRecoveryRequired`，等待宿主核实，避免盲目重复副作用。详见 [Session 恢复](sessions.md)。

恢复必须重新装配原来的工作目录、后端和钩子；这些资源不会被 Session 序列化。工具版本默认是 `pi-tools-1`，自定义后端语义改变时应显式修改 `tool.version`，让恢复校验发现变化。

同一事件循环内，`edit` 与 `write` 对相同规范路径串行修改，并等待正在进行的 I/O 完成后释放队列。这是进程内队列，不涉及 DB 表锁；不提供跨进程修改互斥。与 pi 一样，文件写入不是可回滚事务，进程强制退出或磁盘故障可能留下未完成文件，Session 不会自动还原文件系统。

`cwd` 是相对路径的基准，不是沙箱：工具允许绝对路径、`..` 和符号链接，`bash` 具有宿主进程的执行权限。B 端租户隔离应在容器／执行后端落实。该模块不实现 Redis 接管、MCP 或权限管理服务。
