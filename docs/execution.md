# 执行环境与凭据管理

当前实现面向可信内部 Demo：每个用户有独立工作目录，每条命令有独立环境和临时目录。
**没有 Linux 多账号、容器、chroot、文件／网络权限沙箱或 CPU／内存配额。**
命令和文件工具仍使用服务进程的操作系统权限，root 启动就是 root 权限。
目录分开不能阻止绝对路径、`..`、符号链接或命令访问其他用户、Session、Memory、密钥文件。
对不互信的用户开放前，应替换执行后端；部署允许时也可先统一用一个低权限服务账号运行。

## 组件边界

- `agent_runtime.secrets.SecretStore`：宿主侧保存、描述、解析和删除用户凭据。
- `agent_runtime.execution.LocalExecutionBackend`：默认本地执行，实现现有四个工具的 operations。
- `create_execution_tools`：普通工具装配；验证身份和允许的引用，在 bash 调用时解析凭据。

Agent loop 不保存密钥、不创建账号、不知道后端运行在本地还是远程。
SDK 不增加调度服务或分布式锁。宿主负责认证授权、凭据管理入口、Session 单写者和资源额度。
实例按租户和用户绑定；同一用户的多会话可以共用 Store 和后端。

## 最小装配

```python
from agent_runtime import Agent, LocalSession
from agent_runtime.execution import LocalExecutionBackend, create_execution_tools

session = LocalSession(tenant_id="company-a", user_id="alice")
backend = LocalExecutionBackend(
    session.info,
    directory="/data/workspaces",
    base_env={"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"},
)
tools = await create_execution_tools(session=session, backend=backend)
agent = Agent(session=session, tools=tools)
await agent.prompt("在工作目录中编写并运行一个脚本。")
```

没有 backend 参数时，根据 Session 身份创建默认 LocalExecutionBackend；缺少 tenant_id 或
user_id 时拒绝装配。后端或凭据 Store 属于其他用户时也拒绝装配和执行。
不根据模型参数切换用户，SessionInfo 不是认证凭据。

本地目录按 `(tenant_id, user_id)` 的哈希分组：

```text
AGENT_WORKSPACE_DIR/<scope-hash>/
  work/       默认 cwd，用户的持久工作文件
  home/       HOME，供 bash 和文件工具的 ~/ 使用
  tmp/        每次调用创建一个随机临时子目录，完成／超时／取消后删除
  outputs/    截断输出的完整脱敏文件，由宿主按保留策略清理
```

工作目录和 HOME 是按用户持久共享的，不是每次执行恢复成空目录。同一用户的并行任务
仍可能修改同一个文件，宿主应按业务约定组织文件和并发写入。
临时文件的清理不撤销工作目录修改，也不是文件的安全擦除。

每条命令新建环境映射，基础环境默认只有 `PATH=os.defpath`、`LANG=C.UTF-8`，再注入本次
授权凭据和后端提供的 HOME/TMPDIR/TMP/TEMP。不修改 `os.environ`，不自动继承服务的
OPENAI_API_KEY、数据库密码、主密钥、代理设置、虚拟环境 PATH 或 shell 启动配置。
`base_env` 是可信宿主配置，应只放非敏感基础变量；敏感值通过 SecretStore 按次请求。
需要使用虚拟环境时，可在命令中使用解释器绝对路径，或显式配置 PATH。

每次调用有独立进程组。POSIX 本地执行模块在正常退出、超时、取消后终止该组中的残留
进程，再清理临时目录；基础 `LocalBashOperations` 仍可由其他调用方选择保留后台进程。
主动 `setsid` 脱离进程组的程序不受该清理保证，进程崩溃也可能留下临时目录。
当前后端不是后台服务托管器；严格进程回收需要部署层 cgroup／容器。
Windows 只能终止直接子进程，未验证；Linux 行为需在实际部署环境验证权限和目录配置。

## 保存密码：只由宿主调用

安装可选依赖：`uv sync --extra secrets`。使用 `cryptography` 的
[Fernet](https://cryptography.io/en/latest/fernet/) 实现认证加密，不自定义密码算法。

```python
from agent_runtime.secrets import LocalSecretStore

store = LocalSecretStore(session.info)  # AGENT_SECRETS_DIR / AGENT_SECRETS_KEY

# 服务端表单/API 收到的值；不能来自模型生成的工具参数或聊天消息。
await store.put(
    "project-api",
    {"PROJECT_API_TOKEN": value_from_secure_form},
    description="访问项目服务的凭据",
)
catalog = await store.list()  # SecretInfo，只含 ref/description/env_names
info = await store.describe("project-api")
# await store.delete("project-api")    # 宿主侧删除
```

主密钥必须由宿主提供，可以通过 `key=` 或环境变量传入，缺少或格式错误直接拒绝启动
该组件；不会生成一个临时密钥后假装已经持久化。使用 `Fernet.generate_key()` 生成一次，
安全保存并独立备份。丢失主密钥将无法解密旧数据；当前不提供自动主密钥轮换或恢复服务。
不要把主密钥放进用户工作区、对话或命令参数。SDK 不读取用户聊天来保存密码。

LocalSecretStore 每个用户、每个 ref 一个加密文件；文件内容包括身份、ref、环境绑定和
描述，解密后验证身份与引用。文件名使用哈希，单文件原子替换、fsync，权限为 0600，
新建用户目录为 0700；依赖 POSIX 文件系统语义。主密钥不写入该目录。
put 替换整个凭据，支持更新值；管理写入由宿主串行处理，没有版本历史或锁。
这保护的是静态存储，不保护掌握主密钥的 root 服务自身，也不能阻止 root bash 读取存储。

`SecretStore.resolve(ref)` 是供可信执行层读取真实值的宿主接口，不能装配为 Agent 工具。
后续 Vault、DB、KMS 等实现继承同一接口，错误信息也必须避免包含明文。
导入模块不会读取密钥、解密文件或启用工具。

## Agent 看到什么，执行时发生什么

宿主显式圈选允许使用的引用，默认一个也不授权：

```python
tools = await create_execution_tools(
    session=session,
    backend=backend,
    secrets=store,
    credential_refs=["project-api"],
)
agent = Agent(session=session, tools=tools)
```

bash 描述中加入凭据目录，只有引用、用途描述和环境变量名，没有实际值：

```json
{"ref": "project-api", "description": "访问项目服务的凭据", "env_names": ["PROJECT_API_TOKEN"]}
```

模型可以编写通过 `os.environ["PROJECT_API_TOKEN"]` 使用凭据的脚本，再请求：

```json
{"command": "python query_project.py", "credentials": ["project-api"], "timeout": 30}
```

未传 credentials 时不注入任何凭据；不提供“获取密码”的工具，也不把 ref 当作授权令牌。
现有 `before_tool_call` 审批先执行，通过后才解析值。执行时再次检查引用是否在允许列表，
逐个 resolve，拒绝重复引用、重复环境变量和已改变的变量名绑定。
PATH/HOME/TMPDIR、BASH_ENV、ENV、LD_* 等执行控制变量不能作为凭据绑定；变量名由平台配置。
目录描述是宿主提供的元数据，不应包含密码。

凭据值每次执行重新读取，因此原变量名不变的值更新、删除会在后续执行中生效。
改变变量名后必须重新装配工具，避免模型按旧接口使用凭据。
更新宿主允许列表也需重新装配；更动态的授权可通过现有审批钩子或自定义 SecretStore 实现。
无法撤回已经注入到运行中进程及其子进程里的值。

## 输出、审计与恢复

本次选中凭据的原文和常见 JSON 转义形式，会在 bash stdout/stderr 进入输出缓冲器之前
替换为 `[REDACTED]`，包括跨数据块的完整匹配。因此实时事件、工具结果以及截断后的
完整输出文件都沿用同一条脱敏路径；普通后端异常信息也做相同匹配。
短尾部可能暂存到下一块或执行结束后再输出，以处理跨块匹配。

**这只是减少意外输出，不保证 Agent 无法获得密码。** 任意 shell 能读取注入变量，
也能变换编码、拆分输出、写入文件或通过网络外传。只匹配本次已选凭据，不为其他工具
建立全局脱敏器；如果命令将密码保存到文件，之后的 read 工具可能读到它。
需要“Agent 控制的程序绝不能接触密码”时，使用持有凭据的可信服务代执行受控操作。

工具调用原参数记录 command、credentials 引用；成功结果的 details.execution 记录使用的
引用和环境变量名。审批拒绝、解析失败、执行失败沿用现有工具结果，不新增审计表。
Session 不保存解析后的 env 或主密钥。宿主配置、自定义后端的调试日志也不应记录它们。

读取工具是 replay=safe，bash/edit/write 是 replay=never。已完成结果使用日志恢复；
已开始但没有可靠结果的 bash 会暂停核实，不自动再次使用凭据执行。
未开始的调用在恢复时按当前权限和凭据值执行，未承诺历史密钥版本重放。
分叉或恢复 Session 不会回滚工作目录、外部系统或凭据更新。
重开后使用同一用户后端重新装配工具即可，资源配置由宿主重新注入。

## 替换执行后端

不需要改 Agent loop 或工具名。传入一个提供下列属性／方法的对象：

| 接口 | 用途 |
| --- | --- |
| identity | SessionInfo，用 tenant_id/user_id 校验绑定 |
| cwd / home | 后端使用的绝对路径；文件工具按这些路径解析相对路径和 ~/ |
| output_dir | **宿主侧**完整输出文件存放目录，宿主保证可恢复访问 |
| async prepare() | 准备运行所需资源，不启动 Agent |
| async access/read_file/mkdir/write_file | 复用现有文件 operations，所有文件访问必须走同一后端 |
| async exec(command, cwd, *, on_data, signal, timeout, env) | 复用 BashOperations，env 是本次解析的凭据变量；后端提供基础环境与临时目录 |

exec 用同步 `on_data(bytes)` 输出，在返回／抛错前停止发送，落实超时和取消，不记录原始 env。
远程结果未知时抛出现有 `ToolRecoveryRequired`，保留核实流程；普通明确失败可以抛异常。
Linux 多用户后端需在子进程层完整降权，远程后端需安全传输凭据并映射所有文件操作。
当前代码不提供这些后端的空壳或默认降级；实现并测试后显式替换 backend。

工作空间工具版本为 `execution-tools-1`，普通 bash 为 `pi-bash-2`（默认不继承宿主环境）。
后端行为发生不兼容改变时，宿主需更新工具版本并按 Session 恢复规则处理未完成调用。

## 验证与示例

`uv run --extra secrets python examples/execution.py` 使用临时加密存储、真实 shell 和离线模型
响应演示工具调用，无在线 API 请求。测试覆盖密文重开／更新／删除、身份检查、审批顺序、
并发环境、跨块及 JSON 脱敏、完整输出文件、取消清理、工具恢复和后端替换。
它们不证明当前实现拥有 Linux 用户或容器的权限隔离。
