# Skill 加载

`agent_runtime.skills` 提供 `load_skill` 和 `load_skill_reference` 两个专用工具，显式传入 `Agent(tools=...)` 后启用。二者通过 `SkillStore` 调用存储，模型只传 Skill 名称及参考项名称。默认的 `LocalSkillStore` 读取本地目录；后续继承同一接口即可接入 DB 或 S3。

## 本地目录与环境配置

在项目 `.env` 中设置：

```dotenv
AGENT_SKILLS_DIR=/data/skills
```

目录约定为每个直接子目录一个 Skill，以目录名称作为调用名称：

```text
/data/skills/
  writing/
    SKILL.md
    references/
      style.md
    templates/
      update.txt
```

`SKILL.md` 可以有标准 YAML 元数据：

```markdown
---
name: writing
description: 编写简短的项目进展说明。
---

# 项目进展说明

先使用 load_skill_reference 加载 references/style.md，再按规则撰写。
```

本地调用名称为 `writing`，元数据中的 `name` 建议与目录名一致；不会另外分配 ID。`description` 从 YAML 中读取，支持多行文本。没有 YAML 头时描述为空。正文原样加载，不自动改写其中的链接或通用 read 指令，因此 Skill 内应使用这两个专用工具。

本地文件按 UTF-8 文本读取。加载 Skill 时只列出参考文件名称，不加载其正文；Skill 目录内的 `references/`、`templates/` 等子目录均可作为参考来源。隐藏文件不参与目录展示。参考内容完整返回，不进行静默截断，也不执行文件中的代码。越出所属 Skill 目录的路径及符号链接会被拒绝。

配置优先级：显式 `directory` > `AGENT_SKILLS_DIR`。相对路径基于进程工作目录，支持 `~`。未配置目录或目录不存在时，在构造本地存储时直接报错。使用自定义 `store` 则无需本地目录配置。

SDK 沿用现有环境配置方式，读取进程环境；使用 `uv run --env-file .env ...` 将 `.env` 加载到进程，生产环境也可直接注入变量。`LocalSkillStore(env={})` 禁用环境继承，适合宿主按租户指定配置。

## 装配与发现

```python
import json
from dataclasses import asdict
from agent_runtime import Agent
from agent_runtime.skills import LocalSkillStore, create_skill_tools

store = LocalSkillStore()  # 从 AGENT_SKILLS_DIR 读取根目录
catalog = await store.discover()

agent = Agent(
    tools=create_skill_tools(store=store),
    system_prompt=(
        "先加载适合任务的 Skill，再按需加载其参考项。可用 Skill："
        + json.dumps([asdict(info) for info in catalog], ensure_ascii=False)
    ),
)
await agent.prompt("写一段项目进展：测试已通过，下一步是代码评审。")
```

工具调用参数：

```json
{"name": "writing"}
```

```json
{"name": "writing", "reference": "references/style.md"}
```

第一个参数对象用于 `load_skill`，第二个用于 `load_skill_reference`。加载 Skill 返回完整说明及可用参考项名称。参考项名称由存储后端解释，本地实现使用相对于该 Skill 目录的路径；数据库实现可以使用逻辑名称，例如 `style-guide`。

目录发现由外层调用 `await store.discover()`，返回 `SkillInfo(name, description)` 列表，不包含正文或参考内容。本版模型工具只有上述两个，不包含 `skill_search`。示例把目录摘要放入 system prompt；SaaS 宿主可以先筛选，再把候选目录交给模型。`create_skill_tools()` 本身不会扫描或自动注入全部目录。

需要分别装配时，使用 `create_load_skill_tool(store)`、`create_load_skill_reference_tool(store)`。也可以通过已有 `assemble_tools` 的 builtin 分支选择这些 `AgentTool`。

## 存储扩展

继承 `SkillStore` 并实现三个异步方法：

| 方法 | 返回值 |
| --- | --- |
| `discover()` | `list[SkillInfo]`，名称与描述 |
| `load(name)` | `Skill(name, description, content, references)`，说明与参考项名称 |
| `load_reference(name, reference)` | 指定参考项的完整文本 |

```python
from agent_runtime.skills import Skill, SkillInfo, SkillStore, create_skill_tools

class DatabaseSkills(SkillStore):
    def __init__(self, repository, tenant_id):
        self.repository = repository
        self.tenant_id = tenant_id

    async def discover(self):
        rows = await self.repository.list_skills(self.tenant_id)
        return [SkillInfo(row.name, row.description) for row in rows]

    async def load(self, name):
        row = await self.repository.get_skill(self.tenant_id, name)
        return Skill(row.name, row.description, row.content, tuple(row.references))

    async def load_reference(self, name, reference):
        return await self.repository.get_reference(self.tenant_id, name, reference)

tools = create_skill_tools(store=DatabaseSkills(repository, tenant_id))
```

以上是接口用法示意，SDK 当前仅实现本地存储。宿主负责注入已限定租户和权限范围的 store，凭据不属于工具参数或目录摘要。三个方法均为只读操作；可以共享无会话可变状态的 store，自定义实现须支持宿主的并发方式。

## 生命周期与恢复

两个工具复用原有参数校验、审批钩子、工具事件和 Session 日志。校验失败或审批拒绝时不读取内容；文件不存在、格式错误等成为普通工具错误。中途取消会停止等待存储调用，本地线程已开始的只读 I/O 可能仍会结束。

完整的工具返回值由现有 `tool_returned` / `tool_completed` 流程保存，随后进入模型上下文。没有额外的 Skill 状态库或加载缓存。恢复时复用已保存的内容，即使源文件已经修改或删除，也不会重新读取已完成的加载结果。

工具标记为 `replay="safe"`：已开始但尚未保存返回值的读取可以重试，并获取当时存储中的内容。`skills-1` 是工具接口版本，不是 Skill 文档版本；当前不对尚未加载的文件做快照。压缩与分叉继续使用现有 Session 规则，压缩后的模型上下文是否保留旧说明由上下文策略决定，完整日志仍保留加载历史。

## 示例与测试

无需模型和 API Key 的示例使用真实本地 Skill 文件：

```bash
AGENT_SKILLS_DIR=examples/skill_catalog uv run python examples/skills.py
```

也可通过参数覆盖目录：

```bash
uv run python examples/skills.py --directory examples/skill_catalog
```

配置 `.env` 的目录后，`uv run --env-file .env python examples/skills.py --agent` 使用真实模型。离线测试覆盖环境配置、独立存储实例、替换后端、审批、取消、路径边界以及源文件变更后的 Session 恢复。

独立的真实模型验证入口为 `uv run --env-file .env python tests/live_skills.py --live`，只允许官方 Responses API 和 `gpt-6-luna`。实测记录见 [在线测试说明](live-testing.md)。
