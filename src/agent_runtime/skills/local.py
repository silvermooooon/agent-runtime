"""Local SkillStore: <directory>/<skill name>/SKILL.md and bundled references."""

import asyncio
import os
from collections.abc import Mapping
from pathlib import Path

import yaml

from .store import Skill, SkillInfo, SkillStore


class LocalSkillStore(SkillStore):
    def __init__(
        self, directory: str | Path | None = None, *, env: Mapping[str, str] | None = None
    ):
        env = os.environ if env is None else env
        directory = directory if directory is not None else env.get("AGENT_SKILLS_DIR", "").strip()
        if not directory:
            raise ValueError("Set AGENT_SKILLS_DIR or pass a skill directory explicitly")
        self.directory = Path(directory).expanduser().resolve()
        if not self.directory.is_dir():
            raise NotADirectoryError(f"Skill directory does not exist: {self.directory}")

    def _skill_directory(self, name):
        if not name or name in (".", "..") or Path(name).name != name:
            raise ValueError("Skill name must be a single directory name")
        path = (self.directory / name).resolve()
        if not path.is_relative_to(self.directory) or path == self.directory:
            raise ValueError("Skill must be inside the configured skill directory")
        return path

    @staticmethod
    def _file(directory, reference):
        path = (directory / reference).resolve()
        if Path(reference).is_absolute() or not path.is_relative_to(directory):
            raise ValueError("Reference must be inside its skill directory")
        if not path.is_file():
            raise FileNotFoundError(f"Skill file not found: {reference}")
        return path

    @staticmethod
    def _description(path):
        # Read YAML metadata only; discovery does not load instruction/reference bodies.
        with path.open(encoding="utf-8-sig") as file:
            if file.readline().strip() != "---":
                return ""
            lines = []
            for line in file:
                if line.strip() == "---":
                    break
                lines.append(line)
            else:
                raise ValueError("Unclosed skill YAML front matter")
        try:
            metadata = yaml.safe_load("".join(lines))
        except yaml.YAMLError:
            raise ValueError("Invalid skill YAML front matter") from None
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict):
            raise ValueError("Skill YAML front matter must be a mapping")
        description = metadata.get("description", "")
        if not isinstance(description, str):
            raise ValueError("Skill description must be text")
        return description.strip()

    def _discover(self):
        result = []
        for child in sorted(self.directory.iterdir()):
            if child.name.startswith(".") or not child.is_dir():
                continue
            # Links outside this store are not advertised as available skills.
            if not child.resolve().is_relative_to(self.directory):
                continue
            if not (child / "SKILL.md").is_file():
                continue
            directory = self._skill_directory(child.name)
            path = self._file(directory, "SKILL.md")
            result.append(SkillInfo(child.name, self._description(path)))
        return result

    async def discover(self):
        return await asyncio.to_thread(self._discover)

    def _load(self, name):
        directory = self._skill_directory(name)
        path = self._file(directory, "SKILL.md")
        references = tuple(
            item.relative_to(directory).as_posix()
            for item in sorted(directory.rglob("*"))
            if item.is_file()
            and item != directory / "SKILL.md"
            and item.resolve().is_relative_to(directory)
            and not any(part.startswith(".") for part in item.relative_to(directory).parts)
        )
        return Skill(
            name, self._description(path), path.read_text(encoding="utf-8-sig"), references
        )

    async def load(self, name):
        return await asyncio.to_thread(self._load, name)

    def _load_reference(self, name, reference):
        directory = self._skill_directory(name)
        self._file(directory, "SKILL.md")  # References belong to an existing skill.
        return self._file(directory, reference).read_text(encoding="utf-8-sig")

    async def load_reference(self, name, reference):
        return await asyncio.to_thread(self._load_reference, name, reference)
