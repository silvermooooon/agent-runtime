"""Storage interface for skill discovery, instructions, and named references."""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class SkillInfo:
    name: str
    description: str


@dataclass(frozen=True)
class Skill(SkillInfo):
    content: str
    references: tuple[str, ...] = ()


class SkillStore(ABC):
    """Subclass for DB/S3/service storage; tool inputs never contain backend addresses.

    The host supplies a store with the appropriate tenant/authorization scope.
    Loading is read-only, so unfinished calls may be retried by Session recovery.
    """

    @abstractmethod
    async def discover(self) -> list[SkillInfo]:
        """Return available names and descriptions, without instruction/reference bodies."""
        ...

    @abstractmethod
    async def load(self, name: str) -> Skill:
        """Load full instructions and the available reference names."""
        ...

    @abstractmethod
    async def load_reference(self, name: str, reference: str) -> str:
        """Load the complete text of one reference belonging to a skill."""
        ...
