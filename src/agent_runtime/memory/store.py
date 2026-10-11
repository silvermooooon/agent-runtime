"""User-scoped memory contracts. No scheduling, authentication or model dependencies."""

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath

from ..sessions import SessionInfo


@dataclass(frozen=True)
class MemorySource:
    session_id: str
    event_id: str

    def __post_init__(self):
        if not self.session_id or not self.event_id:
            raise ValueError("Memory sources require session_id and event_id")


@dataclass(frozen=True)
class MemoryEntry:
    key: str
    content: str
    version: str
    sources: tuple[MemorySource, ...] = ()
    updated_at: int | None = None

    def to_dict(self):
        return asdict(self)


class MemoryConflict(RuntimeError):
    def __init__(self, key, current):
        super().__init__(f"Memory changed; read and reconcile {key} before writing again")
        self.key, self.current = key, current

    def to_dict(self):
        return {
            "status": "conflict",
            "key": self.key,
            "current": self.current.to_dict() if self.current else None,
        }


def validate_key(key):
    if (
        not isinstance(key, str)
        or not key
        or "\\" in key
        or "\x00" in key
        or any(part.startswith(".") or not part for part in key.split("/"))
        or PurePosixPath(key).is_absolute()
    ):
        raise ValueError("Memory key must be a relative name without hidden or parent segments")
    return key


def check_version(key, current, expected_version):
    if (current.version if current else None) != expected_version:
        raise MemoryConflict(key, current)


def search_entries(entries, query, limit):
    if not query.strip() or not 1 <= limit <= 50:
        raise ValueError("Search requires non-empty query and limit between 1 and 50")
    terms = query.casefold().split()
    matches = []
    for entry in entries:
        text = (entry.key + "\n" + entry.content).casefold()
        if all(term in text for term in terms):
            matches.append(entry)
    matches.sort(key=lambda item: item.key)
    return [
        {
            "key": e.key,
            "snippet": e.content[:500],
            "version": e.version,
            "sources": [asdict(s) for s in e.sources],
        }
        for e in matches[:limit]
    ]


class MemoryStore(ABC):
    """One user's store, reusable across that user's sessions. The host authorizes access."""

    staged = False

    def __init__(self, identity: SessionInfo):
        if not identity.tenant_id or not identity.user_id:
            raise ValueError("Memory requires both tenant_id and user_id")
        self.identity = identity

    def check_identity(self, identity):
        if (identity.tenant_id, identity.user_id) != (
            self.identity.tenant_id,
            self.identity.user_id,
        ):
            raise ValueError("Memory store belongs to another tenant or user")

    @abstractmethod
    async def read(self, key) -> MemoryEntry | None: ...

    @abstractmethod
    async def search(self, query, limit=10) -> list[dict]: ...

    @abstractmethod
    async def write(
        self, key, content, *, sources, expected_version, operation_id=None
    ) -> MemoryEntry: ...

    @abstractmethod
    async def delete(self, key, *, expected_version, operation_id=None): ...
