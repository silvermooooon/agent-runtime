"""Optional user memory. Nothing is enabled until tools or a dream are explicitly assembled."""

from .dreaming import run_memory_dream
from .git import GitMemoryError, GitMemoryStore, MemoryPublishUnknown
from .local import LocalMemoryStore
from .store import MemoryConflict, MemoryEntry, MemorySource, MemoryStore
from .tools import create_memory_tools, load_memory_index

__all__ = [
    "MemoryStore",
    "MemoryEntry",
    "MemorySource",
    "MemoryConflict",
    "LocalMemoryStore",
    "GitMemoryStore",
    "GitMemoryError",
    "MemoryPublishUnknown",
    "create_memory_tools",
    "load_memory_index",
    "run_memory_dream",
]
