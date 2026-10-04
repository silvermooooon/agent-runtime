"""Overridable async filesystem backends and pi's per-file mutation queue."""

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Protocol
from weakref import WeakKeyDictionary


async def settle(awaitable):
    """Keep in-flight I/O owned until settled, even under repeated task cancellation."""
    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        # Retrieve failures without leaking an unobserved task exception.
        if not task.cancelled():
            task.exception()
        raise asyncio.CancelledError
    return task.result()


class ReadOperations(Protocol):
    async def access(self, path: str) -> None: ...
    async def read_file(self, path: str) -> bytes: ...


class WriteOperations(Protocol):
    async def mkdir(self, path: str) -> None: ...
    async def write_file(self, path: str, content: str) -> None: ...


class EditOperations(ReadOperations, Protocol):
    async def write_file(self, path: str, content: str) -> None: ...


class LocalFileOperations:
    """Can be subclassed or replaced with a duck-typed SSH/container backend."""

    async def access(self, path: str) -> None:
        await settle(asyncio.to_thread(Path(path).stat))

    async def read_file(self, path: str) -> bytes:
        return await settle(asyncio.to_thread(Path(path).read_bytes))

    async def mkdir(self, path: str) -> None:
        await settle(asyncio.to_thread(Path(path).mkdir, parents=True, exist_ok=True))

    async def write_file(self, path: str, content: str) -> None:
        await settle(asyncio.to_thread(Path(path).write_bytes, content.encode("utf-8")))


_queues = WeakKeyDictionary()


@asynccontextmanager
async def file_mutation(path: str, namespace=None):
    """Serialize write/edit for each canonical path in an event loop, across factories.

    This is an in-process queue, not a filesystem lock or a cross-worker lease.
    Custom backends get separate namespaces; share their instance to share the queue.
    """
    loop = asyncio.get_running_loop()
    queues = _queues.setdefault(loop, {})
    key = (id(namespace) if namespace is not None else None, os.path.realpath(path))
    entry = queues.setdefault(key, [asyncio.Lock(), 0])
    entry[1] += 1
    try:
        async with entry[0]:
            yield
    finally:
        entry[1] -= 1
        if not entry[1]:
            del queues[key]
