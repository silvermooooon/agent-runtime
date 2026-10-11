"""Host-only credential storage. Agent-facing descriptions never contain values."""

import re
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass

from ..sessions import SessionInfo


class SecretError(RuntimeError):
    """A credential could not be read; messages must not contain plaintext values."""


@dataclass(frozen=True)
class SecretInfo:
    ref: str
    description: str
    env_names: tuple[str, ...]

    def to_dict(self):
        return asdict(self)


def validate_values(values):
    if not isinstance(values, dict) or not values:
        raise ValueError("Credentials require a non-empty environment mapping")
    for name, value in values.items():
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("Invalid credential environment variable name")
        if not isinstance(value, str) or not value or "\0" in value:
            raise ValueError("Credential values must be non-empty strings without NUL")


class SecretStore(ABC):
    """Bound to a user; put/resolve/delete are host APIs, never model tools.

    The host authenticates users and serializes administrative writes. Resolving on each execution
    observes deletion/rotation, but cannot revoke values already injected into a running process.
    """

    def __init__(self, identity: SessionInfo):
        if not identity.tenant_id or not identity.user_id:
            raise ValueError("Credentials require tenant_id and user_id")
        self.identity = identity

    def check_identity(self, identity):
        if (identity.tenant_id, identity.user_id) != (
            self.identity.tenant_id,
            self.identity.user_id,
        ):
            raise ValueError("Credential store belongs to another tenant or user")

    @abstractmethod
    async def put(self, ref, values, *, description="") -> SecretInfo: ...

    @abstractmethod
    async def describe(self, ref) -> SecretInfo: ...

    @abstractmethod
    async def list(self) -> list[SecretInfo]: ...

    @abstractmethod
    async def resolve(self, ref) -> dict[str, str]: ...

    @abstractmethod
    async def delete(self, ref): ...
