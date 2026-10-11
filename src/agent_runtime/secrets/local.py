"""Small encrypted file store. The master key is supplied separately by the host."""

import asyncio
import hashlib
import json
import os
import tempfile
from pathlib import Path

from ..tools.operations import settle
from .store import SecretError, SecretInfo, SecretStore, validate_values


class LocalSecretStore(SecretStore):
    def __init__(self, identity, directory=None, *, key=None, env=None):
        super().__init__(identity)
        try:
            from cryptography.fernet import Fernet
        except ImportError:
            raise ImportError("Install agent-runtime[secrets] for LocalSecretStore") from None
        env = os.environ if env is None else env
        key = key if key is not None else env.get("AGENT_SECRETS_KEY")
        if not key:
            raise ValueError("Provide a Fernet key via key or AGENT_SECRETS_KEY")
        try:
            self._cipher = Fernet(key)
        except (ValueError, TypeError):
            raise ValueError("Invalid credential encryption key") from None
        self._scope = [identity.tenant_id, identity.user_id]
        scope_hash = hashlib.sha256(json.dumps(self._scope).encode()).hexdigest()
        root = directory or env.get("AGENT_SECRETS_DIR") or ".agent-runtime/secrets"
        self.directory = Path(root).expanduser().resolve() / scope_hash

    def _path(self, ref):
        if not isinstance(ref, str) or not ref:
            raise ValueError("Credential reference must be a non-empty string")
        path = self.directory / (hashlib.sha256(ref.encode()).hexdigest() + ".secret")
        if not path.resolve().is_relative_to(self.directory):
            raise SecretError("Credential path escapes its user directory")
        return path

    def _read(self, ref):
        from cryptography.fernet import InvalidToken

        try:
            encrypted = self._path(ref).read_bytes()
        except FileNotFoundError:
            raise SecretError("Credential unavailable") from None
        try:
            data = json.loads(self._cipher.decrypt(encrypted))
            if data["scope"] != self._scope or data["ref"] != ref:
                raise ValueError("Identity mismatch")
            validate_values(data["values"])
            if not isinstance(data["description"], str):
                raise ValueError("Invalid description")
            return data
        except (InvalidToken, ValueError, TypeError, KeyError):
            raise SecretError("Credential is unreadable or belongs to another identity") from None

    @staticmethod
    def _info(data):
        return SecretInfo(data["ref"], data["description"], tuple(sorted(data["values"])))

    @staticmethod
    def _sync(directory):
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    async def put(self, ref, values, *, description=""):
        path = self._path(ref)
        validate_values(values)
        if not isinstance(description, str):
            raise ValueError("Credential description must be text")
        data = {
            "scope": self._scope,
            "ref": ref,
            "description": description,
            "values": dict(values),
        }

        def write():
            encrypted = self._cipher.encrypt(json.dumps(data).encode())
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as file:
                    temporary = Path(file.name)
                    file.write(encrypted)
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(temporary, path)
                self._sync(path.parent)
            finally:
                if temporary:
                    temporary.unlink(missing_ok=True)
            return self._info(data)

        return await settle(asyncio.to_thread(write))

    async def describe(self, ref):
        return self._info(await settle(asyncio.to_thread(self._read, ref)))

    async def resolve(self, ref):
        return (await settle(asyncio.to_thread(self._read, ref)))["values"]

    async def list(self):
        def descriptions():
            from cryptography.fernet import InvalidToken

            result = []
            for path in sorted(self.directory.glob("*.secret")):
                try:
                    # Recover the opaque reference, then apply the same integrity/scope checks.
                    data = json.loads(self._cipher.decrypt(path.read_bytes()))
                    ref = data["ref"]
                    if path != self._path(ref):
                        raise ValueError("Reference mismatch")
                    result.append(self._info(self._read(ref)))
                except FileNotFoundError:
                    continue
                except (InvalidToken, ValueError, TypeError, KeyError):
                    raise SecretError("Credential catalog is unreadable") from None
            return sorted(result, key=lambda info: info.ref)

        return await settle(asyncio.to_thread(descriptions))

    async def delete(self, ref):
        def delete():
            path = self._path(ref)
            if path.exists():
                path.unlink()
                self._sync(path.parent)

        await settle(asyncio.to_thread(delete))
