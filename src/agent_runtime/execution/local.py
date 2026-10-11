"""Trusted-demo execution: separate environments/directories, same OS account and privileges."""

import asyncio
import hashlib
import json
import os
import tempfile
from pathlib import Path

from ..tools import LocalBashOperations, LocalFileOperations
from ..tools.operations import settle


class LocalExecutionBackend(LocalFileOperations):
    """Implements all four tools' operations. No sandbox, account management or process pool."""

    def __init__(self, identity, directory=None, *, base_env=None, shell_path=None, env=None):
        if not identity.tenant_id or not identity.user_id:
            raise ValueError("User execution requires tenant_id and user_id")
        self.identity = identity
        config = os.environ if env is None else env
        root = directory or config.get("AGENT_WORKSPACE_DIR") or ".agent-runtime/workspaces"
        scope = json.dumps([identity.tenant_id, identity.user_id])
        self.directory = (
            Path(root).expanduser().resolve() / hashlib.sha256(scope.encode()).hexdigest()
        )
        self.cwd = str(self.directory / "work")
        self.home = str(self.directory / "home")
        self.output_dir = self.directory / "outputs"
        self.base_env = (
            dict(base_env) if base_env is not None else {"PATH": os.defpath, "LANG": "C.UTF-8"}
        )
        self._bash = LocalBashOperations(shell_path, cleanup_background=True)

    async def prepare(self):
        def prepare():
            for path in (self.cwd, self.home, self.directory / "tmp"):
                Path(path).mkdir(parents=True, exist_ok=True, mode=0o700)

        await settle(asyncio.to_thread(prepare))

    async def exec(self, command, cwd, *, on_data, signal, timeout, env):
        signal.throw_if_aborted()
        await self.prepare()
        temporary = await settle(
            asyncio.to_thread(tempfile.TemporaryDirectory, dir=self.directory / "tmp")
        )
        try:
            # A new mapping for every call; never mutate os.environ or persist credentials in HOME.
            variables = {
                **self.base_env,
                **env,
                "HOME": self.home,
                "TMPDIR": temporary.name,
                "TMP": temporary.name,
                "TEMP": temporary.name,
            }
            return await self._bash.exec(
                command, cwd, on_data=on_data, signal=signal, timeout=timeout, env=variables
            )
        finally:
            await settle(asyncio.to_thread(temporary.cleanup))
