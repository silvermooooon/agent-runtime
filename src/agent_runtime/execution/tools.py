"""Assemble the existing coding tools against one user-bound execution backend."""

import json
from dataclasses import replace

from ..secrets.store import validate_values
from ..sessions import SessionError
from ..tools import create_bash_tool, create_coding_tools
from ..types import AbortSignal
from ._redact import Redactor
from .local import LocalExecutionBackend

RESERVED_ENV = {
    "PATH",
    "HOME",
    "TMPDIR",
    "TMP",
    "TEMP",
    "BASH_ENV",
    "ENV",
    "IFS",
    "CDPATH",
    "SHELLOPTS",
    "BASHOPTS",
    "GLOBIGNORE",
    "PYTHONPATH",
    "PYTHONHOME",
}


def check_bindings(names):
    if any(
        name in RESERVED_ENV or name.startswith(("LD_", "DYLD_", "BASH_FUNC_")) for name in names
    ):
        raise ValueError("Credentials cannot override execution environment controls")


def check_scope(session, backend, secrets):
    identity = session.info
    if not identity.tenant_id or not identity.user_id:
        raise ValueError("User execution requires tenant_id and user_id")
    if (identity.tenant_id, identity.user_id) != (
        backend.identity.tenant_id,
        backend.identity.user_id,
    ):
        raise ValueError("Execution backend belongs to another tenant or user")
    if secrets is not None:
        secrets.check_identity(identity)


class _CredentialOutput:
    """Per-call adapter; no shared credential state and no raw output spool."""

    def __init__(self, backend, values):
        self.backend = backend
        self.redactor = Redactor(values.values())

    async def exec(self, command, cwd, *, on_data, **options):
        def data(chunk):
            filtered = self.redactor.feed(chunk)
            if filtered:
                on_data(filtered)

        try:
            return await self.backend.exec(command, cwd, on_data=data, **options)
        except SessionError:
            raise
        except Exception as error:
            raise RuntimeError(self.redactor.text(str(error))) from None
        finally:
            remaining = self.redactor.feed(b"", final=True)
            if remaining:
                on_data(remaining)


async def create_execution_tools(*, session, backend=None, secrets=None, credential_refs=()):
    """Explicit opt-in. The host selects allowed refs; models only request from that list.

    backend provides identity/cwd/home/output_dir/prepare plus existing file and bash operations.
    No Linux accounts, containers, implicit credentials or authentication are created here.
    """
    backend = backend if backend is not None else LocalExecutionBackend(session.info)
    check_scope(session, backend, secrets)
    refs = tuple(dict.fromkeys(credential_refs))
    if refs and secrets is None:
        raise ValueError("Credential references require a SecretStore")
    catalog = {}
    for ref in refs:
        info = await secrets.describe(ref)
        check_bindings(info.env_names)
        catalog[ref] = info
    await backend.prepare()
    file_options = {"operations": backend, "home": backend.home}
    tools = create_coding_tools(
        backend.cwd,
        read_options=file_options,
        edit_options=file_options,
        write_options=file_options,
        bash_options={"operations": backend, "output_dir": backend.output_dir, "env": {}},
    )
    bash = next(tool for tool in tools if tool.name == "bash")
    bash.parameters["additionalProperties"] = False
    if catalog:
        bash.parameters["properties"]["credentials"] = {
            "type": "array",
            "items": {"type": "string", "enum": list(catalog)},
            "uniqueItems": True,
            "description": "Credential references to inject only for this execution; omit for none",
        }
        bash.description += (
            " Available credentials (references and environment names only): "
            + json.dumps([info.to_dict() for info in catalog.values()], ensure_ascii=False)
            + ". Reference variables from scripts; never print or persist their values."
        )

    async def execute_bash(call_id, args, signal=None, on_update=None):
        signal = signal or AbortSignal()
        signal.throw_if_aborted()
        requested = args.get("credentials", [])
        if not isinstance(requested, list) or any(not isinstance(ref, str) for ref in requested):
            raise ValueError("credentials must be a list of references")
        if len(set(requested)) != len(requested) or any(ref not in catalog for ref in requested):
            raise PermissionError("Credential reference is not permitted for this Agent")
        values = {}
        for ref in requested:
            resolved = await secrets.resolve(ref)  # Re-authorize in a remote store if needed.
            validate_values(resolved)
            check_bindings(resolved)
            if tuple(sorted(resolved)) != tuple(sorted(catalog[ref].env_names)):
                raise PermissionError("Credential bindings changed; reassemble the Agent tools")
            if values.keys() & resolved.keys():
                raise ValueError("Requested credentials have conflicting environment names")
            values.update(resolved)
        signal.throw_if_aborted()
        tool = create_bash_tool(
            backend.cwd,
            operations=_CredentialOutput(backend, values),
            output_dir=backend.output_dir,
            env=values,
        )
        result = await tool.execute(call_id, args, signal, on_update)
        result.details = {
            **(result.details or {}),
            "execution": {"credentials": list(requested), "env_names": sorted(values)},
        }
        return result

    bash.execute = execute_bash
    assembled = []
    for tool in tools:
        original = tool.execute

        async def execute(call_id, args, signal=None, on_update=None, *, invoke=original):
            check_scope(session, backend, secrets)
            return await invoke(call_id, args, signal, on_update)

        assembled.append(replace(tool, execute=execute, version="execution-tools-1"))
    return assembled
