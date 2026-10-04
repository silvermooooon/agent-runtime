"""Python port of pi's local bash execution, streamed output and timeout semantics."""

import asyncio
import math
import os
import shutil
import signal as process_signal
import subprocess
import time
from pathlib import Path
from typing import Protocol

from ..types import AbortSignal, AgentTool, AgentToolResult, maybe_await
from ._output import OutputAccumulator
from ._truncate import DEFAULT_MAX_BYTES, format_size
from .operations import settle


class BashOperations(Protocol):
    async def exec(self, command, cwd, *, on_data, signal, timeout, env) -> int | None: ...


class _ProcessProtocol(asyncio.SubprocessProtocol):
    def __init__(self, on_data):
        self.on_data = on_data
        self.done = asyncio.get_running_loop().create_future()
        self.process_done = asyncio.get_running_loop().create_future()
        self.exited = False
        self.closed_pipes = set()
        self.idle_timer = None

    def connection_made(self, transport):
        self.transport = transport

    def pipe_data_received(self, fd, data):
        if not self.done.done():
            try:
                self.on_data(data)
                if self.exited:
                    self.arm_idle_timer()
            except Exception as error:
                self.done.set_exception(error)

    def process_exited(self):
        self.exited = True
        if not self.process_done.done():
            self.process_done.set_result(self.transport.get_returncode())
        if self.closed_pipes == {1, 2}:
            self.finish()
        else:
            self.arm_idle_timer()

    def pipe_connection_lost(self, fd, exc):
        if fd in (1, 2):
            self.closed_pipes.add(fd)
        if self.exited and self.closed_pipes == {1, 2}:
            self.finish()

    def arm_idle_timer(self):
        if self.idle_timer:
            self.idle_timer.cancel()
        self.idle_timer = asyncio.get_running_loop().call_later(0.1, self.finish)

    def finish(self):
        if self.idle_timer:
            self.idle_timer.cancel()
        if not self.done.done():
            self.done.set_result(self.transport.get_returncode())


class LocalBashOperations:
    def __init__(self, shell_path=None):
        self.shell_path = shell_path

    async def exec(self, command, cwd, *, on_data, signal, timeout, env):
        signal.throw_if_aborted()
        if not Path(cwd).is_dir():
            raise ValueError(
                f"Working directory does not exist: {cwd}\nCannot execute bash commands."
            )
        shell = self.shell_path or (
            "/bin/bash"
            if Path("/bin/bash").exists()
            else shutil.which("bash") or shutil.which("sh")
        )
        if not shell:
            raise ValueError("No bash shell found; configure shell_path or a custom BashOperations")
        protocol = _ProcessProtocol(on_data)
        transport = None
        watcher = timer = None
        completed = False

        async def spawn():
            nonlocal transport
            transport, _ = await asyncio.get_running_loop().subprocess_exec(
                lambda: protocol,
                shell,
                "-c",
                command,
                cwd=cwd,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=os.name == "posix",
            )

        try:
            await settle(spawn())
            watcher = asyncio.create_task(signal.wait())
            if timeout is not None:
                timer = asyncio.create_task(asyncio.sleep(timeout))
            await asyncio.wait(
                [protocol.done, watcher, *([timer] if timer else [])],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if signal.aborted:
                raise RuntimeError("aborted")
            if timer and timer.done():
                raise TimeoutError(f"timeout:{timeout}")
            code = protocol.done.result()
            completed = True
            return 128 - code if code is not None and code < 0 else code
        finally:
            for task in (watcher, timer):
                if task:
                    task.cancel()
            if transport:
                if not completed:
                    try:
                        if os.name == "posix":
                            os.killpg(transport.get_pid(), process_signal.SIGKILL)
                        elif transport.get_returncode() is None:
                            transport.kill()
                    except ProcessLookupError:
                        pass
                    await settle(protocol.process_done)
                transport.close()
                if not protocol.done.done():
                    await settle(protocol.done)
                elif not protocol.done.cancelled():
                    protocol.done.exception()
            if protocol.idle_timer:
                protocol.idle_timer.cancel()
            await asyncio.gather(*(t for t in (watcher, timer) if t), return_exceptions=True)


def create_bash_tool(
    cwd: str | Path,
    *,
    operations: BashOperations | None = None,
    command_prefix: str | None = None,
    shell_path: str | None = None,
    spawn_hook=None,
    output_dir: str | Path | None = None,
) -> AgentTool:
    cwd = str(Path(cwd).expanduser().absolute())
    ops = operations if operations is not None else LocalBashOperations(shell_path)

    async def execute(call_id, args, signal=None, on_update=None):
        signal = signal or AbortSignal()
        signal.throw_if_aborted()
        timeout = args.get("timeout")
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (float, int))
            or not math.isfinite(timeout)
            or not 0 < timeout <= 2147483.647
        ):
            raise ValueError(
                "Invalid timeout: must be finite, positive and at most 2147483.647 seconds"
            )
        command = args["command"]
        context = {
            "command": f"{command_prefix}\n{command}" if command_prefix else command,
            "cwd": cwd,
            "env": dict(os.environ),
        }
        for name in (
            "PI_SESSION_ID",
            "PI_SESSION_FILE",
            "PI_PROVIDER",
            "PI_MODEL",
            "PI_REASONING_LEVEL",
        ):
            context["env"].pop(name, None)
        if spawn_hook:
            context = await maybe_await(spawn_hook(context))
        signal.throw_if_aborted()
        output = OutputAccumulator(output_dir=output_dir)
        started = time.monotonic()
        last_update = 0
        accepting = True
        timer = None
        updates = []
        update_errors = []

        def emit():
            nonlocal last_update, timer
            timer = None
            last_update = time.monotonic()
            snapshot = output.snapshot()
            try:
                result = on_update(
                    AgentToolResult(
                        [{"type": "text", "text": snapshot["content"]}],
                        details={"truncation": snapshot, "fullOutputPath": output.path}
                        if snapshot["truncated"]
                        else None,
                    )
                )
            except Exception as error:
                # Timed emissions run outside the awaiting task; propagate at settlement.
                update_errors.append(error)
                return
            if result is not None:
                updates.append(asyncio.create_task(maybe_await(result)))

        def data(chunk):
            nonlocal timer
            if not accepting:
                return
            output.append(chunk)
            if on_update:
                delay = 0.1 - (time.monotonic() - last_update)
                if delay <= 0:
                    if timer:
                        timer.cancel()
                    emit()
                elif timer is None:
                    timer = asyncio.get_running_loop().call_later(delay, emit)

        error = None
        try:
            if on_update:
                await maybe_await(on_update(AgentToolResult()))
            try:
                code = await ops.exec(
                    context["command"],
                    context["cwd"],
                    on_data=data,
                    signal=signal,
                    timeout=timeout,
                    env=context["env"],
                )
            except Exception as caught:
                error = caught
                code = None
        finally:
            accepting = False
            if timer:
                timer.cancel()
            try:
                output.finish()
                if on_update:
                    emit()
                snapshot = output.snapshot()
            finally:
                output.close()
            if updates:
                results = await asyncio.gather(*updates, return_exceptions=True)
                update_errors.extend(r for r in results if isinstance(r, BaseException))
            if update_errors:
                raise update_errors[0]
        text = snapshot["content"] or ("" if error else "(no output)")
        details = None
        if snapshot["truncated"]:
            details = {"truncation": snapshot, "fullOutputPath": output.path}
            if snapshot["lastLinePartial"]:
                note = (
                    f"Showing last {format_size(snapshot['outputBytes'])} "
                    f"of line {snapshot['totalLines']} "
                    f"(line is {format_size(output.last_line_bytes)})"
                )
            else:
                note = (
                    f"Showing lines {snapshot['totalLines'] - snapshot['outputLines'] + 1}"
                    f"-{snapshot['totalLines']} of {snapshot['totalLines']}"
                )
                if snapshot["truncatedBy"] == "bytes":
                    note += f" ({format_size(DEFAULT_MAX_BYTES)} limit)"
            text += f"\n\n[{note}. Full output: {output.path}]"
        if error:
            message = str(error)
            if message == "aborted":
                message = "Command aborted"
            elif message.startswith("timeout:"):
                message = f"Command timed out after {timeout} seconds"
            raise RuntimeError(f"{text}\n\n{message}".strip()) from error
        if code is None:
            raise RuntimeError(f"{text}\n\nCommand terminated without an exit code")
        full, truncated = output.full_output()
        structured = {
            "output": full,
            "truncated": truncated,
            "exit_code": code,
            "wall_time_seconds": round(time.monotonic() - started, 1),
        }
        if truncated and output.path:
            structured["full_output_path"] = output.path
        if code:
            text += f"\n\nCommand exited with code {code}"
        return AgentToolResult(
            [{"type": "text", "text": text}],
            details=details,
            structured_content=structured,
            is_error=code != 0,
        )

    return AgentTool(
        "bash",
        "Execute a bash command in the current working directory. Returns stdout and "
        "stderr, truncated to the last 2000 lines or 50KB. Full truncated output is saved to "
        "a temp file. Optionally provide a timeout in seconds; no default timeout.",
        {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "Shell command to execute"},
                "timeout": {
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "maximum": 2147483.647,
                    "description": "Optional timeout in seconds",
                },
            },
            "required": ["command"],
        },
        execute,
        label="bash",
        replay="never",
        version="pi-tools-1",
    )
