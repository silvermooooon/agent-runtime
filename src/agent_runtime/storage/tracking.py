"""Usage capture around the injected model adapter, shared by turns and compaction."""

import asyncio

from ..types import maybe_await


async def tracked_stream(session, attempt, stream_fn, model, context, options):
    async def finish(*, message=None, error=None):
        if session._failed:
            return
        reason = (message or {}).get("stopReason")
        status = (
            "interrupted"
            if isinstance(error, asyncio.CancelledError) or reason == "aborted"
            else "failed"
            if error or reason == "error"
            else "completed"
        )
        if message is not None and options.get("_purpose") != "compaction":
            # Full generation output is already in model_completed/model_attempt.
            message = {
                key: value
                for key, value in message.items()
                if key in ("usage", "usageStatus", "stopReason", "responseId")
            }
        await session.commit(
            "model_call_finished",
            attempt_id=attempt,
            status=status,
            message=message,
            error=str(error) if error else None,
        )

    try:
        response = await maybe_await(stream_fn(model, context, options))
    except BaseException as error:
        await finish(error=error)
        raise

    class TrackedResponse:
        def __init__(self):
            self.iterator = response.__aiter__()
            self.finished = False
            self.task = getattr(response, "task", None)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return await self.iterator.__anext__()
            except StopAsyncIteration:
                raise
            except BaseException as error:
                await self.complete(error=error)
                raise

        async def complete(self, **kwargs):
            if not self.finished:
                self.finished = True
                await finish(**kwargs)

        async def result(self):
            try:
                message = await response.result()
            except BaseException as error:
                await self.complete(error=error)
                raise
            await self.complete(message=message)
            return message

    return TrackedResponse()
