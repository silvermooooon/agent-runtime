"""Stateful wrapper ported from pi's Agent, with provider/model convenience construction."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field, replace
from uuid import uuid4

from .agent_loop import run_agent_loop, run_agent_loop_continue, run_agent_loop_resume
from .ai import Models
from .ai.estimate import estimate_message_tokens
from .compaction import CompactionSettings, Compactor
from .sessions import LocalSession, SessionError, ToolRecoveryRequired
from .sessions.base import model_record, restore_model, saved_options, tool_records
from .sessions.projection import apply_compaction
from .stream_fn import get_default_stream_fn
from .transcript import current_system_message, system_prompt, tool_declaration
from .types import (
    AbortSignal,
    AgentContext,
    AgentLoopConfig,
    Message,
    Model,
    assistant_message,
    default_convert_to_llm,
    maybe_await,
    user_message,
)


class PendingMessageQueue:
    def __init__(self, mode="one-at-a-time"):
        if mode not in ("all", "one-at-a-time"):
            raise ValueError("Invalid queue mode")
        self.mode = mode
        self.messages = []

    def peek(self):
        return list(self.messages) if self.mode == "all" else self.messages[:1]

    def drain(self):
        messages = self.peek()
        del self.messages[: len(messages)]
        return messages


@dataclass
class AgentState:
    model: Model
    thinking_level: str | None = None
    messages: list[Message] = field(default_factory=list)
    tools: list = field(default_factory=list)
    is_streaming: bool = False
    streaming_message: Message | None = None
    pending_tool_calls: set[str] = field(default_factory=set)
    error_message: str | None = None

    @property
    def system_prompt(self):
        return system_prompt(self.messages)


class Agent:
    """One execution flow per session, confined to its event loop; the host owns scheduling."""

    def __init__(
        self,
        *,
        model: Model | str | None = None,
        session=None,
        provider: str | None = None,
        api: str | None = None,
        base_url: str | None = None,
        config=None,
        env=None,
        models=None,
        stream_fn=None,
        system_prompt="",
        tools=None,
        messages=None,
        parameters=None,
        api_key=None,
        thinking_level=None,
        convert_to_llm=default_convert_to_llm,
        transform_context=None,
        get_api_key=None,
        before_tool_call=None,
        after_tool_call=None,
        finish_turn=None,
        prepare_request=None,
        prepare_next_turn=None,
        steering_mode=None,
        follow_up_mode=None,
        tool_execution=None,
        compaction: CompactionSettings | Compactor | None = None,
    ):
        self.session = session if session is not None else LocalSession()
        restored = self.session.snapshot
        tool_execution = tool_execution or restored["tool_execution"]
        steering_mode = steering_mode or restored["queue_modes"]["steering"]
        follow_up_mode = follow_up_mode or restored["queue_modes"]["follow_up"]
        if model is None and restored["model"]:
            model = self.session.restored_model()
        if not isinstance(model, Model):
            models = models or Models(config=config, env=env)
            model = models.get_model(provider, model, api=api, base_url=base_url)
        elif api or base_url:
            model = replace(model, api=api or model.api, base_url=base_url or model.base_url)
        if not models and not stream_fn:
            try:
                stream_fn = get_default_stream_fn()
            except RuntimeError:
                models = Models(config=config, env=env)
        if tool_execution not in ("parallel", "sequential"):
            raise ValueError("Invalid tool_execution mode")
        self.stream_function = stream_fn or (
            models.stream_simple if models else get_default_stream_fn()
        )
        transcript = restored["messages"] if restored["model"] else list(messages or [])
        tools = list(tools or [])
        if (system_prompt or tools) and (not transcript or transcript[0]["role"] != "system"):
            transcript.insert(
                0,
                {
                    "role": "system",
                    "content": system_prompt,
                    "timestamp": 0,
                    "toolsAdded": [tool_declaration(t) for t in tools],
                },
            )
        self.parameters = {
            **(models.config.parameters if models else {}),
            **restored["options"],
            **(parameters or {}),
        }
        self.state = AgentState(
            model,
            thinking_level if thinking_level is not None else self.parameters.get("reasoning"),
            transcript,
            tools,
        )
        self.api_key = api_key
        self.convert_to_llm = convert_to_llm
        self.transform_context = transform_context
        self.compactor = (
            Compactor(compaction) if isinstance(compaction, CompactionSettings) else compaction
        )
        self.get_api_key = get_api_key
        self.before_tool_call = before_tool_call
        self.after_tool_call = after_tool_call
        self.finish_turn = finish_turn
        self.prepare_request = prepare_request
        self.prepare_next_turn = prepare_next_turn
        self.tool_execution = tool_execution
        self._steering = PendingMessageQueue(steering_mode)
        self._follow_up = PendingMessageQueue(follow_up_mode)
        self._steering.messages = restored["steering"]
        self._follow_up.messages = restored["follow_up"]
        self._listeners = []
        self.signal = None
        self._idle = asyncio.Event()
        self._idle.set()

    def subscribe(self, listener):
        """Listeners run in registration order and are awaited, including agent_end."""
        if listener not in self._listeners:
            self._listeners.append(listener)

        def unsubscribe():
            if listener in self._listeners:
                self._listeners.remove(listener)

        return unsubscribe

    def steer(self, message):
        message = user_message(message) if isinstance(message, str) else dict(message)
        message.setdefault("queueId", uuid4().hex)
        self._steering.messages.append(message)

    def follow_up(self, message):
        message = user_message(message) if isinstance(message, str) else dict(message)
        message.setdefault("queueId", uuid4().hex)
        self._follow_up.messages.append(message)

    async def enqueue(self, message, *, follow_up=False):
        """Queue input in memory. The running loop persists it at its next safe boundary."""
        (self.follow_up if follow_up else self.steer)(message)

    def clear_steering_queue(self):
        self._steering.messages.clear()

    def clear_follow_up_queue(self):
        self._follow_up.messages.clear()

    def clear_all_queues(self):
        self.clear_steering_queue()
        self.clear_follow_up_queue()

    def has_queued_messages(self):
        return bool(self._steering.messages or self._follow_up.messages)

    def peek_queued_messages(self):
        return self._steering.peek() or self._follow_up.peek()

    def abort(self):
        if self.signal:
            self.signal.abort()

    async def wait_for_idle(self):
        await self._idle.wait()

    def reset(self):
        self._check_idle()
        if self.session.resumable:
            raise SessionError("Use await reset_session() to explicitly abandon unfinished work")
        baseline = current_system_message(self.state.messages)
        self.state.messages = [baseline] if baseline else []
        self.state.error_message = None
        self.state.streaming_message = None
        self.state.pending_tool_calls.clear()
        self.clear_all_queues()

    def _check_idle(self):
        if self.state.is_streaming:
            raise RuntimeError("Agent is already processing; use steer() or follow_up()")

    async def prompt(self, message, images=None):
        self._check_idle()
        if isinstance(message, str):
            message = user_message(message)
            message["content"].extend(images or [])
        return await self._run(message if isinstance(message, list) else [message])

    async def continue_(self):
        """pi's continue(), renamed because continue is a Python keyword."""
        self._check_idle()
        if not any(m["role"] != "system" for m in self.state.messages):
            raise ValueError("No messages to continue from")
        if self.state.messages[-1]["role"] == "assistant":
            steering = self._steering.drain()
            if steering:
                return await self._run(steering, skip_initial_steering=True)
            follow_up = self._follow_up.drain()
            if follow_up:
                return await self._run(follow_up)
            raise ValueError("Cannot continue from message role: assistant")
        return await self._run(None)

    async def resume(self):
        """Resume the durable pending model/tool/turn boundary, including assistant tool plans."""
        self._check_idle()
        return await self._run(None, resume=True)

    async def compact(self, instructions=None, *, summary=None, first_kept_message_id=None):
        """Compact an idle session; optionally supply a summary and an explicit retained boundary.

        Returns the committed compaction event. None boundary with a supplied summary retains
        no old conversational messages. Generated summaries use the configured recent budget.
        """
        self._check_idle()
        if self.session.resumable:
            raise SessionError("Resume unfinished work before manual compaction")
        try:
            self.state.is_streaming = True
            self.signal = AbortSignal()
            self._idle.clear()
            return await self._compact(
                self.state.model,
                {**self.parameters, "reasoning": self.state.thinking_level},
                self.signal,
                reason="manual",
                instructions=instructions,
                summary=summary,
                first_kept_message_id=first_kept_message_id,
            )
        finally:
            self.state.messages = self.session.build_context()
            self.state.is_streaming = False
            self.signal = None
            self.session.live.clear()
            self._idle.set()

    async def _compact_before_request(self, context, model, options, signal):
        await self.session.sync_context(context.messages)
        if self.compactor.should_compact(context.messages, model):
            await self._compact(model, options, signal, reason="threshold")
        self.state.messages = self.session.build_context()
        return self.session.build_context()

    async def _compact(
        self,
        model,
        options,
        signal,
        *,
        reason,
        instructions=None,
        summary=None,
        first_kept_message_id=None,
    ):
        compactor = self.compactor or Compactor()
        plan = compactor.prepare(self.session) if summary is None else None
        if summary is None and plan is None:
            if reason == "manual":
                raise ValueError("Nothing to compact with the current keep_recent_tokens budget")
            return None
        boundary = plan.first_kept_message_id if plan else first_kept_message_id
        tokens_before = (
            plan.tokens_before
            if plan
            else sum(estimate_message_tokens(m) for m in self.session.build_context())
        )
        candidate = self.session.snapshot
        prior_compaction = candidate["compaction"]
        apply_compaction(
            candidate,
            {
                "id": "validation",
                "type": "compaction",
                "time": 0,
                "data": {
                    "summary": summary if summary is not None else "pending",
                    "first_kept_message_id": boundary,
                },
            },
        )
        await self.session.commit(
            "compaction_started",
            reason=reason,
            first_kept_message_id=boundary,
            tokens_before=tokens_before,
            model=model_record(model),
            settings=asdict(compactor.settings),
            instructions=instructions,
        )
        details = {"reason": reason, "provided": summary is not None}
        try:
            await self._process({"type": "compaction_start", "reason": reason})
            if summary is None:
                request_options = {**options}
                if self.api_key:
                    request_options["api_key"] = self.api_key
                if self.get_api_key:
                    key = await maybe_await(self.get_api_key(model.provider))
                    if key:
                        request_options["api_key"] = key
                summary, generated = await compactor.generate(
                    plan, model, self.stream_function, request_options, signal, instructions
                )
                details.update(generated)
                details["parameters"] = saved_options(request_options)
            signal.throw_if_aborted()
            record = await self.session.compact(
                summary, boundary, tokens_before=tokens_before, details=details
            )
        except (Exception, asyncio.CancelledError) as error:
            if self.session.snapshot["compaction"] != prior_compaction:
                # Cancellation may arrive while commit waits for fsync. A committed summary
                # stays successful in the journal even if the caller's task is cancelled.
                raise
            if not self.session._failed:
                await self.session.commit(
                    "compaction_failed",
                    reason=reason,
                    error=str(error),
                    aborted=signal.aborted or isinstance(error, asyncio.CancelledError),
                )
            await self._process(
                {
                    "type": "compaction_end",
                    "reason": reason,
                    "result": None,
                    "errorMessage": str(error),
                    "aborted": signal.aborted or isinstance(error, asyncio.CancelledError),
                }
            )
            raise
        self.state.messages = self.session.build_context()
        await self._process(
            {
                "type": "compaction_end",
                "reason": reason,
                "result": record,
                "aborted": False,
            }
        )
        return record

    async def _run(self, prompts, skip_initial_steering=False, resume=False):
        self._check_idle()
        saved = self.session.snapshot
        if resume and not self.session.resumable:
            raise SessionError("No unfinished session run to resume")
        if not resume and self.session.resumable:
            raise SessionError("Session has unfinished work; call resume() or reset_session()")
        if resume:
            missing = [
                name
                for name in saved["required_hooks"]
                if getattr(self, name) is None or getattr(self, name) is default_convert_to_llm
            ]
            if missing:
                raise SessionError(f"Re-inject required runtime hooks before resuming: {missing}")
            if saved["phase"] == "model" and tool_records(self.state.tools) != saved["tools"]:
                raise SessionError("Re-inject the original tools before resuming a model request")
            self.state.messages = saved["messages"]
            original = restore_model(saved["model"])
            if (original.id, original.provider, original.api) != (
                self.state.model.id,
                self.state.model.provider,
                self.state.model.api,
            ):
                raise SessionError("Resume requires the original provider, model and API")
            self.state.model = replace(original, headers=self.state.model.headers)
            for name, queue in (("steering", self._steering), ("follow_up", self._follow_up)):
                persisted = saved[name]
                ids = {message.get("queueId") for message in persisted}
                queue.messages = persisted + [
                    message for message in queue.messages if message.get("queueId") not in ids
                ]
        self.state.is_streaming = True
        self.state.error_message = None
        self.state.streaming_message = None
        self.signal = AbortSignal()
        self._idle.clear()

        async def steering():
            nonlocal skip_initial_steering
            await self._save_queues()
            if skip_initial_steering:
                skip_initial_steering = False
                return []
            return self._steering.drain()

        async def follow_up():
            await self._save_queues()
            return self._follow_up.drain()

        options = {**self.parameters, "reasoning": self.state.thinking_level}
        if self.api_key:
            options["api_key"] = self.api_key
        config = AgentLoopConfig(
            model=self.state.model,
            options=options,
            convert_to_llm=self.convert_to_llm,
            transform_context=self.transform_context,
            get_api_key=self.get_api_key,
            get_steering_messages=steering,
            get_follow_up_messages=follow_up,
            before_tool_call=self.before_tool_call,
            after_tool_call=self.after_tool_call,
            finish_turn=self.finish_turn,
            prepare_request=self.prepare_request,
            prepare_next_turn=self.prepare_next_turn,
            compact_context=self._compact_before_request if self.compactor else None,
            tool_execution=self.tool_execution,
            session=self.session,
            queue_modes={"steering": self._steering.mode, "follow_up": self._follow_up.mode},
            queue_state={
                "steering": list(self._steering.messages),
                "follow_up": list(self._follow_up.messages),
            },
        )
        context = AgentContext(list(self.state.messages), list(self.state.tools))
        try:
            if resume:
                result = await run_agent_loop_resume(
                    context, config, self._process, self.signal, self.stream_function
                )
            elif prompts is None:
                result = await run_agent_loop_continue(
                    context, config, self._process, self.signal, self.stream_function
                )
            else:
                result = await run_agent_loop(
                    prompts, context, config, self._process, self.signal, self.stream_function
                )
            await self._save_queues()
            return result
        except asyncio.CancelledError:
            self.signal.abort()
            if not self.session._failed and self.session.resumable:
                await self.session.commit(
                    "run_interrupted", status="interrupted", reason="Python task cancelled"
                )
            raise
        except SessionError as error:
            self.state.error_message = str(error)
            if isinstance(error, ToolRecoveryRequired) and not self.session._failed:
                await self.session.commit(
                    "run_interrupted", status="waiting_recovery", reason=str(error)
                )
            raise
        except Exception as error:
            if isinstance(error, BaseExceptionGroup) and error.split(SessionError)[0]:

                def recovery_ids(exception):
                    if isinstance(exception, ToolRecoveryRequired):
                        return exception.call_ids
                    if isinstance(exception, BaseExceptionGroup):
                        return [i for child in exception.exceptions for i in recovery_ids(child)]
                    return []

                unknown = recovery_ids(error)
                if unknown and not self.session._failed:
                    recovery = ToolRecoveryRequired(unknown)
                    self.state.error_message = str(recovery)
                    await self.session.commit(
                        "run_interrupted", status="waiting_recovery", reason=str(recovery)
                    )
                    raise recovery from error
                self.state.error_message = str(error)
                raise SessionError("Session failed during parallel tool execution") from error
            if self.session.resumable:
                await self.session.commit(
                    "run_interrupted",
                    status="stopped" if self.signal.aborted else "failed",
                    reason=str(error),
                )
            message = assistant_message(
                self.state.model,
                stopReason="aborted" if self.signal.aborted else "error",
                errorMessage=str(error),
            )
            for event in (
                {"type": "message_start", "message": message},
                {"type": "message_end", "message": message},
                {"type": "turn_end", "message": message, "toolResults": []},
                {"type": "agent_end", "messages": [message]},
            ):
                await self._process(event)
            return [message]
        finally:
            self.state.is_streaming = False
            self.state.streaming_message = None
            self.state.pending_tool_calls.clear()
            self.signal = None
            self.session.live.clear()
            self._idle.set()

    async def _save_queues(self):
        await self.session.sync_queues(self._steering.messages, self._follow_up.messages)

    async def reset_session(self):
        """Explicitly abandon pending work and append a reset; old audit records remain."""
        self._check_idle()
        self.state.is_streaming = True
        self._idle.clear()
        try:
            baseline = current_system_message(self.session.build_context())
            messages = [baseline] if baseline else []
            await self.session.commit("history_reset", messages=messages)
            self.state.messages = messages
            self.state.error_message = None
            self.clear_all_queues()
        finally:
            self.state.is_streaming = False
            self._idle.set()

    async def _process(self, event):
        self.session.observe(event)
        kind = event["type"]
        if kind in ("message_start", "message_update"):
            self.state.streaming_message = event["message"]
        elif kind == "message_end":
            self.state.streaming_message = None
            self.state.messages.append(event["message"])
        elif kind == "tool_execution_start":
            self.state.pending_tool_calls.add(event["toolCallId"])
        elif kind == "tool_execution_end":
            self.state.pending_tool_calls.discard(event["toolCallId"])
        elif kind == "turn_end":
            self.state.error_message = event["message"].get("errorMessage")
        elif kind == "agent_end":
            self.state.streaming_message = None
        for listener in tuple(self._listeners):
            await maybe_await(listener(event, self.signal))
