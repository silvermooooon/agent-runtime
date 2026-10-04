"""Stateful wrapper ported from pi's Agent, with provider/model convenience construction."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace

from .agent_loop import run_agent_loop, run_agent_loop_continue
from .ai import Models
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
    def __init__(
        self,
        *,
        model: Model | str | None = None,
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
        steering_mode="one-at-a-time",
        follow_up_mode="one-at-a-time",
        tool_execution="parallel",
    ):
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
        transcript = list(messages or [])
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
        self.parameters = {**(models.config.parameters if models else {}), **(parameters or {})}
        self.state = AgentState(
            model,
            thinking_level if thinking_level is not None else self.parameters.get("reasoning"),
            transcript,
            tools,
        )
        self.api_key = api_key
        self.convert_to_llm = convert_to_llm
        self.transform_context = transform_context
        self.get_api_key = get_api_key
        self.before_tool_call = before_tool_call
        self.after_tool_call = after_tool_call
        self.finish_turn = finish_turn
        self.prepare_request = prepare_request
        self.prepare_next_turn = prepare_next_turn
        self.tool_execution = tool_execution
        self._steering = PendingMessageQueue(steering_mode)
        self._follow_up = PendingMessageQueue(follow_up_mode)
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
        self._steering.messages.append(
            user_message(message) if isinstance(message, str) else message
        )

    def follow_up(self, message):
        self._follow_up.messages.append(
            user_message(message) if isinstance(message, str) else message
        )

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

    async def _run(self, prompts, skip_initial_steering=False):
        self._check_idle()
        self.state.is_streaming = True
        self.state.error_message = None
        self.state.streaming_message = None
        self.signal = AbortSignal()
        self._idle.clear()

        def steering():
            nonlocal skip_initial_steering
            if skip_initial_steering:
                skip_initial_steering = False
                return []
            return self._steering.drain()

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
            get_follow_up_messages=self._follow_up.drain,
            before_tool_call=self.before_tool_call,
            after_tool_call=self.after_tool_call,
            finish_turn=self.finish_turn,
            prepare_request=self.prepare_request,
            prepare_next_turn=self.prepare_next_turn,
            tool_execution=self.tool_execution,
        )
        context = AgentContext(list(self.state.messages), list(self.state.tools))
        try:
            if prompts is None:
                return await run_agent_loop_continue(
                    context, config, self._process, self.signal, self.stream_function
                )
            return await run_agent_loop(
                prompts, context, config, self._process, self.signal, self.stream_function
            )
        except asyncio.CancelledError:
            self.signal.abort()
            raise
        except Exception as error:
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
            self._idle.set()

    async def _process(self, event):
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
