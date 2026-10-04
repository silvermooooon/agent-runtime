"""Port of pi agent-loop.ts: loop order, lifecycle events and tool scheduling."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace

from .event_stream import EventStream
from .sessions.base import SessionError, restore_model, tool_message
from .stream_fn import get_default_stream_fn
from .transcript import declare_tool_changes
from .types import (
    AbortSignal,
    AgentContext,
    AgentLoopConfig,
    AgentToolResult,
    maybe_await,
    timestamp,
)
from .validation import validate_tool_arguments


async def _hook(callback, *args):
    return await maybe_await(callback(*args)) if callback else None


async def run_agent_loop(prompts, context, config, emit, signal=None, stream_fn=None):
    initial = declare_tool_changes(context, prompts)
    if config.session:
        await config.session.begin(context, initial, config)
    messages = list(initial)
    current = AgentContext(list(context.messages) + initial, list(context.tools))
    await maybe_await(emit({"type": "agent_start"}))
    await maybe_await(emit({"type": "turn_start"}))
    for message in initial:
        await _emit_message(message, emit)
    await _run_loop(
        current,
        messages,
        config,
        emit,
        signal or AbortSignal(),
        stream_fn or get_default_stream_fn(),
    )
    return messages


def _check_continue(context):
    if not context.messages:
        raise ValueError("Cannot continue: no messages in context")
    if context.messages[-1]["role"] == "assistant":
        raise ValueError("Cannot continue from message role: assistant")


async def run_agent_loop_continue(context, config, emit, signal=None, stream_fn=None):
    _check_continue(context)
    if config.session:
        await config.session.begin(context, [], config)
    messages = []
    current = AgentContext(list(context.messages), list(context.tools))
    await maybe_await(emit({"type": "agent_start"}))
    await maybe_await(emit({"type": "turn_start"}))
    await _run_loop(
        current,
        messages,
        config,
        emit,
        signal or AbortSignal(),
        stream_fn or get_default_stream_fn(),
    )
    return messages


async def run_agent_loop_resume(context, config, emit, signal=None, stream_fn=None):
    if not config.session or not config.session.resumable:
        raise SessionError("No unfinished session run to resume")
    await config.session.commit("run_resumed")
    state = config.session.snapshot
    current = AgentContext(state["messages"], list(context.tools))
    messages = state["run_messages"]
    await maybe_await(emit({"type": "agent_start"}))
    await maybe_await(emit({"type": "turn_start"}))
    await _run_loop(
        current,
        messages,
        config,
        emit,
        signal or AbortSignal(),
        stream_fn or get_default_stream_fn(),
        recovering=True,
    )
    return messages


def _start_stream(executor):
    stream = EventStream(lambda e: e["type"] == "agent_end", lambda e: e["messages"])

    async def run():
        try:
            stream.end(await executor(stream.send))
        except BaseException as error:
            stream.fail(error)

    stream.task = asyncio.create_task(run())
    return stream


def agent_loop(prompts, context, config, signal=None, stream_fn=None):
    return _start_stream(
        lambda emit: run_agent_loop(prompts, context, config, emit, signal, stream_fn)
    )


def agent_loop_continue(context, config, signal=None, stream_fn=None):
    _check_continue(context)
    return _start_stream(
        lambda emit: run_agent_loop_continue(context, config, emit, signal, stream_fn)
    )


def _apply_update(context, config, update):
    if not update:
        return context, config
    options = dict(config.options)
    if "thinking_level" in update:
        options["reasoning"] = update["thinking_level"]
    return (
        update.get("context", context),
        replace(config, model=update.get("model", config.model), options=options),
    )


async def _run_loop(context, new_messages, config, emit, signal, stream_fn, recovering=False):
    last_turn = None
    explicit_continue = False
    session = config.session
    restored = session.snapshot if recovering else None
    pending = (
        await _hook(config.get_steering_messages) or []
        if not recovering or (restored["phase"] == "model" and not restored["request"])
        else []
    )
    while True:
        has_tools = True
        while has_tools or pending:
            recovering_turn = restored and restored["phase"] != "model"
            recovering_request = restored and restored["phase"] == "model" and restored["request"]
            if not recovering_turn and not recovering_request:
                prepared = []
                if last_turn:
                    update = await _hook(config.prepare_next_turn, last_turn)
                    context, config = _apply_update(context, config, update)
                    prepared = (update or {}).get("messages", [])
                    if not pending:
                        pending = await _hook(config.get_steering_messages) or []
                    await maybe_await(emit({"type": "turn_start"}))
                additions = declare_tool_changes(context, prepared + pending)
                if session and additions:
                    await session.commit("messages_added", messages=additions)
                for message in additions:
                    await _emit_message(message, emit)
                    context.messages.append(message)
                    new_messages.append(message)
                pending = []
                update = await _hook(
                    config.prepare_request,
                    {
                        "context": context,
                        "model": config.model,
                        "thinking_level": config.options.get("reasoning", "off"),
                    },
                    signal,
                )
                context, config = _apply_update(context, config, update)
            if restored and (recovering_turn or recovering_request):
                original = restore_model(restored["model"])
                config = replace(
                    config,
                    model=replace(original, headers=config.model.headers),
                    options={**config.options, **restored["options"]},
                )
            if recovering_turn:
                message = restored["assistant"]
            else:
                message = await _stream_assistant(
                    context,
                    config,
                    emit,
                    signal,
                    stream_fn,
                    prepared_request=restored["request"] if recovering_request else None,
                )
                new_messages.append(message)
            results = []
            hard_exit = message["stopReason"] in ("error", "aborted")
            calls = [c for c in message["content"] if c["type"] == "toolCall"]
            has_tools = False
            if recovering_turn and restored["phase"] in ("finish_turn", "after_turn"):
                results = [
                    tool_message(
                        c,
                        restored["results"][c["id"]]["result"],
                        restored["results"][c["id"]]["time"],
                    )
                    for c in calls
                ]
                has_tools = bool(calls) and not all(
                    r["result"].get("terminate") is True for r in restored["results"].values()
                )
            elif calls and not hard_exit:
                if session and signal.aborted:
                    await session.commit(
                        "run_interrupted",
                        status="stopped",
                        reason="Stop requested before tool execution",
                    )
                    await maybe_await(emit({"type": "agent_end", "messages": new_messages}))
                    return
                if recovering_turn:
                    session.check_recovery_tools(calls, context.tools)
                results, terminate = await _execute_tools(
                    context, message, calls, config, signal, emit
                )
                context.messages.extend(results)
                new_messages.extend(results)
                if session:
                    if signal.aborted:
                        await session.commit(
                            "run_interrupted",
                            status="stopped",
                            reason="Stop requested during tool batch",
                        )
                        await maybe_await(
                            emit({"type": "turn_end", "message": message, "toolResults": results})
                        )
                        await maybe_await(emit({"type": "agent_end", "messages": new_messages}))
                        return
                    await session.commit("tools_completed")
                has_tools = not terminate
            last_turn = {
                "message": message,
                "toolResults": results,
                "context": context,
                "newMessages": new_messages,
            }
            decision = (
                restored["decision"]
                if recovering_turn and restored["phase"] == "after_turn"
                else await _hook(config.finish_turn, last_turn, signal)
            )
            if session:
                if hard_exit:
                    await session.commit(
                        "run_interrupted",
                        status="stopped" if signal.aborted else "failed",
                        reason=message.get("errorMessage"),
                    )
                elif not (recovering_turn and restored["phase"] == "after_turn"):
                    await session.commit("turn_completed", decision=decision)
            restored = None
            await maybe_await(
                emit({"type": "turn_end", "message": message, "toolResults": results})
            )
            if hard_exit or (decision or {}).get("action") == "end":
                if session and not hard_exit:
                    await session.commit("run_completed")
                await maybe_await(emit({"type": "agent_end", "messages": new_messages}))
                return
            explicit_continue = (decision or {}).get("action") == "continue"
            pending = await _hook(config.get_steering_messages) or []
            if has_tools or pending:
                explicit_continue = False
        follow_up = await _hook(config.get_follow_up_messages) or []
        if follow_up:
            explicit_continue = False
            pending = follow_up
            continue
        if explicit_continue:
            explicit_continue = False
            continue
        break
    if session:
        await session.commit("run_completed")
    await maybe_await(emit({"type": "agent_end", "messages": new_messages}))


async def _stream_assistant(context, config, emit, signal, stream_fn, prepared_request=None):
    if not prepared_request and config.compact_context:
        context.messages = await _hook(
            config.compact_context, context, config.model, config.options, signal
        )
    messages = context.messages
    if prepared_request:
        llm_messages = prepared_request.get("llm_messages")
        if llm_messages is None:
            llm_messages = config.session.build_context()
    elif config.transform_context:
        messages = await _hook(config.transform_context, messages, signal)
        llm_messages = await _hook(config.convert_to_llm, messages)
    else:
        llm_messages = await _hook(config.convert_to_llm, messages)
    key = await _hook(config.get_api_key, config.model.provider)
    options = {**config.options, "signal": signal}
    if key:
        options["api_key"] = key
    if config.session:
        options["_session"] = config.session
        if prepared_request and prepared_request.get("provider_parameters"):
            options["_resume_parameters"] = prepared_request["provider_parameters"]
    if config.session and not prepared_request:
        await config.session.request(context, config.model, options, llm_messages)
    signal.throw_if_aborted()
    response = await maybe_await(stream_fn(config.model, AgentContext(llm_messages), options))
    try:
        return await _consume_assistant(response, context, config, emit)
    except BaseException:
        # A failed sink must not leave a background HTTP producer filling an abandoned queue.
        task = getattr(response, "task", None)
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        raise


async def _consume_assistant(response, context, config, emit):
    added = False
    async for event in response:
        kind = event["type"]
        if kind == "start":
            context.messages.append(event["partial"])
            added = True
            await maybe_await(
                emit({"type": "message_start", "message": deepcopy(event["partial"])})
            )
        elif kind in ("done", "error"):
            break
        elif added and "partial" in event:
            context.messages[-1] = event["partial"]
            await maybe_await(
                emit(
                    {
                        "type": "message_update",
                        "message": deepcopy(event["partial"]),
                        "assistantMessageEvent": deepcopy(event),
                    }
                )
            )
    final = await response.result()
    final["thinkingLevel"] = config.options.get("reasoning")
    if config.session:
        await config.session.model_finished(final)
    if added:
        context.messages[-1] = final
    else:
        context.messages.append(final)
        await maybe_await(emit({"type": "message_start", "message": deepcopy(final)}))
    await maybe_await(emit({"type": "message_end", "message": final}))
    return final


def _error_result(text):
    return {"content": [{"type": "text", "text": text}], "details": {}, "isError": True}


async def _prepare_tool(context, assistant, call, config, signal):
    tool = next((t for t in context.tools if t.name == call["name"]), None)
    if tool is None:
        return None, None, _error_result(f"Tool {call['name']} not found")
    try:
        args = call["arguments"]
        started = config.session.started_call(call["id"]) if config.session else None
        if started:
            args = deepcopy(started["args"])
        elif tool.prepare_arguments:
            args = tool.prepare_arguments(deepcopy(args))
        args = validate_tool_arguments(tool, args)
        if config.session and config.session.returned_result(call["id"]) is not None:
            return tool, args, None
        before = await _hook(
            config.before_tool_call,
            {"assistantMessage": assistant, "toolCall": call, "args": args, "context": context},
            signal,
        )
        signal.throw_if_aborted()
        if before and before.get("block"):
            result = _error_result(before.get("reason") or "Tool execution was blocked")
            result["terminate"] = before.get("terminate", False)
            return None, None, result
        return tool, args, None
    except SessionError:
        raise
    except Exception as error:
        return None, None, _error_result(str(error))


async def _execute_prepared(context, assistant, call, tool, args, config, signal, on_update):
    accepting = True
    updates = []

    def update(result):
        if not accepting:
            return
        result = result.to_dict() if isinstance(result, AgentToolResult) else result
        updates.append(asyncio.create_task(maybe_await(on_update(result))))

    result = config.session.returned_result(call["id"]) if config.session else None
    try:
        if result is None:
            try:
                signal.throw_if_aborted()
                result = await maybe_await(tool.execute(call["id"], args, signal, update))
                result = result.to_dict() if isinstance(result, AgentToolResult) else dict(result)
            except SessionError:
                raise
            except Exception as error:
                result = _error_result(str(error))
            if config.session:
                await config.session.tool_returned(call, result)
    finally:
        accepting = False
        if updates:
            await asyncio.gather(*updates)
    try:
        after = await _hook(
            config.after_tool_call,
            {
                "assistantMessage": assistant,
                "toolCall": call,
                "args": args,
                "result": result,
                "isError": result.get("isError", False),
                "context": context,
            },
            signal,
        )
        if after:
            if "content" in after and "structuredContent" not in after:
                result.pop("structuredContent", None)
            result.update(
                {
                    k: v
                    for k, v in after.items()
                    if k
                    in ("content", "details", "structuredContent", "isError", "usage", "terminate")
                }
            )
    except SessionError:
        raise
    except Exception as error:
        result = _error_result(str(error))
    return result


async def run_tool_call(
    call,
    *,
    tools,
    assistant_message,
    context,
    signal=None,
    before_tool_call=None,
    after_tool_call=None,
    on_update=None,
):
    """Run nested calls through the same validation and permission hooks as normal tools."""
    signal = signal or AbortSignal()
    config = AgentLoopConfig(
        model=None, before_tool_call=before_tool_call, after_tool_call=after_tool_call
    )
    resolution = AgentContext(context.messages, list(tools))
    tool, args, result = await _prepare_tool(resolution, assistant_message, call, config, signal)
    if result is None:
        result = await _execute_prepared(
            context,
            assistant_message,
            call,
            tool,
            args,
            config,
            signal,
            on_update or (lambda _: None),
        )
    return {"toolCall": call, "result": result, "isError": result.get("isError", False)}


async def _execute_tools(context, assistant, calls, config, signal, emit):
    sequential = config.tool_execution == "sequential" or any(
        t.execution_mode == "sequential" and any(c["name"] == t.name for c in calls)
        for t in context.tools
    )
    truncated = assistant["stopReason"] == "length"
    outcomes, tasks, messages = [], [], []

    async def end(call, result):
        if config.session:
            await config.session.tool_finished(call, result)
        await maybe_await(
            emit(
                {
                    "type": "tool_execution_end",
                    "toolCallId": call["id"],
                    "toolName": call["name"],
                    "result": result,
                    "isError": result.get("isError", False),
                }
            )
        )
        return call, result

    async def execute(call, tool, args):
        if config.session and config.session.returned_result(call["id"]) is None:
            await config.session.tool_started(call, args)
        result = await _execute_prepared(
            context,
            assistant,
            call,
            tool,
            args,
            config,
            signal,
            lambda partial: emit(
                {
                    "type": "tool_execution_update",
                    "toolCallId": call["id"],
                    "toolName": call["name"],
                    "args": call["arguments"],
                    "partialResult": partial,
                }
            ),
        )
        return await end(call, result)

    def result_message(outcome):
        return (
            config.session.result_message(outcome[0]) if config.session else _tool_message(*outcome)
        )

    for call in calls:
        await maybe_await(
            emit(
                {
                    "type": "tool_execution_start",
                    "toolCallId": call["id"],
                    "toolName": call["name"],
                    "args": call["arguments"],
                }
            )
        )
        saved = config.session.tool_result(call["id"]) if config.session else None
        if saved is not None:
            tool, args, result = None, None, saved
        elif truncated:
            tool, args, result = (
                None,
                None,
                _error_result(
                    f'Tool call "{call["name"]}" was not executed: response hit the output '
                    "token limit; re-issue the call with complete arguments."
                ),
            )
        else:
            tool, args, result = await _prepare_tool(context, assistant, call, config, signal)
        if result is not None:
            outcome = await end(call, result)
        elif sequential:
            outcome = await execute(call, tool, args)
        else:
            # Do not execute until ALL calls have passed sequential preflight, as in pi.
            outcome = None
        if sequential or truncated:
            outcomes.append(outcome)
            msg = result_message(outcome)
            await _emit_message(msg, emit)
            messages.append(msg)
        else:
            tasks.append((call, tool, args, outcome))
        if signal.aborted:
            break
    if not sequential and not truncated:

        async def resolve(entry):
            call, tool, args, outcome = entry
            return outcome if outcome is not None else await execute(call, tool, args)

        # TaskGroup cancels sibling executions if an event sink fails.
        async with asyncio.TaskGroup() as group:
            running = [group.create_task(resolve(entry)) for entry in tasks]
        outcomes = [task.result() for task in running]
        for outcome in outcomes:
            msg = result_message(outcome)
            await _emit_message(msg, emit)
            messages.append(msg)
    return messages, bool(outcomes) and all(r.get("terminate") is True for _, r in outcomes)


def _tool_message(call, result):
    return {
        "role": "toolResult",
        "toolCallId": call["id"],
        "toolName": call["name"],
        "content": result.get("content") or [],
        "details": result.get("details"),
        "isError": result.get("isError", False),
        "timestamp": timestamp(),
        **({"usage": result["usage"]} if "usage" in result else {}),
    }


async def _emit_message(message, emit):
    await maybe_await(emit({"type": "message_start", "message": message}))
    await maybe_await(emit({"type": "message_end", "message": message}))
