"""Provider/model facade: explicit catalog, stream_simple and complete_simple."""

from __future__ import annotations

import asyncio
from contextlib import aclosing
from copy import deepcopy
from dataclasses import replace

from ..config import ProviderConfig, RuntimeConfig
from ..event_stream import AssistantMessageEventStream
from ..types import AbortSignal, Model, OperationAborted, assistant_message, maybe_await
from .messages import build_payload
from .models import builtin_models, clamp_thinking_level, supported_thinking_levels
from .options import ParameterReport, adjust_max_tokens_for_thinking, normalize_parameters
from .parsers import AnthropicParser, CompletionsParser, ResponsesParser
from .tool_names import ToolNameStream
from .transport import stream_http

PARSERS = {
    "openai-responses": ResponsesParser,
    "openai-completions": CompletionsParser,
    "anthropic-messages": AnthropicParser,
}
PATHS = {
    "openai-responses": "/responses",
    "openai-completions": "/chat/completions",
    "anthropic-messages": "/messages",
}


class Models:
    """Instance-local registry so tenant credentials and model definitions are not global."""

    def __init__(
        self,
        *,
        api_keys=None,
        client=None,
        include_builtins=True,
        config: RuntimeConfig | None = None,
        env=None,
        providers=None,
    ):
        self._models = {}
        self._explicit_models = set()
        self._streams = {}
        self._api_keys = dict(api_keys or {})
        self.config = deepcopy(config) if config is not None else RuntimeConfig.from_env(env)
        self._providers = RuntimeConfig.from_env({}).providers
        self._providers.update(deepcopy(self.config.providers))
        self._explicit_providers = set()
        for provider in providers or []:
            self.register_provider(provider)
        self.client = client
        for model in builtin_models() if include_builtins else []:
            self._models[(model.provider, model.id)] = model

    def register_provider(self, provider: ProviderConfig):
        self._providers[provider.id] = deepcopy(provider)
        self._explicit_providers.add(provider.id)

    def register_model(self, model: Model):
        if not model.id or not model.provider or model.max_tokens <= 0:
            raise ValueError("Model needs id, provider and positive max_tokens")
        self._models[(model.provider, model.id)] = deepcopy(model)
        self._explicit_models.add((model.provider, model.id))

    def register_api(self, api: str, stream_fn):
        self._streams[api] = stream_fn

    def get_model(
        self,
        provider: str | None = None,
        name: str | None = None,
        *,
        api: str | None = None,
        base_url: str | None = None,
    ) -> Model:
        provider = provider or self.config.provider
        name = name or self.config.model
        if not name:
            raise ValueError("Pass model=... or set AGENT_MODEL")
        configured = self._providers.get(provider)
        known = self._models.get((provider, name))
        if known is None and configured is None:
            raise ValueError(
                f"Unknown provider {provider}; register_provider or register_model first"
            )
        model = (
            deepcopy(known)
            if known
            else Model(id=name, provider=provider, api=configured.api, base_url=configured.base_url)
        )
        # Explicit model metadata overrides provider/environment defaults.
        if (provider, name) not in self._explicit_models and configured:
            model = replace(model, api=configured.api, base_url=configured.base_url)
        env_api = (
            self.config.api
            if (provider, name) not in self._explicit_models
            and provider not in self._explicit_providers
            else None
        )
        return replace(model, api=api or env_api or model.api, base_url=base_url or model.base_url)

    def get_models(self, provider=None):
        return [
            deepcopy(m) for m in self._models.values() if provider is None or m.provider == provider
        ]

    def get_providers(self):
        return sorted(set(self._providers) | {m.provider for m in self._models.values()})

    def stream_simple(self, model: Model, context, options=None):
        options = {**self.config.parameters, "timeout": self.config.timeout, **(options or {})}
        if model.api in self._streams:
            return self._streams[model.api](model, context, options)
        stream = AssistantMessageEventStream()
        named_stream = ToolNameStream(stream, context)
        output = assistant_message(model)
        signal = options.get("signal") or AbortSignal()

        async def produce():
            async def request():
                signal.throw_if_aborted()
                if model.api not in PARSERS:
                    raise ValueError(f"No provider adapter for API: {model.api}")
                provider = self._providers.get(model.provider)
                restored = options.get("_resume_parameters")
                if restored:
                    report = ParameterReport(
                        parameters=deepcopy(restored["parameters"]),
                        dropped=deepcopy(restored["dropped"]),
                        adjusted=deepcopy(restored["adjusted"]),
                    )
                    options["timeout"] = restored["timeout"]
                else:
                    report = normalize_parameters(
                        model,
                        options,
                        provider_policy=provider.parameter_policy if provider else None,
                        context=context,
                    )
                session = options.get("_session")
                if session and not restored:
                    await session.commit(
                        "provider_parameters",
                        parameters=report.parameters,
                        dropped=report.dropped,
                        adjusted=report.adjusted,
                        timeout=options.get("timeout", 120),
                    )
                if options.get("on_parameters"):
                    await maybe_await(options["on_parameters"](deepcopy(report), model))
                payload = build_payload(model, context, report.parameters)
                if options.get("on_payload"):
                    # Observation only: mutation of a copy cannot bypass parameter filtering.
                    await maybe_await(options["on_payload"](deepcopy(payload), model))
                key = options.get("api_key") or self._api_keys.get(model.provider)
                if not key and provider:
                    key = provider.api_key
                headers = {**model.headers, **options.get("headers", {})}
                if model.api == "anthropic-messages":
                    headers.setdefault("anthropic-version", "2023-06-01")
                    if key:
                        headers.setdefault("x-api-key", key)
                elif key:
                    headers.setdefault("Authorization", f"Bearer {key}")
                if not any(name.lower() in ("authorization", "x-api-key") for name in headers):
                    raise ValueError(f"No API key for provider: {model.provider}")
                parser = PARSERS[model.api](output, named_stream)
                named_stream.push({"type": "start", "partial": deepcopy(output)})

                async def on_response(info):
                    if options.get("on_response"):
                        await maybe_await(options["on_response"](info, model))

                events = stream_http(
                    (options.get("base_url") or model.base_url).rstrip("/") + PATHS[model.api],
                    payload,
                    headers,
                    client=self.client,
                    timeout=options.get("timeout", 120),
                    on_response=on_response,
                )
                async with aclosing(events):
                    async for event in events:
                        if options.get("on_provider_stream_event"):
                            await maybe_await(
                                options["on_provider_stream_event"](deepcopy(event), model)
                            )
                        parser.feed(event)
                        if parser.terminal:
                            break
                if isinstance(parser, CompletionsParser) and not parser.terminal:
                    parser.finish()
                if not parser.terminal:
                    raise RuntimeError("Provider stream ended before a terminal event")
                signal.throw_if_aborted()

            try:
                await signal.run(request())
                named_stream.push(
                    {"type": "done", "reason": output["stopReason"], "message": output}
                )
            except (Exception, asyncio.CancelledError) as error:
                aborted = signal.aborted or isinstance(
                    error, (OperationAborted, asyncio.CancelledError)
                )
                output["stopReason"] = "aborted" if aborted else "error"
                output["errorMessage"] = str(error) or "Operation aborted"
                named_stream.push(
                    {"type": "error", "reason": output["stopReason"], "error": output}
                )

        stream.task = asyncio.create_task(produce())
        return stream

    async def complete_simple(self, model, context, options=None):
        stream = await maybe_await(self.stream_simple(model, context, options))
        # Consume to avoid accumulating all delta snapshots when only a result is wanted.
        async for _ in stream:
            pass
        return await stream.result()


__all__ = [
    "Models",
    "Model",
    "ParameterReport",
    "normalize_parameters",
    "build_payload",
    "clamp_thinking_level",
    "supported_thinking_levels",
    "adjust_max_tokens_for_thinking",
]
