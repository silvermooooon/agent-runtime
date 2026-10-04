"""Environment defaults and explicit provider configuration. No implicit .env search or mutation."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlparse

from .types import ParameterPolicy


@dataclass
class ProviderConfig:
    id: str
    api: str = "openai-responses"
    base_url: str = "https://api.openai.com/v1"
    api_key: str | None = field(default=None, repr=False)
    parameter_policy: ParameterPolicy = field(default_factory=ParameterPolicy)


@dataclass
class RuntimeConfig:
    provider: str = "openai"
    model: str | None = None
    api: str | None = None
    timeout: float = 120
    parameters: dict = field(default_factory=dict)
    providers: dict[str, ProviderConfig] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> RuntimeConfig:
        env = os.environ if env is None else env

        def text(name, default=None):
            value = env.get(name, "").strip()
            return value or default

        def number(name, default=None, integer=False):
            value = text(name)
            if value is None:
                return default
            try:
                result = int(value) if integer else float(value)
            except ValueError:
                raise ValueError(
                    f"{name} must be {'an integer' if integer else 'a number'}"
                ) from None
            if not math.isfinite(result):
                raise ValueError(f"{name} must be finite")
            return result

        timeout = number("AGENT_TIMEOUT_SECONDS", 120)
        if timeout <= 0:
            raise ValueError("AGENT_TIMEOUT_SECONDS must be positive")
        maximum = number("AGENT_MAX_TOKENS", integer=True)
        if maximum is not None and maximum <= 0:
            raise ValueError("AGENT_MAX_TOKENS must be positive")
        parameters = {}
        for name, value in (
            ("max_tokens", maximum),
            ("temperature", number("AGENT_TEMPERATURE")),
            ("reasoning", text("AGENT_REASONING")),
        ):
            if value is not None:
                parameters[name] = value
        providers = {}
        for provider, prefix, api, default_url in (
            ("openai", "OPENAI", "openai-responses", "https://api.openai.com/v1"),
            ("openai-chat", "OPENAI", "openai-completions", "https://api.openai.com/v1"),
            ("anthropic", "ANTHROPIC", "anthropic-messages", "https://api.anthropic.com/v1"),
        ):
            url = text(prefix + "_BASE_URL", default_url).rstrip("/")
            if urlparse(url).scheme not in ("http", "https") or not urlparse(url).netloc:
                raise ValueError(f"{prefix}_BASE_URL must be an absolute HTTP(S) URL")
            providers[provider] = ProviderConfig(provider, api, url, text(prefix + "_API_KEY"))
        return cls(
            provider=text("AGENT_PROVIDER", "openai"),
            model=text("AGENT_MODEL"),
            api=text("AGENT_API"),
            timeout=timeout,
            parameters=parameters,
            providers=providers,
        )
