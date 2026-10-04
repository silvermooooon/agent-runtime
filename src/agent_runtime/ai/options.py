"""pi-style per-API option mapping plus explicitly requested unsupported-parameter filtering."""

from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import dataclass, field

from ..types import Model, ParameterPolicy
from .estimate import clamp_max_tokens_to_context
from .models import THINKING_LEVELS, clamp_thinking_level

DEFAULT_THINKING_BUDGETS = {"minimal": 1024, "low": 2048, "medium": 8192, "high": 16384}
RUNTIME_OPTIONS = {
    "_session",
    "_resume_parameters",
    "api_key",
    "signal",
    "headers",
    "timeout",
    "on_payload",
    "on_response",
    "on_provider_stream_event",
    "on_parameters",
    "session_id",
    "thinking_budgets",
    "base_url",
}
ALIASES = {
    "max_output_tokens": "max_tokens",
    "max_completion_tokens": "max_tokens",
    "reasoning_effort": "reasoning",
}
SUPPORTED = {
    "openai-responses": {
        "temperature",
        "max_tokens",
        "reasoning",
        "reasoning_summary",
        "top_p",
        "tool_choice",
        "service_tier",
        "metadata",
    },
    "openai-completions": {
        "temperature",
        "max_tokens",
        "reasoning",
        "top_p",
        "tool_choice",
        "frequency_penalty",
        "presence_penalty",
        "stop",
        "seed",
    },
    "anthropic-messages": {
        "temperature",
        "max_tokens",
        "reasoning",
        "top_p",
        "top_k",
        "stop_sequences",
        "tool_choice",
        "metadata",
    },
}


@dataclass
class ParameterReport:
    parameters: dict = field(default_factory=dict)
    dropped: dict[str, str] = field(default_factory=dict)
    adjusted: dict[str, dict] = field(default_factory=dict)


def adjust_max_tokens_for_thinking(base, model_max, level, custom_budgets=None):
    budgets = {**DEFAULT_THINKING_BUDGETS, **(custom_budgets or {})}
    level = "high" if level in ("xhigh", "max") else level
    budget = budgets[level]
    if not isinstance(budget, int) or isinstance(budget, bool) or budget < 1024:
        raise ValueError("Thinking budgets must be integers >= 1024")
    ceiling = model_max if base is None else min(base + budget, model_max)
    if ceiling <= budget:
        budget = min(budget, max(0, ceiling - 1024))
    return ceiling, budget


def normalize_parameters(
    model: Model,
    options: dict | None = None,
    *,
    provider_policy: ParameterPolicy | None = None,
    context=None,
) -> ParameterReport:
    if model.api not in SUPPORTED:
        raise ValueError(f"Unsupported API: {model.api}")
    custom_budgets = (options or {}).get("thinking_budgets")
    options = deepcopy({k: v for k, v in (options or {}).items() if k not in RUNTIME_OPTIONS})
    report = ParameterReport()
    values = {}
    for key, value in options.items():
        target = ALIASES.get(key, key)
        if target not in SUPPORTED[model.api]:
            report.dropped[key] = f"Unsupported by {model.api}"
        elif value is not None:
            values[target] = value
    provider_policy = provider_policy or ParameterPolicy()
    policy = model.parameter_policy
    disabled = (
        set(model.compat.get("unsupported_parameters", []))
        | provider_policy.unsupported
        | policy.unsupported
    )
    for name in disabled:
        if name in values:
            values.pop(name)
            report.dropped[name] = "Disabled by provider/model metadata"
    # Rewrites only act on supplied parameters; they do not silently enable new options.
    fixed = {**provider_policy.fixed_values, **policy.fixed_values}
    if "fixed_temperature" in model.compat:
        fixed.setdefault("temperature", model.compat["fixed_temperature"])
    maps = {**provider_policy.value_maps, **policy.value_maps}
    ranges = {**provider_policy.ranges, **policy.ranges}
    for name in list(values):
        original = values[name]
        if name in maps:
            try:
                values[name] = maps[name].get(values[name], values[name])
            except TypeError:
                pass
        if name in fixed:
            values[name] = deepcopy(fixed[name])
        if name in ranges:
            low, high = ranges[name]
            if low > high:
                raise ValueError(f"Invalid metadata range for {name}")
            value = values[name]
            if type(value) in (int, float) and math.isfinite(value):
                values[name] = min(high, max(low, value))
        if values[name] != original:
            report.adjusted[name] = {"from": original, "to": values[name]}
    for key, maximum in (
        ("temperature", 1 if model.api == "anthropic-messages" else 2),
        ("top_p", 1),
    ):
        value = values.get(key)
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0 <= value <= maximum
        ):
            values.pop(key)
            report.dropped[key] = f"Expected a finite number between 0 and {maximum}"
    if "top_k" in values and (type(values["top_k"]) is not int or values["top_k"] < 0):
        values.pop("top_k")
        report.dropped["top_k"] = "Expected a non-negative integer"

    requested = values.pop("reasoning", None)
    if requested == "none":
        requested = "off"
    level = None
    if requested is not None:
        if model.reasoning is False or requested not in THINKING_LEVELS:
            report.dropped["reasoning"] = "Reasoning unsupported or unknown reasoning level"
        else:
            level = clamp_thinking_level(model, requested)
            if level != requested:
                report.adjusted["reasoning"] = {"from": requested, "to": level}
    effort = (
        model.thinking_level_map.get(level, "none" if level == "off" else level)
        if level is not None
        else model.compat.get("default_reasoning")
    )

    omitted = set()
    if model.compat.get("supports_temperature") is False:
        omitted.add("temperature")
    if model.compat.get("sampling_requires_reasoning_none") and effort != "none":
        omitted.update(("temperature", "top_p"))
    if model.api == "anthropic-messages" and level not in (None, "off"):
        omitted.update(("temperature", "top_p", "top_k"))
    if effort not in (None, "off", "none"):
        omitted.update(provider_policy.omit_when_reasoning | policy.omit_when_reasoning)
    for key in omitted:
        if key in values:
            values.pop(key)
            report.adjusted.pop(key, None)
            report.dropped[key] = "Parameter unavailable with this model/reasoning configuration"

    cap = values.pop("max_tokens", None)
    if cap is not None and (type(cap) is not int or cap <= 0):
        report.dropped["max_tokens"] = "Expected a positive integer"
        cap = None
    maximum = min(cap, model.max_tokens) if cap is not None else model.max_tokens
    if context is not None:
        maximum = clamp_max_tokens_to_context(model, context, maximum)
    if cap is not None and cap != maximum:
        report.adjusted["max_tokens"] = {"from": cap, "to": maximum}
    if model.api == "openai-responses":
        if model.compat.get("supports_max_output_tokens", True):
            values["max_output_tokens"] = max(16, maximum)
            if maximum < 16:
                report.adjusted["max_tokens"] = {"from": maximum, "to": 16}
        elif cap is not None:
            report.dropped["max_tokens"] = "Output cap unsupported by model"
        summary = values.pop("reasoning_summary", None)
        if level is not None:
            values["reasoning"] = {"effort": effort}
        if summary in ("auto", "concise", "detailed") and model.reasoning is not False:
            values.setdefault("reasoning", {"effort": effort or "medium"})["summary"] = summary
        elif summary is not None:
            report.dropped["reasoning_summary"] = "Unsupported reasoning summary"
        if model.reasoning is not False:
            # Preserve encrypted reasoning for stateless replay, including API-default reasoning.
            values["include"] = ["reasoning.encrypted_content"]
    elif model.api == "openai-completions":
        values[model.compat.get("max_tokens_field", "max_completion_tokens")] = maximum
        if level is not None and model.compat.get("supports_reasoning_effort", True):
            values["reasoning_effort"] = effort
    else:
        if level not in (None, "off"):
            if model.compat.get("force_adaptive_thinking"):
                values["thinking"] = {"type": "adaptive"}
                values["output_config"] = {"effort": effort}
            else:
                maximum, budget = adjust_max_tokens_for_thinking(
                    maximum, model.max_tokens, level, custom_budgets
                )
                if context is not None:
                    maximum = clamp_max_tokens_to_context(model, context, maximum)
                    budget = min(budget, max(0, maximum - 1024))
                if budget >= 1024:
                    values["thinking"] = {"type": "enabled", "budget_tokens": budget}
                else:
                    report.dropped["reasoning"] = "Insufficient output budget for thinking"
        elif level == "off":
            values["thinking"] = {"type": "disabled"}
        values["max_tokens"] = maximum
        if "metadata" in values:
            metadata = values["metadata"]
            if isinstance(metadata, dict) and isinstance(metadata.get("user_id"), str):
                values["metadata"] = {"user_id": metadata["user_id"]}
            else:
                values.pop("metadata")
                report.dropped["metadata"] = "Anthropic accepts metadata.user_id"
        if isinstance(values.get("tool_choice"), str):
            choice = values["tool_choice"]
            values["tool_choice"] = {"type": "any" if choice == "required" else choice}
    report.parameters = values
    return report
