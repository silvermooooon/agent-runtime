"""Small, explicit model catalog; follows pi's Model + compat + thinkingLevelMap design."""

from copy import deepcopy

from ..config import DEFAULT_MODEL
from ..types import Model

THINKING_LEVELS = ("off", "minimal", "low", "medium", "high", "xhigh", "max")


def supported_thinking_levels(model: Model) -> list[str]:
    if model.reasoning is False:
        return ["off"]
    if model.reasoning is None:
        return list(THINKING_LEVELS)
    return [
        level
        for level in THINKING_LEVELS
        if (level not in model.thinking_level_map or model.thinking_level_map[level] is not None)
        and (level not in ("xhigh", "max") or level in model.thinking_level_map)
    ]


def clamp_thinking_level(model: Model, level: str) -> str:
    available = supported_thinking_levels(model)
    if level in available:
        return level
    if level not in THINKING_LEVELS:
        return available[0] if available else "off"
    index = THINKING_LEVELS.index(level)
    for candidate in (*THINKING_LEVELS[index:], *reversed(THINKING_LEVELS[:index])):
        if candidate in available:
            return candidate
    return "off"


def builtin_models() -> list[Model]:
    openai_url = "https://api.openai.com/v1"
    # https://developers.openai.com/api/docs/models/gpt-6-luna
    # https://developers.openai.com/api/docs/guides/latest-model (parameter compatibility)
    result = [
        Model(
            DEFAULT_MODEL,
            "openai",
            "openai-responses",
            openai_url,
            reasoning=True,
            max_tokens=128000,
            context_window=1050000,
            thinking_level_map={"off": "none", "minimal": None, "xhigh": "xhigh", "max": "max"},
            compat={"default_reasoning": "medium", "sampling_requires_reasoning_none": True},
        ),
        Model(
            "gpt-4.1",
            "openai",
            "openai-responses",
            openai_url,
            reasoning=False,
            max_tokens=32768,
            context_window=1047576,
        ),
    ]
    # Concrete older Claude family using pi's budget-based thinking branch.
    result.append(
        Model(
            "claude-sonnet-4-20250514",
            "anthropic",
            "anthropic-messages",
            "https://api.anthropic.com/v1",
            reasoning=True,
            max_tokens=64000,
            context_window=200000,
        )
    )
    chat = deepcopy(result[1])
    chat.provider = "openai-chat"
    chat.api = "openai-completions"
    result.append(chat)
    return result
