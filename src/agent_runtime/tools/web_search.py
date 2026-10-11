"""OpenAI Responses native web search; execution stays on the provider."""

from copy import deepcopy
from typing import Literal

from ..types import ProviderTool


def create_web_search_tool(
    *,
    search_context_size: Literal["low", "medium", "high"] | None = None,
    external_web_access: bool | None = None,
    filters: dict | None = None,
    user_location: dict | None = None,
) -> ProviderTool:
    """Pass to Agent(tools=...) to enable; omitted options use the provider's defaults."""
    if search_context_size is not None and search_context_size not in ("low", "medium", "high"):
        raise ValueError("search_context_size must be low, medium or high")
    options = {"type": "web_search"}
    for key, value in (
        ("search_context_size", search_context_size),
        ("external_web_access", external_web_access),
        ("filters", filters),
        ("user_location", user_location),
    ):
        if value is not None:
            options[key] = deepcopy(value)
    return ProviderTool("web_search", "openai-responses", options)
