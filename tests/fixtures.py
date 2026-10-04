from agent_runtime import Model, Models, ParameterPolicy


def models_for_tests(**kwargs):
    """Synthetic capabilities deliberately have no real model-name heuristics."""
    kwargs.setdefault("env", {})
    registry = Models(**kwargs)
    for name, off in (("test-reasoner", "none"), ("test-always-reasoning", None)):
        registry.register_model(
            Model(
                name,
                "openai",
                "openai-responses",
                "https://example.invalid/v1",
                reasoning=True,
                max_tokens=128000,
                thinking_level_map={"off": off, "minimal": None, "xhigh": "xhigh", "max": "max"},
                compat={"default_reasoning": "medium"},
                parameter_policy=ParameterPolicy(
                    omit_when_reasoning=frozenset({"temperature", "top_p"})
                ),
            )
        )
    return registry
