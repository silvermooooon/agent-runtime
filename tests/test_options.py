import unittest
from dataclasses import replace

from fixtures import models_for_tests as Models
from test_loop import add_tool

from agent_runtime import AgentContext, Model, ParameterPolicy, normalize_parameters, user_message
from agent_runtime.ai import adjust_max_tokens_for_thinking, clamp_thinking_level
from agent_runtime.validation import validate_tool_arguments


class OptionTests(unittest.TestCase):
    def setUp(self):
        self.models = Models()
        self.reasoner = self.models.get_model("openai", "test-reasoner")

    def test_reasoner_default_and_reasoning_remove_sampling(self):
        for level in (None, "medium", "high"):
            report = normalize_parameters(
                self.reasoner,
                {"temperature": 0.3, "top_p": 0.8, "reasoning": level, "unknown": "ignored"},
            )
            self.assertNotIn("temperature", report.parameters)
            self.assertNotIn("top_p", report.parameters)
            self.assertEqual(set(report.dropped), {"temperature", "top_p", "unknown"})

    def test_reasoner_reasoning_none_permits_sampling(self):
        report = normalize_parameters(self.reasoner, {"reasoning": "none", "temperature": 0.3})
        self.assertEqual(report.parameters["temperature"], 0.3)
        self.assertEqual(report.parameters["reasoning"]["effort"], "none")

    def test_fixed_temperature_metadata(self):
        model = Model(
            "fixed",
            "custom",
            "openai-completions",
            "https://example.invalid",
            compat={"fixed_temperature": 1},
        )
        report = normalize_parameters(model, {"temperature": 0.5})
        self.assertEqual(report.parameters["temperature"], 1)
        self.assertEqual(report.adjusted["temperature"], {"from": 0.5, "to": 1})
        self.assertEqual(
            normalize_parameters(model, {"temperature": 1}).parameters["temperature"], 1
        )

    def test_sampling_cannot_bypass_filter_via_raw_payload(self):
        report = normalize_parameters(
            self.reasoner,
            {
                "sampling_params": {"temperature": 0.4},
                "input": [],
                "model": "other",
                "stream": False,
            },
        )
        self.assertEqual(set(report.dropped), {"sampling_params", "input", "model", "stream"})

    def test_provider_specific_max_token_fields(self):
        chat = self.models.get_model("openai-chat", "gpt-4.1")
        ant = self.models.get_model("anthropic", "claude-sonnet-4-20250514")
        for model, field in (
            (self.reasoner, "max_output_tokens"),
            (chat, "max_completion_tokens"),
            (ant, "max_tokens"),
        ):
            self.assertEqual(
                normalize_parameters(model, {"max_tokens": 500}).parameters[field], 500
            )
        compat = replace(chat, compat={"max_tokens_field": "max_tokens"})
        self.assertEqual(
            normalize_parameters(compat, {"max_tokens": 500}).parameters["max_tokens"], 500
        )

    def test_cap_minimum_and_model_maximum(self):
        small = normalize_parameters(self.reasoner, {"max_tokens": 2})
        self.assertEqual(small.parameters["max_output_tokens"], 16)
        large = normalize_parameters(self.reasoner, {"max_tokens": 999999})
        self.assertEqual(large.parameters["max_output_tokens"], self.reasoner.max_tokens)
        self.assertIn("max_tokens", large.adjusted)

    def test_invalid_sampling_and_token_values_dropped(self):
        model = self.models.get_model("openai", "gpt-4.1")
        for value in (float("nan"), float("inf"), -1, 3, True, "0.5"):
            self.assertNotIn(
                "temperature", normalize_parameters(model, {"temperature": value}).parameters
            )
        for value in (0, -1, True, 2.5, "10"):
            self.assertIn("max_tokens", normalize_parameters(model, {"max_tokens": value}).dropped)

    def test_non_reasoning_model_drops_reasoning(self):
        model = self.models.get_model("openai", "gpt-4.1")
        report = normalize_parameters(model, {"reasoning": "high", "reasoning_summary": "auto"})
        self.assertNotIn("reasoning", report.parameters)
        self.assertEqual(set(report.dropped), {"reasoning", "reasoning_summary"})

    def test_pi_thinking_clamp(self):
        self.assertEqual(clamp_thinking_level(self.reasoner, "minimal"), "low")
        always_reasoning = self.models.get_model("openai", "test-always-reasoning")
        self.assertEqual(clamp_thinking_level(always_reasoning, "off"), "low")
        older = Model("older", "custom", "openai-responses", "", reasoning=True)
        self.assertEqual(clamp_thinking_level(older, "max"), "high")

    def test_anthropic_thinking_filters_sampling_and_budget(self):
        model = self.models.get_model("anthropic", "claude-sonnet-4-20250514")
        report = normalize_parameters(
            model,
            {
                "reasoning": "low",
                "temperature": 1,
                "max_tokens": 1000,
                "thinking_budgets": {"low": 3000},
            },
        )
        self.assertNotIn("temperature", report.parameters)
        self.assertEqual(report.parameters["thinking"]["budget_tokens"], 3000)
        self.assertEqual(report.parameters["max_tokens"], 4000)
        self.assertEqual(adjust_max_tokens_for_thinking(1000, 2000, "high"), (2000, 976))

    def test_anthropic_metadata_and_api_specific_fields(self):
        model = self.models.get_model("anthropic", "claude-sonnet-4-20250514")
        report = normalize_parameters(
            model,
            {
                "metadata": {"user_id": "test", "other": 123},
                "service_tier": "fast",
                "tool_choice": "required",
            },
        )
        self.assertEqual(report.parameters["metadata"], {"user_id": "test"})
        self.assertEqual(report.parameters["tool_choice"], {"type": "any"})
        self.assertIn("service_tier", report.dropped)

    def test_normalization_does_not_mutate_input(self):
        options = {"reasoning": "minimal", "temperature": 0.5, "max_tokens": 1}
        original = options.copy()
        normalize_parameters(self.reasoner, options)
        self.assertEqual(options, original)

    def test_model_registry_is_instance_local_and_returns_copies(self):
        self.reasoner.compat["fixed_temperature"] = 1
        self.assertNotIn(
            "fixed_temperature", self.models.get_model("openai", "test-reasoner").compat
        )
        self.models.register_model(Model("custom", "private", "custom", ""))
        with self.assertRaises(ValueError):
            Models().get_model("private", "custom")

    def test_optional_null_and_union_schema_validation(self):
        tool = add_tool()
        tool.parameters["properties"]["optional"] = {"type": "string"}
        arguments = {"a": "2", "b": 3, "optional": None}
        self.assertEqual(validate_tool_arguments(tool, arguments), {"a": 2, "b": 3})
        self.assertIn("optional", arguments)
        tool.parameters["properties"]["a"] = {"anyOf": [{"type": "integer"}, {"type": "null"}]}
        self.assertEqual(validate_tool_arguments(tool, {"a": "4", "b": 1})["a"], 4)

    def test_provider_and_model_policies_merge_without_changing_model_identity(self):
        model = Model(
            "arbitrary",
            "tenant",
            "openai-responses",
            "",
            reasoning=True,
            parameter_policy=ParameterPolicy(
                fixed_values={"temperature": 1},
                value_maps={"reasoning": {"fast": "low"}},
                ranges={"top_p": (0.2, 0.8)},
            ),
        )
        provider = ParameterPolicy(
            unsupported=frozenset({"metadata"}), fixed_values={"temperature": 0.5}
        )
        report = normalize_parameters(
            model,
            {"temperature": 0.3, "reasoning": "fast", "top_p": 0.9, "metadata": {}},
            provider_policy=provider,
        )
        self.assertEqual(report.parameters["temperature"], 1)
        self.assertEqual(report.parameters["reasoning"]["effort"], "low")
        self.assertEqual(report.parameters["top_p"], 0.8)
        self.assertIn("metadata", report.dropped)
        self.assertEqual(model.id, "arbitrary")

    def test_unsupported_takes_precedence_over_fixed_rewrite(self):
        model = Model(
            "arbitrary",
            "tenant",
            "openai-responses",
            "",
            parameter_policy=ParameterPolicy(
                unsupported=frozenset({"temperature"}), fixed_values={"temperature": 1}
            ),
        )
        report = normalize_parameters(model, {"temperature": 0.2})
        self.assertNotIn("temperature", report.parameters)
        self.assertIn("temperature", report.dropped)

    def test_unknown_capabilities_apply_api_rules_without_name_heuristics(self):
        model = self.models.get_model("openai", "my-gateway-deployment")
        self.assertIsNone(model.reasoning)
        report = normalize_parameters(model, {"reasoning": "max", "unknown": 1})
        self.assertEqual(report.parameters["reasoning"]["effort"], "max")
        self.assertIn("unknown", report.dropped)

    def test_context_budget_is_clamped_using_pi_estimator(self):
        model = Model(
            "limited", "custom", "openai-responses", "", context_window=8192, max_tokens=8000
        )
        report = normalize_parameters(
            model, {"max_tokens": 8000}, context=AgentContext([user_message("x" * 400)])
        )
        self.assertEqual(report.parameters["max_output_tokens"], 8192 - 4096 - 100)
