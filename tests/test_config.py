import json
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from test_providers import response_events, sse

from agent_runtime import (
    Agent,
    AgentContext,
    Model,
    Models,
    ParameterPolicy,
    ProviderConfig,
    RuntimeConfig,
)


class ConfigTests(unittest.IsolatedAsyncioTestCase):
    def test_responses_default_and_luna_model(self):
        with patch.dict("os.environ", {}, clear=True):
            default = Agent()
            self.assertEqual(default.state.model.id, "gpt-6-luna")
            self.assertEqual(default.state.model.api, "openai-responses")
            agent = Agent(model="deployment")
            self.assertEqual(agent.state.model.provider, "openai")
            self.assertEqual(agent.state.model.api, "openai-responses")

    def test_env_configuration_and_explicit_precedence(self):
        env = {
            "AGENT_MODEL": "env-model",
            "OPENAI_BASE_URL": "https://env.invalid/v1/",
            "OPENAI_API_KEY": "env-key",
            "AGENT_MAX_TOKENS": "1000",
            "AGENT_REASONING": "low",
            "AGENT_TIMEOUT_SECONDS": "30",
        }
        agent = Agent(env=env)
        self.assertEqual(agent.state.model.id, "env-model")
        self.assertEqual(agent.state.model.base_url, "https://env.invalid/v1")
        self.assertEqual(agent.parameters["max_tokens"], 1000)
        explicit = Agent(
            env=env,
            model="explicit",
            base_url="https://explicit.invalid/v1",
            api="openai-completions",
            parameters={"max_tokens": 500},
        )
        self.assertEqual(explicit.state.model.id, "explicit")
        self.assertEqual(explicit.state.model.api, "openai-completions")
        self.assertEqual(explicit.state.model.base_url, "https://explicit.invalid/v1")
        self.assertEqual(explicit.parameters["max_tokens"], 500)

    def test_luna_metadata_filters_sampling_without_name_heuristics(self):
        from agent_runtime import normalize_parameters

        model = Models(env={}).get_model()
        report = normalize_parameters(model, {"temperature": 0.3, "top_p": 0.8})
        self.assertEqual(set(report.dropped), {"temperature", "top_p"})
        report = normalize_parameters(model, {"reasoning": "none", "temperature": 0.3})
        self.assertEqual(report.parameters["reasoning"], {"effort": "none"})
        self.assertEqual(report.parameters["temperature"], 0.3)
        report = normalize_parameters(model, {"reasoning": "minimal"})
        self.assertEqual(report.parameters["reasoning"], {"effort": "low"})

    def test_default_does_not_replace_explicit_model(self):
        from agent_runtime import LocalSession

        session = LocalSession(directory=None)
        self.assertEqual(Agent(session=session, env={}).state.model.id, "gpt-6-luna")
        self.assertEqual(
            Agent(session=session, model="deployment", env={}).state.model.id, "deployment"
        )

    async def test_env_endpoint_and_credentials_reach_http_request(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(200, content=sse(response_events()))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            models = Models(
                env={
                    "AGENT_MODEL": "env-model",
                    "OPENAI_API_KEY": "env-key",
                    "OPENAI_BASE_URL": "https://gateway.invalid/v1",
                },
                client=client,
            )
            agent = Agent(models=models)
            await agent.prompt("hello")
        self.assertEqual(str(requests[0].url), "https://gateway.invalid/v1/responses")
        self.assertEqual(requests[0].headers["authorization"], "Bearer env-key")
        self.assertEqual(json.loads(requests[0].content)["model"], "env-model")

    async def test_request_key_overrides_instance_key_overrides_env(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(200, content=sse(response_events()))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            models = Models(
                env={"OPENAI_API_KEY": "env"}, api_keys={"openai": "instance"}, client=client
            )
            model = models.get_model("openai", "deployment")
            await models.complete_simple(model, AgentContext(), {"api_key": "request"})
            await models.complete_simple(model, AgentContext())
        self.assertEqual(
            [r.headers["authorization"] for r in requests], ["Bearer request", "Bearer instance"]
        )

    def test_env_snapshot_and_no_cross_instance_mutation(self):
        environment = {"OPENAI_API_KEY": "first", "OPENAI_BASE_URL": "https://first.invalid/v1"}
        models = Models(env=environment)
        environment["OPENAI_BASE_URL"] = "https://second.invalid/v1"
        self.assertEqual(
            models.get_model("openai", "deployment").base_url, "https://first.invalid/v1"
        )
        other = Models(env=environment)
        self.assertEqual(
            other.get_model("openai", "deployment").base_url, "https://second.invalid/v1"
        )

    def test_explicit_model_and_provider_overrides_environment(self):
        models = Models(env={"OPENAI_BASE_URL": "https://env.invalid/v1"})
        models.register_provider(ProviderConfig("openai", base_url="https://provider.invalid/v1"))
        self.assertEqual(
            models.get_model("openai", "gpt-4.1").base_url, "https://provider.invalid/v1"
        )
        models.register_model(
            Model("gpt-4.1", "openai", "openai-responses", "https://model.invalid/v1")
        )
        self.assertEqual(models.get_model("openai", "gpt-4.1").base_url, "https://model.invalid/v1")

    async def test_provider_policy_applies_in_real_payload_path(self):
        requests = []

        def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, content=sse(response_events()))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            models = Models(
                env={},
                client=client,
                providers=[
                    ProviderConfig(
                        "gateway",
                        base_url="https://gateway.invalid/v1",
                        api_key="test",
                        parameter_policy=ParameterPolicy(
                            fixed_values={"temperature": 1}, unsupported=frozenset({"top_p"})
                        ),
                    )
                ],
            )
            await Agent(
                provider="gateway",
                model="any-name",
                models=models,
                parameters={"temperature": 0.2, "top_p": 0.9},
            ).prompt("go")
        self.assertEqual(requests[0]["temperature"], 1)
        self.assertNotIn("top_p", requests[0])

    def test_invalid_env_errors_do_not_expose_credentials(self):
        for env in (
            {"AGENT_TIMEOUT_SECONDS": "-1"},
            {"AGENT_TIMEOUT_SECONDS": "nan"},
            {"AGENT_MAX_TOKENS": "1.5"},
            {"OPENAI_BASE_URL": "not-a-url"},
        ):
            with self.assertRaises(ValueError):
                RuntimeConfig.from_env(env)
        cfg = RuntimeConfig.from_env({"OPENAI_API_KEY": "secret-marker"})
        self.assertNotIn("secret-marker", repr(cfg))

    def test_template_contains_every_supported_environment_key(self):
        template = (Path(__file__).parents[1] / ".env.template").read_text()
        for name in (
            "AGENT_PROVIDER",
            "AGENT_MODEL",
            "AGENT_API",
            "AGENT_TIMEOUT_SECONDS",
            "AGENT_MAX_TOKENS",
            "AGENT_TEMPERATURE",
            "AGENT_REASONING",
            "OPENAI_API_KEY",
            "OPENAI_BASE_URL",
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_BASE_URL",
        ):
            self.assertIn(name + "=", template)

    def test_blank_optional_environment_values_are_ignored(self):
        cfg = RuntimeConfig.from_env(
            {
                "AGENT_MODEL": "",
                "OPENAI_BASE_URL": "",
                "AGENT_MAX_TOKENS": "",
                "AGENT_TIMEOUT_SECONDS": "",
            }
        )
        self.assertEqual(cfg.model, "gpt-6-luna")
        self.assertEqual(cfg.timeout, 120)
        self.assertEqual(cfg.providers["openai"].base_url, "https://api.openai.com/v1")
