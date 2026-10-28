import unittest

from glm_proxy_app.transforms import (
    _extract_additional_tools,
    _inject_tool_rules,
    _merge_duplicate_tool_outputs,
    _route_mode,
    _select_upstream_model,
    _stream_timeout_error,
    _strip_gpt_state,
)


class RouteModeTests(unittest.TestCase):
    def test_responses_url_mode(self):
        upstream = {"responses_url": "https://example.test/v1/responses"}
        self.assertEqual("responses_url", _route_mode(upstream, True, False))
        self.assertIsNone(_route_mode(upstream, False, False))

    def test_messages_mode(self):
        upstream = {"anthropic_url": "https://example.test/v1/messages"}
        self.assertEqual("messages", _route_mode(upstream, False, True))
        self.assertIsNone(_route_mode(upstream, True, False))

    def test_openai_mode_excludes_responses_url(self):
        upstream = {"openai_url": "https://example.test/v1"}
        self.assertEqual("openai", _route_mode(upstream, False, False))
        with_responses = {"openai_url": "https://example.test/v1",
                          "responses_url": "https://example.test/responses"}
        self.assertEqual("responses_url", _route_mode(with_responses, True, False))
        self.assertIsNone(_route_mode(with_responses, False, False))


class ToolchainTests(unittest.TestCase):
    """v5.2.0 曾误删这三段导致模型无法执行命令——锁定防再删。"""

    def test_extract_additional_tools(self):
        body = {"input": [
            {"type": "additional_tools", "role": "developer",
             "tools": [{"type": "function", "name": "shell_command", "description": "x"}]},
            {"type": "message", "role": "user", "content": "hi"},
        ], "tools": []}
        _extract_additional_tools(body)
        self.assertEqual([t["name"] for t in body["tools"]], ["shell_command"])
        self.assertEqual(len(body["input"]), 1)

    def test_extract_additional_tools_idempotent(self):
        body = {"input": [
            {"type": "additional_tools", "role": "developer",
             "tools": [{"type": "function", "name": "exec", "description": "x"}]},
        ], "tools": []}
        _extract_additional_tools(body)
        _extract_additional_tools(body)
        self.assertEqual(len(body["tools"]), 1)

    def test_inject_tool_rules_targets_exec(self):
        body = {"tools": [
            {"type": "function", "name": "functions-exec",
             "description": "run js",
             "parameters": {"type": "object"}},
        ]}
        _inject_tool_rules(body)
        self.assertIn("V8", body["tools"][0]["description"])
        # 幂等
        once = body["tools"][0]["description"]
        _inject_tool_rules(body)
        self.assertEqual(once, body["tools"][0]["description"])

    def test_inject_tool_rules_ignores_other_tools(self):
        body = {"tools": [{"type": "function", "name": "shell_command",
                           "description": "x", "parameters": {}}]}
        _inject_tool_rules(body)
        self.assertEqual(body["tools"][0]["description"], "x")


class ModelSelectionTests(unittest.TestCase):
    def test_case_insensitive_available_models(self):
        upstream = {"model": "default-model",
                    "available_models": ["GLM-5.3-Flash"]}
        self.assertEqual(_select_upstream_model(upstream, "glm-5.3-flash"), "GLM-5.3-Flash")
        self.assertEqual(_select_upstream_model(upstream, "other"), "default-model")


if __name__ == "__main__":
    unittest.main()
