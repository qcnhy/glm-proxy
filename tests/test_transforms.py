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
    def test_responses_direct_is_explicit(self):
        upstream = {"openai_url": "https://example.test", "responses_direct": True}
        self.assertEqual("responses_direct", _route_mode(upstream, True, False))
        self.assertIsNone(_route_mode(upstream, False, False))

    def test_relay_and_messages_follow_capabilities(self):
        upstream = {
            "openai_url": "https://example.test/v1",
            "relay_port": 4444,
            "anthropic_url": "https://example.test/v1/messages",
        }
        self.assertEqual("responses_relay", _route_mode(upstream, True, False))
        self.assertEqual("messages", _route_mode(upstream, False, True))
        self.assertEqual("openai", _route_mode(upstream, False, False))

    def test_responses_never_fall_back_to_anthropic_messages(self):
        messages_only = {"anthropic_url": "https://example.test/v1/messages"}
        self.assertIsNone(_route_mode(messages_only, True, False))
        self.assertEqual("messages", _route_mode(messages_only, False, True))


class ModelSelectionTests(unittest.TestCase):
    def setUp(self):
        self.upstream = {
            "model": "glm-5.3",
            "messages_model": "glm-5.3-flash",
            "available_models": ["glm-5.3", "glm-5.3-flash", "glm-5.2"],
        }

    def test_matching_client_model_is_forwarded(self):
        self.assertEqual("glm-5.2", _select_upstream_model(self.upstream, "glm-5.2"))

    def test_unknown_model_uses_endpoint_default(self):
        self.assertEqual("glm-5.3", _select_upstream_model(self.upstream, "gpt-5.6-sol"))
        self.assertEqual("glm-5.3-flash", _select_upstream_model(self.upstream, "unknown", True))

    def test_missing_catalog_keeps_legacy_default_behavior(self):
        upstream = {"model": "gpt-5.6-sol"}
        self.assertEqual("gpt-5.6-sol", _select_upstream_model(upstream, "gpt-5.4"))


class StreamTimeoutTests(unittest.TestCase):
    def test_timeout_before_output_is_fallback_eligible(self):
        err = _stream_timeout_error(False, 45)
        self.assertEqual("first_output_timeout", err["code"])
        self.assertIn("45s", err["message"])

    def test_timeout_after_output_is_incomplete_not_fallback(self):
        err = _stream_timeout_error(True, 45)
        self.assertEqual("incomplete_stream", err["code"])

    def test_timeout_message_falls_back_to_request_timeout(self):
        err = _stream_timeout_error(False, None)
        self.assertIn("300s", err["message"])


class RequestNormalizationTests(unittest.TestCase):
    def test_exec_rules_are_business_logic_and_idempotent(self):
        body = {"tools": [{"type": "custom", "name": "exec", "description": "run js"}]}
        _inject_tool_rules(body)
        first = body["tools"][0]["description"]
        self.assertIn("PURE V8 JavaScript", first)
        self.assertIn("tools.apply_patch", first)
        _inject_tool_rules(body)
        self.assertEqual(first, body["tools"][0]["description"])

    def test_nested_function_exec_receives_rules(self):
        body = {"tools": [{"type": "function", "function": {
            "name": "functions-exec", "description": "run js"
        }}]}
        _inject_tool_rules(body)
        self.assertIn("tools.apply_patch", body["tools"][0]["function"]["description"])

    def test_gpt_state_is_removed_without_dropping_replayable_history(self):
        body = {
            "previous_response_id": "resp_previous",
            "input": [
                {"type": "message", "role": "user", "content": "keep"},
                {"type": "reasoning", "id": "rs_reasoning", "summary": []},
                {"type": "item_reference", "id": "rs_reference"},
                {"type": "item_reference", "id": "msg_reference"},
                {"type": "item_reference", "id": "fc_reference"},
                {"type": "message", "id": "msg_full", "role": "assistant", "content": "keep"},
                {"type": "function_call_output", "call_id": "call-1", "output": "keep"},
            ],
        }
        self.assertEqual((4, True), _strip_gpt_state(body))
        self.assertNotIn("previous_response_id", body)
        self.assertEqual(
            ["message", "message", "function_call_output"],
            [item["type"] for item in body["input"]],
        )

    def test_additional_tools_move_to_top_level_without_duplicates(self):
        body = {
            "tools": [{"type": "function", "name": "existing"}],
            "input": [
                {"type": "message", "role": "user", "content": "hello"},
                {"type": "additional_tools", "tools": [
                    {"type": "function", "name": "existing"},
                    {"type": "function", "name": "new_tool"},
                ]},
            ],
        }
        _extract_additional_tools(body)
        self.assertEqual(["existing", "new_tool"], [tool["name"] for tool in body["tools"]])
        self.assertEqual(["message"], [item["type"] for item in body["input"]])

    def test_duplicate_tool_outputs_are_merged_in_order(self):
        body = {"input": [
            {"type": "function_call_output", "call_id": "call-1", "output": "head"},
            {"type": "message", "role": "user", "content": "between"},
            {"type": "function_call_output", "call_id": "call-1", "output": "tail"},
        ]}
        self.assertEqual(1, _merge_duplicate_tool_outputs(body))
        self.assertEqual("head\ntail", body["input"][0]["output"])
        self.assertEqual(2, len(body["input"]))


if __name__ == "__main__":
    unittest.main()
