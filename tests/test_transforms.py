import unittest

from glm_proxy_app.transforms import (
    _extract_additional_tools,
    _merge_duplicate_tool_outputs,
    _route_mode,
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


class RequestNormalizationTests(unittest.TestCase):
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
