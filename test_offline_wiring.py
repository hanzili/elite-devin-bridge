import json
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from dataclasses import replace
from bridge_core import Rejected
from bridge_store import Ledger
from offline_wiring import OfflineBridge, OutputPolicy, NOTICES
from test_bridge_core import POLICY
from test_bridge_store import KEY, ROOT, command


def frame(n=1, action="run", text="Describe this test workspace only.", **event_overrides):
    body = {"request_id": f"request_{n:03}", "action": action}
    event = {"type": "message", "channel": "C_TEST", "user": "U_OWNER"}
    if action == "run":
        body["workspace"] = "scratch"; event["ts"] = ROOT
    else:
        event["ts"] = f"1700000001.{n:06}"; event["thread_ts"] = ROOT
    if action in {"run", "reply"}:
        body["text"] = text
    event["text"] = ("<@U_WORKER> " if action == "run" else "") + "bridge.v1 " + json.dumps(body)
    event.update(event_overrides)
    return {"type": "events_api", "envelope_id": f"envelope-{n}", "payload": {
        "type": "event_callback", "team_id": "T_TEST", "api_app_id": "A_WORKER",
        "event_id": f"Ev{n}", "event": event}}


class WiringTests(unittest.TestCase):
    def setUp(self):
        self.ledger = Ledger(":memory:")
        self.bridge = OfflineBridge(self.ledger, POLICY, "A_WORKER", {"scratch": "/sandbox/demo"})

    def tearDown(self):
        self.ledger.close()

    def feed(self, packet):
        self.bridge.feed_acp_fixture((json.dumps(packet) + "\n").encode())

    def handshake(self, load=True):
        request_id = self.bridge.initialize()
        self.feed({"jsonrpc": "2.0", "id": request_id,
                   "result": {"protocolVersion": 1, "agentCapabilities": {"loadSession": load}}})

    def running(self):
        self.handshake(); self.bridge.receive_socket_fixture(frame()); self.bridge.dispatch(KEY)
        session_id = self.bridge.acp.packets()[-1]["id"]
        self.feed({"jsonrpc": "2.0", "id": session_id, "result": {"sessionId": "session-001"}})
        return self.bridge.acp.packets()[-1]["id"]

    def update(self, text, kind="agent_message_chunk", session="session-001"):
        self.feed({"jsonrpc": "2.0", "method": "session/update", "params": {
            "sessionId": session, "update": {"sessionUpdate": kind,
                                             "content": {"type": "text", "text": text}}}})

    def finish(self, request_id, reason="end_turn"):
        self.feed({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": reason}})

    def flush(self):
        while self.bridge.flush_one_slack_fixture():
            pass
        return self.bridge.slack.messages

    def test_persist_before_ack(self):
        with patch.object(self.ledger, "accept", side_effect=sqlite3.OperationalError("test DB failure")):
            with self.assertRaises(sqlite3.OperationalError):
                self.bridge.receive_socket_fixture(frame())
        self.assertEqual(self.bridge.slack.acks, [])

    def test_missing_or_conflicting_receiver_identity_rejected(self):
        for app_id in (None, "", "A_DOT", "U_WORKER", "<unknown>"):
            with self.subTest(app_id=app_id), self.assertRaises(Rejected):
                OfflineBridge(self.ledger, POLICY, app_id, {"scratch": "/sandbox/demo"})

    def test_duplicate_socket_delivery_ack_without_duplicate_output(self):
        self.assertTrue(self.bridge.receive_socket_fixture(frame()))
        self.assertTrue(self.bridge.receive_socket_fixture(frame()))
        self.assertEqual(len(self.bridge.slack.acks), 2)
        self.assertEqual(len(self.flush()), 1)

    def test_receiver_app_id_not_confused_with_sender_app_id(self):
        value = frame(user="U_DOT", bot_id="B_DOT", app_id="A_DOT")
        self.assertTrue(self.bridge.receive_socket_fixture(value))
        wrong = frame(2); wrong["payload"]["api_app_id"] = "A_DOT"
        self.assertFalse(self.bridge.receive_socket_fixture(wrong))

    def test_other_sender_is_acked_but_never_queued(self):
        self.assertFalse(self.bridge.receive_socket_fixture(frame(user="U_INTRUDER")))
        self.assertEqual(len(self.bridge.slack.acks), 1)
        self.assertEqual(self.ledger.db.execute("SELECT COUNT(*) FROM commands").fetchone()[0], 0)
        self.assertEqual(self.flush(), [])

    def test_unmentioned_run_is_acked_but_not_created(self):
        value = frame()
        value["payload"]["event"]["text"] = value["payload"]["event"]["text"].removeprefix("<@U_WORKER> ")
        self.assertFalse(self.bridge.receive_socket_fixture(value))
        self.assertEqual(len(self.bridge.slack.acks), 1)
        self.assertEqual(self.ledger.db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 0)
        self.assertEqual(self.bridge.acp.lines, [])

    def test_dot_mismatched_user_is_not_created(self):
        self.assertFalse(self.bridge.receive_socket_fixture(frame(bot_id="B_DOT", app_id="A_DOT")))
        self.assertEqual(self.ledger.db.execute("SELECT COUNT(*) FROM commands").fetchone()[0], 0)

    def test_mentioned_dot_round_trip_stays_in_same_thread(self):
        self.handshake()
        self.assertTrue(self.bridge.receive_socket_fixture(frame(user="U_DOT", bot_id="B_DOT", app_id="A_DOT")))
        self.bridge.dispatch(KEY)
        session_request = self.bridge.acp.packets()[-1]["id"]
        self.feed({"jsonrpc": "2.0", "id": session_request, "result": {"sessionId": "session-001"}})
        prompt_id = self.bridge.acp.packets()[-1]["id"]
        self.assertTrue(self.bridge.receive_socket_fixture(frame(2, "cancel", user="U_DOT", bot_id="B_DOT", app_id="A_DOT")))
        self.finish(prompt_id, "cancelled")
        self.assertTrue(all(m["channel"] == "C_TEST" and m["thread_ts"] == ROOT for m in self.flush()))

    def test_worker_echo_is_not_a_command(self):
        self.assertFalse(self.bridge.receive_socket_fixture(frame(bot_id="B_WORKER", app_id="A_WORKER")))
        self.assertEqual(self.flush(), [])

    def test_wrong_workspace_or_channel_does_not_dispatch(self):
        wrong = frame(); wrong["payload"]["team_id"] = "T_OTHER"
        self.assertFalse(self.bridge.receive_socket_fixture(wrong))
        self.assertFalse(self.bridge.receive_socket_fixture(frame(channel="C_OTHER")))
        self.assertEqual(self.bridge.acp.lines, [])

    def test_handshake_required_before_dispatch(self):
        self.bridge.receive_socket_fixture(frame())
        with self.assertRaises(Rejected):
            self.bridge.dispatch(KEY)

    def test_foreign_route_dispatch_rejected(self):
        self.handshake()
        foreign = replace(command(), team_id="T_OTHER", channel_id="C_OTHER")
        self.ledger.accept(foreign)
        from bridge_store import ThreadKey
        key = ThreadKey("T_OTHER", "C_OTHER", ROOT)
        with self.assertRaises(Rejected):
            self.bridge.dispatch(key)
        with self.assertRaises(Rejected):
            self.bridge.restore(key)
        self.assertEqual(self.ledger.snapshot(key)["state"], "queued")

    def test_foreign_channel_session_update_fails_closed(self):
        self.running()
        foreign = replace(command(3), channel_id="C_OTHER")
        self.ledger.accept(foreign)
        from bridge_store import ThreadKey
        key = ThreadKey("T_TEST", "C_OTHER", ROOT)
        self.ledger.reserve_prompt(key); self.ledger.bind_session(key, "request_003", "foreign-session")
        self.update("Do not route this", session="foreign-session")
        self.assertFalse(self.bridge._initialized)
        self.assertNotIn("Do not route this", json.dumps(self.flush()))

    def test_unsupported_acp_version_stays_blocked(self):
        request_id = self.bridge.initialize()
        self.feed({"jsonrpc": "2.0", "id": request_id, "result": {"protocolVersion": 2}})
        self.assertFalse(self.bridge._initialized)

    def test_no_advertised_terminal_or_fs_capabilities(self):
        self.bridge.initialize()
        self.assertEqual(self.bridge.acp.packets()[0]["params"]["clientCapabilities"], {})

    def test_end_to_end_fixture_with_default_withholding(self):
        request_id = self.running(); self.update("Ordinary-looking model text also stays local.")
        self.finish(request_id)
        messages = self.flush()
        self.assertEqual([m["text"] for m in messages if not m["text"].startswith("bridge.summary.v1")],
                         [NOTICES["accepted"], NOTICES["working"], NOTICES["complete"]])
        self.assertTrue(any(m["text"].startswith("bridge.summary.v1") for m in messages))
        self.assertTrue(all(m["channel"] == "C_TEST" and m["thread_ts"] == ROOT for m in messages))

    def test_preapproved_exact_nonce_output(self):
        self.bridge.output_policy = OutputPolicy(frozenset({"PONG: smoke-2026"}))
        request_id = self.running(); self.update("PONG: "); self.update("smoke-2026")
        self.finish(request_id)
        self.assertEqual(self.flush()[-1]["text"], "PONG: smoke-2026")

    def test_unapproved_extra_output_is_not_released(self):
        self.bridge.output_policy = OutputPolicy(frozenset({"PONG: smoke-2026"}))
        request_id = self.running(); self.update("PONG: smoke-2026\nAdditional private data")
        self.finish(request_id)
        self.assertEqual(self.flush()[-1]["text"], NOTICES["complete"])

    def test_secret_is_not_released_even_if_allowlisted(self):
        secret = "xoxb-TEST-SECRET-NOT-A-CREDENTIAL"
        self.bridge.output_policy = OutputPolicy(frozenset({secret}))
        request_id = self.running(); self.update(secret); self.finish(request_id)
        self.assertNotIn(secret, json.dumps(self.flush()))

    def test_mentions_are_not_released_even_if_allowlisted(self):
        self.bridge.output_policy = OutputPolicy(frozenset({"<!channel> ping"}))
        request_id = self.running(); self.update("<!channel> ping"); self.finish(request_id)
        self.assertNotIn("<!channel>", json.dumps(self.flush()))

    def test_reasoning_and_tool_chunks_never_release(self):
        self.bridge.output_policy = OutputPolicy(frozenset({"EXACT-TEST"}))
        request_id = self.running()
        self.update("EXACT-TEST", "agent_thought_chunk")
        self.update("EXACT-TEST", "tool_call")
        self.finish(request_id)
        self.assertEqual(self.flush()[-1]["text"], NOTICES["complete"])

    def test_unknown_session_update_cannot_poison_result(self):
        self.bridge.output_policy = OutputPolicy(frozenset({"EXACT-TEST"}))
        request_id = self.running(); self.update("EXACT-TEST", session="other-session")
        self.finish(request_id)
        self.assertEqual(self.flush()[-1]["text"], NOTICES["complete"])

    def test_permission_always_denied_and_raw_details_not_sent(self):
        self.running()
        self.feed({"jsonrpc": "2.0", "id": "perm-test", "method": "session/request_permission",
                   "params": {"sessionId": "session-001", "toolCall": {
                       "title": "PRIVATE-TOOL-TITLE", "rawInput": {"secret": "PRIVATE-RAW-DATA"}},
                       "options": [{"optionId": "allow-all", "kind": "allow_always"}]}})
        self.assertEqual(self.bridge.acp.packets()[-1]["result"]["outcome"]["outcome"], "cancelled")
        outgoing = json.dumps(self.flush())
        self.assertNotIn("PRIVATE-", outgoing)
        self.assertIn(NOTICES["out_of_scope"], outgoing)

    def test_unsupported_agent_method_never_executes(self):
        self.running()
        self.feed({"jsonrpc": "2.0", "id": "terminal-test", "method": "terminal/create",
                   "params": {"command": "NEVER-EXECUTE-THIS"}})
        self.assertEqual(self.bridge.acp.packets()[-1]["error"]["code"], -32601)
        self.assertNotIn("NEVER-EXECUTE-THIS", json.dumps(self.flush()))

    def test_ambiguous_permission_denial_freezes(self):
        self.running(); self.bridge.acp.fail_after_next_write = True
        self.feed({"jsonrpc": "2.0", "id": "permission", "method": "session/request_permission",
                   "params": {"sessionId": "session-001"}})
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")
        self.assertFalse(self.bridge._initialized)

    def test_cancel_before_session_response_never_prompts(self):
        self.handshake(); self.bridge.receive_socket_fixture(frame()); self.bridge.dispatch(KEY)
        session_request = self.bridge.acp.packets()[-1]["id"]
        self.bridge.receive_socket_fixture(frame(2, "cancel"))
        self.feed({"jsonrpc": "2.0", "id": session_request, "result": {"sessionId": "late-session"}})
        self.assertFalse(any(p.get("method") == "session/prompt" for p in self.bridge.acp.packets()))
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "cancelled")

    def test_cancel_running_waits_for_terminal_response(self):
        request_id = self.running(); self.bridge.receive_socket_fixture(frame(2, "cancel"))
        self.assertEqual(self.bridge.acp.packets()[-1]["method"], "session/cancel")
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "cancel_requested")
        self.finish(request_id, "cancelled")
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "cancelled")
        self.assertEqual(self.flush()[-1]["text"], NOTICES["stopping"])
        self.bridge.confirm_exit(KEY, True)
        self.assertEqual(self.flush()[-1]["text"], NOTICES["stopped_verified"])

    def test_duplicate_cancel_event_does_not_send_twice(self):
        self.running(); self.bridge.receive_socket_fixture(frame(2, "cancel"))
        self.bridge.receive_socket_fixture(frame(2, "cancel"))
        self.assertEqual(sum(p.get("method") == "session/cancel" for p in self.bridge.acp.packets()), 1)

    def test_ambiguous_cancel_write_invalidates_connection(self):
        self.running(); self.bridge.acp.fail_after_next_write = True
        self.bridge.receive_socket_fixture(frame(2, "cancel"))
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")
        self.assertFalse(self.bridge._initialized)

    def test_ambiguous_prompt_write_freezes(self):
        self.handshake(); self.bridge.receive_socket_fixture(frame()); self.bridge.dispatch(KEY)
        session_request = self.bridge.acp.packets()[-1]["id"]
        self.bridge.acp.fail_after_next_write = True
        self.feed({"jsonrpc": "2.0", "id": session_request, "result": {"sessionId": "session-001"}})
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")
        with self.assertRaises(Rejected):
            self.bridge.dispatch(KEY)

    def test_malformed_pending_response_freezes_active(self):
        request_id = self.running()
        self.feed({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": ["invalid"]}})
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")

    def test_malformed_stdio_freezes_and_never_echoes(self):
        self.running(); self.bridge.feed_acp_fixture(b"PRIVATE INVALID JSON\n")
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")
        self.assertNotIn("PRIVATE INVALID", json.dumps(self.flush()))

    def test_oversized_stdio_freezes(self):
        self.running(); self.bridge.feed_acp_fixture(b"x" * 262145)
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")

    def test_chunked_stdio_is_parsed(self):
        request_id = self.bridge.initialize()
        raw = json.dumps({"jsonrpc": "2.0", "id": request_id, "result": {"protocolVersion": 1}}).encode() + b"\n"
        self.bridge.feed_acp_fixture(raw[:12]); self.bridge.feed_acp_fixture(raw[12:])
        self.assertTrue(self.bridge._initialized)

    def test_large_agent_text_is_withheld_without_persisting(self):
        request_id = self.running(); self.update("x" * 65537); self.finish(request_id)
        self.assertEqual(self.flush()[-1]["text"], NOTICES["complete"])

    def test_uncertain_slack_send_is_not_retried(self):
        self.bridge.receive_socket_fixture(frame())
        self.bridge.slack.fail_after_next_send = True
        self.assertTrue(self.bridge.flush_one_slack_fixture())
        self.assertFalse(self.bridge.flush_one_slack_fixture())
        self.assertEqual(len(self.bridge.slack.messages), 1)
        self.assertEqual(self.ledger.db.execute("SELECT state FROM outbox").fetchone()[0], "uncertain")

    def test_missing_accepted_outbox_intent_is_repaired(self):
        # Simulate crash after command commit and before notice commit.
        self.ledger.accept(command())
        self.assertEqual(self.flush()[0]["text"], NOTICES["accepted"])
        self.assertFalse(self.bridge.flush_one_slack_fixture())

    def test_missing_final_outbox_intent_is_repaired(self):
        request_id = self.running()
        # Simulate observed completion committed by runtime before an abrupt exit.
        self.ledger.finish_turn(KEY, "request_001", "end_turn")
        self.assertEqual(self.flush()[-1]["text"], NOTICES["complete"])
        self.assertFalse(self.bridge.flush_one_slack_fixture())

    def test_repair_preserves_uncertain_outbox(self):
        self.bridge.receive_socket_fixture(frame())
        item = self.ledger.reserve_outbox("T_TEST", "C_TEST")
        self.ledger.recover_inflight()
        self.assertFalse(self.bridge.flush_one_slack_fixture())
        self.assertEqual(self.bridge.slack.messages, [])

    def test_untrusted_outbox_content_fails_closed(self):
        self.bridge.receive_socket_fixture(frame()); self.flush()
        self.ledger.queue_outbox(KEY, "bad-content", "PRIVATE-UNAPPROVED")
        self.assertFalse(self.bridge.flush_one_slack_fixture())
        self.assertNotIn("PRIVATE-UNAPPROVED", json.dumps(self.bridge.slack.messages))

    def test_recovery_gate_uses_supported_load_and_exact_session(self):
        request_id = self.running(); self.finish(request_id)
        self.ledger.recover_inflight()
        self.assertTrue(self.bridge.restore(KEY))
        packet = self.bridge.acp.packets()[-1]
        self.assertEqual(packet["method"], "session/load")
        self.assertEqual(packet["params"]["sessionId"], "session-001")
        self.feed({"jsonrpc": "2.0", "id": packet["id"], "result": {}})
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "ready")

    def test_invalidated_idle_connection_requires_restore_after_reinitialize(self):
        request_id = self.running(); self.finish(request_id)
        self.bridge.receive_socket_fixture(frame(2, "reply"))
        self.bridge.feed_acp_fixture(b"invalid protocol\n")
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "restoring")
        self.handshake()
        self.assertFalse(self.bridge.dispatch(KEY))
        self.assertTrue(self.bridge.restore(KEY))
        load_id = self.bridge.acp.packets()[-1]["id"]
        self.feed({"jsonrpc": "2.0", "id": load_id, "result": {}})
        self.assertTrue(self.bridge.dispatch(KEY))

    def test_connection_invalidation_does_not_touch_other_routes(self):
        self.running()
        foreign = replace(command(3), channel_id="C_OTHER")
        self.ledger.accept(foreign)
        from bridge_store import ThreadKey
        key = ThreadKey("T_TEST", "C_OTHER", ROOT)
        self.ledger.reserve_prompt(key); self.ledger.bind_session(key, "request_003", "foreign-session")
        self.ledger.begin_prompt_send(key, "request_003")
        self.ledger.mark_prompt_sent(key, "request_003")
        self.ledger.finish_turn(key, "request_003", "end_turn")
        self.bridge.feed_acp_fixture(b"invalid protocol\n")
        self.assertEqual(self.ledger.snapshot(key)["state"], "ready")

    def test_multithread_runtime_use_is_rejected(self):
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.bridge.receive_socket_fixture, frame())
            with self.assertRaises(Rejected):
                future.result()


if __name__ == "__main__":
    unittest.main()
