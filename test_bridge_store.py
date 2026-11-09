import json
from pathlib import Path
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from bridge_core import parse_command, Rejected
from bridge_store import Ledger, ThreadKey
from test_bridge_core import POLICY


ROOT = "1700000000.000001"
KEY = ThreadKey("T_TEST", "C_TEST", ROOT)


def command(n=1, action="run", root=ROOT, text="inspect only", event_id=None, request_id=None):
    payload = {"request_id": request_id or f"request_{n:03}", "action": action}
    message = {"type": "message", "channel": "C_TEST", "user": "U_OWNER"}
    if action == "run":
        payload["workspace"] = "scratch"
        message["ts"] = root
    else:
        message["ts"] = f"1700000001.{n:06}"
        message["thread_ts"] = root
    if action in {"run", "reply"}:
        payload["text"] = text
    message["text"] = ("<@U_WORKER> " if action == "run" else "") + "bridge.v1 " + json.dumps(payload)
    return parse_command({"type": "event_callback", "team_id": "T_TEST",
                          "event_id": event_id or f"Ev{n}", "event": message}, POLICY)


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "ledger.sqlite")
        self.ledger = Ledger(self.path)

    def tearDown(self):
        self.ledger.close()
        self.directory.cleanup()

    def start(self):
        self.ledger.accept(command())
        reserved = self.ledger.reserve_prompt(KEY)
        self.ledger.bind_session(KEY, reserved.request_id, "session-001")
        self.ledger.begin_prompt_send(KEY, reserved.request_id)
        self.ledger.mark_prompt_sent(KEY, reserved.request_id)
        return reserved

    def test_persist_and_event_replay(self):
        self.assertFalse(self.ledger.accept(command()).duplicate)
        self.ledger.close(); self.ledger = Ledger(self.path)
        self.assertTrue(self.ledger.accept(command()).duplicate)
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "queued")

    def test_v1_ledger_upgrades_with_outbox(self):
        # Only this test's empty temporary database is modified.
        self.ledger.db.execute("DROP TABLE outbox")
        self.ledger.db.execute("PRAGMA user_version = 1")
        self.ledger.close(); self.ledger = Ledger(self.path)
        self.assertEqual(self.ledger.db.execute("PRAGMA user_version").fetchone()[0], 3)
        self.ledger.accept(command())
        self.ledger.queue_outbox(KEY, "migration-test", "Fixed test notice")
        self.assertIsNotNone(self.ledger.reserve_outbox("T_TEST", "C_TEST"))

    def test_same_message_different_event_id(self):
        self.ledger.accept(command())
        self.assertTrue(self.ledger.accept(command(event_id="EvReplay")).duplicate)
        self.assertEqual(self.ledger.db.execute("SELECT COUNT(*) FROM commands").fetchone()[0], 1)

    def test_event_id_reuse_rejected(self):
        self.ledger.accept(command())
        with self.assertRaisesRegex(Rejected, "event id"):
            self.ledger.accept(command(text="changed"))

    def test_message_reuse_rejected(self):
        self.ledger.accept(command())
        with self.assertRaisesRegex(Rejected, "message identity"):
            self.ledger.accept(command(2, request_id="request_002"))

    def test_request_id_cannot_move_threads(self):
        self.ledger.accept(command())
        with self.assertRaisesRegex(Rejected, "request id"):
            self.ledger.accept(command(2, root="1700000002.000001", request_id="request_001"))

    def test_reply_request_replay_different_slack_message(self):
        self.ledger.accept(command())
        self.ledger.accept(command(2, "reply"))
        self.assertTrue(self.ledger.accept(command(3, "reply", request_id="request_002")).duplicate)

    def test_unbound_thread_rejected(self):
        for action in ("reply", "cancel", "status"):
            with self.subTest(action=action), self.assertRaises(Rejected):
                self.ledger.accept(command(2, action))

    def test_reserve_only_one_prompt(self):
        self.ledger.accept(command())
        self.assertIsNotNone(self.ledger.reserve_prompt(KEY))
        self.assertIsNone(self.ledger.reserve_prompt(KEY))

    def test_competing_connections_reserve_once(self):
        self.ledger.accept(command())
        def reserve(_):
            ledger = Ledger(self.path)
            try:
                return ledger.reserve_prompt(KEY)
            finally:
                ledger.close()
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(reserve, range(4)))
        self.assertEqual(sum(value is not None for value in results), 1)

    def test_session_cannot_be_rebound(self):
        self.start()
        with self.assertRaises(Rejected):
            self.ledger.bind_session(KEY, "request_001", "another-session")

    def test_session_cannot_cross_threads(self):
        self.start()
        root = "1700000003.000001"
        key = ThreadKey("T_TEST", "C_TEST", root)
        self.ledger.accept(command(3, root=root))
        self.ledger.reserve_prompt(key)
        with self.assertRaisesRegex(Rejected, "already bound"):
            self.ledger.bind_session(key, "request_003", "session-001")

    def test_cross_thread_completion_rejected(self):
        self.start()
        with self.assertRaises(Rejected):
            self.ledger.finish_turn(ThreadKey("T_TEST", "C_OTHER", ROOT), "request_001", "end_turn")

    def test_complete_then_reply_same_session(self):
        self.start()
        self.ledger.accept(command(2, "reply"))
        self.assertIsNone(self.ledger.reserve_prompt(KEY))
        self.ledger.finish_turn(KEY, "request_001", "end_turn")
        next_turn = self.ledger.reserve_prompt(KEY)
        self.assertEqual(next_turn.session_id, "session-001")
        self.assertEqual(next_turn.request_id, "request_002")

    def test_no_completion_before_prompt_sent(self):
        self.ledger.accept(command()); self.ledger.reserve_prompt(KEY)
        self.ledger.bind_session(KEY, "request_001", "session-001")
        with self.assertRaises(Rejected):
            self.ledger.finish_turn(KEY, "request_001", "end_turn")

    def test_queue_limit(self):
        self.ledger.close(); self.ledger = Ledger(self.path, max_pending=1)
        self.ledger.accept(command())
        with self.assertRaisesRegex(Rejected, "queue is full"):
            self.ledger.accept(command(2, "reply"))

    def test_status_does_not_execute(self):
        self.ledger.accept(command()); self.ledger.accept(command(2, "status"))
        self.assertEqual(self.ledger.command_state(KEY, "request_002")["state"], "done")

    def test_cancel_unstarted_task(self):
        self.ledger.accept(command()); self.ledger.accept(command(2, "reply"))
        self.ledger.accept(command(3, "cancel"))
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "cancelled")
        self.assertEqual(self.ledger.command_state(KEY, "request_001")["state"], "cancelled")
        self.assertEqual(self.ledger.command_state(KEY, "request_002")["state"], "cancelled")
        self.assertIsNone(self.ledger.reserve_prompt(KEY))

    def test_cancel_active_waits_for_observed_terminal_response(self):
        self.start(); self.ledger.accept(command(2, "reply"))
        self.ledger.accept(command(3, "cancel"))
        control = self.ledger.reserve_cancel(KEY)
        self.assertEqual(control.session_id, "session-001")
        self.assertIsNone(self.ledger.reserve_cancel(KEY))
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "cancel_requested")
        self.ledger.finish_turn(KEY, "request_001", "cancelled")
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "cancelled")
        self.assertEqual(self.ledger.command_state(KEY, "request_003")["state"], "done")

    def test_cancel_racing_normal_completion_records_real_result(self):
        self.start(); self.ledger.accept(command(2, "cancel"))
        self.ledger.finish_turn(KEY, "request_001", "end_turn")
        self.assertEqual(self.ledger.command_state(KEY, "request_001")["stop_reason"], "end_turn")
        self.assertEqual(self.ledger.command_state(KEY, "request_001")["state"], "done")
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "cancelled")

    def test_cancel_while_initializing_cannot_dispatch_without_session(self):
        self.ledger.accept(command()); self.ledger.reserve_prompt(KEY)
        self.ledger.accept(command(2, "cancel"))
        self.assertIsNone(self.ledger.reserve_cancel(KEY))
        self.assertIsNone(self.ledger.reserve_prompt(KEY))

    def test_cancellation_before_send_blocks_prompt(self):
        self.ledger.accept(command()); self.ledger.reserve_prompt(KEY)
        self.ledger.accept(command(2, "cancel"))
        self.ledger.bind_session(KEY, "request_001", "session-001")
        with self.assertRaises(Rejected):
            self.ledger.begin_prompt_send(KEY, "request_001")
        with self.assertRaises(Rejected):
            self.ledger.mark_prompt_sent(KEY, "request_001")
        self.assertIsNone(self.ledger.reserve_cancel(KEY))
        self.ledger.abort_prepared(KEY, "request_001")
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "cancelled")
        self.assertEqual(self.ledger.command_state(KEY, "request_001")["stop_reason"], "never_sent")

    def test_cancel_after_send_intent_waits_for_write_observation(self):
        self.ledger.accept(command()); self.ledger.reserve_prompt(KEY)
        self.ledger.bind_session(KEY, "request_001", "session-001")
        self.ledger.begin_prompt_send(KEY, "request_001")
        self.ledger.accept(command(2, "cancel"))
        self.assertIsNone(self.ledger.reserve_cancel(KEY))
        with self.assertRaises(Rejected):
            self.ledger.abort_prepared(KEY, "request_001")
        self.ledger.mark_prompt_sent(KEY, "request_001")
        self.assertIsNotNone(self.ledger.reserve_cancel(KEY))

    def test_idle_session_recovery_requires_live_restore(self):
        self.start(); self.ledger.finish_turn(KEY, "request_001", "end_turn")
        self.ledger.accept(command(2, "reply"))
        self.ledger.recover_inflight()
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "restoring")
        self.assertIsNone(self.ledger.reserve_prompt(KEY))
        with self.assertRaises(Rejected):
            self.ledger.confirm_restored(KEY, "other-session")
        self.ledger.confirm_restored(KEY, "session-001")
        self.assertEqual(self.ledger.reserve_prompt(KEY).request_id, "request_002")

    def test_cancelled_task_rejects_new_prompts(self):
        self.ledger.accept(command()); self.ledger.accept(command(2, "cancel"))
        with self.assertRaises(Rejected):
            self.ledger.accept(command(3, "reply"))

    def test_crash_after_reservation_never_replays(self):
        self.ledger.accept(command()); self.ledger.reserve_prompt(KEY)
        self.ledger.close(); self.ledger = Ledger(self.path)
        self.assertEqual(self.ledger.recover_inflight(), 1)
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")
        self.assertIsNone(self.ledger.reserve_prompt(KEY))
        self.assertEqual(self.ledger.recover_inflight(), 0)

    def test_crash_after_prompt_sent_never_replays(self):
        self.start(); self.ledger.accept(command(2, "reply"))
        self.assertEqual(self.ledger.recover_inflight(), 1)
        self.assertEqual(self.ledger.command_state(KEY, "request_001")["state"], "uncertain")
        self.assertIsNone(self.ledger.reserve_prompt(KEY))
        with self.assertRaises(Rejected):
            self.ledger.accept(command(3, "cancel"))
        self.ledger.accept(command(4, "status"))

    def test_crash_does_not_freeze_never_reserved_queue(self):
        self.ledger.accept(command())
        self.assertEqual(self.ledger.recover_inflight(), 0)
        self.assertIsNotNone(self.ledger.reserve_prompt(KEY))

    def test_failed_transaction_leaves_no_receipt(self):
        with self.assertRaises(Rejected):
            self.ledger.accept(command(2, "reply"))
        self.assertEqual(self.ledger.db.execute("SELECT COUNT(*) FROM receipts").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
