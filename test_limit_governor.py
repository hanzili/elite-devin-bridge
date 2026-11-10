#!/usr/bin/env python3
"""Limit governor tests: error classification, reset parsing,
transcript-tail verdicts, registry persistence, and the bridge-level
park -> cooldown -> resume loop against fake_acp."""
import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import limit_governor as lg
import session_bridge as sb
import test_session_bridge as tsb

RATE_MSG = ("Reached free model rate limit. Upgrade to Max for higher "
            "limits, or switch to a different model. Your limit will "
            "reset in 14 hours 52 minutes (at Nov 8 04:01 UTC).")


class ClassifyTest(unittest.TestCase):
    def test_rate_limit_message(self):
        info = lg.classify_error(Exception(RATE_MSG))
        self.assertEqual(info["kind"], "rate_limited")
        self.assertIsNotNone(info["reset_at"])

    def test_error_kind_and_retryable(self):
        err = sb.AcpError({"code": -32010,
                           "message": "Connection error",
                           "data": {"cognition.ai/errorKind":
                                    "unavailable",
                                    "cognition.ai/retryable": True}})
        info = lg.classify_error(err)
        self.assertEqual(info["kind"], "retryable")

    def test_send_message_continue_is_retryable(self):
        info = lg.classify_error(
            Exception("Connection error, send a message to continue "
                      "retrying"))
        self.assertEqual(info["kind"], "retryable")

    def test_plain_error_not_parkable(self):
        self.assertIsNone(lg.classify_error(Exception("syntax error")))
        self.assertIsNone(lg.classify_error(Exception("")))


class ParseResetTest(unittest.TestCase):
    def test_duration_form(self):
        before = time.time()
        t = lg.parse_reset("Your limit will reset in 14 hours "
                           "52 minutes (at Nov 8 04:01 UTC).")
        self.assertAlmostEqual(t, before + 14 * 3600 + 52 * 60,
                               delta=5)

    def test_seconds_only(self):
        t = lg.parse_reset("reset in 36 seconds")
        self.assertAlmostEqual(t, time.time() + 36, delta=5)

    def test_at_date_form(self):
        # A date without the duration part still parses (year rolls
        # forward when the date already passed this year).
        t = lg.parse_reset("reset (at Dec 31 23:59 UTC)")
        self.assertIsNotNone(t)

    def test_garbage(self):
        self.assertIsNone(lg.parse_reset("no reset here"))
        self.assertIsNone(lg.parse_reset("reset in "))


class TailVerdictTest(unittest.TestCase):
    def test_limited(self):
        nodes = [
            {"role": "assistant", "content": ""},
            {"role": "tool",
             "content": "Reached free model rate limit. Please switch "
                        "to a different model."},
        ]
        self.assertEqual(lg.tail_verdict(nodes), "limited")

    def test_quoted_limit_is_not_victim(self):
        # exec output prefixing the error text must not trip it.
        nodes = [
            {"role": "tool",
             "content": "Output from command in shell x1: AcpError: "
                        "Reached free model rate limit."},
            {"role": "assistant", "content": "done"},
        ]
        self.assertEqual(lg.tail_verdict(nodes), "midturn")

    def test_midturn(self):
        nodes = [{"role": "tool",
                  "content": "Output from command in shell a: ok"}]
        self.assertEqual(lg.tail_verdict(nodes), "midturn")

    def test_idle(self):
        nodes = [{"role": "assistant", "content": "完成了"}]
        self.assertEqual(lg.tail_verdict(nodes), "idle")


class GovernorTest(unittest.TestCase):
    def test_persist_roundtrip_and_due(self):
        d = tempfile.mkdtemp()
        g = lg.Governor(d)
        g.park("t1", {"kind": "bridge", "text": "x",
                      "session_id": "s1", "resume_at": time.time() - 1})
        g2 = lg.Governor(d)
        self.assertIn("t1", g2.entries)
        due = g2.due()
        self.assertEqual([k for k, _ in due], ["t1"])
        popped = g2.pop("t1", "dispatch")
        self.assertEqual(popped["session_id"], "s1")
        self.assertIsNone(lg.Governor(d).get("t1"))

    def test_attempt_counter(self):
        g = lg.Governor(tempfile.mkdtemp())
        g.park("t", {"kind": "bridge", "resume_at": 0})
        e = g.park("t", {"kind": "bridge", "resume_at": 0})
        self.assertEqual(e["attempt"], 2)
        self.assertGreater(e["first_hit"], 0)

    def test_event_log(self):
        d = tempfile.mkdtemp()
        g = lg.Governor(d)
        g.park("t", {"kind": "bridge", "resume_at": 0,
                     "session_id": "s"})
        g.pop("t", "x")
        evs = lg.Governor(d).events_tail(10)
        names = [e["event"] for e in evs]
        self.assertIn("parked", names)
        self.assertIn("unparked", names)


class BridgeLimitTest(unittest.TestCase):
    def setUp(self):
        self._sweep = sb.LIMIT_SWEEP_SEC
        self._gap = sb.LIMIT_RESUME_GAP
        self._den = sb.DRAFT_ENABLED
        sb.LIMIT_SWEEP_SEC = 0.2
        sb.LIMIT_RESUME_GAP = 0.2
        sb.DRAFT_ENABLED = True
        self._pctx_put = os.environ.get("PCTX_MESSAGES_PUT")
        os.environ["PCTX_MESSAGES_PUT"] = ""

    def tearDown(self):
        sb.LIMIT_SWEEP_SEC = self._sweep
        sb.LIMIT_RESUME_GAP = self._gap
        sb.DRAFT_ENABLED = self._den
        if self._pctx_put is None:
            os.environ.pop("PCTX_MESSAGES_PUT", None)
        else:
            os.environ["PCTX_MESSAGES_PUT"] = self._pctx_put

    def _sweeper(self, b):
        t = threading.Thread(target=b.limit_sweeper, daemon=True)
        t.start()
        return t

    def test_rate_limited_turn_parks_and_resumes(self):
        b, web = tsb.make_bridge(mode="ratelimit")
        self._sweeper(b)
        b.handle_event(tsb.root_ev("1700.1"))
        # parked: ⏸️ post + registry entry + rate_limited state
        self.assertTrue(web.wait_for("限额", 10), web.texts())
        e = b.governor.get("1700.1")
        self.assertIsNotNone(e)
        self.assertEqual(e["error_kind"], "rate_limited")
        st = sb.read_state(b._workdir("1700.1"))
        self.assertEqual(st["state"], "rate_limited")
        # after the 2s fake reset window the sweeper re-prompts on the
        # same live session and the turn completes
        self.assertTrue(web.wait_for("限额冷却结束", 30), web.texts())
        self.assertTrue(web.wait_for("任务结果", 30), web.texts())
        self.assertIsNone(b.governor.get("1700.1"))
        b.sessions["1700.1"].terminate()

    def test_retryable_error_parks(self):
        b, web = tsb.make_bridge(mode="unavailable")
        b.handle_event(tsb.root_ev("1700.2"))
        self.assertTrue(web.wait_for("限额", 10), web.texts())
        e = b.governor.get("1700.2")
        self.assertIsNotNone(e)
        self.assertEqual(e["error_kind"], "retryable")
        b.sessions["1700.2"].terminate()

    def test_user_reply_drops_parked(self):
        b, web = tsb.make_bridge(mode="ratelimit")
        b.handle_event(tsb.root_ev("1700.3"))
        self.assertTrue(web.wait_for("限额", 10), web.texts())
        b.handle_event(tsb.reply_ev("1700.3", "1700.4", "再试一次",
                                    user="U1"))
        end = time.monotonic() + 10
        while time.monotonic() < end and b.governor.get("1700.3"):
            time.sleep(0.05)
        self.assertIsNone(b.governor.get("1700.3"))
        self.assertTrue(web.wait_for("任务结果", 15), web.texts())
        b.sessions["1700.3"].terminate()

    def test_stop_drops_parked(self):
        b, web = tsb.make_bridge(mode="ratelimit")
        b.handle_event(tsb.root_ev("1700.5"))
        self.assertTrue(web.wait_for("限额", 10), web.texts())
        b.handle_event(tsb.reply_ev("1700.5", "1700.6", "停止",
                                    user="U1"))
        self.assertIsNone(b.governor.get("1700.5"))

    def test_adopt_limited_picks_up_old_failures(self):
        d = tempfile.mkdtemp()
        wd = os.path.join(d, "1800_1")
        os.makedirs(wd)
        sb.write_state(wd, "failed", "",
                       "AcpError: " + RATE_MSG,
                       extra={"session_id": "old-sid"})
        b, web = tsb.make_bridge(tasks_dir=d)
        adopted = b.adopt_limited()
        self.assertEqual(adopted, ["1800.1"])
        e = b.governor.get("1800.1")
        self.assertEqual(e["session_id"], "old-sid")

    def test_adopt_limited_ignores_plain_failures(self):
        d = tempfile.mkdtemp()
        wd = os.path.join(d, "1801_1")
        os.makedirs(wd)
        sb.write_state(wd, "failed", "", "ValueError: bad input",
                       extra={"session_id": "s2"})
        b, web = tsb.make_bridge(tasks_dir=d)
        self.assertEqual(b.adopt_limited(), [])
        self.assertIsNone(b.governor.get("1801.1"))


if __name__ == "__main__":
    unittest.main()
