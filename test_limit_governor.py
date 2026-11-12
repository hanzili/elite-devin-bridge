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

# Pool-routing is off for these tests — they assert the legacy
# single-account behavior (early-resume on acct switch, live-session
# resume). Pool tests live in test_acct_pool.py.
sb.ACCT_POOL_ENABLED = False

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


class SuspectDueTest(unittest.TestCase):
    def test_due_never_dispatches_suspects(self):
        g = lg.Governor(tempfile.mkdtemp())
        g.park("ext:s", {"kind": "external", "session_id": "s",
                         "suspect": True,
                         "resume_at": time.time() - 1})
        self.assertEqual(g.due(), [])


class ExternalScanTest(unittest.TestCase):
    """_ext_scan gating: unlocked victims park for auto-resume,
    live-locked ones degrade to watch-only 'suspects'."""

    def setUp(self):
        self.b, self.web = tsb.make_bridge()
        self._list = sb.list_devin_sessions
        self._sess = sb.devin_session
        self._lock = sb.devin_lock_pid
        self._steal = sb.LIMIT_EXT_STEAL
        sb.LIMIT_EXT_STEAL = False
        self.rows = []
        sb.list_devin_sessions = lambda limit=15: self.rows
        sb.devin_session = lambda sid: next(
            (r for r in self.rows if r["id"] == sid), None)
        sb.devin_lock_pid = lambda sid: next(
            (r["lock_pid"] for r in self.rows if r["id"] == sid),
            None)

    def tearDown(self):
        sb.list_devin_sessions = self._list
        sb.devin_session = self._sess
        sb.devin_lock_pid = self._lock
        sb.LIMIT_EXT_STEAL = self._steal

    def _row(self, sid, age=60, lock=None):
        return {"id": sid, "workdir": "/tmp/wd-" + sid,
                "title": "task " + sid,
                "activity": time.time() - age, "lock_pid": lock}

    def _verdict(self, mapping, fresh=False):
        node_ts = time.time() if fresh else time.time() - 3600
        self.b._ext_tail_verdict = lambda sid, tail=6: \
            mapping.get(sid, ("idle", None, None))[:2] + (node_ts,) \
            if sid in mapping else ("idle", None, None)

    def test_unlocked_limited_parks(self):
        self.rows = [self._row("s-ext")]
        self._verdict({"s-ext": ("limited", time.time() + 30)})
        self.b._ext_scan()
        e = self.b.governor.get("ext:s-ext")
        self.assertIsNotNone(e)
        self.assertFalse(e.get("suspect"))
        self.assertGreater(e["resume_at"], time.time())

    def test_locked_limited_is_suspect(self):
        self.rows = [self._row("s-lock", lock=4321)]
        self._verdict({"s-lock": ("limited", time.time() + 30)})
        self.b._ext_scan()
        e = self.b.governor.get("ext:s-lock")
        self.assertIsNotNone(e)
        self.assertTrue(e.get("suspect"))
        self.assertEqual(self.b.governor.due(), [])

    def test_suspect_promotes_when_lock_dies(self):
        self.rows = [self._row("s-lock", lock=4321)]
        self._verdict({"s-lock": ("limited", None)})
        self.b._ext_scan()
        self.rows[0]["lock_pid"] = None
        self.b._ext_scan()
        e = self.b.governor.get("ext:s-lock")
        self.assertIsNotNone(e)
        self.assertFalse(e.get("suspect"))
        self.assertLess(e["resume_at"], time.time() + 30)

    def test_unlocked_midturn_parks_without_window(self):
        # A mid-turn tail on a session nobody owns is an orphan —
        # rescuable even without an open limit window.
        self.rows = [self._row("s-dead")]
        self._verdict({"s-dead": ("midturn", None)})
        self.b._ext_scan()
        self.assertIsNotNone(self.b.governor.get("ext:s-dead"))

    def test_locked_stale_midturn_suspects_without_window(self):
        # Ambiguous: dead turn or a >10min silent tool call — watch it,
        # never auto-steal unless a limit window is actually open.
        self.rows = [self._row("s-live", lock=777)]
        self._verdict({"s-live": ("midturn", None)})
        self.b._ext_scan()
        e = self.b.governor.get("ext:s-live")
        self.assertIsNotNone(e)
        self.assertTrue(e.get("suspect"))

    def test_steal_midturn_only_inside_window(self):
        sb.LIMIT_EXT_STEAL = True
        self.rows = [self._row("s-live", lock=777)]
        self._verdict({"s-live": ("midturn", None)})
        self.b._ext_scan()
        self.assertTrue(self.b.governor.get("ext:s-live")["suspect"])
        self.b.governor.pop("ext:s-live", "test")
        self.b.governor.last_limit_at = time.time()
        self.b._ext_scan()
        e = self.b.governor.get("ext:s-live")
        self.assertIsNotNone(e)
        self.assertFalse(e.get("suspect"))

    def test_suspect_superseded_when_owner_resumes(self):
        self.rows = [self._row("s-sup", lock=5)]
        self._verdict({"s-sup": ("limited", None)})
        self.b._ext_scan()
        self.rows[0]["activity"] = time.time() + 1
        self.b._ext_scan()
        self.assertIsNone(self.b.governor.get("ext:s-sup"))

    def test_steal_mode_parks_locked_directly(self):
        sb.LIMIT_EXT_STEAL = True
        self.rows = [self._row("s-st", lock=42)]
        self._verdict({"s-st": ("limited", time.time() + 30)})
        self.b._ext_scan()
        e = self.b.governor.get("ext:s-st")
        self.assertIsNotNone(e)
        self.assertFalse(e.get("suspect"))

    def test_resume_locked_turns_suspect(self):
        wd = tempfile.mkdtemp()
        e = {"kind": "external", "session_id": "s-l",
             "workdir": wd, "attempt": 1}
        self.rows = [self._row("s-l", lock=999)]
        self.b._ext_resume(dict(e))
        e2 = self.b.governor.get("ext:s-l")
        self.assertIsNotNone(e2)
        self.assertTrue(e2.get("suspect"))

    def test_fresh_tails_are_live_and_skipped(self):
        # Nodes still being written = turn in flight, even if a
        # limit error was absorbed mid-tail earlier.
        self.rows = [self._row("s-busy"), self._row("s-busy2", lock=9)]
        self._verdict({"s-busy": ("limited", None),
                       "s-busy2": ("limited", None)}, fresh=True)
        self.b.governor.last_limit_at = time.time()
        self.b._ext_scan()
        self.assertIsNone(self.b.governor.get("ext:s-busy2"))
        # unlocked + fresh limit node = the turn just died, park it
        e = self.b.governor.get("ext:s-busy")
        self.assertIsNotNone(e)
        self.assertFalse(e.get("suspect"))

    def test_locked_fresh_midturn_is_live(self):
        self.rows = [self._row("s-run", lock=5)]
        self._verdict({"s-run": ("midturn", None)}, fresh=True)
        self.b.governor.last_limit_at = time.time()
        self.b._ext_scan()
        self.assertIsNone(self.b.governor.get("ext:s-run"))

    def test_unlocked_fresh_midturn_is_live(self):
        self.rows = [self._row("s-nl")]
        self._verdict({"s-nl": ("midturn", None)}, fresh=True)
        self.b._ext_scan()
        self.assertIsNone(self.b.governor.get("ext:s-nl"))

    def test_acct_switch_pulls_resumes_forward(self):
        self.b.governor.park("t1", {
            "kind": "bridge", "text": "x", "session_id": "s",
            "acct": "aaa", "resume_at": time.time() + 3600})
        self.b.governor.park("ext:s2", {
            "kind": "external", "session_id": "s2", "suspect": True,
            "acct": "aaa", "resume_at": time.time() + 3600})
        self.b._acct = "aaa"
        orig = sb._acct_fingerprint
        sb._acct_fingerprint = lambda: "bbb"
        try:
            self.b._acct_drift_check()
        finally:
            sb._acct_fingerprint = orig
        e = self.b.governor.get("t1")
        self.assertEqual(e["acct"], "bbb")
        self.assertLess(e["resume_at"], time.time() + 60)
        # suspects stay watch-only
        self.assertTrue(self.b.governor.get("ext:s2")["suspect"])

    def test_bridge_owned_sessions_skipped(self):
        self.rows = [self._row("s-own")]
        self._verdict({"s-own": ("limited", None)})
        s = type("S", (), {"session_id": "s-own"})()
        self.b.sessions["1111.1"] = s
        self.b._ext_scan()
        self.assertIsNone(self.b.governor.get("ext:s-own"))


if __name__ == "__main__":
    unittest.main()
