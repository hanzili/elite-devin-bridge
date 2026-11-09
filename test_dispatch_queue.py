#!/usr/bin/env python3
"""Offline tests for the auto-dispatch concurrency cap + FIFO queue
in session_bridge. Bot/app-posted root mentions are capped at
MAX_AUTO concurrent sessions; human mentions always start."""
import sys
import tempfile
import time
import unittest

sys.path.insert(0, __file__.rsplit("/", 1)[0] or ".")
import session_bridge as sb
from test_session_bridge import (FakeWeb, make_bridge, root_ev,
                                 reply_ev, wait_running)

CHANNEL = "C1"


def bot_root_ev(ts, text="<@BTEST> task"):
    ev = root_ev(ts, text)
    ev["bot_id"] = "BOTHER"
    ev["subtype"] = "bot_message"
    return ev


def app_root_ev(ts, text="<@BTEST> task"):
    ev = root_ev(ts, text)
    ev["app_id"] = "AOTHER"
    return ev


def wait_session(b, ts, timeout=10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if ts in b.sessions:
            return b.sessions[ts]
        time.sleep(0.05)
    return None


class DispatchQueueTest(unittest.TestCase):
    def test_bot_dispatch_queues_at_cap(self):
        b, web = make_bridge()
        self.addCleanup(lambda: [s.terminate()
                                 for s in list(b.sessions.values())])
        b.handle_event(bot_root_ev("a.1", "<@BTEST> slow"), "EvA1")
        self.assertIsNotNone(wait_running(b, "a.1"))
        b.handle_event(bot_root_ev("a.2"), "EvA2")
        b.handle_event(bot_root_ev("a.3"), "EvA3")
        time.sleep(0.3)
        self.assertNotIn("a.2", b.sessions)
        self.assertNotIn("a.3", b.sessions)
        self.assertEqual([q[0] for q in b.dispatch_queue], ["a.2", "a.3"])
        st = sb.read_state(b._workdir("a.2"))
        self.assertEqual(st["state"], "queued")
        self.assertTrue(web.wait_for("排队中"))

    def test_app_id_dispatch_counts_as_auto(self):
        b, web = make_bridge()
        self.addCleanup(lambda: [s.terminate()
                                 for s in list(b.sessions.values())])
        b.handle_event(bot_root_ev("a.1", "<@BTEST> slow"), "EvA1")
        self.assertIsNotNone(wait_running(b, "a.1"))
        b.handle_event(app_root_ev("a.9"), "EvA9")
        time.sleep(0.3)
        self.assertNotIn("a.9", b.sessions)
        self.assertEqual([q[0] for q in b.dispatch_queue], ["a.9"])

    def test_human_mention_bypasses_cap(self):
        b, web = make_bridge()
        self.addCleanup(lambda: [s.terminate()
                                 for s in list(b.sessions.values())])
        b.handle_event(bot_root_ev("a.1", "<@BTEST> slow"), "EvA1")
        self.assertIsNotNone(wait_running(b, "a.1"))
        b.handle_event(root_ev("h.1"), "EvH1")
        self.assertIsNotNone(wait_session(b, "h.1"))
        self.assertEqual(len(b.dispatch_queue), 0)
        self.assertFalse(getattr(b.sessions["h.1"], "auto", False))
        self.assertTrue(getattr(b.sessions["a.1"], "auto", False))

    def test_stop_cancels_queued_task(self):
        b, web = make_bridge()
        self.addCleanup(lambda: [s.terminate()
                                 for s in list(b.sessions.values())])
        b.handle_event(bot_root_ev("a.1", "<@BTEST> slow"), "EvA1")
        self.assertIsNotNone(wait_running(b, "a.1"))
        b.handle_event(bot_root_ev("a.2"), "EvA2")
        time.sleep(0.3)
        b.handle_event(reply_ev("a.2", "a.2s", "停止"), "EvS")
        self.assertEqual(len(b.dispatch_queue), 0)
        st = sb.read_state(b._workdir("a.2"))
        self.assertEqual(st["state"], "stopped")
        self.assertTrue(web.wait_for("已取消排队"))

    def test_queue_pumps_after_running_ends(self):
        b, web = make_bridge()
        self.addCleanup(lambda: [s.terminate()
                                 for s in list(b.sessions.values())])
        b.handle_event(bot_root_ev("a.1", "<@BTEST> slow"), "EvA1")
        self.assertIsNotNone(wait_running(b, "a.1"))
        b.handle_event(bot_root_ev("a.2"), "EvA2")
        time.sleep(0.3)
        b.handle_event(reply_ev("a.1", "a.1s", "停止"), "EvS1")
        # a.1 finishes cancelled -> finally reaps idle auto + pumps a.2
        self.assertTrue(web.wait_for("轮到本任务", timeout=15))
        self.assertIsNotNone(wait_session(b, "a.2"))
        self.assertNotIn("a.1", b.sessions)
        self.assertEqual(len(b.dispatch_queue), 0)

    def test_queued_reply_merges_text(self):
        b, web = make_bridge()
        self.addCleanup(lambda: [s.terminate()
                                 for s in list(b.sessions.values())])
        b.handle_event(bot_root_ev("a.1", "<@BTEST> slow"), "EvA1")
        self.assertIsNotNone(wait_running(b, "a.1"))
        b.handle_event(bot_root_ev("a.2", "<@BTEST> base"), "EvA2")
        time.sleep(0.3)
        b.handle_event(reply_ev("a.2", "a.2r", "补充说明"), "EvR")
        self.assertEqual(b.dispatch_queue[0][1], "base\n补充说明")
        self.assertTrue(web.wait_for("已并入"))


if __name__ == "__main__":
    unittest.main()
