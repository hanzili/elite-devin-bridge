#!/usr/bin/env python3
"""Offline tests for !model / !mode bridge commands. These must hit
the real ACP config methods (session/set_config_option,
session/set_mode) — devin acp does not advertise slash commands, so
forwarded text never switches anything."""
import sys
import time
import unittest

sys.path.insert(0, __file__.rsplit("/", 1)[0] or ".")
from test_session_bridge import (FakeWeb, make_bridge, root_ev,
                                 reply_ev, wait_running)


class ModelCmdTest(unittest.TestCase):
    def _bridge(self):
        b, web = make_bridge(timeout=15)
        self.addCleanup(lambda: [s.terminate()
                                 for s in list(b.sessions.values())])
        return b, web

    def _idle_session(self, b, web, ts="m.1"):
        b.handle_event(root_ev(ts), "EvR-" + ts)
        self.assertTrue(web.wait_for("任务结果", 20))
        end = time.monotonic() + 5
        while time.monotonic() < end:
            s = b.sessions.get(ts)
            if s and not s.running:
                return s
            time.sleep(0.05)
        self.fail("session never became idle")

    def test_bang_model_switches_via_config_option(self):
        b, web = self._bridge()
        s = self._idle_session(b, web)
        b.handle_event(reply_ev("m.1", "m.2", "!model swe-2-flash"),
                       "EvM2")
        self.assertTrue(web.wait_for("已切到 `swe-2-flash`", 15))
        self.assertEqual(s.model_name, "swe-2-flash")

    def test_bang_model_no_arg_reports_current(self):
        b, web = self._bridge()
        self._idle_session(b, web)
        b.handle_event(reply_ev("m.1", "m.3", "!model"), "EvM3")
        self.assertTrue(web.wait_for("当前 model：`SWE-2`", 15))
        self.assertTrue(web.wait_for("id `swe-2-high`", 15))

    def test_bang_model_bad_value_relayed(self):
        b, web = self._bridge()
        self._idle_session(b, web)
        b.handle_event(reply_ev("m.1", "m.4", "!model bad-value"),
                       "EvM4")
        self.assertTrue(web.wait_for("被拒绝", 15))
        self.assertTrue(web.wait_for("unknown option value", 15))

    def test_bang_model_while_running_refused(self):
        b, web = self._bridge()
        b.handle_event(root_ev("r.1", "<@BTEST> slow"), "EvR1")
        s = wait_running(b, "r.1")
        self.assertIsNotNone(s)
        b.handle_event(reply_ev("r.1", "r.2", "!model swe-2-high"),
                       "EvR2")
        self.assertTrue(web.wait_for("会话还在执行", 15))
        b.handle_event(reply_ev("r.1", "r.3", "停止"), "EvR3")
        self.assertTrue(web.wait_for("（已停止）", 20))

    def test_bang_mode_switches_via_set_mode(self):
        b, web = self._bridge()
        s = self._idle_session(b, web)
        b.handle_event(reply_ev("m.1", "m.5", "!mode plan"), "EvM5")
        self.assertTrue(web.wait_for("已切到 `plan`", 15))
        self.assertEqual(s.mode_id, "plan")

    def test_bang_model_no_session(self):
        b, web = self._bridge()
        b.handle_event(reply_ev("z.1", "z.2", "!model swe-2-high"),
                       "EvZ2")
        self.assertTrue(web.wait_for("本线程还没有会话", 15))


if __name__ == "__main__":
    unittest.main()
