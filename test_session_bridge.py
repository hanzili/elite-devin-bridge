#!/usr/bin/env python3
"""Offline tests for session_bridge using the fake `devin acp`
subprocess. No Slack, no real devin, no credentials."""
import contextlib
import io
import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from urllib.error import URLError

# The tests assert zh UI strings; pin the UI language before import.
os.environ.setdefault("BRIDGE_LANG", "zh")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import session_bridge as sb

# Account-pool routing is off for these tests — they exercise the
# legacy single-credential path; pool tests live in test_acct_pool.py.
sb.ACCT_POOL_ENABLED = False

FAKE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    "fake_acp.py")
CHANNEL = "C1"
BOT = "BTEST"


class FakeWeb:
    def __init__(self):
        self.posts = []
        self.updates = []
        self.blocks = []
        self.dm_posts = []
        self.dm_updates = []
        self.dm_update_fail = False
        self.lock = threading.Lock()

    def post(self, ts, text, blocks=None):
        with self.lock:
            self.posts.append((ts, text))
            if blocks is not None:
                self.blocks.append((ts, blocks))
            return f"m{len(self.posts)}"

    def update(self, msg_ts, text, blocks=None):
        with self.lock:
            self.updates.append((msg_ts, text))
            self.posts.append((msg_ts, text))
            if blocks is not None:
                self.blocks.append((msg_ts, blocks))

    def dm_post(self, uid, text, blocks=None):
        with self.lock:
            self.dm_posts.append((uid, text, blocks))
            n = len(self.dm_posts)
            return {"channel": f"D{uid}", "ts": f"dm{n}"}

    def dm_update(self, chan, ts, text, blocks=None):
        with self.lock:
            if self.dm_update_fail:
                raise OSError("dm update failed")
            self.dm_updates.append((chan, ts, text))

    def texts(self, ts=None):
        with self.lock:
            return [t for t_, t in self.posts if ts is None or t_ == ts]

    def wait_for(self, needle, timeout=15):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if any(needle in t for t in self.texts()):
                return True
            time.sleep(0.05)
        return False


def root_ev(ts, text="<@BTEST> go"):
    return {"type": "message", "channel": CHANNEL, "ts": ts, "text": text}


def reply_ev(root, ts, text, user=None):
    ev = {"type": "message", "channel": CHANNEL, "ts": ts,
          "thread_ts": root, "text": text}
    if user:
        ev["user"] = user
    return ev


def make_bridge(mode="normal", timeout=5, tasks_dir=None, **kwargs):
    web = FakeWeb()
    b = sb.SessionBridge(
        web.post, tasks_dir=tasks_dir or tempfile.mkdtemp(),
        acp_cmd=[sys.executable, FAKE, mode],
        timeout=timeout, idle_sec=3600, own_bot_id=BOT,
        channel=CHANNEL, **kwargs)
    return b, web


def wait_running(b, ts, timeout=10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        s = b.sessions.get(ts)
        if s and s.running:
            return s
        time.sleep(0.05)
    return None


class BridgeTest(unittest.TestCase):
    def setUp(self):
        self._grace = sb.CANCEL_GRACE
        sb.CANCEL_GRACE = 1.5
        self._gap = sb.PROGRESS_MIN_GAP
        sb.PROGRESS_MIN_GAP = 0
        self._perm_to = sb.PERM_TIMEOUT
        sb.PERM_TIMEOUT = 2
        self._max_post = sb.MAX_POST
        sb.MAX_POST = 3400
        self._gpoll = sb.GOAL_POLL_SEC
        sb.GOAL_POLL_SEC = 0.05
        self._gwatch = sb.GOAL_WATCH_SEC
        sb.GOAL_WATCH_SEC = 0.1
        self._dgap = sb.DRAFT_MIN_GAP
        sb.DRAFT_MIN_GAP = 0
        # drafts are a testable feature regardless of deployment env
        self._den = sb.DRAFT_ENABLED
        sb.DRAFT_ENABLED = True
        # test prompts must never write into the real personal-context
        # message queue via the session-bound message tap
        self._pctx_put = os.environ.get("PCTX_MESSAGES_PUT")
        os.environ["PCTX_MESSAGES_PUT"] = ""
        # permission tests drive the manual allow/deny path
        self._aa = sb.AUTO_APPROVE
        sb.AUTO_APPROVE = False

    def tearDown(self):
        sb.CANCEL_GRACE = self._grace
        sb.PROGRESS_MIN_GAP = self._gap
        sb.PERM_TIMEOUT = self._perm_to
        sb.MAX_POST = self._max_post
        sb.GOAL_POLL_SEC = self._gpoll
        sb.GOAL_WATCH_SEC = self._gwatch
        sb.DRAFT_MIN_GAP = self._dgap
        sb.DRAFT_ENABLED = self._den
        if self._pctx_put is None:
            os.environ.pop("PCTX_MESSAGES_PUT", None)
        else:
            os.environ["PCTX_MESSAGES_PUT"] = self._pctx_put
        sb.AUTO_APPROVE = self._aa

    def tearDownClass():
        pass

    def test_missing_binary_fails_cleanly(self):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=tempfile.mkdtemp(),
            acp_cmd=["/nonexistent/devin-xyz"], timeout=3,
            channel=CHANNEL)
        b.handle_event(root_ev("m.1"), "EvM1")
        self.assertTrue(web.wait_for("启动 devin 失败"))
        self.assertTrue(web.wait_for("FileNotFoundError"))
        self.assertNotIn("m.1", b.sessions)
        st = sb.read_state(b._workdir("m.1"))
        self.assertEqual(st["state"], "failed")

    def test_echo_flow_status_and_idle_state(self):
        b, web = make_bridge()
        b.handle_event(root_ev("e.1"), "EvE1")
        self.assertTrue(web.wait_for("已接单"))
        self.assertTrue(web.wait_for("任务结果：一切正常。"))
        b.handle_event(reply_ev("e.1", "e.2", "状态"), "EvE2")
        self.assertTrue(web.wait_for("等待中（空闲）"))
        st = sb.read_state(b._workdir("e.1"))
        self.assertEqual(st["state"], "idle")
        b.sessions["e.1"].terminate()

    def test_timeout_fires_on_silence(self):
        b, web = make_bridge(mode="silent", timeout=2)
        b.handle_event(root_ev("t.1"), "EvT1")
        self.assertTrue(web.wait_for("无活动超过", 30))
        st = sb.read_state(b._workdir("t.1"))
        self.assertEqual(st["state"], "timeout")
        self.assertNotIn("t.1", b.sessions)
        b.handle_event(reply_ev("t.1", "t.2", "状态"), "EvT2")
        self.assertTrue(web.wait_for("超时终止"))

    def test_start_timeout_when_acp_wedged(self):
        b, web = make_bridge(mode="hanginit", timeout=2)
        b.handle_event(root_ev("h.1"), "EvH1")
        self.assertTrue(web.wait_for("启动 devin 失败", 30))
        self.assertNotIn("h.1", b.sessions)

    def test_stop_graceful_cancel(self):
        b, web = make_bridge(timeout=60)
        b.handle_event(root_ev("s.1", "<@BTEST> slow"), "EvS1")
        s = wait_running(b, "s.1")
        self.assertIsNotNone(s, "prompt never started")
        b.handle_event(reply_ev("s.1", "s.2", "停止"), "EvS2")
        self.assertTrue(web.wait_for("正在停止"))
        self.assertTrue(web.wait_for("（已停止）", 15))
        self.assertIn("s.1", b.sessions)
        b.handle_event(reply_ev("s.1", "s.3", "状态"), "EvS3")
        self.assertTrue(web.wait_for("等待中（空闲）"))
        s.terminate()

    def test_stop_forced_kills_own_group_and_child(self):
        b, web = make_bridge(mode="spawn", timeout=60)
        b.handle_event(root_ev("k.1"), "EvK1")
        s = wait_running(b, "k.1")
        self.assertIsNotNone(s, "prompt never started")
        pgid = s.proc.pid
        b.handle_event(reply_ev("k.1", "k.2", "停止"), "EvK2")
        self.assertTrue(web.wait_for("已停止", 30))
        self.assertNotIn("k.1", b.sessions)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.killpg(pgid, 0)
            except OSError:
                break
            time.sleep(0.1)
        else:
            self.fail("process group (incl. spawned child) still alive")
        st = sb.read_state(b._workdir("k.1"))
        self.assertEqual(st["state"], "stopped")

    def test_stop_with_no_session_reports_state(self):
        b, web = make_bridge()
        b.handle_event(reply_ev("n.1", "n.2", "停止"), "EvN1")
        self.assertTrue(web.wait_for("没有运行中的会话"))

    def test_duplicate_event_id_and_ts_not_reexecuted(self):
        b, web = make_bridge()
        ev = root_ev("d.1")
        b.handle_event(ev, "EvD1")
        b.handle_event(ev, "EvD1")
        b.handle_event(ev, "EvD2")
        self.assertTrue(web.wait_for("任务结果", 15))
        self.assertEqual(
            len([t for t in web.texts() if "已接单" in t]), 1)
        self.assertEqual(len(b.sessions), 1)
        b.sessions["d.1"].terminate()

    def test_long_report_fully_delivered(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("l.1", "<@BTEST> long"), "EvL1")
        self.assertTrue(web.wait_for("第 119 段", 30))
        chunk_posts = [t for t in web.texts("l.1")
                       if t.startswith("(")]
        self.assertGreaterEqual(len(chunk_posts), 3)
        for t in chunk_posts:
            self.assertLessEqual(len(t), sb.MAX_POST + 8)
        joined = "\n".join(
            t.split("\n", 1)[1] if t.startswith("(") else t
            for t in chunk_posts)
        for i in (0, 60, 119):
            self.assertIn(f"第 {i} 段", joined)
        b.sessions["l.1"].terminate()

    def test_crash_mid_prompt_reported(self):
        b, web = make_bridge(mode="die")
        b.handle_event(root_ev("c.1"), "EvC1")
        self.assertTrue(web.wait_for("会话已结束", 15))
        self.assertNotIn("c.1", b.sessions)
        st = sb.read_state(b._workdir("c.1"))
        self.assertEqual(st["state"], "crashed")

    def test_crash_mid_prompt_auto_resumed(self):
        b, web = make_bridge(mode="dieonce")
        b.handle_event(root_ev("d.1"), "EvD1")
        self.assertTrue(web.wait_for("自动恢复", 15))
        self.assertTrue(web.wait_for("已恢复旧会话", 15))
        self.assertTrue(web.wait_for("任务结果：一切正常。", 15))
        st = sb.read_state(b._workdir("d.1"))
        self.assertEqual(st["state"], "idle")
        s = b.sessions.get("d.1")
        self.assertIsNotNone(s)
        s.terminate()

    def test_streaming_draft_live_updates(self):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=tempfile.mkdtemp(),
            acp_cmd=[sys.executable, FAKE, "normal"], timeout=5,
            idle_sec=3600, own_bot_id=BOT, channel=CHANNEL,
            update=web.update)
        b.handle_event(root_ev("s.1"), "EvS1")
        end = time.monotonic() + 15
        settled = False
        while time.monotonic() < end:
            if any(t == "任务结果：一切正常。" for _, t in web.updates):
                settled = True
                break
            time.sleep(0.05)
        self.assertTrue(settled)
        self.assertTrue(any(t.endswith(sb.DRAFT_CURSOR)
                            for _, t in web.posts))
        final = [t for _, t in web.posts if t == "任务结果：一切正常。"]
        self.assertEqual(len(final), 1)
        b.sessions["s.1"].terminate()

    def test_streaming_draft_segments_and_settles(self):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=tempfile.mkdtemp(),
            acp_cmd=[sys.executable, FAKE, "normal"], timeout=15,
            idle_sec=3600, own_bot_id=BOT, channel=CHANNEL,
            update=web.update)
        b.handle_event(root_ev("s.2", "<@BTEST> long"), "EvS2")
        end = time.monotonic() + 30
        settled = False
        while time.monotonic() < end:
            if any("第 119 段" in t and not t.endswith(sb.DRAFT_CURSOR)
                   for _, t in web.updates):
                settled = True
                break
            time.sleep(0.05)
        self.assertTrue(settled)
        s = b.sessions["s.2"]
        self.assertGreaterEqual(len(s.draft_msgs), 2)
        s.terminate()

    def test_interrupted_sweep_and_unknown_status(self):
        tasks = tempfile.mkdtemp()
        wd = os.path.join(tasks, "x_1")
        os.makedirs(wd)
        sb.write_state(wd, "running", "干活")
        self.assertEqual(len(sb.mark_interrupted(tasks)), 1)
        st = sb.read_state(wd)
        self.assertEqual(st["state"], "interrupted")

        b, web = make_bridge(tasks_dir=tasks)
        b.handle_event(reply_ev("x.1", "x.2", "状态"), "EvX2")
        self.assertTrue(web.wait_for("已中断"))
        b.handle_event(reply_ev("zz.9", "zz.10", "状态"), "EvZ")
        self.assertTrue(web.wait_for("未知"))
        b.handle_event(reply_ev("x.1", "x.3", "停止"), "EvX3")
        self.assertTrue(web.wait_for("没有运行中的会话"))

    def test_queued_replies_run_in_order_after_stop(self):
        b, web = make_bridge(timeout=60)
        b.handle_event(root_ev("q.1", "<@BTEST> slow"), "EvQ1")
        s = wait_running(b, "q.1")
        self.assertIsNotNone(s)
        b.handle_event(reply_ev("q.1", "q.2", "second"), "EvQ2")
        end = time.monotonic() + 5
        while s.queued < 1 and time.monotonic() < end:
            time.sleep(0.05)
        b.handle_event(reply_ev("q.1", "q.3", "停止"), "EvQ3")
        self.assertTrue(web.wait_for("（已停止）", 15))
        self.assertTrue(web.wait_for("任务结果：一切正常。", 15))
        s.terminate()

    def test_permission_waits_for_user_allow(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("p.1", "<@BTEST> perm"), "EvP1")
        self.assertTrue(web.wait_for("待审批", 30))
        b.handle_event(reply_ev("p.1", "p.1a", "允许", user="U1"), "EvP1A")
        self.assertTrue(web.wait_for("已批准"))
        self.assertTrue(web.wait_for("审批结果:selected:a1", 30))
        recs, status = sb.load_approvals(b._workdir("p.1"))
        self.assertEqual(status, "ok")
        self.assertEqual(len(recs), 1)
        r = recs[0]
        self.assertEqual(r["status"], "resolved")
        self.assertEqual(r["resolution"], "allow_once")
        self.assertFalse(r["needs_user"])
        self.assertEqual(r["resolver"], "user:U1")
        self.assertEqual(r["session_id"], "fake-session-1")
        self.assertIn("pytest", r["summary"])
        snap = sb.collect_approvals(b.tasks_dir)
        entry = [e for e in snap if e["thread_ts"] == "p.1"]
        self.assertEqual(len(entry), 1)
        self.assertTrue(entry[0]["covered"])
        self.assertEqual(entry[0]["resolved"], 1)
        self.assertEqual(entry[0]["pending"], [])
        b.sessions["p.1"].terminate()

    def test_permission_user_deny(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("p.6", "<@BTEST> perm"), "EvP6")
        self.assertTrue(web.wait_for("待审批", 30))
        b.handle_event(reply_ev("p.6", "p.6a", "拒绝", user="U2"), "EvP6D")
        self.assertTrue(web.wait_for("已回绝"))
        self.assertTrue(web.wait_for("审批结果:cancelled", 30))
        recs, _ = sb.load_approvals(b._workdir("p.6"))
        self.assertEqual(recs[0]["resolution"], "cancelled")
        self.assertEqual(recs[0]["resolver"], "user:U2")
        self.assertFalse(recs[0]["needs_user"])
        b.sessions["p.6"].terminate()

    def test_permission_timeout_auto_denied(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("p.7", "<@BTEST> perm"), "EvP7")
        self.assertTrue(web.wait_for("待审批", 30))
        self.assertTrue(web.wait_for("审批超时", 30))
        self.assertTrue(web.wait_for("审批结果:cancelled", 30))
        recs, _ = sb.load_approvals(b._workdir("p.7"))
        self.assertEqual(recs[0]["resolver"], "timeout")
        self.assertTrue(recs[0]["needs_user"])
        b.sessions["p.7"].terminate()

    def test_permission_no_allow_option_denied(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("p.2", "<@BTEST> permdeny"), "EvP2")
        self.assertTrue(web.wait_for("审批结果:cancelled", 30))
        self.assertTrue(web.wait_for("无可允许项"))
        recs, _ = sb.load_approvals(b._workdir("p.2"))
        self.assertEqual(recs[0]["resolution"], "cancelled")
        self.assertEqual(recs[0]["resolver"], "no_allow_option")
        self.assertTrue(recs[0]["needs_user"])
        snap = sb.collect_approvals(b.tasks_dir)
        entry = [e for e in snap if e["thread_ts"] == "p.2"]
        self.assertEqual(len(entry[0]["needs_user"]), 1)
        b.sessions["p.2"].terminate()

    def test_auto_approve_resolves_permission(self):
        sb.AUTO_APPROVE = True
        b, web = make_bridge()
        b.handle_event(root_ev("aa.1", "<@BTEST> perm"), "EvAA1")
        self.assertTrue(web.wait_for("审批结果:selected", 15))
        recs, _ = sb.load_approvals(b._workdir("aa.1"))
        self.assertEqual(recs[0]["resolution"], "allow_always")
        b.sessions["aa.1"].terminate()

    def test_decide_without_pending_reports_none(self):
        b, web = make_bridge()
        b.handle_event(reply_ev("d.9", "d.9b", "允许"), "EvD9")
        self.assertTrue(web.wait_for("没有待审批"))

    def test_stop_with_pending_permission_closes_it(self):
        b, web = make_bridge(timeout=60)
        b.handle_event(root_ev("p.8", "<@BTEST> perm"), "EvP8")
        s = wait_running(b, "p.8")
        self.assertIsNotNone(s)
        self.assertTrue(web.wait_for("待审批", 30))
        b.handle_event(reply_ev("p.8", "p.8a", "停止"), "EvP8S")
        self.assertTrue(web.wait_for("（已停止）", 20))
        recs, _ = sb.load_approvals(b._workdir("p.8"))
        self.assertEqual(recs[0]["resolution"], "cancelled")
        self.assertEqual(recs[0]["resolver"], "task_stop")
        self.assertTrue(recs[0]["needs_user"])
        s.terminate()

    def test_pending_approval_marked_interrupted_on_restart(self):
        tasks = tempfile.mkdtemp()
        wd = os.path.join(tasks, "x_7")
        os.makedirs(wd)
        sb.write_state(wd, "running", "干活")
        sb.record_approval(wd, {"request_id": "perm-9", "session_id": "s1",
                                "tool_call_id": "t1", "kind": "execute",
                                "summary": "pytest -x",
                                "options": ["allow_once"],
                                "requested_at": time.time(),
                                "status": "pending"})
        self.assertEqual(len(sb.mark_interrupted(tasks)), 1)
        recs, _ = sb.load_approvals(wd)
        self.assertEqual(recs[0]["status"], "interrupted")
        self.assertEqual(recs[0]["resolution"], "lost_on_restart")
        self.assertTrue(recs[0]["needs_user"])
        snap = sb.collect_approvals(tasks)
        self.assertEqual(len(snap), 1)
        self.assertEqual(snap[0]["pending"], [])
        self.assertEqual(len(snap[0]["needs_user"]), 1)
        self.assertTrue(snap[0]["covered"])
        self.assertEqual(snap[0]["thread_ts"], "x.7")

    def test_approvals_query_lists_records(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("p.3", "<@BTEST> perm"), "EvP3")
        self.assertTrue(web.wait_for("待审批", 30))
        b.handle_event(reply_ev("p.3", "p.3a", "允许"), "EvP3A")
        self.assertTrue(web.wait_for("审批结果:selected:a1", 30))
        b.handle_event(reply_ev("p.3", "p.3b", "审批"), "EvP3Q")
        self.assertTrue(web.wait_for("无待审批（台账已核验）"))
        self.assertTrue(web.wait_for("allow_once"))
        self.assertTrue(web.wait_for("核验时间"))
        b.sessions["p.3"].terminate()

    def test_approvals_query_pending_appears_then_clears(self):
        tasks = tempfile.mkdtemp()
        wd = os.path.join(tasks, "x_8")
        os.makedirs(wd)
        sb.write_state(wd, "running", "干活")
        sb.record_approval(wd, {"request_id": "perm-7", "session_id": "s1",
                                "tool_call_id": "t1", "kind": "execute",
                                "summary": "pytest -x",
                                "options": ["allow_once"],
                                "requested_at": time.time() - 3,
                                "status": "pending"})
        b, web = make_bridge(tasks_dir=tasks)
        b.handle_event(reply_ev("x.8", "x.8b", "审批"), "EvX8Q1")
        self.assertTrue(web.wait_for("待审批 perm-7"))
        self.assertTrue(web.wait_for("阻塞中"))
        sb.resolve_approval(wd, "perm-7", session_id="s1",
                            resolution="allow_once",
                            resolver="bridge_policy", needs_user=False)
        b.handle_event(reply_ev("x.8", "x.8c", "审批"), "EvX8Q2")
        self.assertTrue(web.wait_for("无待审批（台账已核验）"))
        self.assertTrue(web.wait_for("最近自动放行 perm-7"))

    def test_approvals_query_does_not_touch_running_session(self):
        b, web = make_bridge(timeout=60)
        b.handle_event(root_ev("p.4", "<@BTEST> slow"), "EvP4")
        s = wait_running(b, "p.4")
        self.assertIsNotNone(s)
        b.handle_event(reply_ev("p.4", "p.4b", "审批"), "EvP4Q")
        self.assertTrue(web.wait_for("核验时间"))
        self.assertIs(b.sessions.get("p.4"), s)
        self.assertTrue(s.running)
        self.assertIsNone(s.proc.poll())
        b.handle_event(reply_ev("p.4", "p.4c", "停止"), "EvP4S")
        self.assertTrue(web.wait_for("（已停止）", 15))
        s.terminate()

    def test_approvals_query_unknown_thread(self):
        b, web = make_bridge()
        b.handle_event(reply_ev("q.9", "q.9b", "审批"), "EvQ9")
        self.assertTrue(web.wait_for("无会话与审批记录"))

    def test_duplicate_permission_request_recorded_once(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("p.5", "<@BTEST> perm"), "EvP5")
        self.assertTrue(web.wait_for("待审批", 30))
        b.handle_event(reply_ev("p.5", "p.5a", "允许"), "EvP5A")
        self.assertTrue(web.wait_for("审批结果:selected:a1", 30))
        s = b.sessions["p.5"]
        msg = {"jsonrpc": "2.0", "id": "perm-dup",
               "method": "session/request_permission",
               "params": {"sessionId": "fake-session-1",
                          "toolCall": {"toolCallId": "t9",
                                       "kind": "execute",
                                       "title": "重复请求"},
                          "options": [{"optionId": "a1",
                                       "kind": "allow_once"}]}}
        s._handle_msg(msg, lambda x: None)
        s._handle_msg(msg, lambda x: None)
        recs, _ = sb.load_approvals(b._workdir("p.5"))
        self.assertEqual(
            len([r for r in recs if r["request_id"] == "perm-dup"]), 1)
        s.terminate()

    def test_root_redelivery_with_state_not_reexecuted(self):
        tasks = tempfile.mkdtemp()
        wd = os.path.join(tasks, "r_5")
        os.makedirs(wd)
        sb.write_state(wd, "interrupted", "重启前任务")
        b, web = make_bridge(tasks_dir=tasks)
        b.handle_event(root_ev("r.5"), "EvR5")
        self.assertTrue(web.wait_for("未重复执行"))
        self.assertNotIn("r.5", b.sessions)
        self.assertNotIn("r.5", b.starting)

    def test_start_failure_kills_spawned_process(self):
        def fake_procs():
            out = subprocess.run(["pgrep", "-f", "fake_acp.py"],
                                 capture_output=True, text=True)
            return [p for p in out.stdout.split() if p]
        baseline = len(fake_procs())
        b, web = make_bridge(mode="hanginit", timeout=2)
        b.handle_event(root_ev("h.9"), "EvH9")
        self.assertTrue(web.wait_for("启动 devin 失败", 30))
        self.assertNotIn("h.9", b.sessions)
        deadline = time.monotonic() + 5
        while len(fake_procs()) > baseline and time.monotonic() < deadline:
            time.sleep(0.1)
        self.assertEqual(len(fake_procs()), baseline)
        st = sb.read_state(b._workdir("h.9"))
        self.assertEqual(st["state"], "failed")

    def test_bang_prefix_forwards_command(self):
        b, web = make_bridge()
        b.handle_event(root_ev("b.1"), "EvB1")
        self.assertTrue(web.wait_for("已接单"))
        self.assertTrue(web.wait_for("任务结果：一切正常。"))
        b.handle_event(reply_ev("b.1", "b.2", "!plan 跑完它"), "EvB2")
        self.assertTrue(web.wait_for("执行指令:/plan 跑完它"))
        b.sessions["b.1"].terminate()

    def test_resume_dead_thread(self):
        b, web = make_bridge()
        wd = b._workdir("rs.1")
        os.makedirs(wd)
        sb.write_state(wd, "interrupted", "重启前任务",
                       extra={"session_id": "old-sess-9"})
        b.handle_event(reply_ev("rs.1", "rs.2", "继续刚才的活"), "EvR1")
        self.assertTrue(web.wait_for("已恢复旧会话"))
        self.assertTrue(web.wait_for("任务结果"))
        st = sb.read_state(wd)
        self.assertEqual(st["session_id"], "old-sess-9")
        self.assertNotEqual(st["state"], "interrupted")
        replies = [t for t in web.texts("rs.1") if "任务结果" in t]
        self.assertEqual(replies[-1], "任务结果：一切正常。")
        b.sessions["rs.1"].terminate()

    def test_resume_fallback_new_session(self):
        b, web = make_bridge()
        wd = b._workdir("rs.2")
        os.makedirs(wd)
        sb.write_state(wd, "interrupted", "重启前任务",
                       extra={"session_id": "bad-id"})
        b.handle_event(reply_ev("rs.2", "rs.2b", "接着干"), "EvR2")
        self.assertTrue(web.wait_for("已开新会话"))
        self.assertTrue(web.wait_for("任务结果"))
        st = sb.read_state(wd)
        self.assertEqual(st["session_id"], "fake-session-1")
        b.sessions["rs.2"].terminate()

    def test_mark_interrupted_preserves_session_id(self):
        tasks = tempfile.mkdtemp()
        wd = os.path.join(tasks, "i_9")
        os.makedirs(wd)
        sb.write_state(wd, "running", "干活",
                       extra={"session_id": "keep-me"})
        self.assertEqual(len(sb.mark_interrupted(tasks)), 1)
        st = sb.read_state(wd)
        self.assertEqual(st["state"], "interrupted")
        self.assertEqual(st["session_id"], "keep-me")
        wd2 = os.path.join(tasks, "i_10")
        os.makedirs(wd2)
        sb.write_state(wd2, "idle", "", extra={"session_id": "keep-2"})
        sb.write_state(wd2, "finished", "", "完成")
        st2 = sb.read_state(wd2)
        self.assertEqual(st2["state"], "finished")
        self.assertEqual(st2["session_id"], "keep-2")

    def test_bang_in_dead_thread_resumes(self):
        b, web = make_bridge()
        wd = b._workdir("rs.3")
        os.makedirs(wd)
        sb.write_state(wd, "interrupted", "",
                       extra={"session_id": "old-sess-7"})
        b.handle_event(reply_ev("rs.3", "rs.3b", "!plan x"), "EvR3")
        self.assertTrue(web.wait_for("已恢复旧会话"))
        self.assertTrue(web.wait_for("执行指令:/plan x"))
        b.sessions["rs.3"].terminate()

    def test_native_goal_file_attach_and_auto_resume(self):
        b, web = make_bridge()
        b.handle_event(root_ev("g.1"), "EvG1")
        self.assertTrue(web.wait_for("已接单"))
        self.assertTrue(web.wait_for("任务结果：一切正常。"))
        wd = b._workdir("g.1")
        b.sessions["g.1"].last_turn_end = 0  # human-placed file, long after last turn
        with open(os.path.join(wd, ".devin-goal-native"), "w") as f:
            f.write("把桥目标跑完")
        self.assertTrue(web.wait_for("检测到 .devin-goal-native", 15))
        self.assertTrue(web.wait_for("自动续跑", 15))
        self.assertTrue(web.wait_for("目标续跑完成。", 15))
        self.assertTrue(os.path.exists(
            os.path.join(wd, ".devin-goal-native.attached")))
        s = b.sessions.get("g.1")
        end = time.monotonic() + 5
        while time.monotonic() < end and not (
                s.goal and s.goal.get("status") == "complete"):
            time.sleep(0.05)
        self.assertEqual(s.goal["status"], "complete")
        self.assertEqual(s.goal["objective"], "把桥目标跑完")
        b.sessions["g.1"].terminate()

    def test_goal_card_shows_model_turns_and_objective(self):
        b, web = make_bridge()
        b.handle_event(root_ev("g.2"), "EvG2")
        self.assertTrue(web.wait_for("已接单"))
        self.assertTrue(web.wait_for("任务结果：一切正常。"))
        wd = b._workdir("g.2")
        b.sessions["g.2"].last_turn_end = 0  # human-placed file, long after last turn
        with open(os.path.join(wd, ".devin-goal-native"), "w") as f:
            f.write("状态卡验证")
        self.assertTrue(web.wait_for("🎯 状态卡验证", 15))
        self.assertTrue(web.wait_for("turn 1", 15))
        self.assertTrue(web.wait_for("SWE-2", 15))
        self.assertTrue(web.wait_for("Code", 15))
        self.assertTrue(web.wait_for("目标续跑完成。", 15))
        b.sessions["g.2"].terminate()

    def test_legacy_devin_goal_file_also_attaches(self):
        b, web = make_bridge()
        b.handle_event(root_ev("g.3"), "EvG3")
        self.assertTrue(web.wait_for("已接单"))
        self.assertTrue(web.wait_for("任务结果：一切正常。"))
        wd = b._workdir("g.3")
        b.sessions["g.3"].last_turn_end = 0  # human-placed file, long after last turn
        with open(os.path.join(wd, ".devin-goal"), "w") as f:
            f.write("老契约目标")
        self.assertTrue(web.wait_for("检测到 .devin-goal", 15))
        self.assertTrue(web.wait_for("目标续跑完成。", 15))
        b.sessions["g.3"].terminate()

    def test_status_query_reports_goal(self):
        b, web = make_bridge()
        b.handle_event(root_ev("g.4"), "EvG4")
        self.assertTrue(web.wait_for("已接单"))
        self.assertTrue(web.wait_for("任务结果：一切正常。"))
        wd = b._workdir("g.4")
        b.sessions["g.4"].last_turn_end = 0  # human-placed file, long after last turn
        with open(os.path.join(wd, ".devin-goal-native"), "w") as f:
            f.write("查询目标")
        self.assertTrue(web.wait_for("目标续跑完成。", 15))
        b.handle_event(reply_ev("g.4", "g.4b", "状态"), "EvG4b")
        self.assertTrue(web.wait_for("🎯 目标: 查询目标", 15))
        self.assertTrue(web.wait_for("已完成", 15))
        b.sessions["g.4"].terminate()

    def test_stop_clears_goal_and_blocks_resume(self):
        b, web = make_bridge()
        b.handle_event(root_ev("sg.1"), "EvSG1")
        self.assertTrue(web.wait_for("任务结果"))
        wd = b._workdir("sg.1")
        b.sessions["sg.1"].last_turn_end = 0  # human-placed file, long after last turn
        with open(os.path.join(wd, ".devin-goal-native"), "w") as f:
            f.write("别复活")
        self.assertTrue(web.wait_for("检测到 .devin-goal-native", 15))
        s = b.sessions["sg.1"]
        end = time.monotonic() + 8
        while time.monotonic() < end and not s.goal:
            time.sleep(0.05)
        self.assertTrue(s.goal)
        b.handle_event(reply_ev("sg.1", "sg.1x", "停止"), "EvSG1x")
        self.assertTrue(web.wait_for("目标已一并清除", 10))
        self.assertIsNone(s.goal)
        for name in (".devin-goal-native",
                     ".devin-goal-native.attached"):
            self.assertFalse(os.path.exists(os.path.join(wd, name)))

    def test_goal_clear_rpc_reaches_acp(self):
        b, web = make_bridge()
        b.handle_event(root_ev("gc.1"), "EvGC1")
        self.assertTrue(web.wait_for("任务结果"))
        wd = b._workdir("gc.1")
        b.sessions["gc.1"].last_turn_end = 0  # human-placed file, long after last turn
        with open(os.path.join(wd, ".devin-goal-native"), "w") as f:
            f.write("被清的目标")
        self.assertTrue(web.wait_for("检测到 .devin-goal-native", 15))
        s = b.sessions["gc.1"]
        end = time.monotonic() + 8
        while time.monotonic() < end and not s.goal:
            time.sleep(0.05)
        self.assertTrue(s.goal)
        b._goal_clear_rpc(s)
        self.assertIsNone(s.goal)
        end = time.monotonic() + 5
        calls = ""
        while time.monotonic() < end:
            try:
                calls = open(os.path.join(wd, ".acp_calls")).read()
            except OSError:
                calls = ""
            if "_cognition.ai/goal/clear" in calls:
                break
            time.sleep(0.05)
        self.assertIn("_cognition.ai/goal/clear", calls)
        s.terminate()

    def test_goal_clear_keyword_dead_thread(self):
        b, web = make_bridge()
        wd = b._workdir("g.9")
        os.makedirs(wd)
        with open(os.path.join(wd, ".devin-goal-native"), "w") as f:
            f.write("挂起的目标")
        b.handle_event(reply_ev("g.9", "g.9b", "取消目标"), "EvG9")
        self.assertTrue(web.wait_for("已清除待挂的目标文件"))
        self.assertFalse(
            os.path.exists(os.path.join(wd, ".devin-goal-native")))

    def test_reload_keyword_sets_event(self):
        b, web = make_bridge()
        b.handle_event(reply_ev("rl.1", "rl.2", "桥重启"), "EvRL")
        self.assertTrue(b.reload_ev.is_set())
        self.assertTrue(web.wait_for("热重启"))

    def test_inflight_recorded_and_cleared(self):
        b, web = make_bridge()
        b.handle_event(root_ev("if.1", "<@BTEST> slow"), "EvIF1")
        s = wait_running(b, "if.1")
        self.assertIsNotNone(s)
        wd = b._workdir("if.1")
        inf = None
        end = time.monotonic() + 5
        while time.monotonic() < end:
            st = sb.read_state(wd)
            if st and st.get("inflight"):
                inf = st["inflight"]
                break
            time.sleep(0.05)
        self.assertIsNotNone(inf)
        self.assertIn("slow", inf["text"])
        b.handle_event(reply_ev("if.1", "if.2", "停止"), "EvIF2")
        self.assertTrue(web.wait_for("（已停止）", 15))
        st = sb.read_state(wd)
        self.assertIsNone(st.get("inflight"))
        b.sessions["if.1"].terminate()

    def test_mark_interrupted_skips_restored(self):
        tasks = tempfile.mkdtemp()
        wd = os.path.join(tasks, "k_1")
        os.makedirs(wd)
        sb.write_state(wd, "running", "x")
        out = sb.mark_interrupted(tasks, skip={wd})
        self.assertEqual(out, [])
        self.assertEqual(sb.read_state(wd)["state"], "running")

    def test_adopt_inflight_replays_prompt(self):
        tasks = tempfile.mkdtemp()
        b, web = make_bridge(tasks_dir=tasks)
        wd = os.path.join(tasks, "a_1")
        os.makedirs(wd)
        sb.write_state(wd, "running", "生成中",
                       extra={"session_id": "old-inflight",
                              "inflight": {"text": "接着跑",
                                           "msg_ts": "m7"}})
        adopted = b.adopt_inflight()
        self.assertIn(wd, adopted)
        self.assertTrue(web.wait_for("恢复重启前未完成", 10))
        self.assertTrue(web.wait_for("已恢复旧会话", 15))
        self.assertTrue(web.wait_for("任务结果：一切正常。", 15))
        b.sessions["a.1"].terminate()

    def test_restore_live_reattaches_process(self):
        tasks = tempfile.mkdtemp()
        b, web = make_bridge(tasks_dir=tasks)
        wd = os.path.join(tasks, "r_1")
        os.makedirs(wd)
        proc = subprocess.Popen(
            [sys.executable, FAKE, "normal"], cwd=wd,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1,
            start_new_session=True)
        self.addCleanup(proc.terminate)
        inf = os.dup(proc.stdin.fileno())
        outf = os.dup(proc.stdout.fileno())
        live = os.path.join(tasks, "live.json")
        with open(live, "w") as f:
            json.dump({"sessions": [{
                "ts": "r.1", "workdir": wd,
                "session_id": "fake-session-1", "pid": proc.pid,
                "stdin_fd": inf, "stdout_fd": outf, "next_id": 5,
                "turns": [{"text": "继续干活", "msg_ts": None,
                           "req_id": None}]}]}, f)
        restored = b.restore_live(live)
        self.assertIn(wd, restored)
        s = b.sessions.get("r.1")
        self.assertIsNotNone(s)
        # >=: the restored-turn replay thread may already have spent
        # request ids (session/prompt, goal/get) before we read it;
        # a lost restore would read 0.
        self.assertGreaterEqual(s.next_id, 5)
        self.assertTrue(web.wait_for("任务结果：一切正常。", 15))
        s.terminate()

    def test_sd_listen_fds_maps_names(self):
        os.environ.update({"LISTEN_PID": str(os.getpid()),
                           "LISTEN_FDS": "2",
                           "LISTEN_FDNAMES": "a:b"})
        try:
            m = sb.SessionBridge._sd_listen_fds()
            self.assertEqual(m, {"a": 3, "b": 4})
        finally:
            for k in ("LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"):
                os.environ.pop(k, None)

    def test_exec_keeps_acp_alive_and_turn_continues(self):
        meta = os.path.join(tempfile.mkdtemp(), "meta.json")
        r = subprocess.run(
            [sys.executable, os.path.abspath(__file__),
             "--exec-helper", meta],
            capture_output=True, text=True, timeout=90)
        self.assertIn("EXEC_RESTORE_PASS", r.stdout,
                      (r.stdout + r.stderr)[-800:])


def _exec_helper_phase1(meta_path):
    """Spawn a fake acp mid-turn, then exec() phase2 in place — the
    real 'systemctl reload' path, proving pipes survive exec."""
    wd = tempfile.mkdtemp()
    proc = subprocess.Popen(
        [sys.executable, FAKE, "normal"], cwd=wd,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, text=True, bufsize=1,
        start_new_session=True)

    def req(i, method, params):
        proc.stdin.write(json.dumps(
            {"jsonrpc": "2.0", "id": i, "method": method,
             "params": params}) + "\n")
        proc.stdin.flush()

    req(1, "initialize", {"protocolVersion": 1,
                          "clientCapabilities": {}})
    sid = None
    for line in proc.stdout:
        m = json.loads(line)
        if m.get("id") == 1:
            req(2, "session/new", {"cwd": wd, "mcpServers": []})
        elif m.get("id") == 2:
            sid = m["result"]["sessionId"]
            break
    req(3, "session/prompt",
        {"sessionId": sid,
         "prompt": [{"type": "text", "text": "trickle"}]})
    got = 0
    for line in proc.stdout:
        m = json.loads(line)
        if m.get("method") == "session/update":
            got += 1
            if got >= 2:
                break
    json.dump({"pid": proc.pid, "stdin_fd": proc.stdin.fileno(),
               "stdout_fd": proc.stdout.fileno(), "workdir": wd,
               "ts": "ex.1", "session_id": sid, "req_id": 3},
              open(meta_path, "w"))
    proc.stdin.flush()
    os.set_inheritable(proc.stdin.fileno(), True)
    os.set_inheritable(proc.stdout.fileno(), True)
    os.execv(sys.executable,
             [sys.executable, os.path.abspath(__file__),
              "--exec-phase2", meta_path])


class BangCommandTest(unittest.TestCase):
    """!-prefixed bridge commands: help card, status, stop, reset,
    goal cancel, unknown-passthrough — plus the bare 帮助 hole."""

    def setUp(self):
        self._aa = sb.AUTO_APPROVE
        sb.AUTO_APPROVE = True
        self._gpoll = sb.GOAL_POLL_SEC
        sb.GOAL_POLL_SEC = 0.05
        self._gwatch = sb.GOAL_WATCH_SEC
        sb.GOAL_WATCH_SEC = 0.1
        self._dgap = sb.DRAFT_MIN_GAP
        sb.DRAFT_MIN_GAP = 0
        self._pgap = sb.PROGRESS_MIN_GAP
        sb.PROGRESS_MIN_GAP = 0
        self._grace = sb.CANCEL_GRACE
        sb.CANCEL_GRACE = 1.5
        self._pctx = os.environ.get("PCTX_MESSAGES_PUT")
        os.environ["PCTX_MESSAGES_PUT"] = ""

    def tearDown(self):
        sb.AUTO_APPROVE = self._aa
        sb.GOAL_POLL_SEC = self._gpoll
        sb.GOAL_WATCH_SEC = self._gwatch
        sb.DRAFT_MIN_GAP = self._dgap
        sb.PROGRESS_MIN_GAP = self._pgap
        sb.CANCEL_GRACE = self._grace
        if self._pctx is None:
            os.environ.pop("PCTX_MESSAGES_PUT", None)
        else:
            os.environ["PCTX_MESSAGES_PUT"] = self._pctx

    def _session(self, b, web, ts="x.1"):
        b.handle_event(root_ev(ts), "Ev-" + ts)
        self.assertTrue(web.wait_for("任务结果"))
        return b.sessions[ts]

    def test_bang_help_posts_card_no_turn(self):
        b, web = make_bridge()
        self._session(b, web, "h.1")
        web.posts.clear()
        b.handle_event(reply_ev("h.1", "h.2", "!help"), "EvH2")
        self.assertTrue(web.wait_for("桥命令"))
        self.assertFalse(web.wait_for("执行指令", 2))
        b.sessions["h.1"].terminate()

    def test_help_word_posts_card_no_turn(self):
        b, web = make_bridge()
        self._session(b, web, "h.3")
        web.posts.clear()
        b.handle_event(reply_ev("h.3", "h.4", "帮助"), "EvH4")
        self.assertTrue(web.wait_for("桥命令"))
        self.assertFalse(web.wait_for("执行指令", 2))
        b.sessions["h.3"].terminate()

    def test_bang_status(self):
        b, web = make_bridge()
        self._session(b, web, "t.1")
        b.handle_event(reply_ev("t.1", "t.2", "!status"), "EvT2")
        self.assertTrue(web.wait_for("状态:"))
        b.sessions["t.1"].terminate()

    def test_bang_queue_empty(self):
        b, web = make_bridge()
        self._session(b, web, "q.1")
        b.handle_event(reply_ev("q.1", "q.2", "!queue"), "EvQ2")
        self.assertTrue(web.wait_for("队列是空的"))
        b.sessions["q.1"].terminate()

    def test_bang_stop_runs_stop_path(self):
        b, web = make_bridge(timeout=60)
        b.handle_event(root_ev("bs.1", "<@BTEST> slow"), "EvBS1")
        self.assertIsNotNone(wait_running(b, "bs.1"))
        b.handle_event(reply_ev("bs.1", "bs.2", "!stop"), "EvBS2")
        self.assertTrue(web.wait_for("正在停止", 10))
        self.assertTrue(web.wait_for("（已停止）", 15))
        b.sessions["bs.1"].terminate()

    def test_bang_unknown_passthrough(self):
        b, web = make_bridge()
        self._session(b, web, "u.1")
        b.handle_event(reply_ev("u.1", "u.2", "!clear now"), "EvU2")
        self.assertTrue(web.wait_for("执行指令:/clear now"))
        b.sessions["u.1"].terminate()

    def test_bang_goal_cancel(self):
        b, web = make_bridge()
        self._session(b, web, "g.5")
        wd = b._workdir("g.5")
        b.sessions["g.5"].last_turn_end = 0  # human-placed file, long after last turn
        with open(os.path.join(wd, ".devin-goal-native"), "w") as f:
            f.write("要撤的目标")
        self.assertTrue(web.wait_for("检测到 .devin-goal-native", 15))
        s = b.sessions["g.5"]
        end = time.monotonic() + 8
        while time.monotonic() < end and not s.goal:
            time.sleep(0.05)
        self.assertTrue(s.goal)
        b.handle_event(reply_ev("g.5", "g.5b", "!goal cancel"), "EvG5b")
        self.assertTrue(web.wait_for("已清除目标", 10))
        self.assertIsNone(s.goal)
        s.terminate()

    def test_bang_reset_then_fresh_session(self):
        b, web = make_bridge()
        old_s = self._session(b, web, "r.1")
        b.handle_event(reply_ev("r.1", "r.2", "!reset"), "EvR2")
        self.assertTrue(web.wait_for("已重置"))
        self.assertNotIn("r.1", b.sessions)
        st = sb.read_state(b._workdir("r.1"))
        self.assertEqual(st["state"], "reset")
        web.posts.clear()
        b.handle_event(reply_ev("r.1", "r.3", "再来一次"), "EvR3")
        end = time.monotonic() + 15
        while time.monotonic() < end and "r.1" not in b.sessions:
            time.sleep(0.05)
        self.assertIn("r.1", b.sessions)
        s = b.sessions["r.1"]
        self.assertIsNot(s, old_s)
        self.assertTrue(web.wait_for("任务结果"))
        s.terminate()

    def test_bang_reload_sets_event(self):
        b, web = make_bridge()
        self._session(b, web, "rl.1")
        b.handle_event(reply_ev("rl.1", "rl.2", "!reload"), "EvRL2")
        self.assertTrue(b.reload_ev.is_set())
        b.sessions["rl.1"].terminate()


class PermButtonTest(unittest.TestCase):
    """Block Kit permission buttons: post actions, click routes the
    exact optionId, message settles."""

    def setUp(self):
        self._aa = sb.AUTO_APPROVE
        sb.AUTO_APPROVE = False
        self._pt = sb.PERM_TIMEOUT
        sb.PERM_TIMEOUT = 30
        self._pctx = os.environ.get("PCTX_MESSAGES_PUT")
        os.environ["PCTX_MESSAGES_PUT"] = ""
        self._pgap = sb.PROGRESS_MIN_GAP
        sb.PROGRESS_MIN_GAP = 0
        self._dgap = sb.DRAFT_MIN_GAP
        sb.DRAFT_MIN_GAP = 0

    def tearDown(self):
        sb.AUTO_APPROVE = self._aa
        sb.PERM_TIMEOUT = self._pt
        sb.PROGRESS_MIN_GAP = self._pgap
        sb.DRAFT_MIN_GAP = self._dgap
        if self._pctx is None:
            os.environ.pop("PCTX_MESSAGES_PUT", None)
        else:
            os.environ["PCTX_MESSAGES_PUT"] = self._pctx

    def _perm_blocks(self, web, timeout=15):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            for ts, blocks in web.blocks:
                for blk in blocks:
                    if blk.get("type") == "actions" and \
                            blk.get("block_id", "").startswith("perm_"):
                        return ts, blk["elements"]
            time.sleep(0.05)
        return None, None

    def _perm_bridge(self, **kw):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=tempfile.mkdtemp(),
            acp_cmd=[sys.executable, FAKE, "normal"],
            timeout=kw.pop("timeout", 60), idle_sec=3600,
            own_bot_id=BOT, channel=CHANNEL,
            update=web.update, **kw)
        return b, web

    def test_perm_posted_with_buttons(self):
        b, web = self._perm_bridge()
        b.handle_event(root_ev("pb.1", "<@BTEST> perm"), "EvPB1")
        ts, elements = self._perm_blocks(web)
        self.assertIsNotNone(elements, "no action buttons posted")
        vals = [json.loads(e["value"]) for e in elements]
        oids = {v["oid"] for v in vals}
        self.assertEqual(oids, {"a1", "aa", "r1"})
        self.assertTrue(all(v["rid"] for v in vals))
        b.sessions["pb.1"].terminate()

    def test_perm_infinite_timeout_still_posts(self):
        """PERMISSION_TIMEOUT_SEC=inf must not crash the card render:
        int(inf) raises OverflowError — found live when production env
        combined inf timeout with manual approval."""
        sb.PERM_TIMEOUT = float("inf")
        try:
            b, web = self._perm_bridge()
            b.handle_event(root_ev("pbi.1", "<@BTEST> perm"), "EvPBI1")
            ts, elements = self._perm_blocks(web)
            self.assertIsNotNone(elements,
                                 "perm card missing under inf timeout")
            ctx = [el.get("text", "")
                   for _, blocks in web.blocks
                   for blk in blocks if blk.get("type") == "context"
                   for el in blk.get("elements", [])]
            self.assertTrue(any("不自动拒绝" in t for t in ctx),
                            f"inf-timeout hint missing: {ctx}")
            b.sessions["pbi.1"].terminate()
        finally:
            sb.PERM_TIMEOUT = 30

    def test_perm_pending_parks_stuck_kill(self):
        """A turn awaiting human approval is parked, not silent:
        exceeding the prompt timeout must neither kill it nor hand a
        negative timeout to the reader — found live: the parked loop
        crashed with 'timeout must be a non-negative number'."""
        b, web = self._perm_bridge(timeout=0.5)
        b.handle_event(root_ev("ppk.1", "<@BTEST> perm"), "EvPPK1")
        ts, elements = self._perm_blocks(web)
        self.assertIsNotNone(elements)
        # far past the 0.5s prompt timeout, still parked on the perm
        time.sleep(1.5)
        self.assertIn("ppk.1", b.sessions,
                      "parked perm session was wrongly killed")
        self.assertFalse(
            any("判定卡死" in t or "non-negative" in t
                for _, t, *_ in web.posts),
            "parked perm either timed out or hit the negative-wait bug")
        b.sessions["ppk.1"].terminate()

    def test_perm_button_selects_exact_option(self):
        b, web = self._perm_bridge()
        b.handle_event(root_ev("pb.2", "<@BTEST> perm"), "EvPB2")
        ts, elements = self._perm_blocks(web)
        self.assertIsNotNone(elements)
        val = next(e["value"] for e in elements
                   if json.loads(e["value"])["oid"] == "aa")
        b.handle_interaction({
            "type": "block_actions",
            "user": {"id": "U99"},
            "message": {"ts": "m1", "thread_ts": "pb.2"},
            "actions": [{"action_id": "perm", "value": val}],
        })
        self.assertTrue(web.wait_for("总是允许", 15))
        self.assertTrue(
            any("✅" in t or "allow_always" in t
                for _, t in web.updates),
            "perm message not settled")
        b.sessions["pb.2"].terminate()

    def test_perm_button_unknown_rid_reports(self):
        b, web = make_bridge()
        b.handle_interaction({
            "type": "block_actions",
            "user": {"id": "U1"},
            "message": {"ts": "m1", "thread_ts": "zz.9"},
            "actions": [{"action_id": "perm",
                         "value": json.dumps(
                             {"rid": "nope", "oid": "a1"})}],
        })
        self.assertTrue(web.wait_for("已处理或已失效"))

    def test_perm_deny_with_reason_modal(self):
        modals = []
        b, web = self._perm_bridge(
            open_modal=lambda tid, v: modals.append((tid, v)))
        b.handle_event(root_ev("pr.1", "<@BTEST> perm"), "EvPR1")
        ts, elements = self._perm_blocks(web)
        self.assertIsNotNone(elements)
        rid = next(iter(b.sessions["pr.1"].pending_perms))
        b.handle_interaction({
            "type": "block_actions", "trigger_id": "trig1",
            "user": {"id": "U9"},
            "message": {"ts": ts, "thread_ts": "pr.1"},
            "actions": [{"action_id": "perm_reason",
                         "value": json.dumps({"rid": rid})}]})
        self.assertEqual(len(modals), 1)
        self.assertEqual(modals[0][0], "trig1")
        view = modals[0][1]
        meta = json.loads(view["private_metadata"])
        self.assertEqual(meta["rid"], rid)
        view["state"] = {"values": {
            "reason": {"text": {"value": "别跑测试"}}}}
        b.handle_interaction({"type": "view_submission", "view": view,
                              "user": {"id": "U9"}})
        self.assertTrue(web.wait_for("审批结果:selected:r1", 15))
        self.assertTrue(
            any("别跑测试" in t and "❌" in t
                for _, t in web.updates),
            f"deny card not updated: {web.updates!r}")
        b.sessions["pr.1"].terminate()

    def test_perm_reason_button_in_card(self):
        b, web = self._perm_bridge()
        b.handle_event(root_ev("pr.2", "<@BTEST> perm"), "EvPR2")
        ts, _ = self._perm_blocks(web)
        self.assertIsNotNone(ts)
        self.assertTrue(
            any("perm_reason" in json.dumps(bk)
                for _, bk in web.blocks),
            "perm card lacks deny-with-reason button")
        b.sessions["pr.2"].terminate()

    def test_card_action_ids_unique(self):
        """Slack rejects the entire message (invalid_blocks) when any
        action_id repeats across its blocks — every card we build must
        keep ids unique."""
        b, web = self._perm_bridge()
        b.handle_event(root_ev("pu.1", "<@BTEST> perm"), "EvPU1")
        ts, _ = self._perm_blocks(web)
        self.assertIsNotNone(ts)
        for ts_, blocks in web.blocks:
            aids = [e.get("action_id")
                    for blk in blocks
                    if blk.get("type") == "actions"
                    for e in blk.get("elements", [])
                    if e.get("type") == "button"]
            self.assertEqual(len(aids), len(set(aids)),
                             f"duplicate action_ids in {ts_}: {aids}")
        b.sessions["pu.1"].terminate()


    def test_user_reply_interrupts_running_turn(self):
        """Speaking while it works = interrupt, not queue: the running
        turn is cancelled and the new message runs next."""
        b, web = make_bridge(timeout=60)
        b.handle_event(root_ev("q.1", "<@BTEST> slow"), "EvQ1")
        s = wait_running(b, "q.1")
        self.assertIsNotNone(s)
        b.handle_event(reply_ev("q.1", "q.2", "可以先回我话么"), "EvQ2")
        # turn settles as interrupted, then the new message runs
        self.assertTrue(web.wait_for("已打断", 15))
        self.assertTrue(web.wait_for("任务结果", 15))
        b.sessions["q.1"].terminate()

    def test_interrupt_ev_cleared_at_acquire(self):
        """An interrupt signalled after the turn already ended must
        NOT cancel the next prompt — it's consumed at lock acquire."""
        b, web = make_bridge()
        b.handle_event(root_ev("iq.1", "<@BTEST> go"), "EvIQ")
        self.assertTrue(web.wait_for("任务结果", 15))
        s = b.sessions["iq.1"]
        s.interrupt_ev.set()          # stale: nothing running now
        b.handle_event(reply_ev("iq.1", "iq.2", "再来"), "EvIQ2")
        self.assertTrue(web.wait_for("任务结果", 15))
        # the second prompt completed normally, not 'cancelled'
        s2 = b.sessions["iq.1"]
        self.assertFalse(s2.interrupted_turn)
        s2.terminate()

    def test_queue_notice_silent_when_idle(self):
        b, web = make_bridge()
        b.handle_event(root_ev("q.4"), "EvQ4")
        self.assertTrue(web.wait_for("任务结果"))
        web.posts.clear()
        b.handle_event(reply_ev("q.4", "q.5", "你好"), "EvQ5")
        time.sleep(0.5)
        self.assertFalse(any("已排队" in t for t in web.texts("q.4")))
        b.sessions["q.4"].terminate()

    def test_goal_cancel_kills_goal_turn(self):
        b, web = make_bridge(timeout=60)
        b.handle_event(root_ev("g.9", "<@BTEST> /goal slow finish later"),
                       "EvG9")
        s = wait_running(b, "g.9")
        self.assertIsNotNone(s)
        b.handle_event(reply_ev("g.9", "g.10", "!goal cancel"), "EvG10")
        self.assertTrue(web.wait_for("叫停", 10))
        # session survives; the turn itself was cancelled
        self.assertIn("g.9", b.sessions)
        self.assertTrue(b.sessions["g.9"].alive)
        b.sessions["g.9"].terminate()

    def test_queued_prompt_keeps_status_card(self):
        b, web = make_bridge(timeout=60)
        b.handle_event(root_ev("k.1", "<@BTEST> slow"), "EvK1")
        s = wait_running(b, "k.1")
        self.assertIsNotNone(s)
        s.status_ts = "card.1"
        b.handle_event(reply_ev("k.1", "k.2", "next"), "EvK2")
        end = time.monotonic() + 5
        while time.monotonic() < end and s.queued == 0:
            time.sleep(0.05)
        self.assertGreater(s.queued, 0)
        self.assertEqual(s.status_ts, "card.1")
        b.handle_event(reply_ev("k.1", "k.3", "!stop"), "EvK3")
        self.assertTrue(web.wait_for("已停止", 10))

    def test_think_posted_quoted_before_reply(self):
        b, web = make_bridge()
        _en = sb.THINK_ENABLED
        sb.THINK_ENABLED = True
        try:
            b.handle_event(root_ev("th.1"), "EvTH1")
            self.assertTrue(web.wait_for("任务结果"))
        finally:
            sb.THINK_ENABLED = _en
        posts = web.texts("th.1")
        think_i = next(i for i, t in enumerate(posts)
                       if t.startswith("🧠 *思路*"))
        ans_i = next(i for i, t in enumerate(posts)
                     if "任务结果" in t and i > think_i)
        self.assertGreater(ans_i, think_i)
        self.assertIn("先理清思路", posts[think_i])
        self.assertIn("> ", posts[think_i])
        b.sessions["th.1"].terminate()

    def test_think_trimmed_head_and_tail(self):
        b, web = make_bridge()
        b.handle_event(root_ev("th.2"), "EvTH2")
        self.assertTrue(web.wait_for("任务结果"))
        s = b.sessions["th.2"]
        _max = sb.THINK_POST_MAX
        _en = sb.THINK_ENABLED
        sb.THINK_POST_MAX = 200
        sb.THINK_ENABLED = True
        try:
            s.think_chunks = ["H" * 300 + "M" * 300 + "T" * 300]
            web.posts.clear()
            b._post_think("th.2", s)
        finally:
            sb.THINK_POST_MAX = _max
            sb.THINK_ENABLED = _en
        out = web.texts("th.2")[-1]
        self.assertTrue(out.startswith("🧠 *思路*"))
        self.assertIn("省略", out)
        self.assertIn("HHHH", out)
        self.assertIn("TTTT", out)
        self.assertNotIn("MMMM", out)
        s.terminate()

    def test_think_disabled_posts_nothing(self):
        _en = sb.THINK_ENABLED
        sb.THINK_ENABLED = False
        try:
            b, web = make_bridge()
            b.handle_event(root_ev("th.3"), "EvTH3")
            self.assertTrue(web.wait_for("任务结果"))
        finally:
            sb.THINK_ENABLED = _en
        self.assertFalse(any(t.startswith("🧠 *思路*")
                             for t in web.texts("th.3")))
        b.sessions["th.3"].terminate()


class RegressionFixesTest(unittest.TestCase):
    """Guard the fixes landed during the full-surface test pass:
    request-id race, reader stealing foreign results, reaction sweep
    cost, stale dispatch-queue entries, lost pending permissions,
    pump noise on closed pipes, mid-text mention silence, late ack,
    log storms, and double stop posts."""

    def setUp(self):
        self._aa = sb.AUTO_APPROVE
        sb.AUTO_APPROVE = False
        self._pctx_put = os.environ.get("PCTX_MESSAGES_PUT")
        os.environ["PCTX_MESSAGES_PUT"] = ""

    def tearDown(self):
        sb.AUTO_APPROVE = self._aa
        if self._pctx_put is None:
            os.environ.pop("PCTX_MESSAGES_PUT", None)
        else:
            os.environ["PCTX_MESSAGES_PUT"] = self._pctx_put

    def _running_session(self, tag="r.1", text="<@BTEST> go",
                         timeout=30):
        b, web = make_bridge(timeout=timeout)
        b.handle_event(root_ev(tag, text), "Ev" + tag)
        self.assertTrue(web.wait_for("任务结果：一切正常。", 30))
        s = b.sessions[tag]
        return b, web, s

    def test_request_ids_unique_under_concurrency(self):
        b, web, s = self._running_session("c.1")
        try:
            s._send = lambda m: None
            s._wait_result = lambda *a, **k: {"id": a[0]}
            ids, lock = [], threading.Lock()

            def hit():
                for _ in range(200):
                    rid = s._request("x", {})
                    with lock:
                        ids.append(rid)

            ts = [threading.Thread(target=hit) for _ in range(8)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            self.assertEqual(len(set(ids)), len(ids),
                             "duplicate JSON-RPC ids allocated")
        finally:
            s.terminate()

    def test_goal_fetch_returns_foreign_result(self):
        b, web, s = self._running_session("g.1")
        try:
            s.q.put(json.dumps({"jsonrpc": "2.0", "id": 4242,
                                "result": {"x": 1}}))
            b._goal_fetch(s, wait=2)
            seen = []
            for _ in range(4):
                try:
                    item = s.q.get(timeout=1)
                except queue.Empty:
                    break
                if item is None:
                    break
                seen.append(json.loads(item).get("id"))
            self.assertIn(4242, seen,
                          "foreign result line was eaten by goal fetch")
        finally:
            s.terminate()

    def test_react_state_unreacts_only_tracked(self):
        adds, rems = [], []
        b, web = make_bridge(react=lambda t, n: adds.append(n),
                             unreact=lambda t, n: rems.append(n))
        b._react_state("t.2", sb.REACT_WORKING)
        adds.clear()
        rems.clear()
        b._react_state("t.2", "white_check_mark")
        self.assertEqual(rems, [sb.REACT_WORKING])
        self.assertEqual(adds, ["white_check_mark"])
        adds.clear()
        rems.clear()
        b._react_state("t.2", "white_check_mark")
        self.assertEqual(rems, [])
        self.assertEqual(adds, [])

    def test_pump_queue_drops_terminal_and_reset(self):
        b, web = make_bridge()
        b.dispatch_queue.append(("d.1", "x", []))
        os.makedirs(b._workdir("d.1"))
        sb.write_state(b._workdir("d.1"), "stopped", "", "user stop")
        b.dispatch_queue.append(("d.2", "y", []))
        os.makedirs(b._workdir("d.2"))
        sb.write_state(b._workdir("d.2"), "reset", "", "user reset")
        b._pump_queue()
        self.assertFalse(b.dispatch_queue)
        self.assertNotIn("d.1", b.starting)
        self.assertNotIn("d.2", b.starting)
        self.assertTrue(web.wait_for("排队任务已取消", 5))
        b.dispatch_queue.append(("d.3", "echo hi", []))
        os.makedirs(b._workdir("d.3"))
        sb.write_state(b._workdir("d.3"), "queued", "", "")
        b._pump_queue()
        self.assertTrue(web.wait_for("任务结果：一切正常。", 30))
        s = b.sessions.get("d.3")
        if s:
            s.terminate()

    def test_perm_send_failure_keeps_pending(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("p.1", "<@BTEST> perm"), "EvP1")
        s = wait_running(b, "p.1")
        self.assertIsNotNone(s, "prompt never started")
        end = time.monotonic() + 10
        while not s.pending_perms and time.monotonic() < end:
            time.sleep(0.05)
        self.assertTrue(s.pending_perms, "perm request never arrived")
        orig_send = s._send
        s._send = lambda m: (_ for _ in ()).throw(
            sb.AcpExited("x"))
        try:
            with self.assertRaises(sb.AcpExited):
                s.user_decide(True, "U1")
            self.assertTrue(s.pending_perms,
                            "pending perm lost on send failure")
        finally:
            s._send = orig_send
            s.terminate()

    def test_midtext_mention_hint(self):
        b, web = make_bridge(own_user_id="UBOT")
        b.handle_event({"type": "message", "channel": CHANNEL,
                        "ts": "m.9", "text": "hi <@UBOT> look"},
                       "EvM9")
        self.assertTrue(web.wait_for("消息开头"))
        n = sum(1 for t in web.texts() if "消息开头" in t)
        b.handle_event({"type": "message", "channel": CHANNEL,
                        "ts": "m.10", "text": "hi <@UALICE> look"},
                       "EvM10")
        time.sleep(0.5)
        self.assertEqual(sum(1 for t in web.texts() if "消息开头" in t),
                         n)

    def test_early_ack_precedes_spawn_failure(self):
        b, web = make_bridge(mode="hanginit", timeout=2)
        b.handle_event(root_ev("h.9"), "EvH9")
        self.assertTrue(web.wait_for("已接单", 15))
        self.assertTrue(web.wait_for("启动 devin 失败", 30))
        texts = web.texts("h.9")
        ack = next(i for i, t in enumerate(texts) if "已接单" in t)
        fail = next(i for i, t in enumerate(texts)
                    if "启动 devin 失败" in t)
        self.assertLess(ack, fail)

    def test_pump_tolerates_closed_fd(self):
        class FakeProc:
            def __init__(self):
                self.stdout = self.DeadStdout()

            class DeadStdout:
                def __iter__(self):
                    raise OSError(9, "Bad file descriptor")

        s = sb.DevinSession.__new__(sb.DevinSession)
        s.proc = FakeProc()
        s.q = queue.Queue()
        s._pump()
        self.assertIsNone(s.q.get_nowait())

    def test_queued_cancel_notice_is_brief(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("qc.1"), "EvQC1")
        self.assertTrue(web.wait_for("任务结果：一切正常。", 30))
        s = b.sessions["qc.1"]
        try:
            flag = b.stop_flags.setdefault("qc.1", threading.Event())
            flag.set()
            b._run_prompt("qc.1", "queued work")
            self.assertTrue(web.wait_for("排队中已取消", 5))
            full = [t for t in web.texts("qc.1") if "（已停止）" in t]
            self.assertEqual(full, [])
        finally:
            s.terminate()

    def test_log_err_throttles_transient_storms(self):
        b, web = make_bridge()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            for _ in range(3):
                b._log_err("post", URLError("boom"))
        lines = [x for x in buf.getvalue().splitlines()
                 if "[error]" in x]
        self.assertEqual(len(lines), 1)

    def test_stop_idle_posts_once(self):
        b, web = make_bridge(timeout=30)
        b.handle_event(root_ev("st.1"), "EvST1")
        self.assertTrue(web.wait_for("任务结果：一切正常。", 30))
        b.handle_event(reply_ev("st.1", "st.2", "停止"), "EvST2")
        self.assertTrue(web.wait_for("会话已停止", 15))
        self.assertFalse(any("正在停止" in t for t in web.texts("st.1")))


class UxPolishTest(unittest.TestCase):
    """Desktop-like UX in Slack: markdown rendering at the transport
    boundary, live control buttons on the status card, and a
    single-message resume receipt."""

    def setUp(self):
        self._aa = sb.AUTO_APPROVE
        sb.AUTO_APPROVE = False
        self._pctx_put = os.environ.get("PCTX_MESSAGES_PUT")
        os.environ["PCTX_MESSAGES_PUT"] = ""

    def tearDown(self):
        sb.AUTO_APPROVE = self._aa
        if self._pctx_put is None:
            os.environ.pop("PCTX_MESSAGES_PUT", None)
        else:
            os.environ["PCTX_MESSAGES_PUT"] = self._pctx_put

    def test_slack_md_inline(self):
        self.assertEqual(sb.slack_md("**加粗** 和 __b__"),
                         "*加粗* 和 *b*")
        self.assertEqual(sb.slack_md("## 小节"), "*小节*")
        self.assertEqual(sb.slack_md("### 小 ###"), "*小*")
        self.assertEqual(sb.slack_md("[链接](https://x.dev)"),
                         "<https://x.dev|链接>")
        self.assertEqual(sb.slack_md("~~划掉~~"), "~划掉~")
        self.assertEqual(sb.slack_md("- [ ] a\n- [x] b"),
                         "- ☐ a\n- ☑ b")
        self.assertEqual(sb.slack_md("---"), "──────────")

    def test_slack_md_table_aligned_fence(self):
        out = sb.slack_md("| 名 | 值 |\n|---|---|\n| 甲 | 1 |\n"
                          "| 乙丙 | 22 |")
        lines = out.splitlines()
        self.assertEqual(lines[0], "```")
        self.assertEqual(lines[-1], "```")
        self.assertIn("乙丙", out)
        self.assertIn("---|", out)
        # slack-native text passes through; conversion is idempotent
        self.assertEqual(sb.slack_md(out), out)
        self.assertEqual(sb.slack_md("*已是粗体*"), "*已是粗体*")

    def test_slack_md_inside_code_fence_untouched(self):
        src = "```\n**not bold**\n| a | b |\n```"
        self.assertEqual(sb.slack_md(src), src)

    def test_post_renders_markdown(self):
        b, web = make_bridge()
        b.post("x.9", "**重点** 见 https://a.b")
        self.assertEqual(web.posts[-1][1], "*重点* 见 https://a.b")

    def test_status_card_has_stop_button_while_running(self):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=tempfile.mkdtemp(),
            acp_cmd=[sys.executable, FAKE], timeout=10,
            idle_sec=3600, own_bot_id=BOT, channel=CHANNEL,
            update=web.update, react=lambda *a: None,
            unreact=lambda *a: None)
        b.handle_event(root_ev("cb.1", "<@BTEST> slow"), "EvCB1")
        s = wait_running(b, "cb.1")
        self.assertIsNotNone(s, "prompt never started")
        end = time.monotonic() + 10
        while time.monotonic() < end and not any(
                "ctl_stop" in json.dumps(bk)
                for _, bk in web.blocks):
            time.sleep(0.05)
        self.assertTrue(any("ctl_stop" in json.dumps(bk)
                            for _, bk in web.blocks),
                        "status card lacks stop button")
        s.terminate()

    def test_status_card_claude_style_header(self):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=tempfile.mkdtemp(),
            acp_cmd=[sys.executable, FAKE], timeout=10,
            idle_sec=3600, own_bot_id=BOT, channel=CHANNEL,
            update=web.update, react=lambda *a: None,
            unreact=lambda *a: None)
        b.handle_event(root_ev("ch.1", "<@BTEST> go"), "EvCH1")
        self.assertTrue(web.wait_for("✅ 完成", 15))
        done = [t for _, t in web.updates if "✅ 完成" in t] \
            + [t for _, t in web.posts if "✅ 完成" in t]
        self.assertTrue(any("· 1 tools" in t or " tools" in t
                            for t in done),
                        f"done card lacks tool count: {done!r}")
        self.assertTrue(any("✅ 完成 · " in t for t in done),
                        f"done card lacks elapsed clock: {done!r}")
        s = b.sessions.get("ch.1")
        if s:
            s.terminate()

    def test_status_card_plan_board(self):
        b, web = make_bridge()
        s = sb.DevinSession("plb.1", tempfile.mkdtemp(),
                            [sys.executable, FAKE], 10)
        s.plan_entries = [
            {"content": "读代码", "status": "completed"},
            {"content": "改 bug", "status": "in_progress"},
            {"content": "跑测试", "status": "pending"}]
        s.turn_started = time.monotonic() - 3
        s.tool_count = 2
        out = b._status_text(s)
        self.assertIn("⏳ 处理中 · ", out)
        self.assertIn("· 2 tools", out)
        self.assertIn("✅ 读代码", out)
        self.assertIn("🔄 *改 bug*", out)
        self.assertIn("☐ 跑测试", out)
        s.terminate()

    def test_ctl_stop_button_stops_turn(self):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=tempfile.mkdtemp(),
            acp_cmd=[sys.executable, FAKE], timeout=10,
            idle_sec=3600, own_bot_id=BOT, channel=CHANNEL,
            update=web.update, react=lambda *a: None,
            unreact=lambda *a: None)
        b.handle_event(root_ev("cb.2", "<@BTEST> slow"), "EvCB2")
        s = wait_running(b, "cb.2")
        self.assertIsNotNone(s, "prompt never started")
        b.handle_interaction({
            "type": "block_actions",
            "message": {"ts": "m1", "thread_ts": "cb.2"},
            "actions": [{"action_id": "ctl_stop", "value": "stop"}],
            "user": {"id": "U1"}})
        self.assertTrue(web.wait_for("已停止", 15))

    def test_ctl_status_button_posts_status(self):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=tempfile.mkdtemp(),
            acp_cmd=[sys.executable, FAKE], timeout=10,
            idle_sec=3600, own_bot_id=BOT, channel=CHANNEL,
            update=web.update, react=lambda *a: None,
            unreact=lambda *a: None)
        b.handle_event(root_ev("cb.3"), "EvCB3")
        self.assertTrue(web.wait_for("任务结果：一切正常。", 30))
        b.handle_interaction({
            "type": "block_actions",
            "message": {"ts": "m1", "thread_ts": "cb.3"},
            "actions": [{"action_id": "ctl_status", "value": "status"}],
            "user": {"id": "U1"}})
        self.assertTrue(web.wait_for("状态: 等待中（空闲）", 15))
        b.sessions["cb.3"].terminate()

    def test_resume_ack_updated_in_place(self):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=tempfile.mkdtemp(),
            acp_cmd=[sys.executable, FAKE], timeout=10,
            idle_sec=3600, own_bot_id=BOT, channel=CHANNEL,
            update=web.update, react=lambda *a: None,
            unreact=lambda *a: None)
        wd = b._workdir("ru.1")
        os.makedirs(wd)
        sb.write_state(wd, "interrupted", "重启前任务",
                       extra={"session_id": "old-sess-9"})
        b.handle_event(reply_ev("ru.1", "ru.2", "继续"), "EvRU1")
        self.assertTrue(web.wait_for("已恢复旧会话", 15))
        self.assertTrue(web.wait_for("任务结果", 15))
        # the ack message was edited into the outcome, not reposted
        self.assertTrue(any("已恢复旧会话" in t
                            for _, t in web.updates))
        posted = [t for t in web.texts("ru.1")
                  if "已恢复旧会话" in t]
        self.assertEqual(posted, [])
        b.sessions["ru.1"].terminate()

    def test_blocked_sender_ignored(self):
        saved = sb.BLOCKED_SENDERS
        sb.BLOCKED_SENDERS = frozenset({"BDOT", "ADOT"})
        try:
            b, web = make_bridge()
            ev = root_ev("bd.1", "<@BTEST> 干活")
            ev["bot_id"] = "BDOT"
            b.handle_event(ev, "EvBD1")
            rep = reply_ev("bd.1", "bd.2", "继续")
            rep["app_id"] = "ADOT"
            b.handle_event(rep, "EvBD2")
            time.sleep(0.3)
            self.assertFalse(web.posts)
            self.assertNotIn("bd.1", b.sessions)
        finally:
            sb.BLOCKED_SENDERS = saved

    def test_goal_replay_skipped_when_cleared(self):
        b, web = make_bridge()
        b.handle_event(root_ev("gr.1"), "EvGR1")
        self.assertTrue(web.wait_for("任务结果：一切正常。"))
        s = b.sessions["gr.1"]
        with open(sb._goal_cleared_path(s.workdir), "w") as f:
            f.write("1")
        b._run_prompt("gr.1", "/goal resume", goal_replay=True)
        self.assertTrue(web.wait_for("已跳过这次自动重放"))
        self.assertFalse(os.path.exists(
            sb._goal_cleared_path(s.workdir)))
        s.terminate()

    def test_goal_file_during_turn_wake_rejected(self):
        b, web = make_bridge()
        b.handle_event(root_ev("gw.1"), "EvGW1")
        self.assertTrue(web.wait_for("任务结果：一切正常。"))
        s = b.sessions["gw.1"]
        s.last_turn_end = time.time()  # just ended -> agent-made file
        wd = b._workdir("gw.1")
        with open(os.path.join(wd, ".devin-goal-native"), "w") as f:
            f.write("自建续跑目标")
        self.assertTrue(web.wait_for("忽略疑似会话自建", 15))
        self.assertFalse(os.path.exists(
            os.path.join(wd, ".devin-goal-native")))
        self.assertFalse(s.goal)
        s.terminate()


class AcpAuthTest(unittest.TestCase):
    """ACP is the sole credential consumer: the host must call
    `authenticate` with _meta.api_key (NOT `meta`) before session/new.
    Regression: 3000.12.x rejects session/new outright when the host
    never authenticated — the production bridge spawned no sessions
    at all until this handshake existed."""

    def setUp(self):
        self._env = {k: os.environ.get(k) for k in
                     ("FAKE_AUTH_METHODS", "FAKE_REQUIRE_AUTH",
                      "DEVIN_ACP_API_KEY", "PCTX_MESSAGES_PUT")}
        os.environ["PCTX_MESSAGES_PUT"] = ""
        self._creds = sb.ACP_CREDENTIALS
        self._dstate = sb.ACP_DESKTOP_STATE
        sb.ACP_DESKTOP_STATE = "/nonexistent/state.vscdb"

    def tearDown(self):
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        sb.ACP_CREDENTIALS = self._creds
        sb.ACP_DESKTOP_STATE = self._dstate

    def test_api_key_env_beats_file(self):
        os.environ["DEVIN_ACP_API_KEY"] = "envkey"
        self.assertEqual(sb._acp_api_key(), "envkey")
        del os.environ["DEVIN_ACP_API_KEY"]
        with tempfile.NamedTemporaryFile(
                "w", suffix=".toml", delete=False) as f:
            f.write('windsurf_api_key = "filekey"\n')
        sb.ACP_CREDENTIALS = f.name
        self.assertEqual(sb._acp_api_key(), "filekey")
        sb.ACP_CREDENTIALS = "/nonexistent/creds.toml"
        self.assertEqual(sb._acp_api_key(), "")
        # desktop blob (Devin Desktop's live account) beats the file
        import sqlite3
        with tempfile.NamedTemporaryFile(
                suffix=".vscdb", delete=False) as f:
            vdb = f.name
        conn = sqlite3.connect(vdb)
        conn.execute("CREATE TABLE ItemTable (key TEXT, value TEXT)")
        conn.execute("INSERT INTO ItemTable VALUES "
                     "('windsurfAuthStatus', ?)",
                     ('{"apiKey":"devin-session-token$desktok"}',))
        conn.commit(); conn.close()
        sb.ACP_DESKTOP_STATE = vdb
        self.assertEqual(sb._acp_api_key(),
                         "devin-session-token$desktok")

    def test_authenticate_before_session_new(self):
        os.environ["FAKE_AUTH_METHODS"] = "1"
        os.environ["FAKE_REQUIRE_AUTH"] = "1"
        os.environ["DEVIN_ACP_API_KEY"] = "k"
        b, web = make_bridge()
        b.handle_event(root_ev("au.1"), "EvAU1")
        self.assertTrue(web.wait_for("任务结果：一切正常。"))
        calls = open(os.path.join(b._workdir("au.1"),
                                  ".acp_calls")).read()
        self.assertLess(calls.index("authenticate"),
                        calls.index("session/new"))
        b.sessions["au.1"].terminate()

    def test_missing_key_fails_with_auth_hint(self):
        os.environ["FAKE_AUTH_METHODS"] = "1"
        os.environ["FAKE_REQUIRE_AUTH"] = "1"
        os.environ.pop("DEVIN_ACP_API_KEY", None)
        sb.ACP_CREDENTIALS = "/nonexistent/creds.toml"
        b, web = make_bridge()
        b.handle_event(root_ev("au.2"), "EvAU2")
        self.assertTrue(web.wait_for("启动 devin 失败", 15))
        self.assertTrue(web.wait_for("未认证"))

    def test_crash_detail_lands_in_settled_card(self):
        """Failures fold their cause into the settled status card —
        one coherent update instead of a '❌ 出错' card plus an
        orphan text post users have to correlate."""
        b, web = make_bridge(mode="die", timeout=3)
        b.update_msg = web.update
        b.handle_event(root_ev("cr.1", "<@BTEST> go"), "EvCR1")
        end = time.monotonic() + 60
        while time.monotonic() < end and not any(
                "💥 进程退出" in t for _, t in web.posts):
            time.sleep(0.1)
        # the settled card carries head + detail together
        self.assertTrue(any(
            "💥 进程退出" in t and "会话已结束" in t
            for _, t in web.posts),
            "settled card lacks the crash detail")
        # and no second message repeats the verdict outside the card
        for _, t in web.posts:
            if "会话已结束" in t:
                self.assertIn("💥 进程退出", t,
                              f"orphan error post: {t!r}")


class InteropTest(unittest.TestCase):
    """Cross-device interop: !sessions lists the global devin store,
    !adopt binds a foreign session to a thread via session/load,
    !handoff surfaces the id for `devin -r`, !release unbinds without
    wiping context, and the pinned board renders a stable overview."""

    def setUp(self):
        import sqlite3
        self._old_db, self._old_locks = sb.DEVIN_DB, sb.DEVIN_LOCKS
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, "sessions.db")
        self.lock_dir = os.path.join(self.tmp, "session_locks")
        os.makedirs(self.lock_dir)
        self._pctx = os.environ.get("PCTX_MESSAGES_PUT")
        os.environ["PCTX_MESSAGES_PUT"] = ""
        self._dmseed = os.environ.pop("BOARD_DM_USERS", None)
        # real host vitals must not spawn cleanup sessions mid-test
        self._ac = sb.AUTOCLEAN
        sb.AUTOCLEAN = False
        self._aud = sb.AUDIT_ENABLED
        sb.AUDIT_ENABLED = False
        db = sqlite3.connect(self.db_path)
        db.execute("CREATE TABLE sessions(id TEXT PRIMARY KEY,"
                   " working_directory TEXT, backend_type TEXT,"
                   " model TEXT, agent_mode TEXT, created_at REAL,"
                   " last_activity_at REAL, title TEXT,"
                   " main_chain_id TEXT, shell_last_seen_index INT,"
                   " cogs_json TEXT, workspace_dirs TEXT,"
                   " hidden INT, metadata TEXT)")
        db.execute("CREATE TABLE message_nodes(node_id INTEGER"
                   " PRIMARY KEY, session_id TEXT, chat_message TEXT,"
                   " created_at REAL)")
        proj_a = os.path.join(self.tmp, "proj-a")
        proj_b = os.path.join(self.tmp, "proj-b")
        proj_c = os.path.join(self.tmp, "proj-c")
        for p in (proj_a, proj_b, proj_c):
            os.makedirs(p)
        for sid, wd, title, act, hid in (
                ("desk-aa", proj_a, "桌面会话A", time.time()-600, 0),
                ("cli-bb", proj_b, "CLI会话B", time.time()-3600, 0),
                ("bad-id", proj_c, "坏会话", time.time()-7200, 0)):
            db.execute("INSERT INTO sessions(id,working_directory,"
                       "title,last_activity_at,hidden) "
                       "VALUES(?,?,?,?,?)", (sid, wd, title, act, hid))
        db.commit(); db.close()
        sb.DEVIN_DB = self.db_path
        sb.DEVIN_LOCKS = self.lock_dir

    def tearDown(self):
        sb.DEVIN_DB, sb.DEVIN_LOCKS = self._old_db, self._old_locks
        if self._pctx is None:
            os.environ.pop("PCTX_MESSAGES_PUT", None)
        else:
            os.environ["PCTX_MESSAGES_PUT"] = self._pctx
        if self._dmseed is not None:
            os.environ["BOARD_DM_USERS"] = self._dmseed
        sb.AUTOCLEAN = self._ac
        sb.AUDIT_ENABLED = self._aud

    def make_pins_bridge(self, **kw):
        pinned = []
        b, web = make_bridge(**kw)
        b.pin_add = pinned.append
        b.update_msg = web.update
        return b, web, pinned

    def test_sessions_list(self):
        b, web = make_bridge()
        b.handle_event(reply_ev("sl.1", "sl.2", "!sessions"), "EvSL")
        self.assertTrue(web.wait_for("desk-aa", 10))
        t = next(t for _, t in web.posts if "desk-aa" in t)
        self.assertIn("cli-bb", t)
        self.assertIn("!adopt", t)

    def test_adopt_loads_and_continues(self):
        b, web = make_bridge()
        b.handle_event(reply_ev("ad.1", "ad.2", "!adopt desk-aa"),
                       "EvAD")
        self.assertTrue(web.wait_for("已接管", 15))
        s = b.sessions["ad.1"]
        self.assertEqual(s.session_id, "desk-aa")
        self.assertTrue(s.acp_cwd.endswith("proj-a"))
        calls = open(os.path.join(s.acp_cwd, ".acp_calls")).read()
        self.assertIn("load:desk-aa", calls)
        # thread reply now prompts the adopted session
        b.handle_event(reply_ev("ad.1", "ad.3", "ping"), "EvAD3")
        self.assertTrue(web.wait_for("✅ 完成", 15))
        s.terminate()

    def test_adopt_bare_picks_newest_unbound(self):
        b, web = make_bridge()
        b.handle_event(reply_ev("an.1", "an.2", "!adopt"), "EvAN")
        # desk-aa is the newest row in the fake store
        self.assertTrue(web.wait_for("已接管 `desk-aa`", 15))
        b.sessions["an.1"].terminate()

    def test_adopt_unknown_id(self):
        b, web = make_bridge()
        b.handle_event(reply_ev("au.1", "au.2", "!adopt no-such"),
                       "EvAU")
        self.assertTrue(web.wait_for("找不到会话", 10))
        self.assertNotIn("au.1", b.sessions)

    def test_adopt_bad_id_falls_back(self):
        b, web = make_bridge()
        b.handle_event(reply_ev("ab.1", "ab.2", "!adopt bad-id"),
                       "EvAB")
        self.assertTrue(web.wait_for("接管失败", 15))
        self.assertNotIn("ab.1", b.sessions)

    def test_adopt_locked_steals_lock(self):
        # a live lock file means another frontend holds it; adopt must
        # remove it so acp's session/load isn't refused outright
        with open(os.path.join(self.lock_dir, "cli-bb.lock"), "w") as f:
            f.write(str(os.getpid()))
        b, web = make_bridge()
        b.handle_event(reply_ev("al.1", "al.2", "!adopt cli-bb"), "EvAL")
        self.assertTrue(web.wait_for("已接管", 15))
        s = b.sessions["al.1"]
        self.assertEqual(s.session_id, "cli-bb")
        self.assertFalse(os.path.exists(
            os.path.join(self.lock_dir, "cli-bb.lock")))
        self.assertTrue(web.wait_for("抢过来", 10))
        s.terminate()

    def _wait_session(self, b, ts, timeout=10):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if ts in b.sessions:
                return b.sessions[ts]
            time.sleep(0.05)

    def test_handoff_and_release(self):
        b, web = make_bridge()
        b.handle_event(root_ev("hr.1", "<@BTEST> ping"), "EvHR")
        self.assertTrue(self._wait_session(b, "hr.1"))
        web.wait_for("✅ 完成", 15)
        sid = b.sessions["hr.1"].session_id
        b.handle_event(reply_ev("hr.1", "hr.2", "!handoff"), "EvHR2")
        self.assertTrue(web.wait_for(f"devin -r {sid}", 10))
        b.handle_event(reply_ev("hr.1", "hr.3", "!release"), "EvHR3")
        self.assertTrue(web.wait_for(f"已释放 `{sid}`", 10))
        self.assertNotIn("hr.1", b.sessions)
        st = sb.read_state(b._workdir("hr.1"))
        self.assertEqual(st["session_id"], sid)

    def test_board_posts_once_and_pins(self):
        b, web, pinned = self.make_pins_bridge()
        b._sys_vitals = lambda: {}  # freeze: live rates would churn text
        b.handle_event(root_ev("bd.1", "<@BTEST> ping"), "EvBD")
        self.assertTrue(self._wait_session(b, "bd.1"))
        web.wait_for("✅ 完成", 15)
        b._board_refresh(manual=True)
        board = [t for t_, t in web.posts
                 if "会话看板" in t]
        self.assertTrue(board)
        top = [t_ for t_, t in web.posts if t_ is None]
        self.assertEqual(len(top), 1)
        self.assertEqual(pinned, [b._board_saved_ts()])
        # refresh with unchanged text: no repost, no update call
        n_posts = len(web.posts)
        n_upd = len(web.updates)
        b._board_refresh(manual=True)
        self.assertEqual(len([t_ for t_, t in web.posts if t_ is None]),
                         1)
        self.assertEqual(len(web.updates), n_upd)
        # drifted content still updates in place, never a second post
        b._board_text = "stale"
        b._board_refresh(manual=True)
        self.assertEqual(len([t_ for t_, t in web.posts if t_ is None]),
                         1)
        self.assertTrue(any(u == b._board_saved_ts()
                            for u, _ in web.updates))
        b.sessions["bd.1"].terminate()

    def test_bang_sessions_at_top_level(self):
        """`!sessions` typed in the channel (mobile flow) must answer —
        replies thread under the command message itself."""
        b, web = make_bridge()
        b.handle_event(root_ev("tl.1", "!sessions"), "EvTLS")
        self.assertTrue(web.wait_for("desk-aa", 10))
        t_ = next(t_ for t_, t in web.posts if "desk-aa" in t)
        self.assertEqual(t_, "tl.1")

    def test_bang_adopt_at_top_level_binds_anchor(self):
        """`!adopt <id>` at top level binds the session to a thread
        anchored on the command message — mobile takeover flow."""
        b, web = make_bridge()
        b.handle_event(root_ev("ta.1", "!adopt desk-aa"), "EvTA")
        self.assertTrue(web.wait_for("已接管", 15))
        s = b.sessions["ta.1"]
        self.assertEqual(s.session_id, "desk-aa")
        b.handle_event(reply_ev("ta.1", "ta.2", "hi"), "EvTA2")
        self.assertTrue(web.wait_for("✅ 完成", 15))
        s.terminate()

    def test_board_shows_real_activity_not_just_lock(self):
        """🔥 = transcript nodes are fresh (a turn is writing), 📂 =
        merely open in a frontend (lock held, nothing running), 💤 =
        idle. A stale-but-locked session must NOT look 'active'."""
        import sqlite3
        db = sqlite3.connect(self.db_path)
        # desk-aa: fresh node = actually working right now
        db.execute("INSERT INTO message_nodes(session_id,chat_message,"
                   "created_at) VALUES('desk-aa','{}',?)",
                   (time.time(),))
        # cli-bb: last node 3h ago but a live lock file — open, idle
        db.execute("INSERT INTO message_nodes(session_id,chat_message,"
                   "created_at) VALUES('cli-bb','{}',?)",
                   (time.time() - 10800,))
        db.commit(); db.close()
        with open(os.path.join(self.lock_dir, "cli-bb.lock"), "w") as f:
            f.write(str(os.getpid()))
        b, web, pinned = self.make_pins_bridge()
        text, blocks = b._board_render()
        aa = next(l for l in text.splitlines() if "desk-aa" in l)
        bb = next(l for l in text.splitlines() if "cli-bb" in l)
        bad = next(l for l in text.splitlines() if "bad-id" in l)
        self.assertIn("🔥", aa)
        self.assertIn("📂", bb)
        self.assertIn("💤", bad)
        # dropdown offers adoptable sessions without typing ids
        acts = [b_ for b_ in blocks if b_.get("type") == "actions"]
        self.assertTrue(acts)
        els = [e for b_ in acts for e in b_.get("elements", [])]
        opts = next(e for e in els
                    if e.get("type") == "static_select")["options"]
        self.assertTrue(any(o["value"] == "cli-bb" for o in opts))
        # hot/locked rows get one-tap 看看 + 接管 buttons in their row
        self.assertTrue(any(e.get("action_id") == "board_adopt"
                            and e.get("value") == "desk-aa"
                            for e in els))
        self.assertTrue(any(e.get("action_id") == "board_peek"
                            and e.get("value") == "desk-aa"
                            for e in els))

    def test_board_adopt_button_spawns_thread(self):
        """One tap on the board's 接管 must bind the session into a
        fresh thread — no command typing on mobile."""
        b, web, pinned = self.make_pins_bridge()
        b.handle_interaction({"type": "block_actions",
                              "message": {"ts": "bd.x"},
                              "user": {"id": "U1"},
                              "actions": [{"action_id": "board_adopt",
                                           "value": "desk-aa"}]})
        self.assertTrue(web.wait_for("已接管", 15))
        i = next(i for i, (t_, t) in enumerate(web.posts)
                 if t_ is None and "接管 `desk-aa`" in t)
        s = b.sessions[f"m{i + 1}"]
        self.assertEqual(s.session_id, "desk-aa")
        s.terminate()

    def test_board_pick_select_adopts(self):
        b, web, pinned = self.make_pins_bridge()
        b.handle_interaction({"type": "block_actions",
                              "message": {"ts": "bd.y"},
                              "user": {"id": "U1"},
                              "actions": [{
                                  "action_id": "board_pick",
                                  "selected_option":
                                      {"value": "cli-bb"}}]})
        self.assertTrue(web.wait_for("已接管", 15))
        s = next(s for s in b.sessions.values()
                 if getattr(s, "session_id", None) == "cli-bb")
        s.terminate()

    def test_board_touch_throttled(self):
        b, web, pinned = self.make_pins_bridge()
        b._board_refresh(manual=True)
        n = len(web.posts)
        # touch inside the gap: no immediate second refresh
        sb.BOARD_GAP = 3600
        try:
            b._board_touch()
            time.sleep(0.3)
        finally:
            sb.BOARD_GAP = 30
        self.assertEqual(len(web.posts), n)

    def _seed_nodes(self, sid, msgs):
        import sqlite3
        db = sqlite3.connect(self.db_path)
        base = (db.execute("SELECT COALESCE(MAX(node_id),0) "
                           "FROM message_nodes").fetchone()[0])
        for i, (cm, ts) in enumerate(msgs):
            db.execute("INSERT INTO message_nodes(node_id,session_id,"
                       "chat_message,created_at) VALUES(?,?,?,?)",
                       (base + i + 1, sid, json.dumps(cm), ts))
        db.commit(); db.close()

    def test_board_hot_row_shows_what_its_doing(self):
        """A 🔥 row must answer '在干嘛', not repeat a useless '刚' —
        render the newest transcript action instead of an age."""
        now = time.time()
        self._seed_nodes("desk-aa", [
            ({"role": "assistant",
              "tool_calls": [{"name": "exec", "arguments":
                              {"command": "docker ps -a"}}]}, now)])
        b, web, pinned = self.make_pins_bridge()
        text, blocks = b._board_render()
        aa = next(l for l in text.splitlines() if "desk-aa" in l)
        self.assertIn("🔥", aa)
        self.assertIn("跑 exec: docker ps -a", aa)
        self.assertNotIn("刚", aa)
        sec = next(b_ for b_ in blocks if b_.get("type") == "section"
                   and "desk-aa" in b_["text"]["text"])
        self.assertIn("跑 exec: docker ps -a", sec["text"]["text"])

    def test_long_running_turn_stays_hot(self):
        """A turn mid-flight writes nothing while a long command runs;
        🔥 must not flicker off at the 2-min freshness edge. The real
        signal is turn position: newest real node is a tool result /
        user msg / tool call → still in flight."""
        now = time.time()
        # desk-aa: tool result landed 5 min ago, long op still running
        self._seed_nodes("desk-aa", [
            ({"role": "tool", "content": "Output from command: build"},
             now - 300)])
        # cli-bb: turn ENDED — final assistant answer 10 min ago
        self._seed_nodes("cli-bb", [
            ({"role": "assistant", "content": "全部完成了"},
             now - 600)])
        b, web, pinned = self.make_pins_bridge()
        text, blocks = b._board_render()
        aa = next(l for l in text.splitlines() if "desk-aa" in l)
        bb = next(l for l in text.splitlines() if "cli-bb" in l)
        self.assertIn("🔥", aa)
        self.assertIn("结果:", aa)
        self.assertNotIn("🔥", bb)
        self.assertIn("💤", bb)

    def test_system_injection_is_not_activity(self):
        """`system` context nodes are appended when a frontend opens
        a session — not work. A session whose tail is only system
        injections must NOT render 🔥 even if the injection is fresh."""
        now = time.time()
        self._seed_nodes("desk-aa", [
            ({"role": "assistant", "content": "done, all good"},
             now - 900),
            ({"role": "system", "content": "<system_info>ctx"},
             now - 5)])
        b, web, pinned = self.make_pins_bridge()
        text, blocks = b._board_render()
        aa = next(l for l in text.splitlines() if "desk-aa" in l)
        self.assertNotIn("🔥", aa)

    def test_board_peek_ephemeral_to_clicker_only(self):
        """看看 = spectate without stealing: an ephemeral answer to
        the clicker, no channel spam, no lock touched."""
        now = time.time()
        self._seed_nodes("desk-aa", [
            ({"role": "user", "content": "看下 yeeorder 部署"}, now - 30),
            ({"role": "assistant",
              "tool_calls": [{"name": "exec", "arguments":
                              {"command": "docker ps"}}]}, now)])
        eph = []
        b, web, pinned = self.make_pins_bridge()
        b.ephemeral_msg = lambda **kw: eph.append(kw)
        b.handle_interaction({"type": "block_actions",
                              "message": {"ts": "bd.z"},
                              "user": {"id": "U9"},
                              "actions": [{"action_id": "board_peek",
                                           "value": "desk-aa"}]})
        self.assertEqual(len(eph), 1)
        self.assertEqual(eph[0]["user"], "U9")
        self.assertIn("docker ps", eph[0]["text"])
        self.assertIn("看下 yeeorder 部署", eph[0]["text"])
        self.assertFalse(any("docker ps" in t for _, t in web.posts))

    def test_peek_bang_spectates(self):
        """`!peek <id>` posts the recent-actions view in-thread."""
        self._seed_nodes("desk-aa", [
            ({"role": "assistant", "content": "迁移跑完了，0 错"},
             time.time())])
        b, web = make_bridge()
        b.handle_event(reply_ev("pk.1", "pk.2", "!peek desk-aa"), "EvPK")
        self.assertTrue(web.wait_for("回: 迁移跑完了", 10))

    def test_peek_bare_uses_thread_session(self):
        """Bare `!peek` spectates the session bound to this thread."""
        self._seed_nodes("desk-aa", [
            ({"role": "user", "content": "hello desk"}, time.time())])
        b, web = make_bridge()
        b.handle_event(reply_ev("pb.1", "pb.2", "!adopt desk-aa"),
                       "EvPB")
        self.assertTrue(web.wait_for("已接管", 15))
        b.handle_event(reply_ev("pb.1", "pb.3", "!peek"), "EvPB3")
        self.assertTrue(web.wait_for("收到: hello desk", 10))
        b.sessions["pb.1"].terminate()

    def _dm_bridge(self, **kw):
        b, web = make_bridge(**kw)
        b.dm_post = web.dm_post
        b.dm_update = web.dm_update
        return b, web

    def test_dm_board_posted_for_known_user(self):
        """A real user's first message earns them a DM mirror of the
        board — the mobile-reachable surface."""
        b, web = self._dm_bridge()
        b.handle_event(reply_ev("dm.1", "dm.2", "!sessions",
                                user="U9"), "EvDM")
        posts = [p for p in web.dm_posts if p[0] == "U9"]
        self.assertEqual(len(posts), 1)
        self.assertIn("会话看板", posts[0][1])
        self.assertEqual(b._dm_boards["U9"]["channel"], "DU9")

    def test_dm_board_updates_in_place(self):
        b, web = self._dm_bridge()
        b.handle_event(reply_ev("dm.1", "dm.2", "!sessions",
                                user="U9"), "EvDM")
        b._board_refresh(manual=True)
        self.assertTrue(web.dm_updates)
        chan, ts, text = web.dm_updates[-1]
        self.assertEqual(chan, "DU9")
        self.assertIn("会话看板", text)
        self.assertEqual(len(web.dm_posts), 1)  # no repost

    def test_dm_board_reposts_when_update_fails(self):
        b, web = self._dm_bridge()
        b.handle_event(reply_ev("dm.1", "dm.2", "!sessions",
                                user="U9"), "EvDM")
        web.dm_update_fail = True
        b._board_refresh(manual=True)
        self.assertEqual(len(web.dm_posts), 2)  # dead entry reposted

    def test_dm_seen_ignores_bot_and_dupes(self):
        b, web = self._dm_bridge()
        b.handle_event(root_ev("dm.1", "<@BTEST> ping"), "EvDM0")
        b.handle_event(reply_ev("dm.1", "dm.2", "a", user="U9"), "EvDM1")
        b.handle_event(reply_ev("dm.1", "dm.3", "b", user="U9"), "EvDM2")
        b.handle_event(reply_ev("dm.1", "dm.4", "c",
                                user="USLACKBOT"), "EvDM3")
        self.assertEqual(len(web.dm_posts), 1)  # once, for U9 only
        self.assertNotIn("USLACKBOT", b._dm_users)
        s = self._wait_session(b, "dm.1")
        if s:
            s.terminate()

    def test_dm_state_persists_across_restart(self):
        td = tempfile.mkdtemp()
        b, web = self._dm_bridge(tasks_dir=td)
        b.handle_event(reply_ev("dm.1", "dm.2", "!sessions",
                                user="U9"), "EvDM")
        b2, _ = self._dm_bridge(tasks_dir=td)
        self.assertIn("U9", b2._dm_users)
        self.assertEqual(b2._dm_boards["U9"]["channel"], "DU9")

    def test_canvas_mirror_created_then_edited(self):
        """First refresh creates the channel canvas (id persisted);
        later refreshes edit it in place with link-friendly markdown."""
        made, edits = [], []

        def mk(title, md):
            made.append(md)
            return "FC1"

        b, web = make_bridge(
            canvas_create=mk,
            canvas_edit=lambda cid, md: edits.append((cid, md)))
        b._board_render = lambda: (
            "🔥 *t* — `s` <https://x.y/p1|进去聊>", [])
        b._board_refresh(manual=True)
        self.assertEqual(len(made), 1)
        self.assertIn("[进去聊](https://x.y/p1)", made[0])  # mrkdwn→md
        b._board_text = "stale"
        b._board_refresh(manual=True)
        self.assertEqual(len(made), 1)  # never a second canvas
        self.assertEqual(edits[-1][0], "FC1")
        self.assertIn("[进去聊](https://x.y/p1)", edits[-1][1])

    def test_board_shows_host_vitals(self):
        b, web = make_bridge()
        text, _ = b._board_render()
        self.assertIn("🖥", text)
        self.assertIn("内存", text)
        self.assertIn("磁盘", text)

    def test_autoclean_spawns_after_strikes(self):
        """Sustained over-threshold → one 🧹 cleanup session, real
        spawn through the auto path."""
        old_strikes = sb.CLEAN_STRIKES
        sb.CLEAN_STRIKES = 3
        sb.AUTOCLEAN = True
        try:
            b, web = make_bridge()
            for _ in range(2):
                b._autoclean({"disk": 99})
            self.assertFalse(web.wait_for("自动清理", 1.5))
            b._autoclean({"disk": 99})  # third strike
            self.assertTrue(web.wait_for("🧹", 10))
            anchor = next(t_ for t_, t in web.posts
                          if "自动清理" in t)
            self.assertIsNone(anchor)  # channel-level post, no thread
            # the spawned session binds under the anchor's message ts
            deadline = time.monotonic() + 10
            while not b.sessions and time.monotonic() < deadline:
                time.sleep(0.05)
            s = next(iter(b.sessions.values()), None)
            self.assertIsNotNone(s)
            if s:
                s.terminate()
        finally:
            sb.CLEAN_STRIKES = old_strikes
            sb.AUTOCLEAN = False

    def test_autoclean_cooldown_and_reset(self):
        old = (sb.CLEAN_STRIKES, sb.CLEAN_COOLDOWN)
        sb.CLEAN_STRIKES, sb.CLEAN_COOLDOWN = 2, 60
        sb.AUTOCLEAN = True
        try:
            b, web = make_bridge()
            b._autoclean({"mem": 99})
            b._autoclean({"mem": 99})  # fires
            n = len([t for _, t in web.posts if "自动清理" in t])
            b._autoclean({"mem": 99})  # strike 1 again
            b._autoclean({"mem": 99})  # would fire but cooldown blocks
            self.assertEqual(
                len([t for _, t in web.posts if "自动清理" in t]), n)
            # recovery resets the streak
            b._autoclean({"mem": 50})
            self.assertEqual(b._clean["mem"][0], 0)
            s = next(iter(b.sessions.values()), None)
            if s:
                s.terminate()
        finally:
            sb.CLEAN_STRIKES, sb.CLEAN_COOLDOWN = old
            sb.AUTOCLEAN = False

    def test_autoclean_respects_auto_slot(self):
        old = sb.CLEAN_STRIKES
        sb.CLEAN_STRIKES = 1
        sb.AUTOCLEAN = True
        try:
            b, web = make_bridge()
            b.starting_auto.add("occupied")
            b._autoclean({"cpu": 99})
            self.assertFalse(web.wait_for("自动清理", 1.5))
        finally:
            sb.CLEAN_STRIKES = old
            sb.AUTOCLEAN = False

    def test_audit_spawns_when_due_then_waits(self):
        sb.AUDIT_ENABLED = True
        try:
            td = tempfile.mkdtemp()
            b, web = make_bridge(tasks_dir=td)
            b._audit_check()  # no audit_at on record → due
            self.assertTrue(web.wait_for("🩺", 10))
            d = json.load(open(os.path.join(td,
                                           ".bridge_board.json")))
            self.assertIn("audit_at", d)
            n = len([t for _, t in web.posts if "体检" in t])
            b._audit_check()  # just ran → not due again
            self.assertEqual(
                len([t for _, t in web.posts if "体检" in t]), n)
            deadline = time.monotonic() + 10
            while not b.sessions and time.monotonic() < deadline:
                time.sleep(0.05)
            s = next(iter(b.sessions.values()), None)
            if s:
                s.terminate()
        finally:
            sb.AUDIT_ENABLED = False

    def test_audit_waits_for_auto_slot(self):
        sb.AUDIT_ENABLED = True
        try:
            td = tempfile.mkdtemp()
            b, web = make_bridge(tasks_dir=td)
            b.starting_auto.add("occupied")
            b._audit_check()
            self.assertFalse(web.wait_for("体检", 1.5))
            # slot busy → not marked done; fires once slot frees
            bp = os.path.join(td, ".bridge_board.json")
            d = json.load(open(bp)) if os.path.exists(bp) else {}
            self.assertNotIn("audit_at", d)
        finally:
            sb.AUDIT_ENABLED = False


class SendablesTest(unittest.TestCase):
    """_collect_sendables: the reply rarely carries absolute paths —
    deliverables must also be caught via tool-call locations and
    basename mentions, not just media files in the workdir."""

    def _sess(self, wd, touched):
        import types
        return types.SimpleNamespace(workdir=wd, touched=touched)

    def test_deliverable_types_and_basename_mentions(self):
        wd = tempfile.mkdtemp()
        since = time.time() - 60
        now = time.time()
        files = {}
        for name in ("report.md", "export.xlsx", "chart.png",
                     "main.py", "old.md"):
            p = os.path.join(wd, name)
            with open(p, "w") as f:
                f.write("data")
            files[name] = p
        os.utime(files["old.md"], (now - 7200, now - 7200))  # stale
        # main.py was tool-touched this turn but is code — excluded
        # unless explicitly named
        s = self._sess(wd, {files["main.py"]: True,
                            files["export.xlsx"]: True})
        b, _ = make_bridge()
        # reply names report.md by basename only
        got, pending, _ = b._collect_sendables(s, "已生成 report.md",
                                               since)
        names = {os.path.basename(p) for p in got}
        self.assertIn("report.md", names)    # basename mention
        self.assertIn("export.xlsx", names)  # fresh deliverable
        self.assertIn("chart.png", names)    # fresh media in workdir
        self.assertNotIn("main.py", names)   # code edit → not spam
        self.assertNotIn("old.md", names)    # stale file → not sent

    def test_code_file_named_in_reply_is_sent(self):
        """Naming a file outright = intent: '改动在 main.py' uploads it
        even though code files aren't auto-collected."""
        wd = tempfile.mkdtemp()
        since = time.time() - 60
        p = os.path.join(wd, "main.py")
        with open(p, "w") as f:
            f.write("print(1)")
        s = self._sess(wd, {p: True})
        b, _ = make_bridge()
        got, _, _ = b._collect_sendables(s, "改动都在 main.py 里",
                                         since)
        self.assertIn(os.path.realpath(p),
                      {os.path.realpath(x) for x in got})

    def test_touched_alone_never_ships(self):
        """Reading/editing a working file is not a deliverable: a
        touched file that isn't named in the reply and isn't a fresh
        deliverable under the workdir stays out of the upload set —
        the old rule spammed README.md/GOAL.md edits into threads."""
        wd = tempfile.mkdtemp()
        since = time.time() - 60
        outside = os.path.join(tempfile.mkdtemp(), "notes.md")
        with open(outside, "w") as f:
            f.write("x")
        s = self._sess(wd, {outside: True})
        b, _ = make_bridge()
        got, pending, _ = b._collect_sendables(s, "看了一下", since)
        self.assertEqual(got, [])
        self.assertEqual(pending, [])

    def test_internal_and_workfiles_not_auto_sent(self):
        """.bridge_state.json / memory.md / README.md must never
        auto-upload even when fresh — they're bridge/agent internals
        or project working files."""
        wd = tempfile.mkdtemp()
        since = time.time() - 60
        for n in (".bridge_state.json", "memory.md", "README.md",
                  "GOAL.md", ".devin-goal"):
            with open(os.path.join(wd, n), "w") as f:
                f.write("x")
        s = self._sess(wd, {})
        b, _ = make_bridge()
        got, pending, _ = b._collect_sendables(s, "done", since)
        self.assertEqual(got + pending, [])
        # …but naming one outright still ships it (intent beats rules)
        got, _, _ = b._collect_sendables(s, "看 README.md", since)
        self.assertIn("README.md", {os.path.basename(p)
                                    for p in got})

    def test_auto_send_dedupes_and_caps(self):
        """Same path never auto-sends twice (shelf-radar triple-send
        regression), and a big batch caps at AUTO_SEND_MAX with the
        rest pooling into pending for `!files`."""
        wd = tempfile.mkdtemp()
        since = time.time() - 60
        paths = []
        for i in range(6):
            p = os.path.join(wd, f"demo-{i}.png")
            with open(p, "w") as f:
                f.write("x")
            paths.append(p)
        s = self._sess(wd, {})
        s.sent_files = {}
        b, _ = make_bridge()
        send1, pend1, mt1 = b._collect_sendables(s, "ok", since)
        s.sent_files.update(mt1)
        s.pending_files = list(pend1)
        self.assertEqual(len(send1), sb.AUTO_SEND_MAX)
        self.assertEqual(len(pend1), 6 - sb.AUTO_SEND_MAX)
        # turn 2: sent + pooled files → nothing resends
        send2, pend2, _ = b._collect_sendables(s, "again", since)
        self.assertEqual(send2, [])
        self.assertEqual(pend2, [])

    def test_named_resend_only_when_changed(self):
        wd = tempfile.mkdtemp()
        since = time.time() - 60
        p = os.path.join(wd, "r.md")
        with open(p, "w") as f:
            f.write("a")
        s = self._sess(wd, {})
        s.sent_files = {}
        b, _ = make_bridge()
        send, _, mt = b._collect_sendables(s, "改在 r.md", since)
        s.sent_files.update(mt)
        send2, _, _ = b._collect_sendables(s, "还是 r.md", since)
        self.assertEqual(send2, [])
        time.sleep(0.02)
        with open(p, "w") as f:
            f.write("bb")
        send3, _, _ = b._collect_sendables(s, "最新 r.md", since)
        self.assertEqual([os.path.basename(x) for x in send3],
                         ["r.md"])



    def test_files_and_send_commands(self):
        """!files lists deliverables with state marks; !send pulls by
        name; !send all drains the pending pool."""
        import types
        wd = tempfile.mkdtemp()
        for n in ("a.png", "b.png", "c.png"):
            with open(os.path.join(wd, n), "w") as f:
                f.write("x")
        ups = []
        b, web = make_bridge(
            upload=lambda ts, p: ups.append((ts, p)))
        pend = os.path.join(wd, "a.png")
        s = types.SimpleNamespace(
            workdir=wd, touched={},
            sent_files={}, pending_files=[pend])
        b.sessions["f.1"] = s
        b._files_list("f.1")
        self.assertTrue(any("a.png" in t and "（未发）" in t
                            for _, t in web.posts))
        self.assertTrue(any("b.png" in t for _, t in web.posts))
        b._send_named("f.1", "a.png")
        self.assertEqual([os.path.basename(p) for _, p in ups],
                         ["a.png"])
        self.assertEqual(s.pending_files, [])
        s.pending_files = [os.path.join(wd, "b.png"),
                           os.path.join(wd, "c.png")]
        b._send_named("f.1", "all")
        self.assertEqual({os.path.basename(p) for _, p in ups},
                         {"a.png", "b.png", "c.png"})
        self.assertEqual(s.pending_files, [])

    def test_board_ephemeral_to_requester(self):
        """!board pops a private snapshot for the requester instead of
        making them scroll to the pin."""
        shots = []
        b, web = make_bridge(
            ephemeral=lambda **kw: shots.append(kw))
        b._board_render = lambda: ("BOARDTEXT", [])
        b._board_refresh = lambda **kw: None
        b._bang("rt.1", "board", "", {"user": "U42"})
        self.assertEqual(len(shots), 1)
        self.assertEqual(shots[0]["user"], "U42")
        self.assertEqual(shots[0]["text"], "BOARDTEXT")
        self.assertEqual(shots[0]["channel"], CHANNEL)


class WireRobustnessTest(unittest.TestCase):
    """Agent→host requests we do not implement (elicitation/create,
    fs/*, terminal/*) must get a protocol-level refusal, not silence —
    an unanswered request stalls the whole turn."""

    def setUp(self):
        self._pctx = os.environ.get("PCTX_MESSAGES_PUT")
        os.environ["PCTX_MESSAGES_PUT"] = ""

    def tearDown(self):
        if self._pctx is None:
            os.environ.pop("PCTX_MESSAGES_PUT", None)
        else:
            os.environ["PCTX_MESSAGES_PUT"] = self._pctx

    def test_unknown_host_request_answered_not_dropped(self):
        b, web = make_bridge()
        b.handle_event(root_ev("wr.1", "<@BTEST> askhost"), "EvWR1")
        self.assertTrue(web.wait_for("host 已答复。", 15))
        self.assertTrue(web.wait_for("✅ 完成", 15))
        calls = open(os.path.join(b._workdir("wr.1"),
                                  ".acp_calls")).read()
        self.assertIn("error:7777:-32601", calls)
        b.sessions["wr.1"].terminate()

    def test_touched_files_listed_in_settled_card(self):
        b, web = make_bridge()
        b.handle_event(root_ev("tf.1", "<@BTEST> edit me"), "EvTF1")
        self.assertTrue(web.wait_for("✅ 完成", 15))
        card = next(t for _, t in web.posts if "✅ 完成" in t)
        self.assertIn("📁", card)
        self.assertIn("draft.md", card)
        self.assertIn("/etc/abs.cfg", card)
        b.sessions["tf.1"].terminate()


def _exec_helper_phase2(meta_path):
    meta = json.load(open(meta_path))
    s = sb.DevinSession.reattach(
        meta["ts"], meta["workdir"], 20, meta["pid"],
        meta["stdin_fd"], meta["stdout_fd"])
    s.session_id = meta["session_id"]
    s.next_id = meta["req_id"]
    flag = threading.Event()
    threading.Timer(2, flag.set).start()
    reply, reason = s.prompt("trickle", lambda a: None, flag,
                             inflight_id=meta["req_id"])
    assert reason in ("end_turn", "cancelled"), f"reason={reason}"
    assert s.chunks, "no streamed chunks after exec"
    assert reply.strip(), "empty reply after exec"
    reply2, reason2 = s.prompt("ping", lambda a: None,
                               threading.Event())
    assert "任务结果" in reply2, reply2
    s.terminate()
    print("EXEC_RESTORE_PASS", flush=True)


class I18nTest(unittest.TestCase):
    """BRIDGE_LANG selects UI strings; public-safe feature defaults."""

    def _fresh(self, env_extra, expr):
        """Import session_bridge in a clean subprocess with env."""
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("BRIDGE_LANG", "AUTOCLEAN",
                                    "AUDIT_ENABLED", "BRIDGE_PAD"))}
        env.update(env_extra)
        env["PYTHONPATH"] = os.path.dirname(os.path.abspath(__file__))
        r = subprocess.run(
            [sys.executable, "-c",
             "import session_bridge as sb\n" + expr],
            capture_output=True, text=True, env=env, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr[-1500:])
        return r.stdout

    def test_default_lang_is_english(self):
        out = self._fresh({}, "print('桥命令' not in sb.HELP_CARD, "
                            "'Bridge commands' in sb.HELP_CARD)")
        self.assertIn("True True", out)

    def test_zh_lang(self):
        out = self._fresh({"BRIDGE_LANG": "zh"},
                          "print('桥命令' in sb.HELP_CARD, "
                          "'桥会话上下文' in sb.AGENTS_TMPL)")
        self.assertIn("True True", out)

    def test_en_templates(self):
        out = self._fresh({"BRIDGE_LANG": "en"},
                          "print('Bridge session context' in sb.AGENTS_TMPL, "
                          "'self-audit' in sb.AUDIT_PROMPT)")
        self.assertIn("True True", out)

    def test_autoclean_opt_in(self):
        out = self._fresh({}, "print(sb.AUTOCLEAN)")
        self.assertIn("False", out)
        out = self._fresh({"AUTOCLEAN": "1"}, "print(sb.AUTOCLEAN)")
        self.assertIn("True", out)

    def test_audit_opt_in(self):
        out = self._fresh({}, "print(sb.AUDIT_ENABLED)")
        self.assertIn("False", out)
        out = self._fresh({"AUDIT_ENABLED": "1"}, "print(sb.AUDIT_ENABLED)")
        self.assertIn("True", out)

    def test_pad_requires_install(self):
        # no ~/remote-pad in test env → forced off via BRIDGE_PAD=0
        out = self._fresh({"BRIDGE_PAD": "0"}, "print(sb.PAD_ENABLED)")
        self.assertIn("False", out)

    def test_bilingual_input_words(self):
        # command keyword sets stay bilingual regardless of BRIDGE_LANG
        self.assertIn("stop", sb.STOP_WORDS)
        self.assertIn("停止", sb.STOP_WORDS)
        self.assertIn("status", sb.STATUS_WORDS)
        self.assertIn("状态", sb.STATUS_WORDS)

    def test_no_private_literals(self):
        src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "session_bridge.py")).read()
        self.assertNotIn("100.69.104.50", src)


if __name__ == "__main__":
    if "--exec-helper" in sys.argv:
        _exec_helper_phase1(sys.argv[2])
    elif "--exec-phase2" in sys.argv:
        _exec_helper_phase2(sys.argv[2])
    else:
        unittest.main(verbosity=2)
