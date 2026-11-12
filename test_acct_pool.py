#!/usr/bin/env python3
"""Account-pool routing tests: Pool pick/cool/dead bookkeeping plus the
bridge-level migrate-on-limit loop — a live session whose org hits the
free-model limit is parked, its acp killed, and the turn is resumed via
session/load on a freshly spawned acp authenticated with a different
pool token."""
import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import acct_pool
import session_bridge as sb
import test_session_bridge as tsb

FakeWeb = tsb.FakeWeb
FAKE = tsb.FAKE

POOL = {"accounts": [
    {"id": "aaa", "org": "org-1", "token": "tok-aaa"},
    {"id": "bbb", "org": "org-2", "token": "tok-bbb"},
    {"id": "ccc", "org": "org-3", "token": "tok-ccc"},
]}


class PoolUnitTest(unittest.TestCase):
    def _pool(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        path = os.path.join(self.dir.name, "accounts.json")
        with open(path, "w") as f:
            json.dump(POOL, f)
        return acct_pool.Pool(path)

    def test_pick_least_loaded(self):
        p = self._pool()
        # pick returns the account record; ties resolve randomly
        self.assertIn(p.pick()["id"], {"aaa", "bbb", "ccc"})
        self.assertEqual(p.pick(loads={"aaa": 2, "bbb": 2})["id"],
                         "ccc")
        self.assertIn(p.pick(loads={"aaa": 1, "ccc": 1})["id"],
                      {"bbb", "ccc", "aaa"})
        self.assertEqual(p.pick(loads={"aaa": 9, "bbb": 9,
                                       "ccc": 1})["id"], "ccc")
        self.assertEqual(p.pick(exclude={"aaa", "bbb"})["id"], "ccc")

    def test_cooling_excludes_until_reset(self):
        p = self._pool()
        p.mark_cooling("aaa", time.time() + 600)
        self.assertIn(p.pick()["id"], {"bbb", "ccc"})
        self.assertFalse(p.healthy_id("aaa"))
        self.assertTrue(p.healthy_id("bbb"))
        rec = p.by_id("aaa")
        self.assertAlmostEqual(rec["cooling_until"],
                               time.time() + 600, delta=2)
        self.assertGreater(rec["last_limit_at"], 0)
        # the box only ever extends (max wins) — an earlier re-mark
        # doesn't shorten a quoted reset
        p.mark_cooling("aaa", time.time() + 60)
        self.assertAlmostEqual(p.by_id("aaa")["cooling_until"],
                               time.time() + 600, delta=2)
        # a cooling_until already in the past reads as healthy
        p2 = self._pool()
        p2.mark_cooling("bbb", time.time() + 60)
        with open(p2.path) as f:
            d = json.load(f)
        for a in d["accounts"]:
            if a["id"] == "bbb":
                a["cooling_until"] = 1
        with open(p2.path, "w") as f:
            json.dump(d, f)
        p2.reload(force=True)
        self.assertTrue(p2.healthy_id("bbb"))

    def test_earliest_release(self):
        p = self._pool()
        p.mark_cooling("aaa", time.time() + 100)
        p.mark_cooling("bbb", time.time() + 50)
        self.assertAlmostEqual(p.earliest_release(),
                               time.time() + 50, delta=2)
        # all boxed -> pick is None
        p.mark_cooling("ccc", time.time() + 200)
        self.assertIsNone(p.pick())
        # dead accounts don't set a release (bbb's +50 is soonest)
        p.mark_dead("ccc", "revoked")
        self.assertAlmostEqual(p.earliest_release(),
                               time.time() + 50, delta=2)

    def test_dead_never_picked(self):
        p = self._pool()
        p.mark_dead("aaa", "x")
        p.mark_dead("bbb", "y")
        self.assertEqual(p.pick()["id"], "ccc")
        p.mark_dead("ccc", "z")
        self.assertIsNone(p.pick())
        self.assertIsNone(p.earliest_release())

    def test_state_persists(self):
        p = self._pool()
        p.mark_cooling("bbb", time.time() + 300)
        p2 = acct_pool.Pool(p.path)
        self.assertFalse(p2.healthy_id("bbb"))
        rec = p2.by_id("bbb")
        self.assertGreater(rec["cooling_until"], time.time())

    def test_by_fp(self):
        p = self._pool()
        self.assertEqual(
            p.by_fp(acct_pool.fingerprint("tok-bbb"))["id"], "bbb")
        self.assertIsNone(p.by_fp("deadbeef"))


class PoolBridgeTest(unittest.TestCase):
    """E2E through the real bridge: root_ev -> _new_session binds a
    pool token; a limit on that org parks the turn, kills the acp, and
    the sweeper resumes it on a new acp under another org's token."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.pool_path = os.path.join(self.tmp.name, "accounts.json")
        with open(self.pool_path, "w") as f:
            json.dump(POOL, f)
        self._enabled, self._file = sb.ACCT_POOL_ENABLED, \
            sb.ACCT_POOL_FILE
        sb.ACCT_POOL_ENABLED, sb.ACCT_POOL_FILE = True, self.pool_path
        # deterministic pick: first healthy candidate, no random tie
        self._choice = acct_pool.random.choice
        acct_pool.random.choice = lambda xs: xs[0]
        self._gap = sb.LIMIT_RESUME_GAP
        sb.LIMIT_RESUME_GAP = 0.2
        self._pctx = os.environ.get("PCTX_MESSAGES_PUT")
        os.environ["PCTX_MESSAGES_PUT"] = ""
        # authenticate fires only when the fake advertises authMethods
        self._fam = os.environ.get("FAKE_AUTH_METHODS")
        os.environ["FAKE_AUTH_METHODS"] = "1"
        self.addCleanup(self._restore)

    def _restore(self):
        sb.ACCT_POOL_ENABLED, sb.ACCT_POOL_FILE = \
            self._enabled, self._file
        acct_pool.random.choice = self._choice
        sb.LIMIT_RESUME_GAP = self._gap
        if self._pctx is None:
            os.environ.pop("PCTX_MESSAGES_PUT", None)
        else:
            os.environ["PCTX_MESSAGES_PUT"] = self._pctx
        if self._fam is None:
            os.environ.pop("FAKE_AUTH_METHODS", None)
        else:
            os.environ["FAKE_AUTH_METHODS"] = self._fam

    def _bridge(self, mode="normal"):
        web = FakeWeb()
        b = sb.SessionBridge(
            web.post, tasks_dir=self.tmp.name,
            acp_cmd=[sys.executable, FAKE, mode],
            timeout=10, idle_sec=3600, own_bot_id=tsb.BOT,
            channel=tsb.CHANNEL)
        self.addCleanup(self._teardown, b)
        return b, web

    @staticmethod
    def _teardown(b):
        for s in list(b.sessions.values()):
            try:
                s.terminate()
            except Exception:
                pass

    def test_new_session_binds_pool_token(self):
        b, web = self._bridge()
        b.handle_event(tsb.root_ev("1701.1"))
        end = time.monotonic() + 10
        while time.monotonic() < end and "1701.1" not in b.sessions:
            time.sleep(0.05)
        s = b.sessions["1701.1"]
        self.assertEqual(s.acct_id, "aaa")
        with open(os.path.join(s.workdir, ".acp_key")) as f:
            self.assertEqual(f.read(), "tok-aaa")
        self.assertTrue(web.wait_for("任务结果", 15), web.texts())

    def test_limit_migrates_to_other_acct(self):
        os.environ["FAKE_LIMIT_KEY"] = "tok-aaa"
        self.addCleanup(os.environ.pop, "FAKE_LIMIT_KEY", None)
        b, web = self._bridge()
        t = __import__("threading").Thread(
            target=b.limit_sweeper, daemon=True)
        t.start()
        b.handle_event(tsb.root_ev("1701.2"))
        # prompt on aaa's token -> rate limited -> migrate card
        self.assertTrue(web.wait_for("已切到账号", 20), web.texts())
        # sweeper dispatches the parked entry onto a fresh acp with
        # another org's token; the same devin session continues
        self.assertTrue(web.wait_for("任务结果", 30), web.texts())
        s = b.sessions["1701.2"]
        self.assertEqual(s.acct_id, "bbb")
        with open(os.path.join(s.workdir, ".acp_key")) as f:
            self.assertEqual(f.read(), "tok-bbb")
        self.assertIsNone(b.governor.get("1701.2"))
        rec = b.acct_pool.by_id("aaa")
        self.assertGreater(rec["last_limit_at"], 0)

    def test_all_cooling_defers_resume(self):
        b, web = self._bridge()
        for a in ("aaa", "bbb", "ccc"):
            b.acct_pool.mark_cooling(a, time.time() + 60)
        b.governor.park("1701.3", {"kind": "bridge", "text": "x",
                                   "session_id": "s", "attempt": 1,
                                   "error_kind": "rate_limited",
                                   "resume_at": time.time() - 1})
        b._limit_sweep()
        e = b.governor.get("1701.3")
        self.assertIsNotNone(e)
        self.assertEqual(e["attempt"], 1)  # not consumed
        self.assertGreater(e["resume_at"], time.time() + 50)

    def test_loads_spread_sessions(self):
        b, web = self._bridge()
        s = sb.DevinSession.__new__(sb.DevinSession)
        s.acct_id, s.alive = "aaa", True
        b.sessions["x"] = s
        self.assertEqual(b._acct_loads()["aaa"], 1)
        self.assertEqual(b._pick_acct()["id"], "bbb")
        b.acct_pool.mark_cooling("bbb", time.time() + 60)
        self.assertEqual(b._pick_acct()["id"], "ccc")
        b.acct_pool.mark_cooling("ccc", time.time() + 60)
        # aaa still healthy even though it hosts a session
        self.assertEqual(b._pick_acct()["id"], "aaa")
        b.acct_pool.mark_cooling("aaa", time.time() + 60)
        self.assertIsNone(b._pick_acct())


if __name__ == "__main__":
    unittest.main()
