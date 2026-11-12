#!/usr/bin/env python3
"""Multi-account token pool for the Slack session bridge.

The pool file (~/.config/devin-bridge/accounts.json) holds org-scoped
devin session tokens minted out-of-band. The bridge spreads new acp
sessions across pool accounts; when the upstream reports a rate/quota
limit on one account it is boxed (cooling_until) and every session
bound to it migrates — kill the stale-token acp process, respawn with
a healthy account's token, session/load the same session id so context
survives. All pool bookkeeping writes back to the same file so an
operator can inspect or hand-edit account state between restarts.

File shape (extra keys per account are preserved on write):

    {"accounts": [{"id": "liz", "org_id": "org-…", "token": "devin-…",
                   "fp": "aaaeb9f0ab01",
                   "cooling_until": 0.0, "last_limit_at": 0.0,
                   "last_ok": 0.0, "dead": false, "dead_reason": ""}]}

session_bridge.py owns the moving parts; this module is deterministic
and unit-testable — no threads, no Slack, no devin.
"""
import hashlib
import json
import os
import random
import threading
import time

DEFAULT_POOL_FILE = os.path.expanduser(
    os.environ.get("BRIDGE_ACCT_POOL",
                   "~/.config/devin-bridge/accounts.json"))
# Disable explicitly: BRIDGE_ACCT_POOL=0|off|"" — the bridge then keeps
# its legacy follow-the-Desktop credential behaviour.
_DISABLED = {"", "0", "off", "none"}


def fingerprint(token):
    """Short id of a credential value; matches session_bridge's
    _acct_fingerprint (sha256[:12])."""
    return hashlib.sha256(token.encode()).hexdigest()[:12] if token \
        else ""


class Pool:
    def __init__(self, path=None):
        self.path = os.path.expanduser(path or DEFAULT_POOL_FILE)
        self.lock = threading.RLock()
        self._mtime = None
        self._accounts = []
        self.reload(force=True)

    # ---- load/save -------------------------------------------------

    def _read(self):
        try:
            st = os.stat(self.path)
        except OSError:
            return None, None
        try:
            with open(self.path) as f:
                return json.load(f), st.st_mtime
        except (OSError, ValueError):
            return None, st.st_mtime

    def reload(self, force=False):
        """Re-read when the file changed on disk (hand edits, re-minted
        tokens, our own writes all show up here)."""
        with self.lock:
            data, mtime = self._read()
            if not force and (mtime is None or mtime == self._mtime):
                return
            accts = []
            if data:
                rows = data.get("accounts") \
                    if isinstance(data, dict) else data
                for a in rows or []:
                    if a.get("token"):
                        a.setdefault("fp", fingerprint(a["token"]))
                        accts.append(a)
            self._accounts = accts
            self._mtime = mtime

    def _save(self, mutate):
        """Re-read the file, apply mutate(accounts), write back
        atomically. Merge-on-write keeps hand edits and fields this
        module doesn't know about."""
        with self.lock:
            data, mtime = self._read()
            data = data or {"accounts": []}
            accts = data.setdefault("accounts", [])
            mutate(accts)
            tmp = f"{self.path}.{os.getpid()}.{threading.get_ident()}.tmp"
            try:
                with open(tmp, "w") as f:
                    json.dump(data, f, ensure_ascii=False, indent=1)
                os.replace(tmp, self.path)
            except OSError:
                return
            self._mtime = os.stat(self.path).st_mtime \
                if os.path.exists(self.path) else None
            for a in accts:
                if a.get("token"):
                    a.setdefault("fp", fingerprint(a["token"]))
            self._accounts = [a for a in accts if a.get("token")]

    # ---- reads ------------------------------------------------------

    def enabled(self):
        with self.lock:
            return bool(self._accounts)

    def accounts(self):
        with self.lock:
            return [dict(a) for a in self._accounts]

    def by_id(self, acct_id):
        with self.lock:
            for a in self._accounts:
                if a.get("id") == acct_id:
                    return dict(a)
            return None

    def by_fp(self, fp):
        """Map a credential fingerprint (e.g. the live Desktop token's)
        to its pool account — the same underlying org regardless of
        which login session minted the token."""
        if not fp:
            return None
        with self.lock:
            for a in self._accounts:
                if a.get("fp") == fp:
                    return dict(a)
            return None

    def healthy_id(self, acct_id, now=None):
        a = self.by_id(acct_id)
        if not a or a.get("dead"):
            return False
        return (a.get("cooling_until") or 0) <= (now or time.time())

    def healthy(self, now=None):
        now = time.time() if now is None else now
        with self.lock:
            return [dict(a) for a in self._accounts
                    if not a.get("dead")
                    and (a.get("cooling_until") or 0) <= now]

    def pick(self, loads=None, exclude=None, now=None):
        """Least-loaded healthy account. loads={acct_id: n_sessions};
        ties resolve randomly so bursts spread instead of stacking."""
        cand = self.healthy(now)
        if exclude:
            cand = [a for a in cand if a.get("id") not in exclude]
        if not cand:
            return None
        loads = loads or {}
        low = min(loads.get(a.get("id"), 0) for a in cand)
        return dict(random.choice(
            [a for a in cand if loads.get(a.get("id"), 0) == low]))

    def earliest_release(self):
        """Soonest cooling_until across boxed accounts, else None."""
        with self.lock:
            rel = [a.get("cooling_until") for a in self._accounts
                   if not a.get("dead") and a.get("cooling_until")]
        return min(rel) if rel else None

    # ---- writes -----------------------------------------------------

    def _mutate(self, acct_id, fn):
        def apply(accts):
            for a in accts:
                if a.get("id") == acct_id:
                    fn(a)
        self._save(apply)

    def mark_cooling(self, acct_id, until, error=""):
        """Penalty-box the account until the server's quoted reset."""
        def m(a):
            a["cooling_until"] = max(until or 0,
                                     a.get("cooling_until") or 0)
            a["last_limit_at"] = time.time()
            if error:
                a["last_error"] = str(error)[:300]
        self._mutate(acct_id, m)

    def mark_ok(self, acct_id):
        def m(a):
            a["last_ok"] = time.time()
            # A working request means any stale cool-down is over.
            if (a.get("cooling_until") or 0) > time.time():
                a["cooling_until"] = 0
        self._mutate(acct_id, m)

    def mark_dead(self, acct_id, reason=""):
        """Token revoked/expired — take the account out of rotation
        until someone re-mints it by hand."""
        def m(a):
            a["dead"] = True
            a["dead_reason"] = str(reason)[:300]
            a["dead_at"] = time.time()
        self._mutate(acct_id, m)

    def revive(self, acct_id):
        def m(a):
            a["dead"] = False
            a["dead_reason"] = ""
            a["cooling_until"] = 0
        self._mutate(acct_id, m)
