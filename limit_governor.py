#!/usr/bin/env python3
"""Server-side model/quota limit governor: park → cooldown → resume.

A turn that dies on a provider limit ("Reached … model rate limit",
"Connection error … retryable", concurrency refusals) is not a task
failure — the work is intact, the account just needs to cool down.
This module holds the pure logic: classifying turn errors, parsing the
server's own reset window, a durable parked-turn registry
(<tasks_dir>/.bridge_limits.json) and a JSONL event log
(<tasks_dir>/.bridge_limits.log) so the story of each limit window —
what tripped, how many sessions were live, when it reset, how each
resume attempt went — is reviewable after the fact.

session_bridge.py owns the moving parts (threads, acp processes,
Slack); everything here is deterministic and unit-testable.
"""
import json
import os
import random
import re
import threading
import time

# Wire messages seen in production:
#   "Reached free model rate limit. Upgrade to Max for higher limits,
#    or switch to a different model. Your limit will reset in
#    14 hours 52 minutes (at Nov 8 04:01 UTC)."
#   "{'code': -32010, 'message': 'Connection error, send a message to
#    continue retrying', 'data': {'cognition.ai/errorKind':
#    'unavailable', 'cognition.ai/retryable': True}}"
LIMIT_RE = re.compile(
    r"reached\s+\S+\s+model\s+rate\s+limit|rate\s+limit|"
    r"concurrent\s+sessions?|too\s+many\s+(?:sessions|requests|"
    r"concurrent)", re.IGNORECASE)
RESET_IN_RE = re.compile(
    r"reset\s+in\s+(?:(\d+)\s*hours?)?\s*(?:(\d+)\s*minutes?)?"
    r"\s*(?:(\d+)\s*seconds?)?", re.IGNORECASE)
RESET_AT_RE = re.compile(
    r"\(at\s+([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{1,2}):(\d{2})"
    r"(?::(\d{2}))?\s*(UTC|GMT)\)", re.IGNORECASE)
RETRYABLE_KINDS = {"unavailable", "overloaded", "internal",
                   "rate_limit", "quota"}
# Transcript evidence: a *tool* node whose content IS the limit error.
# Anchored — code/agents quoting the message inside exec output keep
# their "Output from command…" prefix and must not count.
TOOL_LIMIT_RE = re.compile(r"\s*Reached\s+\S+\s+model\s+rate\s+limit",
                           re.IGNORECASE)
MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
          "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}

REGISTRY_FILE = ".bridge_limits.json"
LOG_FILE = ".bridge_limits.log"


def parse_reset(msg):
    """Epoch when the server says the limit resets, or None.
    Handles both 'reset in 14 hours 52 minutes' and '(at Nov 8 04:01
    UTC)' — the message usually carries both."""
    m = RESET_IN_RE.search(msg or "")
    if m and any(m.groups()):
        sec = (int(m.group(1) or 0) * 3600
               + int(m.group(2) or 0) * 60
               + int(m.group(3) or 0))
        if sec:
            return time.time() + sec
    m = RESET_AT_RE.search(msg or "")
    if m:
        mon = MONTHS.get(m.group(1).lower())
        if mon:
            import calendar
            now = time.gmtime()
            year = now.tm_year
            day = int(m.group(2))
            hh, mm = int(m.group(3)), int(m.group(4))
            ss = int(m.group(5) or 0)
            for y in (year, year + 1):
                try:
                    t = calendar.timegm((y, mon, day, hh, mm, ss,
                                         0, 0, 0))
                except (ValueError, OverflowError):
                    continue
                if t > time.time() - 60:
                    return t
    return None


def classify_error(exc):
    """-> {'kind', 'reset_at', 'message'} for parkable errors, else None.
    kind: 'rate_limited' (server quoted a quota/rate window — sleep
    until reset) or 'retryable' (transient server refusal — bounded
    exponential backoff)."""
    msg = str(exc) or ""
    error_kind = getattr(exc, "error_kind", None)
    retryable = bool(getattr(exc, "retryable", False))
    if LIMIT_RE.search(msg) or error_kind in ("rate_limit", "quota"):
        return {"kind": "rate_limited", "reset_at": parse_reset(msg),
                "message": msg}
    if retryable or error_kind in RETRYABLE_KINDS \
            or "send a message to continue" in msg:
        return {"kind": "retryable", "reset_at": parse_reset(msg),
                "message": msg}
    return None


def tail_verdict(nodes):
    """Classify a session's transcript tail (parsed message_nodes,
    newest first):
      'limited' — a rate-limit error sits in the last few nodes
      'midturn' — the tail ends mid-turn (tool result or tool call);
                  the turn plausibly died in flight
      'idle'    — looks finished/awaiting input
    """
    for j in nodes[:5]:
        if j.get("role") == "tool" \
                and TOOL_LIMIT_RE.match(j.get("content") or ""):
            return "limited"
    last = nodes[0] if nodes else {}
    if last.get("role") == "tool" or last.get("tool_calls"):
        return "midturn"
    return "idle"


def backoff(attempt, base=30.0, cap=300.0):
    """Exponential backoff for retryable errors (no server window):
    30s, 60s, 120s, 240s, 300s…, plus jitter so a batch of parked turns
    doesn't resume in lockstep."""
    delay = min(base * (2 ** max(0, attempt - 1)), cap)
    return delay + random.uniform(0, min(15.0, delay * 0.3))


class Governor:
    """Durable registry of parked turns + append-only event log.

    entries[key] = {
        kind: 'bridge' | 'external',
        text, files, msg_ts,          # bridge: the turn to replay
        session_id, workdir, title,   # for session/load resume
        error, error_kind,            # what parked it
        attempt, first_hit, resume_at,
    }
    """

    def __init__(self, tasks_dir, printer=None):
        self.path = os.path.join(tasks_dir, REGISTRY_FILE)
        self.log_path = os.path.join(tasks_dir, LOG_FILE)
        self.lock = threading.RLock()
        self._print = printer or (lambda msg: None)
        self.last_resume_at = 0.0
        data = self._read()
        self.entries = data.get("entries") or {}
        self.last_limit_at = data.get("last_limit_at") or 0.0

    def _read(self):
        try:
            with open(self.path) as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def _save(self):
        tmp = f"{self.path}.{os.getpid()}.{threading.get_ident()}.tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({"entries": self.entries,
                           "last_limit_at": self.last_limit_at},
                          f, ensure_ascii=False)
            os.replace(tmp, self.path)
        except OSError:
            pass

    def event(self, name, **fields):
        rec = {"t": time.time(), "event": name}
        rec.update(fields)
        try:
            with self.lock:
                with open(self.log_path, "a") as f:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except OSError:
            pass
        detail = " ".join(f"{k}={v}" for k, v in fields.items()
                          if v is not None and k != "error")
        err = fields.get("error")
        if err:
            detail += (" " if detail else "") + f"err={str(err)[:160]}"
        self._print(f"[limit] {name} {detail}".rstrip())

    def park(self, key, entry):
        with self.lock:
            old = self.entries.get(key) or {}
            entry = dict(entry)
            entry.setdefault("attempt", old.get("attempt", 0) + 1)
            entry.setdefault("first_hit", old.get("first_hit")
                             or time.time())
            self.entries[key] = entry
            self.last_limit_at = time.time()
            self._save()
            self.event("parked", key=key, kind=entry.get("error_kind"),
                       attempt=entry.get("attempt"),
                       resume_in=int(entry.get("resume_at", 0)
                                     - time.time()),
                       session=entry.get("session_id"),
                       live=entry.get("live_sessions"),
                       active=entry.get("active_turns"),
                       error=entry.get("error"))
            return entry

    def pop(self, key, reason=""):
        with self.lock:
            e = self.entries.pop(key, None)
            if e is not None:
                self._save()
                self.event("unparked", key=key, reason=reason)
            return e

    def get(self, key):
        with self.lock:
            e = self.entries.get(key)
            return dict(e) if e else None

    def due(self, now=None):
        """Parked entries whose cooldown has elapsed, earliest first."""
        now = time.time() if now is None else now
        with self.lock:
            ready = [(k, dict(e)) for k, e in self.entries.items()
                     if e.get("resume_at", 0) <= now]
        ready.sort(key=lambda kv: kv[1].get("resume_at", 0))
        return ready

    def snapshot(self):
        with self.lock:
            out = [dict(e, key=k) for k, e in self.entries.items()]
        out.sort(key=lambda e: e.get("resume_at", 0))
        return out

    def events_tail(self, n=15):
        try:
            with open(self.log_path) as f:
                lines = f.readlines()[-n:]
        except OSError:
            return []
        out = []
        for line in lines:
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
        return out
