"""Durable offline bridge ledger. This module cannot start processes or send data.

The transport must authenticate and parse events before calling accept().
Store writes record intent, not proof that any external action succeeded.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
import sqlite3
from bridge_core import Command, Rejected


@dataclass(frozen=True)
class ThreadKey:
    team_id: str
    channel_id: str
    thread_ts: str

    def sql(self):
        return (self.team_id, self.channel_id, self.thread_ts)


@dataclass(frozen=True)
class Receipt:
    request_id: str
    duplicate: bool


@dataclass(frozen=True)
class Dispatch:
    request_id: str
    action: str
    text: str | None
    workspace_alias: str
    session_id: str | None
    thread: ThreadKey


SCHEMA = """
BEGIN IMMEDIATE;
CREATE TABLE tasks (
 team_id TEXT NOT NULL, channel_id TEXT NOT NULL, thread_ts TEXT NOT NULL,
 workspace_alias TEXT NOT NULL, state TEXT NOT NULL,
 session_id TEXT, active_request_id TEXT, scope_json TEXT NOT NULL DEFAULT 'null',
 PRIMARY KEY (team_id, channel_id, thread_ts),
 UNIQUE (team_id, session_id)
);
CREATE TABLE commands (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,
 team_id TEXT NOT NULL, request_id TEXT NOT NULL,
 channel_id TEXT NOT NULL, thread_ts TEXT NOT NULL,
 action TEXT NOT NULL, text TEXT, fingerprint TEXT NOT NULL,
 state TEXT NOT NULL, stop_reason TEXT,
 UNIQUE(team_id, request_id),
 FOREIGN KEY(team_id, channel_id, thread_ts)
 REFERENCES tasks(team_id, channel_id, thread_ts)
);
CREATE TABLE messages (
 team_id TEXT NOT NULL, channel_id TEXT NOT NULL, message_ts TEXT NOT NULL,
 request_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
 PRIMARY KEY(team_id, channel_id, message_ts)
);
CREATE TABLE receipts (
 team_id TEXT NOT NULL, event_id TEXT NOT NULL,
 request_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
 PRIMARY KEY(team_id, event_id)
);
CREATE TABLE outbox (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 team_id TEXT NOT NULL, channel_id TEXT NOT NULL, thread_ts TEXT NOT NULL,
 dedupe_key TEXT NOT NULL, text TEXT NOT NULL, state TEXT NOT NULL,
 slack_ts TEXT,
 UNIQUE(team_id, channel_id, thread_ts, dedupe_key),
 FOREIGN KEY(team_id, channel_id, thread_ts)
 REFERENCES tasks(team_id, channel_id, thread_ts)
);
PRAGMA user_version = 3;
COMMIT;
"""

MIGRATE_V1 = """
BEGIN IMMEDIATE;
CREATE TABLE outbox (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 team_id TEXT NOT NULL, channel_id TEXT NOT NULL, thread_ts TEXT NOT NULL,
 dedupe_key TEXT NOT NULL, text TEXT NOT NULL, state TEXT NOT NULL,
 slack_ts TEXT,
 UNIQUE(team_id, channel_id, thread_ts, dedupe_key),
 FOREIGN KEY(team_id, channel_id, thread_ts)
 REFERENCES tasks(team_id, channel_id, thread_ts)
);
PRAGMA user_version = 2;
COMMIT;
"""


class Ledger:
    def __init__(self, path: str, max_pending: int = 8):
        if not isinstance(max_pending, int) or isinstance(max_pending, bool) or max_pending < 1:
            raise ValueError("max_pending must be positive")
        self.max_pending = max_pending
        self.db = sqlite3.connect(path, isolation_level=None, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version == 0:
            self.db.executescript(SCHEMA)
        elif version == 1:
            self.db.executescript(MIGRATE_V1)
            self._migrate_scope_column()
        elif version == 2:
            self._migrate_scope_column()
        elif version != 3:
            self.db.close()
            raise Rejected("unsupported ledger schema")

    def _migrate_scope_column(self):
        columns = [row[1] for row in self.db.execute("PRAGMA table_info(tasks)")]
        if "scope_json" not in columns:
            self.db.execute("ALTER TABLE tasks ADD COLUMN scope_json "
                            "TEXT NOT NULL DEFAULT 'null'")
        self.db.execute("PRAGMA user_version = 3")

    def close(self):
        self.db.close()

    def tasks_in_route(self, team_id, channel_id):
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM tasks WHERE team_id=? AND channel_id=? ORDER BY thread_ts",
            (team_id, channel_id))]

    def recover_route(self, team_id, channel_id):
        """Before fresh transport I/O, under the externally held worker lease."""
        result = self.invalidate_acp_route(team_id, channel_id)
        with self._transaction():
            cursor = self.db.execute(
                "UPDATE outbox SET state='uncertain' WHERE team_id=? AND channel_id=? AND state='sending'",
                (team_id, channel_id))
            result["outbox_uncertain"] = cursor.rowcount
        return result

    @contextmanager
    def _transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def _task(self, key: ThreadKey):
        return self.db.execute(
            "SELECT * FROM tasks WHERE team_id=? AND channel_id=? AND thread_ts=?", key.sql()
        ).fetchone()

    def _command(self, key: ThreadKey, request_id: str):
        return self.db.execute(
            "SELECT * FROM commands WHERE team_id=? AND request_id=? AND channel_id=? AND thread_ts=?",
            (key.team_id, request_id, key.channel_id, key.thread_ts)).fetchone()

    def snapshot(self, key: ThreadKey):
        task = self._task(key)
        if task is None:
            raise Rejected("thread is not bound")
        return dict(task)

    def thread_for_session(self, team_id: str, session_id: str) -> ThreadKey | None:
        row = self.db.execute("SELECT * FROM tasks WHERE team_id=? AND session_id=?",
                              (team_id, session_id)).fetchone()
        if row is None:
            return None
        return ThreadKey(row["team_id"], row["channel_id"], row["thread_ts"])

    def commands_in_route(self, team_id: str, channel_id: str):
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM commands WHERE team_id=? AND channel_id=? ORDER BY seq",
            (team_id, channel_id)).fetchall()]

    def outbox_exists(self, key: ThreadKey, dedupe_key: str) -> bool:
        return self.db.execute("SELECT 1 FROM outbox WHERE team_id=? AND channel_id=? "
                               "AND thread_ts=? AND dedupe_key=?", (*key.sql(), dedupe_key)).fetchone() is not None

    def command_state(self, key: ThreadKey, request_id: str):
        command = self._command(key, request_id)
        if command is None:
            raise Rejected("command does not belong to this thread")
        return dict(command)

    def accept(self, command: Command) -> Receipt:
        """Atomic persist-before-ack. An existing request cannot change meaning."""
        key = ThreadKey(command.team_id, command.channel_id, command.thread_ts)
        payload = asdict(command)
        payload.pop("source_event_id")
        payload.pop("source_message_ts")
        fingerprint = _hash(payload)
        event_hash = _hash({"command": fingerprint, "message_ts": command.source_message_ts})
        with self._transaction():
            receipt = self.db.execute(
                "SELECT * FROM receipts WHERE team_id=? AND event_id=?",
                (command.team_id, command.source_event_id)).fetchone()
            if receipt:
                if receipt["fingerprint"] != event_hash:
                    raise Rejected("event id reused with changed content")
                return Receipt(receipt["request_id"], True)
            message = self.db.execute(
                "SELECT * FROM messages WHERE team_id=? AND channel_id=? AND message_ts=?",
                (command.team_id, command.channel_id, command.source_message_ts)).fetchone()
            if message and message["fingerprint"] != fingerprint:
                raise Rejected("message identity reused with changed content")
            existing = self.db.execute(
                "SELECT * FROM commands WHERE team_id=? AND request_id=?",
                (command.team_id, command.request_id)).fetchone()
            if existing:
                if existing["fingerprint"] != fingerprint:
                    raise Rejected("request id reused with changed content or thread")
                duplicate = True
            else:
                duplicate = False
                task = self._task(key)
                state = "queued"
                if command.action == "run":
                    if task:
                        raise Rejected("thread already has a task")
                    self.db.execute(
                        "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
                        (*key.sql(), command.workspace, "queued", None, None,
                         json.dumps(command.scope) if command.scope is not None else "null"))
                else:
                    if task is None:
                        raise Rejected("thread is not bound")
                    if command.action == "status":
                        state = "done"
                    elif command.action == "reply":
                        if task["state"] not in {"queued", "starting", "ready", "busy"}:
                            raise Rejected("thread is not accepting prompts")
                        pending = self.db.execute(
                            "SELECT COUNT(*) FROM commands WHERE team_id=? AND channel_id=? AND thread_ts=? "
                            "AND action IN ('run','reply') AND state='queued'", key.sql()).fetchone()[0]
                        if pending >= self.max_pending:
                            raise Rejected("thread queue is full")
                    elif command.action == "cancel":
                        if task["state"] in {"uncertain", "failed"}:
                            raise Rejected("reconcile uncertain execution before control actions")
                        if task["state"] in {"cancel_requested", "cancelled"}:
                            raise Rejected("cancellation already requested")
                        target = "cancel_requested" if task["active_request_id"] else "cancelled"
                        self.db.execute("UPDATE tasks SET state=? WHERE team_id=? AND channel_id=? AND thread_ts=?",
                                        (target, *key.sql()))
                        self.db.execute(
                            "UPDATE commands SET state='cancelled' WHERE team_id=? AND channel_id=? AND thread_ts=? "
                            "AND action IN ('run','reply') AND state='queued'", key.sql())
                        if target == "cancelled":
                            state = "done"
                    else:
                        raise Rejected("unsupported command")
                self.db.execute(
                    "INSERT INTO commands(team_id,request_id,channel_id,thread_ts,action,text,fingerprint,state) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (command.team_id, command.request_id, command.channel_id, command.thread_ts,
                     command.action, command.text, fingerprint, state))
            if not message:
                self.db.execute("INSERT INTO messages VALUES (?,?,?,?,?)",
                                (command.team_id, command.channel_id, command.source_message_ts,
                                 command.request_id, fingerprint))
            self.db.execute("INSERT INTO receipts VALUES (?,?,?,?)",
                            (command.team_id, command.source_event_id, command.request_id, event_hash))
            return Receipt(command.request_id, duplicate)

    def reserve_prompt(self, key: ThreadKey) -> Dispatch | None:
        """Durably record dispatch intent BEFORE launching or sending externally."""
        with self._transaction():
            task = self._task(key)
            if task is None:
                raise Rejected("thread is not bound")
            if task["state"] not in {"queued", "ready"} or task["active_request_id"]:
                return None
            command = self.db.execute(
                "SELECT * FROM commands WHERE team_id=? AND channel_id=? AND thread_ts=? "
                "AND action IN ('run','reply') AND state='queued' ORDER BY seq LIMIT 1", key.sql()).fetchone()
            if command is None:
                return None
            state = "busy" if task["session_id"] else "starting"
            self.db.execute("UPDATE tasks SET state=?,active_request_id=? "
                            "WHERE team_id=? AND channel_id=? AND thread_ts=?",
                            (state, command["request_id"], *key.sql()))
            self.db.execute("UPDATE commands SET state='prepared' WHERE seq=?", (command["seq"],))
            return self._dispatch(key, task, command)

    def bind_session(self, key: ThreadKey, request_id: str, session_id: str):
        """Bind only an observed ACP session/new response for this reserved task."""
        if not isinstance(session_id, str) or not session_id or len(session_id) > 256:
            raise Rejected("invalid session id")
        with self._transaction():
            task, command = self._require_active(key, request_id)
            if task["state"] not in {"starting", "cancel_requested"} or task["session_id"]:
                raise Rejected("session cannot be rebound")
            if command["state"] != "prepared":
                raise Rejected("invalid session binding transition")
            if self.db.execute("SELECT 1 FROM tasks WHERE team_id=? AND session_id=?",
                               (key.team_id, session_id)).fetchone():
                raise Rejected("session is already bound to another thread")
            state = "cancel_requested" if task["state"] == "cancel_requested" else "busy"
            self.db.execute("UPDATE tasks SET state=?,session_id=? "
                            "WHERE team_id=? AND channel_id=? AND thread_ts=?", (state, session_id, *key.sql()))

    def begin_prompt_send(self, key: ThreadKey, request_id: str):
        """Record actual-send intent; the runtime must serialize external I/O.

        A cancellation accepted before this transition blocks sending. Once
        this transition commits, uncertain writes must freeze, not retry.
        """
        with self._transaction():
            task, command = self._require_active(key, request_id)
            if task["state"] != "busy" or not task["session_id"] or command["state"] != "prepared":
                raise Rejected("prompt is not ready to send")
            self.db.execute("UPDATE commands SET state='dispatching' WHERE seq=?", (command["seq"],))

    def mark_prompt_sent(self, key: ThreadKey, request_id: str):
        """Record observed successful write, not authorization to perform it."""
        with self._transaction():
            task, command = self._require_active(key, request_id)
            if (task["state"] not in {"busy", "cancel_requested"} or not task["session_id"]
                    or command["state"] != "dispatching"):
                raise Rejected("invalid prompt-sent transition")
            self.db.execute("UPDATE commands SET state='running' WHERE seq=?", (command["seq"],))

    def reserve_cancel(self, key: ThreadKey) -> Dispatch | None:
        with self._transaction():
            task = self._task(key)
            if task is None:
                raise Rejected("thread is not bound")
            if task["state"] != "cancel_requested" or not task["session_id"]:
                return None
            active = self._command(key, task["active_request_id"])
            if active is None or active["state"] != "running":
                return None
            command = self.db.execute(
                "SELECT * FROM commands WHERE team_id=? AND channel_id=? AND thread_ts=? "
                "AND action='cancel' AND state='queued' ORDER BY seq LIMIT 1", key.sql()).fetchone()
            if command is None:
                return None
            self.db.execute("UPDATE commands SET state='dispatching' WHERE seq=?", (command["seq"],))
            return self._dispatch(key, task, command)

    def abort_prepared(self, key: ThreadKey, request_id: str):
        """Complete cancellation before begin_prompt_send, with no prompt I/O.

        This ends the logical task, not the ACP process. Runtime still owns
        cleanup of any process used only for initialization/session creation.
        """
        with self._transaction():
            task, command = self._require_active(key, request_id)
            if task["state"] != "cancel_requested" or command["state"] != "prepared":
                raise Rejected("cannot assert an unsent prompt after send intent")
            self.db.execute("UPDATE commands SET state='cancelled',stop_reason='never_sent' WHERE seq=?",
                            (command["seq"],))
            self.db.execute("UPDATE tasks SET state='cancelled',active_request_id=NULL "
                            "WHERE team_id=? AND channel_id=? AND thread_ts=?", key.sql())
            self.db.execute("UPDATE commands SET state='done',stop_reason='never_sent' "
                            "WHERE team_id=? AND channel_id=? AND thread_ts=? "
                            "AND action='cancel' AND state='queued'", key.sql())

    def finish_turn(self, key: ThreadKey, request_id: str, stop_reason: str):
        """Call only after observing the bound prompt's terminal ACP response."""
        if stop_reason not in {"end_turn", "max_tokens", "max_turn_requests", "refusal", "cancelled"}:
            raise Rejected("unrecognized stop reason")
        with self._transaction():
            task, command = self._require_active(key, request_id)
            if command["state"] != "running" or task["state"] not in {"busy", "cancel_requested"}:
                raise Rejected("invalid turn completion transition")
            cancelling = task["state"] == "cancel_requested"
            state = "cancelled" if cancelling or stop_reason == "cancelled" else "ready"
            self.db.execute("UPDATE commands SET state=?,stop_reason=? WHERE seq=?",
                            ("cancelled" if stop_reason == "cancelled" else "done", stop_reason, command["seq"]))
            self.db.execute("UPDATE tasks SET state=?,active_request_id=NULL "
                            "WHERE team_id=? AND channel_id=? AND thread_ts=?", (state, *key.sql()))
            if state == "cancelled":
                self.db.execute("UPDATE commands SET state='cancelled' "
                                "WHERE team_id=? AND channel_id=? AND thread_ts=? "
                                "AND action IN ('run','reply') AND state='queued'", key.sql())
                self.db.execute("UPDATE commands SET state='done',stop_reason=? "
                                "WHERE team_id=? AND channel_id=? AND thread_ts=? "
                                "AND action='cancel' AND state IN ('queued','dispatching')",
                                (stop_reason, *key.sql()))

    def recover_inflight(self) -> int:
        """After exclusive worker restart: freeze uncertain work, NEVER replay it.

        The production worker needs an exclusive process-level lease before
        opening/recovering the ledger. The ledger is not that lease. Returns
        only the number of active tasks frozen as uncertain; zero does not
        mean no recovery work remains, since idle sessions become restoring.
        """
        with self._transaction():
            rows = self.db.execute("SELECT * FROM tasks WHERE active_request_id IS NOT NULL "
                                   "AND state IN ('starting','busy','cancel_requested')").fetchall()
            for row in rows:
                key = ThreadKey(row["team_id"], row["channel_id"], row["thread_ts"])
                self.db.execute("UPDATE tasks SET state='uncertain' "
                                "WHERE team_id=? AND channel_id=? AND thread_ts=?", key.sql())
                self.db.execute("UPDATE commands SET state='uncertain' "
                                "WHERE team_id=? AND channel_id=? AND thread_ts=? "
                                "AND state IN ('prepared','dispatching','running')", key.sql())
            self.db.execute("UPDATE tasks SET state='restoring' "
                            "WHERE state='ready' AND session_id IS NOT NULL AND active_request_id IS NULL")
            self.db.execute("UPDATE outbox SET state='uncertain' WHERE state='sending'")
            return len(rows)

    def freeze_active(self, key: ThreadKey, request_id: str):
        """A local transport failure may have happened after an external effect."""
        with self._transaction():
            task, _ = self._require_active(key, request_id)
            self.db.execute("UPDATE tasks SET state='uncertain' "
                            "WHERE team_id=? AND channel_id=? AND thread_ts=?", key.sql())
            self.db.execute("UPDATE commands SET state='uncertain' "
                            "WHERE team_id=? AND channel_id=? AND thread_ts=? "
                            "AND state IN ('prepared','dispatching','running')", key.sql())

    def invalidate_acp_route(self, team_id: str, channel_id: str):
        """A failed ACP connection invalidates active AND idle bindings in its route."""
        with self._transaction():
            rows = self.db.execute("SELECT * FROM tasks WHERE team_id=? AND channel_id=? "
                                   "AND active_request_id IS NOT NULL "
                                   "AND state IN ('starting','busy','cancel_requested')",
                                   (team_id, channel_id)).fetchall()
            for row in rows:
                key = ThreadKey(row["team_id"], row["channel_id"], row["thread_ts"])
                self.db.execute("UPDATE tasks SET state='uncertain' "
                                "WHERE team_id=? AND channel_id=? AND thread_ts=?", key.sql())
                self.db.execute("UPDATE commands SET state='uncertain' "
                                "WHERE team_id=? AND channel_id=? AND thread_ts=? "
                                "AND state IN ('prepared','dispatching','running')", key.sql())
            cursor = self.db.execute("UPDATE tasks SET state='restoring' "
                                     "WHERE team_id=? AND channel_id=? AND state='ready' "
                                     "AND session_id IS NOT NULL AND active_request_id IS NULL",
                                     (team_id, channel_id))
            return {"uncertain": len(rows), "restoring": cursor.rowcount}

    def queue_outbox(self, key: ThreadKey, dedupe_key: str, text: str) -> int:
        """Only the trusted output-policy layer may provide text here."""
        if not isinstance(text, str) or not text or len(text.encode("utf-8")) > 3000:
            raise Rejected("invalid outgoing text")
        with self._transaction():
            if self._task(key) is None:
                raise Rejected("outbox thread is not bound")
            existing = self.db.execute("SELECT * FROM outbox WHERE team_id=? AND channel_id=? "
                                       "AND thread_ts=? AND dedupe_key=?", (*key.sql(), dedupe_key)).fetchone()
            if existing:
                if existing["text"] != text:
                    raise Rejected("outbox identity reused with different text")
                return existing["id"]
            cursor = self.db.execute("INSERT INTO outbox(team_id,channel_id,thread_ts,dedupe_key,text,state) "
                                     "VALUES (?,?,?,?,?,'queued')", (*key.sql(), dedupe_key, text))
            return cursor.lastrowid

    def reserve_outbox(self, team_id: str, channel_id: str):
        with self._transaction():
            row = self.db.execute("SELECT * FROM outbox WHERE team_id=? AND channel_id=? "
                                  "AND state='queued' ORDER BY id LIMIT 1", (team_id, channel_id)).fetchone()
            if row is None:
                return None
            self.db.execute("UPDATE outbox SET state='sending' WHERE id=?", (row["id"],))
            return dict(row)

    def finish_outbox(self, item_id: int, slack_ts: str | None):
        with self._transaction():
            row = self.db.execute("SELECT * FROM outbox WHERE id=?", (item_id,)).fetchone()
            if row is None or row["state"] != "sending":
                raise Rejected("outbox message was not reserved")
            self.db.execute("UPDATE outbox SET state=?,slack_ts=? WHERE id=?",
                            ("sent" if slack_ts is not None else "uncertain", slack_ts, item_id))

    def confirm_restored(self, key: ThreadKey, session_id: str):
        """After an observed supported ACP session/load on the new connection."""
        with self._transaction():
            task = self._task(key)
            if (task is None or task["state"] != "restoring" or task["active_request_id"]
                    or not session_id or task["session_id"] != session_id):
                raise Rejected("session restoration does not match the bound thread")
            self.db.execute("UPDATE tasks SET state='ready' "
                            "WHERE team_id=? AND channel_id=? AND thread_ts=?", key.sql())

    def _require_active(self, key, request_id):
        task, command = self._task(key), self._command(key, request_id)
        if task is None or command is None or task["active_request_id"] != request_id:
            raise Rejected("request is not this thread's active prompt")
        return task, command

    @staticmethod
    def _dispatch(key, task, command):
        return Dispatch(command["request_id"], command["action"], command["text"],
                        task["workspace_alias"], task["session_id"], key)


def _hash(value):
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    except (UnicodeError, ValueError, TypeError) as error:
        raise Rejected("command cannot be serialized safely") from error
