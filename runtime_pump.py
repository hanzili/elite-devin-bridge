"""Manual single-actor runtime pump. No loop, thread, service or auto-start.

Only the configured SDK callback may enqueue frames. The caller owns an
exclusive worker lease BEFORE constructing/opening the ledger. This module
cannot acquire that deployment authority, trust a directory or load secrets.
"""
from queue import Queue, Empty, Full
import threading
import time
from bridge_core import Rejected, parse_command
from bridge_store import ThreadKey
from slack_adapter import AdapterFailure


class RuntimePump:
    def __init__(self, engine, slack, acp, *, max_frames=32, max_tasks=1,
                 max_prompts=4, max_runtime_seconds=60, clock=time.monotonic):
        for value in (max_frames, max_tasks, max_prompts):
            if type(value) is not int or value < 1:
                raise Rejected("positive runtime limits required")
        if type(max_runtime_seconds) not in (int, float) or not 0 < max_runtime_seconds <= 3600:
            raise Rejected("bounded task observation window required")
        if engine.slack is not slack or engine.acp is not acp:
            raise Rejected("runtime adapters must match engine adapters")
        self.engine, self.slack, self.acp = engine, slack, acp
        self._queue = Queue(maxsize=max_frames)
        self._owner = threading.get_ident()
        self._clock = clock
        self._duration = max_runtime_seconds
        self._max_tasks, self._max_prompts = max_tasks, max_prompts
        self._started = False
        self._ever_started = False
        self._deadline = None
        self._generation = acp.generation
        self.state = "not_started"
        self.failure = None
        self.cleanup = None

    def _actor(self):
        if threading.get_ident() != self._owner:
            raise Rejected("runtime operations require the original actor thread")

    def enqueue_socket(self, frame):
        """Bound to this socket adapter only; callback thread does NO I/O/SQL."""
        if not self._started or self.failure:
            return False
        try:
            self._queue.put_nowait(frame)
            return True
        except Full:
            return False

    def start(self):
        """Explicit live boundary: authenticates Slack and initializes existing ACP.

        Do not call on actual clients until the user has approved this test.
        The ACP adapter must already have been launched under its own gates.
        """
        self._actor()
        if self._ever_started:
            raise Rejected("runtime cannot restart or replay automatically")
        self._ever_started = True
        self._started = True
        self.state = "starting"
        self._deadline = self._clock() + self._duration
        try:
            self.engine.ledger.recover_route(self.engine.policy.team_id, self.engine.policy.channel_id)
            self.slack.start(self.enqueue_socket)
            self.engine.initialize()
            if self.engine.protocol_failed:
                raise AdapterFailure("ACP initialization failed")
        except Exception:
            self._halt("runtime_start_failed")
            raise AdapterFailure("Runtime startup failed; no automatic retry") from None

    def _route_tasks(self):
        return self.engine.ledger.tasks_in_route(self.engine.policy.team_id, self.engine.policy.channel_id)

    def _within_budget(self, frame):
        # Parse twice deliberately: this check is a local cap, not authentication.
        try:
            command = parse_command(frame.get("payload"), self.engine.policy)
        except (Rejected, AttributeError):
            return True  # Engine acknowledges and discards irrelevant input.
        tasks = self._route_tasks()
        bound = any(t["thread_ts"] == command.thread_ts for t in tasks)
        existing = self.engine.ledger.db.execute(
            "SELECT 1 FROM commands WHERE team_id=? AND request_id=?",
            (command.team_id, command.request_id)).fetchone()
        if existing:
            return True  # Preserve durable dedup even when a cap has been hit.
        commands = self.engine.ledger.commands_in_route(command.team_id, command.channel_id)
        prompts = sum(c["action"] in {"run", "reply"} for c in commands)
        return not ((command.action == "run" and not bound and len(tasks) >= self._max_tasks)
                    or (command.action in {"run", "reply"} and prompts >= self._max_prompts))

    def step(self):
        """Process at most one frame/read/send on the actor; never sleeps/retries."""
        self._actor()
        if not self._started or self.failure:
            raise Rejected("runtime is not active")
        try:
            if self.acp.generation is not self._generation:
                raise AdapterFailure("ACP transport generation changed")
            if self._clock() >= self._deadline:
                self._halt("runtime_deadline_reached")
                return False
            if self.slack.fault:
                raise AdapterFailure("Slack transport fault")
            try:
                frame = self._queue.get_nowait()
            except Empty:
                frame = None
            if frame is not None:
                if self._within_budget(frame):
                    self.engine.receive_socket_frame(frame)
                else:
                    # Do not create task/output intents or execute over-budget work.
                    self.slack.ack(frame.get("envelope_id"))
            data = self.acp.read(0.0)
            if data == b"":
                raise AdapterFailure("ACP EOF")
            if data:
                self.engine.feed_acp_bytes(data)
            if self.engine.protocol_failed:
                raise AdapterFailure("ACP protocol failure")
            if self.engine.initialized:
                self.state = "active"
                tasks = self._route_tasks()
                # Exactly one active prompt across the whole approved route.
                if not any(t["active_request_id"] for t in tasks):
                    for task in tasks:
                        if task["state"] in {"queued", "ready"}:
                            key = ThreadKey(task["team_id"], task["channel_id"], task["thread_ts"])
                            if self.engine.dispatch(key):
                                break
                if self.engine.protocol_failed:
                    raise AdapterFailure("ACP dispatch failure")
            self.engine.flush_one_slack()
            if self.slack.fault:
                raise AdapterFailure("Slack send fault")
            return True
        except Exception:
            self._halt("runtime_transport_failed")
            raise AdapterFailure("Runtime paused after transport failure; no automatic replay") from None

    def _halt(self, reason):
        self._started = False
        self.failure = reason
        self.state = "halted"
        ledger_invalidated = False
        try:
            self.engine.transport_lost()
            ledger_invalidated = True
        except Exception:
            # Disk/SQLite failures must not skip independent transport cleanup.
            pass
        # Closing adapters can terminate only the specifically owned process.
        # Never turn this into a user-facing assertion that descendants stopped.
        slack_closed = False
        acp_result = None
        try:
            self.slack.close()
            slack_closed = True
        except Exception:
            pass
        try:
            acp_result = self.acp.close()
        except Exception:
            pass
        self.cleanup = {"ledger_invalidated": ledger_invalidated,
                        "slack_closed": slack_closed, "acp": acp_result}
        while True:
            try:
                self._queue.get_nowait()
            except Empty:
                break

    def close(self):
        self._actor()
        if not self._ever_started:
            raise Rejected("runtime has not started")
        if self.state == "halted":
            raise Rejected("cleanup already attempted; reconcile any uncertain outcome")
        self._halt("operator_stop")
        return self.cleanup
