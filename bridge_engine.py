"""Transport-independent bridge engine. No I/O side effects at import.

A supplied adapter owns authentication and execution. The engine never treats
a caller-provided JSON envelope as proof of Slack origin or authority.
"""
from dataclasses import dataclass
import json
import re
import threading
from bridge_core import (Policy, Rejected, SLACK_TS, _unique_keys, parse_command,
                         acp_initialize, acp_new_session, acp_prompt, acp_cancel,
                         approve_permission, deny_permission)
from bridge_store import Ledger, ThreadKey
from result_summary import (SUMMARY_CONTRACT, SUMMARY_MARKER, Trace,
                            is_valid_rendered, parse_model_summary, render_summary)
from task_scope import TaskScope


NOTICES = {
    "accepted": "Task received by the Elite bridge.",
    "working": "Devin is working on this task.",
    "complete": "Devin finished this turn. Its output is held for local review.",
    "stopped": "This task is stopped. No further prompts will run in this thread.",
    "permission": "Devin requested a tool action. It was not approved; review it locally on Elite.",
    "out_of_scope": "Devin requested an action outside this task's approved scope. It was denied and recorded locally.",
    "stopping": "Cancellation requested. Waiting for confirmation that Devin has actually stopped.",
    "stopped_verified": "Task cancelled. Devin ended its turn and its process exit was confirmed.",
    "stopped_unverified": "Task cancelled, but Devin's process exit was not confirmed. Verify locally before assuming it stopped.",
    "uncertain": "The task connection is uncertain. It is paused for local reconciliation and will not be retried automatically.",
    "restore": "This task needs its existing Devin session restored before it can continue.",
    "status": "The bridge has recorded this status request. Detailed state stays local.",
    "summary_withheld": "The structured result summary was withheld for local review.",
}

# Defense in depth only. Exact approved output, not these patterns, is the gate.
SENSITIVE = re.compile(
    r"(?:xox[baprs]-|sk-[A-Za-z0-9]|AKIA[A-Z0-9]{16}|"
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----|bearer\s+|password\s*[:=]|"
    r"api[_ -]?key\s*[:=]|secret\s*[:=]|token\s*[:=])", re.IGNORECASE)
SLACK_CONTROL = re.compile(r"(?:<[@!]|@(?:channel|here|everyone)\b)", re.IGNORECASE)


@dataclass(frozen=True)
class OutputPolicy:
    """Exact outputs preapproved for THIS channel, e.g. one nonce-only smoke test.

    Not a general claim that model-generated summaries contain no secrets.
    Default: no model text leaves the machine, even if it looks harmless.
    """
    exact_smoke_outputs: frozenset[str] = frozenset()

    def release(self, candidate: str) -> str | None:
        if not isinstance(candidate, str):
            return None
        # Structured summaries: schema re-validated, still scanned, never raw output.
        if candidate.startswith(SUMMARY_MARKER):
            if is_valid_rendered(candidate) and not SENSITIVE.search(candidate) \
                    and not SLACK_CONTROL.search(candidate):
                return candidate
            return None
        if candidate not in self.exact_smoke_outputs:
            return None
        try:
            if len(candidate.encode("utf-8")) > 1000:
                return None
        except UnicodeError:
            return None
        if SENSITIVE.search(candidate) or SLACK_CONTROL.search(candidate):
            return None
        return candidate


@dataclass(frozen=True)
class Pending:
    method: str
    key: ThreadKey | None = None
    command_id: str | None = None


class BridgeEngine:
    """Serialized policy/state actor with explicitly injected transports.

    Construct only after the operator's authorization gates. Constructors do
    not authenticate, connect, start processes or establish workspace trust.
    """
    def __init__(self, ledger: Ledger, policy: Policy, receiver_app_id: str,
                 configured_paths: dict[str, str], slack_transport, acp_transport,
                 output_policy: OutputPolicy | None = None):
        if (not isinstance(receiver_app_id, str)
                or not re.fullmatch(r"A[A-Z0-9_]{2,63}", receiver_app_id)
                or receiver_app_id == policy.dot_app_id):
            raise Rejected("bridge receiving app must be known and distinct from dot")
        self.ledger = ledger
        self.policy = policy
        self.receiver_app_id = receiver_app_id
        self.configured_paths = dict(configured_paths)
        self.output_policy = output_policy or OutputPolicy()
        self.acp = acp_transport
        self.slack = slack_transport
        self._owner_thread = threading.get_ident()
        self._next_id = 1
        self._pending: dict[int, Pending] = {}
        self._buffers: dict[str, str | None] = {}
        self._traces: dict[str, Trace] = {}
        self._scopes: dict[ThreadKey, TaskScope] = {}
        self._approved_calls: dict[str, tuple] = {}  # toolCallId -> ("exec",cmd)|("write",paths)
        self._input = bytearray()
        self._initialized = False
        self._protocol_failed = False
        self._load_session = False

    def _actor(self):
        if threading.get_ident() != self._owner_thread:
            raise Rejected("use one serialized actor thread for this engine")

    def _scope(self, key: ThreadKey):
        if key.team_id != self.policy.team_id or key.channel_id != self.policy.channel_id:
            raise Rejected("thread is outside this bridge's approved route")

    def initialize(self):
        self._actor()
        if self._initialized or any(p.method == "initialize" for p in self._pending.values()):
            raise Rejected("initialization already started")
        self._protocol_failed = False
        return self._rpc(acp_initialize(0), Pending("initialize"))

    def receive_socket_frame(self, frame: dict) -> bool:
        """Handle a frame provided only by the bound authenticated adapter.

        Frame contents alone NEVER prove authentication. Persist before ack.
        """
        self._actor()
        if not isinstance(frame, dict) or frame.get("type") != "events_api":
            return False
        envelope_id = frame.get("envelope_id")
        if (not isinstance(envelope_id, str) or len(envelope_id) > 128
                or not re.fullmatch(r"[A-Za-z0-9_-]+", envelope_id)):
            return False
        payload = frame.get("payload")
        if (not isinstance(payload, dict) or payload.get("team_id") != self.policy.team_id
                or payload.get("api_app_id") != self.receiver_app_id):
            return False
        try:
            command = parse_command(payload, self.policy)
            receipt = self.ledger.accept(command)
        except Rejected:
            self.slack.ack(envelope_id)  # Authenticated but irrelevant/invalid events are discarded.
            return False
        # Ledger failures are deliberately not caught: do not acknowledge an unpersisted command.
        self.slack.ack(envelope_id)
        if receipt.duplicate:
            return True
        key = ThreadKey(command.team_id, command.channel_id, command.thread_ts)
        if command.action == "cancel":
            self._control(key, command.request_id)
        elif command.action == "status":
            self._notice(key, command.request_id + ":status", "status")
        else:
            self._notice(key, command.request_id + ":accepted", "accepted")
        return True

    def dispatch(self, key: ThreadKey) -> bool:
        self._actor()
        self._scope(key)
        if not self._initialized:
            raise Rejected("ACP handshake is not complete")
        reserved = self.ledger.reserve_prompt(key)
        if reserved is None:
            return False
        if reserved.session_id:
            self._prompt(key, reserved.request_id, reserved.session_id, reserved.text)
        else:
            packet = acp_new_session(0, reserved.workspace_alias, self.configured_paths)
            self._rpc(packet, Pending("session/new", key, reserved.request_id))
        return True

    def restore(self, key: ThreadKey) -> bool:
        self._actor()
        self._scope(key)
        task = self.ledger.snapshot(key)
        if not self._initialized or task["state"] != "restoring":
            raise Rejected("session is not awaiting restoration")
        if not self._load_session:
            self._notice(key, "restore-unsupported", "restore")
            return False
        if any(p.method == "session/load" and p.key == key for p in self._pending.values()):
            return False
        params = acp_new_session(0, task["workspace_alias"], self.configured_paths)["params"]
        params["sessionId"] = task["session_id"]
        self._rpc({"jsonrpc": "2.0", "method": "session/load", "params": params},
                  Pending("session/load", key))
        return True

    def _prompt(self, key, command_id, session_id, text):
        self.ledger.begin_prompt_send(key, command_id)
        self._buffers[command_id] = ""
        self._traces[command_id] = Trace()
        request_id = self._rpc(acp_prompt(0, session_id, text + SUMMARY_CONTRACT),
                               Pending("session/prompt", key, command_id))
        if request_id is not None:
            self.ledger.mark_prompt_sent(key, command_id)
            self._notice(key, command_id + ":working", "working")

    def _control(self, key, cancel_id):
        task = self.ledger.snapshot(key)
        if task["state"] == "cancelled":
            self._notice(key, cancel_id + ":stopped", "stopped")
            return
        active_id = task["active_request_id"]
        active = self.ledger.command_state(key, active_id)
        if active["state"] == "prepared":
            self.ledger.abort_prepared(key, active_id)
            # Session may exist even though no prompt was sent; the runtime
            # still owes an exit confirmation before claiming "stopped".
            self._notice(key, cancel_id + ":stopped", "stopping")
            return
        reserved = self.ledger.reserve_cancel(key)
        if reserved:
            try:
                self.acp.write(acp_cancel(reserved.session_id))
            except OSError:
                self._freeze(key, active_id)
                self._protocol_failure()

    def _rpc(self, packet, pending):
        request_id = self._next_id; self._next_id += 1
        packet = dict(packet); packet["id"] = request_id
        self._pending[request_id] = pending
        try:
            self.acp.write(packet)
        except OSError:
            self._pending.pop(request_id, None)
            if pending.key and pending.command_id:
                self._freeze(pending.key, pending.command_id)
            self._protocol_failure()
            return None
        return request_id

    def feed_acp_bytes(self, data: bytes):
        """Parse bounded bytes from the bound current ACP transport generation."""
        self._actor()
        if not isinstance(data, bytes):
            raise Rejected("ACP input must be bytes")
        if len(self._input) + len(data) > 262144:
            self._protocol_failure()
            return
        self._input.extend(data)
        while b"\n" in self._input:
            line, _, remaining = self._input.partition(b"\n")
            self._input = bytearray(remaining)
            if not line.strip():
                continue
            try:
                packet = json.loads(line.decode("utf-8"), object_pairs_hook=_unique_keys)
                self._on_acp(packet)
            except (UnicodeError, ValueError, TypeError, RecursionError, KeyError, OSError):
                self._protocol_failure()
                return

    def _on_acp(self, packet):
        if not isinstance(packet, dict) or packet.get("jsonrpc") != "2.0":
            raise Rejected("invalid ACP packet")
        if "method" in packet:
            if "result" in packet or "error" in packet:
                raise Rejected("ambiguous ACP packet")
            if "id" in packet:
                self._agent_request(packet)
            elif packet["method"] == "session/update":
                self._update(packet.get("params"))
            return
        request_id = packet.get("id")
        if type(request_id) is not int or request_id not in self._pending:
            return  # Unknown or duplicate replies cannot attach to another task.
        pending = self._pending[request_id]
        if ("result" in packet) == ("error" in packet):
            raise Rejected("invalid ACP response")
        if "error" in packet:
            if pending.key and pending.command_id:
                self._freeze(pending.key, pending.command_id)
            else:
                self._protocol_failure()
            self._pending.pop(request_id, None)
            return
        result = packet["result"]
        if not isinstance(result, dict):
            raise Rejected("invalid ACP result")
        if pending.method == "initialize":
            if type(result.get("protocolVersion")) is not int or result["protocolVersion"] != 1:
                raise Rejected("unsupported ACP version")
            caps = result.get("agentCapabilities", {})
            if not isinstance(caps, dict):
                raise Rejected("invalid agent capabilities")
            self._load_session = caps.get("loadSession") is True
            self._initialized = True
        elif pending.method == "session/new":
            task = self.ledger.snapshot(pending.key)
            if task["state"] == "cancelled":
                self._pending.pop(request_id, None)
                return  # No prompt was sent. Live runtime must clean up the idle agent session.
            session_id = result.get("sessionId")
            self.ledger.bind_session(pending.key, pending.command_id, session_id)
            command = self.ledger.command_state(pending.key, pending.command_id)
            self._prompt(pending.key, pending.command_id, session_id, command["text"])
        elif pending.method == "session/load":
            task = self.ledger.snapshot(pending.key)
            self.ledger.confirm_restored(pending.key, task["session_id"])
        elif pending.method == "session/prompt":
            self.ledger.finish_turn(pending.key, pending.command_id, result.get("stopReason"))
            candidate = self._buffers.pop(pending.command_id, None)
            task = self.ledger.snapshot(pending.key)
            reason = result.get("stopReason")
            status = ("cancelled" if task["state"] == "cancelled" or reason == "cancelled"
                      else "done" if reason in ("end_turn", "max_tokens") else "failed")
            self._queue_summary(pending.key, pending.command_id, status, candidate)
            if task["state"] == "cancelled":
                # Exit confirmation is a separate runtime step; not assumed here.
                self._notice(pending.key, pending.command_id + ":final", "stopping")
            else:
                released = self.output_policy.release(candidate) if candidate else None
                if released is None:
                    self._notice(pending.key, pending.command_id + ":final", "complete")
                else:
                    self.ledger.queue_outbox(pending.key, pending.command_id + ":final", released)
        self._pending.pop(request_id, None)

    def _agent_request(self, packet):
        request_id = packet["id"]
        if type(request_id) not in (int, str) or isinstance(request_id, str) and len(request_id) > 128:
            raise Rejected("invalid agent request id")
        if packet["method"] != "session/request_permission":
            self.acp.write({"jsonrpc": "2.0", "id": request_id,
                            "error": {"code": -32601, "message": "Client capability is not supported"}})
            return
        params = packet.get("params")
        if not isinstance(params, dict) or not isinstance(params.get("sessionId"), str):
            raise Rejected("invalid permission request")
        key = self.ledger.thread_for_session(self.policy.team_id, params["sessionId"])
        if key is None:
            self.acp.write(deny_permission(request_id))
            return
        self._scope(key)
        task = self.ledger.snapshot(key)
        active = task["active_request_id"]
        decision = self._evaluate_scope(key, task, params.get("toolCall"))
        if decision.approved:
            option = self._pick_allow_option(params.get("options"))
            if option is not None:
                self._record_approval(key, active, params["toolCall"], decision)
                self.acp.write(approve_permission(request_id, option))
                return
            decision = decision.__class__(False, decision.kind,
                                          decision.paths, decision.command)
        trace = self._traces.get(active) or self._traces.setdefault(active, Trace())
        trace.record_denial(decision.kind)
        if active:
            # One notice per command; denied operation pauses it, task stays bound.
            self._notice(key, active + ":scope", "out_of_scope")
        self.acp.write(deny_permission(request_id))

    def _evaluate_scope(self, key, task, tool_call):
        """Rule-check a permission request. Never trusts titles or bot text."""
        if key not in self._scopes:
            raw = json.loads(task.get("scope_json") or "null")
            self._scopes[key] = TaskScope.parse(raw)
        workspace = self.configured_paths.get(task["workspace_alias"], "/")
        return self._scopes[key].evaluate(workspace, tool_call)

    @staticmethod
    def _pick_allow_option(options):
        if not isinstance(options, list):
            return None
        for option in options:
            if isinstance(option, dict) and option.get("kind") == "allow_once" \
                    and isinstance(option.get("optionId"), str):
                return option["optionId"]
        for option in options:
            if isinstance(option, dict) and str(option.get("kind", "")).startswith("allow") \
                    and isinstance(option.get("optionId"), str):
                return option["optionId"]
        return None

    def _record_approval(self, key, command_id, tool_call, decision):
        trace = self._traces.get(command_id)
        if trace is None:
            return
        tool_call_id = tool_call.get("toolCallId") if isinstance(tool_call, dict) else None
        if decision.kind == "exec" and decision.command:
            trace.record_test(decision.command)
            if isinstance(tool_call_id, str):
                self._approved_calls[tool_call_id] = (command_id, "exec", decision.command)
        elif decision.kind == "write":
            for rel in decision.paths:
                trace.record_file(rel)
            if isinstance(tool_call_id, str):
                self._approved_calls[tool_call_id] = (command_id, "write", decision.paths)

    def _update(self, params):
        if not isinstance(params, dict) or not isinstance(params.get("sessionId"), str):
            raise Rejected("invalid session update")
        key = self.ledger.thread_for_session(self.policy.team_id, params["sessionId"])
        if key is None:
            return
        self._scope(key)
        task = self.ledger.snapshot(key)
        command_id = task["active_request_id"]
        if not command_id or command_id not in self._buffers:
            return
        update = params.get("update")
        if not isinstance(update, dict):
            raise Rejected("invalid update shape")
        # Hidden reasoning, raw tool output, files and terminal content are ignored.
        kind = update.get("sessionUpdate")
        if kind in ("tool_call", "tool_call_update"):
            self._trace_tool_update(command_id, update)
            return
        if kind != "agent_message_chunk":
            return
        content = update.get("content")
        if not isinstance(content, dict) or content.get("type") != "text":
            return
        text = content.get("text")
        if not isinstance(text, str):
            raise Rejected("invalid text update")
        previous = self._buffers.get(command_id)
        if previous is None:
            return
        candidate = previous + text
        if len(candidate.encode("utf-8")) > 65536:
            self._buffers[command_id] = None
        else:
            self._buffers[command_id] = candidate

    def _trace_tool_update(self, command_id, update):
        """Record observed outcomes for scope-approved calls only; no raw output."""
        call_id = update.get("toolCallId")
        if not isinstance(call_id, str):
            return
        entry = self._approved_calls.pop(call_id, None)
        if entry is None or entry[0] != command_id:
            return
        trace = self._traces.get(command_id)
        if trace is None:
            return
        status = update.get("status")
        if entry[1] == "exec" and isinstance(status, str):
            trace.record_test(entry[2], status)
        elif entry[1] == "write" and status == "completed":
            for rel in entry[2]:
                trace.record_file(rel)

    def _queue_summary(self, key, command_id, status, candidate):
        trace = self._traces.pop(command_id, Trace())
        model = parse_model_summary(candidate) if candidate else None
        try:
            text = render_summary(status, trace, model)
        except Rejected:
            text = None
        # Validate BEFORE queueing: an unreleasable summary never enters the
        # outbox, so it can never block later notices at flush time.
        if text is None or self.output_policy.release(text) is None:
            self._notice(key, command_id + ":summary", "summary_withheld")
            return
        self.ledger.queue_outbox(key, command_id + ":summary", text)

    def confirm_exit(self, key, verified: bool):
        """Runtime calls this after observing the task's process end state.

        A cancelled task does NOT claim the devin process exited until the
        process supervisor reports it; anything else is unverified.
        """
        self._actor()
        self._scope(key)
        if type(verified) is not bool:
            raise Rejected("exit verification must be an observed boolean")
        task = self.ledger.snapshot(key)
        if task["state"] != "cancelled":
            raise Rejected("task is not awaiting exit confirmation")
        if not self.ledger.outbox_exists(key, "cancel:exit"):
            self._notice(key, "cancel:exit",
                         "stopped_verified" if verified else "stopped_unverified")

    def _notice(self, key, dedupe_key, kind):
        self._scope(key)
        self.ledger.queue_outbox(key, dedupe_key, NOTICES[kind])

    def _freeze(self, key, command_id):
        self._scope(key)
        self.ledger.freeze_active(key, command_id)
        self._buffers.pop(command_id, None)
        self._traces.pop(command_id, None)
        self._notice(key, command_id + ":uncertain", "uncertain")

    def _protocol_failure(self):
        self._initialized = False
        self._protocol_failed = True
        self._input.clear()
        self.ledger.invalidate_acp_route(self.policy.team_id, self.policy.channel_id)
        self._pending.clear()
        self._buffers.clear()
        self._traces.clear()
        self._approved_calls.clear()

    def _repair_notices(self):
        """Rebuild missing STATIC intents after a crash between state/outbox commits.

        Never reconstruct model output or reset sent/uncertain outbox rows.
        If an exact smoke-test result was not committed, fallback is a static
        local-review notice, not an invented or repeated result.
        """
        def ensure(key, dedupe_key, kind):
            if not self.ledger.outbox_exists(key, dedupe_key):
                self._notice(key, dedupe_key, kind)

        for command in self.ledger.commands_in_route(self.policy.team_id, self.policy.channel_id):
            key = ThreadKey(command["team_id"], command["channel_id"], command["thread_ts"])
            request_id = command["request_id"]
            action, state, reason = command["action"], command["state"], command["stop_reason"]
            if action in {"run", "reply"}:
                ensure(key, request_id + ":accepted", "accepted")
                if state == "running" or reason and reason != "never_sent":
                    ensure(key, request_id + ":working", "working")
                if state in {"done", "cancelled"} and reason and reason != "never_sent":
                    task = self.ledger.snapshot(key)
                    kind = "stopping" if task["state"] == "cancelled" else "complete"
                    ensure(key, request_id + ":final", kind)
                elif state == "uncertain":
                    ensure(key, request_id + ":uncertain", "uncertain")
            elif action == "status":
                ensure(key, request_id + ":status", "status")
            elif action == "cancel" and state == "done" and reason in {None, "never_sent"}:
                ensure(key, request_id + ":stopped", "stopped")

    def reconcile_exits(self):
        """Startup-only reconciliation: cancelled tasks whose exit was never
        confirmed are reported UNVERIFIED — a restart cannot prove the old
        process died. Not part of _repair_notices, which runs on every flush
        and must not race a live exit confirmation."""
        self._actor()
        for task in self.ledger.tasks_in_route(self.policy.team_id, self.policy.channel_id):
            key = ThreadKey(task["team_id"], task["channel_id"], task["thread_ts"])
            if task["state"] == "cancelled" and not self.ledger.outbox_exists(key, "cancel:exit"):
                self._notice(key, "cancel:exit", "stopped_unverified")

    def flush_one_slack(self) -> bool:
        """Send once via the adapter; uncertain results are never auto-retried."""
        self._actor()
        self._repair_notices()
        item = self.ledger.reserve_outbox(self.policy.team_id, self.policy.channel_id)
        if item is None:
            return False
        key = ThreadKey(item["team_id"], item["channel_id"], item["thread_ts"])
        self.ledger.snapshot(key)  # Recheck the exact bound conversation.
        text = item["text"]
        if text not in NOTICES.values() and self.output_policy.release(text) is None:
            self.ledger.finish_outbox(item["id"], None)
            return False
        try:
            reply = self.slack.post({"channel": key.channel_id, "thread_ts": key.thread_ts,
                                     "text": text, "unfurl_links": False, "unfurl_media": False,
                                     "parse": "none"})
            valid = (reply.get("ok") is True and reply.get("channel") == key.channel_id
                     and isinstance(reply.get("ts"), str) and SLACK_TS.fullmatch(reply["ts"]))
            self.ledger.finish_outbox(item["id"], reply["ts"] if valid else None)
        except OSError:
            self.ledger.finish_outbox(item["id"], None)
        return True

    def transport_lost(self):
        """Invalidate pending and idle sessions after transport failure; no replay."""
        self._actor()
        self._protocol_failure()

    @property
    def initialized(self):
        return self._initialized

    @property
    def protocol_failed(self):
        return self._protocol_failed
