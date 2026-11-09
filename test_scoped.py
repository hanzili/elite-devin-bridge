"""Offline tests for per-task scope authorization, structured summaries and
verified cancellation. Standard library only; no Slack, no real devin."""
import json
import unittest
from unittest.mock import patch

from bridge_core import Rejected, parse_command
from bridge_store import Ledger
from offline_wiring import OfflineBridge
from result_summary import (SUMMARY_CONTRACT, SUMMARY_MARKER, Trace,
                            is_valid_rendered, parse_model_summary, render_summary)
from runtime_scoped import poll_process_exit
from task_scope import TaskScope
from test_bridge_core import POLICY
from test_bridge_store import KEY, ROOT


def scoped_frame(n=1, scope=None, text="work"):
    body = {"request_id": f"request_{n:03}", "action": "run",
            "workspace": "scratch", "text": text}
    if scope is not None:
        body["scope"] = scope
    event = {"type": "message", "channel": "C_TEST", "user": "U_OWNER",
             "ts": ROOT, "text": "<@U_WORKER> bridge.v1 " + json.dumps(body)}
    return {"type": "events_api", "envelope_id": f"envelope-{n}", "payload": {
        "type": "event_callback", "team_id": "T_TEST", "api_app_id": "A_WORKER",
        "event_id": f"Ev{n}", "event": event}}


def thread_frame(n, action="cancel"):
    body = {"request_id": f"request_{n:03}", "action": action}
    event = {"type": "message", "channel": "C_TEST", "user": "U_OWNER",
             "ts": f"1700000001.{n:06}", "thread_ts": ROOT,
             "text": "bridge.v1 " + json.dumps(body)}
    return {"type": "events_api", "envelope_id": f"env-t{n}", "payload": {
        "type": "event_callback", "team_id": "T_TEST", "api_app_id": "A_WORKER",
        "event_id": f"Evt{n}", "event": event}}


SCOPE = {"writable": ["src/**", "README.md"], "tests": ["pytest -x", "make test"]}


class ScopeParseTests(unittest.TestCase):
    def test_valid_scope(self):
        s = TaskScope.parse(SCOPE)
        self.assertEqual(s.writable, ("src/**", "README.md"))
        self.assertEqual(s.tests, ("pytest -x", "make test"))

    def test_none_scope_is_deny_all(self):
        s = TaskScope.parse(None)
        self.assertFalse(s.writable and s.tests)

    def test_unknown_keys_and_bad_entries_rejected(self):
        for bad in ({"evil": []}, {"writable": ["/abs"]}, {"writable": [".."]},
                    {"writable": ["../x"]}, {"writable": ["~/.ssh"]},
                    {"tests": ["pytest; rm -rf /"]}, {"tests": ["pytest && id"]},
                    {"tests": ["pytest | tee x"]}, {"tests": ["pytest $(id)"]},
                    {"tests": ["x" * 400]}, {"writable": ["a"] * 40}):
            with self.subTest(bad=bad), self.assertRaises(Rejected):
                TaskScope.parse(bad)

    def test_file_allowed(self):
        s = TaskScope.parse(SCOPE)
        self.assertEqual(s.file_allowed("/ws", "/ws/src/a/b.py"), "src/a/b.py")
        self.assertEqual(s.file_allowed("/ws", "src/x.py"), "src/x.py")
        self.assertEqual(s.file_allowed("/ws", "README.md"), "README.md")
        self.assertIsNone(s.file_allowed("/ws", "/ws/docs/x.md"))
        self.assertIsNone(s.file_allowed("/ws", "/etc/passwd"))
        self.assertIsNone(s.file_allowed("/ws", "/ws/../etc/passwd"))
        self.assertIsNone(s.file_allowed("/ws", "src/../../etc/passwd"))

    def test_command_exact_match_not_prefix(self):
        s = TaskScope.parse(SCOPE)
        self.assertTrue(s.command_allowed("pytest -x"))
        self.assertTrue(s.command_allowed("pytest  -x"))     # whitespace normalized
        self.assertTrue(s.command_allowed('pytest "-x"'))
        self.assertFalse(s.command_allowed("pytest"))        # missing arg
        self.assertFalse(s.command_allowed("pytest -x foo")) # extra arg
        self.assertFalse(s.command_allowed("pytest -x; id"))
        self.assertFalse(s.command_allowed("pytest -x && id"))
        self.assertFalse(s.command_allowed("pytest -x > /tmp/o"))
        self.assertFalse(s.command_allowed("pytest -x$(id)"))
        self.assertFalse(s.command_allowed(""))

    def test_evaluate(self):
        s = TaskScope.parse(SCOPE)
        ws = "/ws"
        ok_exec = s.evaluate(ws, {"kind": "execute", "rawInput": {"command": "pytest -x"}})
        self.assertTrue(ok_exec.approved)
        bad_exec = s.evaluate(ws, {"kind": "execute", "rawInput": {"command": "rm -rf /"}})
        self.assertFalse(bad_exec.approved)
        ok_write = s.evaluate(ws, {"kind": "edit", "locations": [{"path": "/ws/src/f.py"}]})
        self.assertTrue(ok_write.approved)
        self.assertEqual(ok_write.paths, ("src/f.py",))
        bad_write = s.evaluate(ws, {"kind": "edit", "rawInput": {"file_path": "/etc/hosts"}})
        self.assertFalse(bad_write.approved)
        self.assertFalse(s.evaluate(ws, {"kind": "fetch", "rawInput": {"url": "x"}}).approved)
        self.assertFalse(s.evaluate(ws, "not-a-dict").approved)


class SummaryTests(unittest.TestCase):
    def test_parse_model_summary(self):
        text = ('done\n```json\n{"bridge_result_v1": {"tests": '
                '[{"name": "pytest -x", "result": "pass"}], '
                '"remaining_issues": "none"}}\n```')
        model = parse_model_summary(text)
        self.assertEqual(model["tests"][0]["name"], "pytest -x")
        self.assertEqual(model["remaining_issues"], "none")

    def test_bad_model_blocks_rejected(self):
        for text in ('```json {"bridge_result_v1": {"tests": "x"}} ```',
                     '```json {"bridge_result_v1": {"extra": 1}} ```',
                     '```json {"bridge_result_v1": {"remaining_issues": "a\nb"}} ```',
                     "no block at all"):
            self.assertIsNone(parse_model_summary(text))

    def test_render_and_validate(self):
        t = Trace()
        t.record_file("src/f.py")
        t.record_test("pytest -x", "completed")
        t.record_denial("write")
        model = {"tests": [{"name": "make test", "result": "fail"}],
                 "remaining_issues": "flaky network test"}
        out = render_summary("done", t, model)
        self.assertTrue(is_valid_rendered(out))
        self.assertIn("files_modified: src/f.py", out)
        self.assertIn("make test -> fail", out)
        self.assertIn("pytest -x -> pass", out)
        self.assertIn("scope_denials: 1", out)

    def test_invalid_render_rejected(self):
        self.assertFalse(is_valid_rendered("bridge.summary.v1\nstatus: pwned"))
        self.assertFalse(is_valid_rendered("random text"))
        t = Trace()
        out = render_summary("done", t)
        self.assertFalse(is_valid_rendered(out + "\nextra"))


class FakeProc:
    def __init__(self, exits=True, after=0):
        self._exits = exits
        self._after = after
        self._polls = 0

    def poll(self):
        self._polls += 1
        if not self._exits:
            return None
        self._after -= 1
        return 0 if self._after < 0 else None


class RuntimeScopedTests(unittest.TestCase):
    def test_poll_exit_true_and_false(self):
        self.assertTrue(poll_process_exit(FakeProc(after=1), timeout=1,
                                          sleeper=lambda s: None))
        self.assertFalse(poll_process_exit(FakeProc(exits=False), timeout=0.2,
                                           sleeper=lambda s: None))


class ScopedEngineTests(unittest.TestCase):
    def setUp(self):
        self.ledger = Ledger(":memory:")
        self.bridge = OfflineBridge(self.ledger, POLICY, "A_WORKER",
                                    {"scratch": "/sandbox/demo"})

    def tearDown(self):
        self.ledger.close()

    def feed(self, packet):
        self.bridge.feed_acp_fixture((json.dumps(packet) + "\n").encode())

    def handshake(self):
        rid = self.bridge.initialize()
        self.feed({"jsonrpc": "2.0", "id": rid,
                   "result": {"protocolVersion": 1, "agentCapabilities": {}}})

    def start_task(self, scope=SCOPE):
        self.handshake()
        self.bridge.receive_socket_fixture(scoped_frame(scope=scope))
        self.bridge.dispatch(KEY)
        rid = self.bridge.acp.packets()[-1]["id"]
        self.feed({"jsonrpc": "2.0", "id": rid, "result": {"sessionId": "sess-1"}})
        return self.bridge.acp.packets()[-1]["id"]  # prompt request id

    def permission(self, tool_call, req_id="perm-1"):
        self.feed({"jsonrpc": "2.0", "id": req_id,
                   "method": "session/request_permission",
                   "params": {"sessionId": "sess-1", "toolCall": tool_call,
                              "options": [{"optionId": "o1", "kind": "allow_once"},
                                          {"optionId": "o2", "kind": "reject_once"}]}})

    def flush(self):
        while self.bridge.flush_one_slack_fixture():
            pass
        return [m["text"] for m in self.bridge.slack.messages]

    def test_scope_persisted_in_command_and_task(self):
        cmd = parse_command(scoped_frame(scope=SCOPE)["payload"], POLICY)
        self.assertEqual(cmd.scope, SCOPE)
        self.bridge.receive_socket_fixture(scoped_frame(scope=SCOPE))
        self.assertIn("src/**", self.ledger.snapshot(KEY)["scope_json"])

    def test_scope_only_on_run(self):
        cmd_frame = thread_frame(9, "cancel")
        cmd_frame["payload"]["event"]["text"] = (
            'bridge.v1 {"request_id": "request_009", "action": "cancel", "scope": {}}')
        self.assertFalse(self.bridge.receive_socket_fixture(cmd_frame))

    def test_malformed_scope_rejected_at_parse(self):
        bad = scoped_frame(scope={"writable": ["../evil"]})
        self.assertFalse(self.bridge.receive_socket_fixture(bad))

    def test_prompt_carries_summary_contract(self):
        self.start_task()
        prompt = self.bridge.acp.packets()[-1]
        self.assertEqual(prompt["method"], "session/prompt")
        sent = prompt["params"]["prompt"][0]["text"]
        self.assertIn("work", sent)
        self.assertIn("bridge_result_v1", sent)

    def test_in_scope_exec_approved(self):
        self.start_task()
        self.permission({"toolCallId": "t1", "kind": "execute",
                         "rawInput": {"command": "pytest -x"}})
        reply = self.bridge.acp.packets()[-1]
        self.assertEqual(reply["id"], "perm-1")
        self.assertEqual(reply["result"]["outcome"]["outcome"], "selected")
        self.assertEqual(reply["result"]["outcome"]["optionId"], "o1")

    def test_out_of_scope_denied_and_noticed(self):
        self.start_task()
        self.permission({"toolCallId": "t1", "kind": "execute",
                         "rawInput": {"command": "rm -rf /"}})
        reply = self.bridge.acp.packets()[-1]
        self.assertEqual(reply["result"]["outcome"]["outcome"], "cancelled")
        texts = self.flush()
        self.assertTrue(any("outside this task's approved scope" in t for t in texts))

    def test_no_scope_denies_everything(self):
        self.start_task(scope=None)
        self.permission({"toolCallId": "t1", "kind": "edit",
                         "locations": [{"path": "/sandbox/demo/src/f.py"}]})
        reply = self.bridge.acp.packets()[-1]
        self.assertEqual(reply["result"]["outcome"]["outcome"], "cancelled")

    def test_write_scope_paths(self):
        self.start_task()
        self.permission({"toolCallId": "w1", "kind": "edit",
                         "locations": [{"path": "/sandbox/demo/src/a.py"}]}, "p-ok")
        self.assertEqual(self.bridge.acp.packets()[-1]["result"]["outcome"]["outcome"],
                         "selected")
        self.permission({"toolCallId": "w2", "kind": "edit",
                         "locations": [{"path": "/etc/passwd"}]}, "p-bad")
        self.assertEqual(self.bridge.acp.packets()[-1]["result"]["outcome"]["outcome"],
                         "cancelled")

    def test_summary_reaches_slack(self):
        prompt_id = self.start_task()
        self.permission({"toolCallId": "t1", "kind": "execute",
                         "rawInput": {"command": "pytest -x"}}, "p1")
        self.feed({"jsonrpc": "2.0", "method": "session/update", "params": {
            "sessionId": "sess-1", "update": {"sessionUpdate": "tool_call_update",
                "toolCallId": "t1", "status": "completed"}}})
        self.permission({"toolCallId": "w1", "kind": "edit",
                         "locations": [{"path": "/sandbox/demo/src/a.py"}]}, "p2")
        self.feed({"jsonrpc": "2.0", "method": "session/update", "params": {
            "sessionId": "sess-1", "update": {"sessionUpdate": "tool_call_update",
                "toolCallId": "w1", "status": "completed"}}})
        self.permission({"toolCallId": "w2", "kind": "edit",
                         "locations": [{"path": "/etc/passwd"}]}, "p3")
        self.feed({"jsonrpc": "2.0", "method": "session/update", "params": {
            "sessionId": "sess-1", "update": {"sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text":
                    'done\n```json\n{"bridge_result_v1": {"remaining_issues": "docs TODO"}}\n```'}}}})
        self.feed({"jsonrpc": "2.0", "id": prompt_id, "result": {"stopReason": "end_turn"}})
        texts = self.flush()
        summary = next(t for t in texts if t.startswith(SUMMARY_MARKER))
        self.assertIn("status: done", summary)
        self.assertIn("files_modified: src/a.py", summary)
        self.assertIn("pytest -x -> pass", summary)
        self.assertIn("scope_denials: 1", summary)
        self.assertIn("remaining_issues: docs TODO", summary)

    def test_cancel_reports_stopping_then_exit_verdict(self):
        prompt_id = self.start_task()
        self.bridge.receive_socket_fixture(thread_frame(2, "cancel"))
        self.feed({"jsonrpc": "2.0", "id": prompt_id, "result": {"stopReason": "cancelled"}})
        texts = self.flush()
        self.assertTrue(any("Waiting for confirmation" in t for t in texts))
        self.bridge.confirm_exit(KEY, True)
        texts = self.flush()
        self.assertTrue(any("process exit was confirmed" in t for t in texts))

    def test_cancel_unverified_when_exit_unobserved(self):
        prompt_id = self.start_task()
        self.bridge.receive_socket_fixture(thread_frame(2, "cancel"))
        self.feed({"jsonrpc": "2.0", "id": prompt_id, "result": {"stopReason": "cancelled"}})
        self.bridge.confirm_exit(KEY, False)
        texts = self.flush()
        self.assertTrue(any("not confirmed" in t for t in texts))
        # A second confirm call cannot flip the recorded verdict
        self.bridge.confirm_exit(KEY, True)
        texts = self.flush()
        self.assertEqual(sum("exit was confirmed" in t for t in texts), 0)

    def test_restart_freezes_and_never_reruns(self):
        self.start_task()
        self.ledger.recover_inflight()
        bridge2 = OfflineBridge(self.ledger, POLICY, "A_WORKER",
                                {"scratch": "/sandbox/demo"})
        rid = bridge2.initialize()
        bridge2.feed_acp_fixture((json.dumps({
            "jsonrpc": "2.0", "id": rid, "result": {
                "protocolVersion": 1, "agentCapabilities": {}}}) + "\n").encode())
        self.assertFalse(bridge2.dispatch(KEY))
        task = self.ledger.snapshot(KEY)
        self.assertEqual(task["state"], "uncertain")
        # Uncertain tasks accept no control or prompt commands
        self.assertFalse(self.bridge.receive_socket_fixture(thread_frame(2, "reply")))


if __name__ == "__main__":
    unittest.main(verbosity=2)
