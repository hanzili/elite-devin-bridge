"""All adapter calls use fakes. No real SDK, credential, socket or subprocess."""
import io
import json
import signal
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from bridge_core import Rejected
from bridge_engine import BridgeEngine, NOTICES, OutputPolicy
from bridge_store import Ledger
from slack_adapter import SlackSocketAdapter, build_sdk_adapter, AdapterFailure
from acp_adapter import LaunchPlan, AcpProcessAdapter, CleanupResult, FdPipeIO, stop_owned_process_group
from runtime_pump import RuntimePump
from test_bridge_core import POLICY
from test_bridge_store import KEY, ROOT
from test_offline_wiring import frame


class FakeWeb:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
        self.auth = {"ok": True, "team_id": "T_TEST", "user_id": "U_WORKER", "bot_id": "B_WORKER"}
        self.fail_post = False

    def auth_test(self):
        self.calls.append("auth_test")
        return dict(self.auth)

    def chat_postMessage(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail_post:
            raise RuntimeError("FAKE-SECRET-PAYLOAD must not escape")
        return {"ok": True, "channel": kwargs["channel"], "ts": "1800000000.000001",
                "PRIVATE_EXTRA": "Do not return SDK details"}


class FakeSocket:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.auto_reconnect_enabled = True
        self.socket_mode_request_listeners = []
        self.calls = []
        self.fail_ack = False

    def connect(self):
        self.calls.append("connect")

    def close(self):
        self.calls.append("close")

    def send_socket_mode_response(self, response):
        self.calls.append(response)
        if self.fail_ack:
            raise RuntimeError("FAKE-SECRET-ACK")

    def emit(self, value, client=None):
        request = SimpleNamespace(**value)
        for listener in self.socket_mode_request_listeners:
            listener(self if client is None else client, request)


class FakeIO:
    def __init__(self, chunk=100000):
        self.chunk = chunk
        self.writes = []
        self.reads = []
        self.fail_after = None

    def write(self, data, timeout):
        if self.fail_after is not None and len(self.writes) >= self.fail_after:
            raise OSError("FAKE-PRIVATE-WRITE-DIAGNOSTIC")
        n = min(self.chunk, len(data))
        self.writes.append(data[:n])
        return n

    def read(self, limit, timeout):
        return self.reads.pop(0) if self.reads else None

    def packets(self):
        return [json.loads(line) for line in b"".join(self.writes).splitlines()]


class FakeProcess:
    def __init__(self):
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO()
        self.pid = 123456789


def fake_acp(io_backend=None):
    io_backend = io_backend or FakeIO()
    proc = FakeProcess()
    stopped = []
    def stop(process):
        assert process is proc
        stopped.append(process)
        return CleanupResult(True, False)
    return AcpProcessAdapter(proc, io_backend, stop, 1.0, "/sandbox/demo"), io_backend, stopped


def fake_slack(output=None):
    web, socket = FakeWeb(), FakeSocket()
    return SlackSocketAdapter(POLICY, "A_WORKER", web, socket,
                              lambda **kwargs: kwargs, output), web, socket


class SlackAdapterTests(unittest.TestCase):
    def test_construction_is_inert(self):
        adapter, web, socket = fake_slack()
        self.assertEqual(web.calls, [])
        self.assertEqual(socket.calls, [])
        self.assertNotIn("token", repr(adapter).lower())

    def test_sdk_factory_is_lazy_and_disables_logging_retries(self):
        adapter = build_sdk_adapter(POLICY, "A_WORKER", "FAKE-BOT", "FAKE-APP",
                                    sdk_classes=(FakeWeb, FakeSocket, lambda **kwargs: kwargs))
        self.assertEqual(adapter._web.kwargs["retry_handlers"], [])
        self.assertTrue(adapter._web.kwargs["logger"].disabled)
        self.assertFalse(adapter._socket.auto_reconnect_enabled)
        self.assertEqual(adapter._web.calls, [])
        self.assertEqual(adapter._socket.calls, [])

    def test_missing_sdk_reconnect_switch_fails_closed(self):
        built = []
        class Unsupported(FakeSocket):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                built.append(self)
                del self.auto_reconnect_enabled
        with self.assertRaises(AdapterFailure):
            build_sdk_adapter(POLICY, "A_WORKER", "FAKE-BOT", "FAKE-APP",
                              sdk_classes=(FakeWeb, Unsupported, dict))
        self.assertEqual(built[0].calls, ["close"])

    def test_invalid_adapter_identity_closes_constructed_socket(self):
        built = []
        class Tracked(FakeSocket):
            def __init__(self, **kwargs):
                super().__init__(**kwargs); built.append(self)
        with self.assertRaises(AdapterFailure):
            build_sdk_adapter(POLICY, "A_DOT", "FAKE-BOT", "FAKE-APP",
                              sdk_classes=(FakeWeb, Tracked, dict))
        self.assertEqual(built[0].calls, ["close"])

    def test_wrong_authenticated_identity_never_connects(self):
        for key in ("team_id", "user_id", "bot_id"):
            adapter, web, socket = fake_slack()
            web.auth[key] = "WRONG"
            with self.subTest(key=key), self.assertRaises(AdapterFailure):
                adapter.start(lambda value: True)
            self.assertNotIn("connect", socket.calls)

    def test_bound_sdk_callback_only_enqueues(self):
        adapter, web, socket = fake_slack()
        received = []
        adapter.start(lambda value: received.append(value) or True)
        socket.emit(frame())
        self.assertEqual(received, [frame()])
        self.assertEqual(socket.calls, ["connect"])
        self.assertEqual(web.calls, ["auth_test"])

    def test_foreign_callback_object_rejected(self):
        adapter, web, socket = fake_slack()
        received = []
        adapter.start(lambda value: received.append(value) or True)
        socket.emit(frame(), client=object())
        self.assertEqual(received, [])

    def test_frame_copy_cannot_change_after_enqueue(self):
        adapter, web, socket = fake_slack()
        received = []
        adapter.start(lambda value: received.append(value) or True)
        value = frame(); socket.emit(value)
        value["payload"]["event"]["text"] = "MUTATED"
        self.assertNotEqual(received[0]["payload"]["event"]["text"], "MUTATED")

    def test_wrong_receiving_app_faults_without_ack(self):
        adapter, web, socket = fake_slack()
        received = []
        adapter.start(lambda value: received.append(value) or True)
        value = frame(); value["payload"]["api_app_id"] = "A_OTHER"
        socket.emit(value)
        self.assertEqual(adapter.fault, "slack_route_mismatch")
        self.assertEqual(received, [])
        self.assertEqual(socket.calls, ["connect"])

    def test_queue_overflow_does_not_ack(self):
        adapter, web, socket = fake_slack()
        adapter.start(lambda value: False)
        socket.emit(frame())
        self.assertEqual(adapter.fault, "slack_input_queue_full")
        self.assertEqual(socket.calls, ["connect"])

    def test_oversized_frame_faults(self):
        adapter, web, socket = fake_slack()
        adapter.start(lambda value: True)
        socket.emit(frame(text="a" * 70000))
        self.assertEqual(adapter.fault, "slack_frame_too_large")

    def test_ack_has_only_envelope_id(self):
        adapter, web, socket = fake_slack()
        adapter.start(lambda value: True)
        adapter.ack("envelope-1")
        self.assertEqual(socket.calls[-1], {"envelope_id": "envelope-1"})

    def test_ack_error_is_sanitized_and_not_retried(self):
        adapter, web, socket = fake_slack()
        adapter.start(lambda value: True); socket.fail_ack = True
        with self.assertRaises(AdapterFailure) as raised:
            adapter.ack("envelope-1")
        self.assertNotIn("FAKE-SECRET", str(raised.exception))
        self.assertEqual(len(socket.calls), 2)

    def test_post_scope_output_and_safe_format(self):
        adapter, web, socket = fake_slack()
        adapter.start(lambda value: True)
        result = adapter.post({"channel": "C_TEST", "thread_ts": ROOT, "text": NOTICES["accepted"]})
        self.assertEqual(set(result), {"ok", "channel", "ts"})
        request = web.calls[-1]
        self.assertFalse(request["mrkdwn"])
        self.assertFalse(request["unfurl_links"])
        self.assertEqual(request["parse"], "none")

    def test_post_unapproved_text_or_channel_or_impersonation_never_calls_api(self):
        for changes in ({"text": "PRIVATE"}, {"channel": "C_OTHER"}, {"username": "Hanzi"}):
            adapter, web, socket = fake_slack(); adapter.start(lambda value: True)
            payload = {"channel": "C_TEST", "thread_ts": ROOT, "text": NOTICES["accepted"]}
            payload.update(changes)
            with self.subTest(changes=changes), self.assertRaises(AdapterFailure):
                adapter.post(payload)
            self.assertEqual(web.calls, ["auth_test"])

    def test_post_error_never_retries_and_masks_exception(self):
        adapter, web, socket = fake_slack(); adapter.start(lambda value: True)
        web.fail_post = True
        with self.assertRaises(AdapterFailure) as raised:
            adapter.post({"channel": "C_TEST", "thread_ts": ROOT, "text": NOTICES["accepted"]})
        self.assertNotIn("FAKE-SECRET", str(raised.exception))
        self.assertEqual(len(web.calls), 2)
        self.assertEqual(adapter.fault, "slack_send_uncertain")

    def test_transport_cannot_restart_after_close(self):
        adapter, web, socket = fake_slack(); adapter.start(lambda value: True)
        adapter.close()
        with self.assertRaises(Rejected):
            adapter.start(lambda value: True)

    def test_slack_effects_cannot_run_on_callback_thread(self):
        adapter, web, socket = fake_slack(); adapter.start(lambda value: True)
        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.assertRaises(Rejected):
                pool.submit(adapter.ack, "envelope-1").result()


class AcpAdapterTests(unittest.TestCase):
    def plan(self, **kwargs):
        values = {"executable": "/approved/bin/devin", "workspace": "/approved/workspace",
                  "environment": {"HOME": "/approved/home", "PATH": "/approved/bin:/usr/bin"}}
        values.update(kwargs)
        return LaunchPlan(**values)

    def test_literal_launch_env_and_no_inheritance(self):
        calls = []; proc = FakeProcess(); io_backend = FakeIO()
        def factory(argv, **kwargs):
            calls.append((argv, kwargs)); return proc
        with patch("acp_adapter.subprocess.Popen", side_effect=AssertionError("real Popen forbidden")):
            adapter = AcpProcessAdapter.launch(self.plan(), lambda plan: None,
                process_factory=factory, io_factory=lambda p, t: io_backend,
                group_stopper=lambda p: CleanupResult(True, False))
        args, options = calls[0]
        self.assertEqual(args, ["/approved/bin/devin", "acp"])
        self.assertFalse(options["shell"])
        self.assertTrue(options["start_new_session"])
        self.assertTrue(options["close_fds"])
        self.assertEqual(set(options["env"]), {"HOME", "PATH"})
        self.assertNotIn("bypass", str(args))

    def test_token_and_loader_env_vars_are_rejected(self):
        for name in ("SLACK_BOT_TOKEN", "WINDSURF_API_KEY", "LD_PRELOAD", "PYTHONPATH", "NODE_OPTIONS"):
            env = dict(self.plan().environment); env[name] = "FAKE-VALUE"
            with self.subTest(name=name), self.assertRaises(Rejected):
                self.plan(environment=env).validate()

    def test_no_cwd_shell_path_injection(self):
        for path in ("relative", "/approved/../elsewhere", "/approved/./workspace", "/bad\x00path"):
            with self.subTest(path=path), self.assertRaises(Rejected):
                self.plan(workspace=path).validate()

    def test_preflight_failure_prevents_any_launch(self):
        calls = []
        def denied(plan):
            raise ValueError("PRIVATE-PREFLIGHT-DATA")
        with self.assertRaises(AdapterFailure) as raised:
            AcpProcessAdapter.launch(self.plan(), denied, process_factory=lambda *a, **k: calls.append(a))
        self.assertEqual(calls, [])
        self.assertNotIn("PRIVATE-", str(raised.exception))

    def test_boolean_preflight_is_not_authority(self):
        with self.assertRaises(AdapterFailure):
            AcpProcessAdapter.launch(self.plan(), lambda plan: True,
                                     process_factory=lambda *a, **k: self.fail("must not launch"))

    def test_io_setup_failure_cleans_only_created_process(self):
        proc = FakeProcess(); cleaned = []
        def broken(p, t):
            raise OSError("PRIVATE-SETUP")
        with self.assertRaises(AdapterFailure):
            AcpProcessAdapter.launch(self.plan(), lambda plan: None,
                process_factory=lambda *a, **k: proc, io_factory=broken,
                group_stopper=lambda p: cleaned.append(p))
        self.assertEqual(cleaned, [proc])
        self.assertTrue(proc.stdin.closed)
        self.assertTrue(proc.stdout.closed)

    def test_setup_closes_pipes_even_when_process_cleanup_fails(self):
        proc = FakeProcess()
        def broken(*args):
            raise OSError("PRIVATE")
        with self.assertRaises(AdapterFailure):
            AcpProcessAdapter.launch(self.plan(), lambda plan: None,
                process_factory=lambda *a, **k: proc, io_factory=broken, group_stopper=broken)
        self.assertTrue(proc.stdin.closed)
        self.assertTrue(proc.stdout.closed)

    def test_short_writes_complete_one_json_line(self):
        adapter, backend, stopped = fake_acp(FakeIO(chunk=3))
        packet = {"jsonrpc": "2.0", "id": 1, "method": "initialize"}
        adapter.write(packet)
        self.assertEqual(backend.packets(), [packet])
        self.assertGreater(len(backend.writes), 1)

    def test_partial_failure_does_not_resend_or_leak(self):
        backend = FakeIO(chunk=3); backend.fail_after = 1
        adapter, _, _ = fake_acp(backend)
        with self.assertRaises(AdapterFailure) as raised:
            adapter.write({"jsonrpc": "2.0", "id": 1})
        self.assertNotIn("PRIVATE", str(raised.exception))
        with self.assertRaises(AdapterFailure):
            adapter.write({"jsonrpc": "2.0", "id": 1})
        self.assertEqual(len(backend.writes), 1)

    def test_session_creation_and_load_are_bound_to_preflighted_workspace(self):
        for method in ("session/new", "session/load"):
            adapter, backend, _ = fake_acp()
            with self.subTest(method=method), self.assertRaises(AdapterFailure):
                adapter.write({"jsonrpc": "2.0", "id": 1, "method": method,
                               "params": {"cwd": "/other/unapproved", "mcpServers": []}})
            self.assertEqual(backend.writes, [])

    def test_preflighted_workspace_can_start_session_without_extra_mcp(self):
        adapter, backend, _ = fake_acp()
        packet = {"jsonrpc": "2.0", "id": 1, "method": "session/new",
                  "params": {"cwd": "/sandbox/demo", "mcpServers": []}}
        adapter.write(packet)
        self.assertEqual(backend.packets(), [packet])
        adapter, backend, _ = fake_acp()
        packet["params"]["mcpServers"] = [{"name": "unapproved"}]
        with self.assertRaises(AdapterFailure):
            adapter.write(packet)
        self.assertEqual(backend.writes, [])

    def test_unimplemented_mode_or_tool_method_is_not_sent(self):
        adapter, backend, _ = fake_acp()
        with self.assertRaises(AdapterFailure):
            adapter.write({"jsonrpc": "2.0", "id": 1, "method": "session/set_mode",
                           "params": {"modeId": "bypass"}})
        self.assertEqual(backend.writes, [])

    def test_invalid_write_count_fails_closed(self):
        adapter, backend, _ = fake_acp(FakeIO(chunk=0))
        with self.assertRaises(AdapterFailure):
            adapter.write({"id": 1})

    def test_read_timeout_eof_and_generation(self):
        first, backend, _ = fake_acp(); second, _, _ = fake_acp()
        self.assertIsNot(first.generation, second.generation)
        self.assertIsNone(first.read())
        backend.reads.extend([b'{"id":1}\n', b''])
        self.assertEqual(first.read(), b'{"id":1}\n')
        self.assertEqual(first.read(), b'')
        with self.assertRaises(AdapterFailure):
            first.write({"id": 2})

    def test_cleanup_does_not_claim_descendant_verification(self):
        adapter, backend, stopped = fake_acp()
        result = adapter.close()
        self.assertTrue(result.parent_exited)
        self.assertFalse(result.descendants_verified)
        self.assertEqual(len(stopped), 1)
        with self.assertRaises(Rejected):
            adapter.close()

    def test_pipe_cleanup_still_occurs_if_process_cleanup_fails(self):
        adapter, backend, _ = fake_acp()
        def failed(process):
            raise OSError("PRIVATE-CLEANUP-DATA")
        adapter._stopper = failed
        with self.assertRaises(AdapterFailure) as raised:
            adapter.close()
        self.assertNotIn("PRIVATE", str(raised.exception))
        self.assertTrue(adapter._process.stdin.closed)
        self.assertTrue(adapter._process.stdout.closed)


class OsBoundaryTests(unittest.TestCase):
    """Mock every OS boundary. No descriptors/process groups are created."""
    def pipes(self):
        return SimpleNamespace(stdin=SimpleNamespace(fileno=lambda: 101),
                               stdout=SimpleNamespace(fileno=lambda: 102))

    def test_fd_construction_only_changes_owned_pipe_flags(self):
        with patch("acp_adapter.os.set_blocking") as flags:
            backend = FdPipeIO(self.pipes(), 2.0)
        self.assertEqual([call.args for call in flags.call_args_list], [(101, False), (102, False)])

    def test_fd_read_timeout_and_eof(self):
        with patch("acp_adapter.os.set_blocking"):
            backend = FdPipeIO(self.pipes(), 2.0)
        with patch("acp_adapter.select.select", return_value=([], [], [])), patch("acp_adapter.os.read") as read:
            self.assertIsNone(backend.read(32768, 0))
            read.assert_not_called()
        with patch("acp_adapter.select.select", return_value=([102], [], [])), patch("acp_adapter.os.read", return_value=b"") as read:
            self.assertEqual(backend.read(32768, 0), b"")
            read.assert_called_once_with(102, 32768)

    def test_fd_write_timeout_never_calls_write(self):
        with patch("acp_adapter.os.set_blocking"):
            backend = FdPipeIO(self.pipes(), 2.0)
        with patch("acp_adapter.select.select", return_value=([], [], [])) as selected, patch("acp_adapter.os.write") as write:
            with self.assertRaises(OSError):
                backend.write(b"data", 0.25)
            write.assert_not_called()
            self.assertEqual(selected.call_args.args[-1], 0.25)

    def test_fd_short_write_is_returned_to_adapter(self):
        with patch("acp_adapter.os.set_blocking"):
            backend = FdPipeIO(self.pipes(), 2.0)
        with patch("acp_adapter.select.select", return_value=([], [101], [])), patch("acp_adapter.os.write", return_value=2):
            self.assertEqual(backend.write(b"data", 1.0), 2)

    def test_readiness_race_eagain_fails_closed_at_adapter(self):
        with patch("acp_adapter.os.set_blocking"):
            backend = FdPipeIO(self.pipes(), 2.0)
        adapter, _, _ = fake_acp(backend)
        with patch("acp_adapter.select.select", return_value=([], [101], [])), patch("acp_adapter.os.write", side_effect=BlockingIOError):
            with self.assertRaises(AdapterFailure):
                adapter.write({"id": 1})
        with self.assertRaises(AdapterFailure):
            adapter.write({"id": 1})

    def test_readiness_race_read_fails_closed(self):
        with patch("acp_adapter.os.set_blocking"):
            backend = FdPipeIO(self.pipes(), 2.0)
        adapter, _, _ = fake_acp(backend)
        with patch("acp_adapter.select.select", return_value=([102], [], [])), patch("acp_adapter.os.read", side_effect=BlockingIOError):
            with self.assertRaises(AdapterFailure):
                adapter.read()

    def process(self, exited=False):
        return SimpleNamespace(pid=123456789, poll=lambda: 0 if exited else None,
                               wait=lambda **kwargs: 0)

    def test_already_exited_parent_does_not_signal_reused_pid(self):
        with patch("acp_adapter.os.killpg") as kill:
            result = stop_owned_process_group(self.process(exited=True))
            kill.assert_not_called()
        self.assertTrue(result.parent_exited)
        self.assertFalse(result.descendants_verified)

    def test_unrelated_process_group_never_signalled(self):
        with patch("acp_adapter.os.getpgid", return_value=999), patch("acp_adapter.os.getpgrp", return_value=123), patch("acp_adapter.os.killpg") as kill:
            with self.assertRaises(AdapterFailure):
                stop_owned_process_group(self.process())
            kill.assert_not_called()

    def test_current_parent_group_never_signalled(self):
        with patch("acp_adapter.os.getpgid", return_value=123456789), patch("acp_adapter.os.getpgrp", return_value=123456789), patch("acp_adapter.os.killpg") as kill:
            with self.assertRaises(AdapterFailure):
                stop_owned_process_group(self.process())
            kill.assert_not_called()

    def test_owned_group_term_then_timeout_kill(self):
        process = self.process()
        with patch.object(process, "wait", side_effect=[subprocess.TimeoutExpired("devin", 3), 0]), \
             patch("acp_adapter.os.getpgid", return_value=process.pid), \
             patch("acp_adapter.os.getpgrp", return_value=999), patch("acp_adapter.os.killpg") as kill:
            result = stop_owned_process_group(process)
        self.assertEqual([call.args for call in kill.call_args_list],
                         [(process.pid, signal.SIGTERM), (process.pid, signal.SIGKILL)])
        self.assertFalse(result.descendants_verified)

    def test_exit_race_before_group_check_never_signals(self):
        with patch("acp_adapter.os.getpgid", side_effect=ProcessLookupError), patch("acp_adapter.os.killpg") as kill:
            with self.assertRaises(ProcessLookupError):
                stop_owned_process_group(self.process())
            kill.assert_not_called()

    def test_exit_race_after_term_does_not_signal_again(self):
        process = self.process()
        with patch.object(process, "wait", side_effect=subprocess.TimeoutExpired("devin", 3)), \
             patch("acp_adapter.os.getpgid", side_effect=[process.pid, ProcessLookupError]), \
             patch("acp_adapter.os.getpgrp", return_value=999), patch("acp_adapter.os.killpg") as kill:
            with self.assertRaises(ProcessLookupError):
                stop_owned_process_group(process)
        self.assertEqual([call.args for call in kill.call_args_list], [(process.pid, signal.SIGTERM)])


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.ledger = Ledger(":memory:")
        self.slack, self.web, self.socket = fake_slack()
        self.acp, self.io, self.stopped = fake_acp()
        self.engine = BridgeEngine(self.ledger, POLICY, "A_WORKER", {"scratch": "/sandbox/demo"}, self.slack, self.acp)
        self.now = 0.0
        self.pump = RuntimePump(self.engine, self.slack, self.acp, clock=lambda: self.now)

    def tearDown(self):
        self.ledger.close()

    def feed(self, packet):
        self.io.reads.append((json.dumps(packet) + "\n").encode())

    def begin(self):
        self.pump.start()
        self.feed({"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": 1}})
        self.pump.step()

    def running(self):
        self.begin(); self.socket.emit(frame()); self.pump.step()
        self.feed({"jsonrpc": "2.0", "id": 2, "result": {"sessionId": "session-001"}})
        self.pump.step()

    def test_constructor_does_not_start(self):
        self.assertEqual(self.web.calls, [])
        self.assertEqual(self.socket.calls, [])
        self.assertEqual(self.io.writes, [])

    def test_background_callback_does_not_touch_sqlite_or_ack(self):
        self.begin()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(self.socket.emit, frame()).result()
        self.assertEqual(self.ledger.tasks_in_route("T_TEST", "C_TEST"), [])
        self.assertEqual(self.socket.calls, ["connect"])
        self.pump.step()
        self.assertEqual(len(self.ledger.tasks_in_route("T_TEST", "C_TEST")), 1)

    def test_real_adapter_call_path_with_fake_clients(self):
        self.running()
        self.feed({"jsonrpc": "2.0", "id": 3, "result": {"stopReason": "end_turn"}})
        self.pump.step(); self.pump.step()
        messages = [c for c in self.web.calls if isinstance(c, dict)]
        self.assertTrue(messages)
        self.assertTrue(all(m["thread_ts"] == ROOT for m in messages))
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "ready")

    def test_fake_permission_and_cancel_use_same_session(self):
        self.running()
        self.feed({"jsonrpc": "2.0", "id": "permission", "method": "session/request_permission",
                   "params": {"sessionId": "session-001", "toolCall": {"rawInput": "PRIVATE"}}})
        self.pump.step()
        self.assertEqual(self.io.packets()[-1]["result"]["outcome"]["outcome"], "cancelled")
        self.socket.emit(frame(2, "cancel")); self.pump.step()
        self.assertEqual(self.io.packets()[-1]["method"], "session/cancel")
        self.feed({"jsonrpc": "2.0", "id": 3, "result": {"stopReason": "cancelled"}})
        self.pump.step()
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "cancelled")
        self.assertNotIn("PRIVATE", str(self.web.calls))

    def test_ack_failure_after_persist_does_not_dispatch(self):
        self.begin(); self.socket.fail_ack = True
        self.socket.emit(frame())
        with self.assertRaises(AdapterFailure):
            self.pump.step()
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "queued")
        self.assertFalse(any(p.get("method") == "session/new" for p in self.io.packets()))
        self.assertEqual(len(self.stopped), 1)

    def test_eof_freezes_active_task_and_cleans_owned_process(self):
        self.running(); self.io.reads.append(b'')
        with self.assertRaises(AdapterFailure):
            self.pump.step()
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")
        self.assertEqual(len(self.stopped), 1)
        self.assertFalse(self.pump.cleanup["acp"].descendants_verified)

    def test_ledger_failure_cannot_skip_cleanup_or_leak_details(self):
        self.running(); self.io.reads.append(b'')
        with patch.object(self.engine, "transport_lost", side_effect=RuntimeError("PRIVATE-LEDGER-DATA")):
            with self.assertRaises(AdapterFailure) as raised:
                self.pump.step()
        self.assertNotIn("PRIVATE", str(raised.exception))
        self.assertFalse(self.pump.cleanup["ledger_invalidated"])
        self.assertTrue(self.pump.cleanup["slack_closed"])
        self.assertEqual(len(self.stopped), 1)

    def test_startup_recovery_failure_prevents_connect_and_still_cleans(self):
        with patch.object(self.ledger, "recover_route", side_effect=RuntimeError("PRIVATE-DB-DATA")):
            with self.assertRaises(AdapterFailure):
                self.pump.start()
        self.assertEqual(self.web.calls, [])
        self.assertNotIn("connect", self.socket.calls)
        self.assertEqual(len(self.stopped), 1)

    def test_fresh_process_requires_ready_session_restoration(self):
        from test_bridge_store import command
        self.ledger.accept(command()); self.ledger.reserve_prompt(KEY)
        self.ledger.bind_session(KEY, "request_001", "old-session")
        self.ledger.begin_prompt_send(KEY, "request_001"); self.ledger.mark_prompt_sent(KEY, "request_001")
        self.ledger.finish_turn(KEY, "request_001", "end_turn")
        self.ledger.accept(command(2, "reply"))
        self.ledger.queue_outbox(KEY, "old-send", NOTICES["complete"])
        self.ledger.reserve_outbox("T_TEST", "C_TEST")
        self.begin()
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "restoring")
        self.assertEqual(self.ledger.db.execute("SELECT state FROM outbox WHERE dedupe_key='old-send'").fetchone()[0], "uncertain")
        self.assertFalse(any(p.get("method") in {"session/new", "session/prompt"} for p in self.io.packets()))

    def test_changed_acp_generation_cannot_feed_old_responses(self):
        self.running(); self.acp.generation = object()
        self.feed({"jsonrpc": "2.0", "id": 3, "result": {"stopReason": "end_turn"}})
        with self.assertRaises(AdapterFailure):
            self.pump.step()
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")

    def test_global_task_cap_blocks_second_root(self):
        self.running()
        self.socket.emit(frame(2, ts="1700000002.000001")); self.pump.step()
        self.assertEqual(len(self.ledger.tasks_in_route("T_TEST", "C_TEST")), 1)

    def test_prompt_count_cap_rejects_extra_followup(self):
        self.pump._max_prompts = 1
        self.running(); self.socket.emit(frame(2, "reply")); self.pump.step()
        self.assertEqual(len(self.ledger.commands_in_route("T_TEST", "C_TEST")), 1)

    def test_deadline_stops_without_replay_or_success_claim(self):
        self.running(); self.now = 61
        self.assertFalse(self.pump.step())
        self.assertEqual(self.ledger.snapshot(KEY)["state"], "uncertain")
        self.assertEqual(self.pump.failure, "runtime_deadline_reached")
        with self.assertRaises(Rejected):
            self.pump.start()

    def test_socket_queue_overflow_halts_without_dispatch(self):
        self.pump = RuntimePump(self.engine, self.slack, self.acp, max_frames=1, clock=lambda: self.now)
        self.begin(); self.socket.emit(frame()); self.socket.emit(frame(2))
        with self.assertRaises(AdapterFailure):
            self.pump.step()
        self.assertEqual(self.ledger.tasks_in_route("T_TEST", "C_TEST"), [])

    def test_post_failure_remains_uncertain_and_unretried(self):
        self.begin(); self.web.fail_post = True
        self.socket.emit(frame())
        with self.assertRaises(AdapterFailure):
            self.pump.step()
        rows = self.ledger.db.execute("SELECT state FROM outbox").fetchall()
        self.assertEqual(rows[0]["state"], "uncertain")
        self.assertEqual(len([c for c in self.web.calls if isinstance(c, dict)]), 1)


if __name__ == "__main__":
    unittest.main()
