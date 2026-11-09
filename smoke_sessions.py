#!/usr/bin/env python3
"""Slack <-> local `devin acp` session bridge. Foreground service (systemd).

Model: each channel root message = one new devin session; replies in that
thread continue the same session; '停止' cancels and kills it. Progress and
results post back into the same thread. No tokens are printed or logged.

Env: SLACK_BOT_TOKEN, SLACK_APP_TOKEN, SLACK_CHANNEL_ID
Opt: DEVIN_TASKS_DIR (default ~/devin-tasks), SESSION_IDLE_SEC (default 3600)
"""
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque

TASKS_DIR = os.path.expanduser(os.environ.get("DEVIN_TASKS_DIR", "~/devin-tasks"))
IDLE_SEC = float(os.environ.get("SESSION_IDLE_SEC", "3600"))
TIMEOUT = float(os.environ.get("PROMPT_TIMEOUT", "900"))
PROGRESS_MIN_GAP = 20
CHANNEL = os.environ.get("SLACK_CHANNEL_ID", "")
MENTION = re.compile(r"<@[A-Z0-9]+>")
STOP_WORDS = {"停止", "stop", "cancel", "取消"}
STATUS_WORDS = {"进度", "progress", "status", "状态"}


class DevinSession:
    """One persistent `devin acp` process bound to one Slack thread."""

    def __init__(self, thread_ts, workdir):
        os.makedirs(workdir, exist_ok=True)
        self.thread_ts = thread_ts
        self.workdir = workdir
        self.proc = subprocess.Popen(
            ["devin", "acp"], cwd=workdir,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1,
            start_new_session=True)
        self.next_id = 0
        self.lock = threading.Lock()          # serialize reads/writes
        self.last_active = time.monotonic()
        self.running = False
        self.last_progress = 0.0
        self.last_activity = "启动中"
        self.chunks = []

    def _send(self, packet):
        self.proc.stdin.write(json.dumps(packet) + "\n")
        self.proc.stdin.flush()

    def _request(self, method, params):
        self.next_id += 1
        self._send({"jsonrpc": "2.0", "id": self.next_id,
                    "method": method, "params": params})
        return self.next_id

    def _read_line(self):
        return self.proc.stdout.readline()

    def _handle_msg(self, msg, on_progress):
        if msg.get("method") == "session/update":
            upd = msg["params"].get("update", {})
            kind = upd.get("sessionUpdate")
            if kind == "agent_message_chunk":
                self.chunks.append(upd.get("content", {}).get("text", ""))
                self.last_activity = "正在输出"
            elif kind in ("tool_call", "tool_call_update"):
                title = upd.get("title") or upd.get("status") or "working"
                self.last_activity = f"工具: {title}"
                on_progress(self.last_activity)
            elif kind == "agent_thought_chunk":
                self.last_activity = "思考中"
        elif msg.get("method") == "session/request_permission":
            # Auto-approve: prefer allow_always, then allow_once, else first option.
            options = msg.get("params", {}).get("options") or []
            pick = next((o for o in options if o.get("kind") == "allow_always"), None) \
                or next((o for o in options if str(o.get("kind", "")).startswith("allow")), None) \
                or (options[0] if options else None)
            if pick:
                self._send({"jsonrpc": "2.0", "id": msg["id"], "result": {
                    "outcome": {"outcome": "selected", "optionId": pick["optionId"]}}})
                self.last_activity = "已批准操作"
            else:
                self._send({"jsonrpc": "2.0", "id": msg["id"],
                            "result": {"outcome": {"outcome": "cancelled"}}})

    def _wait_for(self, want_id, deadline, on_progress):
        while time.monotonic() < deadline:
            line = self._read_line()
            if not line:
                raise RuntimeError("devin acp exited")
            self._handle_msg(json.loads(line), on_progress)
            if json.loads(line).get("id") == want_id and \
                    ("result" in json.loads(line) or "error" in json.loads(line)):
                return json.loads(line)
        raise TimeoutError("devin timed out")

    def start(self):
        deadline = time.monotonic() + TIMEOUT
        i = self._request("initialize", {"protocolVersion": 1,
                                         "clientCapabilities": {}})
        self._wait_for(i, deadline, lambda x: None)
        n = self._request("session/new", {"cwd": self.workdir, "mcpServers": []})
        self.session_id = self._wait_for(n, deadline, lambda x: None)["result"]["sessionId"]
        m = self._request("session/set_mode", {"sessionId": self.session_id,
                                               "modeId": "bypass"})
        self._wait_for(m, deadline, lambda x: None)

    def prompt(self, text, on_progress, stop_flag):
        """Returns (reply_text, stop_reason)."""
        with self.lock:
            self.running = True
            self.chunks = []
            try:
                pid = self._request("session/prompt", {
                    "sessionId": self.session_id,
                    "prompt": [{"type": "text", "text": text}]})
                deadline = time.monotonic() + TIMEOUT
                while time.monotonic() < deadline:
                    if stop_flag.is_set():
                        self._send({"jsonrpc": "2.0", "method": "session/cancel",
                                    "params": {"sessionId": self.session_id}})
                    line = self._read_line()
                    if not line:
                        raise RuntimeError("devin acp exited")
                    msg = json.loads(line)
                    self._handle_msg(msg, on_progress)
                    if msg.get("id") == pid and ("result" in msg or "error" in msg):
                        if "error" in msg:
                            raise RuntimeError(str(msg["error"]))
                        return ("".join(self.chunks).strip() or "(无输出)",
                                msg["result"].get("stopReason", "?"))
                raise TimeoutError("prompt timed out")
            finally:
                self.running = False
                self.last_active = time.monotonic()

    def close(self):
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
        except Exception:
            pass
        try:
            self.proc.wait(5)
        except Exception:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except Exception:
                pass


def main():
    for name in ("SLACK_BOT_TOKEN", "SLACK_APP_TOKEN"):
        if not os.environ.get(name):
            sys.exit(f"{name} not set")
    if not CHANNEL:
        sys.exit("SLACK_CHANNEL_ID not set")

    from slack_sdk import WebClient
    from slack_sdk.socket_mode import SocketModeClient
    from slack_sdk.socket_mode.request import SocketModeRequest
    from slack_sdk.socket_mode.response import SocketModeResponse

    web = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
    socket = SocketModeClient(app_token=os.environ["SLACK_APP_TOKEN"],
                              web_client=web, auto_reconnect_enabled=True)
    own_bot_id = web.auth_test().get("bot_id")

    sessions = {}             # thread_ts -> DevinSession
    stop_flags = {}           # thread_ts -> Event
    seen = deque(maxlen=2000)
    seen_ids = set()
    state_lock = threading.Lock()

    def post(thread_ts, text):
        try:
            if len(text) > 3500:
                text = text[:3500] + "…"
            web.chat_postMessage(channel=CHANNEL, thread_ts=thread_ts, text=text)
        except Exception as e:
            print(f"[error] post: {type(e).__name__}", flush=True)

    def progress_fn(ts):
        def cb(activity):
            s = sessions.get(ts)
            if s and time.monotonic() - s.last_progress >= PROGRESS_MIN_GAP:
                s.last_progress = time.monotonic()
                post(ts, f"⏳ {activity}")
        return cb

    def run_prompt(ts, text):
        s = sessions.get(ts)
        if not s:
            return
        flag = stop_flags.setdefault(ts, threading.Event())
        try:
            reply, reason = s.prompt(text, progress_fn(ts), flag)
            flag.clear()
            label = {"end_turn": "", "cancelled": "（已停止）"}.get(reason, f"（{reason}）")
            post(ts, f"{reply}{label}")
        except Exception as e:
            flag.clear()
            post(ts, f"任务出错: {type(e).__name__}: {e}")

    def new_session(ts, text):
        try:
            wd = os.path.join(TASKS_DIR, ts.replace(".", "_"))
            s = DevinSession(ts, wd)
            s.start()
        except Exception as e:
            post(ts, f"启动 devin 失败: {type(e).__name__}: {e}")
            return
        with state_lock:
            sessions[ts] = s
        post(ts, "已接单，开始处理。")
        run_prompt(ts, text)

    def handle(client, req: SocketModeRequest):
        if req.type != "events_api":
            return
        client.send_socket_mode_response(
            SocketModeResponse(envelope_id=req.envelope_id))
        ev = req.payload.get("event", {})
        eid = req.payload.get("event_id")
        if eid:
            if eid in seen_ids:
                return
            seen_ids.add(eid)
            seen.append(eid)
            if len(seen_ids) > 2 * seen.maxlen:
                seen_ids.intersection_update(seen)
        if (ev.get("type") != "message" or ev.get("channel") != CHANNEL
                or ev.get("subtype") not in (None, "bot_message")
                or ev.get("bot_id") == own_bot_id):
            return
        text = MENTION.sub("", ev.get("text", "")).strip()
        ts = ev["ts"]
        root = ev.get("thread_ts", ts)
        is_root = root == ts
        if not text:
            return
        low = text.lower()
        if is_root:
            if own_mention := MENTION.match(ev.get("text", "")):
                threading.Thread(target=new_session, args=(ts, text),
                                 daemon=True).start()
            return
        # thread reply
        if low in STOP_WORDS:
            flag = stop_flags.get(root)
            if flag:
                flag.set()
            s = sessions.get(root)
            if s:
                post(root, "正在停止…")
                threading.Thread(target=s.close, daemon=True).start()
                with state_lock:
                    sessions.pop(root, None)
            else:
                post(root, "该线程没有运行中的会话。")
        elif low in STATUS_WORDS:
            s = sessions.get(root)
            if s:
                mins = int((time.monotonic() - s.last_progress) / 60)
                post(root, f"状态: {'运行中' if s.running else '空闲'}，最近动作: {s.last_activity}")
            else:
                post(root, "该线程没有会话。")
        elif root in sessions:
            threading.Thread(target=run_prompt, args=(root, text),
                             daemon=True).start()

    def reaper():
        while True:
            time.sleep(300)
            idle = [ts for ts, s in sessions.items()
                    if not s.running and time.monotonic() - s.last_active > IDLE_SEC]
            for ts in idle:
                with state_lock:
                    s = sessions.pop(ts, None)
                if s:
                    s.close()
                    post(ts, "会话空闲超时已回收；在本线程继续发言会报无会话，请开新任务。")

    threading.Thread(target=reaper, daemon=True).start()
    socket.socket_mode_request_listeners.append(handle)
    socket.connect()
    print(f"session bridge up in {CHANNEL}, tasks dir {TASKS_DIR}", flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        socket.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
