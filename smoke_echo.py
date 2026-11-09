#!/usr/bin/env python3
"""Minimal Slack <-> local `devin acp` echo smoke test. Foreground only.

Requires slack_sdk (isolated venv) and env vars:
  SLACK_BOT_TOKEN, SLACK_APP_TOKEN, SLACK_CHANNEL_ID
Optional: DEVIN_TEST_DIR (default ~/devin-echo-test), SMOKE_TIMEOUT.

Posts the devin reply into the same thread. No tokens are printed or logged.
"""
import json
import os
import re
import subprocess
import sys
import threading
import time
from collections import deque

TEST_DIR = os.path.expanduser(os.environ.get("DEVIN_TEST_DIR", "~/devin-echo-test"))
TIMEOUT = float(os.environ.get("SMOKE_TIMEOUT", "120"))
CHANNEL = os.environ.get("SLACK_CHANNEL_ID", "")
MENTION = re.compile(r"<@[A-Z0-9]+>")


class DevinACP:
    """One-shot `devin acp` subprocess; line-delimited JSON-RPC over stdio."""

    def __init__(self, cwd):
        self.proc = subprocess.Popen(
            ["devin", "acp"], cwd=cwd,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1,
            start_new_session=True)
        self.next_id = 0

    def _send(self, packet):
        self.proc.stdin.write(json.dumps(packet) + "\n")
        self.proc.stdin.flush()

    def request(self, method, params):
        self.next_id += 1
        self._send({"jsonrpc": "2.0", "id": self.next_id,
                    "method": method, "params": params})
        return self.next_id

    def read_until(self, want_id, chunks, deadline):
        """Collect updates; return the response for want_id."""
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError("devin acp closed stdout")
            msg = json.loads(line)
            if msg.get("method") == "session/update":
                update = msg["params"].get("update", {})
                if update.get("sessionUpdate") == "agent_message_chunk":
                    chunks.append(update.get("content", {}).get("text", ""))
            elif msg.get("method") == "session/request_permission":
                self._send({"jsonrpc": "2.0", "id": msg["id"],
                            "result": {"outcome": {"outcome": "cancelled"}}})
            elif msg.get("id") == want_id and ("result" in msg or "error" in msg):
                return msg
        raise TimeoutError("devin response timed out")

    def echo(self, text):
        deadline = time.monotonic() + TIMEOUT
        init = self.request("initialize", {"protocolVersion": 1,
                                           "clientCapabilities": {}})
        self.read_until(init, [], deadline)
        new = self.request("session/new", {"cwd": TEST_DIR, "mcpServers": []})
        session = self.read_until(new, [], deadline)["result"]["sessionId"]
        chunks = []
        prompt = self.request("session/prompt", {
            "sessionId": session,
            "prompt": [{"type": "text", "text": text}]})
        self.read_until(prompt, chunks, deadline)
        return "".join(chunks).strip() or "(empty reply)"

    def close(self):
        try:
            self.proc.terminate()
            self.proc.wait(5)
        except Exception:
            try:
                self.proc.kill()
            except Exception:
                pass


def main():
    for name in ("SLACK_BOT_TOKEN", "SLACK_APP_TOKEN"):
        if not os.environ.get(name):
            sys.exit(f"{name} not set")
    if not CHANNEL:
        sys.exit("SLACK_CHANNEL_ID not set")
    if not os.path.isdir(TEST_DIR):
        sys.exit(f"test dir missing: {TEST_DIR}")

    from slack_sdk import WebClient
    from slack_sdk.socket_mode import SocketModeClient
    from slack_sdk.socket_mode.request import SocketModeRequest
    from slack_sdk.socket_mode.response import SocketModeResponse

    web = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
    socket = SocketModeClient(app_token=os.environ["SLACK_APP_TOKEN"],
                              web_client=web, auto_reconnect_enabled=True)
    busy = threading.Lock()
    seen_events = deque(maxlen=2000)
    seen_ids = set()

    own_bot_id = web.auth_test().get("bot_id")

    def handle(client, req: SocketModeRequest):
        print(f"[frame] type={req.type}", flush=True)
        if req.type != "events_api":
            return
        client.send_socket_mode_response(
            SocketModeResponse(envelope_id=req.envelope_id))
        event = req.payload.get("event", {})
        # Slack retries event delivery; dedupe before any side effect.
        event_id = req.payload.get("event_id")
        if event_id:
            if event_id in seen_ids:
                return
            seen_ids.add(event_id)
            seen_events.append(event_id)
            if len(seen_ids) > 2 * seen_events.maxlen:
                seen_ids.intersection_update(seen_events)
        print(f"[event] type={event.get('type')} channel={event.get('channel')} "
              f"subtype={event.get('subtype')} user={event.get('user')} "
              f"bot={event.get('bot_id')}", flush=True)
        if (event.get("type") != "message" or event.get("channel") != CHANNEL
                or event.get("subtype") not in (None, "bot_message")
                or event.get("bot_id") == own_bot_id):
            return
        text = MENTION.sub("", event.get("text", "")).strip()
        if not text:
            return
        thread_ts = event.get("thread_ts") or event["ts"]

        def work():
            if not busy.acquire(blocking=False):
                web.chat_postMessage(channel=CHANNEL, thread_ts=thread_ts,
                                     text="busy — one echo at a time")
                return
            try:
                agent = DevinACP(TEST_DIR)
                try:
                    reply = agent.echo(text)
                finally:
                    agent.close()
                if len(reply) > 3500:
                    reply = reply[:3500] + "…"
                web.chat_postMessage(channel=CHANNEL, thread_ts=thread_ts,
                                     text=reply)
            except Exception as e:
                print(f"[error] {type(e).__name__}: {e}", flush=True)
                try:
                    web.chat_postMessage(channel=CHANNEL, thread_ts=thread_ts,
                                         text=f"echo failed: {type(e).__name__}: {e}")
                except Exception as e2:
                    print(f"[error] post failed: {type(e2).__name__}", flush=True)
            finally:
                busy.release()

        threading.Thread(target=work, daemon=True).start()

    socket.socket_mode_request_listeners.append(handle)
    socket.connect()
    print(f"smoke echo listening in {CHANNEL}, dir {TEST_DIR} — Ctrl+C to stop",
          flush=True)
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
        sys.exit(1)  # nonzero exit so the supervisor restarts it
