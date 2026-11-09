"""Slack SDK adapter source. Import is inert; no SDK imported automatically.

Construction/start are separate. Only a locally authorized caller may supply
credentials, build SDK clients and call start(). Offline tests inject fakes.
"""
import json
import logging
import re
import threading
from bridge_core import Policy, Rejected, SLACK_TS
from bridge_engine import NOTICES, OutputPolicy


class AdapterFailure(OSError):
    """Sanitized transport error. Never include SDK exception text/payloads."""


class SlackSocketAdapter:
    def __init__(self, policy: Policy, receiver_app_id: str, web_client,
                 socket_client, response_factory, output_policy=None):
        if (not isinstance(receiver_app_id, str) or not re.fullmatch(r"A[A-Z0-9_]{2,63}", receiver_app_id)
                or receiver_app_id == policy.dot_app_id):
            raise Rejected("receiving app identity is unresolved or conflicting")
        self.policy = policy
        self.receiver_app_id = receiver_app_id
        self._web = web_client
        self._socket = socket_client
        self._response_factory = response_factory
        self._output = output_policy or OutputPolicy()
        self._owner = threading.get_ident()
        self._started = False
        self._used = False
        self._sink = None
        self.fault = None

    def __repr__(self):
        return "<SlackSocketAdapter credentials=hidden>"

    def _actor(self):
        if threading.get_ident() != self._owner:
            raise Rejected("Slack side effects must use the actor thread")

    def start(self, frame_sink):
        """AUTHENTICATES AND CONNECTS if explicitly called. Not run in this task.

        SDK callbacks only enqueue into frame_sink. It must be bounded and must
        not call the engine/SQLite. No live reconnect/retry is implemented here.
        """
        self._actor()
        if self._used or not callable(frame_sink):
            raise Rejected("transport is single-use and requires a bounded sink")
        self._used = True
        try:
            auth = self._web.auth_test()
            if (auth.get("ok") is not True or auth.get("team_id") != self.policy.team_id
                    or auth.get("user_id") != self.policy.worker_user_id
                    or auth.get("bot_id") != self.policy.worker_bot_id):
                raise AdapterFailure("Slack authenticated identity does not match configuration")
            # The receiving app_id is ALSO checked on every delivered event.
            self._sink = frame_sink
            self._socket.socket_mode_request_listeners.append(self._on_sdk_request)
            self._started = True
            self._socket.connect()
        except Exception:
            self._started = False
            self.fault = "slack_start_failed"
            try:
                self._socket.close()
            except Exception:
                pass
            raise AdapterFailure("Slack startup failed; inspect only sanitized local status") from None

    def _on_sdk_request(self, client, request):
        """Called by the SDK. Contents are not logged; no user code is executed."""
        if client is not self._socket or not self._started or self.fault:
            return
        if getattr(request, "type", None) != "events_api":
            return
        try:
            frame = {"type": "events_api", "envelope_id": request.envelope_id,
                     "payload": request.payload}
            encoded = json.dumps(frame, ensure_ascii=True, separators=(",", ":"))
            if len(encoded) > 65536:
                self.fault = "slack_frame_too_large"
                return
            # Detached copy prevents a callback producer changing queued data.
            frame = json.loads(encoded)
            payload = frame.get("payload")
            if (not isinstance(payload, dict) or payload.get("team_id") != self.policy.team_id
                    or payload.get("api_app_id") != self.receiver_app_id):
                self.fault = "slack_route_mismatch"
                return
            if self._sink(frame) is not True:
                self.fault = "slack_input_queue_full"
                # No ack: persistence was not observed, so do not claim receipt.
        except Exception:
            self.fault = "slack_input_failed"

    def ack(self, envelope_id):
        self._actor()
        if (not self._started or self.fault or not isinstance(envelope_id, str)
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", envelope_id)):
            raise AdapterFailure("Slack acknowledgement unavailable")
        try:
            response = self._response_factory(envelope_id=envelope_id)
            self._socket.send_socket_mode_response(response)
        except Exception:
            self.fault = "slack_ack_uncertain"
            raise AdapterFailure("Slack acknowledgement outcome is uncertain") from None

    def post(self, payload):
        self._actor()
        if not self._started or self.fault:
            raise AdapterFailure("Slack transport unavailable")
        if not isinstance(payload, dict) or set(payload) - {
                "channel", "thread_ts", "text", "unfurl_links", "unfurl_media", "parse"}:
            raise AdapterFailure("Unsupported outgoing Slack fields")
        text, thread_ts = payload.get("text"), payload.get("thread_ts")
        if (payload.get("channel") != self.policy.channel_id or not isinstance(thread_ts, str)
                or not SLACK_TS.fullmatch(thread_ts)
                or not isinstance(text, str)
                or (text not in NOTICES.values() and self._output.release(text) is None)):
            raise AdapterFailure("Outgoing Slack policy rejected content or route")
        try:
            result = self._web.chat_postMessage(channel=self.policy.channel_id,
                thread_ts=thread_ts, text=text, parse="none", mrkdwn=False,
                unfurl_links=False, unfurl_media=False)
            # Do not return SDK objects containing token/headers/request details.
            return {"ok": result.get("ok"), "channel": result.get("channel"), "ts": result.get("ts")}
        except Exception:
            self.fault = "slack_send_uncertain"
            raise AdapterFailure("Slack message outcome is uncertain; never auto-retry") from None

    def close(self):
        self._actor()
        self._started = False
        try:
            self._socket.close()
        except Exception:
            self.fault = "slack_close_uncertain"
            raise AdapterFailure("Slack close outcome is uncertain") from None


def build_sdk_adapter(policy, receiver_app_id, bot_token, app_token, output_policy=None,
                      *, sdk_classes=None):
    """EXPLICIT live-construction boundary, not called by imports/tests/CLI.

    Supplying actual credentials and configuring their use requires approval.
    No credential loader/store is included. SDK/version compatibility has not
    been checked against the user's local installation. Fakes may be injected.
    """
    if not isinstance(bot_token, str) or not bot_token or not isinstance(app_token, str) or not app_token:
        raise Rejected("both authorized credentials must be supplied securely")
    if sdk_classes is None:
        # Lazy import only after an approved installation/construction decision.
        from slack_sdk import WebClient
        from slack_sdk.socket_mode import SocketModeClient
        from slack_sdk.socket_mode.response import SocketModeResponse
    else:
        WebClient, SocketModeClient, SocketModeResponse = sdk_classes
    logger = logging.Logger("elite_bridge_disabled_sdk_log")
    logger.disabled = True
    logger.propagate = False
    socket = None
    try:
        # Disable automatic Web API retries, including ambiguous chat.postMessage.
        web = WebClient(token=bot_token, timeout=10, retry_handlers=[], logger=logger)
        socket = SocketModeClient(app_token=app_token, web_client=web, logger=logger)
        # The installed SDK must expose this switch. No implicit reconnection.
        if not hasattr(socket, "auto_reconnect_enabled"):
            raise AdapterFailure("SDK reconnect behavior requires local compatibility review")
        socket.auto_reconnect_enabled = False
        return SlackSocketAdapter(policy, receiver_app_id, web, socket, SocketModeResponse, output_policy)
    except Exception:
        if socket is not None:
            try:
                socket.close()
            except Exception:
                pass
        raise AdapterFailure("Slack SDK construction failed; no connection was requested") from None
