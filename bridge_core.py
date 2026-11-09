"""Offline policy/protocol draft. No network, process execution, tokens, or daemon.

ONLY pass event envelopes obtained from an authenticated Slack transport.
Parsing this format is NOT authorization to execute the requested work.
"""
from dataclasses import dataclass
import json
import re


class Rejected(ValueError):
    pass


@dataclass(frozen=True)
class Policy:
    team_id: str
    channel_id: str
    owner_user_id: str
    dot_bot_id: str
    dot_app_id: str
    worker_bot_id: str
    dot_user_id: str
    worker_user_id: str
    workspace_aliases: frozenset[str]

    def __post_init__(self):
        # Unresolved configuration must fail before any event can be accepted.
        for field in ("team_id", "channel_id", "owner_user_id", "dot_bot_id",
                      "dot_app_id", "worker_bot_id", "dot_user_id", "worker_user_id"):
            value = getattr(self, field)
            if not isinstance(value, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", value):
                raise Rejected("missing or invalid configured identity")
        if len({self.owner_user_id, self.dot_user_id, self.worker_user_id}) != 3:
            raise Rejected("owner, dot and bridge must have distinct user identities")
        if self.dot_bot_id == self.worker_bot_id:
            raise Rejected("dot and bridge must have distinct bot identities")
        if (not isinstance(self.workspace_aliases, frozenset) or not self.workspace_aliases
                or any(not isinstance(alias, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", alias)
                       for alias in self.workspace_aliases)):
            raise Rejected("invalid workspace aliases")


@dataclass(frozen=True)
class Command:
    request_id: str
    action: str
    channel_id: str
    thread_ts: str
    workspace: str | None
    text: str | None
    source_event_id: str
    team_id: str
    source_message_ts: str
    origin_id: str
    scope: dict | None = None


PREFIX = "bridge.v1 "
SLACK_TS = re.compile(r"^[0-9]{1,16}\.[0-9]{6}$")
REQUEST_ID = re.compile(r"^[A-Za-z0-9_-]{8,80}$")


def parse_command(envelope: dict, policy: Policy) -> Command:
    """Validate trusted transport metadata, never names or claims in text.

    The production transport must first verify Slack origin (Socket Mode token
    connection, or HTTP signature and replay window). A caller-supplied bool
    such as verified=True is intentionally not accepted as proof.
    """
    if not isinstance(envelope, dict) or envelope.get("type") != "event_callback":
        raise Rejected("not an event callback")
    if envelope.get("team_id") != policy.team_id:
        raise Rejected("wrong workspace")
    if envelope.get("is_ext_shared_channel"):
        raise Rejected("Slack Connect channels are outside this draft's scope")
    event_id = envelope.get("event_id")
    if (not isinstance(event_id, str) or not event_id or len(event_id) > 128
            or not re.fullmatch(r"[A-Za-z0-9_-]+", event_id)):
        raise Rejected("missing event id")
    event = envelope.get("event")
    if not isinstance(event, dict) or event.get("type") != "message":
        raise Rejected("not a message event")
    if event.get("channel") != policy.channel_id:
        raise Rejected("wrong channel")
    if event.get("subtype") not in (None, "bot_message"):
        raise Rejected("edits, deletes and other subtypes do not run commands")
    if event.get("hidden") or event.get("bot_id") == policy.worker_bot_id:
        raise Rejected("hidden message or worker echo")
    bot_id = event.get("bot_id")
    if bot_id:
        profile = event.get("bot_profile") or {}
        if not isinstance(profile, dict):
            raise Rejected("invalid bot profile")
        app_id = event.get("app_id") or profile.get("app_id")
        if event.get("app_id") and profile.get("app_id") and event["app_id"] != profile["app_id"]:
            raise Rejected("inconsistent bot app identity")
        if (bot_id != policy.dot_bot_id or app_id != policy.dot_app_id
                or event.get("user") != policy.dot_user_id):
            raise Rejected("unapproved bot identity")
        if profile.get("id") and profile["id"] != bot_id:
            raise Rejected("inconsistent bot profile identity")
        origin_id = f"bot:{policy.dot_user_id}:{bot_id}:{app_id}"
    elif (event.get("user") != policy.owner_user_id or event.get("subtype")
          or event.get("app_id") or event.get("bot_profile")):
        raise Rejected("unapproved human identity")
    else:
        origin_id = f"user:{policy.owner_user_id}"
    ts = event.get("ts")
    root = event.get("thread_ts", ts)
    if not isinstance(ts, str) or not SLACK_TS.fullmatch(ts):
        raise Rejected("invalid message timestamp")
    if not isinstance(root, str) or not SLACK_TS.fullmatch(root):
        raise Rejected("invalid thread timestamp")
    text = event.get("text")
    if not isinstance(text, str):
        raise Rejected("not an explicit bridge command")
    try:
        if len(text.encode("utf-8")) > 16000:
            raise Rejected("command too long")
    except UnicodeEncodeError as error:
        raise Rejected("command is not valid Unicode") from error
    # Slack's actual mention token, not a display name or a mention inside JSON.
    # Only the configured bridge can be addressed; do not use app_mention as a
    # second task entry point because message.groups already carries this text.
    mention = f"<@{policy.worker_user_id}>"
    addressed = text.startswith(mention) and text[len(mention):].startswith((" ", "\t", "\r", "\n"))
    if addressed:
        text = text[len(mention):].lstrip(" \t\r\n")
    if not text.startswith(PREFIX):
        raise Rejected("not an explicit bridge command")
    try:
        payload = json.loads(text[len(PREFIX):], object_pairs_hook=_unique_keys)
    except (ValueError, TypeError, RecursionError) as error:
        raise Rejected("invalid command JSON") from error
    if not isinstance(payload, dict):
        raise Rejected("command must be an object")
    if set(payload) - {"request_id", "action", "workspace", "text", "scope"}:
        raise Rejected("unknown command keys")
    request_id = payload.get("request_id")
    if not isinstance(request_id, str) or not REQUEST_ID.fullmatch(request_id):
        raise Rejected("invalid request id")
    action = payload.get("action")
    workspace, prompt = payload.get("workspace"), payload.get("text")
    if action == "run":
        if not addressed:
            raise Rejected("new task must explicitly mention the bridge bot")
        if root != ts:
            raise Rejected("start each task with a new root message")
        if not isinstance(workspace, str) or workspace not in policy.workspace_aliases:
            raise Rejected("workspace alias not allowed")
        scope = payload.get("scope")
        if scope is not None:
            # Fail fast on malformed scope; the durable fingerprint covers it.
            from task_scope import TaskScope
            TaskScope.parse(scope)
    elif action in ("reply", "cancel", "status"):
        if root == ts:
            raise Rejected("existing task command must be in its thread")
        if "workspace" in payload:
            raise Rejected("cannot change workspace within a thread")
        if "scope" in payload:
            raise Rejected("cannot change scope within a thread")
    else:
        raise Rejected("unsupported action")
    if action in ("run", "reply"):
        if not isinstance(prompt, str) or not prompt.strip():
            raise Rejected("missing task text")
        try:
            prompt.encode("utf-8")
        except UnicodeEncodeError as error:
            raise Rejected("task text is not valid Unicode") from error
    elif "text" in payload:
        raise Rejected("control command cannot carry task text")
    return Command(request_id, action, policy.channel_id, root, workspace, prompt,
                   event_id, policy.team_id, ts, origin_id,
                   payload.get("scope"))


def _unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def acp_initialize(request_id: int) -> dict:
    """Advertise only supported capabilities; no impersonated editor identity."""
    return {"jsonrpc": "2.0", "id": request_id, "method": "initialize", "params": {
        "protocolVersion": 1,
        "clientInfo": {"name": "slack-devin-bridge", "version": "0.0.1-draft"},
        "clientCapabilities": {},
    }}


def acp_new_session(request_id: int, workspace_alias: str, configured_paths: dict) -> dict:
    """Paths are local administrator configuration, never Slack input paths."""
    cwd = configured_paths.get(workspace_alias)
    if not isinstance(cwd, str) or not cwd.startswith("/"):
        raise Rejected("workspace alias must resolve to a configured absolute path")
    return {"jsonrpc": "2.0", "id": request_id, "method": "session/new",
            "params": {"cwd": cwd, "mcpServers": []}}


def acp_prompt(request_id: int, bound_session_id: str, text: str) -> dict:
    return {"jsonrpc": "2.0", "id": request_id, "method": "session/prompt",
            "params": {"sessionId": bound_session_id,
                       "prompt": [{"type": "text", "text": text}]}}


def acp_cancel(bound_session_id: str) -> dict:
    return {"jsonrpc": "2.0", "method": "session/cancel",
            "params": {"sessionId": bound_session_id}}


def deny_permission(request_id: int | str) -> dict:
    """Fail closed for anything the task scope did not explicitly approve."""
    return {"jsonrpc": "2.0", "id": request_id,
            "result": {"outcome": {"outcome": "cancelled"}}}


def approve_permission(request_id: int | str, option_id: str) -> dict:
    """Approve only after a rule check proved the request in-scope."""
    return {"jsonrpc": "2.0", "id": request_id,
            "result": {"outcome": {"outcome": "selected", "optionId": option_id}}}
