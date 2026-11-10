#!/usr/bin/env python3
"""Controllable fake `devin acp` for session_bridge tests.

Process mode from argv[1]:
  normal    - answer prompts per prompt-text keywords
  silent    - never respond to prompt; ignore session/cancel
  die       - exit on session/prompt
  dieonce   - exit on the first session/prompt only (marker file in cwd),
              then behave like normal
  spawn     - spawn `sleep 300` child in the same process group, then
              behave like silent
  hanginit  - never respond to anything

Prompt-text keywords (normal mode):
  "/goal X" - attach native goal: status running during a ~1s turn,
              then paused (so the bridge's auto-resume is testable)
  "/goal resume" - goal turns += 50, status complete
  "/..."    - echo "执行指令:<text>" so command forwarding is testable
  "slow"    - periodic chunks up to 60s; answers cancel with cancelled
  "long"    - chunks totalling >9000 chars
  "perm"    - emit session/request_permission with allow options, wait
              for the client's decision, report the outcome
  "permdeny"- same but offers only a reject option
  otherwise - tool_call + a small chunk, then end_turn

_cognition.ai/goal/get answers {"goal": {...}} or {} when none;
_cognition.ai/goal/clear with a matching goalId clears it.

session/load echoes the requested sessionId after replaying one
history chunk; "bad-id" answers a JSON-RPC error instead.
"""
import json
import os
import subprocess
import sys
import threading
import time

MODE = sys.argv[1] if len(sys.argv) > 1 else "normal"
cancelled = threading.Event()
session_id = "fake-session-1"
PENDING_PERMS = {}
PERM_EVENTS = {}
GOAL = {}


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def update(upd):
    send({"jsonrpc": "2.0", "method": "session/update",
          "params": {"sessionId": session_id, "update": upd}})


def chunk(text):
    update({"sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": text}})


def think(text):
    update({"sessionUpdate": "agent_thought_chunk",
            "content": {"type": "text", "text": text}})


def ask_permission(text, mid):
    deny_only = "permdeny" in text
    options = ([{"optionId": "r1", "name": "拒绝", "kind": "reject_once"}]
               if deny_only else
               [{"optionId": "a1", "name": "允许一次", "kind": "allow_once"},
                {"optionId": "aa", "name": "总是允许", "kind": "allow_always"},
                {"optionId": "r1", "name": "拒绝", "kind": "reject_once"}])
    rid = f"perm-{mid}"
    ev = threading.Event()
    PERM_EVENTS[rid] = ev
    send({"jsonrpc": "2.0", "id": rid,
          "method": "session/request_permission",
          "params": {"sessionId": session_id,
                     "toolCall": {"toolCallId": "tc1", "kind": "execute",
                                  "title": "运行测试命令",
                                  "rawInput": {"command": "pytest -x"},
                                  "locations": [{"path": "/tmp/demo"}]},
                     "options": options}})
    ev.wait(timeout=float(os.environ.get("FAKE_PERM_WAIT", "15")))
    resp = PENDING_PERMS.pop(rid, None) or {}
    PERM_EVENTS.pop(rid, None)
    out = (resp.get("result") or {}).get("outcome") or {}
    chunk("审批结果:" + str(out.get("outcome", "?"))
          + ":" + str(out.get("optionId", "-")))
    if cancelled.is_set():
        send({"jsonrpc": "2.0", "id": mid,
              "result": {"stopReason": "cancelled"}})
        return True


def do_prompt(mid, text):
    tc = {"sessionUpdate": "tool_call", "toolCallId": "t1",
          "title": "读取文件", "status": "in_progress"}
    if "edit" in text:
        tc["locations"] = [{"path": os.path.join(os.getcwd(),
                                                 "draft.md")},
                           {"path": "/etc/abs.cfg"}]
    update(tc)
    if text.startswith("/goal resume"):
        if GOAL:
            GOAL["turns"] += 50
            GOAL["status"] = "complete"
        chunk("目标续跑完成。")
        send({"jsonrpc": "2.0", "id": mid,
              "result": {"stopReason": "end_turn"}})
        return
    if text.startswith("/goal "):
        GOAL.update({"id": "0", "objective": text[6:].strip(),
                     "status": "running", "turns": 1})
        for _ in range(4):
            chunk("goal 执行中 ")
            time.sleep(0.25)
            if cancelled.is_set():
                send({"jsonrpc": "2.0", "id": mid,
                      "result": {"stopReason": "cancelled"}})
                return
        GOAL.update({"status": "paused", "turns": 50})
        chunk("goal 暂停。")
        send({"jsonrpc": "2.0", "id": mid,
              "result": {"stopReason": "end_turn"}})
        return
    if text.startswith("/"):
        chunk("执行指令:" + text.strip())
        send({"jsonrpc": "2.0", "id": mid,
              "result": {"stopReason": "end_turn"}})
        return
    if "askhost" in text:
        # an agent→host request the bridge does not implement: it
        # must answer (e.g. -32601), not drop it and stall the turn
        send({"jsonrpc": "2.0", "id": 7777,
              "method": "elicitation/create",
              "params": {"message": "pick one", "mode": "form"}})
        chunk("host 已答复。")
    elif "long" in text:
        for i in range(120):
            chunk(f"第 {i} 段 " + "x" * 80 + "\n")
    elif "trickle" in text:
        for i in range(100):
            if cancelled.is_set():
                send({"jsonrpc": "2.0", "id": mid,
                      "result": {"stopReason": "cancelled"}})
                return
            chunk(f"滴答{i} ")
            time.sleep(0.05)
    elif "slow" in text:
        for _ in range(600):
            if cancelled.is_set():
                send({"jsonrpc": "2.0", "id": mid,
                      "result": {"stopReason": "cancelled"}})
                return
            time.sleep(0.1)
    elif "perm" in text:
        if ask_permission(text, mid):
            return
    else:
        update({"sessionUpdate": "tool_call_update",
                "toolCallId": "t1", "status": "completed"})
        think("先理清思路：确认输入、组织回答。")
        chunk("任务结果：一切正常。")
    send({"jsonrpc": "2.0", "id": mid, "result": {"stopReason": "end_turn"}})


def prompt_text(msg):
    try:
        return " ".join(p.get("text", "")
                        for p in msg["params"]["prompt"]).lower()
    except (KeyError, TypeError, AttributeError):
        return ""


def main():
    if MODE == "spawn":
        subprocess.Popen(["sleep", "300"])
    authed = [False]
    for raw in sys.stdin:
        try:
            msg = json.loads(raw)
        except ValueError:
            continue
        method = msg.get("method")
        mid = msg.get("id")
        if method:
            try:
                with open(".acp_calls", "a") as f:
                    f.write(method + "\n")
            except OSError:
                pass
        if "error" in msg and method is None:
            try:
                with open(".acp_calls", "a") as f:
                    f.write(f"error:{mid}:{msg['error'].get('code')}\n")
            except OSError:
                pass
        if method == "initialize":
            if MODE != "hanginit":
                result = {"protocolVersion": 1,
                          "agentCapabilities": {}}
                if os.environ.get("FAKE_AUTH_METHODS"):
                    result["authMethods"] = [{
                        "id": "devin-browser",
                        "name": "Log in with browser",
                        "description": "Sign in via your browser"}]
                send({"jsonrpc": "2.0", "id": mid, "result": result})
        elif method == "authenticate":
            authed[0] = True
            send({"jsonrpc": "2.0", "id": mid, "result": {}})
        elif method == "session/new":
            if os.environ.get("FAKE_REQUIRE_AUTH") and not authed[0]:
                send({"jsonrpc": "2.0", "id": mid,
                      "error": {"code": -32000,
                                "message": "ACP host has not "
                                "authenticated. Call the `authenticate` "
                                "ACP method with `meta.api_key` set to "
                                "the user's API key"}})
                continue
            update({"sessionUpdate": "available_commands_update",
                    "availableCommands": [
                        {"name": "goal",
                         "description": "Set a goal the agent keeps working on until it is done",
                         "input": {"hint": "[<objective>|resume|clear]"},
                         "_meta": {"cognition.ai/category": "Session"}},
                        {"name": "plan",
                         "description": "Switch to Plan mode",
                         "input": {"hint": "[prompt]"},
                         "_meta": {"cognition.ai/category": "Session"}},
                        {"name": "mcp",
                         "description": "List MCP servers",
                         "_meta": {"cognition.ai/category": "System"}},
                        {"name": "pstack:poteto-mode",
                         "description": "poteto agent style"}]})
            send({"jsonrpc": "2.0", "id": mid,
                  "result": {"sessionId": session_id,
                             "modes": {
                                 "currentModeId": "accept-edits",
                                 "availableModes": [
                                     {"id": "accept-edits",
                                      "name": "Code"}]},
                             "configOptions": [
                                 {"id": "model", "name": "Model",
                                  "type": "select",
                                  "currentValue": "swe-2-high",
                                  "options": [
                                      {"value": "swe-2-high",
                                       "name": "SWE-2"}]}]}})
        elif method == "session/load":
            sid = (msg.get("params") or {}).get("sessionId")
            try:
                with open(".acp_calls", "a") as f:
                    f.write(f"load:{sid}\n")
            except OSError:
                pass
            if os.environ.get("FAKE_REQUIRE_AUTH") and not authed[0]:
                send({"jsonrpc": "2.0", "id": mid,
                      "error": {"code": -32000,
                                "message": "ACP host has not "
                                "authenticated."}})
            elif sid == "bad-id":
                send({"jsonrpc": "2.0", "id": mid,
                      "error": {"code": -32602,
                                "message": "unknown session"}})
            else:
                chunk("历史回放。")
                send({"jsonrpc": "2.0", "id": mid,
                      "result": {"sessionId": sid}})
        elif method == "session/set_mode":
            send({"jsonrpc": "2.0", "id": mid, "result": {}})
        elif method == "session/prompt":
            if MODE == "die":
                os._exit(3)
            if MODE == "dieonce" and not os.path.exists(".dieonce"):
                open(".dieonce", "w").close()
                os._exit(3)
            if MODE == "ratelimit" and \
                    not os.path.exists(".ratelimit"):
                open(".ratelimit", "w").close()
                send({"jsonrpc": "2.0", "id": mid,
                      "error": {"code": -32603,
                                "message": "Reached free model rate "
                                "limit. Upgrade to Max for higher "
                                "limits, or switch to a different "
                                "model. Your limit will reset in "
                                "2 seconds (at Nov 10 00:00 UTC)."}})
                continue
            if MODE == "unavailable" and \
                    not os.path.exists(".unavailable"):
                open(".unavailable", "w").close()
                send({"jsonrpc": "2.0", "id": mid,
                      "error": {"code": -32010,
                                "message": "Connection error, send a "
                                "message to continue retrying",
                                "data": {"cognition.ai/errorKind":
                                         "unavailable",
                                         "cognition.ai/retryable":
                                         True}}})
                continue
            if MODE in ("silent", "spawn", "hanginit"):
                continue
            threading.Thread(target=do_prompt,
                             args=(mid, prompt_text(msg)),
                             daemon=True).start()
        elif method == "_cognition.ai/goal/get":
            send({"jsonrpc": "2.0", "id": mid,
                  "result": ({"goal": dict(GOAL)} if GOAL else {})})
        elif method == "_cognition.ai/goal/clear":
            if GOAL.get("id") == (msg.get("params") or {}).get("goalId"):
                GOAL.clear()
                if mid is not None:
                    send({"jsonrpc": "2.0", "id": mid, "result": {}})
            elif mid is not None:
                send({"jsonrpc": "2.0", "id": mid,
                      "error": {"code": -32602, "message": "Invalid params",
                                "data": "That goal is not the session's current goal."}})
        elif method == "session/cancel":
            cancelled.set()
        elif mid in PERM_EVENTS:
            PENDING_PERMS[mid] = msg
            PERM_EVENTS[mid].set()


if __name__ == "__main__":
    main()
