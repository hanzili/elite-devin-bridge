#!/usr/bin/env python3
"""Slack <-> local `devin acp` session bridge, reliability revision.

One channel-root mention starts one `devin acp` session bound to that
Slack thread. Thread replies feed the same session; '停止' cancels the
active prompt and only ever touches the session that thread owns.

Reliability properties vs smoke_sessions.py:
  - Bounded reads: a reader thread drains acp stdout into a queue, so
    every wait (startup, prompt, cancel grace) has a real deadline.
    Silence no longer stalls the loop forever.
  - Duplicate deliveries (event_id and channel+ts) are dropped before
    they can re-execute; one thread owns one session.
  - Stop has a deterministic outcome: session/cancel, a short grace,
    then kill of that session's own process group, and an explicit
    final post either way.
  - Prompt timeout is silence-based: a turn that produces no session
    activity for PROMPT_TIMEOUT seconds is cancelled, killed and
    recorded. An actively working turn is never cut off by wall clock.
  - Long replies are delivered whole, chunked under the post limit.
  - Reply text streams live: the first agent_message_chunk opens a
    draft message that chat_updates in place (throttled) with a cursor
    marker; overflow spills into follow-on drafts with the same
    segmentation post_long used, and turn end settles them as the
    final reply. Without a wired chat_update transport it falls back
    to one post at turn end.
  - Progress display is one live status message per thread (chat_update
    in place) showing the current tool, plus a lifecycle reaction on
    the thread root (hourglass -> check / stop / alarm / boom / zzz /
    arrows_counterclockwise for bridge reloads).
  - Live reload: SIGHUP / '桥重启' dumps the session table and exec()
    the same script with pipe fds kept; systemd 'restart' (SIGTERM)
    parks those fds in the unit's fd store instead. acp processes,
    context, drafts and in-flight turns carry over; 'systemctl stop'
    still ends everything (fds close with the unit).
  - A recorded in-flight prompt (.bridge_state.json 'inflight') is
    replayed via session/load after a hard death (SIGKILL, OOM, or a
    restart before live-reload support existed).
  - '停止' cancels the turn AND clears any attached goal so the
    drive loop cannot resurrect it; '取消目标' clears only the goal.
  - Terminal state is recorded per thread workdir (.bridge_state.json)
    so status answers distinguish running / idle / finished / stopped /
    interrupted (bridge restart) / unknown.
  - Every session/request_permission is written to a per-thread ledger
    (.bridge_approvals.json) before it is answered, so approval state is
    queryable read-only and survives restarts. The '审批' thread command
    and `--approvals` CLI read files only; they never touch the session.
  - BRIDGE_AUTO_APPROVE (default on) answers each request locally,
    preferring allow_always; set to 0 to wait for a human
    '允许'/'拒绝' in-thread with PERM_TIMEOUT auto-deny instead.
  - DEVIN_SESSION_MODE (default "bypass") is sent via session/set_mode
    after session/new; empty skips it.
  - Attachments flow both ways: user files are downloaded into the
    thread workdir inbox/ and their paths appended to the prompt;
    absolute file paths in replies (plus new media in the workdir)
    are uploaded back into the Slack thread.
  - Basic memory: each workdir gets an AGENTS.md pointing at a shared
    memory file (BRIDGE_MEMORY_FILE) plus a per-thread memory file.

Env: SLACK_BOT_TOKEN, SLACK_APP_TOKEN, SLACK_CHANNEL_ID
Opt: DEVIN_TASKS_DIR (~/devin-tasks), SESSION_IDLE_SEC (3600),
     PROMPT_TIMEOUT (1800, idle silence), CANCEL_GRACE_SEC (10),
     DEVIN_ACP ("devin acp"),
     DEVIN_SESSION_MODE ("bypass"), BRIDGE_AUTO_APPROVE ("1"),
     BRIDGE_MAX_POST (12000), BRIDGE_PROGRESS_GAP (3),
     BRIDGE_MAX_AUTO_SESSIONS (1) — cap on concurrent sessions
     started by bot/app-posted root mentions; extras queue FIFO.
     Human-posted mentions always start immediately.
     BRIDGE_MEMORY_FILE (~/.config/devin-bridge/memory.md)
     BRIDGE_GOAL_AUTO_RESUME ("1") — paused native /goal runs are
     resumed automatically up to BRIDGE_GOAL_RESUME_MAX (40) times.
     BRIDGE_GOAL_POLL_SEC (8) — in-turn goal/get cadence.
     BRIDGE_GOAL_WATCH_SEC (3) — idle watcher period for
     .devin-goal-native / .devin-goal files, which are attached as
     native /goal (claimed by rename to *.attached).
     BRIDGE_LIMIT_* — server-side limit governor: turns killed by
     "Reached … rate limit" / retryable refusals park into
     .bridge_limits.json and auto-resume after the server's reset
     window (or backoff 30s→300s), one per RESUME_GAP, up to
     RESUME_MAX (8). Desktop/CLI sessions whose transcript tail shows
     the same stall are resumed via session/load when EXTERNAL=1
     (lock takeover EXT_STEAL=1, lookback EXT_WINDOW=3600s, per-scan
     cap EXT_MAX=6). History: .bridge_limits.log, `!limits` in Slack.
"""
import array
import hashlib
import json
import math
import os
import queue
import random
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import unicodedata
import urllib.request
from collections import Counter, deque

import limit_governor

# UI language: "en" (default) or "zh". Input parsing accepts both
# languages regardless — L() only switches what the bridge prints.
BRIDGE_LANG = os.environ.get("BRIDGE_LANG", "en").lower()

def L(zh, en):
    """Return the UI string for the active BRIDGE_LANG."""
    return zh if BRIDGE_LANG == "zh" else en

TASKS_DIR = os.path.expanduser(os.environ.get("DEVIN_TASKS_DIR", "~/devin-tasks"))
IDLE_SEC = float(os.environ.get("SESSION_IDLE_SEC", "3600"))
TIMEOUT = float(os.environ.get("PROMPT_TIMEOUT", "1800"))
CANCEL_GRACE = float(os.environ.get("CANCEL_GRACE_SEC", "10"))
PERM_TIMEOUT = float(os.environ.get("PERMISSION_TIMEOUT_SEC", "300"))


def _perm_timeout_hint():
    """Human label for the auto-deny timeout. `inf` means the request
    never auto-denies — int(inf) raises OverflowError, so guard it."""
    if not math.isfinite(PERM_TIMEOUT):
        return L("不自动拒绝", "no auto-deny")
    return L(f"{int(PERM_TIMEOUT)} 秒未答复自动拒绝", f"auto-deny after {int(PERM_TIMEOUT)}s without reply")
PROGRESS_MIN_GAP = float(os.environ.get("BRIDGE_PROGRESS_GAP", "3"))
DRAFT_ENABLED = os.environ.get("BRIDGE_DRAFT", "1") != "0"
DRAFT_MIN_GAP = float(os.environ.get("BRIDGE_DRAFT_GAP", "1.5"))
DRAFT_CURSOR = " ▍"
# chat.update rejects longer text than chat.postMessage does; draft
# segments must stay comfortably under that tighter limit.
DRAFT_POST = int(os.environ.get("BRIDGE_DRAFT_POST", "3900"))
# Reasoning visibility: thought chunks are kept out of the reply
# itself but surface as a live 🧠 status glimpse plus one trimmed
# post-turn message (head=plan, tail=conclusion).
THINK_ENABLED = os.environ.get("BRIDGE_THINK", "0") != "0"
THINK_POST_MAX = int(os.environ.get("BRIDGE_THINK_MAX", "3000"))
REACT_WORKING = "hourglass_flowing_sand"
REACT_RELOADING = "arrows_counterclockwise"
REACT_LIMITED = "double_vertical_bar"
REACTS_ALL = {REACT_WORKING, REACT_RELOADING, REACT_LIMITED,
              "white_check_mark",
              "octagonal_sign", "alarm_clock", "boom", "x", "zzz"}
CHANNEL = os.environ.get("SLACK_CHANNEL_ID", "")
# Senders the bridge must ignore completely — matched against an
# event's bot_id / app_id / user. Comma-separated; used to revoke a
# bot's delegated authority (e.g. the dogson auto-dispatcher) without
# touching workspace admin settings.
BLOCKED_SENDERS = frozenset(
    x.strip() for x in
    os.environ.get("BRIDGE_BLOCK_SENDERS", "").split(",") if x.strip())
ACP_CMD = shlex.split(os.environ.get("DEVIN_ACP", "devin acp"))
# ACP never reads local CLI credentials: the host must call
# `authenticate` with _meta.api_key before session/new. Source order:
# DEVIN_ACP_API_KEY env, then the Devin Desktop account blob
# (follows whichever account is signed into the Desktop — switching
# accounts there switches the bridge's billing account too), then the
# CLI credential store's windsurf_api_key (refreshed on `devin auth
# login`).
ACP_CREDENTIALS = os.environ.get(
    "DEVIN_ACP_CREDENTIALS",
    "~/.local/share/devin/credentials.toml")
ACP_DESKTOP_STATE = os.environ.get(
    "DEVIN_ACP_DESKTOP_STATE",
    "~/.config/Devin/User/globalStorage/state.vscdb")


def _acct_fingerprint():
    """Short fingerprint of the credential the bridge would use right
    now. Limits are per-account; the user rotates accounts on Desktop,
    which the bridge follows — a fingerprint change means every parked
    turn is worth retrying early (the parked reset window belonged to
    the previous account's quota)."""
    key = _acp_api_key()
    return hashlib.sha256(key.encode()).hexdigest()[:12] if key else ""


def _acp_api_key():
    key = os.environ.get("DEVIN_ACP_API_KEY", "").strip()
    if key:
        return key
    try:
        import sqlite3
        db = sqlite3.connect(
            "file:" + os.path.expanduser(ACP_DESKTOP_STATE)
            + "?mode=ro", uri=True)
        try:
            row = db.execute(
                "SELECT value FROM ItemTable "
                "WHERE key='windsurfAuthStatus'").fetchone()
        finally:
            db.close()
        if row:
            v = row[0] if isinstance(row[0], str) \
                else row[0].decode("utf8", "replace")
            m = re.search(r'"apiKey":"(devin-session-token\$[^"]+)"', v)
            if m:
                return m.group(1)
    except Exception:
        pass
    try:
        with open(os.path.expanduser(ACP_CREDENTIALS)) as f:
            m = re.search(r'windsurf_api_key\s*=\s*["\']([^"\']+)',
                          f.read())
        return m.group(1).strip() if m else ""
    except OSError:
        return ""


# Devin CLI/Desktop's global session store: every session this machine
# knows (interactive `devin`, Desktop tabs, and this bridge's acp
# sessions) lives here, so it is the single inventory for cross-device
# listing and !adopt lookups. session_locks/<id>.lock holds a PID while
# a devin frontend keeps the session open.
DEVIN_DB = os.path.expanduser(os.environ.get(
    "BRIDGE_DEVIN_DB", "~/.local/share/devin/cli/sessions.db"))
DEVIN_LOCKS = os.environ.get(
    "BRIDGE_DEVIN_LOCKS",
    os.path.join(os.path.dirname(DEVIN_DB), "session_locks"))


def _devin_db():
    import sqlite3
    return sqlite3.connect("file:" + DEVIN_DB + "?mode=ro", uri=True)


def list_devin_sessions(limit=15):
    """Newest sessions across CLI/Desktop/bridge. Read-only; a missing
    or locked store just yields an empty list."""
    try:
        db = _devin_db()
        try:
            rows = db.execute(
                "SELECT id, working_directory, title, last_activity_at"
                " FROM sessions WHERE hidden=0"
                " ORDER BY last_activity_at DESC LIMIT ?", (limit,)
            ).fetchall()
        finally:
            db.close()
    except Exception:
        return []
    return [{"id": r[0], "workdir": r[1], "title": r[2] or "",
             "activity": r[3] or 0,
             "lock_pid": devin_lock_pid(r[0])} for r in rows]


def devin_session(sid):
    try:
        db = _devin_db()
        try:
            r = db.execute(
                "SELECT id, working_directory, title, last_activity_at"
                " FROM sessions WHERE id=?", (sid,)).fetchone()
        finally:
            db.close()
    except Exception:
        return None
    if not r:
        return None
    return {"id": r[0], "workdir": r[1], "title": r[2] or "",
            "activity": r[3] or 0, "lock_pid": devin_lock_pid(r[0])}


def _first_line(s, n=40):
    lines = (s or "").splitlines()
    return lines[0][:n] if lines else ""


def _one_line(s, n=60):
    return re.sub(r"\s+", " ", (s or "")).strip()[:n]


# remote-pad lives outside the bridge: inputctl injects keys/pointer via
# ydotoold, screengrab pulls a frame through the ScreenCast portal.
PAD_TOKEN = os.path.expanduser(os.environ.get(
    "BRIDGE_PAD_TOKEN", "~/.config/remote-pad/token"))
PAD_DIR = os.path.expanduser(os.environ.get("BRIDGE_PAD_DIR",
                                           "~/remote-pad"))
INPUTCTL = os.path.join(PAD_DIR, "inputctl.py")
SCREENGRAB = os.path.join(PAD_DIR, "screengrab.py")
# Remote-control commands (!pad/!shot/!type/!key) only exist when a
# remote-pad install is present — BRIDGE_PAD=1 forces on, =0 forces off.
_pad_env = os.environ.get("BRIDGE_PAD", "auto")
PAD_ENABLED = _pad_env == "1" or (_pad_env != "0"
                                 and os.path.isdir(PAD_DIR))


def _open_capped_append(path, cap=512 * 1024):
    """Append-mode writer that keeps the log under ~cap bytes by
    dropping the oldest half on open (acp stderr is chatty)."""
    try:
        if os.path.getsize(path) > cap:
            with open(path, "rb") as f:
                f.seek(-cap // 2, os.SEEK_END)
                keep = f.read()
            with open(path, "wb") as f:
                f.write(b"--- log truncated ---\n" + keep)
    except OSError:
        pass
    return open(path, "ab", buffering=0)


def _stderr_hint(workdir):
    """Last ERROR/WARN line in the session's acp stderr log, cleaned
    for Slack ('' when nothing useful is there)."""
    path = os.path.join(workdir, ".acp_stderr.log")
    try:
        with open(path, "rb") as f:
            if os.path.getsize(path) > 16384:
                f.seek(-16384, os.SEEK_END)
            tail = f.read().decode("utf-8", "replace")
    except OSError:
        return ""
    lines = [l for l in tail.splitlines()
             if " ERROR " in l or " WARN " in l]
    if not lines:
        return ""
    m = re.search(r"(?:ERROR|WARN)\s+(.*)", lines[-1])
    return _one_line(m.group(1), 140) if m else ""


def devin_lock_pid(sid):
    """Live PID holding <sid>'s lock file, or None (stale/absent = the
    session is free to adopt)."""
    try:
        with open(os.path.join(DEVIN_LOCKS, sid + ".lock")) as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return pid
    except (OSError, ValueError):
        return None


SESSION_MODE = os.environ.get("DEVIN_SESSION_MODE", "bypass")
AUTO_APPROVE = os.environ.get("BRIDGE_AUTO_APPROVE", "1") != "0"
MAX_POST = int(os.environ.get("BRIDGE_MAX_POST", "12000"))
# Auto-dispatched (bot-posted) tasks run at most this many `devin acp`
# sessions at once; the rest queue FIFO. Human-posted mentions always
# start immediately. Bounds memory and prevents dispatch pile-ups.
MAX_AUTO = int(os.environ.get("BRIDGE_MAX_AUTO_SESSIONS", "1"))
MEMORY_FILE = os.environ.get("BRIDGE_MEMORY_FILE",
                             "~/.config/devin-bridge/memory.md")
# Session-bound user messages are also recorded into the personal-context
# queue so instructions have a traceable source id; PCTX_MESSAGES_PUT=""
# disables the tap entirely (e.g. on hosts without the pipeline skill, or
# in tests — read lazily so tests can set it after import).
PCTX_MESSAGES_PUT_DEFAULT = \
    "~/.agents/skills/personal-context-pipeline/scripts/messages.py"
# Deliverable file types pushed into the thread after a turn. Code
# files (.py/.ts/…) deliberately excluded — edits to them show up in
# the 📁 list but aren't spammed as attachments unless the reply
# names them outright.
SEND_EXT = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".mp4", ".mov",
            ".mp3", ".wav", ".pdf", ".svg",
            ".md", ".markdown", ".txt", ".csv", ".tsv",
            ".xlsx", ".xls", ".docx", ".doc", ".pptx",
            ".html", ".htm", ".ipynb", ".json",
            ".zip", ".tar", ".gz", ".sql"}
# Auto-send budget per turn: beyond this, files pool into `!files`.
AUTO_SEND_MAX = 3
# Never uploaded even when named — bridge/agent internal state.
INTERNAL_FILE_RE = re.compile(
    r"(^|/)\.[^/]+$|(^|/)(memory\.md|\.devin-goal[^/]*)$", re.I)
# Working files that are almost never the deliverable — skipped by
# auto-send but still sent when the reply names them.
WORKFILE_NAMES = {"readme", "skill", "agents", "claude", "goal",
                  "todo", "changelog", "contributing", "data",
                  "license", "license.md", "notes", "summary"}
MAX_UPLOAD_BYTES = 200 * 1024 * 1024
GOAL_POLL_SEC = float(os.environ.get("BRIDGE_GOAL_POLL_SEC", "8"))
GOAL_WATCH_SEC = float(os.environ.get("BRIDGE_GOAL_WATCH_SEC", "3"))
GOAL_RESUME_MAX = int(os.environ.get("BRIDGE_GOAL_RESUME_MAX", "40"))
GOAL_AUTO_RESUME = os.environ.get("BRIDGE_GOAL_AUTO_RESUME", "1") != "0"
GOAL_FILES = (".devin-goal-native", ".devin-goal")
GOAL_STATUS_CN = {"running": L("进行中", "in progress"), "paused": L("已暂停", "paused"),
                  "blocked": L("已阻塞", "blocked"), "complete": L("已完成", "done"),
                  "cleared": L("已清除", "cleared")}
# Server-side limit handling: a turn killed by "Reached … rate limit"
# or a retryable refusal parks into .bridge_limits.json instead of
# failing; the sweeper re-runs it after the server's quoted reset
# window (or bounded backoff), one resume per BRIDGE_LIMIT_RESUME_GAP
# seconds so a batch doesn't re-trip the cap in lockstep. The same
# watcher scans the shared devin session store for Desktop/CLI sessions
# whose transcript tail shows they stalled on the limit; those are
# resumed via session/load + a nudge prompt once no live process owns
# the session; sessions still locked by a live frontend are tracked as
# 'suspects' (visible in !limits) and only taken over when
# BRIDGE_LIMIT_EXT_STEAL=1. History lands in .bridge_limits.log (JSONL).
LIMIT_RESUME_MAX = int(os.environ.get("BRIDGE_LIMIT_RESUME_MAX", "8"))
LIMIT_BASE_BACKOFF = float(os.environ.get("BRIDGE_LIMIT_BACKOFF", "30"))
LIMIT_BACKOFF_CAP = float(os.environ.get("BRIDGE_LIMIT_BACKOFF_CAP",
                                         "300"))
LIMIT_RESUME_GAP = float(os.environ.get("BRIDGE_LIMIT_RESUME_GAP",
                                        "15"))
LIMIT_SWEEP_SEC = float(os.environ.get("BRIDGE_LIMIT_SWEEP_SEC", "15"))
LIMIT_ADOPT_AGE = float(os.environ.get("BRIDGE_LIMIT_ADOPT_AGE",
                                       "21600"))
LIMIT_EXT = os.environ.get("BRIDGE_LIMIT_EXTERNAL", "1") != "0"
# Off by default: unlinking a lock a LIVE frontend still holds lets a
# second driver into a session that frontend believes it owns. Enable
# only when the dead-turn victims matter more than that race.
LIMIT_EXT_STEAL = os.environ.get("BRIDGE_LIMIT_EXT_STEAL", "0") != "0"
LIMIT_EXT_WINDOW = float(os.environ.get("BRIDGE_LIMIT_EXT_WINDOW",
                                        "3600"))
# Unlocked victims (stale lock / no owner) get a much longer lookback:
# an orphaned dead turn stays orphaned, so it is still worth rescuing
# hours later. Live-locked sessions stay tight — the frontend owner
# will retry them itself.
LIMIT_EXT_ORPHAN_WINDOW = float(os.environ.get(
    "BRIDGE_LIMIT_EXT_ORPHAN_WINDOW", "21600"))
LIMIT_EXT_SCAN = float(os.environ.get("BRIDGE_LIMIT_EXT_SCAN_SEC", "90"))
LIMIT_EXT_MAX = int(os.environ.get("BRIDGE_LIMIT_EXT_MAX", "6"))
LIMIT_EXT_POST = os.environ.get("BRIDGE_LIMIT_EXT_POST", "1") != "0"
# A mid-turn tail is only 'dead' when its newest node is this stale —
# long-running tool calls and thinking steps can silence a live turn
# for minutes, so locked sessions need real staleness before we even
# watch them, let alone take them over.
LIMIT_EXT_FRESH = float(os.environ.get("BRIDGE_LIMIT_EXT_FRESH", "600"))
EXT_RESUME_PROMPT = os.environ.get(
    "BRIDGE_LIMIT_EXT_PROMPT",
    L("（自动恢复：服务端限额冷却已结束）请继续完成刚才被中断的工作；", "(auto-resume: server-side limit cooldown ended) please continue the interrupted work;") +
    L("如果上一轮其实已经做完，简短回复已完成即可。", "If the last turn actually finished, just reply briefly that it's done."))
RESUME_NUDGE = (L("（自动恢复：上一轮因服务端限额中断）请继续未完成的", "(auto-resume: last turn was cut by a server-side limit) please continue the unfinished") +
                L("工作；如果已经做完，简短回复已完成即可。", 'work; if it already finished, a brief "done" reply is fine.'))
AGENTS_TMPL = L("""# 桥会话上下文

- 本目录绑定 Slack 线程 {ts}，是你与该线程的持久工作目录。
- 全局记忆：{mem} —— 开工先读；学到需要跨会话保留的事（用户偏好、
  决定、关键路径、项目状态）按日期小节追加，保持精简。
- 线程记忆：本目录 `.bridge_thread_memory.md`，规则同上。
- 给用户发文件/图片：回复里直接写绝对路径，桥会自动上传到线程。
- 用户附件：下载到本目录 `inbox/` 并把路径写进 prompt。
- 本桥无超时、无长度截断、无审批时限限制。
- 回复直接写标准 Markdown：**粗体**、#标题、| 表格 |、[文字](链接)、
  - [ ] 任务清单等，桥会自动转成 Slack 能正常渲染的格式，不用
  特意迁就 Slack 语法。
- 清理进程只杀自己登记过的 PID/进程组：禁止 `pkill -f "devin acp"` 之类
  全模式匹配——所有线程会话共用同一命令行，误杀会带走全部会话（含你自己）。
""", """# Bridge session context

- This directory is bound to Slack thread {ts} — your persistent
  workdir for the thread.
- Global memory: {mem} — read it first; append durable cross-session
  facts (user preferences, decisions, key paths, project state) under
  dated headings, keep it concise.
- Thread memory: `.bridge_thread_memory.md` here, same rules.
- To send the user files/images: write the absolute path in your reply;
  the bridge uploads it to the thread.
- User attachments land in `inbox/` here; reference their paths.
- No timeout, no length cap, no approval deadline on this bridge.
- Reply in standard Markdown: **bold**, # headings, | tables |,
  [links](url), - [ ] checklists — the bridge renders it for Slack.
- Only kill PIDs/process groups you registered: never broad-match like
  `pkill -f "devin acp"` — all thread sessions share one command line
  and a stray match kills every session (including yours).
""")
MENTION = re.compile(r"<@[A-Z0-9]+>")
STOP_WORDS = {"停止", "stop", "cancel", "取消", "停", "停下", "停一下",
              "打断", "别跑了", "终止", "停止运行", "取消任务"}
STATUS_WORDS = {"进度", "progress", "status", "状态"}
APPROVAL_WORDS = {"审批", "approvals", "approval", "权限"}
ALLOW_WORDS = {"允许", "allow", "approve", "同意", "批准"}
DENY_WORDS = {"拒绝", "deny", "reject"}
GOAL_CLEAR_WORDS = {"取消目标", "清除目标", "清空目标", "删除目标",
                    "取消goal", "goal clear", "clear goal"}
LIMIT_WORDS = {"限额", "限流", "冷却队列", "限额队列", "限额状态"}
RELOAD_WORDS = {"桥重启", "重启桥", "热重启", "桥reload", "reload bridge"}
HELP_WORDS = {"帮助", "help", "使用说明", "命令帮助", "怎么使用",
              "怎么用", "操作说明"}
# "!<head> [arg]": head keys the bridge command table; anything not
# listed falls through to the session as a "/<head>" slash command.
BANG_CMDS = {
    "stop": "stop", "cancel": "stop", "abort": "stop", "halt": "stop",
    "status": "status", "stats": "status",
    "queue": "queue", "q": "queue",
    "reload": "reload", "restart": "reload", "reboot": "reload",
    "help": "help", "h": "help", "帮助": "help",
    "allow": "allow", "approve": "allow", "yes": "allow", "y": "allow",
    "deny": "deny", "reject": "deny", "no": "deny", "n": "deny",
    "approvals": "approvals", "perms": "approvals", "perm": "approvals",
    "reset": "reset", "new": "reset", "wipe": "reset",
    "limits": "limits", "limit": "limits", "cooldown": "limits",
    "sessions": "sessions", "ls": "sessions", "list": "sessions",
    "adopt": "adopt", "takeover": "adopt", "attach": "adopt",
    "handoff": "handoff", "handover": "handoff",
    "release": "release", "detach": "release",
    "peek": "peek", "look": "peek", "看看": "peek",
    "board": "board", "看板": "board",
    "files": "files", "ls-files": "files", "文件": "files",
    "send": "send", "get": "send", "发": "send",
    "pad": "pad", "remote": "pad", "遥控": "pad",
    "shot": "shot", "ss": "shot", "screenshot": "shot", "截图": "shot",
    "type": "type", "输入": "type",
    "key": "key", "press": "key", "按键": "key",
}
# "!goal <x>" stays a session slash command except these, which the
# bridge answers itself (goal files + RPC + markers).
GOAL_ARG_CLEAR = {"clear", "cancel", "stop", "off", "remove", "rm",
                  "取消", "删除", "清除"}
HELP_CARD = L("""*桥命令*（`!` 开头是命令，不带 `!` 就是聊天）
• `!stop` — 立刻停止当前执行；挂着的目标会一并清除
• `!status` / `!queue` — 当前状态 / 排队情况
• `!goal <内容>` — 挂一个自动长跑目标；`!goal cancel` 撤掉
• `!allow` / `!deny` — 审批待处理请求（或直接点按钮）
• `!approvals` — 待审批列表
• `!reset` — 重置本线程会话，下次发言全新开始
• `!sessions` — 列出本机所有 Devin 会话（桌面/CLI/桥，可接管状态）
• `!adopt [会话id]` — 接管桌面/CLI 会话到本线程；不带 id 接管最近一个
• `!handoff` — 显示本线程会话 id，桌面 `devin -r` 或会话列表接管
• `!release` — 桥松开本线程会话（上下文保留，随时可再接回）
• `!limits` — 限额冷却队列（哪些回合在等重试、何时自动续跑）
• `!peek [会话id]` — 只看不动：最近几条动态（不接管不抢锁）
• `!board` — 原地弹一份即时看板（只你可见）；频道名下面书签直达置顶看板
• `!files` — 本线程可取的产出文件清单
• `!send <名字|all>` — 拉取文件：单个指名或全部待发
• `!pad` — 手机遥控入口：web 控制台/RDP/KDE Connect 连接卡
• `!type <文字>` — 打进电脑当前窗口（中文自动走剪贴板）
• `!key <组合>` — 给电脑按键，如 `!key ctrl+alt+t`、`!key enter`
• `!shot` — 截一张电脑桌面发上来（首次需在本机点一次共享）
• `!reload` — 桥服务热重启，在跑的会话不中断
• `!help` — 本卡片
其它 `!命令` 原样发给会话（如 `!mode`、`!clear`）。
也能直接说中文：停止 / 取消目标 / 状态 / 桥重启 / 允许 / 拒绝 / 审批。""",
"""*Bridge commands* (`!`-prefixed are commands; plain text chats)
• `!stop` — stop the current run; clears any attached goal
• `!status` / `!queue` — current state / queued work
• `!goal <text>` — attach a long-running auto goal; `!goal cancel` drops it
• `!allow` / `!deny` — answer a pending approval (or tap its buttons)
• `!approvals` — pending approval list
• `!reset` — reset this thread's session; next message starts fresh
• `!sessions` — list this machine's Devin sessions (Desktop/CLI/bridge)
• `!adopt [session-id]` — pull a Desktop/CLI session into this thread; no id adopts the latest
• `!handoff` — show this thread's session id for `devin -r` or Desktop
• `!release` — release the session (context kept; re-adopt any time)
• `!limits` — rate-limit cooldown queue (parked turns, retry times)
• `!peek [session-id]` — look without touching: recent activity, no lock
• `!board` — an ephemeral live board only you can see; the channel bookmark jumps to the pinned board
• `!files` — files produced in this thread, ready to pull
• `!send <name|all>` — pull files: one by name or all pending
• `!reload` — hot-restart the bridge; running sessions unaffected
• `!help` — this card
Any other `!cmd` is passed to the session as a slash command (e.g. `!mode`, `!clear`).""")
TERMINAL = {"finished", "stopped", "failed", "timeout", "crashed",
            "interrupted", "reaped"}
CRASH_RESUME_MAX = 2
# Live-reload: the bridge can replace itself without killing acp
# children. SIGHUP / the 桥重启 keyword dump the session table and
# exec() the same script, keeping every pipe fd; systemd restart
# (SIGTERM) parks those fds in systemd's fd store instead. Either way
# restored sessions keep their process, context and in-flight turn.
LIVE_FILE = os.path.join(TASKS_DIR, ".bridge_live.json")
LIVE_ENV = "BRIDGE_LIVE_FILE"
ADOPT_MAX_AGE = 6 * 3600
# Dashboard auto-refresh throttle — a burst of session events collapses
# into one in-place update instead of N chat_update calls.
BOARD_GAP = int(os.environ.get("BRIDGE_BOARD_GAP", "30"))
# Host vitals on the board; a metric over threshold for CLEAN_STRIKES
# consecutive board samples auto-spawns a cleanup session (auto slot).
VITAL_DISK = float(os.environ.get("VITAL_DISK_WARN", "85"))
VITAL_MEM = float(os.environ.get("VITAL_MEM_WARN", "90"))
VITAL_CPU = float(os.environ.get("VITAL_CPU_WARN", "90"))
CLEAN_STRIKES = int(os.environ.get("CLEAN_STRIKES", "3"))
CLEAN_COOLDOWN = float(os.environ.get("CLEAN_COOLDOWN", "1800"))
AUTOCLEAN = os.environ.get("AUTOCLEAN", "0").lower() \
    not in ("0", "no", "false")
CLEAN_PROMPTS = {
    "disk": L("本机磁盘占用已到 {disk}%。请诊断根分区最占空间的目录", "Host disk usage hit {disk}%. Diagnose the biggest directories on the root partition") +
            L("（du -x -d1 逐层看 /、~、/var、~/.cache、journal 日志、", "(du -x -d1 across /, ~, /var, ~/.cache, journal logs,") +
            L("docker overlay），然后只做安全清理：apt clean、", "docker overlay), then only safe cleanup: apt clean,") +
            L("journalctl --vacuum-time=7d、清 7 天前的 /tmp、", "journalctl --vacuum-time=7d, clear /tmp files older than 7 days,") +
            L("docker system prune（只清未用镜像和已停容器）。", "docker system prune (unused images and stopped containers only).") +
            L("绝不删用户文档、git 仓库、数据库、非缓存数据。", "Never delete user docs, git repos, databases, or non-cache data.") +
            L("报告：释放了多大、现在还剩多少。", "Report: how much was freed and how much remains."),
    "mem": L("本机内存占用 {mem}% 且持续偏高。用 ps aux --sort=-rss ", "Host memory is at {mem}% and staying high. Use ps aux --sort=-rss to ") +
           L("找出 RSS 最大的 15 个进程，判断哪些疑似泄露/僵尸/失控。", "find the top-15 RSS processes and judge which look leaked/zombie/runaway.") +
           L("可以回收明确的 zombie/defunct；不要杀 devin、slack、", "You may reap clear zombie/defunct procs; don't kill devin, slack,") +
           L("浏览器、系统服务或你拿不准的进程。报告：主要内存被谁占、", "browsers, system services, or anything you're unsure of. Report: who holds the memory,") +
           L("你处理了什么、建议用户做什么。", "what you did, and what you recommend the user do."),
    "cpu": L("本机 CPU 占用 {cpu}% 且持续多轮。用 top -b -n1 和 ", "Host CPU is at {cpu}% and stayed high across samples. Use top -b -n1 and ") +
           L("ps aux --sort=-%cpu 找前 10 的烧 CPU 进程，判断是否有", "ps aux --sort=-%cpu to find the top-10 CPU burners and check for") +
           L("失控/死循环任务。报告发现；对确认的失控任务给出处理", "runaway/looping tasks. Report findings; for confirmed runaways give") +
           L("建议，不拿准的进程不要直接 kill。", "recommendations; don't kill anything you're unsure about."),
}
# Bridge self-audit: every AUDIT_INTERVAL a 🩺 session reviews the
# bridge's own logs/state/Slack surfaces and fixes what it safely can.
AUDIT_INTERVAL = float(os.environ.get("AUDIT_INTERVAL",
                                      str(12 * 3600)))
AUDIT_ENABLED = os.environ.get("AUDIT_ENABLED", "0").lower() \
    not in ("0", "no", "false")
AUDIT_PROMPT = L("""你是这座 Slack↔Devin 桥的定期体检医生（每 12 小时一次自动体检）。

桥本体：
- 代码 ~/elite-devin-bridge/session_bridge.py，服务 systemctl --user
  status session-bridge，日志 journalctl --user -u session-bridge
  -n 400 --no-pager
- 每个 Slack 线程一个工作目录 ~/devin-tasks/<ts>_<rand>/，内含
  .state.json（running/failed/finished/queued）、inbox/ 用户附件
- 看板/私聊看板/Canvas 状态：~/devin-tasks/.bridge_board.json
- 限额挂起状态：~/devin-tasks/.bridge_limits.json（如存在）

本次健康摘要（桥自检）：
{digest}

请做体检：
1. 翻 journalctl 最近 400 行：找反复出现的错误模式（上传超时、
   socket 断连、API 报错、崩溃重启），判断哪些是真问题
2. 扫 ~/devin-tasks：state=failed 或 queued/running 状态卡了
   超过 24 小时的线程目录、孤儿 inbox 大文件、.state.json 异常
3. 验证 Slack 面：频道置顶看板还活着吗（.bridge_board.json 的
   ts）、DM 看板/Canvas 条目是否有效
4. 安全修复：把卡死/终态很久的线程目录整体挪到
   ~/devin-tasks/.trash/（不删）、清掉明显孤儿文件。
   不要重启服务、不要改 .bridge_board.json、不要动凭据、
   不要碰正在运行的会话目录
5. 在线程里输出简短报告：发现的问题、已修的问题、
   需要用户/开发者注意的事项（如果一切正常就说正常）。""",
"""You are the scheduled self-audit doctor for this Slack↔Devin bridge
(runs every AUDIT_INTERVAL seconds when AUDIT_ENABLED=1).

Bridge internals:
- Code: ~/elite-devin-bridge/session_bridge.py; service: systemctl
  --user status session-bridge; logs: journalctl --user -u
  session-bridge -n 400 --no-pager
- One workdir per Slack thread: ~/devin-tasks/<ts>_<rand>/ containing
  .state.json (running/failed/finished/queued) and inbox/ attachments
- Board/DM-board/Canvas state: ~/devin-tasks/.bridge_board.json
- Parked limit turns: ~/devin-tasks/.bridge_limits.json (if present)

Health digest produced by the bridge itself:
{digest}

Audit steps:
1. Scan the last 400 journalctl lines for repeating error patterns
   (upload timeouts, socket drops, API errors, crash restarts) and
   judge which are real problems
2. Scan ~/devin-tasks for thread dirs stuck in failed or
   queued/running state over 24h, orphaned large inbox files, and
   malformed .state.json
3. Verify Slack surfaces: is the pinned channel board still alive
   (the ts in .bridge_board.json), are DM-board/canvas entries valid
4. Safe fixes only: move long-stuck or terminal thread dirs into
   ~/devin-tasks/.trash/ (never delete), remove obvious orphans.
   Do NOT restart the service, modify .bridge_board.json, touch
   credentials, or touch dirs of sessions that are still running
5. Post a short report in the thread: problems found, fixes applied,
   and items needing human attention (if all clear, say so).""")
# 'In flight' windows: a mid-turn node (tool result, user msg,
# assistant+tool_calls) can legitimately go quiet for a long-running
# command, so the mid-turn cap is generous; a final assistant answer
# only holds 🔥 for a short grace (it might precede more tool calls).
BOARD_LIVE_SEC = int(os.environ.get("BRIDGE_BOARD_LIVE_SEC", "120"))
BOARD_TURN_CAP = int(os.environ.get("BRIDGE_BOARD_TURN_CAP", "3600"))
BOARD_MAX_ROWS = int(os.environ.get("BRIDGE_BOARD_MAX_ROWS", "10"))
# A prompt whose result line sat in the reader's userspace buffer at
# exec time is unrecoverable; give restored turns a bounded stall
# before falling back to session/load replay.
LOST_TURN_GRACE = 120

STATE_NAMES = {
    "starting": L("启动中", "starting"),
    "queued": L("排队中", "queued"),
    "idle": L("等待中（空闲）", "waiting (idle)"),
    "running": L("执行中", "running"),
    "stopping": L("正在停止", "stopping"),
    "finished": L("已完成", "done"),
    "stopped": L("已停止", "stopped"),
    "failed": L("出错结束", "ended with error"),
    "timeout": L("静默超时终止", "silence-timeout terminated"),
    "crashed": L("进程异常退出", "process exited"),
    "interrupted": L("已中断（桥服务重启）", "interrupted (bridge restarted)"),
    "reaped": L("已回收（空闲超时）", "reaped (idle timeout)"),
    "rate_limited": L("限额冷却中（待自动重试）", "limit cooldown (auto-retry pending)"),
}

# Serializes every read-modify-write of the per-thread state and
# approvals files. Writers live on different threads (session/prompt
# loop, socket handler, reaper, startup sweep); writes are rare, so one
# process-wide lock is enough. RLock allows mark_interrupted to call
# write_state while it holds the lock for the ledger pass.
_file_lock = threading.RLock()


class AcpExited(RuntimeError):
    pass


class AcpError(RuntimeError):
    """An error object from the acp wire, unwrapped for humans:
    {'code':…, 'message':…, 'data':{…retryable…}} must not reach
    Slack as a raw dict repr."""

    def __init__(self, err):
        data = err.get("data") if isinstance(err, dict) else None
        self.data = data or {}
        self.error_kind = self.data.get("cognition.ai/errorKind")
        self.retryable = isinstance(err, dict) and bool(
            (err.get("data") or {}).get("cognition.ai/retryable"))
        msg = (err.get("message") or str(err)) \
            if isinstance(err, dict) else str(err)
        super().__init__(msg)


class PromptTimeout(TimeoutError):
    pass


class StopStalled(RuntimeError):
    pass


def _dur(sec):
    m = int(sec) // 60
    return f"{m}m" if m < 60 else f"{m // 60}h{m % 60}m"


def _state_path(workdir):
    return os.path.join(workdir, ".bridge_state.json")


def write_state(workdir, state, activity="", detail="", extra=None):
    data = {"state": state, "activity": activity, "detail": detail,
            "updated": time.time()}
    if extra:
        data.update(extra)
    tmp = (f"{_state_path(workdir)}.{os.getpid()}."
           f"{threading.get_ident()}.tmp")
    try:
        with _file_lock:
            cur = read_state(workdir)
            if cur is not None:
                prev = cur.get("state")
                # A recorded terminal state is final for this workdir.
                # Stale writers (prompt thread racing stop/reaper, a
                # redelivery after restart) must not resurrect the task
                # or replace the terminal outcome.
                if prev in TERMINAL and prev != state:
                    return
                # The acp session id outlives the process; interrupted
                # and terminal writes rebuild data without it, so carry
                # it forward for a later session/load resume.
                if cur.get("session_id") and "session_id" not in data:
                    data["session_id"] = cur["session_id"]
            with open(tmp, "w") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp, _state_path(workdir))
    except OSError:
        pass


def read_state(workdir):
    try:
        with open(_state_path(workdir)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _goal_cleared_path(workdir):
    return os.path.join(workdir, ".bridge_goal_cleared")


def _goal_mark_cleared(workdir):
    """Marker that the last goal was dropped by a person, not by the
    drive loop ending naturally. A replayed "/goal…" turn (adopted
    inflight, crash/timeout resume) consults this once and skips —
    that is the 'cancelled goal resurrects on restart' hole."""
    try:
        with open(_goal_cleared_path(workdir), "w") as f:
            f.write(str(time.time()))
    except OSError:
        pass


APPROVALS_FILE = ".bridge_approvals.json"
APPROVALS_KEEP = 100


def _approvals_path(workdir):
    return os.path.join(workdir, APPROVALS_FILE)


def load_approvals(workdir):
    """-> (records, status); status is ok|missing|corrupt."""
    try:
        with open(_approvals_path(workdir)) as f:
            data = json.load(f)
    except FileNotFoundError:
        return [], "missing"
    except (OSError, ValueError):
        return [], "corrupt"
    recs = data.get("records")
    return (recs if isinstance(recs, list) else [], "ok")


def _write_approvals(workdir, records):
    tmp = (f"{_approvals_path(workdir)}.{os.getpid()}."
           f"{threading.get_ident()}.tmp")
    try:
        with open(tmp, "w") as f:
            json.dump({"records": records[-APPROVALS_KEEP:],
                       "updated": time.time()}, f, ensure_ascii=False)
        os.replace(tmp, _approvals_path(workdir))
    except OSError:
        pass


def record_approval(workdir, rec):
    with _file_lock:
        records, status = load_approvals(workdir)
        if status == "corrupt":
            return
        key = (rec.get("session_id"), rec.get("request_id"))
        if not any((r.get("session_id"), r.get("request_id")) == key
                   for r in records):
            records.append(rec)
            _write_approvals(workdir, records)


def resolve_approval(workdir, request_id, session_id=None, **fields):
    with _file_lock:
        records, status = load_approvals(workdir)
        if status != "ok":
            return
        changed = False
        for r in records:
            if r.get("request_id") == request_id \
                    and (session_id is None
                         or r.get("session_id") == session_id) \
                    and r.get("status") == "pending":
                r.update(fields)
                r["status"] = "resolved"
                r["resolved_at"] = time.time()
                changed = True
        if changed:
            _write_approvals(workdir, records)


def _perm_summary(tool_call):
    """Non-sensitive one-line summary: title + first path/command."""
    if not isinstance(tool_call, dict):
        return L("(未知操作)", "(unknown action)")
    parts = []
    title = tool_call.get("title")
    if isinstance(title, str) and title.strip():
        parts.append(title.strip())
    raw = tool_call.get("rawInput")
    if isinstance(raw, dict):
        for key in ("command", "cmd", "path", "file_path"):
            if isinstance(raw.get(key), str):
                parts.append(raw[key])
                break
    for loc in tool_call.get("locations") or []:
        if isinstance(loc, dict) and isinstance(loc.get("path"), str):
            parts.append(loc["path"])
            break
    text = " ".join(parts) or tool_call.get("toolCallId") or L("(未知操作)", "(unknown action)")
    return re.sub(r"\s+", " ", text)[:160]


def mark_interrupted(tasks_dir, skip=()):
    """A hard bridge death kills every acp child that was not carried
    over by a live restore; recorded non-terminal state outside `skip`
    is now interrupted. Returns [(thread_ts, prior_state)]."""
    try:
        names = os.listdir(tasks_dir)
    except OSError:
        return []
    out = []
    for name in names:
        wd = os.path.join(tasks_dir, name)
        if wd in skip:
            continue
        st = read_state(wd)
        if st and st.get("state") not in TERMINAL:
            write_state(wd, "interrupted", st.get("activity", ""),
                        L("桥服务重启，会话随之结束；回复本线程可恢复", "bridge restarted, session ended; reply in this thread to restore"))
            out.append((".".join(name.rsplit("_", 1)), st))
        with _file_lock:
            records, ledger = load_approvals(wd)
            if ledger == "ok":
                changed = False
                for r in records:
                    if r.get("status") == "pending":
                        r["status"] = "interrupted"
                        r["resolution"] = "lost_on_restart"
                        r["needs_user"] = True
                        r["resolved_at"] = time.time()
                        changed = True
                if changed:
                    _write_approvals(wd, records)
    return out


def collect_approvals(tasks_dir, stale_sec=None):
    """Read-only snapshot for the coordinator: bridge sessions plus their
    approval ledgers. covered=False means the ledger cannot confirm the
    absence of pending requests; callers must not report 'no pending
    approvals' unless covered is True."""
    if stale_sec is None:
        stale_sec = TIMEOUT + CANCEL_GRACE + 60
    out = []
    try:
        names = sorted(os.listdir(tasks_dir))
    except OSError:
        return out
    now = time.time()
    for name in names:
        wd = os.path.join(tasks_dir, name)
        if not os.path.isdir(wd):
            continue
        st = read_state(wd)
        records, ledger = load_approvals(wd)
        if st is None and ledger == "missing":
            continue
        session_live = bool(st) and st.get("state") not in TERMINAL
        pending, stale, needs_user = [], [], []
        for r in records:
            if r.get("status") == "pending":
                fresh = session_live \
                    and now - r.get("requested_at", 0) <= stale_sec
                (pending if fresh else stale).append(r)
            elif r.get("needs_user") or r.get("status") == "interrupted":
                needs_user.append(r)
        out.append({
            "dir": name,
            "thread_ts": ".".join(name.rsplit("_", 1)),
            "session_state": (st or {}).get("state"),
            "session_id": (st or {}).get("session_id"),
            "activity": (st or {}).get("activity"),
            "ledger": ledger,
            "pending": pending,
            "stale_pending": stale,
            "needs_user": needs_user,
            "resolved": sum(1 for r in records
                            if r.get("status") == "resolved"),
            "covered": ledger != "corrupt",
            "checked_at": now,
        })
    return out


def split_chunks(text, limit=None):
    limit = limit or MAX_POST
    if len(text) <= limit:
        return [text]
    chunks, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if cur:
                chunks.append(cur)
                cur = ""
            chunks.append(line[:limit])
            line = line[limit:]
        if cur and len(cur) + 1 + len(line) > limit:
            chunks.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        chunks.append(cur)
    return chunks or [L("(无输出)", "(no output)")]


_MD_LINK = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+)\)")
_MD_BOLD = re.compile(r"\*\*(?=\S)(.*?\S)\*\*|__(?=\S)(.*?\S)__")
_MD_STRIKE = re.compile(r"~~(?=\S)(.*?\S)~~")
_MD_HEAD = re.compile(r"^#{1,6}\s+(.*?)\s*#*\s*$")
_MD_HR = re.compile(r"^\s*(?:-{3,}|\*{3,}|_{3,})\s*$")
_MD_TASK = re.compile(r"^(\s*[-*+]\s+)\[( |x|X)\]\s*")
_TBL_SEP_CELL = re.compile(r"^:?-{2,}:?$")


def _dispw(s):
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1
               for c in s)


def _pad(s, w):
    return s + " " * max(0, w - _dispw(s))


def _is_table_row(line):
    st = line.strip()
    return st.startswith("|") and st.endswith("|") and st.count("|") >= 3


def _md_table(block):
    """Render a markdown table as an aligned monospace code block:
    Slack has no table markup, but a fenced block keeps the columns
    readable — CJK cells padded by display width, not len()."""
    rows = []
    for line in block:
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if all(_TBL_SEP_CELL.match(c) for c in cells):
            rows.append(None)
        else:
            rows.append(cells)
    cols = max(len(r) for r in rows if r)
    widths = [0] * cols
    for r in rows:
        if r:
            for i in range(cols):
                cell = r[i] if i < len(r) else ""
                widths[i] = max(widths[i], _dispw(cell))
    out = ["```"]
    for r in rows:
        if r is None:
            out.append("|" + "|".join("-" * (w + 2)
                                      for w in widths) + "|")
        else:
            out.append("| " + " | ".join(
                _pad(r[i] if i < len(r) else "", widths[i])
                for i in range(cols)) + " |")
    out.append("```")
    return out


def _md_line(line):
    m = _MD_HEAD.match(line)
    if m:
        return "*" + m.group(1) + "*"
    if _MD_HR.match(line):
        return "──────────"
    line = _MD_TASK.sub(
        lambda m: m.group(1) + ("☑ " if m.group(2).lower() == "x"
                                else "☐ "), line)
    line = _MD_BOLD.sub(
        lambda m: "*" + (m.group(1) or m.group(2)) + "*", line)
    line = _MD_STRIKE.sub(lambda m: "~" + m.group(1) + "~", line)
    return _MD_LINK.sub(lambda m: f"<{m.group(2)}|{m.group(1)}>", line)


def slack_md(text):
    """Standard Markdown -> Slack mrkdwn. Agents write normal Markdown;
    this layer makes it render like a real client instead of leaking
    raw ** / ## / pipe-table syntax. Idempotent: Slack-native text
    passes through unchanged."""
    lines = text.split("\n")
    out, i, fence = [], 0, False
    while i < len(lines):
        line = lines[i]
        if line.lstrip().startswith("```"):
            fence = not fence
            out.append(line)
            i += 1
            continue
        if fence:
            out.append(line)
            i += 1
            continue
        if _is_table_row(line):
            block = []
            while i < len(lines) and _is_table_row(lines[i]):
                block.append(lines[i])
                i += 1
            out.extend(_md_table(block))
            continue
        out.append(_md_line(line))
        i += 1
    return "\n".join(out)


def fmt_state(st):
    name = STATE_NAMES.get(st.get("state", ""), st.get("state", "?"))
    when = time.strftime("%H:%M:%S", time.localtime(st.get("updated", 0)))
    detail = st.get("detail") or ""
    return f"{name}（{when}）" + (f"：{detail}" if detail else "")


class _InheritedProc:
    """Popen-shaped view of an acp process whose pipes survived a
    bridge restart. The process stays our child across exec() (fd
    numbers unchanged) or comes back through systemd's fd store."""

    def __init__(self, pid, stdin_fd, stdout_fd):
        self.pid = pid
        self.stdin = os.fdopen(stdin_fd, "w", buffering=1,
                               encoding="utf-8", errors="replace")
        self.stdout = os.fdopen(stdout_fd, "r", buffering=1,
                                encoding="utf-8", errors="replace")
        self.returncode = None

    def wait(self, timeout=None):
        deadline = (None if timeout is None
                    else time.monotonic() + timeout)
        while self.returncode is None:
            try:
                done, status = os.waitpid(self.pid, os.WNOHANG)
            except ChildProcessError:
                self.returncode = 0
            else:
                if done:
                    self.returncode = status
            if self.returncode is None:
                if deadline is not None and time.monotonic() > deadline:
                    raise subprocess.TimeoutExpired("acp", timeout)
                time.sleep(0.05)
        return self.returncode

    def poll(self):
        if self.returncode is None:
            try:
                self.wait(timeout=0)
            except subprocess.TimeoutExpired:
                pass
        return self.returncode


class DevinSession:
    """One persistent `devin acp` process bound to one Slack thread."""

    def __init__(self, thread_ts, workdir, cmd, timeout, acp_cwd=None):
        os.makedirs(workdir, exist_ok=True)
        self.thread_ts = thread_ts
        self.workdir = workdir
        # acp_cwd is where the agent process runs and where session/new
        # |load anchors the session; workdir stays the bridge's own
        # per-thread state dir. They differ when the bridge adopts a
        # session whose home project lives elsewhere.
        self.acp_cwd = acp_cwd or workdir
        self.timeout = timeout
        exe = shutil.which(cmd[0])
        if not exe:
            raise FileNotFoundError(L(f"{cmd[0]!r} 不在 PATH", f"{cmd[0]!r} not on PATH"))
        errlog = _open_capped_append(
            os.path.join(workdir, ".acp_stderr.log"))
        try:
            self.proc = subprocess.Popen(
                cmd, cwd=self.acp_cwd,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=errlog, text=True, bufsize=1,
                start_new_session=True)
        finally:
            errlog.close()
        self._init_state()
        write_state(self.workdir, "starting", L("acp 已启动", "acp started"))

    @classmethod
    def reattach(cls, thread_ts, workdir, timeout, pid, stdin_fd,
                 stdout_fd):
        """Adopt a live acp process that survived a bridge restart; no
        spawn, no handshake, in-flight turns keep flowing."""
        self = cls.__new__(cls)
        self.thread_ts = thread_ts
        self.workdir = workdir
        self.timeout = timeout
        self.proc = _InheritedProc(pid, stdin_fd, stdout_fd)
        self._init_state()
        return self

    def _init_state(self):
        self.q = queue.Queue()
        self.eof = False
        self.on_text = None
        self.notify_perm = None
        self.perm_update = None
        self.draft_msgs = []
        self.draft_sent = []
        self.draft_at = 0.0
        self.next_id = 0
        self.lock = threading.Lock()
        self.last_active = time.monotonic()
        self.last_progress = 0.0
        self.last_shown = None
        self.status_ts = None
        self.last_activity = L("启动中", "starting")
        self.last_activity_at = time.monotonic()
        self.chunks = []
        self.think_chunks = []
        self.think_buf = ""
        self.queued = 0
        self.running = False
        self.stopping = False
        self.interrupt_ev = threading.Event()
        self.interrupted_turn = False
        self.interrupting = False
        self.active_turn = None
        self.last_queue_notice = 0.0
        self.alive = True
        self.resumed = False
        self.notify = None
        self.pending_perms = {}
        self._seen_perm_ids = set()
        self.slash_cmds = set()
        self.mode_names = {}
        self.mode_name = None
        self.model_name = None
        self._tool_titles = {}
        self.recent = deque(maxlen=8)
        self.tool_count = 0
        self.touched = {}
        self.sent_files = {}     # path -> mtime of last upload
        self.pending_files = []  # pooled for !files / !send
        self.turn_started = 0.0
        self.plan_entries = []
        self.goal = None
        self.goal_started_at = 0.0
        self.drive_lock = threading.Lock()
        self._goal_get_ids = set()
        self._last_goal_poll = 0.0
        self.last_turn_end = 0.0
        self.turns = []
        self.turns_lock = threading.Lock()
        self._wlock = threading.Lock()
        self._id_lock = threading.Lock()
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        try:
            for line in self.proc.stdout:
                self.q.put(line)
        except (ValueError, OSError):
            pass
        finally:
            self.q.put(None)

    def _read_line(self, timeout):
        """None = still waiting (bounded); raises AcpExited on EOF."""
        if self.eof:
            raise AcpExited("devin acp exited")
        try:
            item = self.q.get(timeout=timeout)
        except queue.Empty:
            return None
        if item is None:
            self.eof = True
            self.q.put(None)
            raise AcpExited("devin acp exited")
        return item

    def _send(self, packet):
        try:
            with self._wlock:
                self.proc.stdin.write(json.dumps(packet) + "\n")
                self.proc.stdin.flush()
        except (BrokenPipeError, ValueError, OSError) as e:
            raise AcpExited("devin acp stdin closed") from e

    def _request(self, method, params):
        # Alloc under its own lock: prompt-loop polls and out-of-turn
        # readers (_goal_fetch) race on this counter, and a duplicate
        # id makes a later response indistinguishable.
        with self._id_lock:
            self.next_id += 1
            mid = self.next_id
        self._send({"jsonrpc": "2.0", "id": mid,
                    "method": method, "params": params})
        return mid

    def _note(self, activity):
        self.last_activity = activity
        self.last_activity_at = time.monotonic()
        self.last_active = self.last_activity_at

    def _handle_msg(self, msg, on_progress):
        if msg.get("id") in self._goal_get_ids:
            self._goal_get_ids.discard(msg.get("id"))
            self._set_goal((msg.get("result") or {}).get("goal"),
                           on_progress)
            return
        if msg.get("method") == "session/update":
            upd = msg["params"].get("update", {})
            kind = upd.get("sessionUpdate")
            if kind == "agent_message_chunk":
                self.chunks.append(upd.get("content", {}).get("text", ""))
                self._note(L("正在输出", "outputting"))
                if self.on_text:
                    try:
                        self.on_text()
                    except Exception:
                        pass
            elif kind in ("tool_call", "tool_call_update"):
                # tool_call_update often carries only a bare status;
                # resolve the title by id instead of downgrading the
                # status card to 'in_progress'/'completed'.
                tid = upd.get("toolCallId")
                title = upd.get("title")
                if title:
                    self._tool_titles[tid or title] = title
                elif tid:
                    title = self._tool_titles.get(tid)
                if not title and kind == "tool_call":
                    title = upd.get("status") or "working"
                for loc in upd.get("locations") or []:
                    p = loc.get("path") if isinstance(loc, dict) \
                        else None
                    if p:
                        self.touched[p] = True
                if title:
                    self._note(L(f"工具: {title}", f"tool: {title}"))
                    if kind == "tool_call" and (
                            not self.recent
                            or self.recent[-1] != self.last_activity):
                        self.tool_count += 1
                        self.recent.append(self.last_activity)
                    on_progress(self.last_activity)
            elif kind == "plan":
                entries = upd.get("entries") or []
                self.plan_entries = entries
                doing = next((e for e in entries
                              if e.get("status") == "in_progress"),
                             None)
                done = sum(1 for e in entries
                           if e.get("status") == "completed")
                if doing:
                    self._note("📋 "
                               + str(doing.get("content", ""))[:80]
                               + f"（{done}/{len(entries)}）")
                elif entries:
                    self._note(L(f"📋 计划 {done}/{len(entries)} 项", f"📋 plan {done}/{len(entries)}"))
                if doing or entries:
                    on_progress(self.last_activity)
            elif kind == "agent_thought_chunk":
                txt = upd.get("content", {}).get("text", "")
                if txt:
                    self.think_chunks.append(txt)
                    self.think_buf = (self.think_buf + txt)[-160:]
                snip = re.sub(r"\s+", " ", self.think_buf)[-80:].strip()
                self._note("🧠 " + snip if snip else L("思考中", "thinking"))
            elif kind == "current_mode_update":
                cid = upd.get("currentModeId")
                if cid:
                    self.mode_name = self.mode_names.get(cid, cid)
            elif kind == "available_commands_update":
                for c in upd.get("availableCommands") or []:
                    if c.get("name"):
                        self.slash_cmds.add(str(c["name"]).lstrip("/"))
        elif msg.get("method") == "session/request_permission":
            self._handle_permission(msg)
        elif msg.get("method") and msg.get("id") is not None:
            # Any other agent→host request (fs/*, terminal/*,
            # elicitation/create, future extensions): a request must
            # be answered — silently dropping it stalls the turn
            # until the silence killer fires. Method-not-found is a
            # definite refusal the agent can react to.
            self._send({"jsonrpc": "2.0", "id": msg["id"],
                        "error": {"code": -32601,
                                  "message": "bridge does not implement "
                                             + str(msg["method"])}})

    def _set_goal(self, goal, on_progress=None):
        old = self.goal or {}
        self.goal = goal or None
        if self.goal and self.goal.get("status") == "running":
            if old.get("objective") != self.goal.get("objective") \
                    or not self.goal_started_at:
                self.goal_started_at = time.monotonic()
        if self.goal != old and on_progress:
            on_progress(self.last_activity)

    def _note_session_meta(self, result):
        for m in (result.get("modes") or {}).get("availableModes") or []:
            if m.get("id"):
                self.mode_names[m["id"]] = m.get("name") or m["id"]
        cur = (result.get("modes") or {}).get("currentModeId")
        if cur:
            self.mode_name = self.mode_names.get(cur, cur)
        for opt in result.get("configOptions") or []:
            if opt.get("id") == "model":
                names = {o.get("value"): o.get("name")
                         for o in opt.get("options") or []}
                val = opt.get("currentValue")
                self.model_name = names.get(val) or val

    def _handle_permission(self, msg):
        """Ledger first. If the platform offers no allow option, deny is
        the only honest answer. With BRIDGE_AUTO_APPROVE (default) the
        request is answered locally, preferring allow_always. Otherwise
        the request stays pending for a human L("允许", "allow")/L("拒绝", "deny") in the
        owning thread; the prompt loop auto-denies after PERM_TIMEOUT."""
        params = msg.get("params") or {}
        tool_call = params.get("toolCall") or {}
        key = (params.get("sessionId"), msg.get("id"))
        if key in self._seen_perm_ids:
            return
        self._seen_perm_ids.add(key)
        options = [o for o in (params.get("options") or [])
                   if isinstance(o, dict) and o.get("optionId") is not None]
        allows = [o for o in options
                  if str(o.get("kind", "")).startswith("allow")]
        summary = _perm_summary(params.get("toolCall"))
        record_approval(self.workdir, {
            "request_id": msg.get("id"),
            "session_id": params.get("sessionId"),
            "tool_call_id": tool_call.get("toolCallId"),
            "kind": tool_call.get("kind"),
            "summary": summary,
            "options": [o.get("kind") for o in options],
            "requested_at": time.time(),
            "status": "pending",
        })
        if not allows:
            self._send({"jsonrpc": "2.0", "id": msg["id"],
                        "result": {"outcome": {"outcome": "cancelled"}}})
            resolve_approval(self.workdir, msg["id"],
                             session_id=params.get("sessionId"),
                             resolution="cancelled",
                             resolver="no_allow_option",
                             needs_user=True)
            self._note(L("审批无允许项，已回绝", "approval had no allowable item, denied"))
            if self.notify:
                self.notify(L("⚠️ 审批请求无可允许项，已回绝：", "⚠️ Approval request has no allowable item, denied:") + summary)
            return
        if AUTO_APPROVE:
            pick = next((o for o in allows
                         if o.get("kind") == "allow_always"), allows[0])
            self._send({"jsonrpc": "2.0", "id": msg["id"], "result": {
                "outcome": {"outcome": "selected",
                            "optionId": pick["optionId"]}}})
            resolve_approval(self.workdir, msg["id"],
                             session_id=params.get("sessionId"),
                             resolution=str(pick.get("kind", "allow")),
                             resolver="auto_approve",
                             needs_user=False)
            self._note(L(f"已自动批准: {summary}", f"auto-approved: {summary}"))
            return
        entry = {
            "session_id": params.get("sessionId"),
            "options": allows,
            "all_options": options,
            "posted_at": time.monotonic(),
            "summary": summary,
            "msg_ts": None,
        }
        self.pending_perms[msg["id"]] = entry
        self._note(L("等待人工审批", "waiting for human approval"))
        if self.notify_perm:
            try:
                entry["msg_ts"] = self.notify_perm(
                    summary, msg["id"], options)
            except Exception:
                entry["msg_ts"] = None
        if entry["msg_ts"] is None and self.notify:
            self.notify(L(f"⚠️ 待审批 {msg['id']}: {summary}\\n", f"⚠️ pending approval {msg['id']}: {summary}\n") +
                        L("回复「允许」批准（优先 allow_once），", 'reply "allow" to approve (prefers allow_once),') +
                        L("「拒绝」回绝；", '"deny" to reject;') +
                        f"{_perm_timeout_hint()}。")

    def _answer_perm(self, rid, allow):
        """Send the decision for one pending request. Returns the
        resolution string, or None if already answered."""
        entry = self.pending_perms.get(rid)
        if entry is None:
            return None
        if allow:
            pick = next((o for o in entry["options"]
                         if o.get("kind") == "allow_once"),
                        entry["options"][0])
            self._send({"jsonrpc": "2.0", "id": rid, "result": {
                "outcome": {"outcome": "selected",
                            "optionId": pick["optionId"]}}})
            self.pending_perms.pop(rid, None)
            return str(pick.get("kind", "allow"))
        self._send({"jsonrpc": "2.0", "id": rid,
                    "result": {"outcome": {"outcome": "cancelled"}}})
        self.pending_perms.pop(rid, None)
        return "cancelled"

    def user_decide(self, allow, who=None):
        """Resolve the oldest pending request from a thread reply.
        Returns (request_id, resolution) or None."""
        if not self.pending_perms:
            return None
        rid = next(iter(self.pending_perms))
        entry = self.pending_perms[rid]
        sid = entry.get("session_id")
        resolution = self._answer_perm(rid, allow)
        if resolution is None:
            return None
        resolve_approval(self.workdir, rid, session_id=sid,
                         resolution=resolution,
                         resolver=f"user:{who}" if who else "user_thread",
                         needs_user=False)
        if self.perm_update and entry.get("msg_ts"):
            mark = "✅" if allow else "❌"
            try:
                self.perm_update(
                    entry["msg_ts"],
                    L(f"{mark} {entry.get('summary', '')}\\n已由 ", f"{mark} {entry.get('summary', '')}\nby ") +
                    L(f"{who or '人工'} {'批准' if allow else '回绝'}", f"{who or 'human'} {'approved' if allow else 'denied'}") +
                    f"（{resolution}）")
            except Exception:
                pass
        return rid, resolution

    def user_decide_option(self, rid, oid, who=None):
        """Button path: answer request `rid` with the exact clicked
        optionId. Returns (option, summary, perm_msg_ts), a string
        error tag, or None when the request is already gone."""
        entry = self.pending_perms.get(rid)
        if entry is None:
            return None
        opt = next((o for o in entry["all_options"]
                    if o.get("optionId") == oid), None)
        if opt is None:
            return "bad_option"
        kind = str(opt.get("kind", ""))
        self._send({"jsonrpc": "2.0", "id": rid, "result": {
            "outcome": {"outcome": "selected",
                        "optionId": oid}}})
        self.pending_perms.pop(rid, None)
        resolve_approval(self.workdir, rid,
                         session_id=entry.get("session_id"),
                         resolution=kind or "selected",
                         resolver=f"button:{who}" if who else "button",
                         needs_user=False)
        return opt, entry.get("summary", ""), entry.get("msg_ts")

    def _perm_sweep(self):
        """Auto-deny pending requests older than PERM_TIMEOUT."""
        now = time.monotonic()
        for rid, entry in list(self.pending_perms.items()):
            if now - entry["posted_at"] > PERM_TIMEOUT:
                self._answer_perm(rid, False)
                if self.perm_update and entry.get("msg_ts"):
                    try:
                        self.perm_update(
                            entry["msg_ts"],
                            L(f"⏰ 超时自动拒绝：{entry.get('summary', '')}", f"⏰ timed out, auto-denied: {entry.get('summary', '')}"))
                    except Exception:
                        pass
                resolve_approval(self.workdir, rid,
                                 session_id=entry.get("session_id"),
                                 resolution="cancelled",
                                 resolver="timeout",
                                 needs_user=True)
                if self.notify:
                    self.notify(L(f"⏰ 审批超时未答复，已回绝 {rid}。", f"⏰ approval timed out unanswered, denied {rid}."))

    def _close_pending(self, resolver):
        for rid, entry in list(self.pending_perms.items()):
            try:
                resolution = self._answer_perm(rid, False)
            except AcpExited:
                resolution = None
            resolve_approval(self.workdir, rid,
                             session_id=entry.get("session_id"),
                             resolution=resolution or "send_failed",
                             resolver=resolver,
                             needs_user=True)

    def _wait_result(self, want_id, deadline, on_progress,
                     putback_foreign=False):
        """Wait for the response to request `want_id`. When
        putback_foreign is set (out-of-turn readers like
        _goal_fetch), a result/error line belonging to some other
        request goes back onto the queue instead of being eaten, so
        a turn that started mid-wait still gets its result."""
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("devin timed out")
            self._perm_sweep()
            line = self._read_line(min(remaining, 1.0))
            if line is None:
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                print(f"[warn] non-json line from acp: {line[:80]!r}",
                      flush=True)
                continue
            if putback_foreign and "id" in msg and \
                    msg["id"] != want_id and \
                    msg["id"] not in self._goal_get_ids and \
                    ("result" in msg or "error" in msg):
                # A result for a live foreign request (e.g. a prompt
                # turn that started mid-wait): hand it back instead of
                # eating it. Stale goal/get responses are not foreign.
                self.q.put(line)
                return None
            self._handle_msg(msg, on_progress)
            if msg.get("id") == want_id and ("result" in msg or "error" in msg):
                return msg

    def start(self, resume_id=None, strict_load=False):
        deadline = time.monotonic() + self.timeout
        i = self._request("initialize", {"protocolVersion": 1,
                                         "clientCapabilities": {}})
        init_res = self._wait_result(i, deadline, lambda x: None)
        auth_methods = (((init_res or {}).get("result") or {})
                        .get("authMethods") or [])
        if auth_methods:
            key = _acp_api_key()
            if not key:
                raise AcpError({
                    "message": L("devin 未认证: acp 不使用本地 CLI 凭据,", "devin not authenticated: acp doesn't use local CLI credentials,") +
                               L("且未找到 api key。请 `devin auth login` ", "and no api key found. Please `devin auth login` ") +
                               L("刷新凭据,或在桥环境设 DEVIN_ACP_API_KEY。", "to refresh credentials, or set DEVIN_ACP_API_KEY in the bridge env.")})
            a = self._request("authenticate", {
                "methodId": auth_methods[0].get("id", "devin-browser"),
                "_meta": {"api_key": key}})
            ar = self._wait_result(a, deadline, lambda x: None)
            if (ar or {}).get("error"):
                raise AcpError(ar["error"])
        res = None
        if resume_id:
            try:
                n = self._request("session/load",
                                  {"sessionId": resume_id,
                                   "cwd": self.acp_cwd,
                                   "mcpServers": []})
                res = self._wait_result(n, deadline, lambda x: None)
                if "error" not in res:
                    self.session_id = (res.get("result") or {}).get(
                        "sessionId") or resume_id
                    self.resumed = True
                elif strict_load:
                    raise AcpError(res["error"])
            except AcpError:
                raise
            except Exception:
                pass
        if not self.resumed:
            n = self._request("session/new", {"cwd": self.acp_cwd,
                                              "mcpServers": []})
            res = self._wait_result(n, deadline, lambda x: None)
            if "error" in (res or {}) and "not authenticated" in \
                    str(res["error"].get("message", "")):
                raise AcpError({
                    "message": L("devin 认证失效: acp 拒绝了 session/new。", "devin auth expired: acp refused session/new.") +
                               L("请 `devin auth login` 重新登录刷新凭据", "run `devin auth login` to refresh credentials") +
                               L("(桥下次启动会话会自动捡起),或换一个未", "(the bridge will pick it up at next session start), or use an un") +
                               L("限额的账号。", "limited account.")})
            self.session_id = res["result"]["sessionId"]
        self._note_session_meta((res or {}).get("result") or {})
        if SESSION_MODE:
            m = self._request("session/set_mode",
                              {"sessionId": self.session_id,
                               "modeId": SESSION_MODE})
            self._wait_result(m, deadline, lambda x: None)
        self._note(L("会话就绪", "session ready"))
        write_state(self.workdir, "idle", self.last_activity,
                    extra={"session_id": self.session_id})
        # session/load replays history as session/update chunks; drop
        # them so they are not re-emitted as the next reply.
        self.chunks = []
        self.think_chunks = []
        self.think_buf = ""

    def prompt(self, text, on_progress, stop_flag, msg_ts=None,
               inflight_id=None, grace=None):
        """Returns (reply_text, stop_reason). Serialized per session.
        inflight_id adopts a prompt request that was already sent
        before a bridge reload; grace bounds its stall."""
        rec = {"text": text, "msg_ts": msg_ts, "req_id": inflight_id}
        with self.turns_lock:
            self.turns.append(rec)
        self.queued += 1
        try:
            with self.lock:
                self.queued -= 1
                # A stale interrupt (set after the previous turn had
                # already finished) belongs to nobody — only the turn
                # that was running when it arrived may honor it.
                self.interrupt_ev.clear()
                if stop_flag.is_set() or not self.alive:
                    return ("", "queued_cancelled")
                self.running = True
                self.stopping = False
                self.interrupted_turn = False
                interrupted = False
                self.active_turn = rec
                if inflight_id is None:
                    self.chunks = []
                    self.draft_msgs = []
                    self.draft_sent = []
                    self.draft_at = 0.0
                    # status_ts deliberately survives across turns:
                    # the card is one live status line per session,
                    # not one new message per turn. Per-turn metrics
                    # (clock, tool count, step trail) reset here.
                    self.turn_started = time.monotonic()
                    self.tool_count = 0
                    self.touched = {}
                    self.recent.clear()
                    self.plan_entries = []
                    self.last_shown = None
                    self.think_chunks = []
                    self.think_buf = ""
                self._note(L("处理中", "working"))
                write_state(self.workdir, "running", self.last_activity)
                try:
                    if inflight_id is not None:
                        pid = inflight_id
                    else:
                        pid = self._request("session/prompt", {
                            "sessionId": self.session_id,
                            "prompt": [{"type": "text", "text": text}]})
                        rec["req_id"] = pid
                        # Recorded so a hard bridge death can replay
                        # this exact turn; cleared by the next write.
                        write_state(
                            self.workdir, "running", self.last_activity,
                            extra={"inflight": {"text": text,
                                                "msg_ts": msg_ts}})
                    cancel_deadline = None
                    stall_deadline = (time.monotonic() + grace
                                      if grace else None)
                    while True:
                        now = time.monotonic()
                        if cancel_deadline is not None and \
                                now > cancel_deadline:
                            raise StopStalled(L("cancel 未在宽限内生效", "cancel didn't take effect within grace"))
                        deadline = self.last_activity_at + self.timeout
                        if stall_deadline is not None:
                            deadline = min(deadline, stall_deadline)
                        # Awaiting a human approval is a parked state,
                        # not silence — PERM_TIMEOUT's reaper owns it.
                        if now >= deadline and not self.pending_perms:
                            raise PromptTimeout()
                        if (stop_flag.is_set()
                                or self.interrupt_ev.is_set()) and \
                                cancel_deadline is None:
                            if self.interrupt_ev.is_set() and \
                                    not stop_flag.is_set():
                                # A new user message interrupted this
                                # turn — return reason 'interrupted'
                                # so the settle card says 打断 not 停止.
                                interrupted = True
                                self.interrupted_turn = True
                                self.interrupting = True
                            else:
                                self.stopping = True
                            self.interrupt_ev.clear()
                            self._send({
                                "jsonrpc": "2.0",
                                "method": "session/cancel",
                                "params": {"sessionId": self.session_id}})
                            self._close_pending("task_stop")
                            cancel_deadline = now + CANCEL_GRACE
                        self._perm_sweep()
                        if self.session_id and \
                                now - self._last_goal_poll >= GOAL_POLL_SEC:
                            self._last_goal_poll = now
                            try:
                                self._goal_get_ids.add(self._request(
                                    "_cognition.ai/goal/get",
                                    {"sessionId": self.session_id}))
                            except Exception:
                                pass
                        # `now` may have passed `deadline` while a perm
                        # request kept the turn parked — never hand a
                        # negative timeout to the reader.
                        line = self._read_line(
                            max(0.05, min(deadline - now, 1.0)))
                        if line is None:
                            continue
                        try:
                            msg = json.loads(line)
                        except ValueError:
                            continue
                        self._handle_msg(msg, on_progress)
                        if msg.get("id") == pid and \
                                ("result" in msg or "error" in msg):
                            if "error" in msg:
                                raise AcpError(msg["error"])
                            return ("".join(self.chunks).strip()
                                    or L("(无输出)", "(no output)"),
                                    "interrupted" if interrupted else
                                    msg["result"].get("stopReason",
                                                      "?"))
                finally:
                    self._close_pending("turn_end")
                    self.running = False
                    self.stopping = False
                    self.interrupting = False
                    self.active_turn = None
                    self.last_active = time.monotonic()
                    self.last_turn_end = time.time()
        finally:
            with self.turns_lock:
                try:
                    self.turns.remove(rec)
                except ValueError:
                    pass

    def terminate(self):
        """Kill this session's own process group only. Returns True when
        the group is verifiably gone."""
        self.alive = False
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
        except OSError:
            pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except OSError:
                pass
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        for f in (getattr(self.proc, "stdin", None),
                  getattr(self.proc, "stdout", None)):
            try:
                if f is not None:
                    f.close()
            except Exception:
                pass
        try:
            os.killpg(self.proc.pid, 0)
            return False
        except OSError:
            return True


def _slack_err(e):
    resp = getattr(e, "response", None)
    data = getattr(resp, "data", None) or {}
    return data.get("error")


def _errname(e):
    err = _slack_err(e)
    return f"{type(e).__name__}({err})" if err else type(e).__name__


class SessionBridge:
    def __init__(self, post, tasks_dir=TASKS_DIR, acp_cmd=ACP_CMD,
                 timeout=TIMEOUT, idle_sec=IDLE_SEC, own_bot_id=None,
                 own_user_id=None,
                 channel=CHANNEL, react=None, unreact=None, update=None,
                 upload=None, status=None, open_modal=None,
                 pin_add=None, pin_list=None, pin_rm=None, team_url=None,
                 ephemeral=None, bookmarks=None, bookmark_add=None,
                 bookmark_edit=None, permalink=None,
                 dm_post=None, dm_update=None,
                 canvas_create=None, canvas_edit=None):
        self.post_msg = post
        self.react_msg = react
        self.unreact_msg = unreact
        self.update_msg = update
        self.upload_msg = upload
        self.open_modal = open_modal
        self.status_msg = status
        self.pin_add = pin_add
        self.pin_list = pin_list
        self.pin_rm = pin_rm
        self.ephemeral_msg = ephemeral
        self.bookmarks = bookmarks
        self.bookmark_add = bookmark_add
        self.bookmark_edit = bookmark_edit
        self.permalink = permalink
        self._board_bookmarked = None
        self.team_url = (team_url or "").rstrip("/")
        self._board_at = 0.0
        self._board_lock = threading.Lock()
        self._board_text = None
        self.tasks_dir = tasks_dir
        self.acp_cmd = acp_cmd
        self.timeout = timeout
        self.idle_sec = idle_sec
        self.own_bot_id = own_bot_id
        self.own_user_id = own_user_id
        self.channel = channel
        self.sessions = {}
        self.starting = set()
        self.starting_auto = set()
        self.dispatch_queue = deque()
        self.crash_retries = {}
        self.stop_flags = {}
        self.reload_ev = threading.Event()
        self.term_ev = threading.Event()
        self.lock = threading.Lock()
        self.seen_ids = set()
        self.seen = deque(maxlen=2000)
        self.seen_msg_keys = set()
        self.seen_msgs = deque(maxlen=2000)
        self._err_seen = set()
        self._err_last = {}
        self._reacted = {}
        self.governor = limit_governor.Governor(
            tasks_dir, printer=lambda m: print(m, flush=True))
        self.limit_attempts = {}
        self._ext_scan_at = 0.0
        self._acct = _acct_fingerprint()
        # DM mirror boards: mobile can't reach channel pins/bookmarks,
        # so every known user gets a copy of the board as the latest
        # message in their DM with the bot — open Slack, tap the bot,
        # board is right there. dm_post/dm_update are the only
        # transports that can address a channel other than self.channel.
        self.dm_post = dm_post
        self.dm_update = dm_update
        # Channel canvas mirror: Slack mobile exposes the channel
        # canvas as a header icon — persistent doc, never scrolls.
        self.canvas_create = canvas_create
        self.canvas_edit = canvas_edit
        bd = self._board_load()
        seed = os.environ.get("BOARD_DM_USERS", "")
        self._dm_users = set(filter(None, seed.split(","))) | \
            set(bd.get("users") or [])
        self._dm_boards = dict(bd.get("dm") or {})
        # vitals: previous counters for rate deltas + autoclean state
        self._cpu_prev = None
        self._net_prev = None
        self._vitals = {}
        self._clean = {}
        self._err_stats = Counter()
        self._started = time.time()

    def _log_err(self, label, e):
        self._err_stats[f"{label}: {_errname(e)}"] += 1
        err = _slack_err(e)
        if err:  # deterministic API-side error: warn once per kind
            key = (label, err)
            if key in self._err_seen:
                return
            self._err_seen.add(key)
            if err == "missing_scope":
                resp = getattr(e, "response", None)
                headers = getattr(resp, "headers", None) or {}
                needed = headers.get("x-accepted-oauth-scopes", "?")
                print(f"[error] {label}: missing Slack scope(s) "
                      f"{needed}; add them in the app config and "
                      f"reinstall the app (further {err} errors for "
                      f"this call are muted)", flush=True)
                return
        # Transient storms (proxy down, DNS flapping) can produce
        # hundreds of identical lines; keep the first per 60s window
        # and fold repeats into a count on the next logged line.
        key = (label, _errname(e))
        now = time.monotonic()
        last, count = self._err_last.get(key, (-1e9, 0))
        if now - last < 60:
            self._err_last[key] = (last, count + 1)
            return
        self._err_last[key] = (now, 0)
        tail = L(f"（60s 内另 {count} 次相同）", f"({count} more identical within 60s)") if count else ""
        print(f"[error] {label}: {_errname(e)}{tail}", flush=True)

    @staticmethod
    def _dedupe(ids, order, key):
        if key in ids:
            return False
        ids.add(key)
        order.append(key)
        if len(ids) > 2 * order.maxlen:
            ids.intersection_update(order)
        return True

    def _workdir(self, ts):
        return os.path.join(self.tasks_dir, ts.replace(".", "_"))

    def post(self, ts, text, blocks=None):
        try:
            if blocks is None:
                return self.post_msg(ts, slack_md(text))
            return self.post_msg(ts, slack_md(text), blocks)
        except Exception as e:
            self._log_err("post", e)

    def _upd(self, msg_ts, text, blocks=None):
        """chat.update boundary: same markdown render as posts, and
        tolerates transports whose update has no blocks parameter."""
        if not self.update_msg:
            return False
        try:
            if blocks is None:
                self.update_msg(msg_ts, slack_md(text))
            else:
                self.update_msg(msg_ts, slack_md(text), blocks)
            return True
        except TypeError:
            try:
                self.update_msg(msg_ts, slack_md(text))
                return True
            except Exception as e:
                self._log_err("update", e)
                return False
        except Exception as e:
            self._log_err("update", e)
            return False

    def post_long(self, ts, text):
        chunks = split_chunks(text)
        n = len(chunks)
        for i, c in enumerate(chunks):
            self.post(ts, f"({i + 1}/{n})\n{c}" if n > 1 else c)

    def _react(self, ts, name):
        if self.react_msg:
            try:
                self.react_msg(ts, name)
            except Exception as e:
                # already_reacted = desired end state, common after a
                # reload when the tracker is cold — not an error.
                if _slack_err(e) != "already_reacted":
                    self._log_err(f"react {name}", e)

    def _react_state(self, ts, name):
        """Exactly one lifecycle reaction on the thread root. The
        tracker holds the last reaction applied here, so a state
        change costs one unreact instead of sweeping all of
        REACTS_ALL; when the tracker is cold (first use after a
        restart) it falls back to the full sweep."""
        if self._reacted.get(ts) == name:
            return
        if self.unreact_msg:
            prev = self._reacted.get(ts)
            others = ({prev} if prev else REACTS_ALL) - {name}
            for other in others:
                try:
                    self.unreact_msg(ts, other)
                except Exception:
                    pass
        self._react(ts, name)
        self._reacted[ts] = name

    def _set_thread_status(self, ts, text):
        """assistant.threads.setStatus — the shimmer line next to the
        bot name. Auto-clears on reply or after ~2min."""
        if self.status_msg:
            try:
                self.status_msg(ts, text)
            except Exception as e:
                self._log_err("set status", e)

    def _status_text(self, s, tail=None, detail=None):
        lines = []
        g = s.goal
        if g:
            obj = (g.get("objective") or "").strip().splitlines()[0][:80]
            st = GOAL_STATUS_CN.get(g.get("status"),
                                    str(g.get("status", "?")))
            meta = f"{st} · turn {g.get('turns', '?')}"
            if s.goal_started_at:
                meta += L(" · 已跑 ", " · ran ") + _dur(
                    time.monotonic() - s.goal_started_at)
            lines.append(f"🎯 {obj}\n{meta}")
        info = " · ".join(x for x in (s.model_name, s.mode_name) if x)
        # Claude-style card: one header line with clock + tool count,
        # then the last few tool steps as a trail; the settled card
        # keeps the same header and drops the trail.
        if tail:
            head = tail
            if s.turn_started:
                head += " · " + _dur(time.monotonic() - s.turn_started)
            if s.tool_count:
                head += f" · {s.tool_count} tools"
            lines.append(head)
            if detail:
                if len(detail) > 500:
                    detail = detail[:500].rstrip() + "…"
                lines.append(detail)
            if s.touched:
                names = [os.path.relpath(p, s.workdir)
                         if p.startswith(s.workdir) else p
                         for p in sorted(s.touched)]
                shown = names[:5]
                more = (L(f" 等{len(names)}个", f" and {len(names)} more") if len(names) > 5 else "")
                lines.append("📁 " + ", ".join(f"`{n}`" for n in shown)
                             + more)
        else:
            head = (L("⏸ 打断中", "⏸ Interrupting") if s.interrupting else
                    L("🛑 停止中", "🛑 Stopping") if s.stopping else L("⏳ 处理中", "⏳ Working"))
            if s.turn_started:
                head += " · " + _dur(time.monotonic() - s.turn_started)
            if s.tool_count:
                head += f" · {s.tool_count} tools"
            lines.append(head)
            if info:
                lines.append(info)
            if s.plan_entries:
                # Palgenius-style step board: the plan itself is the
                # checklist, bold on the step in flight.
                for e in s.plan_entries[:4]:
                    st = e.get("status")
                    icon = {"completed": "✅", "in_progress": "🔄",
                            "pending": "☐"}.get(st, "·")
                    txt = str(e.get("content", ""))[:80]
                    lines.append(f"{icon} "
                                 + (f"*{txt}*" if st == "in_progress"
                                    else txt))
                if len(s.plan_entries) > 4:
                    lines.append(L(f"· …{len(s.plan_entries) - 4} 项", f"· …{len(s.plan_entries) - 4} more"))
            else:
                trail = list(s.recent)[-3:]
                if len(s.recent) > 3:
                    lines.append(f"· …{len(s.recent) - 3} earlier")
                lines.extend("· " + r for r in trail)
            act = s.last_activity
            if act and (not s.recent or act != s.recent[-1]):
                lines.append("> " + act)
            return "\n".join(lines)
        if info:
            lines.append(info)
        return "\n".join(lines)

    def _ctl_blocks(self, text):
        """Status-card blocks: the text plus live controls (stop /
        status) while a turn runs — one tap instead of typing a
        command. Cleared when the card settles at turn end."""
        return [
            {"type": "section",
             "text": {"type": "mrkdwn", "text": slack_md(text)}},
            {"type": "actions", "block_id": "ctl",
             "elements": [
                 {"type": "button",
                  "text": {"type": "plain_text", "text": L("🛑 停止", "🛑 Stop")},
                  "action_id": "ctl_stop",
                  "value": "stop",
                  "style": "danger"},
                 {"type": "button",
                  "text": {"type": "plain_text", "text": L("📋 状态", "📋 Status")},
                  "action_id": "ctl_status",
                  "value": "status"}]},
        ]

    def _status_line(self, s, text):
        """One in-place-updated status message per prompt: live
        controls while running, plain text once settled. Falls back
        to plain posts when no chat_update transport is wired."""
        blocks = self._ctl_blocks(text) if s.running or s.stopping \
            else []
        if s.status_ts and self.update_msg:
            if self._upd(s.status_ts, text, blocks):
                return
        s.status_ts = self.post(s.thread_ts, text, blocks or None)

    def _finish(self, ts, s, react, text, detail=None):
        self._react_state(ts, react)
        self._status_line(s, self._status_text(s, tail=text,
                                               detail=detail))
        self._board_touch()

    def _draft_tick(self, ts):
        """Throttled entry: each agent_message_chunk calls this; the
        actual re-render is rate-limited so Slack sees a live draft."""
        s = self.sessions.get(ts)
        if not DRAFT_ENABLED or not s or not self.update_msg \
                or not s.alive:
            return
        now = time.monotonic()
        if now - s.draft_at < DRAFT_MIN_GAP:
            return
        s.draft_at = now
        self._draft_render(ts, s, final=False)

    def _draft_render(self, ts, s, final):
        """Converge Slack on the reply-so-far: update existing draft
        messages in place, post new ones when the text outgrows them.
        Draft segments use the tighter DRAFT_POST cap (chat.update is
        stingier than chat.postMessage)."""
        if not self.update_msg:
            return
        # Render once at source: draft updates go straight to
        # update_msg (not through post()), so the conversion has to
        # happen here. slack_md is idempotent — the self.post below
        # re-applying it is harmless.
        text = slack_md("".join(s.chunks))
        if not text:
            return
        segs = split_chunks(text, DRAFT_POST)
        n = len(segs)
        for i, seg in enumerate(segs):
            body = f"({i + 1}/{n})\n{seg}" if n > 1 else seg
            if not final and i == n - 1:
                body += DRAFT_CURSOR
            if i < len(s.draft_sent) and s.draft_sent[i] == body:
                continue
            if i < len(s.draft_msgs):
                try:
                    self.update_msg(s.draft_msgs[i], body)
                    s.draft_sent[i] = body
                except Exception as e:
                    if _slack_err(e) == "msg_too_long":
                        # Beyond any fix: stop retrying this message
                        # so the error can't storm every tick.
                        s.draft_sent[i] = body
                    else:
                        self._log_err("draft update", e)
            else:
                mts = self.post(s.thread_ts, body)
                if not mts:
                    return
                s.draft_msgs.append(mts)
                s.draft_sent.append(body)

    def _draft_final(self, ts, s):
        if s.draft_msgs:
            self._draft_render(ts, s, final=True)

    def _post_think(self, ts, s):
        """Post the turn's reasoning as one quoted, trimmed message:
        head keeps the plan, tail keeps the conclusion."""
        if not THINK_ENABLED:
            return
        text = "".join(s.think_chunks).strip()
        if not text:
            return
        if len(text) > THINK_POST_MAX:
            head = THINK_POST_MAX * 2 // 5
            text = (text[:head].rstrip() +
                    L("\\n…（中间省略）…\\n", "\n…(middle omitted)…\n") +
                    text[-(THINK_POST_MAX - head):].lstrip())
        self.post(ts, L("🧠 *思路*\\n", "🧠 *Thinking*\n") +
                  "\n".join("> " + line for line in text.splitlines()))

    def _final_reply(self, ts, s, reply):
        """Settle a finished turn: thinking posts first, then the
        answer — streamed drafts just lose the cursor."""
        self._post_think(ts, s)
        if s.draft_msgs:
            self._draft_final(ts, s)
        else:
            self.post_long(ts, reply or L("(无输出)", "(no output)"))

    def _progress(self, ts):
        def cb(activity):
            s = self.sessions.get(ts)
            if s and activity != s.last_shown and \
                    time.monotonic() - s.last_progress >= PROGRESS_MIN_GAP:
                s.last_progress = time.monotonic()
                s.last_shown = activity
                self._status_line(s, self._status_text(s))
                self._set_thread_status(ts, activity)
        return cb

    def _goal_fetch(self, s, wait=10):
        """One-shot _cognition.ai/goal/get outside a prompt turn.
        Registers the id so a concurrent prompt-loop reader also
        resolves it into s.goal; a stolen response just means the
        state was updated elsewhere."""
        if not (s.alive and s.session_id):
            return None
        try:
            rid = s._request("_cognition.ai/goal/get",
                             {"sessionId": s.session_id})
            s._goal_get_ids.add(rid)
            try:
                s._wait_result(rid, time.monotonic() + wait,
                               lambda x: None, putback_foreign=True)
            finally:
                s._goal_get_ids.discard(rid)
            return s.goal
        except Exception:
            return None

    def _goal_drive(self, ts, s, flag):
        """Epilogue after a prompt turn: while the goal sits paused,
        send /goal resume and run the next segment. Re-entrancy is
        one driver per session; queued user prompts take over."""
        if not GOAL_AUTO_RESUME:
            return
        if not s.drive_lock.acquire(blocking=False):
            return
        try:
            resumes = 0
            while self.sessions.get(ts) is s and s.alive \
                    and not flag.is_set() and not s.queued:
                g = self._goal_fetch(s)
                if not g or g.get("status") != "paused":
                    return
                resumes += 1
                if resumes > GOAL_RESUME_MAX:
                    self.post(ts, L(f"🎯 已自动续跑 {GOAL_RESUME_MAX} 段，", f"🎯 auto-resumed {GOAL_RESUME_MAX} segments;") +
                                  L("暂停自动 resume；发 !goal resume ", "auto-resume paused; send !goal resume ") +
                                  L("手动继续。", "to continue manually."))
                    return
                self.post(ts, L(f"🎯 目标已跑 {g.get('turns', '?')} turn ", f"🎯 goal has run {g.get('turns', '?')} turns ") +
                              L(f"暂停，自动续跑（第 {resumes} 段）…", f"paused, auto-resuming (segment {resumes})…"))
                self._status_line(s, self._status_text(s, L("目标续跑中", "goal resuming")))
                seg_start = time.time()
                reply, reason = s.prompt("/goal resume",
                                         self._progress(ts), flag)
                if flag.is_set() or reason in ("cancelled",
                                               "interrupted"):
                    return
                self._final_reply(ts, s, reply)
                self._send_files(ts, s, reply, seg_start)
        finally:
            s.drive_lock.release()

    def _goal_watch(self, ts, s):
        """Idle-time watcher: an agent-declared goal file becomes a
        native /goal. Claim-by-rename makes each file one-shot."""
        while self.sessions.get(ts) is s and s.alive:
            try:
                self._maybe_attach_goal(ts, s)
            except Exception as e:
                self._log_err("goal_watch", e)
            time.sleep(GOAL_WATCH_SEC)

    def _maybe_attach_goal(self, ts, s):
        if s.running or s.queued or not s.alive:
            return
        for name in GOAL_FILES:
            path = os.path.join(s.workdir, name)
            try:
                mt = os.path.getmtime(path)
                if time.time() - mt < 2:
                    return
                # A file dropped during a turn or in its 30s wake is
                # almost certainly the session trying to self-continue;
                # goals only attach from an idle, human-placed file.
                # Claim-by-rename consumes it so it cannot keep retrying.
                if mt - s.last_turn_end < 30:
                    try:
                        os.rename(path, path + ".attached")
                    except OSError:
                        pass
                    self.post(ts, L(f"🎯 忽略疑似会话自建的目标文件 ", f"🎯 ignoring a goal file the session seems to have self-created ") +
                                  L(f"{name}（回合刚结束）；人工挂目标", f"{name} (turn just ended); manual goal attach") +
                                  L("请在空闲后重写该文件。", "please rewrite the file once it's idle."))
                    return
                with open(path, encoding="utf-8",
                          errors="replace") as f:
                    obj = f.read().strip()
            except OSError:
                continue
            if not obj:
                try:
                    os.unlink(path)
                except OSError:
                    pass
                continue
            try:
                os.rename(path, path + ".attached")
            except OSError:
                continue
            self.post(ts, L(f"🎯 检测到 {name}，挂为原生 goal：", f"🎯 detected {name}, attaching as native goal:") +
                          f"{obj.splitlines()[0][:80]}")
            threading.Thread(target=self._run_prompt,
                             args=(ts, "/goal " + obj[:4000]),
                             daemon=True).start()
            return

    def _remove(self, ts):
        with self.lock:
            self.sessions.pop(ts, None)
            self.stop_flags.pop(ts, None)

    def _dump_session(self, ts, s):
        """One live-session record for .bridge_live.json, or None when
        the acp can no longer be carried over."""
        if not getattr(s, "session_id", None):
            return None
        try:
            os.kill(s.proc.pid, 0)
            inf, outf = s.proc.stdin.fileno(), s.proc.stdout.fileno()
        except (OSError, ValueError, AttributeError):
            return None
        with s.turns_lock:
            turns = [dict(t) for t in s.turns]
        return {"ts": ts, "workdir": s.workdir,
                "session_id": s.session_id, "pid": s.proc.pid,
                "stdin_fd": inf, "stdout_fd": outf,
                "next_id": s.next_id, "auto": getattr(s, "auto", False),
                "model_name": s.model_name, "mode_name": s.mode_name,
                "mode_names": s.mode_names,
                "slash_cmds": sorted(s.slash_cmds),
                "goal": s.goal,
                "goal_elapsed": (time.monotonic() - s.goal_started_at
                                 if s.goal_started_at else 0),
                "status_ts": s.status_ts,
                "chunks": list(s.chunks),
                "draft_msgs": list(s.draft_msgs),
                "draft_sent": list(s.draft_sent),
                "turns": turns,
                "last_activity": s.last_activity,
                "last_turn_end": s.last_turn_end}

    def dump_live(self):
        """Snapshot every carryable session + the auto-dispatch queue
        to LIVE_FILE. Returns the session records written."""
        recs = []
        for ts, s in list(self.sessions.items()):
            d = self._dump_session(ts, s)
            if d:
                recs.append(d)
        with self.lock:
            queued = [list(q) for q in self.dispatch_queue]
        tmp = LIVE_FILE + f".{os.getpid()}.tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({"saved_at": time.time(), "sessions": recs,
                           "dispatch_queue": queued}, f,
                          ensure_ascii=False)
            os.replace(tmp, LIVE_FILE)
        except OSError as e:
            self._log_err("dump_live", e)
        return recs

    def request_reload(self):
        self.reload_ev.set()

    def _prepare_inherit(self):
        """exec() keeps only fds marked inheritable; buffered writes
        must reach the pipe first so nothing is stranded."""
        for s in list(self.sessions.values()):
            try:
                s.proc.stdin.flush()
                os.set_inheritable(s.proc.stdin.fileno(), True)
                os.set_inheritable(s.proc.stdout.fileno(), True)
            except (OSError, ValueError, AttributeError):
                pass

    def reload_exec(self):
        """Seamless restart: dump state, mark pipes inheritable, and
        exec() this same script. acp processes and in-flight turns
        ride through untouched."""
        recs = self.dump_live()
        self._prepare_inherit()
        os.environ[LIVE_ENV] = LIVE_FILE
        # Announcement is best-effort and time-boxed: Slack retries
        # must not stall the exec (or worse, a SIGTERM deadline).
        deadline = time.monotonic() + 6
        for d in recs:
            if time.monotonic() > deadline:
                break
            self._react_state(d["ts"], REACT_RELOADING)
            self.post(d["ts"], L("🔄 桥服务热重启：会话与生成中的内容", "🔄 Bridge hot restart: sessions and in-flight content") +
                               L("保持，几秒后接管…", "kept, taking over in a few seconds…"))
        os.execv(sys.executable, [sys.executable] + sys.argv)

    def reload_term(self):
        """systemd 'restart' path: park acp pipe fds in the service's
        fd store so the next process inherits them, then let the
        service manager bring up the replacement."""
        recs = self.dump_live()
        self._fdstore_push(recs)
        # Same 6s cap: the fd park is already done, but a SIGKILL
        # deadline is looming — never let Slack retries eat it.
        deadline = time.monotonic() + 6
        for d in recs:
            if time.monotonic() > deadline:
                break
            self._react_state(d["ts"], REACT_RELOADING)
            self.post(d["ts"], L("🔄 桥服务重启：会话保持，新进程接管…", "🔄 Bridge restarting: session kept, new process taking over…"))

    def _fdstore_push(self, recs):
        """sd_notify FDSTORE: systemd holds the fds across a full
        restart, so acp stdin never sees EOF."""
        notify = os.environ.get("NOTIFY_SOCKET")
        if not notify:
            return
        addr = "\0" + notify[1:] if notify.startswith("@") else notify
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            sock.connect(addr)
            for d in recs:
                for key, fd in (("in", d["stdin_fd"]),
                                ("out", d["stdout_fd"])):
                    fds = array.array("i", [fd])
                    sock.sendmsg(
                        [f"FDSTORE=1\nFDNAME=acp-{d['ts']}-{key}"
                         .encode()],
                        [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)])
        except OSError as e:
            self._log_err("fdstore", e)

    @staticmethod
    def _sd_listen_fds():
        """name -> fd map handed back by systemd on restart."""
        try:
            if int(os.environ.get("LISTEN_PID", "0")) != os.getpid():
                return {}
            n = int(os.environ.get("LISTEN_FDS", "0"))
        except ValueError:
            return {}
        names = os.environ.get("LISTEN_FDNAMES", "").split(":")
        return {names[i]: 3 + i for i in range(n) if i < len(names)}

    def restore_live(self, path=None):
        """Reattach sessions dumped by a previous bridge process.
        Returns the set of workdirs now owned live."""
        path = path or os.environ.get(LIVE_ENV) or LIVE_FILE
        try:
            with open(path) as f:
                data = json.load(f)
        except (OSError, ValueError):
            return set()
        fdstore = self._sd_listen_fds()
        restored = set()
        for d in data.get("sessions", []):
            ts, wd = d["ts"], d["workdir"]
            try:
                os.kill(d["pid"], 0)
            except OSError:
                continue
            inf = fdstore.get(f"acp-{ts}-in", d["stdin_fd"])
            outf = fdstore.get(f"acp-{ts}-out", d["stdout_fd"])
            try:
                s = DevinSession.reattach(ts, wd, self.timeout,
                                        d["pid"], inf, outf)
            except OSError:
                continue
            s.session_id = d["session_id"]
            s.next_id = d.get("next_id", 0)
            s.auto = d.get("auto", False)
            s.model_name = d.get("model_name")
            s.mode_name = d.get("mode_name")
            s.mode_names = d.get("mode_names") or {}
            s.slash_cmds = set(d.get("slash_cmds") or [])
            s.goal = d.get("goal")
            if d.get("goal_elapsed"):
                s.goal_started_at = (time.monotonic()
                                     - d["goal_elapsed"])
            s.status_ts = d.get("status_ts")
            s.chunks = list(d.get("chunks") or [])
            s.draft_msgs = list(d.get("draft_msgs") or [])
            s.draft_sent = list(d.get("draft_sent") or [])
            s.last_activity = d.get("last_activity") or L("处理中", "working")
            s.last_turn_end = d.get("last_turn_end") or 0.0
            s.notify = lambda msg, t=ts: self.post(t, msg)
            s.on_text = lambda t=ts: self._draft_tick(t)
            with self.lock:
                self.sessions[ts] = s
                self.stop_flags[ts] = threading.Event()
            restored.add(wd)
            self._react_state(ts, REACT_RELOADING)
            threading.Thread(target=self._goal_watch, args=(ts, s),
                             daemon=True).start()
            threading.Thread(target=self._drain_restored,
                             args=(ts, s, d), daemon=True).start()
        with self.lock:
            for q in data.get("dispatch_queue") or []:
                if q and not any(x[0] == q[0]
                                 for x in self.dispatch_queue):
                    self.dispatch_queue.append(tuple(q))
        try:
            os.unlink(path)
        except OSError:
            pass
        return restored

    def _drain_restored(self, ts, s, d):
        """Replay a restored session's recorded turns: a sent-but-
        unanswered prompt is awaited by request id, queued ones run
        fresh, then the session simply stays live."""
        turns = d.get("turns") or []
        for t in turns:
            self._run_prompt(ts, t.get("text", ""),
                             msg_ts=t.get("msg_ts"),
                             inflight_id=t.get("req_id"),
                             inflight_replay=True)
        if not turns:
            # Silent takeover: the pre-exec post already announced
            # L("几秒后接管", "taking over in seconds"), and main() posts one channel-level
            # summary — a per-thread line here is pure noise.
            write_state(s.workdir, "idle", s.last_activity,
                        L("桥热重启，会话保持", "bridge hot restart, session kept"))

    def adopt_inflight(self, skip=()):
        """Hard-death replay: workdirs whose last write recorded a
        running turn get session/load + the same prompt again.
        Returns workdirs adopted."""
        adopted = set()
        try:
            names = os.listdir(self.tasks_dir)
        except OSError:
            return adopted
        for name in names:
            wd = os.path.join(self.tasks_dir, name)
            if wd in skip:
                continue
            st = read_state(wd)
            if not st or not st.get("session_id"):
                continue
            inf = st.get("inflight")
            if not isinstance(inf, dict) or not inf.get("text"):
                continue
            if time.time() - st.get("updated", 0) > ADOPT_MAX_AGE:
                continue
            ts = ".".join(name.rsplit("_", 1))
            adopted.add(wd)
            self._react_state(ts, REACT_RELOADING)
            self.post(ts, L("🔄 恢复重启前未完成的回合…", "🔄 Resuming the turn interrupted by restart…"))
            threading.Thread(
                target=self._resume_session,
                args=(ts, inf["text"], None, st["session_id"]),
                kwargs={"msg_ts": inf.get("msg_ts"),
                        "ack_post": False, "replayed": True},
                daemon=True).start()
        return adopted

    def _auto_live(self):
        return (sum(1 for s in self.sessions.values()
                    if getattr(s, "auto", False))
                + len(self.starting_auto))

    def _pump_queue(self):
        """Start queued auto-dispatched tasks while an auto slot is
        free. Call after any session is removed or turns idle.
        Never holds self.lock across the spawn."""
        while True:
            with self.lock:
                if not self.dispatch_queue or \
                        self._auto_live() >= MAX_AUTO:
                    return
                ts, text, files = self.dispatch_queue.popleft()
                self.starting.add(ts)
                self.starting_auto.add(ts)
            # Entries survive exec reloads via LIVE_FILE: re-validate
            # before spawning so a task cancelled or reset while it
            # waited never starts behind the user's back.
            st = read_state(self._workdir(ts))
            if st and (st.get("state") in TERMINAL or
                       st.get("state") == "reset"):
                with self.lock:
                    self.starting.discard(ts)
                    self.starting_auto.discard(ts)
                self.post(ts, L("排队任务已取消（该线程任务已停止或重置）。", "queued task cancelled (this thread's task was stopped or reset)."))
                continue
            self.post(ts, L("轮到本任务，开始处理。", "your turn — working on it now."))
            threading.Thread(target=self._new_session,
                             args=(ts, text, files),
                             kwargs={"queued": True},
                             daemon=True).start()

    def _reap_idle_auto(self):
        """When queued work is waiting, reap idle auto sessions early
        so the slot frees now instead of at idle expiry."""
        for ts, s in list(self.sessions.items()):
            if not self.dispatch_queue:
                return
            if getattr(s, "auto", False) and not s.running:
                dead = s.terminate()
                write_state(s.workdir, "reaped", s.last_activity,
                            L("空闲回收，给排队任务让位", "idle reaped to make room for the queued task"))
                self._remove(ts)
                self._finish(ts, s, "zzz", L("💤 空闲回收", "💤 Idle reaped"))
                self.post(ts, L("任务已完成；空闲会话回收以运行排队任务", "task done; idle session reaped to run queued task")
                              + ("" if dead else L("（进程退出未确认）", "(process exit unconfirmed)"))
                              + "。")

    # ---- limit governor -------------------------------------------

    def _limit_park(self, ts, s, text, msg_ts, exc):
        """Park a turn killed by a server-side limit into the cooldown
        registry. True → caller skips the failure card; False → not a
        limit error, or the retry budget is spent (generic failure
        handling proceeds, whose rate-limit regex still renders a
        sensible detail line)."""
        info = limit_governor.classify_error(exc)
        if info is None:
            return False
        attempt = self.limit_attempts.get(ts, 0) + 1
        if attempt > LIMIT_RESUME_MAX:
            return False
        self.limit_attempts[ts] = attempt
        now = time.time()
        reset_at = info.get("reset_at")
        if reset_at and reset_at > now:
            resume_at = reset_at + random.uniform(2, 12)
            wait_for = (L("到 ", "at ")
                        + time.strftime("%H:%M",
                                        time.localtime(resume_at)))
        else:
            delay = limit_governor.backoff(attempt, LIMIT_BASE_BACKOFF,
                                           LIMIT_BACKOFF_CAP)
            resume_at = now + delay
            wait_for = L(f"约 {_dur(delay)} 后", f"in ~{_dur(delay)}")
        live = sum(1 for x in self.sessions.values() if x.alive)
        active = sum(1 for x in self.sessions.values() if x.running)
        self.governor.park(ts, {
            "kind": "bridge", "text": text, "msg_ts": msg_ts,
            "session_id": getattr(s, "session_id", None),
            "error": info["message"][:300], "error_kind": info["kind"],
            "reset_at": reset_at, "resume_at": resume_at,
            "attempt": attempt, "acct": _acct_fingerprint(),
            "live_sessions": live, "active_turns": active})
        write_state(s.workdir, "rate_limited", s.last_activity,
                    L(f"{info['kind']}，{wait_for}自动重试", f"{info['kind']}, auto-retry {wait_for}") +
                    L(f"（第 {attempt} 次）", f"(attempt {attempt})"))
        self._react_state(ts, REACT_LIMITED)
        self._status_line(s, L(f"⏸️ 限额冷却 · {wait_for}自动重试", f"⏸️ limit cooldown · auto-retry {wait_for}"))
        self.post(ts, L(f"⏸️ 触发服务端限额（{info['kind']}），本回合挂起，", f"⏸️ hit a server-side limit ({info['kind']}); this turn is parked,") +
                      L(f"{wait_for}自动重试", f"{wait_for} auto-retry") +
                      L(f"（第 {attempt}/{LIMIT_RESUME_MAX} 次）。", f"(attempt {attempt}/{LIMIT_RESUME_MAX}).") +
                      L("回复任意消息立即重试；`!limits` 看冷却队列。", "reply anything to retry now; `!limits` shows the cooldown queue."))
        self._board_touch()
        return True

    def limit_sweeper(self):
        """Cooldown scheduler: fire due resumes at most one per
        LIMIT_RESUME_GAP; on a slower cadence scan the shared session
        store for Desktop/CLI sessions the same limit took out."""
        while True:
            try:
                self._acct_drift_check()
                self._limit_sweep()
                if LIMIT_EXT and time.monotonic() - self._ext_scan_at \
                        > LIMIT_EXT_SCAN:
                    self._ext_scan_at = time.monotonic()
                    self._ext_scan()
                # Slow-cadence keep-alive for the pinned board: session
                # state also drifts outside bridge events (Desktop opens
                # /closes locks, external resumes finish). The render is
                # local-only; unchanged text costs no API call.
                self._board_touch()
            except Exception as e:
                self._log_err("limit sweep", e)
            time.sleep(LIMIT_SWEEP_SEC)

    def _acct_drift_check(self):
        """The user rotates Devin accounts on Desktop; the bridge
        follows that credential. A fingerprint change means the quota
        behind every parked turn is fresh — pull every queued resume
        forward instead of waiting out the old account's window."""
        acct = _acct_fingerprint()
        if not acct or acct == self._acct:
            return
        self.governor.event("acct_switch", prev=self._acct, acct=acct)
        self._acct = acct
        for e in self.governor.snapshot():
            key = e.get("key")
            if not key or e.get("suspect"):
                continue
            e2 = dict(e)
            e2.pop("key", None)
            e2["acct"] = acct
            e2["resume_at"] = time.time() + random.uniform(3, 15)
            self.governor.park(key, e2)

    def _limit_sweep(self):
        now = time.time()
        if now - self.governor.last_resume_at < LIMIT_RESUME_GAP:
            return
        due = self.governor.due(now)
        if not due:
            return
        key, entry = due[0]
        self.governor.pop(key, "dispatch")
        self.governor.last_resume_at = now
        self.governor.event("resume_attempt", key=key,
                            attempt=entry.get("attempt"),
                            session=entry.get("session_id"))
        if entry.get("kind") == "external":
            threading.Thread(target=self._ext_resume, args=(entry,),
                             daemon=True).start()
            return
        s = self.sessions.get(key)
        if s and s.alive:
            self.post(key, L("🔄 限额冷却结束，重试上一回合…", "🔄 Cooldown over, retrying last turn…"))
            threading.Thread(
                target=self._run_prompt,
                args=(key, entry.get("text") or RESUME_NUDGE, None),
                kwargs={"msg_ts": entry.get("msg_ts")},
                daemon=True).start()
        elif entry.get("session_id"):
            threading.Thread(
                target=self._resume_session,
                args=(key, entry.get("text") or RESUME_NUDGE, None,
                      entry["session_id"]),
                kwargs={"msg_ts": entry.get("msg_ts"),
                        "replayed": True},
                daemon=True).start()
        else:
            self.governor.event("resume_skip", key=key,
                                reason="no_session")

    # ---- external (Desktop/CLI) victims ---------------------------

    def _bridge_session_ids(self):
        owned = {getattr(s, "session_id", None)
                 for s in self.sessions.values()}
        try:
            for name in os.listdir(self.tasks_dir):
                st = read_state(os.path.join(self.tasks_dir, name))
                if st and st.get("session_id"):
                    owned.add(st["session_id"])
        except OSError:
            pass
        for e in self.governor.snapshot():
            if e.get("session_id"):
                owned.add(e["session_id"])
        owned.discard(None)
        return owned

    def _ext_tail_verdict(self, sid, tail=6):
        """-> (verdict, reset_at|None, newest_node_ts|None): the
        session's last transcript nodes say 'limited' (a rate-limit
        error sits in the tail), 'midturn' (turn cut mid-flight), or
        'idle'. newest_node_ts is the created_at of the freshest node —
        the liveness signal: a live turn writes nodes continuously, a
        dead turn's newest node stays stale."""
        try:
            db = _devin_db()
            try:
                rows = db.execute(
                    "SELECT chat_message, created_at FROM message_nodes"
                    " WHERE session_id=? ORDER BY node_id DESC LIMIT ?",
                    (sid, tail)).fetchall()
            finally:
                db.close()
        except Exception:
            return "idle", None, None
        nodes = []
        newest = None
        for cm, ts in rows:
            if newest is None and ts:
                newest = ts
            try:
                nodes.append(json.loads(cm))
            except ValueError:
                continue
        verdict = limit_governor.tail_verdict(nodes)
        reset = None
        if verdict == "limited":
            for j in nodes[:5]:
                c = j.get("content") or ""
                if limit_governor.TOOL_LIMIT_RE.match(c):
                    reset = limit_governor.parse_reset(c)
                    break
        return verdict, reset, newest

    def _ext_scan(self):
        """Park external sessions the limit took out.

        A transcript tail showing a limit error, or one that ends
        mid-turn, marks a victim. Sessions no live process owns
        (stale/absent lock) park for auto-resume — nothing else can be
        running them. Sessions a live frontend still locks become
        'suspects': tracked in !limits, promoted to resumable when the
        lock dies (or right away with EXT_STEAL). Mid-turn tails only
        count as suspects while a limit window is open — a healthy
        busy session looks identical otherwise."""
        now = time.time()
        owned = self._bridge_session_ids()
        for e in self.governor.snapshot():
            key = e.get("key") or ""
            sid = e.get("session_id")
            if not key.startswith("ext:") or not sid:
                continue
            row = devin_session(sid)
            if not row:
                self.governor.pop(key, "gone")
                continue
            if row["activity"] > e.get("first_hit", 0):
                self.governor.pop(key, "superseded")
            elif not e.get("suspect"):
                continue
            elif not row["lock_pid"] or (
                    LIMIT_EXT_STEAL and (
                        e.get("verdict") == "limited"
                        or now - self.governor.last_limit_at
                        < LIMIT_EXT_WINDOW)):
                e2 = dict(e)
                e2.pop("key", None)
                e2.pop("suspect", None)
                e2["resume_at"] = now + random.uniform(5, 25)
                self.governor.park(key, e2)
                self.governor.event("suspect_promoted", session=sid)
            elif now - e.get("first_hit", now) > LIMIT_EXT_WINDOW:
                self.governor.pop(key, "expired")
        parked_new = 0
        for row in list_devin_sessions(limit=60):
            sid = row["id"]
            orphan_window = LIMIT_EXT_WINDOW if row["lock_pid"] \
                else LIMIT_EXT_ORPHAN_WINDOW
            if sid in owned or not row.get("workdir") \
                    or self.governor.get("ext:" + sid) \
                    or now - row["activity"] > orphan_window:
                continue
            verdict, reset, node_ts = self._ext_tail_verdict(sid)
            if verdict not in ("limited", "midturn"):
                continue
            locked = bool(row["lock_pid"])
            # Fresh nodes = someone is still writing; a live turn can
            # even carry an absorbed limit error mid-tail. Only stale
            # tails count as dead on locked sessions. An unlocked
            # session showing a limit error just died — no driver can
            # exist without the lock.
            stale = node_ts is not None \
                and now - node_ts > LIMIT_EXT_FRESH
            if not stale and (verdict != "limited" or locked):
                continue
            suspect = False
            if locked:
                # Stealing a live lock only gets justified by hard
                # evidence: a limit error in the tail, or a stale
                # mid-turn during an open limit window. A stale locked
                # mid-turn otherwise is ambiguous (dead turn vs >10min
                # silent tool call) — watch it as a suspect either way.
                window_open = now - self.governor.last_limit_at \
                    < LIMIT_EXT_WINDOW
                if not LIMIT_EXT_STEAL or (
                        verdict == "midturn" and not window_open):
                    suspect = True
            if suspect:
                self.governor.park("ext:" + sid, {
                    "kind": "external", "session_id": sid,
                    "workdir": row["workdir"],
                    "title": row.get("title") or "",
                    "error_kind": "rate_limited", "suspect": True,
                    "verdict": verdict,
                    "reset_at": reset, "acct": self._acct,
                    "resume_at": now + LIMIT_EXT_WINDOW})
                self.governor.event("external_suspect", session=sid,
                                    pid=row["lock_pid"],
                                    verdict=verdict)
                continue
            entry = {"kind": "external", "session_id": sid,
                     "workdir": row["workdir"],
                     "title": row.get("title") or "",
                     "error_kind": "rate_limited", "acct": self._acct,
                     "reset_at": reset,
                     "resume_at": (reset + random.uniform(2, 12))
                     if reset else now + limit_governor.backoff(
                         1, LIMIT_BASE_BACKOFF, LIMIT_BACKOFF_CAP)}
            self.governor.park("ext:" + sid, entry)
            parked_new += 1
            if parked_new >= LIMIT_EXT_MAX:
                break

    def _ext_resume(self, entry):
        """Drive one stalled external session back to life: steal the
        (dead-turn) session lock if a frontend still holds it, spawn a
        one-shot acp, session/load, then prompt it to continue."""
        sid = entry.get("session_id")
        wd = entry.get("workdir")
        if not sid or not wd or not os.path.isdir(wd):
            self.governor.event("external_skip", session=sid,
                                reason="workdir_gone")
            return
        pid = devin_lock_pid(sid)
        if pid and not LIMIT_EXT_STEAL:
            e = dict(entry)
            e.pop("attempt", None)
            e["suspect"] = True
            e["resume_at"] = time.time() + LIMIT_EXT_WINDOW
            self.governor.park("ext:" + sid, e)
            self.governor.event("external_locked", session=sid, pid=pid)
            return
        if pid:
            verdict, _r, nts = self._ext_tail_verdict(sid)
            if verdict == "idle" or (
                    verdict == "midturn" and nts is not None
                    and time.time() - nts < LIMIT_EXT_FRESH):
                self.governor.event("external_revived", session=sid,
                                    pid=pid, verdict=verdict)
                return
            try:
                os.unlink(os.path.join(DEVIN_LOCKS, sid + ".lock"))
                self.governor.event("external_lock_steal",
                                    session=sid, pid=pid)
            except OSError:
                pass
        ext_wd = os.path.join(self.tasks_dir, ".ext_sessions", sid)
        s = None
        try:
            s = DevinSession(None, ext_wd, self.acp_cmd, self.timeout,
                             acp_cwd=wd)
            s.start(resume_id=sid, strict_load=True)
            reply, reason = s.prompt(EXT_RESUME_PROMPT,
                                     lambda *a: None,
                                     threading.Event())
            self.governor.event("external_resumed", session=sid,
                                stop=reason)
            self.limit_attempts.pop("ext:" + sid, None)
            if LIMIT_EXT_POST:
                title = _first_line(entry.get("title")) or sid
                self.post(None, L("🔄 限额恢复：桌面/CLI 会话 ", "🔄 Limit lifted: Desktop/CLI session ") +
                                L(f"`{sid}`（{title}）已自动续跑。", f"`{sid}` ({title}) auto-resumed."))
        except Exception as e:
            info = limit_governor.classify_error(e)
            if info and entry.get("attempt", 1) < LIMIT_RESUME_MAX:
                e2 = dict(entry)
                e2.pop("attempt", None)
                e2["error"] = info["message"][:300]
                e2["error_kind"] = info["kind"]
                e2["reset_at"] = info.get("reset_at")
                e2["resume_at"] = (
                    info["reset_at"] + random.uniform(2, 12)
                    if info.get("reset_at") else
                    time.time() + limit_governor.backoff(
                        entry.get("attempt", 1)))
                self.governor.park("ext:" + sid, e2)
            else:
                self.governor.event("external_fail", session=sid,
                                    error=f"{type(e).__name__}: {e}")
        finally:
            if s is not None:
                s.terminate()
            self._board_touch()

    def adopt_limited(self):
        """Pre-feature casualties: workdirs left 'failed'/'interrupted'
        on a limit signature (or still showing a cooldown state the
        registry lost) get parked for auto-resume instead of waiting
        for someone to reply by hand."""
        adopted = []
        try:
            names = os.listdir(self.tasks_dir)
        except OSError:
            return adopted
        now = time.time()
        for name in names:
            wd = os.path.join(self.tasks_dir, name)
            st = read_state(wd)
            if not st or not st.get("session_id"):
                continue
            if st.get("state") not in ("failed", "interrupted",
                                       "rate_limited"):
                continue
            if now - st.get("updated", 0) > LIMIT_ADOPT_AGE:
                continue
            detail = st.get("detail") or ""
            limited = st.get("state") == "rate_limited" \
                or limit_governor.LIMIT_RE.search(detail) \
                or "continue retrying" in detail
            if not limited:
                continue
            ts = ".".join(name.rsplit("_", 1))
            if self.governor.get(ts):
                continue
            self.governor.park(ts, {
                "kind": "bridge", "text": RESUME_NUDGE,
                "session_id": st["session_id"],
                "error": (detail or "resumed_after_restart")[:300],
                "error_kind": "rate_limited", "acct": self._acct,
                "resume_at": now + random.uniform(10, 40)})
            adopted.append(ts)
            self._react_state(ts, REACT_LIMITED)
        return adopted

    def _limits(self, root, arg=""):
        """!limits: cooldown queue + recent limit events, or drop this
        thread's parked turn with `!limits clear`."""
        if (arg or "").strip().lower() in ("clear", "drop", L("取消", "cancel")):
            e = self.governor.pop(root, "manual_clear")
            self.post(root, L("已取消本线程的限额重试。", "cancelled this thread's limit retries.") if e
                      else L("本线程没有在冷却的回合。", "no cooling-down turns on this thread."))
            return
        parked = self.governor.snapshot()
        now = time.time()
        lines = []
        if parked:
            lines.append(L("*冷却队列*", "*Cooldown queue*"))
            for e in parked:
                if e.get("kind") == "external":
                    who = L("桌面/CLI", "Desktop/CLI")
                elif e["key"] == root:
                    who = L("本线程", "this thread")
                else:
                    who = L(f"线程 {e['key']}", f"thread {e['key']}")
                if e.get("suspect"):
                    when = (L("前端仍占锁，仅观察（steal=1 可接管）", "frontend still holds the lock, watching only (steal=1 to take over)")
                            if not LIMIT_EXT_STEAL else L("等待接管", "awaiting takeover"))
                else:
                    wait = int(e.get("resume_at", 0) - now)
                    when = L(f"{wait}s 后重试", f"retry in {wait}s") if wait > 0 else L("待调度", "pending")
                lines.append(
                    f"• {who} `{e.get('session_id') or '?'}` " +
                    L(f"{e.get('error_kind')} · 第 ", f"{e.get('error_kind')} · attempt ") +
                    L(f"{e.get('attempt', '?')} 次 · {when}", f"attempt {e.get('attempt', '?')} · {when}"))
        else:
            lines.append(L("冷却队列是空的。", "cooldown queue is empty."))
        evs = self.governor.events_tail(8)
        if evs:
            lines.append(L("*最近限额事件*", "*Recent limit events*"))
            for e in evs:
                t = time.strftime("%H:%M:%S",
                                  time.localtime(e.get("t", 0)))
                who = e.get("key") or e.get("session") or ""
                desc = (e.get("error") or "")[:60]
                lines.append(f"• {t} `{e.get('event')}` {who}"
                             + (f" — {desc}" if desc else ""))
        self.post(root, "\n".join(lines))

    def _seed_workdir(self, workdir, ts):
        """Drop an AGENTS.md wiring shared + thread memory into the
        session's context, and seed the shared memory file."""
        try:
            os.makedirs(workdir, exist_ok=True)
        except OSError:
            return
        mem = os.path.expanduser(MEMORY_FILE)
        if not os.path.exists(mem):
            try:
                os.makedirs(os.path.dirname(mem), exist_ok=True)
                with open(mem, "w") as f:
                    f.write(L("# 跨会话记忆\\n\\n记录需要长期记住的事实、", "# Cross-session memory\n\nDurable facts to remember:") +
                            L("偏好、决定与关键路径。按日期小节追加，", "preferences, decisions, key paths. Append under date headings,") +
                            L("保持精简。\\n", "keep it concise.\n"))
            except OSError:
                pass
        agents = os.path.join(workdir, "AGENTS.md")
        if not os.path.exists(agents):
            try:
                with open(agents, "w") as f:
                    f.write(AGENTS_TMPL.format(mem=mem, ts=ts))
            except OSError:
                pass

    def _save_inbound(self, workdir, files):
        """Download Slack file attachments into workdir/inbox/ and
        return prompt lines with their local paths."""
        inbox = os.path.join(workdir, "inbox")
        try:
            os.makedirs(inbox, exist_ok=True)
        except OSError:
            return []
        token = os.environ.get("SLACK_BOT_TOKEN", "")
        out = []
        for f in files:
            url = f.get("url_private_download") or f.get("url_private")
            name = re.sub(r"[^\w.一-鿿-]+", "_",
                          f.get("name") or f.get("id") or "file")
            if not url:
                out.append(L(f"[附件 {name}: 无下载地址]", f"[attachment {name}: no download url]"))
                continue
            dest = os.path.join(inbox, name)
            try:
                req = urllib.request.Request(
                    url, headers={"Authorization": f"Bearer {token}"})
                with urllib.request.urlopen(req, timeout=60) as r, \
                        open(dest, "wb") as w:
                    shutil.copyfileobj(r, w)
                out.append(L(f"[附件已存] {dest}", f"[attachment saved] {dest}"))
            except Exception as e:
                out.append(L(f"[附件 {name} 下载失败: {type(e).__name__}]", f"[attachment {name} download failed: {type(e).__name__}]"))
        return out

    def _collect_sendables(self, s, reply, since):
        """(send_now, pending): what goes into the thread vs. pools for
        `!files`/`!send`.

        *named* — the reply mentions an absolute path or basename;
        that's intent, always sent (any type). *auto* — new files
        with a deliverable extension written under the workdir this
        turn, minus working/internal files, deduped per path for the
        life of the session, capped at AUTO_SEND_MAX per turn; the
        rest pool into `pending`.
        """
        sent = getattr(s, "sent_files", None)
        if sent is None:
            sent = s.sent_files = {}
        named, auto = [], []

        def ok(p):
            try:
                return os.path.isfile(p) and \
                    0 < os.path.getsize(p) <= MAX_UPLOAD_BYTES
            except OSError:
                return False

        def fresh(p):
            try:
                return os.path.getmtime(p) > since
            except OSError:
                return False

        def internal(p):
            return bool(INTERNAL_FILE_RE.search(p))

        for m in re.findall(r"(/[^\s\"'`<>\[\]{}()|]+)", reply or ""):
            p = m.rstrip(".,;:，。；：）)")
            if ok(p) and not internal(p):
                named.append(os.path.realpath(p))
        mentioned = {os.path.basename(m) for m in re.findall(
            r"[\w.\\/-]*[\w-]+\.[A-Za-z0-9]{1,6}", reply or "")
            if os.path.basename(m)}
        # Basename mentions resolve against anything the session
        # touched — but touching alone never ships a file.
        for p in s.touched:
            base = os.path.basename(p)
            if base in mentioned and ok(p) and not internal(p):
                named.append(os.path.realpath(p))

        skip_dirs = {"inbox", "node_modules", "__pycache__", "dist",
                     ".git", ".venv", "venv"}
        named_set = set(named)
        for dirpath, dirs, names in os.walk(s.workdir):
            dirs[:] = [d for d in dirs if not d.startswith(".")
                       and d not in skip_dirs]
            for n in names:
                p = os.path.join(dirpath, n)
                if not ok(p) or not fresh(p) or internal(p):
                    continue
                rp = os.path.realpath(p)
                if rp in named_set:
                    continue
                if n in mentioned:
                    named.append(rp)
                    named_set.add(rp)
                    continue
                stem = os.path.splitext(n)[0].lower()
                if n.startswith(".") or \
                        os.path.splitext(n)[1].lower() not in SEND_EXT \
                        or stem in WORKFILE_NAMES:
                    continue
                auto.append(rp)

        send_now, pending = [], []
        seen = set()
        pooled = {os.path.realpath(p) for p in
                  getattr(s, "pending_files", [])}
        for p in named:
            if p in seen:
                continue
            seen.add(p)
            try:
                mt = os.path.getmtime(p)
            except OSError:
                continue
            # Named re-sends only when the file actually changed —
            # pointing at the same untouched file twice isn't a new
            # upload.
            if sent.get(p) == mt:
                continue
            send_now.append((p, mt))
        for p in auto:
            if p in sent or p in seen or p in pooled:
                continue  # one auto-send per path per session;
                          # pooled files wait for `!send`
            if len(send_now) >= AUTO_SEND_MAX:
                pending.append(p)
                continue
            try:
                mt = os.path.getmtime(p)
            except OSError:
                continue
            seen.add(p)
            send_now.append((p, mt))
        return [p for p, _ in send_now], pending, \
            {p: mt for p, mt in send_now}

    def _send_files(self, ts, s, reply, since):
        if not self.upload_msg:
            return
        send_now, pending, sent_new = self._collect_sendables(
            s, reply, since)
        for p in send_now:
            for attempt in range(3):
                try:
                    self.upload_msg(ts, p)
                    s.sent_files[p] = sent_new[p]
                    break
                except Exception as e:
                    # Transport failures (timeout, dropped connection)
                    # are worth a retry — Slack's upload endpoint is
                    # flaky under load. API-level errors aren't.
                    if attempt < 2 and isinstance(e, OSError):
                        time.sleep(1.5 * (attempt + 1))
                        continue
                    self._log_err(f"upload {p}", e)
                    self.post(ts, L(f"📎 文件发送失败 ", f"📎 file send failed ") +
                                  f"{os.path.basename(p)} " +
                                  L(f"({_errname(e)})，路径: {p}", f"({_errname(e)}), path: {p}"))
                    break
        if pending:
            s.pending_files = list(
                dict.fromkeys(getattr(s, "pending_files", [])
                              + pending))
            names = ", ".join(f"`{os.path.basename(p)}`"
                              for p in pending[:4])
            more = (L(f" 等{len(pending)}个", f" and {len(pending)} more") if len(pending) > 4 else "")
            self.post(ts, L(f"📁 还有新文件没发：{names}{more} — ", f"📁 new files not sent: {names}{more} — ") +
                          L("`!files` 看全部，`!send <名字>` 或 ", "`!files` lists all, `!send <name>` or ") +
                          L("`!send all` 拉取。", "`!send all` pulls everything."))

    _SKIP_DIRS = ("inbox", "node_modules", "__pycache__", "dist",
                  ".git", ".venv", "venv")

    def _deliverables_walk(self, wd):
        """Uploadable deliverables under the workdir, mtime-desc."""
        out = []
        for dirpath, dirs, names in os.walk(wd):
            dirs[:] = [d for d in dirs if not d.startswith(".")
                       and d not in self._SKIP_DIRS]
            for n in names:
                p = os.path.join(dirpath, n)
                if INTERNAL_FILE_RE.search(p) or \
                        os.path.splitext(n)[1].lower() not in SEND_EXT:
                    continue
                try:
                    if 0 < os.path.getsize(p) <= MAX_UPLOAD_BYTES:
                        out.append((os.path.getmtime(p), p))
                except OSError:
                    pass
        return [p for _, p in sorted(out, reverse=True)]

    def _files_list(self, root):
        """!files — what this thread can pull, marked by state."""
        s = self.sessions.get(root)
        wd = getattr(s, "workdir", None) or self._workdir(root)
        sent = getattr(s, "sent_files", {}) if s else {}
        pend = {os.path.realpath(p) for p in
                getattr(s, "pending_files", [])
                if os.path.isfile(p)} if s else set()
        lines = [L("📁 *本线程可取的文件*", "📁 *Files available in this thread*")]
        shown = 0
        seen = set()
        for p in sorted(pend, key=lambda p: -os.path.getmtime(p)) + \
                self._deliverables_walk(wd):
            rp = os.path.realpath(p)
            if rp in seen:
                continue
            seen.add(rp)
            mark = L("（未发）", "(unsent)") if rp in pend else \
                   (L("（已发过）", "(sent)") if rp in sent else "")
            lines.append(f"• `{os.path.basename(p)}`{mark}")
            shown += 1
            if shown >= 15:
                lines.append("• …")
                break
        if not shown:
            lines.append(L("（还没有产出文件）", "(no files produced yet)"))
        lines.append(L("发 `!send <名字>` 拉单个，`!send all` 全拉。", "`!send <name>` pulls one, `!send all` pulls all."))
        self.post(root, "\n".join(lines))

    def _send_named(self, root, arg):
        """!send <name|path|all> — pull files on demand."""
        s = self.sessions.get(root)
        name = (arg or "").strip().strip("`").strip('"').strip("'")
        if not name:
            self.post(root, L("用法：`!send <文件名|路径>` 或 `!send all`", "usage: `!send <filename|path>` or `!send all`") +
                            L("（`!files` 先看有什么）。", "(see `!files` first)."))
            return
        if name.lower() == "all":
            if s is None:
                self.post(root, L("没有待发文件。", "no files waiting to send."))
                return
            pend = [p for p in s.pending_files if os.path.isfile(p)]
            if not pend:
                self.post(root, L("没有待发文件。", "no files waiting to send."))
                return
            ok_ = 0
            for p in pend:
                try:
                    self.upload_msg(root, p)
                    s.sent_files[p] = os.path.getmtime(p)
                    ok_ += 1
                except Exception as e:
                    self._log_err(f"upload {p}", e)
            s.pending_files = [p for p in s.pending_files
                               if p not in pend]
            self.post(root, L(f"📎 已发 {ok_}/{len(pend)} 个文件。", f"📎 sent {ok_}/{len(pend)} files."))
            return
        cands = []
        if os.path.isabs(name) and os.path.isfile(name):
            cands = [name]
        else:
            wd = getattr(s, "workdir", None) or self._workdir(root)
            for pool in (getattr(s, "pending_files", []) if s else [],
                         list(getattr(s, "touched", {})) if s else []):
                cands += [p for p in pool
                          if os.path.basename(p) == name
                          and os.path.isfile(p)]
            if not cands:
                for dirpath, dirs, names in os.walk(wd):
                    dirs[:] = [d for d in dirs
                               if not d.startswith(".")
                               and d not in self._SKIP_DIRS]
                    cands += [os.path.join(dirpath, n) for n in names
                              if n == name]
        if not cands:
            self.post(root, L(f"没找到 `{name}`——`!files` ", f"`{name}` not found — `!files` ") +
                            L("看可取的文件。", "to see available files."))
            return
        p = os.path.realpath(cands[0])
        try:
            self.upload_msg(root, p)
            if s is not None:
                s.sent_files[p] = os.path.getmtime(p)
                s.pending_files = [q for q in s.pending_files
                                   if os.path.realpath(q) != p]
        except Exception as e:
            self._log_err(f"upload {p}", e)
            self.post(root, L(f"📎 发 {os.path.basename(p)} 失败", f"📎 failed to send {os.path.basename(p)}") +
                            f"({_errname(e)})。")

    def _ledger_user_msg(self, msg_ts, root_ts, text):
        """Record a session-bound user message into the personal-context
        queue. source id = the slack event ts; ingest dedups replays on
        the same id, so redelivery cannot create a second record."""
        put = os.path.expanduser(os.environ.get(
            "PCTX_MESSAGES_PUT", PCTX_MESSAGES_PUT_DEFAULT))
        if not put or not os.path.exists(put):
            return
        try:
            r = subprocess.run(
                # system python3: the bridge venv has no PyYAML, which the
                # pipeline scripts import via store.py
                ["python3", put, "put", text,
                 "--source", "slack", "--id", msg_ts,
                 "--thread", root_ts],
                capture_output=True, timeout=10, check=False)
            if r.returncode != 0:
                self._log_err("ledger_user_msg rc", RuntimeError(
                    r.stderr.decode("utf-8", "replace").strip()[:200]))
        except Exception as e:
            self._log_err("ledger_user_msg", e)

    def _run_prompt(self, ts, text, files=None, msg_ts=None,
                    inflight_id=None, inflight_replay=False,
                    goal_replay=False):
        s = self.sessions.get(ts)
        if not s:
            return
        # Any fresh prompt supersedes a parked one — the sweeper already
        # popped the entry it's dispatching, and a human reply shouldn't
        # later be shadowed by a stale auto-resume of the old turn.
        self.governor.pop(ts, "new_turn")
        if text.lstrip().startswith("/goal"):
            cleared = _goal_cleared_path(s.workdir)
            if os.path.exists(cleared):
                try:
                    os.unlink(cleared)
                except OSError:
                    pass
                if goal_replay:
                    self.post(ts, L("目标是人工清除的，已跳过这次自动", "goal was cleared manually — skipped this auto") +
                                  L("重放；要重跑请重新发 !goal。", "replay; to re-run, send !goal again."))
                    return
        if files:
            lines = self._save_inbound(s.workdir, files)
            if lines:
                text = (text + "\n" if text else "") + "\n".join(lines)
        self._ledger_user_msg(msg_ts or ts, ts, text)
        flag = self.stop_flags.setdefault(ts, threading.Event())
        self._react_state(ts, REACT_WORKING)
        status_stop = threading.Event()
        status_lock = threading.Lock()

        def status_keepalive():
            # setStatus fades after ~2min without a reply; refresh it so
            # the shimmer survives long silent stretches
            while not status_stop.wait(60):
                if self.sessions.get(ts) is not s:
                    return
                with status_lock:
                    if status_stop.is_set():
                        return
                    self._set_thread_status(
                        ts, s.last_shown or L("处理中…", "working…"))
        if self.status_msg:
            self._set_thread_status(ts, L("处理中…", "working…"))
            threading.Thread(target=status_keepalive, daemon=True).start()
        turn_start = time.time()
        try:
            reply, reason = s.prompt(text, self._progress(ts), flag,
                                     msg_ts=msg_ts,
                                     inflight_id=inflight_id,
                                     grace=(LOST_TURN_GRACE
                                            if inflight_id else None))
            if self.sessions.get(ts) is not s:
                return
            flag.clear()
            self.crash_retries.pop(ts, None)
            self.limit_attempts.pop(ts, None)
            write_state(s.workdir, "idle", s.last_activity,
                        L("任务已取消", "task cancelled") if reason in
                        ("cancelled", "queued_cancelled")
                        else L("任务被打断", "task interrupted") if reason == "interrupted"
                        else L("上一任务已完成", "previous task completed"))
            if reason in ("cancelled", "queued_cancelled",
                          "interrupted"):
                interrupted = reason == "interrupted"
                if interrupted:
                    self._finish(ts, s, "double_vertical_bar",
                                 L("⏸ 已打断", "⏸ Interrupted"))
                else:
                    self._finish(ts, s, "octagonal_sign", L("🛑 已停止", "🛑 Stopped"))
                self._post_think(ts, s)
                # Work produced before the cut-off still belongs in
                # the thread — send files on interrupted turns too.
                self._send_files(ts, s, reply, turn_start)
                if reason == "queued_cancelled":
                    self.post(ts, L("（排队中已取消）", "(cancelled while queued)"))
                elif interrupted:
                    self.post(ts, (reply + "\n" if reply else "")
                              + L("（已打断，接着处理新消息）", "(interrupted, now handling your new message)"))
                else:
                    self.post(ts, (reply + "\n" if reply else "")
                              + L("（已停止）", "(stopped)"))
            else:
                self._final_reply(ts, s, reply)
                self._send_files(ts, s, reply, turn_start)
                self._goal_drive(ts, s, flag)
                write_state(s.workdir, "idle", s.last_activity,
                            L("上一任务已完成", "previous task completed"))
                self._finish(ts, s, "white_check_mark", L("✅ 完成", "✅ Done"))
        except StopStalled as e:
            dead = s.terminate()
            write_state(s.workdir, "stopped", s.last_activity, str(e))
            self._remove(ts)
            self._finish(ts, s, "octagonal_sign", L("🛑 已停止", "🛑 Stopped"))
            self.post(ts, L("已停止（取消未响应，已强制结束该会话）。", "stopped (cancel went unanswered; session force-ended).") if dead
                      else L("已发出停止并结束会话；进程退出未确认，请检查。", "stop sent and session ended; process exit unconfirmed, please check."))
        except PromptTimeout:
            dead = s.terminate()
            sid = getattr(s, "session_id", None)
            if inflight_replay and sid and \
                    self.crash_retries.get(ts, 0) < CRASH_RESUME_MAX:
                # A restored turn that never produced output probably
                # lost its result in the exec gap; replay it on a
                # loaded session instead of failing the user.
                self.crash_retries[ts] = \
                    self.crash_retries.get(ts, 0) + 1
                write_state(s.workdir, "crashed", s.last_activity,
                            L("热重启后续等超时", "post-hot-restart wait timed out"))
                self._remove(ts)
                self.post(ts, L("热重启后回合失联，自动恢复会话并重跑…", "turn lost after hot restart; auto-restoring session and re-running…"))
                self._resume_session(ts, text, files, sid,
                                     msg_ts=msg_ts, replayed=True)
                return
            write_state(s.workdir, "timeout", s.last_activity,
                        L(f"静默超过 {int(self.timeout)}s", f"silent for {int(self.timeout)}s"))
            self._remove(ts)
            self._finish(ts, s, "alarm_clock",
                         L(f"⏰ 静默 {int(self.timeout)}s", f"⏰ silent {int(self.timeout)}s"),
                         detail=L(f"无活动超过 {int(self.timeout)}s，", f"no activity for {int(self.timeout)}s,") +
                                L("判定卡死，会话已终止。", "judged stuck; session terminated.") +
                         ("" if dead else L("（进程退出未确认，请检查）", "(process exit unconfirmed, please check)")))
        except AcpExited:
            s.terminate()
            write_state(s.workdir, "crashed", s.last_activity)
            self._remove(ts)
            sid = getattr(s, "session_id", None)
            retries = self.crash_retries.get(ts, 0)
            if sid and retries < CRASH_RESUME_MAX:
                # The process died, not the session: reload the same acp
                # session in a fresh process and re-run this turn.
                self.crash_retries[ts] = retries + 1
                self._draft_final(ts, s)
                self.post(ts, L("devin 进程异常退出，自动恢复会话并重跑", "devin process exited unexpectedly; auto-restoring and re-running") +
                              L("本轮…", "this turn…"))
                self._resume_session(ts, text, files, sid, msg_ts=msg_ts,
                                     replayed=True)
            else:
                self._finish(ts, s, "boom", L("💥 进程退出", "💥 process exited"),
                             detail=L("devin 进程异常退出，会话已结束。", "devin process exited unexpectedly; session ended.")
                             if not retries else
                             L("自动恢复后再次退出，会话已结束。", "exited again after auto-restore; session ended."))
        except Exception as e:
            parked = (not flag.is_set()
                      and self._limit_park(ts, s, text, msg_ts, e))
            flag.clear()
            if parked:
                return
            write_state(s.workdir, "failed", s.last_activity,
                        f"{type(e).__name__}: {e}")
            hint = (L("；回复本线程任意消息可让它重试", "; reply anything in this thread to retry")
                    if getattr(e, "retryable", False) else "")
            rl = re.search(
                r"[Rr]ate limit.*?reset in ([^(]+)(?:\(at ([^)]+)\))?",
                str(e))
            if rl:
                detail = (L(f"模型限额，约 {rl.group(1).strip()} 后重置", f"model rate limit, resets in ~{rl.group(1).strip()}")
                          + (f"（{rl.group(2)}）" if rl.group(2) else "")
                          + L("；回复本线程任意消息可让它重试。", "; reply anything in this thread to retry."))
            else:
                detail = f"{type(e).__name__}: {e}{hint}"
            self._finish(ts, s, "x", L("❌ 出错", "❌ Error"), detail=detail)
        finally:
            status_stop.set()
            with status_lock:
                self._set_thread_status(ts, "")
            self._draft_final(ts, s)
            self._reap_idle_auto()
            self._pump_queue()

    def _new_session(self, ts, text, files=None, queued=False):
        wd = self._workdir(ts)
        self._seed_workdir(wd, ts)
        ack_ts = None
        if not queued:
            # Ack before the spawn+handshake, which can take many
            # seconds; silence reads as 'ignored me'. The ack becomes
            # the live status card below, not a second message.
            ack_ts = self.post(ts, L("已接单，开始处理。", "accepted, working on it."))
        s = None
        try:
            s = DevinSession(ts, wd, self.acp_cmd, self.timeout)
            s.notify = lambda msg: self.post(ts, msg)
            s.on_text = lambda: self._draft_tick(ts)
            s.notify_perm = lambda summ, rid, opts: \
                self._post_perm(ts, summ, rid, opts)
            s.perm_update = self._perm_update
            s.start()
        except Exception as e:
            if s is not None:
                s.terminate()
            write_state(wd, "failed", "", f"{type(e).__name__}: {e}")
            self._react_state(ts, "x")
            self.post(ts, L(f"启动 devin 失败: {type(e).__name__}: {e}", f"failed to start devin: {type(e).__name__}: {e}"))
            with self.lock:
                self.starting.discard(ts)
                self.starting_auto.discard(ts)
            self._pump_queue()
            return
        with self.lock:
            s.auto = ts in self.starting_auto
            self.sessions[ts] = s
            self.starting.discard(ts)
            self.starting_auto.discard(ts)
            self.crash_retries.pop(ts, None)
        s.status_ts = ack_ts
        threading.Thread(target=self._goal_watch, args=(ts, s),
                         daemon=True).start()
        self._board_touch()
        self._run_prompt(ts, text, files)

    def _resume_session(self, root, text, files, session_id,
                        msg_ts=None, ack_post=True, replayed=False):
        with self.lock:
            if root in self.sessions or root in self.starting:
                return
            self.starting.add(root)
        wd = self._workdir(root)
        s = None
        ack_ts = None
        try:
            # The terminal-finality guard in write_state protects the
            # recorded outcome; resume deliberately starts fresh on this
            # workdir, so the old record must go before the session's
            # own writes begin.
            with _file_lock:
                try:
                    os.unlink(_state_path(wd))
                except OSError:
                    pass
            if ack_post:
                ack_ts = self.post(root, L("已接单，正在恢复旧会话…", "accepted, restoring previous session…"))
            s = DevinSession(root, wd, self.acp_cmd, self.timeout)
            s.notify = lambda msg: self.post(root, msg)
            s.on_text = lambda: self._draft_tick(root)
            s.notify_perm = lambda summ, rid, opts: \
                self._post_perm(root, summ, rid, opts)
            s.perm_update = self._perm_update
            s.start(resume_id=session_id)
        except Exception as e:
            if s is not None:
                s.terminate()
            write_state(wd, "failed", "", f"{type(e).__name__}: {e}",
                        extra={"session_id": session_id})
            self._react_state(root, "x")
            hint = _stderr_hint(wd)
            self.post(root, L(f"恢复会话失败: {type(e).__name__}: {e}", f"failed to restore session: {type(e).__name__}: {e}")
                            + (f"（acp: {hint}）" if hint else ""))
            with self.lock:
                self.starting.discard(root)
            self._pump_queue()
            return
        with self.lock:
            self.sessions[root] = s
            self.starting.discard(root)
        threading.Thread(target=self._goal_watch, args=(root, s),
                         daemon=True).start()
        # One receipt per resume: the ack is updated in place to the
        # outcome, so a clean restore leaves exactly one message.
        outcome = (L("已恢复旧会话，继续处理。", "restored the previous session, continuing.") if s.resumed else
                   L("旧会话无法恢复（可能已被清理），已开新会话继续。", "old session couldn't be restored (likely cleaned); started a fresh one."))
        if not (ack_ts and self._upd(ack_ts, outcome)):
            self.post(root, outcome)
        self._board_touch()
        self._run_prompt(root, text, files, msg_ts=msg_ts,
                         goal_replay=replayed)

    def _stop(self, root):
        self.governor.pop(root, "stopped")
        self.limit_attempts.pop(root, None)
        s = self.sessions.get(root)
        if not s:
            with self.lock:
                hit = next((q for q in self.dispatch_queue
                            if q[0] == root), None)
                if hit:
                    self.dispatch_queue.remove(hit)
            if hit:
                write_state(self._workdir(root), "stopped", "",
                            L("排队中取消", "cancel while queued"))
                self.post(root, L("已取消排队，本任务不会执行。", "queue cancelled; this task won't run."))
                return
            if root in self.starting:
                self.post(root, L("会话正在启动，请稍候再发停止。", "session is starting, try stop again in a moment."))
                return
            st = read_state(self._workdir(root))
            tail = L(f"（上次：{fmt_state(st)}）", f"(last: {fmt_state(st)})") if st else ""
            self.post(root, L(f"该线程没有运行中的会话{tail}。", f"no running session on this thread{tail}."))
            return
        flag = self.stop_flags.setdefault(root, threading.Event())
        flag.set()
        had_goal, _ = self._goal_clear_full(s)
        gnote = L("（目标已一并清除）", "(goal cleared too)") if had_goal else ""
        if not s.running:
            dead = s.terminate()
            write_state(s.workdir, "stopped", s.last_activity,
                        L("空闲时收到停止", "stop received while idle"))
            self._remove(root)
            self._finish(root, s, "octagonal_sign", L("🛑 已停止", "🛑 Stopped"))
            self.post(root, L("会话已停止。", "session stopped.") + gnote +
                      ("" if dead else L("进程退出未确认，请检查。", "process exit unconfirmed, please check.")))
        else:
            self.post(root, L("正在停止…", "stopping…") + gnote)
        self._pump_queue()

    def _goal_clear_rpc(self, s):
        """Best-effort goal drop: notify the acp, wipe local goal
        state and pending goal files so neither the drive loop nor
        the file watcher can resurrect it."""
        if s.alive and getattr(s, "session_id", None):
            params = {"sessionId": s.session_id}
            gid = (s.goal or {}).get("id")
            if gid is not None:
                params["goalId"] = gid
            try:
                s._send({"jsonrpc": "2.0",
                         "method": "_cognition.ai/goal/clear",
                         "params": params})
            except AcpExited:
                pass
        s.goal = None
        _goal_mark_cleared(s.workdir)
        for name in GOAL_FILES:
            for p in (os.path.join(s.workdir, name),
                      os.path.join(s.workdir, name + ".attached")):
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def _goal_clear_full(self, s):
        """Clear whatever goal the acp holds, not just the cached
        one. While a turn runs, the prompt loop owns the reader and
        already polls goal state, so we skip the fetch (its
        _wait_result could steal the turn's result line) and clear
        with what we know."""
        if not s.goal and not s.running and s.alive and \
                getattr(s, "session_id", None):
            try:
                self._goal_fetch(s, wait=1.5)
            except Exception:
                pass
        had = bool(s.goal)
        self._goal_clear_rpc(s)
        # 'cancel' reads to users as 'stop the goal now': if the
        # running turn is the goal prompt itself, cancel it too.
        # Queued messages and non-goal turns are left alone.
        rec = s.active_turn
        killed = False
        if s.running and rec and \
                (rec.get("text") or "").lstrip().startswith("/goal"):
            try:
                s._send({"jsonrpc": "2.0", "method": "session/cancel",
                         "params": {"sessionId": s.session_id}})
                s.stopping = True
                s._close_pending("goal_cancel")
                killed = True
            except Exception:
                pass
        return had, killed

    def _goal_clear_cmd(self, root):
        s = self.sessions.get(root)
        if s:
            had, killed = self._goal_clear_full(s)
            if had and killed:
                self.post(root, L("已清除目标，正在跑的目标回合并已叫停。", "goal cleared; the running goal turn was also stopped."))
            elif had:
                self.post(root, L("已清除目标，自动续跑不会再拉起。", "goal cleared; auto-resume won't bring it back."))
            elif killed:
                self.post(root, L("目标已不在了；", "goal is gone;") +
                                L("正在跑的目标回合也已叫停。", "the running goal turn was also stopped."))
            else:
                self.post(root, L("该会话当前没有挂着的目标。", "this session has no attached goal."))
            return
        removed = False
        for name in GOAL_FILES:
            for p in (os.path.join(self._workdir(root), name),
                      os.path.join(self._workdir(root),
                                   name + ".attached")):
                try:
                    os.unlink(p)
                    removed = True
                except OSError:
                    pass
        _goal_mark_cleared(self._workdir(root))
        self.post(root, L("已清除待挂的目标文件。", "cleared the pending goal file.") if removed
                  else L("该线程没有活动会话，也没有目标文件。", "no active session and no goal file on this thread."))

    def _pending_hint(self, root):
        records, _ = load_approvals(self._workdir(root))
        n = sum(1 for r in records if r.get("status") == "pending")
        return L(f"，待审批 {n} 项", f", {n} pending approval(s)") if n else ""

    def _status(self, root):
        s = self.sessions.get(root)
        if s:
            ago = int(time.monotonic() - s.last_activity_at)
            if s.stopping:
                state = L("正在停止", "stopping")
            elif s.running:
                state = L("执行中", "running")
            else:
                state = L("等待中（空闲）", "waiting (idle)")
            q = L(f"，排队 {s.queued} 条", f", {s.queued} queued") if s.queued else ""
            info = " · ".join(x for x in (s.model_name, s.mode_name)
                              if x)
            sid = getattr(s, "session_id", None)
            self.post(root, L(f"状态: {state}，最近活动: ", f"status: {state}, last activity: ") +
                            L(f"{s.last_activity}（{ago} 秒前）{q}", f"{s.last_activity} ({ago}s ago){q}") +
                            f"{self._pending_hint(root)}"
                            + (f"，{info}" if info else ""))
            if sid:
                self.post(root, L(f"会话 `{sid}`——桌面 `devin -r {sid}` ", f"session `{sid}` — `devin -r {sid}` on desktop ") +
                                L("可接管；`!handoff` 看详情。", "can be adopted; `!handoff` for details."))
            if s.goal:
                g = s.goal
                self.post(root, L(f"🎯 目标: ", f"🎯 goal: ") +
                                f"{(g.get('objective') or '')[:60]} "
                                f"（{GOAL_STATUS_CN.get(g.get('status'), '?')}"
                                f" · turn {g.get('turns', '?')}）")
            parked = self.governor.get(root)
            if parked:
                wait = int(parked.get("resume_at", 0) - time.time())
                self.post(root, L("⏸️ 限额冷却中：", "⏸️ Rate-limit cooldown:")
                                + (L(f"{wait}s 后", f"in {wait}s") if wait > 0 else L("即将", "soon"))
                                + L(f"自动重试（第 ", f"auto-retry (#") +
                                  L(f"{parked.get('attempt', '?')} 次）。", f"{parked.get('attempt', '?')})."))
            return
        if root in self.starting:
            self.post(root, L("状态: 启动中", "status: starting"))
            return
        st = read_state(self._workdir(root))
        self.post(root, (L(f"状态: {fmt_state(st)}", f"status: {fmt_state(st)}") if st
                         else L("状态: 未知（本服务无该线程记录）", "status: unknown (no record for this thread)"))
                  + self._pending_hint(root))

    def _decide(self, root, allow, ev):
        s = self.sessions.get(root)
        who = ev.get("user") or ev.get("bot_id")
        try:
            res = s.user_decide(allow, who) if s else None
        except AcpExited:
            self.post(root, L("审批应答发送失败：会话进程已退出。", "approval reply failed: session process exited."))
            return
        if not res:
            self.post(root, L("当前没有待审批的请求。", "no pending approval requests."))
            return
        rid, resolution = res
        self.post(root, L(f"已{'批准' if allow else '回绝'} ", f"{'approved' if allow else 'denied'} ") +
                        f"{rid}（{resolution}）。")

    def _approvals(self, root):
        """Read-only ledger view for one thread. Files only; the session
        process is never touched, so this cannot wake or interrupt it."""
        wd = self._workdir(root)
        st = read_state(wd)
        records, ledger = load_approvals(wd)
        if st is None and ledger == "missing" and root not in self.sessions:
            self.post(root, L("该线程无会话与审批记录。", "no session or approval records on this thread."))
            return
        live = root in self.sessions or \
            (bool(st) and st.get("state") not in TERMINAL)
        stale_sec = self.timeout + CANCEL_GRACE + 60
        now = time.time()
        lines = []
        pend = [r for r in records if r.get("status") == "pending"]
        fresh = [r for r in pend
                 if live and now - r.get("requested_at", 0) <= stale_sec]
        stale = [r for r in pend if r not in fresh]
        if ledger == "corrupt":
            lines.append(L("审批台账损坏，无法确认是否有待审批。", "approval ledger corrupt; can't confirm pending approvals."))
        for r in fresh:
            waited = int(now - r.get("requested_at", now))
            lines.append(L(f"⏳ 待审批 {r.get('request_id')}: ", f"⏳ pending approval {r.get('request_id')}: ") +
                         L(f"{r.get('summary')}（已等待 {waited}s，阻塞中）", f"{r.get('summary')} (waiting {waited}s, blocked)"))
        if not fresh:
            lines.append(L("无待审批（台账已核验）。", "none pending (ledger verified).") if ledger == "ok"
                         else L("无待审批（无台账文件，即未记录到任何请求）。", "none pending (no ledger file — nothing was recorded)."))
        for r in stale:
            lines.append(L(f"⚠️ 待核实 {r.get('request_id')}: 记录停在待审批", f"⚠️ to verify {r.get('request_id')}: record stuck at pending") +
                         L("但会话已不活跃，决议未知。", "but the session is no longer active; resolution unknown."))
        done = [r for r in records
                if r.get("status") in ("resolved", "interrupted")]
        for r in [r for r in done if r.get("needs_user")][-3:]:
            lines.append(L(f"需人工 {r.get('request_id')}: {r.get('summary')}", f"needs human {r.get('request_id')}: {r.get('summary')}") +
                         f" → {r.get('resolution', '?')}")
        auto = [r for r in done if not r.get("needs_user")]
        if auto:
            r = auto[-1]
            lines.append(L(f"最近自动放行 {r.get('request_id')}: ", f"recent auto-allowed {r.get('request_id')}: ") +
                         f"{r.get('summary')} → {r.get('resolution', '?')}；" +
                         L(f"累计 {len(auto)} 项", f"{len(auto)} total"))
        lines.append(L("核验时间 ", "verified at ") + time.strftime("%H:%M:%S")
                     + L(f"，台账 {len(records)} 条。", f", ledger has {len(records)} records."))
        self.post(root, "\n".join(lines))

    def _bang(self, root, cmd, arg, ev):
        """Dispatch a recognized !-command; every branch posts a
        receipt so the user is never left guessing."""
        if cmd == "stop":
            self._stop(root)
        elif cmd == "status":
            self._status(root)
        elif cmd == "queue":
            self._queue_status(root)
        elif cmd == "reload":
            self.request_reload()
            self.post(root, L("收到，桥服务热重启（在跑的会话不中断）。", "got it — bridge hot-restarting (running sessions unaffected)."))
        elif cmd == "help":
            self.post(root, HELP_CARD)
        elif cmd == "allow":
            self._decide(root, True, ev)
        elif cmd == "deny":
            self._decide(root, False, ev)
        elif cmd == "approvals":
            self._approvals(root)
        elif cmd == "reset":
            self._reset(root)
        elif cmd == "sessions":
            self._sessions_list(root)
        elif cmd == "adopt":
            self._adopt(root, arg)
        elif cmd == "handoff":
            self._handoff(root)
        elif cmd == "release":
            self._release(root)
        elif cmd == "limits":
            self._limits(root, arg)
        elif cmd == "peek":
            self._peek(root, arg)
        elif cmd == "board":
            self._board_show(root, ev)
            self._board_refresh(manual=True)
        elif cmd == "files":
            self._files_list(root)
        elif cmd == "send":
            self._send_named(root, arg)
        elif cmd in ("pad", "shot", "type", "key"):
            if not PAD_ENABLED:
                self.post(root, L("本机没装 remote-pad，遥控命令不可用。",
                                  "remote-pad isn't installed on this host; "
                                  "remote-control commands are unavailable."))
            elif cmd == "pad":
                self._pad(root)
            elif cmd == "shot":
                threading.Thread(target=self._shot, args=(root,),
                                 daemon=True).start()
            elif cmd == "type":
                threading.Thread(target=self._inject_text,
                                 args=(root, arg), daemon=True).start()
            elif cmd == "key":
                threading.Thread(target=self._inject_key,
                                 args=(root, arg), daemon=True).start()

    def _queue_status(self, root):
        with self.lock:
            pos = next((i + 1 for i, q in
                        enumerate(self.dispatch_queue)
                        if q[0] == root), None)
            total = len(self.dispatch_queue)
        s = self.sessions.get(root)
        if pos:
            self.post(root, L(f"本任务在排队中（第 {pos}/{total} 位）；", f"this task is queued ({pos}/{total});") +
                            L("发 `!stop` 可取消排队。", "`!stop` cancels the queue."))
            return
        if s and s.queued:
            self.post(root, L(f"会话内还有 {s.queued} 条待处理消息。", f"{s.queued} more messages pending in this session."))
            return
        if total:
            self.post(root, L(f"本线程没有排队任务；全局队列 {total} 条。", f"no queued task on this thread; global queue has {total}."))
            return
        self.post(root, L("队列是空的。", "queue is empty."))

    def _reset(self, root):
        """Drop the session binding; the next in-thread reply starts a
        brand-new session on the same workdir."""
        self.governor.pop(root, "reset")
        self.limit_attempts.pop(root, None)
        s = self.sessions.get(root)
        if s and (s.running or s.queued):
            self.post(root, L("会话还在执行，先 `!stop` 再 `!reset`。", "session still running — `!stop` first, then `!reset`."))
            return
        wd = self._workdir(root)
        if s:
            dead = s.terminate()
            with self.lock:
                self.sessions.pop(root, None)
                self.stop_flags.pop(root, None)
            self._finish(root, s, "x", L("🛑 会话被 !reset 重置", "🛑 Session reset by !reset"))
            if not dead:
                self.post(root, L("警告：进程退出未确认。", "warning: process exit unconfirmed."))
        for name in GOAL_FILES:
            for p in (os.path.join(wd, name),
                      os.path.join(wd, name + ".attached")):
                try:
                    os.unlink(p)
                except OSError:
                    pass
        # The "reset" state is the in-thread restart marker:
        # handle_event turns the next reply into a fresh session even
        # across a bridge restart.
        write_state(wd, "reset", "", L("用户重置；下条回复开新会话", "reset by user; next reply starts a new session"))
        self.post(root, L("已重置：下次在本线程发言将开启全新会话", "reset: your next message in this thread starts a fresh session") +
                        L("（旧上下文不保留）。", "(previous context is not kept)."))

    # ---- cross-device session interop -----------------------------
    # devin sessions are global (sessions.db): one id resumes anywhere —
    # `devin -r <id>` on the CLI, the Desktop session list, or an ACP
    # session/load. These commands bind/unbind that global session to a
    # Slack thread so either side can take over the other's work.

    def _sessions_list(self, root):
        live = {s.session_id: ts for ts, s in self.sessions.items()
                if getattr(s, "session_id", None)}
        rows = list_devin_sessions(15)
        if not rows:
            self.post(root, L("读不到本机 Devin 会话库（", "can't read the machine's Devin session store (") +
                            f"{DEVIN_DB}）。")
            return
        now = time.time()
        lines = [L("*本机 Devin 会话*（新→旧；`!adopt <id>` 接管到本线程）", "*Devin sessions on this machine* (newest first; `!adopt <id>` to pull one into this thread)")]
        for r in rows:
            sid = r["id"]
            ago = int((now - r["activity"]) / 60)
            age = f"{ago}m" if ago < 60 else f"{ago // 60}h"
            title = _first_line(r["title"], 28)
            if sid in live:
                mark = L("▶ 桥接管中", "▶ Bridged here") + (L("（本线程）", "(this thread)")
                                       if live[sid] == root else "")
            else:
                if self._turn_live(sid):
                    mark = "🔥 " + (self._last_action(sid) or L("在跑", "running"))
                elif r["lock_pid"]:
                    mark = L("🔒 桌面/CLI 打开中", "🔒 Open on Desktop/CLI")
                else:
                    mark = L("💤 空闲", "💤 Idle")
            lines.append(L(f"• `{sid}` {mark} {age}前 — {title}", f"• `{sid}` {mark} {age} ago — {title}"))
        lines.append(L("桌面接管桥会话：`devin -r <id>` 或 Desktop 会话列表；", "Desktop takes over a bridged session via `devin -r <id>` or the session list;") +
                     L("桥会话同在此库中。", "bridged sessions live in the same store."))
        self.post(root, "\n".join(lines))

    def _adopt(self, root, arg):
        sid = (arg or "").strip().split()[0] if arg else ""
        if not sid:
            # Mobile-friendly: bare `!adopt` grabs the most recent
            # UNLOCKED session not already bound to a thread. Stealing a
            # live lock needs the explicit `!adopt <id>` form — the
            # newest session is very often the one the user is still
            # typing into somewhere else.
            bound = {getattr(s, "session_id", None)
                     for s in self.sessions.values()}
            rows = [r for r in list_devin_sessions(15)
                    if r["id"] not in bound]
            cand = next((r for r in rows if not r["lock_pid"]), None)
            if not cand:
                hint = (L(f"最近的会话都被占着（🔒）：点名接管 ", f"recent sessions are all held (🔒): adopt by name ") +
                        f"`!adopt {rows[0]['id']}`" if rows else
                        L("没有可接管的会话", "no adoptable sessions"))
                self.post(root, hint + L("（`!sessions` 看全表）。", "(`!sessions` for the full list)."))
                return
            row = cand
            sid = row["id"]
        else:
            row = devin_session(sid)
            if not row:
                self.post(root, L(f"找不到会话 `{sid}`", f"can't find session `{sid}`") +
                                L("（`!sessions` 看可用 id）。", "(`!sessions` for available ids)."))
                return
        s = self.sessions.get(root)
        if s and (s.running or s.queued):
            self.post(root, L("本线程会话还在执行，先 `!stop`。", "this thread's session is still running — `!stop` first."))
            return
        if s and s.session_id == sid:
            self.post(root, L(f"`{sid}` 已经是本线程的会话。", f"`{sid}` is already this thread's session."))
            return
        if s:
            s.terminate()
            with self.lock:
                self.sessions.pop(root, None)
                self.stop_flags.pop(root, None)
        self._react(root, "eyes")
        threading.Thread(target=self._adopt_session,
                         args=(root, row), daemon=True).start()

    def _adopt_session(self, root, row):
        sid = row["id"]
        with self.lock:
            if root in self.sessions or root in self.starting:
                return
            self.starting.add(root)
        wd = self._workdir(root)
        self._seed_workdir(wd, root)
        with _file_lock:
            try:
                os.unlink(_state_path(wd))
            except OSError:
                pass
        ack_ts = self.post(root, L(f"正在接管会话 `{sid}`…", f"adopting session `{sid}`…"))
        # A live lock means another devin frontend (Desktop tab, `devin
        # -r`) has it open — acp refuses session/load outright. Taking
        # over = taking the lock: removing the file lets our acp claim
        # it. The other side keeps its stale view; whoever prompts next
        # owns the turn, same as two `devin -r` instances racing.
        locked = row["lock_pid"]
        if locked:
            try:
                os.unlink(os.path.join(DEVIN_LOCKS, sid + ".lock"))
            except OSError:
                pass
        acp_cwd = row["workdir"]
        gone = acp_cwd and not os.path.isdir(acp_cwd)
        s = None
        try:
            s = DevinSession(root, wd, self.acp_cmd, self.timeout,
                             acp_cwd=None if gone else acp_cwd or None)
            s.notify = lambda msg: self.post(root, msg)
            s.on_text = lambda: self._draft_tick(root)
            s.notify_perm = lambda summ, rid, opts: \
                self._post_perm(root, summ, rid, opts)
            s.perm_update = self._perm_update
            s.start(resume_id=sid, strict_load=True)
        except Exception as e:
            if s is not None:
                s.terminate()
            write_state(wd, "failed", "", f"{type(e).__name__}: {e}",
                        extra={"session_id": sid})
            self._react_state(root, "x")
            err = getattr(e, "args", [None])[0] or {}
            hint = _stderr_hint(wd)
            if "already open" in str(err):
                self.post(root, L(f"⛔ `{sid}` 还被桌面/CLI 抓着，抢锁失败。", f"⛔ `{sid}` is still held by Desktop/CLI; couldn't take the lock.") +
                                L("先去那边关掉这个会话（Desktop 关闭 tab，", "close this session on that side first (close the Desktop tab,") +
                                L("或退出 `devin -r`），再 `!adopt`。", "or quit `devin -r`), then `!adopt`."))
            else:
                self.post(root, L(f"接管失败: {type(e).__name__}: {e}", f"adopt failed: {type(e).__name__}: {e}")
                                + (f"（acp: {hint}）" if hint else ""))
            with self.lock:
                self.starting.discard(root)
            return
        with self.lock:
            self.sessions[root] = s
            self.starting.discard(root)
        s.status_ts = ack_ts
        threading.Thread(target=self._goal_watch, args=(root, s),
                         daemon=True).start()
        title = _first_line(row["title"])
        if gone:
            self.post(root, L(f"⚠️ 原目录 `{acp_cwd}` 已不存在，", f"⚠️ original dir `{acp_cwd}` no longer exists;") +
                            L("会话锚在本线程目录。", "session anchored to this thread's directory."))
        if s.resumed:
            stole = (L(f"（从桌面/CLI 手里抢过来的，那边还开着，", f"(taken over from Desktop/CLI, which is still open —") +
                     L("两边别同时发）", "don't drive both sides at once)") if locked else "")
            outcome = (L(f"✅ 已接管 `{sid}`（{title}）{stole}——", f"✅ adopted `{sid}` ({title}){stole} —") +
                       L(f"完整上下文在。项目目录 `{s.acp_cwd}`；", f"full context intact. Project dir `{s.acp_cwd}`;") +
                       L("直接回复继续，`!release` 交还。", "reply to continue, `!release` to hand it back."))
        else:
            outcome = (L(f"⚠️ `{sid}` 加载失败（可能已被清理），已在 ", f"⚠️ `{sid}` failed to load (maybe cleaned); started a new session in ") +
                       f"`{s.acp_cwd}` 开新会话顶替。")
        if not (ack_ts and self._upd(ack_ts, outcome)):
            self.post(root, outcome)
        self._board_touch()

    def _peek(self, root, arg):
        """`!peek [id]` — spectate a session's recent actions without
        touching its lock. Bare form peeks at this thread's session."""
        sid = (arg or "").strip()
        if not sid:
            s = self.sessions.get(root)
            sid = getattr(s, "session_id", None) if s else None
            if not sid:
                self.post(root, L("`!peek <会话id>` 看它在干嘛（不动锁）;", "`!peek <session-id>` to see what it's doing (no lock);") +
                                L("`!sessions` 拿全表。", "`!sessions` for the full list."))
                return
        if not devin_session(sid):
            self.post(root, L(f"找不到会话 `{sid}`（`!sessions` 看可用 id）。", f"can't find session `{sid}` (`!sessions` for available ids)."))
            return
        self.post(root, self._peek_text(sid))

    def _handoff(self, root):
        """Surface this thread's session id so a human can pick the
        same conversation up in the CLI (`devin -r`) or the Desktop
        session list. Non-destructive: the bridge keeps the binding;
        whoever prompts next owns the turn."""
        s = self.sessions.get(root)
        sid = getattr(s, "session_id", None)
        if not sid:
            st = read_state(self._workdir(root))
            sid = (st or {}).get("session_id")
        if not sid:
            self.post(root, L("本线程还没有会话可移交。", "no session on this thread to hand off."))
            return
        row = devin_session(sid) or {}
        title = _first_line(row.get("title"))
        lines = [L(f"会话 `{sid}`", f"session `{sid}`")
                 + (f"（{title}）" if title else "")]
        lines.append(L("它在本机全局会话库，Desktop 会话列表里能直接看到。", "It's in the machine-wide session store — visible in Desktop's session list."))
        if root in self.sessions:
            lines.append(L("桥的 acp 正持有它的锁——先发 `!release` 松手，", "the bridge's acp holds its lock — `!release` first,") +
                         L(f"再 `devin -r {sid}` 或 Desktop 里打开。", f"then `devin -r {sid}` or open it in Desktop."))
        else:
            lines.append(L(f"桌面 `devin -r {sid}` 直接接管；", f"`devin -r {sid}` on desktop takes it over;") +
                         L("本线程 `!adopt` 也能接回。", "`!adopt` in this thread also brings it back."))
        self.post(root, "\n".join(lines))

    def _release(self, root):
        """Drop the bridge's process+binding without wiping context:
        the global session stays in the store, resumable via Desktop,
        `devin -r`, or a later `!adopt`."""
        s = self.sessions.get(root)
        sid = getattr(s, "session_id", None) if s else None
        if s and (s.running or s.queued):
            self.post(root, L("会话还在执行，先 `!stop` 再 `!release`。", "session still running — `!stop` first, then `!release`."))
            return
        if not s:
            self.post(root, L("本线程没有桥接会话可释放。", "no bridged session on this thread to release."))
            return
        s.terminate()
        with self.lock:
            self.sessions.pop(root, None)
            self.stop_flags.pop(root, None)
        write_state(self._workdir(root), "released", "",
                    L(f"会话 {sid or '?'} 已释放，可在桌面接管", f"session {sid or '?'} released; can be adopted on desktop"),
                    extra={"session_id": sid})
        self._react_state(root, "hand")
        self.post(root, L(f"已释放 `{sid}`——上下文保留在本机会话库。", f"released `{sid}` — context kept in the machine session store.") +
                        L(f"桌面 `devin -r {sid}` 接管；本线程再发消息", f"`devin -r {sid}` on desktop takes over; message this thread again") +
                        L("或 `!adopt` 也能接回来。", "or `!adopt` to bring it back."))
        self._board_touch()

    # Phone-side remote control lives in ~/remote-pad (ydotoold +
    # wl-clipboard + ScreenCast portal). These commands are thin
    # wrappers so the same actions are reachable from a Slack thread.

    def _pad_ips(self):
        ips = []
        try:
            r = subprocess.run(["tailscale", "ip", "-4"],
                               capture_output=True, text=True, timeout=5)
            ips += r.stdout.split()[:1]
        except (OSError, subprocess.SubprocessError):
            pass
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("192.168.1.1", 80))
            ips.append(s.getsockname()[0])
            s.close()
        except OSError:
            pass
        return ips or ["127.0.0.1"]

    def _pad(self, root):
        try:
            tok = open(PAD_TOKEN).read().strip()
            ok = bool(re.fullmatch(r"[0-9a-f]{32}", tok))
        except OSError:
            ok = False
        if not ok:
            self.post(root, L("remote-pad 没就绪（缺 token）。", "remote-pad isn't ready (missing token)."))
            return
        urls = " ｜ ".join(f"http://{ip}:8877/t/{tok}/"
                          for ip in self._pad_ips())
        self.post(root,
                  L("*手机遥控这台电脑*\\n", "*Remote-control this computer from your phone*\n") +
                  L(f"• Web 控制台（打字/触控板/快捷键）：{urls}\\n", f"• Web console (typing/trackpad/shortcuts): {urls}\n") +
                  L("• 完整桌面 RDP：`3389` 已开（Windows App / Jump 等", "• Full-desktop RDP: `3389` is open (Windows App / Jump etc") +
                  L("客户端，账号密码=远程桌面设置的那组）\\n", "client; credentials are the remote-desktop pair)\n") +
                  L("• KDE Connect（手机键盘/触控板/剪贴板，推荐）：装 KDE ", "• KDE Connect (phone keyboard/trackpad/clipboard, recommended): install the KDE ") +
                  L("Connect app → 配对界面右上角 ⋯ → Add devices by IP → ", "Connect app → pairing screen top-right ⋯ → Add devices by IP → ") +
                  L(f"填 `{self._pad_ips()[0]}`；电脑上跑 `{PAD_DIR}/kdec.sh list`",
                    f"enter `{self._pad_ips()[0]}`; on the computer run `{PAD_DIR}/kdec.sh list`") +
                  L(" + `kdec.sh pair <id>` 确认配对\\n", " + `kdec.sh pair <id>` to confirm pairing\n") +
                  L("• `!type 文字` `!key ctrl+alt+t` `!shot` 可在本线程", "• `!type <text>` `!key ctrl+alt+t` `!shot` work in this thread") +
                  L("直接注入\\n", "to inject directly\n") +
                  L("手机要开着 Tailscale，或连家里同一个 Wi-Fi。", "phone needs Tailscale on, or the same home Wi-Fi."))

    def _shot(self, root):
        if not os.path.exists(SCREENGRAB):
            self.post(root, L("screengrab 还没装。", "screengrab isn't installed yet."))
            return
        try:
            r = subprocess.run([sys.executable, SCREENGRAB, "20"],
                               capture_output=True, timeout=35)
        except subprocess.TimeoutExpired:
            r = None
        png = r.stdout if r and r.returncode == 0 else b""
        if png:
            p = os.path.join(self._workdir(root), "shot.png")
            try:
                with open(p, "wb") as f:
                    f.write(png)
                if self.upload_msg:
                    self.upload_msg(root, p)
                    self._react(root, "camera_with_flash")
                    return
            except Exception as e:
                self._log_err("shot upload", e)
        why = ((r.stderr if r else b"") or b"")
        why = why.decode("utf-8", "replace")
        self.post(root, L("📷 暂时截不到桌面：", "📷 Can't capture the desktop right now:") + _one_line(why, 110)
                        + L("（GNOME 只放行「本机点过共享」的抓屏；", "(GNOME only allows screen capture after a local share click;") +
                          L("看实时桌面走 RDP 客户端）", "use an RDP client for the live desktop)"))

    def _inject_text(self, root, arg):
        if not arg:
            self.post(root, L("`!type <文字>` 打进电脑当前焦点窗口", "`!type <text>` types into the computer's focused window") +
                            L("（中文自动转剪贴板粘贴）。", "(non-ASCII text goes through clipboard paste)."))
            return
        try:
            r = subprocess.run([sys.executable, INPUTCTL, "type",
                                arg[:2000]],
                               capture_output=True, text=True,
                               timeout=30)
            out = (r.stdout or r.stderr or "").strip()
        except (OSError, subprocess.SubprocessError) as e:
            out = f"{type(e).__name__}: {e}"
        self._react(root, "keyboard")
        self.post(root, L(f"⌨️ 已发送 {len(arg)} 字到电脑", f"⌨️ sent {len(arg)} chars to the computer")
                        + ("" if out in ("", "ok") else f"：{out[:80]}"))

    def _inject_key(self, root, arg):
        if not arg:
            self.post(root, L("`!key <组合>` 给电脑按键，", "`!key <combo>` presses keys on the computer,") +
                            L("如 `!key ctrl+alt+t`、`!key enter`。", "e.g. `!key ctrl+alt+t`, `!key enter`."))
            return
        try:
            r = subprocess.run([sys.executable, INPUTCTL, "key", arg],
                               capture_output=True, text=True,
                               timeout=15)
            out = (r.stdout or r.stderr or "").strip()
        except (OSError, subprocess.SubprocessError) as e:
            out = f"{type(e).__name__}: {e}"
        self._react(root, "keyboard")
        self.post(root, f"⌨️ `{arg}`"
                        + ("" if out in ("", "ok") else f"：{out[:80]}"))

    # ---- pinned channel dashboard ---------------------------------
    # One top-level message, pinned, updated in place. Active sessions
    # live inside threads so nothing re-orders the channel; this board
    # is the stable overview the user asked for instead of sessions
    # popping to the top on every update.

    def _board_path(self):
        return os.path.join(self.tasks_dir, ".bridge_board.json")

    def _board_load(self):
        try:
            with open(self._board_path()) as f:
                return json.load(f) or {}
        except (OSError, ValueError):
            return {}

    def _board_write(self, d):
        try:
            os.makedirs(self.tasks_dir, exist_ok=True)
            with open(self._board_path(), "w") as f:
                json.dump(d, f)
        except OSError:
            pass

    def _board_saved_ts(self):
        return self._board_load().get("ts")

    def _board_save_ts(self, ts):
        d = self._board_load()
        d["ts"] = ts
        self._board_write(d)

    def _dm_persist(self):
        d = self._board_load()
        d["dm"] = self._dm_boards
        d["users"] = sorted(self._dm_users)
        self._board_write(d)

    def _session_fresh(self, sid):
        """Newest transcript node ts — the honest 'is it working'
        signal: a live turn writes nodes continuously, an idle session
        stays stale no matter who holds the lock."""
        try:
            db = _devin_db()
            try:
                row = db.execute(
                    "SELECT MAX(created_at) FROM message_nodes"
                    " WHERE session_id=?", (sid,)).fetchone()
            finally:
                db.close()
            return row[0] if row and row[0] else None
        except Exception:
            return None

    def _tail_nodes(self, sid, n=6):
        """Newest n transcript nodes (oldest→newest) as
        (parsed_message, created_at) — the raw material for 'what is
        it doing right now' views."""
        try:
            db = _devin_db()
            try:
                rows = db.execute(
                    "SELECT chat_message, created_at FROM message_nodes"
                    " WHERE session_id=? ORDER BY node_id DESC LIMIT ?",
                    (sid, n)).fetchall()
            finally:
                db.close()
        except Exception:
            return []
        out = []
        for cm, ts in rows:
            try:
                out.append((json.loads(cm), ts))
            except (ValueError, TypeError):
                pass
        return out[::-1]

    @staticmethod
    def _node_snippet(m):
        """One transcript node → a phone-length 'what happened'
        line, e.g. L("跑 exec: docker ps", "ran exec: docker ps") or L("回: 已接管", "reply: adopted")."""
        role = m.get("role")
        content = (m.get("content") or "").strip()
        if role == "assistant":
            tcs = m.get("tool_calls") or []
            if tcs:
                tc = tcs[0]
                args = tc.get("arguments") or {}
                if not isinstance(args, dict):
                    args = {}
                arg = (args.get("command") or args.get("file_path")
                       or args.get("path") or args.get("query")
                       or next(iter(args.values()), ""))
                extra = f" +{len(tcs) - 1}" if len(tcs) > 1 else ""
                return L(f"跑 {tc.get('name', '?')}: ", f"ran {tc.get('name', '?')}: ") + \
                       f"{_one_line(arg, 50)}{extra}"
            if content:
                return L("回: ", "reply: ") + _one_line(content, 56)
            return L("思考中…", "thinking…")
        if role == "tool":
            # 'Output from command in shell xxx:' is transport noise —
            # the useful part is the output itself.
            body = re.sub(r"^Output from command in shell \S+:\s*",
                          "", content)
            body = re.sub(r"Exit code: \d+", "", body)
            return L("结果: ", "result: ") + _one_line(body or content, 56)
        if role == "user":
            return L("收到: ", "got: ") + _one_line(content, 56)
        return _one_line(content, 56)

    def _last_action(self, sid):
        rows = self._tail_nodes(sid, 1)
        return self._node_snippet(rows[-1][0]) if rows else ""

    def _turn_live(self, sid):
        """'Is a turn actually in flight?' — the honest running
        signal. Walk back past `system` context injections (they're
        appended when a frontend opens a session, not real work) to
        the newest real node:
          user / tool result / assistant+tool_calls → mid-turn; live
              while younger than BOARD_TURN_CAP (long commands write
              nothing for minutes)
          assistant final text → the turn ended; it only keeps 🔥
              for BOARD_LIVE_SEC (a reply may precede more work)
        """
        now = time.time()
        for m, ts in reversed(self._tail_nodes(sid, 8)):
            if (m.get("role") or "") == "system":
                continue
            age = now - ts
            if m.get("role") == "assistant" \
                    and not m.get("tool_calls"):
                return age < BOARD_LIVE_SEC
            return age < BOARD_TURN_CAP
        return False

    def _peek_text(self, sid):
        """Read-only 'what is it doing' view — safe to peek at a live
        session without stealing its lock."""
        row = devin_session(sid) or {}
        title = _first_line(row.get("title"), 40) or sid
        rows = self._tail_nodes(sid, 6)
        head = L(f"👀 *{title}* `{sid}` — 最近动态", f"👀 *{title}* `{sid}` — recent activity")
        if not rows:
            return head + L("：暂无记录。", ": no records.")
        now = time.time()
        lines = [head]
        for m, ts in rows:
            ago = max(0, int(now - (ts or now)))
            rel = (L("刚", "just now") if ago < 60 else L(f"{ago // 60}m前", f"{ago // 60}m ago")
                   if ago < 3600 else L(f"{ago // 3600}h前", f"{ago // 3600}h ago"))
            lines.append(f"· _{rel}_ {self._node_snippet(m)}")
        return "\n".join(lines)

    def _board_permalink(self, ts):
        if self.team_url:
            return (f"{self.team_url}/archives/{self.channel}"
                    f"/p{ts.replace('.', '')}?thread_ts={ts}"
                    f"&cid={self.channel}")
        return ("https://slack.com/app_redirect?channel="
                f"{self.channel}&message={ts}")

    def _board_rows(self):
        """Shared data for text + block renders. Rows are what the
        user scans on a phone: title first, id demoted, state marks
        only where they carry information."""
        now = time.time()
        parked = {e.get("key"): e for e in self.governor.snapshot()}
        bound, hot, cool, idle = [], [], [], []
        for ts, s in self.sessions.items():
            sid = getattr(s, "session_id", None)
            if not sid:
                continue
            row = devin_session(sid) or {}
            title = _first_line(row.get("title"), 42)
            if s.running:
                mark, rank = "🔥", 0
            elif ts in parked or ("ext:" + str(sid)) in parked:
                e = parked.get(ts) or parked.get("ext:" + str(sid))
                wait = ""
                if e and e.get("resume_at"):
                    m = max(0, int((e["resume_at"] - now) / 60))
                    wait = L(f" ~{m}m后", f" ~{m}m")
                mark, rank = f"⏸{wait}", 1
            else:
                mark, rank = "💤", 2
            bound.append((rank, s.last_activity_at, sid, title, mark, ts))
        bound.sort(key=lambda x: (x[0], -x[1]))
        bound_ids = {b[2] for b in bound}
        for r in list_devin_sessions(30):
            sid = r["id"]
            if sid in bound_ids:
                continue
            fresh = self._session_fresh(sid)
            basis = fresh or r["activity"]
            ago = int((now - basis) / 60)
            age = (L("刚", "just now") if ago < 1 else f"{ago}m" if ago < 60
                   else f"{ago // 60}h" if ago < 1440
                   else f"{ago // 1440}d")
            title = _first_line(r["title"], 42)
            key = "ext:" + str(sid)
            if key in parked:
                e = parked[key]
                m = max(0, int(((e.get("resume_at") or now) - now) / 60))
                cool.append((basis, sid, title, L(f"⏸ ~{m}m后", f"⏸ ~{m}m"), age))
            elif self._turn_live(sid):
                hot.append((basis, sid, title, "🔥",
                            self._last_action(sid)))
            else:
                mark = "📂" if r["lock_pid"] else "💤"
                idle.append((basis, sid, title, mark, age))
        for lst in (hot, cool, idle):
            lst.sort(key=lambda x: -x[0])
        return bound, hot, cool, idle

    def _sys_vitals(self):
        """Host vitals straight from /proc — cpu% (delta between
        renders), mem%, disk% on /, net throughput B/s. No deps."""
        v = {}
        try:
            with open("/proc/stat") as f:
                vals = [int(x) for x in f.readline().split()[1:8]]
            idle, total = vals[3] + vals[4], sum(vals)
            if self._cpu_prev:
                dt, di = total - self._cpu_prev[0], \
                    idle - self._cpu_prev[1]
                if dt > 0:
                    v["cpu"] = round((1 - di / dt) * 100)
            self._cpu_prev = (total, idle)
        except (OSError, ValueError, IndexError):
            pass
        try:
            mi = {}
            with open("/proc/meminfo") as f:
                for line in f:
                    k, _, rest = line.partition(":")
                    mi[k.strip()] = int(rest.split()[0])
            t, a = mi["MemTotal"], mi.get("MemAvailable", 0)
            v["mem"] = round((1 - a / max(t, 1)) * 100)
            v["mem_gb"] = round((t - a) / 1048576, 1)
            v["mem_tot"] = round(t / 1048576)
        except (OSError, ValueError, KeyError):
            pass
        try:
            st = os.statvfs("/")
            v["disk"] = round((1 - st.f_bavail / st.f_blocks) * 100)
        except (OSError, ZeroDivisionError):
            pass
        try:
            rx = tx = 0
            with open("/proc/net/dev") as f:
                for line in f.readlines()[2:]:
                    if ":" not in line:
                        continue
                    name, rest = line.split(":", 1)
                    if name.strip() == "lo":
                        continue
                    cols = rest.split()
                    rx += int(cols[0])
                    tx += int(cols[8])
            now = time.monotonic()
            if self._net_prev and now > self._net_prev[0]:
                dt = now - self._net_prev[0]
                v["rx"] = (rx - self._net_prev[1]) / dt
                v["tx"] = (tx - self._net_prev[2]) / dt
            self._net_prev = (now, rx, tx)
        except (OSError, ValueError, IndexError):
            pass
        self._vitals = v
        return v

    @staticmethod
    def _fmt_rate(bps):
        for u in ("B", "K", "M", "G"):
            if bps < 1024 or u == "G":
                return f"{bps:.0f}{u}" if u == "B" \
                    else f"{bps:.1f}{u}"
            bps /= 1024

    def _vitals_line(self, v):
        """🖥 CPU 34% · mem 62% · ⚠️disk 91% · ⬇0.4M ⬆0.1M"""
        def tag(key, txt, warn):
            return f"⚠️{txt}" if v.get(key, 0) >= warn else txt
        parts = []
        if "cpu" in v:
            parts.append(tag("cpu", f"CPU {v['cpu']}%", VITAL_CPU))
        if "mem" in v:
            parts.append(tag("mem", L(f"内存 {v['mem']}%", f"mem {v['mem']}%"), VITAL_MEM))
        if "disk" in v:
            parts.append(tag("disk", L(f"磁盘 {v['disk']}%", f"disk {v['disk']}%"), VITAL_DISK))
        if "rx" in v:
            parts.append(f"⬇{self._fmt_rate(v['rx'])}/s "
                         f"⬆{self._fmt_rate(v['tx'])}/s")
        return "🖥 " + " · ".join(parts) if parts else ""

    def _board_render(self):
        vitals = self._sys_vitals()
        bound, hot, cool, idle = self._board_rows()
        lines = [L("📋 *会话看板* · 更新于 ", "📋 *Session Board* · updated ") + time.strftime("%m-%d %H:%M")]
        if bound:
            lines.append(L("*💬 桥接管中*", "*💬 Bridged sessions*"))
            for _, _, sid, title, mark, ts in bound[:6]:
                lines.append(f"• *{title}*\n  `{sid}` {mark} — " +
                         L(f"<{self._board_permalink(ts)}|进去聊>", f"<{self._board_permalink(ts)}|Open>"))
        if hot:
            lines.append(L(f"*🔥 在跑 ({len(hot)})*", f"*🔥 Running ({len(hot)})*"))
            for _, sid, title, mark, action in hot[:BOARD_MAX_ROWS]:
                lines.append(f"• *{title}*\n  `{sid}` {mark} " +
                         L(f"{action or '进行中'}", f"{action or 'working'}"))
            if len(hot) > BOARD_MAX_ROWS:
                lines.append(L(f"  …另 {len(hot) - BOARD_MAX_ROWS}", f"  …{len(hot) - BOARD_MAX_ROWS} more") +
                         L(" 个在跑（底部下拉可接管）", " more running (adopt from the menu below)"))
        if cool:
            lines.append(L("*⏸ 限额冷却*", "*⏸ Rate-limit cooldown*"))
            for _, sid, title, mark, age in cool[:4]:
                lines.append(f"• *{title}*\n  `{sid}` {mark}")
        opened = [i for i in idle if i[3] == "📂"]
        asleep = [i for i in idle if i[3] == "💤"]
        if opened:
            names = " · ".join(f"`{i[1]}`" for i in opened[:4])
            more = f" +{len(opened) - 4}" if len(opened) > 4 else ""
            lines.append(L(f"📂 桌面开着没跑({len(opened)}):{names}{more}", f"📂 open-not-running({len(opened)}):{names}{more}"))
        if asleep:
            names = " · ".join(f"`{i[1]}`" for i in asleep[:4])
            more = f" +{len(asleep) - 4}" if len(asleep) > 4 else ""
            lines.append(L(f"💤 空闲({len(asleep)}):{names}{more}", f"💤 idle({len(asleep)}):{names}{more}"))
        if not bound and not hot and not cool and not idle:
            lines.append(L("暂无会话。", "no sessions yet."))
        vline = self._vitals_line(vitals)
        if vline:
            lines.append(vline)
        lines.append(L("_🔥干活中 ⏸限额冷却 📂被打开没跑 · 下拉接管任意会话_", "_🔥 running ⏸ cooling down 📂 open-not-running · adopt any session below_"))
        text = "\n".join(lines)

        def sec(txt, btn=None):
            b = {"type": "section",
                 "text": {"type": "mrkdwn", "text": txt}}
            if btn:
                b["accessory"] = btn
            return b

        def row_actions(sid):
            """[看看] peeks without stealing (ephemeral); [接管] takes
            over into a fresh thread."""
            return {"type": "actions", "elements": [
                {"type": "button", "action_id": "board_peek",
                 "text": {"type": "plain_text", "text": L("看看", "Peek")},
                 "value": sid},
                {"type": "button", "action_id": "board_adopt",
                 "text": {"type": "plain_text", "text": L("接管", "Adopt")},
                 "value": sid}]}

        blocks = [sec(lines[0])]
        for _, _, sid, title, mark, ts in bound[:6]:
            blocks.append(sec(
                f"*{title}*\n`{sid}` {mark}",
                {"type": "button",
                 "text": {"type": "plain_text", "text": L("进去聊", "Open")},
                 "url": self._board_permalink(ts)}))
        for _, sid, title, mark, action in hot[:BOARD_MAX_ROWS]:
            blocks.append(sec(f"🔥 *{title}*\n`{sid}` · " +
                              L(f"_{action or '进行中'}_", f"_{action or 'working'}_")))
            blocks.append(row_actions(sid))
        for _, sid, title, mark, age in cool[:4]:
            blocks.append(sec(f"*{title}*\n`{sid}` {mark}"))
            blocks.append(row_actions(sid))
        for line in lines[1:]:
            if line.startswith(("📂", "💤", "🖥")):
                blocks.append({"type": "context", "elements": [
                    {"type": "mrkdwn", "text": line}]})
        # The adopt launcher: one dropdown = take over any unbound
        # session — no typing session ids on a phone.
        pickable = ([i for i in idle] + hot[BOARD_MAX_ROWS:] + cool[4:])[:20]
        if pickable:
            blocks.append({"type": "actions", "elements": [
                {"type": "static_select", "action_id": "board_pick",
                 "placeholder": {"type": "plain_text",
                                 "text": L("接管其他会话…", "Adopt another session…")},
                 "options": [
                     {"text": {"type": "plain_text",
                               "text": f"{i[3]} {i[2][:44]} · {i[1]}"},
                     "value": i[1]} for i in pickable]},
                {"type": "button", "action_id": "board_reload",
                 "text": {"type": "plain_text", "text": "🔄"}}]})
        else:
            blocks.append({"type": "actions", "elements": [
                {"type": "button", "action_id": "board_reload",
                 "text": {"type": "plain_text", "text": L("🔄 刷新", "🔄 Refresh")}}]})
        return text, blocks

    def _board_touch(self):
        """Cheap trigger after session lifecycle events; real refresh
        is throttled so bursts of activity don't spam chat.update."""
        if self.pin_add is None:
            return
        if time.monotonic() - self._board_at < BOARD_GAP:
            return
        threading.Thread(target=self._board_refresh,
                         daemon=True).start()

    def _board_refresh(self, manual=False, reply_to=None):
        with self._board_lock:
            self._board_at = time.monotonic()
            text, blocks = self._board_render()
            ts = self._board_saved_ts()
            # Periodic touches re-render constantly; only hit the API
            # when the content actually changed, or the board message
            # itself is missing (repost + repin).
            if ts and text == self._board_text:
                updated = True
            else:
                updated = bool(ts and self.update_msg
                               and self._upd(ts, text, blocks))
                if not updated:
                    ts = self.post(None, text, blocks)
                    if ts:
                        self._board_save_ts(ts)
                        if self.pin_add:
                            try:
                                self.pin_add(ts)
                            except Exception as e:
                                if _slack_err(e) != "already_pinned":
                                    self._log_err("pin_add", e)
                if updated or ts:
                    self._board_text = text
                self._board_bookmark(ts)
            self._dm_sync(text, blocks)
            self._canvas_sync(text)
            self._autoclean(self._vitals)
            self._audit_check()
        if reply_to:
            self.post(reply_to, L("看板已刷新（频道置顶那条 📋）。", "board refreshed (the pinned 📋 one in the channel)."))

    def _board_adopt(self, sid):
        """Board button/dropdown adopt: spawn a fresh anchor so the
        takeover lands in its own thread — one tap, zero typing."""
        row = devin_session(sid)
        if not row:
            self.post(None, L(f"`{sid}` 已不在会话库（可能刚被清理）。", f"`{sid}` is gone from the session store (maybe just cleaned)."))
            return
        bound = {ts for ts, s in self.sessions.items()
                 if getattr(s, "session_id", None) == sid}
        if bound:
            self.post(next(iter(bound)),
                      L("这个会话已绑在本线程——直接回复继续。", "this session is bound to this thread — just reply to continue."))
            return
        anchor = self.post(
            None, L(f"📥 接管 `{sid}` —— {_first_line(row.get('title'), 40)}", f"📥 Adopt `{sid}` — {_first_line(row.get('title'), 40)}"))
        if anchor:
            self._adopt(anchor, sid)

    def _board_show(self, root, ev):
        """!board — an ephemeral snapshot right where the user typed;
        no scrolling to the pin. The pinned board still refreshes
        via _board_refresh alongside."""
        who = (ev or {}).get("user")
        if self.ephemeral_msg and who:
            text, blocks = self._board_render()
            try:
                self.ephemeral_msg(channel=self.channel, user=who,
                                   text=text, blocks=blocks)
                return
            except Exception as e:
                self._log_err("board ephemeral", e)
        self.post(root, L("看板在频道置顶那条 📋——和机器人的私聊里也", "the board is the pinned 📋 message in the channel —") +
                        L("有一份实时同步的，手机上最顺手。", "there's a synced copy in the bot DM — handiest on mobile."))

    def _board_bookmark(self, ts):
        """Keep a channel header bookmark aimed at the pinned board —
        desktop-only surface; the DM mirror is the mobile answer."""
        if not (self.bookmarks and self.bookmark_add
                and self.permalink and ts):
            return
        if getattr(self, "_board_bookmarked", None) == ts:
            return
        try:
            link = self.permalink(ts)
            hit = next((b for b in self.bookmarks()
                        if L("看板", "board") in (b.get("title") or "")), None)
            if hit and hit.get("link") != link \
                    and self.bookmark_edit:
                self.bookmark_edit(hit["id"], link)
            elif not hit:
                self.bookmark_add(L("📋 会话看板", "📋 Session Board"), link)
            self._board_bookmarked = ts
        except Exception as e:
            self._log_err("board bookmark", e)

    def _dm_seen(self, user):
        """Any real user who talks to (or clicks) the bridge gets a DM
        board: one message in their bot DM, kept current by _dm_sync.
        Skips the bot itself and Slack's own plumbing users."""
        if not user or user in ("USLACKBOT", self.own_user_id) \
                or user in self._dm_users:
            return
        self._dm_users.add(user)
        self._dm_persist()
        try:
            self._dm_sync(*self._board_render())
        except Exception as e:
            self._log_err("dm board first paint", e)

    def _dm_sync(self, text, blocks):
        """Mirror the channel board into every known user's bot DM.
        First contact posts (capturing the DM channel id); refreshes
        update in place; a dead message is reposted on the next pass."""
        if not (self.dm_post and self.dm_update and self._dm_users):
            return
        changed = False
        for uid in list(self._dm_users):
            ent = self._dm_boards.get(uid) or {}
            try:
                if ent.get("channel") and ent.get("ts"):
                    try:
                        self.dm_update(ent["channel"], ent["ts"],
                                       text, blocks)
                        continue
                    except Exception as e:
                        self._log_err("dm board update", e)
                r = self.dm_post(uid, text, blocks)
                if r and r.get("channel") and r.get("ts"):
                    self._dm_boards[uid] = {"channel": r["channel"],
                                            "ts": r["ts"]}
                    changed = True
            except Exception as e:
                self._log_err("dm board post", e)
                self._dm_boards.pop(uid, None)
                changed = True
        if changed:
            self._dm_persist()

    def _autoclean(self, v):
        """A metric over threshold for CLEAN_STRIKES consecutive board
        samples spawns a 🧹 cleanup session (auto slot, cooldown)."""
        if not (AUTOCLEAN and v):
            return
        checks = (("disk", VITAL_DISK), ("mem", VITAL_MEM),
                  ("cpu", VITAL_CPU))
        now = time.monotonic()
        for key, warn in checks:
            st = self._clean.setdefault(key, [0, 0.0])
            st[0] = st[0] + 1 if v.get(key, 0) >= warn else 0
            if st[0] < CLEAN_STRIKES \
                    or now - st[1] < CLEAN_COOLDOWN:
                continue
            st[1] = now
            st[0] = 0
            self._spawn_cleanup(key, v)

    def _spawn_cleanup(self, metric, v):
        label = {"disk": L("磁盘", "disk"), "mem": L("内存", "memory"), "cpu": "CPU"}[metric]
        val = v.get(metric)
        if self._auto_live() >= MAX_AUTO:
            return
        try:
            anchor = self.post(
                None, L(f"🧹 *自动清理*：{label} {val}% 超阈值，", f"🧹 *Auto-clean*: {label} {val}% over threshold,") +
                      L("已派会话诊断并安全释放。", "spawned a session to diagnose and safely free it."))
        except Exception as e:
            self._log_err("cleanup anchor", e)
            return
        if not anchor:
            return
        prompt = CLEAN_PROMPTS[metric].format(**v)
        with self.lock:
            self.starting.add(anchor)
            self.starting_auto.add(anchor)
        threading.Thread(target=self._new_session,
                         args=(anchor, prompt, None),
                         daemon=True).start()

    def _audit_check(self):
        """Every AUDIT_INTERVAL, spawn a 🩺 self-audit session — reads
        logs/state, verifies surfaces, fixes what's safely fixable."""
        if not AUDIT_ENABLED:
            return
        d = self._board_load()
        if time.time() - (d.get("audit_at") or 0) < AUDIT_INTERVAL:
            return
        if self._auto_live() >= MAX_AUTO:
            return
        d["audit_at"] = time.time()
        self._board_write(d)
        self._audit_spawn()

    def _audit_spawn(self):
        up = int(time.time() - self._started)
        errs = ", ".join(f"{k}×{n}" for k, n in
                         self._err_stats.most_common(8)) or L("无", "none")
        try:
            parked = len(self.governor.snapshot() or {})
        except Exception:
            parked = 0
        v = self._vitals
        digest = (L(f"桥已运行 {up//3600}h{(up%3600)//60}m · ", f"bridge up {up//3600}h{(up%3600)//60}m · ") +
                  L(f"接管会话 {len(self.sessions)} · ", f"sessions {len(self.sessions)} · ") +
                  L(f"排队 {len(self.dispatch_queue)} · ", f"queued {len(self.dispatch_queue)} · ") +
                  L(f"崩溃重试中 {len(self.crash_retries)} · ", f"crash-retrying {len(self.crash_retries)} · ") +
                  L(f"限额挂起 {parked} · ", f"parked {parked} · ") +
                  L(f"DM 看板 {len(self._dm_boards)} 份\\n", f"DM boards {len(self._dm_boards)}\n") +
                  L(f"系统体征：CPU {v.get('cpu','?')}% · ", f"host vitals: CPU {v.get('cpu','?')}% · ") +
                  L(f"内存 {v.get('mem','?')}% · 磁盘 {v.get('disk','?')}%\\n", f"mem {v.get('mem','?')}% · disk {v.get('disk','?')}%\n") +
                  L(f"错误统计（本次运行）：{errs}", f"error counts (this run): {errs}"))
        try:
            anchor = self.post(
                None, L("🩺 *桥定期体检*：正在检查日志/状态/Slack 面，", "🩺 *Bridge self-audit*: checking logs/state/Slack surfaces,") +
                      L("报告稍后发到本线程。", "report follows in this thread."))
        except Exception as e:
            self._log_err("audit anchor", e)
            return
        if not anchor:
            return
        prompt = AUDIT_PROMPT.format(digest=digest)
        with self.lock:
            self.starting.add(anchor)
            self.starting_auto.add(anchor)
        threading.Thread(target=self._new_session,
                         args=(anchor, prompt, None),
                         daemon=True).start()

    def _canvas_sync(self, text):
        """Mirror the board into the channel canvas — a persistent doc
        reachable from the channel header on mobile, unlike pins."""
        if not (self.canvas_create or self.canvas_edit):
            return
        md = re.sub(r'<(https?://[^|>]+)\|([^>]+)>',
                    r'[\2](\1)', text)
        try:
            d = self._board_load()
            cid = d.get("canvas")
            if not cid and self.canvas_create:
                cid = self.canvas_create(L("📋 会话看板", "📋 Session Board"), md)
                if cid:
                    d["canvas"] = cid
                    self._board_write(d)
                    return
            if cid and self.canvas_edit:
                self.canvas_edit(cid, md)
        except Exception as e:
            self._log_err("canvas sync", e)

    def _board_peek(self, sid, who, chan=None):
        """Peek without stealing: the last few transcript actions go
        to an ephemeral message only the clicker sees. Falls back to a
        reply under the board itself when no ephemeral transport."""
        text = self._peek_text(sid)
        if self.ephemeral_msg and who:
            try:
                self.ephemeral_msg(channel=chan or self.channel,
                                   user=who, text=text)
                return
            except Exception as e:
                self._log_err("ephemeral", e)
        board = self._board_saved_ts()
        self.post(board, text)

    def handle_interaction(self, payload):
        """Slack interactive payloads: permission option buttons, the
        deny-with-reason modal, and the stop/status controls on the
        live status card."""
        ptype = payload.get("type")
        if ptype == "view_submission":
            self._perm_reason_submit(payload)
            return
        if ptype != "block_actions":
            return
        msg = payload.get("message") or {}
        root = msg.get("thread_ts") or msg.get("ts")
        who = (payload.get("user") or {}).get("id")
        chan = (payload.get("channel") or {}).get("id")
        self._dm_seen(who)
        for action in payload.get("actions") or []:
            aid = action.get("action_id")
            if aid == "ctl_stop":
                self._stop(root)
                continue
            if aid == "ctl_status":
                self._status(root)
                continue
            if aid == "board_reload":
                self._board_refresh()
                continue
            if aid in ("board_adopt", "board_pick"):
                sid = action.get("value") or \
                    (action.get("selected_option") or {}).get("value")
                if sid:
                    self._board_adopt(sid)
                continue
            if aid == "board_peek":
                sid = action.get("value")
                if sid:
                    self._board_peek(sid, who, chan)
                continue
            if aid == "perm_reason":
                is_perm = False
            else:
                is_perm = aid == "perm" or aid.startswith("perm_opt_")
            if not is_perm and aid != "perm_reason":
                continue
            try:
                data = json.loads(action.get("value") or "{}")
            except ValueError:
                continue
            if aid == "perm_reason":
                self._perm_reason_open(payload, root, data.get("rid"),
                                       msg.get("ts"))
            else:
                self._perm_button(root, data.get("rid"), data.get("oid"),
                                  who, msg.get("ts"))

    def _perm_button(self, root, rid, oid, who, msg_ts):
        s = self.sessions.get(root)
        res = None
        if s:
            try:
                res = s.user_decide_option(rid, oid, who)
            except AcpExited:
                res = "dead"
        if res is None:
            self.post(root, L("该审批请求已处理或已失效。", "this approval request was handled or expired."))
            return
        if res == "dead":
            self.post(root, L("审批应答发送失败：会话进程已退出。", "approval reply failed: session process exited."))
            return
        if res == "bad_option":
            self.post(root, L("按钮与请求不匹配（可能已过期）。", "button doesn't match the request (probably expired)."))
            return
        opt, summary, perm_ts = res
        kind = str(opt.get("kind", ""))
        allow = kind.startswith("allow")
        name = opt.get("name") or kind or oid
        mark = "✅" if allow else "❌"
        self._perm_update(perm_ts or msg_ts,
                          L(f"{mark} {summary}\\n已由 ", f"{mark} {summary}\nby ") +
                          L(f"{who or '人工'} 选择：{name}", f"{who or 'human'} chose: {name}"))

    def _perm_update(self, msg_ts, text):
        if msg_ts:
            self._upd(msg_ts, text, blocks=[])

    def _perm_reason_open(self, payload, root, rid, msg_ts):
        """'Deny with a note' — a modal for free-text feedback that is
        forwarded to the session with the rejection, so the agent can
        adjust instead of hitting a silent wall."""
        if not self.open_modal:
            self.post(root, L("请直接回复一句拒绝理由，会连同拒绝转达。", "reply with a one-line deny reason — it's forwarded with the rejection."))
            return
        view = {
            "type": "modal",
            "callback_id": "perm_reason",
            "private_metadata": json.dumps(
                {"root": root, "rid": rid, "mts": msg_ts}),
            "title": {"type": "plain_text", "text": L("拒绝并说明", "Deny with reason")},
            "submit": {"type": "plain_text", "text": L("发送", "Send")},
            "close": {"type": "plain_text", "text": L("取消", "cancel")},
            "blocks": [{
                "type": "input", "block_id": "reason", "optional": True,
                "label": {"type": "plain_text",
                          "text": L("拒绝理由（连同拒绝发给会话）", "deny reason (sent to the session with the rejection)")},
                "element": {"type": "plain_text_input",
                            "action_id": "text", "multiline": True}}],
        }
        try:
            self.open_modal(payload.get("trigger_id"), view)
        except Exception as e:
            self._log_err("open modal", e)
            self.post(root, L("弹窗失败：请直接回复一句拒绝理由。", "dialog failed: please reply with a deny reason."))

    def _perm_reason_submit(self, payload):
        view = payload.get("view") or {}
        try:
            meta = json.loads(view.get("private_metadata") or "{}")
        except ValueError:
            return
        root, rid, msg_ts = (meta.get("root"), meta.get("rid"),
                             meta.get("mts"))
        reason = (((view.get("state") or {}).get("values") or {})
                  .get("reason", {}).get("text", {}).get("value") or "")
        who = (payload.get("user") or {}).get("id")
        s = self.sessions.get(root)
        entry = (s.pending_perms.get(rid) or {}) if s else {}
        if not entry:
            # The request already expired (timeout/turn end) before the
            # human submitted — don't claim a decision was delivered.
            self._perm_update(msg_ts, L("该审批请求已处理或已失效。", "this approval request was handled or expired."))
            return
        pick = next((o for o in entry.get("all_options") or []
                     if not str(o.get("kind", "")).startswith("allow")),
                    None)
        if s and pick:
            try:
                s.user_decide_option(rid, pick.get("optionId"), who)
            except AcpExited:
                pass
        summary = entry.get("summary") or L("审批请求", "approval request")
        note = L(f"（理由：{reason.strip()}）", f"(reason: {reason.strip()})") if reason.strip() else ""
        self._perm_update(msg_ts,
                          L(f"❌ {summary}\\n已由 {who or '人工'} 拒绝{note}", f"❌ {summary}\ndenied by {who or 'human'}{note}"))
        if s:
            self._run_prompt(
                root,
                L(f"用户拒绝了审批请求「{summary}」。", f'the user denied the approval request "{summary}".')
                + (L(f"理由：{reason.strip()}", f"reason: {reason.strip()}") if reason.strip() else "")
                + L("请据此调整做法后继续。", "please adjust your approach accordingly and continue."))

    def _post_perm(self, root, summary, rid, options):
        """One Block Kit button per permission option; the clicked
        optionId is answered back verbatim."""
        elements = []
        for i, o in enumerate(options[:5]):
            kind = str(o.get("kind", ""))
            label = (o.get("name") or kind or "?")[:80]
            elements.append({
                "type": "button",
                "text": {"type": "plain_text", "text": label[:75]},
                # action_id must be unique per message or Slack rejects
                # the whole card with invalid_blocks — the option id
                # rides in `value`, the prefix just routes the click.
                "action_id": f"perm_opt_{i}",
                "value": json.dumps({"rid": rid,
                                     "oid": o.get("optionId")}),
                "style": ("primary" if kind.startswith("allow")
                          else "danger"),
            })
        blocks = [
            {"type": "section",
             "text": {"type": "mrkdwn",
                      "text": L(f"⚠️ 待审批：{slack_md(summary)}", f"⚠️ pending approval: {slack_md(summary)}")}},
            {"type": "actions",
             "block_id": f"perm_{rid}",
             "elements": elements},
            {"type": "actions",
             "elements": [{
                 "type": "button",
                 "text": {"type": "plain_text",
                          "text": L("✏️ 拒绝并附言", "✏️ Deny with note")},
                 "action_id": "perm_reason",
                 "value": json.dumps({"rid": rid})}]},
            {"type": "context",
             "elements": [{"type": "mrkdwn",
                           "text": f"{_perm_timeout_hint()}" +
                                   L(" · 也可回复 允许 / 拒绝", " · or reply allow / deny")}]},
        ]
        try:
            return self.post_msg(root, L("⚠️ 待审批：", "⚠️ Pending approval:") +
                                 slack_md(summary), blocks=blocks)
        except TypeError:
            return self.post(root, L(f"⚠️ 待审批 {rid}: {summary}\\n", f"⚠️ pending approval {rid}: {summary}\n") +
                                   L("回复「允许」或「拒绝」。", 'reply "allow" or "deny".'))
        except Exception as e:
            self._log_err("perm post", e)
            return None

    def handle_event(self, ev, event_id=None):
        if event_id and not self._dedupe(self.seen_ids, self.seen, event_id):
            return
        if (ev.get("type") != "message" or ev.get("channel") != self.channel
                or ev.get("subtype") not in (None, "bot_message",
                                             "file_share")
                or (self.own_bot_id
                    and ev.get("bot_id") == self.own_bot_id)):
            return
        if BLOCKED_SENDERS and \
                ({ev.get("bot_id"), ev.get("app_id"), ev.get("user")}
                 & BLOCKED_SENDERS):
            return
        ts = ev.get("ts")
        if not ts:
            return
        if not self._dedupe(self.seen_msg_keys, self.seen_msgs,
                            (ev.get("channel"), ts)):
            return
        self._dm_seen(ev.get("user"))
        text = MENTION.sub("", ev.get("text", "")).strip()
        files = ev.get("files") or []
        root = ev.get("thread_ts", ts)
        is_root = root == ts
        if not text and not files:
            return
        if files and not text:
            text = L("（见附件）", "(see attachment)")
        low = text.lower()
        # "!" is the command prefix: a known bridge command runs
        # locally; anything else is rewritten to "/…" and forwarded to
        # the session (Slack eats a real leading "/").
        if len(text) > 1 and text[0] in "!！":
            body = text[1:].strip()
            head, _, arg = body.partition(" ")
            head = head.lower()
            arg = arg.strip()
            if not is_root and head == "goal" \
                    and arg.lower() in GOAL_ARG_CLEAR:
                self._goal_clear_cmd(root)
                return
            if head in BANG_CMDS:
                # Top-level commands run too: replies thread under the
                # command message, so `!adopt` typed in the channel
                # binds the adopted session to a fresh thread (mobile).
                self._bang(root, BANG_CMDS[head], arg, ev)
                return
            if body and body[0].isascii() and body[0].isalpha():
                text = "/" + body
                low = text.lower()
        if is_root:
            if MENTION.match(ev.get("text", "")):
                auto = bool(ev.get("bot_id") or ev.get("app_id"))
                with self.lock:
                    busy = ts in self.sessions or ts in self.starting
                    queued = any(q[0] == ts
                                 for q in self.dispatch_queue)
                # A state file means this ts was already accepted before a
                # possible restart; an unacked redelivery must not re-run it.
                if not busy and read_state(self._workdir(ts)) is not None:
                    busy = True
                if busy:
                    self.post(ts, L("该线程已有会话记录，未重复执行；", "this thread already has a session; not re-running;") +
                                  L("请在原线程回复继续，或发新根消息开新任务。", "reply in the original thread to continue, or post a new top-level message for a new task."))
                    return
                if queued:
                    return
                queue_it = False
                with self.lock:
                    if auto and self._auto_live() >= MAX_AUTO:
                        queue_it = True
                        self.dispatch_queue.append((ts, text, files))
                    else:
                        self.starting.add(ts)
                        if auto:
                            self.starting_auto.add(ts)
                if queue_it:
                    wd = self._workdir(ts)
                    self._seed_workdir(wd, ts)
                    write_state(wd, "queued", "", L("等待自动任务槽位", "waiting for an auto-task slot"))
                    self.post(ts, L("已有自动任务在运行，本任务排队中（第 ", "an auto task is already running; this task queued (#") +
                                  L(f"{len(self.dispatch_queue)} 位）；", f"{len(self.dispatch_queue)});") +
                                  L('回复\\"停止\\"可取消排队。', 'reply "stop" to cancel the queue.'))
                    self._reap_idle_auto()
                    self._pump_queue()
                    return
                threading.Thread(target=self._new_session,
                                 args=(ts, text, files),
                                 daemon=True).start()
            elif self.own_user_id and \
                    f"<@{self.own_user_id}>" in ev.get("text", ""):
                # Mentioned mid-message: matching is anchored at the
                # start, so without a hint this silently does nothing.
                self.post(ts, L("收到 @我：请把 @我 放在消息开头并带上", "Got the @me: put the mention at the start of the message with") +
                              L("任务内容，我才能接单。", "task content for me to pick it up."))
            return
        if low in STOP_WORDS:
            self._stop(root)
        elif low in GOAL_CLEAR_WORDS:
            self._goal_clear_cmd(root)
        elif low in RELOAD_WORDS:
            self.request_reload()
            self.post(root, L("收到，桥服务热重启（在跑的会话不中断）。", "got it — bridge hot-restarting (running sessions unaffected)."))
        elif low in STATUS_WORDS:
            self._status(root)
        elif low in LIMIT_WORDS:
            self._limits(root, "")
        elif low in APPROVAL_WORDS:
            self._approvals(root)
        elif low in ALLOW_WORDS:
            self._decide(root, True, ev)
        elif low in DENY_WORDS:
            self._decide(root, False, ev)
        elif low in HELP_WORDS:
            self.post(root, HELP_CARD)
        elif root in self.sessions:
            s = self.sessions[root]
            if s.running:
                # Speaking while it works = interrupt: cancel the
                # in-flight turn, then this message runs next (FIFO
                # with other queued user messages is preserved).
                s.interrupt_ev.set()
            elif s.queued and \
                    time.monotonic() - s.last_queue_notice > 60:
                s.last_queue_notice = time.monotonic()
                self.post(root, L("⏸ 收到，已排队——前面还有你的消息没处理完。", "⏸ Got it, queued — your earlier message is still being processed."))
            self._react(ts, "eyes")
            threading.Thread(target=self._run_prompt,
                             args=(root, text, files, ts),
                             daemon=True).start()
        elif any(q[0] == root for q in self.dispatch_queue):
            with self.lock:
                pos = next((i for i, q in
                            enumerate(self.dispatch_queue)
                            if q[0] == root), None)
                if pos is not None:
                    q = self.dispatch_queue[pos]
                    self.dispatch_queue[pos] = (
                        q[0], (q[1] + "\n" + text).strip(),
                        (q[2] or []) + (files or []))
                    pos += 1
            self.post(root, L("任务在排队中", "task is queued") +
                      (L(f"（第 {pos} 位），此回复已并入任务内容。", f"(#{pos}); this reply was merged into the task.")
                       if pos else "。"))
        else:
            wd = self._workdir(root)
            st = read_state(wd)
            if st and st.get("state") == "reset":
                # !reset armed this thread: consume the marker and
                # start a brand-new session on the same workdir.
                try:
                    os.unlink(_state_path(wd))
                except OSError:
                    pass
                self._react(ts, "eyes")
                threading.Thread(target=self._new_session,
                                 args=(root, text, files),
                                 daemon=True).start()
            elif st and st.get("session_id"):
                self._react(ts, "eyes")
                threading.Thread(target=self._resume_session,
                                 args=(root, text, files,
                                       st["session_id"]),
                                 daemon=True).start()
            elif MENTION.match(ev.get("text", "")):
                tail = L(f"（上次：{fmt_state(st)}）", f"(last: {fmt_state(st)})") if st else ""
                self.post(root, L(f"该线程没有活跃会话{tail}；", f"no active session on this thread{tail};") +
                                L("开新任务请发新根消息并@我。", "to start a new task, post a new top-level message and @me."))

    def reaper(self):
        while True:
            time.sleep(300)
            self._reap_idle_auto()
            idle = [ts for ts, s in list(self.sessions.items())
                    if not s.running and s.queued == 0
                    and not self.governor.get(ts)
                    and time.monotonic() - s.last_active > self.idle_sec]
            for ts in idle:
                with self.lock:
                    s = self.sessions.pop(ts, None)
                    self.stop_flags.pop(ts, None)
                if s:
                    dead = s.terminate()
                    write_state(s.workdir, "reaped", s.last_activity,
                                L("空闲超时回收", "idle-timeout reaped"))
                    self._finish(ts, s, "zzz", L("💤 空闲回收", "💤 Idle reaped"))
                    self.post(ts, L("会话空闲超时已回收", "idle-timeout session reaped")
                              + ("" if dead else L("（进程退出未确认）", "(process exit unconfirmed)"))
                              + L("；继续发言请开新任务。", "; to keep going, start a new task."))
            self._pump_queue()


def main():
    for name in ("SLACK_BOT_TOKEN", "SLACK_APP_TOKEN"):
        if not os.environ.get(name):
            sys.exit(f"{name} not set")
    if not CHANNEL:
        sys.exit("SLACK_CHANNEL_ID not set")

    from http.client import RemoteDisconnected
    from urllib.error import URLError
    from slack_sdk import WebClient
    from slack_sdk.http_retry.builtin_handlers import (
        ConnectionErrorRetryHandler, RateLimitErrorRetryHandler,
        ServerErrorRetryHandler)
    from slack_sdk.socket_mode import SocketModeClient
    from slack_sdk.socket_mode.request import SocketModeRequest
    from slack_sdk.socket_mode.response import SocketModeResponse

    web = WebClient(token=os.environ["SLACK_BOT_TOKEN"], timeout=15)
    web.retry_handlers.append(
        RateLimitErrorRetryHandler(max_retry_count=3))
    web.retry_handlers.append(
        ServerErrorRetryHandler(max_retry_count=3))
    web.retry_handlers.append(ConnectionErrorRetryHandler(
        max_retry_count=3,
        error_types=[URLError, ConnectionResetError, RemoteDisconnected,
                     TimeoutError]))
    # Uploads get their own client: files_upload_v2 is several HTTP
    # round-trips (upload URL → bytes → complete) and Slack's endpoint
    # routinely exceeds the shared 15s read timeout on MBs — that's
    # exactly why deliverables were silently timing out.
    web_up = WebClient(token=os.environ["SLACK_BOT_TOKEN"], timeout=300)
    web_up.retry_handlers.append(ConnectionErrorRetryHandler(
        max_retry_count=3,
        error_types=[URLError, ConnectionResetError, RemoteDisconnected,
                     TimeoutError]))
    socket = SocketModeClient(app_token=os.environ["SLACK_APP_TOKEN"],
                              web_client=web, auto_reconnect_enabled=True)
    auth = web.auth_test()
    own_bot_id = auth.get("bot_id")
    own_user_id = auth.get("user_id")

    def post(ts, text, blocks=None):
        kw = {"channel": CHANNEL, "text": text, "blocks": blocks}
        if ts is not None:
            kw["thread_ts"] = ts
        return web.chat_postMessage(**kw).get("ts")

    def react(ts, name):
        web.reactions_add(channel=CHANNEL, timestamp=ts, name=name)

    def unreact(ts, name):
        web.reactions_remove(channel=CHANNEL, timestamp=ts, name=name)

    def update(msg_ts, text, blocks=None):
        kw = {"channel": CHANNEL, "ts": msg_ts, "text": text}
        if blocks is not None:
            kw["blocks"] = blocks
        web.chat_update(**kw)

    def upload(ts, path):
        web_up.files_upload_v2(channel=CHANNEL, thread_ts=ts,
                               file=path, title=os.path.basename(path))

    def set_status(ts, text):
        web.assistant_threads_setStatus(channel_id=CHANNEL, thread_ts=ts,
                                        status=text)

    def open_modal(trigger_id, view):
        web.views_open(trigger_id=trigger_id, view=view)

    def pin_add(ts):
        web.pins_add(channel=CHANNEL, timestamp=ts)

    def pin_list():
        return web.pins_list(channel=CHANNEL).get("items") or []

    def pin_rm(ts):
        web.pins_remove(channel=CHANNEL, timestamp=ts)

    def ephemeral(**kw):
        web.chat_postEphemeral(**kw)

    def permalink(ts):
        return web.chat_getPermalink(channel=CHANNEL,
                                     message_ts=ts).get("permalink")

    def bookmarks():
        return web.bookmarks_list(channel_id=CHANNEL) \
            .get("bookmarks") or []

    def bookmark_add(title, link):
        web.bookmarks_add(channel_id=CHANNEL, title=title, link=link,
                          type="link")

    def bookmark_edit(bid, link):
        web.bookmarks_edit(bookmark_id=bid, channel_id=CHANNEL,
                           link=link)

    def dm_post(uid, text, blocks=None):
        r = web.chat_postMessage(channel=uid, text=text,
                                 blocks=blocks)
        return {"channel": r.get("channel"), "ts": r.get("ts")}

    def dm_update(chan, ts, text, blocks=None):
        kw = {"channel": chan, "ts": ts, "text": text}
        if blocks is not None:
            kw["blocks"] = blocks
        web.chat_update(**kw)

    def canvas_create(title, md):
        r = web.conversations_canvases_create(
            channel_id=CHANNEL, title=title,
            document_content={"type": "markdown", "markdown": md})
        return r.get("canvas_id")

    def canvas_edit(cid, md):
        web.canvases_edit(canvas_id=cid, changes=[{
            "operation": "replace",
            "document_content": {"type": "markdown",
                                 "markdown": md}}])

    bridge = SessionBridge(post, own_bot_id=own_bot_id,
                           own_user_id=own_user_id,
                           react=react, unreact=unreact, update=update,
                           upload=upload, status=set_status,
                           open_modal=open_modal,
                           pin_add=pin_add, pin_list=pin_list,
                           pin_rm=pin_rm, team_url=auth.get("url"),
                           ephemeral=ephemeral, bookmarks=bookmarks,
                           bookmark_add=bookmark_add,
                           bookmark_edit=bookmark_edit,
                           permalink=permalink,
                           dm_post=dm_post, dm_update=dm_update,
                           canvas_create=canvas_create,
                           canvas_edit=canvas_edit)
    signal.signal(signal.SIGHUP,
                  lambda *a: bridge.reload_ev.set())
    signal.signal(signal.SIGTERM,
                  lambda *a: bridge.term_ev.set())
    restored = bridge.restore_live()
    adopted = bridge.adopt_inflight(skip=restored)
    limited = bridge.adopt_limited()
    interrupted = mark_interrupted(bridge.tasks_dir,
                                   skip=restored | adopted)
    for ts, st in interrupted:
        bridge._react_state(ts, REACT_RELOADING)
        if time.time() - st.get("updated", 0) < 86400:
            bridge.post(ts, L("🔄 桥服务已重启，本会话中断；", "🔄 Bridge restarted, this session was interrupted;") +
                            L("回复本线程可自动恢复上下文。", "reply in this thread to auto-restore context."))
    if restored or adopted or interrupted or limited:
        print(f"[info] startup: {len(restored)} live-restored, "
              f"{len(adopted)} inflight-adopted, "
              f"{len(limited)} limit-parked, "
              f"{len(interrupted)} interrupted", flush=True)
        # One channel-level line replaces per-thread L("会话保持", "session kept")
        # posts: with N sessions a reload used to cost 2-4 messages
        # per thread.
        if restored or adopted or limited:
            bits = [L(f"接管 {len(restored)} 个会话", f"adopted {len(restored)} sessions")] if restored else []
            if adopted:
                bits.append(L(f"{len(adopted)} 个未跑完回合续跑", f"{len(adopted)} unfinished turns resumed"))
            if limited:
                bits.append(L(f"{len(limited)} 个限额回合排队冷却", f"{len(limited)} limited turns parked to cool down"))
            bridge.post(None, L("🔄 桥已热重启：", "🔄 Bridge hot-restarted:") + "，".join(bits) +
                            L("，线程里继续即可。", "; just continue in the thread."))

    def handle(client, req: SocketModeRequest):
        if req.type == "events_api":
            client.send_socket_mode_response(
                SocketModeResponse(envelope_id=req.envelope_id))
            bridge.handle_event(req.payload.get("event", {}),
                                req.payload.get("event_id"))
        elif req.type == "interactive":
            client.send_socket_mode_response(
                SocketModeResponse(envelope_id=req.envelope_id))
            try:
                bridge.handle_interaction(req.payload or {})
            except Exception as e:
                bridge._log_err("interaction", e)

    threading.Thread(target=bridge.reaper, daemon=True).start()
    threading.Thread(target=bridge.limit_sweeper, daemon=True).start()
    socket.socket_mode_request_listeners.append(handle)
    socket.connect()
    print(f"session bridge up in {CHANNEL}, tasks dir {TASKS_DIR}",
          flush=True)
    # First render after boot: whatever the board said before restart
    # is stale by definition (sessions were restored/adopted/parked).
    threading.Thread(target=bridge._board_refresh, daemon=True).start()
    try:
        while True:
            if bridge.reload_ev.is_set():
                socket.close()
                bridge.reload_exec()
            if bridge.term_ev.is_set():
                socket.close()
                bridge.reload_term()
                return
            time.sleep(0.5)
    except KeyboardInterrupt:
        socket.close()


if __name__ == "__main__":
    if "--approvals" in sys.argv:
        print(json.dumps(collect_approvals(TASKS_DIR),
                         ensure_ascii=False, indent=1))
        sys.exit(0)
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
