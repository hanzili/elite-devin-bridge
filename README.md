# Elite Devin Bridge

**English** | [中文](README.zh-CN.md)

A Slack ↔ `devin acp` session bridge that makes a coding agent feel like a
native Slack teammate instead of a command relay.

One mention in a channel spawns a persistent agent session bound to that
Slack thread. Replies in the thread feed the same session. Everything a
real teammate should do — show progress, survive restarts, stop on demand,
ask for permission — is handled by the bridge, not by prompting the model.

## Features

- **Thread-bound persistent sessions** — channel-root mention → session;
  thread replies continue it. Sessions live in per-thread workdirs with
  durable state files.
- **Live status cards** — one message per thread, updated in place:
  elapsed time, tool count, model + permission mode, recent tool calls,
  plan entries, current activity. Settles into a final card with
  touched-file list (`tool_call.locations`) when the turn ends.
- **Lifecycle reactions** on the root message: ⏳ running → ✅ done /
  🛑 stopped / 💥 crashed / ⏰ timeout / 🔄 reload.
- **Controls** — `🛑 停止` / `📋 状态` buttons on the status card, plus
  `!stop`, `!status`, `!queue`, `!reload`, `!reset`, `!help`, `!allow`,
  `!deny`, `!approvals`, `!peek`, `!files`, `!send`, `!board` text
  commands. Unknown `!x` is forwarded to the agent as a slash command.
- **Interactive approvals** — Block Kit card with one button per option,
  a "reject with note" modal, a persistent per-thread approval ledger,
  and stale/expired guards. Pending approvals park the turn (exempt from
  the silence timeout) instead of being killed.
- **Restart & crash resilience** — SIGHUP re-execs in place keeping every
  ACP fd; `systemctl restart` parks fds in the systemd fd store and
  restores live sessions (`7 live-restored` in production). In-flight
  prompts are recorded and replayed after hard death; a 🔄 notice is
  posted into each restored thread.
- **Limit governor** — turns killed by server-side limits ("Reached …
  rate limit" / retryable refusals) park into a durable cooldown
  registry instead of failing, wait out the server's own reset window
  (or bounded backoff), then resume staggered so a batch can't re-trip
  the cap. Desktop/CLI sessions the same account-level limit took out
  are detected via the shared session store and resumed through
  `session/load`. Event history in `.bridge_limits.log`, `!limits` for
  the live queue; see `LIMIT_GOVERNOR.md`.
- **Streaming & chunking** — live draft message updated in place, with
  overflow spilling into follow-up posts and a smaller `chat.update`
  budget to avoid `msg_too_long` storms.
- **Attachments both ways** — user files land in `inbox/` and are
  appended to the prompt; absolute paths and new media in the workdir
  are uploaded back to the thread.
- **Sender policy** — allowlist/blocklist via `BRIDGE_BLOCK_SENDERS`,
  strict own-message dedup, human mentions always start immediately
  while bot-posted dispatches queue under `BRIDGE_MAX_AUTO_SESSIONS`.
- **ACP authentication** — the bridge performs `initialize →
  authenticate{_meta.api_key} → session/new|load`. Key resolution:
  `DEVIN_ACP_API_KEY` → Devin Desktop active-account key
  (`state.vscdb` `windsurfAuthStatus`) → `credentials.toml`.
- **Cross-device session interop** — all Devin sessions live in one
  global store (`~/.local/share/devin/cli/sessions.db`), so a session
  started anywhere resumes anywhere else: `!sessions` lists them with
  ownership marks, `!adopt <id>` binds a Desktop/CLI session to the
  thread via `session/load` (running in its own project dir), `!handoff`
  surfaces the id for `devin -r` / the Desktop session list, and
  `!release` unbinds without wiping context. A pinned channel dashboard
  (`!board`, auto-refreshed on session lifecycle) gives a stable
  overview — updates happen inside threads, so active sessions never
  re-order the channel.
- **Dashboard, three surfaces** — the same live board is pinned in the
  channel, mirrored into each user's bot DM (`BOARD_DM_USERS` seed +
  auto-learn anyone who interacts), and rendered into the channel
  canvas. 🔥 rows show the actual current action (not just "active"),
  every row offers 看看 (peek without stealing) and 接管 (adopt into a
  fresh thread — one tap, no typing), and a dropdown takes over any
  session. Mobile-friendly: no scrolling to a pin.
- **Talking interrupts** — a message that arrives mid-turn interrupts
  the turn (`⏸ 已打断`, session/cancel under the hood) and runs next;
  it never waits for the current output to finish. Distinct from
  `!stop` (🛑).
- **Intent-gated file delivery** — files the agent explicitly names in
  its reply always upload; fresh deliverables in the workdir auto-send
  capped at 3 per turn (README/GOAL/working files and internal state
  never do). Overflow goes to a pending pool: `!files` lists what
  exists, `!send <name>` / `!send all` pulls it on demand. Uploads
  retry on transient failure with a 300s budget.
- **Host vitals + self-maintenance** — a `🖥 CPU · 内存 · 磁盘 · ⬇⬆`
  line on the board reads /proc directly; a metric sustained over
  `VITAL_*_WARN` spawns a 🧹 cleanup session (bounded safe frees:
  apt/journal/tmp/docker prune — never user data), and every
  `AUDIT_INTERVAL` (12h) a 🩺 self-audit session reviews bridge
  logs/state/Slack surfaces against an injected health digest and
  fixes what's safely fixable.
- **Goal runs** — `/goal` in a thread attaches a long-running objective
  with periodic resumption (`!goal cancel` / 取消目标 to clear).

## Architecture

```
Slack (Socket Mode) ──▶ session_bridge.py ──▶ devin acp (per thread)
        ▲                      │
        └──── chat.postMessage ┘── workdir per thread (state.json,
             reactions, blocks        inbox/, AGENTS.md, ledger)
```

- `session_bridge.py` — the production bridge (socket-mode client, ACP
  subprocess manager, status/approval cards, recovery).
- `bridge_core.py`, `bridge_store.py`, `bridge_engine.py` — sender/mention
  policy, durable dedup + thread/outbox state (SQLite), serialized engine.
- `slack_adapter.py`, `acp_adapter.py` — explicit SDK/subprocess
  boundaries (lazy imports; tests never touch the network).
- `runtime_pump.py`, `runtime_scoped.py`, `offline_wiring.py`,
  `task_scope.py`, `result_summary.py` — runtime plumbing and the
  memory-only simulator used by tests.
- `fake_acp.py` — in-process fake ACP server for deterministic tests.
- `smoke_echo.py`, `smoke_sessions.py` — manual smoke tools.

## Setup

1. **Slack app** — create an app from `manifest.private-channel.draft.json`
   (Socket Mode on, `message.groups` + `groups:history` for private
   channels; add `message.channels`/`channels:history` for public ones).
   Install it, invite it to the channel, copy the `xoxb-`/`xapp-` tokens.
2. **Env** — copy `env.example` to `~/.config/devin-bridge/env`, fill in
   tokens + `SLACK_CHANNEL_ID`, `chmod 600`.
3. **Python** — 3.10+ venv with `pip install slack-sdk` (that's the only
   dependency; everything else is stdlib).
4. **Agent** — install the [Devin CLI](https://devin.ai) so `devin acp`
   is on PATH, and log in (`devin auth login` or Devin Desktop; the
   bridge picks up the credential at session start).
5. **Service** — `cp session-bridge.service ~/.config/systemd/user/`,
   `systemctl --user daemon-reload && systemctl --user start
   session-bridge`. The unit uses `FileDescriptorStoreMax` +
   `KillMode=process` so `systemctl restart` preserves live sessions;
   `systemctl reload` (SIGHUP) hot-reloads the code without dropping
   anything.

## Test

```
python3 -m unittest              # 304 tests, all offline
```

The suite runs entirely against `fake_acp.py`, fake Slack transports and
temp SQLite files — no Slack network, no real `devin` binary, no tokens.

## Notes

- Chinese replies are first-class: command words (停止/状态/审批/取消目标…),
  status text and error cards are bilingual; the agent itself replies in
  whatever language you use.
- Optional hook: if `PCTX_MESSAGES_PUT` points at an ingest script it is
  called per user message (source-id dedup); unset or missing = no-op.
- The bridge never kills by command-line pattern — only PIDs/process
  groups it owns. Multiple bridges on one host are safe.

## License

MIT
