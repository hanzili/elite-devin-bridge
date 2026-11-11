# Elite Devin Bridge

[English](README.md) | **中文**

一座 Slack ↔ `devin acp` 会话桥，让编码 agent 像 Slack 里的原生队友，
而不是一条命令转发管道。

频道里 @ 一次就拉起一个绑定在该线程下的持久 agent 会话，线程里的回复
继续喂给同一个会话。一个真队友该做的事——汇报进度、崩溃自愈、随时可
停、危险操作先请示——全部由桥负责，不靠提示词约束模型。

## 功能

- **线程级持久会话** —— 频道根消息 @ → 建会话；线程回复继续同一
  会话。每个线程一个独立工作目录 + 持久状态文件。
- **实时状态卡** —— 每线程一条消息原地更新：已用时长、工具调用数、
  模型 + 权限模式、最近工具调用、计划条目、当前活动。回合结束沉淀为
  结算卡，附碰过的文件清单（`tool_call.locations`）。
- **生命周期表情** 打在根消息上：⏳ 运行 → ✅ 完成 / 🛑 已停 /
  💥 崩溃 / ⏰ 超时 / 🔄 重载。
- **控制** —— 状态卡上的 `🛑 停止` / `📋 状态` 按钮，以及 `!stop`
  `!status` `!queue` `!reload` `!reset` `!help` `!allow` `!deny`
  `!approvals` `!peek` `!files` `!send` `!board` 文本命令。
  未知的 `!x` 会作为斜杠命令转给 agent。
- **交互式审批** —— Block Kit 卡片每个选项一个按钮、"带理由回绝"
  弹窗、每线程持久的审批台账、过期/陈旧防护。待审批的回合进入挂起
  态（豁免静默超时），不会被误杀。
- **重启与崩溃韧性** —— SIGHUP 原地 re-exec，所有 ACP fd 不丢；
  `systemctl restart` 把 fd 存进 systemd fd store 恢复活会话（生产
  实测 7 个会话无损接管）。进行中的 prompt 有记录，硬死亡后重放；
  每个恢复的线程都会收到一条 🔄 提示。
- **限额管家（Limit governor）** —— 被服务端限额杀死的回合（"Reached
  … rate limit" 等）不会失败，而是进入持久冷却注册表，等服务端自己的
  重置窗口（或有界退避）后错峰恢复，一批任务不会二次撞墙。被同一
  账号级限额波及的 Desktop/CLI 会话也会从共享会话库里被发现并经
  `session/load` 接管续跑。事件流水在 `.bridge_limits.log`，`!limits`
  看实时队列；详见 `LIMIT_GOVERNOR.md`。
- **流式与分片** —— 草稿消息原地更新，超长溢出续发新消息并收缩
  `chat.update` 预算，避免 `msg_too_long` 风暴。
- **双向附件** —— 用户附件落到 `inbox/` 追加进 prompt；workdir 里的
  新交付物回传到线程。
- **发件人策略** —— `BRIDGE_BLOCK_SENDERS` 黑白名单、自身消息严格
  去重；真人 @ 永远立即开工，机器人派单在
  `BRIDGE_MAX_AUTO_SESSIONS` 下排队。
- **ACP 鉴权** —— 桥完成 `initialize → authenticate{_meta.api_key}
  → session/new|load`。密钥解析顺序：`DEVIN_ACP_API_KEY` → Devin
  Desktop 当前登录态（`state.vscdb` `windsurfAuthStatus`）→
  `credentials.toml`。
- **跨设备会话互通** —— 所有 Devin 会话在同一个全局存储
  （`~/.local/share/devin/cli/sessions.db`），哪儿开的哪儿都能续：
  `!sessions` 带归属标记列出，`!adopt <id>` 经 `session/load` 把
  Desktop/CLI 会话绑进线程（在它自己的项目目录下跑），`!handoff`
  给出 `devin -r` / Desktop 会话列表用的 id，`!release` 解绑但保留
  上下文。置顶频道看板（`!board` 或生命周期自动刷新）提供稳定
  总览——所有更新都发生在线程内，活跃会话永远不把频道顶乱序。
- **看板，三个面** —— 同一份实时看板：频道置顶、每个用户与机器人
  私聊里的镜像（`BOARD_DM_USERS` 种子 + 互动过的用户自动获得）、
  频道 Canvas。🔥 行显示会话当下真实动作（不是干巴巴的"活跃"），
  每行带「看看」（不抢锁地窥屏）和「接管」（一键抢进新线程，手机上
  零输入），底部下拉可接管任意会话。移动端不用翻 pin。
- **说话即打断** —— 回合中发的消息 = 打断当前回合（底层走
  session/cancel，标记 `⏸ 已打断`），新消息接着执行——不傻等
  当前输出跑完。与 `!stop`（🛑）语义区分。
- **按需发文件** —— agent 回复里点名的文件必发；workdir 新交付物
  自动发但每回合上限 3 个（README/GOAL/工程文件和内部状态永不自动
  发）。超出的进暂存池：`!files` 看清单，`!send <名字>` / `!send
  all` 随取。上传失败自动重试，300 秒预算。
- **机器体征 + 自维护** —— 看板底部 `🖥 CPU · 内存 · 磁盘 · ⬇⬆`
  直读 /proc；某项指标持续超 `VITAL_*_WARN` 阈值自动派 🧹 清理
  会话（有界安全清理：apt/journal/tmp/docker prune，绝不动用户
  数据）；每 `AUDIT_INTERVAL`（默认 12 小时）派 🩺 体检会话对照
  注入的健康摘要审查桥自身日志/状态/Slack 面并做安全修复。
- **目标长跑** —— 线程里 `/goal` 挂一个长目标定期续跑
  （`!goal cancel` / 取消目标 清除）。

## 架构

```
Slack (Socket Mode) ──▶ session_bridge.py ──▶ devin acp (per thread)
        ▲                      │
        └──── chat.postMessage ┘── workdir per thread (state.json,
             reactions, blocks        inbox/, AGENTS.md, ledger)
```

- `session_bridge.py` —— 生产桥（socket-mode 客户端、ACP 子进程管理、
  状态/审批卡、恢复）。
- `bridge_core.py`、`bridge_store.py`、`bridge_engine.py` —— 发件人/@
  策略、持久去重 + 线程/发件箱状态（SQLite）、串行引擎。
- `slack_adapter.py`、`acp_adapter.py` —— 明确的 SDK/子进程边界
  （惰性导入；测试不触网）。
- `runtime_pump.py`、`runtime_scoped.py`、`offline_wiring.py`、
  `task_scope.py`、`result_summary.py` —— 运行时管线与测试用的
  纯内存模拟器。
- `fake_acp.py` —— 进程内假 ACP 服务，测试可重现。
- `smoke_echo.py`、`smoke_sessions.py` —— 手动冒烟工具。

## 部署

1. **Slack app** —— 用 `manifest.private-channel.draft.json` 建应用
   （开 Socket Mode，私有频道要 `message.groups` + `groups:history`；
   公开频道换 `message.channels`/`channels:history`）。安装、拉进
   频道、拿 `xoxb-`/`xapp-` token。
2. **环境** —— `env.example` 拷到 `~/.config/devin-bridge/env`，填
   token + `SLACK_CHANNEL_ID`，`chmod 600`。
3. **Python** —— 3.10+ 虚拟环境 `pip install slack-sdk`（唯一依赖，
   其余全标准库）。
4. **Agent** —— 装 [Devin CLI](https://devin.ai) 让 `devin acp` 在
   PATH 上，并登录（`devin auth login` 或 Devin Desktop；桥在会话
   启动时自取凭据）。
5. **服务** —— `cp session-bridge.service ~/.config/systemd/user/`，
   `systemctl --user daemon-reload && systemctl --user start
   session-bridge`。unit 配了 `FileDescriptorStoreMax` +
   `KillMode=process`，`systemctl restart` 不丢活会话；
   `systemctl reload`（SIGHUP）热重载代码不掉任何东西。

## 测试

```
python3 -m unittest              # 全部离线跑
```

整套测试只打 `fake_acp.py`、假 Slack 传输层和临时 SQLite——不碰
Slack 网络、不用真 `devin` 二进制、不用 token。

## 备注

- 中文是一等公民：命令词（停止/状态/审批/取消目标…）、状态文案、
  错误卡全双语；agent 用什么语言回取决于你用什么语言。
- 可选钩子：`PCTX_MESSAGES_PUT` 指向一个摄取脚本时会在每条用户
  消息上调用（source-id 去重）；不设或缺失即 no-op。
- 桥绝不按命令行模式杀进程——只杀自己登记的 PID/进程组。同一台
  机器多个桥共存安全。

## License

MIT
