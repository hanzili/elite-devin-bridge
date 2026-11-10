# Limit Governor — server-side 限额冷却与自动恢复

> 2026-11-10 落地。动机：SWE-2（`swe-2-max`）的免费模型速率限制会在
> 高并发时段把多个在跑 session 的回合一起掐断——错误不是任务失败，
> 活还在，账号只需要冷却。桥现在把这类回合**挂起（park）→ 冷却 →
> 自动重试**，并把每次事件写日志。

## 逆向出来的限流逻辑（sessions.db 取证，96 个限额节点 / 8 个会话）

- **撞的不是"并发 session 数"，是 `Reached … model rate limit`——
  按账号聚合的请求速率预算（RPM）**。并发 session 共享同一份速率
  预算；N 个 session 一起跑 swe-2-max，聚合速率顶线时每个在发
  请求的回合各自报错。会话总数不是触发变量，请求速率才是。
- **错误面有两层**：
  1. *工具级吸收*——`run_subagent`/`exec` 等工具结果里返回限额
     文本，会话看到错误后能自己绕路（`cultured-sock` 实测改前台
     单 agent 继续）。transcript 里 96/96 个限额节点全是这类，
     **没有一个直接杀死回合**。
  2. *回合级杀死*——`session/prompt` 直接回 error：
     `{'code':-32010,'message':'Connection error, send a message to
     continue retrying','data':{errorKind:'unavailable',
     retryable:true}}` 或 `AcpError: Reached free model rate
     limit…`。**这类杀死回合且不落 message_nodes**——就是"所有
     session 被停下来"的那一刀。州的州长管的就是它。
- **reset 窗口双峰**：约一半报 `reset in <2min`（短窗口限速），
  约三分之一报 `reset in 1–24h`（如 14h52m，更像长窗口配额）。
  冷却必须按服务器给的 reset 时间睡，而不是固定退避。
- **进程日志取证**（`devin_*.log`，2026-11-10 12:55–12:57 集群）：
  - CLI 内部对限额**自带 3 次控制循环重试**
    （`control_loop attempt=1..3`，间隔约 50–60s），耗尽才把错误
    吐上 ACP 层——所以一个可见限额错误实际已消耗多倍速率预算。
  - 错误有两种出参形态：`-32010 Unavailable` 带
    `errorKind:"unavailable", retryable:true`，以及
    `-32603 Internal error` 不带 data——分类器按消息文本锚定，
    两种都覆盖。
  - 同一集群里所有错误报**同一个 reset 时刻**（`at 05:01 UTC`）
    ——窗口是服务端共享的墙钟边界，不是逐请求滚动窗口。
- **切号行为**：限速按"账号×模型"的速率预算走；用户实测三账号
  轮切——同强度跑同一模型，每个号各自顶线，换号后错误依旧。
  桥会跟随 Desktop 当前登录的凭据（`_acp_api_key`），所以
  **切号后停泊项不等旧账号窗口、直接提前重试**（见 acct_switch
  事件）。治本不是加号，是错开模型（错误文本自带的出路
  `switch to a different model`）。
- SWE-2 免费窗口 11-30 结束后转按 token 计费，"free model rate
  limit" 大概率退场，改走 ACU/quota——同一套分类器覆盖
  （quota/rate_limit/concurrency 关键字都按 parkable 处理）。

## 行为

### 桥内 session（Slack 线程）
回合抛限额类错误 → 不写 `failed`、不发 ❌：
- 状态写 `rate_limited`，根消息反应 ⏸️，线程收到冷却卡
  （重试时间 + 第 n/8 次 + "回复立即重试"）
- `{text, msg_ts, session_id, resume_at, attempt}` 入
  `.bridge_limits.json`（持久化，桥重启不丢）
- `resume_at` = 服务器 reset 时间 + 2–12s 抖动；无 reset 则
  30s→60s→120s→240s→300s 指数退避
- sweeper 每 15s 唤醒，**每条 RESUME_GAP(15s) 只放一个**——N 个
  同时挂起的回合错峰复活，不再次齐射顶线
- 进程还活着就直接在同一 acp 上重发 prompt（上下文最热）；进程
  死了走 `session/load` 按 session_id 恢复
- 再撞限额 → 重新入队，attempt+1；超过 8 次才落 ❌ 终卡
- 人工优先：线程里发任何消息立即重试并丢弃挂起回合；
  `!stop`/`!reset`/`!limits clear` 都会清掉挂起项
- 启动时收编旧案：`failed`/`interrupted` 且 detail 含限额签名、
  6h 内的 workdir 自动入队（本次升级前就位的受害者）

### 外部 session（Desktop/CLI —— 与桥共享同一账号限速）
每 90s 扫 `sessions.db` 最近活跃的 session。判死用两个信号：
- transcript 尾部 5 条内有 rate-limit 节点 → `limited`（锚定匹配
  `Reached … rate limit` 开头的 tool 节点；exec 输出里引用错误
  文本不算，防误伤）
- 尾部停在回合中途（tool/带 tool_calls 的 assistant）→ `midturn`
- **新鲜度**：最新节点时间戳静默超过 `EXT_FRESH`(600s) 才算死——
  正在跑的回合持续写节点，活的 midturn（长 exec、思考步）尾部
  长得一模一样，fresh tail 一律不碰。

然后按锁分级：
- **无锁/死锁**（锁文件 PID 已死）：直接停泊，到点一次性
  `devin acp` → `session/load` → 发"继续"提示 → 回合结束即销毁。
  无人所有即无人会重试，接管总是对的。
- **活锁**（前端进程还在）：只登记为 `suspect`——`!limits` 可见
  "前端占锁，仅观察"，不自动接管。锁死后自动晋升恢复正常；
  用户自己复活它（activity 前进）→ `superseded` 出列；
  超窗未动 → `expired`。`BRIDGE_LIMIT_EXT_STEAL=1`（默认关）
  才会摘除锁文件接管，且接管前再复核一次尾部确实死了。
- 每轮扫描最多收 6 个，恢复后频道发一行汇总

### 切号提前重试
限速按账号计费；桥跟随 Desktop 当前登录的凭据。sweeper 每轮
对凭据指纹做漂移检查（`_acct_fingerprint`），变了就把所有
非 suspect 停泊项的 `resume_at` 提到现在（+3–15s 抖动）——
停泊的 reset 窗口是旧账号的配额，新账号是新额度，没必要等。
事件 `acct_switch`。

### 观测
- `.bridge_limits.log`（JSONL）：`parked`（带当时 live_sessions /
  active_turns）、`unparked`、`resume_attempt`、`external_*`、
  `external_resumed`、`external_fail`
- `!limits`（线程内）/ "限额" / "限流"：冷却队列 + 最近事件
- 撞线时记录的 `live_sessions`/`active_turns` 即是"怎么到上限的"
  数据面——攒几次就能读出实际触发阈值

## 配置（env.example 有全量注释）

`BRIDGE_LIMIT_RESUME_MAX=8` `BRIDGE_LIMIT_BACKOFF=30`
`BRIDGE_LIMIT_BACKOFF_CAP=300` `BRIDGE_LIMIT_RESUME_GAP=15`
`BRIDGE_LIMIT_SWEEP_SEC=15` `BRIDGE_LIMIT_ADOPT_AGE=21600`
`BRIDGE_LIMIT_EXTERNAL=1` `BRIDGE_LIMIT_EXT_STEAL=0`
`BRIDGE_LIMIT_EXT_WINDOW=3600` `BRIDGE_LIMIT_EXT_ORPHAN_WINDOW=21600`
`BRIDGE_LIMIT_EXT_SCAN_SEC=90`
`BRIDGE_LIMIT_EXT_MAX=6` `BRIDGE_LIMIT_EXT_POST=1`
`BRIDGE_LIMIT_EXT_FRESH=600`

## 已知边界

- 外部 session 的"回合级"错误不落 message_nodes；只靠 transcript
  尾迹判断，未留痕的死法（直接 -32603 且 tail 又像 idle）识别
  不到——stale midturn 兜底覆盖了主要形态；回合死在 prompt 落库
  之前（tail 完好 idle）则完全不可见，只能等前端侧重试
- 活锁 session 默认只观察不接管：Desktop 进程持有的会话即使回合
  死了，抢锁会让另一边前端看到锁丢失；要么用户在前端里回一句
  让它自己 retry，要么显式开 EXT_STEAL
- 外部恢复若发生在桥死亡中途，该次恢复尝试会随 acp 进程消失；
  下一轮扫描按 tail 状态重新收编
- RPM 限速下治本手段是降聚合请求速率（错峰已在做；进一步可加
  BRIDGE_MAX_ACTIVE_TURNS 或按模型分流，另见
  `DEVIN_REFUSAL_FALLBACK`）
