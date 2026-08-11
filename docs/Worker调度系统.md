# Worker 调度系统

> 对应代码：`src/market_data_center/scheduler.py`、`scheduling_catalog.py`、`worker_admin.py`、`settings.py`
> 治理依据：宪法第 11 条（进程内调度）、ADR-0017（统一 Worker 进程内调度）、ADR-0018（Worker 本地只读管理页面）、ADR-0016（跨平台调度与运行可靠性）

本文档梳理 Worker 常驻进程的完整工作原理：它如何启动、如何调度定时任务、每个任务做什么、如何自愈、如何观测。

## 一句话定位

**Worker 是项目唯一的常驻采集进程，所有定时任务都在它内部的 APScheduler 里执行，不使用任何操作系统级计划任务（宪法第 11 条）。** 操作系统层（systemd / Windows 脚本 / 容器编排）只负责让 Worker 进程活着，不负责定时触发。

## 架构总览

```
                    ┌─────────────────────────────────────┐
                    │   market-data-center worker          │
                    │   (常驻进程, 单实例)                  │
                    └────────────────┬────────────────────┘
                                     │
        ┌────────────────────────────┼─────────────────────────┐
        │                            │                         │
┌───────▼──────────┐    ┌───────────▼──────────┐   ┌──────────▼──────────┐
│ PostgreSQL        │    │ APScheduler          │   │ Worker Admin Page   │
│ advisory lock     │    │ (进程内调度)          │   │ 127.0.0.1:8765      │
│ (单实例保证)       │    │                      │   │ (只读 HTML, ADR-0018)│
└───────────────────┘    │ JobStore: SQLite     │   └─────────────────────┘
                         │ Executor: 单线程      │
                         │ 10 个定时 job         │
                         └──────────┬───────────┘
                                    │ 按时触发
                    ┌───────────────┼───────────────┐
                    │               │               │
              ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
              │ 业务 service│   │ operations │   │ Raw Store │
              │ (采集/计算) │   │ 表(执行记录)│   │ (可追溯)  │
              └───────────┘   └───────────┘   └───────────┘
```

### 核心设计约束

| 约束 | 实现 | 作用 |
|---|---|---|
| **单实例** | PostgreSQL advisory lock `market-data-center:scheduler`（`pg_try_advisory_lock`） | 全集群只有一个 Worker 持锁运行，第二个启动即退出 |
| **单线程执行** | `ThreadPoolExecutor(max_workers=1)` + 每个 job `max_instances=1` + `coalesce=True` | 任意时刻最多 1 个 job 在跑；积压多次合并为 1 次 |
| **持久化调度** | `SQLAlchemyJobStore`（SQLite `data/scheduler/jobs.sqlite`） | Worker 重启后调度状态不丢 |
| **可靠执行记录** | 每个 job 包成 `WorkflowExecutionService.start().succeed()/fail()`，写入 `operations` schema | 真正的执行历史在 PostgreSQL，不在 SQLite |
| **优雅退出** | `SIGTERM`/`SIGINT` → `scheduler.shutdown(wait=True)` | 等当前 job 跑完再退出 |
| **进程内调度** | 全部 job 在 Worker 进程的 APScheduler 里（宪法第 11 条） | 无 OS 级计划任务 |

## 生命周期（`run_worker`）

启动 `market-data-center worker` 后的完整流程：

1. **配置日志** → 实例化 `SchedulerSettings`
2. **若 `--check`**：只读健康检查，打印 JSON 报告后退出（退出码 0=健康/1=不健康），供探针使用
3. **注册信号处理**：`SIGINT`/`SIGTERM` → 优雅关闭
4. **获取 PostgreSQL advisory lock**（拿不到即抛异常退出 → 单实例保证）
5. **锁内依次**：
   - **立即同步执行一次** `run_stale_recovery_job`（清理上次进程残留的 running 记录）
   - **刷新 PYTDX 节点池**：探测并原子发布；失败时使用 last-good；新旧池均无效则拒绝启动
   - 构建调度器 `build_scheduler()`，注册每 12 小时刷新任务和其他启用任务
   - 启动 Worker Admin Page（`127.0.0.1:8765`）
   - `scheduler.start()`（**阻塞**，直到收到 shutdown）
6. **退出时**（finally）：关 admin page、关调度器、释放锁、释放连接池

## 调度核心（`build_scheduler`）

```
BlockingScheduler
├── jobstores: { default: SQLAlchemyJobStore(SQLite) }   ← 持久化
├── executors: { default: ThreadPoolExecutor(max_workers=1) }  ← 单线程
├── timezone:  Asia/Shanghai
└── jobs:      遍历 job_definitions(settings)，enabled 的才 add_job
               add_job(..., replace_existing=True, coalesce=True,
                       max_instances=1, misfire_grace_time=timeout)
```

**触发器生成**（`_trigger`，scheduler.py:384-396）：
- `cron` 类型 → `CronTrigger(day_of_week, hour, minute, timezone)`
- `interval` 类型 → `IntervalTrigger(hours=N, timezone)`

## 定时任务目录（9 个 job）

任务定义在 `scheduling_catalog.py`。每个 job 有：`code`（APScheduler job id）、`display_name`、`workflow_code`、`trigger_type`、计划时间、`enabled`、`timeout_seconds`、`recovery_policy`。

### 任务一览（按业务时间顺序）

| # | Job ID | 名称 | Workflow | 触发 | 默认时间 | 启用 |
|---|---|---|---|---|---|---|
| 1 | `opening-auction-limit-up-quotes` | 集合竞价涨停池五档采集 | `auction_collection` | cron 周一至周五 | 09:15 | ✅ |
| 2 | `call-auction-market-snapshot-daily` | 沪深全市场开盘竞价快照 | `call_auction_market_snapshot` | cron 周一至周五 | 09:26 | ✅ |
| 3 | `eod-quote-snapshot-daily` | 收盘五档快照 | `eod_quote_snapshot` | cron 周一至周五 | 21:10 | ✅ |
| 4 | `daily-run` | 日 K 与基础数据更新 | `daily_market` | cron 周一至周五 | 20:00 | ✅ |
| 5 | `stock-daily-indicators-daily` | 股票每日指标更新 | `stock_daily_indicator` | cron 周一至周五 | 20:30 | ✅ |
| 6 | `mainboard-price-limit-stock-pools-daily` | 沪深主板昨日涨跌停股票池 | `stock_pool` | cron 周一至周五 | 21:00 | ✅ |
| 7 | `today-limit-up-snapshot-daily` | 同日涨停不可变快照 | `today_limit_up_snapshot` | cron 周一至周五 | 22:00 | 默认关闭 |
| 7 | `deducted-profit-daily` | 扣非净利润增量同步 | `deducted_profit` | cron 每天 | 20:00 | ✅ |
| 8 | `recover-stale-ingestion-runs` | 陈旧运行恢复 | `stale_run_recovery` | interval | 每 1 小时 | ✅ |
| 9 | `pytdx-pool-refresh` | PYTDX 节点池刷新 | `pytdx_pool_refresh` | interval | 每 12 小时 | ✅ |

> 时间与调度策略固定在 `scheduling_catalog.py`，不能通过 `.env` 覆盖。09:26 任务在上海时间 09:25--09:30 窗口内采集；09:29:30 后不发起新请求。项目所有者已移除 `call-auction-snapshot-daily` 及其 21:30 自动最终化，且没有替代计划、环境时间、cron、timer 或 Windows Task Scheduler。`CALL_AUCTION_SNAPSHOT_ENABLED` 只启停 09:26 来源采集。旧 SQLite JobStore 中该精确 job ID 会在构建 Scheduler 时清理。

### 每个 job 做什么（scheduler.py 里的执行函数）

所有 job 遵循同一模式：开一条 `operations` 工作流记录 → 执行业务 → 成功 `succeed()`/失败 `fail()`。这样任意中断都在 `operations` 表留下状态，供 stale recovery 清理。

| Job 函数 | 做什么 |
|---|---|
| `run_daily_market_job` | 同步 security + trading_calendar + 远程 pytdx 日K（`bar_lookback_days=1, calendar_lookback_days=14`，单分片） |
| `run_stock_daily_indicator_job` | tushare 全市场每日指标同步 + Core 保留策略（retention） |
| `run_stale_recovery_job` | **3 步**：恢复 stale ingestion run（>60min）、恢复 stale workflow run、恢复过期 auction session。每小时 + 启动时各跑一次 |
| `run_deducted_profit_job` | tushare 扣非净利润增量同步（按披露变化发现新公告/修订） |
| `run_stock_pool_job` | 解析基准交易日 → 构建下一交易日生效的涨跌停股票池（依赖当日日K+指标成功） |
| `run_auction_collection_job` | pytdx_hq 集合竞价五档采集（09:15-09:25 按 5 秒节奏采样，默认启用） |
| `run_eod_quote_snapshot_job` | 对当日 ready 涨停池采集收盘五档快照（默认启用） |
| `run_call_auction_market_snapshot_job` | 09:26 从一个 quote-capable endpoint 采集 SSE/SZSE `stock`、`listed` 全集的开盘竞价来源快照；BSE、ETF、可转债和指数不进入本任务 |
| `run_pytdx_pool_refresh_job` | 有界探测候选节点能力；成功时原子发布，失败时保留 last-good |

## 自愈与可靠性（ADR-0016）

**陈旧运行恢复**是 Worker 的自愈机制，处理"进程崩溃后留下的 running 状态记录"：

- `run_stale_recovery_job`（每小时 + Worker 启动时立即一次）
- 阈值：超过 **60 分钟**仍 `running` 的 ingestion/workflow/auction 记录
- 动作：标记为 failed，释放占用的状态，让下次调度能正常重试
- 不回填历史数据，只清理状态

**misfire 处理**：代码目录固定 `misfire_grace_time = 21600` 秒（6 小时）。错过触发时间的 job 在宽限期内仍会补跑，超期则跳过。

## 配置边界

执行时间、时区、采样节奏、misfire/timeout 和 interval 均由受控代码目录固定。`.env` 不含任务 hour/minute，只保留运行路径、管理端口和三个可选任务开关：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `SCHEDULER_STORE_PATH` | `data/scheduler/jobs.sqlite` | APScheduler 持久化路径 |
| `WORKER_ADMIN_PORT` | `8765` | 管理页面端口 |
| `AUCTION_COLLECTION_ENABLED` | `true` | 集合竞价采集开关 |
| `EOD_QUOTE_SNAPSHOT_ENABLED` | `true` | 收盘五档任务开关 |
| `CALL_AUCTION_SNAPSHOT_ENABLED` | `true` | 只控制 09:26 沪深全市场开盘竞价来源采集 |
| `TODAY_LIMIT_UP_SNAPSHOT_ENABLED` | `false` | 只控制 22:00 同日涨停快照；迁移和出站预检前保持关闭 |
| `PYTDX_POOL_PATH` | `data/pytdx_pool.json` | 统一版本化能力节点池路径；生产使用持久化绝对路径 |

## 健康检查（`worker --check`）

只读、不启动常驻进程，打印 JSON 报告并以退出码反映健康度（0=健康/1=不健康）：

```bash
market-data-center worker --check
```

**`SchedulerHealthReport`**（scheduler.py:51-57）字段：
- `healthy: bool` — 综合判定
- `persisted_job_ids` — SQLite JobStore 里实际持久化的 job id
- `stale_run_count` — 超过 60min 仍 running 的记录数
- `latest_snapshot_date` / `latest_snapshot_rows` — 最新每日指标快照

**healthy 判定**（全部 AND）：
1. JobStore 可读
2. 所有 enabled job 都已持久化（`expected ⊆ persisted`）
3. 无 stale run
4. 最新快照行数 > 0
5. 快照日期在 10 天内

> 注意：`healthy=false` 不一定是故障——刚部署还没跑过、或数据有点旧都会导致。它反映"调度器是否就绪且数据新鲜"。

## 管理页面（ADR-0018）

**地址**：`http://127.0.0.1:8765/admin/scheduled-tasks`（仅 Worker 运行时可访问）

**技术栈**：Python 标准库 `http.server`（不引入 FastAPI/Flask，ADR-0018 第 1 点）

**展示内容**：
- 摘要卡片：Worker 存活、调度健康、JobStore 可读、陈旧运行数、最新快照日期
- 定时任务表：10 个任务的 ID/名称/描述/Workflow/步骤/触发类型/计划/状态/超时恢复/下次运行/已持久化
- 最近工作流执行：最近 10 条 operations 记录（workflow/status/attempt/触发来源/起止/行数/错误）

**安全约束**（ADR-0018）：
- 硬编码 `127.0.0.1` 回环，不可配置外部绑定
- **只读**：POST 返回 405，无任何写入/启停/删除/立即执行能力
- SQLite 以 `mode=ro` 只读打开，不读取 `job_state`（pickle blob）
- 严格 CSP 安全头，不泄露路径/凭据/密钥
- 认证由 SSH 回环端口转发承担（页面本身不认证）

## 运维操作

### 启动 Worker
```powershell
# Windows（项目入口，同时启动 API + Worker）
.\serve.cmd

# 或只启动 Worker
.\.venv\Scripts\market-data-center.exe worker
```
> Linux 用 systemd（见 `deploy/linux/market-data-center-worker.service`）。

### 查看管理页面
浏览器打开 `http://127.0.0.1:8765/admin/scheduled-tasks`

### 健康检查
```powershell
.\.venv\Scripts\market-data-center.exe worker --check
```

### 手动触发一次性采集（不经 Worker 调度）
```powershell
.\.venv\Scripts\market-data-center.exe daily-run --as-of-date 2026-08-05
```

### 停止 Worker
在 Worker 进程窗口按 `Ctrl+C`（优雅退出，等当前 job 跑完）。

## 关键文件

| 文件 | 职责 |
|---|---|
| `src/market_data_center/scheduler.py` | 调度核心：`run_worker`、`build_scheduler`、job 执行函数、健康检查 |
| `src/market_data_center/scheduling_catalog.py` | 任务与 Workflow controlled catalog |
| `src/market_data_center/worker_admin.py` | 管理页面：标准库 HTTP server + HTML 渲染 |
| `src/market_data_center/settings.py` | `SchedulerSettings` 配置 |
| `src/market_data_center/persistence/postgres.py:439-453` | advisory lock 实现（单实例） |
| `src/market_data_center/persistence/operations_postgres.py` | workflow 执行记录读写 |
| `deploy/linux/market-data-center-worker.service` | systemd 单元（Linux 部署） |
| `serve.cmd` / `deploy/windows/start-services.ps1` | Windows 启动脚本 |
