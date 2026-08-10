# Worker 日常采集与调度

第一阶段使用 `daily-run` 完成每天的行情入库，不由 GitHub Actions 调度生产采集。一次运行按固定顺序执行：

1. 同步证券主数据；
2. 同步截至运行日的统一 A 股日历；
3. 确定截至运行日的最近交易日；
4. 只对该交易日尚无事实的上市股票读取远程 pytdx Daily Bar，并通过自然键幂等写入。

`daily-run` 不回看、不修复历史日 K，也不自动计算 Derived/Metrics。停牌或本地文件在当日没有记录的证券记为 `unavailable`，不会导致整批失败；文件损坏、市场哨兵陈旧、数据库错误仍会使任务失败。

Provider 的原始响应先写入不可变 Raw 对象，再执行 Record DTO 标准化。若标准化失败，采集批次、Raw Manifest 和阻断级 `QualityResult` 会一起落库，Core 不写入该批次，因此错误来源仍可追溯和重放。

默认日历窗口为最近 14 个自然日。周末或节假日运行时取窗口内最近的实际交易日。兼容参数 `--bar-lookback-days` 仅保留命令行兼容性，不再触发历史修复。

## 手工运行

数据库连接和本地 Raw 根目录由环境变量注入：

```bash
market-data-center daily-run
market-data-center daily-run --as-of-date 2026-07-29
market-data-center daily-run --calendar-lookback-days 30
```

默认使用 ADR-0005 的自动路由。为了可复现诊断，可以显式使用支持全部三个数据集的 `baostock` 或 `akshare`：

```bash
market-data-center --provider baostock daily-run --as-of-date 2026-07-29
```

`pytdx` 只支持 Daily Bar，不能单独承担完整的 `daily-run`；自动模式在 Daily Bar 步骤固定使用远程 pytdx。停牌、节点缺数或不支持 BSE 产生的缺口保持可见，不通过 BaoStock/AKShare 补数。

pytdx 还可从通达信 `T0002/hq_cache` 读取行业和概念完整快照，但 Security 和 Trading Calendar 仍由 BaoStock/AKShare 提供。分类成员引用尚未进入 Security 的代码时，整个成员快照按质量规则阻断，不能静默删掉未知成员。

`daily-bars-bulk` 仍保留为显式人工工具，但不进入日常调度。当前项目决策是不再补历史日 K；除非项目所有者再次明确要求，不运行该命令。

## 跨平台调度

全部生产定时任务由统一的 `market-data-center worker` 进程负责，APScheduler 只是其内部
组件：工作日 20:00 执行普通 `daily-run`，
20:30 执行 Tushare 每日指标，并每小时恢复超时停留在 `running` 的采集批次。单线程执行器
保证任务不重叠，PostgreSQL advisory lock 保证同一时刻只有一个 Scheduler 实例持有主锁。

每天 20:00（包括周末）执行扣非净利润增量同步。该任务按披露变化发现受影响证券，不按
交易日触发，也不进行全市场历史回填；详见 ADR-0020。

周一至周五 21:00 构建沪深主板昨日涨停与昨日跌停两份不可变股票池。触发时间不是依赖
完成的证明：任务还会检查 basis 当日 `daily_market`、`stock_daily_indicator` WorkflowRun
均成功，并校验精确交易日、日 K、每日指标和 lineage；缺失时失败且不回退旧快照。

收盘五档任务默认关闭；完成 pytdx 收盘盘口字段与数量单位的实盘验证后，通过
`EOD_QUOTE_SNAPSHOT_ENABLED=true` 显式开启。任务在工作日 21:10 运行，只读取当天最新
`ready` 涨停池，保存原始 JSONL、Manifest、质量结果和标准快照。当天池缺失时失败，空池
合法跳过；任务禁止把当前实时报价写成其他历史日期，也不会回退旧股票池。

pytdx 要求 `PYTDX_DAILY_BAR_ENDPOINTS` 指向有序、人工验收的 `host:port` 列表。连接和
读取采用有限超时，建立会话时有限 failover；成功会话不切换 endpoint。公共节点无 SLA，
可能限流、下线或缺少 BSE。Raw 与 JobStore 必须使用持久目录，例如：

```dotenv
PYTDX_DAILY_BAR_ENDPOINTS=<host1>:7709,<host2>:7709
RAW_DATA_ROOT=/var/lib/market-data-center/raw
SCHEDULER_STORE_PATH=/var/lib/market-data-center/scheduler/jobs.sqlite
```

## 股票每日指标定时采集

Tushare 每日指标由 APScheduler 进程在周一至周五 20:30 触发。Worker 先用 Tushare 同步
当日交易日历；当日休市时直接跳过，开市时按 `trade_date` 一次获取全市场快照。采集
成功或部分成功后，删除 Core 中早于一个自然月截止日的每日指标。Raw、Manifest、
IngestionRun 和 QualityResult 不删除。

项目根目录的 `.env` 必须配置 `DATABASE_URL` 和 `TUSHARE_TOKEN`。应用最新 migration
并安装依赖后可直接前台启动：

```bash
uv sync --all-groups
uv run market-data-center worker
```

Worker 内部使用 `data/scheduler/jobs.sqlite` 持久化任务状态。Linux 部署将项目安装在
`/opt/market-data-center`，复制 `deploy/linux/market-data-center-worker.service` 到
`/etc/systemd/system/` 后执行：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now market-data-center-worker
sudo systemctl status market-data-center-worker
```

只读健康检查可用于 systemd、容器或外部监控探针：

```bash
uv run market-data-center worker --check
```

退出码 `0` 表示全部启用任务均已持久化、没有超过一小时的陈旧运行，且最近每日指标快照
不为空且不超过十天；否则退出码为 `1`，并输出 JSON 诊断信息。

Worker 运行时还提供仅绑定本机 loopback 的只读任务页面：

```text
http://127.0.0.1:8765/admin/scheduled-tasks
```

页面与安全边界详见 `docs/Worker本地只读管理页面.md`。

容器部署需要将 `data/scheduler` 和 Raw 根目录挂载为持久卷，并以
`market-data-center worker` 作为容器主进程。不要同时启动多个 Worker 实例；
现有 PostgreSQL advisory lock 是最后的重复执行保护，不替代单实例部署约束。

手工执行同一工作流或显式清理：

```powershell
uv run market-data-center --provider tushare stock-daily-indicators-daily
uv run market-data-center stock-daily-indicator-retention --cutoff-date 2026-07-01
```

物理清理只影响 `core.stock_daily_indicator` 且使用排他截止日，即保留截止日当天；
详见 ADR-0015。

## PostgreSQL 集成测试

不得在生产数据库上执行集成测试。`deploy/testing/compose.yml` 提供临时 PostgreSQL 17；
测试夹具会为每组用例创建独立数据库，脚本结束时删除容器和临时卷：

```bash
./deploy/testing/run-postgres-integration.sh
```

Windows 使用 `deploy/testing/run-postgres-integration.ps1`。两者都会设置仅限本地测试的
`TEST_DATABASE_URL` 并执行 `uv run pytest -m integration`。

## 生产 migration 与 smoke check

`.github/workflows/production.yml` 提供人工触发的生产工作流。`check` 只核对 migration 版本、Schema、RLS、数据库事实和 PostgREST；`apply` 先应用尚未执行的 migration，再完成同一组检查。

基础检查要求 Security、Trading Calendar、股票 Daily Bar、Raw 和成功批次非空，并探测当前全部 `api_v1` 视图。完成 THS 初始化后，使用严格模式验证板块定义、日 K 和成分快照也已进入数据库与 API：

```bash
uv run python scripts/smoke_check.py --require-board-index
```

GitHub `production` Environment 需要配置 `MIGRATION_DATABASE_URL`、`DATABASE_URL`、`SUPABASE_URL` 和 `SUPABASE_PUBLISHABLE_KEY`。仓库只引用 Secret 名称，不保存值。Environment 审批、备份确认和凭据轮换可在功能闭环完成后继续加固。
