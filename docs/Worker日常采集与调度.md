# Worker 日常采集与调度

Daily Bar 的 Provider 请求与 Raw lineage 仍逐证券独立，但验证后的数据库事实按
`DAILY_BAR_WRITE_BATCH_SIZE`（默认 100，1..500）有界成批提交。每批单事务，失败整批回滚并让
工作流失败；日志中的 `daily_bar_commit` 给出位置、run/row 数和提交耗时。不要用超大批次绕过
内存背压或延长锁持有时间。

全市场任务中，少量证券的 provider 失败或请求不可用保留为明确缺口，Daily Bar job 与
`daily_market` workflow 记为 `partial`，成功证券照常提交，使依赖方可以按既有
`succeeded`/`partial` 门槛继续。若请求非空且没有任何证券成功，或数据库批量事务失败，任务
仍记为 `failed`。系统不使用其他证券、日期或 provider 填补缺口。

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

龙虎榜任务在受控目录中固定为周一至周五 20:30，但默认
`TRADING_BILLBOARD_ENABLED=false`，生产环境必须在东财来源权利审查留档后才可启用。显式手工采集为
`market-data-center trading-billboard-collect --trade-date YYYY-MM-DD --confirm-eastmoney-source-terms-reviewed`；
回填使用 `--start-date/--end-date` 且最长 366 个自然日。任务只采每日上榜证券汇总与买入/卖出前五
席位，Raw 路径按 `eastmoney/trading_billboard/YYYY/MM/DD/<ingestion_id>.jsonl` 分区，schema 为
`eastmoney.trading_billboard.v1`。非交易日以零行成功结束；失败不跨日期、不切换来源拼批次。

每天 20:00（包括周末）执行扣非净利润增量同步。该任务按披露变化发现受影响证券，不按
交易日触发，也不进行全市场历史回填；详见 ADR-0020。

股东人数增量任务登记为每天 21:00（包括周末）运行，读取截至当天最近 30 个自然日的
Tushare 公告窗口；`SHAREHOLDER_COUNT_DAILY_ENABLED=false` 为默认值，只有运维显式启用后
Worker 才注册该任务。接口命中 3000 行上限时按公告日期递归拆分，单日全市场仍满额时改为
逐证券查询；一次日增量的全部请求批次在同一事务发布，任何分支失败都不写入部分事实。
该调度只做滚动增量，不自动启动全历史回填。

全历史回填是单独的受控命令，按包含已退市股票的标准 symbol 排序逐证券提交，可用
`--resume-after-symbol` 从已完成证券之后继续。交互运行会打印范围并要求输入确认；无人值守
必须显式给出 `--yes`。以下命令只展示调用方式，不代表已对生产环境执行：

Tushare 返回字段存在但 `holder_num` 为空的行时，Worker 保留 Raw、登记
`shareholder_count.missing_source_value` 质量拒绝并跳过 Core 写入；同批合法行继续提交。
零、负数、非整数字符串或字段缺失仍会硬失败。

```bash
uv run market-data-center shareholder-count-daily --as-of-date 2026-08-24 --provider tushare
uv run market-data-center shareholder-count-backfill --cutoff-date 2026-08-24 --yes --provider tushare
```

周一至周五 21:00 构建沪深主板昨日涨停与昨日跌停两份不可变股票池。触发时间不是依赖
完成的证明：任务还会检查 basis 当日 `daily_market`、`stock_daily_indicator` WorkflowRun
均成功，并校验精确交易日、日 K、每日指标和 lineage；缺失时失败且不回退旧快照。

固定板块 `THS:883423` 的日线由 Worker 在工作日 15:30、16:30、17:30 提供三个收盘后
执行机会。每轮先比较统一交易日历的最近应有交易日和数据库最新板块日线：已覆盖则幂等
跳过，有尾部缺口才通过 `akshare_ths` 标准 Pipeline 采集。每轮 Provider 失败最多短重试
三次，成功即停止；当天最终仍失败时，下一交易日继续从最新已存日期补采。调度时间固定在
代码目录，`.env` 只可用 `BOARD_INDEX_DAILY_BAR_ENABLED` 启用或停用。

收盘五档任务默认启用，在工作日 21:10 运行，只读取当天最新
`ready` 涨停池，保存原始 JSONL、Manifest、质量结果和标准快照。当天池缺失时失败，空池
合法跳过；任务禁止把当前实时报价写成其他历史日期，也不会回退旧股票池。

集合竞价涨停池五档默认启用，工作日 09:15 启动并按 30 秒节奏采样至 09:25，每只股票
单独发起一次 PYTDX 请求。独立的沪深全市场竞价序列任务也在 09:15 启动，冻结当日 SSE/SZSE
`stock`、`listed` 全集，从 09:15:00 到 09:25:20（含首尾）每 20 秒采一轮，共 32 轮；每批最多
80 只，每轮最多使用两个 endpoint 做完整全集 attempt，partial 结果不跨 endpoint 拼接。错过的轮次
显式记为 failed，最后一轮 deadline 为 09:25:40，不补采过去时槽。两个 09:15 任务只在专用
`morning_auction` 两线程 executor 内并行，其他 Worker 任务仍使用单线程 executor。另有沪深全市场
开盘竞价来源采集任务在工作日 09:25:30 运行：只采集 `SSE`、`SZSE` 的 `stock`、`listed` 证券，BSE
暂缓，ETF、可转债和指数不进入集合。每次尝试固定一个 quote-capable endpoint，按最多 80 只分批；
每个 endpoint 只允许形成完整全集，至多进行两次完整尝试，绝不拼接 endpoint 的 partial 结果。新
请求的硬截止为 09:29:30；09:30 后观察到的记录不能进入成功快照。失败或 partial 尝试仍保留 Raw、
Manifest、质量结果和 ingestion lineage。

项目所有者已移除工作日 21:30 “今日竞价量”自动最终化，不提供替代调度、环境时间或 OS 计划任务。
数据库最终化实现和历史 workflow code 仅作为非调度的内部能力保留。`CALL_AUCTION_SNAPSHOT_ENABLED`
只控制 09:25:30 来源采集。该数据集的来源 Raw 继续长期保留，但 operational Raw replay 暂停；只有持久化并
验证原始冻结 SSE/SZSE listed-stock 全集的确定性身份后，才可通过后续接受决策重新启用。

Worker 启动时先探测一个有界候选集，按 quote、SSE 日 K、SZSE 日 K 和 BSE 日 K 能力生成
统一的版本化 PYTDX 节点池，之后每 1 小时刷新。刷新失败时继续使用最后一个有效池；首次
启动且新旧池均不可用时拒绝启动。消费者按能力筛选节点，建立会话时有限 failover，成功
会话不切换 endpoint。公共节点无 SLA，可能限流、下线或缺少 BSE。Raw、节点池和 JobStore
必须使用持久目录，例如：

```dotenv
PYTDX_POOL_PATH=/var/lib/market-data-center/pytdx_pool.json
PYTDX_VIPDOC_PATH=D:\new_tdx64\vipdoc
RAW_DATA_ROOT=/var/lib/market-data-center/raw
SCHEDULER_STORE_PATH=/var/lib/market-data-center/scheduler/jobs.sqlite
EOD_QUOTE_SNAPSHOT_ENABLED=true
CALL_AUCTION_SNAPSHOT_ENABLED=true
CALL_AUCTION_MARKET_SERIES_ENABLED=true
TRADING_BILLBOARD_ENABLED=false
BOARD_INDEX_DAILY_BAR_ENABLED=true
SHAREHOLDER_COUNT_DAILY_ENABLED=false
```

### 竞价序列诊断与保留

`realtime.call_auction_market_series_session` 保存冻结全集和整体状态，`round` 保存 32 个计划时槽及
attempt 选择，按月分区的 `snapshot` 保存来源事实。可先检查 session/round，再按 selected ingestion
追踪 Raw、Manifest 和质量结果：

```sql
select trade_date,status,expected_rounds,succeeded_rounds,partial_rounds,failed_rounds,
       universe_count,error_summary
from realtime.call_auction_market_series_session
order by started_at desc limit 5;

select sample_seq,scheduled_at,status,attempt_count,successful_quotes,failed_quotes,error_summary
from realtime.call_auction_market_series_round
where session_id = :session_id order by sample_seq;

select to_regclass('realtime.call_auction_market_series_snapshot_' ||
                   to_char(current_date, 'YYYYMM')) as current_partition;
```

`partial` 且 `attempt_count=2` 通常表示两个节点都未完整覆盖冻结全集；`missed_sampling_round` 表示
Worker 到达时已经越过该轮 deadline；节点池为空会在建立 Session 前失败；分区查询返回 null 时停止
Worker 并走受保护 migration，Worker 自身不得执行 DDL。Raw 和 Manifest 长期保留。在线事实只保留最近
12 个完整月份：常规发布通过新的 ordered migration 先创建后续月份，确认 Raw/Manifest/备份可恢复后，
才允许在受控窗口 drop 更早分区。初始分区覆盖至 2027-09-30，必须在 2027-09 前发布后续分区 migration；
APScheduler job、Worker 和 `.env` 均不得创建或删除分区。

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

GitHub `production` Environment 需要配置 `MIGRATION_DATABASE_URL` 和 `DATABASE_URL`。仓库只引用 Secret 名称，不保存值。Environment 审批、备份确认和凭据轮换可在功能闭环完成后继续加固。
