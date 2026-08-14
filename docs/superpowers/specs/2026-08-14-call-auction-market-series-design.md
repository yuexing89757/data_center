# 沪深全市场开盘竞价序列快照设计

- 日期：2026-08-14
- 状态：Approved for planning
- GitHub Issue：#48
- 关联决策：ADR-0012、ADR-0017、ADR-0022、ADR-0026、ADR-0027、ADR-0028、ADR-0034

## 目标

在工作日上海时间 09:15:00 至 09:25:20（含端点）期间，每 20 秒记录一次沪深上市股票全集的开盘集合竞价来源快照。新数据集与现有 09:26 `realtime.call_auction_market_snapshot` 完全隔离，不改变后者的表、任务、开关、API 或一字板语义。

新数据集只保存客观来源事实：最新价、昨收、截至观察时点的最高价/最低价、累计成交量/额，以及精确的计划时间、观察时间和 ingestion lineage。首版不发布公共查询接口，不将来源字段解释为逐笔、Level-2 或真实连续五档委托簿。

## 范围

包含：

- 一个由 Worker 代码目录控制的工作日 09:15 长会话任务；
- 32 个内部采样点：`09:15:00 + sample_seq × 20s`，`sample_seq=0..31`；
- 会话启动时冻结一次 `SSE/SZSE + stock + listed` 证券全集；
- 单 endpoint、每批最多 80 只的 PYTDX 顺序请求和有界 endpoint failover；
- 独立 session、round 和月度分区 snapshot 表；
- Raw JSONL、Manifest、质量结果、IngestionRun、WorkflowRun 和 JobExecution lineage；
- 与现有涨停池五档任务隔离的两线程早盘执行器；
- PostgreSQL 12 个月在线保留和 Raw 长期保留。

不包含：

- BSE、ETF、可转债、指数；
- FastAPI、PostgREST RPC、公开 View 或 Agent 工具；
- 对现有 09:26 数据集、任务或读取契约的修改；
- OS cron、systemd timer、Windows Task Scheduler；
- 秒级补采、历史回填、跨 endpoint 拼接或 Raw replay；
- 主观标签、策略、回测和逐笔/Level-2 语义。

## 任务与并发

新增任务 `call-auction-market-series` 和 workflow/dataset code `call_auction_market_series`。任务由 `CALL_AUCTION_MARKET_SERIES_ENABLED` 独立控制并默认启用；环境变量只允许启用或停用，09:15、09:25:20 和 20 秒 cadence 均固定在受控代码目录。

APScheduler 仍只注册一个 09:15 cron job。该 job 在一个长会话内生成 32 个计划点，不注册 32 个子 job。新增命名执行器 `morning_auction`，`max_workers=2`，仅分配：

1. 现有 `opening-auction-limit-up-quotes`；
2. 新 `call-auction-market-series`。

两项任务各自 `max_instances=1`，使用独立数据库 Engine、PYTDX连接和 Raw 写入。默认执行器继续 `max_workers=1`，盘后与恢复任务的并发边界不变。现有 09:26 job 仍使用默认执行器；新任务最后一轮的硬截止不晚于 09:25:40，为 09:26 留出至少 20 秒隔离时间。

## 冻结全集

会话开始时从 Core 读取按标准 symbol 排序且唯一的沪深上市股票全集。全集必须非空，只允许 `SSE:`、`SZSE:` 股票。Session 持久化：

- 完整 `universe_symbols text[]`；
- `universe_count`；
- 对规范 JSON 数组计算的 SHA-256 `universe_hash`。

32 轮必须使用同一冻结全集。Worker 重启恢复时读取持久化全集，不重新查询 Core，不因证券状态后续变化改变历史会话输入。

## 每轮请求和截止时间

每轮在 `scheduled_at` 到达后开始。Provider 对冻结全集按标准顺序、每批最多 80 只调用 `get_security_quotes`。一个 IngestionRun 全程固定一个 quote-capable endpoint。

轮次硬截止为下一个 20 秒计划点；最后一轮硬截止为 09:25:40。若第一 endpoint 未形成完整全集，并且当前时间加连接/请求预算仍早于硬截止，可选择第二 endpoint，从全集第一只重新采集并创建新的 IngestionRun。最多两个 endpoint attempt，不允许合并两个 partial attempt。

在硬截止后不得发起新批次。正在进行的 Provider 必须通过 deadline 检查停止后续批次，成功事实和 Raw 仍按 partial attempt 保存。若上一轮延误导致后续计划点已错过，服务写入明确的 failed round，不请求历史行情、不回填、不伪造 `observed_at`。

## 数据模型

### `realtime.call_auction_market_series_session`

一行代表一个交易日和 workflow attempt 的长会话：

- `session_id`、`trade_date`；
- `window_start`、`window_end`、`cadence_seconds=20`、`expected_rounds=32`；
- `universe_symbols`、`universe_count`、`universe_hash`；
- `status`：running/succeeded/partial/failed；
- `started_at`、`finished_at`；
- 成功、部分、失败轮数及成功/失败报价总数；
- 有界 `error_summary`。

同一交易日的恢复或重新调度产生新的 session，不覆盖旧 session。

### `realtime.call_auction_market_series_round`

自然键 `(session_id, sample_seq)`：

- `sample_seq` 必须为 `0..31`；
- `scheduled_at` 必须等于 session start 加 `sample_seq × 20s`；
- `collected_at`、状态、attempt count；
- 预期、成功、失败报价数；
- `selected_ingestion_id` 指向成功 attempt，或没有成功时指向最终 partial attempt；
- 有界 `error_summary`。

所有 endpoint attempts 均保留在 `ingestion.ingestion_run`；round 只选择规范读取 lineage，不隐藏其他 attempt。

### `realtime.call_auction_market_series_snapshot`

事实字段与 09:26 表一致，并增加轮次身份：

- `trade_date`、`session_id`、`sample_seq`、`scheduled_at`；
- `ingestion_id`、`symbol`、`observed_at`；
- `last_price`、`previous_close`、`high_price`、`low_price`；
- `cumulative_volume`、`cumulative_amount`；
- `source_code='pytdx_hq'`、`created_at`。

表按 `trade_date` 月度 Range Partition。分区主键包含 `(trade_date, ingestion_id, symbol)`。父表索引支持 `(trade_date, sample_seq, symbol)` 和 ingestion lineage 查询。Snapshot 只允许 select/insert，不授权 update/delete；不同 attempt append-only 保存。

## 状态与质量

Round 只有同时满足以下条件才是 `succeeded`：

- response 与冻结全集一一对应；
- 无缺失、重复、未知或 BSE symbol；
- Raw 行数、标准记录数与冻结全集数量一致；
- 所有观察时间带时区，日期正确，且不晚于该轮硬截止；
- 价格、金额和数量满足现有 Decimal、元、股、非负和 OHLC 范围约束；
- Provider 未抛错且 normalization error 为空；
- 规范 attempt 来自一个 endpoint。

只要一轮不是 succeeded，整个 Session 不能标记 succeeded。Session 可以 partial，并完整保留每轮状态、Raw、Manifest、质量结果和错误摘要。空池、非交易日、冻结全集非法或没有 quote endpoint 时 Session 明确失败/跳过，不回退旧日期或旧全集。

## 存储与保留

当前约 5,208 只证券，每轮约 66 个请求，每交易日约 `5,208 × 32 = 166,656` 条规范事实，按 250 个交易日约 4,166 万行/年。

PostgreSQL 在线保留最近 12 个完整月份。初始 ordered migration 必须创建当前月及足够的未来月度分区；后续分区创建和过期分区删除只能通过 ordered migration 和受保护发布流程执行，Worker 永不执行 DDL。删除旧分区前必须验证对应 Raw、Manifest 和备份。Raw JSONL、Manifest、IngestionRun 和质量结果长期保留。

首版 Raw replay fail closed。即使 Session 已保存冻结全集，只有 replay 设计明确规范 ingestion attempt、Raw envelope 与目标 round 的一一关系后，才能通过后续 Accepted ADR 启用。

## 安全与读取

三个新表启用 RLS，仅 `market_data_worker` 获得完成采集所需的最小权限。匿名、authenticated、`market_data_api` 和 FastAPI 进程均不直接读取内部表。首版不修改任何已签入 OpenAPI/Agent/FastAPI contract。

## 验证

单元测试必须覆盖：

- 32 个精确采样点和阶段边界；
- 冻结全集排序、唯一性、哈希和恢复；
- 80 只分批及 5,208 规模顺序；
- 单 endpoint attempt、第二 endpoint 全量重试、禁止 partial 拼接；
- 20 秒 deadline、最后 09:25:40 deadline 和 missed round；
- 成功/partial/failed Session 汇总；
- 两个早盘 job 使用 `morning_auction`，其他 job 使用 default executor；
- 新开关只影响新任务，`.env` 时间变量无效。

PostgreSQL integration tests 必须使用隔离 `TEST_DATABASE_URL`，覆盖 migration、分区路由、FK、约束、RLS、grants、append-only、selected ingestion lineage 和事务失败回滚。生产迁移只能走受保护 workflow。

下一交易日 live gate 不手工补采：检查32轮计划与实际时间、每轮耗时、endpoint attempts、完整率、Raw/Manifest、质量结果、数据库行数及09:26任务不受影响。外部节点故障保持显式 partial，不把未验证运行称为生产成功。

## 实施边界

实施按 TDD 进行，顺序为领域模型和时槽、migration、Persistence、Provider/Service、Scheduler、生产检查与文档。不得在 `.env` 增加时间或 cadence，不创建 OS 调度，不修改现有09:26公共读取语义。
