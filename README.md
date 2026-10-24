# Market Data Center

竞价九合一的12类来源已接入apiServer“实时接口”：东方财富行情/股池/日K/题材，
腾讯分时/K线/指数，新浪日K。使用既有X-API-Key，请求时读取且不入库。
来源网站、上游地址、参数及示例见[实时接口说明](docs/竞价九合一来源实时接口.md)。

A phase-one A-share daily market data pipeline using Python 3.12 and PostgreSQL. Security and calendar use BaoStock/AKShare routing; pytdx quote and Daily Bar reads share one capability-aware endpoint pool maintained by the Worker.

Remote Daily Bars keep category 9 and the standard TDX protocol. The reviewed candidate catalog
supplements pytdx's legacy list; probes and collection share strict Decimal decoding and reject
empty-shell or truncated pages. New remote Raw v2 retains source lot volumes and normalizes them
to shares. Under ADR-0068, legacy remote v1 replay also normalizes source lots to shares;
immutable Raw stays unchanged and stored history changes only through approved repair. See
[Daily Bar validation and remote diagnostics](docs/DailyBar数据质量验收.md).

Stock quantity contracts, evidence-bound repair/rollback, historical certification limits
and blocked snapshot rebuild dependencies are recorded in the
[quantity remediation delivery runbook](docs/stock-quantity-unit-remediation-2026-10-23.md).
Local implementation does not certify production history or authorize production operations.

The three Regulation benchmark indices use Tushare `index_daily` (ADR-0048 clarification,
Issue #69); this does not change ordinary stock routing or replace existing historical Raw.

## Minimal Linux production release

Build the committed Linux source package with
`uv run python scripts/build_release.py --platform linux`. The production shape is one
systemd-supervised `market-data-center worker` plus PostgreSQL. The independently
packaged HTTP API is not enabled by the Worker release; no OS-level collection schedule is registered. See
[the minimal production release runbook](docs/最小生产发布运行手册.md) for installation,
release gates, read-only smoke checks, and rollback boundaries. Packaging does not prove that
production backup restore or credential rotation has passed.

The current same-host production services use loopback PostgreSQL with separate credentials and
systemd 255 restart-backoff drop-ins (10–120 seconds). Preserve the protected environment and
drop-ins on later releases; the [runbook](docs/最小生产发布运行手册.md) records verification and rollback.

## Windows deployment

Install `uv`, then prepare the environment with the root deployment script. On the first run it creates `.env`; fill `DATABASE_URL` and `RAW_DATA_ROOT`, then run the same command again. The Worker builds its PYTDX pool when it starts:

```powershell
.\deploy.cmd
```

The command installs the locked production dependencies and validates the worker and API executables. It does **not** register any Windows Task Scheduler entry. Use `-Check` to additionally run a read-only worker health check.

Start the long-lived API and worker services with:

```powershell
.\serve.cmd
```

This launches the FastAPI read-only API (`http://127.0.0.1:8000`) and the `market-data-center worker` process in separate console windows. The worker hosts an in-process APScheduler that drives every scheduled job — PYTDX pool refresh, daily-run, stock daily indicators, stock pools, deducted profit, stale-run recovery and quote snapshots. Job fire times are fixed in the controlled code catalog; `.env` may only enable or disable catalog jobs. Stop each service with Ctrl+C in its window.

See [INSTALL-WINDOWS.md](INSTALL-WINDOWS.md) for the Chinese installation and verification guide.

## Development

Kaipanla auction-pool reads use `GET /api/v1/realtime/kaipanla/auction-pool` with
optional `trade_date`, bounded `offset`/`limit` and `exclude_st`. Historical paging is
verified; current HTTP placeholder responses return 502 (APP Socket is not integrated).
Amounts are CNY and ratios are percentages; no database/Raw writes occur.
See [the auction API guide](docs/开盘啦竞价实时接口.md).

Kaipanla alerts are exposed by ten authenticated FastAPI routes under
`/api/v1/realtime/kaipanla/alerts`, grouped as `实时接口`. They fetch on request without
database/Raw writes or Worker jobs. See [the API guide](docs/开盘啦异动提醒实时接口.md)
for date handling, paging, source-column limitations and examples. Restart the API process
after updating code; no database migration is needed for these routes. Confirmed layouts
also expose source rule text, today's/next-trading-day trigger thresholds and explicitly
stated 10-day Kaipanla counts; unprovided values remain null, not zero or official counts.

Kaipanla home-page market-emotion money-effect history is exposed by
`GET /api/v1/realtime/kaipanla/market-emotion/money-effect`. It returns the verified list
fields only (date, board success rate and board profit rate), with bounded paging and no
detail expansion, persistence or Worker job. See
[the money-effect API guide](docs/开盘啦市场情绪赚钱效应实时接口.md).

The historical market-emotion chart is available at
`GET /api/v1/realtime/kaipanla/market-emotion/chart?offset=0&limit=100`.
See [the chart API guide](docs/开盘啦市场情绪图实时接口.md).

The newest "What to trade tomorrow/today" topic-date group is available at
`GET /api/v1/realtime/kaipanla/hot-talks`. It reads the source directly without
credentials or an emulator and returns ranked titles, source heat values, publication
timestamps and vote flags. See [the topic API guide](docs/开盘啦明天炒什么实时接口.md).

Kaipanla board reads provide three authenticated GET routes under
`/api/v1/realtime/kaipanla/boards`: latest rankings, `/history`, and
`/{board_code}/members/history` with institutional profit forecasts. Each request reads one
bounded HTTP page (up to 60 records), validates the actual source date and preserves Decimal
precision, without database/Raw writes or Worker jobs. Historical members require a date
before today in Shanghai; same-day TLS quotes are outside this scope. See
[the board API guide](docs/开盘啦板块精选实时接口.md).

Regulation public reads (migration `20260922000100`):
`GET /api/v1/regulation/triggers` and
`GET /api/v1/regulation/recent-events/next-triggers` require an exact `trade_date`
and API key, with `limit` 1–500 and an optional calculation-pinned `cursor`.
They read published calculations only; no ingestion or recomputation is triggered.
The recent-event query uses the actual immutable event input list captured by the
Worker. Legacy runs without that list return 404 rather than reconstructing history;
an explicitly captured empty list returns 200 with no items. These are deterministic
rule conditions, not predictions. Deployment requires both API and Worker updates;
this change does not alter job schedules or automatically recompute historical runs.

Ingestion reliability: invalid normalized shareholder-count rows are retained in Raw and
ERROR audit records, while valid rows publish with a partial outcome (ADR-0057). Regulation
benchmark collection verifies and fills its 30-trading-session input window. The Worker's
singleton database lock uses autocommit and a 30-second heartbeat; loss stops scheduling.
See [recovery notes](docs/Raw重放与运行恢复.md) before any authorized production replay.

Top-10 float-holder facts (ADR-0064, migration `20261005000100`) are append-only rows in
`core.top10_float_holder`, keyed by `(symbol, end_date, announcement_date, holder_rank)` with
ranks derived per announcement group by descending share amount. A response hitting the
6000-row provider limit is superseded by per-report-period split requests (most recent 8
quarter ends); a still-truncated split fails explicitly. The weekly job
`top10-float-holders-weekly` runs Saturdays 08:00 Asia/Shanghai over a rolling 9-day
announcement window and stays opt-in via `TOP10_FLOAT_HOLDERS_WEEKLY_ENABLED` (default
false). Manual catch-up:

```bash
uv run market-data-center top10-float-holders --start-date 2026-09-26 --end-date 2026-10-04
```

No `api_v1` view or RPC ships in the first phase; the public read contract is deferred until
consumer analytics needs are settled.

Eastmoney board money-flow facts (ADR-0065, migration `20261006000100`) are append-only rows
in `core.board_moneyflow`, keyed by `(trade_date, board_code)` with `DC:BK0145`-style codes,
covering concept/industry/region boards (~1000 rows per trading day, one request). The daily
job `board-moneyflow-daily` runs weekdays 17:00 Asia/Shanghai over a rolling 5-day window and
stays opt-in via `BOARD_MONEYFLOW_DAILY_ENABLED`. Manual catch-up:

```bash
uv run market-data-center board-moneyflow --start-date 2026-07-06 --end-date 2026-10-05
```

Build one exact-date same-day limit-up snapshot manually (never falls back to an older date):

```bash
uv run market-data-center today-limit-up-snapshot --trade-date 2026-08-11
```

The command checks exact-date Daily Bar, stock-indicator/free-float-share and ready mainboard
limit-up-pool dependencies before provider I/O. Missing/failed dependencies create an idempotent
`deferred` snapshot; partial inputs or row/source/order-book gaps create `partial`. The scheduled
Worker job is fixed at 22:00 Asia/Shanghai on weekdays and remains opt-in through
`TODAY_LIMIT_UP_SNAPSHOT_ENABLED=false` until its migration and network preflight are deployed.
Public-node access uses two whole-request attempts by default, each with a 10-second HTTP timeout;
configure the bounds with `TODAY_LIMIT_UP_TIMEOUT_SECONDS` and
`TODAY_LIMIT_UP_MAX_ATTEMPTS`. The provider has no availability or historical-backfill SLA.

GitHub Issues are the project's only task backlog and planning system. Linear is not used or synchronized.

```bash
uv sync --all-groups
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

External read-only HTTP queries are provided by an independent FastAPI process that connects
directly to PostgreSQL and calls only bounded `api_v1` functions. PostgREST is an independent layer
that is not part of the current deployment. See [the API guide](docs/FastAPI外部接口.md) and
[Linux runbook](docs/Standalone-PostgreSQL-FastAPI-Linux.md). MCP remains deferred.

Configuration is loaded from environment variables. Copy `.env.example` locally and replace placeholders; never commit the resulting `.env` file.

Backup, independent restore verification, credential rotation, and database network hardening are documented in [docs/PostgreSQL备份恢复与凭据轮换.md](docs/PostgreSQL备份恢复与凭据轮换.md).

The repeatable Daily Bar coverage, invariant, source, and lineage audit is documented in [docs/DailyBar数据质量验收.md](docs/DailyBar数据质量验收.md).

Capital history is synchronized with `market-data-center capital --source-symbol SSE:600000 --mode backfill` and reconciled later with `--mode incremental`. Its accepted boundary is documented in [ADR-0007](docs/adr/ADR-0007-Capital与公司行为基础事实.md).

Tushare stock daily indicators support both a single-symbol range and one complete
trade-date snapshot. Prefer the bulk command for routine market-wide ingestion:

```bash
market-data-center --provider tushare stock-daily-indicators-bulk --trade-date 2026-07-31
```

The scheduled workflow synchronizes the actual trading day, collects the complete snapshot,
and then retains one calendar month in Core while preserving Raw and audit lineage:

```bash
market-data-center --provider tushare stock-daily-indicators-daily
```

Professional technical factors are collected separately through Tushare `stk_factor_pro`.
The Worker registers `stock-technical-factor-daily` for 21:00, 22:00 and 23:00 Asia/Shanghai on weekdays,
checks the actual trading calendar, and preserves all 226 factor values and their adjustment
bases. Existing daily indicators keep their original behavior. The authenticated
`POST /v1/stock-technical-factors/query` reads up to 100 codes for an exact trading date.
See the [technical-factor runbook](docs/runbooks/stock-technical-factor.md) for migration
prerequisites, single-day retries, Raw replay and missing-data behavior.
`stock-technical-factor-catchup` retries the exact previous Shanghai date at 08:30 daily;
closed dates are skipped and incomplete source snapshots remain failed. The API request body is
`{"trade_date":"2026-10-15","codes":["601188","000001","600000"]}`; codes remain strings,
and unavailable records are reported in `missing_codes` without substituting a different date.

Run the unified cross-platform collection Worker as the single supervised application process:

```bash
market-data-center worker
```

APScheduler runs inside the Worker and persists scheduling state in
`data/scheduler/jobs.sqlite`; mount that directory when running in a container. A systemd unit
is provided under `deploy/linux/`. No separate Scheduler application is deployed.

While the Worker is running, its loopback-only, read-only task page is available at
`http://127.0.0.1:8765/admin/scheduled-tasks`; see
[the local administration page runbook](docs/Worker本地只读管理页面.md).

Deducted-profit facts are discovered incrementally from Tushare disclosures every calendar day
at 20:00. The 2000-point path uses `disclosure_date` plus per-affected-symbol `fina_indicator`;
it does not require VIP access or perform an implicit history backfill. Consumers use the bounded
`query_deducted_profits_as_of` PostgREST RPC.

Main-board previous-day limit-up and limit-down pools are calculated from internal unadjusted
Daily Bars and exact daily indicators at 21:00 on trading days. Consumers read one immutable,
exact-date snapshot through `query_stock_pool_snapshot`; the API never falls back to an older
effective date. See `docs/领域详设-StockPool-2026-08-02.md`.

The authenticated FastAPI endpoint `GET /api/v1/limit-up-pool?trade_date=YYYY-MM-DD` exposes the
exact event-date mainboard limit-up snapshot. Its free-float market capitalization is strictly
the same-date unadjusted close multiplied by same-date free-float shares; no other market-value
field or date fallback is used.

`GET /api/v1/daily-limit-up-list?trade_date=YYYY-MM-DD&version=&offset=0&limit=200` exposes the
versioned immutable `today_limit_up` domain snapshot, including its ready/partial/deferred/failed
state, bounded quality summary, objective price/capitalization facts, optional sealing/order-book
facts, and provider-neutral lineage. It never substitutes an older date or missing enrichment;
members are deterministically ordered by symbol. The generic `/api/v1/limit-up-pool` contract is
unchanged.

`POST /api/v1/call-auction-market-snapshots/query` batch-reads the exact-date 09:25:20 final
Shanghai/Shenzhen auction-series batch and calculates seal amount for 1–500 six-digit codes. It
selects one coherent succeeded attempt, falling back to its partial attempt only when no succeeded
attempt exists, and reports missing codes without substituting another date, earlier round, or the
retired standalone snapshot dataset.

`POST /api/v1/call-auction-market-series-snapshots/query` reads the exact-date 09:15:00–09:25:20
PYTDX full-market series for 1–500 six-digit codes. Each item includes its deterministic `HHMMSS`
batch code and bid/ask levels 1–5; a missing price with positive volume is preserved as a source
fact. The retired limit-up-pool auction collector is not registered by the Worker, while its
historical workflow and stored facts remain readable.

`GET /api/v1/call-auction-grab-lines?trade_date=YYYY-MM-DD&min_grab_line_pct=0&min_change_pct=0`
calculates the exact-date
full-market auction price move from 09:24:53 to 09:25:20 relative to the stored previous close.
It returns SSE/SZSE stocks whose exact Decimal percentage is strictly greater than `n`, ordered
from highest to lowest. Both rounds must come from one coherent successful session; the API never
mixes sessions or dates, and does not trigger collection.

The default-enabled `call-auction-market-series-archive-daily` Worker job runs every day at 02:30
Asia/Shanghai and idempotently copies all completed-date online facts into the monthly partitioned
`realtime.call_auction_market_series_snapshot_history` table. At 03:00, `data-cleanup-daily`
deletes online details older than the latest three completed `CN_A_SHARE` trading days only after
every target row is verified in history, then deletes history older than six calendar months.
Archive gaps fail closed. Session/round metadata, Raw and lineage records, quality and operations
history remain retained. Times, retention rules, and target tables are code-owned; `.env` exposes
only the two enable switches.

The local ADR-0058 extension adds verified lossless gzip for registered JSONL Raw older than
seven days, followed by bounded archival of the two approved quality rules older than 30 days.
It preserves original manifests and uses the same Worker maintenance lock and budget. Preview
counts are explicitly bounded lower bounds when incomplete. See
[Raw compression and cleanup](docs/Raw无损压缩与清理闭环.md) for registration, crash recovery,
deployment and first-execution requirements; this does not assert production execution.

DragonTiger collection is opt-in and remains disabled until source-rights review is recorded
(`DRAGON_TIGER_ENABLED=false`). The `dragon-tiger-collect` command requires
`--confirm-eastmoney-source-terms-reviewed`. The Worker catalog first runs the independent Tushare
BSE `L/D/P` security sync at 20:15, then DragonTiger at 20:30 Asia/Shanghai. New collection writes
immutable JSONL Raw (`eastmoney.dragon_tiger.v3`) before normalization; historical
`eastmoney.trading_billboard.v1` and `eastmoney.dragon_tiger.v2` Raw remain replayable.
Historical collection is resumable, continues after individual-date failures, and accepts at most
730 calendar days. Orphan files can first be inspected with `dragon-tiger-raw-recovery --dry-run`
and registered only with `--execute --confirm`.

Reviewed hot-money actors and effective-dated stable-seat mappings are maintained in the
versioned `catalogs/hot_money_roster.v1.json` file. Validate it with
`market-data-center hot-money-catalog-sync --catalog catalogs/hot_money_roster.v1.json --dry-run`
before an explicitly confirmed publish. The opt-in `dragon-tiger-seat-profile-daily` Worker job
runs at 21:00 on weekdays after same-date Daily Market and DragonTiger success, and materializes
versioned T+1/T+3/T+5 objective seat profiles without producing a subjective score.

Authenticated reads are `GET /api/v1/dragon-tiger/events/by-date`,
`GET /api/v1/dragon-tiger/events/by-symbol/{code}`,
`GET /api/v1/dragon-tiger/seats/{seat_id}/trades`, and
`GET /api/v1/dragon-tiger/events/{event_id}/metrics`. The fixed reviewed roster can be queried by
exact date through `GET /api/v1/trading-billboard/hot-money-actions?trade_date=YYYY-MM-DD`.
Queries are bounded and never fall back to
another date. Metrics are deterministic objective amounts/counts/concentrations; no subjective
score or strategy label is published. Event responses separate trigger-window semantics from the
amount period, expose one-sided disclosure and safe quality codes, and no longer contain
`period_type`.

Daily Bar bulk ingestion keeps one provider/Raw/ingestion lineage unit per security while writing
validated facts in bounded PostgreSQL transactions. Configure `DAILY_BAR_WRITE_BATCH_SIZE`
(default 100, range 1..500); see `docs/DailyBar批量写入与性能基线-2026-08-11.md`.

Classification uses complete, Shanghai-date snapshots. Capture the catalog before its members:

```bash
market-data-center classification-catalog --classification-type industry
market-data-center classification-members --classification-type industry --classification-code BK0475
```

Industry and concept snapshots default to local TDX classification files, with AKShare as an explicit optional adapter. Snapshot and effective-interval semantics are documented in [ADR-0008](docs/adr/ADR-0008-Classification分类与成分历史.md).

Versioned adjusted bars, returns, moving averages, market capitalization, and classification metrics can be recalculated manually from Core facts, but are not part of the current daily schedule:

```bash
market-data-center derived-recompute --start-date 2026-01-01 --end-date 2026-07-29 --mode incremental
```

`api_v1.daily_bars` remains unadjusted. Derived views include the calculation ID, algorithm version, calculation range, input hash, and calculation timestamp; see [ADR-0009](docs/adr/ADR-0009-版本化复权行情与客观Metrics.md).

The external HTTP contract is [FastAPI OpenAPI v1](contracts/fastapi-openapi-v1.json). FastAPI
executes bounded PostgreSQL `api_v1` functions directly; consumers never receive database
addresses, source payload field names, Raw objects, or secrets. Only contract-defined immutable
lineage identifiers are exposed where required for reproducibility.

Tencent five-level quotes use an explicit, bounded ingestion command and are never fetched by an
API request:

```bash
market-data-center realtime-quotes --symbols SSE:601003 SSE:600123 \
  --confirm-bounded-tencent-request
```

The command converts lot quantities to shares, stores immutable Raw plus append-only normalized
facts, and does not register a schedule. Authenticated consumers call
`POST /api/v1/realtime-quotes/latest/query` now performs a bounded request-time Tencent read and
does not query or write the quote database, save Raw, or trigger collection. The retained
`max_age_seconds` field is compatibility-only. See
[ADR-0045](docs/adr/ADR-0045-腾讯实时五档API请求时直连且不落库.md).

The third-party dynamic board index `THS:883423` is isolated from Security and
ordinary Daily Bar facts. Synchronize its explicit directory before bars and
today's complete constituent snapshot:

```bash
market-data-center board-index
market-data-center board-index-daily-bar --start-date 2026-01-01 --end-date 2026-07-29
market-data-center board-index-constituents
```

The dedicated `akshare_ths` adapter is selected automatically for these commands.
The Worker also collects `THS:883423` daily bars at the code-owned weekday slots 15:30, 16:30,
and 17:30 with bounded retries and tail-gap recovery. The bias API reads only persisted data.
THS exposes current constituents rather than trustworthy historical membership,
so historical snapshots are accumulated by daily runs and can be replayed from
immutable Raw data. See [ADR-0003](docs/adr/ADR-0003-同花顺动态板块指数.md) and
[the collection runbook](docs/同花顺动态板块指数采集.md).

The daily local-TDX workflow and cross-platform APScheduler deployment are documented in [docs/Worker日常采集与调度.md](docs/Worker日常采集与调度.md).

The opt-in Regulation workflow runs inside the Worker at 22:00 Asia/Shanghai on weekdays. It
collects SSE/SZSE official regulation events and the three allowlisted official benchmark indices,
evaluates the configured exchange rules, and persists deterministic next-session trigger scenarios.
An independent 08:30 reconciliation run versions late official events. Enable these jobs only after
source-rights review and the ordered migrations; run one exact date manually with
`market-data-center regulation-calculate --trade-date YYYY-MM-DD`. Its output is a rules-based
condition calculation, not a prediction or a statement that suspension or another action is certain.

Run PostgreSQL integration tests against a disposable local database (never production):

```bash
./deploy/testing/run-postgres-integration.sh
```

On Windows use `deploy/testing/run-postgres-integration.ps1`. Both scripts start the
Compose service, run the integration marker, and remove the temporary volume afterward.

Verified Raw replay, stale-run recovery, and read-only cross-provider Daily Bar comparison are documented in [docs/Raw重放与运行恢复.md](docs/Raw重放与运行恢复.md).

独立异动监控的只读接口、连续历史预检、发布边界和本地验收见
[异动监控运行手册](docs/runbooks/regulation-monitor.md)。代码可用不等于生产批次已发布。

Quality cleanup now requires a complete verified archive before deleting whitelisted old evidence.
`uv run market-data-center data-cleanup` is read-only by default. See
[质量归档与受限清理](docs/质量归档与受限清理.md) for bounds, permissions, and deployment prerequisites.

Production migration and smoke verification can be started manually through the protected `Production migration and smoke check` GitHub Actions workflow.

The operator-controlled `call-auction-indicative-detail` command captures one SSE/SZSE symbol for
the current Shanghai trading date only. It remains disabled unless
`--confirm-current-day-single-symbol` is supplied, is never scheduled, and stores virtual auction
indicative/matching observations—not exchange trade ticks or order-by-order data.

The CLI uses deterministic provider routing by default: BaoStock then AKShare for security/calendar, and pytdx only for Daily Bars. At startup and every hour, the Worker probes a bounded candidate set and atomically publishes one versioned pool with quote, SSE, SZSE and BSE capability flags. Consumers filter that pool by capability, use bounded connection failover, and keep one endpoint for a successful batch. Missing or unavailable bars remain explicit gaps and are not filled from other providers. Public TDX nodes have no availability or rate-limit guarantee. Use `--provider baostock|akshare|pytdx` to bypass routing for reproducible diagnostics.

## 本地日线恢复验收补充

本地单日采集会先保留较早记录计算昨收价，再裁剪请求日期。已有空昨收记录需要显式重新
采集，普通缺行补采不会自动修复字段；验收同时检查记录覆盖、昨收完整性和 Raw 重放。

The owner-authorized 2026-10-23 cleanup uses separate administrator preview/archive/apply tools.
Offline Raw retirement preserves manifests, blocks replay until verified restoration, and excludes
retired objects from scheduled compression. See the [monthly cleanup audit](docs/runbooks/month-retention-production-2026-10-23.md)
for the exact cutoff, retained calculation inputs, archive evidence, and unfinished exceptions.
This one-off operation does not change the Worker's recurring retention periods.

2026-10-23新增历史清理范围见[ADR-0070](docs/adr/ADR-0070-定时历史结果归档清理.md)及[清理闭环](docs/Raw无损压缩与清理闭环.md)：竞价附属事实、技术因子、龙虎榜、监管结果及旧版本快照按一个自然月、完整依赖组归档后清理；部署状态以发布审计为准。

2026-10-23涨停列表已恢复64条主板记录，状态partial，2只来源缺失保持可见；封板资金按元原值提供。
单位证据、测试及线上验收见[恢复记录](docs/runbooks/limit-up-recovery-2026-10-24.md)。

Tushare字段单位、精确换算、封单字段差异及待认证范围见[Tushare接口字段单位契约](docs/Tushare接口字段单位契约.md)（2026-10-24核验）。

2026-10-23技术因子快照已补齐5571条，公开目录v3保留Tushare原生OBV/EMV/MAEMV值，不作手股转换；[定时任务原因及恢复验收](docs/runbooks/stock-technical-factor.md)。

2026-10-23跌停列表已通过Tushare limit_list_d D恢复新版本：8个规范成员、6条来源封单金额，2个来源缺口保持null/partial；[恢复与验收记录](docs/runbooks/limit-down-tushare-2026-10-24.md)。
