# Market Data Center

A phase-one A-share daily market data pipeline using Python 3.12 and PostgreSQL. Security and calendar use BaoStock/AKShare routing; stock Daily Bar uses only explicitly configured remote TDX endpoints through pytdx.

## Minimal Linux production release

Build the committed Linux source package with
`uv run python scripts/build_release.py --platform linux`. The production shape is one
systemd-supervised `market-data-center worker` plus PostgreSQL. The independently
packaged HTTP API is not enabled by the Worker release; no OS-level collection schedule is registered. See
[the minimal production release runbook](docs/最小生产发布运行手册.md) for installation,
release gates, read-only smoke checks, and rollback boundaries. Packaging does not prove that
production backup restore or credential rotation has passed.

## Windows deployment

Install `uv`, then prepare the environment with the root deployment script. On the first run it creates `.env`; fill `DATABASE_URL`, `RAW_DATA_ROOT`, and `PYTDX_DAILY_BAR_ENDPOINTS`, then run the same command again:

```powershell
.\deploy.cmd
```

The command installs the locked production dependencies and validates the worker and API executables. It does **not** register any Windows Task Scheduler entry. Use `-Check` to additionally run a read-only worker health check.

Start the long-lived API and worker services with:

```powershell
.\serve.cmd
```

This launches the FastAPI read-only API (`http://127.0.0.1:8000`) and the `market-data-center worker` process in separate console windows. The worker hosts an in-process APScheduler that drives every scheduled job — daily-run, stock daily indicators, stock pools, deducted profit, stale-run recovery and the optional auction collection. Job fire times are configured in `.env` (`DAILY_RUN_HOUR`/`DAILY_RUN_MINUTE`, etc.). Stop each service with Ctrl+C in its window.

See [INSTALL-WINDOWS.md](INSTALL-WINDOWS.md) for the Chinese installation and verification guide.

## Development

GitHub Issues are the project's only task backlog and planning system. Linear is not used or synchronized.

```bash
uv sync --all-groups
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

External read-only HTTP queries are provided by an independent FastAPI process that connects
directly to PostgreSQL and calls only bounded `api_v1` functions. The initial deployment may reuse
the existing Supabase-hosted PostgreSQL without using Supabase URLs, keys, Auth, Studio, or PostgREST.
See [the API guide](docs/FastAPI外部接口.md) and
[Linux runbook](docs/Standalone-PostgreSQL-FastAPI-Linux.md). MCP remains deferred.

Configuration is loaded from environment variables. Copy `.env.example` locally and replace placeholders; never commit the resulting `.env` file.

Backup, independent restore verification, credential rotation, and database network hardening are documented in [docs/Supabase备份恢复与凭据轮换.md](docs/Supabase备份恢复与凭据轮换.md).

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
Daily Bars and exact daily indicators at 19:30 on trading days. Consumers read one immutable,
exact-date snapshot through `query_stock_pool_snapshot`; the API never falls back to an older
effective date. See `docs/领域详设-StockPool-2026-08-02.md`.

The authenticated FastAPI endpoint `GET /api/v1/limit-up-pool?trade_date=YYYY-MM-DD` exposes the
exact event-date mainboard limit-up snapshot. Its free-float market capitalization is strictly
the same-date unadjusted close multiplied by same-date free-float shares; no other market-value
field or date fallback is used.

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
executes bounded PostgreSQL `api_v1` functions directly; consumers never receive database,
provider, lineage, Raw, or secret fields.

The third-party dynamic board index `THS:883423` is isolated from Security and
ordinary Daily Bar facts. Synchronize its explicit directory before bars and
today's complete constituent snapshot:

```bash
market-data-center board-index
market-data-center board-index-daily-bar --start-date 2026-01-01 --end-date 2026-07-29
market-data-center board-index-constituents
```

The dedicated `akshare_ths` adapter is selected automatically for these commands.
THS exposes current constituents rather than trustworthy historical membership,
so historical snapshots are accumulated by daily runs and can be replayed from
immutable Raw data. See [ADR-0003](docs/adr/ADR-0003-同花顺动态板块指数.md) and
[the collection runbook](docs/同花顺动态板块指数采集.md).

The daily local-TDX workflow and cross-platform APScheduler deployment are documented in [docs/Worker日常采集与调度.md](docs/Worker日常采集与调度.md).

Run PostgreSQL integration tests against a disposable local database (never production):

```bash
./deploy/testing/run-postgres-integration.sh
```

On Windows use `deploy/testing/run-postgres-integration.ps1`. Both scripts start the
Compose service, run the integration marker, and remove the temporary volume afterward.

Verified Raw replay, stale-run recovery, and read-only cross-provider Daily Bar comparison are documented in [docs/Raw重放与运行恢复.md](docs/Raw重放与运行恢复.md).

Production migration and smoke verification can be started manually through the protected `Production migration and smoke check` GitHub Actions workflow.

The CLI uses deterministic provider routing by default: BaoStock then AKShare for security/calendar, and remote pytdx only for Daily Bars. TDX endpoints are an ordered, explicitly configured list; the provider performs bounded connection failover but never discovers nodes or switches endpoints inside a successful batch. Missing or unavailable bars remain explicit gaps and are not filled from other providers. Public TDX nodes have no availability or rate-limit guarantee, and BSE coverage must be verified per endpoint. Use `--provider baostock|akshare|pytdx` to bypass routing for reproducible diagnostics.
