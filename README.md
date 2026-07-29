# Market Data Center

A phase-one A-share daily market data pipeline using Python 3.12 and a self-hosted Supabase deployment. Security and calendar use BaoStock/AKShare routing; stock Daily Bar uses only pytdx local Shanghai/Shenzhen/Beijing `.day` files.

## Development

GitHub Issues are the project's only task backlog and planning system. Linear is not used or synchronized.

```bash
uv sync --all-groups
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

The project currently has no FastAPI or MCP service. Read queries are provided by bounded Supabase PostgREST views and RPCs under `api_v1`.

Configuration is loaded from environment variables. Copy `.env.example` locally and replace placeholders; never commit the resulting `.env` file.

Backup, independent restore verification, credential rotation, and database network hardening are documented in [docs/Supabase备份恢复与凭据轮换.md](docs/Supabase备份恢复与凭据轮换.md).

The repeatable Daily Bar coverage, invariant, source, and lineage audit is documented in [docs/DailyBar数据质量验收.md](docs/DailyBar数据质量验收.md).

Capital history is synchronized with `market-data-center capital --source-symbol SSE:600000 --mode backfill` and reconciled later with `--mode incremental`. Its accepted boundary is documented in [ADR-0007](docs/adr/ADR-0007-Capital与公司行为基础事实.md).

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

Stable consumer and Agent reads use bounded Supabase PostgREST RPCs. The checked-in contracts are [OpenAPI v1](contracts/postgrest-openapi-v1.json) and [Agent tools v1](contracts/agent-tools-v1.json). ADR-0010 keeps FastAPI and MCP deferred because current queries remain inside PostgreSQL/PostgREST.

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

The daily local-TDX workflow and Windows scheduled task are documented in [docs/Worker日常采集与调度.md](docs/Worker日常采集与调度.md).

Verified Raw replay, stale-run recovery, and read-only cross-provider Daily Bar comparison are documented in [docs/Raw重放与运行恢复.md](docs/Raw重放与运行恢复.md).

Production migration and smoke verification can be started manually through the protected `Production migration and smoke check` GitHub Actions workflow.

The CLI uses deterministic provider routing by default: BaoStock then AKShare for security/calendar, and local pytdx only for daily bars. Missing local daily bars remain explicit gaps and are not filled from network providers. Use `--provider baostock|akshare|pytdx` to bypass routing for reproducible diagnostics.
