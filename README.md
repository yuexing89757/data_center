# Market Data Center

A phase-one A-share daily market data pipeline using Python 3.12 and a self-hosted Supabase deployment. BaoStock is the default provider; AKShare is explicitly selectable, and pytdx reads local Shanghai/Shenzhen `.day` files for daily-bar supplementation.

## Development

```bash
uv sync --all-groups
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

The first phase intentionally has no FastAPI service. Read queries are provided by Supabase PostgREST views under `api_v1`.

Configuration is loaded from environment variables. Copy `.env.example` locally and replace placeholders; never commit the resulting `.env` file.

Backup, independent restore verification, credential rotation, and database network hardening are documented in [docs/Supabase备份恢复与凭据轮换.md](docs/Supabase备份恢复与凭据轮换.md).

The repeatable Daily Bar coverage, invariant, source, and lineage audit is documented in [docs/DailyBar数据质量验收.md](docs/DailyBar数据质量验收.md).

The daily incremental workflow and systemd timer are documented in [docs/Worker日常采集与调度.md](docs/Worker日常采集与调度.md).

Production migration and smoke verification can be started manually through the protected `Production migration and smoke check` GitHub Actions workflow.

The CLI uses deterministic provider routing by default: BaoStock then AKShare for security/calendar, and local pytdx then BaoStock then AKShare for daily bars. Use `--provider baostock|akshare|pytdx` to bypass routing for reproducible diagnostics.
