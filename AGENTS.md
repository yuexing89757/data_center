# Agent development guide

## Authority and scope

- Follow documents in this order: `docs/项目宪法-MarketDataCenter-2026-07-24.md`,
  accepted ADRs, `docs/股票数据中心技术方案.md`, current domain designs, then the
  relevant GitHub Issue or PR. When documentation conflicts with implemented behavior,
  verify migrations and tests and report the discrepancy instead of guessing.
- GitHub Issues are the only task-planning system of record. Do not create, read, or
  synchronize Linear issues for this project.
- Keep Market Data Center responsible for traceable source facts, deterministic objective
  derivations, quality checks, persistence, and stable read contracts. Trading strategies,
  subjective labels, backtests, portfolio management, and application-specific judgments
  belong in consumer projects.
- New domains and material architecture changes require a GitHub Issue, an accepted ADR,
  domain design, SQL migration, and tests before implementation. Do not introduce FastAPI,
  MCP, minute/tick data, or new derived-price semantics without an accepted ADR.

## Current architecture

- Runtime: Python 3.12 managed by `uv`.
- Storage: PostgreSQL. Production schema changes are made only by ordered files in
  `supabase/migrations/*.sql`; never use `create_all()`, Alembic, or ad-hoc DDL to mutate the
  production schema.
- Public reads: bounded, read-only PostgREST views and RPCs in `api_v1`. Consumers must not
  depend directly on `core`, `capital`, `classification`, `derived`, `metrics`, `ingestion`,
  or `audit`. Keep `contracts/postgrest-openapi-v1.json` and
  `contracts/agent-tools-v1.json` synchronized with public contract changes.
- External reads: ADR-0011 accepts a separate FastAPI process protected by API key. It may
  call only bounded `api_v1` RPCs, must not access internal schemas directly, and must not
  write data or trigger ingestion. Keep `contracts/fastapi-openapi-v1.json` synchronized.
- Raw data: immutable Parquet/JSONL objects in the configured Worker filesystem, with
  manifests and ingestion lineage in PostgreSQL. Never edit Raw objects in place or commit
  Raw market data.
- Scheduling (constitution principle 11, ADR-0017): all scheduled jobs run inside the
  `market-data-center worker` process via APScheduler. Never register operating-system-level
  scheduled tasks (Windows Task Scheduler, cron, etc.) as collection triggers — not in code,
  deploy scripts, documentation, or agent-generated instructions. New jobs are added to the
  Worker's job catalog only; the OS layer's sole job is to keep the Worker process alive
  (boot start, crash restart).
- Local administration: the Worker may expose the ADR-0018 read-only task page on hard-coded
  IPv4 loopback. It is not a public API: do not add remote binding, task mutations, serialized
  JobStore state, secrets, or database paths to it.
- Operations: workflow/job definitions are a controlled code catalog shared by scheduler
  registration, execution recording, and the local page. Durable WorkflowRun/JobExecution facts
  belong in the `operations` schema; never copy or deserialize APScheduler `job_state` into them.

## Provider routing and coverage

- Provider-specific field names and units stop at the provider adapter boundary. Providers
  return standard domain records and never leak source payload fields downstream.
- Automatic routing follows ADR-0005/ADR-0024: Security and Trading Calendar try BaoStock then
  AKShare; ordinary stock Daily Bar uses remote pytdx only. Missing, stale, or suspended
  Daily Bars remain explicit gaps and are not filled from network providers.
- ADR-0024 pytdx Daily Bar reads only explicitly configured remote TDX endpoints. It performs
  no endpoint discovery, uses bounded connection attempts/timeouts, and keeps gaps visible.
- The THS dynamic board index adapter (`akshare_ths`, including `THS:883423`) is isolated
  from ordinary Security and Daily Bar routing. Do not treat it as a standard stock symbol.
- A successful ingestion run has one actual provider. Do not merge partial results from
  multiple providers into a single successful batch or silently arbitrate conflicting facts.

## Domain and data rules

- Standard symbols are `SSE:600000`, `SZSE:000001`, and `BSE:920000`; use `symbol` for
  cross-domain joins and preserve leading zeroes.
- The unified calendar market is `CN_A_SHARE`; dates use exchange-local trading dates and
  timestamps use `Asia/Shanghai` where a timestamp is required.
- `core.daily_bar` is an unadjusted daily fact keyed by `(symbol, trade_date)`. Volume is in
  shares and amount is in CNY. Do not fabricate bars, forward-fill gaps, or overwrite Core
  with adjusted prices.
- Use `Decimal` for prices, ratios, and amounts. Never route market values through `float`.
  Preserve `None` as missing data; zero and missing have different meanings.
- Domain records never contain `ingestion_id` or `calculation_id`. The pipeline attaches
  ingestion lineage with `IngestionEnvelope`; persistence attaches calculation lineage at
  the write boundary.
- Calculators are pure and deterministic: no database access, I/O, batch creation, locking,
  or transaction management. Derivation service and persistence own those responsibilities.
- Derived results are versioned and reproducible. Never mix calculation IDs in one logical
  result, silently change an existing algorithm version, or omit calculation metadata from
  public derived views.
- Mainboard price-limit facts and immutable stock-pool snapshots are deterministic derived
  data. Up/down pools share the `stock_pool` boundary; consumers must request an exact
  effective trading date and must never fall back to an older ready snapshot.
- Validate natural-key uniqueness, OHLC ranges, nonnegative values, calendar membership,
  security lifecycle, and provider units before persistence. Do not downgrade a hard data
  invariant into a warning merely to keep an ingestion run green.

## Implementation workflow

- Before editing, inspect the governing ADR/domain design, nearby tests, and the relevant
  migration or public contract. Prefer the smallest change that satisfies the issue.
- Preserve existing user changes in a dirty worktree. Do not reformat or rewrite unrelated
  files, and do not use destructive Git commands to clear local changes.
- Keep responsibilities separated: provider adaptation in `providers/`, orchestration in
  `pipeline.py`/services, validation in domain validation modules, pure computation in
  calculators, and SQL access in persistence modules.
- Public behavior changes require focused unit tests. Schema and PostgREST changes require a
  migration plus PostgreSQL integration/API contract tests. Provider changes require mocked
  adapter tests covering source errors, units, identifiers, missing values, and Raw replay.
- Update the relevant ADR clarification, domain design, runbook, README, and checked-in
  contracts when behavior or operations change. Do not document proposals as current facts.
- Never run production migrations, ingestion, recovery, credential rotation, or other
  external mutations unless the user explicitly requests that operation. Prefer read-only
  diagnostics when investigating.

## Verification commands

Install all dependency groups once:

```bash
uv sync --all-groups
```

Run focused tests while iterating, then the complete local gate before handoff:

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest

# Local read-only API (requires FASTAPI_API_KEY and a DATABASE_URL or
# FASTAPI_DATABASE_URL in .env). Runs in a separate process from the
# ingestion worker.
uv run market-data-api
```

PostgreSQL integration tests require an isolated disposable database supplied through
`TEST_DATABASE_URL`; never point them at production:

```bash
uv run pytest -m integration
```

If a required check cannot run, report the exact command and reason. Do not claim success
from partial checks.

## Secrets and operational safety

- Never commit or print `.env`, credentials, API keys, JWT secrets, database URLs, local TDX
  paths, backups, or Raw market data. Use documented environment variables and redact values
  from logs and reports.
- Client-safe publishable/anon credentials are for `api_v1` reads only. Service/secret keys,
  migration roles, and Worker database credentials must not enter client contracts.
- Production migrations use the protected workflow and migration role; the Worker uses its
  dedicated least-privilege role. Keep RLS, grants, statement timeouts, and bounded query
  limits intact unless an accepted decision explicitly changes them.
- Treat backup restore, replay, and recovery procedures as operationally sensitive. Resolve
  exact targets and verify preconditions before any mutation.
