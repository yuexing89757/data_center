# Pysnowball Auction Five-Level Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch only the opening-auction limit-up-pool five-level collector to the pysnowball pankou source.

**Architecture:** Add a provider adapter that maps one standard SSE/SZSE symbol per bounded HTTP request into the existing five-level record. Wire only `opening-auction-limit-up-quotes` to it, keep all PYTDX full-market paths unchanged, and extend source constraints without changing the read contract.

**Tech Stack:** Python 3.12, pydantic-settings, urllib/JSON Decimal decoding, PostgreSQL ordered migrations, pytest.

**Spec:** `docs/superpowers/specs/2026-08-17-pysnowball-auction-five-level-design.md`

## Global Constraints

- Only `opening-auction-limit-up-quotes` uses pysnowball; no PYTDX fallback or provider mixing.
- Only levels 1 through 5 enter the existing domain and database schema.
- `PYSNOWBALL_TOKEN` never enters Git, Raw, request parameters, logs, or API responses.
- Task time and 30-second cadence remain controlled in code; environment configuration only enables/disables the task.
- Full-market auction series, 09:26 snapshot, EOD snapshot, and Daily Bar provider routes do not change.
- Prices and amounts never pass through `float`; missing and zero remain distinct.

---

### Task 1: Provider identity, secret settings, and database constraints

**Files:**
- Modify: `src/market_data_center/domain/ingestion.py`
- Modify: `src/market_data_center/settings.py`
- Modify: `.env.example`
- Create: `supabase/migrations/20260817000300_allow_pysnowball_auction_quotes.sql`
- Modify: `tests/test_ingestion_models.py`
- Modify: `tests/test_settings.py`
- Modify: `tests/test_postgres_integration.py`

**Interfaces:**
- Produces: `ProviderCode.PYSNOWBALL == "pysnowball"`.
- Produces: `PysnowballSettings.pysnowball_token: SecretStr` with `resolved_token() -> str`.
- Produces: database constraints accepting both historical `pytdx_hq` and new `pysnowball` auction facts.

- [ ] **Step 1: Write failing enum and secret tests**

Add tests proving `ProviderCode("pysnowball")` resolves and that `PysnowballSettings`
rejects a blank Token while `resolved_token()` returns the configured Cookie string without
including it in `repr(settings)`.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `uv run pytest tests/test_ingestion_models.py tests/test_settings.py -q`

Expected: FAIL because `ProviderCode.PYSNOWBALL` and `PysnowballSettings` do not exist.

- [ ] **Step 3: Add the minimal enum and settings implementation**

Add `PYSNOWBALL = "pysnowball"` and a settings class with one required `SecretStr` field.
Add only `PYSNOWBALL_TOKEN=<server-only-xueqiu-cookie>` to `.env.example`; do not add timing,
cadence, batch-size, or retry environment values.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run: `uv run pytest tests/test_ingestion_models.py tests/test_settings.py -q`

Expected: PASS.

- [ ] **Step 5: Add the ordered migration and integration assertion**

Create a migration that recreates the current `ingestion_run_provider_check` with
`pysnowball` added, changes the auction session provider check to
`provider_code in ('pytdx_hq','pysnowball')`, and changes the five-level row source check to
`source_code in ('pytdx_hq','pysnowball')`. Extend the auction persistence integration test
with one pysnowball session/run/quote while retaining its existing pytdx fixture.

- [ ] **Step 6: Run the isolated migration test when available**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k auction -q`

Expected: PASS when `TEST_DATABASE_URL` points to an isolated disposable database. If it is
unset, record the exact skip/blocker and do not point the test at production.

### Task 2: Pysnowball five-level provider adapter

**Files:**
- Create: `src/market_data_center/providers/pysnowball_quote.py`
- Create: `tests/test_pysnowball_quote_provider.py`

**Interfaces:**
- Produces: `PysnowballQuoteProvider(settings, *, client=None, clock=...)`.
- Produces: `fetch_five_level_quotes(symbols, *, deadline=None) -> RealtimeQuoteFetch`.
- Consumes: `PysnowballSettings.resolved_token()` and the existing provider-neutral DTOs.

- [ ] **Step 1: Write failing mapping and Decimal tests**

Use a fake HTTP client at the network boundary with a complete literal pankou response.
Assert `SSE:600000 -> SH600000`, `SZSE:000001 -> SZ000001`, `current -> last_price`,
`bp1..5/bc1..5` and `sp1..5/sc1..5` mapping, exact Decimal values, share quantities, UTC
source timestamp, and `source_code == "pysnowball"`.

- [ ] **Step 2: Run the provider tests and verify RED**

Run: `uv run pytest tests/test_pysnowball_quote_provider.py -q`

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement the minimal bounded client and normalizer**

Use the pankou endpoint
`https://stock.xueqiu.com/v5/stock/realtime/pankou.json?symbol=<source-symbol>`, Cookie-only
secret injection, a fixed code-owned request timeout, `parse_float=Decimal`, sanitized
exceptions, and one HTTP call per symbol. Normalize zero-price levels to `(None, None)` and
leave unsupported quote fields as `None`.

- [ ] **Step 4: Run the provider tests and verify GREEN**

Run: `uv run pytest tests/test_pysnowball_quote_provider.py -q`

Expected: PASS.

- [ ] **Step 5: Add failure-continuation and deadline tests**

Add literal tests where the middle symbol raises an HTTP/auth error and later symbols still
produce records, plus a deadline case where unattempted symbols are returned in
`failed_symbols`. Assert Raw rows never contain the Token.

- [ ] **Step 6: Run all provider-focused tests**

Run: `uv run pytest tests/test_pysnowball_quote_provider.py tests/test_pytdx_hq_provider.py -q`

Expected: PASS, proving the new adapter did not change PYTDX behavior.

### Task 3: Isolate scheduler wiring and provider-aware auction lineage

**Files:**
- Modify: `src/market_data_center/auction_service.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `tests/test_auction.py`
- Modify: `tests/test_scheduler.py`
- Modify: `docs/adr/ADR-0022-集合竞价涨停池五档快照采集.md`
- Modify: `docs/领域详设-RealtimeQuote-2026-08-02.md`
- Modify: `docs/集合竞价五档采集运行手册.md`
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/数据库导航.md`

**Interfaces:**
- Consumes: `PysnowballQuoteProvider` and `ProviderCode.PYSNOWBALL`.
- Changes: auction ingestion and Raw lineage use `self._provider.source_code`.
- Preserves: all full-market and EOD provider factories remain `PytdxHqProvider`.

- [ ] **Step 1: Write failing service and scheduler tests**

Change the auction fixture provider to `source_code="pysnowball"` and assert the emitted
IngestionRun, session, Raw path, manifest, and quote row all carry pysnowball. Replace the old
scheduler assertion about `pytdx_hq_batch_size=1` with behavior proving the runner constructs
`PysnowballSettings`/`PysnowballQuoteProvider`; retain the existing full-market tests that
assert PYTDX batch size 80.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `uv run pytest tests/test_auction.py tests/test_scheduler.py -q`

Expected: FAIL because auction ingestion and scheduler wiring still hard-code PYTDX.

- [ ] **Step 3: Implement provider-aware lineage and isolated wiring**

Build `ProviderCode(self._provider.source_code)`, use the same source for Raw storage, and
construct only the limit-up-pool job with `PysnowballQuoteProvider`. Pass `max_retries=0` so
one failed single-symbol request is not retried into the next 30-second round. Pass each
round's fixed deadline to the provider so remaining calls stop before the next sample.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run: `uv run pytest tests/test_auction.py tests/test_scheduler.py -q`

Expected: PASS.

- [ ] **Step 5: Update accepted operational documentation**

Revise ADR-0022 with a dated clarification that only this job is pysnowball-only. Update the
domain design and runbooks with the Token preflight, per-symbol partial behavior, source
diagnostic SQL, and the explicit statement that full-market tasks remain PYTDX.

- [ ] **Step 6: Run the local verification gate**

Run:

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: all four commands exit 0. Do not perform a live full-pool network test.
