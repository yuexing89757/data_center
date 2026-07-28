# Agent development guide

## Scope

- Follow `docs/项目宪法-MarketDataCenter-2026-07-24.md` and accepted ADRs.
- Phase 1 uses BaoStock by default, with AKShare as the explicit optional provider accepted by ADR-0002.
- Phase 1 uses self-hosted Supabase, SQL migrations and PostgREST.
- Do not introduce FastAPI, automatic multi-provider routing or adjusted-price facts without a new ADR.

## Commands

```bash
uv sync --all-groups
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

## Guardrails

- Provider-specific fields stop at the provider boundary.
- Domain records never contain `ingestion_id`; use `IngestionEnvelope` in the pipeline.
- Use `Decimal` for prices and amounts; never convert market prices through `float`.
- Production schema changes only through `supabase/migrations/*.sql`.
- Never commit `.env`, credentials, database URLs or raw market data.
