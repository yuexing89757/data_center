# Agent development guide

## Scope

- Follow `docs/项目宪法-MarketDataCenter-2026-07-24.md` and accepted ADRs.
- Phase 1 uses BaoStock by default, with AKShare and pytdx as explicit optional providers accepted by ADR-0002/ADR-0004.
- Phase 1 uses self-hosted Supabase, SQL migrations and PostgREST.
- FastAPI is the accepted external read-only protocol layer (ADR-0011). It may only call the bounded `api_v1` RPCs and must not touch `core`/`capital`/`classification`/`derived`/`metrics`/`ingestion`/`audit` schemas directly, write data, or run ingestion. Do not add new service-layer frameworks without a new ADR.
- Do not introduce adjusted-price facts (ADR-0009 governs versioned derived bars), MCP, or minute bars without a new ADR. Automatic provider routing follows ADR-0005.
- GitHub Issues are the only task-planning system of record. Do not create, read or synchronize Linear issues for this project.

## Commands

```bash
uv sync --all-groups
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest

# Local read-only API (requires FASTAPI_API_KEY and a DATABASE_URL or
# FASTAPI_DATABASE_URL in .env). Runs in a separate process from the
# ingestion worker.
uv run market-data-api
```

## Guardrails

- Provider-specific fields stop at the provider boundary.
- Domain records never contain `ingestion_id`; use `IngestionEnvelope` in the pipeline.
- Use `Decimal` for prices and amounts; never convert market prices through `float`.
- Production schema changes only through `supabase/migrations/*.sql`.
- Never commit `.env`, credentials, database URLs or raw market data.
