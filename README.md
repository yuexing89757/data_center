# Market Data Center

A phase-one A-share daily market data pipeline using BaoStock, Python 3.12 and a self-hosted Supabase deployment.

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
