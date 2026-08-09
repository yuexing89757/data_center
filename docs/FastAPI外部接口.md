# FastAPI external read-only API

The FastAPI process is an independent protocol boundary that connects directly to the existing
Supabase-hosted PostgreSQL. It does not call or require Supabase URLs/keys, PostgREST, Auth, Studio,
providers, the Worker scheduler, Raw storage, or persistence services. Its SQL is limited to three
bounded `api_v1` functions. A future move to standalone PostgreSQL does not change the API contract.

```dotenv
FASTAPI_DATABASE_URL='postgresql+psycopg://<api-login>:<password>@<host>:5432/<database>'
FASTAPI_API_KEY='<at-least-32-random-characters>'
FASTAPI_HOST=127.0.0.1
FASTAPI_PORT=8000
```

`FASTAPI_DATABASE_URL` is mandatory and identifies a dedicated login inheriting only the
`market_data_api` group role. The API never falls back to the Worker's `DATABASE_URL`. Connections
force read-only transactions and a five-second statement timeout.

Stable v1 routes are security search (100 rows), unadjusted daily bars (5,000 rows and 3,661 days),
and classification members (5,000 rows). Business routes require `X-API-Key`. `/healthz` is
process-local and `/readyz` verifies a bounded database query. Prices and amounts remain decimal
strings. Errors never return SQL, internal schema names, database addresses, or credentials.

Keep the application on `127.0.0.1`. Public authentication, HTTPS/domain, reverse proxy, firewall,
rate limits, request-log retention, and API-key rotation are separate deployment decisions. See
`Standalone-PostgreSQL-FastAPI-Linux.md` for Linux gates.
