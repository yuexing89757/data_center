# ADR-0025: Direct PostgreSQL FastAPI service

- Status: Accepted
- Date: 2026-08-09
- Issue: #38
- Clarifies: ADR-0011

## Context

The collection Worker is already an independent Linux process. External consumers now need a
stable HTTP interface. The first API deployment will reuse the existing Supabase-hosted PostgreSQL
database, but it connects to PostgreSQL directly: Supabase gateway, keys, Auth, Studio, and
PostgREST are not API runtime dependencies.

## Decision

1. `market-data-api` is a process and systemd unit separate from the Worker. It has no scheduler,
   provider, Raw-data, persistence, migration, or ingestion entry point.
2. The API connects directly to the existing PostgreSQL endpoint through `FASTAPI_DATABASE_URL`.
   Database hosting does not change this boundary. It never falls back to the Worker's
   `DATABASE_URL` and accepts no Supabase URL or key.
3. SQL is restricted to the accepted, bounded `api_v1` query functions. The API does not query
   internal schemas or expose lineage, provider payload fields, Raw objects, or credentials.
4. A `market_data_api` NOLOGIN group role receives only schema usage and EXECUTE on the explicitly
   published v1
   functions published by this service. A separately managed LOGIN role inherits that group role.
   Every API connection also sets transaction read-only and a five-second statement timeout.
5. The first stable HTTP contract remains `/api/v1`: security search (100 rows), unadjusted daily
   bars (5,000 rows and 3,661 days), classification members (5,000 rows), and the exact-date
   mainboard limit-up pool (5,000 rows). Decimal values remain
   JSON strings. Breaking changes require `/api/v2`.
6. `/healthz` checks only the process. `/readyz` performs a bounded `select 1`. Business routes use
   the existing API-key mechanism until the user separately chooses the external authentication
   design. The Linux unit binds to IPv4 loopback by default.
7. HTTPS, domain/DNS, reverse proxy, public binding, firewall changes, rate limiting, and credential
   provisioning are deployment gates and are not performed by this change.
8. The current migration sequence is retained, including its historical directory and history
   schema names. Those names do not create a Supabase runtime dependency. A later rename would add
   migration risk without changing behavior.

## Optional standalone PostgreSQL portability

The ordered migrations use ordinary PostgreSQL DDL, RLS, PL/pgSQL, `btree_gist`, and `pgcrypto`.
They are portable to a supported standalone PostgreSQL installation when those two contrib
extensions are available and the migration role can create extensions, schemas, roles, functions,
policies, and grants. Conditional `anon`, `authenticated`, and `authenticator` blocks become no-ops
when Supabase roles are absent. The PostgREST configuration migration is likewise a no-op without
`authenticator`. These compatibility blocks are historical, not runtime dependencies.

## Consequences

- Worker and API failures, credentials, and lifecycles remain isolated.
- Reusing the current PostgreSQL database requires no data cutover and does not interrupt the Worker.
- A future standalone cutover remains separately gated: apply migrations to an empty target, transfer
  application data, verify it, and switch protected credentials in an approved window.
- API deployment is blocked on the API-role migration, a distinct login credential, backup evidence,
  API authentication choice, and HTTPS/reverse-proxy design. Standalone hosting is not a prerequisite.
