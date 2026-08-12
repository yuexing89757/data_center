# Optional future standalone PostgreSQL cutover plan

FastAPI connects to PostgreSQL through a direct database connection. No data migration is required
for the current deployment. This document is optional future work and is not authorization to
provision, copy, stop, migrate, or switch production.

## Portability

The ordered migrations are substantially portable to standard PostgreSQL. They use normal DDL, RLS,
PL/pgSQL, `pgcrypto`, and `btree_gist`. The target needs those contrib extensions and a migration role
able to create extensions, schemas, roles, functions, policies, and grants. Conditional grants to
`anon`/`authenticated` and the conditional PostgREST `authenticator` setting become no-ops when those
roles are absent. Historical names `supabase/migrations` and
`supabase_migrations.schema_migrations` do not create a runtime dependency and remain stable.

## Decisions required

- PostgreSQL major version (initially match source), host/provider, region, storage/IOPS, encryption,
  private networking, monitoring, maintenance policy, and capacity.
- Backup destination, keys, retention, restore-test cadence, RPO and RTO.
- Maintenance/final-write window and connection-routing strategy.
- Separate migration, Worker, API-readonly, backup, and operator roles and credential rotation.
- API authentication and HTTPS/domain/reverse proxy; these block external exposure.

## Rehearsal

1. Back up the source database and Raw objects separately and record encrypted artifact hashes.
2. Provision an isolated empty target with the selected PostgreSQL version and extensions.
3. Apply every ordered repository migration. Never use ORM `create_all`, a schema dump, or manual DDL.
4. Transfer data only for repository-owned schemas: `ingestion`, `audit`, `core`, `capital`,
   `classification`, `derived`, `metrics`, `operations`, `realtime`, `stock_pool`, and
   `convertible_bond`. Do not copy gateway configuration, ownership, ACLs, or secrets. Retain
   migration history produced in step 3.
5. Verify migration/schema checks, per-table row counts, natural keys, min/max business dates,
   FK/orphans, representative decimal aggregates, ingestion/Raw lineage, and sampled content hashes.
   Run Worker/API read-only smoke with distinct target credentials. Complete and time a target restore.

## Cutover and rollback gates

1. Require independently restored fresh database and Raw backups. Pre-validate least-privilege target
   credentials without logging values.
2. In an approved window, stop only the Worker for final sync and record the last completed workflow,
   ingestion IDs, and scheduler state. Keep the API off.
3. Export final application-schema data, restore to the already migrated target, repeat verification,
   and preserve the unchanged old database for rollback.
4. Update only the protected Worker URL, run read-only checks, and start one Worker after explicit
   authorization. Observe a scheduled cycle and Raw/lineage persistence.
5. Configure and preflight the API's separate URL, then start it only after separate authorization.
   Public exposure still waits for authentication and HTTPS/reverse-proxy approval.
6. On failure, stop the target-connected process, restore the prior protected Worker URL, and restart
   the old deployment under explicit authorization. Never improvise reverse SQL migrations.
