# Direct PostgreSQL FastAPI Linux runbook

This runbook packages but does not authorize deployment or public exposure. Worker and API remain
separate services, users, environment files, credentials, and failure domains.

On 2026-09-22, the production API and Worker were explicitly authorized to use the verified
same-host PostgreSQL through `127.0.0.1:5432`, retaining their separate database identities.
Production systemd 255 drop-ins now increase failure-restart delays from 10 seconds over five
steps to a 120-second ceiling. This is a configuration-only change, not a database cutover or
an API release. Preserve these drop-ins and protected environment files during later releases;
see the [configuration maintenance record](最小生产发布运行手册.md#同机数据库直连与重启退避2026-09-22).

## Preconditions

### Regulation query release (2026-09-22)

Apply ordered migration `20260922000100` through the protected production workflow
before updating API and Worker to the same reviewed commit. The two new RPCs are
read-only and executable only by `market_data_api`; do not grant internal-table reads.
Preserve the current environment files, least-privilege credentials and restart drop-ins.
Restart the single Worker only after checking that no collection/recovery workflow is
active, outside the auction collection window. No job time or enablement changes are needed.

Authenticated smoke queries use `trade_date`, optional `limit` (1–500), and `cursor`:
`/api/v1/regulation/triggers` and `/api/v1/regulation/recent-events/next-triggers`.
Check `/healthz`, `/readyz`, `/openapi.json`, and `scripts/check_fastapi_release.py`.
PARTIAL computations return 200 with coverage; missing exact-date calculations return
404. Old calculations without `input_event_keys` also return 404 for recent events,
even when live events exist. Do not fabricate this list or silently use another version.
Future scheduled calculations capture it automatically; historical recomputation needs
separate explicit authorization. A 404 on a legacy run is not a failed migration.

The owner explicitly waived database/Raw backup and restore drills for this release
only. This does not waive migration permissions, isolated integration tests or runtime
health checks. No data is deleted by this migration; application rollback keeps the
additive schema and restores the previously verified release.

1. Apply repository migrations to the existing production PostgreSQL through the protected workflow.
   Do not copy or cut over data. Migration
   `20260809000100_create_fastapi_reader_role.sql` creates the NOLOGIN `market_data_api` role;
   `20260810000100_add_fastapi_limit_up_pool.sql` adds only the bounded limit-up query and its
   execute grant.
2. Create a separate LOGIN role outside source control, grant it `market_data_api`, and store its URL
   only as `FASTAPI_DATABASE_URL`. Never grant Worker, migration, ownership, or internal-schema rights.
3. Extract the verified API release under `/home/project-api` so installing or rolling back the API
   never replaces files used by the running Worker. Install production dependencies there with
   `uv sync --locked --extra api --no-dev`.
4. Create OS user/group `market-data-api` without a login shell. Install the API environment template
   as `/etc/market-data-center/api.env`, root-owned mode 0600, and replace placeholders locally.
5. Install the API unit, run `systemd-analyze verify`, and retain `FASTAPI_HOST=127.0.0.1`.
   The realtime quote route makes bounded outbound HTTPS requests to Tencent; keep
   `FASTAPI_TENCENT_QUOTE_DEADLINE_SECONDS=8` unless an accepted decision changes the limit.
6. For the live single-symbol auction endpoint, keep
   `FASTAPI_AUCTION_RAW_ROOT=/var/lib/market-data-api/raw`. The unit creates the owning state
   directory; verify it is writable only by the API OS identity. The API database login receives
   no table DML. Its sole mutation capability is EXECUTE on the bounded SECURITY DEFINER function
   that appends this domain's current-date, single-symbol facts.

The exact database setup gate is:

1. Apply the pending ordered API-role and limit-up query migrations with the protected migration
   connection. Verify backup/restore evidence before any production migration.
2. In a protected operator session, create a randomly named LOGIN role with a generated password,
   grant it membership in `market_data_api`, and set role defaults for read-only transactions and a
   five-second statement timeout. Store the resulting direct PostgreSQL URL only in the root-owned
   API environment file. Never reuse Worker or migration credentials.
3. Validate membership, function execution, lack of internal writes, and session read-only settings
   using `check_fastapi_release.py`. This preflight is required before service installation/start.

Before any start, load the protected environment and run the read-only preflight:

```bash
set -a; . /etc/market-data-center/api.env; set +a
/home/project-api/.venv/bin/python /home/project-api/scripts/check_fastapi_release.py --require-loopback
```

Starting/enabling the unit is a separate authorization. After an authorized start, run
`sudo sh /home/project-api/deploy/linux/api-smoke-check.sh`. It checks health, readiness, and one bounded
security query and cannot trigger collection. Do not open a firewall port or change the bind address.

Rollback stops/disables only the API unit and restores the prior API package. It never rolls back SQL
ad hoc and never stops the Worker unless a separately approved database cutover requires it.

The existing PostgreSQL host remains the source of truth for this deployment. The standalone
PostgreSQL cutover document is optional future work and is not an API deployment prerequisite.
