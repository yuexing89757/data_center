# Direct PostgreSQL FastAPI Linux runbook

This runbook packages but does not authorize deployment or public exposure. Worker and API remain
separate services, users, environment files, credentials, and failure domains.

## Preconditions

1. Apply repository migrations to the existing production PostgreSQL through the protected workflow.
   Do not copy or cut over data. Migration
   `20260809000100_create_fastapi_reader_role.sql` creates the NOLOGIN `market_data_api` role and its
   three function grants.
2. Create a separate LOGIN role outside source control, grant it `market_data_api`, and store its URL
   only as `FASTAPI_DATABASE_URL`. Never grant Worker, migration, ownership, or internal-schema rights.
3. Extract the verified API release under `/home/project-api` so installing or rolling back the API
   never replaces files used by the running Worker. Install production dependencies there with
   `uv sync --locked --extra api --no-dev`.
4. Create OS user/group `market-data-api` without a login shell. Install the API environment template
   as `/etc/market-data-center/api.env`, root-owned mode 0600, and replace placeholders locally.
5. Install the API unit, run `systemd-analyze verify`, and retain `FASTAPI_HOST=127.0.0.1`.

The exact database setup gate is:

1. Apply only the pending ordered API-role migration with the protected migration connection.
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
