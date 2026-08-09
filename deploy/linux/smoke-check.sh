#!/usr/bin/env sh
set -eu

project_dir="${1:-/home/project}"
cd "$project_dir"

if [ ! -x .venv/bin/market-data-center ]; then
    echo "worker executable is missing: $project_dir/.venv/bin/market-data-center" >&2
    exit 1
fi

systemctl is-active --quiet market-data-center-worker.service
.venv/bin/market-data-center worker --check
.venv/bin/python scripts/check_pytdx_daily_bar_endpoints.py --require-all

if [ -z "${MIGRATION_DATABASE_URL:-}" ]; then
    echo "MIGRATION_DATABASE_URL is required for the read-only migration check" >&2
    exit 1
fi
if [ -z "${DATABASE_URL:-}" ]; then
    echo "DATABASE_URL is required for the read-only data smoke check" >&2
    exit 1
fi

.venv/bin/python scripts/apply_migrations.py check --postgres-only
.venv/bin/python scripts/smoke_check.py --postgres-only
