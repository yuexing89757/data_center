#!/usr/bin/env sh
set -eu

project_dir="${1:-/home/project-api}"
env_file="${2:-/etc/market-data-center/api.env}"

cd "$project_dir"
set -a
# The protected file must use shell-safe quoting as shown in the template.
. "$env_file"
set +a

.venv/bin/python scripts/check_fastapi_release.py --require-loopback
.venv/bin/python scripts/smoke_fastapi.py
