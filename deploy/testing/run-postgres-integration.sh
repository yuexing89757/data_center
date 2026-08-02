#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
compose_file="$script_dir/compose.yml"
export TEST_DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:55432/postgres"

cleanup() {
  docker compose -f "$compose_file" down --volumes
}
trap cleanup EXIT INT TERM

docker compose -f "$compose_file" up -d --wait
uv run pytest -m integration
