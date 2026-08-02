$ErrorActionPreference = "Stop"

$composeFile = Join-Path $PSScriptRoot "compose.yml"
$env:TEST_DATABASE_URL = "postgresql://postgres:postgres@127.0.0.1:55432/postgres"

try {
    docker compose -f $composeFile up -d --wait
    uv run pytest -m integration
}
finally {
    docker compose -f $composeFile down --volumes
}
