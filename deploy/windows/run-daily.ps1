param(
    [string]$ProjectPath = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)

$ErrorActionPreference = "Stop"
$worker = Join-Path $ProjectPath ".venv\Scripts\market-data-center.exe"
if (-not (Test-Path -LiteralPath $worker -PathType Leaf)) {
    throw "Worker executable not found: $worker. Run 'uv sync --all-groups' first."
}

Set-Location -LiteralPath $ProjectPath
& $worker daily-run
exit $LASTEXITCODE
