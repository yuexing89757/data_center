# Start the Market Data Center API and the long-lived scheduler worker.
#
# The worker hosts the APScheduler-based job catalog (daily-run, daily
# indicators, stock pools, deducted profit, stale-run recovery and the
# optional auction collection). Starting it here means every scheduled
# job is driven by the in-process scheduler; no Windows Task Scheduler
# entry is registered.
#
# The worker reads PYTDX_VIPDOC_PATH via os.getenv (not from WorkerSettings),
# so we export it from .env here as a deployment-layer bridge.
param(
    [string]$ProjectPath = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)

$ErrorActionPreference = "Stop"

$api = Join-Path $ProjectPath ".venv\Scripts\market-data-api.exe"
$worker = Join-Path $ProjectPath ".venv\Scripts\market-data-center.exe"
$envFile = Join-Path $ProjectPath ".env"

if (-not (Test-Path -LiteralPath $api -PathType Leaf)) {
    throw "API executable not found: $api. Run '.\deploy.cmd' first."
}
if (-not (Test-Path -LiteralPath $worker -PathType Leaf)) {
    throw "Worker executable not found: $worker. Run '.\deploy.cmd' first."
}
if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw ".env not found at $envFile. Run '.\deploy.cmd' first to create it."
}

# Bridge PYTDX_VIPDOC_PATH from .env into the process environment so the
# pytdx provider (os.getenv) can see it. pydantic-settings does not push
# .env values into os.environ.
function Get-DotEnvValue {
    param([Parameter(Mandatory)][string]$Name)
    $line = Get-Content -LiteralPath $envFile |
        Where-Object { $_ -match "^\s*$([regex]::Escape($Name))\s*=" } |
        Select-Object -Last 1
    if ($null -eq $line) { return $null }
    return ($line -split "=", 2)[1].Trim().Trim('"').Trim("'")
}

$vipdoc = Get-DotEnvValue -Name "PYTDX_VIPDOC_PATH"
if (-not [string]::IsNullOrWhiteSpace($vipdoc)) {
    $env:PYTDX_VIPDOC_PATH = $vipdoc
}

Set-Location -LiteralPath $ProjectPath

Write-Host "Starting Market Data Center services (API + worker)..."
Write-Host "  Project: $ProjectPath"
Write-Host "  API:     $api  (http://127.0.0.1:8000)"
Write-Host "  Worker:  $worker worker  (APScheduler drives all jobs)"
Write-Host "Press Ctrl+C in each window to stop a service."

# Launch each long-lived process in its own console window so their logs
# stay visible and each can be stopped independently.
Start-Process -FilePath $api -ArgumentList @("--host", "127.0.0.1", "--port", "8000") -WorkingDirectory $ProjectPath
Start-Process -FilePath $worker -ArgumentList @("worker") -WorkingDirectory $ProjectPath

Write-Host "Services launched."
