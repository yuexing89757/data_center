param(
    [string]$ProjectPath = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$HostAddress = "",
    [int]$Port = 0
)

$ErrorActionPreference = "Stop"
$api = Join-Path $ProjectPath ".venv\Scripts\market-data-api.exe"
if (-not (Test-Path -LiteralPath $api -PathType Leaf)) {
    throw "API executable not found: $api. Run '.\deploy.cmd' first."
}

$arguments = @()
if (-not [string]::IsNullOrWhiteSpace($HostAddress)) {
    $arguments += @("--host", $HostAddress)
}
if ($Port -gt 0) {
    $arguments += @("--port", $Port)
}

Set-Location -LiteralPath $ProjectPath
& $api @arguments
exit $LASTEXITCODE
