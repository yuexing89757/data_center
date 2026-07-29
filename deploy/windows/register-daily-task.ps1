param(
    [string]$ProjectPath = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$TaskName = "MarketDataCenter-Daily",
    [datetime]$At = "18:30"
)

$ErrorActionPreference = "Stop"
$runScript = Join-Path $ProjectPath "deploy\windows\run-daily.ps1"
if (-not (Test-Path -LiteralPath $runScript -PathType Leaf)) {
    throw "Daily runner not found: $runScript"
}

$arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$runScript`" -ProjectPath `"$ProjectPath`""
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $arguments
$trigger = New-ScheduledTaskTrigger -Daily -At $At
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Read the latest local TDX Daily Bars and synchronize Market Data Center." `
    -Force
