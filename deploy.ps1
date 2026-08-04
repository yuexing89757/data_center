[CmdletBinding()]
param(
    [string]$TaskName = "MarketDataCenter-Daily",
    [datetime]$At = "18:30",
    [switch]$SkipTask,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
$projectPath = $PSScriptRoot
$envFile = Join-Path $projectPath ".env"
$envExample = Join-Path $projectPath ".env.example"
$registerScript = Join-Path $projectPath "deploy\windows\register-daily-task.ps1"
$worker = Join-Path $projectPath ".venv\Scripts\market-data-center.exe"
$api = Join-Path $projectPath ".venv\Scripts\market-data-api.exe"

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    Copy-Item -LiteralPath $envExample -Destination $envFile
    throw "Created .env from .env.example. Fill DATABASE_URL, RAW_DATA_ROOT and PYTDX_VIPDOC_PATH, then run .\deploy.cmd again."
}

function Get-DotEnvValue {
    param([Parameter(Mandatory)][string]$Name)

    $line = Get-Content -LiteralPath $envFile |
        Where-Object { $_ -match "^\s*$([regex]::Escape($Name))\s*=" } |
        Select-Object -Last 1
    if ($null -eq $line) {
        return $null
    }

    return ($line -split "=", 2)[1].Trim().Trim('"').Trim("'")
}

foreach ($name in @("DATABASE_URL", "RAW_DATA_ROOT", "PYTDX_VIPDOC_PATH")) {
    $value = Get-DotEnvValue -Name $name
    if ([string]::IsNullOrWhiteSpace($value) -or $value.Contains("<")) {
        throw "Set $name in .env, then run .\deploy.cmd again."
    }
}

$apiKey = Get-DotEnvValue -Name "FASTAPI_API_KEY"
if ([string]::IsNullOrWhiteSpace($apiKey) -or $apiKey.Contains("<")) {
    $randomBytes = New-Object byte[] 32
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($randomBytes)
    }
    finally {
        $generator.Dispose()
    }
    $apiKey = -join ($randomBytes | ForEach-Object { $_.ToString("x2") })
    $envLines = [System.Collections.Generic.List[string]](Get-Content -LiteralPath $envFile)
    $apiKeyLine = -1
    for ($index = 0; $index -lt $envLines.Count; $index++) {
        if ($envLines[$index] -match "^\s*FASTAPI_API_KEY\s*=") {
            $apiKeyLine = $index
            break
        }
    }
    if ($apiKeyLine -ge 0) {
        $envLines[$apiKeyLine] = "FASTAPI_API_KEY=$apiKey"
    }
    else {
        $envLines.Add("FASTAPI_API_KEY=$apiKey")
    }
    [System.IO.File]::WriteAllLines(
        $envFile,
        $envLines,
        [System.Text.UTF8Encoding]::new($false)
    )
    Write-Host "Generated FASTAPI_API_KEY in .env."
}

$vipdocPath = Get-DotEnvValue -Name "PYTDX_VIPDOC_PATH"
if (-not (Test-Path -LiteralPath $vipdocPath -PathType Container)) {
    throw "PYTDX_VIPDOC_PATH does not exist or is not a directory."
}

$uv = Get-Command uv -ErrorAction SilentlyContinue
if ($null -eq $uv) {
    throw "uv is required. Install it with: winget install --id astral-sh.uv -e"
}

Push-Location -LiteralPath $projectPath
try {
    & $uv.Source sync --no-dev --locked
    if ($LASTEXITCODE -ne 0) {
        throw "uv sync failed with exit code $LASTEXITCODE."
    }

    if (-not (Test-Path -LiteralPath $worker -PathType Leaf)) {
        throw "Worker executable was not created: $worker"
    }
    if (-not (Test-Path -LiteralPath $api -PathType Leaf)) {
        throw "API executable was not created: $api"
    }

    & $worker --help *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Worker validation failed with exit code $LASTEXITCODE."
    }
    & $api --help *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "API validation failed with exit code $LASTEXITCODE."
    }

    if (-not $SkipTask) {
        & $registerScript -ProjectPath $projectPath -TaskName $TaskName -At $At
    }

    if ($RunNow) {
        & $worker daily-run
        if ($LASTEXITCODE -ne 0) {
            throw "Initial daily run failed with exit code $LASTEXITCODE."
        }
    }
}
finally {
    Pop-Location
}

Write-Host "Deployment completed."
Write-Host "Worker: $worker"
Write-Host "API: $api"
if (-not $SkipTask) {
    Write-Host "Scheduled task: $TaskName at $($At.ToString('HH:mm'))"
}
