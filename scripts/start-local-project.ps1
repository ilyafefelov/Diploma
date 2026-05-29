param(
    [int]$ApiPort = 8000,
    [int]$DashboardPort = 64163,
    [switch]$SkipCompose,
    [switch]$WithTelemetry
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$pythonPath = Join-Path $repoRoot ".venv\Scripts\python.exe"
$srcPath = Join-Path $repoRoot "src"
$logDir = Join-Path $repoRoot ".tmp_runtime\local-start"

function Test-CommandExists {
    param([string]$CommandName)
    return [bool](Get-Command $CommandName -ErrorAction SilentlyContinue)
}

function Test-PortListening {
    param([int]$Port)
    return [bool](Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
}

function Start-LoggedProcess {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory,
        [string]$LogPrefix
    )

    $stdoutPath = Join-Path $logDir "$LogPrefix.out.log"
    $stderrPath = Join-Path $logDir "$LogPrefix.err.log"

    return Start-Process `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -WorkingDirectory $WorkingDirectory `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -WindowStyle Hidden `
        -PassThru
}

if (-not (Test-Path $pythonPath)) {
    throw "Project virtual environment not found at $pythonPath. Run: uv sync --extra dev"
}

if (-not (Test-CommandExists "npm.cmd")) {
    throw "npm.cmd was not found on PATH. Install Node.js or open a shell where npm is available."
}

New-Item -ItemType Directory -Force $logDir | Out-Null
Set-Location $repoRoot

if (-not $SkipCompose) {
    if (-not (Test-CommandExists "docker")) {
        throw "docker was not found on PATH. Start Docker Desktop or rerun with -SkipCompose."
    }

    $composeServices = @(
        "postgres",
        "mqtt",
        "mlflow",
        "dagster-webserver",
        "dagster-daemon"
    )
    if ($WithTelemetry) {
        $composeServices += @("telemetry-ingestor", "telemetry-publisher")
    }

    Write-Host "Starting Docker services: $($composeServices -join ', ')" -ForegroundColor Cyan
    & docker compose up -d @composeServices
}

$env:PYTHONPATH = @($repoRoot, $srcPath, $env:PYTHONPATH) -ne "" -join [System.IO.Path]::PathSeparator
$env:SMART_ARBITRAGE_API_PORT = "$ApiPort"

if (Test-PortListening -Port $ApiPort) {
    Write-Host "FastAPI already listening on port $ApiPort; leaving it running." -ForegroundColor Yellow
} else {
    $apiProcess = Start-LoggedProcess `
        -FilePath $pythonPath `
        -ArgumentList @("-m", "uvicorn", "api.main:app", "--host", "127.0.0.1", "--port", "$ApiPort", "--reload") `
        -WorkingDirectory $repoRoot `
        -LogPrefix "api-$ApiPort"
    Write-Host "Started FastAPI process $($apiProcess.Id)." -ForegroundColor Green
}

if (Test-PortListening -Port $DashboardPort) {
    Write-Host "Dashboard already listening on port $DashboardPort; leaving it running." -ForegroundColor Yellow
} else {
    $dashboardProcess = Start-LoggedProcess `
        -FilePath "npm.cmd" `
        -ArgumentList @("-C", "dashboard", "run", "dev", "--", "--host", "127.0.0.1", "--port", "$DashboardPort") `
        -WorkingDirectory $repoRoot `
        -LogPrefix "dashboard-$DashboardPort"
    Write-Host "Started dashboard process $($dashboardProcess.Id)." -ForegroundColor Green
}

Write-Host ""
Write-Host "Local URLs" -ForegroundColor Cyan
Write-Host "  Dashboard: http://127.0.0.1:$DashboardPort/operator"
Write-Host "  API:       http://127.0.0.1:$ApiPort"
Write-Host "  API docs:  http://127.0.0.1:$ApiPort/docs"
Write-Host "  Dagster:   http://127.0.0.1:3001"
Write-Host "  MLflow:    http://127.0.0.1:5000"
Write-Host ""
Write-Host "Logs" -ForegroundColor Cyan
Write-Host "  $logDir"
