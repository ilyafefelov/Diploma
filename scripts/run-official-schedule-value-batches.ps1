param(
    [int]$TotalAnchorsPerTenant = 104,
    [int]$BatchSize = 4,
    [int]$StartAnchorIndex = 0,
    [ValidateSet("chronological", "latest_first")]
    [string]$AnchorBatchOrder = "chronological",
    [string]$EnabledOfficialModelsCsv = "nbeatsx_official_v0,tft_official_v0",
    [int]$NbeatsxMaxSteps = 100,
    [int]$TftMaxEpochs = 15,
    [string]$GeneratedAtIso = "",
    [int]$BatchTimeoutSeconds = 7200,
    [switch]$SkipDownstreamGate
)

$ErrorActionPreference = "Stop"

if ($TotalAnchorsPerTenant -le 0) {
    throw "TotalAnchorsPerTenant must be positive."
}
if ($BatchSize -le 0) {
    throw "BatchSize must be positive."
}
if ($StartAnchorIndex -lt 0) {
    throw "StartAnchorIndex must be non-negative."
}
if ([string]::IsNullOrWhiteSpace($EnabledOfficialModelsCsv)) {
    throw "EnabledOfficialModelsCsv must contain at least one official model."
}
if ($NbeatsxMaxSteps -le 0) {
    throw "NbeatsxMaxSteps must be positive."
}
if ($TftMaxEpochs -le 0) {
    throw "TftMaxEpochs must be positive."
}
if ([string]::IsNullOrWhiteSpace($GeneratedAtIso)) {
    $GeneratedAtIso = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runSlug = "official-schedule-value-" + ($GeneratedAtIso -replace "[:]", "" -replace "[^0-9A-Za-z_-]", "-")
$runDir = Join-Path $root ".tmp_runtime\official_schedule_value_batches\$runSlug"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$runLog = Join-Path $runDir "run.log"

function Write-RunLog {
    param([string]$Message)
    $line = "[$((Get-Date).ToUniversalTime().ToString("s"))Z] $Message"
    $line | Tee-Object -FilePath $runLog -Append
}

function Invoke-DockerProcess {
    param(
        [string[]]$Arguments,
        [string]$Name
    )
    $stdoutPath = Join-Path $runDir "$Name.out.log"
    $stderrPath = Join-Path $runDir "$Name.err.log"
    Write-RunLog "START $Name docker $($Arguments -join ' ')"
    $process = Start-Process `
        -FilePath "docker" `
        -ArgumentList $Arguments `
        -WorkingDirectory $root `
        -NoNewWindow `
        -PassThru `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath
    if ($BatchTimeoutSeconds -gt 0) {
        Wait-Process -Id $process.Id -Timeout $BatchTimeoutSeconds -ErrorAction SilentlyContinue
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            Write-RunLog "TIMEOUT $Name after $BatchTimeoutSeconds seconds"
            throw "$Name timed out after $BatchTimeoutSeconds seconds. See $stdoutPath and $stderrPath"
        }
    } else {
        $process.WaitForExit()
    }
    $process.WaitForExit()
    $process.Refresh()
    $exitCode = $process.ExitCode
    if ($null -eq $exitCode) {
        if (Select-String -LiteralPath $stderrPath -Pattern "RUN_SUCCESS" -Quiet) {
            Write-RunLog "DONE $Name via Dagster RUN_SUCCESS marker"
            return
        }
        Write-RunLog "FAILED $Name exit=<missing>"
        throw "$Name finished without an exit code and no Dagster RUN_SUCCESS marker. See $stdoutPath and $stderrPath"
    }
    if ($exitCode -ne 0) {
        Write-RunLog "FAILED $Name exit=$exitCode"
        throw "$Name failed with exit code $exitCode. See $stdoutPath and $stderrPath"
    }
    Write-RunLog "DONE $Name"
}

$tenantIds = "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,client_004_kharkiv_hospital,client_005_odesa_hotel"
$officialSelection = "observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_rolling_origin_benchmark_frame"
$downstreamSelection = "dfl_official_schedule_candidate_library_frame,dfl_official_schedule_candidate_library_v2_frame,dfl_official_schedule_value_learner_v2_frame,dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame,dfl_official_schedule_value_learner_v2_robustness_frame,dfl_official_schedule_value_production_gate_frame"

Write-RunLog "GeneratedAtIso=$GeneratedAtIso TotalAnchorsPerTenant=$TotalAnchorsPerTenant BatchSize=$BatchSize StartAnchorIndex=$StartAnchorIndex AnchorBatchOrder=$AnchorBatchOrder EnabledOfficialModelsCsv=$EnabledOfficialModelsCsv NbeatsxMaxSteps=$NbeatsxMaxSteps TftMaxEpochs=$TftMaxEpochs"

for ($anchorIndex = $StartAnchorIndex; $anchorIndex -lt $TotalAnchorsPerTenant; $anchorIndex += $BatchSize) {
    $batchConfigPath = Join-Path $runDir ("official-batch-{0}.yaml" -f $anchorIndex)
    @"
ops:
  official_forecast_rolling_origin_benchmark_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      max_eval_anchors_per_tenant: $TotalAnchorsPerTenant
      anchor_batch_start_index: $anchorIndex
      anchor_batch_size: $BatchSize
      anchor_batch_order: "$AnchorBatchOrder"
      enabled_official_model_names_csv: "$EnabledOfficialModelsCsv"
      resume_generated_at_iso: "$GeneratedAtIso"
      merge_persisted_batches: true
      horizon_hours: 24
      nbeatsx_max_steps: $NbeatsxMaxSteps
      nbeatsx_random_seed: 20260511
      tft_max_epochs: $TftMaxEpochs
      tft_batch_size: 32
      tft_learning_rate: 0.005
      tft_hidden_size: 12
      tft_hidden_continuous_size: 6
"@ | Set-Content -LiteralPath $batchConfigPath -Encoding UTF8

    $containerId = (& docker compose ps -q dagster-webserver).Trim()
    if ([string]::IsNullOrWhiteSpace($containerId)) {
        throw "dagster-webserver container is not running."
    }
    $containerConfigPath = "/tmp/official-batch-$anchorIndex.yaml"
    & docker cp $batchConfigPath "${containerId}:$containerConfigPath"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to copy $batchConfigPath into dagster-webserver container."
    }

    Invoke-DockerProcess `
        -Name ("official-batch-{0}" -f $anchorIndex) `
        -Arguments @(
            "compose", "exec", "-T", "dagster-webserver",
            "uv", "run", "dagster", "asset", "materialize",
            "-m", "smart_arbitrage.defs",
            "--select", $officialSelection,
            "-c", $containerConfigPath
        )
}

if (-not $SkipDownstreamGate) {
    Invoke-DockerProcess `
        -Name "official-downstream-gate" `
        -Arguments @(
            "compose", "exec", "-T", "dagster-webserver",
            "uv", "run", "dagster", "asset", "materialize",
            "-m", "smart_arbitrage.defs",
            "--select", $downstreamSelection,
            "-c", "configs/real_data_official_schedule_value_promotion_week3.yaml"
        )
}

Write-RunLog "COMPLETE official schedule/value batch run"
Write-Output "Run directory: $runDir"
