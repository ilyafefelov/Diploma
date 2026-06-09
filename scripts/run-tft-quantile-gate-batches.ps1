param(
    [int]$TotalAnchors = 365,
    [int]$BatchSize = 2,
    [int]$StartAnchorIndex = 0,
    [int]$EndAnchorIndex = 0,
    [ValidateSet("chronological", "latest_first")]
    [string]$AnchorBatchOrder = "chronological",
    [ValidateSet("compose", "host")]
    [string]$LocalMode = "host",
    [int]$TftMaxEpochs = 30,
    [int]$TftMaxSteps = 20,
    [int]$TftBatchSize = 8,
    [int]$TftHiddenSize = 12,
    [int]$TftHiddenContinuousSize = 6,
    [double]$TftLearningRate = 0.005,
    [string]$TftAccelerator = "auto",
    [string]$TftDevices = "auto",
    [string]$GeneratedAtIso = "",
    [int]$BatchTimeoutSeconds = 7200,
    [string]$HostPostgresDsn = "",
    [string]$HostMlflowTrackingUri = "http://localhost:5000",
    [string]$DagsterHome = "",
    [switch]$ReuseMaterializedInputs,
    [switch]$SkipDownstreamGate
)

$ErrorActionPreference = "Stop"

if ($TotalAnchors -le 0) {
    throw "TotalAnchors must be positive."
}
if ($BatchSize -le 0) {
    throw "BatchSize must be positive."
}
if ($StartAnchorIndex -lt 0) {
    throw "StartAnchorIndex must be non-negative."
}
if ($EndAnchorIndex -lt 0) {
    throw "EndAnchorIndex must be non-negative."
}
if ($TftMaxEpochs -le 0) {
    throw "TftMaxEpochs must be positive."
}
if ($TftMaxSteps -le 0) {
    throw "TftMaxSteps must be positive."
}
if ($TftBatchSize -le 0) {
    throw "TftBatchSize must be positive."
}
if ([string]::IsNullOrWhiteSpace($GeneratedAtIso)) {
    $GeneratedAtIso = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")
}
if ([string]::IsNullOrWhiteSpace($HostPostgresDsn)) {
    $hostPostgresPort = $env:SMART_ARBITRAGE_POSTGRES_PORT
    if ([string]::IsNullOrWhiteSpace($hostPostgresPort)) {
        $hostPostgresPort = "5432"
    }
    $HostPostgresDsn = "postgresql://smart:arbitrage@localhost:$hostPostgresPort/smart_arbitrage"
}
$ResolvedEndAnchorIndex = $TotalAnchors
if ($EndAnchorIndex -gt 0) {
    $ResolvedEndAnchorIndex = $EndAnchorIndex
}
if ($ResolvedEndAnchorIndex -le $StartAnchorIndex) {
    throw "EndAnchorIndex must be greater than StartAnchorIndex when provided."
}
if ($ResolvedEndAnchorIndex -gt $TotalAnchors) {
    throw "EndAnchorIndex cannot exceed TotalAnchors."
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runSlug = "tft-quantile-gate-" + ($GeneratedAtIso -replace "[:]", "" -replace "[^0-9A-Za-z_-]", "-")
$runDir = Join-Path $root ".tmp_runtime\tft_quantile_gate_batches\$runSlug"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$runLog = Join-Path $runDir "run.log"
if (-not [string]::IsNullOrWhiteSpace($DagsterHome)) {
    if (-not [System.IO.Path]::IsPathRooted($DagsterHome)) {
        $DagsterHome = Join-Path $root $DagsterHome
    }
    New-Item -ItemType Directory -Force -Path $DagsterHome | Out-Null
}

function Write-RunLog {
    param([string]$Message)
    $line = "[$((Get-Date).ToUniversalTime().ToString("s"))Z] $Message"
    $line | Tee-Object -FilePath $runLog -Append
}

function Invoke-ProcessWithLogs {
    param(
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$Name
    )
    $stdoutPath = Join-Path $runDir "$Name.out.log"
    $stderrPath = Join-Path $runDir "$Name.err.log"
    Write-RunLog "START $Name $FilePath $($Arguments -join ' ')"
    $process = Start-Process `
        -FilePath $FilePath `
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

function Invoke-DagsterMaterialize {
    param(
        [string]$Name,
        [string]$Selection,
        [string]$ConfigPath
    )
    if ($LocalMode -eq "compose") {
        $containerId = (& docker compose ps -q dagster-webserver).Trim()
        if ([string]::IsNullOrWhiteSpace($containerId)) {
            throw "dagster-webserver container is not running."
        }
        $containerConfigPath = "/tmp/$Name.yaml"
        & docker cp $ConfigPath "${containerId}:$containerConfigPath"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to copy $ConfigPath into dagster-webserver container."
        }
        Invoke-ProcessWithLogs `
            -Name $Name `
            -FilePath "docker" `
            -Arguments @(
                "compose", "exec", "-T", "dagster-webserver",
                "uv", "run", "dagster", "asset", "materialize",
                "-m", "smart_arbitrage.defs",
                "--select", $Selection,
                "-c", $containerConfigPath
            )
        return
    }

    $env:PYTHONPATH = "$root;$root\src"
    $env:SMART_ARBITRAGE_MARKET_DATA_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_FORECAST_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_DFL_TRAINING_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_BATTERY_TELEMETRY_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_SIMULATED_TRADE_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_OPERATOR_STATUS_DSN = $HostPostgresDsn
    $env:MLFLOW_TRACKING_URI = $HostMlflowTrackingUri
    if (-not [string]::IsNullOrWhiteSpace($DagsterHome)) {
        $env:DAGSTER_HOME = $DagsterHome
    }
    $dagsterExe = Join-Path $root ".venv\Scripts\dagster.exe"
    if (Test-Path -LiteralPath $dagsterExe) {
        Invoke-ProcessWithLogs `
            -Name $Name `
            -FilePath $dagsterExe `
            -Arguments @(
                "asset", "materialize",
                "-m", "smart_arbitrage.defs",
                "--select", $Selection,
                "-c", $ConfigPath
            )
        return
    }

    Invoke-ProcessWithLogs `
        -Name $Name `
        -FilePath "uv" `
        -Arguments @(
            "run", "dagster", "asset", "materialize",
            "-m", "smart_arbitrage.defs",
            "--select", $Selection,
            "-c", $ConfigPath
        )
}

$tenantIds = "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,client_004_kharkiv_hospital,client_005_odesa_hotel"
$calibratedTftModels = "tft_official_global_panel_p10_v1_horizon_quantile_calibrated_v1,tft_official_global_panel_v1_horizon_quantile_calibrated_v1,tft_official_global_panel_p90_v1_horizon_quantile_calibrated_v1"
$officialSelection = "observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_exogenous_governance_frame,tft_official_global_panel_rolling_strict_lp_benchmark_frame"
if ($ReuseMaterializedInputs) {
    $officialSelection = "tft_official_global_panel_rolling_strict_lp_benchmark_frame"
}
$downstreamSelection = "tft_official_global_panel_horizon_quantile_calibration_frame,tft_official_global_panel_horizon_quantile_calibrated_strict_lp_benchmark_frame,dfl_tft_calibrated_quantile_schedule_candidate_library_frame,dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame,dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame"

Write-RunLog "GeneratedAtIso=$GeneratedAtIso TotalAnchors=$TotalAnchors BatchSize=$BatchSize StartAnchorIndex=$StartAnchorIndex EndAnchorIndex=$ResolvedEndAnchorIndex AnchorBatchOrder=$AnchorBatchOrder LocalMode=$LocalMode TftMaxEpochs=$TftMaxEpochs TftMaxSteps=$TftMaxSteps ReuseMaterializedInputs=$ReuseMaterializedInputs DagsterHome=$DagsterHome"

$manifestPath = Join-Path $runDir "attempt_manifest.json"
$manifestArgs = @(
    "run", "python", "scripts/build_official_evidence_attempt_manifest.py",
    "--attempt-kind", "official_global_panel_backfill",
    "--generated-at-iso", $GeneratedAtIso,
    "--total-anchors", "$TotalAnchors",
    "--batch-size", "$BatchSize",
    "--start-anchor-index", "$StartAnchorIndex",
    "--end-anchor-index", "$ResolvedEndAnchorIndex",
    "--anchor-batch-order", $AnchorBatchOrder,
    "--enabled-official-models-csv", "tft_official_global_panel_v1",
    "--nbeatsx-max-steps", "0",
    "--tft-max-epochs", "$TftMaxEpochs",
    "--asset-selection", $officialSelection,
    "--downstream-selection", $downstreamSelection,
    "--run-root", ".tmp_runtime/tft_quantile_gate_batches",
    "--output", $manifestPath
)
if ($SkipDownstreamGate) {
    $manifestArgs += "--skip-downstream-gate"
}
& uv @manifestArgs
if ($LASTEXITCODE -ne 0) {
    throw "Failed to write attempt manifest at $manifestPath."
}
Write-RunLog "WROTE attempt_manifest.json"

for ($anchorIndex = $StartAnchorIndex; $anchorIndex -lt $ResolvedEndAnchorIndex; $anchorIndex += $BatchSize) {
    $batchConfigPath = Join-Path $runDir ("tft-quantile-gate-batch-{0}.yaml" -f $anchorIndex)
    @"
# Generated from configs/real_data_official_global_panel_tft_quantile_schedule_value_365_week3.yaml
ops:
  observed_market_price_history_bronze:
    config:
      start_date: "2025-01-01"
      end_date: "2026-04-30"
  tenant_historical_weather_bronze:
    config:
      tenant_ids_csv: "$tenantIds"
      start_date: "2025-01-01"
      end_date: "2026-04-30"
      location_config_path: "simulations/tenants.yml"
  official_global_panel_training_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      horizon_hours: 24
      temporal_scaler_type: "robust"
  tft_official_global_panel_rolling_strict_lp_benchmark_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      max_eval_windows: $TotalAnchors
      anchor_batch_start_index: $anchorIndex
      anchor_batch_size: $BatchSize
      horizon_hours: 24
      anchor_batch_order: "$AnchorBatchOrder"
      resume_generated_at_iso: "$GeneratedAtIso"
      merge_persisted_batches: true
      tft_max_epochs: $TftMaxEpochs
      tft_max_steps: $TftMaxSteps
      tft_batch_size: $TftBatchSize
      tft_learning_rate: $TftLearningRate
      tft_hidden_size: $TftHiddenSize
      tft_hidden_continuous_size: $TftHiddenContinuousSize
      tft_accelerator: "$TftAccelerator"
      tft_devices: "$TftDevices"
  tft_official_global_panel_horizon_quantile_calibration_frame:
    config:
      min_prior_anchors: 30
      rolling_calibration_window_anchors: 60
  dfl_tft_calibrated_quantile_schedule_candidate_library_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "$calibratedTftModels"
      final_validation_anchor_count_per_tenant: 18
      perturb_spread_scale_grid_csv: "0.85,0.95,1.05,1.15"
      perturb_mean_shift_grid_uah_mwh_csv: "-300.0,-150.0,150.0,300.0"
  dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "$calibratedTftModels"
      final_validation_anchor_count_per_tenant: 18
      min_validation_tenant_anchor_count_per_source_model: 90
      min_prior_mean_improvement_ratio_vs_v2: 0.01
      min_mean_regret_improvement_ratio_vs_baseline: 0.0
  dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "$calibratedTftModels"
      final_validation_anchor_count_per_tenant: 18
      min_validation_tenant_anchor_count_per_source_model: 90
      min_prior_mean_improvement_ratio_vs_v2: 0.01
      min_mean_regret_improvement_ratio_vs_baseline: 0.0
"@ | Set-Content -LiteralPath $batchConfigPath -Encoding UTF8

    Invoke-DagsterMaterialize `
        -Name ("tft-quantile-gate-batch-{0}" -f $anchorIndex) `
        -Selection $officialSelection `
        -ConfigPath $batchConfigPath
}

if (-not $SkipDownstreamGate) {
    $downstreamConfigPath = Join-Path $runDir "tft-quantile-gate-downstream.yaml"
    Copy-Item `
        -LiteralPath (Join-Path $root "configs\real_data_official_global_panel_tft_quantile_schedule_value_365_week3.yaml") `
        -Destination $downstreamConfigPath `
        -Force
    Invoke-DagsterMaterialize `
        -Name "tft-quantile-gate-downstream" `
        -Selection $downstreamSelection `
        -ConfigPath $downstreamConfigPath
}

Write-RunLog "COMPLETE TFT quantile gate batch run"
Write-Output "Run directory: $runDir"
