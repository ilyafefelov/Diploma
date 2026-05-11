param(
    [int]$TotalAnchors = 365,
    [int]$BatchSize = 4,
    [int]$StartAnchorIndex = 0,
    [int]$EndAnchorIndex = 0,
    [ValidateSet("chronological", "latest_first")]
    [string]$AnchorBatchOrder = "chronological",
    [int]$NbeatsxMaxSteps = 25,
    [string]$GeneratedAtIso = "",
    [int]$BatchTimeoutSeconds = 7200,
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
if ($NbeatsxMaxSteps -le 0) {
    throw "NbeatsxMaxSteps must be positive."
}
if ([string]::IsNullOrWhiteSpace($GeneratedAtIso)) {
    $GeneratedAtIso = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")
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
$runSlug = "official-global-panel-" + ($GeneratedAtIso -replace "[:]", "" -replace "[^0-9A-Za-z_-]", "-")
$runDir = Join-Path $root ".tmp_runtime\official_global_panel_batches\$runSlug"
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
$officialSelection = "observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_exogenous_governance_frame,nbeatsx_official_global_panel_rolling_strict_lp_benchmark_frame"
$downstreamSelection = "nbeatsx_official_global_panel_rolling_horizon_calibration_frame,nbeatsx_official_global_panel_rolling_calibrated_strict_lp_benchmark_frame,dfl_official_global_panel_schedule_candidate_library_frame,dfl_official_global_panel_schedule_candidate_library_v2_frame,dfl_official_global_panel_schedule_value_learner_v2_frame,dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame,dfl_official_global_panel_schedule_value_learner_v2_robustness_frame,dfl_official_global_panel_schedule_value_production_gate_frame"

Write-RunLog "GeneratedAtIso=$GeneratedAtIso TotalAnchors=$TotalAnchors BatchSize=$BatchSize StartAnchorIndex=$StartAnchorIndex EndAnchorIndex=$ResolvedEndAnchorIndex AnchorBatchOrder=$AnchorBatchOrder NbeatsxMaxSteps=$NbeatsxMaxSteps"

for ($anchorIndex = $StartAnchorIndex; $anchorIndex -lt $ResolvedEndAnchorIndex; $anchorIndex += $BatchSize) {
    $batchConfigPath = Join-Path $runDir ("official-global-panel-batch-{0}.yaml" -f $anchorIndex)
    @"
# Generated from configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml
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
  nbeatsx_official_global_panel_rolling_strict_lp_benchmark_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      max_eval_windows: $TotalAnchors
      anchor_batch_start_index: $anchorIndex
      anchor_batch_size: $BatchSize
      horizon_hours: 24
      nbeatsx_max_steps: $NbeatsxMaxSteps
      nbeatsx_random_seed: 20260511
      anchor_batch_order: "$AnchorBatchOrder"
      resume_generated_at_iso: "$GeneratedAtIso"
      merge_persisted_batches: true
  nbeatsx_official_global_panel_rolling_horizon_calibration_frame:
    config:
      min_prior_anchors: 14
      rolling_calibration_window_anchors: 28
  dfl_official_global_panel_schedule_candidate_library_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "nbeatsx_official_global_panel_v1,nbeatsx_official_global_panel_horizon_calibrated_v1"
      final_validation_anchor_count_per_tenant: 18
      perturb_spread_scale_grid_csv: "0.9,1.1"
      perturb_mean_shift_grid_uah_mwh_csv: "-250.0,250.0"
  dfl_official_global_panel_schedule_candidate_library_v2_frame:
    config:
      blend_weights_csv: "0.25,0.5,0.75"
      residual_min_prior_anchors: 14
      min_final_holdout_tenant_anchor_count_per_source_model: 90
  dfl_official_global_panel_schedule_value_learner_v2_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "nbeatsx_official_global_panel_v1,nbeatsx_official_global_panel_horizon_calibrated_v1"
      final_validation_anchor_count_per_tenant: 18
      min_validation_tenant_anchor_count_per_source_model: 90
  dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "nbeatsx_official_global_panel_v1,nbeatsx_official_global_panel_horizon_calibrated_v1"
      final_validation_anchor_count_per_tenant: 18
      min_validation_tenant_anchor_count_per_source_model: 90
  dfl_official_global_panel_schedule_value_learner_v2_robustness_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "nbeatsx_official_global_panel_v1,nbeatsx_official_global_panel_horizon_calibrated_v1"
      validation_window_count: 4
      validation_anchor_count: 18
      min_prior_anchors_before_window: 30
      min_robust_passing_windows: 3
      min_validation_tenant_anchor_count_per_source_model: 90
  dfl_official_global_panel_schedule_value_production_gate_frame:
    config:
      source_model_names_csv: "nbeatsx_official_global_panel_v1,nbeatsx_official_global_panel_horizon_calibrated_v1"
      min_tenant_count: 5
      min_validation_tenant_anchor_count_per_source_model: 90
      min_mean_regret_improvement_ratio: 0.05
      min_rolling_window_count: 4
      min_rolling_strict_pass_windows: 3
"@ | Set-Content -LiteralPath $batchConfigPath -Encoding UTF8

    $containerId = (& docker compose ps -q dagster-webserver).Trim()
    if ([string]::IsNullOrWhiteSpace($containerId)) {
        throw "dagster-webserver container is not running."
    }
    $containerConfigPath = "/tmp/official-global-panel-batch-$anchorIndex.yaml"
    & docker cp $batchConfigPath "${containerId}:$containerConfigPath"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to copy $batchConfigPath into dagster-webserver container."
    }

    Invoke-DockerProcess `
        -Name ("official-global-panel-batch-{0}" -f $anchorIndex) `
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
        -Name "official-global-panel-downstream-gate" `
        -Arguments @(
            "compose", "exec", "-T", "dagster-webserver",
            "uv", "run", "dagster", "asset", "materialize",
            "-m", "smart_arbitrage.defs",
            "--select", $downstreamSelection,
            "-c", "configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml"
        )
}

Write-RunLog "COMPLETE official global-panel batch run"
Write-Output "Run directory: $runDir"
