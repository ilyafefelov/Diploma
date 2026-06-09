param(
    [int]$TotalAnchors = 365,
    [int]$BatchSize = 2,
    [int]$StartAnchorIndex = 0,
    [int]$EndAnchorIndex = 0,
    [ValidateSet("chronological", "latest_first")]
    [string]$AnchorBatchOrder = "chronological",
    [ValidateSet("compose", "host")]
    [string]$LocalMode = "host",
    [int]$NbeatsxMaxSteps = 20,
    [int]$TftMaxEpochs = 5,
    [int]$TftMaxSteps = 8,
    [int]$TftBatchSize = 8,
    [int]$TftHiddenSize = 8,
    [int]$TftHiddenContinuousSize = 4,
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
if ($NbeatsxMaxSteps -le 0) {
    throw "NbeatsxMaxSteps must be positive."
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
$runSlug = "poland-lag24-calibrated-" + ($GeneratedAtIso -replace "[:]", "" -replace "[^0-9A-Za-z_-]", "-")
$runDir = Join-Path $root ".tmp_runtime\poland_lag24_calibrated_batches\$runSlug"
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

function Read-LocalEnvFile {
    param([string]$Path)

    $values = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $values
    }
    foreach ($rawLine in Get-Content -LiteralPath $Path) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            continue
        }
        $key, $value = $line.Split("=", 2)
        $values[$key.Trim()] = $value.Trim().Trim('"').Trim("'")
    }
    return $values
}

function Resolve-EntsoeToken {
    $localEnvValues = Read-LocalEnvFile -Path (Join-Path $root ".env")
    $tokenKeys = @(
        "ENTSOE_TOKEN",
        "ENTSOE_SECURITY_TOKEN",
        "ENTSO_E_SECURITY_TOKEN",
        "entsoe_token",
        "entsoe_security_token",
        "entso_e_security_token"
    )
    foreach ($key in $tokenKeys) {
        $candidate = [System.Environment]::GetEnvironmentVariable($key)
        if (-not [string]::IsNullOrWhiteSpace($candidate)) {
            return $candidate
        }
        if ($localEnvValues.ContainsKey($key) -and -not [string]::IsNullOrWhiteSpace($localEnvValues[$key])) {
            return $localEnvValues[$key]
        }
    }
    return $null
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
        $dockerArgs = @("compose", "exec", "-T")
        if ($entsoeTokenAvailable) {
            $dockerArgs += @("-e", "ENTSOE_TOKEN")
        }
        $dockerArgs += @(
            "dagster-webserver",
            "uv", "run", "dagster", "asset", "materialize",
            "-m", "smart_arbitrage.defs",
            "--select", $Selection,
            "-c", $containerConfigPath
        )
        Invoke-ProcessWithLogs -Name $Name -FilePath "docker" -Arguments $dockerArgs
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
    if ($entsoeTokenAvailable) {
        $env:ENTSOE_TOKEN = $entsoeTokenValue
    }
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

$entsoeTokenValue = Resolve-EntsoeToken
$entsoeTokenAvailable = -not [string]::IsNullOrWhiteSpace($entsoeTokenValue)
if ($entsoeTokenAvailable -and [string]::IsNullOrWhiteSpace($env:ENTSOE_TOKEN)) {
    $env:ENTSOE_TOKEN = $entsoeTokenValue
}

$tenantIds = "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,client_004_kharkiv_hospital,client_005_odesa_hotel"
$polandModels = "nbeatsx_official_global_panel_poland_lag24_experimental_v1,tft_official_global_panel_poland_lag24_experimental_v1,nbeatsx_official_global_panel_poland_lag24_horizon_calibrated_v1,tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1"
$officialSelection = "observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame,entsoe_neighbor_market_feature_candidate_frame,nbu_eur_uah_fx_metadata_frame,poland_neighbor_market_snapshot_bronze,poland_neighbor_market_snapshot_feature_candidate_frame,entsoe_poland_lagged_feature_candidate_frame,entsoe_poland_feature_governance_frame,official_forecast_exogenous_governance_frame,official_forecast_exogenous_feature_route_frame,official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame"
if ($ReuseMaterializedInputs) {
    $officialSelection = "official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame"
}
$downstreamSelection = "official_global_panel_poland_lag24_experimental_nbeatsx_horizon_calibration_frame,official_global_panel_poland_lag24_experimental_tft_horizon_quantile_calibration_frame,official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame,dfl_poland_lag24_calibrated_schedule_candidate_library_frame,dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame,dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame,dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame,dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_frame,dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,dfl_poland_lag24_calibrated_vs_v2_plus_comparison_frame,dfl_poland_lag24_prior_tail_risk_veto_frame"

Write-RunLog "GeneratedAtIso=$GeneratedAtIso TotalAnchors=$TotalAnchors BatchSize=$BatchSize StartAnchorIndex=$StartAnchorIndex EndAnchorIndex=$ResolvedEndAnchorIndex AnchorBatchOrder=$AnchorBatchOrder LocalMode=$LocalMode NbeatsxMaxSteps=$NbeatsxMaxSteps TftMaxEpochs=$TftMaxEpochs TftMaxSteps=$TftMaxSteps ReuseMaterializedInputs=$ReuseMaterializedInputs EntsoeTokenAvailable=$entsoeTokenAvailable DagsterHome=$DagsterHome"

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
    "--enabled-official-models-csv", "nbeatsx_official_global_panel_poland_lag24_experimental_v1,tft_official_global_panel_poland_lag24_experimental_v1",
    "--nbeatsx-max-steps", "$NbeatsxMaxSteps",
    "--tft-max-epochs", "$TftMaxEpochs",
    "--asset-selection", $officialSelection,
    "--downstream-selection", $downstreamSelection,
    "--run-root", ".tmp_runtime/poland_lag24_calibrated_batches",
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

$lastBatchStartIndex = $null
for ($anchorIndex = $StartAnchorIndex; $anchorIndex -lt $ResolvedEndAnchorIndex; $anchorIndex += $BatchSize) {
    $lastBatchStartIndex = $anchorIndex
    $batchConfigPath = Join-Path $runDir ("poland-lag24-calibrated-batch-{0}.yaml" -f $anchorIndex)
    @"
# Generated from configs/real_data_official_global_panel_poland_lag24_calibrated_schedule_value_week3.yaml
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
  entsoe_neighbor_market_feature_candidate_frame:
    config:
      sample_country_codes_csv: "PL"
      sample_period_start_utc: "202412310000"
      sample_period_end_utc: "202604300000"
      fetch_enabled: true
  nbu_eur_uah_fx_metadata_frame:
    config:
      lag_hours: 24
      fetch_enabled: true
  poland_neighbor_market_snapshot_bronze:
    config:
      snapshot_csv_path: ""
      source_url: "https://transparencyplatform.zendesk.com/hc/en-us/articles/35960137882129-File-Library-Guide"
      source_access_method: "entsoe_fms_file_library"
      source_retrieved_at_utc: ""
      source_publication_timestamp_utc: ""
      source_license_status: "requires_entsoe_terms_mapping"
      snapshot_kind: "day_ahead_price_eur_mwh"
  poland_neighbor_market_snapshot_feature_candidate_frame:
    config:
      ua_decision_anchor_timestamp_utc: "2025-12-31T12:00:00+00:00"
      prior_eur_uah_fx_rate: 0.0
      prior_eur_uah_fx_timestamp_utc: ""
      fx_rate_source: ""
  entsoe_poland_feature_governance_frame:
    config:
      publication_timestamp_utc: ""
      ua_decision_anchor_timestamp_utc: "2025-12-31T12:00:00+00:00"
      prior_eur_uah_fx_rate: 0.0
      prior_eur_uah_fx_timestamp_utc: ""
      fx_rate_source: ""
      timezone_dst_mapping_ready: true
      licensing_approved: true
      market_rules_mapped: true
      domain_shift_validated: false
  entsoe_poland_lagged_feature_candidate_frame:
    config:
      lag_hours: 24
      prior_eur_uah_fx_rate: 0.0
      prior_eur_uah_fx_timestamp_utc: ""
      fx_rate_source: ""
  official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      enabled_forecast_model_names_csv: "nbeatsx_official_global_panel_poland_lag24_experimental_v1,tft_official_global_panel_poland_lag24_experimental_v1"
      max_eval_windows: $TotalAnchors
      anchor_batch_start_index: $anchorIndex
      anchor_batch_size: $BatchSize
      horizon_hours: 24
      nbeatsx_max_steps: $NbeatsxMaxSteps
      nbeatsx_random_seed: 20260520
      tft_max_epochs: $TftMaxEpochs
      tft_max_steps: $TftMaxSteps
      tft_batch_size: $TftBatchSize
      tft_learning_rate: $TftLearningRate
      tft_hidden_size: $TftHiddenSize
      tft_hidden_continuous_size: $TftHiddenContinuousSize
      tft_accelerator: "$TftAccelerator"
      tft_devices: "$TftDevices"
      anchor_batch_order: "$AnchorBatchOrder"
      resume_generated_at_iso: "$GeneratedAtIso"
      merge_persisted_batches: true
  official_global_panel_poland_lag24_experimental_nbeatsx_horizon_calibration_frame:
    config:
      min_prior_anchors: 30
      rolling_calibration_window_anchors: 60
  official_global_panel_poland_lag24_experimental_tft_horizon_quantile_calibration_frame:
    config:
      min_prior_anchors: 30
      rolling_calibration_window_anchors: 60
  dfl_poland_lag24_calibrated_schedule_candidate_library_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "$polandModels"
      final_validation_anchor_count_per_tenant: 18
      perturb_spread_scale_grid_csv: "0.9,1.1"
      perturb_mean_shift_grid_uah_mwh_csv: "-250.0,250.0"
  dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame:
    config:
      blend_weights_csv: "0.25,0.5,0.75"
      residual_min_prior_anchors: 14
      min_final_holdout_tenant_anchor_count_per_source_model: 90
  dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame:
    config:
      rank_perturbation_delta_uah_mwh: 250.0
      robust_spread_scales_csv: "0.8,0.9"
      strict_neighborhood_shift_hours_csv: "-1,1"
      block_reconcile_hours_csv: "3,6"
      terminal_target_shift_uah_mwh: 100.0
  dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "$polandModels"
      final_validation_anchor_count_per_tenant: 18
      min_validation_tenant_anchor_count_per_source_model: 90
  dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "$polandModels"
      final_validation_anchor_count_per_tenant: 18
      min_validation_tenant_anchor_count_per_source_model: 90
      min_prior_mean_improvement_ratio_vs_v2: 0.01
  dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame:
    config:
      tenant_ids_csv: "$tenantIds"
      forecast_model_names_csv: "$polandModels"
      final_validation_anchor_count_per_tenant: 18
      min_validation_tenant_anchor_count_per_source_model: 90
      min_prior_mean_improvement_ratio_vs_v2: 0.01
"@ | Set-Content -LiteralPath $batchConfigPath -Encoding UTF8

    Invoke-DagsterMaterialize `
        -Name ("poland-lag24-calibrated-batch-{0}" -f $anchorIndex) `
        -Selection $officialSelection `
        -ConfigPath $batchConfigPath
}

if (-not $SkipDownstreamGate) {
    if ($null -eq $lastBatchStartIndex) {
        throw "No Poland lag-24 batches were materialized before downstream gate."
    }
    $downstreamConfigPath = Join-Path $runDir "poland-lag24-calibrated-downstream.yaml"
    Copy-Item `
        -LiteralPath (Join-Path $runDir ("poland-lag24-calibrated-batch-{0}.yaml" -f $lastBatchStartIndex)) `
        -Destination $downstreamConfigPath `
        -Force
    Invoke-DagsterMaterialize `
        -Name "poland-lag24-calibrated-downstream" `
        -Selection $downstreamSelection `
        -ConfigPath $downstreamConfigPath
}

Write-RunLog "COMPLETE Poland lag-24 calibrated batch run"
Write-Output "Run directory: $runDir"
