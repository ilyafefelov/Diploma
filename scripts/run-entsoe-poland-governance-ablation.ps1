param(
    [string]$ConfigPath = "configs\real_data_dfl_entsoe_poland_feature_ablation_week3.yaml",
    [string]$RunSlug = "week3_dfl_entsoe_poland_feature_ablation_v1",
    [string]$OutputRoot = "data\research_runs",
    [switch]$SkipMaterialization,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$pythonPath = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $pythonPath)) {
    $pythonPath = "python"
}

$resolvedConfigPath = if ([System.IO.Path]::IsPathRooted($ConfigPath)) {
    $ConfigPath
} else {
    Join-Path $projectRoot $ConfigPath
}
if (-not (Test-Path -LiteralPath $resolvedConfigPath)) {
    throw "ConfigPath does not exist: $resolvedConfigPath"
}

$resolvedOutputRoot = if ([System.IO.Path]::IsPathRooted($OutputRoot)) {
    $OutputRoot
} else {
    Join-Path $projectRoot $OutputRoot
}
$exportDir = Join-Path $resolvedOutputRoot $RunSlug
New-Item -ItemType Directory -Force -Path $exportDir | Out-Null

$picklePath = Join-Path $exportDir "dfl_market_coupling_v2_plus_ablation_frame.pkl"
$receiptPath = Join-Path $exportDir "entsoe-poland-governance-run-receipt.json"
$materializationLogPath = Join-Path $exportDir "entsoe-poland-governance-materialization.log"

$entsoeTokenAvailable = -not [string]::IsNullOrWhiteSpace($env:ENTSOE_TOKEN)
if (-not $entsoeTokenAvailable) {
    $entsoeTokenAvailable = -not [string]::IsNullOrWhiteSpace($env:ENTSOE_SECURITY_TOKEN)
}
if (-not $entsoeTokenAvailable) {
    $entsoeTokenAvailable = -not [string]::IsNullOrWhiteSpace($env:ENTSO_E_SECURITY_TOKEN)
}

$assetSelection = "forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame,entsoe_neighbor_market_feature_candidate_frame,entsoe_poland_feature_governance_frame,entsoe_neighbor_market_aligned_feature_panel_frame,official_forecast_exogenous_governance_frame,official_forecast_exogenous_feature_route_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame,dfl_market_coupling_v2_plus_ablation_frame"

$materializationCommand = @(
    "docker", "compose", "exec", "-T", "dagster-webserver",
    "uv", "run", "dagster", "asset", "materialize",
    "-m", "smart_arbitrage.defs",
    "--select", $assetSelection,
    "-c", $ConfigPath
)

$receipt = @{
    schema_version = 1
    run_slug = $RunSlug
    config_path = $resolvedConfigPath
    output_dir = $exportDir
    entsoe_token_available = [bool]$entsoeTokenAvailable
    asset_selection = $assetSelection
    materialization_command = $materializationCommand -join " "
    claim_boundary = "Offline Strategy Promotion evidence only; no European rows in Ukrainian training; not market execution"
    market_execution_enabled = $false
    dry_run = [bool]$DryRun
    skip_materialization = [bool]$SkipMaterialization
}

$receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding UTF8
Write-Output "Receipt: $receiptPath"

if ($DryRun) {
    Write-Output "[DRY-RUN] $($materializationCommand -join ' ')"
    Write-Output "[DRY-RUN] docker cp <dagster-webserver>:/opt/dagster/dagster_home/storage/dfl_market_coupling_v2_plus_ablation_frame $picklePath"
    Write-Output "[DRY-RUN] $pythonPath scripts\materialize_market_coupling_ablation_packet.py --ablation-frame-pickle $picklePath --run-slug $RunSlug"
    return
}

Push-Location $projectRoot
try {
    if (-not $SkipMaterialization) {
        & $materializationCommand[0] $materializationCommand[1..($materializationCommand.Length - 1)] 2>&1 |
            Tee-Object -FilePath $materializationLogPath
        if ($LASTEXITCODE -ne 0) {
            throw "ENTSO-E Poland governance materialization failed with exit code $LASTEXITCODE."
        }
    }

    $dagsterRunId = ""
    if (Test-Path -LiteralPath $materializationLogPath) {
        $logText = Get-Content -LiteralPath $materializationLogPath -Raw
        $match = [regex]::Match(
            $logText,
            "\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"
        )
        if ($match.Success) {
            $dagsterRunId = $match.Value
        }
    }

    $containerId = (docker compose ps -q dagster-webserver).Trim()
    if ([string]::IsNullOrWhiteSpace($containerId)) {
        throw "Could not find dagster-webserver container for evidence export."
    }
    docker cp "${containerId}:/opt/dagster/dagster_home/storage/dfl_market_coupling_v2_plus_ablation_frame" $picklePath
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to copy dfl_market_coupling_v2_plus_ablation_frame from Dagster storage."
    }

    $exportArguments = @(
        "scripts\materialize_market_coupling_ablation_packet.py",
        "--ablation-frame-pickle", $picklePath,
        "--run-slug", $RunSlug,
        "--materialization-command", ($materializationCommand -join " ")
    )
    if (-not [string]::IsNullOrWhiteSpace($dagsterRunId)) {
        $exportArguments += @("--dagster-run-id", $dagsterRunId)
    }
    & $pythonPath @exportArguments
    if ($LASTEXITCODE -ne 0) {
        throw "ENTSO-E Poland governance packet export failed with exit code $LASTEXITCODE."
    }
} finally {
    Pop-Location
}
