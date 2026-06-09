param(
    [ValidateSet("local", "hf")]
    [string]$Backend = "local",
    [ValidateSet("compose", "host")]
    [string]$LocalMode = "compose",
    [int]$TotalAnchorsPerTenant = 18,
    [int]$BatchSize = 4,
    [int]$StartAnchorIndex = 0,
    [ValidateSet("chronological", "latest_first")]
    [string]$AnchorBatchOrder = "latest_first",
    [string]$EnabledOfficialModelsCsv = "tft_official_v0",
    [int]$NbeatsxMaxSteps = 25,
    [int]$TftMaxEpochs = 5,
    [string]$GeneratedAtIso = "",
    [int]$BatchTimeoutSeconds = 7200,
    [string]$GitRef = "main",
    [string]$RepoUrl = "https://github.com/ilyafefelov/Diploma.git",
    [string]$ArtifactRepoId = "",
    [string]$Flavor = "t4-small",
    [string]$Timeout = "4h",
    [string]$RunSlug = "",
    [string]$OutputRoot = ".tmp_runtime\official_evidence",
    [string]$HostPostgresDsn = "",
    [string]$HostMlflowTrackingUri = "http://localhost:5000",
    [switch]$SkipDownstreamGate,
    [switch]$Submit,
    [switch]$DryRun
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
if ([string]::IsNullOrWhiteSpace($RunSlug)) {
    $safeTimestamp = $GeneratedAtIso -replace "[:]", "" -replace "[^0-9A-Za-z_-]", "-"
    $RunSlug = "official-evidence-$Backend-$safeTimestamp"
}
if ([string]::IsNullOrWhiteSpace($HostPostgresDsn)) {
    $hostPostgresPort = $env:SMART_ARBITRAGE_POSTGRES_PORT
    if ([string]::IsNullOrWhiteSpace($hostPostgresPort)) {
        $hostPostgresPort = "5432"
    }
    $HostPostgresDsn = "postgresql://smart:arbitrage@localhost:$hostPostgresPort/smart_arbitrage"
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$resolvedOutputRoot = Join-Path $root $OutputRoot
$runDir = Join-Path $resolvedOutputRoot $RunSlug
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$receiptPath = Join-Path $runDir "official-evidence-runner-receipt.json"
$runtimePreflightPath = Join-Path $runDir "training-runtime-preflight.json"

function ConvertTo-ReceiptJson {
    param([hashtable]$Receipt)
    $Receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding UTF8
    Write-Output "Receipt: $receiptPath"
}

function Invoke-CommandOrDryRun {
    param(
        [string[]]$Command,
        [string]$WorkingDirectory
    )
    if ($DryRun) {
        Write-Output "[DRY-RUN] $($Command -join ' ')"
        return
    }
    $executable = $Command[0]
    $arguments = @()
    if ($Command.Length -gt 1) {
        $arguments = $Command[1..($Command.Length - 1)]
    }
    Push-Location $WorkingDirectory
    try {
        & $executable @arguments
        if ($LASTEXITCODE -ne 0) {
            throw "Command failed with exit code $LASTEXITCODE`: $($Command -join ' ')"
        }
    }
    finally {
        Pop-Location
    }
}

function New-OfficialBatchConfig {
    param(
        [string]$Path,
        [int]$AnchorIndex
    )
    @"
ops:
  official_forecast_rolling_origin_benchmark_frame:
    config:
      tenant_ids_csv: "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,client_004_kharkiv_hospital,client_005_odesa_hotel"
      max_eval_anchors_per_tenant: $TotalAnchorsPerTenant
      anchor_batch_start_index: $AnchorIndex
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
"@ | Set-Content -LiteralPath $Path -Encoding UTF8
}

function Set-HostEvidenceEnvironment {
    $env:PYTHONPATH = "$root;$root\src"
    $env:SMART_ARBITRAGE_MARKET_DATA_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_FORECAST_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_DFL_TRAINING_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_BATTERY_TELEMETRY_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_SIMULATED_TRADE_DSN = $HostPostgresDsn
    $env:SMART_ARBITRAGE_OPERATOR_STATUS_DSN = $HostPostgresDsn
    $env:MLFLOW_TRACKING_URI = $HostMlflowTrackingUri
}

$commonReceipt = @{
    schema_version = 1
    backend = $Backend
    local_mode = $LocalMode
    run_slug = $RunSlug
    generated_at_iso = $GeneratedAtIso
    total_anchors_per_tenant = $TotalAnchorsPerTenant
    batch_size = $BatchSize
    anchor_batch_order = $AnchorBatchOrder
    enabled_official_models_csv = $EnabledOfficialModelsCsv
    nbeatsx_max_steps = $NbeatsxMaxSteps
    tft_max_epochs = $TftMaxEpochs
    claim_boundary = "Offline Strategy Promotion evidence only; not market execution"
    market_execution_enabled = $false
    dry_run = [bool]$DryRun
}

if ($Backend -eq "local") {
    if ($Submit) {
        throw "Submit is only valid with Backend=hf."
    }
    $hostPythonPath = ".\.venv\Scripts\python.exe"
    $hostDagsterPath = ".\.venv\Scripts\dagster.exe"
    $preflightCommand = @(
        $hostPythonPath,
        "scripts\check_training_runtime.py",
        "--output", $runtimePreflightPath,
        "--include-docker"
    )
    $commonReceipt["runtime_preflight_path"] = $runtimePreflightPath
    $commonReceipt["runtime_preflight_command"] = $preflightCommand -join " "

    if ($LocalMode -eq "host") {
        Set-HostEvidenceEnvironment
        $officialSelection = "observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_rolling_origin_benchmark_frame"
        $downstreamSelection = "dfl_official_schedule_candidate_library_frame,dfl_official_schedule_candidate_library_v2_frame,dfl_official_schedule_value_learner_v2_frame,dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame,dfl_official_schedule_value_learner_v2_robustness_frame,dfl_official_schedule_value_production_gate_frame"
        $manifestPath = Join-Path $runDir "attempt_manifest.json"
        $manifestCommand = @(
            $hostPythonPath,
            "scripts\build_official_evidence_attempt_manifest.py",
            "--attempt-kind", "official_schedule_value",
            "--generated-at-iso", $GeneratedAtIso,
            "--total-anchors", "$TotalAnchorsPerTenant",
            "--batch-size", "$BatchSize",
            "--start-anchor-index", "$StartAnchorIndex",
            "--anchor-batch-order", $AnchorBatchOrder,
            "--enabled-official-models-csv", $EnabledOfficialModelsCsv,
            "--nbeatsx-max-steps", "$NbeatsxMaxSteps",
            "--tft-max-epochs", "$TftMaxEpochs",
            "--asset-selection", $officialSelection,
            "--downstream-selection", $downstreamSelection,
            "--run-root", ".tmp_runtime/official_evidence",
            "--output", $manifestPath
        )
        if ($SkipDownstreamGate) {
            $manifestCommand += "--skip-downstream-gate"
        }
        $commonReceipt["host_postgres_dsn"] = ($HostPostgresDsn -replace "://([^:/@]+):([^@]+)@", '://$1:***@')
        $commonReceipt["host_mlflow_tracking_uri"] = $HostMlflowTrackingUri
        $hostCommands = @(($preflightCommand -join " "), ($manifestCommand -join " "))
        for ($anchorIndex = $StartAnchorIndex; $anchorIndex -lt $TotalAnchorsPerTenant; $anchorIndex += $BatchSize) {
            $batchConfigPath = Join-Path $runDir ("official-host-batch-{0}.yaml" -f $anchorIndex)
            New-OfficialBatchConfig -Path $batchConfigPath -AnchorIndex $anchorIndex
            $hostCommands += (@(
                $hostDagsterPath,
                "asset", "materialize",
                "-m", "smart_arbitrage.defs",
                "--select", $officialSelection,
                "-c", $batchConfigPath
            ) -join " ")
        }
        if (-not $SkipDownstreamGate) {
            $hostCommands += (@(
                $hostDagsterPath,
                "asset", "materialize",
                "-m", "smart_arbitrage.defs",
                "--select", $downstreamSelection,
                "-c", "configs/real_data_official_schedule_value_promotion_week3.yaml"
            ) -join " ")
        }
        $commonReceipt["local_command"] = $hostCommands
        ConvertTo-ReceiptJson -Receipt $commonReceipt
        Invoke-CommandOrDryRun -Command $preflightCommand -WorkingDirectory $root
        Invoke-CommandOrDryRun -Command $manifestCommand -WorkingDirectory $root
        for ($anchorIndex = $StartAnchorIndex; $anchorIndex -lt $TotalAnchorsPerTenant; $anchorIndex += $BatchSize) {
            $batchConfigPath = Join-Path $runDir ("official-host-batch-{0}.yaml" -f $anchorIndex)
            Invoke-CommandOrDryRun -Command @(
                $hostDagsterPath,
                "asset", "materialize",
                "-m", "smart_arbitrage.defs",
                "--select", $officialSelection,
                "-c", $batchConfigPath
            ) -WorkingDirectory $root
        }
        if (-not $SkipDownstreamGate) {
            Invoke-CommandOrDryRun -Command @(
                $hostDagsterPath,
                "asset", "materialize",
                "-m", "smart_arbitrage.defs",
                "--select", $downstreamSelection,
                "-c", "configs/real_data_official_schedule_value_promotion_week3.yaml"
            ) -WorkingDirectory $root
        }
        exit 0
    }

    $localRunnerPath = Join-Path $PSScriptRoot "run-official-schedule-value-batches.ps1"
    $localArgs = @(
        $localRunnerPath,
        "-TotalAnchorsPerTenant", "$TotalAnchorsPerTenant",
        "-BatchSize", "$BatchSize",
        "-StartAnchorIndex", "$StartAnchorIndex",
        "-AnchorBatchOrder", $AnchorBatchOrder,
        "-EnabledOfficialModelsCsv", $EnabledOfficialModelsCsv,
        "-NbeatsxMaxSteps", "$NbeatsxMaxSteps",
        "-TftMaxEpochs", "$TftMaxEpochs",
        "-GeneratedAtIso", $GeneratedAtIso,
        "-BatchTimeoutSeconds", "$BatchTimeoutSeconds"
    )
    if ($SkipDownstreamGate) {
        $localArgs += "-SkipDownstreamGate"
    }
    $commonReceipt["local_command"] = "powershell -NoProfile -ExecutionPolicy Bypass -File $($localArgs -join ' ')"
    ConvertTo-ReceiptJson -Receipt $commonReceipt
    Invoke-CommandOrDryRun -Command $preflightCommand -WorkingDirectory $root
    Invoke-CommandOrDryRun -Command (@("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File") + $localArgs) -WorkingDirectory $root
    exit 0
}

$payloadPath = Join-Path $runDir "hf-official-evidence-payload.json"
$hfReceiptPath = Join-Path $runDir "hf-official-evidence-submit-receipt.json"
$buildArgs = @(
    ".\.venv\Scripts\python.exe",
    "scripts\build_hf_official_schedule_value_job.py",
    "--repo-url", $RepoUrl,
    "--git-ref", $GitRef,
    "--total-anchors-per-tenant", "$TotalAnchorsPerTenant",
    "--batch-size", "$BatchSize",
    "--anchor-batch-order", $AnchorBatchOrder,
    "--enabled-official-models-csv", $EnabledOfficialModelsCsv,
    "--nbeatsx-max-steps", "$NbeatsxMaxSteps",
    "--tft-max-epochs", "$TftMaxEpochs",
    "--flavor", $Flavor,
    "--timeout", $Timeout,
    "--run-slug", $RunSlug,
    "--artifact-repo-id", $ArtifactRepoId,
    "--output", $payloadPath
)
$submitArgs = @(
    ".\.venv\Scripts\python.exe",
    "scripts\submit_hf_official_schedule_value_job.py",
    "--payload", $payloadPath,
    "--output", $hfReceiptPath
)
if ($Submit) {
    $submitArgs += "--submit"
}
$commonReceipt["hf_payload_path"] = $payloadPath
$commonReceipt["hf_receipt_path"] = $hfReceiptPath
$commonReceipt["hf_submit_requested"] = [bool]$Submit
$commonReceipt["hf_flavor"] = $Flavor
$commonReceipt["hf_timeout"] = $Timeout
$commonReceipt["artifact_repo_id"] = $ArtifactRepoId
$commonReceipt["build_command"] = $buildArgs -join " "
$commonReceipt["submit_command"] = $submitArgs -join " "
ConvertTo-ReceiptJson -Receipt $commonReceipt

Invoke-CommandOrDryRun -Command $buildArgs -WorkingDirectory $root
Invoke-CommandOrDryRun -Command $submitArgs -WorkingDirectory $root
