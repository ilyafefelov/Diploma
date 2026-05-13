param(
    [ValidateSet("local", "hf")]
    [string]$Backend = "local",
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

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$resolvedOutputRoot = Join-Path $root $OutputRoot
$runDir = Join-Path $resolvedOutputRoot $RunSlug
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$receiptPath = Join-Path $runDir "official-evidence-runner-receipt.json"

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

$commonReceipt = @{
    schema_version = 1
    backend = $Backend
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
