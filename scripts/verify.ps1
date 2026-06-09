$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
$ExpectedVenv = Join-Path $RepoRoot ".venv"
$ExpectedPython = Join-Path $ExpectedVenv "Scripts\python.exe"
$Failed = $false

function Write-Status {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Status,
        [Parameter(Mandatory = $true)]
        [string] $Message
    )

    Write-Host "[$Status] $Message"
}

function Normalize-PathString {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    return [System.IO.Path]::GetFullPath($Path).TrimEnd("\")
}

function Test-PythonModule {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ModuleName
    )

    & python -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('$ModuleName') else 1)" | Out-Null
    return $LASTEXITCODE -eq 0
}

function Resolve-LocalTool {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ToolName
    )

    $localCommand = Join-Path $ExpectedVenv "Scripts\$ToolName.exe"
    if (Test-Path -LiteralPath $localCommand) {
        return $localCommand
    }

    $command = Get-Command $ToolName -ErrorAction SilentlyContinue
    if ($null -ne $command) {
        return $command.Source
    }

    return $null
}

function Invoke-OptionalPythonTool {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DisplayName,
        [Parameter(Mandatory = $true)]
        [string] $ModuleName,
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments
    )

    if (-not (Test-PythonModule -ModuleName $ModuleName)) {
        Write-Status "SKIP" "$DisplayName is not installed in the active environment."
        return
    }

    Write-Status "RUN" "$DisplayName"
    & python -m $ModuleName @Arguments
    if ($LASTEXITCODE -ne 0) {
        Write-Status "FAIL" "$DisplayName failed with exit code $LASTEXITCODE."
        $script:Failed = $true
    }
}

function Initialize-VerificationDagsterHome {
    if ($env:DAGSTER_HOME) {
        Write-Status "OK" "Using existing DAGSTER_HOME: $env:DAGSTER_HOME"
        return
    }

    $dagsterHome = Join-Path $RepoRoot ".tmp_dagster_home_verify"
    New-Item -ItemType Directory -Path $dagsterHome -Force | Out-Null

    $dagsterYaml = Join-Path $dagsterHome "dagster.yaml"
    if (-not (Test-Path -LiteralPath $dagsterYaml)) {
        "telemetry:`n  enabled: false`n" | Set-Content -Path $dagsterYaml -Encoding utf8
    }

    $env:DAGSTER_HOME = $dagsterHome
    Write-Status "OK" "Using temporary DAGSTER_HOME: $env:DAGSTER_HOME"
}

function Initialize-VerificationTemp {
    $tempPath = Join-Path $RepoRoot ".tmp_dagster_home_verify\tmp"
    New-Item -ItemType Directory -Path $tempPath -Force | Out-Null
    $env:TEMP = $tempPath
    $env:TMP = $tempPath
    $env:TMPDIR = $tempPath
    Write-Status "OK" "Using temporary directory: $tempPath"
}

function Invoke-OptionalLavaNpzMarginSmoke {
    $candidateFramePathValue = $env:SMART_ARBITRAGE_VERIFY_LAVA_NPZ_CANDIDATE_FRAME_PICKLE
    if ([string]::IsNullOrWhiteSpace($candidateFramePathValue)) {
        Write-Status "SKIP" "LAVA NPZ margin-smoke candidate frame is not configured. Set SMART_ARBITRAGE_VERIFY_LAVA_NPZ_CANDIDATE_FRAME_PICKLE to run this non-promotion check."
        return
    }

    $candidateFramePath = [System.IO.Path]::GetFullPath($candidateFramePathValue)
    if (-not (Test-Path -LiteralPath $candidateFramePath)) {
        Write-Status "FAIL" "Configured LAVA NPZ margin-smoke candidate frame does not exist: $candidateFramePath"
        $script:Failed = $true
        return
    }

    $outputDirValue = $env:SMART_ARBITRAGE_VERIFY_LAVA_NPZ_OUTPUT_DIR
    if ([string]::IsNullOrWhiteSpace($outputDirValue)) {
        $outputDirValue = Join-Path $RepoRoot ".tmp_runtime\verify_lava_npz_margin_smoke"
    }
    $outputDir = [System.IO.Path]::GetFullPath($outputDirValue)
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

    $v13SummaryPathValue = $env:SMART_ARBITRAGE_VERIFY_LAVA_NPZ_V13_SUMMARY_JSON
    if ([string]::IsNullOrWhiteSpace($v13SummaryPathValue)) {
        $v13SummaryPathValue = Join-Path $RepoRoot "data\research_runs\week3_dfl_ua_context_acquisition_v13_safe_switch_only\dfl_ua_context_v13_acquisition_summary.json"
    }
    $v13SummaryPath = [System.IO.Path]::GetFullPath($v13SummaryPathValue)

    $packetScript = Join-Path $RepoRoot "scripts\materialize_lava_npz_margin_smoke_packet.py"
    $validatorScript = Join-Path $RepoRoot "scripts\validate_lava_npz_margin_smoke_packet.py"
    $manifestPath = Join-Path $outputDir "lava_npz_margin_smoke_manifest.json"
    $validationPath = Join-Path $outputDir "lava_npz_margin_smoke_packet_validation.json"

    $packetArguments = @(
        $packetScript,
        "--candidate-frame-pickle", $candidateFramePath,
        "--output-dir", $outputDir,
        "--seed", "0",
        "--window-id", "verify_lava_npz_margin_smoke",
        "--max-instances", "8",
        "--max-neighbors", "4"
    )
    if (Test-Path -LiteralPath $v13SummaryPath) {
        $packetArguments += @("--v13-acquisition-summary-json", $v13SummaryPath)
    } else {
        Write-Status "SKIP" "V13 acquisition summary attachment is unavailable for LAVA NPZ margin smoke: $v13SummaryPath"
    }

    Write-Status "RUN" "Optional LAVA NPZ margin-smoke packet; market_execution_enabled=false, promotion_gate=false, permits_model_training=false"
    & python @packetArguments
    if ($LASTEXITCODE -ne 0) {
        Write-Status "FAIL" "LAVA NPZ margin-smoke packet failed with exit code $LASTEXITCODE."
        $script:Failed = $true
        return
    }

    Write-Status "RUN" "Revalidating optional LAVA NPZ margin-smoke packet"
    & python $validatorScript --manifest $manifestPath --output $validationPath
    if ($LASTEXITCODE -ne 0) {
        Write-Status "FAIL" "LAVA NPZ margin-smoke packet validation failed with exit code $LASTEXITCODE."
        $script:Failed = $true
    }
}

function Invoke-DtLavaPrototypeReadinessPacket {
    $outputDir = Join-Path $RepoRoot ".tmp_runtime\dt_lava_prototype_readiness"
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

    $v13SummaryPathValue = $env:SMART_ARBITRAGE_VERIFY_LAVA_NPZ_V13_SUMMARY_JSON
    if ([string]::IsNullOrWhiteSpace($v13SummaryPathValue)) {
        $v13SummaryPathValue = Join-Path $RepoRoot "data\research_runs\week3_dfl_ua_context_acquisition_v13_safe_switch_only\dfl_ua_context_v13_acquisition_summary.json"
    }
    $v13SummaryPath = [System.IO.Path]::GetFullPath($v13SummaryPathValue)
    if (-not (Test-Path -LiteralPath $v13SummaryPath)) {
        Write-Status "SKIP" "DT/LAVA prototype readiness packet needs a V13 acquisition summary: $v13SummaryPath"
        return
    }

    $readinessScript = Join-Path $RepoRoot "scripts\materialize_dt_lava_prototype_readiness_packet.py"
    $readinessArguments = @(
        $readinessScript,
        "--v13-acquisition-summary-json", $v13SummaryPath,
        "--output-dir", $outputDir
    )

    $offlineStrategyRegistryValue = $env:SMART_ARBITRAGE_VERIFY_OFFLINE_STRATEGY_PROMOTION_REGISTRY_JSON
    if ([string]::IsNullOrWhiteSpace($offlineStrategyRegistryValue)) {
        $offlineStrategyRegistryValue = Join-Path $RepoRoot "data\research_runs\week3_official_global_panel_365_strategy_promotion\dfl_schedule_value_production_gate_registry.json"
    }
    $offlineStrategyRegistryPath = [System.IO.Path]::GetFullPath($offlineStrategyRegistryValue)
    if (Test-Path -LiteralPath $offlineStrategyRegistryPath) {
        $readinessArguments += @("--offline-strategy-promotion-registry-json", $offlineStrategyRegistryPath)
    } else {
        Write-Status "SKIP" "Offline strategy promotion registry attachment is unavailable for DT/LAVA readiness: $offlineStrategyRegistryPath"
    }

    $candidateFramePathValue = $env:SMART_ARBITRAGE_VERIFY_LAVA_NPZ_CANDIDATE_FRAME_PICKLE
    if (-not [string]::IsNullOrWhiteSpace($candidateFramePathValue)) {
        $readinessArguments += @("--candidate-frame-pickle", [System.IO.Path]::GetFullPath($candidateFramePathValue))
    }

    $lavaValidationPath = Join-Path $RepoRoot ".tmp_runtime\verify_lava_npz_margin_smoke\lava_npz_margin_smoke_packet_validation.json"
    if (Test-Path -LiteralPath $lavaValidationPath) {
        $readinessArguments += @("--lava-npz-smoke-validation-json", $lavaValidationPath)
    } else {
        Write-Status "SKIP" "LAVA NPZ smoke packet validation attachment is unavailable for DT/LAVA readiness: $lavaValidationPath"
    }

    $materializationBlockersValue = $env:SMART_ARBITRAGE_VERIFY_DT_LAVA_MATERIALIZATION_BLOCKERS_CSV
    if (-not [string]::IsNullOrWhiteSpace($materializationBlockersValue)) {
        foreach ($blocker in $materializationBlockersValue.Split(",")) {
            if (-not [string]::IsNullOrWhiteSpace($blocker)) {
                $readinessArguments += @("--materialization-blocker", $blocker.Trim())
            }
        }
    }

    Write-Status "RUN" "DT/LAVA prototype readiness packet; market_execution_enabled=false"
    & python @readinessArguments
    if ($LASTEXITCODE -ne 0) {
        Write-Status "FAIL" "DT/LAVA prototype readiness packet failed with exit code $LASTEXITCODE."
        $script:Failed = $true
    }
}

Push-Location $RepoRoot
try {
    Write-Status "RUN" "Checking active Python path"

    if (-not (Test-Path -LiteralPath $ExpectedPython)) {
        Write-Status "FAIL" "Expected root venv Python is missing: $ExpectedPython"
        exit 1
    }

    $activePython = (& python -c "import sys; print(sys.executable)").Trim()
    Write-Status "INFO" "Active Python: $activePython"
    Write-Status "INFO" "Expected Python: $ExpectedPython"

    $normalizedActivePython = Normalize-PathString -Path $activePython
    $normalizedExpectedPython = Normalize-PathString -Path $ExpectedPython
    if ($normalizedActivePython -ne $normalizedExpectedPython) {
        Write-Status "FAIL" "Activate the root venv first: .\.venv\Scripts\Activate.ps1"
        exit 1
    }

    $env:VIRTUAL_ENV = $ExpectedVenv
    $env:PYTHONPATH = @(
        (Join-Path $RepoRoot "src"),
        $RepoRoot,
        $env:PYTHONPATH
    ) -join [System.IO.Path]::PathSeparator
    Write-Status "OK" "Python path matches the project root venv."

    Initialize-VerificationTemp

    Invoke-OptionalPythonTool -DisplayName "Ruff" -ModuleName "ruff" -Arguments @("check", "src", "tests", "api")
    Invoke-OptionalPythonTool -DisplayName "Mypy" -ModuleName "mypy" -Arguments @("--config-file", "pyproject.toml")
    Invoke-OptionalPythonTool -DisplayName "Pytest" -ModuleName "pytest" -Arguments @("-p", "no:cacheprovider", "tests")
    Invoke-DtLavaPrototypeReadinessPacket
    Invoke-OptionalLavaNpzMarginSmoke

    if (-not (Test-PythonModule -ModuleName "dagster")) {
        Write-Status "FAIL" "Dagster is not installed in the active environment."
        exit 1
    }

    Initialize-VerificationDagsterHome

    $dgCommand = Resolve-LocalTool -ToolName "dg"
    $dagsterCommand = Resolve-LocalTool -ToolName "dagster"

    if ($null -ne $dgCommand) {
        Write-Status "RUN" "Validating Dagster definitions with dg check defs"
        & $dgCommand check defs
        if ($LASTEXITCODE -ne 0) {
            Write-Status "FAIL" "dg check defs failed with exit code $LASTEXITCODE."
            $Failed = $true
        }
    } elseif ($null -ne $dagsterCommand) {
        Write-Status "RUN" "Validating Dagster definitions with dagster definitions validate"
        & $dagsterCommand definitions validate
        if ($LASTEXITCODE -ne 0) {
            Write-Status "FAIL" "dagster definitions validate failed with exit code $LASTEXITCODE."
            $Failed = $true
        }
    } else {
        Write-Status "FAIL" "Dagster is installed, but neither dg nor dagster CLI is available."
        $Failed = $true
    }

    if ($Failed) {
        exit 1
    }

    Write-Status "OK" "Verification completed successfully."
} finally {
    Pop-Location
}
