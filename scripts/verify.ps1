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
