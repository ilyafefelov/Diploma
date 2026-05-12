param(
    [string]$ManifestPath = "",
    [string]$StrategyKind = "",
    [string]$GeneratedAtIso = "",
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    throw "ManifestPath is required."
}

if ([string]::IsNullOrWhiteSpace($StrategyKind)) {
    throw "StrategyKind is required."
}

$projectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$summaryScript = Join-Path $projectRoot "scripts\summarize_official_evidence_attempt_resume.py"
$pythonPath = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $pythonPath)) {
    $pythonPath = "python"
}

$resolvedManifestPath = (Resolve-Path -LiteralPath $ManifestPath).Path
$arguments = @(
    $summaryScript,
    "--manifest",
    $resolvedManifestPath,
    "--strategy-kind",
    $StrategyKind
)

if (-not [string]::IsNullOrWhiteSpace($GeneratedAtIso)) {
    $arguments += @("--generated-at-iso", $GeneratedAtIso)
}

if (-not [string]::IsNullOrWhiteSpace($OutputPath)) {
    if ([System.IO.Path]::IsPathRooted($OutputPath)) {
        $resolvedOutputPath = $OutputPath
    } else {
        $resolvedOutputPath = Join-Path (Get-Location).Path $OutputPath
    }
    $arguments += @("--output", $resolvedOutputPath)
}

Push-Location $projectRoot
try {
    & $pythonPath @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Official evidence monitor failed with exit code $LASTEXITCODE."
    }
} finally {
    Pop-Location
}

if (-not [string]::IsNullOrWhiteSpace($OutputPath)) {
    Get-Content -LiteralPath $resolvedOutputPath -Raw
}
