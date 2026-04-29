param(
	[int]$Port = 8000
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$pythonPath = Join-Path $repoRoot ".venv\Scripts\python.exe"
$srcPath = Join-Path $repoRoot "src"

if (-not (Test-Path $pythonPath)) {
	Write-Host "Project virtual environment not found at $pythonPath" -ForegroundColor Red
	exit 1
}

Set-Location $repoRoot

$pythonPathEntries = @($repoRoot, $srcPath)
if ($env:PYTHONPATH) {
	$pythonPathEntries += $env:PYTHONPATH
}
$env:PYTHONPATH = ($pythonPathEntries -join [System.IO.Path]::PathSeparator)

Write-Host "Starting Smart Energy Arbitrage API" -ForegroundColor Green
Write-Host "  URL: http://127.0.0.1:$Port" -ForegroundColor Cyan
Write-Host "  Docs: http://127.0.0.1:$Port/docs" -ForegroundColor Cyan
Write-Host "  OpenAPI: http://127.0.0.1:$Port/openapi.json" -ForegroundColor Cyan
Write-Host "  PYTHONPATH: $env:PYTHONPATH" -ForegroundColor DarkGray

& $pythonPath -m uvicorn api.main:app --host 127.0.0.1 --port $Port --reload