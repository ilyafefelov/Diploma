param(
  [switch]$SkipDashboardTests,
  [switch]$SkipFullVerify,
  [switch]$SkipSmoke,
  [switch]$SkipLinkCheck
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

function Invoke-AuditStep {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][scriptblock]$Command
  )

  Write-Host ""
  Write-Host "==> $Name"
  $global:LASTEXITCODE = 0
  & $Command
  if ($LASTEXITCODE -ne 0) {
    throw "Step failed: $Name"
  }
}

function Test-GeneratedArtifactsNotTracked {
  $tracked = @(
    git ls-files `
      outputs `
      output `
      analysis_outputs `
      reports `
      .codex-remote-attachments `
      .env `
      .obsidian `
      .tmp_dagster_home* `
      .tmp_runtime `
      .tmp_uv_cache* `
      tmp_uv_cache* `
      .uv-cache `
      AGENTS.md `
      master_execution_prompt.md `
      _legacy_smart-energy-ai `
      node_modules `
      dashboard/node_modules `
      dashboard/.nuxt `
      dashboard/.output `
      dashboard/.vercel | Where-Object { $_ }
  )

  if ($tracked.Count -gt 0) {
    $preview = $tracked | Select-Object -First 25
    throw "Generated/runtime artifacts are still tracked:`n$($preview -join "`n")"
  }

  Write-Host "Generated/runtime artifact check passed."
}

function Test-SourcePdfsNotTracked {
  $tracked = @(
    git ls-files `
      "docs/thesis/sources/*.pdf" `
      "docs/technical/papers/*.pdf" | Where-Object { $_ }
  )

  if ($tracked.Count -gt 0) {
    throw "Third-party source PDFs are still tracked:`n$($tracked -join "`n")"
  }

  Write-Host "Third-party source PDF check passed."
}

function Test-CuratedMarkdownLinks {
  $files = @(
    "README.md",
    "dashboard/README.md",
    "docs/technical/FINAL_DEFENSE_RUNBOOK.md",
    "docs/technical/FINAL_EVIDENCE_INDEX.md",
    "docs/technical/FINAL_METRICS_ATLAS.md",
    "docs/technical/FINAL_REVIEW_CHECKLIST.md",
    "docs/technical/BUSINESS_VALUE_NOTE.md",
    "docs/technical/final-demo-assets/README.md",
    "docs/thesis/sources/README.md",
    "docs/sources/market-coupling-ablation-v1-source-capture-2026-05-16.md"
  )

  $missing = New-Object System.Collections.Generic.List[string]

  foreach ($file in $files) {
    if (-not (Test-Path -LiteralPath $file)) {
      $missing.Add("$file -> file missing")
      continue
    }

    $body = Get-Content -LiteralPath $file -Raw
    $matches = [regex]::Matches($body, "\[[^\]]+\]\((?<href>[^)]+)\)")
    $baseDir = Split-Path -Parent $file
    if ([string]::IsNullOrWhiteSpace($baseDir)) {
      $baseDir = "."
    }

    foreach ($match in $matches) {
      $href = $match.Groups["href"].Value.Trim()
      if ($href.StartsWith("http://") -or
          $href.StartsWith("https://") -or
          $href.StartsWith("mailto:") -or
          $href.StartsWith("#")) {
        continue
      }

      $href = $href.Trim("<", ">")
      $href = ($href -split "#")[0]
      if ([string]::IsNullOrWhiteSpace($href)) {
        continue
      }

      $candidate = Join-Path $baseDir $href
      if (-not (Test-Path -LiteralPath $candidate)) {
        $missing.Add("$file -> $href")
      }
    }
  }

  if ($missing.Count -gt 0) {
    throw "Missing curated markdown links:`n$($missing -join "`n")"
  }

  Write-Host "Curated markdown link check passed."
}

Invoke-AuditStep "Whitespace diff check" {
  git diff --check
}

Invoke-AuditStep "Generated/runtime artifacts are not tracked" {
  Test-GeneratedArtifactsNotTracked
}

Invoke-AuditStep "Third-party source PDFs are not tracked" {
  Test-SourcePdfsNotTracked
}

if (-not $SkipLinkCheck) {
  Invoke-AuditStep "Curated markdown links" {
    Test-CuratedMarkdownLinks
  }
}

if (-not $SkipDashboardTests) {
  Invoke-AuditStep "Dashboard typecheck" {
    npm -C dashboard run typecheck
  }

  Invoke-AuditStep "Dashboard unit tests" {
    npm -C dashboard run test:unit
  }
}

if (-not $SkipSmoke) {
  Invoke-AuditStep "HF value-aligned browser smoke" {
    npm -C dashboard run smoke:hf-value-aligned
  }
}

if (-not $SkipFullVerify) {
  Invoke-AuditStep "Full repository verify wrapper" {
    .\.venv\Scripts\Activate.ps1
    .\scripts\verify.ps1
  }
}

Write-Host ""
Write-Host "Final repository audit passed."
