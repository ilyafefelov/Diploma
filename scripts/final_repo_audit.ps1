param(
  [switch]$SkipDashboardTests,
  [switch]$SkipFullVerify,
  [switch]$SkipSmoke,
  [switch]$SkipLinkCheck,
  [switch]$RequireCleanWorkingTree
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
      .agents `
      .codex `
      .code `
      .github `
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

function Test-CleanWorkingTree {
  $status = @(git status --short)
  if ($LASTEXITCODE -ne 0) {
    throw "Could not read git status."
  }

  if ($status.Count -gt 0) {
    $preview = $status | Select-Object -First 40
    throw "Working tree is not clean. Commit, stage for review, or intentionally remove these changes before final submission:`n$($preview -join "`n")"
  }

  Write-Host "Working tree is clean."
}

function Test-FrontFacingLaneBoundary {
  $requiredSnippets = @{
    "README.md" = @(
      "The primary defense path is the operator-preview product surface",
      "not the legacy workspace or unfinished research lanes",
      "no V13 training claim"
    )
    "dashboard/README.md" = @(
      "Historical sources may remain available for diagnostics",
      "used as the main defense path"
    )
    "docs/README.md" = @(
      "Final GitHub Review Entry Points",
      "Long research histories and legacy extraction notes are supporting context"
    )
    "docs/technical/FINAL_DEFENSE_RUNBOOK.md" = @(
      "Primary path only",
      "Do not open legacy or unfinished research lanes as the main demo"
    )
    "docs/technical/FINAL_REVIEW_CHECKLIST.md" = @(
      "Primary-vs-supporting lane boundary",
      "Strict clean-tree submission gate"
    )
  }

  $missing = New-Object System.Collections.Generic.List[string]

  foreach ($file in $requiredSnippets.Keys) {
    if (-not (Test-Path -LiteralPath $file)) {
      $missing.Add("$file -> file missing")
      continue
    }

    $body = Get-Content -LiteralPath $file -Raw
    foreach ($snippet in $requiredSnippets[$file]) {
      if (-not $body.Contains($snippet)) {
        $missing.Add("$file -> missing boundary snippet: $snippet")
      }
    }
  }

  if ($missing.Count -gt 0) {
    throw "Front-facing lane boundary check failed:`n$($missing -join "`n")"
  }

  Write-Host "Front-facing lane boundary check passed."
}

function Test-SourcePdfsNotTracked {
  $tracked = @(
    git ls-files `
      "docs/thesis/sources" `
      "docs/technical/papers" | Where-Object { $_ -match '\.pdf$' }
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
    "docs/technical/FINAL_UNIVERSITY_RUBRIC_MATRIX.md",
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

Invoke-AuditStep "Front-facing lane boundaries are explicit" {
  Test-FrontFacingLaneBoundary
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

if ($RequireCleanWorkingTree) {
  Invoke-AuditStep "Strict clean-tree submission gate" {
    Test-CleanWorkingTree
  }
}

Write-Host ""
Write-Host "Final repository audit passed."
