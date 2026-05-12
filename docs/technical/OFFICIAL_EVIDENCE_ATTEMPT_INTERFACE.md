# Official Evidence Attempt Interface

Date: 2026-05-12

This document defines the execution metadata layer for serious official
NBEATSx/TFT evidence runs. It does not add a new model and does not change the
Offline Strategy Promotion claim. Its purpose is operational: every long
official run should now have a small manifest that explains how to resume,
audit, and later offload the run.

## Problem

Official global-panel training and schedule/value scoring are now strong enough
to support the thesis evidence, but they are operationally expensive. A serious
run may take hours, and a timeout must not make completed anchor batches
ambiguous.

Before this slice, the retry contract was spread across:

- PowerShell runner parameters;
- generated per-batch YAML files;
- Dagster run logs;
- Postgres `generated_at` batches;
- Hugging Face Jobs payload generation.

## Interface

The deep module is:

- `smart_arbitrage.forecasting.official_evidence_attempts`

It builds `attempt_manifest.json` for local runs and
`official_evidence_attempt_manifest.json` for Hugging Face Jobs artifacts.

Manifest responsibilities:

| Field | Meaning |
|---|---|
| `attempt_kind` | `official_schedule_value` or `official_global_panel_backfill`. |
| `resume_generated_at_iso` | Fixed identity for persisted strategy rows in one evidence attempt. |
| `batch_plan` | Planned anchor batches with start, size, and exclusive end. |
| `resume_policy` | How to resume: keep the same generated timestamp and restart from the first missing or failed batch start index. |
| `claim_boundary` | Offline Strategy Promotion language, `not_market_execution=true`, `market_execution_enabled=false`. |
| `asset_selection` / `downstream_selection` | The exact asset selections used by the attempt. |

## Local Runner Integration

Both long-running local runners now write the manifest before materializing the
first batch:

- `scripts/run-official-schedule-value-batches.ps1`;
- `scripts/run-official-global-panel-batches.ps1`.

The manifest is written under the existing run directory:

- `.tmp_runtime/official_schedule_value_batches/<run-slug>/attempt_manifest.json`;
- `.tmp_runtime/official_global_panel_batches/<run-slug>/attempt_manifest.json`.

This keeps the current resume behavior but makes it explicit for humans,
automation, and future evidence registries.

## Resume Summary

Monitoring automation can now compute the next resume point from the manifest
instead of parsing free-form `run.log` text. The repo-local wrapper is:

- `scripts/monitor-official-evidence-attempt.ps1`

It requires `-ManifestPath` and `-StrategyKind`, accepts an optional
`-GeneratedAtIso`, and can also write the emitted JSON to `-OutputPath`.
The wrapper delegates to the Python helper below and reads persisted counts from
Postgres through the existing strategy-evaluation DSN environment variables:
`SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN` or
`SMART_ARBITRAGE_MARKET_DATA_DSN`.

Example:

```powershell
.\scripts\monitor-official-evidence-attempt.ps1 `
  -ManifestPath .tmp_runtime\official_global_panel_batches\<run-slug>\attempt_manifest.json `
  -StrategyKind official_global_panel_nbeatsx_rolling_strict_lp_benchmark `
  -OutputPath .tmp_runtime\official_global_panel_batches\<run-slug>\resume-summary.json
```

The lower-level helper remains available for direct/manual count checks:

- `smart_arbitrage.forecasting.official_evidence_attempts.summarize_official_evidence_attempt_resume`
- `scripts/summarize_official_evidence_attempt_resume.py`

Example:

```powershell
.\.venv\Scripts\python.exe scripts\summarize_official_evidence_attempt_resume.py `
  --manifest .tmp_runtime\official_global_panel_batches\<run-slug>\attempt_manifest.json `
  --persisted-anchor-counts-csv "nbeatsx_official_global_panel_v1=365,nbeatsx_official_global_panel_horizon_calibrated_v1=365"
```

When `SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN` or
`SMART_ARBITRAGE_MARKET_DATA_DSN` is configured, the same command can read
Postgres counts directly:

```powershell
.\.venv\Scripts\python.exe scripts\summarize_official_evidence_attempt_resume.py `
  --manifest .tmp_runtime\official_global_panel_batches\<run-slug>\attempt_manifest.json `
  --strategy-kind official_global_panel_nbeatsx_rolling_strict_lp_benchmark
```

The summary reports `status`, `effective_persisted_anchor_count`,
`completed_batch_start_indices`, and `next_anchor_index`. When multiple source
models are passed, the effective count is the minimum source count, so a
partially persisted model cannot be hidden by a complete one. The claim boundary
is copied through unchanged: Offline Strategy Promotion evidence only,
`market_execution_enabled=false`.

For future long official runs, use the PowerShell monitor wrapper first. Manual
`run.log` inspection and ad hoc SQL should be reserved for diagnosis after the
wrapper reports an invalid manifest, missing DSN, or missing persisted rows.

## Evidence Packet Export

The Schedule/Value production-gate registry exporter can now attach the same
attempt manifest and monitor snapshot to the final local evidence folder:

```powershell
.\scripts\monitor-official-evidence-attempt.ps1 `
  -ManifestPath .tmp_runtime\official_global_panel_batches\<run-slug>\attempt_manifest.json `
  -StrategyKind official_global_panel_nbeatsx_rolling_strict_lp_benchmark `
  -OutputPath .tmp_runtime\official_global_panel_batches\<run-slug>\resume-summary.json

.\.venv\Scripts\python.exe scripts\materialize_schedule_value_production_gate_registry.py `
  --gate-frame-pickle data\research_runs\<run-slug>\dfl_official_global_panel_schedule_value_production_gate_frame.pkl `
  --run-slug week3_official_global_panel_365_strategy_promotion `
  --attempt-manifest .tmp_runtime\official_global_panel_batches\<run-slug>\attempt_manifest.json `
  --monitor-snapshot .tmp_runtime\official_global_panel_batches\<run-slug>\resume-summary.json
```

The export writes `attempt_manifest.json` and `resume-summary.json` beside the
registry JSON/Markdown artifacts and records their names in the registry
metadata. That folder is the preferred supervisor-facing evidence packet.

## Hugging Face Jobs Integration

`scripts/build_hf_official_schedule_value_job.py` now embeds the same manifest
shape in the generated UV-job payload. The payload still does not submit by
itself. It remains a safe, inspectable artifact until the branch is pushed and
the Hugging Face account/token setup is ready.

`scripts/submit_hf_official_schedule_value_job.py` is the guarded wrapper for
that payload. By default it writes only a dry-run receipt. A real paid job
requires `--submit`; when artifact upload is configured, the wrapper resolves
`HF_TOKEN` only in memory and never writes it to the local receipt.

Current Hugging Face Jobs documentation supports:

- UV-script jobs;
- custom `timeout` values for long-running jobs;
- secret passing for tokens;
- selecting compute flavor.

The repo therefore keeps the current offload strategy:

1. build and inspect the payload locally;
2. write a dry-run receipt with the guarded submission wrapper;
3. submit only after `HF_TOKEN` and artifact repo permissions are available;
4. persist outputs as artifacts, not as live strategy defaults.

## Claim Boundary

The manifest deliberately uses thesis language:

- `offline_strategy_promotion_language=true`;
- `not_market_execution=true`;
- `market_execution_enabled=false`;
- `claim_scope=offline_strategy_promotion_evidence_attempt`.

Internal assets may still contain historical names such as
`production_gate_frame` or `production_promote`. Documentation and summaries
must translate that to **Offline Strategy Promotion** unless and until the
project explicitly moves to real market execution.

## Verification

Focused tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider `
  tests\forecasting\test_official_evidence_attempts.py `
  tests\cloud\test_hf_official_jobs.py `
  tests\test_project_entrypoints.py
```

Full verification remains:

```powershell
.\.venv\Scripts\Activate.ps1
.\scripts\verify.ps1
uv run dg list defs --json
uv run dg check defs
docker compose config --quiet
git diff --check
```

## Next Work

1. If the Codex heartbeat monitor is recreated, point it at
   `scripts/monitor-official-evidence-attempt.ps1` instead of direct `run.log`
   and SQL inspection.
2. Use the HF submission wrapper only after the payload is tested on a pushed
   branch and a paid HF Jobs account/token path is confirmed.
