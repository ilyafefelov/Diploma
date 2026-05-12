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

## Hugging Face Jobs Integration

`scripts/build_hf_official_schedule_value_job.py` now embeds the same manifest
shape in the generated UV-job payload. The payload still does not submit by
itself. It remains a safe, inspectable artifact until the branch is pushed and
the Hugging Face account/token setup is ready.

Current Hugging Face Jobs documentation supports:

- UV-script jobs;
- custom `timeout` values for long-running jobs;
- secret passing for tokens;
- selecting compute flavor.

The repo therefore keeps the current offload strategy:

1. build and inspect the payload locally;
2. submit only after `HF_TOKEN` and artifact repo permissions are available;
3. persist outputs as artifacts, not as live strategy defaults.

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

1. Use `attempt_manifest.json` in the monitoring automation so it can compute
   the next resume anchor without parsing free-form logs.
2. Add an evidence-registry export that copies the manifest next to the final
   Offline Strategy Promotion summary. The 365-anchor official global-panel
   run now has a local export at
   `data/research_runs/week3_official_global_panel_365_strategy_promotion/`.
   Because the original run began before this manifest interface existed, the
   backfilled manifest records the resumed `8..365` segment while Postgres row
   counts and the run logs confirm full 365-anchor coverage.
3. Add a Hugging Face submission wrapper only after the payload is tested on a
   pushed branch and a paid HF Jobs account/token path is confirmed.
