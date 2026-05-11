# Official NBEATSx/TFT Schedule-Value Promotion Gate

Date: 2026-05-11

This slice routes serious official NBEATSx/TFT rolling-origin forecasts through
the same feasible schedule library, Schedule/Value Learner V2, rolling
robustness check, and offline promotion gate used by the compact in-repo
forecast candidates.

Claim boundary: official forecasts remain research evidence. This is not live
market execution, not a deployed Decision Transformer controller, and not a full
end-to-end DFL claim. The frozen `strict_similar_day` control remains the
fallback unless the strict LP/oracle promotion gate passes.

## Rationale

The official model path is necessary because compact in-repo candidates are
diagnostics, not a fair final test of the external forecasting libraries.
Nixtla NeuralForecast expects long tabular series with `unique_id`, `ds`, and
`y`, while the project already exports the matching SOTA-ready training frame.
NBEATSx is the exogenous-variable variant of N-BEATS, and TFT is a
multi-horizon transformer architecture with interpretable covariate selection.

Recent offline sequence-model work, including Decision Transformer variants,
supports a later trajectory-learning path, but the immediate thesis-safe step is
still forecast-to-schedule evidence: train official forecasts with masked future
targets, strict-score their schedules, then let the same prior-only
schedule/value learner decide whether any official source can beat the frozen
control.

## Assets

| Asset | Purpose |
|---|---|
| `official_forecast_rolling_origin_benchmark_frame` | Retrains official NBEATSx/TFT per rolling anchor using only prior rows and strict-scores the forecasts. |
| `dfl_official_schedule_candidate_library_frame` | Converts official strict LP/oracle rows into strict/raw/perturbation schedule candidates. |
| `dfl_official_schedule_candidate_library_v2_frame` | Adds strict/raw blend and prior-residual schedule candidates. |
| `dfl_official_schedule_value_learner_v2_frame` | Selects the schedule-scoring profile from train-selection anchors only. |
| `dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame` | Emits strict/raw/learner final-holdout rows for the official sources. |
| `dfl_official_schedule_value_learner_v2_robustness_frame` | Replays the learner over four prior-only rolling validation windows. |
| `dfl_official_schedule_value_production_gate_frame` | Emits the final offline promotion/fallback decision per official source. |
| `dfl_official_schedule_value_production_gate_evidence` | Dagster asset check for claim flags, coverage, and disabled market execution. |

Tracked config:
[configs/real_data_official_schedule_value_promotion_week3.yaml](../../configs/real_data_official_schedule_value_promotion_week3.yaml).

## Promotion Semantics

The official source models are evaluated independently:

- `nbeatsx_official_v0`;
- `tft_official_v0`.

The gate requires:

- five canonical tenants;
- 90 final validation tenant-anchors per source model;
- thesis-grade observed OREE/Open-Meteo provenance;
- zero safety violations;
- no train/final leakage;
- at least 5% mean regret improvement versus `strict_similar_day`;
- median regret not worse than `strict_similar_day`;
- at least three of four rolling strict-control windows passing.

`market_execution_enabled` remains `false` even if an official source passes.

## Materialization

Rebuild the backend/Dagster services so the new assets and config are visible:

```powershell
docker compose config --quiet
docker compose up -d --build postgres mlflow dagster-webserver dagster-daemon api
```

Then materialize:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_rolling_origin_benchmark_frame,dfl_official_schedule_candidate_library_frame,dfl_official_schedule_candidate_library_v2_frame,dfl_official_schedule_value_learner_v2_frame,dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame,dfl_official_schedule_value_learner_v2_robustness_frame,dfl_official_schedule_value_production_gate_frame -c configs/real_data_official_schedule_value_promotion_week3.yaml
```

The config asks for 104 rolling anchors per tenant because the robustness gate
needs four 18-anchor validation windows plus at least 30 prior anchors. This is
expected to be a long CPU run. If the optional official backends or local runtime
cannot complete it, the result should be documented as adapter/runtime evidence,
not silently replaced by compact-model rows.

### Resumable 104-anchor runner

For unattended local CPU execution, prefer the resumable batch runner:

```powershell
.\scripts\run-official-schedule-value-batches.ps1 `
  -TotalAnchorsPerTenant 104 `
  -BatchSize 4 `
  -BatchTimeoutSeconds 10800
```

The runner assigns one fixed `GeneratedAtIso` to the whole evidence attempt,
materializes the official rolling-origin asset in anchor batches, persists each
batch through `forecast_strategy_evaluations`, and then materializes the
downstream official schedule/value gate. Logs are written under
`.tmp_runtime/official_schedule_value_batches/`.

If a batch fails, rerun with the same `-GeneratedAtIso` and the failed
`-StartAnchorIndex`. Already persisted batches are merged back into the asset
output when `merge_persisted_batches=true`, so the next successful batch resumes
the same evidence run instead of starting over.

## Current Status

Implementation is additive:

- no public FastAPI/dashboard contract changes;
- no changes to existing compact DFL asset keys;
- no changes to Pydantic schemas, resources, IO managers, or dependencies;
- official promotion evidence is not persisted into the existing compact
  schedule/value read model.

Verification completed before runtime execution:

- `.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\dfl\test_official_schedule_value.py tests\dfl\test_schedule_value_learner.py tests\dfl\test_schedule_value_learner_robustness.py tests\dfl\test_schedule_value_promotion_gate.py tests\strategy\test_official_forecast_rolling.py`
- `.\.venv\Scripts\Activate.ps1; .\scripts\verify.ps1`
- `uv run dg list defs --json`
- `uv run dg check defs`
- `docker compose config --quiet`
- `git diff --check`

Runtime execution attempt:

| Field | Value |
|---|---|
| Attempted run id | `2d85501d-3024-4c10-b983-3aca40bfa288` |
| Command scope | full 104-anchor official NBEATSx/TFT path plus schedule/value gate |
| Result | failed after Codex shell timeout at 3600 seconds |
| Last active step | `official_forecast_rolling_origin_benchmark_frame` |
| Completed in that run | observed market bronze, tenant weather bronze, real-data benchmark silver |
| Not completed in that run | official rolling forecast benchmark, official schedule library, official schedule/value learner, robustness frame, production gate |

The timeout is a runtime capacity finding, not a promotion result. The serious
official path is now wired and verified, but the local CPU-backed Compose run did
not finish enough official rolling-origin anchors to create promotion-grade
metrics. Until a longer unattended/GPU-backed run completes, the official
NBEATSx/TFT models remain adapter-ready research candidates, not promoted
controllers.

Downstream smoke validation:

| Field | Value |
|---|---|
| Smoke run id | `bea10f88-ed52-4f7d-b85b-93b149262c51` |
| Input official benchmark | existing 4-anchor official rolling-origin artifact |
| Scope | official schedule library, library v2, learner v2, strict LP benchmark, robustness, production gate |
| Result | run succeeded |
| Promotion-grade? | no; only 10 latest validation tenant-anchors per source and 2 rolling windows |

Smoke gate summary:

| Source | Latest validation tenant-anchors | Mean-regret improvement vs strict | Median not worse | Rolling strict-pass windows | Production promote | Market execution enabled | Blocker |
|---|---:|---:|---|---:|---|---|---|
| `nbeatsx_official_v0` | 10 | 0.0% | yes | 0 / 2 | false | false | `mean_improvement_below_threshold` |
| `tft_official_v0` | 10 | 0.0% | yes | 0 / 2 | false | false | `mean_improvement_below_threshold` |

The smoke result proves the official rows can now flow through the same
schedule/value candidate library and gate. It does not change the thesis claim:
the official models still need a completed 104-anchor or larger rolling-origin
run before they can be compared fairly with the compact schedule/value
promotion evidence.

Follow-up execution protocol:

- use the resumable batch runner for the next 104-anchor attempt;
- keep the outer per-batch timeout at least three hours on local CPU;
- record the generated timestamp and failed anchor index if a resume is needed;
- do not treat partial official rows as promotion-grade evidence.
