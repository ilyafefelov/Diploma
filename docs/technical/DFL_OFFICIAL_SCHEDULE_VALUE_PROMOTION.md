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

## Current Status

Implementation is additive:

- no public FastAPI/dashboard contract changes;
- no changes to existing compact DFL asset keys;
- no changes to Pydantic schemas, resources, IO managers, or dependencies;
- official promotion evidence is not persisted into the existing compact
  schedule/value read model.

Materialized metrics will be recorded here after the Compose-backed run.
