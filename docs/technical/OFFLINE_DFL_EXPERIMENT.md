# Offline DFL Experiment

Date: 2026-05-07

This slice starts the first bounded differentiable-training experiment after
the DFL readiness gate. It is intentionally narrow: one Dagster Gold research
asset trains horizon-specific price biases through the existing relaxed LP and
scores later Dnipro anchors. It is not full DFL, not a Decision Transformer, and
not market execution.

## Scope

| Field | Value |
|---|---|
| Asset | `offline_dfl_experiment_frame` |
| Dagster group | `gold_dfl_training` |
| Tags | `medallion=gold`, `domain=dfl_research`, `ml_stage=pilot`, `evidence_scope=not_market_execution`, `market_venue=DAM` |
| Tenant | `client_003_dnipro_factory` |
| Source asset | `real_data_rolling_origin_benchmark_frame` |
| Data window | Observed OREE DAM + historical Open-Meteo, `2026-01-01` to `2026-04-30` |
| Run config | [../../configs/real_data_calibration_week4.yaml](../../configs/real_data_calibration_week4.yaml) |
| Claim scope | `offline_dfl_experiment_not_full_dfl` |

The implementation uses `build_offline_dfl_experiment_frame` to:

1. sort each tenant/model stream by `anchor_timestamp`;
2. reserve a later holdout slice by `validation_fraction`;
3. train only on prior anchors, capped by `max_train_anchors`;
4. learn one additive bias per horizon step through the differentiable relaxed
   LP;
5. evaluate the learned bias and the raw forecast on the held-out anchors with
   the same relaxed LP/oracle comparison.

## Materialization

Command used:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_experiment_frame -c configs/real_data_calibration_week4.yaml
```

Run outcome:

| Field | Value |
|---|---|
| Dagster run id | `54afa042-332c-459e-b6ea-e1b0308fa508` |
| Materialized asset | `offline_dfl_experiment_frame` |
| Materialized rows | 2 |
| Latest raw benchmark batch | `2026-05-07T10:01:50.67257Z` |
| Benchmark evidence check | `dnipro_thesis_grade_90_anchor_evidence` passed |
| Local Dagster IO path | `/opt/dagster/dagster_home/storage/offline_dfl_experiment_frame` |

## Results

Both rows use 32 prior training anchors and 18 held-out validation anchors. The
holdout starts strictly after the training window.

| Model | Train anchors | Validation anchors | Holdout window | Raw relaxed regret | Offline DFL relaxed regret | Delta | Result |
|---|---:|---:|---|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 32 | 18 | `2026-04-12 23:00` to `2026-04-29 23:00` | 1477.37 | 1499.85 | -22.47 | Worse than raw |
| `tft_silver_v0` | 32 | 18 | `2026-04-12 23:00` to `2026-04-29 23:00` | 1974.55 | 2460.07 | -485.52 | Worse than raw |

The first experiment therefore stays diagnostic. It proves the differentiable
LP training loop can run on the gated Dnipro evidence without leakage, but it
does not improve the held-out relaxed regret and must not be promoted as a DFL
win.

## API Context

The dashboard read models remain unchanged. After this run, the raw benchmark
latest batch was refreshed; the calibration/selector read models still point to
the prior 90-anchor calibration batch from the DFL readiness gate.

| Endpoint | Generated at | Anchors | Models | Tier | Mean regret | Best / diagnostic |
|---|---|---:|---:|---|---:|---|
| `/dashboard/real-data-benchmark` | `2026-05-07T10:01:50.67257Z` | 90 | 3 | `thesis_grade` | 1938.98 | `strict_similar_day` |
| `/dashboard/calibrated-ensemble-benchmark` | `2026-05-07T02:24:42.974392Z` | 90 | 1 | `thesis_grade` | 1479.65 | `calibrated_value_aware_ensemble_v0` |
| `/dashboard/risk-adjusted-value-gate` | `2026-05-07T02:24:42.974392Z` | 90 | 1 | `thesis_grade` | 1428.59 | `risk_adjusted_value_gate_v0` |
| `/dashboard/forecast-dispatch-sensitivity` | `2026-05-07T02:24:42.974392Z` | 90 | 5 | Diagnostic rows | n/a | 4 buckets |

Postgres contains older persisted Dnipro batches as well as the latest batch.
The API read models continue to report the latest intended batch for each
strategy.

## Verification

Commands run:

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\dfl\test_offline_dfl_experiment.py tests\assets\test_dfl_research_assets.py tests\assets\test_medallion_tags.py tests\test_project_entrypoints.py
.\scripts\verify.ps1
uv run dg list defs --json
uv run dg check defs
docker compose config --quiet
git diff --check
```

Results:

| Check | Result |
|---|---|
| Focused tests | 9 passed |
| Repo verification | Ruff passed, Mypy passed, 165 Pytest tests passed, `dg check defs` passed |
| Dagster list/check | `offline_dfl_experiment_frame` registered; definitions load |
| Compose config | Passed |
| Whitespace check | Passed |

Focused test coverage includes a no-leakage assertion: mutating validation
actual prices does not change the learned horizon biases, which confirms the
training step only sees prior anchors.

## Claim Boundary

This artifact can support the statement:

> A bounded offline DFL experiment is now materialized in Dagster and tested for
> temporal holdout discipline, but the first held-out relaxed-LP result is
> negative and remains research-only.

It cannot support these statements:

- full DFL has been achieved;
- a Decision Transformer has been trained from this evidence;
- the learned biases should be used for market execution;
- the offline relaxed-LP result beats the calibrated selectors.

## Next Technical Step

The next DFL slice should improve the experiment design before expanding scope:

1. add a validation-safe model selection rule or checkpointing criterion;
2. compare relaxed-LP training loss against strict-LP realized regret on the
   same anchors;
3. test whether horizon bias is too weak by adding a small linear covariate
   adapter over forecast diagnostics;
4. keep the Dnipro 90-anchor split frozen until the offline experiment beats
   raw candidates and selector baselines on held-out anchors.
