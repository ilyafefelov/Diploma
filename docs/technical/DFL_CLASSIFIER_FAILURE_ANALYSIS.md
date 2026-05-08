# DFL Classifier Failure Analysis

Date: 2026-05-08

This note records the next evidence slice after the supervised action-label
classifier and value-aware classifier were blocked by the strict LP/oracle
promotion gate. The result is useful negative evidence: both candidates remain
feasible and no-leakage, but they are not decision-value controllers.

## Scope

| Field | Value |
|---|---|
| New asset | `dfl_action_classifier_failure_analysis_frame` |
| Dagster group | `gold_dfl_training` |
| Asset check | `dfl_action_classifier_failure_analysis_evidence` |
| Upstream labels | `dfl_action_label_panel_frame` |
| Upstream strict projections | `dfl_action_classifier_strict_lp_benchmark_frame`, `dfl_value_aware_action_classifier_strict_lp_benchmark_frame` |
| Tenants | all five canonical Ukrainian tenants |
| Source models | `tft_silver_v0`, `nbeatsx_silver_v0` |
| Final holdout | 18 anchors per tenant/model, 90 tenant-anchors per source model |
| Claim scope | `dfl_action_classifier_failure_analysis_not_full_dfl` |

The analysis remains research-only: not full DFL, not Decision Transformer
control, not market execution, and not a promoted controller.

## Why The Gate Blocks The Classifiers

The existing evidence is internally consistent:

| Candidate | Rows | Mean regret UAH | Median regret UAH | Status |
|---|---:|---:|---:|---|
| `strict_similar_day` | 180 | 314.81 | 202.61 | frozen control |
| `dfl_action_classifier_v0_tft_silver_v0` | 90 | 1,157.40 | 715.66 | blocked |
| `dfl_action_classifier_v0_nbeatsx_silver_v0` | 90 | 1,186.83 | 1,054.08 | blocked |
| `dfl_value_aware_action_classifier_v1_tft_silver_v0` | 90 | 1,198.74 | 975.43 | blocked |
| `dfl_value_aware_action_classifier_v1_nbeatsx_silver_v0` | 90 | 1,498.95 | 1,341.77 | blocked |

The failure mode is methodological, not a materialization bug:

- Per-hour charge/discharge/hold classification is a weak proxy for cumulative
  arbitrage value.
- SOC is path-dependent: an early wrong action changes later feasible actions.
- Class balance is dominated by hold labels, so accuracy can look acceptable
  while rare high-value charge/discharge opportunities are missed.
- Action labels discard price-spread magnitude and opportunity value.
- The value-aware voting variant still votes on static hourly labels, so it
  does not solve the trajectory/value mismatch.

The new failure-analysis asset makes this visible through regret-weighted
confusion, active-hour precision/recall, missed high-value charge/discharge
hours, false active actions, top/bottom price-rank misses, and value loss versus
`strict_similar_day`.

## Research Grounding

| Source | Use in this slice |
|---|---|
| [Decision-Focused Learning survey](https://huggingface.co/papers/2307.13565) | Frames DFL as learning for downstream constrained decision quality, not proxy metrics. |
| [Electricity price prediction for ESS arbitrage: decision-focused approach](https://arxiv.org/abs/2305.00362) | Supports regret/oracle decision error as the target for storage arbitrage forecasting. |
| [Smart Predict-and-Optimize](https://arxiv.org/abs/1911.10092) | Supports optimizing prediction models through downstream regret. |
| [Decision-Focused Forecasting for multistage optimisation](https://arxiv.org/abs/2405.14719) | Explains why energy storage arbitrage needs intertemporal/SOC-path-aware learning. |
| [DAgger imitation-learning warning](https://api.emergentmind.com/papers/1011.0686) | Explains why supervised action imitation can compound errors in sequential decisions. |
| [Electricity forecast economic evaluation beyond RMSE/MAE](https://ideas.repec.org/p/arx/papers/2511.13616.html) | Supports price-extrema, dispersion, association, and profit/value metrics for BESS forecasts. |
| [BESS dispatch forecast impact](https://www.dfki.de/web/forschung/projekte-publikationen/publikation/16172) | Supports realized-revenue and oracle-dispatch evaluation for storage forecasts. |
| [TSFM leakage evaluation](https://huggingface.co/papers/2510.13654) | Keeps final holdout, temporal split, and no-leakage evidence explicit. |

## Materialization Protocol

After backend/Dagster services are rebuilt with the current source:

```powershell
docker compose config --quiet
docker compose up -d --build postgres mlflow dagster-webserver dagster-daemon api
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,dfl_data_coverage_audit_frame,dfl_action_label_panel_frame,dfl_action_classifier_baseline_frame,dfl_action_classifier_strict_lp_benchmark_frame,dfl_value_aware_action_classifier_strict_lp_benchmark_frame,dfl_action_classifier_failure_analysis_frame -c configs/real_data_dfl_data_expansion_week3.yaml
```

Local export slug for concise evidence:

```text
week3_dfl_classifier_failure_analysis
```

Generated `data/` artifacts should remain local unless a concise summary is
intentionally selected for the report.

## Latest Execution

The 2026-05-08 Compose-backed run completed successfully.

| Field | Value |
|---|---|
| Dagster run id | `9a3eb772-dbd5-4023-beff-ed8f5a69e326` |
| MLflow run id | `dcb1abe5ae0d44d2b23aa7de0354f9a6` |
| New diagnostic rows | 20 expected rows: 5 tenants x 2 source models x 2 classifier variants |
| Asset check | `dfl_action_classifier_failure_analysis_evidence` passed |
| Check description | `DFL classifier failure analysis is no-leakage research evidence.` |

Latest persisted strict-projection rows:

| Strategy kind | Forecast model | Rows | Tenants | Final anchors | Mean regret UAH |
|---|---|---:|---:|---:|---:|
| `dfl_action_classifier_strict_lp_projection` | `dfl_action_classifier_v0_tft_silver_v0` | 90 | 5 | 18 | 1,157.40 |
| `dfl_action_classifier_strict_lp_projection` | `dfl_action_classifier_v0_nbeatsx_silver_v0` | 90 | 5 | 18 | 1,186.83 |
| `dfl_action_classifier_strict_lp_projection` | `strict_similar_day` | 180 | 5 | 18 | 314.81 |
| `dfl_value_aware_action_classifier_strict_lp_projection` | `dfl_value_aware_action_classifier_v1_tft_silver_v0` | 90 | 5 | 18 | 1,198.74 |
| `dfl_value_aware_action_classifier_strict_lp_projection` | `dfl_value_aware_action_classifier_v1_nbeatsx_silver_v0` | 90 | 5 | 18 | 1,498.95 |
| `dfl_value_aware_action_classifier_strict_lp_projection` | `strict_similar_day` | 180 | 5 | 18 | 314.81 |

## Claim Boundary

This slice supports:

> The action classifiers are blocked because classification/imitation evidence
> does not translate into strict LP/oracle decision value against the frozen
> `strict_similar_day` control.

It does not support:

- full DFL success;
- Decision Transformer control;
- live trading;
- European data ingestion;
- replacing `strict_similar_day`.

## Next Technical Direction

Do not continue with static hourly classifier tweaks. The next model-improvement
slice should use richer Ukrainian coverage and a trajectory/value learner over
feasible LP-generated schedules. The candidate set should include
`strict_similar_day`, raw TFT/NBEATSx LP schedules, calibrated schedules, and
price-perturbation schedules, with selection learned only from prior anchors
and final scoring still performed by the strict LP/oracle gate.
