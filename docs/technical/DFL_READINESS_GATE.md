# DFL Readiness Gate

Date: 2026-05-07

This gate turns the Dnipro 90-anchor calibration preview into an explicit
stability checkpoint before any full differentiable DFL training experiment.
The current system remains calibration and selector evidence only. It is not a
full DFL result, not a market execution system, and not a claim that a learned
policy is production-ready.

## Current Decision

| Gate lane | Decision | Meaning |
|---|---|---|
| Week 3 accepted thesis evidence | Pass | Dnipro 30-anchor real-data benchmark remains the current weekly headline. |
| Dnipro 90-anchor calibration preview | Pass | Sufficient to start a controlled offline DFL experiment design later. |
| Offline DFL experiment | Diagnostic pass | The first relaxed-LP training asset materializes and preserves temporal holdout discipline, but its held-out regret is worse than raw candidates. |
| Full differentiable DFL claim | Blocked | Requires a trained differentiable objective, frozen temporal splits, and comparison against the same oracle/regret protocol. |
| Market execution claim | Blocked | Requires a live/paper execution protocol, gatekeeper audit trail, and operator approval boundary. |

The practical decision is conservative: the 90-anchor evidence can seed an
offline DFL experiment, but it cannot be reported as full DFL.

Latest verified gate run:

| Field | Value |
|---|---|
| Dagster run id | `b55b9e01-8688-4fc2-abe6-6380b96502b9` |
| Run config | [configs/real_data_calibration_week4.yaml](../../configs/real_data_calibration_week4.yaml) |
| Latest Dnipro batch | `2026-05-07T02:24:42.974392Z` |
| Export slug | `week3_dfl_readiness_gate_dnipro_90` |
| Export directory | `data/research_runs/week3_dfl_readiness_gate_dnipro_90` |
| Manifest path | `data/research_runs/week3_dfl_readiness_gate_dnipro_90/research_layer_manifest.json` |
| Claim scope | `calibration_selector_evidence_not_full_dfl` |
| Claim flags | `not_full_dfl=true`, `not_market_execution=true` |

## Dagster Evidence Checks

The following asset checks are registered in `smart_arbitrage.defs` and attach
pass/fail metadata to existing assets without changing asset keys, resources, IO
managers, API contracts, dashboard contracts, or Pydantic schemas.

| Asset | Check | Severity on failure | Evidence rule |
|---|---|---|---|
| `real_data_rolling_origin_benchmark_frame` | `dnipro_thesis_grade_90_anchor_evidence` | `ERROR` | Latest Dnipro batch has at least 90 anchors, exactly the three raw candidates, thesis-grade data, observed rows only, and forecast horizons strictly after anchors. |
| `dfl_training_frame` | `dfl_training_readiness_evidence` | `WARN` | Dnipro training rows exist, raw candidates and selector rows are represented, and all rows stay research-only/not-market-execution. |
| `horizon_regret_weighted_forecast_strategy_benchmark_frame` | `horizon_calibration_no_leakage_evidence` | `ERROR` | Horizon-aware calibrated candidates keep 90 anchors and carry prior-anchor calibration metadata only. |
| `calibrated_value_aware_ensemble_frame` | `calibrated_selector_cardinality_evidence` | `ERROR` | One selector row per Dnipro anchor, thesis-grade evidence, no full-DFL claim. |
| `risk_adjusted_value_gate_frame` | `risk_adjusted_selector_cardinality_evidence` | `ERROR` | One risk-gate selector row per Dnipro anchor, thesis-grade evidence, no full-DFL claim. |

`WARN` is used only for DFL readiness notes because a missing DFL training table
should not invalidate the raw benchmark. Thesis-grade provenance, latest-batch
freshness, no-leakage evaluation, and selector cardinality are blocking `ERROR`
checks.

Latest check results:

| Check | Result | Key metadata |
|---|---|---|
| `dnipro_thesis_grade_90_anchor_evidence` | Pass | `anchor_count=90`, `model_count=3`, `data_quality_tiers=["thesis_grade"]`, `leaky_horizon_rows=0`. |
| `dfl_training_readiness_evidence` | Pass | `row_count=360`, `anchor_count=90`, raw candidates plus `value_aware_ensemble_v0`, `market_execution_rows=0`. |
| `horizon_calibration_no_leakage_evidence` | Pass | `anchor_count=90`, `model_count=5`, `missing_model_anchor_pairs=0`, `leaky_rows=0`. |
| `calibrated_selector_cardinality_evidence` | Pass | `row_count=90`, `duplicate_anchor_count=0`, `full_dfl_claim_rows=0`. |
| `risk_adjusted_selector_cardinality_evidence` | Pass | `row_count=90`, `duplicate_anchor_count=0`, `full_dfl_claim_rows=0`. |

API read-model validation:

| Endpoint | Anchors | Models | Rows | Tier | Mean regret UAH | Best model / diagnostic |
|---|---:|---:|---:|---|---:|---|
| `/dashboard/real-data-benchmark` | 90 | 3 | 270 | `thesis_grade` | 1938.98 | `strict_similar_day` |
| `/dashboard/calibrated-ensemble-benchmark` | 90 | 1 | 90 | `thesis_grade` | 1479.65 | `calibrated_value_aware_ensemble_v0` |
| `/dashboard/risk-adjusted-value-gate` | 90 | 1 | 90 | `thesis_grade` | 1428.59 | `risk_adjusted_value_gate_v0` |
| `/dashboard/forecast-dispatch-sensitivity` | 90 | 5 | 450 | Diagnostic rows | n/a | Forecast/dispatch sensitivity buckets. |

## Pass Criteria

The DFL readiness gate passes for an offline experiment only when all of these
conditions are true:

| Criterion | Required state |
|---|---|
| Tenant scope | `client_003_dnipro_factory` only. |
| Data window | Observed OREE DAM and historical Open-Meteo, `2026-01-01` to `2026-04-30`. |
| Anchor coverage | Latest Dnipro batch has `anchor_count >= 90`. |
| Raw forecast candidates | `strict_similar_day`, `tft_silver_v0`, and `nbeatsx_silver_v0` are all represented for every anchor. |
| Data quality | Latest batch reports `data_quality_tier=thesis_grade`; synthetic rows do not enter the claim. |
| Temporal evaluation | Forecast horizons are strictly after their anchor timestamps. |
| Calibration metadata | Horizon-aware calibrated rows use prior-anchor metadata only. |
| Selector cardinality | Calibrated ensemble and risk gate each publish one selector row per anchor. |
| Claim boundary | Evidence remains `research_only` or `not_market_execution`; no row claims full DFL. |

## Failure Handling

| Failure | Action |
|---|---|
| Fewer than 90 latest-batch Dnipro anchors | Rerun calibration materialization before DFL planning. |
| Non-thesis-grade rows in the latest claim batch | Treat the run as demo evidence only and block DFL readiness. |
| Missing raw model coverage | Fix the rolling-origin benchmark path before using downstream calibration evidence. |
| Forecast horizon before or equal to anchor | Treat as temporal leakage and block the claim. |
| Calibrated metadata using future anchors | Treat as temporal leakage and block the claim. |
| Selector duplicate/missing anchor rows | Fix selector persistence/read-model logic before presenting selector evidence. |
| Full-DFL or market-execution claim flag appears | Re-label the artifact or block the run from supervisor-facing evidence. |

## Research Guardrails

| Source | Gate role |
|---|---|
| [TSFM leakage evaluation](https://huggingface.co/papers/2510.13654) | Reinforces latest-batch temporal evaluation, strict no-leakage rolling origin, and explicit source-linked evidence before claims. |
| [PriceFM](https://huggingface.co/papers/2508.04875) | Supports future electricity-price foundation-model experiments after the benchmark protocol is enforceable. |
| [THieF](https://huggingface.co/papers/2508.11372) | Supports later temporal hierarchy forecasting after calibration evidence is stable. |
| [Distributional RL energy arbitrage](https://huggingface.co/papers/2401.00015) | Supports later risk-sensitive strategy discussion, not the current MVP implementation. |
| [Dagster asset checks](https://release-1-8-9.dagster.dagster-docs.io/concepts/assets/asset-checks/define-execute-asset-checks) | Justifies making evidence rules visible as asset-attached checks in lineage. |

## First DFL Step After Pass

After the gate passed, the first DFL slice stayed offline and bounded:

1. Freeze the Dnipro 90-anchor temporal split and manifest.
2. Build a small differentiable-training experiment from `dfl_training_frame`.
3. Train against decision regret while preserving the same LP/oracle evaluator.
4. Compare against strict similar-day, raw TFT/NBEATSx, calibrated candidates, and selector outputs.
5. Report improvement only if the learned objective beats calibrated/selector baselines on the same no-leakage anchors.

This slice can claim "DFL experiment started" because the gate passed. It still
cannot claim full DFL or market execution without a stronger held-out result and
a separate execution/gatekeeper audit protocol.

## Offline DFL Experiment Result

The next bounded slice was executed after this gate and is documented in
[OFFLINE_DFL_EXPERIMENT.md](OFFLINE_DFL_EXPERIMENT.md). It adds
`offline_dfl_experiment_frame` as a Gold research asset, not a public API or
dashboard contract.

Latest offline run:

| Field | Value |
|---|---|
| Dagster run id | `54afa042-332c-459e-b6ea-e1b0308fa508` |
| Asset | `offline_dfl_experiment_frame` |
| Latest raw benchmark batch | `2026-05-07T10:01:50.67257Z` |
| Evidence check | `dnipro_thesis_grade_90_anchor_evidence` passed |
| Output rows | 2 |
| Claim scope | `offline_dfl_experiment_not_full_dfl` |

Held-out relaxed-LP result:

| Model | Raw relaxed regret | Offline DFL relaxed regret | Delta | Decision |
|---|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 1477.37 | 1499.85 | -22.47 | Keep diagnostic only. |
| `tft_silver_v0` | 1974.55 | 2460.07 | -485.52 | Keep diagnostic only. |

Interpretation: the first differentiable relaxed-LP loop runs on the gated
evidence, but it does not improve held-out regret. The next DFL step should
improve validation-safe training design before broadening tenant scope or
making stronger thesis claims.
