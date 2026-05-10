# DFL Source-Specific Research Challenger Gate

Date: 2026-05-10

This slice separates TFT and NBEATSx instead of forcing one aggregate promotion
decision. It combines four existing evidence paths:

- residual/DT fallback strict LP/oracle evidence;
- feature-aware selector strict LP/oracle evidence;
- rolling strict-failure robustness windows;
- prior feature-audit clusters.

Claim boundary: research evidence only. This is not full DFL, not deployed
Decision Transformer control, not production promotion, and not market
execution. `strict_similar_day` remains the frozen Level 1 control and default
fallback.

## Assets

| Artifact | Purpose |
|---|---|
| `smart_arbitrage.dfl.source_specific_challenger` | Source-level helper that merges latest strict evidence, rolling robustness, and audit context. |
| `dfl_source_specific_research_challenger_frame` | One row per source model with latest signal, rolling evidence, cluster context, and claim flags. |
| `dfl_source_specific_research_challenger_evidence` | Dagster asset check for source coverage, provenance, claim flags, and rolling context. |
| [real_data_dfl_source_specific_challenger_week3.yaml](../../configs/real_data_dfl_source_specific_challenger_week3.yaml) | Tracked Week 3 run config for the source-specific gate. |

## Gate Semantics

| Label | Meaning |
|---|---|
| `latest_source_signal` | Source-specific fallback beats `strict_similar_day` by at least 5% mean regret on the latest final holdout and does not worsen median regret. |
| `rolling_development_pass` | Source improves over raw neural schedules across rolling windows. |
| `robust_research_challenger` | Latest source signal plus at least 3 of 4 rolling strict-control window passes. |
| `production_promote` | Always `false` in this slice. |

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_panel_experiment_frame,offline_dfl_decision_target_panel_frame,offline_dfl_decision_target_strict_lp_benchmark_frame,offline_dfl_action_target_panel_frame,offline_dfl_action_target_strict_lp_benchmark_frame,dfl_schedule_candidate_library_frame,dfl_pipeline_integrity_audit_frame,dfl_schedule_candidate_library_v2_frame,dfl_strict_failure_selector_robustness_frame,tenant_consumption_schedule_bronze,tenant_historical_net_load_silver,dfl_strict_failure_prior_feature_panel_frame,dfl_strict_failure_feature_audit_frame,dfl_feature_aware_strict_failure_selector_frame,dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame,dfl_real_data_trajectory_dataset_frame,dfl_residual_schedule_value_model_frame,dfl_offline_dt_candidate_frame,dfl_residual_dt_fallback_strict_lp_benchmark_frame,dfl_source_specific_research_challenger_frame -c configs/real_data_dfl_source_specific_challenger_week3.yaml
```

Local export slug:

`data/research_runs/week3_source_specific_research_challenger/`

## Materialized Evidence

| Field | Value |
|---|---|
| Dagster run id | `be22b25b-a1c5-40d9-9049-a01efb8e7e5f` |
| Run status | `SUCCESS` |
| New asset check | `dfl_source_specific_research_challenger_evidence` passed |
| Source rows | 2 |
| Validation tenant-anchors | 90 per source model |
| Claim flags | `not_full_dfl=true`, `not_market_execution=true`, `production_promote=false` |

One existing check is now stale for this all-tenant panel:
`dnipro_thesis_grade_90_anchor_evidence` reported `anchor_count must be 90;
observed 104`. That check was written for the earlier Dnipro 90-anchor preview,
while this source-specific run uses the current 104-anchor all-tenant panel.

## Source Results

| Source model | Strict mean regret UAH | Fallback mean regret UAH | Mean improvement vs strict | Strict median UAH | Fallback median UAH | Latest signal | Rolling strict passes | Gate label |
|---|---:|---:|---:|---:|---:|---|---:|---|
| `nbeatsx_silver_v0` | 314.81 | 318.37 | -1.13% | 202.61 | 172.49 | false | 0 / 4 | `rolling_development_only` |
| `tft_silver_v0` | 314.81 | 258.12 | 18.01% | 202.61 | 136.05 | true | 0 / 4 | `latest_signal_not_robust` |

Feature-aware selector context:

| Source model | Feature-aware improvement vs strict | Feature-aware development pass |
|---|---:|---|
| `nbeatsx_silver_v0` | -5.51% | true |
| `tft_silver_v0` | 0.00% | true |

Feature-audit context:

| Source model | Dominant failure cluster |
|---|---|
| `nbeatsx_silver_v0` | `strict_stable_region` |
| `tft_silver_v0` | `strict_stable_region` |

## Interpretation

TFT is now a source-specific research challenger on the latest final holdout:
it beats `strict_similar_day` by 18.01% mean regret and improves median regret.
That is the strongest DFL-adjacent signal so far.

It is still not robust enough to promote. The rolling strict-control robustness
count is 0 of 4 in the current combined gate, so the correct claim is
`latest_signal_not_robust`, not production readiness.

NBEATSx remains blocked versus strict control because its fallback mean regret
is worse than `strict_similar_day`, even though the median improves.

## Next Decision

The next slice should not add another DT variant. The immediate blocker is
robustness: either recover more Ukrainian history, strengthen prior-only regime
features, or make the source-specific TFT switch rule survive earlier rolling
windows. Production promotion remains blocked until the same source beats
`strict_similar_day` by the strict gate across rolling evidence.
