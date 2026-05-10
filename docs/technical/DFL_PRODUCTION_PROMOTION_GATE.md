# DFL Production Promotion Gate

Date: 2026-05-10

This slice defines production promotion for the research stack: an offline
read-model strategy may automatically choose a DFL/TFT challenger instead of
`strict_similar_day` only when the strict LP/oracle evidence is robust. It does
not enable live market execution.

Claim boundary: offline strategy evidence only. This is not live trading, not a
market-execution policy, and not a deployed Decision Transformer controller.
`strict_similar_day` remains the frozen default unless this gate explicitly
passes.

## Assets

| Artifact | Purpose |
|---|---|
| `smart_arbitrage.dfl.production_promotion_gate` | Promotion helper that combines source-specific challenger evidence, rolling robustness, feature-audit regimes, and data coverage. |
| `dfl_production_promotion_gate_frame` | One row per source/regime with the promotion decision, blocker, fallback strategy, coverage summary, and claim flags. |
| `dfl_production_promotion_gate_evidence` | Dagster asset check for claim flags, coverage validity, promotion consistency, and market-execution boundary. |
| [real_data_dfl_production_promotion_gate_week3.yaml](../../configs/real_data_dfl_production_promotion_gate_week3.yaml) | Tracked Week 3 config for the source/regime promotion gate. |
| [DFL_REGIME_GATED_TFT_SELECTOR_V2.md](DFL_REGIME_GATED_TFT_SELECTOR_V2.md) | Follow-up v2 evidence that feeds this gate when available. |

## Promotion Semantics

| Gate field | Required condition |
|---|---|
| `latest_source_signal` | The source/regime fallback beats `strict_similar_day` by at least 5% mean regret on latest holdout and does not worsen median regret. |
| `rolling_strict_pass_window_count` | At least 3 of 4 rolling validation windows pass strict-control comparison. |
| `coverage_expansion_available` | Ukrainian observed OREE/Open-Meteo coverage meets the configured backfill target, or the true ceiling is explicitly documented and accepted for a narrower gate. |
| `production_promote` | `true` only when all evidence, coverage, robustness, safety, no-leakage, and claim-boundary checks pass. |
| `market_execution_enabled` | Always `false` in this slice. |

Promotion blockers are explicit: `evidence_invalid`, `coverage_insufficient`,
`latest_source_signal_missing`, `mean_improvement_below_threshold`,
`median_degraded`, `rolling_not_robust`, `strict_stable_region`, or `none`.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,dfl_data_coverage_audit_frame,offline_dfl_panel_experiment_frame,offline_dfl_decision_target_panel_frame,offline_dfl_decision_target_strict_lp_benchmark_frame,offline_dfl_action_target_panel_frame,offline_dfl_action_target_strict_lp_benchmark_frame,dfl_schedule_candidate_library_frame,dfl_pipeline_integrity_audit_frame,dfl_schedule_candidate_library_v2_frame,dfl_strict_failure_selector_robustness_frame,tenant_consumption_schedule_bronze,tenant_historical_net_load_silver,dfl_strict_failure_prior_feature_panel_frame,dfl_strict_failure_feature_audit_frame,dfl_feature_aware_strict_failure_selector_frame,dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame,dfl_real_data_trajectory_dataset_frame,dfl_residual_schedule_value_model_frame,dfl_offline_dt_candidate_frame,dfl_residual_dt_fallback_strict_lp_benchmark_frame,dfl_source_specific_research_challenger_frame,dfl_production_promotion_gate_frame -c configs/real_data_dfl_production_promotion_gate_week3.yaml
```

Local ignored export:

`data/research_runs/week3_dfl_production_promotion_gate/production_promotion_gate_summary.json`

## Materialized Evidence

| Field | Value |
|---|---|
| Dagster run id | `0cd165b5-1105-4cc1-a279-0e1144dd171b` |
| Run status | `SUCCESS` |
| New asset | `dfl_production_promotion_gate_frame` materialized |
| New asset check | `dfl_production_promotion_gate_evidence` did not pass |
| Gate rows | 5 source/regime rows |
| Production promotions | 0 |
| Market execution | `false` for every row |

The asset check failed because the backfill/coverage evidence is not
thesis-grade for the configured 180-anchor promotion target:

| Coverage field | Observed value |
|---|---:|
| Tenants | 5 |
| Eligible anchors per tenant | 104 |
| Target anchors per tenant | 180 |
| Missing price hours per tenant | 1 |
| Missing weather hours per tenant | 1 |
| Observed price coverage ratio | 0.9996527778 |
| Observed weather coverage ratio | 0.9996527778 |
| Data quality tier | `coverage_gap` |

Asset-check failure messages:

- `coverage audit requires thesis_grade rows`
- `coverage audit requires observed price coverage ratio of 1.0`
- `coverage audit requires observed weather coverage ratio of 1.0`

## Gate Result

| Source/regime | Latest mean improvement vs strict | Rolling strict passes | Coverage expansion | Promotion blocker | Production promote |
|---|---:|---:|---|---|---|
| `tft_silver_v0` / `high_spread_volatility` | 18.01% | 0 / 4 | false | `evidence_invalid` | false |
| `tft_silver_v0` / `strict_stable_region` | 18.01% | 0 / 4 | false | `evidence_invalid` | false |
| `nbeatsx_silver_v0` / `high_spread_volatility` | -1.13% | 0 / 4 | false | `evidence_invalid` | false |
| `nbeatsx_silver_v0` / `strict_failure_captured` | -1.13% | 0 / 4 | false | `evidence_invalid` | false |
| `nbeatsx_silver_v0` / `strict_stable_region` | -1.13% | 0 / 4 | false | `evidence_invalid` | false |

TFT still has the useful latest-holdout signal from the source-specific
challenger gate: 258.12 UAH mean regret versus 314.81 UAH for
`strict_similar_day`, with better median regret. The promotion gate correctly
refuses to use that single-window signal as a default strategy because rolling
strict-control robustness is 0 of 4 and the 180-anchor coverage target is not
available.

## Decision

No source/regime is promoted. The safe offline default remains
`strict_similar_day`.

This is the correct thesis finding: the project now has a Dagster-visible
promotion state that can set `production_promote=true`, but current Ukrainian
evidence blocks promotion. The next technical step is not another DT variant; it
is either recovering more observed Ukrainian history or tightening prior-only
regime gates until rolling windows pass without weakening the strict LP/oracle
promotion rule.

## Regime-Gated TFT V2 Update

The v2 selector was materialized on 2026-05-10 with Dagster run id
`1b901874-b713-4762-9154-2e822f91be8d`, then this production gate was rerun with
Dagster run id `e683a4b4-ce32-470b-8c61-71342ff23fa3` so it consumed the new v2
strict evidence.

V2 did not promote anything. It selected `strict_similar_day` for all evaluated
selector rows, so both source-specific v2 selectors matched strict-control mean
regret: `710.445` UAH, median regret `442.848` UAH. Raw neural schedules still
lost badly in the same strict frame: NBEATSx mean regret `1,525.526` UAH and TFT
mean regret `1,800.934` UAH.

The key blockers are now explicit:

| Source/regime | Validation tenant-anchors | Rolling windows | V2 evidence used | Promotion result |
|---|---:|---:|---|---|
| `tft_silver_v0` / `strict_stable_region` | 90 | 4 | true | blocked, strict-stable region |
| `tft_silver_v0` / `high_spread_volatility` | 54 | 1 | true | blocked, prior-regime undercoverage |
| `nbeatsx_silver_v0` / `strict_stable_region` | 90 | 4 | true | blocked, source not TFT |
| `nbeatsx_silver_v0` / `strict_failure_captured` | 18 | 1 | true | blocked, source not TFT |
| `nbeatsx_silver_v0` / `high_spread_volatility` | 18 | 1 | true | blocked, source not TFT |

The production gate check still fails, which is the intended conservative
state. The coverage audit remains `coverage_gap` for the configured 180-anchor
target: five tenants have 104 eligible anchors each, and `2026-03-29 23:00`
remains an unrecoverable price-and-weather gap in the current observed feature
frame.
