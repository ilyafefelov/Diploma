# DFL Strict Challenger Diagnostics

Date: 2026-05-08

This slice asks a stricter question than the previous trajectory ranker:

> Does the non-strict candidate library contain any feasible schedule that could
> beat the frozen `strict_similar_day` control on the final holdout?

The answer matters before training another selector. If the best available
non-strict schedule still loses to `strict_similar_day`, the bottleneck is data
coverage, candidate generation, or feature context. If the non-strict upper
bound wins on enough anchors, the next useful model is a selector that learns
when to distrust strict control.

## Assets

| Asset | Purpose |
|---|---|
| `dfl_pipeline_integrity_audit_frame` | Checks temporal horizon boundaries and keeps actual-derived schedule diagnostics out of ranker inputs. |
| `dfl_schedule_candidate_library_v2_frame` | Adds deterministic strict/raw blends and prior-only strict residual candidates to the feasible schedule library. |
| `dfl_non_strict_oracle_upper_bound_frame` | Selects the best final-holdout non-strict candidate per tenant/source/anchor as a theoretical upper bound. |
| `dfl_strict_baseline_autopsy_frame` | Marks high-regret strict-control anchors and whether a non-strict candidate could have improved them. |

Config:
[real_data_dfl_strict_challenger_week3.yaml](../../configs/real_data_dfl_strict_challenger_week3.yaml).

## Why This Is The Right Next Diagnostic

The current feature ranker improved raw neural schedules by roughly 39%, but it
still lost to `strict_similar_day`. It also selected the strict-control family
often, which means the selector was acting conservatively rather than finding a
better non-strict controller.

This slice separates two questions:

- **Candidate-set adequacy:** does any non-strict feasible schedule beat strict?
- **Selector learnability:** can a prior-only model identify those anchors
  without looking at final-holdout actuals?

Only the first question is implemented here.

## Research Boundary

- Decision-focused learning and SPO/SPO+ motivate optimizing downstream
  decision loss rather than forecast-only or action-classification proxies.
- Energy-storage arbitrage is multistage and SOC-path dependent, so hourly
  labels are insufficient.
- TSFM leakage work motivates explicit temporal evaluation and latest-batch
  checks.

This slice remains:

- Ukrainian OREE/Open-Meteo only;
- strict LP/oracle scored;
- `not_full_dfl=true`;
- `not_market_execution=true`;
- not a Decision Transformer controller.

## Expected Evidence

The materialization should report:

- market anchor count versus tenant-anchor count;
- no leaky horizon rows;
- zero overlap between ranker selection features and actual-derived diagnostics;
- v2 candidate-family counts;
- non-strict upper-bound mean regret versus strict;
- high-regret strict anchors where a non-strict schedule could have helped.

## Latest Materialized Evidence

Run:

- Dagster run id: `48b9c0b4-9d12-4237-a436-549424956ac1`.
- Scope: downstream-only materialization from the existing checked 104-anchor
  upstream benchmark and candidate library.
- Asset check: `dfl_non_strict_oracle_upper_bound_evidence` passed.

Pipeline integrity audit:

| Metric | Value |
|---|---:|
| Benchmark rows | 1,560 |
| Existing schedule-candidate rows | 6,780 |
| Market anchors | 104 |
| Tenant anchors | 520 |
| Source models | 2 |
| Leaky horizon rows | 0 |
| Ranker feature overlap with actual-derived diagnostics | 0 |

Candidate library v2:

| Candidate family | Rows |
|---|---:|
| `strict_control` | 1,040 |
| `raw_source` | 1,040 |
| `forecast_perturbation` | 4,160 |
| `panel_v2` | 180 |
| `decision_target_v3` | 180 |
| `action_target_v4` | 180 |
| `strict_raw_blend_v2` | 3,120 |
| `strict_prior_residual_v2` | 1,010 |

Non-strict oracle upper bound:

| Metric | Value |
|---|---:|
| Final-holdout tenant/source/anchor rows | 180 |
| Tenants | 5 |
| Source models | 2 |
| Best non-strict mean regret | 185.74 UAH |
| Strict-control mean regret | 314.81 UAH |
| Non-strict rows beating strict | 146 / 180 |

Selected upper-bound families:

| Family | Rows |
|---|---:|
| `strict_raw_blend_v2` | 117 |
| `strict_prior_residual_v2` | 26 |
| `forecast_perturbation` | 12 |
| `action_target_v4` | 12 |
| `decision_target_v3` | 8 |
| `panel_v2` | 4 |
| `raw_source` | 1 |

Autopsy result:

| Metric | Value |
|---|---:|
| Rows | 180 |
| Strict high-regret rows | 46 |
| Rows recommending strict-failure selector training | 146 |
| Rows recommending data/candidate expansion first | 0 |

Interpretation: the current candidate library is not the blocker. The blocker is
selector learnability under no-leakage constraints. The next slice should train
a prior-only strict-failure selector to identify when non-strict schedules should
replace `strict_similar_day` on final holdout.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_panel_experiment_frame,offline_dfl_panel_strict_lp_benchmark_frame,offline_dfl_decision_target_panel_frame,offline_dfl_decision_target_strict_lp_benchmark_frame,offline_dfl_action_target_panel_frame,offline_dfl_action_target_strict_lp_benchmark_frame,dfl_trajectory_value_candidate_panel_frame,dfl_schedule_candidate_library_frame,dfl_trajectory_feature_ranker_frame,dfl_trajectory_feature_ranker_strict_lp_benchmark_frame,dfl_pipeline_integrity_audit_frame,dfl_schedule_candidate_library_v2_frame,dfl_non_strict_oracle_upper_bound_frame,dfl_strict_baseline_autopsy_frame -c configs/real_data_dfl_strict_challenger_week3.yaml
```

Local export slug:

```text
week3_dfl_strict_challenger_diagnostics
```

Generated `data/` outputs should stay local unless a concise summary is
intentionally selected for the report.

## Decision Rule

- If the non-strict oracle upper bound cannot beat strict, do not train another
  selector yet. Expand UA data or candidate generation first.
- If the upper bound beats strict on meaningful slices, train a prior-only
  strict-failure selector over those slices.
- Production promotion remains blocked unless the selected non-strict strategy
  beats `strict_similar_day` under the conservative strict LP/oracle gate.
