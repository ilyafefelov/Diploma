# DFL Residual + Offline DT Research Challenger

Date: 2026-05-10

This slice is the fastest clean path from the stabilized AFL/DFL foundation to a
real Decision-Transformer-shaped research challenger. It stays thesis-safe:
`strict_similar_day` remains the frozen Level 1 control and fallback, and every
new result is research evidence only.

Claim boundary: not full DFL, not deployed Decision Transformer control, not a
promoted strategy, and not market execution.

## Scope

| Field | Value |
|---|---|
| Tenants | Five canonical tenants from `simulations/tenants.yml` |
| Market | Ukrainian OREE DAM |
| Context | Open-Meteo/weather and configured load context already in the DFL panel |
| Panel ceiling | Current verified 104-anchor panel |
| Final holdout | Latest 18 anchors per tenant/source model |
| Source models | `tft_silver_v0`, `nbeatsx_silver_v0` |
| Control/fallback | `strict_similar_day` |

Run config:
[real_data_dfl_residual_dt_challenger_week3.yaml](../../configs/real_data_dfl_residual_dt_challenger_week3.yaml).

## Assets

| Asset | Purpose |
|---|---|
| `dfl_real_data_trajectory_dataset_frame` | Expands feasible LP-scored schedule candidates into step-level trajectories with state, action, reward, return-to-go, split, provenance, and claim flags. |
| `dfl_residual_schedule_value_model_frame` | Selects a tiny residual schedule/value rule using only train/inner anchors. |
| `dfl_residual_schedule_value_strict_lp_benchmark_frame` | Strict LP/oracle evidence for residual-selected schedules on the final holdout. |
| `dfl_offline_dt_candidate_frame` | Reuses the existing DT policy primitive as a tiny offline research candidate over high-value train trajectories and compares it to filtered behavior cloning. |
| `dfl_offline_dt_candidate_strict_lp_benchmark_frame` | Strict LP/oracle evidence for offline DT and filtered behavior cloning. |
| `dfl_residual_dt_fallback_strict_lp_benchmark_frame` | Strict-default fallback wrapper: use `strict_similar_day` unless prior-only confidence permits residual or DT. |

New check:

- `dfl_residual_dt_fallback_strict_lp_benchmark_frame:dfl_residual_dt_fallback_evidence`
  validates structural evidence and reports the promotion decision as metadata.

## Training And Selection Rules

- Teacher labels are allowed only for train/inner anchors.
- Final-holdout actuals may affect scoring labels only, never selected weights,
  families, or DT checkpoints.
- The residual selector chooses a candidate schedule family from prior/train
  regret summaries.
- The offline DT candidate is deliberately tiny: context length `24`, hidden
  dimension `32`, one layer, two heads, max five epochs, fixed seed.
- The DT path must be compared against filtered behavior cloning because DT is
  not assumed to beat simpler imitation by default.

## Fallback Gate

The fallback starts with `strict_similar_day`. It only permits a challenger when
prior-only evidence shows:

- at least 5% inner/train mean-regret advantage versus strict;
- median regret is not worse than strict;
- no OOD regime flag;
- zero safety violations and thesis-grade observed coverage.

Final promotion remains blocked unless strict LP/oracle scoring beats
`strict_similar_day` by at least 5% mean regret and does not worsen median
regret.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_panel_experiment_frame,offline_dfl_decision_target_panel_frame,offline_dfl_decision_target_strict_lp_benchmark_frame,offline_dfl_action_target_panel_frame,offline_dfl_action_target_strict_lp_benchmark_frame,dfl_schedule_candidate_library_frame,dfl_schedule_candidate_library_v2_frame,dfl_strict_failure_selector_robustness_frame,tenant_consumption_schedule_bronze,tenant_historical_net_load_silver,dfl_strict_failure_prior_feature_panel_frame,dfl_real_data_trajectory_dataset_frame,dfl_residual_schedule_value_model_frame,dfl_residual_schedule_value_strict_lp_benchmark_frame,dfl_offline_dt_candidate_frame,dfl_offline_dt_candidate_strict_lp_benchmark_frame,dfl_residual_dt_fallback_strict_lp_benchmark_frame -c configs/real_data_dfl_residual_dt_challenger_week3.yaml
```

Local export slug after materialization:

`week3_dfl_residual_dt_research_challenger`

## Materialized Evidence

Latest checked run:

| Field | Value |
|---|---|
| Dagster run id | `54891d01-d57e-49a6-8191-9f3ea0afc425` |
| Materialized assets | trajectory dataset, residual model, residual strict benchmark, offline DT candidate, offline DT strict benchmark, fallback strict benchmark |
| Asset check | `dfl_residual_dt_fallback_evidence` passed structurally |
| Postgres cleanup | Removed 1,710 stale malformed rows from the first pre-fix persistence attempt; malformed residual/DT evaluation IDs now count `0` |

Latest Postgres evidence rows:

| Strategy kind | Rows | Tenants | Models | Anchors |
|---|---:|---:|---:|---:|
| `dfl_residual_schedule_value_strict_lp_benchmark` | 540 | 5 | 5 | 18 |
| `dfl_offline_dt_candidate_strict_lp_benchmark` | 540 | 5 | 5 | 18 |
| `dfl_residual_dt_fallback_strict_lp_benchmark` | 900 | 5 | 9 | 18 |

Fallback strict-LP/oracle result by source model:

| Source model | Role | Rows | Mean regret UAH | Median regret UAH |
|---|---|---:|---:|---:|
| `nbeatsx_silver_v0` | `strict_reference` | 90 | 314.81 | 202.61 |
| `nbeatsx_silver_v0` | `fallback_strategy` | 90 | 318.37 | 172.49 |
| `tft_silver_v0` | `strict_reference` | 90 | 314.81 | 202.61 |
| `tft_silver_v0` | `fallback_strategy` | 90 | 258.12 | 136.05 |

Interpretation:

- TFT-source fallback is a useful research-challenger signal: it improves mean
  regret versus `strict_similar_day` on this final holdout while also improving
  median regret.
- NBEATSx-source fallback remains blocked: mean regret is worse than
  `strict_similar_day`, even though median regret improves.
- The aggregate promotion decision remains blocked. This is intentionally
  conservative because the configured challenger path must not silently promote
  one source while another fails the strict control gate.
- The old full-upstream materialization attempt timed out; the successful run
  used existing upstream candidate-library artifacts and rematerialized only the
  residual/DT assets listed above.

## Interpretation

This path is the first full-stack research challenger that includes a real-data
trajectory dataset, a residual schedule/value selector, an offline DT-shaped
candidate, behavior cloning, and a strict fallback wrapper. A failed promotion
is still useful evidence: it means full DFL/DT was tested under the same frozen
control protocol and the strict comparator remains stronger.

## Source-Specific Follow-Up

The follow-up source-specific gate is tracked in
[DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER.md](DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER.md).
It separates the TFT and NBEATSx fallback evidence instead of collapsing both
sources into one aggregate promotion decision.

Latest result:

- Run `be22b25b-a1c5-40d9-9049-a01efb8e7e5f` materialized
  `dfl_source_specific_research_challenger_frame`.
- `dfl_source_specific_research_challenger_evidence` passed.
- TFT has a latest-holdout source signal: 258.12 UAH mean regret versus
  314.81 UAH for `strict_similar_day`, with better median regret.
- NBEATSx remains blocked: 318.37 UAH mean regret versus 314.81 UAH for
  `strict_similar_day`.
- Neither source is robust across rolling strict-control windows in this
  combined gate, so production promotion remains blocked.
