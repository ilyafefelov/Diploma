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

## Interpretation

This path is the first full-stack research challenger that includes a real-data
trajectory dataset, a residual schedule/value selector, an offline DT-shaped
candidate, behavior cloning, and a strict fallback wrapper. A failed promotion
is still useful evidence: it means full DFL/DT was tested under the same frozen
control protocol and the strict comparator remains stronger.
