# DFL Trajectory/Value Selector v1

Date: 2026-05-08

This slice moves past static hourly action classifiers. It builds a prior-only
selector over feasible schedules that were already scored through the strict
LP/oracle path. The selector is research evidence only: it is not full DFL, not
Decision Transformer control, and not market execution.

## Scope

- Ukrainian source of truth: observed OREE DAM plus tenant Open-Meteo features.
- Config: [real_data_dfl_trajectory_value_week3.yaml](../../configs/real_data_dfl_trajectory_value_week3.yaml).
- Candidate sources: `strict_similar_day`, raw `tft_silver_v0`, raw
  `nbeatsx_silver_v0`, panel v2, decision-target v3, and action-target v4.
- Selector rule: per tenant/source model, choose the schedule family with the
  lowest prior/train-selection regret. Final-holdout rows are never used for
  family selection.
- Final holdout: latest 18 anchors per tenant, `2026-04-12 23:00` through
  `2026-04-29 23:00`.

## UA Coverage Ceiling

The refreshed coverage audit was run with a 120-anchor target against the
current materialized Ukrainian feature/benchmark frames.

| Tenant count | Eligible anchors per tenant | Target anchors | Meets 120-anchor target | First eligible anchor | Last eligible anchor |
|---:|---:|---:|---|---|---|
| 5 | 104 | 120 | no | `2026-01-08 23:00` | `2026-04-29 23:00` |

Each tenant currently reports one price gap and one weather gap in the wider
feature frame. The true current ceiling is therefore 104 eligible anchors per
tenant for this observed window, not 120/180/365. A full upstream
re-materialization at `max_anchors=120` exceeded the local command timeout; the
selector evidence below uses the existing checked 104-anchor benchmark/panel
state and a 90 tenant-anchor final holdout.

## Latest Evidence

Materialized assets:

- `dfl_data_coverage_audit_frame`
- `dfl_trajectory_value_candidate_panel_frame`
- `dfl_trajectory_value_selector_frame`
- `dfl_trajectory_value_selector_strict_lp_benchmark_frame`

Export slug:
`data/research_runs/week3_dfl_trajectory_value_selector_v1`

The candidate panel contains 900 final-holdout schedule rows:
5 tenants x 2 source models x 18 final anchors x 5 schedule families.

| Source model | Family | Rows | Mean regret UAH | Median regret UAH | Prior selection score |
|---|---|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | `strict_control` | 90 | 314.81 | 202.61 | 895.77 |
| `nbeatsx_silver_v0` | `raw_source` | 90 | 813.40 | 520.48 | 1154.50 |
| `nbeatsx_silver_v0` | `panel_v2` | 90 | 816.62 | 490.54 | 2027.36 |
| `nbeatsx_silver_v0` | `decision_target_v3` | 90 | 814.17 | 461.35 | 801.96 |
| `nbeatsx_silver_v0` | `action_target_v4` | 90 | 851.99 | 530.35 | 791.13 |
| `tft_silver_v0` | `strict_control` | 90 | 314.81 | 202.61 | 895.77 |
| `tft_silver_v0` | `raw_source` | 90 | 1003.54 | 477.99 | 1360.01 |
| `tft_silver_v0` | `panel_v2` | 90 | 989.55 | 435.16 | 1038.69 |
| `tft_silver_v0` | `decision_target_v3` | 90 | 1015.36 | 477.99 | 732.21 |
| `tft_silver_v0` | `action_target_v4` | 90 | 959.84 | 479.04 | 606.87 |

Latest strict selector batch:

| Model | Tenant-anchor rows | Mean regret UAH | Median regret UAH | Finding |
|---|---:|---:|---:|---|
| `strict_similar_day` | 90 reference anchors per source model | 314.81 | 202.61 | Frozen Level 1 control still wins. |
| `nbeatsx_silver_v0` | 90 | 813.40 | 520.48 | Raw neural comparator. |
| `dfl_trajectory_value_selector_v1_nbeatsx_silver_v0` | 90 | 603.29 | 274.05 | Improves vs raw by 25.83%, blocked vs strict. |
| `tft_silver_v0` | 90 | 1003.54 | 477.99 | Raw neural comparator. |
| `dfl_trajectory_value_selector_v1_tft_silver_v0` | 90 | 619.78 | 229.19 | Improves vs raw by 38.24%, blocked vs strict. |

Selected family distribution:

| Source model | Selected family | Tenants |
|---|---|---:|
| `nbeatsx_silver_v0` | `strict_control` | 3 |
| `nbeatsx_silver_v0` | `action_target_v4` | 1 |
| `nbeatsx_silver_v0` | `decision_target_v3` | 1 |
| `tft_silver_v0` | `action_target_v4` | 3 |
| `tft_silver_v0` | `strict_control` | 2 |

## Decision

The development diagnostic is useful: the selector improves final-holdout mean
regret versus both raw neural schedules.

Production promotion remains blocked:

- `strict_similar_day` still has much lower mean and median regret.
- Selector candidates do not beat strict control by the required 5% mean-regret
  margin.
- Selector medians remain worse than strict control.
- Claim flags stay `not_full_dfl=true` and `not_market_execution=true`.

## Next Work

The next technical slice should not tune another hourly classifier. The
evidence points to two higher-value directions:

- recover more Ukrainian observed anchors if source history can be backfilled;
- build richer trajectory/value features over feasible schedules, including
  spread, SOC path, opportunity windows, and tenant context, while keeping
  final-holdout selection blocked.
