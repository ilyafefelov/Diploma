# DFL Trajectory Feature Ranker v1

Date: 2026-05-08

This slice expands the DFL-lite trajectory evidence from a family selector into
a richer feasible schedule library plus a prior-only feature ranker. It remains
research evidence only: not full DFL, not Decision Transformer control, and not
market execution.

## Scope

- Ukrainian source of truth: observed OREE DAM plus tenant Open-Meteo context.
- Config:
  [real_data_dfl_trajectory_ranker_week3.yaml](../../configs/real_data_dfl_trajectory_ranker_week3.yaml).
- Candidate library asset: `dfl_schedule_candidate_library_frame`.
- Ranker asset: `dfl_trajectory_feature_ranker_frame`.
- Strict evidence asset: `dfl_trajectory_feature_ranker_strict_lp_benchmark_frame`.
- Strategy kind: `dfl_trajectory_feature_ranker_strict_lp_benchmark`.
- Final holdout: latest 18 anchors per tenant/source model, `2026-04-12 23:00`
  through `2026-04-29 23:00`.

The full upstream rematerialization target was attempted first but exceeded the
local 30-minute command timeout. The downstream ranker assets were then
materialized successfully from the existing checked `104`-anchor upstream
benchmark and trajectory/value candidate assets.

## Candidate Library

The schedule library stores feasible LP-scored schedule rows with forecast,
actual, dispatch, SOC, value, regret, throughput, degradation proxy, feature,
split, provenance, and claim-boundary fields.

Materialized row count: `6,780`.

| Source model | Candidate family | Split | Rows |
|---|---|---|---:|
| each source model | `strict_control` | train-selection | 430 |
| each source model | `strict_control` | final-holdout | 90 |
| each source model | `raw_source` | train-selection | 430 |
| each source model | `raw_source` | final-holdout | 90 |
| each source model | `forecast_perturbation` | train-selection | 1,720 |
| each source model | `forecast_perturbation` | final-holdout | 360 |
| each source model | `panel_v2` | final-holdout | 90 |
| each source model | `decision_target_v3` | final-holdout | 90 |
| each source model | `action_target_v4` | final-holdout | 90 |

Perturbations are deterministic forecast transforms only. They do not mutate
actual prices or final-holdout selection metadata.

## Ranker Rule

Ranker v1 grid-searches a small set of linear feature-scoring profiles on
`train_selection` anchors only. The selected profile is then applied to the
final holdout. Features include forecast spread, throughput, degradation cost,
SOC slack, and prior family regret. Final-holdout actuals affect only strict
scoring, never weight/profile selection.

Selection rows: `10` =
5 tenants x 2 source models.

Selected profile distribution:

| Source model | Weight profile | Tenants |
|---|---|---:|
| `nbeatsx_silver_v0` | `prior_regret_only` | 4 |
| `nbeatsx_silver_v0` | `prior_spread_value` | 1 |
| `tft_silver_v0` | `prior_regret_only` | 5 |

Selected final-holdout family counts:

| Source model | Selected family | Rows |
|---|---|---:|
| `nbeatsx_silver_v0` | `strict_control` | 64 |
| `nbeatsx_silver_v0` | `decision_target_v3` | 18 |
| `nbeatsx_silver_v0` | `action_target_v4` | 8 |
| `tft_silver_v0` | `strict_control` | 54 |
| `tft_silver_v0` | `action_target_v4` | 36 |

## Strict LP/Oracle Evidence

Dagster run id: `db2f6e2d-ae39-49fe-86f0-0e594af29a1e`.

Export slug:
`data/research_runs/week3_dfl_trajectory_feature_ranker_v1`.

Strict benchmark rows: `540`.

Claim checks:

| Check | Result |
|---|---|
| Tenants | 5 |
| Source models | 2 |
| Final-holdout tenant-anchors | 90 |
| Rows | 540 |
| `data_quality_tier` | `thesis_grade` |
| `not_full_dfl` | true |
| `not_market_execution` | true |

Latest strict batch:

| Model | Rows | Tenants | Tenant-anchors | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|---:|---:|
| `strict_similar_day` | 180 | 5 | 90 | 314.81 | 202.61 |
| `nbeatsx_silver_v0` | 90 | 5 | 90 | 813.40 | 520.48 |
| `dfl_trajectory_feature_ranker_v1_nbeatsx_silver_v0` | 90 | 5 | 90 | 497.30 | 238.15 |
| `tft_silver_v0` | 90 | 5 | 90 | 1003.54 | 477.99 |
| `dfl_trajectory_feature_ranker_v1_tft_silver_v0` | 90 | 5 | 90 | 607.96 | 218.72 |

Development diagnostic:

- NBEATSx ranker improves mean regret versus raw NBEATSx by `38.86%`.
- TFT ranker improves mean regret versus raw TFT by `39.42%`.

Production promotion remains blocked:

- `strict_similar_day` still has the best mean and median regret.
- The ranker does not beat strict control by the required 5% mean-regret
  margin.
- The ranker medians remain worse than strict control.

This is useful evidence: schedule-feature selection is materially better than
raw neural schedules, but still not good enough to replace the frozen Level 1
control comparator.

## Research Source Boundary

The source registry stays research-only for external datasets.

- [TSFM leakage evaluation](https://huggingface.co/papers/2510.13654) supports
  strict latest-batch, no-leakage, temporal evaluation.
- [DFL survey](https://huggingface.co/papers/2307.13565), ESS DFL arbitrage,
  SPO/SPO+, and perturbed DFL storage sources motivate optimizing downstream
  decision value rather than forecast or classification proxies.
- [RunyaoYu/PriceFM](https://huggingface.co/datasets/RunyaoYu/PriceFM) remains
  include/watch for future European external validation. The Hugging Face
  Dataset Viewer is valid, with `140,257` rows, `191` columns, one train split,
  15-minute UTC timestamps, and European price/load/generation/solar/wind
  columns.
- [lipiecki/thief](https://huggingface.co/datasets/lipiecki/thief) remains
  watch-only because Dataset Viewer currently reports unavailable.

European rows remain `training_use_allowed=false` until currency, timezone,
market-rule, price-cap, API/licensing, and domain-shift blockers are resolved.

## Next Work

The next slice should keep this as a selector diagnostic and move toward one of
two routes:

- recover more Ukrainian observed anchors if OREE/Open-Meteo source history can
  support it;
- train a tiny trajectory/value model over the schedule library with the same
  prior-only split and strict LP/oracle final-holdout gate.
