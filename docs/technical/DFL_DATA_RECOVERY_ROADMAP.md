# DFL Data Recovery Roadmap

Date: 2026-05-08

This roadmap follows the blocked action-classifier result. The next source of
improvement should be data coverage and trajectory/value evidence, not another
static per-hour classifier tweak.

## UA-First Recovery

Current checked Ukrainian panel:

| Evidence item | Current state |
|---|---:|
| Canonical tenants | 5 |
| Source models | 2 |
| Eligible anchors per tenant/model | 104 |
| Train-selection rows | 860 |
| Final-holdout rows | 180 |
| Final-holdout anchors per tenant/model | 18 |
| Persisted action-label rows | 1,040 |
| Claim flags | `not_full_dfl=true`, `not_market_execution=true` |

Next audit target:

- determine whether OREE/Open-Meteo local/source history can support more than
  104 eligible anchors per tenant;
- if yes, backfill toward 180-365 anchors per tenant with the same
  rolling-origin/no-leakage split discipline;
- if no, document the true ceiling and move to richer features plus
  trajectory/value learning.

Latest ceiling check:

- Config: [real_data_dfl_trajectory_value_week3.yaml](../../configs/real_data_dfl_trajectory_value_week3.yaml).
- Target: 120 eligible anchors per tenant.
- Current result: 104 eligible anchors per tenant for all five canonical tenants.
- Eligible window: `2026-01-08 23:00` through `2026-04-29 23:00`.
- Current result does not meet the 120-anchor target.
- One price gap and one weather gap remain visible per tenant in the current
  feature frame.

The audit must keep tenant-specific anchor eligibility, price/weather gap
counts, latest-batch freshness, and observed/thesis-grade coverage visible in
Dagster metadata.

## European Dataset Bridge

European data is useful now for source mapping and future external validation,
but not for Ukrainian DFL training yet.

| Source | Status | Metadata / reason |
|---|---|---|
| [RunyaoYu/PriceFM](https://huggingface.co/datasets/RunyaoYu/PriceFM) | include/watch | Hugging Face Dataset Viewer works; 140,257 rows; 15-minute timestamps; European price/load/generation columns; future external validation only. |
| [PriceFM paper](https://huggingface.co/papers/2508.04875) | include/watch | European cross-region electricity-price foundation-model direction. |
| [lipiecki/thief](https://huggingface.co/datasets/lipiecki/thief) | watch | Paper is relevant, but Dataset Viewer is currently unavailable. |
| [THieF paper](https://huggingface.co/papers/2508.11372) | watch | Future temporal hierarchy/block-product forecasting idea. |
| [ENTSO-E Transparency Platform](https://www.entsoe.eu/data/transparency-platform/) | watch | Future market-coupling and external validation context. |
| [Open Power System Data time series](https://data.open-power-system-data.org/time_series/) | watch | Future hourly European price/load/renewables context. |
| [Ember API](https://ember-energy.org/data/api/) | watch | Future generation, demand, emissions, and carbon-intensity context. |
| [Nord Pool Data Portal](https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/) | watch/restricted | Commercial/API-gated source; reference only unless access is approved. |

European rows remain `training_use_allowed=false` until these blockers are
resolved:

- currency and UAH/EUR normalization;
- timezone and DST alignment;
- Ukrainian DAM versus European market-rule differences;
- price caps and market-coupling semantics;
- licensing/API access;
- domain-shift validation against Ukrainian OREE evidence.

## Trajectory/Value Learner Direction

The next modeling slice should learn over feasible schedules rather than raw
hourly labels.

Candidate schedule sources:

- frozen `strict_similar_day` LP schedule;
- raw `tft_silver_v0` and `nbeatsx_silver_v0` LP schedules;
- calibrated forecast schedules;
- simple price-perturbation schedules that preserve LP feasibility;
- later, differentiable relaxed-LP candidates if the data gate improves.

Training target:

- select or rank feasible schedules using prior anchors only;
- optimize realized strict LP/oracle regret, net value, and safety;
- keep final-holdout scoring unchanged.

Promotion remains blocked unless the candidate beats `strict_similar_day` under
the conservative strict LP/oracle gate.

Latest trajectory/value selector result:

- New assets: `dfl_trajectory_value_candidate_panel_frame`,
  `dfl_trajectory_value_selector_frame`, and
  `dfl_trajectory_value_selector_strict_lp_benchmark_frame`.
- Candidate panel: 900 final-holdout rows across 5 tenants, 2 source models,
  18 anchors, and 5 schedule families.
- Selector evidence: 90 tenant-anchor rows per source model.
- NBEATSx selector mean regret: 603.29 UAH, improving 25.83% versus raw
  NBEATSx but still losing to 314.81 UAH strict-control regret.
- TFT selector mean regret: 619.78 UAH, improving 38.24% versus raw TFT but
  still losing to 314.81 UAH strict-control regret.
- Decision: development diagnostic is useful; production promotion remains
  blocked.
- Tracked note:
  [DFL_TRAJECTORY_VALUE_SELECTOR.md](DFL_TRAJECTORY_VALUE_SELECTOR.md).

Latest trajectory feature-ranker result:

- New assets: `dfl_schedule_candidate_library_frame`,
  `dfl_trajectory_feature_ranker_frame`, and
  `dfl_trajectory_feature_ranker_strict_lp_benchmark_frame`.
- Config:
  [real_data_dfl_trajectory_ranker_week3.yaml](../../configs/real_data_dfl_trajectory_ranker_week3.yaml).
- Dagster run id: `db2f6e2d-ae39-49fe-86f0-0e594af29a1e`.
- Schedule library: 6,780 feasible LP-scored schedule rows.
- Ranker selection rows: 10, one per tenant/source model.
- Strict benchmark: 540 rows, with 90 final-holdout tenant-anchors per source
  model.
- NBEATSx ranker mean regret: 497.30 UAH, improving 38.86% versus raw NBEATSx
  but still losing to 314.81 UAH strict-control regret.
- TFT ranker mean regret: 607.96 UAH, improving 39.42% versus raw TFT but still
  losing to 314.81 UAH strict-control regret.
- Decision: stronger development diagnostic than the family selector; production
  promotion remains blocked by the frozen `strict_similar_day` control.
- Tracked note:
  [DFL_TRAJECTORY_FEATURE_RANKER.md](DFL_TRAJECTORY_FEATURE_RANKER.md).

Latest strict-challenger diagnostic implementation:

- New helper: `smart_arbitrage.dfl.strict_challenger`.
- New assets: `dfl_pipeline_integrity_audit_frame`,
  `dfl_schedule_candidate_library_v2_frame`,
  `dfl_non_strict_oracle_upper_bound_frame`, and
  `dfl_strict_baseline_autopsy_frame`.
- New asset check: `dfl_non_strict_oracle_upper_bound_evidence`.
- Config:
  [real_data_dfl_strict_challenger_week3.yaml](../../configs/real_data_dfl_strict_challenger_week3.yaml).
- Purpose: separate candidate-set adequacy from selector learnability. The
  non-strict oracle upper bound answers whether any available non-strict
  feasible schedule can beat `strict_similar_day` on final holdout.
- Claim boundary: research-only, not full DFL, not Decision Transformer control,
  and not market execution.
- Latest run id: `48b9c0b4-9d12-4237-a436-549424956ac1`.
- Result: the non-strict upper bound produced 180 final-holdout tenant/source
  anchors and beat strict control on 146 rows. Best non-strict mean regret was
  185.74 UAH versus 314.81 UAH for `strict_similar_day`.
- Autopsy: 46 strict high-regret rows and 146 rows recommending a prior-only
  strict-failure selector; zero rows recommended data/candidate expansion first.
- Decision rule: if the non-strict upper bound loses, expand UA data coverage or
  candidate generation before training another selector. If it wins on meaningful
  slices, train a prior-only strict-failure selector.
- Tracked note:
  [DFL_STRICT_CHALLENGER_DIAGNOSTICS.md](DFL_STRICT_CHALLENGER_DIAGNOSTICS.md).

Latest strict-failure selector implementation:

- New helper: `smart_arbitrage.dfl.strict_failure_selector`.
- New assets: `dfl_strict_failure_selector_frame` and
  `dfl_strict_failure_selector_strict_lp_benchmark_frame`.
- New asset check: `dfl_strict_failure_selector_evidence`.
- Config:
  [real_data_dfl_strict_failure_selector_week3.yaml](../../configs/real_data_dfl_strict_failure_selector_week3.yaml).
- Purpose: test whether prior-only regret evidence can identify when to replace
  `strict_similar_day` with the best prior non-strict schedule family.
- Selection rule: choose a switch threshold on train-selection anchors only;
  final-holdout actuals affect strict scoring only.
- Claim boundary: research-only, not full DFL, not Decision Transformer
  control, and not market execution.
- Latest run id: `568a8a8d-c210-44d0-9842-08300dfe0781`.
- Asset check: `dfl_strict_failure_selector_evidence` passed.
- Strict benchmark: 720 rows, with 90 selector final-holdout tenant-anchors per
  source model.
- Result: `dfl_strict_failure_selector_v1_tft_silver_v0` reached 267.79 UAH
  mean regret and 149.01 UAH median regret, improving 73.32% versus raw TFT and
  14.94% versus `strict_similar_day`.
- Result: `dfl_strict_failure_selector_v1_nbeatsx_silver_v0` reached 299.73
  UAH mean regret and 182.76 UAH median regret, improving 63.15% versus raw
  NBEATSx and 4.79% versus `strict_similar_day`.
- Decision: development evidence passes. TFT-source selector passes the
  per-source strict threshold, while the overall multi-source gate remains
  conservatively labeled `diagnostic_pass_production_blocked` because NBEATSx is
  just below the 5% strict-improvement threshold.
- Tracked note:
  [DFL_STRICT_FAILURE_SELECTOR.md](DFL_STRICT_FAILURE_SELECTOR.md).

Strict-failure robustness gate:

- New asset: `dfl_strict_failure_selector_robustness_frame`.
- New asset check: `dfl_strict_failure_selector_robustness_evidence`.
- Config:
  [real_data_dfl_strict_failure_selector_robustness_week3.yaml](../../configs/real_data_dfl_strict_failure_selector_robustness_week3.yaml).
- Purpose: replay the strict-failure selector across four latest-first
  18-anchor validation windows, each with at least 30 prior anchors before the
  validation start.
- Selection rule: thresholds are learned from anchors strictly before the
  validation window; validation actuals affect scoring only.
- Claim boundary: even if the TFT-source selector passes repeated windows, the
  claim is `robust_research_challenger`, not production control.
- Latest run id: `fd21fada-f453-404b-96a1-27d99b14b1a1`.
- Asset check: `dfl_strict_failure_selector_robustness_evidence` passed.
- Result: all eight source/window rows improve over the raw neural schedule, so
  development evidence is stable.
- Result: no source is a robust strict-control challenger yet. TFT passes the
  strict threshold only in the latest window, then loses or ties strict control
  in earlier windows.
- Decision: production promotion remains blocked; the next work should improve
  prior-window features or extend Ukrainian observed coverage before claiming a
  robust selector.
- Tracked note:
  [DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS.md](DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS.md).

Strict-failure prior feature audit:

- New historical context asset: `tenant_historical_net_load_silver`.
- New audit assets: `dfl_strict_failure_prior_feature_panel_frame` and
  `dfl_strict_failure_feature_audit_frame`.
- New asset check: `dfl_strict_failure_feature_audit_evidence`.
- Config:
  [real_data_dfl_strict_failure_feature_audit_week3.yaml](../../configs/real_data_dfl_strict_failure_feature_audit_week3.yaml).
- Latest run id: `b9a48061-079f-4a92-9daf-699398f67906`.
- Historical load proxy: 14,395 rows, five tenants, `2026-01-01 00:00` through
  `2026-04-30 23:00` UTC.
- Feature panel: 720 prior-window rows.
- Audit panel: 40 tenant/source/window rows.
- Cluster result: 30 `strict_stable_region`, 6 `high_spread_volatility`, and
  4 `strict_failure_captured` rows.
- Interpretation: the current selector can rescue raw neural schedules, but it
  needs feature-aware regime gating before it can robustly challenge
  `strict_similar_day`.
- Tracked note:
  [DFL_STRICT_FAILURE_FEATURE_AUDIT.md](DFL_STRICT_FAILURE_FEATURE_AUDIT.md).

## Acceptance For Next Slice

The next slice is ready when:

- the UA coverage audit states whether 180-365 anchors per tenant are possible;
- external sources are registered but not mixed into training;
- a trajectory/value dataset contract is defined from existing feasible
  schedule rows;
- tests prove final holdout does not influence feature selection, schedule
  generation, or model selection;
- the strict LP/oracle gate remains the only promotion authority.
