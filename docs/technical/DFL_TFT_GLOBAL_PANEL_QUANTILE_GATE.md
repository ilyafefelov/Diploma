# TFT Global-Panel Quantile Schedule/Value Gate

Date: 2026-05-18

This slice gives TFT the same kind of decision-value treatment that made the
official global-panel NBEATSx V2+ lane useful. It does not claim raw TFT
forecast superiority and it does not change any dashboard/API default.

Claim boundary: this is Offline Strategy Promotion evidence only. It is not live
market execution, not a deployed Decision Transformer controller, and not an
EU/Poland market-coupling training run. `strict_similar_day` remains the frozen
control/fallback and Ukrainian-only Schedule/Value Learner V2+ remains the
comparator until a TFT or combined lane beats it under the unchanged strict
LP/oracle gate.

## Baseline To Beat

The frozen comparator is Ukrainian-only official global-panel NBEATSx V2+:

| Metric | Value |
|---|---:|
| Calibrated V2+ mean regret | 174.77 UAH |
| Raw V2+ mean regret | 193.36 UAH |
| Improvement vs `strict_similar_day` | 43.73% |
| Rolling robustness | 4 / 4 windows |
| Market execution | false |

Any TFT result must beat the calibrated V2+ mean regret without median
degradation before it can become the new thesis headline. A combined
NBEATSx+TFT candidate lane must satisfy the same rule.

## Why TFT Is Evaluated This Way

Earlier compact TFT evidence showed useful latest-holdout signals, but it failed
rolling robustness. The next fair test is therefore not a harder isolated raw
TFT run. The fair test is:

1. Train one official TFT over the five-tenant panel.
2. Preserve tenant identity in the global panel.
3. Use TFT's p10, p50, and p90 quantile forecasts as separate candidate schedule
   sources.
4. Calibrate horizon/quantile behavior using prior anchors only.
5. Score the resulting schedules through the same strict LP/oracle evaluator as
   NBEATSx V2+.

The target metric remains strict LP/oracle regret, not MAE/RMSE or visual
forecast quality.

## Assets

| Asset | Purpose |
|---|---|
| `tft_official_global_panel_rolling_strict_lp_benchmark_frame` | Trains one official TFT per rolling anchor over the five-tenant panel and strict-scores p10/p50/p90 quantile forecasts. |
| `tft_official_global_panel_horizon_quantile_calibration_frame` | Builds prior-only horizon/quantile calibration rows for TFT quantile sources. |
| `dfl_tft_quantile_schedule_candidate_library_frame` | Converts TFT p10/p50/p90 strict benchmark rows into feasible schedule candidates. |
| `dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame` | Compares frozen NBEATSx V2+ rows against TFT-quantile V2/V2+ rows under the unchanged strict LP/oracle gate. |
| `dfl_tft_augmented_v2_plus_evidence` | Asset check for coverage, claim boundary, and strict comparator discipline. |
| `dfl_tft_combined_v2_plus_strict_lp_benchmark_frame` | Tests whether TFT quantile schedules add value as complementary candidates on top of frozen NBEATSx V2+. |
| `dfl_tft_combined_v2_plus_evidence` | Asset check for combined NBEATSx+TFT coverage, claim boundary, and no-live-execution flags. |

Tracked config:
[real_data_official_global_panel_tft_quantile_schedule_value_week3.yaml](../../configs/real_data_official_global_panel_tft_quantile_schedule_value_week3.yaml).

## Implementation Notes

- `tft_official_v0` stays the existing single-horizon adapter.
- `tft_official_global_panel_v1` is the additive global-panel evidence lane.
- The adapter emits `tft_official_global_panel_p10_v1`,
  `tft_official_global_panel_v1`, and `tft_official_global_panel_p90_v1` source
  rows for schedule/value scoring.
- Host-mode runs can use CUDA when the local PyTorch runtime is available.
  Compose and CPU runs remain deterministic fallbacks. On CUDA, Lightning uses
  deterministic warning mode rather than hard deterministic failure because TFT's
  interpolation backward path does not have a deterministic CUDA implementation.
- TFT runs are bounded with `tft_max_steps` in addition to `tft_max_epochs` so a
  screen run cannot silently become an unbounded overnight job.
- The current horizon quantile calibration path records prior-only horizon
  biases and a conservative quantile spread scale. It is safe for evidence
  routing, but materialized empirical coverage should be reviewed before making
  any stronger calibration claim.

## Materialized Screen: 2026-05-18

The first bounded host-CUDA screen used the latest 18 anchors, five tenants,
`tft_max_steps=8`, and generated timestamp `2026-05-18T14:00:00+00:00`. The
TFT adapter ran on CUDA and persisted 360 strict LP/oracle rows.

| Source | Rows | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|
| `strict_similar_day` | 90 | 310.58 | 198.39 |
| `tft_official_global_panel_p10_v1` | 90 | 3104.44 | 2495.48 |
| `tft_official_global_panel_v1` | 90 | 2703.00 | 2195.14 |
| `tft_official_global_panel_p90_v1` | 90 | 2859.87 | 2473.32 |

The quantile schedule-candidate library materialized 1,620 final-holdout
candidate rows. The augmented V2+ schedule/value gate materialized as blocked
evidence with `tft_gate_blocker=missing_tft_train_rows`, because an 18-anchor
screen contains only final-holdout rows and therefore has no prior/train anchors
from which to select a TFT schedule/value profile. This is the correct no-leakage
behavior. A real TFT-augmented V2+ gate needs a larger run with prior anchors
before the 18-anchor final holdout.

Interpretation: this bounded screen does not support TFT promotion. Raw
global-panel TFT quantiles underperformed both `strict_similar_day` and the
frozen Ukrainian-only NBEATSx V2+ comparator. The next TFT attempt should use a
larger prior+holdout run and richer context, not a claim that this screen is a
failed full TFT schedule/value experiment.

Local packet:
`data/research_runs/week3_tft_quantile_latest18_screen/`.

## Prior+Holdout Screen: 2026-05-18

The second bounded host-CUDA screen used 36 anchors: 18 prior/train-selection
anchors plus the latest 18 final-holdout anchors. It used the same conservative
local GPU settings (`tft_max_steps=8`, batch size 8, hidden size 8) and
materialized the full TFT-augmented V2+ gate.

Raw strict LP/oracle rows across all 36 anchors:

| Source | Rows | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|
| `strict_similar_day` | 180 | 578.94 | 312.76 |
| `tft_official_global_panel_p10_v1` | 180 | 3158.21 | 2717.61 |
| `tft_official_global_panel_v1` | 180 | 2991.32 | 2485.08 |
| `tft_official_global_panel_p90_v1` | 180 | 2997.03 | 2565.38 |

Final-holdout schedule/value gate rows:

| Source | Selection role | Mean regret UAH | Median regret UAH |
|---|---|---:|---:|
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | V2+ | 174.77 | 67.30 |
| `nbeatsx_official_global_panel_v1` | V2+ | 193.36 | 68.89 |
| `tft_official_global_panel_p10_v1` | V2+ | 337.74 | 244.92 |
| `tft_official_global_panel_v1` | V2+ | 337.74 | 244.92 |
| `tft_official_global_panel_p90_v1` | V2+ | 337.74 | 244.92 |
| `strict_similar_day` | strict reference | 310.58 | 198.39 |

The 36-anchor screen closes the missing-train-row caveat from the latest-18
screen. TFT now had prior/train anchors, the gate materialized, and the result
still did not beat frozen Ukrainian-only V2+. The TFT schedule/value selector
improved strongly over raw TFT, but the final TFT V2+ regret remained worse than
both `strict_similar_day` and NBEATSx V2+. This is negative evidence for the
current small global-panel TFT configuration, not a general claim that TFT cannot
work with richer context or longer training.

Local packet:
`data/research_runs/week3_tft_quantile_36_anchor_prior_holdout_screen/`.

## Combined NBEATSx+TFT Complementary Path

The first augmented gate answered whether TFT can stand alone after the same
V2/V2+ schedule-value treatment. A separate combined path now answers the more
specific question: can TFT contribute one or more schedules that improve the
frozen Ukrainian-only NBEATSx V2+ selector?

The combined path keeps the calibrated NBEATSx V2+ schedule as the default
fallback for every tenant/anchor. For each tenant, it uses prior/train rows only
to select the best TFT candidate key (`source_model_name`, `candidate_family`,
`candidate_model_name`). TFT is allowed into the final holdout only when its
prior mean regret beats the frozen V2+ prior profile by the configured threshold.
If that evidence is absent or weak, the emitted combined row is exactly the
frozen V2+ fallback. Final-holdout actuals affect scoring only.

This means a combined result can pass only if TFT improves the final strict
LP/oracle mean regret versus calibrated V2+ and does not worsen median regret.
Matching V2+ through fallback is valid negative evidence, but it is not a
promotion.

Materialized combined result on the 36-anchor screen, Dagster run
`df0d00ac-49ae-4e56-bc82-7e33c43b7e36`:

| Role | Rows | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|
| Frozen calibrated NBEATSx V2+ | 90 | 174.77 | 67.30 |
| Combined NBEATSx+TFT selector | 90 | 174.77 | 67.30 |

The combined gate was blocked, not promoted. It selected the frozen V2+ fallback
for all 90 tenant-anchor rows because TFT prior/train candidates did not clear
the required improvement threshold. Therefore the current evidence says TFT does
not yet help as complementary schedules under the small 36-anchor, bounded
global-panel configuration.

## Materialization

Screen the latest anchors before attempting a full 365-anchor run:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select tft_official_global_panel_rolling_strict_lp_benchmark_frame,tft_official_global_panel_horizon_quantile_calibration_frame,dfl_tft_quantile_schedule_candidate_library_frame,dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame,dfl_tft_combined_v2_plus_strict_lp_benchmark_frame -c configs/real_data_official_global_panel_tft_quantile_schedule_value_week3.yaml
```

For host CUDA execution, use the unified official-evidence runner pattern where
possible, or run the matching Dagster selection from the activated root venv so
the Windows CUDA torch install is visible.

## Acceptance Rule

The slice can have three valid outcomes:

1. TFT alone beats V2+: promote TFT as the new Offline Strategy Promotion
   headline only if mean regret improves, median regret does not degrade, rolling
   robustness holds, and `market_execution_enabled=false`.
2. TFT helps only as complementary schedules: keep NBEATSx V2+ as the named
   baseline but document the combined NBEATSx+TFT schedule/value improvement.
3. TFT fails versus V2+: document it as evidence that the current
   Ukrainian-only feature/candidate space favors NBEATSx V2+.

No outcome enables live execution or dashboard/API default switching.
