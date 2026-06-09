# Progress Brief: Since Week 2 Demo

Date: 2026-05-13
Audience: supervisor and peer progress meeting
Claim boundary: **Offline Strategy Promotion only**. No live market execution, no deployed Decision Transformer, and no dashboard/API default switch.

## One-Minute Summary

Since the Week 2 demo, the project moved from an operator-facing MVP preview to an evidence-backed research pipeline. The strongest current result is not that raw NBEATSx/TFT forecasts beat the baseline. They do not. The stronger result is that an official global-panel NBEATSx schedule/value learner can choose feasible LP-scored schedules that beat the frozen `strict_similar_day` comparator under strict LP/oracle regret on a 365-anchor Ukrainian panel.

## What Changed

- The Week 2 contour was stabilized as a baseline: OREE/Open-Meteo/tenant data -> Dagster -> `strict_similar_day` -> LP dispatch -> FastAPI/Nuxt preview.
- The research layer was expanded to rolling-origin evidence, strict LP/oracle regret, DFL-style schedule/value selection, and Dagster-visible checks.
- Official global-panel NBEATSx evidence was run on a 365-anchor Ukrainian backfill panel.
- Market-coupling and external exogenous features were added as governed candidates, but remain blocked from Ukrainian training.

## Main Results

Raw official forecasts over 365 anchors:

| Strategy | Mean regret UAH | Median regret UAH | Interpretation |
|---|---:|---:|---|
| `strict_similar_day` | 431.52 | 217.27 | Frozen control remains strong |
| `nbeatsx_official_global_panel_v1` | 708.14 | 446.08 | Raw official forecast loses |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 602.51 | 351.25 | Calibration helps, but still loses |

Schedule/value learner on latest holdout:

| Source | Strict mean/median | Learner mean/median | Improvement vs strict | Rolling strict passes |
|---|---:|---:|---:|---:|
| Raw global-panel NBEATSx source | 310.58 / 198.39 | 225.44 / 109.69 | 27.41% | 4/4 |
| Calibrated global-panel NBEATSx source | 310.58 / 198.39 | 206.37 / 96.02 | 33.55% | 4/4 |

## Exogenous Feature Evidence

Ukrainian prior-only context features helped the offline policy experiments:

- AFL audit covered 1,560 rows across five tenants and two compact models.
- Weather/load context is now present as prior-only input.
- Remaining failures are decision/ranking failures, not missing context:
  - LP-value failure: 80.23%.
  - rank/extrema failure: 64.83%.
  - spread-shape failure: 55.19%.

Feature-aware selectors improved strongly versus raw compact neural schedules:

| Selector | Mean regret UAH | Median regret UAH | Improvement vs raw | Improvement vs strict |
|---|---:|---:|---:|---:|
| NBEATSx feature-aware selector | 299.73 | 182.76 | 63.15% | 4.79% |
| TFT feature-aware selector | 299.19 | 160.52 | 70.19% | 4.96% |

These selectors were useful development evidence but narrowly missed the conservative 5% strict-control gate. The later schedule/value learner cleared that gate.

## External Sources Are Not In Training Yet

ENTSO-E, OPSD, Ember, Nord Pool, PriceFM, and THieF are documented as future market-coupling or external-validation sources, but they are not used in the current training rows or promoted result. The current market-coupling route records six external feature candidates and zero approved external training columns.

The current 365-anchor evidence uses Ukrainian-only inputs: OREE DAM prices, Open-Meteo/weather context, tenant load/configuration context, and strict LP/oracle scoring. External features remain blocked until publication time, timezone/DST handling, prior-known FX normalization, licensing, market-rule mapping, and domain-shift checks pass.

## Horizon-Aware Regret-Weighted Calibration

Horizon-aware regret-weighted calibration is a prior-only correction that learns which forecast horizon hours matter most for downstream LP regret. In BESS arbitrage, an error during a likely charge or discharge hour can cost much more than the same absolute error during a low-value hour.

On the Dnipro 90-anchor preview, this calibration improved both compact neural candidates versus raw variants:

| Candidate | Raw mean regret UAH | Calibrated mean regret UAH | Result |
|---|---:|---:|---|
| TFT | 2361.96 | 1727.29 | improved, but still behind strict |
| NBEATSx | 2070.28 | 1804.38 | improved, but still behind strict |
| `strict_similar_day` | 1384.70 | 1384.70 | remained frozen control |

The interpretation is narrow: regret-aware calibration helped neural forecasts become more decision-aligned, but it did not yet prove raw neural forecast superiority.

## Why The Later Schedule/Value Learner Cleared The Gate

The feature-aware selectors used prior-only regime features and nearly beat the strict control. The schedule/value learner improved the architecture: instead of selecting only from simple regime rules, it selected among feasible LP-scored schedules using schedule/value features such as forecast spread, LP objective value, throughput, degradation proxy, SOC slack, and prior family regret.

That changed the target from "better forecast" to "better feasible schedule." The final schedule was still scored by the same strict LP/oracle gate, with final-holdout actuals used for scoring only.

## Presentation Visuals

The deck includes generated explanation slides for the supervisor meeting:

- a strategy ladder comparing `strict_similar_day`, raw NBEATSx/TFT, horizon-aware calibration, feature-aware selectors, and Schedule/Value Learner V2;
- a Schedule/Value Learner V2 architecture diagram showing the candidate-library flow, LP-scored schedules, prior-only learner, strict LP/oracle gate, and the 27.41% / 33.55% latest-holdout improvements.
- a Schedule/Value Learner V2 feature map clarifying that the weight profile is selected offline from prior anchors, not learned with gradient descent.

## What Is Not Claimed

- No live trading.
- No deployed Decision Transformer controller.
- No raw forecast-model superiority.
- No EU-derived training rows.
- No market-coupling feature effect on the promoted result yet.

## Next Work

1. Use GPU/host-CUDA and Hugging Face Jobs for heavier official evidence attempts.
2. Recover more Ukrainian observed history if available.
3. Approve market-coupling features only after publication-time, timezone, FX, licensing, and domain-shift gates pass.
4. Build stronger DFL/DT candidates only after the schedule/value evidence remains robust.
