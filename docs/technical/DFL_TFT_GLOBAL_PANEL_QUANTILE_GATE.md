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
| `tft_official_global_panel_horizon_quantile_calibrated_strict_lp_benchmark_frame` | Re-scores raw and horizon-calibrated TFT p10/p50/p90 forecasts through the strict LP/oracle evaluator. |
| `dfl_tft_quantile_schedule_candidate_library_frame` | Converts TFT p10/p50/p90 strict benchmark rows into feasible schedule candidates. |
| `dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame` | Compares frozen NBEATSx V2+ rows against TFT-quantile V2/V2+ rows under the unchanged strict LP/oracle gate. |
| `dfl_tft_augmented_v2_plus_evidence` | Asset check for coverage, claim boundary, and strict comparator discipline. |
| `dfl_tft_combined_v2_plus_strict_lp_benchmark_frame` | Tests whether TFT quantile schedules add value as complementary candidates on top of frozen NBEATSx V2+. |
| `dfl_tft_combined_v2_plus_evidence` | Asset check for combined NBEATSx+TFT coverage, claim boundary, and no-live-execution flags. |
| `dfl_tft_calibrated_quantile_schedule_candidate_library_frame` | Converts calibrated TFT p10/p50/p90 strict rows into the same schedule-candidate contract. |
| `dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame` | Tests whether calibrated TFT schedules add complementary value on top of frozen NBEATSx V2+. |

Tracked config:
[real_data_official_global_panel_tft_quantile_schedule_value_week3.yaml](../../configs/real_data_official_global_panel_tft_quantile_schedule_value_week3.yaml).

Serious 365-anchor config:
[real_data_official_global_panel_tft_quantile_schedule_value_365_week3.yaml](../../configs/real_data_official_global_panel_tft_quantile_schedule_value_365_week3.yaml).

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
- The horizon quantile calibration path is now strict-scored as its own
  `tft_official_global_panel_horizon_quantile_calibrated_strict_lp_benchmark_frame`
  asset. Raw TFT rows remain visible, but calibrated p10/p50/p90 rows can also
  enter the schedule-candidate library without mutating earlier evidence.

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

The first combined path keeps the calibrated NBEATSx V2+ schedule as the default
fallback for every tenant/anchor. For each tenant, it uses prior/train rows only
to select the best TFT candidate key (`source_model_name`, `candidate_family`,
`candidate_model_name`). This was intentionally conservative, but it is still
coarse because it treats TFT as one tenant-level alternative.

The next combined path is candidate-level and is documented in
[DFL_NBEATSX_TFT_COMBINED_PORTFOLIO.md](DFL_NBEATSX_TFT_COMBINED_PORTFOLIO.md).
It builds a portfolio from frozen NBEATSx V2+, strict fallback, calibrated TFT
p10/p50/p90 schedules, and cross-model schedule candidates. It adds prior-only
disagreement features such as peak/trough disagreement, quantile spread,
schedule distance from V2+, terminal SOC delta, and throughput delta. TFT is
allowed into final scoring only when train/prior evidence predicts a non-
degrading improvement; otherwise the emitted row remains the frozen V2+ fallback.
Final-holdout actuals affect scoring only.

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

The follow-up full-data candidate-portfolio gate is documented in
[DFL_NBEATSX_TFT_COMBINED_PORTFOLIO.md](DFL_NBEATSX_TFT_COMBINED_PORTFOLIO.md).
It found local TFT opportunities on `24 / 90` latest tenant-anchors, but the true
rolling strict replay still failed `0 / 4` windows. The latest windows fell back
to V2+; older windows selected TFT-derived schedules and worsened mean/median
regret. This is negative evidence for TFT as a robust complementary schedule
source under the current Ukrainian-only feature space.

## Materialization

Screen the latest anchors before attempting a full 365-anchor run:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select tft_official_global_panel_rolling_strict_lp_benchmark_frame,tft_official_global_panel_horizon_quantile_calibration_frame,dfl_tft_quantile_schedule_candidate_library_frame,dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame,dfl_tft_combined_v2_plus_strict_lp_benchmark_frame -c configs/real_data_official_global_panel_tft_quantile_schedule_value_week3.yaml
```

For host CUDA execution, use the unified official-evidence runner pattern where
possible, or run the matching Dagster selection from the activated root venv so
the Windows CUDA torch install is visible.

For the serious 365-anchor lane, use the resumable TFT runner. It writes
`attempt_manifest.json`, persists batches under
`.tmp_runtime/tft_quantile_gate_batches/`, and can run in host mode so the local
CUDA PyTorch install is visible:

```powershell
.\scripts\run-tft-quantile-gate-batches.ps1 `
  -TotalAnchors 365 `
  -BatchSize 2 `
  -AnchorBatchOrder chronological `
  -LocalMode host `
  -TftMaxEpochs 30 `
  -TftMaxSteps 20 `
  -TftBatchSize 8
```

For a bounded proof batch before committing to the full run:

```powershell
.\scripts\run-tft-quantile-gate-batches.ps1 `
  -TotalAnchors 365 `
  -BatchSize 1 `
  -EndAnchorIndex 1 `
  -AnchorBatchOrder chronological `
  -LocalMode host `
  -TftMaxEpochs 30 `
  -TftMaxSteps 20 `
  -SkipDownstreamGate
```

Once enough batches are persisted, rerun without `-SkipDownstreamGate` or
materialize the downstream calibrated selection:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select tft_official_global_panel_horizon_quantile_calibration_frame,tft_official_global_panel_horizon_quantile_calibrated_strict_lp_benchmark_frame,dfl_tft_calibrated_quantile_schedule_candidate_library_frame,dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame,dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame -c configs/real_data_official_global_panel_tft_quantile_schedule_value_365_week3.yaml
```

For the candidate-level combined portfolio after both V2+ and calibrated TFT
candidate rows exist:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_nbeatsx_tft_complementarity_audit_frame,dfl_nbeatsx_tft_candidate_portfolio_v1_frame,dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame,dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame,dfl_nbeatsx_tft_meta_selector_robustness_frame -c configs/real_data_dfl_nbeatsx_tft_combined_portfolio_week3.yaml
```

The 2026-05-19 selector diagnostic found that this latest-holdout strict frame
can show TFT complementarity but cannot prove that the prior selector is safe
across rolling windows. The thesis-safe fix is the true rolling strict path:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame,dfl_nbeatsx_tft_meta_selector_prior_rolling_robustness_frame -c configs/real_data_dfl_nbeatsx_tft_combined_portfolio_week3.yaml
```

This path rebuilds V2+ from anchors before each validation window and
synthesizes V2+ fallback candidate rows before testing calibrated TFT
candidates. It must be used before treating TFT as a robust complement or
before moving to DT/LAVA-style work.

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
