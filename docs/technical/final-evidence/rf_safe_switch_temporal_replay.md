# Random-Forest Safe-Switch Temporal Replay

Date: 2026-07-12

This is a post-defense corrective experiment. It is not part of the defended
thesis result and does not replace the preserved final PDF.

Source artifacts:

- `data/research_runs/rf_safe_switch_temporal_suite_2026_07_12/rf_safe_switch_temporal_suite_summary.json`
- `data/research_runs/rf_safe_switch_temporal_suite_2026_07_12/rf_safe_switch_temporal_suite_rows.csv`
- `arxiv/evidence/lineage/rf_safe_switch_temporal_suite_summary.json`
- `arxiv/evidence/lineage/rf_safe_switch_temporal_suite_rows.csv`

## Protocol

The estimator is the same safe-switch `RandomForestRegressor` family used by
the historical artifact: 500 trees, maximum depth 6, minimum leaf size 1, a
20 UAH predicted-improvement threshold, a 150 UAH tail-loss threshold, and a
0.5 family-risk cap. The historical identifier `dt_v2_plus` is not used as the
model name.

The suite covers both co-primary NBEATSx source lines. For evaluation window 1,
training uses windows 4, 3, and 2; for window 2 it uses windows 4 and 3; for
window 3 it uses window 4. Each evaluation window has 360 candidate rows, 90
profile-date outcomes, and 18 distinct market dates. Training has 1,080, 720,
and 360 candidate rows respectively.

The frozen operating threshold is 20 UAH. Latest-window threshold sensitivity
also evaluates 0, 5, 10, and 50 UAH. The 20 UAH rows use seeds 42, 2026, and 7;
other threshold rows use seed 42. Seeds are model-stability sensitivity, and the
windows are protocol sensitivity; neither is treated as independent market
replication.

All 14 protocol rows pass an exact model-input-plus-target fingerprint audit
with zero overlapping candidate rows and `independent_holdout=true` for the
declared temporal splits.

## Latest-window result

| Field | Value |
| --- | ---: |
| Evaluation rows | 90 profile-date rows |
| Distinct market dates | 18 |
| RF safe-switch mean regret | 174.7684 UAH |
| V2+ fallback mean regret | 174.7684 UAH |
| RF minus V2+ | 0.0000 UAH |
| Non-V2+ switches | 0 / 90 |
| Abstentions to V2+ | 90 / 90 |
| Distinct switch dates | 0 |
| Observed tail losses | 0 |

The primary seed is 42. Seeds 2026 and 7 produce the same all-abstain path and
are model-stability checks, not independent replications. At the market-date
cluster level, all 18 dates are ties. The declared three-date moving-block
bootstrap therefore returns `[0.0, 0.0]` UAH; this degenerate interval describes
the identical selected path and must not be interpreted as certainty about
future RF performance.

The raw NBEATSx source has the same latest-window behavior: 193.3590 UAH for
both RF safe-switch and V2+, 0/90 switches, and 90/90 abstentions. For both
sources, thresholds 0, 5, 10, 20, and 50 UAH all produce zero switches. At the
0 UAH threshold, all 90 rows are recorded as predicted improvement below the
threshold, so the latest-window abstention is not created by choosing an
artificially high switch cutoff.

## Rolling temporal result at the frozen 20 UAH threshold

Positive delta means the RF safe-switch has higher regret than V2+. Switch and
tail-loss counts are for primary seed 42; the final column gives the delta range
across seeds 42, 2026, and 7.

| Source | Evaluation window | RF regret | V2+ regret | RF minus V2+ | Switches | Tail losses | Seed delta range |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Calibrated | 1 | 174.77 | 174.77 | 0.00 | 0 | 0 | [0.00, 0.00] |
| Calibrated | 2 | 226.12 | 226.12 | 0.00 | 0 | 0 | [0.00, 0.00] |
| Calibrated | 3 | 516.35 | 393.27 | +123.08 | 43 | 19 | [+107.50, +123.08] |
| Raw | 1 | 193.36 | 193.36 | 0.00 | 0 | 0 | [0.00, 0.00] |
| Raw | 2 | 344.20 | 279.02 | +65.18 | 5 | 4 | [+65.18, +65.18] |
| Raw | 3 | 507.94 | 411.50 | +96.44 | 8 | 5 | [+96.44, +96.44] |

The suite contains zero beneficial protocol rows, eleven ties created by full
abstention, and three harmful rows. For calibrated window 3, the date-cluster
mean-delta interval is [49.98, 202.10] UAH, with losses on 11 of 18 dates. Raw
windows 2 and 3 have sparse one-date loss clusters and moving-block intervals
whose lower bound is 0.00 UAH; their primary-seed mean harms remain 65.18 and
96.44 UAH.

## Interpretation

The temporal suite does not reproduce the historical 168.1566 UAH result and
contains no evidence of RF improvement. On the latest window it abstains
completely; on three earlier source/window protocols it switches and increases
regret, with substantial observed tail losses. This is negative retrospective
evidence against promoting the RF safe-switch under the frozen rules.

The suite is time-separated relative to each declared rolling split, but it was
designed and executed after the lineage problem was discovered. It is not
untouched prospective confirmation, and the 14 rows are not 14 independent
market experiments.

`promotion_gate_passed=false` and `market_execution_enabled=false` remain
mandatory.
