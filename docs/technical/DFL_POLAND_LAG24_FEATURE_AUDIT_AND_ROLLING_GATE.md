# Poland Lag-24 Feature Audit And Rolling Gate

This evidence slice closes the first token-backed ENTSO-E/NBU Poland feature
route as a research-only shadow challenger. It does not replace the frozen
Ukrainian-only Schedule/Value Learner V2+ headline and it does not enable market
execution.

## Comparator

The comparator stays frozen:

| Metric | Frozen Ukrainian-only calibrated V2+ |
|---|---:|
| Mean regret | `174.77` UAH |
| Median regret | `67.30` UAH |
| Rolling robustness | `4 / 4` windows |
| Market execution | `false` |

Any Poland-enhanced row must beat this comparator before it can become a new
Offline Strategy Promotion headline.

## What Was Tested

The Poland route uses ENTSO-E Poland day-ahead context as point-in-time
exogenous features, not as European target rows in the Ukrainian training panel.
The feature lane includes:

- lag-24 Poland DAM price in UAH/MWh;
- 1h and 24h deltas;
- rolling 24h/168h level and spread context;
- daily peak/trough and rank-style regime features;
- lagged PL-vs-UA spread, spread delta, and spread ratio features where full
  coverage is available.

The route feeds experimental official global-panel NBEATSx/TFT model names, then
the same calibration, schedule library, V2+ selector, strict LP/oracle scoring,
and rolling robustness logic.

## Feature Consumption Audit

Asset:

- `dfl_poland_lag24_feature_consumption_audit_frame`

The audit answers whether the Poland columns actually satisfy the training
contract before model quality is interpreted. It checks:

- column is in the NeuralForecast training feature contract;
- non-null coverage;
- non-constant variance;
- source-backed rows;
- `delivery_timestamp_utc - source_delivery_timestamp_utc >= 24h`;
- scaler route is train-only and the feature is retained by contract;
- claim boundary remains research-only.

Materialized packet:

- `data/research_runs/week3_poland_lag24_feature_audit_rolling_gate/`

Materialized result:

| Audit result | Count |
|---|---:|
| Feature columns audited | `24` |
| Passed training-consumption audit | `17` |
| Blocked by null coverage | `7` |
| Timestamp alignment | `lagged_24h_prior_safe` |

Interpretation: the richer Poland route is not being silently ignored. Most
features pass the route contract, but the UA-spread-derived subset still needs
coverage repair before it should be treated as a fully consumed feature family.

## Rolling Comparison Against Frozen V2+

Assets:

- `dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_robustness_frame`;
- `dfl_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame`.

The first robustness frame checks whether each Poland-enhanced source row is
internally robust versus its own strict/fallback rows. The second frame is the
actual thesis comparison: Poland-enhanced rows must beat frozen Ukrainian-only
V2+ in rolling windows.

Materialized result:

| Source row | Passing windows vs frozen V2+ | Status |
|---|---:|---|
| `nbeatsx_official_global_panel_poland_lag24_horizon_calibrated_v1` | `1 / 4` | `positive_not_promoted` |
| `tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1` | `1 / 4` | `positive_not_promoted` |

The latest holdout was positive but below the replacement threshold: calibrated
Poland TFT V2+ improved mean regret by `3.16%` versus frozen V2+, while the gate
requires at least `5%` and rolling robustness. Older validation windows were not
stable enough.

## Decision

Poland is not discarded. The correct status is:

> Positive shadow evidence, not promoted.

This means Poland/TFT context is promising and should remain a challenger, but
the dashboard and thesis must keep V2+ as the current headline until Poland
passes the same strict LP/oracle promotion rule.

## Next Work

The next implementation branch has started and is intentionally placed before
DT/LAVA:

1. Repair the null-blocked Poland columns with prior-safe neutral values instead
   of future-filled values. Missing first deltas and missing early PL-vs-UA
   spread deltas now become explicit `0.0` neutral context, so downstream
   feature-contract tests can distinguish "no prior comparison yet" from a
   dropped feature.
2. Add richer causal Poland context: morning/evening block means, evening-vs-
   morning spread, centered daily rank, PL peak/trough span, PL-vs-UA
   rank/peak/trough disagreement, spread momentum sign, and separate PL/UA
   daily spreads. These remain lag-24, point-in-time exogenous columns.
3. Add a small tabular candidate-value/ranking model over feasible schedule
   candidates. It trains on train/prior anchors, uses schedule features plus
   repaired Poland context, predicts schedule-level regret, and falls back to
   frozen V2+ unless prior evidence is strong.
4. Start DT/LAVA only after this candidate-value layer produces better teacher
   labels or explains why the Poland/TFT opportunity remains unsafe to exploit.

Additive assets for this branch:

- `dfl_poland_lag24_candidate_value_label_panel_frame`;
- `dfl_poland_lag24_candidate_value_ranker_frame`;
- `dfl_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame`.

Materialized result for the first ranker pass:

| Row | Final rows | Mean regret, UAH | Median regret, UAH | Status |
|---|---:|---:|---:|---|
| Frozen Ukrainian-only V2+ reference | `90` | `174.77` | `67.30` | headline comparator |
| Poland lag-24 candidate-value ranker, calibrated NBEATSx source | `90` | `334.02` | `223.15` | negative evidence |
| Poland lag-24 candidate-value ranker, calibrated TFT source | `90` | `445.75` | `358.19` | negative evidence |

The ranker used train/prior rows and did not use final-holdout actuals for
feature construction, but it selected Poland candidates too aggressively. This
is useful evidence: a naive tabular value ranker over the current feature space
does not safely exploit the Poland near-miss. DT/LAVA should therefore use this
label panel as failure evidence and should not imitate the same objective
unchanged.

Claim boundary remains unchanged: Offline Strategy Promotion evidence only,
`market_execution_enabled=false`, no live dispatch, no dashboard/API default
switch, and no European rows as Ukrainian training rows.
