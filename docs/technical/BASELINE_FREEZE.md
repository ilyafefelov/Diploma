# Baseline Freeze: Strict Similar-Day Level 1 Control

Date: 2026-05-07

This document freezes `strict_similar_day` as the Level 1 control comparator for
the thesis benchmark. The freeze is methodological: future research candidates
may improve decision value, but they must beat this control under the same
rolling-origin LP/oracle protocol before promotion.

## Frozen Control

| Field | Frozen value |
|---|---|
| Control model | `strict_similar_day` |
| Forecast family | Level 1 Naive Forecast |
| Market scope | DAM only |
| Interval | Hourly, 60 minutes |
| Currency | UAH |
| Forecast rule | Copy one historically analogous slot with no smoothing. |
| Weekday lag | `t-24h` for Tue-Fri slots. |
| Special-day lag | `t-168h` for Mon, Sat, Sun slots. |
| Dispatch solver | LP through `HourlyDamBaselineSolver`. |
| Execution policy | Rolling horizon; score full horizon, commit only first action in live-style preview. |
| Battery layer | Feasibility-and-economics preview model with throughput/EFC degradation proxy. |
| Oracle use | Offline scoring only; never a deployable forecast or dispatch policy. |

The freeze does not freeze every surrounding implementation detail forever. It
freezes the comparator semantics that make later NBEATSx, TFT, calibration, DFL,
and Decision Transformer experiments interpretable.

## Evidence Reconciliation

| Evidence lane | Claim boundary |
|---|---|
| Week 3 Dnipro 30-anchor benchmark | Accepted Week 3 thesis-grade result. |
| Dnipro 90-anchor calibration preview | Prepared-ahead calibration/selector evidence; not the Week 3 headline. |
| All-tenant 90-anchor diagnostic snapshots | Aggregate context only; do not mix with tenant-specific latest-batch values. |
| Horizon-aware calibration | Calibration evidence; not full DFL and not baseline replacement. |
| Selector gates | Selector diagnostics only; not market execution. |
| Offline DFL v0 | Negative diagnostic evidence; it runs but does not improve held-out relaxed regret. |

If numbers differ across these lanes, the lane label wins. Supervisor-facing
reports must state the tenant, anchor count, generated batch, and claim scope.

## Promotion Rule

A new candidate can only move beyond research evidence when it:

1. uses the same tenant/latest-batch comparison as the control;
2. has at least 90 thesis-grade Dnipro anchors for the promotion check;
3. preserves observed coverage and rolling-origin temporal ordering;
4. has zero safety or market-execution claim violations;
5. improves mean regret by at least 5 percent versus `strict_similar_day`;
6. does not worsen median regret versus `strict_similar_day`.

The current offline DFL v0 fails this promotion rule because held-out relaxed
regret worsens for both tested neural candidates. This is useful research
evidence, not a failed production release.

## What This Freeze Prevents

- Replacing the control with a smoother seasonal average without renaming the
  comparator.
- Reporting forecast MAE/RMSE improvements as strategy improvements without
  LP/oracle regret.
- Mixing Dnipro latest-batch results with all-tenant or older persisted batches.
- Treating calibration selectors as full DFL or market execution.
- Training on oracle/future prices and then presenting the result as a
  deployable forecast.

## Future Changes

Any future baseline revision must use a new explicit comparator name and must
leave `strict_similar_day` available for historical comparison. Examples:

- `seasonal_average_day`;
- `calendar_weighted_similar_day`;
- `weather_matched_similar_day`;
- `market_rule_aware_naive_v1`.

These can be useful research candidates, but they are not the frozen Level 1
control.
