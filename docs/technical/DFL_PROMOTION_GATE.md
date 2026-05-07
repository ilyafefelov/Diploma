# DFL Promotion Gate

Date: 2026-05-07

This document records the conservative promotion rule added after the Week 3
deep-research intake. The rule prevents forecast, calibration, selector, or DFL
candidates from being described as improved control unless they beat the frozen
`strict_similar_day` comparator on same-scope evidence.

## Gate Rule

The first gate is intentionally narrow:

| Check | Required state |
|---|---|
| Tenant | `client_003_dnipro_factory` unless a later all-tenant slice is approved. |
| Batch scope | Same latest generated batch for candidate and `strict_similar_day`. |
| Anchor coverage | At least 90 anchors. |
| Data quality | `thesis_grade` only. |
| Provenance | Observed coverage ratio of 1.0. |
| Safety | Zero safety violations and no market-execution claim. |
| Mean regret | Candidate improves mean regret by at least 5 percent. |
| Median regret | Candidate median regret is not worse than the strict control. |

This is a promotion gate, not a training objective. It does not change the
existing API, dashboard, or Dagster asset contracts.

## Current Decision

The current offline DFL v0 result is blocked:

- validation holdout has 18 anchors, not the required 90;
- `nbeatsx_silver_v0` relaxed regret worsened from 1477.37 to 1499.85;
- `tft_silver_v0` relaxed regret worsened from 1974.55 to 2460.07.

The correct thesis statement is therefore:

> A bounded offline DFL experiment has started and is testable, but the first
> held-out relaxed-LP result is negative and remains diagnostic only.

## Research Rationale

- [TSFM leakage evaluation](https://huggingface.co/papers/2510.13654) supports
  strict temporal/latest-batch evaluation and source separation before claims.
- [Decision-Focused Learning survey](https://huggingface.co/papers/2307.13565)
  supports decision-quality evaluation through optimization, while reinforcing
  that candidate promotion needs an empirical benchmark.
- PriceFM and THieF remain future forecast-layer references; they do not change
  the current promotion decision.
