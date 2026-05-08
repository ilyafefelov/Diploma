# AFE To AFL To DFL Roadmap

Date: 2026-05-08

This note resolves the current terminology and implementation path:

- **AFE** means Automated Feature Engineering: catalog, availability, leakage
  policy, and feature governance for forecast inputs.
- **AFL** means Arbitrage-Focused Learning: forecast-layer learning rows where
  features are prior-only and labels are realized decision value/regret.
- **DFL** means Decision-Focused Learning: candidates are promoted only by the
  strict LP/oracle gate against the frozen `strict_similar_day` control.

All three remain research evidence. None of this is full DFL, Decision
Transformer control, live trading, or market execution.

## Current Implemented Chain

| Layer | Implemented artifact | Purpose |
|---|---|---|
| AFE | `forecast_afe_feature_catalog_frame` | Sidecar registry of usable features, blocked future bridges, and temporal availability rules. |
| AFE semantic context | `grid_event_signal_silver` | Leakage-safe official Ukrenergo Telegram grid-event features joined by tenant/hour. |
| AFL | `afl_training_panel_frame` | Prior-only forecast features plus realized decision-value labels for forecast hardening. |
| DFL-lite selector | `dfl_feature_aware_strict_failure_selector_*` | Prior-only rule selection and strict LP/oracle scoring. |
| Semantic audit | `dfl_semantic_event_strict_failure_audit_frame` | Explains whether grid-event semantics correlate with strict-control failure windows. |

## Why This Order

The near-miss feature-aware selector is already useful evidence: compact
NBEATSx/TFT schedules can be rescued in some regimes, but the strict-control
threshold is still not cleared. Adding another model before fixing feature
governance would make the thesis harder to defend.

The immediate research path is therefore:

1. Keep `strict_similar_day` frozen as the Level 1 comparator.
2. Catalog every forecast feature with source, availability, and leakage policy.
3. Use official Ukrainian semantic context first: Ukrenergo public Telegram
   grid-event signals.
4. Keep European market context as blocked future bridge rows until licensing,
   timezone, currency, market-rule, and temporal-availability mapping are done.
5. Use AFL rows to harden NBEATSx/TFT forecasts against decision-value labels.
6. Only then attempt stronger DFL training or differentiable LP objectives.

## Claim Boundary

Current semantic AFE is not broad news analysis. It is a deterministic, auditable
official-source signal path. Broad news/Telegram/LLM sentiment extraction stays
out of scope until it has source licensing, timestamp provenance, language
normalization, and leakage tests.

The next eligible modeling slice after this audit is feature-aware forecast
hardening or official NBEATSx/TFT training smoke-to-strict scoring, not a
Decision Transformer policy.

