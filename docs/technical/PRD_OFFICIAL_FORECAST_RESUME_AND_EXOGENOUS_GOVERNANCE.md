# PRD: Resumable Official Forecasts And Exogenous Feature Governance

Date: 2026-05-11

## Problem Statement

The project has reached the point where compact in-repo NBEATSx/TFT evidence is
no longer enough. The thesis needs a fair official-library comparison that routes
serious NBEATSx/TFT rolling forecasts through the same schedule/value candidate
library and strict LP/oracle promotion gate used by the current DFL evidence
stack.

The current blocker is operational and methodological. A full 104-anchor
official rolling-origin run is long on local CPU, and a one-shot materialization
can lose about an hour of work when the outer process times out. At the same
time, simply increasing epochs is not a clean thesis route unless exogenous
market-coupling, weather, load, and temporal-availability features are governed
before they enter official or DFL training.

## Solution

Add a resumable official rolling-origin execution path and record a follow-up
governance track for exogenous features. The official forecast run should be
split into smaller persisted anchor batches that share a fixed generation
timestamp. Each batch should persist its rows immediately, and later batches
should be able to merge already persisted rows so the downstream
schedule/value gate can run after the full 104-anchor panel is available.

In parallel, record a source and PRD trail for the next feature-governance slice:
market-coupling and external exogenous data can be used only after licensing,
timezone, currency, market-rule, temporal-availability, and domain-shift checks
pass. Until then, Ukrainian observed OREE/Open-Meteo data remains the training
source of truth.

## User Stories

1. As a thesis author, I want official NBEATSx/TFT forecasts scored through the
   same strict LP/oracle gate, so that the comparison with compact candidates is
   academically fair.
2. As a thesis author, I want the 104-anchor official run to resume by anchor
   batch, so that a timeout does not discard a long CPU run.
3. As a thesis author, I want each batch to use the same generation timestamp,
   so that persisted rows can be treated as one evidence run.
4. As a thesis author, I want partial official rows to remain research-only, so
   that incomplete evidence is not mistaken for promotion-grade evidence.
5. As a thesis author, I want a clear command for unattended execution, so that
   a long run can continue while I am away from the workstation.
6. As a thesis author, I want per-batch logs, so that a failed batch can be
   diagnosed without rerunning completed batches.
7. As a thesis author, I want downstream schedule/value assets to consume the
   completed official run, so that official forecasts are judged by decision
   value rather than forecast loss only.
8. As a thesis author, I want the strict similar-day comparator to remain the
   frozen fallback, so that no official source is promoted by convenience.
9. As a supervisor, I want the official run documented separately from smoke
   evidence, so that thesis claims do not overstate small samples.
10. As a supervisor, I want the source status of European and market-coupling
    data recorded, so that future external features have an auditable boundary.
11. As a data engineer, I want anchor-batch slicing to be configured at the
    Dagster asset level, so that Compose-backed materialization can run
    repeatable slices.
12. As a data engineer, I want persisted batches to be queryable by strategy
    kind and generation timestamp, so that the asset can reconstruct cumulative
    evidence.
13. As a data engineer, I want the null store to keep the same interface, so
    that tests and local non-Postgres workflows remain simple.
14. As a researcher, I want exogenous features separated into allowed,
    watch-only, and blocked statuses, so that training inputs stay leak-safe.
15. As a researcher, I want ENTSO-E, OPSD, Ember, Nord Pool, and PriceFM-style
    sources treated as future bridge context first, so that Ukrainian training
    is not polluted by mismatched markets.
16. As a researcher, I want Open-Meteo weather availability recorded explicitly,
    so that weather features can be aligned to the forecast decision time.
17. As a researcher, I want official NBEATSx/TFT models to receive governed
    exogenous variables only after availability checks pass, so that improved
    model capacity does not introduce leakage.
18. As an operator-demo reviewer, I want the offline promotion gate to keep
    market execution disabled, so that dashboard/read-model evidence is not
    confused with live trading.
19. As a future agent, I want a clear next-slice PRD, so that it can pick up
    source governance and official training without re-litigating the boundary.
20. As a future agent, I want a precise blocker taxonomy, so that failure means
    timeout, undercoverage, mean-regret failure, median degradation, leakage,
    or source-governance failure rather than an ambiguous "model failed."

## Implementation Decisions

- Official rolling-origin generation remains the source asset for official
  NBEATSx/TFT strict LP/oracle evidence.
- Anchor slicing is additive: a zero batch size means the original all-anchor
  behavior, while positive batch size materializes a bounded anchor slice.
- A fixed generation timestamp is the run identity for resumable batches.
- The strategy evaluation store exposes a generated-at lookup for cumulative
  batch recovery.
- Batch materialization is orchestrated by a local script rather than a new
  service or dependency.
- The script runs Compose-backed Dagster materializations, writes per-batch
  logs, and supports resume from a later anchor index.
- Downstream official schedule/value assets run only after the official rolling
  rows are available.
- Existing compact schedule/value assets and public API/dashboard contracts are
  not changed by this PRD.
- Market-coupling and other external exogenous data remain blocked from
  training until a governance gate records licensing, timezone, currency,
  market-rule, temporal availability, and domain-shift status.
- Ukrainian observed OREE/Open-Meteo evidence remains the thesis-grade training
  source of truth for the current diploma scope.

## Testing Decisions

- Tests should verify behavior through public builder, store, and asset
  interfaces rather than private helper internals.
- The rolling-origin builder should prove that a configured anchor batch returns
  only the intended anchor slice and preserves a fixed generation timestamp.
- The strategy evaluation store should prove that rows can be recovered by
  strategy kind and generation timestamp.
- The Dagster asset function should prove that persisted rows from earlier
  batches are merged when resume mode is enabled.
- The batch runner should pass PowerShell syntax validation before unattended
  execution.
- Existing official schedule/value tests remain the regression suite for the
  downstream promotion path.
- Full repository verification should still include Dagster definition checks
  and Compose configuration validation.

## Out of Scope

- No live market execution.
- No dashboard default switch to official forecasts.
- No public API contract change.
- No new dependency solely for batching.
- No European data ingestion into Ukrainian DFL training.
- No weakening of the strict LP/oracle promotion gate.
- No claim that official NBEATSx/TFT is promoted before the 104-anchor or larger
  run completes and passes the gate.

## Further Notes

This PRD records two next-step directions. First, complete the serious official
104-anchor evidence run with resumable batches and a longer external timeout.
Second, add market-coupling/exogenous feature governance and route only approved
features into official/DFL training. If additional observed Ukrainian history
can be recovered, the promotion gate should rerun on the larger panel. If not,
the project should tighten prior-only regime gates without weakening the strict
LP/oracle rule.
