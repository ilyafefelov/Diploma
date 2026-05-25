# ADR: Keep V2+ as Thesis Headline and Gate DT/LAVA/Poland/TFT

Status: Proposed for immediate adoption

Date: 2026-05-25

## Context

The project now contains multiple evidence lanes:

- Strict LP/oracle comparator.
- Schedule/value learner V2+.
- TFT/NBEATSx forecast adapter evidence.
- Poland lagged exogenous feature research.
- DT research shadow.
- LAVA NPZ smoke contract.
- V13 Ukrainian source-readiness/acquisition gate.

The thesis draft already separates offline read-model evidence from market execution. The repo artifacts confirm that this separation is necessary: V13 is not ready, explicit DAM publication receipts are missing, DT/LAVA are not promotable, and the market execution flag remains false.

## Decision

The thesis headline result should remain:

> Offline DAM recommendation preview using V2+ schedule/value learner evidence under a frozen strict LP/oracle comparator, with no market execution.

DT, LAVA, Poland-enhanced features, TFT quantile variants, and V4/V5 context-repair experiments should be presented as research branches or non-promoted challengers unless their artifact packets independently satisfy:

- source-governance checks,
- exact run/packet traceability,
- strict LP/oracle comparison,
- rolling robustness,
- no-market-execution boundary,
- and, for V13/DT/LAVA promotion, explicit source-readiness preconditions.

## Consequences

Positive:

- The thesis story becomes defensible and evidence-backed.
- Negative evidence becomes an academic asset rather than a weakness.
- The dashboard can show shadow diagnostics without implying production default changes.
- The project avoids unsafe claims around market submission.

Tradeoffs:

- The result is narrower than a full autonomous trader.
- DT/LAVA excitement must be framed as roadmap/prototype work.
- V13 remains blocked until data acquisition closes.

## Rejected Alternatives

### Claim full DFL or predict-then-bid

Rejected because the repo does not currently implement the complete tri-layer stack: price prediction, storage optimization, market-clearing/settlement assumptions, and execution-ready governance.

### Promote DT shadow

Rejected because the DT shadow is a research evaluation over candidate indexes/schedule families. It is not a deployed hourly BUY/SELL/HOLD controller and has `promotable_v13_permitted_training_rows=0`.

### Promote LAVA based on smoke evidence

Rejected because the LAVA packet is an 8-instance CI/prototype smoke check. It validates contracts and boundaries, not deployment readiness.

### Use Poland features as the headline result

Rejected because current evidence is mixed: latest-holdout signal is promising, but rolling robustness and full 90-row comparison do not promote it over Ukrainian-only V2+.

