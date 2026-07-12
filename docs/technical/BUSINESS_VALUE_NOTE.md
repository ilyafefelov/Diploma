# Business Value Note

Date: 2026-06-16

This note frames the project for a business or product reviewer without
overstating the current engineering boundary.

## Product Position

The system is an operator-preview layer for Ukrainian BESS arbitrage. It helps a
human operator inspect source-backed DAM/IDM hourly price context, battery state,
candidate schedule value, and safety/readiness blockers before any real market
or dispatch workflow exists.

The current repository proves decision-support quality and evidence
traceability. It does not prove autonomous trading, legal market submission, or
inverter dispatch.

## Stakeholders

| Stakeholder | Need | Current repository answer |
| --- | --- | --- |
| BESS operator | Clear hourly recommendation context | `/operator` shows DAM/IDM preview, schedule/value comparison, SOC/SOH context, and guardrail status |
| Technical reviewer | Reproducible evidence | Final evidence summaries, API docs, tests, and verification wrapper |
| Academic committee | Defendable novelty without overclaiming | V2+ headline evidence plus DT/HF shadow evidence under strict boundaries |
| Future pilot owner | Safe path to operational validation | No-execution design, explicit blockers, and source-readiness gates |

## Value Levers

- Better schedule/value selection than the strict similar-day comparator in the
  offline evidence packet.
- Transparent guardrails for abstention and source-readiness blockers.
- Faster operator review because market context, battery context, and evidence
  status are visible in one dashboard.
- Lower implementation risk for future pilots because the current system keeps
  market execution disabled and separates preview from market submission.

## What The Dashboard Should Communicate

- The selected row is a recommendation preview, not a command.
- Official/source-backed price context is preferred; forecast-store rows are
  used only for unpublished horizons.
- V2+ remains the headline/default evidence.
- The historical `dt_v2_plus` artifact is a random-forest exact-mirror diagnostic, not DT/OOS evidence; the separate HF transformer-backbone path is also manual/non-independent evidence, not promoted control.
- A HOLD or abstention is a valid safety outcome.

## What It Should Not Claim

- Guaranteed profit or ROI.
- Production market execution readiness.
- Replacement of operator responsibility.
- Legal compliance for market submission.
- A deployed Decision Transformer or full DFL controller.

## Pilot Readiness Interpretation

The repository is suitable as a controlled pilot foundation only after separate
work adds operational ownership, explicit market integration, source/publication
receipts, monitoring, incident response, legal review, and an execution-specific
gate. Until then, the correct business description is:

> Source-backed DAM/IDM hourly operator recommendation preview with offline and
> shadow evidence; market execution disabled.

