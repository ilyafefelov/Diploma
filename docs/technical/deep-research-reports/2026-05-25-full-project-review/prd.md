# PRD: Academic MVP and Next Readiness Slice

## Product Name

DAM Operator Recommendation Preview for Ukrainian BESS Arbitrage

## Problem

An operator needs a transparent, source-governed, offline recommendation preview for BESS charging/discharging decisions on the Ukrainian DAM. The system must explain why a schedule is suggested, compare it against strict LP/oracle controls, and preserve hard safety boundaries.

## Users

- Thesis supervisor and committee.
- BESS operator or analyst reviewing a day-ahead schedule.
- Engineer maintaining Dagster assets, FastAPI read models, and dashboard evidence.

## Current MVP

The current MVP is not an autonomous market trader. It is:

- DAM delivery-day recommendation preview.
- Offline/read-model evidence.
- FastAPI read-model output.
- Nuxt dashboard visualization.
- Dagster-materialized research packets.
- Deterministic safety/gatekeeper validation.

## In Scope

- Read-only operator preview for 24-hour DAM schedule.
- Strict LP/oracle comparator.
- V2+ schedule/value learner headline evidence.
- Candidate/shadow diagnostics for DT, LAVA, TFT, and Poland feature research.
- Source-governance labels showing when evidence is blocked.
- Thesis-ready artifact links and validation commands.

## Out of Scope

- Market-submittable bids.
- Real OREE/market submission payloads.
- Automated dispatch commands.
- Full differentiable predict-then-bid controller.
- Deployed Decision Transformer control.
- DT/LAVA training promotion before V13 readiness.

## Functional Requirements

1. The dashboard must show the default recommendation source and whether it is promoted or shadow-only.
2. The API must return read-model payloads only and never market order payloads.
3. The dashboard must display `market_execution_enabled=false` or equivalent boundary context wherever strategy status is shown.
4. Experiment packets must include run slug, comparator, coverage, metrics, claim boundary, and artifact list.
5. V13 reports must show missing required source families and safe-switch counts.
6. The thesis must map each headline claim to a local artifact path or externally linked source.

## Non-Functional Requirements

- Verification must pass Ruff, Mypy, Pytest, Dagster definitions validation, dashboard typecheck, dashboard tests, and Compose config before claiming a fully green repo.
- Dashboard layout must not hide main content behind fixed overlays on desktop or mobile.
- Artifacts must preserve exact dates, run IDs, and claim boundaries.
- External market rules must be date-aware.

## Acceptance Criteria

Academic MVP acceptance:

- Credentialless academic MVP validation passes.
- V2+ packet is linked and reproducible.
- Dashboard screenshots show readable default V2+ recommendation state.
- No market execution claim appears in thesis, README, dashboard, API docs, or demo deck.

Next readiness-slice acceptance:

- `uv run mypy .` passes.
- The schedule dock no longer occludes content.
- README "latest verification" statement is corrected.
- V4/V5 thesis claims have exact artifact paths or are softened.
- Legacy duplicate `src/gatekeeper/` cap constants are removed, quarantined, or marked obsolete.

