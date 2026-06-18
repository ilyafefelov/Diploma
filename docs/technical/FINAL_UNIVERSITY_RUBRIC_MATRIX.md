# Final University Rubric Matrix

Date: 2026-06-17

Scope: software product / experimental part of the diploma review, 50 points.
This is an internal defense aid, not a guaranteed grade. Its purpose is to map
each commission criterion to the exact demo steps, repository evidence, tests,
and claim boundaries that should be shown during the final defense.

Defensible project statement:

> Source-backed DAM/IDM hourly operator recommendation preview for BESS
> arbitrage, with offline strategy evidence, deterministic safety boundaries,
> and market execution disabled.

Do not defend the project as a live trading bot, deployed DT controller, full
DFL controller, or market-submittable bidding engine.

## Score Target

| Criterion | Defensible target | Why this is defensible | Primary evidence |
| --- | ---: | --- | --- |
| Implementation matches the declared task | 8-9 / 10 | The implemented surface matches the current thesis boundary: DAM/IDM hourly recommendation preview, tenant/venue/date selection, human-review workflow, and fail-closed source blockers. | [README](../../README.md), [Final defense runbook](FINAL_DEFENSE_RUNBOOK.md), `/operator`, `/defense` |
| Correctness of technologies and methods | 8 / 10 | The stack uses source-backed read models, FastAPI contracts, Pydantic safety boundaries, Dagster assets, forecast-store rows, strict LP/oracle comparators, and Nuxt dashboard presentation. | [API endpoints](API_ENDPOINTS.md), [Final evidence index](FINAL_EVIDENCE_INDEX.md), [Current V13 boundary](CURRENT_GOAL_BOUNDARY_V13.md) |
| Technical complexity and implementation efficiency | 8-9 / 10 | The project combines orchestration, market data readiness, forecast adapters, multi-tenant strategy evidence, API read models, and interactive dashboard flows without enabling unsafe execution. | [Architecture/data flow](ARCHITECTURE_AND_DATA_FLOW.md), [API freshness/performance](API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md), [Metrics atlas](FINAL_METRICS_ATLAS.md) |
| Code or experimental-method quality | 8 / 10 | The repository has typed API/read-model contracts, documented evidence packets, bounded claim language, final review helpers, and explicit separation of generated/local artifacts from tracked submission files. | [Repository review checklist](FINAL_REVIEW_CHECKLIST.md), [Evidence index](FINAL_EVIDENCE_INDEX.md), [Dashboard README](../../dashboard/README.md) |
| Testing, validation, or accuracy evaluation | 8-9 / 10 | The defense can show focused backend, dashboard, smoke, audit, and research validation commands, plus strategy metrics under frozen comparator boundaries. | [Verification commands](../../README.md#verification), [Final repo audit script](../../scripts/final_repo_audit.ps1), [HF demo packet](final-evidence/hf_live_safe_switch_value_aligned_shadow_demo_packet.md) |

Reasonable defended total: **40-44 / 50** if the demo stack is fresh and the
claim boundary is stated correctly. A cleaner live demo with visible non-HOLD,
HOLD/abstention, chart/regret evidence, and passing verification can support the
upper end. Overclaiming execution, full DFL, or DT deployment can quickly push
the same work into the mid-score band because the committee will grade it
against a product that was not actually built.

## What To Show For Each Criterion

| Criterion | Show in demo | Say in defense | Avoid saying |
| --- | --- | --- | --- |
| Declared-task fit | `/operator` with tenant, DAM/IDM venue, latest/today/tomorrow/day+2, selected strategy, and boundary strip. | "The product goal is operator recommendation preview, not automated trading." | "The bot trades on DAM/IDM." |
| Correct methods | `/docs` OpenAPI, read-model endpoints, gatekeeper/boundary cards, and evidence index. | "Every candidate remains a preview row and is checked against deterministic constraints and source readiness." | "ML directly controls the battery or market orders." |
| Complexity/efficiency | Dagster/read-model route, forecast-store ensure flow, dashboard charts, and multi-tenant evidence cards. | "The system connects data acquisition, forecast/read models, strategy comparison, API, and dashboard into one reproducible path." | "It is just a static dashboard." |
| Code/experiment quality | README, API docs, final evidence index, final review checklist, and clean repository audit. | "The repository separates tracked defense artifacts from generated local caches and preserves explicit claim boundaries." | "All research lanes are production-ready." |
| Testing/validation | Run or cite `scripts/verify.ps1`, `npm -C dashboard run typecheck`, `npm -C dashboard run test:unit`, `npm -C dashboard run smoke:hf-value-aligned`, and final audit. | "Validation includes unit/type checks, dashboard smoke, API probes, and strategy metrics against frozen comparators." | "A good-looking demo is the validation." |

## Evidence-To-Question Map

| Commission question | Fast answer | Evidence to open |
| --- | --- | --- |
| What exactly was implemented? | A DAM/IDM hourly BESS operator recommendation preview with read-model evidence and no market execution. | [README](../../README.md), [FINAL_DEFENSE_RUNBOOK.md](FINAL_DEFENSE_RUNBOOK.md) |
| How do you know the algorithm is correct enough for a diploma MVP? | Schedules are compared against strict LP/oracle evidence, source blockers fail closed, and safety/execution contracts are explicit. | [FINAL_METRICS_ATLAS.md](FINAL_METRICS_ATLAS.md), [FINAL_EVIDENCE_INDEX.md](FINAL_EVIDENCE_INDEX.md) |
| Why use these technologies? | Dagster gives reproducible assets, FastAPI gives inspectable read models, Pydantic gives deterministic validation, Nuxt gives the operator surface, and Postgres/forecast-store rows make the demo source-backed. | [ARCHITECTURE_AND_DATA_FLOW.md](ARCHITECTURE_AND_DATA_FLOW.md), [API_ENDPOINTS.md](API_ENDPOINTS.md) |
| Is it optimized or scalable? | The MVP focuses on reproducible read models and bounded local materialization; heavy research lanes are explicitly separated from the live demo path. | [API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md](API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md), [FINAL_REVIEW_CHECKLIST.md](FINAL_REVIEW_CHECKLIST.md) |
| What are the limitations? | Future-date previews require source-backed forecast rows; V13 source readiness can block; DT/LAVA and full DFL are research/future work; execution remains disabled. | [CURRENT_GOAL_BOUNDARY_V13.md](CURRENT_GOAL_BOUNDARY_V13.md), [DFL_UA_CONTEXT_ACQUISITION_V13.md](DFL_UA_CONTEXT_ACQUISITION_V13.md) |

## Defense Risk Controls

- If a future date has no official OREE row and no forecast-store row, show the
  blocker as correct fail-closed behavior. Do not fill it with synthetic prices.
- If HF shadow is selected, call it manual shadow/demo evidence. Do not say it
  replaced V2+ as a production default.
- If DT or LAVA appears in evidence cards, call it offline research or secondary
  evidence. Do not say it is a deployed controller.
- If source-readiness cards show missing receipts or safe-switch examples, use
  them as evidence of governance and safety gates, not as product failure.
- End every demo route with `market_execution_enabled=false`, no `ProposedBid`,
  and no market order payload.

## Minimum Passing Defense Path

1. Open [README](../../README.md) and state the product boundary.
2. Open `/operator` and show one source-backed DAM/IDM preview.
3. Show one non-HOLD action and one HOLD/abstention/fail-closed case.
4. Open `/defense` and point to V2+, DT/V2+, and HF as layered evidence with
   different promotion levels.
5. Open `/docs` or [API_ENDPOINTS.md](API_ENDPOINTS.md) and show read-model
   endpoints plus the non-execution boundary.
6. Show verification commands or recent terminal output for typecheck, unit
   tests, smoke, and final repository audit.

## Remaining Work That Should Be Framed Honestly

- OREE source/publication receipts and some safe-switch evidence remain
  acquisition gates, not completed market-execution readiness.
- Full differentiable DFL and DT/LAVA policy deployment are future research
  paths. The current defended artifact is an operator-preview/read-model system.
- The dashboard is a decision-support surface. It is not a settlement system,
  SCADA controller, or exchange integration.
- Public GitHub quality still depends on keeping generated artifacts, local
  caches, third-party PDFs, and stale screenshots out of the tracked submission.
