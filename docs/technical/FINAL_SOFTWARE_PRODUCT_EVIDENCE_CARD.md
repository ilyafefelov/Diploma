# Final Software Product Evidence Card

Date: 2026-06-18

Purpose: one-page commission aid for the 50-point software product /
experimental-part rubric. Use it during the live defense or when a reviewer
opens the repository without the demo running.

Defensible product statement:

> Source-backed DAM/IDM hourly operator recommendation preview for BESS
> arbitrage, with offline strategy evidence, deterministic safety boundaries,
> and `market_execution_enabled=false`.

Do not defend this repository as a trading bot, market-submission engine,
deployed DT controller, full DFL controller, or production dispatch system.

## Rubric-To-Evidence Map

| Criterion | High-level claim to defend | Open first | Verification / proof path |
| --- | --- | --- | --- |
| Implementation matches the declared task | The implemented product is a human operator preview for DAM/IDM hourly decisions, with tenant, venue, target-date, source-readiness, and strategy evidence controls. | [README](../../README.md), `/operator`, [Final defense runbook](FINAL_DEFENSE_RUNBOOK.md) | Show tenant `client_003_dnipro_factory`, DAM then IDM, latest/today/tomorrow/day+2, one non-HOLD preview, and one HOLD/source-blocked case. |
| Correct algorithms / solution | Price context is source-first; schedules are evaluated through feasible LP/oracle and V2+ evidence; HF/DT lanes are guarded shadow selectors with abstention. | [Metrics atlas](FINAL_METRICS_ATLAS.md), [Evidence index](FINAL_EVIDENCE_INDEX.md), [API endpoints](API_ENDPOINTS.md) | Explain `market_execution_enabled=false`, no `ProposedBid`, source-backed forecast rows for unpublished horizons, deterministic safety gates, and regret/value scoring. |
| Technical complexity and efficiency | The repo connects Dagster assets, forecast-store materialization, FastAPI read models, Nuxt operator UI, source governance, and multi-strategy evidence without enabling unsafe execution. | [Architecture/data flow](ARCHITECTURE_AND_DATA_FLOW.md), [API freshness/performance](API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md), Dagster filters in the runbook | Run or cite `uv run dg list defs --assets "tag:operator_preview=true"` and `uv run dg list defs --assets "tag:read_model_boundary=not_market_execution"`. |
| Code / experimental method quality | Final entry points are documented; generated/runtime artifacts are excluded; primary demo lanes are separated from supporting research; claims map to tracked evidence. | [Repository review checklist](FINAL_REVIEW_CHECKLIST.md), [Final evidence index](FINAL_EVIDENCE_INDEX.md), [Dashboard README](../../dashboard/README.md) | Show clean Git status, curated evidence paths under `docs/`, API docs, and the primary-vs-supporting lane boundary. |
| Testing, validation, accuracy evaluation | The defense has repeatable checks for code quality, dashboard correctness, Dagster definitions, final repo hygiene, API behavior, and strategy metrics. | [README verification](../../README.md#verification), [final repo audit](../../scripts/final_repo_audit.ps1) | Run or cite final audit, `npm -C dashboard run typecheck`, `npm -C dashboard run test:unit`, `uv run dg check defs`, and focused API/research tests. |

## Five-Minute Commission Walkthrough

1. Open [README](../../README.md) and state the product boundary in one
   sentence.
2. Open `/operator` and show tenant, DAM/IDM venue, target-date controls, and
   the first-viewport boundary strip.
3. Show a normal preview row and a fail-closed HOLD/source-readiness case.
4. Open `/defense` and point to V2+ as headline evidence, DT/V2+ as secondary
   shadow evidence, and HF value-aligned as manual shadow/demo evidence.
5. Open FastAPI `/docs` or [API endpoints](API_ENDPOINTS.md) and show that the
   API is a read-model surface, not a market order surface.
6. Show verification output or the commands under [README verification](../../README.md#verification).

## Answer Template For Each Rubric Item

| Question from commission | Short answer |
| --- | --- |
| Does the implementation match the topic? | Yes. The delivered surface is a DAM/IDM hourly BESS operator preview, exactly matching the defended scope. It is intentionally not an execution engine. |
| Why are the methods correct? | The system uses source-backed price context, feasible LP/oracle comparison, regret/value metrics, Pydantic safety boundaries, and fail-closed blockers instead of synthetic fallback prices. |
| Where is the technical complexity? | The complexity is in the integrated pipeline: Dagster materialization, forecast-store rows, multi-tenant read models, FastAPI contracts, Nuxt dashboard, and strategy evidence gates. |
| How is code quality demonstrated? | The repo has clear entry points, API docs, final runbooks, an evidence index, generated-artifact exclusions, and explicit primary-vs-supporting lane boundaries. |
| How is validation demonstrated? | The final audit, dashboard typecheck/unit tests, Dagster definitions validation, API tests, smoke checks, and frozen strategy metrics provide repeatable validation. |

## Risk Controls

- If future-date data is missing, show the source-readiness blocker. Do not
  present synthetic prices.
- If HF/DT abstains, explain that abstention is the safe behavior under weak
  evidence.
- If asked about execution, repeat: no `ProposedBid`, no market payload,
  `market_execution_enabled=false`.
- If asked about unfinished research, frame DFL/DT/V13 as supporting/future
  research lanes, not the delivered product.

## Defensible Score Band

With a fresh local demo and the final audit passing, a reasonable defense target
for this 50-point section is **40-44 / 50**. The main way to lose points is not
the implementation itself, but overclaiming it as live trading, full DFL, or a
deployed Decision Transformer controller.
