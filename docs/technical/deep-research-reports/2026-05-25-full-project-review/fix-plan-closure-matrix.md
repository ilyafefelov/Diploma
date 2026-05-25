# Fix-Plan Closure Matrix

Date: 2026-05-25

Purpose: single defense-ready status map for every `fix-plan.md` item. A row is closed only when there is current proof. Rows that still need implementation are not counted as closed.

## Current Command Snapshot

| Check | Current Evidence | Status |
|---|---|---|
| Ruff | `uv run ruff check .` -> `All checks passed!` | Passed |
| Mypy | `uv run mypy .` -> `Success: no issues found in 247 source files` | Passed |
| Pytest | `uv run pytest -p no:cacheprovider tests` -> 945 passed in 616.85s | Passed |
| Dagster defs check | `uv run dg check defs` -> all component YAML validated and definitions loaded | Passed |
| Dagster defs list | `uv run dg list defs --json` exit code 0 | Passed |
| Compose config | `docker compose config --quiet` exit code 0 | Passed |
| Dashboard typecheck | `npm run typecheck` in `dashboard` completed | Passed |
| Dashboard Vitest | `npm exec -- vitest run` in `dashboard` -> 12 files passed, 66 tests passed | Passed |
| Stale verification wording | Search for stale full-verification/current-state pass-count phrases returned no active hits | Passed |
| Market execution boundary | Search for enabled market-execution flag text found only guard/error/prohibition/diagnostic wording | Boundary preserved |
| Operator dock browser evidence | Browser desktop plus Playwright desktop/mobile JSON/screenshots under `assets/` show `noDockOcclusion=true`, `noHorizontalOverflow=true`, and zero console/page errors | Passed |

## Closure Rows

| Fix-plan item | Current status | Proof | Next action |
|---|---|---|---|
| 1. Restore full verification | Closed | Ruff, Mypy, Pytest, Dagster defs check/list, Compose config, dashboard typecheck, and dashboard Vitest pass | Keep this matrix current if new code changes land |
| 2. Fix operator schedule dock occlusion | Closed | C1 captured the defect; post-fix Browser/Playwright evidence shows the dock is normal-flow (`position: relative`) and does not occlude content on desktop/mobile | Keep `operator-hud.css` normal-flow dock contract covered by Vitest |
| 3. Correct stale verification claims | Closed | README has a dated full-green command matrix; historical Week 3 artifacts are labeled historical; stale phrase search has no active hits | Keep README updated after future verification reruns |
| 4. Add artifact paths for V4/V5/Poland claims | Closed for current local evidence | `docs/thesis/appendices/evidence-manifest.md` maps major claims; Chapter 4 softens V4/V5 missing-packet claims and links Poland packet paths/run IDs | If V4/V5 packets are found outside `data/research_runs`, add exact artifact paths; otherwise keep draft/pending-evidence wording |
| 5. Quarantine stale duplicate contracts | Closed | `src/gatekeeper/schemas.py` now starts with an obsolete legacy-schema warning; import search shows active code imports canonical `smart_arbitrage.gatekeeper.schemas`; `tests/test_market_rules.py` passed | Remove the legacy file in a later explicit cleanup if desired |
| 6. Make V13 acquisition actionable | Closed as external acquisition blocker | Latest V13 preflight snapshot records missing DAM publication receipts and safe-switch CSVs while preserving `market_execution_enabled=false`, `dt_lava_ready=false`, and `permits_model_training=false` | Acquire source-backed OREE DAM receipt rows and source-backed safe-switch examples; validate with the documented validator CLIs before rerunning preflight |
| 7. Build thesis evidence manifest | Closed | `docs/thesis/appendices/evidence-manifest.md` maps V2+, TFT, Poland, DT shadow, LAVA smoke, V13, and V4/V5 gaps to artifacts/hashes/boundaries | Keep manifest updated when new packets are materialized |
| 8. Strengthen dashboard verification | Closed | In-app Browser desktop evidence and Playwright desktop/mobile evidence check page identity, nonblank content, no framework overlay, console/page errors, overflow, dock occlusion, and market-execution wording | Keep screenshots/JSON in `assets/` with future UI changes |

## Remaining Required Work

No local implementation work remains for `fix-plan.md`.

Remaining external blocker:

- V13 acquisition/source-readiness still needs source-backed OREE DAM receipt rows and source-backed safe-switch examples. This remains outside the current local closure because it depends on external source data/credentials. Do not describe V13 as DT/LAVA-ready, training-ready, or market-executable until that acquisition evidence passes the documented validators and preflight.

## Non-Negotiable Boundary

V13 is closed only as an external acquisition blocker; do not convert missing DAM receipts or missing safe-switch examples into readiness, DT/LAVA promotion, or market execution.
