# Fix-Plan Closure Goal

Date: 2026-05-25

This file turns `fix-plan.md` into an execution objective with acceptance criteria. It is intentionally a planning artifact: no code behavior is changed by this document.

## Proposed Goal

Close every actionable item from the 2026-05-25 full project review so the Diploma project can be defended as a verified offline DAM recommendation-preview evidence system, with clean verification, non-overlapping dashboard UX, current thesis/documentation claims, explicit artifact traceability, and preserved no-market-execution boundaries.

## Definition of Done

The goal is complete only when all of these are true:

1. Full repo verification is green or any remaining non-green command is explicitly out of scope with evidence.
2. `/operator` no longer hides content behind the schedule dock on desktop or mobile, proven by browser evidence.
3. Active docs no longer claim stale verification counts such as `151 passed`.
4. Every V4/V5/Poland thesis claim either has a local artifact path/run ID/validation command or is softened as draft/pending evidence.
5. The stale duplicate `src/gatekeeper/` contract cannot mislead reviewers about the active market caps.
6. V13 acquisition remains actionable but correctly blocked until explicit DAM publication receipts and safe-switch evidence are available.
7. A thesis evidence manifest maps major claims to artifacts, commands, dates, and sources.
8. Dashboard verification includes checks for console errors, overflow, overlay occlusion, mobile readability, and no-market-execution wording.

## Closure Evidence Snapshot

Checked on 2026-05-25:

- `uv run ruff check .` passed.
- `uv run mypy .` passed across 247 source files.
- `uv run pytest -p no:cacheprovider tests` passed: 945 tests in 616.85s.
- `uv run dg check defs` passed.
- `uv run dg list defs --json` passed.
- `docker compose config --quiet` passed.
- Dashboard `npm run typecheck` passed.
- Dashboard `npm exec -- vitest run` passed: 12 files, 66 tests.
- `README.md` now states the current 2026-05-25 full-green command matrix.
- `docs/technical/assets/week3-architecture-api-reconciliation-infographic.svg:61` now labels the old pytest count as historical.
- `docs/technical/WEEK3_ARCHITECTURE_API_FOLLOWUP_REPORT.md:97` now labels the old full-verification count as historical.
- `/operator` dock post-fix evidence is stored in `assets/operator-dock-browser-desktop-after.json`, `assets/operator-dock-playwright-desktop-1440x1100-after.json`, and `assets/operator-dock-playwright-mobile-390x844-after.json`; desktop/mobile checks report `noDockOcclusion=true` and `noHorizontalOverflow=true`.
- `src/gatekeeper/schemas.py` now starts with an obsolete legacy-schema warning; active `src/smart_arbitrage/gatekeeper/schemas.py` remains canonical.
- V4/V5 run IDs are present in `docs/thesis/chapters/04-results-and-discussion.md`, but no matching local `data/research_runs/` packet was found by exact run ID search.
- Poland prior-veto evidence exists at `data/research_runs/week3_poland_lag24_prior_tail_risk_veto/`; it improves mean/median but does not pass the 5% replacement threshold.

## Execution Order

### Phase 1 - Verification Baseline

Objective: make the current verification story honest and reproducible.

Tasks:

- Fix the remaining `sitecustomize.py` Mypy issue with the smallest local typing change.
- Run `uv run ruff check .` and `uv run mypy .`.
- Run the slower checks after the fast lane is green:
  - `uv run pytest -p no:cacheprovider tests`
  - `uv run dg check defs`
  - `uv run dg list defs --json`
  - `docker compose config --quiet`

Acceptance:

- Mypy passes.
- README and current docs state the latest command matrix accurately.

Risk: low.

### Phase 2 - Dashboard Dock and Browser Verification

Objective: remove the operator-page occlusion defect and make it testable.

Tasks:

- Fix `.schedule-dock` layout or bottom spacing.
- Add or update browser/visual checks for desktop and mobile.
- Re-run dashboard typecheck and Vitest.
- Capture fresh screenshots for `/operator` and `/defense`.

Acceptance:

- No horizontal overflow.
- No console errors.
- Fixed/sticky overlay does not cover main content.
- Mobile screenshot is readable.
- Dashboard still preserves no-market-execution wording.

Risk: medium.

### Phase 3 - Documentation and Artifact Traceability

Objective: make thesis and active docs defensible.

Tasks:

- Replace active stale verification claims.
- Mark historical stale counts as dated/historical when they are intentionally preserved.
- Add artifact paths for V4/V5 or soften the exact claims until packets are present.
- Confirm Poland claims point to the current near-miss/prior-veto packet and do not imply promotion.
- Build a thesis evidence manifest with claim, artifact, command, date, and boundary.

Acceptance:

- No active current-state doc says `151 passed`.
- V4/V5/Poland claims are traceable or explicitly pending.
- Manifest exists and covers headline Chapter 4 claims.

Risk: medium.

### Phase 4 - Legacy Contract Quarantine

Objective: prevent stale duplicate contracts from misleading reviewers.

Tasks:

- Verify no active imports depend on `src/gatekeeper/schemas.py`.
- Either remove it, move it to an explicit legacy/quarantine path, or add an unmistakable obsolete warning plus canonical import guidance.
- Preserve active `smart_arbitrage.gatekeeper.schemas` behavior and tests.

Acceptance:

- `rg` shows no active code imports the stale path.
- Reviewers cannot reasonably mistake the stale `16000.0` cap for the active contract.
- Market-rule tests still pass.

Risk: low.

### Phase 5 - V13 Actionability

Objective: keep V13 as a real acquisition backlog, not a vague blocker.

Tasks:

- Run the V13 acquisition preflight.
- Document the exact missing receipt and safe-switch evidence.
- If source-backed CSVs exist, validate them first; otherwise leave V13 blocked with next acquisition steps.

Acceptance:

- V13 status is current and explicit.
- `market_execution_enabled=false`, `dt_lava_ready=false`, and `permits_model_training=false` remain fixed unless source evidence truly changes.

Risk: high because real closure depends on external data/credentials.

## Final Local Status

The local fix-plan rows are closed and verified. The remaining non-local condition is V13 source acquisition: explicit DAM publication receipts and safe-switch examples must be obtained and validated before any DT/LAVA readiness, model-training, or market-execution claim.
