# Fix-Plan Execution Backlog

Date: 2026-05-25

This backlog decomposes `fix-plan.md` and `fix-plan-closure-goal.md` into independently executable work items. It is ordered to remove the highest defense risk first while preserving the no-market-execution boundary.

## Epic A - Verification Baseline

### A1. Fix remaining Mypy failure in `sitecustomize.py`

Status: complete on 2026-05-25

Files:

- `sitecustomize.py`
- optional `tests/test_sitecustomize.py` if strict runtime-test-first is chosen

Current evidence:

- `uv run mypy sitecustomize.py` originally failed at `sitecustomize.py:20` and `sitecustomize.py:22`.
- `sitecustomize.py` now binds concrete `str` values before assigning the platform lambdas.
- `uv run mypy .` now passes across 247 source files.
- Detailed RFC: `implementation-rfc-a1-sitecustomize-mypy.md`.
- Next-batch approval RFC: `implementation-rfc-next-approval-batch.md`.

Implementation intent:

- Keep behavior identical.
- Narrow `_machine` and `_processor` to concrete `str` values before assigning lambdas, or replace lambdas with tiny typed helper closures.

Acceptance:

- `uv run mypy .` passes.
- `uv run ruff check .` passes.

Risk: low.

Approval status: implemented under the active fix-plan goal.

### A2. Run fast verification lane

Status: complete on 2026-05-25

Commands:

```powershell
uv run ruff check .
uv run mypy .
```

Acceptance:

- Both commands pass.

Risk: low.

### A3. Run full verification lane

Status: complete on 2026-05-25

Commands:

```powershell
uv run pytest -p no:cacheprovider tests
uv run dg check defs
uv run dg list defs --json
docker compose config --quiet
```

Acceptance:

- Pytest passes.
- Dagster definitions validate.
- Dagster definitions list reports asset/check/job/schedule counts.
- Compose config is valid.

Evidence:

- `uv run pytest -p no:cacheprovider tests` -> 945 passed in 616.85s.
- `uv run dg check defs` -> all component YAML validated and all definitions loaded.
- `uv run dg list defs --json` -> exit code 0.
- `docker compose config --quiet` -> exit code 0.

Risk: medium because pytest is slow.

## Epic B - Current Verification Claims

### B1. Replace active stale README verification claim

Status: complete as full-green update on 2026-05-25

Files:

- `README.md`

Current evidence:

- `uv run ruff check .` passed on 2026-05-25.
- `uv run mypy .` passed on 2026-05-25.
- `README.md` now states the full-green closure snapshot.

Acceptance:

- README contains a current command matrix with date and pass/fail status.
- README does not imply full green without dated command evidence.

Risk: low.

### B2. Classify stale historical verification artifacts

Status: complete on 2026-05-25

Files:

- `docs/technical/assets/week3-architecture-api-reconciliation-infographic.svg`
- `docs/technical/WEEK3_ARCHITECTURE_API_FOLLOWUP_REPORT.md`

Acceptance:

- If these are historical artifacts, they are clearly dated/historical.
- If reused as current artifacts, stale `151 passed` is replaced.

Evidence:

- `docs/technical/WEEK3_ARCHITECTURE_API_FOLLOWUP_REPORT.md` now says `Historical full verification at report time`.
- `docs/technical/assets/week3-architecture-api-reconciliation-infographic.svg` now says `Historical pytest: 151 passed`.

Risk: low.

## Epic C - Operator Dashboard Dock

### C1. Characterize current dock layout with browser evidence

Status: complete on 2026-05-25

Files/targets:

- `/operator`
- `dashboard/app/assets/css/operator-hud.css`
- `dashboard/app/components/dashboard/operator/OperatorScheduleDock.vue`

Evidence:

- `operator-dock-c1-evidence.md`
- `assets/operator_c1_measurements_calibrated.json`
- `assets/operator_c1_desktop_css_1440x1100.png`
- `assets/operator_c1_mobile_css_390x844.png`
- Next-batch approval RFC: `implementation-rfc-next-approval-batch.md`

Acceptance:

- Browser measurement records viewport, dock rect, document scroll height, horizontal overflow, console errors, and screenshot path.

Risk: low.

### C2. Fix dock occlusion

Status: complete on 2026-05-25

Implemented change:

- `.schedule-dock` participates in normal layout with `position: relative`.
- `.operator-shell` no longer reserves fixed bottom padding for a fixed overlay.
- `dashboard/app/utils/operatorHudCss.test.ts` asserts the no-overlay CSS contract.

Acceptance:

- Dock no longer covers main content on desktop or mobile.
- No horizontal overflow.
- No console errors.
- Dashboard wording still preserves no-market-execution boundary.

Risk: medium.

### C3. Add dashboard verification coverage

Status: complete on 2026-05-25

Files:

- `dashboard/app/utils/operatorHudCss.test.ts`
- `operator-dock-c1-evidence.md`
- `assets/operator-dock-browser-desktop-after.json`
- `assets/operator-dock-playwright-desktop-1440x1100-after.json`
- `assets/operator-dock-playwright-mobile-390x844-after.json`

Acceptance:

- Check covers console errors, overflow, overlay occlusion, mobile readability, and no-market-execution wording.
- `npm run typecheck` and `npm exec -- vitest run` pass in `dashboard`.

Evidence:

- In-app Browser desktop evidence: `noDockOcclusion=true`, `horizontalOverflowPx=0`, no warning/error logs.
- Playwright desktop/mobile evidence: `noDockOcclusion=true`, `noHorizontalOverflow=true`, zero console/page errors.
- `npm run typecheck` passed.
- `npm exec -- vitest run` passed: 12 files, 66 tests.

Risk: medium.

## Epic D - Thesis Claim Traceability

### D1. Build thesis evidence manifest

Status: complete on 2026-05-25

New file:

- `docs/thesis/appendices/evidence-manifest.md`

Evidence:

- Manifest covers V2+, TFT, Poland richer calibrated, Poland prior-veto, DT shadow, LAVA smoke, V13, and V4/V5 traceability gaps.

Acceptance:

- Manifest maps major Chapter 4 claims to artifact path, run ID or slug, command, date, status, and claim boundary.
- V2+, TFT, Poland, DT shadow, LAVA, and V13 are all covered.

Risk: low.

### D2. Harden V4/V5 claims

Status: complete on 2026-05-25

Files:

- `docs/thesis/chapters/04-results-and-discussion.md`
- supporting appendices

Evidence:

- Chapter 4 now marks the V4 and V5 run-ID claims as draft/pending-evidence because local `data/research_runs/` packet paths were not found.
- Exact V4/V5 numeric claims are no longer presented as defended thesis results without packet paths.

Acceptance:

- V4/V5 claims include exact artifact paths when available.
- If local packets are missing, wording says draft/pending artifact link rather than presenting untraceable final numeric evidence.

Risk: medium.

### D3. Harden Poland claims

Status: complete on 2026-05-25

Files:

- `docs/thesis/chapters/04-results-and-discussion.md`
- evidence manifest

Evidence:

- Chapter 4 points Poland near-miss and prior-veto claims to local packet paths and Dagster run IDs.
- Evidence manifest records the richer calibrated Poland packet and the prior-veto packet with hashes and non-promotion boundaries.

Acceptance:

- Poland near-miss and prior-veto claims point to local packets.
- Wording says not promoted unless promotion gates pass.
- European rows are not described as Ukrainian training targets.

Risk: medium.

## Epic E - Legacy Contract Quarantine

### E1. Confirm stale path is inactive

Status: complete on 2026-05-25

Commands:

```powershell
rg -n "from src\\.gatekeeper|import gatekeeper|src/gatekeeper|MARKET_PRICE_CAPS" src api tests docs -g "*.py" -g "*.md"
```

Acceptance:

- No active code imports `src/gatekeeper/schemas.py`.

Evidence:

- `rg` showed active imports use `smart_arbitrage.gatekeeper.schemas`.
- Matches for `src/gatekeeper/schemas.py` were the stale file itself and review/planning docs, not active imports.
- Next-batch approval RFC: `implementation-rfc-next-approval-batch.md`

Risk: low.

### E2. Quarantine or mark obsolete `src/gatekeeper/schemas.py`

Status: complete on 2026-05-25

Implemented:

- Added an unmistakable module-level obsolete warning pointing to `smart_arbitrage.gatekeeper.schemas`.
- Kept the file in place to avoid a move/delete in this closure batch.

Acceptance:

- Reviewers cannot reasonably mistake `BALANCING: 16000.0` for the active cap.
- Active market-rule tests pass.

Evidence:

- `rg -n "from src\\.gatekeeper|import gatekeeper|src/gatekeeper|MARKET_PRICE_CAPS" src api tests docs -g "*.py" -g "*.md"` shows active code imports the canonical `smart_arbitrage.gatekeeper.schemas`.
- `uv run pytest -p no:cacheprovider tests/test_market_rules.py` passed.

Risk: low.

## Epic F - V13 Actionability

### F1. Refresh V13 preflight

Status: complete on 2026-05-25

Command:

```powershell
.\.venv\Scripts\python.exe scripts\preflight_ua_context_v13_acquisition_inputs.py --config configs\real_data_dfl_ua_context_v13_acquisition_week3.yaml --output .tmp_runtime\v13_acquisition_inputs_preflight.json
```

Acceptance:

- Current blocker state is captured.
- `market_execution_enabled=false`, `dt_lava_ready=false`, and `permits_model_training=false` remain fixed unless source evidence truly changes.

Evidence:

- `v13-f1-preflight-evidence.md`
- `assets/v13_acquisition_inputs_preflight_2026-05-25.json`

Risk: low to medium.

### F2. Document acquisition backlog

Status: complete on 2026-05-25

Acceptance:

- Missing explicit DAM receipt rows and safe-switch counts are documented.
- Next actions are concrete: which CSVs, validators, and source evidence are needed.

Evidence:

- `v13-f2-acquisition-backlog.md`

Risk: medium to high due external data dependencies.

## Recommended Execution Sequence

All local items in A through E are closed and verified. F remains closed as an external acquisition blocker: source-backed DAM receipts and source-backed safe-switch examples are required before rerunning V13 preflight toward readiness.
