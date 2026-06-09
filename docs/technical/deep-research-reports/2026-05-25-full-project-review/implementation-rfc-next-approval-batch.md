# Implementation RFC - Next Approval Batch

Date: 2026-05-25

Purpose: prepare the next code/layout changes needed to close `fix-plan.md` while preserving current behavior boundaries and `market_execution_enabled=false`.

## Scope

This batch covers the remaining local edits that need approval before implementation:

1. A1: fix the remaining Mypy failure in `sitecustomize.py`.
2. C2: fix operator schedule dock occlusion.
3. E2: quarantine or clearly obsolete-mark the stale duplicate gatekeeper schema.

## A1 - `sitecustomize.py` Mypy Fix

Current behavior:

- On Windows only, `sitecustomize.py` reads `PROCESSOR_ARCHITEW6432`, `PROCESSOR_ARCHITECTURE`, and `PROCESSOR_IDENTIFIER`.
- If present, it replaces `platform.machine` and `platform.processor` with zero-argument lambdas to avoid slow/blocking Windows WMI paths during imports.
- Original Mypy output failed only on these lambdas because their closures were inferred as returning `str | None`.

Files to modify:

- `sitecustomize.py`
- Optional: `tests/test_sitecustomize.py` only if strict runtime-test-first is required.

Proposed change:

- Keep the Windows-only behavior identical.
- Bind concrete local `str` values before assigning the lambdas, or use a tiny typed helper closure returning `str`.
- Do not change env var names, platform patch timing, or non-Windows behavior.

Risk: low.

Verification:

```powershell
uv run ruff check .
uv run mypy .
```

## C2 - Operator Schedule Dock Occlusion

Current behavior:

- `.schedule-dock` is `position: fixed` at the bottom of the viewport.
- `.operator-shell` reserves fixed bottom padding: `12rem` desktop and `15.5rem` mobile.
- Browser evidence captured on 2026-05-25 shows the dock covering a large part of the viewport: desktop 38.47% and mobile 84.14%.

Files to modify:

- `dashboard/app/assets/css/operator-hud.css`
- `dashboard/app/components/dashboard/operator/OperatorScheduleDock.vue` only if CSS-only layout cannot satisfy the measured acceptance checks.

Proposed change:

- Preferred first pass: make `.schedule-dock` participate in normal document layout on constrained/mobile viewports and avoid relying on fixed bottom padding.
- For desktop, either keep a much smaller sticky/fixed dock with adequate reserved space or convert it to normal flow if that produces the most stable layout.
- Preserve the hourly recommendation table, shadow-preview wording, and no-market-execution labels.

Risk: medium, because layout changes affect defense screenshots and mobile ergonomics.

Verification:

```powershell
cd dashboard
npm run typecheck
npm exec -- vitest run
```

Browser verification:

- `/operator` desktop and mobile screenshots.
- No horizontal overflow.
- No console errors.
- Dock does not cover main content.
- No market-execution wording regression.

## E2 - Stale Duplicate Gatekeeper Schema

Current behavior:

- Active code imports canonical contracts from `smart_arbitrage.gatekeeper.schemas`.
- `src/gatekeeper/schemas.py` is a stale duplicate and contains `BALANCING: 16000.0`, while the canonical active cap is `17000.0`.
- Import search found no active code importing `src/gatekeeper/schemas.py`.

Files to modify:

- `src/gatekeeper/schemas.py`
- Possibly docs references that mention this path as a risk.

Proposed change:

- Add an unmistakable module-level obsolete warning pointing to `smart_arbitrage.gatekeeper.schemas`.
- Prefer not to move or delete the file in this batch; marking it obsolete is the smallest safe change.
- Do not alter canonical `src/smart_arbitrage/gatekeeper/schemas.py`.

Risk: low.

Verification:

```powershell
rg -n "from src\\.gatekeeper|import gatekeeper|src/gatekeeper|MARKET_PRICE_CAPS" src api tests docs -g "*.py" -g "*.md"
uv run pytest -p no:cacheprovider tests/test_market_rules.py
uv run ruff check .
uv run mypy .
```

## Batch Exit Criteria

- A1 Mypy blocker is gone.
- C2 browser evidence proves the dock no longer occludes content on desktop/mobile.
- E2 prevents reviewers from mistaking the stale duplicate schema for the canonical active contract.
- The review verification matrix is updated with new command/browser evidence.
- `market_execution_enabled=false` remains unchanged.
