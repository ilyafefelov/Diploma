# Fix and Improvement Plan

Closure status, 2026-05-25: local actionable rows are closed and verified. Full local verification is green; the operator dock no longer occludes content on desktop/mobile browser evidence; stale active verification claims were replaced; Chapter 4 claims are traceable or softened; the stale duplicate gatekeeper schema is marked obsolete; and V13 remains an external acquisition/source-readiness blocker, not DT/LAVA readiness or market execution.

## Immediate Fixes

### 1. Restore full verification

Files:

- `sitecustomize.py`
- `api/main.py`

Actions:

- Fix Windows platform lambda typing in `sitecustomize.py`.
- Add typed extraction or narrowed helper models for `schedule_input` rows in `api/main.py`.
- Narrow `explicit_pairs` before calling `float(before)` and `float(after)`.
- Re-run:
  - `uv run ruff check .`
  - `uv run mypy .`
  - `uv run pytest -p no:cacheprovider tests`
  - `uv run dg check defs`

Risk: low to medium. The changes should be local, but API shadow preview typing touches product-facing read-model helpers.

### 2. Fix operator schedule dock occlusion

Files:

- `dashboard/app/assets/css/operator-hud.css`
- likely `dashboard/app/components/dashboard/operator/OperatorScheduleDock.vue`

Actions:

- Either make the dock part of normal layout or reserve exact bottom space dynamically.
- Add a visual regression check for desktop and mobile.
- Re-run dashboard typecheck and vitest.

Risk: medium. Layout changes can affect defense screenshots and mobile behavior.

### 3. Correct stale verification claims

Files:

- `README.md`
- any reused active architecture infographic or current report page that says `151 passed`
- PR text if it will be used for defense

Actions:

- Replace stale "latest full verification" with current command matrix.
- If Mypy is still failing, state that explicitly.

Risk: low.

## Short-Term Thesis Hardening

### 4. Add artifact paths for every V4/V5/Poland claim

Files:

- `docs/thesis/chapters/04-results-and-discussion.md`
- online Google Docs thesis draft
- supporting appendices

Actions:

- Add run slug, packet path, validation command, and claim boundary for each headline paragraph.
- If a packet is not present locally, label the claim as "draft pending artifact link" or remove exact numeric claims.

Risk: medium. This may change thesis wording but improves defensibility.

### 5. Quarantine stale duplicate contracts

Files:

- `src/gatekeeper/schemas.py`
- `pyproject.toml`
- docs that reference legacy paths

Actions:

- Either remove the stale duplicate, rename it as legacy, or add a clear module-level obsolete warning.
- Prefer importing canonical constants from `src/smart_arbitrage/gatekeeper/schemas.py` if the file must remain.

Risk: low if no active imports exist; verify with `rg`.

## Medium-Term Evidence Improvements

### 6. Make V13 acquisition actionable

Actions:

- Acquire explicit DAM publication receipts with row-level `timestamp` and `source_publication_timestamp`.
- Validate safe-switch examples until each tenant/source has at least 20 prior/train non-tail-risk material examples.
- Keep `dt_lava_ready=false` until both gates pass.

Risk: high because it depends on external data/credentials.

### 7. Build a thesis evidence manifest

Actions:

- Create a single manifest table mapping every claim to command, artifact, date, and source.
- Include local hashes for packets used in Chapter 4.

Risk: low.

### 8. Strengthen dashboard verification

Actions:

- Add browser checks for:
  - no console errors,
  - no horizontal overflow,
  - fixed/sticky overlay not covering main content,
  - mobile readability,
  - no market-execution language.

Risk: medium.

## Research Roadmap

1. Treat Poland features as a governed exogenous-feature lane until rolling robustness improves.
2. Treat TFT as a forecast adapter until it improves downstream schedule value.
3. Treat DT shadow as a candidate-index/schedule-family sequence-policy research lane.
4. Treat LAVA as contract/prototype evidence until V13 source readiness and promotion gates pass.
5. Do not start market-submittable bidding until governance, receipts, settlement assumptions, and safety approvals are explicit.
