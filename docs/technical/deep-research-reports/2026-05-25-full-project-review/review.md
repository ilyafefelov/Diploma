# Critical Review

Review date: 2026-05-25

## Executive Verdict

The project is coherent as an offline evidence system for DAM delivery-day operator recommendation preview. The thesis should not claim live bidding, deployed DFL, deployed Decision Transformer control, LAVA promotion, or market-submittable bid generation. The current evidence supports this narrower story:

1. Ukrainian DAM recommendation preview is the product surface.
2. Strict LP/oracle remains the frozen comparator and safety reference.
3. V2+ is the strongest offline research challenger and thesis headline.
4. TFT, Poland lagged features, DT shadow, and LAVA NPZ smoke are valuable research branches, but not promoted.
5. V13 is a source-readiness/acquisition gate, not a modeling slice.

Closure update on 2026-05-25: the local verification lane is green, the `/operator` schedule dock no longer overlays content on desktop/mobile evidence viewports, and the stale duplicate `src/gatekeeper/schemas.py` is clearly marked obsolete. The remaining non-local blocker is V13 acquisition: explicit DAM publication receipts and sufficient source-backed safe-switch examples are still required before DT/LAVA readiness, model training, or market execution claims.

## Findings

### P1 - Current verification was not green at review start; closed in closure pass

Original review command results:

- `uv run ruff check .` passed.
- `uv run mypy .` failed with 11 errors.
- `uv run pytest -p no:cacheprovider tests` passed: 944 tests.
- `uv run dg check defs` passed.
- `uv run dg list defs --json` loaded 335 assets, 63 asset checks, 2 jobs, 2 schedules, 0 sensors.
- `docker compose config --quiet` passed.
- `dashboard`: `npm run typecheck` passed.
- `dashboard`: `npm exec -- vitest run` passed, 64 tests.

Current closure refresh is recorded in `verification-refresh-2026-05-25.md`: Ruff passes, Mypy passes across 247 source files, full pytest passes with 945 tests, `dg check defs` passes, `dg list defs --json` passes, Compose config passes, dashboard typecheck passes, and dashboard Vitest passes with 66 tests.

Current closure rerun:

- `uv run ruff check .` passed.
- `uv run mypy .` passed.
- `uv run pytest -p no:cacheprovider tests` passed: 945 tests in 616.85s.

Specific original Mypy failures observed and closed:

- `sitecustomize.py:20` and `sitecustomize.py:22`: lambdas return values still inferred as `str | None`.
- `api/main.py:4756-4759` and `api/main.py:4807-4810`: untyped `schedule_input[...]` values flow into `int`, `datetime`, and `float` conversions as `object`.
- `api/main.py:5003`: `float(after)` and `float(before)` are still typed as possibly `None`.

Closure risk: future README or thesis updates must keep the command matrix dated; do not reuse older pass counts as current verification.

### P1 - Operator dashboard fixed dock occluded content; closed in C2

The original `/operator` page loaded and showed live values after waiting, but the schedule dock was a fixed overlay with a measured height of about 431 px at 1440 x 1100. The shell padding was only `12rem` on desktop and `15.5rem` on mobile.

Evidence:

- CSS shell padding is defined in `dashboard/app/assets/css/operator-hud.css:2-8`.
- `.schedule-dock` uses `position: fixed` in `dashboard/app/assets/css/operator-hud.css:1826-1835`.
- Mobile shell padding is defined in `dashboard/app/assets/css/operator-hud.css:2215-2219`.
- Captured screenshot: [assets/operator_desktop_after_8s.png](assets/operator_desktop_after_8s.png).

Closure evidence: `operator-dock-c1-evidence.md` records the original defect and the C2 fix. Post-fix Browser/Playwright artifacts show `noDockOcclusion=true`, `noHorizontalOverflow=true`, zero console/page errors, and no forbidden market-execution wording on desktop and mobile viewports.

### P1 - README verification statement was stale

Original evidence: `README.md:196` presented the latest verification as:

```text
151 passed
```

This was not compatible with the current test count and Mypy failure at review time. Historical reports may keep old counts if clearly dated, but the active README should not present them as latest verification. Closure work on 2026-05-25 replaced this with the full-green command matrix.

Risk: reviewers and supervisors will distrust the evidence trail if the first verification statement they see is stale.

### P2 - Legacy duplicate gatekeeper schema contains stale market cap

Canonical code:

- `src/smart_arbitrage/gatekeeper/schemas.py:15-19` sets `BALANCING` cap to `17000.0`.
- `configs/market_rules_ua.yaml:15-27` models NEURC Resolution No. 621 with DAM/IDM max `15000.0`, balancing max `17000.0`, DAM/IDM min `10.0`, balancing min `0.01`.

Legacy duplicate before E2:

- `src/gatekeeper/schemas.py:9-14` still says balancing cap `16000.0`.
- `pyproject.toml:94-101` excludes `src/gatekeeper/` from Mypy.

Closure: `src/gatekeeper/schemas.py` now starts with an obsolete legacy-schema warning pointing reviewers to `smart_arbitrage.gatekeeper.schemas`; active import search still points to the canonical package path, and `tests/test_market_rules.py` passes.

### P2 - V4/V5 online thesis claims need artifact-path hardening

The Google Docs thesis draft includes V4/V5 and Poland-enhanced claims. Some are coherent with the project direction, but the exact V4/V5 run IDs and slugs named in the online text were not easy to verify from the local `data/research_runs/` directory during this pass.

Risk: a defense reader may ask for the exact packet path. The thesis should add artifact paths or soften claims until every run ID is backed by a local or Drive-linked packet.

### P2 - LAVA smoke evidence is useful but easy to overstate

The LAVA NPZ margin smoke packet validates an 8-instance CI/prototype contract and keeps:

- `promotion_gate=false`
- `dt_lava_ready=false`
- `permits_model_training=false`
- `market_execution_enabled=false`
- `v13_gate_status=data_acquisition_needed`

It also has a top-level negative delta against its zero-violation margin reference. The nested baseline comparison is more favorable on the selected 8-instance subset, but that is not a thesis-level promotion result.

Risk: any broad "LAVA improves the strategy" claim is unsafe. The defensible claim is "LAVA NPZ smoke validates the prototype contract and non-execution boundary."

### P2 - Poland lagged features are promising but not promoted

The Poland lagged feature audit found:

- 24 Poland columns.
- 17 passed training-consumption audit.
- 7 blocked by null coverage.
- Timestamp alignment status: `lagged_24h_prior_safe`.
- Rolling status: `positive_not_promoted`.

The richer calibrated comparison keeps the Ukrainian-only V2+ baseline stronger on full 90-row mean regret than Poland variants. The rolling gate records a latest-holdout positive signal below the promotion threshold.

Risk: the thesis should describe Poland as context-governed exogenous feature research, not as a promoted replacement.

## Strengths

- Boundary discipline is strong. Reviewed gates preserve `market_execution_enabled=false`.
- The V2+ evidence packet is clear: 90 tenant-anchors, 5 tenants, strict mean regret `310.58`, V2 mean regret `206.37`, V2+ mean regret `174.77`, and 4/4 rolling robustness.
- Dagster definitions load successfully with a large asset/check surface.
- The dashboard and defense pages render without console errors in the captured browser pass.
- The credentialless academic MVP gate passes while explicitly keeping market execution disabled.
- The thesis glossary correctly distinguishes FastAPI read-model APIs from market execution APIs.

## Thesis-Safe Claim Set

Use this wording family:

- "DAM delivery-day operator recommendation preview."
- "Offline read-model evidence system."
- "V2+ schedule/value learner is the current thesis headline challenger under a frozen strict LP/oracle comparator."
- "DT shadow and LAVA are research/prototype lanes, not deployed controllers."
- "V13 remains blocked by explicit DAM publication receipts and sufficient safe-switch examples."

Avoid:

- "Live bidding."
- "Market-submittable bids."
- "Full DFL controller."
- "Decision Transformer deployment."
- "LAVA promoted."
- "Poland features replace Ukrainian-only V2+."
