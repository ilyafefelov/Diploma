# Fix-Plan Verification Matrix

Date: 2026-05-25

This matrix defines the evidence required before the fix-plan closure goal can be marked complete.

Current closure status and next actions are tracked in `fix-plan-closure-matrix.md`.

| Requirement | Proof Needed | Current Status | Source of Truth |
|---|---|---|---|
| Full verification restored | Ruff, Mypy, Pytest, Dagster defs, Dagster list defs, Compose config all pass or have documented out-of-scope rationale | Complete; all refreshed lanes pass | Command output and `verification-refresh-2026-05-25.md` |
| API typing issue resolved | `uv run mypy .` has no `api/main.py` errors | Complete; full Mypy passed 247 source files | Command output |
| README verification current | README has dated command matrix, not stale `151 passed` | Complete; README now records the full-green 2026-05-25 matrix | `README.md` |
| Stale active verification claims removed | Search finds no current-state stale full-verification claim outside clearly historical artifacts | Complete for README and known historical artifacts; stale exact-phrase search now has no active-source hits | Stale verification phrase search |
| Operator dock not occluding content | Browser measurement proves dock does not cover main content on desktop/mobile | Complete; post-fix desktop/mobile evidence reports `noDockOcclusion=true` and `noHorizontalOverflow=true` | Browser/Playwright evidence in `assets/` |
| Dashboard tests pass | Dashboard typecheck and Vitest pass | Current rerun passed: Nuxt typecheck completed and Vitest reported 12 files / 65 tests passed; rerun again after dock work | Dashboard command output and `verification-refresh-2026-05-25.md` |
| Dashboard no-execution wording preserved | Browser/text check finds no market-execution overclaim | Needs check after dashboard work | Browser/DOM evidence |
| V4/V5 claims traceable | Each claim has artifact path/run ID/validation command or softened wording | Complete as wording hardening; Chapter 4 now marks missing V4/V5 packet paths as draft/pending-evidence instead of defended numeric claims | Chapter 4 plus `docs/thesis/appendices/evidence-manifest.md` |
| Poland claims traceable and non-promoted | Poland claims point to local packets and state non-promotion where gates fail | Complete; Chapter 4 links Poland claims to packet paths/run IDs, and the manifest records non-promotion boundaries | `data/research_runs/`, Chapter 4, evidence manifest |
| Thesis evidence manifest exists | Manifest maps major claims to artifact, command, date, boundary | Complete on 2026-05-25 for current local artifacts | `docs/thesis/appendices/evidence-manifest.md` |
| Legacy duplicate contract quarantined | Stale `src/gatekeeper/schemas.py` cannot be mistaken as active cap source | Complete; file has an obsolete legacy-schema module warning and active imports use canonical `smart_arbitrage.gatekeeper.schemas` | File state, `rg` import check, and `tests/test_market_rules.py` |
| Active market caps preserved | Active tests confirm canonical caps, including balancing `17000.0` | Needs rerun after quarantine | `tests/test_market_rules.py`, command output |
| V13 blocker state current | Preflight output records current missing receipt/safe-switch evidence | Complete on 2026-05-25 | `.tmp_runtime/v13_acquisition_inputs_preflight.json`, `v13-f1-preflight-evidence.md`, `v13-f2-acquisition-backlog.md` |
| V13 execution boundary preserved | `market_execution_enabled=false`, `dt_lava_ready=false`, `permits_model_training=false` remain fixed unless source evidence changes | Complete in latest preflight; re-check after any V13 input change | V13 preflight output |

## Completion Audit Procedure

Before marking the goal complete:

1. Re-read `fix-plan.md`, `fix-plan-closure-goal.md`, this matrix, and the generated evidence manifest.
2. Run or inspect every source-of-truth item in the table.
3. Mark each row as proven, contradicted, incomplete, weak evidence, or missing.
4. Continue work for every row that is not proven.
5. Only mark the goal complete when every row is proven and no required work remains.
