# Week 5 Demo Freeze Checklist

Date: 2026-05-26

Purpose: freeze the current credentialless MVP package for supervisor review and
PR handoff. This checklist is not a new research run and does not change the
claim boundary.

## Demo Story

1. MVP boundary: credentialless DAM delivery-day operator preview, not market
   submission.
2. Headline model result: V2+ remains the strongest offline/read-model evidence
   with `174.77` UAH mean regret.
3. DT evidence: direct DT trains as research-shadow evidence, but the
   apples-to-apples DT packet is worse than real V2+ (`460.30` vs `174.77` UAH)
   and is not promoted. The regret-aware follow-up trains the right value-gap
   objective, but abstains back to V2+ on all `90` anchors.
4. V13 blocker: safe-switch support is staged, but explicit DAM publication
   receipts are still missing.
5. Execution boundary: `market_execution_enabled=false`, no `ProposedBid`, no
   market order payload, no dispatch command.

## Open During Review

| Item | Link |
|---|---|
| Supervisor summary | [supervisor-summary.md](./supervisor-summary.md) |
| Demo script | [demo-script.md](./demo-script.md) |
| Week 5 report | [report.md](./report.md) |
| Evidence manifest | [../../appendices/evidence-manifest.md](../../appendices/evidence-manifest.md) |
| V2+ evidence | [../../../technical/DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS.md](../../../technical/DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS.md) |
| DT apples-to-apples evidence | [../../../technical/DT_V2_PLUS_APPLES_TO_APPLES_SHADOW.md](../../../technical/DT_V2_PLUS_APPLES_TO_APPLES_SHADOW.md) |
| Regret-aware selector evidence | [../../../technical/REGRET_AWARE_V2_PLUS_SELECTOR_SHADOW.md](../../../technical/REGRET_AWARE_V2_PLUS_SELECTOR_SHADOW.md) |
| V13 blocker note | [../../../technical/deep-research-reports/2026-05-25-full-project-review/v13-f3-acquisition-sprint-2026-05-26.md](../../../technical/deep-research-reports/2026-05-25-full-project-review/v13-f3-acquisition-sprint-2026-05-26.md) |
| Thesis draft chapters | [../../chapters/03-Methodology.md](../../chapters/03-Methodology.md) and [../../chapters/04-results-and-discussion.md](../../chapters/04-results-and-discussion.md) |
| Google Docs thesis draft | [Draft.Thesis.2.goit.energy_arbitrage.Fefelov](https://docs.google.com/document/d/1jjja9ng99O-xCisijMUbPrEM-3UJi_hilwnFJY8nups/edit) |

## Live Smoke

Start API:

```powershell
.\api\start-dev.ps1 -Port 8010
```

If `8010` is already occupied by a stale local service, use a clean port:

```powershell
.\api\start-dev.ps1 -Port 8011
```

Start dashboard:

```powershell
cd dashboard
npm run dev
```

If the dashboard must point at the alternate API port:

```powershell
$env:NUXT_API_BASE="http://127.0.0.1:8011"
$env:NUXT_IGNORE_LOCK="1"
.\node_modules\.bin\nuxi.cmd dev --port 64164
```

Check:

- `http://127.0.0.1:8010/health`
- `http://localhost:64163/operator`
- `http://127.0.0.1:8010/dashboard/shadow-recommendation-preview?tenant_id=client_003_dnipro_factory&preview_source=dt_v2_plus_apples_to_apples_shadow`
- `http://127.0.0.1:8010/dashboard/shadow-recommendation-preview?tenant_id=client_003_dnipro_factory&preview_source=regret_aware_v2_plus_selector_shadow`

Expected shadow preview facts:

- `preview_source_id=dt_v2_plus_apples_to_apples_shadow`
- `preview_status=apples_to_apples_not_promoted`
- `market_execution_enabled=false`
- `promotion_gate_passed=false`
- `dt_lava_ready=false`
- `dt_selected_mean_regret_uah` is about `460.30`
- `v2_plus_mean_regret_uah` is about `174.77`
- `preview_source_id=regret_aware_v2_plus_selector_shadow`
- `preview_status=regret_aware_abstention_not_promoted`
- `selector_mean_regret_uah` is about `174.77`
- `v2_plus_mean_regret_uah` is about `174.77`
- `non_v2_plus_switch_count=0`
- `abstention_count=90`

## Verification Before PR

```powershell
git diff --check
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_project_entrypoints.py
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\dfl\test_dt_research_shadow.py tests\dfl\test_regret_aware_v2_plus_selector.py tests\api\test_main.py::test_apples_to_apples_dt_shadow_preview_uses_real_v2_plus_artifacts tests\api\test_main.py::test_regret_aware_selector_shadow_preview_abstains_to_v2_plus_without_promotion tests\api\test_main.py::test_regret_aware_selector_shadow_preview_rejects_market_execution_artifacts
cd dashboard
npm exec vitest run app/utils/operatorShadowPreview.test.ts
npm run typecheck
```

Boundary scan:

```powershell
rg -n "market_execution_enabled=true|dt_lava_ready=true|permits_model_training=true|ProposedBid" docs\thesis docs\technical
```

Only negative/boundary references to `ProposedBid` are expected.
