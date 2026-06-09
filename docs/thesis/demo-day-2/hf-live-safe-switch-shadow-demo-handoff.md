# HF Live Safe-Switch Shadow Demo Handoff

## What This Artifact Proves

This handoff records a manually selected, source-backed HF value-aligned shadow preview for the operator dashboard. It proves that the demo path can load official or pre-publication DAM/IDM context, score deterministic LP-free candidate templates, and either select a guarded non-fallback schedule or abstain to the V2+ fallback.

Evidence packet:

- `data/research_runs/hf_live_safe_switch_shadow_demo_evidence_2026_06_01/summary.json`
- `data/research_runs/hf_live_safe_switch_shadow_demo_evidence_2026_06_01/demo_cases.csv`
- `data/research_runs/hf_live_safe_switch_shadow_demo_evidence_2026_06_01/responses/`
- `data/research_runs/hf_live_safe_switch_shadow_demo_evidence_2026_06_01/dashboard_browser_verification.json`
- `data/research_runs/hf_live_safe_switch_shadow_demo_evidence_2026_06_01/screenshots/`

## Four Demo Cases

| Case | Venue | Delivery date | Outcome | Evidence |
|---|---:|---:|---|---|
| Official DAM proof | DAM | 2026-05-02 | `strict_reference`, 8 non-HOLD rows | Official published context, non-fallback selected |
| Forecast DAM action | DAM | 2026-06-02 | `schedule_value_learner_v2_reference`, 4 non-HOLD rows | Pre-publication forecast context, forecast guard passed |
| Forecast DAM abstention | DAM | 2026-06-03 | V2+ HOLD fallback | Forecast context loaded; guard abstained |
| IDM abstention | IDM | 2026-06-02 | V2+ HOLD fallback | IDM forecast context loaded; guard abstained |

Packet-level metrics:

- Cases passed: `4`
- Non-fallback cases: `2`
- Guarded abstention cases: `2`
- `market_execution_enabled=false`
- `promotion_gate_passed=false`
- `production_market_promotion_gate_passed=false`
- `proposed_bid_emitted=false`
- `market_order_payload_emitted=false`

## Demo Boundary

This is supervisor/demo evidence only. It is not V13 training, not a production replacement for V2+, not a replacement for LP in production optimization, and not market execution. The dashboard source remains manually selectable as `hf_live_safe_switch_value_aligned_shadow`; V2+ remains the fallback/comparator.

V13 remains blocked for market-submission readiness until explicit OREE/SCMO publication receipt evidence is solved.

## Browser Verification

The dashboard smoke opened `/operator`, selected each HF demo preset, captured one viewport screenshot per case, and checked the visible hourly table:

- Official DAM proof: `24` rows, `8` non-HOLD rows, visible outcome matches.
- Forecast DAM action: `24` rows, `4` non-HOLD rows, visible outcome matches.
- Forecast DAM abstention: `24` rows, `0` non-HOLD rows, visible guarded HOLD outcome matches.
- IDM abstention: `24` rows, `0` non-HOLD rows, visible guarded HOLD outcome matches.

## Readiness Matrix Smoke

The value-aligned HF shadow readiness smoke now checks the manually selected source across:

- `DAM`: latest official, today, tomorrow, day+2
- `IDM`: latest official, today, tomorrow, day+2

Expected behavior is either `24` source-backed hourly rows or an explicit blocked reason. For the current local proof run on `2026-06-02`, all eight matrix cases returned `24` rows, no `/dashboard/operator-recommendation` request was made after HF selection, no stale `2026-05-02` proof fallback appeared, and execution flags stayed false.

Tracked commands:

```powershell
npm -C dashboard run smoke:hf-value-aligned
```

The reusable browser smoke writes screenshots and `summary.json` to `SMART_ARBITRAGE_BROWSER_SMOKE_DIR` or a temporary directory. The matching CI workflow is `.github/workflows/hf-value-aligned-shadow-browser-smoke.yml`; it is manual and self-hosted because the smoke intentionally exercises the source-backed local stack instead of mocked or synthetic prices.
