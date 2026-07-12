# Final Defense Runbook

Date: 2026-06-16

This runbook is the shortest reliable path for showing the project from the
GitHub repository and local demo stack.

## Goal

Show that the project is a professional, source-backed BESS operator-preview
system:

1. It reads official or forecast-store DAM/IDM context.
2. It produces hourly recommendation previews for a human operator.
3. It compares strategy evidence against strict baselines.
4. It keeps execution disabled: no `ProposedBid`, no market payload, no dispatch.

For the university 50-point software/experimental rubric, use
[FINAL_UNIVERSITY_RUBRIC_MATRIX.md](FINAL_UNIVERSITY_RUBRIC_MATRIX.md). It maps
each scoring criterion to the demo path, repository evidence, verification
commands, and wording boundaries.

Primary path only:

- Start from README, `/operator`, `/defense`, and FastAPI `/docs`.
- Do not open legacy or unfinished research lanes as the main demo.
- Use DFL/DT/V13/HF research documents only as supporting evidence after the
  product boundary is already clear.
- If asked about legacy, say it is extraction/archive context and not the
  delivered product surface.

## Start

From the repository root:

Windows:

```powershell
.\scripts\start-local-project.ps1 -ApiPort 8000 -DashboardPort 64163
```

macOS/Linux:

```bash
bash ./scripts/start-local-project.sh --api-port 8000 --dashboard-port 64163
```

Open:

- `http://127.0.0.1:64163/operator`
- `http://127.0.0.1:64163/defense`
- `http://127.0.0.1:8000/docs`

If `:8000` serves stale routes:

```powershell
docker compose stop api
.\api\start-dev.ps1 -Port 8000
```

On macOS/Linux:

```bash
docker compose stop api
SMART_ARBITRAGE_API_PORT=8000 bash ./api/start-dev.sh
```

## Optional Forecast-Store Seed

Run this only if tomorrow/day+2 preview rows are unavailable:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_operator_preview_forecast_store.py --market-venue DAM --horizon-hours 72 --nbeatsx-max-steps 1 --tft-max-epochs 1
.\.venv\Scripts\python.exe scripts\materialize_operator_preview_forecast_store.py --market-venue IDM --horizon-hours 72 --nbeatsx-max-steps 1 --tft-max-epochs 1
```

Expected boundary:

- `claim_boundary=operator_preview_forecast_rows_not_market_execution`
- `market_execution_enabled=false`

## Dagster Evidence Filters

Use these in Dagster UI search or with `dg list defs` when the commission asks
which assets support the final operator-preview story:

```powershell
uv run dg list defs --assets "tag:operator_preview=true"
uv run dg list defs --assets "tag:hfdt_live_shadow_preview=true"
uv run dg list defs --assets "tag:read_model_boundary=not_market_execution"
```

These filters expose read-model evidence assets only. They do not indicate
market execution, production promotion, or `ProposedBid` emission.

## Main Demo Script

1. Start at the README and state the boundary:
   - "This is a DAM/IDM hourly recommendation preview."
   - "It is not a trading bot and does not submit bids."
2. Open `/operator`.
3. Select tenant `client_003_dnipro_factory`.
4. Point to the boundary strip:
   - `Preview only`;
   - `No ProposedBid`;
   - `No market payload`;
   - `Human review required`.
5. Show DAM latest official preview.
6. Switch to IDM and explain it is the same hourly preview lane, not live
   intraday bidding.
7. Show target date modes:
   - latest official;
   - today;
   - tomorrow;
   - day+2.
8. Select `HF live safe-switch value-aligned shadow`.
9. Show:
   - one non-HOLD case when value and safety guards pass;
   - one HOLD/fallback case when guards abstain.
10. Use the `Defense path` button or open `/defense` and show V2+, the historical random-forest artifact, and HF,
    and HF as layered evidence:
   - V2+ is headline/default evidence;
   - historical `dt_v2_plus` is a random-forest exact-mirror diagnostic, not DT/OOS evidence;
   - HF value-aligned is a separate transformer-backbone manual diagnostic/read-model path without independent realized-regret evidence.
11. End on `market_execution_enabled=false`, no `ProposedBid`, no market order
    payload.

## Defense Preset

Use this preset when time is short:

| Control | Value |
| --- | --- |
| Tenant | `client_003_dnipro_factory` |
| Venue | `DAM`, then `IDM` |
| Dates | latest official, today, tomorrow, day+2 |
| Shadow source | `HF live safe-switch value-aligned shadow` |
| Closeout route | `/defense` |

If future-date rows are unavailable, do not force demo prices. Show the
source-readiness blocker and state that the system fails closed instead of
rendering synthetic prices.

## Useful API Probes

Health:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/health"
```

Default DAM operator preview:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/operator-recommendation?tenant_id=client_003_dnipro_factory&market_venue=DAM"
```

HF value-aligned shadow day+2:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/shadow-recommendation-preview?tenant_id=client_003_dnipro_factory&preview_source=hf_live_safe_switch_value_aligned_shadow&market_venue=DAM&target_delivery_date=2026-06-04"
```

Academic MVP readiness:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/academic-mvp-readiness"
```

## Verification Before Submission

Lightweight final audit:

```powershell
.\scripts\final_repo_audit.ps1 -SkipFullVerify -SkipSmoke
```

Strict clean-tree submission gate after the final commit/stage boundary:

```powershell
.\scripts\final_repo_audit.ps1 -RequireCleanWorkingTree -SkipFullVerify -SkipSmoke
git status --short
```

Full verification:

```powershell
.\.venv\Scripts\Activate.ps1
.\scripts\verify.ps1
npm -C dashboard run typecheck
npm -C dashboard run test:unit
npm -C dashboard run smoke:hf-value-aligned
```

If the full wrapper is too slow, run the focused checks from the root README.

## What Not To Say

- Do not say "live trading bot".
- Do not say "deployed Decision Transformer controller".
- Do not say "full differentiable DFL controller".
- Do not say "market-submittable DAM/IDM bid engine".
- Do not say "HF replaced V2+ in production".

## Correct Closeout Sentence

The project is ready to defend as a source-backed DAM/IDM hourly operator
recommendation preview with V2+ headline retrospective evidence, a historical
`dt_v2_plus` random-forest exact-mirror diagnostic, and separate HF
transformer-backbone manual/read-model evidence. Market
execution remains disabled.
