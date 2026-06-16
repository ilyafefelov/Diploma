# Dashboard Notes

The Nuxt dashboard is a read-model interface for the academic MVP. It does not
submit bids, dispatch hardware, or emit market order payloads.

## Final Review Routes

| Route | Purpose |
| --- | --- |
| `/operator` | Main commission demo surface for DAM/IDM hourly recommendation preview. |
| `/defense` | Evidence surface for V2+, DT/V2+, HF shadow, readiness, and boundaries. |
| `/` | Redirects to `/operator`. |

## Operator Preview Sources

Default preview:

```text
GET /dashboard/operator-recommendation
```

The default recommendation is official-OREE-row-first. DAM is the default
hourly planning preview. IDM is available as a separate hourly read-model lane:

```text
GET /dashboard/operator-recommendation?market_venue=IDM
```

For an explicit future delivery date, the API checks official OREE rows first.
If the official row is not complete, a complete forecast-store horizon can feed
the preview as `pre_publication_forecast`:

```text
GET /dashboard/operator-recommendation?market_venue=DAM&target_delivery_date=2026-06-04
```

Forecast-store rows can be seeded from the repo root:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_operator_preview_forecast_store.py --market-venue DAM --horizon-hours 72 --nbeatsx-max-steps 1 --tft-max-epochs 1
.\.venv\Scripts\python.exe scripts\materialize_operator_preview_forecast_store.py --market-venue IDM --horizon-hours 72 --nbeatsx-max-steps 1 --tft-max-epochs 1
```

Every operator preview keeps `market_execution_enabled=false`.

## Manual Shadow Preview

Manual shadow previews are loaded through:

```text
GET /dashboard/shadow-recommendation-preview
```

Final defense source ids:

| Source id | Label | Boundary |
| --- | --- | --- |
| `dt_v2_plus_safe_switch_selector_shadow` | DT/V2+ safe-switch selector | Corrected canonical secondary evidence: `168.16` UAH mean regret, `4 / 90` switches, `86 / 90` abstentions, not promoted. |
| `hf_live_safe_switch_value_aligned_shadow` | HF live safe-switch value-aligned shadow | Manual shadow/demo challenger: `158.71` UAH frozen mean regret signal, 8/8 DAM/IDM readiness, not a production controller. |
| `v13_dt_lava_promoted_training` | V13/DT/LAVA blocked | Roadmap/source-readiness blocker only, no schedule rows. |

Historical sources may remain available for diagnostics, but they should not be
used as the main defense path unless the final runbook explicitly calls for
them.

Every shadow preview keeps:

- `market_execution_enabled=false`
- no `ProposedBid`
- no market order payload
- no dashboard/API default switch

## Verification

```powershell
npm -C dashboard run typecheck
npm -C dashboard run test:unit
npm -C dashboard run smoke:hf-value-aligned
```

The browser smoke writes screenshots and JSON summaries under
`.tmp_runtime\hf_value_aligned_shadow_browser_smoke\`. Treat these as UI
regression evidence, not training or market-execution evidence.

## Troubleshooting

If the dashboard returns `404` or `502` for current backend routes while
`start-local-project.ps1` reports that FastAPI is already listening on `:8000`,
restart the current-workspace API:

```powershell
docker compose stop api
.\api\start-dev.ps1 -Port 8000
```
