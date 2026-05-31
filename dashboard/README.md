# Dashboard Notes

The Nuxt dashboard is a read-model interface for the academic MVP. It does not
submit bids, dispatch hardware, or emit market order payloads.

## Operator Preview Sources

The default operator recommendation is the official OREE row routed through a
deterministic hourly LP preview:

```text
GET /dashboard/operator-recommendation
```

For an explicit future delivery date, the API checks the official OREE row for
that date first. If it is not published yet, a complete NBEATSx/TFT
forecast-store horizon can feed deterministic LP as a
`pre_publication_forecast` operator preview. The advisor also ranks complete
NBEATSx/TFT LP scenario candidates by decision value/regret, reports the
candidate gatekeeper status, and still abstains from market execution:

```text
GET /dashboard/operator-recommendation?strategy_id=nbeatsx_official_v0&target_delivery_date=2026-05-20
```

DAM is the default venue. IDM is available as a separate hourly
planning/read-model lane:

```text
GET /dashboard/operator-recommendation?market_venue=IDM
```

IDM v1 is not a 15-minute bid/submission lane. It uses the same non-execution
boundary as DAM: NBEATSx/TFT are scenario evidence, AFL/DFL are decision-value
evidence, and DT/policy advisor metadata ranks candidates or abstains while the
deterministic LP schedule remains active.

Manual shadow previews are loaded through:

```text
GET /dashboard/shadow-recommendation-preview
```

Current manual source ids:

| Source id | Label | Boundary |
|---|---|---|
| `dt_shadow` | DT Shadow | Existing HF/local DT research-shadow smoke, not promoted |
| `dt_direct_candidate_shadow` | Direct DT Shadow | Fresh HF DT trained directly on candidate-index/schedule-family teacher targets; ties the V13 fallback row and remains worse than strict/oracle |
| `dt_v2_plus_apples_to_apples_shadow` | DT vs real V2+ Shadow | Apples-to-apples DT check against real V2+; DT loses to V2+ and remains non-promoted |
| `regret_aware_v2_plus_selector_shadow` | Regret-aware V2+ selector | Corrected value-gap selector diagnostic; abstains to V2+ with `0 / 90` non-V2+ switches and `90 / 90` abstentions, not promoted |
| `dt_v2_plus_safe_switch_selector_shadow` | DT V2+ safe-switch selector | Current corrected residual DT/V2+ shadow; `168.16` vs `174.77` UAH mean regret, `4 / 90` non-V2+ switches, `3 / 15` safe-switch opportunities recovered, zero tail-risk losses, not promoted |
| `poland_tft_shadow` | Poland/TFT Shadow | Positive/near-miss diagnostic evidence, not default |
| `dfl_diagnostics` | DFL diagnostics | Candidate-value diagnostic evidence, not production |
| `v13_dt_lava_promoted_training` | V13/DT/LAVA blocked | Roadmap-only blocked source, no schedule rows |

Every shadow preview keeps `market_execution_enabled=false`, does not emit
`ProposedBid`, and does not change the official-row LP recommendation.

The operator panel summarizes the current ML architecture story as: V2+ remains
the default/fallback result at `174.77` UAH mean regret, apples-to-apples DT is
kept as historical negative evidence at `460.30` UAH, and the current DT V2+
safe-switch shadow is a research diagnostic at `168.16` UAH mean regret. It is
not promoted because the explicit promotion gate remains false; the dashboard
only lets an operator inspect it manually.

## Verification

```powershell
npm run typecheck
npm exec vitest run app/utils/operatorShadowPreview.test.ts
```
