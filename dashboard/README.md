# Dashboard Notes

The Nuxt dashboard is a read-model interface for the academic MVP. It does not
submit bids, dispatch hardware, or emit market order payloads.

## Operator Preview Sources

The default operator recommendation remains the best-valid V2+ fallback from:

```text
GET /dashboard/operator-recommendation
```

Manual shadow previews are loaded through:

```text
GET /dashboard/shadow-recommendation-preview
```

Current manual source ids:

| Source id | Label | Boundary |
|---|---|---|
| `dt_shadow` | DT Shadow | Existing HF/local DT research-shadow smoke, not promoted |
| `dt_direct_candidate_shadow` | Direct DT Shadow | Fresh HF DT trained directly on candidate-index/schedule-family teacher targets; ties V2+ and remains worse than strict/oracle |
| `poland_tft_shadow` | Poland/TFT Shadow | Positive/near-miss diagnostic evidence, not default |
| `dfl_diagnostics` | DFL diagnostics | Candidate-value diagnostic evidence, not production |
| `v13_dt_lava_promoted_training` | V13/DT/LAVA blocked | Roadmap-only blocked source, no schedule rows |

Every shadow preview keeps `market_execution_enabled=false`, does not emit
`ProposedBid`, and does not change the default V2+ recommendation.

## Verification

```powershell
npm run typecheck
npm exec vitest run app/utils/operatorShadowPreview.test.ts
```
