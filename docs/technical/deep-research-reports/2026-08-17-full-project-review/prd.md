# Product release definition

## Product

A source-backed Ukrainian BESS operator recommendation preview and academic
evidence system, with private operator/defense routes and a public static BESS
arbitrage/forecast portfolio surface.

## Users

- Human BESS operator reviewing hourly recommendations and evidence.
- Thesis supervisor/reviewer evaluating reproducibility and claim discipline.
- Public portfolio visitor evaluating source-backed analytics.

## Required behavior

- Display source, freshness, forecast, LP/value, SOC, and gate information.
- Fail closed when V13 packets, source receipts, or research inputs are absent.
- Preserve deterministic physical/contract validation.
- Generate static public routes for GitHub Pages.
- Keep `market_execution_enabled=false`; emit no market order or autonomous
  dispatch payload.

## Release acceptance

- All local gates in `review.md` pass.
- Hosted PR CI is green on the exact head SHA.
- No actionable unresolved review thread remains.
- Merged main deploys successfully to GitHub Pages.
- Live public routes and essential static assets return HTTP 200.
- GitHub dependency alerts are re-audited after lockfile merge.

## Non-goals

- Market-submittable DAM/IDM bids.
- Autonomous BESS control or settlement integration.
- Claiming DFL/DT promotion without materialized gate packets.
- Treating ENTSO-E, Nord Pool, or AEMO data as Ukrainian target proof.
