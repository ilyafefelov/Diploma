# ENTSO-E Neighbor-Market Access Gate

Date: 2026-05-11

This slice prepares the first concrete market-coupling sample path without
fetching or training on ENTSO-E data. It records the query shape, neighboring
market candidates, access blocker, and claim boundary needed before Polish or
other neighboring day-ahead prices can become Ukrainian TFT/NBEATSx/DFL
features.

Current result: no ENTSO-E security token is available in the local environment,
so every fetch is blocked. This is expected and thesis-safe.

## Dagster Assets

| Asset | Purpose |
|---|---|
| `market_coupling_temporal_availability_frame` | Source-level readiness gate for external market sources. |
| `entsoe_neighbor_market_query_spec_frame` | ENTSO-E day-ahead price query spec and missing-token access evidence. |

Asset check:

| Check | Requirement |
|---|---|
| `entsoe_neighbor_market_access_evidence` | Query rows must remain research-only, use day-ahead price request shape `A44/A01`, block training use, and prevent fetch when no token is available. |

Materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame `
  -c configs/real_data_entsoe_neighbor_market_access_week3.yaml
```

Validated run:

| Field | Value |
|---|---:|
| Dagster run | `6ffa967d-d5d1-4580-8c38-ed7282456ff1` |
| Query-spec rows | 5 |
| Mapped EIC rows | 4 |
| EIC review-required rows | 1 |
| Fetch-allowed rows | 0 |
| Training-allowed rows | 0 |
| Bad request-shape rows | 0 |
| Asset check | `entsoe_neighbor_market_access_evidence` passed |

The zero fetch-allowed rows are expected: no `ENTSOE_SECURITY_TOKEN` or
`ENTSO_E_SECURITY_TOKEN` was present in the local environment. This is a
source-access blocker, not a model failure.

## Query Spec

The prepared request shape is ENTSO-E day-ahead price data:

- document type: `A44` price document;
- process type: `A01` day-ahead;
- market venue label: `neighbor_DAM`;
- time policy: request in UTC and align to `Europe/Kyiv` decision anchors;
- publication policy: source publication timestamp must be before the Ukrainian
  decision anchor;
- training use: always `false` in this slice.

## Neighbor Candidates

| Country | EIC status | EIC | Current access status |
|---|---|---|---|
| Poland | mapped | `10YPL-AREA-----S` | blocked without token |
| Slovakia | mapped | `10YSK-SEPS-----K` | blocked without token |
| Hungary | mapped | `10YHU-MAVIR----U` | blocked without token |
| Romania | mapped | `10YRO-TEL------P` | blocked without token |
| Moldova | review required | n/a | excluded until ENTSO-E bidding-zone mapping is verified |

The mapped EIC values are query-spec metadata, not evidence that data has been
downloaded or licensed for thesis training. Moldova is intentionally not guessed.

## Research Grounding

ENTSO-E identifies areas through EIC area codes, and the Transparency Platform
API exposes day-ahead price documents using `A44`. The SDAC literature supports
neighbor-market context because cross-zonal day-ahead markets are coupled
through common market processes and cross-border constraints.

This does not imply that European rows may be mixed into Ukrainian training.
Before that, the project must still prove:

- API terms and redistribution rights;
- publication timestamp before the Ukrainian decision anchor;
- timezone and DST alignment;
- EUR/UAH or other currency normalization using information available at the
  decision time;
- Ukrainian DAM versus neighboring market-rule and price-cap mapping;
- domain-shift validation against Ukrainian OREE holdout evidence.

## Next Actions

1. Request or configure an ENTSO-E security token outside git.
2. Fetch a tiny source-backed sample for Poland first.
3. Persist only source metadata and a no-training sample audit.
4. Add a strict no-leakage availability check before any feature enters
   NBEATSx/TFT/DFL training.
