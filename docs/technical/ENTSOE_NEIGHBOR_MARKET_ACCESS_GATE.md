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
| `entsoe_neighbor_market_sample_audit_frame` | Optional tiny source-sample audit for mapped neighbors; defaults to fetch-disabled and never unlocks training by itself. |
| `entsoe_neighbor_market_feature_candidate_frame` | Source-backed feature-candidate adapter for ENTSO-E samples; all rows stay blocked until the market-coupling feature route approves them. |

Asset check:

| Check | Requirement |
|---|---|
| `entsoe_neighbor_market_access_evidence` | Query rows must remain research-only, use day-ahead price request shape `A44/A01`, block training use, and prevent fetch when no token is available. |
| `entsoe_neighbor_market_sample_audit_evidence` | Sample rows must remain research-only and non-feature/non-training until full governance passes. |
| `entsoe_neighbor_market_feature_candidate_evidence` | Feature candidates must be source-backed before they can even be considered, and must remain non-training/non-feature until governance passes. |

Materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame `
  -c configs/real_data_entsoe_neighbor_market_access_week3.yaml
```

Sample-audit materialization, fetch-disabled by default:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame,entsoe_neighbor_market_sample_audit_frame `
  -c configs/real_data_entsoe_neighbor_market_sample_week3.yaml
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

Validated sample-audit run:

| Field | Value |
|---|---:|
| Dagster run | `37c9bfa1-e5b8-44eb-bec2-3d3e3b6351b6` |
| Sample rows | 1 |
| Country | `PL` |
| Fetch enabled | `false` |
| Security token available | `false` |
| Fetch status | `skipped_fetch_disabled` |
| Source-backed rows | 0 |
| Training allowed rows | 0 |
| Feature allowed rows | 0 |
| Asset check | `entsoe_neighbor_market_sample_audit_evidence` passed |

## Sample Audit Contract

The sample audit prepares the next source-backed step without mixing European
data into Ukrainian training:

- default sample country: Poland (`PL`);
- default sample period: `202601010000` to `202601020000` UTC;
- default `fetch_enabled=false`;
- if a token and explicit fetch config are provided, parsed ENTSO-E price points
  are counted as source-backed sample rows;
- `training_use_allowed=false` and `feature_use_allowed=false` remain mandatory;
- sample rows keep `not_full_dfl=true` and `not_market_execution=true`.

The audit can prove access and parsing. It cannot by itself approve the feature
for NBEATSx/TFT/DFL training.

## Feature Candidate Adapter

The first source-backed adapter is intentionally narrow. If the sample audit is
fetch-disabled or no rows are parsed, it emits a guard row with
`source_backed=false`. If ENTSO-E price points are parsed, it emits candidate
columns such as `entsoe_pl_day_ahead_price_eur_mwh`.

In both cases:

- `training_use_allowed=false`;
- `feature_use_allowed=false`;
- `publication_time_status=blocked_missing_publication_timestamp`;
- `is_prior_to_ua_decision_anchor=false`;
- `currency_policy=blocked_until_eur_to_uah_prior_only_normalization`;
- `currency_normalization_status=blocked_missing_prior_eur_uah_fx_rate`;
- `neighbor_market_price_uah_mwh=null`;
- `not_full_dfl=true`;
- `not_market_execution=true`.

The candidate rows flow into
[MARKET_COUPLING_EXOGENOUS_FEATURE_INTERFACE.md](MARKET_COUPLING_EXOGENOUS_FEATURE_INTERFACE.md),
which is the only route that can later approve prior-only external features for
official global-panel training.

## Publication-Time And Currency Gates

The feature-candidate adapter now records the two blockers that matter most
before any neighboring-market price can become a model input:

| Column | Current value | Meaning |
|---|---|---|
| `publication_timestamp_utc` | empty | ENTSO-E delivery prices have not yet been tied to a publication timestamp available before the Ukrainian decision anchor. |
| `publication_time_status` | `blocked_missing_publication_timestamp` | The feature is not prior-only yet. |
| `ua_decision_anchor_policy` | `publication_must_precede_ukrainian_dam_decision_anchor` | Any future approval must prove publication before the Ukrainian DAM decision time. |
| `is_prior_to_ua_decision_anchor` | `false` | Current rows are blocked even when prices are parsed. |
| `fx_rate_source` / `fx_rate_timestamp_utc` | empty | No prior-known EUR/UAH FX source is attached. |
| `currency_normalization_status` | `blocked_missing_prior_eur_uah_fx_rate` | EUR prices cannot be compared to UAH OREE prices yet. |
| `neighbor_market_price_uah_mwh` | `null` | No normalized UAH feature is emitted. |

The asset check rejects inconsistent evidence, for example a row marked
`publication_time_verified_prior_to_ua_anchor` without a publication timestamp,
or a row marked `prior_eur_uah_normalized` without prior FX metadata and a UAH
price. Source-backed ENTSO-E parsing is therefore useful evidence, but it is
still not enough to enter NBEATSx/TFT/DFL training.

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
3. Attach source-backed publication-time evidence for the parsed delivery
   prices.
4. Add a prior-known EUR/UAH FX source before emitting any UAH-normalized
   neighbor-price feature.
5. Rerun the feature route and official global-panel parity only after those
   gates pass.
