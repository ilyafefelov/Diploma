# Market-Coupling Temporal Availability Gate

Date: 2026-05-11

This slice turns the European/neighbor-market bridge from a roadmap note into a
Dagster-visible evidence gate. It does not ingest ENTSO-E, OPSD, Ember, Nord
Pool, PriceFM, or THieF rows into Ukrainian training. It records what must be
true before any external market signal can become a TFT/NBEATSx/DFL feature.

Claim boundary:

- Ukrainian OREE DAM remains the target market.
- Current thesis-grade evidence remains Ukrainian observed OREE/Open-Meteo.
- External rows are future covariates or external-validation sources only.
- `training_use_allowed=false` for every external source.
- This is not full DFL, not Decision Transformer control, and not market
  execution.

## Dagster Assets

| Asset | Purpose |
|---|---|
| `forecast_afe_feature_catalog_frame` | Registers Ukrainian training features and blocked external bridge sources. |
| `market_coupling_temporal_availability_frame` | Converts external bridge rows into source-specific availability/readiness evidence. |
| `entsoe_neighbor_market_query_spec_frame` | Prepares ENTSO-E day-ahead price query specs for Poland/neighbor candidates while blocking fetch/training without a token. |
| `entsoe_neighbor_market_feature_candidate_frame` | Converts ENTSO-E sample rows into blocked source-backed feature candidates. |
| `poland_neighbor_market_snapshot_bronze` | Parses no-token local/public Poland CSV snapshots as source evidence only. |
| `poland_neighbor_market_snapshot_feature_candidate_frame` | Converts no-token Poland snapshots into the same blocked feature-candidate contract. |
| `official_forecast_exogenous_feature_route_frame` | Provides the single approved/blocked prior-only external feature route consumed by official global-panel training. |

Asset check:

| Check | Requirement |
|---|---|
| `market_coupling_temporal_availability_evidence` | External rows must remain blocked from training, list all blockers, define publication-time policy, and keep research-only claim flags. |
| `official_forecast_exogenous_feature_route_evidence` | The official-training route must reject ungoverned external features before they can enter global-panel NBEATSx/TFT/DFL training. |
| `entsoe_neighbor_market_feature_candidate_evidence` | Source-backed ENTSO-E candidates must remain blocked until every governance gate passes. |

The deeper route contract is documented in
[MARKET_COUPLING_EXOGENOUS_FEATURE_INTERFACE.md](MARKET_COUPLING_EXOGENOUS_FEATURE_INTERFACE.md).
The no-token Poland snapshot lane is documented in
[POLAND_NEIGHBOR_MARKET_SNAPSHOT.md](POLAND_NEIGHBOR_MARKET_SNAPSHOT.md).

Latest local validation:

- Dagster run: `84e2647f-0e34-4dcb-8f87-955aa9bb0e08`.
- Asset check: `market_coupling_temporal_availability_evidence` passed.
- Rows: `6`.
- Sources: `6`.
- External rows allowed for training: `0`.
- Missing blocker rows: `0`.
- Rows marked `training_ready`: `0`.
- PriceFM observation count captured in metadata: `140,257`.

Materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame `
  -c configs/real_data_market_coupling_availability_week3.yaml
```

## Source Status

| Source | Status | Current role | Training blocker |
|---|---|---|---|
| [ENTSO-E Transparency Platform](https://www.entsoe.eu/data/transparency-platform/) | `include_after_mapping` | Future neighboring-zone DAM/load/generation/cross-border covariate. | API terms, bidding-zone mapping, publication-time mapping, timezone/DST, currency, market rules, domain shift. |
| [PriceFM](https://huggingface.co/papers/2508.04875) / [RunyaoYu/PriceFM](https://huggingface.co/datasets/RunyaoYu/PriceFM) | `include_watch` | Future European external-validation and graph-market context. | Offline dataset status, licensing review, domain shift, no operational publication-time policy. |
| [OPSD time series](https://data.open-power-system-data.org/time_series/) | `include_watch` | Future open external-validation dataset. | Licensing and market-rule mapping before any comparison. |
| [Ember API](https://ember-energy.org/data/api/) | `watch` | Future generation-mix context. | Coarse temporal resolution and API terms. |
| [Nord Pool Data Portal](https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/) | `watch_restricted` | Restricted Nordic/Baltic price context. | Commercial/restricted access. |
| [THieF](https://huggingface.co/papers/2508.11372) | `watch` | Future temporal-hierarchy research source. | Dataset availability and source review. |

All external rows carry the same blocker list:

```text
licensing,timezone,currency,market_rules,temporal_availability,domain_shift
```

## Hugging Face Snapshot

The `RunyaoYu/PriceFM` Dataset Viewer was checked on 2026-05-11:

- Viewer/search/filter/statistics capabilities are available.
- Split: `default/train`.
- Rows: `140,257`.
- First-row column count: `191`.
- First columns include `time_utc`, country-level generation/load/price/solar/wind fields.

This makes PriceFM useful for external validation and literature framing, but
not a direct Ukrainian training source. It has no operational availability
timestamp for Ukrainian DAM decisions and uses European market regimes.

## Why This Matters For DFL

Recent official NBEATSx/TFT rolling runs proved that the adapter path works, but
the forecasts still lose to `strict_similar_day` under strict LP/oracle scoring.
The next high-leverage improvement is therefore better exogenous context, not
another neural variant with the same inputs.

Market-coupling literature supports this direction because neighboring
electricity markets can influence price formation. The engineering risk is
leakage: if a neighboring price is published after the Ukrainian decision time,
or if it is normalized with future information, it becomes invalid training
input. This gate keeps those features blocked until the source-specific
availability rules are explicit.

The 2026-05-12 feature-interface update keeps that boundary in code. Official
global-panel training now consumes `official_forecast_exogenous_feature_route_frame`.
Because the route currently has zero approved external feature columns, the
365-anchor Offline Strategy Promotion evidence remains Ukrainian-only
OREE/Open-Meteo evidence. Source-backed ENTSO-E sample rows can prove parsing,
but they are still not training rows. The route also has its own Dagster asset
check, so an accidental approval without full governance fails before official
training treats the feature as allowed.

Latest materialization on 2026-05-12 used fetch-disabled ENTSO-E sample config,
so the feature-candidate frame produced one guarded Poland row with
`source_backed=false`, `training_use_allowed=false`, and `feature_use_allowed=false`.
The official training frame consumed the route and reported
`external_feature_training_status=blocked_by_governance`.

## Next Slice

The next executable slice has started with
[ENTSOE_NEIGHBOR_MARKET_ACCESS_GATE.md](ENTSOE_NEIGHBOR_MARKET_ACCESS_GATE.md):
it records `A44/A01` day-ahead price query specs for Poland, Slovakia, Hungary,
Romania, and a Moldova review placeholder. No fetch is allowed until an ENTSO-E
security token is configured.

After that, the next step should be one of:

1. ENTSO-E API/manual sample mapping for Poland, Slovakia, Hungary, Romania, and
   Moldova-adjacent context, with publication timestamps and terms documented.
2. A no-ingestion feature simulation that uses lagged neighbor-market proxies
   only after the availability policy is mapped.
3. DFL v2 schedule/value training using only Ukrainian and already-valid
   prior-only features if external availability remains blocked.
