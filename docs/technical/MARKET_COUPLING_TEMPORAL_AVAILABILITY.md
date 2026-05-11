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

Asset check:

| Check | Requirement |
|---|---|
| `market_coupling_temporal_availability_evidence` | External rows must remain blocked from training, list all blockers, define publication-time policy, and keep research-only claim flags. |

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
