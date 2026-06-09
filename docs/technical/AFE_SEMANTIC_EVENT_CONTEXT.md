# AFE Semantic Event Context

Date: 2026-05-08

This slice adds a controlled semantic feature path for the DFL/AFL roadmap. The
source is existing public Ukrenergo Telegram ingestion, not broad scraped news
and not LLM extraction.

Claim boundary: semantic AFE is research evidence only. It is not full DFL, not
Decision Transformer control, not live market execution, and not a production
trading signal.

## Assets And Checks

| Asset / check | Purpose |
|---|---|
| `ukrenergo_grid_events_bronze` | Bronze rule-based parsing of public Ukrenergo Telegram posts. |
| `grid_event_signal_silver` | Existing Silver/demo feature path; the audit reuses the same builder against real-data benchmark timestamps. |
| `forecast_afe_feature_catalog_frame` | AFE registry with implemented Ukrainian features and blocked future EU bridge rows. |
| `market_coupling_temporal_availability_frame` | Source-specific readiness gate for ENTSO-E/PriceFM/OPSD/Ember/Nord Pool/THieF before external training use. |
| `dfl_semantic_event_strict_failure_audit_frame` | Explains whether grid-event features coincide with strict-control failure windows. |
| `dfl_semantic_event_strict_failure_audit_evidence` | Dagster check for source, coverage, claim flags, and no future event freshness. |

Config:
[real_data_afe_semantic_event_context_week3.yaml](../../configs/real_data_afe_semantic_event_context_week3.yaml).

Materialization target:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select ukrenergo_grid_events_bronze,real_data_benchmark_silver_feature_frame,forecast_afe_feature_catalog_frame,dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame,dfl_semantic_event_strict_failure_audit_frame -c configs/real_data_afe_semantic_event_context_week3.yaml
```

Materialized evidence, 2026-05-08:

| Metric | Value |
|---|---:|
| Audit rows | 10 |
| Tenants | 5 |
| Source models | 2 |
| Validation tenant-anchors | 180 |
| Semantic event anchors matched to benchmark window | 0 |
| Strict-control failure anchors | 44 |
| `not_full_dfl` / `not_market_execution` | true / true |

Dagster materialization succeeded and
`dfl_semantic_event_strict_failure_audit_evidence` passed. The important
finding is negative but useful: the current public Ukrenergo Telegram scrape
does not provide matching pre-anchor semantic event coverage for the
January-April 2026 benchmark window. The feature path is now governed and
leak-safe, but semantic context should not be treated as the explanation for
the latest strict-control failure pattern until historical event coverage is
backfilled or another official source is added.

## AFE Catalog Rules

Implemented training-eligible features are Ukrainian and temporal:

- OREE DAM price-history features;
- Open-Meteo weather context with explicit research boundary;
- tenant configuration/load proxy fields;
- official Ukrenergo grid-event semantic features.

External market bridge rows are registered but blocked:

- ENTSO-E;
- Hugging Face PriceFM;
- THieF;
- OPSD;
- Ember;
- Nord Pool.

Every external row remains `training_use_allowed=false` until timezone, DST,
currency, market-rule, price-cap, API/licensing, and temporal-availability
mapping are complete.

## Market-Coupling Source Refresh

The AFE catalog now records market-coupling research sources with explicit
blockers instead of vague placeholders:

| Source | Catalog status | Role | Training use |
|---|---|---|---|
| [ENTSO-E Transparency Platform](https://www.entsoe.eu/data/transparency-platform/) | `include_after_mapping` | future neighboring-zone DAM/load/generation covariate | blocked |
| [PriceFM](https://huggingface.co/papers/2508.04875) / `RunyaoYu/PriceFM` | `include_watch` | future European external-validation and graph-market context | blocked |
| [OPSD time series](https://data.open-power-system-data.org/time_series/) | `include_watch` | future open external-validation dataset | blocked |
| [Ember API](https://ember-energy.org/data/api/) | `watch` | future generation-mix context | blocked |
| [Nord Pool Data Portal](https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/) | `watch_restricted` | restricted Nordic/Baltic price context | blocked |
| [THieF](https://huggingface.co/papers/2508.11372) | `watch` | future temporal-hierarchy research source | blocked |

This follows the market-coupling literature: European day-ahead prices can carry
cross-border information, but only if publication timing, gate closure, market
rules, currency, timezone, and licensing are mapped before training. The current
Ukrainian thesis panel therefore stays UA-first until those blockers are cleared.

Latest local validation:

- Dagster run: `4399f250-8ad4-4279-af39-a74b306d432d`.
- Catalog rows: `18`.
- External bridge rows: `6`.
- External rows allowed for training: `0`.
- External rows missing blocker metadata: `0`.
- Follow-up gate:
  [MARKET_COUPLING_TEMPORAL_AVAILABILITY.md](MARKET_COUPLING_TEMPORAL_AVAILABILITY.md).

## Semantic Event Features

The current semantic grid-event features are:

- `grid_event_count_24h`;
- `tenant_region_affected`;
- `national_grid_risk_score`;
- `days_since_grid_event`;
- `outage_flag`;
- `saving_request_flag`;
- `solar_shift_hint`;
- `event_source_freshness_hours`.

The audit treats these as explanatory context. It does not change selector
decisions yet. Validation outcomes can be labels, but selector features must
remain prior-only.

## Next Use

If the audit shows strict-control failure windows concentrate around grid-event
signals, the next selector experiment may add `selector_feature_*` grid-event
features with the same prior-only rule. If not, the result is still useful:
semantic context is ruled out as the primary explanation, and the next work
should prioritize official NBEATSx/TFT hardening or more Ukrainian coverage.
