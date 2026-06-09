# API Read-Model Freshness And Performance

This note records the Week 3 follow-up check for the Postgres-backed dashboard
research read models. The scope is intentionally narrow: preserve API response
contracts and improve the lookup path used by existing endpoints.

## Read-Model Contract

The dashboard evidence endpoints read the latest persisted batch for one
`tenant_id` and one `strategy_kind` from `forecast_strategy_evaluations`.

Affected endpoints:

- `/dashboard/real-data-benchmark`
- `/dashboard/calibrated-ensemble-benchmark`
- `/dashboard/risk-adjusted-value-gate`
- `/dashboard/forecast-dispatch-sensitivity`

Freshness is exposed through the existing `generated_at` field in each response.
For the Dnipro calibration preview, the current API-visible batch is:

| Endpoint | Freshness timestamp | Coverage |
|---|---|---|
| `/dashboard/real-data-benchmark` | `2026-05-06T22:57:36.014876Z` | `anchor_count=90`, `model_count=3`, `data_quality_tier=thesis_grade` |
| `/dashboard/calibrated-ensemble-benchmark` | `2026-05-06T22:57:36.014876Z` | `anchor_count=90`, `model_count=1` |
| `/dashboard/risk-adjusted-value-gate` | `2026-05-06T22:57:36.014876Z` | `anchor_count=90`, `model_count=1` |
| `/dashboard/forecast-dispatch-sensitivity` | `2026-05-06T22:57:36.014876Z` | `anchor_count=90`, `model_count=5`, `row_count=450` |

The table also contains older Dnipro rows from earlier materializations, so
freshness must be interpreted by the latest `generated_at` batch, not total row
count. At measurement time, Dnipro had `108` distinct persisted anchors across
historical batches, while the latest benchmark response correctly returned the
current `90`-anchor preview batch.

## Postgres Index

The store now creates this read-path index during schema setup:

```sql
CREATE INDEX IF NOT EXISTS forecast_strategy_evaluations_latest_read_idx
ON forecast_strategy_evaluations (
    tenant_id,
    strategy_kind,
    generated_at DESC,
    anchor_timestamp,
    rank_by_regret,
    forecast_model_name
);
```

This matches the endpoint query pattern:

1. Filter by `tenant_id`.
2. Filter by `strategy_kind`.
3. Find the latest `generated_at`.
4. Return that batch ordered by anchor, regret rank, and model name.

## Measured Effect

Measured on the local Compose Postgres store after `ANALYZE
forecast_strategy_evaluations`.

| Query | Before | After |
|---|---:|---:|
| Dnipro real-data benchmark latest-batch query | sequential scans, `19.302 ms`, `1219` shared buffer hits | index-only latest lookup + bitmap heap scan, `1.183 ms`, `60` shared buffer hits |
| Dnipro risk-adjusted gate latest-batch query | sequential scans, `13.815 ms`, `1219` shared buffer hits | index scan + index-only latest lookup, `1.572 ms`, `24` shared buffer hits |

Warm HTTP smoke timings with `curl.exe` remain higher than raw DB timings because
the API still converts rows through Polars and Pydantic response models:

| Endpoint | Warm observed range |
|---|---:|
| `/dashboard/real-data-benchmark` | `0.395-0.718 s` |
| `/dashboard/calibrated-ensemble-benchmark` | `0.118-0.194 s` |
| `/dashboard/risk-adjusted-value-gate` | `0.249-0.637 s` |
| `/dashboard/forecast-dispatch-sensitivity` | `0.387-1.166 s` |

Current conclusion: Postgres lookup is no longer the bottleneck for these
Dnipro read models. The next performance work, if needed, should profile API
serialization and the derived sensitivity rebuild rather than adding more
database indexes.
