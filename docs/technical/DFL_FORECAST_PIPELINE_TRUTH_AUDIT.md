# DFL Forecast Pipeline Truth Audit

Date: 2026-05-11

This slice is the first implementation step from
[DFL_FULL_PROMOTION_EXPERIMENT_PLAN.md](DFL_FULL_PROMOTION_EXPERIMENT_PLAN.md).
It checks whether the compact NBEATSx/TFT/DFL failures are genuine model
failures or whether the rolling-origin benchmark has a lower-level forecast
alignment problem.

Claim boundary: this is diagnostic evidence only. It is not full DFL, not a
Decision Transformer controller, not a promoted strategy, and not market
execution. `strict_similar_day` remains the frozen Level 1 control comparator.

## Asset

| Asset | Purpose |
|---|---|
| `forecast_pipeline_truth_audit_frame` | Summarizes forecast-vector provenance, unit sanity, horizon ordering, off-by-one shift diagnostics, vector round-trip integrity, and perfect-forecast sanity by forecast model. |

Config:
[real_data_dfl_forecast_pipeline_truth_audit_week3.yaml](../../configs/real_data_dfl_forecast_pipeline_truth_audit_week3.yaml).

Materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,forecast_pipeline_truth_audit_frame -c configs/real_data_dfl_forecast_pipeline_truth_audit_week3.yaml
```

Asset check:

- `forecast_pipeline_truth_audit_evidence`

The check fails on blocking evidence problems:

- non-`thesis_grade` provenance;
- observed coverage below `1.0`;
- non-finite or out-of-cap UAH/MWh prices;
- forecast/actual vectors that cannot round-trip through JSON cleanly;
- non-monotonic horizon timestamps;
- forecast intervals at or before the anchor timestamp;
- non-hourly horizon gaps.

Off-by-one horizon shift evidence is reported as a diagnostic warning, not as an
automatic failure. A shifted forecast can happen by chance in a weak model, so it
is used to focus investigation before official model reruns rather than to
invalidate the benchmark by itself.

## Why This Matters

The literature on decision-focused learning and temporal benchmark evaluation
requires that the forecast-to-optimize stack be temporally clean before stronger
model claims are made. In this project, that means the code must prove:

- each anchor uses a future horizon strictly after the anchor timestamp;
- the same forecast vector that is stored is the one scored by the LP/oracle
  path;
- prices are interpreted as UAH/MWh and remain inside configured DAM sanity
  bounds;
- thesis-grade scoring uses observed OREE/Open-Meteo provenance;
- forecast errors are not accidentally caused by a one-hour DST or serialization
  shift.

Only after this audit is clean should the project spend compute on serious
official NBEATSx/TFT rolling forecasts or DFL v2 training.

## Current Status

The helper module `smart_arbitrage.dfl.forecast_pipeline_truth` is implemented
and verified with focused tests for:

- perfect forecast sanity;
- horizon shift detection;
- provenance/unit/leaky-horizon blocking failures;
- vector round-trip failure detection;
- Dagster asset and asset-check registration.

The Compose-backed materialization run
`b78b16aa-1da8-4f58-8ce1-89c5d508a9e2` completed successfully on the
five-tenant 104-anchor Ukrainian panel. The new asset check
`forecast_pipeline_truth_audit_evidence` passed.

The older Dnipro-specific `dnipro_thesis_grade_90_anchor_evidence` check also
ran because it targets the upstream benchmark asset. It reported
`anchor_count must be 90; observed 104`, which is the known scope mismatch for
the newer all-tenant 104-anchor panel and is not a failure of the new truth
audit.

## Materialized Result

| Forecast model | Tenants | Anchors | Rows | Horizon | Data quality | Observed coverage | Blocking failures | Shift warnings | Zero-shift best anchors |
|---|---:|---:|---:|---:|---|---:|---:|---:|---:|
| `strict_similar_day` | 5 | 104 | 520 | 24 h | `thesis_grade` | 1.0 | 0 | 165 | 355 |
| `nbeatsx_silver_v0` | 5 | 104 | 520 | 24 h | `thesis_grade` | 1.0 | 0 | 377 | 143 |
| `tft_silver_v0` | 5 | 104 | 520 | 24 h | `thesis_grade` | 1.0 | 0 | 328 | 192 |

Blocking checks are clean:

- unit sanity failures: `0`;
- vector round-trip failures: `0`;
- leaky horizon rows: `0`;
- non-hourly horizon gaps: `0`;
- non-thesis / non-observed provenance failures: `0`.

Interpretation: the current benchmark path does not show a source, unit,
serialization, or obvious horizon-leakage defect. The high shifted-best counts
for compact NBEATSx/TFT remain useful diagnostic evidence: before claiming model
quality, the next official forecast experiment should compare zero-shift and
shifted-error summaries, but the shift warnings alone do not prove a broken
pipeline.

## Acceptance Criteria

- `forecast_pipeline_truth_audit_frame` materializes from
  `real_data_rolling_origin_benchmark_frame`.
- `forecast_pipeline_truth_audit_evidence` passes without blocking failures.
- Any shifted-horizon warnings are documented as diagnostic evidence.
- If blocking failures appear, DFL/DT reruns stop until the specific pipeline
  defect is fixed.
