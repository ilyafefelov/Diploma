# AFL Forecast Error Audit

Date: 2026-05-09

This slice materializes the existing AFL evidence path before adding another
model. It classifies compact NBEATSx/TFT failures by decision-relevant failure
mode, so the next DFL work optimizes the right target.

Claim boundary: this is forecast/AFL evidence only. It is not full DFL, not
Decision Transformer control, not production bidding, and not market execution.
The frozen `strict_similar_day` LP comparator remains the promotion authority.

## Assets And Checks

| Asset / check | Purpose |
|---|---|
| `forecast_candidate_forensics_frame` | Labels each forecast candidate as frozen control, compact Silver, official backend readiness, or unclassified research. |
| `afl_training_panel_frame` | Sidecar rows with prior-only `feature_*` columns and realized `label_*` decision-value columns. |
| `afl_forecast_error_audit_frame` | Classifies spread-shape, rank/extrema, LP-value, and weather/load context gaps. |
| `afl_forecast_error_audit_evidence` | Checks claim flags and verifies selector features do not include label columns. |

Config:
[real_data_afl_forecast_error_audit_week3.yaml](../../configs/real_data_afl_forecast_error_audit_week3.yaml).

Materialization target:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,forecast_candidate_forensics_frame,afl_training_panel_frame,afl_forecast_error_audit_frame -c configs/real_data_afl_forecast_error_audit_week3.yaml
```

## Failure Categories

| Category | Definition |
|---|---|
| `spread_shape_failure` | Forecast spread differs from actual spread by at least 25%. |
| `rank_extrema_failure` | Top/bottom price rank score falls below 0.5. |
| `lp_value_failure` | Candidate regret is worse than `strict_similar_day` on the same tenant/anchor/split. |
| `weather_load_regime_failure` | Weather/load context is unavailable or insufficient for regime diagnosis. |
| `strict_control_high_regret_overlap` | Candidate failure overlaps anchors where strict control itself has high regret for that tenant/split. |

`selector_feature_columns_csv` must contain only prior-available `feature_*`
columns. Realized `label_*` columns are allowed for diagnosis and future
training labels, but not for selector features.

## Official Training Readiness

Tracked readiness config:
[real_data_official_forecast_training_readiness_week3.yaml](../../configs/real_data_official_forecast_training_readiness_week3.yaml).

The official NBEATSx/TFT adapters now keep smoke defaults in code, while the
tracked readiness config passes CPU-safe serious local settings:

- NBEATSx: deterministic seed and `max_steps=100`.
- TFT: deterministic seed, `max_epochs=15`, small hidden sizes, and CPU-friendly
  batch sizing.
- Both: strict LP/oracle scoring through `official_forecast_strict_lp_benchmark_frame`
  before any DFL claim.

## Current Finding

Materialized evidence, 2026-05-09:

| Metric | Value |
|---|---:|
| Audit rows | 20 |
| Tenants | 5 |
| Source models | 2 |
| AFL panel rows covered | 1,560 |
| Mean LP-value failure rate | 80.23% |
| Mean spread-shape failure rate | 55.19% |
| Mean rank/extrema failure rate | 64.83% |
| Weather/load regime context | prior-only context present |
| `not_full_dfl` / `not_market_execution` | true / true |

Dagster materialization run `3608be63-dbeb-434d-8771-054176196924` materialized
`tenant_historical_net_load_silver`, `forecast_candidate_forensics_frame`,
`afl_training_panel_frame`, and `afl_forecast_error_audit_frame`, and
`afl_forecast_error_audit_evidence` passed. A full upstream benchmark recompute
with the same config still hit the client timeout inside
`real_data_rolling_origin_benchmark_frame`, so this evidence should be read as a
downstream AFL audit over the latest stored 108-anchor benchmark frame, not as a
fresh upstream benchmark recomputation.

| Model | Split | Rows | Mean regret | Strict mean regret | LP-value failure | Spread-shape failure | Rank/extrema failure |
|---|---|---:|---:|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | final holdout | 90 | 1,121.04 UAH | 314.81 UAH | 76.67% | 17.78% | 53.33% |
| `nbeatsx_silver_v0` | train selection | 430 | 1,543.39 UAH | 822.18 UAH | 75.58% | 55.12% | 80.00% |
| `tft_silver_v0` | final holdout | 90 | 1,665.41 UAH | 314.81 UAH | 88.89% | 85.56% | 51.11% |
| `tft_silver_v0` | train selection | 430 | 1,614.25 UAH | 822.18 UAH | 79.77% | 62.33% | 74.88% |

Prior-only context feature checks:

- `feature_prior_weather_context_row_count`: mean `25.0`, minimum `25.0`.
- `feature_prior_net_load_context_row_count`: mean `25.0`, minimum `25.0`.
- Realized rank/extrema overlap is no longer a selector input; it is recorded as
  `diagnostic_forecast_top3_bottom3_rank_overlap`.

Interpretation:

- The dominant problem is still **LP-value failure**: both compact neural
  candidates lose to the frozen `strict_similar_day` comparator on roughly 70%
  of rows.
- Rank/extrema errors are substantial, especially in train-selection windows,
  which supports AFL-weighted forecast hardening before full DFL.
- Spread-shape errors are present but not sufficient alone to explain the final
  holdout failures.
- Weather/load regime features are now represented as prior-only AFL context,
  but the current aggregate context-missing rate is `0%`. The dominant failure
  remains LP-value and rank/spread shape, not missing context rows.

## Official Forecast Strict Scoring

The official backend readiness path also materialized on 2026-05-09:

- Training/materialization run: `1d608ba9-6fff-474e-b582-7766a2871c59` produced
  `nbeatsx_official_price_forecast` and `tft_official_price_forecast`.
- Strict scoring run: `68d74ecb-2d5c-49d5-b25e-99b06ec4b3ba` materialized
  `official_forecast_strict_lp_benchmark_frame`.
- NBEATSx backend status: `trained`, 168 training rows, 24 horizon rows.
- TFT backend status: `trained`, 336 training rows, 24 horizon rows.

This is a single current-horizon readiness score across five tenants, not the
rolling 104-anchor thesis panel.

| Model | Rows | Mean regret | Median regret | Finding |
|---|---:|---:|---:|---|
| `strict_similar_day` | 5 | 1,903.90 UAH | 1,811.95 UAH | Frozen control still wins this readiness run. |
| `nbeatsx_official_v0` | 5 | 6,008.01 UAH | 5,614.92 UAH | Official adapter works, but value evidence is weak. |
| `tft_official_v0` | 5 | 2,540.37 UAH | 2,419.04 UAH | Closer to strict than NBEATSx, still blocked. |

The first official scoring attempt exposed a diagnostic edge case: optional
quantile columns may contain missing values. The evaluator now skips missing
optional quantile diagnostics while keeping point forecasts required for LP
scoring.

## Next Decision

- Treat AFL context hardening and official scoring as complete readiness
  evidence for this slice.
- Do not promote official NBEATSx/TFT from the single-horizon readiness score.
- Continue DFL through a tiny decision-loss correction path, but keep strict LP
  scoring as the only promotion authority.
