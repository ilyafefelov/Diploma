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

The official NBEATSx/TFT adapters already exist, but their defaults remain
smoke-level. The next code slice should add explicit Dagster config plumbing
only after this audit shows whether compact candidates are primarily limited by
forecast shape/rank quality.

Intended CPU-safe settings:

- NBEATSx: deterministic seed, more than the smoke `max_steps=10`, early
  stopping if the backend supports it.
- TFT: deterministic seed, small hidden size, 10-30 CPU epochs.
- Both: strict LP/oracle scoring before any DFL claim.

## Current Finding

Materialized evidence, 2026-05-09:

| Metric | Value |
|---|---:|
| Audit rows | 20 |
| Tenants | 5 |
| Source models | 2 |
| AFL panel rows covered | 1,040 |
| Mean LP-value failure rate | 70.22% |
| Mean spread-shape failure rate | 36.67% |
| Mean rank/extrema failure rate | 50.65% |
| Weather/load regime context | unavailable in AFL panel |
| `not_full_dfl` / `not_market_execution` | true / true |

Dagster materialization of the downstream audit succeeded from the already
stored `real_data_rolling_origin_benchmark_frame`, and
`afl_forecast_error_audit_evidence` passed. A full upstream benchmark recompute
with the same config hit the client timeout inside
`real_data_rolling_origin_benchmark_frame`, so the evidence recorded here should
be read as an AFL audit over the latest stored benchmark, not as a fresh
upstream benchmark recomputation.

| Model | Split | Rows | Mean regret | Strict mean regret | LP-value failure | Spread-shape failure | Rank/extrema failure |
|---|---|---:|---:|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | final holdout | 90 | 813.40 UAH | 314.81 UAH | 70.00% | 12.22% | 33.33% |
| `nbeatsx_silver_v0` | train selection | 430 | 1,373.90 UAH | 822.18 UAH | 72.33% | 38.14% | 71.63% |
| `tft_silver_v0` | final holdout | 90 | 1,003.54 UAH | 314.81 UAH | 71.11% | 48.89% | 31.11% |
| `tft_silver_v0` | train selection | 430 | 1,318.11 UAH | 822.18 UAH | 67.44% | 47.44% | 66.51% |

Interpretation:

- The dominant problem is still **LP-value failure**: both compact neural
  candidates lose to the frozen `strict_similar_day` comparator on roughly 70%
  of rows.
- Rank/extrema errors are substantial, especially in train-selection windows,
  which supports AFL-weighted forecast hardening before full DFL.
- Spread-shape errors are present but not sufficient alone to explain the final
  holdout failures.
- Weather/load regime features are not yet represented in the AFL panel, so the
  next feature slice should expose tenant load/weather context as prior-only
  `feature_*` columns before using them in a selector or DFL loss.

## Next Decision

- First add prior-only tenant weather/load context to `afl_training_panel_frame`
  so the audit can separate data-regime failures from pure forecast-shape
  failures.
- Then run serious CPU-safe official NBEATSx/TFT forecasts through the same AFL
  audit and strict LP/oracle gate.
- Only after that build DFL loss v1 around relaxed decision regret, spread/rank
  AFL terms, a small MAE stabilizer, and degradation/throughput regularization.
