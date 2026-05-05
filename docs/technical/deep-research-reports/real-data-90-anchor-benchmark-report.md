# Real-Data 90-Anchor DAM Benchmark Report

Run date: 2026-05-05  
Branch: `codex/real-data-benchmark`  
Dagster run id: `b5287069-24e8-4f53-9ed9-6ca214fe81a3`  
MLflow run: <http://localhost:5000/#/experiments/2/runs/522825b0446040e48075199eed40633c>

## Executive Summary

The full real-data benchmark completed successfully after the smoke test passed. The final run materialized observed OREE DAM prices and tenant historical weather, built leakage-free Silver/Gold benchmark inputs, evaluated three forecast candidates through the Level 1 LP simulator, persisted the results to Postgres, and logged the benchmark to MLflow.

The latest persisted benchmark batch contains 1,350 rows: 5 tenants x 90 daily rolling-origin anchors x 3 forecast candidates. All rows are marked `thesis_grade` with `observed_coverage_ratio = 1.0`. The valid benchmark anchors span `2026-01-26 23:00:00` through `2026-05-03 23:00:00`, scoring each 24-hour horizon against observed DAM prices.

The strongest model in this run was not the neural forecast. `strict_similar_day` produced the lowest average regret and won 230 of 450 tenant-anchor comparisons. This is academically useful: it shows the benchmark is strict enough to reject weaker neural candidates, and it supports the thesis framing that NBEATSx/TFT are candidate forecast layers, not automatically superior strategy layers.

## Persisted Artifacts

The live local database is in Docker volume `smart-energy-arbitrage_postgres-data`.

The restoreable Postgres dump is:

`data/db_backups/smart_arbitrage_real_data_90_anchor_20260505T024136.dump`

The compact analysis exports are:

`data/research_runs/real_data_90_anchor_20260505T024136/analysis_summary.json`

`data/research_runs/real_data_90_anchor_20260505T024136/benchmark_model_summary.csv`

`data/research_runs/real_data_90_anchor_20260505T024136/benchmark_tenant_model_summary.csv`

`data/research_runs/real_data_90_anchor_20260505T024136/benchmark_month_summary.csv`

`data/research_runs/real_data_90_anchor_20260505T024136/benchmark_worst_regret_rows.csv`

`data/research_runs/real_data_90_anchor_20260505T024136/benchmark_rows_latest.csv`

## Data Sources

Observed prices came from the Market Operator OREE hourly buying and selling prices page for DAM/IDM data: <https://www.oree.com.ua/index.php/pricectr?lang=english>.

Tenant weather came from Open-Meteo historical weather. Open-Meteo documents the archive endpoint as location-specific historical hourly weather data: <https://open-meteo.com/en/docs/historical-weather-api>.

The benchmark uses observed DAM price rows only. Synthetic data is not silently admitted in benchmark mode. Weather is tenant/location-specific, while DAM prices remain market-wide.

## Run Coverage

| Metric | Value |
|---|---:|
| Tenants | 5 |
| Forecast candidates | 3 |
| Anchors per tenant | 90 |
| Persisted benchmark rows | 1,350 |
| First anchor | 2026-01-26 23:00 |
| Last anchor | 2026-05-03 23:00 |
| Data quality tier | thesis_grade |
| Observed price coverage | 1.0 |
| Mean degradation penalty | 673.07 UAH |
| Mean throughput | 0.7992 MWh |

The OREE source needed polite monthly fetching with delay/retry behavior. Fast repeated requests sometimes returned empty/protected responses. During the first full attempts, the benchmark also exposed a real continuity problem around the March DST/source gap. The Gold anchor selector was corrected to require a complete 168-hour training window plus a complete 24-hour realized horizon before accepting an anchor. That is the correct benchmark behavior because missing observed data should reduce valid anchors, not be patched with synthetic values.

## Model-Level Results

| Model | Rows | Mean regret UAH | Median regret UAH | Mean regret ratio | Mean decision value UAH | Mean oracle value UAH | Wins | Win rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| strict_similar_day | 450 | 851.04 | 535.62 | 0.2591 | 2,795.55 | 3,646.59 | 230 | 51.11% |
| tft_silver_v0 | 450 | 1,128.75 | 732.66 | 0.3232 | 2,517.84 | 3,646.59 | 128 | 28.44% |
| nbeatsx_silver_v0 | 450 | 1,164.17 | 833.18 | 0.3364 | 2,482.42 | 3,646.59 | 92 | 20.44% |

Interpretation:

- `strict_similar_day` is the current benchmark winner. This is plausible for short-horizon Ukrainian DAM data because the market has strong daily block structure, capped price periods, repeated peak/off-peak shape, and only about four months of available 2026 training history in this run.
- `tft_silver_v0` beat `nbeatsx_silver_v0` on average regret, median regret, and win rate, but still did not beat the strict baseline.
- The neural candidates should be described as compact diploma-scope implementations. They are not a SOTA claim yet.

## Tenant-Level Results

| Tenant | Best model | Best mean regret UAH | Oracle mean UAH | Notes |
|---|---|---:|---:|---|
| client_001_kyiv_mall | strict_similar_day | 803.07 | 3,419.32 | Neural models are close but still worse. |
| client_002_lviv_office | strict_similar_day | 389.62 | 1,633.06 | Lowest absolute regret because battery scale/value opportunity is smaller. |
| client_003_dnipro_factory | strict_similar_day | 1,430.11 | 6,031.59 | Highest value tenant; forecast mistakes are amplified by larger dispatch value. |
| client_004_kharkiv_hospital | strict_similar_day | 1,115.74 | 5,074.14 | Strict baseline has strongest win rate among this tenant's candidates. |
| client_005_odesa_hotel | strict_similar_day | 516.64 | 2,074.82 | Strict baseline wins despite lower absolute opportunity. |

## Month Effects

The monthly summary shows the neural models were not uniformly weak. In January, TFT had the lowest average regret. In May, TFT won 10 of 15 comparisons. For February through April, strict similar-day dominated. This suggests the next modeling work should not blindly replace the baseline; it should use an ensemble/gating rule or DFL objective that learns when the neural forecast is actually decision-useful.

## Source-Code Findings

The relevant implementation path is:

- `src/smart_arbitrage/assets/bronze/market_weather.py`: observed OREE and Open-Meteo ingestion.
- `src/smart_arbitrage/forecasting/neural_features.py`: leakage-free feature masking and feature frame construction.
- `src/smart_arbitrage/assets/silver/neural_forecasts.py`: Silver forecast assets.
- `src/smart_arbitrage/assets/gold/forecast_strategy.py`: rolling-origin benchmark asset, accepted-anchor selection, MLflow logging.
- `src/smart_arbitrage/strategy/forecast_strategy_evaluation.py`: LP/oracle scoring and observed-only benchmark checks.
- `src/smart_arbitrage/resources/strategy_evaluation_store.py`: Postgres persistence and dashboard read model access.
- `api/main.py`: `/dashboard/real-data-benchmark`.

The important code fix from this run is in `_daily_benchmark_anchors`: a benchmark anchor now requires every timestamp from `anchor - 167h` through `anchor + 24h`. This protects both training-window lag features and the realized scoring horizon. It also prevents hidden leakage/fill behavior when the observed source has gaps.

## Literature Context

TFT remains a valid model choice because it was designed for multi-horizon forecasting with mixed static, known-future, and observed-historical covariates, plus interpretability through feature selection and attention-style mechanisms: <https://huggingface.co/papers/1912.09363>.

NBEATSx remains a valid EPF candidate because the paper extends N-BEATS with exogenous variables and reports strong electricity-price-forecasting performance across markets: <https://arxiv.org/abs/2104.05522>.

The EPF benchmark literature warns that new forecasting methods must be compared against strong simple baselines, across enough markets/time, and with rigorous evaluation: <https://arxiv.org/abs/2008.08004>. Our result is aligned with this warning because the simple strict-similar-day model beat the compact neural candidates.

Decision-focused learning is the right next research step because DFL optimizes prediction models for downstream constrained decisions instead of pure forecast error: <https://arxiv.org/abs/2307.13565>. The storage-specific perturbed DFL paper is directly relevant because it connects decision-focused losses, differentiability, and linear storage models for arbitrage/behavior prediction: <https://arxiv.org/abs/2406.17085>.

Recent Hugging Face paper pages show that electricity price forecasting is moving toward richer European and cross-scale methods. PriceFM uses a large European dataset and graph/topology-aware foundation modeling: <https://huggingface.co/papers/2508.04875>. THieF argues that reconciling hourly, block, and baseload forecasts can improve day-ahead price forecasts: <https://huggingface.co/papers/2508.11372>. These are SOTA-adjacent directions, but they exceed the current 8-week diploma scope unless reduced to a small experimental slice.

## Academic Claim Boundary

This benchmark supports the following thesis-grade claim:

The system can run a reproducible, observed-data rolling-origin DAM benchmark for five simulated BESS tenants, compare strict similar-day, NBEATSx, and TFT forecast candidates through the same Level 1 LP dispatch evaluator, persist regret/value/degradation metrics, and expose dashboard-ready read models.

It does not yet support these stronger claims:

- Full SOTA electricity price forecasting. The neural implementations are compact project versions, not full hyperparameter-tuned NBEATSx/TFT/PriceFM/THieF studies.
- Full digital twin battery modeling. The simulator is still Level 1: SOC feasibility, throughput, EFC proxy, and degradation-cost proxy.
- Completed DFL. This benchmark prepares the regret/objective basis for DFL, but the forecast models are still evaluated through predict-then-optimize.
- Operational weather-forecast claims. The benchmark uses historical weather context; a strict live trading claim should replace future historical weather values with forecasts available at anchor time.

## Recommended Next Slice

1. Keep `strict_similar_day` as the control baseline and dashboard default comparator.
2. Add forecast-error metrics beside regret so we can diagnose whether neural underperformance is from price error, objective mismatch, or LP dispatch sensitivity.
3. Add an ensemble gate: choose strict/TFT/NBEATSx per anchor using only pre-anchor validation history.
4. Convert the benchmark outputs into a DFL training table: features, forecast candidate, LP action, oracle value, regret, degradation penalty.
5. Implement a small perturbed-DFL or regret-weighted training experiment for one tenant first, then expand to all five tenants if runtime is acceptable.
6. Add a future-weather forecast mode before making any live operational claim.

## Follow-Up Research Slice Status

This follow-up slice keeps `strict_similar_day` as the control and adds research-grade diagnostics around the existing benchmark instead of changing dashboard code.

Implemented:

- Date-aware Ukrainian market-rule features from `configs/market_rules_ua.yaml`, including NEURC Resolution No. 621 effective `2026-04-30`.
- Forecast diagnostics in each benchmark payload: MAE, RMSE, sMAPE, direction, ranking quality, top-k price recall, quantile pinball loss when available, and price-cap violation count.
- A value-aware ensemble gate `value_aware_ensemble_v0` that selects only from prior-anchor validation regret and defaults to `strict_similar_day` when no prior validation history exists.
- DFL-ready training examples persisted through a backend store: tenant, anchor, forecast model, LP committed action, oracle value, regret, degradation, throughput, EFC proxy, market regime features, and forecast diagnostics.
- A small regret-weighted bias-correction pilot for one tenant/model, explicitly marked `pilot_not_full_dfl`.
- A `forecast_only` weather mode in the neural feature builder that masks future historical weather unless the row is marked as forecast weather.

Still not claimed:

- Full DFL with a differentiable optimizer.
- Decision Transformer policy learning.
- Full SOTA NBEATSx/TFT training.
- Operational live trading claims using realized future weather.

## Materialized Research Layer Results

The downstream research layer was materialized from the persisted 90-anchor benchmark rows without re-scraping OREE or Open-Meteo. It selected the latest complete batch for each tenant, then built and persisted the value-aware ensemble rows, DFL-ready examples, and the one-tenant regret-weighted pilot.

New exports:

`data/research_runs/research_layer_20260505T_next_slice/research_layer_summary.json`

`data/research_runs/research_layer_20260505T_next_slice/research_layer_model_summary.csv`

`data/research_runs/research_layer_20260505T_next_slice/dfl_training_summary.csv`

`data/research_runs/research_layer_20260505T_next_slice/regret_weighted_dfl_pilot_summary.json`

Restoreable database dump after adding the research-layer rows:

`data/db_backups/smart_arbitrage_research_layer_20260505T_next_slice.dump`

Postgres persistence after this slice:

| Table / rows | Count |
|---|---:|
| `forecast_strategy_evaluations` total rows | 1,818 |
| `value_aware_ensemble_v0` rows | 450 |
| `dfl_training_examples` rows | 1,800 |
| `regret_weighted_dfl_pilot_runs` rows | 1 |

Updated model comparison including the ensemble gate:

| Model | Rows | Mean regret UAH | Median regret UAH | Mean forecast MAE UAH/MWh | Directional accuracy | Wins | Win rate |
|---|---:|---:|---:|---:|---:|---:|---:|
| strict_similar_day | 450 | 851.04 | 535.62 | 2,369.93 | 0.6155 | 230 | 51.11% |
| value_aware_ensemble_v0 | 450 | 905.94 | 568.58 | 2,389.34 | 0.6129 | 208 | 46.22% |
| tft_silver_v0 | 450 | 1,128.75 | 732.66 | 2,758.14 | 0.5858 | 128 | 28.44% |
| nbeatsx_silver_v0 | 450 | 1,164.17 | 833.18 | 2,744.26 | 0.5637 | 92 | 20.44% |

Interpretation:

- The value-aware ensemble gate improves over both compact neural candidates, but it still does not beat the strict similar-day control.
- `strict_similar_day` remains the default comparator and the safest dashboard default.
- The ensemble result is still academically useful because it shows that pre-anchor validation regret can avoid some neural underperformance, but the current selector is not strong enough to replace the control baseline.
- Forecast-error diagnostics now explain part of the regret pattern: the strict baseline has lower MAE, better directional accuracy, better spread ranking, and better top-k price recall than the compact neural candidates in this stored run.

DFL training table:

| Source model | Rows | Mean regret UAH | Mean training weight | Mean degradation penalty UAH | Mean throughput MWh |
|---|---:|---:|---:|---:|---:|
| strict_similar_day | 450 | 851.04 | 1.2591 | 710.94 | 0.8442 |
| value_aware_ensemble_v0 | 450 | 905.94 | 1.2793 | 706.65 | 0.8391 |
| tft_silver_v0 | 450 | 1,128.75 | 1.3232 | 587.51 | 0.6976 |
| nbeatsx_silver_v0 | 450 | 1,164.17 | 1.3364 | 720.75 | 0.8558 |

Regret-weighted DFL pilot:

| Field | Value |
|---|---:|
| Tenant | `client_003_dnipro_factory` |
| Model | `tft_silver_v0` |
| Train rows | 72 |
| Validation rows | 18 |
| Regret-weighted bias | -752.67 UAH/MWh |
| Validation weighted MAE before | 1,352.12 UAH/MWh |
| Validation weighted MAE after | 873.84 UAH/MWh |
| Weighted MAE delta | 478.28 UAH/MWh |
| Mean validation regret | 1,009.08 UAH |

Decision:

The pilot is positive enough to expand to all five tenants as a diagnostic experiment. It is not yet a profit/regret improvement claim because it only validates a regret-weighted forecast calibration step; the corrected forecasts still need strict LP re-evaluation before any DFL performance claim.

## Verification

Commands run successfully:

```powershell
uv run ruff check .
uv run mypy .
uv run pytest
uv run dg check defs
docker compose config --quiet
```

API smoke checks:

- `GET /health` returned `{"status":"ok"}`.
- `GET /dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory` returned the latest 90-anchor, 3-model benchmark summary and rows.

Docker services were up for Postgres, MLflow, FastAPI, Dagster webserver, Dagster daemon, MQTT, and telemetry ingestor. The API remained exposed on `127.0.0.1:8001`, while Dagster UI remained on `127.0.0.1:3001`.
