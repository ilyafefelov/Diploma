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

## Live Exogenous Variables

The MVP now adds a live exogenous-signal layer for dashboard-ready context and future forecast features:

- Open-Meteo remains the primary no-key weather source for tenant/location-specific live forecasts and historical weather.
- OREE DAM rows already provide market-wide price, volume, low-volume, and price-spike metadata.
- Public Ukrenergo Telegram posts at <https://t.me/s/Ukrenergo> are ingested as observed grid-event text because direct `ua.energy/news` and WordPress JSON access was WAF-rejected during the source probe.
- The Silver grid-event layer converts posts into transparent hourly features: recent event count, national grid risk score, tenant-region affected flag, outage flag, evening saving request, solar-shift hint, and event freshness.
- The API exposes these signals through `GET /dashboard/exogenous-signals?tenant_id=...` without changing dashboard UI code.

Claim boundary: these news/Telegram features are operational context covariates. They are not yet proven causal price predictors and should not be used for live trading claims until evaluated through the same leakage-free rolling-origin benchmark.

Deferred sources:

- Energy Map's hourly IPS balance dataset is highly relevant for load/generation/interstate-flow features, but the probed dataset page reports subscription/download limits.
- ENTSO-E Transparency Platform has useful price/load/generation/cross-border document types, but production use requires a security token.
- Hugging Face datasets such as Spain energy-weather and Chronos electricity corpora are useful for method validation or pretraining experiments, not direct live Ukraine features.
- Time-Series-Library/TimeXer is a SOTA-adjacent exogenous Transformer reference, but remains outside tonight's MVP scope.

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

- Full DFL forecast fine-tuning. A differentiable relaxed-LP primitive now exists, but the compact forecast models are not yet trained end-to-end on regret.
- Completed Decision Transformer policy learning. Offline trajectory rows, a small return-conditioned policy class, and a safety projection layer now exist; trained DT evaluation is still future work.
- Full SOTA NBEATSx/TFT training. A backend-neutral Silver schema now exists for NeuralForecast/PyTorch-Forecasting experiments, but current benchmark results still use compact local candidates.
- Operational live trading claims using realized future weather.

## Framework Foundation Slice

The latest implementation tightened the Dagster medallion structure and added research primitives needed before a full DFL/DT run:

- All Dagster assets in the Bronze/Silver/Gold groups now carry matching `medallion` tags for clearer UI filtering and lineage.
- `real_data_benchmark_silver_feature_frame` moves observed DAM price + tenant weather feature joining into Silver before Gold rolling-origin scoring.
- `sota_forecast_training_frame` exposes the leakage-safe Silver frame as `unique_id`, `ds`, `y`, known-future covariates, historical-observed covariates, and static covariate metadata for full NBEATSx/TFT backends.
- `dfl_relaxed_lp_pilot_frame` evaluates forecast rows through a `cvxpylayers` relaxed storage LP and reports relaxed oracle regret. This is a pilot primitive, not the final thesis metric.
- `decision_transformer_trajectory_frame` converts simulated dispatch transitions into offline state/action/reward/return-to-go rows.
- `simulated_live_trading_frame` provides simulated paper-trading replay rows with `paper_trade_provenance="simulated"` and no settlement IDs.

## Materialized Research Layer Results

The downstream research layer was materialized from the persisted 90-anchor benchmark rows without re-scraping OREE or Open-Meteo. It selected the latest complete batch for each tenant, then built and persisted the value-aware ensemble rows, DFL-ready examples, one-tenant regret-weighted pilot, all-tenant TFT/NBEATSx regret-weighted calibration rows, and a strict LP re-evaluation of the corrected forecasts.

Dagster downstream materialization run: `495dcfd3-cd1a-411f-9c9e-45de80941ac5`
MLflow regret-weighted expansion run: <http://localhost:5000/#/experiments/3/runs/59316dec9d4246cc98f848eb7816a1b2>

New exports:

`data/research_runs/dfl_forecast_expansion_20260505T132500/research_layer_summary.json`

`data/research_runs/dfl_forecast_expansion_20260505T132500/research_layer_model_summary.csv`

`data/research_runs/dfl_forecast_expansion_20260505T132500/dfl_training_summary.csv`

`data/research_runs/dfl_forecast_expansion_20260505T132500/regret_weighted_calibration_summary.csv`

`data/research_runs/dfl_forecast_expansion_20260505T132500/regret_weighted_benchmark_summary.csv`

`data/research_runs/dfl_forecast_expansion_20260505T132500/regret_weighted_dfl_pilot_summary.json`

Restoreable database dump after adding this research-layer slice:

`data/db_backups/smart_arbitrage_dfl_forecast_expansion_20260505T132500.dump`

Additional horizon-aware calibration exports:

`data/research_runs/horizon_dfl_expansion_20260505T140430/research_layer_summary.json`

`data/research_runs/horizon_dfl_expansion_20260505T140430/horizon_regret_weighted_calibration_summary.csv`

`data/research_runs/horizon_dfl_expansion_20260505T140430/horizon_regret_weighted_benchmark_summary.csv`

Dagster horizon-aware materialization run: `16311f60-acb2-4eba-a158-0d04d083e7a6`
MLflow horizon-aware run: <http://localhost:5000/#/experiments/4/runs/9d61ef79a0d34214b2de6617346a616e>

Restoreable database dump after the horizon-aware slice:

`data/db_backups/smart_arbitrage_horizon_dfl_expansion_20260505T140430.dump`

Postgres persistence after this slice:

| Table / rows | Count |
|---|---:|
| `real_data_rolling_origin_benchmark` rows used by latest batch | 1,350 |
| `value_aware_ensemble_v0` rows | 450 |
| `dfl_training_examples` rows | 1,800 |
| `regret_weighted_dfl_pilot_runs` rows | 1 |
| `regret_weighted_forecast_calibration_benchmark` rows | 2,250 |
| `horizon_regret_weighted_forecast_calibration_benchmark` rows | 2,250 |

Updated model comparison including the ensemble gate:

| Model | Rows | Mean regret UAH | Median regret UAH | Mean forecast MAE UAH/MWh | Directional accuracy | Wins | Win rate |
|---|---:|---:|---:|---:|---:|---:|---:|
| strict_similar_day | 450 | 851.04 | 535.62 | 2,369.93 | 0.6155 | 230 | 51.11% |
| value_aware_ensemble_v0 | 450 | 905.94 | 568.58 | 2,389.34 | 0.6129 | 208 | 46.22% |
| tft_silver_v0 | 450 | 1,128.75 | 732.66 | 2,758.14 | 0.5858 | 128 | 28.44% |
| nbeatsx_silver_v0 | 450 | 1,164.17 | 833.18 | 2,744.26 | 0.5637 | 92 | 20.44% |

Strict LP re-evaluation of regret-weighted corrected forecasts:

| Model | Rows | Mean regret UAH | Median regret UAH | Mean decision value UAH | Wins | Win rate |
|---|---:|---:|---:|---:|---:|---:|
| strict_similar_day | 450 | 851.04 | 535.62 | 2,795.55 | 225 | 50.00% |
| tft_regret_weighted_calibrated_v0 | 450 | 1,125.56 | 752.80 | 2,521.03 | 67 | 14.89% |
| tft_silver_v0 | 450 | 1,128.75 | 732.66 | 2,517.84 | 62 | 13.78% |
| nbeatsx_silver_v0 | 450 | 1,164.17 | 833.18 | 2,482.42 | 34 | 7.56% |
| nbeatsx_regret_weighted_calibrated_v0 | 450 | 1,171.75 | 833.18 | 2,474.83 | 62 | 13.78% |

Regret-weighted calibration summary:

| Source model | Corrected model | Status | Rows | Mean bias UAH/MWh | Median bias UAH/MWh |
|---|---|---|---:|---:|---:|
| nbeatsx_silver_v0 | nbeatsx_regret_weighted_calibrated_v0 | calibrated | 380 | -431.18 | -581.89 |
| nbeatsx_silver_v0 | nbeatsx_regret_weighted_calibrated_v0 | insufficient_prior_history | 70 | 0.00 | 0.00 |
| tft_silver_v0 | tft_regret_weighted_calibrated_v0 | calibrated | 380 | -635.25 | -992.87 |
| tft_silver_v0 | tft_regret_weighted_calibrated_v0 | insufficient_prior_history | 70 | 0.00 | 0.00 |

Interpretation:

- `strict_similar_day` remains the control winner and dashboard default comparator.
- `value_aware_ensemble_v0` improves over both compact neural candidates, but still does not beat the strict control.
- The one-tenant regret-weighted pilot improved weighted MAE, but strict LP re-evaluation shows this simple bias calibration is not enough to claim DFL profit improvement.
- TFT calibration is mildly useful as a diagnostic: mean regret improves by about 3.19 UAH versus raw TFT, but median regret worsens.
- NBEATSx calibration is neutral/negative: it increases mean regret versus raw NBEATSx, even though it increases rank-1 wins. This means a scalar bias correction is too crude for NBEATSx dispatch value.
- This is an academically valid negative/neutral result. It argues for a better value-oriented objective or relaxed differentiable layer, not for dropping the strict baseline.

Horizon-aware regret-weighted corrected forecasts:

| Model | Rows | Mean regret UAH | Median regret UAH | Mean decision value UAH | Wins | Win rate |
|---|---:|---:|---:|---:|---:|---:|
| tft_horizon_regret_weighted_calibrated_v0 | 450 | 834.32 | 558.87 | 2,812.26 | 99 | 22.00% |
| strict_similar_day | 450 | 851.04 | 535.62 | 2,795.55 | 171 | 38.00% |
| nbeatsx_horizon_regret_weighted_calibrated_v0 | 450 | 941.74 | 653.24 | 2,704.85 | 80 | 17.78% |
| tft_silver_v0 | 450 | 1,128.75 | 732.66 | 2,517.84 | 61 | 13.56% |
| nbeatsx_silver_v0 | 450 | 1,164.17 | 833.18 | 2,482.42 | 39 | 8.67% |

Horizon-aware calibration summary:

| Source model | Corrected model | Status | Rows | Mean horizon bias UAH/MWh | Median horizon bias UAH/MWh | Mean max abs horizon bias UAH/MWh |
|---|---|---|---:|---:|---:|---:|
| nbeatsx_silver_v0 | nbeatsx_horizon_regret_weighted_calibrated_v0 | calibrated | 380 | -431.18 | -581.89 | 3,057.19 |
| nbeatsx_silver_v0 | nbeatsx_horizon_regret_weighted_calibrated_v0 | insufficient_prior_history | 70 | 0.00 | 0.00 | 0.00 |
| tft_silver_v0 | tft_horizon_regret_weighted_calibrated_v0 | calibrated | 380 | -635.25 | -992.87 | 4,352.84 |
| tft_silver_v0 | tft_horizon_regret_weighted_calibrated_v0 | insufficient_prior_history | 70 | 0.00 | 0.00 | 0.00 |

Interpretation of the horizon-aware result:

- Horizon-aware TFT correction is the first DFL-inspired diagnostic that beats `strict_similar_day` on mean regret: `834.32` UAH versus `851.04` UAH.
- This is not enough to replace the strict baseline. The strict control still has lower median regret and far more rank-1 wins.
- The result is still stronger than scalar bias correction because it preserves the 24-hour price-shape structure that the LP optimizer needs.
- The thesis-safe claim is: horizon-structured value calibration is a promising bridge toward DFL. It is not yet full differentiable DFL because the forecast model is not trained through an optimizer layer.

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

Calibrated horizon-aware ensemble gate follow-up:

This follow-up materialized `calibrated_value_aware_ensemble_v0`, a selector that only chooses among `strict_similar_day`, `tft_horizon_regret_weighted_calibrated_v0`, and `nbeatsx_horizon_regret_weighted_calibrated_v0`. The selector uses mean regret from prior anchors only and ignores raw compact neural rows.

Artifacts:

`data/research_runs/calibrated_ensemble_20260505T143258/calibrated_ensemble_summary.csv`

`data/research_runs/calibrated_ensemble_20260505T143258/research_layer_summary.json`

Dagster materialization run: `1b7959eb-f745-4a0a-9457-5b3d3bd85a89`
MLflow run: <http://localhost:5000/#/experiments/5/runs/661189d0b8a1497784e26f3831f77fc7>

Restoreable database dump:

`data/db_backups/smart_arbitrage_calibrated_ensemble_20260505T143258.dump`

Calibrated selector result:

| Model | Rows | Mean regret UAH | Median regret UAH | Mean decision value UAH |
|---|---:|---:|---:|---:|
| tft_horizon_regret_weighted_calibrated_v0 | 450 | 834.32 | 558.87 | 2,812.26 |
| strict_similar_day | 450 | 851.04 | 535.62 | 2,795.55 |
| calibrated_value_aware_ensemble_v0 | 450 | 913.92 | 565.50 | 2,732.67 |
| nbeatsx_horizon_regret_weighted_calibrated_v0 | 450 | 941.74 | 653.24 | 2,704.85 |

Selector breakdown:

| Selected source | Rows | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|
| strict_similar_day | 218 | 943.08 | 595.75 |
| tft_horizon_regret_weighted_calibrated_v0 | 185 | 859.49 | 557.56 |
| nbeatsx_horizon_regret_weighted_calibrated_v0 | 47 | 992.87 | 533.49 |

Interpretation:

- The calibrated gate is a negative selector result, not a promotion candidate. It improves over raw compact NBEATSx/TFT but is worse than both strict similar-day and horizon-aware TFT on mean regret.
- The failure mode is useful: trailing mean regret over prior anchors is too blunt for switching among volatile strategies. It selects strict or NBEATSx in periods where horizon-aware TFT would have been better.
- Dashboard default should remain `strict_similar_day`. Horizon-aware TFT remains a promising research candidate, but it needs a better pre-anchor selector or a true value-oriented DFL objective before operational claims.
- Backend read model added: `GET /dashboard/calibrated-ensemble-benchmark?tenant_id=...`. No dashboard UI was changed.

Risk-adjusted selector and dispatch-sensitivity diagnostics follow-up:

This follow-up materialized `forecast_dispatch_sensitivity_frame` and `risk_adjusted_value_gate_v0`. The sensitivity frame links forecast error, forecast/realized dispatch spread, LP action, degradation, throughput, and regret. The risk-adjusted gate chooses among `strict_similar_day`, `tft_horizon_regret_weighted_calibrated_v0`, and `nbeatsx_horizon_regret_weighted_calibrated_v0` using only prior-anchor median regret, tail regret, and win rate.

Artifacts:

`data/research_runs/risk_gate_diagnostics_20260505T151401/research_layer_summary.json`

`data/research_runs/risk_gate_diagnostics_20260505T151401/forecast_dispatch_sensitivity_summary.csv`

`data/research_runs/risk_gate_diagnostics_20260505T151401/risk_adjusted_value_gate_summary.csv`

MLflow risk-adjusted gate run: <http://127.0.0.1:5000/#/experiments/6/runs/e30a3095d8bd48eb9e01b317e6b60bc1>

Restoreable database dump:

`data/db_backups/smart_arbitrage_risk_gate_diagnostics_20260505T151401.dump`

Postgres persistence after this slice:

| Table / rows | Count |
|---|---:|
| `horizon_regret_weighted_forecast_calibration_benchmark` rows | 2,250 |
| `forecast_dispatch_sensitivity_frame` exported diagnostic rows | 2,250 |
| `calibrated_value_aware_ensemble_gate` rows | 450 |
| `risk_adjusted_value_gate` rows | 450 |

Risk-adjusted selector result:

| Model | Rows | Mean regret UAH | Median regret UAH | Mean decision value UAH |
|---|---:|---:|---:|---:|
| tft_horizon_regret_weighted_calibrated_v0 | 450 | 834.32 | 558.87 | 2,812.26 |
| strict_similar_day | 450 | 851.04 | 535.62 | 2,795.55 |
| calibrated_value_aware_ensemble_v0 | 450 | 913.92 | 565.50 | 2,732.67 |
| risk_adjusted_value_gate_v0 | 450 | 918.76 | 566.70 | 2,727.83 |
| nbeatsx_horizon_regret_weighted_calibrated_v0 | 450 | 941.74 | 653.24 | 2,704.85 |

Risk-adjusted selector breakdown:

| Selected source | Rows | Mean regret UAH |
|---|---:|---:|
| tft_horizon_regret_weighted_calibrated_v0 | 209 | 809.11 |
| strict_similar_day | 163 | 1,058.98 |
| nbeatsx_horizon_regret_weighted_calibrated_v0 | 78 | 919.54 |

Sensitivity diagnosis:

| Diagnostic bucket | Interpretation |
|---|---|
| `forecast_error` | Dominant bucket for all models; most high-regret rows still have large price errors. |
| `spread_objective_mismatch` | Smaller bucket where mean price error is not extreme, but the forecasted dispatch spread differs materially from the realized spread. |
| `lp_dispatch_sensitivity` | Rare strict/TFT rows where price error and spread error are moderate but LP action sensitivity still creates regret. |
| `low_regret` | Rows below 250 UAH regret; useful positive examples for DFL table balancing. |

Interpretation:

- The risk-adjusted gate is also a negative selector result. It improves slightly over the mean-regret calibrated gate for selected TFT rows, but overall it is worse than both `strict_similar_day` and horizon-aware TFT.
- Tail-risk weighting does not solve the selector problem yet because the best candidate changes by tenant/anchor regime, and the selector still sees only coarse trailing regret statistics.
- The sensitivity frame is useful for the next DFL slice: it separates obvious price-error rows from LP sensitivity rows, so a future value-oriented loss can emphasize forecast shape and spread ordering instead of only scalar bias.
- Backend read model added: `GET /dashboard/risk-adjusted-value-gate?tenant_id=...`. No dashboard UI was changed.
- Forecast-dispatch explainability read model added: `GET /dashboard/forecast-dispatch-sensitivity?tenant_id=...`. It rebuilds sensitivity rows from persisted horizon-aware benchmark payloads and does not create bids or dispatch commands.

Runtime and GPU note:

The current compact NBEATSx/TFT code uses PyTorch but the installed wheel is CPU-only (`torch 2.11.0+cpu`, `torch.cuda.is_available() == False`). The machine has an NVIDIA GTX 1050 Ti with 4 GB VRAM, but this slice is dominated by small rolling-origin training windows, tiny LP solves, Polars transforms, and Dagster/process overhead. GPU enablement is unlikely to materially improve this MVP slice. GPU becomes useful later only if the project switches to heavier NeuralForecast/PyTorch Forecasting training or larger TimeXer-style experiments with a CUDA-enabled PyTorch build.

Research support:

- Decision-focused learning is the right framing because it trains prediction models for downstream constrained decision quality rather than forecast error alone: <https://huggingface.co/papers/2307.13565>.
- Perturbed DFL for strategic energy storage supports the next step beyond bias calibration: a differentiable decision-focused loss over a storage optimization layer: <https://arxiv.org/abs/2406.17085>.
- Predict-then-bid storage work supports the thesis architecture of price prediction plus storage optimization plus market-clearing-aware training: <https://arxiv.org/abs/2505.01551>.
- TimeXer is relevant future work for exogenous time-series modeling, but it is not needed for tonight's MVP because the current bottleneck is validated decision quality, not another heavy model: <https://huggingface.co/papers/2402.19072>.
- Time-Series-Library is useful as a benchmark/reference implementation source for future TimeXer experiments: <https://huggingface.co/lwaekfjlk/Time-Series-Library>.

## Research Read-Model Smoke

This slice persisted the new framework primitives behind backend read models and validated them through Dagster plus the Docker FastAPI service. The smoke was intentionally capped: it proves lineage, persistence, and API access without replacing the 90-anchor benchmark as the empirical source of truth.

Dagster smoke runs:

- Simulated DAM trade training -> offline DT trajectories -> simulated paper trading: `61ba2806-ee72-40fe-afbc-f8ccecb4e6f5`.
- Observed OREE/Open-Meteo Silver bridge -> capped Gold benchmark -> relaxed LP DFL pilot: `6e6ea066-f528-47ab-a5a2-eb1bc257d4ea`.
- Live exogenous Silver features -> SOTA forecast training contract: `2e5750e2-daf6-491a-a465-c502317f08d6`.

Postgres persistence after this read-model smoke:

| Table / rows | Count |
|---|---:|
| `dfl_relaxed_lp_pilot_runs` | 1 |
| `decision_transformer_trajectories` | 6 |
| `simulated_live_trading_rows` | 6 |

Docker API smoke on `http://localhost:8001`:

- `GET /dashboard/dfl-relaxed-pilot?tenant_id=client_003_dnipro_factory` returned 1 row with `academic_scope="differentiable_relaxed_lp_pilot_not_final_dfl"`.
- `GET /dashboard/decision-transformer-trajectories?tenant_id=client_003_dnipro_factory` returned 6 rows with `academic_scope="offline_dt_training_trajectory_not_live_policy"`.
- `GET /dashboard/simulated-live-trading?tenant_id=client_003_dnipro_factory` returned 6 rows with `simulated_only=true`.

Restoreable database dump after this slice:

`data/db_backups/smart_arbitrage_20260505_research_read_models.dump`

Claim boundary: these endpoints are dashboard-ready read models only. They do not promote the relaxed LP pilot to full DFL, do not train a deployable Decision Transformer, and do not execute live trades.

## Future Stack And Operator Dashboard Update

The latest follow-up slice adds a target-architecture read model and dashboard surface without changing the benchmark source of truth. It is meant to show where the system is going:

```text
NBEATSx/TFT forecast stack
  -> policy preview / value-gap evidence
  -> deterministic battery projection and gatekeeper boundary
  -> operator dashboard graph
```

Backend additions:

- `GET /dashboard/future-stack-preview?tenant_id=...` exposes NBEATSx/TFT forecast paths, backend availability for official SOTA libraries, selected forecast model, and claim boundary text.
- `GET /dashboard/decision-policy-preview?tenant_id=...` exposes projected offline DT policy-preview rows with value gap, feasible action, SOC before/after, and gatekeeper status.
- `GET /dashboard/operator-recommendation?tenant_id=...&strategy_id=...` now includes policy mode, selected policy id, policy readiness, forecast model series, and value-gap series for `/operator`.

Dashboard additions:

- `/operator` now has an NBEATSx/TFT forecast-stack graph and a DT value-gap/action graph fed by FastAPI read models.
- `/defense` now shows the future-stack forecast rows and a separate DT policy-preview boundary, so offline trajectory data is not confused with a deployed policy.
- The UI keeps DT as preview-only while `market_execution_enabled=false`; this prevents a premature live-trading claim.

Verification for this slice:

- Backend focused checks passed for DT policy preview, simulated-trade store persistence, Dagster asset wiring, and API responses.
- Dashboard `npm run lint`, `npm run typecheck`, `npm run build`, and `npx vitest run app/utils/defenseDataset.test.ts` passed.

Claim boundary: this update makes the future stack visible to operators and examiners, but it is still a read-model surface. Full NeuralForecast NBEATSx / PyTorch-Forecasting TFT and a deployable Decision Transformer require a separate materialized all-tenant experiment and promotion criteria.

## Verification

Commands run successfully:

```powershell
uv run ruff check .
uv run mypy .
uv run pytest
uv run dg check defs
uv run dg list defs --json
docker compose config --quiet
```

API smoke checks:

- `GET /health` returned `{"status":"ok"}`.
- `GET /dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory` returned the latest 90-anchor, 3-model benchmark summary and rows.
- `GET /dashboard/calibrated-ensemble-benchmark?tenant_id=client_003_dnipro_factory` returned the latest 90-anchor calibrated gate rows.
- `GET /dashboard/risk-adjusted-value-gate?tenant_id=client_003_dnipro_factory` returns the latest risk-adjusted selector rows after rebuilding the API service with this slice.
- `GET /dashboard/dfl-relaxed-pilot?tenant_id=client_003_dnipro_factory` returned the persisted relaxed-LP pilot row.
- `GET /dashboard/decision-transformer-trajectories?tenant_id=client_003_dnipro_factory` returned the persisted offline DT trajectory rows.
- `GET /dashboard/simulated-live-trading?tenant_id=client_003_dnipro_factory` returned simulated-only paper-trading rows.

Docker services were up for Postgres, MLflow, FastAPI, Dagster webserver, Dagster daemon, MQTT, and telemetry ingestor. The API is exposed on `127.0.0.1:8001`, while Dagster UI is on `127.0.0.1:3001`. Port `8000` is still occupied by a Windows `Manager` process that could not be stopped due to access denial, so the backend stack uses `SMART_ARBITRAGE_API_PORT=8001`.
