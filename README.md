# Smart Energy Arbitrage 2026

Research framework for BESS energy arbitrage in Ukraine. Current MVP is not a trading bot. It is a reproducible Dagster/FastAPI/Postgres/MLflow stack for observed DAM data, tenant weather, battery telemetry, Level 1 LP dispatch evaluation, forecast comparison, and thesis-safe regret analysis.

## Current Status

- Real-data benchmark: observed OREE DAM prices plus tenant Open-Meteo weather.
- Tenants: 5 simulated BESS tenants from `simulations/tenants.yml`.
- Main control: `strict_similar_day`.
- Forecast candidates: compact `nbeatsx_silver_v0` and `tft_silver_v0`.
- Research layer: forecast diagnostics, value-aware ensemble gate, calibrated horizon-aware ensemble gate, risk-adjusted selector diagnostics, DFL-ready training table, scalar and horizon-aware regret-weighted TFT/NBEATSx calibration, strict LP/oracle re-evaluation.
- New framework primitives: explicit Bronze/Silver/Gold asset tags, a real-data Silver benchmark feature bridge, SOTA-ready `unique_id`/`ds`/`y` training schema, differentiable relaxed-LP DFL pilot rows, offline Decision Transformer trajectory rows, DT safety projection, and simulated paper-trading replay rows.
- Dashboard UI is separate and was not changed in the latest research slice.

## Pipeline And LP Baseline

The current operational core is a deterministic Level 1 LP baseline, not an ML
policy. The pipeline is:

```text
Bronze market/weather/telemetry data
  -> Silver hourly features and forecast inputs
  -> strict similar-day forecast or ML forecast candidates
  -> LP battery dispatch optimizer
  -> Pydantic safety validation
  -> oracle/regret benchmark and dashboard/API read models
```

The LP maximizes forecast market value minus degradation cost under SOC, power,
capacity, and efficiency constraints. It solves a 24-hour DAM schedule but commits
only the first interval in a rolling-horizon pattern. NBEATSx/TFT are research
forecast candidates upstream of the LP; the LP itself does not train or learn.

Full formula, ML/non-ML boundaries, SOC handling, and academic support are documented
in [docs/technical/BASELINE_LP_AND_DATA_PIPELINE.md](docs/technical/BASELINE_LP_AND_DATA_PIPELINE.md).

## Operator Weather Signal

The operator dashboard weather line is intentionally a read model, not the LP
decision input. It shows:

```text
price_after_weather = market_price + weather_bias
```

`weather_bias` is a calibrated non-negative weather uplift in `UAH/MWh`, derived
from cloud cover, precipitation, humidity excess, temperature gap, effective
solar, and wind speed. This is fine for MVP explanation and supervisor demos, but
it should not be described as a causal price model or routed directly into the LP.
The planned upgrade path is weather-aware forecasting first, then LP dispatch:

```text
weather features -> NBEATSx/TFT/TimeXer-style price forecast -> LP schedule
```

The academic boundary and exact formula are documented in
[docs/technical/BASELINE_LP_AND_DATA_PIPELINE.md#operator-weather-signal](docs/technical/BASELINE_LP_AND_DATA_PIPELINE.md#operator-weather-signal).

Latest materialized result:

| Model | Rows | Mean regret UAH | Median regret UAH | Win rate |
|---|---:|---:|---:|---:|
| tft_horizon_regret_weighted_calibrated_v0 | 450 | 834.32 | 558.87 | 22.00% |
| strict_similar_day | 450 | 851.04 | 535.62 | 38.00% |
| calibrated_value_aware_ensemble_v0 | 450 | 913.92 | 565.50 | n/a |
| risk_adjusted_value_gate_v0 | 450 | 918.76 | 566.70 | n/a |
| nbeatsx_horizon_regret_weighted_calibrated_v0 | 450 | 941.74 | 653.24 | 17.78% |
| tft_silver_v0 | 450 | 1,128.75 | 732.66 | 13.56% |
| nbeatsx_silver_v0 | 450 | 1,164.17 | 833.18 | 8.67% |

Interpretation: horizon-aware TFT calibration is the first diagnostic to beat the strict control on mean regret, but strict similar-day still has better median regret and more rank-1 wins. The calibrated and risk-adjusted gates are negative selector results: they are better than raw compact neural candidates but worse than both strict and horizon-TFT. This is not full DFL and is not a dashboard default yet; it is evidence that horizon-structured value calibration is worth expanding into a real DFL objective.

## Local Stack

```powershell
uv sync --all-extras
docker compose up -d postgres mqtt mlflow dagster-webserver dagster-daemon api
```

If port `8000` is occupied by Windows `Manager`, run API on `8001`:

```powershell
$env:SMART_ARBITRAGE_API_PORT='8001'
docker compose up -d api dagster-webserver dagster-daemon
```

Useful local URLs:

- FastAPI health: `http://localhost:8001/health`
- Calibrated ensemble API: `http://localhost:8001/dashboard/calibrated-ensemble-benchmark?tenant_id=client_003_dnipro_factory`
- Risk-adjusted value gate API: `http://localhost:8001/dashboard/risk-adjusted-value-gate?tenant_id=client_003_dnipro_factory`
- Forecast-dispatch sensitivity API: `http://localhost:8001/dashboard/forecast-dispatch-sensitivity?tenant_id=client_003_dnipro_factory`
- Relaxed DFL pilot API: `http://localhost:8001/dashboard/dfl-relaxed-pilot?tenant_id=client_003_dnipro_factory`
- Offline DT trajectories API: `http://localhost:8001/dashboard/decision-transformer-trajectories?tenant_id=client_003_dnipro_factory`
- Simulated live-trading API: `http://localhost:8001/dashboard/simulated-live-trading?tenant_id=client_003_dnipro_factory`
- Dagster UI: `http://localhost:3001`
- MLflow UI: `http://localhost:5000`

## Verification

```powershell
uv run ruff check .
uv run mypy .
uv run pytest
uv run dg check defs
uv run dg list defs --json
docker compose config --quiet
```

Latest full verification: `113 passed`.

## Research Artifacts

- Baseline LP and pipeline note: `docs/technical/BASELINE_LP_AND_DATA_PIPELINE.md`
- Main report: `docs/technical/deep-research-reports/real-data-90-anchor-benchmark-report.md`
- Latest exports: `data/research_runs/risk_gate_diagnostics_20260505T151401/`
- Latest DB dump: `data/db_backups/smart_arbitrage_20260505_research_read_models.dump`
- MLflow run: `smart-arbitrage-horizon-regret-weighted-dfl-expansion`, run `9d61ef79a0d34214b2de6617346a616e`
- Calibrated ensemble MLflow run: `smart-arbitrage-calibrated-ensemble-gate`, run `661189d0b8a1497784e26f3831f77fc7`
- Risk-adjusted gate MLflow run: `smart-arbitrage-risk-adjusted-value-gate`, run `e30a3095d8bd48eb9e01b317e6b60bc1`

Latest read-model smoke:

| Output | Rows | Scope |
|---|---:|---|
| `dfl_relaxed_lp_pilot_runs` | 1 | differentiable relaxed LP primitive, not full DFL |
| `decision_transformer_trajectories` | 6 | offline trajectory data, not live policy |
| `simulated_live_trading_rows` | 6 | simulated paper-trading replay, no settlement IDs |

`data/` and `mlruns/` are local artifacts and are intentionally not tracked.

## Research Basis

- DFL survey: <https://huggingface.co/papers/2307.13565>
- Perturbed DFL for strategic energy storage: <https://arxiv.org/abs/2406.17085>
- Decision-focused predict-then-bid for strategic energy storage: <https://arxiv.org/abs/2505.01551>
- TimeXer for exogenous time-series forecasting: <https://huggingface.co/papers/2402.19072>
- Time-Series-Library reference implementation: <https://huggingface.co/lwaekfjlk/Time-Series-Library>

## Claim Boundaries

- `sota_forecast_training_frame` is a backend contract for full NeuralForecast NBEATSx and PyTorch-Forecasting TFT experiments. It is not itself a tuned SOTA model result.
- `dfl_relaxed_lp_pilot_frame` uses `cvxpylayers` as a differentiable relaxed LP primitive. Final thesis metrics must still come from the strict LP/simulator path.
- `decision_transformer_trajectory_frame` and `DecisionTransformerPolicy` provide offline return-conditioned policy scaffolding plus deterministic action projection. They are not yet a trained deployable DT strategy.
- `simulated_live_trading_frame` is paper-trading replay only. It never carries real settlement IDs and must not be described as market execution.

GPU note: machine has GTX 1050 Ti, but current Python env has CPU-only PyTorch (`torch 2.11.0+cpu`). Current workload is mostly small rolling-origin training, tiny LP solves, Polars transforms, and Dagster/process overhead. GPU is not expected to help this MVP slice materially. CUDA PyTorch becomes useful only for heavier NeuralForecast/PyTorch Forecasting/TimeXer experiments.
