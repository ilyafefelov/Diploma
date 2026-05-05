# Smart Energy Arbitrage 2026

Research framework for BESS energy arbitrage in Ukraine. Current MVP is not a trading bot. It is a reproducible Dagster/FastAPI/Postgres/MLflow stack for observed DAM data, tenant weather, battery telemetry, Level 1 LP dispatch evaluation, forecast comparison, and thesis-safe regret analysis.

## Current Status

- Real-data benchmark: observed OREE DAM prices plus tenant Open-Meteo weather.
- Tenants: 5 simulated BESS tenants from `simulations/tenants.yml`.
- Main control: `strict_similar_day`.
- Forecast candidates: compact `nbeatsx_silver_v0` and `tft_silver_v0`.
- Research layer: forecast diagnostics, value-aware ensemble gate, DFL-ready training table, scalar and horizon-aware regret-weighted TFT/NBEATSx calibration, strict LP/oracle re-evaluation.
- Dashboard UI is separate and was not changed in the latest research slice.

Latest materialized result:

| Model | Rows | Mean regret UAH | Median regret UAH | Win rate |
|---|---:|---:|---:|---:|
| tft_horizon_regret_weighted_calibrated_v0 | 450 | 834.32 | 558.87 | 22.00% |
| strict_similar_day | 450 | 851.04 | 535.62 | 38.00% |
| nbeatsx_horizon_regret_weighted_calibrated_v0 | 450 | 941.74 | 653.24 | 17.78% |
| tft_silver_v0 | 450 | 1,128.75 | 732.66 | 13.56% |
| nbeatsx_silver_v0 | 450 | 1,164.17 | 833.18 | 8.67% |

Interpretation: horizon-aware TFT calibration is the first diagnostic to beat the strict control on mean regret, but strict similar-day still has better median regret and more rank-1 wins. This is not full DFL and is not a dashboard default yet; it is evidence that horizon-structured value calibration is worth expanding into a real DFL objective.

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

Latest full verification: `91 passed`.

## Research Artifacts

- Main report: `docs/technical/deep-research-reports/real-data-90-anchor-benchmark-report.md`
- Latest exports: `data/research_runs/horizon_dfl_expansion_20260505T140430/`
- Latest DB dump: `data/db_backups/smart_arbitrage_horizon_dfl_expansion_20260505T140430.dump`
- MLflow run: `smart-arbitrage-horizon-regret-weighted-dfl-expansion`, run `9d61ef79a0d34214b2de6617346a616e`

`data/` and `mlruns/` are local artifacts and are intentionally not tracked.

## Research Basis

- DFL survey: <https://huggingface.co/papers/2307.13565>
- Perturbed DFL for strategic energy storage: <https://arxiv.org/abs/2406.17085>
- Decision-focused predict-then-bid for strategic energy storage: <https://arxiv.org/abs/2505.01551>
- TimeXer for exogenous time-series forecasting: <https://huggingface.co/papers/2402.19072>
- Time-Series-Library reference implementation: <https://huggingface.co/lwaekfjlk/Time-Series-Library>

GPU note: machine has GTX 1050 Ti, but current Python env has CPU-only PyTorch (`torch 2.11.0+cpu`). Current workload is mostly small rolling-origin training, tiny LP solves, Polars transforms, and Dagster/process overhead. GPU is not expected to help this MVP slice materially. CUDA PyTorch becomes useful only for heavier NeuralForecast/PyTorch Forecasting/TimeXer experiments.
