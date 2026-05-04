# Backend Telemetry and Simulated Training Slice

This slice keeps the battery model scoped as a Level 1 simulator: hourly SOC feasibility, throughput, EFC, and degradation-cost proxy. It does not claim full P2D, SEI, thermal, C-rate, or calendar-ageing digital twin behavior.

## Runtime Flow

- Docker Compose runs Postgres, Mosquitto MQTT, FastAPI, Dagster webserver/daemon, MLflow, a simulated telemetry publisher, and a telemetry ingestor.
- Host ports are configurable through environment variables. The API container still listens on `8000`; if the host already uses `8000`, run Compose with `SMART_ARBITRAGE_API_PORT=8001` instead of changing API code.
- Simulated MQTT topic format: `smart-arbitrage/{tenant_id}/battery/telemetry`.
- Raw telemetry is ingested every 5 simulated minutes into `battery_telemetry_observations`.
- Dagster asset `battery_state_hourly_silver` aggregates raw telemetry into `battery_state_hourly_snapshots`.
- Baseline LP uses the latest fresh hourly SOC snapshot when available; otherwise it falls back to the tenant registry initial SOC.

## Forecast and Training Data

- `neural_forecast_feature_frame` accepts optional hourly battery features: SOC, SOH, throughput, EFC delta, and telemetry freshness.
- These telemetry features are physical-state context, not a claim that battery state causes DAM prices.
- NBEATSx and TFT forecast runs persist summaries and forecast observations, and log metrics, forecast manifests, forecast rows, and frozen forecast candidate model versions to MLflow when `MLFLOW_TRACKING_URI` is configured.
- MLflow registered models are named `smart-arbitrage-nbeatsx-silver` and `smart-arbitrage-tft-silver`. They are registered forecast artifacts for reproducibility, not production dispatch policies.
- `simulated_trade_training_frame` generates DAM-only simulated trajectories with IDM-compatible naming reserved for later.
- Simulated transitions store state, action, feasible dispatch, reward, degradation penalty, baseline value, oracle value, regret, and a `ClearedTrade` payload with `provenance="simulated"`.

## E2E Experiment Check

Recommended local smoke run when `8000` is occupied:

```powershell
$env:SMART_ARBITRAGE_API_PORT = "8001"
docker compose up -d postgres mqtt mlflow api dagster-webserver dagster-daemon telemetry-ingestor telemetry-publisher
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select battery_telemetry_bronze,battery_state_hourly_silver
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select weather_forecast_bronze,dam_price_history,neural_forecast_feature_frame,nbeatsx_price_forecast,tft_price_forecast,simulated_trade_training_frame
Invoke-RestMethod "http://localhost:8001/health"
Invoke-RestMethod "http://localhost:8001/dashboard/battery-state?tenant_id=client_001_kyiv_mall"
Invoke-RestMethod "http://localhost:8001/dashboard/baseline-lp-preview?tenant_id=client_001_kyiv_mall"
```

Expected evidence:

- Dagster run success for telemetry, forecast, and simulated trade-training assets.
- `battery_telemetry_observations` and `battery_state_hourly_snapshots` have rows for all five tenants.
- `forecast_run_summaries` contains NBEATSx and TFT rows.
- MLflow contains experiment `smart-arbitrage-forecast-research` and registered models for both forecast candidates.
- `simulated_dispatch_transitions` contains only `cleared_trade_provenance = simulated`.

## Dashboard Plan For Later

- Show physical truth now from `/dashboard/battery-state.latest_telemetry`.
- Show planning truth from baseline/projected-state traces.
- Add stale telemetry warnings when baseline falls back to tenant defaults.
- Add model comparison cards for strict similar-day, NBEATSx, and TFT.
- Add regret panels for baseline-vs-oracle simulation trajectories and Gatekeeper outcomes.
