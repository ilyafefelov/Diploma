# Backend Telemetry and Simulated Training Slice

This slice keeps the battery model scoped as a Level 1 simulator: hourly SOC feasibility, throughput, EFC, and degradation-cost proxy. It does not claim full P2D, SEI, thermal, C-rate, or calendar-ageing digital twin behavior.

## Runtime Flow

- Docker Compose runs Postgres, Mosquitto MQTT, FastAPI, Dagster webserver/daemon, MLflow, a simulated telemetry publisher, and a telemetry ingestor.
- Simulated MQTT topic format: `smart-arbitrage/{tenant_id}/battery/telemetry`.
- Raw telemetry is ingested every 5 simulated minutes into `battery_telemetry_observations`.
- Dagster asset `battery_state_hourly_silver` aggregates raw telemetry into `battery_state_hourly_snapshots`.
- Baseline LP uses the latest fresh hourly SOC snapshot when available; otherwise it falls back to the tenant registry initial SOC.

## Forecast and Training Data

- `neural_forecast_feature_frame` accepts optional hourly battery features: SOC, SOH, throughput, EFC delta, and telemetry freshness.
- These telemetry features are physical-state context, not a claim that battery state causes DAM prices.
- NBEATSx and TFT forecast runs persist summaries and forecast observations, and log compact metrics to MLflow when `MLFLOW_TRACKING_URI` is configured.
- `simulated_trade_training_frame` generates DAM-only simulated trajectories with IDM-compatible naming reserved for later.
- Simulated transitions store state, action, feasible dispatch, reward, degradation penalty, baseline value, oracle value, regret, and a `ClearedTrade` payload with `provenance="simulated"`.

## Dashboard Plan For Later

- Show physical truth now from `/dashboard/battery-state.latest_telemetry`.
- Show planning truth from baseline/projected-state traces.
- Add stale telemetry warnings when baseline falls back to tenant defaults.
- Add model comparison cards for strict similar-day, NBEATSx, and TFT.
- Add regret panels for baseline-vs-oracle simulation trajectories and Gatekeeper outcomes.
