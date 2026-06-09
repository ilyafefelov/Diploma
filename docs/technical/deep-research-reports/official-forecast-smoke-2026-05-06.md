# Official NBEATSx/TFT Adapter Smoke Result - 2026-05-06

## Scope

This is a smoke-sized implementation check for the optional official forecast backends. It is not a thesis-grade rolling-origin benchmark and it is not a promoted operator strategy.

Command:

```powershell
.\.venv\Scripts\python.exe scripts\run_official_forecast_smoke.py --horizon-hours 6 --nbeatsx-max-steps 1 --tft-max-epochs 1
```

Artifacts:

- `reports/official_forecast_smoke/official_forecast_smoke_20260506T051503Z_summary.json`
- `reports/official_forecast_smoke/official_forecast_smoke_20260506T051503Z_forecasts.csv`

## Result

- Runtime: `torch 2.11.0+cpu`; no CUDA/MPS acceleration available in the current environment.
- Market source: local Dagster storage `data/dagster_home/storage/dam_price_history`.
- Tenant: `client_003_dnipro_factory`.
- Horizon: 6 hourly rows.
- `nbeatsx_official_v0`: 6 forecast rows from NeuralForecast NBEATSx.
- `tft_official_v0`: 6 forecast rows from PyTorch Forecasting TFT with quantile output.
- Adapter errors: none.

## Quality Flag

The smoke run produced useful diagnostics:

- `tft_official_v0` stayed inside the DAM cap range used for smoke validation.
- `nbeatsx_official_v0` produced out-of-cap values in the one-step smoke configuration.

Interpretation: the official adapters execute successfully, but the current NBEATSx smoke configuration is not calibrated enough for value claims. It should be treated as backend readiness evidence only until a proper rolling-origin tuning/evaluation run is materialized.

## Next Research Step

1. Persist official forecast rows through the forecast store or Dagster Silver assets.
2. Add forecast-quality flags to the dashboard/defense surfaces when official rows are present.
3. Run a longer rolling-origin official-forecast benchmark before making any SOTA or live-operator claim.
4. Only after official forecast rows are stable should the Decision Transformer policy be evaluated against the same strict LP/simulator and hold/no-arbitrage baseline.
