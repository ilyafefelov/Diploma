# Official Global-Panel NBEATSx

Date: 2026-05-11

This slice starts the stronger official-forecast lane without replacing the
compact in-repo candidates. The goal is to give Nixtla NBEATSx a fairer
training contract before spending CPU/GPU time on full rolling-origin
promotion evidence.

## Why This Exists

The compact `nbeatsx_silver_v0` and `tft_silver_v0` candidates are still useful
for smoke tests, deterministic evidence plumbing, and fast DFL research assets.
They are not the thesis-grade official library candidates.

The previous serious official path used `nbeatsx_official_v0` and
`tft_official_v0`, but retrained per tenant and per anchor. That is clean for
rolling-origin evaluation, but it gives the neural models little data per fit
and creates heavy CPU cost. The global-panel lane instead builds one
point-in-time panel:

- `unique_id = tenant_id:DAM`;
- `ds` = hourly timestamp;
- `y` = observed UAH/MWh price on train rows and null on forecast rows;
- known-future exogenous columns are separated from historical-observed columns;
- scaler metadata is explicit and train-only per `unique_id`.

This is closer to how NeuralForecast/NBEATSx should be used for a small panel
of related time series.

## Assets

| Asset | Layer | Purpose |
|---|---|---|
| `official_global_panel_training_frame` | Silver | Builds the multi-tenant point-in-time `unique_id/ds/y` panel and records scaler/exogenous metadata. |
| `nbeatsx_official_global_panel_price_forecast` | Silver | Trains one Nixtla NBEATSx model over the panel and predicts all forecast rows. |
| `nbeatsx_official_global_panel_strict_lp_benchmark_frame` | Gold | Strict-scores the global-panel NBEATSx schedule beside frozen `strict_similar_day`. |
| `nbeatsx_official_global_panel_horizon_calibration_frame` | Gold | Builds prior-only horizon-bias calibration rows from strict-scored global-panel evidence. |
| `nbeatsx_official_global_panel_calibrated_strict_lp_benchmark_frame` | Gold | Strict-scores raw and calibrated global-panel NBEATSx beside `strict_similar_day`. |

Tracked config:
[configs/real_data_official_global_panel_nbeatsx_week3.yaml](../../configs/real_data_official_global_panel_nbeatsx_week3.yaml).

## Contract

The global-panel training frame must satisfy:

- all forecast-horizon `y` values are null;
- feature/scaler metadata is fitted only on train rows per `unique_id`;
- `weather_temperature` and other known future fields appear only in the
  known-future feature list;
- lagged/rolling price features appear in historical-observed feature lists;
- `not_full_dfl=true` and `not_market_execution=true` remain claim boundaries.

Mutating final-holdout actual prices may change final LP/oracle scoring later,
but it must not change forecast features, scaler metadata, or selected model
configuration.

## Current Claim

Implemented now:

- additive global-panel SOTA training contract;
- additive official global-panel NBEATSx adapter surface;
- additive strict LP/oracle scoring asset for the global-panel NBEATSx output.
- additive prior-only horizon calibration and calibrated strict LP gate.

Not implemented yet:

- rolling-origin global-panel cross-validation;
- source/regime production promotion;
- Hugging Face Jobs execution wrapper.

## Next Required Work

1. Add rolling global-panel cross-validation so one NBEATSx fit can serve a
   panel/window instead of retraining tenant-by-anchor.
2. Feed calibrated rows into the existing schedule/value candidate library and
   strict promotion gate.
3. Package the same command for cloud offload only after latest-window screening
   shows the official source is close to `strict_similar_day`.

The frozen `strict_similar_day` baseline remains the default fallback until the
strict LP/oracle gate passes.
