# Nixtla NeuralForecast / NBEATSx Source Capture

Captured: 2026-05-11
Scope: official NeuralForecast usage, NBEATSx calibration direction, and thesis-safe rolling-origin evaluation.

## Why This Capture Exists

The project currently has two NBEATSx-related paths:

- `nbeatsx_silver_v0`: compact in-repo PyTorch candidate for fast smoke/dev evidence.
- `nbeatsx_official_v0`: official Nixtla NeuralForecast adapter path for serious research evidence.

This source capture records why the official Nixtla path should be treated as the thesis-grade NBEATSx candidate, while the compact path remains a local baseline/prototype. It also records the governance rules needed before routing official predictions into DFL or promotion gates.

## Sources

| Source | Status | Project Use |
|---|---|---|
| [NeuralForecast Quickstart](https://nixtlaverse.nixtla.io/neuralforecast/docs/getting-started/quickstart.html) | include | Confirms the canonical `NeuralForecast(models=[...], freq=...)`, `.fit(df=...)`, `.predict(...)` workflow and the required long DataFrame shape: `unique_id`, `ds`, `y`. |
| [NBEATSx model docs](https://nixtlaverse.nixtla.io/neuralforecast/models.nbeatsx.html) | include | Defines NBEATSx as the exogenous-variable extension of N-BEATS, with `futr_exog_list`, `hist_exog_list`, `stat_exog_list`, scaling, training steps, and model hyperparameters. |
| [Exogenous Variables guide](https://nixtlaverse.nixtla.io/neuralforecast/docs/capabilities/exogenous_variables.html) | include | Supports the project direction that weather, load, calendar, market-regime, and future-known context should be governed explicitly as future, historic, or static exogenous variables. |
| [Cross-validation guide](https://nixtlaverse.nixtla.io/neuralforecast/docs/capabilities/cross_validation.html) | include | Supports rolling-window / cutoff-based validation before strict LP/oracle promotion. Useful for replacing ad hoc long runs with smaller screening windows. |

## Evidence Extracted For The Project

- NeuralForecast expects long-form panel data with `unique_id`, `ds`, and `y`; this matches the current `sota_forecast_training_frame` contract.
- NBEATSx is the correct official candidate for exogenous electricity-price forecasting, not merely a generic neural baseline.
- Exogenous variables must be separated by temporal availability:
  - future-known: calendar, weather forecast, declared market-rule changes;
  - historic-only: observed load, observed prices, realized weather and event indicators;
  - static: tenant, market, location, BESS configuration.
- The exogenous-variable guide explicitly warns that misclassifying historic variables as future variables causes leakage. This supports the current AFE/AFL feature-contract hardening.
- Cross-validation with multiple windows and optional refit is the correct way to screen official models before expensive full promotion runs.

## Project Decisions

1. `nbeatsx_official_v0` should become the serious NBEATSx evidence path.
2. `nbeatsx_silver_v0` should remain a compact development/control candidate, not the thesis-grade NBEATSx claim.
3. Raw official NBEATSx outputs should not be promoted directly when they produce out-of-range or poorly calibrated price forecasts.
4. Add a prior-only calibrated official candidate before DFL:
   - clamp to Ukrainian DAM price floor/cap;
   - apply tenant/horizon residual calibration from prior anchors only;
   - preserve raw official output as an audit column;
   - strict-score both raw and calibrated variants through the same LP/oracle gate.
5. Use smaller latest-window screening runs before full `104`-anchor official materialization.

## Claim Boundary

This source package supports an official NBEATSx research path. It does not justify:

- live market execution;
- replacing `strict_similar_day` without strict LP/oracle promotion;
- using final-holdout actuals for calibration;
- mixing European market data into Ukrainian training without licensing, timezone, currency, market-rule, and domain-shift controls.
