# Official Forecast And Exogenous Governance Source Capture

Date: 2026-05-11

Purpose: record the sources used to justify the next official NBEATSx/TFT
materialization and the market-coupling/exogenous feature governance track.

Claim boundary: these sources justify future feature engineering and official
forecast training. They do not prove promoted DFL, Decision Transformer control,
or live market execution.

## Source Status

| Status | Source | What it supports | Current training use |
|---|---|---|---|
| include | [Nixtla NeuralForecast end-to-end walkthrough](https://nixtlaverse.nixtla.io/neuralforecast/docs/tutorials/getting_started_complete.html) | Long-format global forecasting workflow, rolling/cross-validation framing, and the `unique_id` / `ds` / `y` convention used by official adapters. | Allowed for adapter implementation guidance; not a dataset. |
| include | [Nixtla NBEATSx documentation](https://nixtlaverse.nixtla.io/neuralforecast/models.nbeatsx.html) | NBEATSx as the official exogenous-variable N-BEATS model family for electricity-price forecasting experiments. | Allowed as model documentation only. |
| include | [Nixtla exogenous variables guide](https://nixtlaverse.nixtla.io/neuralforecast/docs/capabilities/exogenous_variables.html) | Distinguishes historic, future, and static exogenous variables; supports the need for temporal-availability governance before training. | Allowed as feature-contract guidance only. |
| include | [Temporal Fusion Transformers, arXiv:1912.09363](https://arxiv.org/abs/1912.09363) | TFT as the official multi-horizon forecast model with static, known-future, and observed-historical covariate handling. | Allowed as model/literature support only. |
| include | [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) | Historical hourly weather provenance, coordinates, date window, and weather variables for Ukrainian tenant context. | Allowed when rows are timestamp-aligned and available at the decision cut. |
| include/watch | [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) | Neighbor-market and market-coupling candidate source for European price and system signals. | Blocked from training until token/licensing, timezone, currency, market-rule, and temporal-availability checks pass. |
| include/watch | [Open Power System Data](https://open-power-system-data.org/) and [OPSD time series](https://data.open-power-system-data.org/time_series/) | European electricity consumption, spot price, wind, and solar time-series bridge for external validation and feature governance. | Blocked from Ukrainian training until domain-shift and market-rule mapping pass. |
| watch | [Ember API](https://ember-energy.org/data/api/) | Open electricity generation, demand, emissions, and carbon-intensity context for macro/regime features. | Blocked from training; possible later low-frequency context only. |
| watch/restricted | [Nord Pool Data Portal](https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/) | Commercial European power-market source with detailed day-ahead, intraday, and power-system data. | Blocked from training until access, redistribution, and licensing are resolved. |

## Governance Rules To Carry Forward

- Ukrainian observed OREE DAM and Open-Meteo remain the current thesis-grade
  training source of truth.
- European rows cannot be mixed into Ukrainian DFL training until licensing,
  timezone alignment, currency normalization, market-rule mapping, temporal
  availability, and domain-shift normalization are all explicit.
- Exogenous features must be labeled by availability class:
  `historic_exogenous`, `future_known_exogenous`, or `static_exogenous`.
- Weather and market-coupling features may enter official NBEATSx/TFT only after
  the feature gate proves they are known before the forecast decision cut.
- Official forecasts still need strict LP/oracle schedule scoring; library loss
  curves are not enough for thesis promotion.

## Near-Term Engineering Use

The immediate engineering use is the resumable official 104-anchor run:

- run official NBEATSx/TFT rolling-origin anchors in persisted batches;
- keep `strict_similar_day` as frozen fallback and promotion authority;
- feed completed official rows into the schedule/value candidate library;
- update the source-governance path before adding any new external signals.
