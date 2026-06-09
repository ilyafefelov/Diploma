# Source Note: Forecast Quality And BESS Day-Ahead Dispatch Value

Source: [The impact of electricity price forecasting on the optimal day-ahead
dispatch of battery energy storage systems](https://www.dfki.de/web/forschung/projekte-publikationen/publikation/16172)

Authors: Leon Tadayon, Dominic Detering, Wolfgang Maass, Georg Frey

Venue: NEIS 2025 - Conference on Sustainable Energy Supply and Energy Storage
Systems, Hamburg, Germany, 15-16 September 2025

Status: include

Local artifact type: source note. A PDF was not exposed from the publication
page during source capture.

## Thesis Use

This source directly supports the project choice to evaluate electricity-price
forecasts by downstream BESS dispatch value, not only by forecast-error metrics.
It is especially relevant for **Arbitrage-Focused Learning (AFL)** and the
later transition to **Decision-Focused Learning (DFL)**.

Use it to support these claims:

- BESS dispatch strategies depend on price forecasts for the dispatch period.
- Forecast quality affects realized dispatch economics.
- Forecast models should be compared by realized revenue or missed revenue
  against a perfect-foresight/oracle dispatch benchmark.
- Battery ageing/degradation costs can materially change dispatch economics.

## Mapping To This Repo

| Source idea | Project artifact |
|---|---|
| Forecasts should be evaluated through BESS dispatch economics | `real_data_rolling_origin_benchmark_frame`, strict LP/oracle regret |
| Perfect foresight provides an upper-bound benchmark | oracle LP scoring in strategy evaluation |
| Ageing costs affect dispatch economics | degradation-aware proxy and net-value metrics |
| Forecast-only metrics are insufficient | `forecast_candidate_forensics_frame`, `afl_training_panel_frame` |

## Claim Boundary

This source supports decision-value forecast evaluation. It does not imply that
the current system performs market execution, full electrochemical degradation
modelling, full DFL, or Decision Transformer control.

