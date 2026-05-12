# Market-Coupling Exogenous Feature Interface Source Capture

Date: 2026-05-12

Purpose: record the sources used to add the guarded market-coupling exogenous
feature route and to freeze the thesis claim as Ukrainian-only Offline Strategy
Promotion evidence until external features pass governance.

## Engineering Sources

| Source | URL | Use in this slice |
|---|---|---|
| ENTSO-E Transparency Platform | https://www.entsoe.eu/data/transparency-platform/ | Source registry for future neighboring-market day-ahead price context. |
| Nixtla NeuralForecast exogenous variables | https://nixtlaverse.nixtla.io/neuralforecast/docs/capabilities/exogenous_variables.html | Confirms the split between static, historic, and future exogenous features and the leakage risk when the split is wrong. |
| Nixtla NBEATSx documentation | https://nixtlaverse.nixtla.io/neuralforecast/models.nbeatsx.html | Supports treating official NBEATSx as the exogenous-capable forecast family. |
| RunyaoYu/PriceFM paper page | https://huggingface.co/papers/2508.04875 | Source metadata for future European graph/market-coupling forecasting context. |
| RunyaoYu/PriceFM model/data links | https://huggingface.co/RunyaoYu/PriceFM | Records model license/linkage and keeps PriceFM as future external-validation context only. |

## Academic Sources

| Source | DOI / URL | Finding used |
|---|---|---|
| Li and Becker, day-ahead EPF under market coupling | https://arxiv.org/abs/2101.05249 | Coupled-market features can affect price prediction; feature selection matters. |
| Redhu and Bremdal, neighboring-zone day-ahead forecasting | DOI: 10.1109/PowerTech55446.2023.10202771 | Neighboring zonal prices can be useful when cross-border connections create price covariance. |
| Mascarenhas et al., asynchronous cross-border market data | https://arxiv.org/abs/2507.13250 | Earlier gate-closure prices can help later markets, but more markets do not always help and recalibration has cost. |
| Saha and Lopez, deep forecaster with exogenous variables | https://arxiv.org/abs/2010.06525 | Exogenous variables such as load and weather can influence price peaks/valleys. |
| Sang et al., ESS arbitrage DFL | DOI: 10.1109/TSG.2022.3166791 / https://arxiv.org/abs/2305.00362 | Downstream arbitrage regret is the acceptance metric, not forecast accuracy alone. |

## Repo Decision

The implementation decision is to route future external features through
`official_forecast_exogenous_feature_route_frame` before official NBEATSx/TFT or
DFL training can use them.

Current state:

- source-backed ENTSO-E sample rows may be parsed;
- all ENTSO-E feature candidates remain `training_use_allowed=false`;
- PriceFM, OPSD, Ember, Nord Pool, and THieF remain research/external-validation
  context only;
- the 365-anchor official global-panel NBEATSx schedule/value Offline Strategy
  Promotion result remains Ukrainian OREE/Open-Meteo evidence only.

## Local Materialization Evidence

Validated after rebuilding backend/Dagster services on 2026-05-12:

- ENTSO-E candidate/access run:
  `55c0b870-7d1d-464a-9553-6b4dc0a738d9`.
- Official global-panel route parity run:
  `6a5bb3b2-ede6-4f38-819c-2f50bc9622f0`.
- `entsoe_neighbor_market_feature_candidate_frame`: `1 x 22`, guarded Poland
  candidate, `source_backed=false`, `training_use_allowed=false`,
  `feature_use_allowed=false`.
- `official_forecast_exogenous_feature_route_frame`: `6 x 23`, all external
  feature candidates blocked.
- `official_forecast_exogenous_feature_route_evidence`: registered as the
  Dagster check for the route, so accidental ungoverned approval fails before
  official training treats the feature as allowed.
- `official_global_panel_training_frame`: `58,190 x 60`,
  `external_feature_training_status=blocked_by_governance`,
  `allowed_external_feature_columns_csv=""`.

## Guardrails Carried Into Code

- External feature rows must not enter training until licensing, timezone,
  currency, market-rule, temporal-availability, and domain-shift blockers are
  cleared.
- Source-backed is not the same as training-approved.
- Official training consumes the feature route, not raw source metadata.
- The feature route is checked directly in Dagster, not only indirectly through
  downstream official training.
- `market_execution_enabled=false` remains mandatory for the thesis evidence
  freeze.
