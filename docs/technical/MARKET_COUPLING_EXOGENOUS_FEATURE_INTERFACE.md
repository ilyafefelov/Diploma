# Market-Coupling Exogenous Feature Interface

Date: 2026-05-12

This slice adds a single guarded interface for future market-coupling
covariates. It does not change the current thesis result: the robust evidence
headline remains the Ukrainian-only official global-panel NBEATSx
schedule/value **Offline Strategy Promotion** result on the 365-anchor backfill
panel. No ENTSO-E, OPSD, Ember, Nord Pool, PriceFM, or THieF rows are allowed
into Ukrainian NBEATSx/TFT/DFL training in this slice.

## Thesis Evidence Freeze

The frozen claim is:

> Official global-panel NBEATSx can feed a robust Offline Strategy Promotion
> DFL-style schedule/value challenger behind the frozen `strict_similar_day`
> fallback on the source-backed Ukrainian OREE/Open-Meteo panel.

The claim explicitly excludes:

- live market execution;
- dashboard/API default switching;
- deployed Decision Transformer control;
- raw NBEATSx forecast superiority over `strict_similar_day`;
- European/neighbor-market rows as current Ukrainian training inputs.

The current market-coupling route is therefore an interface and governance
gate. It prepares future features, but it intentionally preserves parity with
the already recorded Ukrainian-only 365-anchor evidence until an external
feature is approved.

## Why The Interface Exists

Electricity-price literature supports exogenous and coupled-market features:
neighboring markets, load, weather, renewable generation, and cross-border
price signals can help forecast day-ahead prices. The same literature also
implies a leakage risk for arbitrage research. A neighboring price is only a
valid training feature if it was published before the Ukrainian decision anchor
and can be normalized without future information.

The repository now concentrates that logic in one module:

- `smart_arbitrage.forecasting.market_coupling_features`

The module emits an approved prior-only exogenous feature route rather than
letting individual assets decide independently whether an external feature is
safe to use.

## Dagster Assets

| Asset | Layer | Purpose |
|---|---|---|
| `market_coupling_temporal_availability_frame` | Gold evidence | Source-level readiness for ENTSO-E, PriceFM, OPSD, Ember, Nord Pool, and THieF. |
| `entsoe_neighbor_market_query_spec_frame` | Gold evidence | ENTSO-E `A44/A01` neighboring day-ahead price query specs. |
| `entsoe_neighbor_market_sample_audit_frame` | Gold evidence | Optional fetch-disabled/source-backed sample audit. |
| `entsoe_neighbor_market_feature_candidate_frame` | Gold evidence | Converts ENTSO-E sample rows into blocked feature-candidate rows. |
| `official_forecast_exogenous_governance_frame` | Silver forecast features | Existing official forecast governance metadata. |
| `official_forecast_exogenous_feature_route_frame` | Silver forecast features | The single training route consumed by official global-panel training. |
| `official_global_panel_training_frame` | Silver forecast features | Records approved/blocked exogenous columns and builds the Nixtla-style panel. |

Asset checks:

| Check | Requirement |
|---|---|
| `market_coupling_temporal_availability_evidence` | Every external source keeps blockers and claim flags until governance passes. |
| `official_forecast_exogenous_feature_route_evidence` | The single official-training route rejects ungoverned external features before they can enter NBEATSx/TFT/DFL training. |
| `entsoe_neighbor_market_access_evidence` | ENTSO-E query specs stay research-only and use day-ahead price request shape `A44/A01`. |
| `entsoe_neighbor_market_sample_audit_evidence` | Sample rows never unlock training by themselves. |
| `entsoe_neighbor_market_feature_candidate_evidence` | Feature candidates remain blocked unless source-backed and fully governed. |

## Route Semantics

The route frame has one row per candidate feature. Current statuses are:

| Status | Meaning | Training effect |
|---|---|---|
| `blocked_by_governance` | Source is listed but not usable as a prior-only feature. | Not routed into training. |
| `source_backed_but_governance_blocked` | A real sample row exists, but at least one governance blocker remains. | Not routed into training. |
| `approved_for_training` | Source-backed row passes licensing, timezone, currency, market-rule, temporal-availability, and domain-shift gates. | May be included by official training. |

Approval requires all of the following:

- source-backed row;
- `training_use_allowed=true`;
- `feature_use_allowed=true`;
- publication timestamp available before the Ukrainian decision anchor;
- timezone/DST alignment to Ukrainian anchors;
- currency normalization using only prior-known information;
- market-rule and price-cap mapping;
- licensing/API terms review;
- domain-shift validation against Ukrainian holdout evidence.

## Current Parity Decision

Current route outcome:

- approved external feature columns: none;
- blocked external feature columns: `entsoe_neighbor_day_ahead_price_context`,
  `europe_generation_mix_context_placeholder`,
  `european_power_system_time_series_placeholder`,
  `nord_pool_price_context_placeholder`,
  `pricefm_european_price_context`, and
  `thief_temporal_hierarchy_context`;
- official global-panel training can consume the route without changing the
  current forecast feature set;
- the route itself is now Dagster-check-backed through
  `official_forecast_exogenous_feature_route_evidence`;
- the 365-anchor Offline Strategy Promotion evidence remains Ukrainian-only and
  source-backed by OREE/Open-Meteo.

This is intentional. The interface makes the next ENTSO-E/OPSD/PriceFM work
safer, but it does not silently change the thesis result.

## Local Materialization Evidence

Validated on 2026-05-12 after rebuilding the backend/Dagster services.

| Run | Selection | Result |
|---|---|---|
| `55c0b870-7d1d-464a-9553-6b4dc0a738d9` | `forecast_afe_feature_catalog_frame,market_coupling_temporal_availability_frame,entsoe_neighbor_market_query_spec_frame,entsoe_neighbor_market_sample_audit_frame,entsoe_neighbor_market_feature_candidate_frame` | `RUN_SUCCESS`; ENTSO-E feature-candidate check passed. |
| `6a5bb3b2-ede6-4f38-819c-2f50bc9622f0` | `observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_exogenous_governance_frame,official_forecast_exogenous_feature_route_frame,official_global_panel_training_frame` | `RUN_SUCCESS`; official global-panel training consumed the guarded route. |

Observed frame summary:

| Frame | Shape | Evidence |
|---|---:|---|
| `entsoe_neighbor_market_feature_candidate_frame` | Guard row | Guard row for `entsoe_pl_day_ahead_price_eur_mwh`; `source_backed=false`, `training_use_allowed=false`, `feature_use_allowed=false`, `publication_time_status=blocked_missing_publication_timestamp`, and `currency_normalization_status=blocked_missing_prior_eur_uah_fx_rate`. |
| `official_forecast_exogenous_feature_route_frame` | `6 x 23` | Six external feature candidates; all `training_use_allowed=false` and `feature_use_allowed=false`. |
| `official_global_panel_training_frame` | `58,190 x 60` | `external_feature_training_status=blocked_by_governance`, `allowed_external_feature_columns_csv=""`, and all six external candidates listed as blocked. |

## First Source-Backed Adapter

The first concrete adapter is ENTSO-E neighbor-market day-ahead price context.
It prepares feature-candidate rows such as:

- `entsoe_pl_day_ahead_price_eur_mwh`;
- `entsoe_sk_day_ahead_price_eur_mwh`;
- `entsoe_hu_day_ahead_price_eur_mwh`;
- `entsoe_ro_day_ahead_price_eur_mwh`.

These rows remain blocked even when a source sample is parsed. They need
publication-time, licensing, market-rule, and currency approval before they can
be routed into official/DFL training. The current candidate contract therefore
keeps parsed EUR prices separate from model inputs: `neighbor_market_price_uah_mwh`
stays null until the row has a prior-known publication timestamp and a prior-known
EUR/UAH FX rate.

## Academic And Source Basis

- Nixtla NeuralForecast supports static, historic, and future exogenous
  variables, and explicitly warns that putting historic-only variables into the
  future feature channel causes leakage.
- Nixtla NBEATSx is the relevant official forecast family because it supports
  exogenous temporal variables.
- Market-coupling EPF studies show that coupled-market and neighboring-zone
  features can improve day-ahead price forecasts, but feature selection and
  market-specific validation remain necessary.
- Decision-focused ESS arbitrage studies justify keeping the final acceptance
  rule on LP/oracle regret and net value rather than forecast metrics alone.

Source capture:
[market-coupling-exogenous-feature-interface-source-capture-2026-05-12.md](../sources/market-coupling-exogenous-feature-interface-source-capture-2026-05-12.md).

## Verification

Focused tests added in this slice cover:

- blocked route rows remain blocked;
- source-backed ENTSO-E samples are still blocked until publication-time,
  currency, and broader governance checks pass;
- official global-panel training records approved and blocked exogenous columns;
- Dagster asset/check registration includes the route check and ENTSO-E feature
  candidate assets;
- false claim flags or accidental training use fail evidence validation.

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider `
  tests\forecasting\test_market_coupling_features.py `
  tests\forecasting\test_entsoe_neighbor_market_access.py `
  tests\forecasting\test_sota_training_frame.py `
  tests\forecasting\test_neural_forecast_silver.py `
  tests\assets\test_dfl_research_assets.py `
  tests\assets\test_evidence_checks.py
```

## Next Work

1. Configure an ENTSO-E token outside git and fetch a tiny Poland sample with
   training still blocked.
2. Add publication-time evidence for the neighboring source.
3. Add prior-only EUR/UAH normalization if the data terms permit it.
4. Rerun official global-panel parity only after at least one feature becomes
   `approved_for_training`.
