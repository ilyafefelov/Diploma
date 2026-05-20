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

## Readiness Preflight

`smart_arbitrage.forecasting.market_coupling_readiness` now summarizes whether
external features can be routed into official forecast training. The preflight
is intentionally stricter than a source-access check:

- missing ENTSO-E token blocks the route;
- source-backed ENTSO-E samples alone do not approve training;
- missing publication-time evidence or prior EUR/UAH FX normalization blocks
  the route;
- timezone/DST, licensing, market-rule, and Ukrainian-domain validation must be
  ready before `external_feature_training_ready=true`.

The output is a local evidence summary, not a public API contract. It preserves
`market_execution_enabled=false` and keeps all unapproved European rows out of
the Ukrainian official/DFL training panel.

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

The first leak-safe extension of that adapter is
`entsoe_poland_lagged_feature_candidate_frame`. It builds
`entsoe_pl_lag24_day_ahead_price_uah_mwh`, where a Ukrainian timestamp `t` uses
the source-backed Poland price from `t - 24h`. This is still an exogenous
feature candidate, not a European training row. It requires full benchmark
timestamp coverage and prior-known NBU EUR/UAH FX metadata before it can enter a
controlled ablation; official training remains blocked until domain shift is
validated under the unchanged V2+ strict LP/oracle gate.

As of the `week3_dfl_entsoe_poland_lag24_nbu_approved_route` packet, the
lagged Poland lane has full benchmark timestamp coverage (`11,638 / 11,638`)
and source-backed NBU EUR/UAH metadata (`485` effective dates). The route is
therefore approved for experimental ablation only:
`approved_for_experimental_ablation=true` and
`approved_for_official_training=false`. Domain-shift validation still requires a
separate Ukrainian-only V2+ versus Ukrainian-plus-Poland comparison.

The same approved-for-ablation lane now has a forecast-training interface for
official global-panel NBEATSx/TFT screens:
`official_global_panel_poland_lag24_experimental_training_frame`. It carries
the lagged Poland level, 1h/24h deltas, daily spread, daily price rank, and
lagged peak/trough hour as `known_future_feature_columns_csv`, so the existing
NBEATSx `futr_exog_list` and TFT `time_varying_known_reals` adapters can test
the features without changing the official Ukrainian-only training route. The
output model names are separated as
`nbeatsx_official_global_panel_poland_lag24_experimental_v1` and
`tft_official_global_panel_poland_lag24_experimental_v1`; they remain
experimental ablation candidates until they beat frozen V2+ under the strict
LP/oracle gate.

The first downstream strict LP/oracle schedule-value screen did not pass that
gate. The local packet
`data/research_runs/week3_poland_lag24_experimental_schedule_value_near_miss/`
records frozen Ukrainian-only V2+ at `174.77` UAH mean regret, Poland lag-24
NBEATSx V2+ at `184.66` UAH, and Poland lag-24 TFT V2+ at `218.12` UAH. This
is useful negative evidence: the route is now mechanically usable by
NBEATSx/TFT, but the current prior-safe Poland feature representation is not
strong enough to replace the Ukrainian-only V2+ schedule/value result.

The next feature representation branch adds prior-safe cross-market pressure
columns to that same route:
`entsoe_pl_lag24_ua_spread_uah_mwh`,
`entsoe_pl_lag24_ua_spread_delta_24h_uah_mwh`, and
`entsoe_pl_lag24_ua_spread_ratio`. These compare the lagged Poland day-ahead
price to the Ukrainian observed DAM price at the same lagged timestamp. They
are designed for the next experimental NBEATSx/TFT run because the failed
lag-24 level-only screen suggests that absolute Poland prices are less useful
than relative neighbor-versus-UA regimes.

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

1. Keep ENTSO-E token-backed and snapshot-backed Poland samples outside git and
   leave official training blocked by default.
2. Run the controlled Ukrainian-only V2+ versus Ukrainian-plus-Poland ablation
   only after the route reports `approved_for_experimental_ablation=true`.
3. Promote no external feature to official training until domain-shift
   validation passes under the same strict LP/oracle gate.
4. Rerun official global-panel parity only after at least one feature becomes
   `approved_for_training`.
