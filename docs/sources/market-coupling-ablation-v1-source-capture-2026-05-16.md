# Market-Coupling Ablation V1 Source Capture

Date: 2026-05-16

Purpose: record the source trail and claim boundary for the governed
market-coupling ablation that compares the Ukrainian-only Schedule/Value Learner
V2+ baseline against a future Ukrainian plus approved neighbor-market feature
route.

## Existing Source Trail

| Source | Location | Use in this slice |
|---|---|---|
| Market-coupling exogenous feature interface capture | [market-coupling-exogenous-feature-interface-source-capture-2026-05-12.md](market-coupling-exogenous-feature-interface-source-capture-2026-05-12.md) | Documents ENTSO-E, Nixtla exogenous variables, PriceFM, and DFL sources behind the approved/blocked feature route. |
| HF Jobs and market-coupling readiness capture | [hf-jobs-market-coupling-readiness-source-capture-2026-05-12.md](hf-jobs-market-coupling-readiness-source-capture-2026-05-12.md) | Records the readiness preflight fields that must pass before external features affect official evidence. |
| ENTSO-E neighbor-market access gate | [../technical/ENTSOE_NEIGHBOR_MARKET_ACCESS_GATE.md](../technical/ENTSOE_NEIGHBOR_MARKET_ACCESS_GATE.md) | Defines the Poland-first query/audit lane and keeps samples non-training. |
| Market-coupling exogenous feature interface | [../technical/MARKET_COUPLING_EXOGENOUS_FEATURE_INTERFACE.md](../technical/MARKET_COUPLING_EXOGENOUS_FEATURE_INTERFACE.md) | Defines `official_forecast_exogenous_feature_route_frame` as the only route into official training. |
| V2+ evidence baseline | [../technical/DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS.md](../technical/DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS.md) | Freezes the Ukrainian-only V2+ comparator and its Offline Strategy Promotion metrics. |

## Thesis Literature Used

| Source | Local archive | Relevance |
|---|---|---|
| Decision-Focused Learning survey | [../thesis/sources/2307.13565v4-decision-focused-learning-survey.pdf](../thesis/sources/2307.13565v4-decision-focused-learning-survey.pdf) | Supports evaluating forecast features by downstream decision value, not only forecast error. |
| ESS arbitrage DFL | [../thesis/sources/2305.00362v1-electricity-price-prediction-ess-arbitrage-dfl.pdf](../thesis/sources/2305.00362v1-electricity-price-prediction-ess-arbitrage-dfl.pdf) | Supports strict regret/net-value evaluation for storage arbitrage. |
| Multistage storage DFL | [../thesis/sources/2405.14719v2.pdf](../thesis/sources/2405.14719v2.pdf) | Reinforces that SOC path and intertemporal decisions matter. |
| Perturbed DFL storage | [../thesis/sources/2406.17085v2-perturbed-decision-focused-learning-energy-storage.pdf](../thesis/sources/2406.17085v2-perturbed-decision-focused-learning-energy-storage.pdf) | Supports future decision-aligned DFL after the feature route is governed. |
| PriceFM | [../thesis/sources/2508.04875-pricefm-electricity-price-forecasting.pdf](../thesis/sources/2508.04875-pricefm-electricity-price-forecasting.pdf) | Remains external-validation and market-coupling context only, not Ukrainian training input. |
| THieF | [../thesis/sources/2508.11372-thief-day-ahead-electricity-price-forecasting.pdf](../thesis/sources/2508.11372-thief-day-ahead-electricity-price-forecasting.pdf) | Remains research context for future temporal hierarchy features. |

## Claim Boundary

The current strongest evidence remains Ukrainian-only:

- OREE DAM prices;
- Open-Meteo/weather context;
- tenant load/configuration context;
- strict LP/oracle scoring;
- `strict_similar_day` fallback.

ENTSO-E Poland and other European sources are not training rows. In this slice
they may appear only as governed exogenous feature candidates. If publication
time, timezone/DST, prior-known EUR/UAH FX, licensing, market-rule mapping, or
domain-shift evidence is missing, the correct ablation result is
`blocked_by_governance`.
