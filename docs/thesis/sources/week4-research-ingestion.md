# Week 4 Research Ingestion

> Supervisor-facing intake note for the Week 4 literature/source refresh. Metadata was checked through Hugging Face paper pages and linked arXiv pages on 2026-05-06. This artifact is a source-selection map, not an implementation claim.

## Purpose

Week 3 produced the first thesis-grade real-data rolling-origin benchmark for `client_003_dnipro_factory`. Week 4 should connect that evidence to the literature: forecasting models are useful only when evaluated without temporal leakage and through downstream decision value. The immediate research question is therefore not "which model is newest?", but "which sources justify the next calibration and robustness step before a full DFL claim?".

## Intake Table

| Status | Source | Why it matters for this thesis | Current action |
|---|---|---|---|
| include | [PriceFM: Foundation Model for Probabilistic Electricity Price Forecasting](https://huggingface.co/papers/2508.04875), arXiv `2508.04875` | Electricity-price-specific foundation model with European cross-region context, exogenous variables, probabilistic forecasting, and graph/topology motivation. Useful for explaining why Ukraine DAM forecasting may eventually need market-coupling and cross-border signals. | Cite as future forecast-layer direction; do not claim implemented. |
| include/watch | [Stealing Accuracy: Predicting Day-ahead Electricity Prices with Temporal Hierarchy Forecasting (THieF)](https://huggingface.co/papers/2508.11372), arXiv `2508.11372` | Shows that hourly DAM forecasts can benefit from reconciliation across hourly, block, and baseload products. This supports the project's later move from single-hour forecasts to structured market products. | Add to source map; treat as calibration/forecasting future work. |
| include | [Rethinking Evaluation in the Era of Time Series Foundation Models: (Un)known Information Leakage Challenges](https://huggingface.co/papers/2510.13654), arXiv `2510.13654` | Directly supports the Week 3/Week 4 rolling-origin design: time-series foundation-model benchmarks can overstate performance when train/test overlap or temporally correlated leakage is not controlled. | Cite as a methodological guardrail for no-leakage rolling-origin evaluation. |
| watch | [TFMAdapter: Lightweight Instance-Level Adaptation of Foundation Models for Forecasting with Covariates](https://huggingface.co/papers/2509.13906), arXiv `2509.13906` | Relevant to exogenous/covariate forecasting because Open-Meteo weather and future market-coupling signals are domain-specific covariates. It is not needed for the current MVP. | Keep as future-work source for covariate adaptation; no implementation now. |
| watch | [Reverso: Efficient Time Series Foundation Models for Zero-shot Forecasting](https://huggingface.co/papers/2602.17634), arXiv `2602.17634` | Relevant to lightweight zero-shot time-series forecasting and cost-aware experimentation. It does not yet beat the need for thesis-specific rolling-origin validation. | Keep as future-work source; no model dependency now. |
| watch | [Distributional Reinforcement Learning-based Energy Arbitrage Strategies in Imbalance Settlement Mechanism](https://huggingface.co/papers/2401.00015), arXiv `2401.00015` | Supports later risk-sensitive and multi-venue strategy discussion. It is imbalance-market/RL work, not a direct DAM-only Week 4 implementation target. | Keep for later risk/multi-venue section; do not use as current DFL evidence. |
| exclude | Generic high-frequency trading RL and unrelated energy-materials search hits | These hits mention arbitrage, RL, or energy but do not address BESS DAM/IDM scheduling, electricity price forecasting, or thesis-grade rolling-origin evaluation. | Exclude from Chapter 2 to keep the literature review focused. |

## Week 3 Evidence Bridge

The new sources reinforce the Week 3 empirical result: `strict_similar_day` remained the strongest control candidate on the Dnipro 30-anchor slice, while `nbeatsx_silver_v0` and `tft_silver_v0` should be treated as forecast candidates requiring calibration. This is consistent with electricity-price forecasting best practice: stronger models need value/regret validation, not only MAE/RMSE claims.

## Week 4 Calibration Implication

The next modeling step should use the existing regret-weighted and horizon-regret-weighted calibration assets. Outputs must be labeled as calibration or selector evidence. They are not full DFL, not market execution, and not a replacement for the real-data benchmark.

Tracked config for this follow-up: [configs/real_data_calibration_week4.yaml](../../../configs/real_data_calibration_week4.yaml).

Latest calibration result: the Dnipro 90-anchor run completed on 2026-05-07 local time. Horizon-aware regret calibration improved the neural candidates versus their raw forms, but `strict_similar_day` remained the strongest individual control. The Week 4 report therefore frames the result as calibration/selector evidence and not as a full DFL claim.

## Firecrawl Decision

Firecrawl is deferred. No local command, dependency, or callable tool was found in the repo/session, and adding it would be a new dependency. For Week 4, Hugging Face paper pages plus direct arXiv/HF links are sufficient.
