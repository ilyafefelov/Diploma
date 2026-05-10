# Core Paper Context

This folder stores the core research references currently used to ground the Level 1 baseline and the later DFL target strategy.

## Downloaded PDFs

- `2505.01551-predict-then-bid.pdf` — A Decision-Focused Predict-then-Bid Framework for Energy Storage Arbitrage
- `2104.05522-nbeatsx.pdf` — Neural basis expansion analysis with exogenous variables: Forecasting electricity prices with NBEATSx

The broader thesis source archive is maintained in
[../../thesis/sources/README.md](../../thesis/sources/README.md). The DFL/DT
refresh added these local artifacts there:

- `2307.13565v4-decision-focused-learning-survey.pdf` — DFL survey and benchmark context for training models on downstream decision quality.
- `2305.00362v1-electricity-price-prediction-ess-arbitrage-dfl.pdf` — ESS arbitrage DFL source connecting forecast errors to regret and storage profit.
- `2405.14719v2.pdf` — multistage DFL source for SOC-path and intertemporal storage effects.
- `2406.17085v2-perturbed-decision-focused-learning-energy-storage.pdf` — perturbed/hybrid DFL source for differentiable storage objectives.
- `1710.08005v5-Smart “Predict, then Optimize” Adam N. Elmachtoub.pdf` — SPO/SPO+ foundation for predict-then-optimize regret.
- `2106.01345v2.pdf` — original Decision Transformer paper for offline sequence-modeling strategy.
- `2305.14550v1-sequence-modeling-offline-rl.pdf` — offline RL comparison source for DT data and robustness caveats.
- `huggingface-decision-transformer-docs.md` — official Hugging Face implementation reference for future offline DT experiments.

## Paywalled References

- Profitability of energy arbitrage net profit for grid-scale battery energy storage considering dynamic efficiency and degradation
  - DOI: `10.1016/j.est.2024.112380`
  - Canonical URL: `https://doi.org/10.1016/j.est.2024.112380`
  - Status: not stored locally because the article is paywalled

- Probabilistic electricity price forecasting based on penalized temporal fusion transformer
  - DOI: `10.1002/for.3084`
  - Canonical URL: `https://doi.org/10.1002/for.3084`
  - Status: not stored locally because the article is paywalled

## Context Split

- Level 1 baseline: hourly DAM-only LP baseline, strict similar-day naive forecast, rolling-horizon execution
- Level 2 target: predict-then-bid DFL architecture with differentiable clearing, venue-aware bidding, and learned strategy layers

## Deep-Research Update

The current literature narrative should be ordered as:

1. Storage-arbitrage economics and price-taking benchmarks.
2. Ageing-aware dispatch and the limitation of linear degradation proxies.
3. Electricity price forecasting with exogenous variables, especially NBEATSx and TFT.
4. Smart Predict-then-Optimize and storage-specific predict-then-bid / DFL.
5. Real-data rolling-origin value/regret evaluation for Ukraine DAM.

The practical thesis implication is that NBEATSx/TFT are forecast candidates, not the contribution by themselves. Their value must be measured through LP decision value, degradation-adjusted net value, and oracle regret.

Recommended additional bibliography entries to keep in the thesis source map:

- Sioshansi et al. (2009), "Estimating the value of electricity storage in PJM: Arbitrage and some welfare effects", DOI `10.1016/j.eneco.2008.10.005`.
- Hesse et al. (2019), "Ageing and Efficiency Aware Battery Dispatch for Arbitrage Markets Using Mixed Integer Linear Programming", DOI `10.3390/en12060999`.
- Maheshwari et al. (2020), "Optimizing the operation of energy storage using a non-linear lithium-ion battery degradation model", DOI `10.1016/j.apenergy.2019.114360`.
- Elmachtoub and Grigas (2022), "Smart Predict, then Optimize", DOI `10.1287/mnsc.2020.3922`.
- Sang et al. (2023), "Electricity Price Prediction for Energy Storage System Arbitrage: A Decision-Focused Approach", arXiv `2305.00362`.
- Persak and Anjos (2024), "Decision-Focused Forecasting: Decision Losses for Multistage Optimisation", arXiv `2405.14719`.
- Yi, Alghumayjan, and Xu (2024), "Perturbed Decision-Focused Learning for Modeling Strategic Energy Storage", arXiv `2406.17085`.
- Olivares et al. (2023), "Neural basis expansion analysis with exogenous variables: Forecasting electricity prices with NBEATSx", DOI `10.1016/j.ijforecast.2022.03.001`.
- Lim et al. (2021), "Temporal Fusion Transformers for Interpretable Multi-horizon Time Series Forecasting", DOI `10.1016/j.ijforecast.2021.03.012`.
- Yi et al. (2025), "A Decision-Focused Predict-then-Bid Framework for Strategic Energy Storage", DOI `10.48550/arXiv.2505.01551`, open preprint and not yet peer-reviewed.
- Chen et al. (2021), "Decision Transformer: Reinforcement Learning via Sequence Modeling", arXiv `2106.01345`.
- Bhargava et al. (2023), "When should we prefer Decision Transformers for Offline Reinforcement Learning?", arXiv `2305.14550`.

## Week 4 Source Refresh

Week 4 refreshes the source map through Hugging Face paper metadata without adding a crawler or new model dependency. The detailed intake lives in [../../thesis/sources/week4-research-ingestion.md](../../thesis/sources/week4-research-ingestion.md).

Add these sources to the thesis narrative as literature context, not implemented features:

- PriceFM, arXiv `2508.04875`: electricity-price foundation model for European probabilistic forecasting with cross-region context.
- THieF, arXiv `2508.11372`: temporal hierarchy forecasting for day-ahead prices across hourly/block/baseload levels.
- TSFM leakage evaluation, arXiv `2510.13654`: guardrail for no-leakage rolling-origin evaluation.
- TFMAdapter, arXiv `2509.13906`: watch-list source for future covariate adaptation.
- Reverso, arXiv `2602.17634`: watch-list source for efficient zero-shot time-series foundation models.
- Distributional RL energy arbitrage, arXiv `2401.00015`: watch-list source for later risk-sensitive and multi-venue strategy work.

The Week 4 modeling follow-up ran existing regret-calibration assets using [../../../configs/real_data_calibration_week4.yaml](../../../configs/real_data_calibration_week4.yaml). Outputs are calibration/selector evidence only, not full DFL or market execution. The concise report/demo package is tracked in [../../thesis/weekly-reports/week4/report.md](../../thesis/weekly-reports/week4/report.md).

## European Dataset Bridge

The Week 3 all-tenant offline DFL panel keeps implemented evidence on Ukrainian
OREE DAM plus Open-Meteo. European data sources are now cited only as future
external-validation and market-coupling context:

- [ENTSO-E Transparency Platform](https://www.entsoe.eu/data/transparency-platform/) — future European generation, load, transmission, and balancing-data context.
- [Open Power System Data](https://open-power-system-data.org/) — future open-data reproducibility source for European power-system packages.
- [OPSD time series](https://data.open-power-system-data.org/time_series/) — future spot-price, load, wind, and solar time-series comparison.
- [Nord Pool Data Portal](https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/) — future Nordic market-data reference if access is approved.
- [Ember API](https://ember-energy.org/data/api) — future high-level electricity demand, generation, emissions, and carbon-intensity context.

No European dataset is ingested in the current codebase, and none of these
sources supports a current market-coupling or external generalization claim.
