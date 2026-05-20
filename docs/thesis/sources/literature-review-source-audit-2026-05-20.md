# Literature Review Source Audit - 2026-05-20

This audit checks the sources listed in `docs/thesis/chapters/02-literature-review.md`
section 2.13 and the matching Google Doc tab `t.wox4yywng724`.
The first pass verified the original 50 references; the citation-completeness pass
added nine missing support sources, and the URL/DOI enrichment pass added the
Jin and Blanco-Encomienda probabilistic-forecasting source used by Methodology.
The current Chapter 2 bibliography has 60 entries.

Verification methods:

- DOI metadata was checked through Crossref or DOI resolution.
- arXiv metadata was checked through the arXiv API.
- Official data, policy, and documentation URLs were checked by HTTP status and page title.
- URL terminal punctuation was normalized in the bibliography so copied links do not include a trailing full stop.

## Corrections Applied

| # | Correction |
|---|------------|
| 1 | Title corrected from `...Energy Storage Arbitrage` to `A Decision-Focused Predict-then-Bid Framework for Strategic Energy Storage`. |
| 2 | NBEATSx arXiv URL corrected from `https://arxiv.org/abs/2201.12886` to `https://arxiv.org/abs/2104.05522`; `2201.12886` is N-HiTS, not NBEATSx. |
| 30 | Title corrected from `Decision Losses for Multistage Optimisation` to `Decision-Focused Forecasting: A Differentiable Multistage Optimisation Architecture`. |
| URL formatting | Terminal punctuation after copied URLs was removed in the literature-review bibliography and Google Doc tab to avoid false 404s from tools that include the period in the URL. |
| 51-59 | Missing inline-support sources were added for LP ESS scheduling, NREL storage cost/performance assumptions, OptNet differentiable optimization layers, MLOps tooling, Pydantic validation, and Medallion architecture. |
| DOI/arXiv URL enrichment | Canonical DOI or arXiv URLs were added to bibliography entries that previously listed only a DOI/arXiv identifier without a clickable URL. |
| 60 | Jin and Blanco-Encomienda was added with verified DOI `10.1002/for.70065`; the working note had treated it as a 2025 item, but Crossref verifies the journal citation as 2026. |

## Audit Table

| # | Source target checked | Method | Result | Notes |
|---|-----------------------|--------|--------|-------|
| 1 | Yi et al., `A Decision-Focused Predict-then-Bid Framework for Strategic Energy Storage`, arXiv `2505.01551` | arXiv API | Pass | Correct target for predict-then-bid strategic storage; preprint, not peer-reviewed journal evidence. |
| 2 | Olivares et al., `Neural basis expansion analysis with exogenous variables: Forecasting electricity prices with NBEATSx`, DOI `10.1016/j.ijforecast.2022.03.001`, arXiv `2104.05522` | Crossref + arXiv API | Pass after correction | This is the correct NBEATSx paper. |
| 3 | Jiang et al., `Probabilistic electricity price forecasting based on penalized temporal fusion transformer`, DOI `10.1002/for.3084` | Crossref | Pass | Correct TFT/probabilistic EPF target. |
| 4 | Elmachtoub and Grigas, `Smart "Predict, then Optimize"`, DOI `10.1287/mnsc.2020.3922` | Crossref | Pass | Correct SPO/SPO+ foundation. |
| 5 | Grimaldi et al., BESS arbitrage profitability with dynamic efficiency/degradation, DOI `10.1016/j.est.2024.112380` | Crossref | Pass | Correct storage-arbitrage/degradation optimization source. |
| 6 | Lim et al., `Temporal Fusion Transformers for Interpretable Multi-horizon Time Series Forecasting`, arXiv `1912.09363` | arXiv API | Pass | Correct original TFT paper. |
| 7 | Chen et al., `Decision Transformer: Reinforcement Learning via Sequence Modeling`, arXiv `2106.01345` | arXiv API | Pass | Correct Decision Transformer paper. |
| 8 | Agrawal et al., `Differentiable Convex Optimization Layers`, arXiv `1910.12430` | arXiv API | Pass | Correct cvxpylayers paper. |
| 9 | Vykhodtsev et al., lithium-ion BESS modelling review, DOI `10.1016/j.rser.2022.112584` | Crossref | Pass | Correct battery modelling review. |
| 10 | Hesse et al., ageing/efficiency-aware battery dispatch MILP, DOI `10.3390/en12060999` | Crossref | Pass | Correct degradation-aware dispatch source. |
| 11 | Maheshwari et al., nonlinear lithium-ion degradation model for storage operation, DOI `10.1016/j.apenergy.2019.114360` | Crossref | Pass | Correct nonlinear degradation source. |
| 12 | Hu et al., BESS utilization in major European electricity markets, DOI `10.1016/j.apenergy.2022.119512` | Crossref | Pass | Correct European BESS market context. |
| 13 | Li et al., temporal-aware DRL for energy storage bidding, DOI `10.1109/TEMPR.2024.3372656` | Crossref | Pass | Correct energy/reserve bidding DRL source. |
| 14 | Lago et al., day-ahead EPF review and benchmark, DOI `10.1016/j.apenergy.2021.116983` | Crossref | Pass | Correct EPF benchmark discipline source. |
| 15 | Wang et al., `TimeXer`, arXiv `2402.19072` | arXiv API | Pass | Correct exogenous-variable Transformer forecasting source. |
| 16 | Yu et al., deep-learning EPF review, arXiv `2602.10071` | arXiv API | Pass | Correct 2026 EPF review preprint. |
| 17 | Yu et al., `PriceFM`, arXiv `2508.04875` | arXiv API | Pass | Correct electricity-price foundation-model source. |
| 18 | Lipiecki et al., THieF, arXiv `2508.11372` | arXiv API | Pass | Correct temporal hierarchy day-ahead price forecasting source. |
| 19 | Meyer et al., TSFM leakage challenges, arXiv `2510.13654` | arXiv API | Pass | Correct no-leakage evaluation guardrail source. |
| 20 | Dange and Sarawagi, `TFMAdapter`, arXiv `2509.13906` | arXiv API | Pass | Correct covariate-adaptation source. |
| 21 | Fu et al., `Reverso`, arXiv `2602.17634` | arXiv API | Pass | Correct zero-shot time-series foundation-model source. |
| 22 | Madahi et al., distributional RL energy arbitrage, arXiv `2401.00015` | arXiv API | Pass | Correct risk-sensitive arbitrage source. |
| 23 | ENTSO-E Transparency Platform | HTTP status + page title | Pass | Official transparency/data source. |
| 24 | Open Power System Data platform | HTTP status + page title | Pass | Official OPSD platform page. |
| 25 | OPSD time-series data package | HTTP status + page title | Pass | Correct OPSD data-package page. |
| 26 | Nord Pool Data Portal | HTTP status + page title | Pass | Correct Nord Pool market-data service page. |
| 27 | Ember data API | HTTP status + page title | Pass | Correct Ember API documentation page. |
| 28 | Mandi et al., DFL survey, DOI `10.1613/jair.1.15320`, arXiv `2307.13565` | Crossref + arXiv API | Pass | Correct DFL foundations/survey source. |
| 29 | Sang et al., ESS arbitrage DFL, DOI `10.1109/TSG.2022.3166791`, arXiv `2305.00362` | Crossref + arXiv API | Pass | Correct storage-specific DFL source. |
| 30 | Persak and Anjos, `Decision-Focused Forecasting: A Differentiable Multistage Optimisation Architecture`, arXiv `2405.14719` | arXiv API | Pass after correction | Correct multistage DFL source; title was corrected. |
| 31 | Yi, Alghumayjan, and Xu, perturbed DFL for strategic energy storage, arXiv `2406.17085` | arXiv API | Pass | Correct perturbed storage DFL source. |
| 32 | Bhargava et al., Decision Transformers for offline RL, arXiv `2305.14550` | arXiv API | Pass | Correct DT/offline RL comparison source. |
| 33 | Hugging Face Decision Transformer docs | HTTP status | Pass | Correct official implementation documentation. |
| 34 | European Commission electricity market design | HTTP status + page title | Pass | Correct official EU market-design policy page. |
| 35 | European Commission day-ahead market dynamic trading news | HTTP status + page title | Pass | Correct official EC news source; date-specific policy/news context. |
| 36 | NEURC Resolution No. 621 of 23 April 2026 | HTTP status + page title | Pass | Correct official Ukrainian price-cap resolution page as of audit date. |
| 37 | JSC Market Operator tariff notice for 2026 | HTTP status | Pass | Correct OREE/Market Operator source; page title encoding is not UTF-8 clean but URL resolves. |
| 38 | Open-Meteo Forecast API docs | HTTP status + page title | Pass | Correct official API documentation. |
| 39 | Open-Meteo Historical Weather API docs | HTTP status + page title | Pass | Correct official historical weather documentation. |
| 40 | Ministry of Economy NECP approval news | HTTP status + page title | Pass | Correct official Ukrainian government news page. |
| 41 | Ukraine National Energy and Climate Plan to 2030 PDF | HTTP status + content type | Pass | Correct official PDF URL as of audit date. |
| 42 | ENTSO-E Ukraine/Moldova Continental Europe synchronisation news | HTTP status + page title | Pass | Correct synchronization source; not proof of full DAM market coupling. |
| 43 | ACER Energy Community market-coupling integration plan news | HTTP status + page title | Pass | Correct ACER page after removing terminal URL punctuation. |
| 44 | European Commission AI Act regulatory framework | HTTP status + page title | Pass | Correct official AI Act page after removing terminal URL punctuation. |
| 45 | Kumtepeli et al., 3D-MILP battery arbitrage, DOI `10.1109/ACCESS.2020.3035504` | Crossref | Pass | Correct electro-thermal/degradation arbitrage source. |
| 46 | Cao et al., DRL storage arbitrage with accurate degradation model, DOI `10.1109/TSG.2020.2986333` | Crossref | Pass | Correct DRL/degradation source. |
| 47 | Li and Becker, market-coupling EPF with LSTM/feature selection, arXiv `2101.05249` | arXiv API | Pass | Correct market-coupling EPF source. |
| 48 | Redhu and Bremdal, neighboring-zones 1D-LSTM EPF, DOI `10.1109/PowerTech55446.2023.10202771` | Crossref | Pass | Correct neighboring-zones EPF source. |
| 49 | Mascarenhas et al., asynchronous cross-border market data for EPF, arXiv `2507.13250` | arXiv API | Pass | Correct European cross-border EPF source. |
| 50 | Nixtla NeuralForecast exogenous-variable docs and NBEATSx docs | HTTP status + page title | Pass | Correct official implementation documentation pages. |
| 51 | Park et al., `Linear Formulation for Short-Term Operational Scheduling of Energy Storage Systems in Power Grids`, DOI `10.3390/en10020207` | Crossref | Pass | Added as explicit support for LP ESS scheduling, SOC, efficiency, power-limit, and energy-limit constraints. |
| 52 | Augustine and Blair, `Storage Futures Study: Storage Technology Modeling Input Data Report`, NREL/TP-5700-78694 | HTTP status + content type | Pass | Added as storage cost/performance support for battery assumptions and degradation/economic context. |
| 53 | NREL, `2023 Annual Technology Baseline: Utility-Scale PV-Plus-Battery` | HTTP status + page title | Pass | Added as official NREL ATB support for PV-plus-battery cost/performance and operating assumptions. |
| 54 | Amos and Kolter, `OptNet: Differentiable Optimization as a Layer in Neural Networks`, PMLR 70:136-145 | HTTP status + page metadata | Pass | Added as foundational optimization-layer support alongside cvxpylayers. |
| 55 | Dagster software-defined assets documentation | HTTP status + page title | Pass | Added as official documentation support for assets, lineage, and materialization framing. |
| 56 | MLflow tracking documentation | HTTP status + page title | Pass | Added as official documentation support for run-level experiment tracking and metrics. |
| 57 | FastAPI documentation | HTTP status + page title | Pass | Added as official documentation support for API/read-model serving claims. |
| 58 | Pydantic strict mode documentation | HTTP status + page title | Pass | Added as official documentation support for strict validation and deterministic gatekeeper semantics. |
| 59 | Databricks Medallion Architecture documentation | HTTP status + page title | Pass | Added as official/reference documentation support for Bronze/Silver/Gold data-layer terminology. |
| 60 | Jin and Blanco-Encomienda, `Seasonal Decomposition-Enhanced Deep Learning Architecture for Probabilistic Forecasting`, DOI `10.1002/for.70065` | Crossref + DOI metadata | Pass | Added as probabilistic/quantile forecasting context; verified journal citation is 2026. |

## Remaining Use Boundaries

- arXiv-only sources should be cited as preprints unless a peer-reviewed DOI is added later.
- Policy and regulatory pages are time-sensitive; rerun the URL check before final thesis submission.
- This audit verifies link-target and metadata correctness. It is not a plagiarism/similarity report.
