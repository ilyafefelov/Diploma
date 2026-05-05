# Бібліографічна база — локальний архів PDF

Цей каталог містить первинні джерела, на які спирається дипломна робота.  
Нижче — систематизований індекс для наукового керівника.

---

## 📌 Ключові джерела (цитуються в поточній версії Розділу 2)

| # | Файл | Автори, рік | Тема | DOI / arXiv |
|---|------|-------------|------|-------------|
| 1 | `2505.01551v2=Decision-Focused Predict-then-Bid Framework for Strategic Energy Storage.pdf` | Yi et al., 2025 | **DFL predict-then-bid для BESS** — центральна цільова архітектура диплома | arXiv:2505.01551 |
| 2 | `1710.08005v5-Smart "Predict, then Optimize" Adam N. Elmachtoub.pdf` | Elmachtoub & Grigas, 2022 | **Smart PTO / SPO+** — методологічна база для regret та decision alignment | 10.1287/mnsc.2020.3922 |
| 3 | `1-s2.0-S0169207022000413-Neuralbasisexpansionanalysiswithexogenousvariables.pdf` | Olivares et al., 2023 | **NBEATSx** — декомпозиційна нейромережа для ціноутворення з екзогенними змінними | 10.1016/j.ijforecast.2022.03.001 |
| 4 | `1912.09363v3.pdf` | Lim et al., 2021 | **Temporal Fusion Transformer (TFT)** — цільовий forecast layer | arXiv:1912.09363 |
| 5 | `2106.01345v2.pdf` | Chen et al., 2021 | **Decision Transformer** — цільова strategy layer через sequence modeling | arXiv:2106.01345 |
| 6 | `1910.12430v1.pdf` | Agrawal et al., 2019 | **Differentiable Convex Optimization Layers (cvxpylayers)** — математична основа DFL-контуру | arXiv:1910.12430 |
| 7 | `1-s2.0-S2352152X24019662-Profitability of energy arbitrage net profit for grid-scale battery energy storage.pdf` | Grimaldi et al., 2024 | **Прибутковість BESS-арбітражу** з урахуванням деградації; джерело capex anchor $210/kWh | 10.1016/j.est.2024.112380 |
| 8 | `energies-12-00999-v2.pdf` | Hesse et al., 2019 | **Ageing-aware dispatch** через MILP для ринку арбітражу | 10.3390/en12060999 |
| 9 | `1-s2.0-S0306261919320471-main.pdf` | Maheshwari et al., 2020 | **Нелінійна деградація LFP** в операційній оптимізації | 10.1016/j.apenergy.2019.114360 |
| 10 | `batteries-11-00392-v2-Modelling of Battery Energy Storage Systems Under Real-World Applications and Conditions.pdf` | (MDPI Batteries 11, 392) | Моделювання BESS у реальних умовах | MDPI Batteries |
| 11 | `Journal of Forecasting - 2025 - Jin - Seasonal Decomposition‐Enhanced Deep Learning Architecture for Probabilistic.pdf` | Jin et al., 2025 | **Probabilistic forecasting** з сезонною декомпозицією | J. of Forecasting, 2025 |
| 12 | `Deep_Learning_for_Electricity_Price_Forecasting_A_.pdf` | Yu et al., 2026 | **Deep-learning EPF review** — day-ahead, intraday, and balancing electricity-market forecasting taxonomy | arXiv:2602.10071 |
| 13 | `nrel-2021-storage-futures-technology-modeling-input-data-report.pdf` | Augustine & Blair, 2021 | **NREL Storage Futures** — cost/performance assumptions for deployed storage technologies | NREL/TP-5700-78694 |
| 14 | `2403.10617v1-depreciation-cost-poor-proxy-grid-storage-optimization.pdf` | Kumtepeli et al., 2024 | **Degradation depreciation as proxy** in rolling-horizon grid-storage optimization | arXiv:2403.10617 / 10.48550/arXiv.2403.10617 |
| 15 | `nrel-atb-2023-pv-plus-battery.md` | NREL ATB, 2023 | **PV-plus-battery guide** — battery RTE, replacement, and cycle-degradation assumptions | NREL ATB web guide |
| 16 | `2402.19072v2-timexer-exogenous-transformer-forecasting.pdf` | Wang et al., 2024 | **TimeXer** — Transformer architecture for time-series forecasting with exogenous variables; future-work reference | arXiv:2402.19072 |
| 17 | `energies-10-00207-linear-formulation-short-term-ess-scheduling.pdf` | Park et al., 2017 | **LP formulation for ESS scheduling** — primary support for SOC, efficiency, power-limit, and energy-limit constraints in the baseline LP | 10.3390/en10020207 |
| 18 | `2307.13565v1-decision-focused-learning-survey.pdf` | Mandi et al., 2024 | **Decision-Focused Learning survey** — methodological framing for optimizing downstream decision quality, not forecast error alone | 10.1613/jair.1.15320 / arXiv:2307.13565 |
| 19 | `amos17a-optnet-differentiable-optimization-layer.pdf` | Amos & Kolter, 2017 | **Optimization layers in neural networks** — foundational reference for differentiating through constrained optimization layers | PMLR 70:136-145 |
| 20 | `2008.08004v2-electricity-price-forecasting-review-benchmark.pdf` | Lago et al., 2021 | **EPF benchmarking best practices** — support for validating weather-aware price forecasts against simple baselines before dispatch use | 10.1016/j.apenergy.2021.116983 / arXiv:2008.08004 |

---

## 🔎 Source Map: Baseline LP and Current Data Pipeline

These sources support [technical/BASELINE_LP_AND_DATA_PIPELINE.md](../../technical/BASELINE_LP_AND_DATA_PIPELINE.md). Each entry below records why the source is used in this project rather than only listing the citation.

| Source file | Summary / why used here |
|---|---|
| `energies-10-00207-linear-formulation-short-term-ess-scheduling.pdf` | Park et al. formulate short-term energy storage scheduling as linear programming while preserving SOC, charging/discharging efficiency, power range, and energy-limit constraints. This directly supports the current `HourlyDamBaselineSolver` formula and justifies using LP as the transparent Level 1 baseline. |
| `2403.10617v1-depreciation-cost-poor-proxy-grid-storage-optimization.pdf` | Kumtepeli et al. explain why degradation depreciation is often used in rolling-horizon storage optimization, while warning that it is only a proxy for future revenue lost to aging. This is why the project documents the EFC/throughput cost as an MVP economic proxy rather than a full battery digital twin. |
| `2008.08004v2-electricity-price-forecasting-review-benchmark.pdf` | Lago et al. review day-ahead electricity price forecasting and argue for rigorous open benchmarks against well-performing simpler models. This supports the project decision to keep the dashboard `weather_bias` as an explanatory sensitivity read model until a weather-aware forecast is validated by rolling-origin realized-value and oracle-regret tests. |
| `1-s2.0-S0169207022000413-Neuralbasisexpansionanalysiswithexogenousvariables.pdf` | Olivares et al. introduce NBEATSx for electricity price forecasting with exogenous variables and interpretable decomposition. This supports the project choice to keep NBEATSx as a Silver-layer research forecast candidate upstream of LP dispatch. |
| `1912.09363v3.pdf` | Lim et al. introduce Temporal Fusion Transformers for interpretable multi-horizon forecasting with feature-selection and attention-based explanations. This supports the TFT research lane and the dashboard/defense need to explain which signals influence price forecasts. |
| `2402.19072v2-timexer-exogenous-transformer-forecasting.pdf` | Wang et al. propose TimeXer for time-series forecasting with exogenous variables, using external signals to improve the target forecast. This supports the future architecture where weather feeds a forecast model upstream of the LP, rather than being injected directly into the LP objective. |
| `Deep_Learning_for_Electricity_Price_Forecasting_A_.pdf` | Yu et al. provide a recent EPF review across day-ahead, intraday, and balancing markets and organize deep-learning EPF by market-aware design choices. This supports the claim that weather-aware modeling belongs in a benchmarked forecasting layer before promotion to operational dispatch. |
| `1910.12430v1.pdf` | Agrawal et al. show how disciplined convex programs can be embedded as differentiable layers in PyTorch/TensorFlow through cvxpylayers. This supports the current relaxed-LP DFL pilot and the planned transition from strict LP evaluation to differentiable training experiments. |
| `amos17a-optnet-differentiable-optimization-layer.pdf` | Amos and Kolter provide the earlier neural-network framing for optimization layers and implicit differentiation through constrained problems. This is used as background for why optimization can become part of a trainable architecture instead of only a post-processing step. |
| `2307.13565v1-decision-focused-learning-survey.pdf` | Mandi et al. survey DFL methods that train models for downstream constrained-decision quality. This supports the project claim boundary: current ML forecasts are not yet full DFL, but the benchmark and relaxed LP prepare that path. |
| `2505.01551v2=Decision-Focused Predict-then-Bid Framework for Strategic Energy Storage.pdf` | Yi et al. apply decision-focused predict-then-bid logic to strategic energy storage by combining prediction, storage optimization, and market-clearing layers. This is the closest target-architecture reference for the thesis, while the current repo remains a strict LP baseline plus DFL pilot rather than a final predict-then-bid system. |

---

## 📂 Додаткові джерела та матеріали

| Файл | Опис | Статус |
|------|------|--------|
| `energies-19-02216.pdf` | Стаття журналу Energies (2026) — ідентифікація потребує перевірки | потребує верифікації |
| `Exploring Lithium-Ion Battery Degradation.pdf` | Огляд деградації літій-іонних батарей | потребує верифікації |
| `Analyzing_Uncertainty_Quantification_in_Statistica.pdf` | UQ у статистичних методах — релевантно для probabilistic forecast layer | потребує верифікації |
| `2106.08702v1.pdf` | arXiv 2106.08702 — ідентифікація потребує перевірки | потребує верифікації |
| `2112.09816v2.pdf` | arXiv 2112.09816 — ідентифікація потребує перевірки | потребує верифікації |
| `2402.19110v1.pdf` | arXiv 2402.19110 (лют. 2024) — ідентифікація потребує перевірки | потребує верифікації |
| `conv_final_thesis_am.pdf` | Дипломна / магістерська робота (A.M.) — можливе методологічне джерело | потребує верифікації |
| `10339524.pdf` | IEEE Xplore (ймовірно) — ідентифікація потребує перевірки | потребує верифікації |
| `782f235253679294adf311ea6a00c3c604b6.pdf` | Джерело з хеш-іменем — ідентифікація потребує перевірки | потребує верифікації |
| `613177195-Towards Model-based Synergistic Learning for Robust Next-Generation MIMO Systems.pdf` | Стаття про MIMO — ймовірно зайвий файл або методологічний reference | потребує верифікації |

---

## 🗂️ Тематичний розподіл

### Decision-Focused Learning та Оптимізація
- Yi et al. 2025 · Elmachtoub & Grigas 2022 · Agrawal et al. 2019 · Amos & Kolter 2017 · Mandi et al. 2024

### Прогнозування цін на електроенергію
- Lago et al. 2021 (EPF benchmark) · Olivares et al. 2023 (NBEATSx) · Lim et al. 2021 (TFT) · Wang et al. 2024 (TimeXer) · Yu et al. 2026 (deep-learning EPF review) · Jin et al. 2025

### Стратегія батарейного накопичувача / Арбітраж
- Park et al. 2017 · Grimaldi et al. 2024 · Hesse et al. 2019 · Chen et al. 2021 (Decision Transformer)

### Фізичне моделювання та деградація батарей
- Maheshwari et al. 2020 · Hesse et al. 2019 · Kumtepeli et al. 2024 · NREL Storage Futures · NREL ATB · `batteries-11-00392-v2` · `Exploring Lithium-Ion Battery Degradation.pdf`

---

> **Примітка для керівника:** Файли з позначкою «потребує верифікації» зібрані в процесі дослідження й очікують остаточної ідентифікації та включення до бібліографії. Ключові джерела таблиці вище вже інтегровані або mapped до [Розділу 2 (Огляд літератури)](../chapters/02-literature-review.md).
