# Source Matrix

This matrix maps literature, regulation, and project evidence to the architecture claims used in the 2026-05-11 review.

## Regulatory and Data Sources

| Source | What it says | Project implication |
|---|---|---|
| European Commission, Electricity market design | EU market design is integrated, renewables/flexibility oriented, and market-coupling based. The 2024 reform entered into force on 2024-07-16. | Market-coupling roadmap is valid, but cross-border features need licensing, publication-time, currency, timezone, and market-rule gates. |
| European Commission, day-ahead and intraday market coupling | Coupling connects power exchanges and TSOs to create a more liquid integrated market; EU day-ahead moved to 15-minute intervals on 2025-09-30. | Ukrainian hourly DAM evidence must not be merged naively with EU 15-minute products. |
| NEURC Resolution No. 621, 2026-04-23 | DAM/IDM caps: 10 to 15,000 UAH/MWh; balancing: 0.01 to 17,000 UAH/MWh; effective 2026-04-30. | Forecast sanity gates need venue-specific cap checks. |
| OREE 2026 tariff notice | DAM/IDM transaction tariff is 6.88 UAH/MWh excluding VAT; fixed software fee is 3,837.84 UAH excluding VAT. | Gross LP schedule value is fine for regret studies; net market profit claims need fee modeling. |
| Open-Meteo forecast/historical docs | Provides hourly weather variables, solar radiation, wind, humidity, precipitation, and historical endpoints. | Weather source is acceptable for MVP, but per-tenant coverage must be manifested. |
| Ukraine NECP to 2030 | Ukraine approved NECP on 2024-06-25 and frames policy around EU integration, energy/climate alignment, and reconstruction. | BESS flexibility and decision support align with national direction. |
| Ukraine Energy Strategy to 2050, referenced by NECP | Energy Strategy through 2050 was approved by Cabinet order No. 373-r of 2023-04-21. | Long-term narrative should be flexibility, resilience, and decarbonization support. |
| ENTSO-E synchronization note | Ukraine and Moldova synchronized with Continental Europe on 2022-03-16. | EU-grid integration context is real, but synchronization is not the same as market coupling. |
| ACER Energy Community MCO plan | Market Coupling Operation integration plan targets Energy Community NEMO integration into EU day-ahead and intraday coupling. | Market-coupling source governance should remain explicit and blocked until requirements are met. |
| EU AI Act overview | Risk-based AI framework; high-risk systems require risk mitigation, data quality, logging, documentation, human oversight, robustness, cybersecurity, and accuracy. | Pydantic Gatekeeper, logging, and market-execution-disabled flags are central governance controls. |

## Scientific Sources

| Source | What it supports | Project mapping |
|---|---|---|
| Yi et al. 2025, Decision-Focused Predict-then-Bid for Strategic Energy Storage | Tri-layer price prediction, storage optimization, and market clearing; implicit differentiation and perturbation for DFL bidding. | Target architecture for full DFL/predict-then-bid, not current deployment state. |
| Sang et al. 2022, Decision-Focused ESS Arbitrage | Optimizing forecasts for downstream arbitrage regret rather than only prediction error. | Supports regret-aware evaluation and calibration. |
| Yi et al. 2024, Perturbed DFL for Strategic Energy Storage | Perturbation-based differentiable loss for storage decisions. | Supports DFL research lane and explains why relaxed pilots exist. |
| Mandi et al. DFL survey | Decision-focused learning integrates prediction with optimization layers. | Supports architecture language and promotion gates. |
| Elmachtoub and Grigas, SPO+ | Smart predict-then-optimize loss aligns prediction with downstream decisions. | Supports schedule/value learner and regret-weighted calibration direction. |
| Olivares et al., NBEATSx | Exogenous interpretable neural forecasting. | Supports NBEATSx forecast candidate lane. Current official NBEATSx output still fails sanity/value gates. |
| Lim et al., TFT | Multi-horizon interpretable forecasting with variable selection and attention. | Supports TFT candidate and defense explainability narrative. |
| Agrawal et al., cvxpylayers | Differentiable convex optimization layers. | Supports relaxed LP DFL primitive. |
| Amos and Kolter, OptNet | Optimization layers inside neural networks. | Supports differentiable optimization concept. |
| Kumtepeli et al., degradation-aware storage arbitrage | SOC/temperature/degradation modeling can materially change arbitrage economics and lifetime. | Supports degradation penalty roadmap, but current system is not full electrochemical digital twin. |
| Cao et al., DRL arbitrage with degradation | Degradation-aware control can be modeled with learning methods and accurate battery aging. | Supports future comparison to DRL/MARL, but current project wisely keeps LP as baseline. |

## Repo Evidence Sources

| Repo artifact | What it establishes |
|---|---|
| `docs/technical/ARCHITECTURE_AND_DATA_FLOW.md` | Durable architecture visual and claim boundaries. |
| `docs/technical/DATA_INGESTION_SOURCES.md` | Source contracts for OREE, Open-Meteo, NEURC caps, OREE fees, synthetic fallback labeling. |
| `docs/technical/DFL_READINESS_GATE.md` | DFL readiness status: thesis evidence accepted, full DFL and market execution blocked. |
| `docs/technical/DFL_SCHEDULE_VALUE_PRODUCTION_GATE.md` | Offline/read-model production gate passes for source-specific schedule/value rows while market execution remains false. |
| `docs/technical/OFFICIAL_FORECAST_ROLLING_ORIGIN_BENCHMARK.md` | Official NBEATSx/TFT adapters execute but have not beaten strict baseline in rolling value tests. |
| `docs/technical/DFL_FORECAST_PIPELINE_TRUTH_AUDIT.md` | Five-tenant 104-anchor compact forecast audit has zero blocking failures and documented shift warnings. |
| `src/smart_arbitrage/defs/__init__.py` | Canonical Dagster definitions registration. |
| `docker-compose.yaml` | Defines Postgres, MQTT, MLflow, FastAPI, Dagster webserver/daemon, telemetry services. |
| Live Postgres data summaries | Market prices are cap-clean; weather has uneven tenant coverage; telemetry has future synthetic rows. |
| Live API smoke checks | Read models are healthy, but battery-state exposes future raw synthetic telemetry and official NBEATSx remains disabled. |

## Index Reconciliation

Fixed on 2026-05-11 after comparing this matrix against `docs/sources/README.md`,
`docs/thesis/sources/README.md`, and
`docs/thesis/chapters/02-literature-review.md`.

| Source group | Previous status | Fix |
|---|---|---|
| European Commission electricity-market design and day-ahead/intraday market coupling | Mentioned only in the architecture review matrix or older EU-context notes, not indexed in `docs/sources`. | Added `docs/sources/architecture-review-source-capture-2026-05-11.md`, indexed it, and mentioned it in Chapter 2. |
| NEURC Resolution No. 621 and OREE 2026 tariff | Present in technical planning docs, but not in the source-capture index or Chapter 2 source list. | Added to the architecture-review source capture, thesis source index, and Chapter 2 references. |
| Open-Meteo forecast/historical docs | Historical API was indexed; forecast API and source-contract role were incomplete in the architecture-review context. | Added both forecast and historical API docs to the new source capture and Chapter 2 source list. |
| Ukraine NECP, Energy Strategy 2050, ENTSO-E synchronization, ACER MCO plan, EU AI Act | Not represented in the thesis source index or Chapter 2 source list. | Added to the new source capture, thesis source index, and Chapter 2 policy/governance discussion. |
| Kumtepeli et al. 2020 and Cao et al. 2020 degradation-aware control sources | The matrix cited them, but the thesis source index/literature review only covered adjacent degradation sources. | Added source-capture entries and Chapter 2 mentions/references. |
| Core DFL/forecasting sources: Yi 2025, Sang, Yi 2024, Mandi, Elmachtoub/Grigas, Olivares, Lim, Agrawal, Amos/Kolter | Already indexed and mentioned. | No duplicate source capture needed; retained cross-reference in the new capture. |

## Claim Mapping

| Claim | Supported? | Evidence |
|---|---|---|
| Reproducible Dagster medallion architecture | Yes | `dg list defs`, docs, defs module. |
| Observed OREE DAM ingestion is usable for thesis baseline | Yes | 3,061 observed rows, no cap violations. |
| Tenant weather ingestion is usable | Partially | Observed rows exist, but coverage differs by tenant. |
| Battery telemetry is live real telemetry | No | Current store is synthetic and has future rows. |
| Strict LP baseline is the current control | Yes | Docs, API selected strategy, benchmark rows. |
| NBEATSx/TFT are production-ready forecasts | No | Official NBEATSx invalid; rolling-origin official forecasts do not beat strict. |
| Schedule/Value Learner V2 is ready for market execution | No | Offline/read-model gate only; `market_execution_enabled=false`. |
| Decision Transformer is deployed | No | Preview-only rows; not live control. |
| Full DFL is implemented | No | Primitive and research lanes exist; full differentiable DFL remains blocked. |
| Pydantic Gatekeeper is the safety boundary | Yes as architecture | Schemas/checks/read models support the boundary; live hardware execution is not enabled. |
