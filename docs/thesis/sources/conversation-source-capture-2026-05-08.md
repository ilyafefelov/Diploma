# Conversation Source Capture - 2026-05-07 to 2026-05-08

Purpose: capture academic, dataset, and policy sources raised in the May 7-8
DFL/AFE/AFL planning conversations that were not already indexed in the thesis
source docs.

Scope rule: sources already present in `docs/thesis/sources/README.md`,
`docs/thesis/sources/week4-research-ingestion.md`,
`docs/technical/papers/README.md`, or the focused DFL technical notes are not
duplicated here except as cross-reference groups.

Claim boundary: these sources support research planning, external validation,
and future feature design. They do not change the current evidence claim:
Ukrainian OREE DAM plus Open-Meteo, strict rolling-origin evaluation, frozen
`strict_similar_day` control, no market execution.

## Already Indexed Elsewhere

| Source group | Existing doc location | Notes |
|---|---|---|
| Core DFL and optimization-layer papers: Yi et al. predict-then-bid, SPO+, cvxpylayers, OptNet, DFL survey | `docs/thesis/sources/README.md` | Primary methodological foundation for DFL and differentiable optimization. |
| NBEATSx, TFT, TimeXer, EPF benchmark/review sources | `docs/thesis/sources/README.md` | Forecast-layer foundations and exogenous-feature context. |
| Week 4 HF paper refresh: PriceFM, THieF, TSFM leakage evaluation, TFMAdapter, Reverso, distributional RL energy arbitrage | `docs/thesis/sources/week4-research-ingestion.md`; `docs/technical/papers/README.md` | Kept as include/watch metadata, not current implementation claims. |
| Storage-specific DFL and action-classifier failure sources: ESS DFL arbitrage, Smart Predict-and-Optimize, multistage DFL, DAgger, economic forecast evaluation, TSFM leakage | `docs/technical/DFL_CLASSIFIER_FAILURE_ANALYSIS.md` | Supports why action classifiers were blocked and why decision quality matters. |
| European dataset bridge: ENTSO-E Transparency Platform, OPSD, OPSD time series, Nord Pool, Ember API, PriceFM dataset, THieF dataset | `docs/technical/DFL_DATA_RECOVERY_ROADMAP.md`; `docs/technical/DFL_DATA_EXPANSION_AND_ACTION_LABELS.md` | Research-only. `training_use_allowed=false` until licensing, timezone, currency, market-rule, and temporal-availability checks pass. |
| Perturbed DFL for strategic energy storage | `docs/technical/deep-research-reports/real-data-90-anchor-benchmark-report.md` | Supports the future differentiable storage layer direction. |

## Newly Captured Academic And Dataset Sources

| Status | Source | Why it matters | Current use |
|---|---|---|---|
| include/watch | [GIFT-Eval: A Benchmark For General Time Series Forecasting Model Evaluation](https://huggingface.co/papers/2410.10393), arXiv `2410.10393` | Time-series foundation-model benchmark with diverse domains, frequencies, and explicit non-leaking pretraining context. | Local PDF: `2410.10393-gift-eval-time-series-forecasting-benchmark.pdf`. |
| watch | [Benchmarking Time Series Foundation Models for Short-Term Household Electricity Load Forecasting](https://huggingface.co/papers/2410.09487), arXiv `2410.09487` | Energy/load forecasting benchmark showing where TSFMs can help with longer context and limited task-specific training. | Local PDF: `2410.09487-tsfm-household-electricity-load-forecasting.pdf`. |
| include/watch | [fev-bench: A Realistic Benchmark for Time Series Forecasting](https://huggingface.co/papers/2509.26468), arXiv `2509.26468` | Emphasizes realistic forecasting tasks with covariates plus bootstrapped confidence intervals, win rates, and skill scores. | Local PDF: `2509.26468-fev-bench-realistic-time-series-forecasting.pdf`. |
| watch | [TFRBench: A Reasoning Benchmark for Evaluating Forecasting Systems](https://huggingface.co/papers/2604.05364), arXiv `2604.05364` | Supports reasoning-based forecast evaluation around cross-channel dependencies, trends, and external events. | Local PDF: `2604.05364-tfrbench-forecasting-reasoning-benchmark.pdf`. |
| watch | [Shielded Controller Units for RL with Operational Constraints Applied to Remote Microgrids](https://huggingface.co/papers/2512.01046), arXiv `2512.01046` | Reinforces the safety architecture idea: learned controllers require explicit operational shields and constraint satisfaction. | Local PDF: `2512.01046-shielded-controller-units-rl-microgrids.pdf`. |
| include | [The impact of electricity price forecasting on the optimal day-ahead dispatch of battery energy storage systems](https://www.dfki.de/web/forschung/projekte-publikationen/publikation/16172) | BESS dispatch source explicitly evaluates forecasts by realized dispatch revenue against perfect-foresight schedules and considers ageing costs. | Local note: `dfki-2025-bess-dispatch-forecast-impact.md`. |

## Newly Captured Policy And Market-Coupling Sources

| Status | Source | Why it matters | Current use |
|---|---|---|---|
| include | [Energy Community: Ukraine signals its dedication to EU electricity market coupling](https://www.energy-community.org/news/Energy-Community-News/2026/4/8.html) | As of 8 April 2026, Ukraine adopted draft Law No. 12087-d as a first step toward transposing the Electricity Integration Package. The source also states that secondary legislation and compliance verification are still needed before market coupling. | Use to state the current boundary: EU signals may matter, but full SDAC/SIDC market coupling is not yet an implemented Ukrainian data assumption. |
| include | [European Commission: 2 years since Ukraine and Moldova synchronised electricity grids with EU](https://energy.ec.europa.eu/news/2-years-ukraine-and-moldova-synchronised-electricity-grids-eu-2024-03-15_en) | Confirms synchronization with the Continental European Network and notes import/export trading opportunities with the EU. | Use to justify future EU-derived AFE features as plausible covariates, not direct coupled-market labels. |
| include/watch | [Republic of Moldova joins the Local Implementation Project for coupling electricity markets](https://energie.gov.md/en/content/republic-moldova-joins-local-implementation-project-coupling-electricity-markets?month=2026-04) | Describes the LIP involving Moldova, Ukraine, Romania, Poland, Slovakia, and Hungary for integration into European day-ahead and intraday markets. | Use as roadmap context for external market features and market-coupling status flags. |
| include | [ENTSO-E Single Day-Ahead Coupling (SDAC)](https://www.entsoe.eu/network_codes/cacm/implementation/sdac/) | Defines SDAC as a pan-European cross-zonal day-ahead market using a common algorithm that accounts for cross-border transmission constraints. | Use to define why cross-border capacity and neighboring-zone prices are relevant future features. |
| include | [IEA: Ukraine's Energy Security and the Coming Winter](https://www.iea.org/reports/ukraines-energy-security-and-the-coming-winter/ukraines-energy-system-under-attack) | Official 2024 energy-security context for attacks on Ukraine's energy system, generation deficit, decentralization, and European interconnection/import support. | Local PDF: `iea-2024-ukraines-energy-security-and-coming-winter.pdf`; local note: `iea-2024-ukraines-energy-security-source-note.md`. |

Local note: `eu-ukraine-market-coupling-policy-notes-2026-05-08.md`.

## Feature-Use Boundary For EU-Derived Signals

EU-derived signals should be planned as AFE covariates, not imported into
Ukrainian training by default.

Allowed after audit:

- lagged neighboring DAM prices and spreads;
- forecast-available cross-border capacity;
- known pre-anchor nominations or auction results;
- neighboring residual-load/weather forecasts;
- market-coupling status flags;
- feature availability timestamps.

Blocked until explicitly validated:

- same-hour realized EU prices as Ukrainian forecast inputs;
- rows without timezone and daylight-saving normalization;
- EUR/MWh rows without deterministic UAH/MWh conversion metadata;
- data whose license does not allow research/training use;
- coupled-market assumptions that are not valid for the delivery date.

## Next Documentation Target

When the AFE sidecar is implemented, move the policy-market-coupling sources
above into an `external_market_context` feature registry with fields:

- `source`;
- `license_status`;
- `timezone`;
- `currency`;
- `availability_timestamp`;
- `market_rule_scope`;
- `training_use_allowed`;
- `claim_boundary`.
