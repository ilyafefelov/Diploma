# Architecture Review Source Capture - 2026-05-11

Purpose: close the indexing gap exposed by
`docs/technical/deep-research-reports/2026-05-11-architecture-review/source-matrix.md`.

Scope: official regulatory, data-governance, market-coupling, and two missing
scientific sources used by the architecture review packet. These sources support
thesis framing and future engineering gates. They do not change the current
implemented claim: Ukrainian OREE DAM plus tenant Open-Meteo evidence,
`strict_similar_day` as frozen LP fallback, offline DFL/DT research only, and
no live market execution.

## Newly Indexed From The Architecture Review Matrix

| Status | Source | What it supports | Current project use |
|---|---|---|---|
| include | [European Commission: Electricity market design](https://energy.ec.europa.eu/topics/markets-and-consumers/electricity-market-design_en) | EU electricity-market reform, flexibility, renewables integration, market coupling, and consumer/market design context. | Policy framing and future market-coupling roadmap only. |
| include/watch | [European Commission: EU electricity trading in the day-ahead markets becomes more dynamic](https://energy.ec.europa.eu/news/eu-electricity-trading-day-ahead-markets-becomes-more-dynamic-2025-10-01_en) | SDAC day-ahead market-time-unit shift to 15-minute trading and EU market-coupling context. | Supports future neighbor-market covariates after licensing, timezone, currency, and publication-time gates. |
| include | [NEURC Resolution No. 621, 2026-04-23](https://www.nerc.gov.ua/acts/pro-hranychni-tsiny-na-rynku-na-dobu-napered-vnutrishnodobovomu-rynku-ta-balansuiuchomu-rynku) | Effective-dated Ukrainian DAM/IDM and balancing-market price caps. | Direct input to forecast sanity gates and market-rule validation. |
| include | [OREE 2026 Market Operator tariff notice](https://www.oree.com.ua/index.php/newsctr/n/30795?lang=english) | 2026 DAM/IDM transaction tariff and fixed software fee. | Future net-profit layer; current LP regret remains gross schedule value minus degradation proxy unless fee modeling is added. |
| include | [Open-Meteo Forecast API](https://open-meteo.com/en/docs) | Forecast weather variables and current/future weather-source contract. | Allowed for forecast-available weather features after temporal-availability checks. |
| include | [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) | Historical hourly weather variables for tenant context. | Already used by thesis-grade benchmark when timestamp-aligned and provenance-marked. |
| include | [Ukraine National Energy and Climate Plan approval notice](https://me.gov.ua/News/Detail?id=2642aff1-2328-4bad-b03f-6f0f7dc292c8&lang=uk-UA) | Ukraine approved the NECP to 2030 on 2024-06-25. | Thesis policy context for EU integration, energy/climate alignment, and reconstruction. |
| include | [Ukraine NECP PDF](https://me.gov.ua/download/2cad4803-661e-4ae9-9748-3006d6eb3e1c/file.pdf) | NECP details and reference to the Energy Strategy of Ukraine through 2050. | Source for energy-strategy framing; not a model/data source. |
| include/watch | [ENTSO-E: Continental Europe synchronization with Ukraine and Moldova](https://www.entsoe.eu/news/2022/03/16/continental-europe-successful-synchronisation-with-ukraine-and-moldova-power-systems/) | Synchronization context for Ukraine/Moldova and Continental Europe. | Supports external market-context rationale; synchronization is not treated as full market coupling. |
| include/watch | [ACER: electricity market coupling integration plan for the Energy Community](https://www.acer.europa.eu/news/acer-will-decide-electricity-market-coupling-integration-plan-energy-community) | Market Coupling Operation integration planning for Energy Community parties. | Roadmap context for future SDAC/SIDC-aligned source governance. |
| include | [European Commission: AI Act regulatory framework](https://digital-strategy.ec.europa.eu/en/policies/regulatory-framework-ai) | Risk-based AI governance, high-risk-system obligations, logging, data quality, human oversight, robustness, and cybersecurity. | Supports Pydantic Gatekeeper, audit logging, human review, and market-execution-disabled boundary. |
| include/watch | [Kumtepeli et al. 2020: Energy Arbitrage Optimization With Battery Storage](https://doi.org/10.1109/ACCESS.2020.3035504) | Degradation-aware arbitrage with electro-thermal performance and semi-empirical aging models. | Supports future degradation-aware digital-twin roadmap; current MVP remains throughput/EFC proxy. |
| include/watch | [Cao et al. 2020: DRL-Based Energy Storage Arbitrage With Accurate Lithium-Ion Battery Degradation Model](https://doi.org/10.1109/TSG.2020.2986333) | Learning-based arbitrage with an accurate battery degradation model and comparison against optimization. | Future comparison point for DRL/MARL alternatives; current project keeps LP as promotion authority. |

## Already Indexed Elsewhere

The same `source-matrix.md` also names core sources that were already indexed
before this capture:

- Yi et al. 2025 predict-then-bid DFL;
- Sang et al. ESS arbitrage DFL;
- Yi, Alghumayjan, and Xu 2024 perturbed DFL;
- Mandi et al. DFL survey;
- Elmachtoub and Grigas SPO+;
- Olivares et al. NBEATSx;
- Lim et al. Temporal Fusion Transformer;
- Agrawal et al. cvxpylayers;
- Amos and Kolter OptNet.

Those remain in `docs/thesis/sources/README.md` and Chapter 2. This capture
adds the sources that were only present in the new architecture-review matrix or
were only partially represented by older policy/data notes.

## Exact Review Source Links

This section mirrors the `Source Links` block from
`docs/technical/deep-research-reports/2026-05-11-architecture-review/review.md`
so the review packet can be audited against `docs/sources`.

### Primary regulatory and data sources

- European Commission, Electricity market design: https://energy.ec.europa.eu/topics/markets-and-consumers/electricity-market-design_en
- NEURC Resolution No. 621, 2026-04-23: https://www.nerc.gov.ua/acts/pro-hranychni-tsiny-na-rynku-na-dobu-napered-vnutrishnodobovomu-rynku-ta-balansuiuchomu-rynku
- OREE 2026 tariff notice: https://www.oree.com.ua/index.php/newsctr/n/30795?lang=english
- Open-Meteo forecast API docs: https://open-meteo.com/en/docs
- Open-Meteo historical weather API docs: https://open-meteo.com/en/docs/historical-weather-api
- Ukraine NECP approval notice: https://me.gov.ua/News/Detail?id=2642aff1-2328-4bad-b03f-6f0f7dc292c8&lang=uk-UA
- Ukraine NECP PDF: https://me.gov.ua/download/2cad4803-661e-4ae9-9748-3006d6eb3e1c/file.pdf
- ENTSO-E synchronization with Ukraine and Moldova: https://www.entsoe.eu/news/2022/03/16/continental-europe-successful-synchronisation-with-ukraine-and-moldova-power-systems/
- ACER market coupling integration plan for Energy Community: https://www.acer.europa.eu/news/acer-will-decide-electricity-market-coupling-integration-plan-energy-community
- European Commission AI Act overview: https://digital-strategy.ec.europa.eu/en/policies/regulatory-framework-ai

### Scientific sources

- Yi et al., Decision-Focused Predict-then-Bid for Strategic Energy Storage: https://arxiv.org/abs/2505.01551
- Sang et al., Electricity Price Prediction for ESS Arbitrage, Decision-Focused Approach: https://doi.org/10.1109/TSG.2022.3166791
- Yi et al., Perturbed Decision-Focused Learning for Strategic Energy Storage: https://arxiv.org/abs/2406.17085
- Mandi et al., Decision-Focused Learning survey: https://arxiv.org/abs/2307.13565
- Olivares et al., NBEATSx / NeuralForecast: https://arxiv.org/abs/2201.12886
- Lim et al., Temporal Fusion Transformers: https://arxiv.org/abs/1912.09363
- Agrawal et al., cvxpylayers: https://arxiv.org/abs/1910.12430
- Amos and Kolter, OptNet: https://arxiv.org/abs/1703.00443
- Kumtepeli et al., energy arbitrage with degradation-aware 3D-MILP: https://doi.org/10.1109/ACCESS.2020.3035504
- Cao et al., DRL energy storage arbitrage with degradation model: https://doi.org/10.1109/TSG.2020.2986333

## Claim Boundaries

- NEURC caps are venue-specific. DAM/IDM and balancing-market bounds must not be
  reused interchangeably.
- OREE fees support future net-profit accounting, not a retroactive change to
  current gross schedule-value experiments.
- Open-Meteo historical weather is acceptable for retrospective benchmarking,
  but forecast-time experiments must use weather data available before the
  decision cut.
- EU market-coupling sources justify future covariates and governance gates, not
  direct mixing of EU rows into Ukrainian DFL training.
- ENTSO-E neighbor-market samples must use the token-redacted Transparency
  Platform request contract captured by `entsoe_neighbor_market_query_spec_frame`:
  `documentType=A44`, `processType=A01`, mapped bidding-zone EIC as both
  `in_Domain` and `out_Domain`, and explicit UTC `periodStart` / `periodEnd`.
- AI Act governance supports deterministic safety boundaries and logging. It
  does not imply the current MVP is a regulated live high-risk deployment.
- Kumtepeli 2020 and Cao 2020 justify richer degradation-aware control as future
  work. The current system must still be described as an economic degradation
  proxy, not a full electrochemical digital twin.
