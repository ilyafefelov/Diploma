# External Benchmark

## Ukrainian Market Rules

The repo's date-aware market-rule config matches the reviewed 2026 NEURC Resolution No. 621 boundary:

- DAM/IDM max price cap: `15000 UAH/MWh`
- Balancing market max price cap: `17000 UAH/MWh`
- DAM/IDM min price cap: `10 UAH/MWh`
- Balancing min price cap: `0.01 UAH/MWh`
- Effective date in repo config: `2026-04-30T00:00:00`

External sources:

- [Interfax Ukraine coverage of NEURC 621](https://en.interfax.com.ua/news/economic/1161619.html)
- [OREE market operator notice on DAM/IDM caps](https://www.oree.com.ua/index.php/web/7004)
- [Zakon Rada record for NEURC Resolution No. 621](https://zakon.rada.gov.ua/go/v0621874-26)

Review conclusion:

- The active market-rule implementation is aligned with the 2026 cap update.
- The stale legacy duplicate under `src/gatekeeper/` should be quarantined to avoid reviewer confusion.

## Industry Comparators

| Product | What it claims publicly | Difference from this thesis |
|---|---|---|
| Tesla Autobidder | Real-time trading/control and value-based asset management for energy storage | Commercial market software; this thesis is offline preview with no market execution |
| Fluence Mosaic | Intelligent automated bidding software for renewables and storage revenue optimization | Commercial bidding software; this thesis has no market-submittable bids |
| Wartsila GEMS IntelliBidder | Cloud-hosted forecasting, schedule commitment, and bid optimization | Commercial operational platform; this thesis is research/read-model evidence |

External sources:

- [Tesla Autobidder](https://www.tesla.com/en_sa/support/energy/tesla-software/autobidder)
- [Fluence Mosaic](https://fluenceenergy.com/mosaic-intelligent-bidding-software/)
- [Wartsila GEMS Digital Energy Platform](https://storage.wartsila.com/technology/gems/)

Review conclusion:

- These products are useful market comparators, but the thesis should not imply equivalent production capability.
- The defensible positioning is "academic offline evidence system inspired by the same BESS optimization problem class."

## Academic Comparators

| Work | Relevance | Difference from this thesis |
|---|---|---|
| Sang et al., "Electricity Price Prediction for Energy Storage System Arbitrage: A Decision-Focused Approach" | Direct DFL-for-ESS-arbitrage comparator using downstream decision quality | This project currently has offline regret/value research, not full deployed DFL |
| Yi et al., "A Decision-Focused Predict-then-Bid Framework for Strategic Energy Storage" | Closest predict-then-bid framing: price prediction plus storage optimization plus market-clearing differentiability | This project does not yet implement the full tri-layer market-clearing stack |
| Olivares et al., NBEATSx EPF | Supports NBEATSx as a legitimate electricity price forecasting adapter | Forecast quality alone does not prove schedule/bidding value |
| Probabilistic TFT EPF work | Supports TFT as a serious EPF research direction | Current local TFT evidence is non-promoted |
| BESS degradation/RL papers | Useful for future digital-twin and degradation-aware control framing | Current thesis should keep degradation and DT/LAVA as bounded evidence, not production dispatch |

External sources:

- [arXiv 2505.01551, Decision-Focused Predict-then-Bid](https://arxiv.org/abs/2505.01551)
- [arXiv 2305.00362, Decision-focused ESS arbitrage](https://arxiv.org/abs/2305.00362)
- [arXiv 2104.05522, NBEATSx electricity prices](https://arxiv.org/abs/2104.05522)
- [DOI 10.1002/for.3084, probabilistic TFT electricity price forecasting](https://ideas.repec.org/a/wly/jforec/v43y2024i5p1465-1491.html)

Review conclusion:

- The thesis is academically relevant because it uses decision-quality metrics and strict comparator evidence.
- It must clearly state that the full predict-then-bid literature includes market-clearing/settlement layers that are not implemented here.
