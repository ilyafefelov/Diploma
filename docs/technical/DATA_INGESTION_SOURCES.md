# Data Ingestion Sources

This document defines the Bronze ingestion source contract for the current MVP. It is intentionally narrower than the final research architecture: the goal is to make live weather and market-price provenance auditable before adding extra event feeds or DFL training data.

Deep-research update: [deep-research-report.md](deep-research-reports/deep-research-report.md) confirms that the current architecture is coherent, but the thesis-grade empirical claim now depends on hardening this document from a demo/live-overlay source contract into a real historical benchmark contract.

## Current Implemented Sources

### OREE DAM Price Data

- URL: https://www.oree.com.ua/index.php/pricectr/data_view
- Fallback page: https://www.oree.com.ua/index.php/pricectr?lang=english
- Market venue: `DAM`
- Market zone: `IPS`
- Time resolution: hourly
- Price unit: `UAH/MWh`
- Current source markers:
  - `source=OREE_DATA_VIEW` for the POST endpoint
  - `source=OREE_HTML` for the page-table fallback
  - `source_kind=observed`
  - `source_url=<source URL>`

OREE returns the `data_view` response as JSON even when the HTTP `content-type` is `text/html`. The scraper must parse the JSON payload first and then extract the embedded `content` HTML table.

### Open-Meteo Weather Data

- URL: https://api.open-meteo.com/v1/forecast
- Grain: hourly forecast rows by tenant latitude, longitude, and timezone
- Current source markers:
  - `source=OPEN_METEO`
  - `source_kind=observed`
  - `source_url=https://api.open-meteo.com/v1/forecast`

Weather is tenant/location-specific. The current tenant registry is [simulations/tenants.yml](../../simulations/tenants.yml).

### Synthetic Fallback Data

Synthetic rows are still kept for demo stability, but they must never be indistinguishable from observed data.

Current source markers:

- Market fallback:
  - `source=SYNTHETIC`
  - `source_kind=synthetic`
  - `source_url=synthetic://smart_arbitrage/market_price_history`
- Weather fallback:
  - `source=SYNTHETIC`
  - `source_kind=synthetic`
  - `source_url=synthetic://smart_arbitrage/weather_forecast`

## Location Semantics

DAM prices are not tenant-location-specific in the same way weather is. For the current Level 1 Market Scope, the academically correct Bronze dataset is:

`tenant/location-specific weather + market-zone DAM price + explicit provenance`

Do not present Kyiv, Lviv, Dnipro, Kharkiv, or Odesa as having separate DAM prices unless a future source provides local tariff, DSO congestion, balancing, or aggregator settlement data.

For thesis experiments, tenant awareness should mean: same market-zone DAM price, different asset parameters, different weather context, different SOC/throughput/degradation economics.

## Research-Grade Bronze Target

The demo Bronze layer may keep synthetic fallback rows for local stability, but the thesis benchmark must separate three modes:

- `observed`: real historical or current rows from OREE, Open-Meteo or another named source.
- `synthetic`: generated rows for demo stability or simulation.
- `derived`: rows calculated from observed/synthetic inputs, with upstream provenance retained.

Required before strong empirical claims:

1. Backfill observed OREE hourly DAM history for the rolling-origin backtest horizon.
2. Add timestamp-aligned historical weather for tenant coordinates, not only forecast weather.
3. Keep every row source-marked so synthetic fallback cannot silently enter the research benchmark.
4. Add NBU FX and Market Operator tariff assumptions as effective-dated economic parameters.
5. Add effective-dated NEURC price caps for the delivery date being evaluated.

The benchmark should then feed `Forecast Strategy Evaluation` through a rolling-origin protocol: train or fit only on past rows, forecast the next horizon, solve LP, score against realized prices, and compare with oracle value/regret.

## Optional Database Persistence

Set `SMART_ARBITRAGE_MARKET_DATA_DSN` to enable Postgres persistence. If it is unset, the assets still return the same Polars DataFrames and the store is a no-op.

The optional store creates:

- `market_price_observations`
- `weather_observations`

The operator status store remains separate and still uses `SMART_ARBITRAGE_OPERATOR_STATUS_DSN`.

## Recommended Next Sources

1. **OREE historical DAM archive/backfill**: mandatory for thesis-grade rolling-origin value and regret claims.
   - https://www.oree.com.ua/index.php/pricectr/data_view
2. **Historical weather API/source**: required to align exogenous features with the observed DAM history.
   - https://open-meteo.com/
3. **NBU FX API**: useful for USD/EUR cost anchors and degradation economics.
   - https://bank.gov.ua/ua/open-data/api-dev
4. **NEURC price-cap rules**: required as effective-dated market constraints.
   - https://www.nerc.gov.ua/
   - Verified current anchor: NEURC Resolution No. 621 from 23 April 2026 sets DAM/IDM max `15,000 UAH/MWh`, DAM/IDM min `10 UAH/MWh`, Balancing max `17,000 UAH/MWh`, Balancing min `0.01 UAH/MWh`, effective 30 April 2026.
   - Official legal text: https://zakon.rada.gov.ua/go/v0621874-26
   - Regulator page: https://www.nerc.gov.ua/acts/pro-hranychni-tsiny-na-rynku-na-dobu-napered-vnutrishnodobovomu-rynku-ta-balansuiuchomu-rynku
5. **Market Operator tariffs and fixed fees**: required for net-value accounting beyond gross spread minus degradation.
   - 2026 tariff notice: https://www.oree.com.ua/index.php/newsctr/n/30795
   - Current 2026 anchor: `6.88 UAH/MWh` transaction tariff for DAM/IDM and `3,837.84 UAH` fixed software payment, both without VAT.
6. **Energy Map**: useful as an alternate/cross-check source for Ukrainian energy datasets.
   - https://energy-map.info/
7. **Ukrenergo news/events**: useful as qualitative event flags for outages, imports, emergency restrictions, and repairs.
   - https://ua.energy/news/
8. **ENTSO-E Transparency Platform**: useful for cross-border flows, load, generation, and regional context if API access is available.
   - https://transparency.entsoe.eu/

For the 8-week diploma scope, OREE + Open-Meteo + explicit provenance is enough for the first defensible live-data Bronze layer. News/events and ENTSO-E are better treated as planned exogenous feature extensions unless the baseline contour is already stable.

After the deep-research update, the priority order is stricter: real OREE history and rolling-origin evaluation come before broad event-feed expansion or DFL performance claims.
