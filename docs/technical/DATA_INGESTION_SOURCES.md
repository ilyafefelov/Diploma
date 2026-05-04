# Data Ingestion Sources

This document defines the Bronze ingestion source contract for the current MVP. It is intentionally narrower than the final research architecture: the goal is to make live weather and market-price provenance auditable before adding extra event feeds or DFL training data.

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

## Optional Database Persistence

Set `SMART_ARBITRAGE_MARKET_DATA_DSN` to enable Postgres persistence. If it is unset, the assets still return the same Polars DataFrames and the store is a no-op.

The optional store creates:

- `market_price_observations`
- `weather_observations`

The operator status store remains separate and still uses `SMART_ARBITRAGE_OPERATOR_STATUS_DSN`.

## Recommended Next Sources

1. **NBU FX API**: useful for USD/EUR cost anchors and degradation economics.
   - https://bank.gov.ua/ua/open-data/api-dev
2. **NEURC price-cap rules**: useful as effective-dated market constraints.
   - https://www.nerc.gov.ua/
3. **Energy Map**: useful as an alternate/cross-check source for Ukrainian energy datasets.
   - https://energy-map.info/
4. **Ukrenergo news/events**: useful as qualitative event flags for outages, imports, emergency restrictions, and repairs.
   - https://ua.energy/news/
5. **ENTSO-E Transparency Platform**: useful for cross-border flows, load, generation, and regional context if API access is available.
   - https://transparency.entsoe.eu/

For the 8-week diploma scope, OREE + Open-Meteo + explicit provenance is enough for the first defensible live-data Bronze layer. News/events and ENTSO-E are better treated as planned exogenous feature extensions unless the baseline contour is already stable.
