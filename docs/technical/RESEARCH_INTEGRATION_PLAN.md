# Research Integration Plan

This plan incorporates the deep-research review in [deep-research-report.md](deep-research-reports/deep-research-report.md) into the project roadmap. The main conclusion is that the repo already has a coherent engineering-first MVP, but thesis-grade claims now depend on a real-data benchmark and rolling-origin decision evaluation before stronger DFL claims.

## Canonical Thesis Position

The safest thesis claim is:

> A reproducible Ukraine DAM BESS arbitrage benchmark that compares strict similar-day, NBEATSx, and TFT forecasts by degradation-adjusted decision value, oracle regret, feasibility, throughput, and EFC, with DFL treated as a later pilot once the benchmark is stable.

This deliberately avoids overclaiming that the current Level 1 simulator is a full digital twin or that SOTA forecast models alone prove a better trading strategy.

The market-timing argument is also now stronger: JSC Market Operator reported on 20 March 2026 that more than 180 companies were actively testing its BESS Economic Dispatch Platform for DAM/IDM arbitrage. That supports the thesis motivation, but the project still needs its own reproducible benchmark before claiming measured performance.
Source: https://www.oree.com.ua/index.php/newsctr/n/32160

## Priority Order

1. **Real-data hardening**
   - Backfill observed OREE hourly DAM history.
   - Add historical weather aligned to tenant coordinates.
   - Preserve provenance so synthetic rows never silently enter benchmark claims.
   - Add effective-dated NBU FX, Market Operator fees, and NEURC price caps.

2. **Rolling-origin benchmark**
   - For each anchor, expose only past data to forecast/model logic.
   - Forecast the next 24 hours.
   - Route every forecast candidate through the same LP contour.
   - Score the feasible schedule against realized prices.
   - Compare with the oracle LP value and report regret.

3. **Forecast upgrade comparison**
   - Compare strict similar-day, NBEATSx, and TFT on forecast metrics and decision metrics.
   - Keep NBEATSx/TFT as forecast candidates until they improve value/regret, not only MAE/RMSE.

4. **Economics and degradation robustness**
   - Add sensitivity for degradation cost, SOC limits, RTE, FX, market fees, and battery capex/lifetime.
   - Keep the Level 1 EFC proxy as the baseline; label richer ageing/digital-twin work as planned.

5. **DFL pilot**
   - Add Decision-Focused Learning only after the benchmark has stable real-data evidence.
   - Evaluate DFL by regret and net value against the same oracle/baseline protocol.

## Current Regulatory And Cost Anchors

These values should become effective-dated assumptions rather than timeless constants:

- NEURC Resolution No. 621 from 23 April 2026, effective 30 April 2026:
  - DAM/IDM maximum price cap: `15,000 UAH/MWh`.
  - DAM/IDM minimum price cap: `10 UAH/MWh`.
  - Balancing maximum price cap: `17,000 UAH/MWh`.
  - Balancing minimum price cap: `0.01 UAH/MWh`.
  - Official legal text: https://zakon.rada.gov.ua/go/v0621874-26
  - NEURC page: https://www.nerc.gov.ua/acts/pro-hranychni-tsiny-na-rynku-na-dobu-napered-vnutrishnodobovomu-rynku-ta-balansuiuchomu-rynku
- Market Operator 2026 DAM/IDM transaction tariff:
  - `6.88 UAH/MWh`, without VAT.
  - Fixed software payment: `3,837.84 UAH`, without VAT.
  - Notice: https://www.oree.com.ua/index.php/newsctr/n/30795

The current DAM-only MVP is not broken by the balancing cap mismatch, but any IDM/Balancing extension should resolve caps by delivery date.

## Dashboard Plan Inputs

Do not touch dashboard code in this planning slice. Future dashboard redesign should show:

- Real-data benchmark mode: observed vs synthetic/derived provenance share.
- Forecast-strategy comparison: strict similar-day vs NBEATSx vs TFT by net value and regret.
- Physical/economic battery stress: throughput, EFC, degradation penalty, SOC window occupancy.
- Cost assumptions panel: FX, transaction tariff, capex, lifetime, cycle/day.
- Effective-dated market constraints: active price caps for selected delivery date and venue.
- Research warning state when synthetic fallback data are present in a thesis benchmark run.

## Done Criteria For The Next Research Slice

- A Dagster materialization can produce a rolling-origin evaluation frame from observed historical DAM rows.
- The output separates forecast metrics, decision metrics, and operational feasibility metrics.
- The benchmark can be filtered by tenant, anchor date, forecast model, and data provenance.
- The API/dashboard read model can show whether the result is demo-grade or thesis-grade.
- Documentation states clearly when a result is based on synthetic fallback and therefore not a market-performance claim.

## Week 3 Evidence Slice Protocol

The Week 3 slice operationalizes the real-data benchmark for one tenant before any wider tenant or DFL expansion:

- Tenant: `client_003_dnipro_factory`.
- Observed data window: `2026-01-01` to `2026-04-30`.
- Benchmark cap: `max_anchors=30`.
- Tracked Dagster run config: [configs/real_data_benchmark_week3.yaml](../../configs/real_data_benchmark_week3.yaml).

Materialize the benchmark through the Compose Dagster service:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame -c configs/real_data_benchmark_week3.yaml
```

Then export downstream research summaries from the persisted benchmark rows:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_research_layer_from_store.py --run-slug week3_real_data_benchmark
```

Acceptance checks:

- `/dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory` returns `data_quality_tier=thesis_grade`.
- The response has `model_count=3`.
- The response has at least `30` anchors.
- Any generated research export must be described as benchmark evidence, not market execution or full DFL.

Latest verified Week 3 run:

- Materialization run succeeded on 2026-05-06 with MLflow run `deb0633303de4430967aece6767315f2`.
- Tenant-specific API response for `client_003_dnipro_factory`: `data_quality_tier=thesis_grade`, `anchor_count=30`, `model_count=3`, `best_model_name=strict_similar_day`.
- Export directory: `data/research_runs/week3_real_data_benchmark`.
- The export command currently aggregates latest persisted batches for all tenants in Postgres; the Week 3 acceptance target is the Dnipro tenant batch with 30 anchors and 90 benchmark rows.
