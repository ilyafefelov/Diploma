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

## Dagster Lineage Taxonomy

Dagster asset keys remain the stable execution contract. The lineage UI now uses medallion-prefixed groups so the graph reads by both layer and context instead of broad `bronze`, `silver`, and `gold` buckets.

- Bronze ingestion groups include `bronze_market_data`, `bronze_weather`, `bronze_grid_events`, `bronze_tenant_load`, and `bronze_battery_telemetry`.
- Silver transformation groups include `silver_forecast_features`, `silver_forecast_candidates`, `silver_real_data_benchmark`, and `silver_simulated_training`.
- Gold evidence groups include `gold_real_data_benchmark`, `gold_calibration`, `gold_selector_diagnostics`, `gold_dfl_training`, `gold_mvp_dispatch`, and `gold_decision_transformer`.

Every active asset carries the standard taxonomy tags: `medallion`, `domain`, `elt_stage`, `ml_stage`, and `evidence_scope`. Optional tags such as `backend` and `market_venue` refine forecast backends and DAM-specific evidence.

Useful Dagster selection strings:

```powershell
--select group:gold_calibration
--select tag:ml_stage=calibration
--select tag:evidence_scope=thesis_grade
```

For the Dnipro calibration preview, read lineage in this order:

1. `bronze_market_data` and `bronze_weather` load observed OREE DAM and historical Open-Meteo rows.
2. `silver_real_data_benchmark` builds tenant-aligned price/weather features.
3. `gold_real_data_benchmark` publishes the rolling-origin raw forecast comparison.
4. `gold_calibration` publishes regret-weighted and horizon-regret-weighted calibration evidence.
5. `gold_selector_diagnostics` publishes selector and dispatch-sensitivity diagnostics. These remain research evidence, not full DFL and not market execution.

## Done Criteria For The Next Research Slice

- A Dagster materialization can produce a rolling-origin evaluation frame from observed historical DAM rows.
- The output separates forecast metrics, decision metrics, and operational feasibility metrics.
- The benchmark can be filtered by tenant, anchor date, forecast model, and data provenance.
- The API/dashboard read model can show whether the result is demo-grade or thesis-grade.
- Documentation states clearly when a result is based on synthetic fallback and therefore not a market-performance claim.

## Calendar Alignment Note

The current academic reporting package remains Week 3. The accepted Week 3 result is the 30-anchor Dnipro real-data benchmark below. The 90-anchor calibration path is already prepared as draft Week 4 evidence, but it should be presented only as next-step calibration/selector material for the second demo and chapter work. It must not replace the Week 3 headline, and it must not be described as full DFL or market execution.

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

## Week 4 Calibration Evidence Protocol

The Week 4 slice keeps the same real-data benchmark path and adds calibration/selector evidence only. It must not be described as full DFL or market execution.

- Tenant: `client_003_dnipro_factory`.
- Observed data window: `2026-01-01` to `2026-04-30`.
- Benchmark cap: `max_anchors=90`.
- Tracked Dagster run config: [configs/real_data_calibration_week4.yaml](../../configs/real_data_calibration_week4.yaml).
- Source map: [docs/thesis/sources/week4-research-ingestion.md](../thesis/sources/week4-research-ingestion.md).

Materialize the calibration path through the Compose Dagster service:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,real_data_value_aware_ensemble_frame,dfl_training_frame,regret_weighted_forecast_calibration_frame,regret_weighted_forecast_strategy_benchmark_frame,horizon_regret_weighted_forecast_calibration_frame,horizon_regret_weighted_forecast_strategy_benchmark_frame,calibrated_value_aware_ensemble_frame,forecast_dispatch_sensitivity_frame,risk_adjusted_value_gate_frame -c configs/real_data_calibration_week4.yaml
```

Then export the research layer:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_research_layer_from_store.py --run-slug week4_calibration_dnipro_90 --calibration-min-prior-anchors 14 --calibration-window-anchors 28
```

The export now writes `research_layer_manifest.json` beside the existing CSV and
summary artifacts. Use this manifest before reporting calibration evidence: it
records the run slug, included tenants, strategy kinds, latest
`generated_at` batch per `tenant_id + strategy_kind`, row/anchor counts,
`data_quality_tiers`, and explicit `not_full_dfl=true` /
`not_market_execution=true` claim flags.

Acceptance checks:

- `/dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory` returns `data_quality_tier=thesis_grade`, `anchor_count=90`, `model_count=3`.
- `/dashboard/calibrated-ensemble-benchmark?tenant_id=client_003_dnipro_factory` returns 90 selector rows with `model_count=1`.
- `/dashboard/risk-adjusted-value-gate?tenant_id=client_003_dnipro_factory` returns 90 selector rows with risk diagnostics.
- `/dashboard/forecast-dispatch-sensitivity?tenant_id=client_003_dnipro_factory` returns 450 diagnostic rows across five candidate streams.
- Exported `research_layer_manifest.json` separates latest tenant batches from older persisted rows and repeats the conservative calibration/selector claim boundary.

Latest verified Week 4 run:

- Materialization run succeeded on 2026-05-07 local time; Dagster run id `ce705fa2-b100-4b17-a33b-2011409f3e90`.
- MLflow runs: rolling benchmark `2f1248a3822f4785af5332e867e09953`, regret-weighted benchmark `89389bea2c62495a99d1581ba7514d90`, horizon-aware benchmark `041bbbe236dd438393e442f9dbff3d59`, calibrated ensemble `fed333f97e9b4e33be2f6adab1415f17`, risk gate `e53ce78fdc1d462f9622e7d660241b20`.
- Tenant-specific API response for `client_003_dnipro_factory`: raw benchmark `data_quality_tier=thesis_grade`, `anchor_count=90`, `model_count=3`, `best_model_name=strict_similar_day`.
- Raw Dnipro means: `strict_similar_day=1384.70` UAH regret, `nbeatsx_silver_v0=2070.28`, `tft_silver_v0=2361.96`.
- Horizon-aware calibration improved neural candidates: `tft_horizon_regret_weighted_calibrated_v0=1727.29` UAH mean regret and `nbeatsx_horizon_regret_weighted_calibrated_v0=1804.38`, while strict similar-day remained the strongest individual control.
- Selector read models: calibrated ensemble mean regret `1479.65` UAH, risk-adjusted gate mean regret `1428.59` UAH.
- Export directory: `data/research_runs/week4_calibration_dnipro_90`.
- The export command still aggregates latest persisted batches for all tenants in Postgres; the Week 4 acceptance target is the tenant-specific Dnipro 90-anchor API result.

## Week 3 Calibration Preview After Lineage Taxonomy

The lineage taxonomy slice reran the same Dnipro 90-anchor calibration path after rebuilding Dagster/API images so the new groups and tags were visible in Dagster UI. This is still prepared-ahead calibration/selector evidence, not a Week 3 headline and not full DFL.

- Dagster run id: `ffc8d05b-7121-4c11-a761-de37535cd161`.
- Export directory: `data/research_runs/week3_calibration_preview_dnipro_90`.
- API validation for `client_003_dnipro_factory`: raw benchmark `data_quality_tier=thesis_grade`, `anchor_count=90`, `model_count=3`, `best_model_name=strict_similar_day`.
- Selector read models: calibrated ensemble 90 rows, risk-adjusted gate 90 rows with diagnostics, forecast-dispatch sensitivity 450 rows across four diagnostic buckets.
- Raw forecast candidate means: `strict_similar_day=1384.70` UAH regret, `nbeatsx_silver_v0=2070.28`, `tft_silver_v0=2361.96`.
- Horizon-aware calibration improved neural candidates: `tft_horizon_regret_weighted_calibrated_v0=1727.29` UAH mean regret and `nbeatsx_horizon_regret_weighted_calibrated_v0=1804.38`, while strict similar-day remained the strongest individual control.
- Selector means: calibrated ensemble `1479.65` UAH regret, risk-adjusted gate `1428.59`.

## Calibration QA Manifest

The research export path now emits a report-ready `research_layer_manifest.json`
for every new run. This is the guardrail against mixing Week 3 Dnipro evidence
with all-tenant or older persisted batches.

Manifest checks before supervisor-facing use:

1. `claim_scope` must be `calibration_selector_evidence_not_full_dfl`.
2. `not_full_dfl` and `not_market_execution` must both be `true`.
3. `latest_generated_at_by_tenant_strategy` must match the intended tenant and
   strategy batches being reported.
4. `anchor_count_by_tenant_strategy` and `row_count_by_tenant_strategy` must
   support the stated tenant-specific claim.
5. `data_quality_tiers` must include only tiers that are acceptable for the
   claim being made; Week 3 thesis evidence requires `thesis_grade`.

Latest manifested calibration registry run:

- Fresh export slug: `week3_calibration_preview_manifested_dnipro_90`.
- Manifest path: `data/research_runs/week3_calibration_preview_manifested_dnipro_90/research_layer_manifest.json`.
- Dnipro latest raw benchmark manifest entry:
  `anchor_count=90`, `row_count=270`,
  `latest_generated_at=2026-05-06T22:57:36.014876+00:00`,
  `data_quality_tiers=["thesis_grade"]`.
- Manifest claim flags: `claim_scope=calibration_selector_evidence_not_full_dfl`,
  `not_full_dfl=true`, `not_market_execution=true`.
- API read models agree on the latest Dnipro batch: raw benchmark 90 anchors
  and 270 rows, calibrated selector 90 rows, risk-adjusted gate 90 rows, and
  forecast-dispatch sensitivity 450 diagnostic rows.
- Postgres still contains older Dnipro persisted rows; the registry separates
  those table totals from the latest generated batch used for reporting.
- Tracked registry:
  [MANIFESTED_CALIBRATION_EVIDENCE_REGISTRY.md](MANIFESTED_CALIBRATION_EVIDENCE_REGISTRY.md).

## DFL Readiness Gate

The next stability slice moves the manifest/API/Postgres evidence rules into
Dagster-visible asset checks before any full differentiable DFL training. The
gate is documented in [DFL_READINESS_GATE.md](DFL_READINESS_GATE.md).

Registered checks:

- `real_data_rolling_origin_benchmark_frame:dnipro_thesis_grade_90_anchor_evidence`
  blocks non-thesis-grade, missing raw candidates, insufficient Dnipro anchors,
  synthetic provenance, and anchor/horizon misuse.
- `dfl_training_frame:dfl_training_readiness_evidence` warns when the DFL
  training table is not ready as research evidence, without invalidating the raw
  benchmark.
- `horizon_regret_weighted_forecast_strategy_benchmark_frame:horizon_calibration_no_leakage_evidence`
  blocks future/leaky calibration metadata and missing 90-anchor coverage.
- `calibrated_value_aware_ensemble_frame:calibrated_selector_cardinality_evidence`
  and `risk_adjusted_value_gate_frame:risk_adjusted_selector_cardinality_evidence`
  block missing or duplicate selector rows per anchor.

Readiness decision: a passing gate is enough to begin a bounded offline DFL
experiment design, but it is still not a full DFL claim and not market
execution. TSFM leakage remains the blocking research guardrail: temporal
evaluation must be latest-batch, no-leakage, and source-linked before stronger
modeling claims are made.

Latest verified DFL readiness gate:

- Dagster run id: `b55b9e01-8688-4fc2-abe6-6380b96502b9`.
- Latest Dnipro generated batch: `2026-05-07T02:24:42.974392Z`.
- Export directory:
  `data/research_runs/week3_dfl_readiness_gate_dnipro_90`.
- Manifest path:
  `data/research_runs/week3_dfl_readiness_gate_dnipro_90/research_layer_manifest.json`.
- All five evidence checks passed for Dnipro 90 anchors: raw benchmark,
  DFL training readiness, horizon calibration no-leakage, calibrated selector,
  and risk-adjusted selector.
- The run exposed and fixed a metadata persistence issue where Polars struct
  inference could drop calibration counts from corrected candidate payloads when
  the strict control row ranked first. The fix preserves calibration metadata;
  it does not change model semantics or public contracts.

## Offline DFL Experiment After Readiness Gate

After the DFL readiness gate passed, the next slice started a bounded offline
DFL experiment without changing API contracts, dashboard contracts, Pydantic
schemas, Dagster asset keys, resources, IO managers, or dependencies.

Implementation:

- New asset: `offline_dfl_experiment_frame`.
- Dagster group: `gold_dfl_training`.
- Source asset: `real_data_rolling_origin_benchmark_frame`.
- Training rule: sort by anchor, hold out the latest validation anchors, and
  train horizon-specific price biases only on earlier anchors.
- Claim scope: `offline_dfl_experiment_not_full_dfl`.

Materialization command:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_experiment_frame -c configs/real_data_calibration_week4.yaml
```

Latest run:

- Dagster run id: `54afa042-332c-459e-b6ea-e1b0308fa508`.
- Latest raw benchmark batch:
  `2026-05-07T10:01:50.67257Z`.
- `real_data_rolling_origin_benchmark_frame:dnipro_thesis_grade_90_anchor_evidence`
  passed during the materialization.
- Output rows: 2, one for `nbeatsx_silver_v0` and one for `tft_silver_v0`.

Held-out result:

| Model | Raw relaxed regret | Offline DFL relaxed regret | Delta | Finding |
|---|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 1477.37 | 1499.85 | -22.47 | Diagnostic only; no improvement. |
| `tft_silver_v0` | 1974.55 | 2460.07 | -485.52 | Diagnostic only; no improvement. |

This is useful negative evidence: the first differentiable relaxed-LP training
loop runs against the gated real-data split and preserves temporal discipline,
but it does not beat the raw held-out relaxed-LP baseline. The next research
slice should improve the validation-safe training design before expanding to
more tenants or stronger DFL claims.

Tracked note:
[OFFLINE_DFL_EXPERIMENT.md](OFFLINE_DFL_EXPERIMENT.md).

## Week 3 Deep Research Source Map And Baseline Freeze

The Week 3 deep-research intake is now indexed under
[deep-research-reports/week3 research/README.md](deep-research-reports/week3%20research/README.md)
with a report-to-code map in
[deep-research-reports/week3 research/source-map.md](deep-research-reports/week3%20research/source-map.md).
The source map separates implemented facts, benchmark-supported evidence,
planned research, and out-of-scope claims.

The main implementation decision from that intake is not to start a larger
neural policy immediately. The current foundation already includes
`dfl_training_frame`, calibration assets, selector diagnostics, Dagster evidence
checks, and a bounded offline DFL experiment. The next safe step is therefore to
freeze the control comparator and add richer sidecar DFL examples plus a
promotion gate.

Tracked baseline freeze:
[BASELINE_FREEZE.md](BASELINE_FREEZE.md).

Freeze summary:

- `strict_similar_day` remains the Level 1 control comparator.
- Forecast candidates must be evaluated through the same LP/oracle protocol.
- Week 3 30-anchor Dnipro evidence, Dnipro 90-anchor preview evidence, and
  all-tenant diagnostic snapshots must stay separately labeled.
- Current offline DFL v0 is negative diagnostic evidence and must not be
  promoted.
- PriceFM and THieF support future forecast-layer direction; TSFM leakage and
  the DFL survey support the current no-leakage, decision-value-first protocol.
