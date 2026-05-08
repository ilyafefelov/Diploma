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

## All-Tenant Offline DFL Panel v2

The next DFL stability slice expanded the offline experiment to all five
canonical tenants without changing API contracts, dashboard contracts, Pydantic
schemas, existing asset keys, resources, IO managers, or dependencies.

Implementation:

- New asset: `offline_dfl_panel_experiment_frame`.
- Dagster group: `gold_dfl_training`.
- Source asset: `real_data_rolling_origin_benchmark_frame`.
- Run config:
  [../../configs/real_data_offline_dfl_panel_week3.yaml](../../configs/real_data_offline_dfl_panel_week3.yaml).
- Tenants: all five canonical tenants in `simulations/tenants.yml`.
- Split rule: 90 anchors per tenant, latest 18 anchors per tenant held out as
  the final validation panel, giving 90 tenant-anchor validation rows per
  model.
- Checkpoint rule: v2 horizon-bias checkpoint is selected only on prior
  inner-validation anchors, not on the final holdout.
- Claim scope: `offline_dfl_panel_experiment_not_full_dfl`.

Materialization command:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_panel_experiment_frame -c configs/real_data_offline_dfl_panel_week3.yaml
```

Latest run:

- Dagster run id: `5b759ed9-ae80-4c10-b049-7d39eed64d04`.
- Output rows: 10, one row per tenant/model pair for two forecast candidates.
- Export directory:
  `data/research_runs/week3_offline_dfl_panel_v2_90`.
- Latest raw benchmark rows: 270 rows and 90 anchors for each of the five
  tenants.
- Dnipro API read model remains thesis-grade with `anchor_count=90`,
  `model_count=3`, and `best_model_name=strict_similar_day`.

Final-holdout result:

| Model | Final holdout tenant-anchors | Raw relaxed regret | v2 relaxed regret | Improvement | Finding |
|---|---:|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 90 | 2154.92 | 2121.83 | 1.54% | Development gate passes; strict promotion still blocked. |
| `tft_silver_v0` | 90 | 2791.38 | 2665.30 | 4.52% | Best relaxed panel signal; strict promotion still blocked. |

This improves on the negative Dnipro-only offline v0 result, but it remains
relaxed-LP development evidence. It must not be described as full DFL, a live
strategy, a Decision Transformer, or a replacement for the frozen
`strict_similar_day` control. Production promotion remains blocked until a later
strict-LP/oracle promotion gate passes.

Tracked note:
[OFFLINE_DFL_PANEL_EXPERIMENT.md](OFFLINE_DFL_PANEL_EXPERIMENT.md).

## Strict-LP Offline DFL Panel Promotion Gate

The follow-up strict gate reuses the existing
`evaluate_forecast_candidates_against_oracle` path so panel v2 candidates are
judged by the same frozen Level 1 LP, oracle regret, UAH economics, SOC
feasibility, and `strict_similar_day` control comparator.

Implementation:

- New asset: `offline_dfl_panel_strict_lp_benchmark_frame`.
- Dagster group: `gold_dfl_training`.
- Strategy kind: `offline_dfl_panel_strict_lp_benchmark`.
- Run config:
  [../../configs/real_data_offline_dfl_panel_strict_week3.yaml](../../configs/real_data_offline_dfl_panel_strict_week3.yaml).
- Split rule: latest 18 final-holdout anchors per tenant, giving 90
  tenant-anchor validation rows per source model.
- Claim scope: `offline_dfl_panel_strict_lp_gate_not_full_dfl`.

Latest strict run:

- Dagster run id: `ebea6ab3-d295-4585-8cc2-566bb7692581`.
- Output rows: 540, covering two source models, five tenants, 18 final-holdout
  timestamps, and three evaluated candidates per source model.
- Local summary:
  `data/research_runs/week3_offline_dfl_panel_strict_gate_90/strict_gate_summary.json`.
- Provenance flags: all 540 rows are `thesis_grade`, observed-only,
  `not_full_dfl=true`, and `not_market_execution=true`.

Strict-gate result:

| Source model | Raw mean regret | V2 mean regret | Strict control mean regret | Improvement vs raw | Improvement vs strict | Decision |
|---|---:|---:|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 813.40 | 816.62 | 314.81 | -0.40% | -159.40% | blocked |
| `tft_silver_v0` | 1003.54 | 989.55 | 314.81 | 1.39% | -214.33% | blocked |

The finding is conservative and useful: v2 checkpointing is not enough to beat
the frozen strict control under strict LP/oracle scoring. Production promotion
remains blocked. The next DFL slice should improve the decision target or
candidate construction before making stronger DFL claims.

Tracked note:
[OFFLINE_DFL_PANEL_STRICT_PROMOTION_GATE.md](OFFLINE_DFL_PANEL_STRICT_PROMOTION_GATE.md).

## Decision-Target Offline DFL v3

The next strict-gate slice responded to the v2 finding by testing a
decision-targeted affine correction selected on prior strict LP/oracle regret.
It still does not train a neural DFL model and must not be presented as a
Decision Transformer, market execution, or a production replacement for
`strict_similar_day`.

Implementation:

- New assets: `offline_dfl_decision_target_panel_frame` and
  `offline_dfl_decision_target_strict_lp_benchmark_frame`.
- Dagster group: `gold_dfl_training`.
- Strategy kind: `offline_dfl_decision_target_strict_lp_benchmark`.
- Run config:
  [../../configs/real_data_offline_dfl_decision_target_week3.yaml](../../configs/real_data_offline_dfl_decision_target_week3.yaml).
- Split rule: latest 18 final-holdout anchors per tenant, giving 90
  tenant-anchor validation rows per source model.
- Claim scope: `offline_dfl_decision_target_v3_strict_lp_gate_not_full_dfl`.

Latest decision-target run:

- Dagster run id: `9f5962e9-fe56-4b45-bcfa-d1a233fbffdb`.
- Output rows: 720, covering two source models, five tenants, 18
  final-holdout timestamps, and four evaluated candidates per source model:
  strict control, raw source, panel v2, and decision-target v3.
- Local summary:
  `data/research_runs/week3_offline_dfl_decision_target_v3_90/decision_target_v3_summary.json`.
- API sanity check for `client_003_dnipro_factory`: `data_quality_tier=thesis_grade`,
  `anchor_count=90`, and `model_count=3`.

Strict-gate result:

| Source model | Raw mean regret | Panel v2 mean regret | V3 mean regret | Strict control mean regret | V3 improvement vs raw | V3 improvement vs panel v2 | V3 improvement vs strict | Decision |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 813.40 | 816.62 | 814.17 | 314.81 | -0.09% | 0.30% | -158.62% | blocked |
| `tft_silver_v0` | 1003.54 | 989.55 | 1015.36 | 314.81 | -1.18% | -2.61% | -222.53% | blocked |

The finding is deliberately conservative. NBEATSx v3 gives a tiny improvement
over panel v2 but remains far behind `strict_similar_day`; TFT v3 regresses.
The promotion gate therefore stays blocked. The next DFL step should use
action-aligned or ranking-aware labels rather than only affine forecast
correction.

Tracked note:
[OFFLINE_DFL_DECISION_TARGET_V3.md](OFFLINE_DFL_DECISION_TARGET_V3.md).

## Action-Target Offline DFL v4

The next strict-gate slice responded to the v3 result by adding raw-rank
charge/discharge emphasis. The candidate is still selected on prior strict
LP/oracle regret only; it is not a neural DFL training loop, not a Decision
Transformer, not market execution, and not a production replacement for
`strict_similar_day`.

Implementation:

- New assets: `offline_dfl_action_target_panel_frame` and
  `offline_dfl_action_target_strict_lp_benchmark_frame`.
- Dagster group: `gold_dfl_training`.
- Strategy kind: `offline_dfl_action_target_strict_lp_benchmark`.
- Run config:
  [../../configs/real_data_offline_dfl_action_target_week3.yaml](../../configs/real_data_offline_dfl_action_target_week3.yaml).
- Split rule: latest 18 final-holdout anchors per tenant, giving 90
  tenant-anchor validation rows per source model.
- Claim scope: `offline_dfl_action_target_v4_strict_lp_gate_not_full_dfl`.

Latest action-target run:

- Dagster run id: `54f1e320-b046-4aab-9d07-ff9c73714622`.
- Output rows: 900, covering two source models, five tenants, 18
  final-holdout timestamps, and five evaluated candidates per source model:
  strict control, raw source, panel v2, decision-target v3, and action-target
  v4.
- Local summary:
  `data/research_runs/week3_offline_dfl_action_target_v4_90/action_target_v4_summary.json`.
- API sanity check for `client_003_dnipro_factory`: `data_quality_tier=thesis_grade`,
  `anchor_count=90`, and `model_count=3`.

Strict-gate result:

| Source model | Raw mean regret | Panel v2 mean regret | V3 mean regret | V4 mean regret | Strict control mean regret | V4 improvement vs raw | V4 improvement vs v3 | V4 improvement vs strict | Decision |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 813.40 | 816.62 | 814.17 | 851.99 | 314.81 | -4.74% | -4.65% | -170.64% | blocked |
| `tft_silver_v0` | 1003.54 | 989.55 | 1015.36 | 959.84 | 314.81 | 4.35% | 5.47% | -204.89% | blocked |

The finding is conservative and useful. TFT v4 improves versus raw TFT, panel
v2, and v3, which suggests action-rank emphasis is a more relevant direction
than affine correction alone. But both source models still lose decisively to
`strict_similar_day`, and their median regrets remain worse than the frozen
control. The promotion gate therefore stays blocked.

Tracked note:
[OFFLINE_DFL_ACTION_TARGET_V4.md](OFFLINE_DFL_ACTION_TARGET_V4.md).

## UA-First DFL Data Expansion And Action Labels

The next foundation slice responds to the v4 result by improving the training
substrate before adding another candidate. The current evidence suggests the
main bottleneck is not neural complexity but in-domain coverage and decision
labels that point directly at strict LP/oracle actions.

Implementation:

- New assets: `dfl_data_coverage_audit_frame` and
  `dfl_action_label_panel_frame`.
- Dagster group: `gold_dfl_training`.
- Run config:
  [../../configs/real_data_dfl_data_expansion_week3.yaml](../../configs/real_data_dfl_data_expansion_week3.yaml).
- Observed Ukrainian window: `2026-01-01` to `2026-04-30`.
- Benchmark cap: `max_anchors=120`; the audit reports the true tenant-specific
  coverage ceiling rather than assuming expansion is possible.
- Action-label split: latest 18 anchors per tenant/model are marked
  `final_holdout`; all earlier anchors are `train_selection`.
- Claim scope: `dfl_action_label_panel_not_full_dfl`.

The action-label panel joins source-model rows with the frozen
`strict_similar_day` control and recomputes oracle LP dispatch from realized
horizon prices. It persists forecast vectors, actual vectors, strict-control
dispatch, oracle dispatch/SOC vectors, action masks, regret, throughput,
degradation proxy, observed coverage, and explicit `not_full_dfl=true` /
`not_market_execution=true` flags.

Verified evidence on 2026-05-07:

- Dagster run `3743f42c-8cc6-4822-a3f0-7730af6af458` materialized
  `dfl_data_coverage_audit_frame` and `dfl_action_label_panel_frame`, then ran
  `dfl_action_label_panel_readiness_evidence` successfully.
- The coverage audit found 104 eligible daily anchors per canonical tenant,
  meeting the 90-anchor target while recording one price/weather source gap per
  tenant in the wider feature frame.
- The action-label panel persisted 1,040 rows in
  `dfl_action_label_vectors`: 5 tenants x 2 source models x 104 anchors.
- Each tenant/model split has 86 `train_selection` rows and 18
  `final_holdout` rows, with the final holdout covering
  `2026-04-12 23:00` through `2026-04-29 23:00`.
- Vector-length validation found 0 bad rows, and all persisted action-label
  rows are observed coverage, `thesis_grade`, `not_full_dfl=true`, and
  `not_market_execution=true`.
- Local export slug:
  `data/research_runs/week3_dfl_data_expansion_ua_panel`.
- Dataset card:
  [DFL_ACTION_LABEL_DATASET_CARD.md](DFL_ACTION_LABEL_DATASET_CARD.md).

European datasets remain a research-only bridge. ENTSO-E, Open Power System
Data, Ember, and Nord Pool are useful for future external validation and
market-coupling context, but they are not training inputs until currency,
timezone, price-cap, market-rule, API/licensing, and domain-shift normalization
questions are resolved.

Tracked note:
[DFL_DATA_EXPANSION_AND_ACTION_LABELS.md](DFL_DATA_EXPANSION_AND_ACTION_LABELS.md).

## Supervised DFL Action Classifier Baseline

The next foundation slice materialized the first supervised action-label
baseline over the checked Ukrainian DFL action-label panel. It is deliberately
small and interpretable: no new dependency, no neural training loop, no Decision
Transformer expansion, and no public API/dashboard contract change.

Implementation:

- New asset: `dfl_action_classifier_baseline_frame`.
- Dagster group: `gold_dfl_training`.
- Upstream asset: `dfl_action_label_panel_frame`.
- Baseline name: `dfl_action_classifier_v0`.
- Claim scope: `dfl_action_classifier_baseline_not_full_dfl`.
- Promotion status: `blocked_classification_only_no_strict_lp_value`.

Latest run:

- Dagster run id: `91fe584d-73f9-41ca-b3e9-88288136b8b7`.
- Training scope: 860 `train_selection` rows, or 20,640 labeled horizon-hours.
- Final holdout: 180 rows, or 4,320 labeled horizon-hours, across five tenants
  and two source models.
- Final-holdout all-source accuracy: 0.6495.
- Final-holdout all-source macro F1: 0.5364.
- Per-model final-holdout accuracy: `tft_silver_v0` 0.6685,
  `nbeatsx_silver_v0` 0.6306.

Follow-up strict LP projection:

- New asset: `dfl_action_classifier_strict_lp_benchmark_frame`.
- Dagster run id: `97cac49e-b3f8-4829-b687-b4b5f3470d07`.
- Strategy kind: `dfl_action_classifier_strict_lp_projection`.
- Final-holdout rows: 360 total; 180 strict-control rows and 90 rows per
  classifier source model.
- Anchor range: `2026-04-12 23:00` to `2026-04-29 23:00`.
- Claim flags: `not_full_dfl=true`, `not_market_execution=true`.
- Leakage check: `uses_final_holdout_for_training=false` for all projected
  classifier rows.

Strict LP/oracle result:

- `strict_similar_day`: 314.81 UAH mean regret, 202.61 UAH median regret.
- `dfl_action_classifier_v0_tft_silver_v0`: 1,157.40 UAH mean regret,
  715.66 UAH median regret.
- `dfl_action_classifier_v0_nbeatsx_silver_v0`: 1,186.83 UAH mean regret,
  1,054.08 UAH median regret.

This closes the classifier baseline slice honestly: the action labels can be
projected into feasible dispatch, but the projected candidates lose badly to the
frozen `strict_similar_day` control. The result remains useful research
evidence for future DFL data/model design, not a promoted controller.

Value-aware follow-up:

- New asset: `dfl_value_aware_action_classifier_strict_lp_benchmark_frame`.
- Dagster run id: `6db74e0f-958d-46ec-8360-8f6a7494fd8f`.
- Strategy kind: `dfl_value_aware_action_classifier_strict_lp_projection`.
- Weighting rule: `1 + (candidate_regret_gap + strict_opportunity) / 500`,
  computed from `train_selection` rows only.
- Final-holdout rows: 360 total; 180 strict-control rows and 90 rows per
  value-aware classifier source model.
- Claim flags: `not_full_dfl=true`, `not_market_execution=true`.
- Leakage check: `uses_final_holdout_for_training=false` for all projected
  classifier rows.

Strict LP/oracle result:

- `strict_similar_day`: 314.81 UAH mean regret, 202.61 UAH median regret.
- `dfl_value_aware_action_classifier_v1_tft_silver_v0`: 1,198.74 UAH mean
  regret, 975.43 UAH median regret.
- `dfl_value_aware_action_classifier_v1_nbeatsx_silver_v0`: 1,498.95 UAH mean
  regret, 1,341.77 UAH median regret.

This blocks the value-aware variant too. The result is technically useful:
weighted action-label voting does not solve the decision-value gap, so the next
DFL slice should either expand Ukrainian in-domain coverage or move from
per-hour action classification toward trajectory/value learning.

Tracked note:
[DFL_ACTION_CLASSIFIER_BASELINE.md](DFL_ACTION_CLASSIFIER_BASELINE.md).

## DFL Classifier Failure Analysis And Data Recovery

The next diagnostics slice formalizes the negative classifier result as useful
evidence. Both the plain and value-aware action classifiers are feasible and
no-leakage, but they lose decisively to the frozen `strict_similar_day` control
under strict LP/oracle regret.

Implementation:

- New helper: `smart_arbitrage.dfl.failure_analysis`.
- New asset: `dfl_action_classifier_failure_analysis_frame`.
- New asset check: `dfl_action_classifier_failure_analysis_evidence`.
- Dagster group: `gold_dfl_training`.
- Claim scope: `dfl_action_classifier_failure_analysis_not_full_dfl`.
- Latest run: Dagster run id `9a3eb772-dbd5-4023-beff-ed8f5a69e326`
  on 2026-05-08; the failure-analysis asset check passed.

The asset summarizes regret-weighted confusion, active-hour precision/recall,
missed high-value charge/discharge hours, false active actions, top/bottom
price-rank misses, SOC-path value loss, and plain-versus-value-aware regret
comparison. The check requires all five tenants, both source models, 90
final-holdout tenant-anchors per source model, thesis-grade observed coverage,
no split leakage, and conservative claim flags.

Research interpretation:

- DFL and SPO sources support optimizing downstream decision loss rather than
  classification or forecast-only proxies.
- Multistage energy-storage DFL sources explain why SOC path dependence makes
  independent hourly labels weak.
- Imitation-learning literature explains why behavior cloning can compound
  action errors in sequential settings.
- BESS forecast-economics sources support price extrema, spread, and realized
  dispatch value metrics.

Data recovery:

- Ukrainian OREE/Open-Meteo remains the training source of truth.
- `RunyaoYu/PriceFM` is include/watch for future European external validation:
  Hugging Face Dataset Viewer currently reports 140,257 rows, 15-minute
  timestamps, and European price/load/generation columns.
- `lipiecki/thief` remains watch: the THieF paper is relevant, but Dataset
  Viewer is currently unavailable.
- ENTSO-E, OPSD, Ember, and Nord Pool remain research-only bridge sources with
  `training_use_allowed=false`.

Tracked notes:
[DFL_CLASSIFIER_FAILURE_ANALYSIS.md](DFL_CLASSIFIER_FAILURE_ANALYSIS.md) and
[DFL_DATA_RECOVERY_ROADMAP.md](DFL_DATA_RECOVERY_ROADMAP.md).

## Trajectory/Value Selector v1

The follow-up slice moved from hourly classification to selection among
feasible strict-LP-scored schedules. This is closer to decision-focused
learning because it ranks complete dispatch trajectories by prior regret, but
it is still a selector diagnostic, not full DFL and not market execution.

Implementation:

- New helper: `smart_arbitrage.dfl.trajectory_value`.
- New assets: `dfl_trajectory_value_candidate_panel_frame`,
  `dfl_trajectory_value_selector_frame`, and
  `dfl_trajectory_value_selector_strict_lp_benchmark_frame`.
- Dagster group: `gold_dfl_training`.
- Strategy kind: `dfl_trajectory_value_selector_strict_lp_benchmark`.
- Run config:
  [../../configs/real_data_dfl_trajectory_value_week3.yaml](../../configs/real_data_dfl_trajectory_value_week3.yaml).
- Selector rule: choose the schedule family with the lowest prior/train-selection
  regret per tenant/source model; final-holdout rows are not used for
  selection.

Coverage finding:

- The refreshed audit targeted 120 anchors per tenant.
- Current observed OREE/Open-Meteo materialized evidence still ceilings at 104
  eligible anchors per tenant.
- Eligible anchor window: `2026-01-08 23:00` through `2026-04-29 23:00`.
- One price gap and one weather gap remain visible per tenant in the wider
  feature frame.

Latest strict selector result:

| Source model | Raw mean regret | Selector mean regret | Strict control mean regret | Selector improvement vs raw | Decision |
|---|---:|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 813.40 | 603.29 | 314.81 | 25.83% | development diagnostic, production blocked |
| `tft_silver_v0` | 1003.54 | 619.78 | 314.81 | 38.24% | development diagnostic, production blocked |

This is the strongest DFL-adjacent evidence so far because it improves over raw
neural schedules without using final-holdout selection. It still does not beat
the frozen `strict_similar_day` control, and median regret remains worse than
strict control, so the promotion gate stays blocked.

Tracked note:
[DFL_TRAJECTORY_VALUE_SELECTOR.md](DFL_TRAJECTORY_VALUE_SELECTOR.md).

## Trajectory Feature Ranker v1

The next slice expanded the feasible schedule evidence into a larger candidate
library and a prior-only linear feature ranker. This is still DFL-lite
trajectory/value evidence, not full differentiable training, not Decision
Transformer control, and not market execution.

Implementation:

- New helper: `smart_arbitrage.dfl.trajectory_ranker`.
- New assets: `dfl_schedule_candidate_library_frame`,
  `dfl_trajectory_feature_ranker_frame`, and
  `dfl_trajectory_feature_ranker_strict_lp_benchmark_frame`.
- Dagster group: `gold_dfl_training`.
- Strategy kind: `dfl_trajectory_feature_ranker_strict_lp_benchmark`.
- Run config:
  [../../configs/real_data_dfl_trajectory_ranker_week3.yaml](../../configs/real_data_dfl_trajectory_ranker_week3.yaml).
- Ranker rule: grid-search a small linear scoring rule over feasible schedule
  features using train-selection anchors only. Final-holdout actuals affect
  strict scoring only.

Latest run:

- Full upstream materialization attempt exceeded the local 30-minute command
  timeout; downstream ranker assets then materialized successfully from the
  existing checked 104-anchor upstream benchmark and trajectory/value assets.
- Dagster run id: `db2f6e2d-ae39-49fe-86f0-0e594af29a1e`.
- Export directory:
  `data/research_runs/week3_dfl_trajectory_feature_ranker_v1`.
- Schedule library: 6,780 rows.
- Ranker selection frame: 10 rows.
- Strict benchmark frame: 540 rows.
- Final-holdout range: `2026-04-12 23:00` to `2026-04-29 23:00`.
- Claim flags: `not_full_dfl=true`, `not_market_execution=true`.

Strict LP/oracle result:

| Model | Tenant-anchor rows | Mean regret UAH | Median regret UAH | Finding |
|---|---:|---:|---:|---|
| `strict_similar_day` | 90 reference anchors per source model | 314.81 | 202.61 | Frozen Level 1 control still wins. |
| `nbeatsx_silver_v0` | 90 | 813.40 | 520.48 | Raw neural comparator. |
| `dfl_trajectory_feature_ranker_v1_nbeatsx_silver_v0` | 90 | 497.30 | 238.15 | Improves vs raw by 38.86%, blocked vs strict. |
| `tft_silver_v0` | 90 | 1003.54 | 477.99 | Raw neural comparator. |
| `dfl_trajectory_feature_ranker_v1_tft_silver_v0` | 90 | 607.96 | 218.72 | Improves vs raw by 39.42%, blocked vs strict. |

This is the strongest DFL-adjacent selector evidence so far because it improves
over both raw neural schedules by roughly 39% while preserving prior-only
selection. It still does not beat the frozen `strict_similar_day` mean or median
regret, so production promotion remains blocked.

Hugging Face source refresh:

- `RunyaoYu/PriceFM` remains include/watch for future European external
  validation. Dataset Viewer is valid with 140,257 rows, 191 columns, one train
  split, 15-minute UTC timestamps, and European price/load/generation columns.
- `lipiecki/thief` remains watch-only because Dataset Viewer is unavailable.
- External European datasets remain `training_use_allowed=false`; Ukrainian
  OREE/Open-Meteo stays the only training source for this slice.

Tracked note:
[DFL_TRAJECTORY_FEATURE_RANKER.md](DFL_TRAJECTORY_FEATURE_RANKER.md).

## Strict Challenger Diagnostics

The next slice turns the trajectory feature-ranker result into a falsifiable
diagnostic: before training another selector, prove whether the candidate
library contains any non-strict schedule that could beat the frozen
`strict_similar_day` control.

Implementation:

- New helper: `smart_arbitrage.dfl.strict_challenger`.
- New assets: `dfl_pipeline_integrity_audit_frame`,
  `dfl_schedule_candidate_library_v2_frame`,
  `dfl_non_strict_oracle_upper_bound_frame`, and
  `dfl_strict_baseline_autopsy_frame`.
- New asset check: `dfl_non_strict_oracle_upper_bound_evidence`.
- Dagster group: `gold_dfl_training`.
- Run config:
  [../../configs/real_data_dfl_strict_challenger_week3.yaml](../../configs/real_data_dfl_strict_challenger_week3.yaml).

The diagnostic separates candidate-set adequacy from selector learnability.
`dfl_non_strict_oracle_upper_bound_frame` selects the best final-holdout
non-strict candidate per tenant/source/anchor. If that upper bound still loses
to `strict_similar_day`, the next work is UA data recovery or richer candidate
generation, not another learner. If it wins on meaningful slices, the next model
should be a prior-only strict-failure selector.

Latest run:

- Dagster run id: `48b9c0b4-9d12-4237-a436-549424956ac1`.
- Scope: downstream-only materialization from the existing checked 104-anchor
  upstream benchmark and schedule library.
- Asset check: `dfl_non_strict_oracle_upper_bound_evidence` passed.
- Pipeline audit: 1,560 benchmark rows, 6,780 existing candidate rows, 104
  market anchors, 520 tenant anchors, zero leaky horizon rows, and zero ranker
  feature overlap with actual-derived diagnostics.
- Candidate library v2: 10,910 rows after adding strict/raw blends and
  prior-only strict residual candidates.
- Non-strict upper bound: 180 final-holdout tenant/source/anchor rows; 185.74
  UAH best non-strict mean regret versus 314.81 UAH strict-control mean regret;
  non-strict schedules beat strict on 146 of 180 rows.
- Autopsy: 46 strict high-regret rows; 146 rows recommend training a prior-only
  strict-failure selector; zero rows recommend data/candidate expansion first.

Interpretation: the candidate set is no longer the immediate blocker. The next
modeling slice should learn when to distrust the frozen strict control using
prior-only evidence, then re-score the selected strategy under the same strict
LP/oracle gate.

This remains research evidence only: not full DFL, not Decision Transformer
control, and not market execution. Production promotion still requires beating
`strict_similar_day` under the conservative strict LP/oracle gate.

Tracked note:
[DFL_STRICT_CHALLENGER_DIAGNOSTICS.md](DFL_STRICT_CHALLENGER_DIAGNOSTICS.md).

## Strict-Failure Selector v1

The strict-challenger diagnostic showed that the candidate library is not the
immediate blocker: the best non-strict feasible schedules can beat
`strict_similar_day` on many final-holdout anchors. This slice therefore adds a
prior-only selector that tries to learn when strict control is likely to fail.

Implementation:

- New helper: `smart_arbitrage.dfl.strict_failure_selector`.
- New assets: `dfl_strict_failure_selector_frame` and
  `dfl_strict_failure_selector_strict_lp_benchmark_frame`.
- New asset check: `dfl_strict_failure_selector_evidence`.
- Dagster group: `gold_dfl_training`.
- Strategy kind: `dfl_strict_failure_selector_strict_lp_benchmark`.
- Run config:
  [../../configs/real_data_dfl_strict_failure_selector_week3.yaml](../../configs/real_data_dfl_strict_failure_selector_week3.yaml).

Selector rule:

- For each tenant/source model, compute prior mean regret for strict control and
  non-strict candidate families using train-selection anchors only.
- Choose a switch threshold from `0, 50, 100, 200, 400` UAH on train-selection
  anchors only.
- On final holdout, switch from `strict_similar_day` to the best prior
  non-strict candidate only when the prior advantage crosses that threshold.
- Final-holdout actuals affect strict scoring only; they cannot change the
  selected threshold or candidate metadata.

The gate remains conservative. Development evidence can pass if the selector
improves over raw neural schedules, but production promotion remains blocked
unless it improves mean regret by at least 5% versus `strict_similar_day` and
does not worsen median regret.

Current status:

- Helper tests cover prior-only threshold selection, final-holdout mutation
  safety, strict/raw/selector coverage, and promotion-gate block/pass cases.
- Dagster asset registration and asset-check registration are covered.
- Latest run: Dagster run id `568a8a8d-c210-44d0-9842-08300dfe0781`; the
  `dfl_strict_failure_selector_evidence` asset check passed.
- Strict benchmark frame: 720 rows, with 90 final-holdout selector rows per
  source model.
- `dfl_strict_failure_selector_v1_tft_silver_v0`: 267.79 UAH mean regret and
  149.01 UAH median regret, improving 73.32% versus raw TFT and 14.94% versus
  `strict_similar_day`.
- `dfl_strict_failure_selector_v1_nbeatsx_silver_v0`: 299.73 UAH mean regret
  and 182.76 UAH median regret, improving 63.15% versus raw NBEATSx and 4.79%
  versus `strict_similar_day`.
- Decision: development evidence passes. The TFT-source selector passes the
  per-source strict threshold, but the overall multi-source gate remains
  conservatively labeled `diagnostic_pass_production_blocked` because NBEATSx is
  just below the 5% strict-improvement threshold.
- Full evidence table: [DFL_STRICT_FAILURE_SELECTOR.md](DFL_STRICT_FAILURE_SELECTOR.md).

This remains research evidence only: not full DFL, not Decision Transformer
control, and not market execution.

## Strict-Failure Selector Robustness Gate

The strict-failure selector result is the first source-specific strict-gate
breakthrough, but it is not promoted yet. The robustness slice tests whether
the result survives earlier temporal windows and tenant slices.

Implementation:

- New helper: `smart_arbitrage.dfl.strict_failure_robustness`.
- New asset: `dfl_strict_failure_selector_robustness_frame`.
- New asset check: `dfl_strict_failure_selector_robustness_evidence`.
- Dagster group: `gold_dfl_training`.
- Run config:
  [../../configs/real_data_dfl_strict_failure_selector_robustness_week3.yaml](../../configs/real_data_dfl_strict_failure_selector_robustness_week3.yaml).

Robustness protocol:

- Use the current checked 104-anchor Ukrainian panel.
- Generate four latest-first validation windows of 18 anchors each.
- Require at least 30 prior anchors before each validation window.
- Select thresholds using anchors strictly before each validation-window start.
- Let validation actuals affect scoring only, never threshold selection.

Gate labels:

- `development_pass`: improves over raw neural schedules.
- `source_specific_strict_pass`: beats `strict_similar_day` by at least 5%
  mean regret and does not worsen median regret in a window.
- `robust_research_challenger`: same source passes in the latest window and at
  least three of four rolling windows.
- `production_promote`: blocked in this slice.

Latest run:

- Dagster run id: `fd21fada-f453-404b-96a1-27d99b14b1a1`.
- Asset check: `dfl_strict_failure_selector_robustness_evidence` passed.
- Robustness frame: 8 rows, two source models x four rolling windows.
- Coverage: 90 validation tenant-anchors per source model per window.
- Result: every window improves over the raw neural schedule.
- Result: no source earns `robust_research_challenger`. TFT passes the strict
  threshold only in the latest window; earlier windows are development evidence
  but not strict-control wins.
- Decision: production promotion remains blocked. The next work should improve
  prior-window features or extend Ukrainian observed coverage before promoting
  any selector.

Tracked note:
[DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS.md](DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS.md).

## Strict-Failure Prior Feature Audit

The robustness gate showed that the selector is useful but not robust versus
`strict_similar_day`. The feature-audit slice adds explanatory context before
changing selector behavior.

Implementation:

- New historical context asset: `tenant_historical_net_load_silver`.
- New helper: `smart_arbitrage.dfl.strict_failure_features`.
- New assets: `dfl_strict_failure_prior_feature_panel_frame` and
  `dfl_strict_failure_feature_audit_frame`.
- New asset check: `dfl_strict_failure_feature_audit_evidence`.
- Dagster group: `gold_dfl_training`.
- Run config:
  [../../configs/real_data_dfl_strict_failure_feature_audit_week3.yaml](../../configs/real_data_dfl_strict_failure_feature_audit_week3.yaml).

Feature protocol:

- `selector_feature_*` columns use only anchors strictly before the validation
  window start.
- `analysis_only_*` columns hold validation outcomes and may not be used for
  selector decisions.
- Historical tenant load is a configured proxy, not measured telemetry.

Latest run:

- Dagster run id: `b9a48061-079f-4a92-9daf-699398f67906`.
- Asset check: `dfl_strict_failure_feature_audit_evidence` passed.
- Historical load proxy: 14,395 rows across five tenants from `2026-01-01` to
  `2026-04-30`.
- Feature panel: 720 rows.
- Audit panel: 40 rows.
- Cluster result: 30 `strict_stable_region`, 6 `high_spread_volatility`, and
  4 `strict_failure_captured` rows.
- Source summary: NBEATSx improves 40.02% versus raw schedules but only 1.54%
  versus strict control on average; TFT improves 43.07% versus raw schedules
  but is 1.60% worse than strict control on average.
- Decision: production promotion remains blocked. The next selector experiment
  should enrich prior-only switching rules with price regime, spread
  volatility, rank stability, calendar/weather/load context, and tenant
  failure clusters.

Tracked note:
[DFL_STRICT_FAILURE_FEATURE_AUDIT.md](DFL_STRICT_FAILURE_FEATURE_AUDIT.md).

## Feature-Aware Strict-Failure Selector

The feature-audit slice explained why the v1 selector only beat
`strict_similar_day` in the latest TFT window. The follow-up adds a deterministic
feature-aware selector without changing the existing v1 selector behavior.

Implementation:

- New helper: `smart_arbitrage.dfl.strict_failure_feature_selector`.
- New assets: `dfl_feature_aware_strict_failure_selector_frame` and
  `dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame`.
- New asset check: `dfl_feature_aware_strict_failure_selector_evidence`.
- Dagster group: `gold_dfl_training`.
- Run config:
  [../../configs/real_data_dfl_feature_aware_strict_failure_selector_week3.yaml](../../configs/real_data_dfl_feature_aware_strict_failure_selector_week3.yaml).

Protocol:

- Rule selection uses only earlier rolling windows (`2-4`).
- Latest window (`1`) actuals affect strict LP/oracle scoring only.
- Selector features include prior regret advantage, price regime, top/bottom
  rank stability, and spread-volatility regime.
- The strict benchmark compares `strict_similar_day`, raw source schedules,
  best-prior non-strict schedules, and the feature-aware selector.
- Production promotion remains blocked unless the conservative strict LP/oracle
  gate clears.

Latest run:

- Dagster run id: `1cb76f8c-e321-4178-b54a-f85cd15838b6`.
- Asset check: `dfl_feature_aware_strict_failure_selector_evidence` passed.
- Selector frame: 10 rows, five tenants x two source models.
- Strict benchmark: 720 rows, with 90 selector final-holdout tenant-anchors per
  source model from `2026-04-12 23:00` through `2026-04-29 23:00`.
- `dfl_feature_aware_strict_failure_selector_v2_nbeatsx_silver_v0`: 299.73 UAH
  mean regret and 182.76 UAH median regret, improving 63.15% versus raw NBEATSx
  and 4.79% versus `strict_similar_day`.
- `dfl_feature_aware_strict_failure_selector_v2_tft_silver_v0`: 299.19 UAH mean
  regret and 160.52 UAH median regret, improving 70.19% versus raw TFT and
  4.96% versus `strict_similar_day`.
- Decision: the selector is still useful development evidence, but it remains
  blocked because neither source clears the conservative 5% strict-control
  threshold.

Tracked note:
[DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR.md](DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR.md).

## Forecast AFL Hardening

The feature-aware selector almost cleared the strict-control threshold, but the
next blocker is forecast substrate quality. `nbeatsx_silver_v0` and
`tft_silver_v0` are compact in-repo research candidates, not completed official
NBEATSx/TFT training runs. The official adapter assets remain readiness paths
until a tracked `sota` run is executed and scored through the same strict
LP/oracle protocol.

Implementation:

- rolling-origin benchmark candidate generation now uses forecast-available
  weather mode for NBEATSx/TFT features;
- new asset `forecast_candidate_forensics_frame` labels frozen control,
  compact Silver candidates, and official-backend readiness candidates;
- new asset `afl_training_panel_frame` creates **Arbitrage-Focused Learning
  (AFL)** rows with prior-only forecast features and realized decision-value
  labels.

Config:
[../../configs/real_data_afl_hardening_week3.yaml](../../configs/real_data_afl_hardening_week3.yaml).

Tracked note:
[DFL_FORECAST_AFL_HARDENING.md](DFL_FORECAST_AFL_HARDENING.md).

## AFE Semantic Event Context

The next feature-governance slice separates **AFE** from **AFL**:

- AFE is feature cataloging, temporal availability, and leakage policy.
- AFL is the arbitrage-focused forecast-learning panel with prior-only features
  and realized decision-value labels.
- DFL remains the strict LP/oracle promotion path.

Implementation:

- New helper: `smart_arbitrage.forecasting.afe`.
- New helper: `smart_arbitrage.dfl.semantic_event_failure_audit`.
- New assets: `forecast_afe_feature_catalog_frame` and
  `dfl_semantic_event_strict_failure_audit_frame`.
- New asset check: `dfl_semantic_event_strict_failure_audit_evidence`.
- Run config:
  [../../configs/real_data_afe_semantic_event_context_week3.yaml](../../configs/real_data_afe_semantic_event_context_week3.yaml).

Protocol:

- `ukrenergo_grid_events_bronze` remains the only semantic source in this slice.
- The semantic audit reuses the `grid_event_signal_silver` feature builder
  against real-data benchmark timestamps and keeps the same
  `published_at <= timestamp` rule.
- The semantic audit explains strict-control failure windows; it does not change
  selector decisions yet.
- European bridge rows remain `training_use_allowed=false`.
- No broad scraped-news ingestion and no LLM event extraction are introduced.

Materialized result, 2026-05-08:

- Dagster materialized the AFE catalog and semantic strict-failure audit with
  the Week 3 config.
- `dfl_semantic_event_strict_failure_audit_evidence` passed.
- The audit produced 10 rows across 5 tenants, 2 source models, and 180
  validation tenant-anchors.
- The current public Ukrenergo Telegram scrape matched 0 semantic event anchors
  in the January-April 2026 benchmark window, while the strict-control failure
  count was 44 anchors. This makes semantic event context a governed future
  feature path, not an active explanation for the current strict selector
  pattern.

Tracked notes:
[AFE_TO_AFL_TO_DFL_ROADMAP.md](AFE_TO_AFL_TO_DFL_ROADMAP.md) and
[AFE_SEMANTIC_EVENT_CONTEXT.md](AFE_SEMANTIC_EVENT_CONTEXT.md).

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

Tracked promotion gate:
[DFL_PROMOTION_GATE.md](DFL_PROMOTION_GATE.md).

Freeze summary:

- `strict_similar_day` remains the Level 1 control comparator.
- Forecast candidates must be evaluated through the same LP/oracle protocol.
- Week 3 30-anchor Dnipro evidence, Dnipro 90-anchor preview evidence, and
  all-tenant diagnostic snapshots must stay separately labeled.
- Current offline DFL v0 is negative diagnostic evidence and must not be
  promoted.
- PriceFM and THieF support future forecast-layer direction; TSFM leakage and
  the DFL survey support the current no-leakage, decision-value-first protocol.
