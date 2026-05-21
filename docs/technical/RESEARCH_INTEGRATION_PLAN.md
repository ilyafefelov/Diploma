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

6. **V2+-anchored LAVA teacher-label bridge before raw-action DT**
   - Treat frozen Ukrainian-only V2+ as the comparator until a stronger
     candidate beats it under the same strict LP/oracle gate.
   - Keep the Poland ranker result as negative evidence: Poland features work
     technically, but the simple ranker overreached on risky schedules.
   - Build teacher labels and feasible schedule-neighbor candidates first:
     V2+ fallback, strict fallback, Poland/TFT near-miss schedules, and
     train-only oracle-neighborhood diagnostics.
   - Train a conservative candidate-level LAVA scorer before attempting another
     DT run. The model may use train/prior labels only and must fall back to V2+
     when prior evidence is weak.
   - Keep ENTSO-E/Poland rows out of Ukrainian target training; Poland remains
     an exogenous feature lane with `market_execution_enabled=false`.

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
`strict_similar_day` control. Offline Strategy Promotion remains blocked until a later
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
the frozen strict control under strict LP/oracle scoring. Offline Strategy Promotion
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
regret, so Offline Strategy Promotion remains blocked.

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
control, and not market execution. Offline Strategy Promotion still requires beating
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
improves over raw neural schedules, but Offline Strategy Promotion remains blocked
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
- Decision: Offline Strategy Promotion remains blocked. The next work should improve
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
- Decision: Offline Strategy Promotion remains blocked. The next selector experiment
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
- Offline Strategy Promotion remains blocked unless the conservative strict LP/oracle
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

## AFL Forecast Error Audit

The next AFL slice classifies compact NBEATSx/TFT forecast failures before
official training or DFL loss work.

Implementation:

- New helper: `smart_arbitrage.forecasting.afl_error_audit`.
- New asset: `afl_forecast_error_audit_frame`.
- New asset check: `afl_forecast_error_audit_evidence`.
- Run config:
  [../../configs/real_data_afl_forecast_error_audit_week3.yaml](../../configs/real_data_afl_forecast_error_audit_week3.yaml).
- Official training readiness config:
  [../../configs/real_data_official_forecast_training_readiness_week3.yaml](../../configs/real_data_official_forecast_training_readiness_week3.yaml).

Protocol:

- Use `forecast_candidate_forensics_frame` and `afl_training_panel_frame`.
- Diagnose spread-shape, rank/extrema, LP-value, and weather/load context gaps.
- Keep `selector_feature_columns_csv` free of realized `label_*` columns.
- Do not train full DFL, expand Decision Transformer control, or promote any
  candidate without strict LP/oracle evidence.

Tracked note:
[DFL_AFL_FORECAST_ERROR_AUDIT.md](DFL_AFL_FORECAST_ERROR_AUDIT.md).

Materialized result, 2026-05-09:

- Downstream AFL audit materialized from the latest stored benchmark frame, and
  `afl_forecast_error_audit_evidence` passed.
- The audit covered 20 rows, 5 tenants, 2 compact source models, and 1,560 AFL
  panel rows.
- Mean LP-value failure is 80.23%, mean rank/extrema failure is 64.83%, and
  mean spread-shape failure is 55.19%.
- Weather/load context is now present as prior-only AFL features. The audit no
  longer reports missing context as the blocker; the blocker is decision-value
  and rank/spread quality.
- Actual-dependent top/bottom rank overlap has been moved out of selector
  features into `diagnostic_forecast_top3_bottom3_rank_overlap`.

## Official Forecast Strict Scoring And DFL v1

The path from AFL audit to real DFL v1 is now implemented as a controlled,
research-only sequence:

- Official training readiness config:
  [../../configs/real_data_official_forecast_training_readiness_week3.yaml](../../configs/real_data_official_forecast_training_readiness_week3.yaml).
- Official NBEATSx/TFT adapters keep smoke defaults in code, while the tracked
  config runs CPU-safe serious settings: NBEATSx `max_steps=100` and TFT
  `max_epochs=15`.
- `official_forecast_strict_lp_benchmark_frame` materialized in run
  `68d74ecb-2d5c-49d5-b25e-99b06ec4b3ba`.
- Single-horizon readiness result: strict control mean regret 1,903.90 UAH,
  official TFT 2,540.37 UAH, official NBEATSx 6,008.01 UAH. This proves the
  official adapters can be strict-scored, but it does not support promotion.

DFL v1 implementation:

- New decision-loss helper:
  `smart_arbitrage.dfl.decision_loss.compute_decision_loss_v1`.
- New assets: `dfl_forecast_dfl_v1_panel_frame` and
  `dfl_forecast_dfl_v1_strict_lp_benchmark_frame`.
- Config:
  [../../configs/real_data_dfl_forecast_v1_week3.yaml](../../configs/real_data_dfl_forecast_v1_week3.yaml).
- Latest run id: `1fc1cc96-92b9-470c-b29a-f416a3ee3b08`.
- Result: the relaxed storage layer now uses scaled cvxpylayer solves, a
  bounded surrogate fallback, strict-vs-relaxed fixture checks, and optimizer
  guards. The final panel status was
  `cvxpylayer_scaled;training_guard:non_finite_gradient;cvxpylayer_scaled`.
  Checkpoint epoch reached `4`, but DFL v1 strict results still matched raw
  forecasts: NBEATSx 1,121.04 UAH mean regret, TFT 1,665.41 UAH mean regret,
  strict control 314.81 UAH.
- Tracked note:
  [DFL_FORECAST_DECISION_LOSS_V1.md](DFL_FORECAST_DECISION_LOSS_V1.md).

Decision: the project has reached the next DFL foundation boundary. The relaxed
storage layer is stable enough to run DFL v1, but the horizon-bias learning
target does not improve strict decision value. The next engineering task is not
a Decision Transformer; it is a richer schedule/value learning target evaluated
through the same strict LP/oracle gate.

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

## Residual DFL + Offline DT Research Challenger

The next clean DFL step is now a strict-default research challenger instead of a
forced model promotion. It reuses the verified Ukrainian observed panel and adds
a real-data trajectory dataset, a residual schedule/value selector, a tiny
offline Decision Transformer candidate, filtered behavior cloning, and a
fallback wrapper.

Implementation:

- New asset: `dfl_real_data_trajectory_dataset_frame`.
- New assets: `dfl_residual_schedule_value_model_frame` and
  `dfl_residual_schedule_value_strict_lp_benchmark_frame`.
- New assets: `dfl_offline_dt_candidate_frame` and
  `dfl_offline_dt_candidate_strict_lp_benchmark_frame`.
- New asset: `dfl_residual_dt_fallback_strict_lp_benchmark_frame`.
- New check:
  `dfl_residual_dt_fallback_strict_lp_benchmark_frame:dfl_residual_dt_fallback_evidence`.
- Run config:
  [../../configs/real_data_dfl_residual_dt_challenger_week3.yaml](../../configs/real_data_dfl_residual_dt_challenger_week3.yaml).

Protocol:

- Source data remains OREE DAM plus Open-Meteo/load context for the five
  canonical Ukrainian tenants.
- Teacher labels are train/inner only; final-holdout labels are scoring-only.
- The fallback defaults to `strict_similar_day` unless prior-only confidence
  says residual DFL or offline DT should be allowed.
- Promotion remains blocked unless strict LP/oracle scoring beats
  `strict_similar_day` by at least 5% mean regret with no median degradation.

Tracked note:
[DFL_RESIDUAL_DT_RESEARCH_CHALLENGER.md](DFL_RESIDUAL_DT_RESEARCH_CHALLENGER.md).

Materialized result:

- Run `54891d01-d57e-49a6-8191-9f3ea0afc425` materialized the residual/DT
  challenger assets and passed the structural fallback evidence check.
- Postgres now contains source-specific strict evidence rows: 540 residual
  strict rows, 540 offline-DT strict rows, and 900 fallback rows.
- TFT-source fallback is a research-challenger signal on the latest holdout
  (`258.12` UAH mean regret versus `314.81` UAH for `strict_similar_day`, with
  better median regret).
- NBEATSx-source fallback is still blocked (`318.37` UAH mean regret versus
  `314.81` UAH for `strict_similar_day`).
- Therefore the aggregate claim stays conservative: residual DFL/offline DT was
  tested as research evidence, but Offline Strategy Promotion remains blocked.

## Source-Specific Robust TFT Challenger Gate

The residual/DT result is now split by source model so a useful TFT signal does
not get hidden by the weaker NBEATSx path.

Implementation:

- New helper: `smart_arbitrage.dfl.source_specific_challenger`.
- New asset: `dfl_source_specific_research_challenger_frame`.
- New asset check: `dfl_source_specific_research_challenger_evidence`.
- Config:
  [../../configs/real_data_dfl_source_specific_challenger_week3.yaml](../../configs/real_data_dfl_source_specific_challenger_week3.yaml).
- Tracked note:
  [DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER.md](DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER.md).

Materialized result:

- Run `be22b25b-a1c5-40d9-9049-a01efb8e7e5f` finished successfully.
- The new source-specific asset check passed.
- TFT latest-holdout fallback: 258.12 UAH mean regret versus 314.81 UAH for
  `strict_similar_day`, an 18.01% improvement with better median regret.
- NBEATSx latest-holdout fallback: 318.37 UAH mean regret versus 314.81 UAH for
  `strict_similar_day`, so it remains blocked.
- Rolling strict-control passes are currently 0 of 4 for both sources in the
  combined gate. The correct label for TFT is therefore
  `latest_signal_not_robust`, not Offline Strategy Promotion.
- Existing check-scope note: `dnipro_thesis_grade_90_anchor_evidence` was built
  for an older Dnipro 90-anchor preview and reports `observed 104` on the
  current all-tenant panel. The source-specific evidence check is the relevant
  check for this run.

Decision: TFT can be called a source-specific research challenger on the latest
holdout, but not a robust controller. The next technical blocker is robustness
across earlier windows or more Ukrainian historical coverage, not another
Decision Transformer variant.

## Production Promotion Gate

The Offline Strategy Promotion slice turns the previous "always false" promotion
field into a real Dagster-visible decision state for offline/read-model
strategy evidence. It still does not enable market execution.

Implementation:

- New helper: `smart_arbitrage.dfl.production_promotion_gate`.
- New asset: `dfl_production_promotion_gate_frame`.
- New asset check: `dfl_production_promotion_gate_evidence`.
- Config:
  [../../configs/real_data_dfl_production_promotion_gate_week3.yaml](../../configs/real_data_dfl_production_promotion_gate_week3.yaml).
- Tracked note:
  [DFL_PRODUCTION_PROMOTION_GATE.md](DFL_PRODUCTION_PROMOTION_GATE.md).

Materialized result:

- Run `0cd165b5-1105-4cc1-a279-0e1144dd171b` finished successfully and
  materialized `dfl_production_promotion_gate_frame`.
- The new asset check did not pass, by design, because the backfill/coverage
  audit is not thesis-grade for the configured 180-anchor promotion target.
- Current observed ceiling: 104 eligible anchors per tenant, one missing price
  hour, one missing weather hour, observed coverage ratios of `0.9996527778`,
  and `data_quality_tier=coverage_gap`.
- TFT latest-holdout improvement remains visible at 18.01%, but rolling
  strict-control passes remain 0 of 4 and coverage expansion is unavailable.
- `production_promote=false` for every source/regime row and
  `market_execution_enabled=false` for every row.

Decision: the gate exists and can promote a future source/regime, but the
current evidence blocks promotion. `strict_similar_day` remains the offline
default fallback. The next route to promotion is either real Ukrainian history
recovery beyond 104 anchors or a prior-only regime gate that survives at least
3 of 4 rolling strict-control windows.

## UA Coverage Repair And Regime-Gated TFT Selector V2

The v2 slice tightened the route to Offline Strategy Promotion: first prove or repair
the Ukrainian coverage ceiling, then allow TFT only in prior-evidenced
strict-failure regimes. It does not add another DT/model variant.

Implementation:

- New helper: `smart_arbitrage.dfl.coverage_repair`.
- New helper: `smart_arbitrage.dfl.regime_gated_tft_selector`.
- New assets: `dfl_ua_coverage_repair_audit_frame`,
  `dfl_regime_gated_tft_selector_v2_frame`, and
  `dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame`.
- New asset check: `dfl_regime_gated_tft_selector_v2_evidence`.
- Config:
  [../../configs/real_data_dfl_regime_gated_tft_selector_v2_week3.yaml](../../configs/real_data_dfl_regime_gated_tft_selector_v2_week3.yaml).
- Tracked note:
  [DFL_REGIME_GATED_TFT_SELECTOR_V2.md](DFL_REGIME_GATED_TFT_SELECTOR_V2.md).

Materialized result:

- Run `1b901874-b713-4762-9154-2e822f91be8d` finished successfully and
  materialized the coverage repair audit plus v2 selector/strict benchmark.
- The exact unrecovered gap is `2026-03-29 23:00` for all five tenants, with
  `gap_kind=price_and_weather_gap` and
  `repair_status=not_recoverable_from_current_feature_frame`.
- The 180-anchor target remains unavailable: all five tenants have 104 eligible
  anchors and `data_quality_tier=coverage_gap`.
- V2 emitted 11 selector-rule rows and 2,880 strict LP/oracle rows.
- V2 selected `strict_similar_day` everywhere. Selector mean regret therefore
  matched strict at `710.445` UAH with median `442.848` UAH for both source
  models.
- The best prior non-strict references remain diagnostic only: NBEATSx source
  best reference mean regret was `696.899` UAH, while TFT source best reference
  mean regret was `736.743` UAH. The v2 gate did not allow these references to
  override strict because prior-regime evidence was insufficient or the regime
  was strict-stable.
- Offline Strategy Promotion gate rerun `e683a4b4-ce32-470b-8c61-71342ff23fa3` consumed v2
  evidence and still set `production_promote=false` for every row.

Decision: v2 made the blocker sharper. The current system should not be
promoted; the next improvement must either add source-backed Ukrainian coverage
or improve prior-only regime features enough to pass rolling strict-control
windows without using validation actuals for selection.

## Forecast Pipeline Truth Audit

The long-running DFL experiment loop starts with a truth audit rather than a new
model. The purpose is to prove whether compact NBEATSx/TFT and DFL failures are
real model failures or a lower-level forecast/scoring alignment problem.

Implementation:

- New helper: `smart_arbitrage.dfl.forecast_pipeline_truth`.
- New asset: `forecast_pipeline_truth_audit_frame`.
- New asset check: `forecast_pipeline_truth_audit_evidence`.
- Config:
  [../../configs/real_data_dfl_forecast_pipeline_truth_audit_week3.yaml](../../configs/real_data_dfl_forecast_pipeline_truth_audit_week3.yaml).
- Tracked note:
  [DFL_FORECAST_PIPELINE_TRUTH_AUDIT.md](DFL_FORECAST_PIPELINE_TRUTH_AUDIT.md).

The audit checks:

- vector round-trip integrity for forecast and actual price vectors;
- UAH/MWh unit sanity and configured price cap bounds;
- strictly future horizon timestamps after each anchor;
- non-hourly gaps that could reveal DST or serialization issues;
- `thesis_grade` / observed coverage provenance;
- off-by-one horizon-shift diagnostics;
- perfect-forecast sanity where a fixture or candidate forecast equals the
  realized vector.

Decision: serious official NBEATSx/TFT rolling forecasts and DFL v2 reruns
should wait until this audit has no blocking failures. Shift warnings are
investigation signals, not automatic promotion blockers, because weak models can
align better at a shifted horizon by chance.

Materialized result:

- Run `b78b16aa-1da8-4f58-8ce1-89c5d508a9e2` finished successfully and
  materialized `forecast_pipeline_truth_audit_frame`.
- The new `forecast_pipeline_truth_audit_evidence` check passed.
- All three benchmark models had 5 tenants, 104 anchors, 520 rows, 24-hour
  horizons, `thesis_grade` provenance, observed coverage `1.0`, and zero
  blocking failures.
- Blocking failure counts were zero for UAH/MWh unit sanity, vector round-trip,
  leaky horizon rows, non-hourly horizon gaps, and observed-data provenance.
- Shift diagnostics remain active: `strict_similar_day` had 165 shifted-best
  anchors, `nbeatsx_silver_v0` had 377, and `tft_silver_v0` had 328. These are
  warning signals for the official forecast experiment, not proof of leakage.

Decision update: the forecast/scoring path is clear enough to proceed to the
official NBEATSx/TFT rolling benchmark slice. If official forecasts still lose,
the next blocker is model/training signal or data coverage, not a basic vector
serialization or horizon leakage defect.

## Official Forecast Rolling-Origin Benchmark

The next step operationalizes the source-check implied by the truth audit:
official NBEATSx/TFT adapters are trained per rolling-origin anchor with future
targets masked and then scored through the same strict LP/oracle evaluator as
`strict_similar_day`.

Implementation:

- New helper: `smart_arbitrage.strategy.official_forecast_rolling`.
- New asset: `official_forecast_rolling_origin_benchmark_frame`.
- Config:
  [../../configs/real_data_official_forecast_rolling_week3.yaml](../../configs/real_data_official_forecast_rolling_week3.yaml).
- Tracked note:
  [OFFICIAL_FORECAST_ROLLING_ORIGIN_BENCHMARK.md](OFFICIAL_FORECAST_ROLLING_ORIGIN_BENCHMARK.md).

Decision rule:

- If official forecasts still lose to `strict_similar_day`, this is evidence
  for data coverage, feature context, or decision-loss work before DFL.
- If official forecasts beat compact candidates but not strict, they can feed
  DFL v2 as better source forecasts.
- If a source beats strict in the latest holdout, it must still survive rolling
  robustness before any Offline Strategy Promotion claim.

Claim boundary: the asset is official forecast evidence only. It is not full
DFL, not Decision Transformer control, and not market execution.

Materialized result:

- Run `768c9796-422d-40b7-8f8d-083a861cc0e7` completed the first official
  rolling proof.
- Scope: five canonical tenants, two anchors per tenant, 24-hour horizon.
- Rows: 30 strict LP/oracle rows.
- `strict_similar_day` remained strongest on mean regret at `1,587.505` UAH.
- `nbeatsx_official_v0` had worse mean regret at `1,782.829` UAH but better
  median regret at `1,398.064` UAH on this tiny sample.
- `tft_official_v0` was worse on both mean and median regret.

Pipeline fix:

- The run exposed a real official NBEATSx adapter issue. Minimum-history rolling
  windows have null `lag_168_price_uah_mwh` values, and NBEATSx was dropping all
  train rows before model fitting.
- The adapter now fills numeric feature nulls before training, matching the TFT
  path and preventing silent empty rolling-origin evidence.

Decision update: official adapters are now materializable in rolling-origin
strict scoring, but they are not yet promotion candidates. The next iteration
should either scale official runs to more anchors after runtime review or move to
market-coupling/exogenous features and decision-loss training using this fixed
source path.

Scale update:

- Run `bbbd5828-2414-42ce-b0df-ad175cbac445` completed the four-anchor official
  scale proof.
- Rows: 60 strict LP/oracle rows.
- `strict_similar_day` remained best: mean regret `1,020.821` UAH, median regret
  `771.866` UAH.
- `nbeatsx_official_v0`: mean regret `1,508.667` UAH, median regret
  `1,277.428` UAH.
- `tft_official_v0`: mean regret `1,535.299` UAH, median regret `1,065.955`
  UAH.
- CPU runtime for the official asset was about 30 minutes.

Decision update: the source adapter is no longer the blocker. The immediate
research blocker is feature/context and decision objective quality. The next
slice should formalize market-coupling/exogenous feature governance and then use
only temporally available, license-safe signals in official forecast/DFL
training. Simply scaling the same feature set to more anchors is lower leverage.

## Market-Coupling AFE Bridge Refresh

The market-coupling bridge is now recorded in the AFE feature catalog as
machine-readable source governance rather than an informal roadmap note.

Implementation:

- Updated helper: `smart_arbitrage.forecasting.afe`.
- Updated tests: `tests/forecasting/test_afe_feature_catalog.py`.
- External bridge rows now include `source_url`, `source_status`,
  `temporal_resolution`, `regions`, `external_validation_role`, and
  `training_blockers_csv`.

Research grounding:

- [PriceFM](https://huggingface.co/papers/2508.04875) supports European
  cross-region price dependency modeling and external validation.
- [TSFM leakage evaluation](https://huggingface.co/papers/2510.13654) keeps the
  no-leakage guardrail explicit for any foundation-model or external-dataset
  comparison.
- [Cross-border asynchronous EPF](https://www.sciencedirect.com/science/article/abs/pii/S0306261925018070)
  motivates neighboring-market price/context features, while warning that
  timing and gate-closure availability matter.
- [Market-coupling feature selection](https://arxiv.org/abs/2101.05249)
  supports feature-selection rather than blind inclusion of all neighboring
  markets.

Decision: EU/neighbor-market signals may become useful for Ukrainian price
forecasting, but they are still blocked from training until licensing, timezone,
currency, market-rule, temporal-availability, and domain-shift mapping are
implemented. The next executable slice should add a concrete UA/EU temporal
availability mapper before any external market data affects NBEATSx/TFT or DFL.

## Market-Coupling Temporal Availability Gate

The concrete availability mapper has been added as
`market_coupling_temporal_availability_frame`, with asset check
`market_coupling_temporal_availability_evidence`.

What changed:

- The gate consumes `forecast_afe_feature_catalog_frame`.
- It emits one readiness row per external bridge source.
- It keeps every row blocked from training.
- It records source-specific next actions, publication-time policy, and blocker
  status.
- It records PriceFM Dataset Viewer metadata checked on 2026-05-11:
  `default/train`, `140,257` rows, `191` first-row columns.

Interpretation:

- ENTSO-E remains the best candidate for future Poland/neighbor market-coupling
  covariates, but it needs document-type, bidding-zone, API terms, publication
  timestamp, timezone/DST, currency, and market-rule mapping.
- PriceFM is useful for European external validation and graph-market research,
  not as Ukrainian training data.
- OPSD, Ember, Nord Pool, and THieF remain blocked or watch-only sources.

Decision: this closes the source-governance gap but does not yet improve model
quality. The next experimental branch is either an ENTSO-E neighbor sample with
verified temporal availability, or a DFL v2 schedule/value learner that uses
only already-valid Ukrainian features.

## ENTSO-E Neighbor-Market Access Gate

The ENTSO-E branch now has an executable query-spec/access gate:
`entsoe_neighbor_market_query_spec_frame`, checked by
`entsoe_neighbor_market_access_evidence`. It also has a sample-audit sidecar,
`entsoe_neighbor_market_sample_audit_frame`, checked by
`entsoe_neighbor_market_sample_audit_evidence`.

What it records:

- ENTSO-E day-ahead price request shape: `document_type=A44`,
  `process_type=A01`.
- Neighbor candidates: Poland, Slovakia, Hungary, Romania, and Moldova as a
  review-required placeholder.
- Fetch/training remains blocked without a configured ENTSO-E security token.
- `training_use_allowed=false` for every row.
- The sample audit defaults to fetch-disabled Poland (`PL`) for a one-day UTC
  window. With an explicit token and fetch config it can parse source-backed
  ENTSO-E price XML, but still keeps both `training_use_allowed=false` and
  `feature_use_allowed=false`.

Interpretation:

- This is the first concrete step toward Polish/neighboring-market covariates.
- It is still not data ingestion and not model evidence.
- The next source-backed step requires an ENTSO-E security token and a tiny
  Poland sample run, followed by publication timestamp capture, currency
  normalization, and a no-leakage availability gate before training use.

## DFL Schedule/Value Learner V2

Because ENTSO-E and European bridge data remain blocked for training, the next
DFL implementation step uses only Ukrainian thesis-grade schedule evidence. The
new `dfl_schedule_value_learner_v2_frame` learns a deterministic prior-only
schedule-scoring profile over `dfl_schedule_candidate_library_v2_frame`, then
`dfl_schedule_value_learner_v2_strict_lp_benchmark_frame` scores the selected
schedules against the same strict LP/oracle gate.

What it adds:

- a sidecar DFL v2 schedule/value learner, not a new public API or dashboard
  contract;
- selector-safe inputs: prior family regret, forecast spread, forecast objective
  value, throughput, degradation proxy, and SOC slack;
- final-holdout scoring rows for `strict_similar_day`, raw source schedules, and
  `dfl_schedule_value_learner_v2_<source_model>`;
- a Dagster asset check, `dfl_schedule_value_learner_v2_evidence`, for coverage
  and claim-boundary validation.

Claim boundary: the learner is offline DFL research evidence only. It may pass a
development gate against raw neural schedules, but Offline Strategy Promotion
still requires the existing strict LP/oracle improvement, median, rolling
robustness, coverage, and no-market-execution gates.

Materialized result on 2026-05-11:

- Dagster run: `cb23badd-5393-438e-9935-d0d31fd6e0e3`.
- Asset check `dfl_schedule_value_learner_v2_evidence` passed.
- Coverage: five tenants, 18 latest holdout anchors per tenant, 90
  tenant-anchors per source model.
- NBEATSx-source learner: mean/median regret `258.227` / `132.616` UAH versus
  strict reference `314.813` / `202.606` UAH.
- TFT-source learner: mean/median regret `248.488` / `89.891` UAH versus strict
  reference `314.813` / `202.606` UAH.

This is the first latest-holdout DFL-style schedule/value evidence that beats
the frozen `strict_similar_day` control under strict LP/oracle scoring. It is
still a research challenger, not a promoted controller.

## DFL Schedule/Value Learner V2 Rolling Robustness

The rolling robustness slice replayed `dfl_schedule_value_learner_v2` over four
latest-first 18-anchor validation windows, selecting weight profiles only from
anchors strictly before each validation window.

Implementation:

- New helper: `smart_arbitrage.dfl.schedule_value_learner_robustness`.
- New asset: `dfl_schedule_value_learner_v2_robustness_frame`.
- New asset check: `dfl_schedule_value_learner_v2_robustness_evidence`.
- Config:
  [../../configs/real_data_dfl_schedule_value_learner_v2_robustness_week3.yaml](../../configs/real_data_dfl_schedule_value_learner_v2_robustness_week3.yaml).
- Tracked note:
  [DFL_SCHEDULE_VALUE_LEARNER_V2_ROBUSTNESS.md](DFL_SCHEDULE_VALUE_LEARNER_V2_ROBUSTNESS.md).

Materialized result on 2026-05-11:

- Dagster run: `3a5ef479-14e9-4a2b-8d31-14882cf005c7`.
- Asset check `dfl_schedule_value_learner_v2_robustness_evidence` passed.
- Coverage: five tenants, four rolling windows, 18 validation anchors per
  tenant/window, 90 validation tenant-anchors per source/window.
- NBEATSx-source learner passed 4 of 4 rolling strict-control windows.
- TFT-source learner passed 3 of 4 rolling strict-control windows. Its oldest
  window failed because mean improvement was only `3.85%` and median regret was
  worse than `strict_similar_day`.

Decision update: both source learners now qualify as robust research
challengers under the current offline evidence gate. They are still not full
DFL, not Decision Transformer control, not live market execution, and not a
dashboard/API default controller. The next required slice is an Offline
Strategy Promotion / default-fallback gate that consumes this robustness result
and records whether internal `production_promote=true` is justified for offline
read-model strategy evidence while keeping `market_execution=false`.

## DFL Schedule/Value Offline Strategy Promotion Gate

The Offline Strategy Promotion / fallback gate for Schedule/Value Learner V2
now consumes both latest-holdout strict LP/oracle evidence and rolling
robustness evidence.
This is a narrower sidecar gate than the earlier 180-anchor source/regime
Offline Strategy Promotion gate: it accepts the current documented 104-anchor Ukrainian panel
scope, requires 90 latest validation tenant-anchors per source model, and keeps
market execution disabled.

Implementation:

- New helper: `smart_arbitrage.dfl.schedule_value_promotion_gate`.
- New asset: `dfl_schedule_value_production_gate_frame`.
- New asset check: `dfl_schedule_value_production_gate_evidence`.
- Config:
  [../../configs/real_data_dfl_schedule_value_production_gate_week3.yaml](../../configs/real_data_dfl_schedule_value_production_gate_week3.yaml).
- Tracked note:
  [DFL_SCHEDULE_VALUE_PRODUCTION_GATE.md](DFL_SCHEDULE_VALUE_PRODUCTION_GATE.md).

Materialized result on 2026-05-11:

- Dagster run: `93d0f01c-5140-4958-a64f-74067144df4f`.
- Asset check `dfl_schedule_value_production_gate_evidence` passed.
- NBEATSx-source learner: internal `production_promote=true` for offline/read-model
  evidence, 90 latest validation tenant-anchors, 17.97% latest mean-regret
  improvement versus `strict_similar_day`, 4 of 4 rolling strict passes.
- TFT-source learner: internal `production_promote=true` for offline/read-model
  evidence, 90 latest validation tenant-anchors, 21.07% latest mean-regret
  improvement versus `strict_similar_day`, 3 of 4 rolling strict passes.
- `market_execution_enabled=false` for every row.
- Local ignored registry:
  `data/research_runs/week3_dfl_schedule_value_production_gate/`.

Decision update: this is the first Offline Strategy Promotion pass in the DFL
evidence stack. The allowed claim is still narrow: source-specific schedule/value
learner promoted for offline/read-model strategy evidence only. It is not live
market execution, not a deployed Decision Transformer controller, not a full
end-to-end differentiable DFL claim, and not an automatic dashboard/API default
change. `strict_similar_day` remains the fallback for undercovered,
out-of-distribution, failed-source, and future live-execution contexts.

Read-model update: the Offline Strategy Promotion state is now persisted through
`DflTrainingStore` into `dfl_schedule_value_production_gate_rows` and exposed by
the opt-in endpoint
`/dashboard/dfl-schedule-value-production-gate`. The endpoint is source-level
evidence only; it repeats `market_execution_enabled=false` and does not return
`ProposedBid`, `ClearedTrade`, or inverter commands.

Validation update: Compose-backed run
`82bf8100-c5d2-4a6e-b6b2-d2a7da72bc46` persisted two latest gate rows. Postgres
reports two promoted rows and `any_market_execution=false`; FastAPI reports
`row_count=2`, `production_promote_count=2`, and the same narrow claim boundary.

## Official Forecast Schedule/Value Offline Strategy Promotion Path

The next iteration extends the schedule/value promotion machinery to official
NBEATSx/TFT forecasts. The purpose is to avoid comparing compact in-repo
forecast probes against a stronger schedule/value learner while leaving the
official adapters only in a small rolling-origin benchmark.

Implementation:

- Existing source asset: `official_forecast_rolling_origin_benchmark_frame`.
- New candidate-library asset:
  `dfl_official_schedule_candidate_library_frame`.
- New v2 candidate-library asset:
  `dfl_official_schedule_candidate_library_v2_frame`.
- New learner and gate assets:
  `dfl_official_schedule_value_learner_v2_frame`,
  `dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame`,
  `dfl_official_schedule_value_learner_v2_robustness_frame`, and
  `dfl_official_schedule_value_production_gate_frame`.
- Tracked config:
  [../../configs/real_data_official_schedule_value_promotion_week3.yaml](../../configs/real_data_official_schedule_value_promotion_week3.yaml).
- Tracked note:
  [DFL_OFFICIAL_SCHEDULE_VALUE_PROMOTION.md](DFL_OFFICIAL_SCHEDULE_VALUE_PROMOTION.md).

The gate uses the same Offline Strategy Promotion semantics as the compact Schedule/Value
Learner V2 path: five tenants, 90 final validation tenant-anchors per source,
three of four rolling strict-control passes, at least 5% mean-regret
improvement versus `strict_similar_day`, median regret not worse, zero safety
violations, thesis-grade observed provenance, and no market-execution claim.

Decision boundary: this path can promote an official source only for
offline/read-model strategy evidence. It does not enable live bids, does not
replace the Pydantic Gatekeeper, and does not claim a deployed Decision
Transformer controller.

Runtime update on 2026-05-11: the first full 104-anchor official attempt hit the
outer one-hour process timeout before producing Offline Strategy Promotion-grade official rows.
The follow-up keeps the same strict LP/oracle rule but changes the execution
protocol: official rolling-origin generation is split into persisted anchor
batches with one fixed generated timestamp, and the downstream official
schedule/value gate runs only after the full batch set is available.

The same planning slice records the next feature track: add
market-coupling/exogenous feature governance, then route only approved
temporally available signals into official NBEATSx/TFT and DFL training.
European and neighbor-market sources remain `training_use_allowed=false` until
licensing, timezone, currency, market-rule, temporal-availability, and
domain-shift checks pass.

## Official Global-Panel Schedule/Value Screen

The official global-panel NBEATSx path now feeds a schedule/value screening
lane without weakening the full Offline Strategy Promotion gate. This is separate from the
older tenant/anchor official path: it uses the governed rolling global-panel
NBEATSx evidence and its prior-only horizon calibration, then builds feasible
schedule candidates for the Schedule/Value Learner V2 machinery.

Implementation:

- Source asset:
  `nbeatsx_official_global_panel_rolling_calibrated_strict_lp_benchmark_frame`.
- New candidate-library assets:
  `dfl_official_global_panel_schedule_candidate_library_frame` and
  `dfl_official_global_panel_schedule_candidate_library_v2_frame`.
- New learner, robustness, and gate assets:
  `dfl_official_global_panel_schedule_value_learner_v2_frame`,
  `dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame`,
  `dfl_official_global_panel_schedule_value_learner_v2_robustness_frame`, and
  `dfl_official_global_panel_schedule_value_production_gate_frame`.
- Tracked config:
  [../../configs/real_data_official_global_panel_nbeatsx_week3.yaml](../../configs/real_data_official_global_panel_nbeatsx_week3.yaml).
- Tracked note:
  [OFFICIAL_GLOBAL_PANEL_NBEATSX.md](OFFICIAL_GLOBAL_PANEL_NBEATSX.md).

Materialized result on 2026-05-11:

- Full run: `e072d319-fc9a-4de6-b648-264d550e93ae`.
- Gate rerun after threshold hardening:
  `1bbf3da4-2678-4702-a7a2-a2f003264b88`.
- Latest validation tenant-anchors per source: 5.
- Rolling screening windows: 2.
- Calibrated-source schedule/value candidate improved latest mean regret from
  `1495.71` UAH for `strict_similar_day` to `598.09` UAH.
- Raw-source schedule/value candidate improved latest mean regret to
  `1347.36` UAH.
- Offline Strategy Promotion remains blocked for both sources with
  `promotion_blocker=validation_undercoverage`.

Decision update: the schedule/value learner can extract a useful latest-window
signal from official global-panel NBEATSx, especially after prior-only horizon
calibration. This is screening evidence only. Offline Strategy Promotion still requires 90
latest validation tenant-anchors per source and three of four rolling
strict-control passes. The next real improvement path is still UA backfill and
governed market-coupling/exogenous features, not loosening the LP/oracle gate.

## UA Backfill Coverage Update

The 2026-only DFL panel remains valid historical evidence, but it is no longer
the true data ceiling. A source-backed Ukrainian backfill probe expanded the
observed OREE/Open-Meteo window to `2025-01-01` through `2026-04-30`.

Implementation:

- Config:
  [../../configs/real_data_dfl_ua_backfill_probe_week3.yaml](../../configs/real_data_dfl_ua_backfill_probe_week3.yaml).
- Assets materialized:
  `observed_market_price_history_bronze`,
  `tenant_historical_weather_bronze`,
  `real_data_benchmark_silver_feature_frame`,
  `dfl_data_coverage_audit_frame`, and
  `dfl_ua_coverage_repair_audit_frame`.

Materialized result on 2026-05-11:

- Dagster run id: `5c110f78-970c-4bb3-8437-8d047b05947e`.
- Five canonical tenants each reached 461 eligible anchors against a
  365-anchor target.
- Eligible anchor window: `2025-01-08 23:00` through `2026-04-29 23:00`.
- `data_quality_tier=thesis_grade` and `meets_target_anchor_count=true` for all
  tenants.
- Calendar coverage remains `calendar_gap` because `2025-03-30 23:00` and
  `2026-03-29 23:00` are source-backed DST/calendar exclusions, not synthesized
  rows.

Decision update: the next official global-panel NBEATSx/TFT and DFL Offline Strategy Promotion
experiments should move from the 104-anchor 2026-only panel to the 365-anchor
UA backfill panel. The strict LP/oracle gate remains unchanged, and the
calendar-gap hours stay excluded rather than imputed for thesis-grade claims.

## Official Global-Panel Backfill Batch Runner

The official global-panel NBEATSx path now has the same resumability discipline
as the earlier tenant/anchor official runner. The rolling strict LP asset accepts
`anchor_batch_start_index`, `anchor_batch_size`, `resume_generated_at_iso`, and
`merge_persisted_batches`, and the helper script
[run-official-global-panel-batches.ps1](../../scripts/run-official-global-panel-batches.ps1)
generates per-batch configs from the 365-anchor backfill template. The runner
keeps `TotalAnchors` separate from `EndAnchorIndex`, so a resume command can
process only anchors `N..M` while preserving the same 365-anchor selection
universe.

Materialized result on 2026-05-11:

- Config:
  [../../configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml](../../configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml).
- Runner directory:
  `.tmp_runtime/official_global_panel_batches/official-global-panel-2026-05-11T203000-0000`.
- Fixed generated timestamp: `2026-05-11 20:30:00+00`.
- Resumed command:
  `.\scripts\run-official-global-panel-batches.ps1 -TotalAnchors 365 -BatchSize 20 -StartAnchorIndex 8 -AnchorBatchOrder chronological -GeneratedAtIso 2026-05-11T20:30:00+00:00 -BatchTimeoutSeconds 7200`.
- Raw strict rows persisted: 3650, covering 365 chronological anchors, five
  tenants, and two forecast models (`strict_similar_day` plus
  `nbeatsx_official_global_panel_v1`).
- Calibrated strict rows persisted: 5475, covering the same 365 anchors and
  adding `nbeatsx_official_global_panel_horizon_calibrated_v1`.
- Anchor range: `2025-04-22 23:00` through `2026-04-29 23:00`.

Decision update: the 365-anchor backfill lane is now complete for official
global-panel NBEATSx raw and horizon-calibrated strict LP/oracle scoring. The
runner remains resumable, but the `2026-05-11 20:30:00+00` timestamp is now a
complete 365-anchor evidence batch rather than a partial smoke.

## Official Global-Panel 104-Anchor Offline Strategy Promotion Gate Result

The first clean latest-first official global-panel run has now reached the
current 104-anchor Offline Strategy Promotion scope. The first 90-anchor attempt correctly failed
at the robustness step because four 18-anchor validation windows plus prior
history require at least 102 anchors. The run was resumed with the same fixed
`generated_at` and extended to 104 anchors before rerunning the downstream
schedule/value gate.

Implementation:

- Config:
  [../../configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml](../../configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml).
- Runner:
  [../../scripts/run-official-global-panel-batches.ps1](../../scripts/run-official-global-panel-batches.ps1).
- Fixed raw-benchmark generated timestamp: `2026-05-11 21:00:00+00`.
- Downstream gate run id: `985444e2-cf43-4e9f-ba82-da015a380727`.
- Downstream gate generated timestamp:
  `2026-05-11 22:20:54.834887+00`.
- Tracked note:
  [OFFICIAL_GLOBAL_PANEL_NBEATSX.md](OFFICIAL_GLOBAL_PANEL_NBEATSX.md).

Raw official rolling benchmark over 104 anchors:

| Strategy | Rows | Tenants | Anchors | Mean regret | Median regret |
|---|---:|---:|---:|---:|---:|
| `strict_similar_day` | 520 | 5 | 104 | 734.99 | 433.36 |
| `nbeatsx_official_global_panel_v1` | 520 | 5 | 104 | 956.36 | 543.51 |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 520 | 5 | 104 | 912.30 | 511.32 |

The raw and horizon-calibrated official NBEATSx forecasts therefore still lose
to the frozen strict control over the full 104-anchor panel. This is not a
forecast-model promotion.

Schedule/value learner latest holdout:

| Source | Strict mean / median | Raw source mean / median | Learner mean / median | Latest improvement vs strict |
|---|---:|---:|---:|---:|
| `nbeatsx_official_global_panel_v1` | 310.58 / 198.39 | 751.91 / 389.02 | 203.27 / 82.88 | 34.55% |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 310.58 / 198.39 | 663.24 / 373.91 | 236.25 / 123.99 | 23.93% |

Rolling robustness:

| Source | Rolling windows | Strict-control pass windows | Development pass windows | Gate label |
|---|---:|---:|---:|---|
| `nbeatsx_official_global_panel_v1` | 4 | 4 | 4 | `robust_research_challenger` |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 4 | 3 | 4 | `robust_research_challenger` |

Offline Strategy Promotion gate:

| Source | Latest validation tenant-anchors | Allowed challenger | Internal `production_promote` | Market execution | Blocker |
|---|---:|---|---|---|---|
| `nbeatsx_official_global_panel_v1` | 90 | `dfl_schedule_value_learner_v2_nbeatsx_official_global_panel_v1` | `true` | `false` | `none` |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 90 | `dfl_schedule_value_learner_v2_nbeatsx_official_global_panel_horizon_calibrated_v1` | `true` | `false` | `none` |

Decision update: the first official global-panel Offline Strategy Promotion-grade result is a
schedule/value learner result, not a raw NBEATSx forecast result. The thesis-safe
claim is that official global-panel NBEATSx can now feed an offline/read-model
default-fallback controller candidate that passes the strict LP/oracle promotion
gate under the current 104-anchor Ukrainian panel. `market_execution_enabled`
remains `false`, the Pydantic Gatekeeper remains mandatory, and live bidding is
still out of scope.

## Official Global-Panel 365-Anchor Robustness Result

The 365-anchor Ukrainian backfill rerun tests whether the 104-anchor Offline Strategy Promotion
result survives a larger source-backed panel. It uses the same official
global-panel NBEATSx raw timestamp (`2026-05-11 20:30:00+00`), the same frozen
`strict_similar_day` comparator, the same strict LP/oracle scoring, and the same
Offline Strategy Promotion thresholds. No EU market-coupling rows enter
training in this slice.

Implementation:

- Config:
  [../../configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml](../../configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml).
- Runner:
  [../../scripts/run-official-global-panel-batches.ps1](../../scripts/run-official-global-panel-batches.ps1).
- Runner directory:
  `.tmp_runtime/official_global_panel_batches/official-global-panel-2026-05-11T203000-0000`.
- Fixed raw-benchmark generated timestamp: `2026-05-11 20:30:00+00`.
- Downstream gate run id: `c7d23435-9230-4452-9a0c-99f72b2573a9`.
- Downstream gate generated timestamp:
  `2026-05-12 03:10:15.226078+00`.
- Tracked note:
  [OFFICIAL_GLOBAL_PANEL_NBEATSX.md](OFFICIAL_GLOBAL_PANEL_NBEATSX.md).

Raw official rolling benchmark over 365 anchors:

| Strategy | Rows | Tenants | Anchors | Mean regret | Median regret |
|---|---:|---:|---:|---:|---:|
| `strict_similar_day` | 1825 | 5 | 365 | 431.52 | 217.27 |
| `nbeatsx_official_global_panel_v1` | 1825 | 5 | 365 | 708.14 | 446.08 |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 1825 | 5 | 365 | 602.51 | 351.25 |

The raw and horizon-calibrated official NBEATSx forecasts still lose to the
frozen strict control over the full 365-anchor panel. This remains evidence
against direct forecast-model promotion.

Schedule/value learner latest holdout:

| Source | Strict mean / median | Raw source mean / median | Learner mean / median | Latest improvement vs strict |
|---|---:|---:|---:|---:|
| `nbeatsx_official_global_panel_v1` | 310.58 / 198.39 | 771.26 / 393.49 | 225.44 / 109.69 | 27.42% |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 310.58 / 198.39 | 622.25 / 290.22 | 206.37 / 96.02 | 33.56% |

Rolling robustness:

| Source | Rolling windows | Strict-control pass windows | Development pass windows | Gate label |
|---|---:|---:|---:|---|
| `nbeatsx_official_global_panel_v1` | 4 | 4 | 3 | `robust_research_challenger` |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 4 | 4 | 4 | `robust_research_challenger` |

Offline Strategy Promotion gate:

| Source | Latest validation tenant-anchors | Allowed challenger | Internal `production_promote` | Market execution | Blocker |
|---|---:|---|---|---|---|
| `nbeatsx_official_global_panel_v1` | 90 | `dfl_schedule_value_learner_v2_nbeatsx_official_global_panel_v1` | `true` | `false` | `none` |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 90 | `dfl_schedule_value_learner_v2_nbeatsx_official_global_panel_horizon_calibrated_v1` | `true` | `false` | `none` |

Decision update: the 104-anchor official global-panel Offline Strategy Promotion result
generalizes to the 365-anchor Ukrainian backfill panel for the schedule/value
learner. The stronger thesis headline is now: official global-panel NBEATSx can
feed a robust Offline Strategy Promotion DFL-style schedule/value challenger behind
`strict_similar_day` fallback. The claim remains bounded: no live market
execution, no dashboard/API default switch, no deployed Decision Transformer,
and no claim that raw NBEATSx forecasts beat `strict_similar_day`.

## Market-Coupling Exogenous Feature Interface Freeze

The market-coupling feature work after the 365-anchor result is an interface
and governance update, not a change to the headline evidence. The current
Offline Strategy Promotion result remains Ukrainian-only: observed OREE DAM,
Open-Meteo/weather context, tenant load/configuration context, strict LP/oracle
scoring, and `strict_similar_day` fallback.

New additive assets:

- `entsoe_neighbor_market_feature_candidate_frame`;
- `official_forecast_exogenous_feature_route_frame`.

New additive check:

- `official_forecast_exogenous_feature_route_evidence`.

The route centralizes external feature approval for official NBEATSx/TFT/DFL
training. Source-backed ENTSO-E rows may now be represented as feature
candidates, but they remain blocked unless they pass licensing, timezone/DST,
currency normalization, market-rule mapping, temporal-availability, and
domain-shift gates. The route check makes that boundary visible in Dagster and
fails if an external feature is marked usable before the full governance state
is ready. This means ENTSO-E, OPSD, Ember, Nord Pool, PriceFM, and THieF are
still research/external-validation context, not Ukrainian training inputs.

ENTSO-E feature candidates now make two concrete blockers explicit. A parsed
neighboring-market price row still carries
`publication_time_status=blocked_missing_publication_timestamp` until the source
proves the price was published before the Ukrainian DAM decision anchor. It also
carries `currency_normalization_status=blocked_missing_prior_eur_uah_fx_rate`
and leaves `neighbor_market_price_uah_mwh=null` until a prior-known EUR/UAH FX
source is attached. This prevents source-backed EUR sample rows from silently
becoming Ukrainian training features.

Academic rationale: market-coupling and neighboring-zone EPF studies support
the idea that coupled-market features can improve day-ahead price forecasts,
while decision-focused ESS arbitrage literature keeps the final acceptance
criterion on strict LP/oracle regret and net value. Therefore the next feature
expansion must preserve prior-only availability and then rerun the same
official global-panel parity and Offline Strategy Promotion gate.

Tracked docs:

- [MARKET_COUPLING_EXOGENOUS_FEATURE_INTERFACE.md](MARKET_COUPLING_EXOGENOUS_FEATURE_INTERFACE.md).
- [../sources/market-coupling-exogenous-feature-interface-source-capture-2026-05-12.md](../sources/market-coupling-exogenous-feature-interface-source-capture-2026-05-12.md).

## Official Evidence Attempt Interface

The official forecast execution layer now has a shared manifest contract for
long local and future Hugging Face Jobs attempts. The implementation does not
change the 365-anchor result; it makes serious official runs easier to resume,
audit, and package.

New evidence-attempt interface:

- module: `smart_arbitrage.forecasting.official_evidence_attempts`;
- local manifest: `attempt_manifest.json`;
- HF artifact manifest: `official_evidence_attempt_manifest.json`;
- scripts using the manifest:
  [run-official-evidence.ps1](../../scripts/run-official-evidence.ps1),
  [run-official-schedule-value-batches.ps1](../../scripts/run-official-schedule-value-batches.ps1),
  [run-official-global-panel-batches.ps1](../../scripts/run-official-global-panel-batches.ps1),
  [build_hf_official_schedule_value_job.py](../../scripts/build_hf_official_schedule_value_job.py),
  [monitor-official-evidence-attempt.ps1](../../scripts/monitor-official-evidence-attempt.ps1),
  and
  [summarize_official_evidence_attempt_resume.py](../../scripts/summarize_official_evidence_attempt_resume.py).

Decision update: future official reruns should cite the manifest in addition to
the generated timestamp. Resume decisions should use the manifest batch plan and
the latest persisted rows, rather than relying only on free-form `run.log`
inspection. The repo-local monitor wrapper is now the preferred operational
entrypoint: it validates the manifest path and strategy kind, preserves an
explicit `GeneratedAtIso` override, calls the resume-summary helper, reads
`forecast_strategy_evaluations` by `strategy_kind + generated_at` through the
strategy-evaluation store, and can write the JSON snapshot to an output path.
The helper uses the minimum persisted anchor count across source models, which
keeps partial official runs conservative when one model has fewer completed
anchors than another.
The schedule-value registry exporter now accepts that manifest and the monitor
snapshot directly, copying both into the local evidence packet so the registry
folder contains run identity, resume status, persisted counts, and the Offline
Strategy Promotion boundary in one place. It also accepts
`--learner-frame-pickle`, which exports a compact Schedule/Value Learner V2
trace summary beside the registry artifacts. That trace records tenant/source
weight-profile selections, selected final candidate-family counts, and the
candidate-library cardinality used by the promotion evidence.

The HF offload path is now guarded by a receipt-first wrapper:
[submit_hf_official_schedule_value_job.py](../../scripts/submit_hf_official_schedule_value_job.py).
Default execution writes a dry-run receipt only. Paid HF submission requires
`--submit`, a pushed branch, a Jobs-capable HF account, `HF_TOKEN`, and a
writable artifact dataset repo. The receipt keeps
`market_execution_enabled=false` and never writes the token to disk.

Operational update: day-to-day runs should now start from
[run-official-evidence.ps1](../../scripts/run-official-evidence.ps1). It uses
`-Backend local -LocalMode compose` for the resumable Compose/Dagster path,
`-Backend local -LocalMode host` for host `.venv` Dagster runs that can see the
local CUDA torch install, and `-Backend hf` for the HF Jobs payload/receipt
path. The runner writes a training-runtime preflight receipt for local runs and
preserves the same run parameters and Offline Strategy Promotion claim boundary.

Tracked docs:

- [OFFICIAL_EVIDENCE_ATTEMPT_INTERFACE.md](OFFICIAL_EVIDENCE_ATTEMPT_INTERFACE.md).
- [../sources/official-evidence-attempt-interface-source-capture-2026-05-12.md](../sources/official-evidence-attempt-interface-source-capture-2026-05-12.md).
- [../sources/hf-jobs-market-coupling-readiness-source-capture-2026-05-12.md](../sources/hf-jobs-market-coupling-readiness-source-capture-2026-05-12.md).

## Offline Strategy Promotion Language Freeze

The thesis-facing term is now **Offline Strategy Promotion**. Internal
compatibility fields such as `production_promote`,
`production_promote_count`, `dfl_schedule_value_production_gate_frame`, and
`/dashboard/dfl-schedule-value-production-gate` remain unchanged for existing
read models and stored evidence.

The API response language is normalized through
`smart_arbitrage.dfl.offline_strategy_promotion`, so callers can keep reading
the stable fields while `academic_scope` states the correct boundary:
offline/read-model strategy evidence only, `strict_similar_day` fallback, and
`market_execution_enabled=false`.

Tracked doc:

- [OFFLINE_STRATEGY_PROMOTION_LANGUAGE.md](OFFLINE_STRATEGY_PROMOTION_LANGUAGE.md).

## Official Global-Panel 365-Anchor Registry Export

The 365-anchor official global-panel result has been packaged into a local
ignored evidence folder:

- `data/research_runs/week3_official_global_panel_365_strategy_promotion/`;
- `dfl_schedule_value_production_gate_registry.json`;
- `dfl_schedule_value_production_gate_registry.md`;
- `attempt_manifest.json`;
- `resume-summary.json`.
- `dfl_schedule_value_learner_v2_trace_summary.json`;
- `dfl_schedule_value_learner_v2_trace_summary.md`;
- `dfl_official_global_panel_schedule_value_learner_v2_frame.pkl`.

The registry records two official NBEATSx source rows with internal
`production_promote=true`, `market_execution_enabled=false`, and
`strict_similar_day_default_fallback`. The result remains thesis-safe Offline
Strategy Promotion only: it supports offline/read-model strategy evidence and
does not permit live market execution.

Future exports should be regenerated from scripts rather than assembled
manually: first run
[monitor-official-evidence-attempt.ps1](../../scripts/monitor-official-evidence-attempt.ps1)
to write `resume-summary.json`, then run
[materialize_schedule_value_production_gate_registry.py](../../scripts/materialize_schedule_value_production_gate_registry.py)
with `--attempt-manifest`, `--monitor-snapshot`, and `--learner-frame-pickle`.
The resulting folder is the supervisor-facing evidence packet.

## Schedule/Value Learner V3 Next Experiment

This section is now historical context. Schedule/Value Learner V3 was the next
additive ranker test after frozen V2, but it did not become the thesis headline.
Future selector work must compare against V2+, not only against frozen V2, raw
official NBEATSx, horizon-aware calibrated official NBEATSx, and
`strict_similar_day`.

Candidate V3 directions:

- expand the fixed weight-profile grid over the existing prior-only schedule
  features;
- add a small regularized ridge/logistic ranker over schedule features;
- keep profile/weight selection on train/prior anchors only;
- never use final-holdout actuals for feature selection, profile selection, or
  candidate-family selection;
- promote only if the unchanged Offline Strategy Promotion gate still passes.

Implementation update on 2026-05-15:

- `smart_arbitrage.dfl.schedule_value_learner_v3` implements the additive V3
  experiment as a deterministic ridge-style schedule/value ranker.
- The compact-model assets are `dfl_schedule_value_learner_v3_frame` and
  `dfl_schedule_value_learner_v3_strict_lp_benchmark_frame`.
- The official global-panel assets are
  `dfl_official_global_panel_schedule_value_learner_v3_frame` and
  `dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame`.
- The tracked config is
  `configs/real_data_official_global_panel_schedule_value_v3_week3.yaml`.
- Result status: materialized and checked. V3 remains diagnostic evidence
  because it did not improve on frozen V2.

Runtime update on 2026-05-15:

- The official global-panel V3 materialization completed and its asset check
  passed, but V3 did not beat frozen V2.
- V3 mean regret was `227.555` versus V2 `225.437` for
  `nbeatsx_official_global_panel_v1`, and `207.537` versus V2 `206.367` for
  `nbeatsx_official_global_panel_horizon_calibrated_v1`.
- The gate decision was `diagnostic_pass_production_blocked`. This kept V3 out
  of the headline path and motivated the later V2+ candidate-library route.

## Schedule/Value Learner V2+ Regret Improvement Slice

The next model slice attacks remaining regret directly instead of adding another
generic ranker. The implementation is additive and keeps V2 frozen:

- `dfl_schedule_value_regret_decomposition_frame` explains remaining V2 regret
  by tenant/source/anchor, selected family, best-candidate gap, rank/extrema
  diagnostics, SOC slack, throughput, and deterministic failure mode.
- `dfl_schedule_candidate_library_v2_plus_frame` adds prior-safe schedule
  families around top/bottom rank perturbations, spread robustness,
  strict-neighborhood timing shifts, temporal block reconciliation, and terminal
  SOC pressure.
- `dfl_schedule_value_learner_v2_plus_frame` defaults to V2 and switches to V2+
  only when train/prior anchors show a clear non-degrading improvement.
- `dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame` scores strict,
  raw, V2, and V2+ with the unchanged strict LP/oracle evaluator.
- Official global-panel mirror assets are available for the 365-anchor NBEATSx
  lane, with config
  `configs/real_data_official_global_panel_schedule_value_v2_plus_week3.yaml`.

V2+ is now materialized and checked on the 365-anchor official global-panel
Ukrainian packet. The comparison export is reproducible through
`scripts/materialize_schedule_value_v2_plus_comparison.py` and stored locally
under
`data/research_runs/week3_official_global_panel_schedule_value_v2_plus_comparison/`.

Latest strict-gate run `b09194b2-8bf7-42fb-bcc7-1567ca47037c` showed:

- calibrated official global-panel NBEATSx V2+ mean regret: 174.77 UAH;
- frozen V2 mean regret on the same rows: 206.37 UAH;
- `strict_similar_day` mean regret: 310.58 UAH;
- improvement versus strict: 43.73%;
- improvement versus frozen V2: 15.31%;
- `market_execution_enabled=false`.

Rolling robustness run `8832f41e-e605-4107-ab6d-028676faa223` replayed four
18-anchor validation windows with prior-only selection. Both raw official
global-panel NBEATSx and horizon-calibrated official global-panel NBEATSx passed
4 / 4 windows against both `strict_similar_day` and frozen V2. The thesis
headline therefore moves from frozen V2 to V2+ as stronger **Offline Strategy
Promotion** evidence, while live market execution, dashboard defaults, and EU
market-coupling training remain disabled.

Next work after this evidence lock should avoid another small selector ranker.
The two useful branches are a governed market-coupling ablation, where external
features can enter only through the approved prior-only route, or a true
decision-aligned DFL/DT bridge that must beat V2+ and behavior-cloning/selector
baselines under the unchanged strict LP/oracle gate.

The Solver-Free-DFL research direction is now scoped as a schedule-neighbor
surrogate spike, not an immediate dependency or controller rewrite. It can become
implementation work only if it improves V2+ under the same strict LP/oracle gate
or explains remaining V2+ regret well enough to justify a concrete candidate
library change. Otherwise it remains future work. See
[DFL_SOLVER_FREE_SURROGATE_SPIKE.md](DFL_SOLVER_FREE_SURROGATE_SPIKE.md).

What not to do next: do not spend another slice on solver-free prior-only
selector variants over the same V2+ candidate library. The archived 2026-05-17
proof and scratch sweeps did not beat V2+ under the unchanged strict LP/oracle
gate, so the next useful work must change candidate-family design or add
governed prior features before revisiting this direction.

The follow-up V3 objective-design slice now changes the candidate and label
basis before any new DT attempt. `dfl_official_global_panel_schedule_candidate_library_v3_frame`
adds prior-template schedules: one family transfers the historical best-family
forecast delta from train-selection anchors, and one family transfers historical
raw-vs-actual residuals from train-selection anchors. Both are applied only from
prior anchors and strict-scored like all other candidates. The companion
`dfl_official_global_panel_candidate_value_label_panel_v3_frame` separates
`selector_feature_*` prior-safe columns from `label_*` realized scoring
outcomes. This makes the next DFL objective about candidate-level value labels
instead of action imitation over the same weak trajectory contract.

## Governed Market-Coupling Ablation V1

The next branch after freezing V2+ is now implemented as a governance-first
ablation, not as direct EU-feature training. The comparator remains the
Ukrainian-only Schedule/Value Learner V2+ result:

- calibrated V2+ mean regret: 174.77 UAH;
- improvement versus `strict_similar_day`: 43.73%;
- rolling robustness: 4 / 4 windows;
- `market_execution_enabled=false`.

Implementation:

- `entsoe_neighbor_market_aligned_feature_panel_frame` aligns Poland-first
  ENTSO-E candidate rows to Ukrainian benchmark timestamps while keeping them
  research-only by default.
- `dfl_market_coupling_v2_plus_ablation_frame` consumes the existing official
  exogenous route, V2+ strict benchmark evidence, and V2+ rolling robustness. If
  no external feature is approved, it emits
  `ablation_status=blocked_by_governance` and does not train the
  Ukrainian-plus-neighbor variant.
- `dfl_market_coupling_v2_plus_ablation_evidence` makes the blocked or completed
  ablation visible as a Dagster asset check.

Decision rule: an external feature can enter official training only if source
coverage, publication time, timezone/DST alignment, prior-known EUR/UAH FX,
licensing, market-rule mapping, and domain-shift checks all pass. Source-backed
samples alone are insufficient. Even after approval, the B variant must improve
mean regret versus Ukrainian-only V2+, avoid median degradation, and preserve
rolling robustness under the unchanged strict LP/oracle gate.

Tracked docs:

- [DFL_MARKET_COUPLING_ABLATION_V1.md](DFL_MARKET_COUPLING_ABLATION_V1.md).
- [../sources/market-coupling-ablation-v1-source-capture-2026-05-16.md](../sources/market-coupling-ablation-v1-source-capture-2026-05-16.md).

Materialized evidence closure:

- Dagster run id: `b1026e47-249f-463d-a60d-b4f01b3897cd`;
- local evidence packet:
  `data/research_runs/week3_dfl_market_coupling_ablation_v1/`;
- evidence check:
  `dfl_market_coupling_v2_plus_ablation_evidence` passed;
- row count: 2 source-model rows;
- status: `blocked_by_governance` for both raw and calibrated official
  global-panel NBEATSx paths;
- approved external feature columns: none;
- market-coupled B training runs: 0;
- claim boundary: Offline Strategy Promotion only,
  `market_execution_enabled=false`, no European rows in Ukrainian training.

Interpretation: this is a successful governance result, not a regret-improvement
result. It confirms that the system refuses to route ENTSO-E/neighbor-market
features into official training until temporal availability, currency,
timezone/DST, licensing, market-rule, and domain-shift evidence are complete.

## ENTSO-E Poland Governance Completion Lane

The next market-coupling slice narrows the external-data question to one
governed route: Poland ENTSO-E day-ahead price context as a point-in-time
exogenous column. It still does not add European training rows.

New implementation:

- `entsoe_poland_feature_governance_frame` validates the Poland candidate row
  against local ENTSO-E token availability, source-backed sample evidence,
  publication time before the Ukrainian decision anchor, timezone/DST readiness,
  prior-known EUR/UAH FX, licensing, market-rule mapping, and domain-shift
  validation.
- `official_forecast_exogenous_feature_route_frame` now consumes that Poland
  governance row and remains the only external-feature route into official
  global-panel training.
- `configs/real_data_dfl_entsoe_poland_feature_ablation_week3.yaml` is the
  tracked config for the lane. Its default state is intentionally blocked until
  the source, publication, FX, licensing, market-rule, timezone/DST, and
  domain-shift controls are provided.

Tracked docs:

- [DFL_ENTSOE_POLAND_GOVERNANCE_ABLATION.md](DFL_ENTSOE_POLAND_GOVERNANCE_ABLATION.md).
- [../sources/entsoe-poland-governance-ablation-source-capture-2026-05-17.md](../sources/entsoe-poland-governance-ablation-source-capture-2026-05-17.md).

Decision: the correct near-term result is either exactly one approved Poland
feature route plus a rerun of the V2+ ablation, or a stronger blocked packet
with explicit blockers. V2+ remains the thesis headline until a governed
Ukrainian-plus-neighbor variant beats it under the same strict LP/oracle gate.

Materialized evidence closure:

- Dagster run id: `65c87210-36f3-4491-add7-995fa0214d86`;
- local evidence packet:
  `data/research_runs/week3_dfl_entsoe_poland_feature_ablation_v1/`;
- status: `blocked_by_governance` for both raw and calibrated official
  global-panel NBEATSx paths;
- approved external feature columns: none;
- blocked Poland feature column: `entsoe_pl_day_ahead_price_uah_mwh`;
- market-coupled B training runs: 0;
- claim boundary: Offline Strategy Promotion only,
  `market_execution_enabled=false`, no European rows in Ukrainian training.

The 2026-05-20 local rerun added a real token-backed source check through the
lowercase `.env` alias `entsoe_token`. The File Library token smoke returned
safe metadata only (`token_available=true`, `token_type=Bearer`,
`expires_in=900`), and the ENTSO-E API source-backed Poland sample produced
`186` feature-candidate rows with status
`source_backed_feature_sample_fetched_not_training`.

Token-backed evidence closure:

- config:
  [real_data_dfl_entsoe_poland_feature_ablation_token_week3.yaml](../../configs/real_data_dfl_entsoe_poland_feature_ablation_token_week3.yaml);
- Dagster run id: `2a1983fd-3b54-4020-9d76-a8fc6c36ef90`;
- local evidence packet:
  `data/research_runs/week3_dfl_entsoe_poland_token_source_governance_v3/`;
- status: `blocked_by_governance` for both raw and calibrated official
  global-panel NBEATSx paths;
- approved external feature columns: none;
- market-coupled B training runs: 0;
- claim boundary: Offline Strategy Promotion only,
  `market_execution_enabled=false`, no European rows in Ukrainian training.

The token and source-backed-sample blockers are now cleared for this smoke run.
The precise remaining blockers are `publication_time`,
`prior_eur_uah_fx_rate`, `currency`, `timezone`, `licensing`, `market_rules`,
`domain_shift`, and `temporal_availability`. Interpretation: source access is
no longer the active blocker, but the system still correctly blocks training
until point-in-time publication metadata, prior-known FX, licensing/rule
evidence, timezone/DST mapping, and domain-shift validation are complete.

The governance route now has a two-stage status. A row may become
`approved_for_experimental_ablation=true` after point-in-time mechanics pass
while still keeping `approved_for_official_training=false` until domain-shift
validation passes. This is needed because domain shift can only be tested by a
controlled Ukrainian-plus-Poland ablation, but that ablation must not be
described as headline official training.

The first practical route has now been implemented as
`entsoe_poland_lagged_feature_candidate_frame`. It emits
`entsoe_pl_lag24_day_ahead_price_uah_mwh`: Ukrainian timestamp `t` receives the
source-backed Poland ENTSO-E day-ahead price from `t - 24h`, converted with
prior-known NBU EUR/UAH metadata when the FX source, timestamp, and rate are
configured. Full benchmark timestamp coverage is required before the feature can
be considered for the controlled ablation. This avoids publication-time and
temporal-availability leakage while still keeping same-delivery Poland prices
blocked from training.

The first lag-24 run, `week3_dfl_entsoe_poland_lag24_governance_attempt`,
materialized the route and exported the ablation packet. It remained
`blocked_by_governance`: no approved external feature columns, no B training,
and `market_execution_enabled=false`. The blocked feature list now includes
`entsoe_pl_lag24_day_ahead_price_uah_mwh`, and the unresolved controls are
`currency,domain_shift,licensing,market_rules,prior_eur_uah_fx_rate,publication_time,temporal_availability,timezone`.

The follow-up run, `week3_dfl_entsoe_poland_lag24_nbu_approved_route`, moved
the same route to `approved_route_pending_materialization`:

- Dagster run id: `5c62678e-d310-4e86-90fc-d0bea701d3aa`;
- `entsoe_pl_lag24_day_ahead_price_uah_mwh` is approved for experimental
  ablation;
- NBU EUR/UAH metadata covers `485` effective dates from `2024-12-31` to
  `2026-04-29`;
- ENTSO-E lagged timestamp coverage is `11,638 / 11,638`;
- `141` small ENTSO-E gaps were filled by deterministic source-side
  interpolation, not by Ukrainian target actuals;
- official training remains blocked by `domain_shift`;
- the first market-coupled B variant had not been trained in this packet;
- `market_execution_enabled=false`.

This closes the source-access and point-in-time mechanics for one Poland lane.
The next research decision is whether the controlled Ukrainian-plus-Poland B
variant improves over frozen Ukrainian-only V2+ under the unchanged strict
LP/oracle gate.

The B comparison is now materialized as
`week3_dfl_entsoe_poland_lag24_b_variant_comparison` with Dagster run
`a32de660-a3be-4e04-b907-fbdf96a9b45b`. The evidence check passed and the
ablation moved to `comparison_complete`, but the Poland-lagged B selector did
not improve either official source:

- calibrated source: Ukrainian-only V2+ `174.77` UAH mean regret versus B
  `174.77` UAH;
- raw source: Ukrainian-only V2+ `193.36` UAH mean regret versus B `193.36`
  UAH;
- rolling robustness remained `4 / 4` because B fell back to Ukrainian-only
  V2+;
- ablation passed: `false`, blocker `mean_not_improved`.

The updated conclusion is narrower: the token-backed Poland lag-24 route is
executable as a governed exogenous ablation, but it does not yet add measurable
decision value over the Ukrainian-only V2+ headline under the strict LP/oracle
gate.

The richer prior-safe Poland follow-up then extended the same lagged lane with
derived regime features: one-hour and 24-hour deltas, daily spread, daily price
rank, and lagged peak/trough timing. This was materialized as
`week3_dfl_entsoe_poland_rich_prior_safe_b_variant_comparison` with Dagster run
`3fe654b3-43e3-471d-9b36-2be5baf16477`. The evidence check passed, the lagged
frame reached `11,638` source-backed benchmark rows with full primary coverage,
and the route remained `approved_for_experimental_ablation=true` only. The
selector evaluated richer prior-only profiles, but all `10 / 10` tenant/source
rows fell back to Ukrainian-only V2+ because train/prior evidence predicted
degradation. Final strict LP/oracle evidence stayed neutral: calibrated source
`174.77` UAH versus B `174.77` UAH, raw source `193.36` UAH versus B `193.36`
UAH, rolling robustness `4 / 4`, `ablation_passed=false`,
`ablation_blocker=mean_not_improved`, and `market_execution_enabled=false`.

This means the first Poland lane is no longer primarily a source-access or
timestamp-coverage problem. The current blocker is decision value: lagged
Poland price regimes, even with spreads/deltas/peak-trough context, did not add
safe improvement over the Ukrainian-only V2+ schedule/value baseline.

The next additive test routes the same prior-safe Poland feature family into
official global-panel predictors before schedule/value selection. The new
training frame,
`official_global_panel_poland_lag24_experimental_training_frame`, appends the
lagged Poland level, deltas, spread, rank, and peak/trough timing columns to
`known_future_feature_columns_csv`. The existing official NBEATSx adapter sees
them as NeuralForecast future exogenous variables, and the existing TFT adapter
sees them as PyTorch Forecasting known reals. The resulting model names are
kept separate:
`nbeatsx_official_global_panel_poland_lag24_experimental_v1` and
`tft_official_global_panel_poland_lag24_experimental_v1`. This is an
experimental ablation screen only; domain-shift validation still blocks
official headline training, and any downstream schedules must beat frozen
Ukrainian-only V2+ before changing thesis claims.

The first screen materialization completed with Dagster run
`9ca621e7-9959-4b65-99fe-68ff4a2d7a15` using
`configs/real_data_official_global_panel_poland_lag24_experimental_forecast_week3.yaml`.
It materialized the feature route, experimental training frame, NBEATSx
experimental forecast, and TFT experimental forecast. This proves the route can
feed official adapters; it does not yet prove schedule-value improvement.

Operational follow-up: use
[run-entsoe-poland-governance-ablation.ps1](../../scripts/run-entsoe-poland-governance-ablation.ps1)
for the next attempt. It records a receipt, runs the exact asset selection,
copies the Dagster-stored ablation frame, and exports the local packet. This
keeps the next token-backed source sample auditable without manual Docker copy
or free-form log inspection.

## V2+-Anchored LAVA Schedule-Neighbor Bridge

The Poland ranker result changed the next DT/LAVA step. It is not enough to
train another small ranker, and it is too early to train a raw hourly-action DT.
The new bridge adds teacher labels and feasible schedule-neighbor candidates
around the current strongest evidence:

- `dfl_v2_plus_schedule_neighbor_teacher_label_frame` classifies current rows
  as `v2_plus_best`, `poland_safe_win`, `poland_tail_risk_loss`,
  `selector_overreach`, or train-only `oracle_only_train_diagnostic`.
- `dfl_lava_schedule_neighbor_candidate_frame` combines frozen V2+, strict
  fallback, Poland/TFT near-miss schedules, and train-only oracle-neighborhood
  diagnostics.
- `dfl_lava_candidate_value_scorer_frame` trains a conservative prior-only
  scorer over full schedule candidates.
- `dfl_lava_candidate_value_strict_lp_benchmark_frame` compares
  `strict_similar_day`, frozen V2+, behavior cloning, and the LAVA scorer under
  the unchanged strict LP/oracle evaluator.

This is still a research bridge. It can replace V2+ only if it beats
`174.77` UAH mean regret, avoids median degradation against `67.30` UAH,
passes rolling robustness before headline replacement, and keeps
`market_execution_enabled=false`.

First materialized result: Dagster run
`30742a14-2712-4640-9ec8-1aff155f52d1` completed the four bridge assets and
persisted `dfl_lava_candidate_value_strict_lp_benchmark`. The result is
negative evidence: the LAVA scorer reached `501.25` UAH mean regret and
`221.77` UAH median regret versus frozen calibrated V2+ at `174.77` /
`67.30`. Behavior cloning matched strict at `310.58` / `198.39`. The bridge
therefore becomes a label/audit layer for the next DT/LAVA design, not a
replacement for V2+.

Technical spec:
[DFL_LAVA_SCHEDULE_NEIGHBOR_BRIDGE.md](DFL_LAVA_SCHEDULE_NEIGHBOR_BRIDGE.md).

## V2+-Anchored DFL/DT Bridge Evidence

The compact residual DFL / offline DT bridge is now treated as negative evidence
against the current thesis baseline rather than as a failed thesis direction.
The bridge compares residual DFL, tiny offline DT, behavior cloning, fallback,
`strict_similar_day`, and V2+ under the same strict LP/oracle evaluator.

Current compact-path result:

- asset: `dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame`;
- check: `dfl_v2_plus_dfl_dt_bridge_evidence`;
- source rows: compact `nbeatsx_silver_v0` and `tft_silver_v0`;
- outcome: no residual/DT challenger beats V2+ mean regret without median
  degradation while preserving the strict-control gate;
- claim boundary: valid negative Offline Strategy Promotion evidence only,
  `market_execution_enabled=false`.

This result does not invalidate Decision Transformers as a research direction.
It shows that the older compact candidate path is weaker than the official
global-panel V2+ schedule/value learner. The next meaningful DFL/DT experiment
therefore uses official V2+ and oracle-style schedules as teacher trajectories,
not compact model schedules.

Official bridge path:

- `dfl_official_global_panel_v2_plus_trajectory_dataset_frame` builds a
  real-data trajectory panel from
  `dfl_official_global_panel_schedule_candidate_library_v2_plus_frame`;
- teacher labels are built only from train/prior anchors;
- final-holdout rows are scoring-only;
- `dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame`
  compares official V2+, residual/value learner, offline DT, behavior cloning,
  fallback, and `strict_similar_day`;
- a challenger can replace V2+ as headline evidence only if it beats V2+ mean
  regret, avoids median degradation, preserves rolling robustness, and keeps
  zero safety violations.

Materialized official result:

- Dagster run id: `53efba76-38cb-4624-9cd8-e15fb8c1c7a9`;
- evidence check:
  `dfl_official_global_panel_v2_plus_dfl_dt_bridge_evidence` passed;
- local negative-evidence packet:
  `data/research_runs/week3_dfl_official_v2_plus_dfl_dt_bridge_negative_evidence/`;
- calibrated official V2+ mean regret: 174.77 UAH;
- calibrated residual/DT mean regret: 367.70 UAH;
- raw official V2+ mean regret: 193.36 UAH;
- raw residual/DT mean regret: 328.51 UAH;
- outcome: blocked versus V2+ for both official source models;
- claim boundary: `market_execution_enabled=false`, no Poland/ENTSO-E training,
  no dashboard/API switch.

Failure audit closure:

- audit asset: `dfl_official_v2_plus_bridge_failure_audit_frame`;
- audit check: `dfl_official_v2_plus_bridge_failure_audit_evidence` passed;
- Dagster run id: `5ccff4bd-4628-4595-bb82-f91cb9194180`;
- audit rows: 720 analysis-only challenger rows;
- dominant blocker: `candidate_family_collapse`, 351 rows or 48.75%;
- interpretation: residual/DT mostly collapses to the same schedule family and
  fails to learn the full schedule-value ordering that V2+ selected offline.

Operational note: the official trajectory frame is large. On local Docker
Desktop, materialize the residual model, offline DT candidate, and final bridge
asset serially after the trajectory asset exists, otherwise two parallel workers
can load the trajectory frame at the same time and destabilize the Docker API.

Tracked docs:

- [DFL_V2_PLUS_DFL_DT_BRIDGE.md](DFL_V2_PLUS_DFL_DT_BRIDGE.md).
- [DFL_OBJECTIVE_REDESIGN_PLAN.md](DFL_OBJECTIVE_REDESIGN_PLAN.md).

## Pairwise Schedule-Value DFL v2

The next objective-redesign slice is implemented as a V2+-anchored,
official-global-panel experiment rather than another Decision Transformer over
the same action-imitation target. The new assets are:

- `dfl_official_global_panel_schedule_value_dfl_v2_frame`;
- `dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame`;
- asset check `dfl_official_global_panel_schedule_value_dfl_v2_evidence`.

The selector computes pairwise schedule-family value scores from train/prior
anchors only, then uses a non-degradation fallback to frozen V2+. Final-holdout
actuals are scoring labels only. A DFL v2 row can become a stronger thesis
headline only if it improves mean regret versus V2+, does not worsen median
regret versus V2+, still beats `strict_similar_day` by at least `5%`, and keeps
`market_execution_enabled=false`.

Tracked run config:

- `configs/real_data_dfl_schedule_value_dfl_v2_week3.yaml`.

Technical details:

- [DFL_SCHEDULE_VALUE_DFL_V2.md](DFL_SCHEDULE_VALUE_DFL_V2.md).

Materialized result:

- Dagster run id: `9af65d45-6c7d-4aec-b71b-7fb31fd2147d`;
- evidence check passed;
- local packet:
  `data/research_runs/week3_dfl_schedule_value_dfl_v2_comparison/`;
- gate decision: `diagnostic_pass_replacement_blocked`;
- calibrated DFL v2 matched V2+ at `174.77` UAH mean regret, so improvement
  versus V2+ was `0.00%`;
- V2+ remains the thesis headline Offline Strategy Promotion evidence.

## Candidate-Value DFL v3

Candidate-Value DFL v3 is the follow-up to the negative DFL v2 result. It does
not run another tiny Decision Transformer. Instead, it expands the official
global-panel candidate library around audited failure modes and trains/selects
a prior-only schedule-level value scorer.

New assets:

- `dfl_official_global_panel_schedule_candidate_library_v3_frame`;
- `dfl_official_global_panel_candidate_value_label_panel_v3_frame`;
- `dfl_official_global_panel_candidate_value_dfl_v3_frame`;
- `dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame`;
- `dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame`;
- asset checks `dfl_official_global_panel_candidate_value_dfl_v3_evidence`
  and `dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_evidence`.

The implemented scorer is `learned_linear_candidate_value_v3`, a small
ridge-style candidate-level value model trained on `train_selection` rows from
the V3 label panel. It scores full candidate schedules from prior-safe
`selector_feature_*` columns and falls back to frozen V2+ unless train/prior
evidence predicts a clear improvement. Final-holdout labels are used only for
strict scoring and audit metrics.

Gate: V3 must beat frozen V2+ mean regret, avoid median degradation, still beat
`strict_similar_day`, and keep `market_execution_enabled=false`. Otherwise V2+
remains the thesis headline.

Materialized result:

- Dagster run `2dcdb48d-70b0-44f5-99b8-b8b5d4d58057`;
- label-panel, strict-benchmark, and failure-audit evidence checks passed;
- gate decision: `diagnostic_pass_replacement_blocked`;
- calibrated V3 matched V2+ at `174.77` UAH mean regret;
- raw V3 matched V2+ at `193.36` UAH mean regret;
- improvement versus V2+ was `0.00%`, so V2+ remains the thesis headline.

The failure audit explains why the new prior-template schedules did not beat
V2+ often enough. On final holdout, `prior_best_family_template_v3` had
`605.71` UAH mean regret for calibrated NBEATSx and `689.66` UAH for raw
NBEATSx, while `prior_oracle_residual_template_v3` had `627.08` UAH and
`729.69` UAH respectively. Their final-holdout win rates versus V2+ were only
`4.44%`, `13.33%`, `5.56%`, and `7.78%` depending on source row. The diagnosis
is therefore `template_not_competitive_vs_v2_plus`: the templates occasionally
help individual anchors, but average prior residual transfer is weaker than the
already robust V2+ schedule/value blend.

Runbook:

- [DFL_CANDIDATE_VALUE_DFL_V3.md](DFL_CANDIDATE_VALUE_DFL_V3.md).

## Plateau-Breaker / Candidate-Value DFL v4

Candidate-Value DFL v4 is the follow-up to the V3 plateau. It does not start
another Decision Transformer run. It first asks why V3 matched V2+ and separates
the plateau into three deterministic causes:

- `candidate_not_better`: no available candidate schedule beats V2+ on the
  anchor;
- `candidate_available_but_not_selected`: a better candidate exists, but the
  scorer/fallback misses it;
- `fallback_too_conservative`: a relaxed fallback threshold would improve
  final-holdout score, but prior/train evidence is too weak for promotion.

The slice adds:

- `dfl_official_global_panel_v2_v3_plateau_autopsy_frame`;
- `dfl_official_global_panel_plateau_data_quality_audit_frame`;
- `dfl_official_global_panel_schedule_candidate_library_v4_frame`;
- `dfl_official_global_panel_candidate_value_label_panel_v4_frame`;
- `dfl_official_global_panel_candidate_value_dfl_v4_frame`;
- `dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame`.

The V4 candidate library targets the actual failure mode rather than simply
adding another scorer: calibrated quantile/risk schedules, block-structured
morning/evening peak schedules, SOC terminal reserve variants,
spread-volatility robust schedules, tenant degradation/throughput sweeps, and
train-only oracle-neighborhood diagnostics. Final-holdout candidate generation
does not use oracle-derived rows.

Gate: V4 can replace the thesis headline only if it improves mean regret versus
V2+, does not worsen median regret versus V2+, still beats `strict_similar_day`
by at least `5%`, preserves rolling robustness, and keeps
`market_execution_enabled=false`. Otherwise V2+ remains the current
Offline Strategy Promotion evidence.

Materialized result:

- Dagster run id: `0c57f795-3b5b-4106-ad9d-0776294a1eb4`;
- candidate library rows: `71,040`;
- V4 label-panel rows: `71,040`;
- V4 learner rows: `10`;
- V4 strict LP/oracle benchmark rows: `720`;
- evidence checks passed;
- calibrated V4 selected V2+ at `174.77` UAH mean regret;
- raw V4 selected V2+ at `193.36` UAH mean regret;
- improvement versus V2+ was `0.00%`, so V2+ remains the thesis headline.

The autopsy now gives a sharper diagnosis than the V3 failure audit. For
calibrated NBEATSx, `71 / 90` final-holdout tenant-anchor rows were
`candidate_not_better` and `19 / 90` were `fallback_too_conservative`. For raw
NBEATSx, `48 / 90` were `candidate_not_better` and `42 / 90` were
`fallback_too_conservative`. The raw source had a pre-fallback candidate mean
of `190.59` UAH versus raw V2+ at `193.36` UAH, but that still did not beat the
calibrated V2+ headline at `174.77` UAH and lacked enough prior evidence for
promotion.

The data audit marks Ukrainian DAM history and regret-cluster alignment as
ready, but still flags weather/load, calendar/event context, and
publication-time availability gaps. That points the next improvement branch
toward point-in-time context and genuinely new schedule shapes, not another
small selector over the same evidence.

Runbook:

- [DFL_PLATEAU_BREAKER_V4.md](DFL_PLATEAU_BREAKER_V4.md).

## Point-In-Time Context Repair + Candidate-Value DFL V5

The V4 plateau result changed the next branch from "try a larger DT" to
"repair point-in-time context first." V5 is implemented as an additive,
Ukrainian-only context-enriched candidate-value gate against the same frozen
Schedule/Value Learner V2+ comparator:

- calibrated V2+ mean regret: `174.77` UAH;
- raw V2+ mean regret: `193.36` UAH;
- rolling robustness: `4 / 4`;
- `market_execution_enabled=false`.

The new context audit converts V4's broad data-quality gaps into exact rows by
tenant, source model, anchor, feature family, and blocker:

- `missing_weather_load_context`;
- `missing_calendar_event_context`;
- `missing_publication_time`;
- `context_available_not_used`;
- `context_ready`.

The feature panel keeps a strict contract: prior-only inputs are named
`selector_feature_*`, while realized values and strict-scoring outcomes stay in
`label_*` or `diagnostic_*` columns. Poland, ENTSO-E, and other European
market-coupling features are absent from this slice.

V5 reuses the V4 candidate schedules and adds context-conditioned selector
features before fitting the candidate-level value scorer. A V5 row can replace
V2+ only if strict LP/oracle scoring improves mean regret versus V2+, avoids
median degradation, preserves rolling robustness, keeps zero safety violations,
and leaves the claim boundary as Offline Strategy Promotion only.

Materialized result:

- Dagster run id: `11a3effb-ffb5-4e1a-97e2-878b00106381`;
- context repair audit rows: `14,600`;
- point-in-time context feature panel rows: `3,650`;
- V5 learner rows: `10`;
- V5 strict LP/oracle benchmark rows: `720`;
- evidence check passed;
- calibrated V5 selected V2+ at `174.77` UAH mean regret;
- raw V5 selected V2+ at `193.36` UAH mean regret;
- improvement versus V2+ was `0.00%`, so V2+ remains the thesis headline.

The context audit did not show a clean new signal ready to replace V2+. It
reported `3,650` rows each for `missing_weather_load_context`,
`missing_calendar_event_context`, and `missing_publication_time`, plus `3,589`
`context_ready` rows for regret-cluster alignment and `61`
`context_available_not_used` rows. The result confirms the plateau diagnosis:
current Ukrainian-only context is not yet rich enough to make V5 beat V2+ under
the unchanged gate.

Tracked implementation:

- `dfl_point_in_time_context_repair_audit_frame`;
- `dfl_point_in_time_context_feature_panel_frame`;
- `dfl_context_enriched_schedule_candidate_library_v5_frame`;
- `dfl_context_enriched_candidate_value_label_panel_v5_frame`;
- `dfl_context_enriched_candidate_value_dfl_v5_frame`;
- `dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame`;
- [DFL_POINT_IN_TIME_CONTEXT_REPAIR.md](DFL_POINT_IN_TIME_CONTEXT_REPAIR.md).

## TFT Global-Panel Quantile Schedule/Value Gate

The next TFT slice is implemented as a parity path, not as a claim that raw TFT
should replace the current V2+ headline. Earlier compact TFT evidence had useful
latest-window signals but failed rolling robustness. The new lane therefore gives
TFT the same decision-value treatment that made official global-panel NBEATSx
V2+ strong:

- train one official TFT over the five-tenant panel;
- keep p10, p50, and p90 quantile forecasts as separate schedule sources;
- calibrate and strict-score horizon/quantile rows using prior anchors only;
- route TFT quantile schedules into the same strict schedule/value gate;
- compare against Ukrainian-only calibrated V2+ at `174.77` UAH mean regret and
  `4 / 4` rolling windows.

Tracked implementation:

- `tft_official_global_panel_rolling_strict_lp_benchmark_frame`;
- `tft_official_global_panel_horizon_quantile_calibration_frame`;
- `tft_official_global_panel_horizon_quantile_calibrated_strict_lp_benchmark_frame`;
- `dfl_tft_quantile_schedule_candidate_library_frame`;
- `dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame`;
- `dfl_tft_calibrated_quantile_schedule_candidate_library_frame`;
- `dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame`;
- `dfl_tft_augmented_v2_plus_evidence`;
- [DFL_TFT_GLOBAL_PANEL_QUANTILE_GATE.md](DFL_TFT_GLOBAL_PANEL_QUANTILE_GATE.md).

The serious follow-up uses
`scripts/run-tft-quantile-gate-batches.ps1` with
`configs/real_data_official_global_panel_tft_quantile_schedule_value_365_week3.yaml`
so the 365-anchor lane is resumable and can run in host CUDA mode.

Decision rule: TFT alone, or the combined NBEATSx+TFT candidate selector, can
become the next headline only if it beats frozen V2+ mean regret, does not
worsen median regret, preserves rolling robustness, and keeps
`market_execution_enabled=false`. No Poland/ENTSO-E feature is routed into this
TFT run.

## NBEATSx + TFT Candidate-Portfolio Meta-Selector

The first combined TFT path was too coarse because it selected one TFT candidate
key per tenant and otherwise fell back to V2+. The next combined test is now
implemented as a candidate-level portfolio:

- frozen Ukrainian-only NBEATSx V2+ remains the default expert;
- calibrated TFT p10/p50/p90 schedules enter only as feasible schedule
  candidates;
- cross-model candidates preserve feasible LP schedules while adding
  disagreement/risk metadata;
- selector inputs are prior-only `selector_feature_*` columns;
- realized regret deltas stay in `label_*` columns;
- final holdout actuals affect scoring only.

Tracked implementation:

- `dfl_nbeatsx_tft_complementarity_audit_frame`;
- `dfl_nbeatsx_tft_candidate_portfolio_v1_frame`;
- `dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame`;
- `dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame`;
- `dfl_nbeatsx_tft_meta_selector_robustness_frame`;
- `dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame`;
- `dfl_nbeatsx_tft_meta_selector_prior_rolling_robustness_frame`;
- `configs/real_data_dfl_nbeatsx_tft_combined_portfolio_week3.yaml`;
- [DFL_NBEATSX_TFT_COMBINED_PORTFOLIO.md](DFL_NBEATSX_TFT_COMBINED_PORTFOLIO.md).

The gate is unchanged: the portfolio can become the next Offline Strategy
Promotion headline only if it improves mean regret versus calibrated V2+ by at
least `5%`, does not worsen median regret, passes rolling robustness, keeps zero
safety violations, and preserves `market_execution_enabled=false`. If it fails,
the result is negative evidence explaining whether TFT had no useful
complementary candidates, the selector missed them, or the fallback was correctly
conservative.

Selector-fix note: the first combined strict frame was latest-holdout only and
therefore could not safely estimate TFT-versus-V2+ complementarity in prior
windows. The rolling strict frame repairs that by rebuilding V2+ from older
anchors inside each validation window and by adding V2+ fallback candidate rows
before the portfolio selector is trained. DT/LAVA-style work should start only
after this rolling evidence is materialized, because otherwise it would inherit
an unverified selector/comparator contract.

Materialized rolling result:

- Dagster run id: `35c6ddcd-ce54-4ae8-b527-670a875faf3f`;
- rolling strict rows: `1,800`;
- rolling robustness rows: `4`;
- rolling asset checks: passed structurally;
- rolling pass count: `0 / 4`;
- latest two windows used V2+ fallback for all `90 / 90` tenant-anchor rows;
- older two windows selected TFT-derived candidates but degraded mean/median
  regret versus V2+.

Interpretation: TFT provides some local complementary schedules (`24 / 90`
latest tenant-anchors), but the current prior-only selector cannot exploit them
robustly. V2+ remains the thesis headline. The next research branch should be
DT/LAVA-style schedule-neighbor or candidate/value supervision against frozen
V2+, not another small selector variant.

Demo visuals for the supervisor/peer meeting are indexed at
[../thesis/demo-day-2/index.md](../thesis/demo-day-2/index.md).

## Poland-Enhanced Calibrated Forecast Screen

The governed Poland lane was also tested one level earlier in the stack: as
point-in-time exogenous features for official global-panel NBEATSx/TFT, followed
by prior-only calibration and the same V2+ schedule/value gate. This keeps the
comparator frozen:

- calibrated Ukrainian-only V2+ mean regret: `174.77` UAH;
- median regret: `67.30` UAH;
- rolling robustness: `4 / 4`;
- `market_execution_enabled=false`.

Implemented additive assets:

- `official_global_panel_poland_lag24_experimental_nbeatsx_horizon_calibration_frame`;
- `official_global_panel_poland_lag24_experimental_tft_horizon_quantile_calibration_frame`;
- `official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame`;
- `dfl_poland_lag24_calibrated_schedule_candidate_library_frame`;
- `dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame`;
- `dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame`;
- `dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame`;
- `dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_frame`;
- `dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame`;
- `dfl_poland_lag24_calibrated_vs_v2_plus_comparison_frame`;
- `configs/real_data_official_global_panel_poland_lag24_calibrated_schedule_value_week3.yaml`.

Materialized result:

- Dagster run id: `25ac4101-b557-42b0-8950-3613dc77ad4e`;
- evidence packet:
  `data/research_runs/week3_poland_lag24_calibrated_experimental_schedule_value/`;
- best Poland row: calibrated TFT V2+ at `181.93` UAH mean regret and
  `44.29` UAH median regret;
- gate blocker: `mean_not_improved_vs_frozen_v2_plus`.

Interpretation: calibration made the Poland TFT branch meaningfully better than
raw Poland TFT, but not enough to beat the frozen Ukrainian-only V2+ mean
regret. This is useful near-miss evidence, not a reason to run the full
365-anchor path yet. The next model-improvement branch should be DT/LAVA-style
candidate/value or schedule-neighbor supervision against frozen V2+, unless a
new external feature representation is added with a stronger prior-safe
hypothesis.

That stronger prior-safe representation was then tested without changing the
claim boundary. The feature lane now includes rolling Poland 24h/168h mean and
spread context, rolling min/max, price-vs-rolling-mean, daily peak/trough
distance and indicator flags, and rolling PL-vs-UA spread context. Dagster run
`58e38050-9db1-4f34-9215-bc3e99644f46` exported
`data/research_runs/week3_poland_lag24_richer_calibrated_experimental_schedule_value/`.
The best row was still calibrated TFT V2+, now at `177.34` UAH mean regret and
`39.46` UAH median regret. This narrowed the gap to frozen Ukrainian-only V2+
from `7.16` UAH to `2.58` UAH, but still did not promote because mean regret did
not beat `174.77` UAH. The next Poland branch should not be a full 365-anchor
rerun of the same representation; it needs either better domain features,
stronger schedule/value use of the low-median TFT signal, or DT/LAVA-style
teacher/candidate supervision.

The tail-risk autopsy in
`data/research_runs/week3_poland_lag24_richer_tail_risk_audit/` shows why this
near miss is not noise. The calibrated Poland TFT V2+ row wins `48` matched
tenant-anchor rows, loses `32`, and ties `10`, while its median regret improves
from `67.30` to `39.46` UAH. The mean still worsens by `2.58` UAH because seven
tail-loss rows contribute `2074.75` UAH of positive regret delta. An
oracle-only row switcher would achieve `143.80` UAH mean regret, but that is a
diagnostic upper bound only because it uses final outcomes. The practical next
research branch is a prior-only tail-risk veto/fallback for Poland-enhanced
schedules, evaluated against the unchanged V2+ gate.

That veto was then implemented as
`scripts/materialize_poland_lag24_prior_veto_packet.py`, then wired into Dagster
as `dfl_poland_lag24_prior_tail_risk_veto_frame` and materialized in run
`cb60e2d9-1b52-43b9-bd57-bfa7fa155e7d`. The local evidence packet is
`data/research_runs/week3_poland_lag24_prior_tail_risk_veto/`. It uses a small
deterministic ridge scorer trained only on earlier anchors and pre-anchor
schedule/forecast/candidate metadata. On the matched 90-row screen it selects
Poland schedules on `34 / 90` rows and improves mean regret from `174.77` to
`167.05` UAH, with median regret improving from `67.30` to `55.97` UAH. This is
the first Poland-enhanced selector to beat frozen V2+ on mean and median in the
screen, but it remains below the `5%` replacement threshold (`4.41%`) and the
current row-level overlap is only `18` anchors per tenant. Therefore it is not
the thesis headline without a larger Poland-enhanced official evidence run and
rolling robustness evidence.

The follow-up implementation adds
`scripts/run-poland-lag24-calibrated-batches.ps1` for that larger run. It writes
an `attempt_manifest.json`, preserves a fixed `generated_at`, persists raw
Poland-enhanced official NBEATSx/TFT rows by batch, merges persisted rows before
downstream calibration, and then runs the calibrated schedule/value comparison
plus `dfl_poland_lag24_prior_tail_risk_veto_frame`. This is the correct path
for testing the veto on rolling/365 evidence. The claim boundary is unchanged:
Offline Strategy Promotion only, `market_execution_enabled=false`, no
dashboard/API default switch, and no European rows as Ukrainian training rows.

The larger Poland-enhanced 365-anchor evidence run then closed the next decision
point. The feature-consumption audit and rolling gate are documented in
[DFL_POLAND_LAG24_FEATURE_AUDIT_AND_ROLLING_GATE.md](DFL_POLAND_LAG24_FEATURE_AUDIT_AND_ROLLING_GATE.md)
and exported locally to
`data/research_runs/week3_poland_lag24_feature_audit_rolling_gate/`.

Materialized outcome:

- feature columns audited: `24`;
- feature columns that pass the training-consumption audit: `17`;
- blocked feature columns: `7`, all blocked by null coverage;
- timestamp alignment: `lagged_24h_prior_safe`;
- best latest-holdout Poland/TFT row improved frozen V2+ by `3.16%`;
- rolling gate against frozen Ukrainian-only V2+: `1 / 4` windows per Poland
  source row.

Interpretation: Poland is positive shadow evidence, not a promoted strategy.
The data is useful enough to keep, and it does appear in the official training
contract, but it is not yet robust enough to replace frozen Ukrainian-only V2+.
The next ML step is not DT immediately. First repair the blocked spread-feature
coverage, improve the causal feature representation, and add a simple tabular
candidate-value model over schedule candidates. DT/LAVA should start after
better teacher/value labels exist.
