# Сценарій демо: Week 4

## Мета демо

Показати supervisor-ready Week 4 package: Hugging Face source refresh -> Dnipro 90-anchor real-data calibration -> selector diagnostics -> conservative thesis claim. Демо має підкреслити, що це calibration/selector evidence, а не live DFL.

## Передумови

1. Postgres, Dagster webserver/daemon, MLflow і API запущені через Docker Compose.
2. API доступний на `http://localhost:8000/`.
3. Dagster UI доступний на `http://localhost:3001/`.
4. MLflow UI доступний на `http://localhost:5000/`.

## Крок 1. Показати research intake

Відкрити:

- [docs/thesis/sources/week4-research-ingestion.md](../../sources/week4-research-ingestion.md)

Що сказати:

- Week 4 починається з літературного intake, а не з додавання нової моделі.
- PriceFM і THieF корисні як майбутній forecast-layer context.
- TSFM leakage evaluation є методологічним guardrail для rolling-origin no-leakage evaluation.
- TFMAdapter, Reverso і Distributional RL energy arbitrage лишаються watch-list sources.
- Firecrawl не додавався як dependency.

## Крок 2. Показати run config

Відкрити:

- [configs/real_data_calibration_week4.yaml](../../../../configs/real_data_calibration_week4.yaml)

Що сказати:

- Scope навмисно вузький: `client_003_dnipro_factory`, observed OREE/Open-Meteo, `2026-01-01` - `2026-04-30`, `max_anchors=90`.
- Config запускає існуючі calibration assets і не змінює Dagster asset keys, resources, IO managers, API contracts або dashboard contracts.

## Крок 3. Показати Dagster materialization

Команда, яка була виконана:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,real_data_value_aware_ensemble_frame,dfl_training_frame,regret_weighted_forecast_calibration_frame,regret_weighted_forecast_strategy_benchmark_frame,horizon_regret_weighted_forecast_calibration_frame,horizon_regret_weighted_forecast_strategy_benchmark_frame,calibrated_value_aware_ensemble_frame,forecast_dispatch_sensitivity_frame,risk_adjusted_value_gate_frame -c configs/real_data_calibration_week4.yaml
```

Що сказати:

- Bronze/Silver/Gold lineage лишається тим самим evidence contour.
- `real_data_value_aware_ensemble_frame` включено як потрібну залежність `dfl_training_frame`.
- Нові outputs є calibration/selector read models, не execution engine.
- Run succeeded: `ce705fa2-b100-4b17-a33b-2011409f3e90`.

## Крок 4. Показати raw benchmark API

Відкрити:

- `http://localhost:8000/dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory`

Очікувані ключові поля:

- `data_quality_tier = thesis_grade`
- `anchor_count = 90`
- `model_count = 3`
- `best_model_name = strict_similar_day`

Короткі числа:

| Model | Mean regret UAH | Median regret UAH | Rank-1 wins |
|---|---:|---:|---:|
| `strict_similar_day` | 1384.70 | 999.20 | 48 |
| `nbeatsx_silver_v0` | 2070.28 | 1805.15 | 15 |
| `tft_silver_v0` | 2361.96 | 1985.18 | 27 |

Що сказати:

- Strict similar-day baseline лишається сильним control.
- Це не провал neural models; це чесний evidence result, який показує потребу в calibration.

## Крок 5. Показати calibration та selectors

Відкрити:

- `http://localhost:8000/dashboard/calibrated-ensemble-benchmark?tenant_id=client_003_dnipro_factory`
- `http://localhost:8000/dashboard/risk-adjusted-value-gate?tenant_id=client_003_dnipro_factory`

Короткі числа:

| Read model | Anchors | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|
| `calibrated_value_aware_ensemble_v0` | 90 | 1479.65 | 1037.48 |
| `risk_adjusted_value_gate_v0` | 90 | 1428.59 | 1011.84 |

Що сказати:

- Selectors істотно кращі за середній raw benchmark mix, але strict similar-day still edges them as an individual model.
- Це саме "calibration second": ми не стверджуємо full DFL, а перевіряємо, чи calibration робить neural candidates кориснішими у downstream LP objective.

## Крок 6. Показати dispatch sensitivity

Відкрити:

- `http://localhost:8000/dashboard/forecast-dispatch-sensitivity?tenant_id=client_003_dnipro_factory`

Очікувані ключові поля:

- `anchor_count = 90`
- `model_count = 5`
- `row_count = 450`
- buckets: `forecast_error=399`, `low_regret=40`, `lp_dispatch_sensitivity=3`, `spread_objective_mismatch=8`

Що сказати:

- Цей endpoint пояснює, чому forecast candidate втратив value: forecast error, spread mismatch або LP dispatch sensitivity.
- Це diagnostic read model для диплома, а не торговий сигнал.

## Крок 7. Показати research export

Показати директорію:

- `data/research_runs/week4_calibration_dnipro_90`

Що сказати:

- Export містить concise summaries для benchmark, calibration, selector, sensitivity і risk gate.
- Export command агрегує latest persisted batches у Postgres; acceptance у цьому демо підтверджена через tenant-specific API для Dnipro 90 anchors.
- Ці файли є research evidence artifacts.

## Короткий фінальний меседж

Week 4 показує правильну академічну траєкторію: source refresh обґрунтовує no-leakage rolling-origin evaluation, а 90-anchor calibration run показує, що regret-aware correction покращує neural forecast candidates, але strict similar-day baseline ще лишається дуже сильним. Наступний крок має бути review-driven: all-tenant robustness або narrow DFL pilot, але не live market execution claim.
