# Сценарій демо: Week 3

## Мета демо

Показати перехід від operator MVP demo до thesis-grade evidence path: observed OREE DAM + historical weather -> Silver benchmark frame -> Gold rolling-origin benchmark -> FastAPI read model -> research exports.

## Передумови

1. Postgres, Dagster webserver/daemon, MLflow і API запущені через Docker Compose.
2. API доступний на `http://localhost:8000/`.
3. Dagster UI доступний на `http://localhost:3001/`.
4. MLflow UI доступний на `http://localhost:5000/`.

## Крок 1. Показати run config

Відкрити:

- [configs/real_data_benchmark_week3.yaml](../../../../configs/real_data_benchmark_week3.yaml)

Що сказати:

- Це tracked config для Week 3 evidence slice.
- Scope навмисно вузький: один tenant, observed window `2026-01-01` - `2026-04-30`, `max_anchors=30`.
- Цей config не змінює asset keys, API contracts або dashboard semantics.

## Крок 2. Показати Dagster lineage

Команда, яка була виконана:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame -c configs/real_data_benchmark_week3.yaml
```

Що сказати:

- Bronze layer бере observed OREE DAM і historical Open-Meteo.
- Silver layer створює tenant-expanded benchmark feature frame.
- Gold layer робить rolling-origin evaluation: кожен anchor бачить лише минуле, forecast candidates проходять через один LP contour, результат скориться проти oracle LP.

## Крок 3. Показати API evidence

Відкрити:

- `http://localhost:8000/dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory`

Очікувані ключові поля:

- `data_quality_tier = thesis_grade`
- `anchor_count = 30`
- `model_count = 3`
- `best_model_name = strict_similar_day`

Що сказати:

- Це вже не synthetic demo history.
- API response є read model для benchmark evidence, не для bid submission.
- `mean_regret_uah` і `median_regret_uah` показують lost value against oracle, а не forecast-only score.

## Крок 4. Пояснити model comparison

Короткі числа для Dnipro batch:

| Model | Mean regret UAH | Median regret UAH | Rank-1 wins |
|---|---:|---:|---:|
| `strict_similar_day` | 772.42 | 616.82 | 15 |
| `nbeatsx_silver_v0` | 1384.01 | 1002.29 | 4 |
| `tft_silver_v0` | 1554.00 | 1010.31 | 11 |

Що сказати:

- Strict control поки сильніший на цьому 30-anchor slice.
- Neural forecasts не можна просувати як better strategy без calibration і decision-value доказів.
- Це саме той результат, який корисний для диплома: він показує чесний benchmark, а не підганяє висновок під очікувану модель.

## Крок 5. Показати research exports

Показати директорію:

- `data/research_runs/week3_real_data_benchmark`

Що сказати:

- Export містить summaries для downstream research layer: DFL-ready table, regret-weighted calibration, sensitivity buckets, ensemble/gate diagnostics.
- Поточний export command агрегує latest persisted batches для всіх tenants, але Week 3 acceptance target стосується Dnipro batch: 30 anchors / 90 benchmark rows.
- Ці файли є research evidence artifacts, не production model registry і не live policy.

## Крок 6. Зафіксувати scope boundary

Що сказати:

> Week 3 не додає market execution і не робить full DFL claim. Він закриває критичний методологічний gap: тепер є observed-only rolling-origin benchmark, через який можна чесно порівнювати strict control, NBEATSx і TFT за regret та decision value.

## Короткий фінальний меседж

Після Week 3 проєкт має не лише operator-facing MVP, а й перший thesis-grade evidence contour. Для одного tenant підтверджено observed-only provenance, 30 rolling-origin anchors, 3 forecast candidates, oracle-regret scoring і downstream research exports. Наступний безпечний крок: розширення на all-tenants / 90 anchors і calibration robustness, а не передчасне оголошення live DFL strategy.
