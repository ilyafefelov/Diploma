# Щотижневий звіт 3

## 1. Фокус тижня

Після завершення поточного demo walkthrough наступний вертикальний зріз було спрямовано не на розширення dashboard, а на зміцнення дослідницької доказової бази. Мета зрізу: отримати відтворюваний rolling-origin benchmark на observed real data для одного tenant і підготувати артефакти, які можна показувати як thesis-grade evidence.

Фокусний tenant: `client_003_dnipro_factory`. Дані: observed OREE DAM та historical Open-Meteo за період `2026-01-01` - `2026-04-30`. Benchmark cap: `max_anchors=30`.

## 2. Виконані завдання

- Додано tracked Dagster run config: [configs/real_data_benchmark_week3.yaml](../../../../configs/real_data_benchmark_week3.yaml).
- Оновлено research protocol у [docs/technical/RESEARCH_INTEGRATION_PLAN.md](../../../technical/RESEARCH_INTEGRATION_PLAN.md).
- Матеріалізовано Compose-backed Dagster chain:
  `observed_market_price_history_bronze -> tenant_historical_weather_bronze -> real_data_benchmark_silver_feature_frame -> real_data_rolling_origin_benchmark_frame`.
- Виправлено крихкість observed OREE ingestion: Docker-side endpoint потребував паузи між monthly data_view requests, інакше частина місяців повертала порожній результат.
- Експортовано downstream research summaries у `data/research_runs/week3_real_data_benchmark`.
- Перевірено FastAPI read model для `client_003_dnipro_factory`.

## 3. Результат benchmark

API endpoint `/dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory` повернув:

| Поле | Значення |
|---|---:|
| `data_quality_tier` | `thesis_grade` |
| `anchor_count` | 30 |
| `model_count` | 3 |
| `best_model_name` | `strict_similar_day` |
| `mean_regret_uah` | 1236.81 |
| `median_regret_uah` | 868.17 |

Per-model summary для Dnipro batch:

| Model | Rows | Mean regret UAH | Median regret UAH | Rank-1 wins |
|---|---:|---:|---:|---:|
| `strict_similar_day` | 30 | 772.42 | 616.82 | 15 |
| `nbeatsx_silver_v0` | 30 | 1384.01 | 1002.29 | 4 |
| `tft_silver_v0` | 30 | 1554.00 | 1010.31 | 11 |

Поточний висновок: strict similar-day baseline лишається сильним контрольним методом на цьому 30-anchor Dnipro slice. Це не заперечує цінність NBEATSx/TFT, але показує, що наступні neural claims мають проходити через decision-value/regret evaluation, а не лише через forecast accuracy.

## 4. Артефакти

- Run config: [configs/real_data_benchmark_week3.yaml](../../../../configs/real_data_benchmark_week3.yaml)
- Research protocol: [docs/technical/RESEARCH_INTEGRATION_PLAN.md](../../../technical/RESEARCH_INTEGRATION_PLAN.md)
- Demo script: [docs/thesis/weekly-reports/week3/demo-script.md](./demo-script.md)
- Exported summaries: `data/research_runs/week3_real_data_benchmark`
- MLflow run: `deb0633303de4430967aece6767315f2`
- API evidence: `GET /dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory`

## 5. Підготовлено наперед, але не як Week 3 claim

Після стабілізації 30-anchor contour було підготовлено draft calibration path для 90 anchors: `data/research_runs/week4_calibration_dnipro_90` і відповідні Week 4 draft artifacts. Це корисний матеріал для другого демо та пояснювальної записки, але він не замінює Week 3 acceptance result. У цьому звіті Week 3 headline лишається вузьким і перевіреним: 30 rolling-origin anchors для `client_003_dnipro_factory`, observed-only provenance, три raw forecast candidates і thesis-grade API read model.

90-anchor calibration треба описувати тільки як підготовлений наступний крок: calibration/selector evidence, не full DFL, не market execution і не доказ того, що neural strategy вже перемагає baseline.

## 6. Ризики та виклики

| Ризик / виклик | Чому це важливо | Відповідь |
|---|---|---|
| OREE data_view чутливий до частоти monthly requests | Без throttling observed-only backfill може хибно виглядати як неповний датасет | Додано паузу між monthly requests і тест на pacing |
| Export script агрегує latest persisted batches для всіх tenants | Week 3 slice фокусується на одному tenant, але export total має більше рядків | У звіті явно відділено Dnipro batch: 30 anchors / 90 benchmark rows |
| Neural candidates програють strict control на цьому slice | Не можна робити SOTA claim із forecast models без value evidence | Наступний крок: calibration/robustness і ширший rolling-origin protocol |
| DFL pilot outputs вже генеруються downstream | Є ризик описати їх як full DFL або live strategy | Усі матеріали маркують їх як downstream research evidence, not market execution |

## 7. План на наступний тиждень

1. Підготувати друге демо на основі вже перевіреного Week 3 benchmark contour і, за потреби, показати 90-anchor calibration як prepared-ahead preview.
2. Підготувати перші submission-ready draft sections: `Вступ` і `Огляд літератури`, прив'язавши літературу до rolling-origin no-leakage benchmark, exogenous forecasting і regret-aware calibration.
3. Використати 90-anchor calibration тільки як Week 4 calibration/selector evidence: raw forecast candidates окремо, calibrated candidates окремо, selector diagnostics окремо.
4. Розширювати benchmark на all-tenants лише після supervisor review, бо Dnipro 30-anchor path уже виконав Week 3 acceptance target.
5. Не переходити до full DFL або market execution claim, поки strict LP/oracle benchmark і calibration semantics не узгоджені з керівником.

## 8. Короткий висновок

Week 3 slice перевів проєкт від demo-ready operator surface до першого відтворюваного thesis-grade evidence path. Для `client_003_dnipro_factory` отримано 30 rolling-origin anchors із observed-only provenance, трьома forecast candidates і oracle-regret scoring через той самий LP контур. Поточний контрольний baseline сильніший за compact neural candidates на цьому slice, тому наступний research крок має бути calibration/robustness, а не передчасний live DFL.
