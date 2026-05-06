# Щотижневий звіт 4

## 1. Фокус тижня

Week 4 був спрямований на supervisor-ready research package: спочатку оновити source map через Hugging Face paper metadata, потім перевірити regret-aware calibration на вже стабільному real-data benchmark. Нових моделей, dashboard UI або market-execution semantics у цьому зрізі не додавалося.

Фокусний tenant: `client_003_dnipro_factory`. Дані: observed OREE DAM та historical Open-Meteo за період `2026-01-01` - `2026-04-30`. Benchmark cap: `max_anchors=90`.

## 2. Виконані завдання

- Зафіксовано Week 4 research intake у [docs/thesis/sources/week4-research-ingestion.md](../../sources/week4-research-ingestion.md): `include`, `watch` та `exclude` статуси для PriceFM, THieF, TSFM leakage evaluation, TFMAdapter, Reverso і Distributional RL energy arbitrage.
- Підтверджено, що [docs/thesis/chapters/02-literature-review.md](../../chapters/02-literature-review.md) уже містить Week 4 literature bridge: real-data benchmark, leakage prevention, exogenous forecasting і обмеження "calibration before DFL".
- Запущено Compose-backed Dagster materialization для існуючих assets без зміни asset keys, Pydantic contracts, resources, IO managers або dashboard contracts.
- Експортовано downstream research summaries у `data/research_runs/week4_calibration_dnipro_90`.
- Перевірено FastAPI read models для Dnipro 90-anchor run.

## 3. Результат Dnipro 90-anchor benchmark

API endpoint `/dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory` повернув:

| Поле | Значення |
|---|---:|
| `data_quality_tier` | `thesis_grade` |
| `anchor_count` | 90 |
| `model_count` | 3 |
| `best_model_name` | `strict_similar_day` |
| `mean_regret_uah` | 1938.98 |
| `median_regret_uah` | 1493.47 |

Per-model summary для raw forecast candidates:

| Model | Rows | Mean regret UAH | Median regret UAH | Rank-1 wins |
|---|---:|---:|---:|---:|
| `strict_similar_day` | 90 | 1384.70 | 999.20 | 48 |
| `nbeatsx_silver_v0` | 90 | 2070.28 | 1805.15 | 15 |
| `tft_silver_v0` | 90 | 2361.96 | 1985.18 | 27 |

Висновок: strict similar-day лишається найсильнішим individual control на Dnipro 90-anchor slice. Це важливо для диплома: neural candidates не можна просувати як кращу стратегію без regret-aware calibration та selector evidence.

## 4. Calibration та selector evidence

Horizon-aware regret-weighted calibration покращила обидва neural candidates порівняно з raw variants:

| Model | Rows | Mean regret UAH | Median regret UAH | Rank-1 wins |
|---|---:|---:|---:|---:|
| `strict_similar_day` | 90 | 1384.70 | 999.20 | 36 |
| `tft_horizon_regret_weighted_calibrated_v0` | 90 | 1727.29 | 1196.85 | 20 |
| `nbeatsx_horizon_regret_weighted_calibrated_v0` | 90 | 1804.38 | 1471.52 | 10 |
| `nbeatsx_silver_v0` | 90 | 2070.28 | 1805.15 | 8 |
| `tft_silver_v0` | 90 | 2361.96 | 1985.18 | 16 |

Selector read models:

| Endpoint | Anchors | Model count | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|---:|
| `/dashboard/calibrated-ensemble-benchmark` | 90 | 1 | 1479.65 | 1037.48 |
| `/dashboard/risk-adjusted-value-gate` | 90 | 1 | 1428.59 | 1011.84 |
| `/dashboard/forecast-dispatch-sensitivity` | 90 | 5 | n/a | n/a |

Dispatch sensitivity returned 450 diagnostic rows: `forecast_error=399`, `low_regret=40`, `lp_dispatch_sensitivity=3`, `spread_objective_mismatch=8`.

Interpretation: calibration materially improves TFT/NBEATSx relative to their raw versions, and the risk-adjusted gate is close to strict similar-day, but strict similar-day remains the best individual control by mean regret. This is calibration/selector evidence, not full DFL and not market execution.

## 5. Артефакти

- Week 4 run config: [configs/real_data_calibration_week4.yaml](../../../../configs/real_data_calibration_week4.yaml)
- Research intake: [docs/thesis/sources/week4-research-ingestion.md](../../sources/week4-research-ingestion.md)
- Literature review draft: [docs/thesis/chapters/02-literature-review.md](../../chapters/02-literature-review.md)
- Research protocol: [docs/technical/RESEARCH_INTEGRATION_PLAN.md](../../../technical/RESEARCH_INTEGRATION_PLAN.md)
- Demo script: [docs/thesis/weekly-reports/week4/demo-script.md](./demo-script.md)
- Exported summaries: `data/research_runs/week4_calibration_dnipro_90`
- Dagster run id: `ce705fa2-b100-4b17-a33b-2011409f3e90`
- MLflow runs: benchmark `2f1248a3822f4785af5332e867e09953`, regret calibration `89389bea2c62495a99d1581ba7514d90`, horizon calibration `041bbbe236dd438393e442f9dbff3d59`, calibrated ensemble `fed333f97e9b4e33be2f6adab1415f17`, risk gate `e53ce78fdc1d462f9622e7d660241b20`

## 6. Ризики та виклики

| Ризик / виклик | Чому це важливо | Відповідь |
|---|---|---|
| Export script агрегує latest persisted batches для всіх tenants | Week 4 acceptance target стосується Dnipro 90 anchors, але export totals більші | У звіті окремо наведено API-verified Dnipro metrics; export path лишається supporting artifact |
| Calibration покращує neural candidates, але не перемагає strict control | Не можна робити SOTA або DFL claim | Результат оформлено як calibration/selector evidence |
| Hugging Face sources можуть виглядати як імплементаційний backlog | Додавання foundation models зараз розширило б scope і dependencies | Source refresh використано тільки як literature map |
| Selector endpoints можуть бути сприйняті як live strategy | Це дослідницькі read models, не bid/dispatch engine | У report/demo явно вказано "not market execution" |

## 7. План на наступний тиждень

1. Підготувати короткий supervisor package для першої submission-ready версії `Вступ` і `Огляд літератури`, використовуючи вже оновлені [01-project-overview.md](../../chapters/01-project-overview.md) та [02-literature-review.md](../../chapters/02-literature-review.md).
2. Розширювати benchmark на all-tenants лише після узгодження з керівником, бо Dnipro 90-anchor slice уже показав змістовний calibration result.
3. Додати thesis narrative про негативні/нейтральні результати як сильну сторону методології: pipeline не підганяє висновок під neural model.
4. Підготувати наступний технічний зріз тільки після review: або all-tenant robustness, або narrow DFL pilot against the fixed benchmark.

## 8. Короткий висновок

Week 4 закрив другий evidence slice: літературний source refresh тепер прямо пов'язаний із реальним Dnipro 90-anchor calibration run. Поточний результат підтримує обережний thesis claim: regret-aware calibration покращує neural forecast candidates, але strict similar-day baseline лишається дуже сильним control. Це правильна підстава для подальшого DFL pilot, але ще не доказ full DFL або live market execution.
