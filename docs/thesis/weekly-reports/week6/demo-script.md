# Сценарій демо: Week 6

## Мета демо

Показати фінальний defendable evidence path: V2+ лишається headline retrospective schedule/value result; історичний `dt_v2_plus` artifact є random-forest exact-mirror diagnostic, не DT/OOS evidence; dashboard показує learned challengers тільки як research/read-model diagnostics, а market execution не ввімкнено.

## Передумови

1. Репозиторій відкрито у `D:\School\GoIT\Courses\Diploma`.
2. Для live UI demo можна запустити local dashboard/API, але сценарій працює і як documentation-first demo.
3. Показувати тільки offline/read-model artifacts. Не називати це market bid, dispatch command або deployed DT controller.

## Крок 1. Показати headline V2+ result

Відкрити:

- [../../chapters/04-results-and-discussion.md](../../chapters/04-results-and-discussion.md)
- [../../appendices/evidence-manifest.md](../../appendices/evidence-manifest.md)

Що сказати:

- Headline result залишається Schedule/Value Learner V2+.
- V2+ має mean regret `174.77` UAH, median regret `67.30` UAH і rolling robustness `4 / 4`.
- Це offline/read-model evidence, а не live trading system.

## Крок 2. Пояснити post-defense random-forest model-lineage correction

Відкрити:

- `runs/dt_v2_plus/aggregate.json`
- `runs/dt_v2_plus/canonical_seed_metrics_manifest.json`
- `runs/dt_v2_plus/seed_42/artifacts/regret_aware_v2_plus_selector_summary.json`

Що сказати:

- Старий `0 / 90` switches result не використовується як висновок: він був stale parsing artifact.
- Double-encoded vector parsing виправлено; manifest має `non_empty_vector_count=720` і `max_vector_length=24` для real vector columns.
- Canonical 3-seed aggregate відтворює `mean_test_regret=168.1566`, але всі seeds мають ідентичний path; training rows exact-mirror evaluation rows.
- На seed summaries selector робить `4 / 90` non-V2+ switches і `86 / 90` abstentions.
- Це construction diagnostic, не positive/OOS evidence; головний blocker — exact mirroring і одна switch date, а не лише threshold `<=166.0` UAH.

Не казати:

- "DT/V2+ promoted".
- "DT is production-ready".
- "Dashboard should default to DT".
- "Market execution is enabled".

## Крок 3. Показати threshold sensitivity

Відкрити:

- `runs/dt_v2_plus_threshold_sensitivity/threshold_0/aggregate.json`
- `runs/dt_v2_plus_threshold_sensitivity/threshold_20/aggregate.json`
- `runs/dt_v2_plus_threshold_sensitivity/threshold_50/aggregate.json`

Що сказати:

- Thresholds `0`, `5`, `10`, `20` дали однаковий secondary result: `168.16` UAH, `4 / 90` switches, `86 / 90` abstentions.
- Threshold `50` лишив mean regret `168.16` UAH, але зменшив switches до `3 / 90`.
- Це означає лише same-packet threshold invariance; exact mirroring не дозволяє називати signal robust або promotion evidence.

## Крок 4. Показати dashboard research badge

Відкрити `/operator` dashboard або кодову точку:

- [../../../dashboard/app/utils/operatorFutureStackPresentation.ts](../../../dashboard/app/utils/operatorFutureStackPresentation.ts)
- [../../../dashboard/app/utils/operatorFutureStackPresentation.test.ts](../../../dashboard/app/utils/operatorFutureStackPresentation.test.ts)

Що сказати:

- Dashboard не перемикає default strategy.
- Research strip показує `Research gate: secondary evidence`.
- Badge явно містить `promotion=false / execution=false`.
- Це demo/diagnostic surface, не operator command.

## Крок 5. Показати verification

Команди, які були виконані:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests\dfl\test_regret_aware_v2_plus_selector.py tests\dfl\test_canonical_experiment_metrics.py
npm -C dashboard exec vitest -- app/utils/operatorFutureStackPresentation.test.ts app/utils/operatorShadowPreviewIntegration.test.ts --run
.\.venv\Scripts\python.exe -m ruff check src\smart_arbitrage\dfl\canonical_experiment_metrics.py src\smart_arbitrage\dfl\regret_aware_v2_plus_selector.py scripts\aggregate_canonical_experiment_metrics.py scripts\materialize_dt_v2_plus_canonical_seed_metrics.py scripts\materialize_regret_aware_v2_plus_selector_packet.py tests\dfl\test_canonical_experiment_metrics.py tests\dfl\test_regret_aware_v2_plus_selector.py
.\.venv\Scripts\python.exe -m mypy src\smart_arbitrage\dfl\canonical_experiment_metrics.py src\smart_arbitrage\dfl\regret_aware_v2_plus_selector.py scripts\aggregate_canonical_experiment_metrics.py scripts\materialize_dt_v2_plus_canonical_seed_metrics.py scripts\materialize_regret_aware_v2_plus_selector_packet.py tests\dfl\test_canonical_experiment_metrics.py tests\dfl\test_regret_aware_v2_plus_selector.py
npm -C dashboard run typecheck
.\.venv\Scripts\dg.exe check defs
```

Що сказати:

- Focused Python tests: `14 passed`.
- Dashboard vitest slice: `9 passed`.
- Ruff, Mypy, Nuxt typecheck, ESLint і Dagster definitions check passed.
- `dg check defs` має лише active-venv warning, definitions load успішно.

## Крок 6. Закрити демо

Фінальний меседж:

> Поточний дипломний результат defendable як credentialless DAM/IDM hourly recommendation preview з V2+ headline retrospective evidence. Історичний `dt_v2_plus` result (`168.16` UAH) створено random forest на exact-mirror packet; усі чотири changed profile rows належать одній delivery date. Це не DT/OOS performance evidence. Dashboard показує challenger лише як research badge, не як default action. `promotion_gate_passed=false`, `market_execution_enabled=false`, і no market submission залишається незмінною межею.
