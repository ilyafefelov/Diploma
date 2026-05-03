# Сценарій демо: Week 2

## Мета демо

Показати, що Slice 1 і Slice 2 вже працюють як зв’язаний operator-facing MVP:
- Slice 1: вибір tenant, weather run-config preview, weather materialization, backend-owned status
- Slice 2: baseline forecast, feasible signed MW recommendation, projected SOC, UAH economics

## Формат

- Тривалість: 7-10 хвилин
- Основний інтерфейс: [dashboard](d:/School/GoIT/Courses/Diploma/dashboard)
- Підтримуючі артефакти: FastAPI docs, Dagster UI, MLflow UI
- Мова пояснення: українська

## Передумови

1. API доступний на `http://127.0.0.1:8010/`
2. Dashboard доступний на `http://localhost:64163/`
3. Dagster UI доступний на `http://127.0.0.1:3000/`
4. MLflow UI доступний на `http://127.0.0.1:5000/`

## Частина 1. Slice 1: tenant-aware weather control

### Крок 1. Показати operator dashboard

Що відкрити:
- `http://localhost:64163/`

Що сказати:
- Це операторська поверхня для поточного MVP.
- На цьому етапі UI працює через same-origin Nuxt proxy, а backend зберігає canonical read models і status semantics.
- Верхній блок показує registry state, selected tenant і control surface status.

### Крок 2. Вибрати tenant

Що зробити:
- У `Lot selector` залишити або вибрати `Dnipro Manufacturing Plant`

Що сказати:
- Вибір tenant визначає локацію, на якій будуються weather-aware preview і baseline recommendation.
- Для MVP це tenant-aware control plane, а не multi-tenant production orchestrator у повному сенсі.

### Крок 3. Показати weather run-config preview

Що зробити:
- Натиснути `Prepare run config`

Що очікувати:
- Статус weather slice переходить у `Run config prepared`
- З’являється `Resolved location`
- У motive bars змінюється `Weather readiness`

Що сказати:
- Це перший operator-visible read model: backend підтверджує, що конкретний tenant уже має готовий weather run-config.
- Тут ще немає dispatch semantics; це лише підготовка bronze/materialization slice.

### Крок 4. Показати weather materialization outcome

Що зробити:
- Натиснути `Materialize weather`

Що очікувати:
- UI показує `Latest weather slice completed`
- Вказані assets materialized
- Dashboard залишається на backend-owned latest state

Що сказати:
- Цей крок демонструє persisted operator status для `weather_control`.
- Для dev-середовища це працює навіть без Postgres DSN, бо є in-memory fallback store.

## Частина 2. Slice 2: baseline LP recommendation preview

### Крок 5. Перейти до baseline recommendation surface

Що показати:
- Блок `Baseline LP recommendation surface`

Що сказати:
- Це перша Slice 2 operator-facing analytical surface.
- Вона показує не bid intent, а recommendation preview для operator review.

### Крок 6. Пояснити forecast horizon

Що показати:
- Графік `Hourly DAM baseline forecast`

Що сказати:
- Forecast формується в рамках Level 1 baseline LP preview.
- Для цього slice forecast використовується як вхід до recommendation preview, а не як самодостатній прогнозний продукт.

### Крок 7. Пояснити feasible plan

Що показати:
- Графік `Signed MW schedule and projected SOC`

Що сказати:
- Синя серія показує feasible signed MW recommendation.
- Зелена серія показує projected SOC на тому ж горизонті.
- Це вже constrained result: враховано `soc_min`, `soc_max`, capacity, max power, round-trip efficiency.

### Крок 8. Пояснити economics

Що показати:
- Плашки `Gross value`, `Degradation`, `Net value`, `Throughput`

Що сказати:
- Тут важливо, що degradation penalty винесено окремо, а не змішано з gross market value.
- Це допомагає показати, що економіка батареї в MVP враховується явно.
- Коректніше називати цей battery layer feasibility-and-economics preview model: він уже враховує SOC, ліміти потужності, спрощений ККД і throughput penalty, але ще не є full digital twin.

### Крок 9. Зафіксувати scope boundary

Що показати:
- Текст `Recommendation preview only, not bid intent`
- Текст `Planning boundary`

Що сказати:
- Поточний Slice 2 не повертає `Proposed Bid`, `Cleared Trade` або `Dispatch Command`.
- Це свідоме обмеження MVP, щоб не змішувати operator preview з майбутніми market execution semantics.
- Multi-market bidding і DRL/DFL належать до наступної фази, а не до поточного demo-stage.

## Частина 3. Підтверджувальні артефакти

### Крок 10. Показати FastAPI docs

Що відкрити:
- `http://127.0.0.1:8010/docs`

Що сказати:
- Тут видно control-plane endpoints для weather status, projected battery state і baseline LP preview.
- Це корисно для пояснення contract-first підходу в MVP.

### Крок 11. Показати Dagster і MLflow як supporting surfaces

Що відкрити:
- Dagster UI: `http://127.0.0.1:3000/`
- MLflow UI: `http://127.0.0.1:5000/`

Що сказати:
- Dagster підтверджує data/asset lineage.
- MLflow підтверджує baseline experiment tracking.
- Для цього тижня ці системи є supporting evidence, а не основною demo surface.

## Короткий фінальний меседж

> На цьому етапі MVP уже має operator-facing контур для двох пов’язаних slices. Slice 1 показує tenant-aware weather control flow з backend-owned status, а Slice 2 показує baseline LP recommendation preview з projected SOC, UAH economics і явним degradation-aware feasibility-and-economics preview model. Обидва slices уже придатні для технічного демо, але ще не претендують на повний market execution контур, DRL/DFL execution layer або повний battery digital twin.
