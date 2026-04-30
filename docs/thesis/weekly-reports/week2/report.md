# Щотижневий звіт 2

## 1. Прогрес тижня та зафіксований MVP-результат

На другому тижні інженерний MVP було розширено від baseline backend-контуру до операторської demo-поверхні, придатної для першого технічного демо. Якщо наприкінці Week 1 система вже мала працездатний Level 1 LP baseline у Dagster та MLflow, то на цьому етапі сформовано операторський control-plane шар: FastAPI read models, same-origin Nuxt proxy та візуальний dashboard для Slice 1 і Slice 2.

Поточний підтверджений scope залишається свідомо обмеженим. Slice 1 охоплює tenant registry, weather run-config preview, weather materialization і backend-owned operator status для `weather_control` та `signal_preview`. Slice 2 охоплює projected battery state simulator, baseline LP preview read model та dashboard surface для baseline forecast, feasible signed MW recommendation, projected SOC trace і UAH economics. Ці результати є recommendation preview і operator review surface, але не є реалізацією `Proposed Bid`, `Cleared Trade` або `Dispatch Command` як market-execution semantics.

## 2. Виконані завдання, досягнення та зміни в проєкті

- Підтверджено локальний tracker flow і зафіксовано його в [docs/technical/TRACKER_FLOW.md](d:/School/GoIT/Courses/Diploma/docs/technical/TRACKER_FLOW.md).
- Реалізовано persisted operator status read model у [src/smart_arbitrage/resources/operator_status_store.py](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/resources/operator_status_store.py) з in-memory fallback для dev-середовища без Postgres DSN.
- Розширено control-plane API в [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py):
  - `GET /dashboard/operator-status`
  - `POST /dashboard/projected-battery-state`
  - `GET /dashboard/baseline-lp-preview`
- Оновлено технічну документацію endpoint-ів у [docs/technical/API_ENDPOINTS.md](d:/School/GoIT/Courses/Diploma/docs/technical/API_ENDPOINTS.md).
- Реалізовано dashboard Slice 1 через [dashboard/app/composables/useWeatherControls.ts](d:/School/GoIT/Courses/Diploma/dashboard/app/composables/useWeatherControls.ts) і backend-owned status sync.
- Реалізовано dashboard Slice 2 через:
  - [dashboard/app/composables/useBaselinePreview.ts](d:/School/GoIT/Courses/Diploma/dashboard/app/composables/useBaselinePreview.ts)
  - [dashboard/app/components/dashboard/HudBaselinePreview.vue](d:/School/GoIT/Courses/Diploma/dashboard/app/components/dashboard/HudBaselinePreview.vue)
  - [dashboard/server/api/control-plane/dashboard/baseline-lp-preview.get.ts](d:/School/GoIT/Courses/Diploma/dashboard/server/api/control-plane/dashboard/baseline-lp-preview.get.ts)
- Створено projected battery state simulator у [src/smart_arbitrage/optimization/projected_battery_state.py](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/optimization/projected_battery_state.py).
- Підготовлено й зафіксовано PRD та issue backlog для operator MVP slices у [docs/technical/PRD-operator-mvp-slices.md](d:/School/GoIT/Courses/Diploma/docs/technical/PRD-operator-mvp-slices.md) і [docs/technical/issues](d:/School/GoIT/Courses/Diploma/docs/technical/issues).
- Усі focused API тести для control-plane slices проходять: `12 passed` у [tests/api/test_main.py](d:/School/GoIT/Courses/Diploma/tests/api/test_main.py).
- Dashboard production build проходить успішно, а baseline LP preview реально відображається в браузері на `http://localhost:3611/`.

## 3. Висновки після першого демо-рівня готовності

На кінець другого тижня проєкт має не лише baseline optimization contour, а й операторську оболонку, через яку можна демонструвати взаємозв’язок між tenant selection, weather slice і baseline recommendation preview. Це важливо для пояснення дипломному керівнику, що система розвивається як керований MLOps/AI engineering продукт, а не як набір розрізнених моделей.

Ключовий висновок цього тижня полягає в тому, що backend-owned read models виявилися правильним кордоном між обчислювальною логікою та dashboard UX. Завдяки цьому вдалося показати операторський сценарій без блокування на production-grade persistence або повній market execution semantics.

## 4. Ризики та виклики

| Ризик / виклик | Чому це важливо | Запланована відповідь |
|---|---|---|
| `operator-status` у live UI може повертати `404/502` для flows без запису в store | Це створює шум у консолі і погіршує сприйняття demo-ready surface | Додати більш м’який frontend handling для відсутнього status record без підсвічування як control-plane error |
| Slice 2 preview зараз використовує tenant-aware synthetic DAM history biasing | Для демо цього достатньо, але для thesis narrative треба чітко відділити synthetic preview від live market execution | Зберегти recommendation-preview framing і окремо позначити майбутню інтеграцію з повним market data path |
| Є ризик переплутати recommendation preview з market semantics | Це методологічно критично для пояснювальної записки й демо | У документації та UI явно повторювати boundary: не bid, не clearing, не dispatch |
| Відсутній зовнішній issue automation через `gh` CLI | Це не блокує локальний розвиток, але ускладнює зовнішній tracker flow | Залишити canonical local backlog у `docs/technical/issues/` до окремого налаштування GitHub tooling |
| У dashboard build присутні warnings про sourcemaps і chunk size | Це не ламає MVP, але лишає технічний борг перед публічною демонстрацією | Не розширювати scope зараз; зафіксувати як post-MVP cleanup |

## 5. План роботи на наступний тиждень

1. Підчистити UX-обробку відсутніх `operator-status` записів, щоб live demo не показувало зайвий control-plane warning там, де flow ще не ініційовано.
2. Підготувати другий демо-сценарій навколо повної історії: tenant registry -> weather slice -> baseline LP recommendation surface.
3. Оновити матеріали для пояснювальної записки так, щоб Slice 1 і Slice 2 були зафіксовані як завершений MVP-level результат.
4. Вирішити, чи потрібні додаткові скриншоти dashboard surface для supervisor-ready звіту або короткого відео.
5. Не розширювати scope на DFL execution semantics, IDM або balancing до завершення packaging та supervisor demo.

## 6. Артефакти

- Код backend: [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py), [src/smart_arbitrage/resources/operator_status_store.py](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/resources/operator_status_store.py), [src/smart_arbitrage/optimization/projected_battery_state.py](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/optimization/projected_battery_state.py)
- Код dashboard: [dashboard/app/pages/index.vue](d:/School/GoIT/Courses/Diploma/dashboard/app/pages/index.vue), [dashboard/app/components/dashboard/HudBaselinePreview.vue](d:/School/GoIT/Courses/Diploma/dashboard/app/components/dashboard/HudBaselinePreview.vue), [dashboard/app/composables/useBaselinePreview.ts](d:/School/GoIT/Courses/Diploma/dashboard/app/composables/useBaselinePreview.ts), [dashboard/app/composables/useWeatherControls.ts](d:/School/GoIT/Courses/Diploma/dashboard/app/composables/useWeatherControls.ts)
- Тести: [tests/api/test_main.py](d:/School/GoIT/Courses/Diploma/tests/api/test_main.py)
- Документація: [docs/technical/API_ENDPOINTS.md](d:/School/GoIT/Courses/Diploma/docs/technical/API_ENDPOINTS.md), [docs/technical/TRACKER_FLOW.md](d:/School/GoIT/Courses/Diploma/docs/technical/TRACKER_FLOW.md), [docs/technical/PRD-operator-mvp-slices.md](d:/School/GoIT/Courses/Diploma/docs/technical/PRD-operator-mvp-slices.md), [docs/technical/OPERATOR_DEMO_READY.md](d:/School/GoIT/Courses/Diploma/docs/technical/OPERATOR_DEMO_READY.md)
- Weekly demo materials: [docs/thesis/weekly-reports/week2/demo-script.md](d:/School/GoIT/Courses/Diploma/docs/thesis/weekly-reports/week2/demo-script.md)
- Live surfaces:
  - Dashboard: `http://localhost:3611/`
  - FastAPI docs: `http://127.0.0.1:8010/docs`
  - Dagster UI: `http://127.0.0.1:3000/`
  - MLflow UI: `http://127.0.0.1:5000/`
- Verified implementation commits:
  - `86f44c5` — Persist operator flow status read models
  - `cdb01b6` — Sync dashboard weather state with operator status read model
  - `25ab328` — Add projected battery state simulator preview
  - `13e482b` — Add baseline LP preview read model
  - `f8792f0` — Surface baseline LP preview in dashboard

## 7. Короткий висновок

Другий тиждень завершено з готовим demo-level operator MVP для Slice 1 і Slice 2. Практично це означає, що в проєкті вже можна послідовно показати tenant-aware weather control flow і baseline LP recommendation preview з projected SOC та UAH economics, при цьому не змішуючи preview semantics із майбутнім market execution контуром.