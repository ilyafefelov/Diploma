# Щотижневий звіт 1

## 1. Статус першого звіту, значущість проєкту та межі поточного MVP

Цей документ є канонічною submission-версією першого weekly report і консолідує весь фактичний прогрес за звітний тиждень. Мета дипломного проєкту полягає у побудові керованої системи автономного енергоарбітражу для BESS на ринку України 2026, яка поєднує збір ринкових і погодних даних, прогнозний шар, оптимізацію рішення, урахування деградації та operator-facing поверхню для перевірки результатів.

Поточний MVP свідомо обмежено Level 1 сценарієм: погодинний DAM, канонічна валюта UAH, strict similar-day forecast, LP baseline з rolling-horizon логікою та Pydantic gatekeeper для фізичних обмежень. Наукова траєкторія проєкту спрямована на перехід від Predict-then-Optimize до Decision-Focused Learning, але DFL, multi-market control і повний digital twin не є вже реалізованим результатом цього звітного тижня.

Поточний батарейний шар слід описувати не як повну фізичну симуляцію, а як feasibility-and-economics preview model. У коді він виконує погодинний projected SOC preview, перевіряє feasible power в межах `max_power_mw`, SOC-вікна та round-trip efficiency і додає інтервальний degradation penalty в UAH. Цього достатньо для baseline optimization, regret-aware evaluation і operator preview, але недостатньо для claims про electrochemical ageing model.

## 2. Що фактично реалізовано за звітний тиждень

- Консолідовано доменний словник і межі предметної області в [CONTEXT.md](CONTEXT.md) та оновлено [AGENTS.md](AGENTS.md), де `docs/syllabus` зафіксовано як академічне source of truth.
- Реалізовано strict Pydantic V2 контракти в [src/smart_arbitrage/gatekeeper/schemas.py](src/smart_arbitrage/gatekeeper/schemas.py): `BatteryPhysicalMetrics`, `NoBid`, `ClearedTrade`, `DispatchCommand`, а також UAH-native economic semantics для degradation cost.
- Реалізовано погодинний DAM LP baseline у [src/smart_arbitrage/assets/gold/baseline_solver.py](src/smart_arbitrage/assets/gold/baseline_solver.py) з strict similar-day forecast, rolling horizon і degradation penalty в objective.
- Реалізовано Dagster asset graph у [src/smart_arbitrage/assets/mvp_demo.py](src/smart_arbitrage/assets/mvp_demo.py), який об’єднує Bronze, Silver і Gold layers та логування regret у MLflow.
- Реалізовано Bronze ingestion у [src/smart_arbitrage/assets/bronze/market_weather.py](src/smart_arbitrage/assets/bronze/market_weather.py): live OREE DAM price overlay, Open-Meteo weather fetch, tenant-aware location resolution і synthetic fallback для demo/stability.
- Реалізовано projected battery state simulator у [src/smart_arbitrage/optimization/projected_battery_state.py](src/smart_arbitrage/optimization/projected_battery_state.py) для hourly SOC trace, feasible MW trace і degradation-aware preview.
- Реалізовано persisted operator status read model у [src/smart_arbitrage/resources/operator_status_store.py](src/smart_arbitrage/resources/operator_status_store.py) з Postgres-backed store і in-memory fallback для dev-середовища.
- Розширено control-plane API в [api/main.py](../../../../api/main.py) endpoint-ами `GET /tenants`, `POST /weather/run-config`, `POST /weather/materialize`, `GET /dashboard/signal-preview`, `GET /dashboard/operator-status`, `POST /dashboard/projected-battery-state`, `GET /dashboard/baseline-lp-preview`.
- Реалізовано operator-facing dashboard surfaces в [dashboard](../../../../dashboard): tenant selection, weather control, operator status sync і baseline LP preview.
- Підготовлено перший submission-ready пакет розділів пояснювальної записки: [docs/thesis/chapters/01-project-overview.md](../../chapters/01-project-overview.md), [docs/thesis/chapters/02-literature-review.md](../../chapters/02-literature-review.md) і source map у [docs/technical/papers/README.md](../../../technical/papers/README.md).
- Підтверджено реальний Dagster asset graph через `uv run dg list defs --json` із `PYTHONPATH=.;src`; зареєстровані assets: `weather_forecast_bronze`, `dam_price_history`, `strict_similar_day_forecast`, `baseline_dispatch_plan`, `validated_dispatch_command`, `oracle_benchmark_metrics`, `baseline_regret_tracking` та інші demo-support assets.
- Focused API tests у [tests/api/test_main.py](../../../../tests/api/test_main.py) покривають ключові control-plane slices, а для dashboard виправлено локальну proxy-невідповідність у [dashboard/server/api/control-plane/tenants.get.ts](../../../../dashboard/server/api/control-plane/tenants.get.ts), щоб tenant selector використовував той самий API port, що й інші dashboard routes.

На кінець звітного тижня проєкт уже має не лише baseline optimization contour, а й operator demo surface, через яку можна показати шлях від tenant selection до weather-aware preview, baseline recommendation і projected SOC trace.

## 3. Чому зараз використовується проста економічна модель penalty за деградацію

Поточний baseline навмисно використовує просту degradation-aware economics model, а не повний battery physics stack. У [src/smart_arbitrage/gatekeeper/schemas.py](src/smart_arbitrage/gatekeeper/schemas.py) економічний контракт задано як:

$$
MC_{deg} = \frac{C_{cycle}}{2 \cdot Capacity_{mwh}}
$$

де `degradation_cost_per_cycle_uah` перетворюється в `degradation_cost_per_mwh_throughput_uah`. Далі в LP-baseline penalty для інтервалу дорівнює:

$$
Penalty_t = MC_{deg} \cdot Throughput_t
$$

Для поточного demo-профілю батареї це значення описується як прозорий capex-throughput proxy з публічних assumptions: видимий capex anchor `210 USD/kWh` із Grimaldi et al.; utility-scale assumptions `15-year lifetime` і `~1 cycle/day` з NREL ATB; офіційний курс НБУ `43.9129 UAH/USD` на `04.05.2026`.

$$
C_{cycle,proxy} = \frac{210 \cdot 1000 \cdot 10 \cdot 43.9129}{15 \cdot 365} = 16\,843.3\ \text{UAH/cycle}
$$

$$
MC_{deg,proxy} = \frac{16\,843.3}{2 \cdot 10} = 842.2\ \text{UAH/MWh throughput}
$$

Тобто поточний MVP уже використовує не «магічне число», а репродукований economic proxy. Водночас академічно коректно далі називати його саме proxy для preview model, а не універсальною фізичною константою деградації LFP-батареї.

У поточній реалізації `Throughput_t` обчислюється з погодинного charge/discharge plan. У [src/smart_arbitrage/assets/gold/baseline_solver.py](src/smart_arbitrage/assets/gold/baseline_solver.py) це робиться як `(charge_mw + discharge_mw) * dt_hours`, а в [src/smart_arbitrage/optimization/projected_battery_state.py](src/smart_arbitrage/optimization/projected_battery_state.py) для operator preview використовується `abs(feasible_net_power_mw) * dt_hours`.

Причини такого вибору на поточному етапі:

1. Модель сумісна з LP-постановкою й не руйнує детермінований baseline, який потрібен як контрольна група для подальшого DFL comparison.
2. Вона використовує тільки ті сигнали, які реально є в поточному MVP: `capacity_mwh`, `max_power_mw`, `round_trip_efficiency`, SOC bounds і погодинний schedule.
3. Вона вже дає operator-facing економічний сигнал у канонічних UAH units, а не лише post-hoc KPI.
4. Вона достатньо проста, щоб її можна було пояснити керівнику, валідувати тестами й використати в early demo без надмірних фізичних припущень.

Водночас ця модель не охоплює temperature effects, C-rate sensitivity, path-dependent ageing, calendar degradation, SEI dynamics або оновлення SOH як стану цифрового двійника. Тому в письмовому обґрунтуванні потрібно чітко розділяти: зараз реалізовано economic degradation penalty для baseline і preview; richer digital twin є planned research/engineering step.

## 4. Як зараз реально працює весь прохід даних

Поточний прохід даних у коді виглядає так:

1. **Tenant registry і location resolution.** API читає tenant metadata з [simulations/tenants.yml](../../../../simulations/tenants.yml) або fallback registry й повертає location-aware entries через `GET /tenants`.
2. **Weather run-config.** `POST /weather/run-config` будує Dagster config для `weather_forecast_bronze` і явно фіксує `tenant_id`, coordinates і timezone.
3. **Bronze weather ingestion.** Asset `weather_forecast_bronze` робить live fetch до Open-Meteo, додає погодні та solar-derived features, а в разі помилки переходить на synthetic weather fallback.
4. **Bronze market ingestion.** Функція `build_demo_market_price_history()` формує synthetic DAM history, після чого `_fetch_oree_data_view_prices()` та HTML fallback `_fetch_oree_prices()` накладають live OREE rows поверх synthetic base.
5. **Bronze enrichment.** `dam_price_history` збагачується weather features через `enrich_market_price_history_with_weather()`; у результаті формується tenant-aware hourly price history з weather context.
6. **Silver forecast.** Asset `strict_similar_day_forecast` будує deployable hourly forecast за canonical rule: для Tue-Fri використовується lag 24h, а для Mon/Sat/Sun lag 168h.
7. **Gold optimization.** Asset `baseline_dispatch_plan` запускає `HourlyDamBaselineSolver`, який оптимізує charge/discharge schedule, SOC trajectory і degradation-aware net objective.
8. **Gold validation and economics.** `validated_dispatch_command` виконує final safety check, `blocked_dispatch_command_demo` демонструє HOLD fallback, `oracle_benchmark_metrics` рахує oracle comparison і regret, а `baseline_regret_tracking` логить результат у MLflow.
9. **Operator-facing preview.** FastAPI read models повертають dashboard surfaces: signal preview, persisted operator status, projected battery state preview і baseline LP preview з forecast, signed MW schedule, projected SOC і UAH economics.

На виході поточна система не видає `Proposed Bid`, `Cleared Trade` чи real market execution. Її поточний результат — recommendation preview, projected SOC trace, validated next dispatch semantics для demo і operator-facing read models.

## 5. Як цей прохід має працювати в ідеалі

Цільова ідеальна версія системи має розширити поточний контур у кількох напрямках:

1. Переходити від DAM-only ingest до multi-venue ingest для DAM/IDM/balancing з richer market coupling signals.
2. Замінити strict similar-day forecast на stronger learned forecast layer з NBEATSx, TFT або сумісним ensemble.
3. Будувати не лише recommendation schedule, а канонічний `Proposed Bid` і bid feasibility envelope.
4. Додавати differentiable або surrogate clearing для DFL training замість pure PTO pipeline.
5. Отримувати observed settlement / cleared allocation від агрегатора і лише після цього формувати `DispatchCommand` для фізичного виконання.
6. Замінити feasibility-and-economics preview model на richer digital twin із deeper SOH / temperature / path-dependent degradation logic.

У письмовому звіті важливо підкреслити, що цей ideal flow є research target architecture. Поточна реалізація зупиняється на Baseline Strategy, operator preview і demo-ready control plane.

## 5.1. Як ми завершимо LP baseline повністю і після цього перейдемо далі

У межах цього диплома "повністю завершити LP baseline" не означає нескінченно ускладнювати baseline або перетворювати його на фінальну target system. Йдеться про інше: baseline треба довести до стабільного контрольного контуру, який можна freeze-нути як чесну контрольну групу для наступних forecast- і DFL-експериментів.

Практично це означає п'ять критеріїв завершення.

1. **Зафіксувати canonical Level 1 scope.** Базовий контур має лишатися DAM-only, hourly, UAH-native і працювати з strict similar-day forecast без прихованого розширення scope на IDM, balancing або multi-market orchestration.
2. **Звести до одного джерела істини battery economics і safety contracts.** Однакові `BatteryPhysicalMetrics`, degradation proxy, SOC window і power limits мають використовуватися в Dagster assets, FastAPI preview, dashboard read models і технічній документації без локальних дублювань.
3. **Закрити end-to-end operator path.** Шлях `tenant -> weather -> forecast -> LP schedule -> projected SOC -> validated preview -> regret/metrics` має працювати стабільно і бути відтворюваним як у локальному запуску, так і в supervisor demo package.
4. **Зібрати focused validation package.** Для baseline потрібні не лише ручні walkthrough, а й мінімальний набір контрольних перевірок: focused API tests, materialization ключових Dagster assets, dashboard build, live preview endpoint і перевірена artifact surface для керівника.
5. **Freeze-нути baseline як benchmark.** Після стабілізації baseline треба перестати безконтрольно змінювати його семантику і зафіксувати його як контрольний контур для regret comparison, safety comparison і UX/demo comparison.

Після цього перехід далі має бути поетапним, а не стрибком у "повний AI" за один крок:

1. Поставити stronger forecast layer на базі NBEATSx/TFT, не ламаючи downstream contracts baseline-контурy.
2. Добудувати ринкову семантику до `Bid Feasibility Envelope`, `Proposed Bid` і `Cleared Trade`, а не зупинятися лише на recommendation preview.
3. Ввести differentiable або surrogate clearing як міст між LP/PTO baseline і повноцінним DFL training loop.
4. Порівнювати нові модулі не самі з собою, а з frozen LP baseline за regret, економікою, safety violations і стійкістю demo-path.

Саме така логіка дозволяє рухатися далі без втрати академічної коректності: LP baseline стає не тимчасовою чернеткою, а повноцінною контрольної групою, від якої вже можна чесно міряти цінність наступних етапів.

## 6. Звідки беруться дані та що реально реалізовано на різних рівнях ELT

### 6.1. Зовнішні джерела даних

- **OREE**: live DAM price rows беруться через `https://www.oree.com.ua/index.php/pricectr/data_view` з fallback на HTML table parsing з `https://www.oree.com.ua/index.php/pricectr?lang=english`.
- **Open-Meteo**: погодні hourly features беруться через `https://api.open-meteo.com/v1/forecast`.
- **Tenant registry**: locations і timezone для експериментів читаються з [simulations/tenants.yml](../../../../simulations/tenants.yml) або fallback YAML registry.
- **Synthetic fallback**: якщо live fetch недоступний, система переходить на deterministic synthetic market/weather data для збереження demo stability.

### 6.2. Приклади по рівнях ELT

- **Extract / Bronze**: `weather_forecast_bronze` витягує raw-ish weather rows і маркує `source`; `dam_price_history` витягує/накладає OREE price rows і додає `source`, `price_spike`, `low_volume`.
- **Transform / Silver**: `strict_similar_day_forecast` перетворює enriched hourly history у deployable forecast without hindsight.
- **Transform / Gold**: `baseline_dispatch_plan`, `validated_dispatch_command`, `oracle_benchmark_metrics` та `baseline_regret_tracking` перетворюють forecast у recommendation schedule, safety-checked command semantics і regret metrics.
- **Load / current state**: Dagster materializes assets and can persist outputs through its IO layer, але в поточному MVP medallion layers існують насамперед як логічні asset stages у Dagster, а не як повністю завершений warehouse-backed raw/silver/gold storage stack.

Це означає, що ELT-прохід у проєкті вже реалізовано логічно та виконувано end-to-end, але не слід overclaim-ити його як завершений промисловий data warehouse. Реально реалізовано extraction, transformation і asset materialization; повний persistent storage design є ще одним етапом розвитку.

## 7. Поточний API і як локально запустити проєкт для перевірки

### 7.1. Поточний API surface

У [api/main.py](../../../../api/main.py) зараз доступні такі основні endpoint-и:

- `GET /health`
- `GET /tenants`
- `POST /weather/run-config`
- `POST /weather/materialize`
- `GET /dashboard/signal-preview`
- `GET /dashboard/operator-status`
- `POST /dashboard/projected-battery-state`
- `GET /dashboard/baseline-lp-preview`

Цей API є control-plane і read-model surface. Він не подає ринкові заявки й не виконує market clearing.

### 7.2. Рекомендований локальний запуск для перевірки

На Windows рекомендована перевірка виглядає так:

1. Запустити FastAPI на тому самому порту, який очікує dashboard за замовчуванням:

```powershell
./api/start-dev.ps1 -Port 8010
```

2. Запустити dashboard окремо з папки [dashboard](../../../../dashboard):

```powershell
Set-Location dashboard
npm install
npm run dev
```

Поточний dev port dashboard: `http://localhost:64163/`.

3. Для перевірки Dagster definitions та asset graph використати:

```powershell
$env:PYTHONPATH='.;src'
uv run dg list defs --json
uv run dg dev
```

4. Для швидкої ручної перевірки operator surfaces відкрити:

- `http://127.0.0.1:8010/docs`
- `http://localhost:64163/`
- `http://127.0.0.1:3000/` для Dagster UI після `dg dev`

Для supervisor-facing перевірки без локального запуску вже доступні:

- `https://dashboard-gilt-one-97.vercel.app/` — root alias, який тепер веде на public report
- `https://dashboard-gilt-one-97.vercel.app/operator` — operator dashboard surface
- `https://dashboard-gilt-one-97.vercel.app/week1/interactive_report1`

## 8. Ризики та виклики

| Ризик / виклик | Чому це важливо | Запланована відповідь |
|---|---|---|
| Live OREE fetch може не повернути рядки й перевести flow на synthetic fallback | Це впливає на довіру до live data path і на supervisor demo | Зберегти fallback, явно логувати джерело даних і мати ready-made explanation, чому demo лишається працездатним |
| Є ризик плутати preview semantics із full market execution | Це може зіпсувати академічну коректність пояснення | У всіх матеріалах чітко фіксувати: current output = recommendation preview, target output = bid/clearing/dispatch |
| Поточний degradation model можна переплутати з full battery physics | Це завищує claims про реалізований battery layer | Вживати canonical phrase feasibility-and-economics preview model і прямо описувати її обмеження |
| Поточний ELT шар ще не є завершеним warehouse-backed medallion storage stack | Інакше звіт може overclaim-ити рівень data platform maturity | Пояснювати, що наразі реалізовано logical Dagster asset layering і end-to-end materialization, а не повний industrial persistence layer |
| DFL, differentiable clearing і richer digital twin ще не реалізовані | Це головна research novelty, але вона належить до наступного етапу | Тримати LP baseline як контрольний контур і переходити до DFL після стабілізації поточного MVP |

## 9. План роботи на наступний тиждень

1. Передати керівнику цей консолідований Week 1 package: weekly report, supervisor summary, перший варіант розділів 1-2 записки та public interactive report.
2. Закрити LP baseline як freeze-нутий control contour: стабілізувати шлях `tenant -> weather -> strict forecast -> LP schedule -> projected SOC -> validated preview -> regret logging` і не розширювати scope, доки цей benchmark не стане повністю відтворюваним.
3. Підчистити й синхронізувати validation package для baseline: focused API tests, Dagster materialization для ключових assets, dashboard build, local run docs і operator-facing demo materials.
4. Після freeze baseline відкрити окремий research slice для stronger forecast layer і переходу від PTO до DFL, не ламаючи вже зафіксовані market/safety contracts.
5. Після feedback від керівника вибрати пріоритет Week 2/3: forecast upgrade, bid/clearing semantics, data persistence hardening або окремий DFL proof-of-concept.

## 10. Артефакти

- Код baseline і контракти: [src/smart_arbitrage/gatekeeper/schemas.py](src/smart_arbitrage/gatekeeper/schemas.py), [src/smart_arbitrage/assets/gold/baseline_solver.py](src/smart_arbitrage/assets/gold/baseline_solver.py), [src/smart_arbitrage/assets/mvp_demo.py](src/smart_arbitrage/assets/mvp_demo.py), [src/smart_arbitrage/assets/bronze/market_weather.py](src/smart_arbitrage/assets/bronze/market_weather.py), [src/smart_arbitrage/optimization/projected_battery_state.py](../../../../src/smart_arbitrage/optimization/projected_battery_state.py)
- Код operator/control plane: [api/main.py](../../../../api/main.py), [src/smart_arbitrage/resources/operator_status_store.py](../../../../src/smart_arbitrage/resources/operator_status_store.py), [dashboard/app/composables/useWeatherControls.ts](../../../../dashboard/app/composables/useWeatherControls.ts), [dashboard/app/composables/useBaselinePreview.ts](../../../../dashboard/app/composables/useBaselinePreview.ts), [dashboard/server/api/control-plane/tenants.get.ts](../../../../dashboard/server/api/control-plane/tenants.get.ts)
- Документація: [CONTEXT.md](CONTEXT.md), [AGENTS.md](AGENTS.md), [docs/technical/API_ENDPOINTS.md](../../../technical/API_ENDPOINTS.md), [docs/technical/OPERATOR_DEMO_READY.md](../../../technical/OPERATOR_DEMO_READY.md), [docs/technical/TRACKER_FLOW.md](../../../technical/TRACKER_FLOW.md), [docs/technical/PRD-operator-mvp-slices.md](../../../technical/PRD-operator-mvp-slices.md)
- Публічні demo surfaces: `https://dashboard-gilt-one-97.vercel.app/operator`, `https://dashboard-gilt-one-97.vercel.app/week1/interactive_report1`
- Week 1 report package: [docs/thesis/weekly-reports/week1/report.md](./report.md), [docs/thesis/weekly-reports/week1/supervisor-summary.md](./supervisor-summary.md), [docs/thesis/weekly-reports/week1/presentation-script.md](./presentation-script.md), [docs/thesis/weekly-reports/week1/supervisor-message.md](./supervisor-message.md), [docs/thesis/weekly-reports/week1/interactive_report1/README.md](./interactive_report1/README.md), [docs/thesis/weekly-reports/week1/interactive_report1/slides.marp.md](./interactive_report1/slides.marp.md)
- Перший пакет розділів пояснювальної записки: [docs/thesis/chapters/01-project-overview.md](../../chapters/01-project-overview.md), [docs/thesis/chapters/02-literature-review.md](../../chapters/02-literature-review.md), [docs/technical/papers/README.md](../../../technical/papers/README.md)
- Тести й скриншоти: [tests/api/test_main.py](../../../../tests/api/test_main.py), [docs/thesis/weekly-reports/week1/assets/dagster-ui.png](./assets/dagster-ui.png), [docs/thesis/weekly-reports/week1/assets/mlflow-ui.png](./assets/mlflow-ui.png)
- Ключові джерела для архітектурних рішень:
  1. Yi et al. A Decision-Focused Predict-then-Bid Framework for Energy Storage Arbitrage. DOI: 10.48550/arXiv.2505.01551.
  2. Olivares et al. Neural basis expansion analysis with exogenous variables: Forecasting electricity prices with NBEATSx. DOI: 10.1016/j.ijforecast.2022.03.001.
  3. Jiang et al. Probabilistic electricity price forecasting based on penalized temporal fusion transformer. DOI: 10.1002/for.3084.
  4. Elmachtoub and Grigas. Smart "Predict, then Optimize". DOI: 10.1287/mnsc.2020.3922.
  5. Grimaldi et al. Profitability of energy arbitrage net profit for grid-scale battery energy storage considering dynamic efficiency and degradation using a linear, mixed-integer linear, and mixed-integer non-linear optimization approach. DOI: 10.1016/j.est.2024.112380.

## 11. Короткий висновок

Перший звітний тиждень завершено не лише з формалізованою концепцією, а й з working baseline contour, operator-facing API surface, tenant-aware dashboard preview та чітко зафіксованими межами поточного MVP. Проєкт уже має достатньо реалізованих артефактів для обговорення з керівником, але водночас чесно відділяє поточний deliverable від фінальної planned version з DFL, bid/clearing semantics і richer battery modeling.