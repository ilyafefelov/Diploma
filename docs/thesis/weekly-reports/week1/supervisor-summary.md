# Week 1: коротка supervisor-facing версія

## 1. Що це за проєкт і який результат Week 1

Дипломний проєкт присвячено побудові керованої системи автономного енергоарбітражу для BESS на ринку України 2026. Практична мета полягає у тому, щоб з ринкових і погодних даних отримувати економічно осмислену та фізично безпечну рекомендацію для роботи батареї. Дослідницька траєкторія проєкту спрямована до Decision-Focused Learning, але поточний результат Week 1 є свідомо обмеженим baseline-контуром.

На кінець звітного тижня реалізовано не повну AI-стратегію, а працездатний MVP Level 1: погодинний DAM, валюта UAH, strict similar-day forecast, LP baseline з rolling-horizon логікою, Pydantic gatekeeper, operator-facing API і dashboard preview. Поточний батарейний шар коректно описувати як feasibility-and-economics preview model, а не як повний digital twin.

## 2. Що вже реально реалізовано

- У `src/smart_arbitrage/assets/bronze/market_weather.py` реалізовано live ingest для OREE та Open-Meteo з tenant-aware location resolution і synthetic fallback.
- У `src/smart_arbitrage/assets/gold/baseline_solver.py` реалізовано LP baseline для погодинного DAM з урахуванням degradation penalty в objective.
- У `src/smart_arbitrage/optimization/projected_battery_state.py` реалізовано hourly projected SOC preview і degradation-aware economics preview.
- У `src/smart_arbitrage/gatekeeper/schemas.py` зафіксовано Pydantic V2 контракти для battery metrics, dispatch semantics і UAH-native degradation cost.
- У `src/smart_arbitrage/assets/mvp_demo.py` зібрано Dagster asset graph Bronze -> Silver -> Gold.
- У `api/main.py` реалізовано control-plane/read-model API для tenant list, weather materialization, signal preview, operator status, projected battery state та baseline LP preview.
- У `dashboard/` реалізовано operator-facing demo surface для tenant selection, weather control, baseline preview і projected SOC trace.

Отже, проєкт уже має повний demo-contour від зовнішніх джерел даних і Dagster assets до API та dashboard surface, придатної для першого walkthrough із керівником.

## 3. Чому зараз використовується проста economic penalty за деградацію

На поточному етапі я не моделюю повну електрохімічну деградацію батареї. Натомість використовується проста і прозора економічна модель штрафу за throughput:

$$
MC_{deg} = \frac{C_{cycle}}{2 \cdot Capacity_{mwh}}
$$

де вартість одного циклу переводиться у вартість 1 МВт·год throughput. Для кожного інтервалу оптимізація додає:

$$
Penalty_t = MC_{deg} \cdot Throughput_t
$$

Для current demo battery `10 MWh` це число тепер параметризується як public-source capex-throughput proxy, а не як довільна локальна константа: `210 USD/kWh` (Grimaldi visible capex anchor) + `15 years` і `~1 cycle/day` (NREL ATB) + `43.9129 UAH/USD` (НБУ, `04.05.2026`). Це дає `16,843.3 UAH/cycle`, тобто `842.2 UAH/MWh throughput`.

Це рішення обрано тому, що воно:

- сумісне з LP-baseline і не ускладнює контрольну постановку;
- спирається лише на ті сигнали, які вже реально є в MVP;
- дає зрозумілий economic signal у UAH для dashboard і пояснення керівнику;
- не створює хибного враження, ніби в проєкті вже реалізовано повний battery digital twin.

## 4. Як зараз працює прохід даних

Поточний end-to-end flow виглядає так: tenant metadata і координати читаються з registry; weather asset отримує конфігурацію через API; Bronze layer забирає погоду з Open-Meteo та накладає live OREE price rows поверх deterministic synthetic base; Silver layer формує strict similar-day forecast; Gold layer оптимізує charge/discharge schedule через LP; після цього API віддає dashboard preview для signal, projected SOC, operator status і baseline recommendation.

Це вже є реальний ELT-контур у термінах extraction, transformation і materialization через Dagster assets. Водночас важливо чесно зафіксувати, що medallion layers наразі реалізовано насамперед як логічні asset stages, а не як повністю завершений warehouse-backed storage stack.

## 5. Які джерела даних і як перевірити проєкт локально

Поточні зовнішні джерела:

- OREE для live DAM rows;
- Open-Meteo для погодних hourly features;
- tenant registry з YAML-конфігурації;
- deterministic synthetic fallback для стабільності demo.

Мінімальна локальна перевірка:

```powershell
./api/start-dev.ps1 -Port 8010
Set-Location dashboard
npm install
npm run dev
```

Після цього доступні:

- `http://127.0.0.1:8010/docs` для API;
- `http://localhost:64163/` для dashboard.

Для Dagster definitions:

```powershell
$env:PYTHONPATH='.;src'
uv run dg list defs --json
uv run dg dev
```

## 6. Основні ризики і найближчий крок

Головні ризики на цьому етапі: нестабільність live OREE fetch, ризик переплутати preview з real market execution, спокуса переоцінити просту degradation model як full battery physics, а також незавершеність повного storage layer. Тому в усіх матеріалах я розділяю три речі: що вже реалізовано, що є demo-stage operator surface, і що лишається фінальною planned version диплома.

Найближчий крок після цього звіту: провести короткий supervisor walkthrough поточного MVP, закрити LP baseline як відтворюваний control contour — stabilise шлях `tenant -> weather -> forecast -> LP -> projected SOC -> validated preview -> regret logging` — і лише після цього відкривати окремий stronger-forecast / DFL research slice. Така послідовність дозволяє не змішувати завершення benchmark-контурy з наступним етапом наукової новизни.

## 7. Що відкрити керівнику одразу

- Production report: `https://dashboard-gilt-one-97.vercel.app/week1/interactive_report1`
- Operator dashboard: `https://dashboard-gilt-one-97.vercel.app/operator`
- Повний Week 1 report: [docs/thesis/weekly-reports/week1/report.md](./report.md)
- Огляд літератури: [docs/thesis/chapters/02-literature-review.md](../../chapters/02-literature-review.md)
- Скриншоти для артефактів: [docs/thesis/weekly-reports/week1/assets/dagster-ui.png](./assets/dagster-ui.png), [docs/thesis/weekly-reports/week1/assets/mlflow-ui.png](./assets/mlflow-ui.png)