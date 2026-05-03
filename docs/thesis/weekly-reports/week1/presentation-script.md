# Сценарій презентації: Week 1

## 1. Короткий prompt, який вміщується у верхню частину файлу

Використай цей текст у Gamma, Canva, PowerPoint Copilot або іншому генераторі:

> Створи українську академічну презентацію на 8 слайдів у стриманому технічному стилі.
> Тема: «Прогрес дипломної роботи за тиждень 1: автономний енергоарбітраж BESS на ринку України 2026».
> Покажи, що поточний MVP обмежено Level 1: погодинний DAM, валюта UAH, strict similar-day forecast, LP baseline з rolling-horizon dispatch.
> Окремо підкресли, що поточний батарейний шар є feasibility-and-economics preview model, а не повним digital twin.
> Поясни, що research-рамка спрямована на перехід від Predict-then-Optimize до Decision-Focused Learning, але DFL ще не є реалізованим результатом Week 1.
> Обов'язково включи: проблему та актуальність, scope Week 1, архітектуру Bronze -> Gold у Dagster, реальний data flow, реалізовані результати тижня, API/dashboard demo surface, артефакти й демо-готовність, ризики, план на Week 2.
> Додай слайди з візуальними вставками Dagster UI та MLflow UI.
> Використовуй короткі буліти, 3-5 тез на слайд, без маркетингових формулювань, з чітким розділенням між «вже реалізовано» і «цільова дослідницька архітектура».

## 2. Швидка структура презентації

- Формат: 8 слайдів
- Тривалість: 5-7 хвилин
- Стиль: академічний, технічний, лаконічний
- Слайд 1: назва і контекст роботи
- Слайд 2: проблема та актуальність
- Слайд 3: scope першого MVP
- Слайд 4: архітектура baseline -> DFL
- Слайд 5: реалізовані результати Week 1
- Слайд 6: API, dashboard і демо-готовність
- Слайд 7: ризики та обмеження
- Слайд 8: план на Week 2

## 3. Детальний покадровий сценарій

### Слайд 1. Назва і контекст роботи

**Назва слайду:**
Прогрес дипломної роботи за тиждень 1

**Текст на слайді:**
- Тема: автономний енергоарбітраж BESS на ринку України 2026
- Напрям: MLOps + optimization + AI decision systems
- Фокус Week 1: концепція, архітектура, базовий MVP-контур

**Що говорити:**
На першому тижні я зафіксував предметну область, межі першого MVP і технічний контур системи. Мета цього етапу полягала не у повній реалізації цільової AI-стратегії, а у побудові стабільного baseline-рішення, яке можна перевіряти, демонструвати і надалі використовувати як контрольну групу.

**Візуальна ідея:**
- Назва роботи
- Підзаголовок з одним реченням про Week 1
- Акуратна схема «Research -> MVP -> Demo»

### Слайд 2. Проблема і актуальність

**Назва слайду:**
Проблема, яку вирішує проєкт

**Текст на слайді:**
- BESS має приймати ринкові рішення в умовах волатильних цін
- Оптимізація має враховувати оперативні обмеження батареї та вартість деградації
- Потрібен міст між прогнозом, прибутком і безпекою виконання
- Український контекст: DAM, прайс-кепи, UAH-native облік

**Що говорити:**
Завдання проєкту полягає у тому, щоб перетворити ринкові дані та прогноз цін у безпечні й економічно доцільні команди для BESS. У такій системі недостатньо лише передбачити ціну; потрібно врахувати SOC, деградацію, правила ринку й обмеження фізичного виконання. Саме тому архітектура будується навколо зв’язку між прогнозом, оптимізацією та safety-перевірками.

**Візуальна ідея:**
- Простий ланцюжок: «ціни -> рішення -> dispatch -> дохід/ризик»

### Слайд 3. Що було зафіксовано на Week 1

**Назва слайду:**
Scope першого MVP

**Текст на слайді:**
- Level 1: погодинний ринок DAM, валюта UAH
- Базовий прогноз: strict similar-day
- Базова стратегія: LP baseline
- Батарейний шар MVP: feasibility-and-economics preview model
- Degradation proxy: `16,843.3 UAH/cycle` -> `842.2 UAH/MWh throughput`
- Виконання: rolling-horizon, commit лише першої команди

**Що говорити:**
Ключове рішення першого тижня полягало у жорсткому обмеженні scope. Я не розширював систему на IDM, balancing або повний DFL-контур. Натомість зафіксував мінімальний, але завершений Level 1 сценарій: погодинний DAM, простий deployable forecast, LP-оптимізація, feasibility-and-economics preview model для батареї та перевірка лише найближчої команди в rolling-horizon режимі. Для demo battery degradation penalty вже подається не як placeholder, а як прозорий public-source capex-throughput proxy: `16,843.3 UAH/cycle` або `842.2 UAH/MWh throughput`. Це важливо, бо на цьому етапі батарейний шар не подається як повний electrochemical digital twin.

**Візуальна ідея:**
- Таблиця «In scope / Out of scope»

### Слайд 4. Архітектурна логіка

**Назва слайду:**
Архітектура і реальний data flow

**Текст на слайді:**
- Поточний MVP: Bronze -> Silver -> Gold у Dagster
- External data: OREE + Open-Meteo + tenant registry
- Gatekeeper на Pydantic V2, MLflow для regret-логування
- Поточний вихід: recommendation preview, а не market execution
- Цільовий research-вектор: PTO -> DFL

**Що говорити:**
На рівні реалізації я побудував baseline-контур у Dagster, де Bronze layer отримує ринкові та погодні дані, Silver формує strict similar-day forecast, а Gold запускає LP baseline і safety-checked preview semantics. Усередині цього контуру батарейний шар виконує роль degradation-aware feasibility-and-economics preview model, а не повноцінного цифрового двійника. Поточний вихід системи потрібно описувати як recommendation preview і projected SOC trace, а не як реальне market execution. На рівні research-рамки визначено цільовий перехід від Predict-then-Optimize до Decision-Focused Learning, але ця частина ще не є реалізованим результатом Week 1.

**Візуальна ідея:**
- Діаграма `sources -> Dagster assets -> API -> dashboard`
- Окремий підпис: `current output = preview`, `target output = bid / clearing / dispatch`

### Слайд 5. Реалізовані результати тижня

**Назва слайду:**
Що вже реалізовано

**Текст на слайді:**
- Канонічні доменні контракти та glossary
- Pydantic-схеми для bid / cleared trade / dispatch
- LP baseline solver для DAM та degradation-aware battery preview
- Bronze ingestion для ціни та погоди
- MVP asset chain у Dagster
- Control-plane API і operator-facing dashboard surface

**Що говорити:**
Основний результат Week 1 полягає в тому, що система перестала бути лише концепцією. Вона вже має канонічні контракти, детермінований baseline solver, реальний Bronze ingest для ціни та погоди, end-to-end asset chain, базове логування результатів в MLflow, а також control-plane API і dashboard surface для operator preview. Тобто на кінець тижня є не тільки опис архітектури, а робочий інженерний контур, який можна показати керівнику.

**Візуальна ідея:**
- Список модулів з короткими тегами: `schemas`, `baseline_solver`, `market_weather`, `mvp_demo`, `api/main.py`, `dashboard/`

### Слайд 6. Артефакти та демо-готовність

**Назва слайду:**
API, dashboard і демо-готовність

**Текст на слайді:**
- `GET /tenants`, weather endpoints, baseline preview, projected battery state
- Dashboard: tenant selection, weather control, projected SOC, UAH economics
- Public report: `dashboard-gilt-one-97.vercel.app/week1/interactive_report1`
- Dagster UI: materialization та lineage
- MLflow UI: baseline experiment
- Weekly report і технічні артефакти

**Що говорити:**
На завершення тижня підготовлено не лише код, а й демонстраційні артефакти та operator-facing surface. Через API і dashboard можна послідовно показати шлях від tenant selection і weather materialization до baseline recommendation preview та projected SOC trace. Dagster показує materialization і lineage, а MLflow фіксує baseline-експеримент. Це означає, що до кінця Week 1 є підстава переходити до першого технічного walkthrough.

**Візуальна ідея:**
- Вставити [docs/thesis/weekly-reports/week1/assets/dagster-ui.png](docs/thesis/weekly-reports/week1/assets/dagster-ui.png)
- Додати [docs/thesis/weekly-reports/week1/assets/mlflow-ui.png](docs/thesis/weekly-reports/week1/assets/mlflow-ui.png)
- Короткий підпис: «Lineage, materialization і baseline experiment evidence»

### Слайд 7. Ризики і обмеження

**Назва слайду:**
Ризики та інженерні обмеження

**Текст на слайді:**
- Live OREE fetch нестабільний -> потрібен fallback
- Є ризик переплутати preview з real market execution
- Проста degradation model не дорівнює full battery physics
- Передчасне розширення scope небезпечне для MVP

**Що говорити:**
На цьому етапі важливо було не лише показати прогрес, а й зафіксувати ризики. Частина з них інженерна: зовнішні джерела даних і незавершеність повного storage layer. Частина комунікаційна: легко переплутати preview semantics з реальним market execution або трактувати просту degradation-aware economics model як повний digital twin. Саме тому стратегія розвитку обрана поетапною: спочатку стабільний baseline і demo-surface, потім DFL.

**Візуальна ідея:**
- Таблиця «Ризик / Відповідь»

### Слайд 8. План на Week 2

**Назва слайду:**
Наступний крок

**Текст на слайді:**
- Передати керівнику Week 1 report, summary і перший варіант літогляду
- Freeze-нути LP baseline як control contour і benchmark
- Провести supervisor walkthrough: tenant -> weather -> baseline -> SOC preview
- Підчистити run/docs/API materials і validation package
- Після freeze baseline вибрати наступний slice: forecast upgrade чи DFL

**Що говорити:**
На другому тижні пріоритетом є не розширення функціональності будь-якою ціною, а завершення LP baseline як стабільного контрольного контуру. Це означає: синхронізувати data path, operator preview, docs і validation package, freeze-нути цей baseline як benchmark, віддати керівнику перший варіант розділів 1-2 записки і лише після цього починати окремий stronger-forecast або DFL slice. Така послідовність потрібна, щоб наступний research layer порівнювався не з рухомою мішенню, а з відтворюваним baseline.

**Візуальна ідея:**
- Roadmap: `Week 1 baseline` -> `Week 2 demo` -> `DFL preparation`

## 4. Коротка версія для усного виступу

Якщо потрібен дуже короткий захист на 2-3 хвилини, можна говорити так:

> На першому тижні я зафіксував концепцію дипломної роботи, її ринкові та технічні межі, а також побудував перший працездатний MVP-контур. Поточний scope свідомо обмежено погодинним ринком DAM, валютою UAH, strict similar-day forecast і LP baseline з rolling-horizon dispatch.
>
> На рівні реалізації вже підготовлено Pydantic-контракти, baseline solver, Bronze ingestion для ціни та погоди, MVP asset chain у Dagster, control-plane API, dashboard preview та MLflow-логування для baseline-експерименту. Батарейний шар у цьому MVP подано як feasibility-and-economics preview model: він відстежує projected SOC, ефективність і throughput-based penalty, але ще не є повним digital twin. Поточна degradation penalty теж пояснюється прозоро: для demo battery це public-source proxy `16,843.3 UAH/cycle` або `842.2 UAH/MWh throughput`, а не локальна магічна константа. На рівні research-рамки зафіксовано напрям переходу від Predict-then-Optimize до Decision-Focused Learning, але ця частина ще не є завершеною реалізацією першого тижня.
>
> Основний результат Week 1 полягає у тому, що проєкт уже має відтворюваний baseline-контур, operator-facing demo surface і готові артефакти для першого walkthrough із керівником. Наступний крок — freeze-нути цей LP baseline як контрольну групу, подати перший варіант огляду літератури та вже після цього переходити до сильнішого forecast layer і DFL-етапу.

## 5. Що вставити в презентацію з файлів репозиторію

- Текстова база: [docs/thesis/weekly-reports/week1/report.md](docs/thesis/weekly-reports/week1/report.md)
- Коротка версія для керівника: [docs/thesis/weekly-reports/week1/supervisor-summary.md](docs/thesis/weekly-reports/week1/supervisor-summary.md)
- Огляд літератури: [docs/thesis/chapters/02-literature-review.md](../../chapters/02-literature-review.md)
- Публічний report route: `https://dashboard-gilt-one-97.vercel.app/week1/interactive_report1`
- Скриншот Dagster: [docs/thesis/weekly-reports/week1/assets/dagster-ui.png](docs/thesis/weekly-reports/week1/assets/dagster-ui.png)
- Скриншот MLflow: [docs/thesis/weekly-reports/week1/assets/mlflow-ui.png](docs/thesis/weekly-reports/week1/assets/mlflow-ui.png)
- Архітектурна мова: [CONTEXT.md](CONTEXT.md)
