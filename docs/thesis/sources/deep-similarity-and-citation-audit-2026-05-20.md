# Deep Similarity And Citation Audit - 2026-05-20

Scope: thesis chapters `01-project-overview.md`, `02-literature-review.md`, `03-Methodology.md`, and `04-results-and-discussion.md`. The Chapter 2 bibliography was excluded from similarity scoring because source titles, DOIs, and URLs are supposed to match exactly.

## Method

- Layer 1: exact 10-word shingle overlap to catch direct copy-paste.
- Layer 2: TF-IDF character 5-8 gram vector similarity to catch near-copy, light paraphrase, and template reuse.
- Layer 3: citation/support coverage heuristic for claim-heavy paragraphs without academic citation, URL/DOI/arXiv reference, or repo artifact reference.

This is deeper than the first exact-overlap screen, but it is still not Turnitin/iThenticate and cannot compare against closed student-paper databases.

## Summary

- Thesis paragraphs checked: 217
- External/source chunks scanned: 12192
- Internal repo-doc chunks scanned: 2872
- Source extraction failures/empty files: 1
- External high/near-copy candidates: 0
- Internal self-similarity candidates: 1
- Claim-heavy paragraphs without obvious support marker: 133

## Priority Interpretation

- External-source plagiarism risk is low in this local corpus: no checked thesis
  paragraph crossed the near-copy threshold against academic/source PDFs or
  source notes.
- Literature review is in good shape after the citation pass. The only
  Chapter 2 support-gap candidate in the first review set is the paragraph about
  the current repository implementation; that paragraph is better supported by
  repo artifact references than by another academic paper.
- Methodology is the main next cleanup area. The heuristic found many
  implementation/evidence claims without an obvious marker. Most are not
  plagiarism risks; they should be handled by adding repo artifact references
  for implementation claims and paper citations only for general scientific or
  market claims.
- The single internal self-similarity candidate is in Chapter 4 and points to a
  technical design note. It has no long exact-token overlap, so it is not a
  direct copy-paste finding, but the thesis paragraph should either reference
  the artifact or be rewritten more independently.

## External Similarity Candidates

No external-source paragraphs crossed the configured near-copy thresholds. This means the checked thesis text does not look like direct or lightly modified source prose from the local archive.

## Internal Repo Self-Similarity Candidates

These are not plagiarism findings by themselves. They indicate thesis text that may reuse project documentation wording and should be kept if it is intentional or rewritten if the thesis should sound more independent.

- `docs/thesis/chapters/04-results-and-discussion.md:443` score=0.648, exact_tokens=0, internal_source=`docs/technical/DFL_CANDIDATE_VALUE_DFL_V3.md`
  - Target: Failure audit пояснив, чому нові prior-template schedules не перемогли V2+ достатньо часто. На final holdout prior_best_family_template_v3 мав mean regret 605.71 UAH для calibrated NBEATSx і 689.66 UAH для raw NBEATSx; prior_oracle_residual_template_v3 мав відповідно 627.08 UAH і 729.69 UAH. Win rate цих сімейств проти V2+ становив лише 4.44%, 13.33%, 5.56%...

## Citation/Support Gap Candidates

These are heuristic candidates, not automatic errors. Methodology/results paragraphs can be supported by repo artifacts rather than papers; if so, add a file/artifact reference or keep the paragraph as implementation description.

- `docs/thesis/chapters/01-project-overview.md:7` heading `1.1. Проблема та актуальність`
  - Text: Для українського контексту 2026 року ця задача є особливо актуальною. Ринок має власні обмеження, прайс-кепи, часову структуру торгів і валютну специфіку, тому систему неможливо безпосередньо перенести з абстрактної дослідницької постановки в прикладний контур без локальної адаптації. Саме це робить тему диплома придатною як для інженерного проєкту, так і дл...
- `docs/thesis/chapters/01-project-overview.md:9` heading `1.1. Проблема та актуальність`
  - Text: Актуальність також підтверджується state of practice в Україні: АТ «Оператор ринку» у березні 2026 року повідомив, що понад 180 компаній тестують його Economic Dispatch Platform для BESS-арбітражу на DAM/IDM. Це означає, що тема не є лише академічною симуляцією; ринок уже потребує інструментів, які поєднують ціни, SOC-aware planning, обмеження батареї та зро...
- `docs/thesis/chapters/01-project-overview.md:13` heading `1.2. Що саме будується в межах диплома`
  - Text: Поточний дипломний проєкт будується як система автономного енергоарбітражу для BESS на ринку України 2026. Її завдання — перетворювати дані про ціни, погодні фактори, обмеження батареї та стан системи на operator-facing recommendation preview, а в цільовій версії — на market-aware decision pipeline.
- `docs/thesis/chapters/01-project-overview.md:15` heading `1.2. Що саме будується в межах диплома`
  - Text: У цьому контексті термін «автономний» не означає, що на поточному етапі система виконує повний цикл від прогнозу до фізичної dispatch-команди без участі оператора. Натомість він позначає архітектурну мету: система повинна бути здатною самостійно генерувати коректні рішення в межах формалізованих ринкових і safety-обмежень. Поточний демонстраційний етап реалі...
- `docs/thesis/chapters/01-project-overview.md:21` heading `1.3. Чому це інженерний диплом із дослідницькою траєкторією`
  - Text: Формально ця робота є інженерним проєктом, оскільки в центрі стоїть побудова працездатної системи з чіткими API, пайплайнами, dashboard-поверхнею, тестами та демонстраційними артефактами. Водночас проєкт має виражену дослідницьку траєкторію, бо його фінальна ціль не обмежується простим rule-based або LP-based scheduling. Він спрямований на перехід до Decisio...
- `docs/thesis/chapters/01-project-overview.md:29` heading `1.4. Поточний підтверджений рівень: MVP baseline`
  - Text: ринок обмежено погодинним DAM; канонічна валюта від початку зафіксована як UAH; базовий forecast реалізовано через strict similar-day rule; основна стратегія — детермінований LP baseline; економіка включає throughput-based degradation penalty; дані та проміжні результати оркеструються через Dagster assets; експериментальні результати й regret логуються в MLf...
- `docs/thesis/chapters/01-project-overview.md:40` heading `1.4. Поточний підтверджений рівень: MVP baseline`
  - Text: Пізніший evidence-cycle змінив силу цього твердження. Початковий ризик synthetic або демонстраційно орієнтованого market-weather шару був коректним методологічним застереженням, але для основного дослідницького контуру вже сформовано source-backed Ukrainian DAM benchmark: observed OREE DAM, Open-Meteo/weather context, tenant configuration/load context, rolli...
- `docs/thesis/chapters/01-project-overview.md:42` heading `1.4. Поточний підтверджений рівень: MVP baseline`
  - Text: Отже, поточна академічна межа формулюється так: диплом уже має відтворюваний observed-data evidence path для offline/read-model strategy evidence, але ще не заявляє ринкову торгівлю в реальному часі, автоматичне перемикання dashboard/API на нову strategy, розгорнутий Decision Transformer або використання European market-coupling rows як training inputs.
- `docs/thesis/chapters/01-project-overview.md:46` heading `1.5. Поточний демонстраційний етап: operator-facing MVP`
  - Text: Окрім baseline-контуру, у проєкті реалізовано демонстраційний operator surface. Це означає, що система має не лише backend-логіку, а й пояснюваний інтерфейс, через який можна продемонструвати контрольований сценарій роботи для наукового керівника та оператора.
- `docs/thesis/chapters/01-project-overview.md:56` heading `1.5. Поточний демонстраційний етап: operator-facing MVP`
  - Text: На цьому етапі шар батареї коректніше описувати не як повноцінну фізичну симуляцію, а як feasibility-and-economics preview model. Поточний контур прогнозує допустимий стан батареї на погодинному горизонті, враховує SOC-вікно, ліміт потужності, спрощений round-trip efficiency та throughput-based degradation penalty. Такий рівень моделі достатній для operator-...
- `docs/thesis/chapters/01-project-overview.md:58` heading `1.5. Поточний демонстраційний етап: operator-facing MVP`
  - Text: Для демонстраційного профілю цей штраф деградації параметризується як public-source capex-throughput proxy, а не як довільна локальна константа: 210 USD/kWh з видимого capex anchor у Grimaldi et al., 15-year lifetime і ~1 cycle/day з NREL ATB та курс НБУ 43.9129 UAH/USD на 04.05.2026. Для демонстраційної батареї 10 MWh це дає 16,843.3 UAH/cycle, тобто 842.2...
- `docs/thesis/chapters/01-project-overview.md:60` heading `1.5. Поточний демонстраційний етап: operator-facing MVP`
  - Text: Критично важливо, що демонстраційний етап не видається за повний механізм ринкового виконання. Поточна dashboard-поверхня демонструє recommendation preview та operator review, але не претендує на завершену реалізацію Proposed Bid, Cleared Trade або Dispatch Command.
- `docs/thesis/chapters/01-project-overview.md:66` heading `1.6. Цільова архітектурна версія`
  - Text: перехід від simple baseline forecast до сильнішого prediction layer на базі NBEATSx і TFT; перехід від Predict-then-Optimize baseline до predict-then-bid / Decision-Focused Learning; differentiable або surrogate-based market clearing як частину навчального контуру; learned strategy layer на кшталт Decision Transformer; глибший digital twin батареї з точнішим...
- `docs/thesis/chapters/01-project-overview.md:76` heading `1.6. Цільова архітектурна версія`
  - Text: Усі перелічені елементи цільової версії слід читати як target/research trajectory: вони не означають поточного market execution, автоматичного перемикання dashboard/API на нову стратегію або розгорнутого Decision Transformer-контролера.
- `docs/thesis/chapters/01-project-overview.md:82` heading `1.7. Роль Dagster, MLflow, FastAPI, dashboard і MCP-інструментів`
  - Text: Dagster відповідає за orchestration, lineage і керування asset graph; MLflow фіксує експерименти, метрики та regret-aware evaluation; FastAPI дає contract-first control plane для operator-facing read models; Nuxt dashboard забезпечує пояснювану демонстраційну поверхню для керівника та майбутнього користувача; MCP- та agent-based tooling використовується як д...
- `docs/thesis/chapters/01-project-overview.md:92` heading `1.8. Роль FastAPI як read-model інтерфейсу`
  - Text: FastAPI-шар у цій роботі виконує роль інженерного інтерфейсу між ML/Dagster pipeline та операторськими read models. Він не є окремим стратегічним рушієм і не виконує ринкові заявки. Його завдання полягає в тому, щоб надати перевірювані контракти для tenant context, weather/materialization control, telemetry state, baseline LP preview, forecast evidence, DFL/...
- `docs/thesis/chapters/01-project-overview.md:99` heading `1.8. Роль FastAPI як read-model інтерфейсу`
  - Text: У такій архітектурі API є способом подання результатів і стану системи, а не джерелом нової стратегії. Рішення про якість ML/DFL-кандидатів приймається в Dagster/Postgres/evidence pipeline через rolling-origin evaluation, LP/oracle regret та promotion gates. API лише публікує ці результати у контрольованій формі для operator-facing та defense-facing сценарії...
- `docs/thesis/chapters/01-project-overview.md:115` heading `1.9. Як цей проєкт співвідноситься з дипломом`
  - Text: Для дипломної роботи цей проєкт цінний з кількох причин. По-перше, він має чітку прикладну проблему і реалістичний інженерний контур. По-друге, він містить природну дослідницьку прогалину між baseline-рішенням і decision-focused цільовою архітектурою. По-третє, він дозволяє поетапно демонструвати прогрес: спочатку концепцію і baseline, далі демонстраційний o...
- `docs/thesis/chapters/01-project-overview.md:117` heading `1.9. Як цей проєкт співвідноситься з дипломом`
  - Text: Отже, диплом не зводиться до «дашборду для батареї» і не зводиться до «чергової ML-моделі». Його змістовне ядро - це побудова відтворюваної архітектури автономного енергоарбітражу, у якій baseline, operator preview і фінальна DFL-траєкторія пов'язані в один логічний контур.
- `docs/thesis/chapters/01-project-overview.md:123` heading `1.10. Перехід до огляду літератури`
  - Text: З урахуванням оновленого огляду джерел ця поетапна логіка формулюється ще точніше: спочатку реальний історичний benchmark, потім порівняння strict similar-day, NBEATSx і TFT за decision value та oracle regret, далі robustness-аналіз деградації, fees і SOC assumptions, і лише після цього DFL-дослід.
- `docs/thesis/chapters/02-literature-review.md:9` heading `2.1. Методологічна роль огляду літератури`
  - Text: Поточна реалізація репозиторію підтверджує інженерну здійсненність Level 1 BESS arbitrage MVP: DAM-only hourly scope, observed OREE DAM та historical Open-Meteo evidence для thesis-grade benchmark, strict_similar_day як контрольний forecast comparator, LP-based dispatch, feasibility-and-economics preview model, Pydantic safety semantics, Dagster Bronze/Silve...
- `docs/thesis/chapters/03-Methodology.md:5` heading `3.1. Загальна методологічна логіка`
  - Text: Методологія цієї дипломної роботи побудована навколо принципу forecast -> optimize -> validate -> compare regret -> promote only if decision value improves. Такий підхід відрізняє роботу від звичайного прогнозного benchmark: модель не вважається кращою лише тому, що має нижчий MAE або RMSE. Для BESS-арбітражу ключовим результатом є економічна якість рішення...
- `docs/thesis/chapters/03-Methodology.md:12` heading `3.1. Загальна методологічна логіка`
  - Text: Базовим контрольним контуром є strict_similar_day. Він залишається замороженим fallback і comparator для всіх ML/DFL-кандидатів. Neural forecast models, schedule/value learners та DFL-style challengers можуть бути розглянуті лише як доказова база для Offline Strategy Promotion: вони можуть підтримувати offline/read-model strategy evidence, але не означають р...
- `docs/thesis/chapters/03-Methodology.md:20` heading `3.1. Загальна методологічна логіка`
  - Text: Для уникнення змішування дослідницьких, демонстраційних і продуктово орієнтованих тверджень у роботі використовується стисла таблиця методів. Таблиця 3.1 показує, які дані використовує кожний метод, чи допускає він майбутню інформацію, яку роль має у decision pipeline та яку межу твердження дозволяє.
- `docs/thesis/chapters/03-Methodology.md:58` heading `3.2. Дані та межа доказовості`
  - Text: Основний evidence layer побудований на українських observed OREE DAM цінах, Open-Meteo/weather контексті та tenant configuration/load context. Дані вирівнюються на погодинному горизонті, після чого формуються tenant-aligned features для rolling-origin evaluation.
- `docs/thesis/chapters/03-Methodology.md:63` heading `3.2. Дані та межа доказовості`
  - Text: Синтетичні або демонстраційні rows не можуть використовуватися для thesis-grade тверджень про ефективність. Вони можуть існувати для демонстраційної стабільності або smoke tests, але benchmark має спиратися на provenance flags, data_quality_tier=thesis_grade, not_market_execution=true і явні claim-boundary поля.
- `docs/thesis/chapters/03-Methodology.md:69` heading `3.2. Дані та межа доказовості`
  - Text: Європейські market-coupling джерела, зокрема ENTSO-E, OPSD, Ember, Nord Pool, PriceFM і THieF, розглядаються як research roadmap та external-validation context. Вони не змішуються з українським training panel, доки не пройдені licensing, timezone/DST, currency normalization, market-rule mapping, publication-time availability та domain-shift gates.
- `docs/thesis/chapters/03-Methodology.md:75` heading `3.2. Дані та межа доказовості`
  - Text: Для ENTSO-E Poland route додано окремий leak-safe варіант: лагована ознака entsoe_pl_lag24_day_ahead_price_uah_mwh. Вона не додає європейські рядки в training panel; вона додає лише погодинний exogenous column, де український timestamp t використовує польську day-ahead price з t - 24h. EUR/MWh конвертується в UAH/MWh тільки через prior-known NBU EUR/UAH meta...
- `docs/thesis/chapters/03-Methodology.md:86` heading `3.3. Rolling-origin протокол`
  - Text: Оцінювання виконується як temporal rolling-origin protocol. Для кожного anchor модель бачить лише інформацію, доступну до decision time. Реалізовані ціни після anchor можуть використовуватися для oracle evaluation, regret labels і diagnostics, але не для формування forecast input для майбутнього виконання рішення або prior-only ознак selector-а.
- `docs/thesis/chapters/03-Methodology.md:92` heading `3.3. Rolling-origin протокол`
  - Text: mermaid flowchart LR A["Observed OREE DAM + weather/load context"] --> B["Bronze ingestion assets"] B --> C["Silver tenant-aligned feature frame"] C --> D["Official NBEATSx/TFT forecast candidates"] C --> E["strict_similar_day frozen control"] D --> F["LP dispatch optimizer"] E --> F F --> G["Strict LP/oracle scoring"] G --> H["Regret, net value, feasibility...
- `docs/thesis/chapters/03-Methodology.md:106` heading `3.3. Rolling-origin протокол`
  - Text: У цьому протоколі oracle LP є лише офлайн-оцінювачем. Він має доступ до realized prices лише для обчислення theoretical best value та regret. Oracle не є стратегією, яку можна застосовувати для виконання ринкових рішень, і не використовується як джерело прогнозу.
- `docs/thesis/chapters/03-Methodology.md:113` heading `3.4. Forecast-to-schedule evaluation`
  - Text: Офіційні NBEATSx/TFT кандидати проходять не окрему forecast-only перевірку, а той самий strict LP/oracle contour, що й baseline. Прогнозна траєкторія перетворюється на schedule через LP dispatch optimizer, після чого schedule оцінюється на realized prices. Це дозволяє порівнювати моделі за тим, що має економічний сенс для BESS:
- `docs/thesis/chapters/03-Methodology.md:126` heading `3.4. Forecast-to-schedule evaluation`
  - Text: Schedule/value learner не замінює фізичний контур виконання. Він вибирає candidate schedule family у offline/read-model evidence stack і завжди залишається за strict_similar_day fallback, доки promotion gate не доводить стійку перевагу.
- `docs/thesis/chapters/03-Methodology.md:133` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: У методології важливо розділити кілька різних класів методів. NBEATSx і TFT належать до forecast layer: вони оцінюють майбутні ціни або розподіл можливих цін. LP optimizer належить до decision layer: він перетворює forecast на feasible battery schedule. AFL, DFL, Schedule/Value Learner і DT-related experiments належать до research/evidence layer: вони переві...
- `docs/thesis/chapters/03-Methodology.md:141` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: Термінологічно в роботі використовуються два окремі поняття: DFL (Decision-Focused Learning) і DT (Decision Transformer). Якщо у робочих нотатках зустрічається скорочення на кшталт "DTFL", у фінальному академічному тексті його краще розкласти на DFL/DT, бо DFL є принципом навчання під downstream decision loss, а DT є окремою sequence-model архітектурою для o...
- `docs/thesis/chapters/03-Methodology.md:162` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: Calibration у цій роботі є окремим методологічним шаром між raw forecast і decision layer. Його задача полягає не в тому, щоб довести перевагу моделі за MAE/RMSE, а в тому, щоб виправити систематичні forecast-зсуви, які найбільше шкодять downstream arbitrage decision. Для BESS-арбітражу однакова forecast помилка в різні години не має однакової ціни: помилка...
- `docs/thesis/chapters/03-Methodology.md:197` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: де \(RR_{\tau}\) - regret ratio попереднього forecast-to-schedule оцінювання. Отже, anchors, на яких forecast призводив до більшої downstream-втрати, мають більшу вагу в bias correction. Якщо prior history ще недостатня, correction дорівнює нулю, а рядок отримує статус insufficient_prior_history. Це важливо для захисту від leakage: calibration не підглядає в...
- `docs/thesis/chapters/03-Methodology.md:204` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: У проєкті існують два практичні calibration контури. Перший був compact research contour для tft_silver_v0 і nbeatsx_silver_v0, де horizon_regret_weighted_forecast_calibration_frame створював tft_horizon_regret_weighted_calibrated_v0 і nbeatsx_horizon_regret_weighted_calibrated_v0. Його роль була діагностичною: перевірити, чи decision-weighted bias correctio...
- `docs/thesis/chapters/03-Methodology.md:216` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: Після calibration прогноз не приймається напряму як рекомендація. Calibrated forecast знову проходить той самий LP dispatch, strict LP/oracle scoring і schedule/value candidate library. Саме тому calibration у роботі є допоміжним decision-aware preprocessing, а не окремим promotion claim. Наприклад, official global-panel NBEATSx після horizon calibration ста...
- `docs/thesis/chapters/03-Methodology.md:229` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: Ця таблиця також визначає, де саме в дипломі кожен метод повинен з'являтися. Розділ 2 обґрунтовує методи через літературу: NBEATSx, TFT, EPF benchmarks, SPO/DFL, storage-specific DFL, differentiable optimization layers і Decision Transformer. Розділ 3 пояснює, як ці методи застосовані в експерименті та які дані вони мають право бачити. Розділ 4 уже показує ф...
- `docs/thesis/chapters/03-Methodology.md:238` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: NBEATSx у цій роботі використовується тому, що electricity-price forecasting часто має трендові, сезонні та екзогенні компоненти. У compact реалізації модель має trend stack і exogenous stack; в official/global-panel lane вона розглядається як серйозніший forecast source для кількох tenants. Але у всіх випадках методологічний критерій однаковий: прогноз NBEA...
- `docs/thesis/chapters/03-Methodology.md:245` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: TFT використовується як багатогоризонтна transformer-style модель із інтерпретованими feature weights і quantile виходами. Його p10/p50/p90 не трактуються автоматично як гарантований confidence interval. У проекті ці квантилі використовуються обережніше: як альтернативні forecast sources для schedule/value gate. Це дозволяє перевірити risk-aware поведінку бе...
- `docs/thesis/chapters/03-Methodology.md:252` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: DFL у методології означає не "вже розгорнуту нейромережу-контролер", а принцип: навчати або вибирати модель за якістю downstream рішення. У поточному стані проєкту це реалізовано через кілька безпечних наближень: AFL labels, forecast-decision-loss v1, schedule/value selectors, candidate-value scorers і V2+-anchored DFL/DT bridge. Усі вони мають однаковий aca...
- `docs/thesis/chapters/03-Methodology.md:261` heading `3.5. Методологічна роль ML, calibration, DFL і DT методів`
  - Text: Decision Transformer розглядається як майбутній policy layer, тому що storage arbitrage є послідовною задачею: рішення в одній годині змінює SOC і обмежує наступні години. Проте поточний DT у репозиторії є policy-preview research primitive. Raw DT action не довіряється напряму: вона має проходити детерміновану projection/feasibility перевірку, порівнюватися...
- `docs/thesis/chapters/03-Methodology.md:286` heading `3.6. Метрики оцінювання і роль regret`
  - Text: Особливе місце займає regret, тобто втрачена економічна можливість порівняно з oracle LP. У цій роботі oracle не є моделлю для використання в реальному часі. Це perfect-foresight evaluator: після завершення horizon він бачить realized prices і розв'язує той самий LP із тими самими battery constraints, SOC bounds, efficiency, market caps та degradation proxy....
- `docs/thesis/chapters/03-Methodology.md:309` heading `3.6. Метрики оцінювання і роль regret`
  - Text: де \(p_{i,t,h}^{real}\) - реалізована ціна DAM у грн/МВт·год, \(P_{i,t,h}^{(s)}\) - signed net power LP schedule: додатне значення означає розряд/продаж, від'ємне - заряд/купівлю, \(\Delta h\) - тривалість інтервалу в годинах, а \(C_{deg}\) - деградаційний штраф, пов'язаний із throughput. У реалізації це відповідає полю decision_value_uah: schedule, побудова...
- `docs/thesis/chapters/03-Methodology.md:333` heading `3.6. Метрики оцінювання і роль regret`
  - Text: де \(\mathcal{A}_{i,t}\) - множина feasible schedules, дозволених battery capacity, power, SOC, efficiency та market-cap constraints. Governance і safety constraints перевіряються окремо як обмежувальні метрики. Тоді regret стратегії:
- `docs/thesis/chapters/03-Methodology.md:342` heading `3.6. Метрики оцінювання і роль regret`
  - Text: Нульовий regret означає, що стратегія досягла oracle-equivalent value під тими самими constraints. Чим більший regret, тим більше економічної цінності втрачено. Нормована версія зберігається як regret_ratio:
- `docs/thesis/chapters/03-Methodology.md:351` heading `3.6. Метрики оцінювання і роль regret`
  - Text: У коді ця логіка реалізована в evaluate_forecast_candidates_against_oracle: для кожного кандидата будується LP schedule, потім _actual_decision_value_uah рахує realized value за фактичними цінами, oracle LP рахує oracle_value_uah, а regret_uah визначається як max(0.0, oracle_value_uah - decision_value_uah). Результат зберігається в strategy evaluation store...
- `docs/thesis/chapters/03-Methodology.md:373` heading `3.6. Метрики оцінювання і роль regret`
  - Text: Mean regret показує середню втрату економічної цінності, а median regret захищає інтерпретацію від одиничних extreme anchors. У promotion gate кандидата недостатньо оцінювати лише за найкращим середнім значенням: він має не погіршувати median regret, проходити rolling-window robustness, не порушувати safety constraints і залишати strict_similar_day fallback....
- `docs/thesis/chapters/03-Methodology.md:385` heading `3.7. Перехід від ML pipeline до рекомендаційного schedule`
  - Text: Кінцевий результат ML/Data pipeline у цій роботі не є безпосередньою командою для біржі або фізичного інвертора. Його коректно описувати як рекомендаційний read-model schedule: погодинний план заряджання, розряджання або утримання батареї на наступний горизонт планування. Для поточного MVP горизонт становить 24 години DAM із погодинним кроком. Такий schedule...
- `docs/thesis/chapters/03-Methodology.md:396` heading `3.7. Перехід від ML pipeline до рекомендаційного schedule`
  - Text: mermaid flowchart LR A["Tenant, SOC, price and context data"] --> B["Forecast or selected strategy"] B --> C["Deterministic LP optimizer"] C --> D["Feasible hourly schedule"] D --> E["Projected SOC, throughput and degradation"] E --> F["Recommendation/read model for operator"] D --> G["Offline realized-value scoring"] G --> H["Oracle, regret and promotion ev...
- `docs/thesis/chapters/03-Methodology.md:407` heading `3.7. Перехід від ML pipeline до рекомендаційного schedule`
  - Text: У простішому формулюванні модель не натискає "купити" або "продати" самостійно. Вона дає прогноз або вибирає candidate schedule family. Далі детермінований LP-шар перетворює це в фізично допустимий погодинний план із SOC constraints, power limits, efficiency та degradation penalty. Після цього read-model шар показує оператору, що саме рекомендується зробити...
- `docs/thesis/chapters/03-Methodology.md:417` heading `Операторський preview-режим`
  - Text: Для operator-facing режиму FastAPI формує рекомендацію у кілька кроків. Спочатку визначається tenant, його battery metrics, location-aware price history, поточний або fallback SOC та доступні strategy options. Якщо вибрана official NBEATSx/TFT strategy має валідні forecast-store rows і ціни не порушують DAM caps, ці forecast points передаються в той самий Le...
- `docs/thesis/chapters/03-Methodology.md:465` heading `Операторський preview-режим`
  - Text: З погляду бідингу та арбітражу кожний ряд schedule можна інтерпретувати як кандидатну дію: напрям BUY/CHARGE, SELL/DISCHARGE або HOLD, обсяг \(|u_h|\Delta h\) у МВт·год і очікуваний net value для цієї години. Однак у поточній дипломній межі це лише bid recommendation, а не executable bid: система ще не формує ринковий order payload, не подає заявку на DAM, н...
- `docs/thesis/chapters/03-Methodology.md:483` heading `Офлайн-навчання та оцінювання`
  - Text: Під час offline training/evaluation той самий forecast-to-LP механізм використовується не для рекомендації оператору, а для створення labels, порівняння стратегій і promotion evidence. Dagster materializes observed DAM prices, weather/context features, forecast candidates, benchmark frames і DFL/schedule-value assets. Для кожного rolling-origin anchor модель...
- `docs/thesis/chapters/03-Methodology.md:491` heading `Офлайн-навчання та оцінювання`
  - Text: Коли realized prices для horizon уже відомі, offline evaluator перераховує економіку вибраного schedule на фактичних цінах, будує oracle LP для того самого tenant-anchor і рахує decision_value_uah, oracle_value_uah, regret_uah та regret_ratio. Ці rows не є рекомендаціями для виконання в реальному часі. Вони є навчальним і доказовим матеріалом: з них формуєть...
- `docs/thesis/chapters/03-Methodology.md:499` heading `Офлайн-навчання та оцінювання`
  - Text: Schedule/Value Learner V2/V2+ працює саме на цьому рівні. Він не генерує сиру команду батареї, а вибирає між already feasible LP-scored schedules за prior-only schedule/value features. Candidate-Value DFL v3 і DT-related experiments також залишаються offline: вони можуть навчатися оцінювати або ранжувати trajectory candidates, але final action усе одно має б...
- `docs/thesis/chapters/03-Methodology.md:512` heading `3.8. Schedule/Value Learner V2 як decision-value selector`
  - Text: Після первинного forecast-to-schedule evaluation у роботі вводиться проміжний decision-aware шар: Schedule/Value Learner V2. Його методологічна роль полягає не в тому, щоб безпосередньо навчати новий контролер батареї, а в тому, щоб порівнювати набір уже feasible LP-scored schedules і вибирати schedule з найкращим очікуваним decision value за prior-only озна...
- `docs/thesis/chapters/03-Methodology.md:531` heading `3.8. Schedule/Value Learner V2 як decision-value selector`
  - Text: Для official global-panel 365-anchor evidence бібліотека будується окремо для кожного tenant, source model та anchor. У поточному конфігу розглядаються два source models: nbeatsx_official_global_panel_v1 і nbeatsx_official_global_panel_horizon_calibrated_v1. Для кожного такого source model формується до десяти конкретних schedule-candidates:

## Interpretation

- External plagiarism risk from direct or lightly modified local source prose is low for the checked chapters.
- Internal self-similarity is mostly a style/traceability issue: thesis chapters may reuse repo docs, but supervisor-facing thesis prose should cite artifacts or be independently phrased where appropriate.
- The citation/support gap list is the most useful next work queue for methodology: add paper citations for general scientific claims and repo artifact references for implementation/evidence claims.
- This audit does not certify originality against other students, because that requires access to the student-paper corpus used by the university similarity system.
