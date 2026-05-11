# Розділ 2. Огляд літератури

## 2.1. Методологічна роль огляду літератури

Автономний енергоарбітраж для battery energy storage systems (BESS) є міждисциплінарною задачею, у якій поєднуються прогнозування цін на електроенергію, оптимізація режимів заряду та розряду, економічне врахування деградації батареї, валідація фізичних обмежень і відтворювана MLOps-інфраструктура. Тому огляд літератури для цієї дипломної роботи не може обмежуватися лише переліком моделей прогнозування. Він має пояснити, чому прогноз сам по собі не є достатнім результатом, чому потрібен прозорий оптимізаційний baseline і чому перехід до Decision-Focused Learning (DFL) має відбуватися після стабілізації реального rolling-origin benchmark.

У межах роботи центральним дослідницьким питанням є не абстрактна точність прогнозу, а якість рішення для українського ринку "на добу наперед" (DAM): чи може decision-aware forecasting зменшити regret відносно замороженого `strict_similar_day` LP-control, зберігаючи SOC-feasibility, UAH-native економіку, throughput/degradation proxy та прозорі safety constraints. Саме тому літературний огляд структурується навколо послідовності `forecast -> optimize -> validate -> compare regret -> promote only if decision value improves`.

Поточна реалізація репозиторію підтверджує інженерну здійсненність Level 1 BESS arbitrage MVP: DAM-only hourly scope, observed OREE DAM та historical Open-Meteo evidence для thesis-grade benchmark, `strict_similar_day` як контрольний forecast comparator, LP-based dispatch, feasibility-and-economics preview model, Pydantic safety semantics, Dagster Bronze/Silver/Gold lineage, FastAPI read models і operator-facing dashboard surfaces. Водночас поточні результати не є live trading, market execution, full digital twin, production Decision Transformer або повністю реалізованим DFL-controller. Це розмежування є принциповим для академічної коректності розділу.

## 2.2. Практичний ринковий контекст BESS-арбітражу

У практичних BESS-системах типовий контур прийняття рішень складається з кількох шарів: збір ринкових і технічних даних, короткострокове прогнозування ціни або навантаження, optimization-based scheduling, перевірка фізичних обмежень, операторський інтерфейс і контрольний журнал подій. Така практика рідко починається з повністю навчуваної end-to-end policy. Спочатку будується інженерно прозорий baseline, який можна перевірити, пояснити й використати як контрольну групу.

Для українського DAM-контексту така поетапність має додаткове значення. Ринок має власні правила, price caps, валюту UAH, локальні особливості попиту та пропозиції, а також поступове зближення з європейською market-coupling логікою. Отже, механічне перенесення європейських або американських BESS-рішень без валідації на українських OREE data не є достатнім доказом ефективності. У дипломній роботі DAM-only Level 1 scope виступає не обмеженням амбіції, а способом ізолювати першу перевірювану задачу: погодинний arbitrage scheduling з єдиною валютою, єдиним ринком і контрольованим набором фізичних припущень.

Європейські джерела про BESS revenue stacking, участь у кількох ринках і ancillary services залишаються важливими для майбутнього розвитку. Однак у поточному scope вони не мають підміняти український evidence layer. Multi-venue DAM/IDM/balancing optimization є логічним наступним етапом, але поточна дипломна аргументація має спиратися на відтворюваний DAM benchmark, а не на ширшу roadmap-архітектуру.

Після стратегічного рев'ю архітектури цей ринковий контекст також прив'язано до первинних регуляторних і політичних джерел. NEURC Resolution No. 621 від 23 квітня 2026 року задає ефективно датовані price caps для українських DAM/IDM і balancing-market контурів, тому forecast sanity gate має бути venue-specific, а не універсальним кліпінгом. OREE 2026 tariff notice додає окремий економічний шар: якщо диплом переходить від gross LP schedule value до net market profit, потрібно враховувати transaction tariff і fixed software fee. European Commission electricity-market-design матеріали, Ukraine NECP to 2030 та Energy Strategy to 2050 використовуються як policy context для flexibility, reconstruction, EU integration і майбутньої market-coupling логіки, але не як доказ поточного live market execution.

## 2.3. Енергоарбітраж BESS як задача оптимізації

Енергоарбітраж батарейного накопичувача зазвичай формулюється як задача вибору моментів заряду та розряду з метою максимізації економічного результату за наявності обмежень на потужність, ємність, початковий і кінцевий state of charge, round-trip efficiency та допустимий діапазон SOC. У базовій постановці така задача добре описується лінійним програмуванням або змішаними цілочисельними моделями. Її перевагою є прозорість: вхідна цінова траєкторія, фізичні обмеження та фінальний schedule пов'язані формально й відтворювано.

Література з battery scheduling показує, що LP/MILP-підходи залишаються сильним контрольним інструментом навіть у роботах, які надалі переходять до складніших learned policies. Park et al. обґрунтовують короткострокове ESS scheduling через лінійні обмеження SOC, power limits та efficiency. Hesse et al. і Maheshwari et al. демонструють, що degradation-aware dispatch може бути розширений через ageing-aware або non-linear battery degradation models, але такі моделі різко підвищують складність і вимоги до параметризації. Vykhodtsev et al. додатково підкреслюють, що в techno-economic studies батарея часто описується спрощеним power-energy representation, який придатний для системного аналізу, але не замінює повний electrochemical digital twin.

Для дипломної роботи з цього випливає практичний висновок: frozen LP baseline є необхідною контрольною групою. Він не претендує на повну фізичну модель батареї, але дає стабільний спосіб порівнювати forecasting та DFL-candidates за UAH net value, throughput, degradation proxy, SOC feasibility та oracle regret. Поточний battery layer тому коректно описувати як feasibility-and-economics preview model, а не як повний P2D/SEI/thermal digital twin.

## 2.4. Прогнозування цін: від baseline до NBEATSx і TFT

Для BESS-арбітражу прогнозування ціни є необхідним, але не достатнім компонентом. Forecast визначає очікувану форму ціни на горизонті, однак економічний результат виникає лише після оптимізаційного перетворення forecast у dispatch schedule. Саме тому література з electricity price forecasting (EPF) має розглядатися разом із decision evaluation.

NBEATSx, описаний Olivares et al., є релевантним для EPF через поєднання neural basis expansion, декомпозиційної логіки та exogenous variables. Для ринку електроенергії це важливо, оскільки цінові ряди мають денні, тижневі, сезонні та регуляторні компоненти, а також реагують на погодні й системні фактори. NBEATSx підтримує майбутній forecast-layer розвиток диплома, але сам факт використання цієї архітектури не гарантує кращого arbitrage schedule.

Temporal Fusion Transformer (TFT), запропонований Lim et al., є іншим важливим напрямом. Його сильна сторона полягає в multi-horizon forecasting, роботі зі static covariates, known future inputs, observed historical inputs, variable selection та attention-based interpretability. Для дипломної роботи це особливо корисно, оскільки майбутній operator-facing system має не лише прогнозувати ціну, а й пояснювати, які фактори вплинули на прогноз. Джерела про probabilistic TFT, TimeXer та deep-learning EPF розширюють цю логіку в напрямі exogenous-aware та uncertainty-aware forecasting.

Водночас результати поточного репозиторію демонструють важливе методологічне обмеження: compact `nbeatsx_silver_v0` і `tft_silver_v0` не мають оцінюватися лише за тим, наскільки вони належать до SOTA-родини. У rolling-origin LP/oracle evidence вони неодноразово програвали `strict_similar_day` за regret, хоча окремі calibration, trajectory-selector та source-specific experiments покращували їх відносно raw neural schedules. Це не спростовує літературу про NBEATSx/TFT, а показує різницю між forecast accuracy і decision value. Для BESS важливі top/bottom price hours, spread shape, rank stability і LP-value, а не лише MAE/RMSE.

## 2.5. Predict-then-Optimize, regret і Decision-Focused Learning

Класичний Predict-then-Optimize (PTO) підхід розділяє прогнозування та оптимізацію: модель спочатку навчається передбачати параметри задачі, а потім оптимізатор використовує ці параметри для прийняття рішення. Elmachtoub and Grigas показують, що такий поділ може бути неузгодженим із кінцевою decision quality, оскільки мала статистична помилка не завжди означає малий decision regret. Їхній Smart Predict-then-Optimize / SPO+ підхід є методологічною основою для оцінювання моделей через downstream objective.

Decision-Focused Learning розвиває цю ідею: модель має навчатися так, щоб покращувати кінцеве рішення, а не лише forecast metric. Mandi et al. систематизують DFL як напрям, у якому learning model і constrained optimization problem поєднуються через decision loss, differentiable optimization, surrogate gradients або gradient-free methods. Для storage arbitrage це особливо важливо, оскільки заряд і розряд пов'язані між собою через SOC trajectory, а помилка на одній годині може змінити feasible actions на наступних годинах.

Sang et al. безпосередньо пов'язують electricity price prediction з ESS arbitrage і показують, що decision-focused price prediction має оцінюватися через regret та arbitrage value. Persak and Anjos додатково підкреслюють multistage nature storage arbitrage: незалежна hourly classification або локальна forecast correction не враховує повної path-dependence задачі. Perturbed DFL for energy storage пояснює, чому для LP-шару можуть бути потрібні perturbed або surrogate losses, якщо strict optimizer є недиференційованим або нестабільним для прямого backpropagation.

У цьому контексті regret у дипломній роботі виконує роль центральної метрики. Він вимірює втрачений економічний результат відносно oracle LP, який має доступ до realized prices для offline evaluation. Oracle не є deployable strategy і не використовується як live forecast input; він потрібен лише для оцінювання. Такий поділ дозволяє не змішувати навчальні labels, offline diagnostics і фактичні operational decisions.

## 2.6. Differentiable optimization і Decision Transformer

Перехід від PTO до DFL технічно спирається на можливість поєднати optimization layer із learning pipeline. Agrawal et al. у роботі про Differentiable Convex Optimization Layers та Amos and Kolter у OptNet показують, що певні convex optimization problems можуть бути включені до neural network як differentiable layers. Для BESS-арбітражу це відкриває шлях до relaxed storage layer, у якому модель отримує градієнт не лише від forecast error, а й від decision loss.

Однак direct differentiable LP не є автоматично достатнім. Storage arbitrage має complementarity-like поведінку charge/discharge, SOC bounds, power limits, degradation proxy і часову залежність між діями. Тому strict LP має залишатися фінальним evaluator, навіть якщо training використовує relaxed або surrogate layer. Поточна дипломна логіка відповідає цій вимозі: relaxed/DFL experiments можуть бути research challengers, але promotion відбувається тільки через strict LP/oracle gate.

Decision Transformer є окремим, пізнішим напрямом. Chen et al. формулюють Decision Transformer як return-conditioned sequence modeling для offline reinforcement learning, де модель генерує дії, умовно на desired return-to-go та історію станів і дій. Для BESS це концептуально привабливо, оскільки schedule можна подати як траєкторію з reward/value signal. Водночас Bhargava et al. та практичні offline RL джерела застерігають, що DT не є гарантовано кращим за behavior cloning або прості value/ranking methods без достатнього обсягу якісних trajectories.

Тому в межах дипломної роботи DT слід позиціонувати як offline research primitive, а не як production controller. Найближчий коректний шлях до DT проходить через trajectory dataset, action/value labels, strict-vs-relaxed fixture checks, residual schedule/value learner і fallback на `strict_similar_day`. Лише після цього tiny offline DT candidate може бути порівняний з filtered behavior cloning та strict LP/oracle benchmark.

## 2.7. Деградація батареї та межа digital twin

Деградація батареї є одним із ключових факторів економіки BESS-арбітражу. Якщо dispatch оцінює лише gross arbitrage revenue, він може завищувати реальну цінність циклування. Grimaldi et al., Hesse et al., Maheshwari et al., Kumtepeli et al. та NREL cost/performance sources показують, що деградація може враховуватися на різних рівнях складності: від простого throughput або equivalent full cycle proxy до нелінійних моделей, залежних від depth of discharge, temperature, C-rate і calendar ageing.

Поточна реалізація диплома використовує throughput/EFC-based degradation proxy як економічний штраф. Такий підхід не є повною electrochemical ageing model, але він краще відповідає Level 1 MVP, ніж ігнорування degradation cost. Він дозволяє включити asset wear у net value та regret comparison, не руйнуючи відтворюваність LP benchmark і не вводячи складні параметри, для яких у поточному observed-data scope немає достатньо telemetry.

Отже, література підтримує обережну позицію: диплом може стверджувати наявність degradation-aware economics proxy, але не повного digital twin. Розширення до cycle-depth-aware, temperature-aware або telemetry-calibrated ageing model є перспективним напрямом, однак воно має виконуватися після стабілізації real-data benchmark і decision-value evaluation.

Додатково Kumtepeli et al. 2020 та Cao et al. 2020 показують дві сильніші майбутні траєкторії: degradation-aware optimization з electro-thermal і semi-empirical ageing models та learning-based arbitrage з точнішою lithium-ion degradation model. Для цього репозиторію ці джерела є аргументом на користь roadmap до глибшого battery model, але не підставою описувати поточний throughput/EFC penalty як повний digital twin.

## 2.8. Відтворювана MLOps-архітектура та evidence pipeline

Для інженерної дипломної роботи важливо не лише запропонувати модель, а й довести, що експериментальний результат відтворюється. Саме тому MLOps-шар є частиною архітектурної аргументації. Dagster software-defined assets забезпечують lineage і materialization protocol, MLflow фіксує run-level метрики, Postgres зберігає read-model evidence, FastAPI надає dashboard-facing endpoints, а Nuxt dashboard показує operator-facing summaries без змішування їх із market execution.

AI governance також стає частиною архітектурного обґрунтування. EU AI Act описує risk-based підхід до AI systems, а для safety-relevant інфраструктурних контурів підкреслює data quality, logging, documentation, human oversight, robustness, cybersecurity і accuracy. Для диплома це підтримує не "автономність будь-якою ціною", а протилежну інженерну позицію: deterministic Pydantic Gatekeeper, explicit validation failures, human/operator review, provenance metadata і `market_execution_enabled=false` мають бути центральними елементами системи.

Medallion architecture у проєкті виконує методологічну функцію. Bronze assets відповідають за ingestion і provenance, Silver assets формують tenant-aligned features, Gold assets публікують forecast, benchmark, calibration, selector, DFL-readiness і diagnostics. Така структура дозволяє відділити observed OREE/Open-Meteo evidence від synthetic/demo fallback і явно позначати data quality tier.

Після Week 3 materializations початковий ризик synthetic/demo-oriented market/weather layer не є коректним описом усього поточного evidence state. Synthetic fallback все ще існує для demo/runtime stability, але supervisor-facing benchmark і DFL-readiness artifacts мають explicit provenance. Там, де результати позначені як `thesis_grade`, вони спираються на observed OREE DAM та historical Open-Meteo, а не на synthetic rows. Водночас ширше розширення до 180+ anchors поки блокується unrecovered `2026-03-29 23:00` price/weather gap, тому такі розширені результати мають маркуватися як coverage-gap research panel, а не як повністю thesis-grade promotion evidence.

## 2.9. Real-data benchmark і сильний baseline

Література з EPF benchmarking, зокрема Lago et al., підкреслює необхідність порівняння складних моделей із сильними простими baseline. У цьому проєкті таку роль виконує `strict_similar_day`. Його сутність полягає у використанні історично доступної подібної доби як прогнозної траєкторії для наступного горизонту; після цього однаковий LP-solver формує feasible charge/discharge schedule, а realized prices використовуються тільки для offline scoring.

Цей baseline є сильним саме тому, що електроенергетичні ціни часто мають повторювані добові та тижневі структури. На короткому горизонті простий seasonal comparator може бути конкурентним або навіть сильнішим за compact neural forecast, особливо якщо training panel малий, а decision objective залежить від правильної ідентифікації кількох найцінніших годин. Тому програш NBEATSx/TFT або DFL-probes strict baseline не слід трактувати як помилку наукової ідеї. Це валідний негативний результат, який показує, що model family не має автоматично замінювати decision benchmark.

Поточні evidence artifacts підтверджують саме таку позицію. Week 3 Dnipro benchmark дав thesis-grade rolling-origin evidence на observed OREE/Open-Meteo scope. Подальші 90-anchor calibration, action-label, trajectory/value, strict-failure selector, robustness і regime-gated TFT experiments показали, що окремі neural-derived candidates можуть покращувати raw neural schedules, а TFT мав credible latest-holdout signal. Проте production promotion залишається заблокованою, оскільки robustness, coverage і rolling strict-control gates не підтвердили стабільної переваги над `strict_similar_day`.

## 2.10. Європейські джерела, market coupling і зовнішня валідація

Європейські dataset sources є важливими для майбутнього розвитку диплома, але їхня роль має бути чітко обмежена. ENTSO-E Transparency Platform, Open Power System Data, OPSD time series, Nord Pool Data Portal, Ember API, PriceFM і THieF задають напрям external validation, market-coupling context, cross-region electricity-price forecasting і temporal hierarchy forecasting. Вони можуть стати джерелом польських, словацьких, угорських, румунських або ширших європейських exogenous features.

Окремо потрібно розрізняти grid synchronization, policy integration і фактичну market coupling implementation. ENTSO-E synchronization notes підтверджують синхронізацію України та Молдови з Continental Europe, але це саме по собі не означає, що український DAM для всіх delivery dates повністю працює як SDAC/SIDC-coupled market. ACER Energy Community MCO integration-plan матеріали і European Commission market-coupling sources корисні для roadmap і external-market feature governance, однак вони вимагають окремих coupling-status flags, publication-time checks і source licensing перед використанням у training panel.

Проте європейські rows не мають змішуватися з українським training/evaluation panel без окремої підготовки. Потрібні licensing checks, timezone та DST alignment, currency normalization, market-rule mapping, price-cap semantics, publication-time availability і domain-shift validation. До виконання цих умов європейські дані можуть використовуватися як roadmap context або future external-validation source, але не як прямий training target для українського DAM arbitrage.

У найближчій архітектурній траєкторії найбільш виправданим є додавання суміжних ринків як exogenous covariates, а не як заміна українського target. Наприклад, польські day-ahead prices, cross-border spreads, lagged regional prices і market-coupling indicators потенційно можуть покращити forecast layer, якщо вони доступні до моменту прийняття рішення. Таке розширення має бути перевірене через той самий rolling-origin LP/oracle protocol.

## 2.11. Поточний стан реалізації та межі тверджень

Огляд літератури обґрунтовує три рівні архітектури, які не можна змішувати. Перший рівень — реалізований Level 1 control contour: observed-data benchmark, `strict_similar_day`, LP dispatch, degradation proxy, feasibility validation, read models і evidence checks. Другий рівень — research challenger layer: NBEATSx/TFT, calibration, selectors, AFL/AFE diagnostics, trajectory/value rankers, residual DFL probes і offline DT preparation. Третій рівень — future production architecture: DFL/DT strategy layer, richer digital twin, multi-venue markets і regulated execution semantics.

Поточні твердження диплома мають залишатися в межах першого і частково другого рівня. Коректно стверджувати, що репозиторій реалізує reproducible forecast-to-optimize research system для українського DAM BESS arbitrage, використовує observed OREE DAM + tenant-aware Open-Meteo у thesis-grade evidence slices, проводить leakage-aware rolling-origin evaluation, оцінює candidates через LP/oracle regret і зберігає `strict_similar_day` як frozen control comparator. Некоректно стверджувати, що система вже виконує live trading, повний multi-market optimization, deployed Decision Transformer policy або повний electrochemical digital twin.

Таке розмежування не послаблює диплом. Навпаки, воно робить наукову позицію сильнішою: негативні або заблоковані neural/DFL candidates стають evidence про складність decision-value improvement, а не невдачею реалізації. Дослідницька новизна полягає в побудові відтворюваного decision-value benchmark і поетапному переході до DFL, а не в передчасному оголошенні SOTA superiority.

## 2.12. Висновки до розділу

Розглянута література показує, що задача автономного BESS-арбітражу має вирішуватися не як ізольоване прогнозування ціни, а як повний decision pipeline. EPF-моделі на кшталт NBEATSx і TFT є обґрунтованими forecast candidates, але їхня цінність має перевірятися через downstream LP scheduling, net value і regret. LP baseline є необхідною контрольною групою, degradation proxy — мінімально необхідним економічним уточненням, а DFL — наступним research layer після стабілізації benchmark.

Для дипломної роботи з цього випливає чітка архітектурна логіка. Спочатку формується observed-data rolling-origin benchmark для українського DAM. Далі strong baseline заморожується як comparator. Після цього NBEATSx/TFT, calibration, AFL/AFE, selectors і residual DFL candidates порівнюються з ним за decision value. Лише candidate, який проходить coverage, no-leakage, safety, mean-regret, median-regret і robustness gates, може претендувати на offline/read-model promotion. Live market execution, deployed DT controller і повний digital twin залишаються поза поточним scope.

Отже, літературний огляд підтримує основну траєкторію диплома: `real-data benchmark -> forecast decision-value comparison -> robustness -> DFL research challenger -> future offline DT`. Така послідовність є академічно обережною, інженерно відтворюваною і сумісною з фактичним станом репозиторію.

## 2.13. Джерела, використані в поточній версії розділу

1. Yi et al. A Decision-Focused Predict-then-Bid Framework for Energy Storage Arbitrage. DOI: 10.48550/arXiv.2505.01551. https://arxiv.org/abs/2505.01551.
2. Olivares et al. Neural basis expansion analysis with exogenous variables: Forecasting electricity prices with NBEATSx. DOI: 10.1016/j.ijforecast.2022.03.001. https://arxiv.org/abs/2201.12886.
3. Jiang et al. Probabilistic electricity price forecasting based on penalized temporal fusion transformer. DOI: 10.1002/for.3084.
4. Elmachtoub and Grigas. Smart "Predict, then Optimize". DOI: 10.1287/mnsc.2020.3922.
5. Grimaldi et al. Profitability of energy arbitrage net profit for grid-scale battery energy storage considering dynamic efficiency and degradation using a linear, mixed-integer linear, and mixed-integer non-linear optimization approach. DOI: 10.1016/j.est.2024.112380.
6. Lim et al. Temporal Fusion Transformers for Interpretable Multi-horizon Time Series Forecasting. DOI: 10.48550/arXiv.1912.09363. https://arxiv.org/abs/1912.09363.
7. Chen et al. Decision Transformer: Reinforcement Learning via Sequence Modeling. DOI: 10.48550/arXiv.2106.01345.
8. Agrawal et al. Differentiable Convex Optimization Layers. DOI: 10.48550/arXiv.1910.12430. https://arxiv.org/abs/1910.12430.
9. Vykhodtsev et al. A review of modelling approaches to characterize lithium-ion battery energy storage systems in techno-economic analyses of power systems. DOI: 10.1016/j.rser.2022.112584.
10. Hesse et al. Ageing and efficiency aware battery dispatch for arbitrage markets using mixed integer linear programming. DOI: 10.3390/en12060999.
11. Maheshwari et al. Optimizing the operation of energy storage using a non-linear lithium-ion battery degradation model. DOI: 10.1016/j.apenergy.2019.114360.
12. Hu et al. Potential utilization of battery energy storage systems (BESS) in the major European electricity markets. DOI: 10.1016/j.apenergy.2022.119512.
13. Li et al. Temporal-Aware Deep Reinforcement Learning for Energy Storage Bidding in Energy and Contingency Reserve Markets. DOI: 10.1109/TEMPR.2024.3372656.
14. Lago et al. Forecasting day-ahead electricity prices: A review of state-of-the-art algorithms, best practices and an open-access benchmark. DOI: 10.1016/j.apenergy.2021.116983.
15. Wang et al. TimeXer: Empowering Transformers for Time Series Forecasting with Exogenous Variables. DOI: 10.48550/arXiv.2402.19072.
16. Yu et al. Deep Learning for Electricity Price Forecasting: A Review of Day-Ahead, Intraday, and Balancing Electricity Markets. DOI: 10.48550/arXiv.2602.10071.
17. Yu et al. PriceFM: Foundation Model for Probabilistic Electricity Price Forecasting. arXiv:2508.04875.
18. Lipiecki et al. Stealing Accuracy: Predicting Day-ahead Electricity Prices with Temporal Hierarchy Forecasting (THieF). arXiv:2508.11372.
19. Meyer et al. Rethinking Evaluation in the Era of Time Series Foundation Models: (Un)known Information Leakage Challenges. arXiv:2510.13654.
20. Dange and Sarawagi. TFMAdapter: Lightweight Instance-Level Adaptation of Foundation Models for Forecasting with Covariates. arXiv:2509.13906.
21. Fu et al. Reverso: Efficient Time Series Foundation Models for Zero-shot Forecasting. arXiv:2602.17634.
22. Madahi et al. Distributional Reinforcement Learning-based Energy Arbitrage Strategies in Imbalance Settlement Mechanism. arXiv:2401.00015.
23. ENTSO-E Transparency Platform. Electricity Market Transparency. https://www.entsoe.eu/data/transparency-platform/.
24. Open Power System Data. Open European power-system data platform. https://open-power-system-data.org/.
25. Open Power System Data. Time series data package. https://data.open-power-system-data.org/time_series/.
26. Nord Pool. Data Portal and market-data services. https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/.
27. Ember. API for open electricity data. https://ember-energy.org/data/api.
28. Mandi et al. Decision-Focused Learning: Foundations, State of the Art, Benchmark and Future Opportunities. DOI: 10.1613/jair.1.15320 / arXiv:2307.13565. https://arxiv.org/abs/2307.13565.
29. Sang et al. Electricity Price Prediction for Energy Storage System Arbitrage: A Decision-Focused Approach. DOI: 10.1109/TSG.2022.3166791 / arXiv:2305.00362. https://doi.org/10.1109/TSG.2022.3166791.
30. Persak and Anjos. Decision-Focused Forecasting: Decision Losses for Multistage Optimisation. arXiv:2405.14719.
31. Yi, Alghumayjan, and Xu. Perturbed Decision-Focused Learning for Modeling Strategic Energy Storage. arXiv:2406.17085. https://arxiv.org/abs/2406.17085.
32. Bhargava et al. When should we prefer Decision Transformers for Offline Reinforcement Learning? arXiv:2305.14550.
33. Hugging Face. Decision Transformer model documentation. https://huggingface.co/docs/transformers/model_doc/decision_transformer.
34. European Commission. Electricity market design. https://energy.ec.europa.eu/topics/markets-and-consumers/electricity-market-design_en.
35. European Commission. EU electricity trading in the day-ahead markets becomes more dynamic. https://energy.ec.europa.eu/news/eu-electricity-trading-day-ahead-markets-becomes-more-dynamic-2025-10-01_en.
36. NEURC. Resolution No. 621 of 23 April 2026 on price caps for the day-ahead, intraday, and balancing markets. https://www.nerc.gov.ua/acts/pro-hranychni-tsiny-na-rynku-na-dobu-napered-vnutrishnodobovomu-rynku-ta-balansuiuchomu-rynku.
37. JSC Market Operator. The Market Operator tariff for 2026 amounts to UAH 6.88 per MWh. https://www.oree.com.ua/index.php/newsctr/n/30795?lang=english.
38. Open-Meteo. Forecast API documentation. https://open-meteo.com/en/docs.
39. Open-Meteo. Historical Weather API documentation. https://open-meteo.com/en/docs/historical-weather-api.
40. Ministry of Economy of Ukraine. The Government approved the National Energy and Climate Plan until 2030. https://me.gov.ua/News/Detail?id=2642aff1-2328-4bad-b03f-6f0f7dc292c8&lang=uk-UA.
41. Ukraine National Energy and Climate Plan to 2030. https://me.gov.ua/download/2cad4803-661e-4ae9-9748-3006d6eb3e1c/file.pdf.
42. ENTSO-E. Continental Europe successful synchronization with Ukraine and Moldova power systems. https://www.entsoe.eu/news/2022/03/16/continental-europe-successful-synchronisation-with-ukraine-and-moldova-power-systems/.
43. ACER. ACER will decide on the electricity market coupling integration plan for the Energy Community. https://www.acer.europa.eu/news/acer-will-decide-electricity-market-coupling-integration-plan-energy-community.
44. European Commission. AI Act regulatory framework. https://digital-strategy.ec.europa.eu/en/policies/regulatory-framework-ai.
45. Kumtepeli et al. Energy Arbitrage Optimization With Battery Storage: 3D-MILP for Electro-Thermal Performance and Semi-Empirical Aging Models. DOI: 10.1109/ACCESS.2020.3035504. https://doi.org/10.1109/ACCESS.2020.3035504.
46. Cao et al. Deep Reinforcement Learning-Based Energy Storage Arbitrage With Accurate Lithium-Ion Battery Degradation Model. DOI: 10.1109/TSG.2020.2986333. https://doi.org/10.1109/TSG.2020.2986333.

Локальна бібліографічна база, source map і PDF-архіви зібрані в [docs/thesis/sources/README.md](../sources/README.md) та [docs/technical/papers/README.md](../../technical/papers/README.md). Metadata-first intake для нових Week 4 джерел зафіксовано в [docs/thesis/sources/week4-research-ingestion.md](../sources/week4-research-ingestion.md).
