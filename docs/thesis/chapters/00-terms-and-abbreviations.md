# Перелік умовних позначень, скорочень і термінів

Цей розділ фіксує скорочення, одиниці вимірювання та ключові терміни, що
використовуються в пояснювальній записці. Якщо термін має одночасно
дослідницьке й операційне значення, нижче наведено саме те значення, у якому він
використовується в межах цієї дипломної роботи.

## Умовні позначення та одиниці вимірювання

| Позначення | Значення |
|---|---|
| `%` | Відсоток; у роботі використовується, зокрема, для improvement ratio, coverage ratio та rolling-window pass rate. |
| `h` | Година; базова тривалість торгового інтервалу в Level 1 DAM-сценарії. |
| `MW` | Мегават; одиниця середньої потужності у bid/schedule інтервалі. |
| `MWh` | Мегават-година; одиниця енергії, яка виводиться з потужності та тривалості інтервалу. |
| `UAH` | Українська гривня; канонічна валюта для regret, value, degradation proxy та market-economics у роботі. |
| `UAH/MWh` | Ціна або штраф на одиницю енергії. |
| `EUR/MWh` | Типова одиниця європейських day-ahead price джерел; не змішується з UAH без prior-known FX normalization. |
| `USD/kWh` | Одиниця capex anchor для battery cost/degradation proxy. |
| `p10`, `p50`, `p90` | 10-й, 50-й та 90-й квантилі прогнозу; у TFT lane використовуються як risk-aware forecast sources для schedule candidates. |

## Скорочення

| Скорочення | Розшифрування | Значення в роботі |
|---|---|---|
| `AFE` | Automated Feature Engineering | Дослідницький контур пошуку та governance нових prior-only features. |
| `AFL` | Arbitrage-Focused Learning | Forecast-layer діагностика, що оцінює помилки прогнозу через arbitrage-relevant signals, а не лише MAE/RMSE. |
| `ACER` | European Union Agency for the Cooperation of Energy Regulators | Policy/source context для європейської ринкової інтеграції та market-coupling roadmap. |
| `AI` | Artificial Intelligence | Загальний клас методів штучного інтелекту; у роботі конкретизується через forecast models, selectors та DFL/DT roadmap. |
| `API` | Application Programming Interface | Контрактний read-model шар FastAPI; не є механізмом ринкового виконання. |
| `ATB` | Annual Technology Baseline | NREL джерело припущень щодо storage cost/performance. |
| `BESS` | Battery Energy Storage System | Система накопичення енергії, для якої будуються price forecasts, schedules та offline strategy evidence. |
| `CSV` | Comma-Separated Values | Табличний формат експорту evidence rows або source snapshots. |
| `CUDA` | Compute Unified Device Architecture | GPU runtime NVIDIA, який може прискорювати локальне training/smoke runs; не змінює claim boundary. |
| `CVXPY` | Convex Optimization in Python | Бібліотека для формулювання convex optimization задач; пов'язана з майбутнім differentiable optimization напрямом. |
| `cvxpylayers` | Differentiable convex optimization layers | Інструмент для future DFL experiments; поточний headline result не є повним differentiable DFL controller. |
| `DAM` | Day-Ahead Market | Ринок "на добу наперед"; основний market venue Level 1 scope. |
| `DFL` | Decision-Focused Learning | Підхід, де модель або selector оптимізується за якістю downstream decision, тобто regret/value після LP, а не лише forecast error. |
| `DOI` | Digital Object Identifier | Стабільний ідентифікатор академічного джерела. |
| `DST` | Daylight Saving Time | Перехід на літній/зимовий час; критичний для timezone alignment у market/weather даних. |
| `DT` | Decision Transformer | Offline RL sequence model, що генерує дії або schedule elements умовно на return-to-go; у роботі є research roadmap, а не розгорнутий контролер. |
| `EFC` | Equivalent Full Cycle | Одиниця деградаційного навантаження батареї, пов'язана з throughput. |
| `ENTSO-E` | European Network of Transmission System Operators for Electricity | Джерело європейських market/grid даних; у поточних результатах є governance-only, не training input. |
| `EPF` | Electricity Price Forecasting | Прогнозування цін електроенергії. |
| `ESS` | Energy Storage System | Загальний термін для систем накопичення; BESS є battery-specific різновидом ESS. |
| `EU` | European Union | Контекст майбутнього market coupling та зовнішніх джерел; не означає, що EU rows входять у поточне тренування. |
| `FX` | Foreign Exchange | Валютний курс; потрібний для prior-known EUR/UAH normalization зовнішніх price features. |
| `GET` | HTTP GET method | API-метод читання read-model даних. |
| `GPU` | Graphics Processing Unit | Обчислювальний прискорювач; у роботі може використовуватися для neural forecasting або small Torch training. |
| `HF` | Hugging Face | Платформа для papers, models, datasets та guarded Jobs/offload workflow. |
| `IDM` | Intraday Market | Внутрішньодобовий ринок; перебуває поза основним Level 1 DAM-only scope. |
| `JSON` | JavaScript Object Notation | Структурований формат evidence summaries, manifests і API payloads. |
| `KPI` | Key Performance Indicator | Оціночний показник; у дипломі основні decision KPIs прив'язані до regret/value та robustness. |
| `LFP` | Lithium Iron Phosphate | Тип літій-іонної батареї, релевантний для degradation/digital-twin roadmap. |
| `LP` | Linear Programming | Лінійне програмування; основа deterministic schedule optimization та oracle evaluator. |
| `LSTM` | Long Short-Term Memory | Рекурентна neural architecture, що згадується як частина ширшого time-series forecasting контексту. |
| `MAE` | Mean Absolute Error | Forecast-only метрика; у роботі не є головним критерієм promotion. |
| `MCO` | Market Coupling Operator | Релевантний policy/market-coupling термін для майбутньої інтеграції. |
| `MCP` | Model Context Protocol | Допоміжний tooling layer для роботи агентів/конекторів; не є предметом наукової новизни диплома. |
| `MILP` | Mixed-Integer Linear Programming | Оптимізаційний клас, релевантний для складніших storage/market formulations; поточний Level 1 solver є LP. |
| `ML` | Machine Learning | Машинне навчання; у роботі використовується для forecast candidates, selectors і future DFL/DT. |
| `MLflow` | Machine Learning Flow | Інструмент tracking/registry для експериментів і моделей. |
| `MLOps` | Machine Learning Operations | Практики керування життєвим циклом ML-пайплайнів, моделей, evidence та deployment boundaries. |
| `MVP` | Minimum Viable Product | Перший працездатний інженерний контур: DAM-only, strict baseline, LP preview, read-model evidence. |
| `NBEATSx` | Neural Basis Expansion Analysis with Exogenous Variables | Нейронна forecasting architecture з exogenous variables; у роботі є головним source family для V2+ evidence. |
| `NECP` | National Energy and Climate Plan | Policy context для української енергетичної трансформації та flexibility roadmap. |
| `NEURC` / `НКРЕКП` | National Energy and Utilities Regulatory Commission of Ukraine | Регуляторний контекст price caps, tariffs та market rules. |
| `NREL` | National Renewable Energy Laboratory | Джерело storage cost/performance assumptions та ATB матеріалів. |
| `NVIDIA` | NVIDIA Corporation | Виробник GPU; згадується в контексті локального CUDA training runtime. |
| `OOD` | Out-of-Distribution | Стан, коли поточний режим даних відрізняється від train/prior evidence; має блокувати небезпечне просування кандидата. |
| `OPSD` | Open Power System Data | Європейське джерело energy data; у роботі governance/research context, не training input. |
| `OREE` | Оператор ринку електроенергії | Українське джерело observed DAM price history для thesis-grade evidence. |
| `P2D` | Pseudo-two-dimensional battery model | Фізична модель батареї, релевантна для майбутнього digital-twin layer; не реалізована як headline result. |
| `PDF` | Portable Document Format | Формат локального архіву статей і syllabus матеріалів. |
| `POST` | HTTP POST method | API-метод запуску або підготовки матеріалізації/команд у read-model workflow. |
| `PTO` | Predict-then-Optimize | Підхід, де прогноз спочатку будується незалежно, а потім подається в optimizer. |
| `PV` | Photovoltaics | Сонячна генерація; може бути контекстом для battery/load сценаріїв. |
| `RL` | Reinforcement Learning | Навчання з підкріпленням; DT належить до offline RL sequence-modeling напряму. |
| `RMSE` | Root Mean Squared Error | Forecast-only метрика; використовується діагностично, але не визначає strategy promotion. |
| `RTE` | Round-Trip Efficiency | Ефективність циклу заряд-розряд батареї. |
| `SDAC` | Single Day-Ahead Coupling | Європейський day-ahead market-coupling контекст; не є автоматичним доказом українського training-feature admission. |
| `SEI` | Solid Electrolyte Interphase | Фізико-хімічний шар у lithium-ion batteries; релевантний для повного ageing/digital-twin roadmap. |
| `SIDC` | Single Intraday Coupling | Європейський intraday market-coupling контекст; поза поточним DAM-only evidence scope. |
| `SOC` | State of Charge | Стан заряду батареї; ключове обмеження LP schedule та safety validation. |
| `SOH` | State of Health | Стан здоров'я батареї; належить до digital-twin/degradation roadmap. |
| `SOTA` | State of the Art | Найсучасніший рівень методів; у роботі не є самостійним доказом якості без strict LP/oracle regret evidence. |
| `SPO` / `SPO+` | Smart Predict-then-Optimize | Decision-aware loss framework для predict-then-optimize задач; методологічна база для regret-aware evaluation. |
| `TFT` | Temporal Fusion Transformer | Multi-horizon forecasting model з quantile outputs і interpretability mechanisms; у поточних результатах не замінює V2+. |
| `THieF` | Temporal Hierarchical Forecasting | Research/source context для temporal hierarchy і майбутніх schedule families; не входить у поточне тренування. |
| `UA` | Ukraine / Ukrainian | Позначення українського market/data scope. |
| `VAT` | Value-Added Tax | Податковий контекст, який може бути релевантним для повного settlement/net-profit шару. |
| `VSN` | Variable Selection Network | Механізм TFT для вибору ознак; корисний для explainability, але не є proof of strategy superiority. |
| `XAI` | Explainable Artificial Intelligence | Пояснюваний AI; у роботі потрібний для інтерпретації forecasts/selectors та defense-facing evidence. |

## Ключові терміни

| Термін | Визначення |
|---|---|
| `strict_similar_day` | Заморожений leakage-free baseline rule, який копіює історично схожий день і є контрольним comparator/fallback для ML/DFL кандидатів. |
| Anchor | Точка rolling-origin оцінювання, у якій модель або selector використовує лише дані, доступні до цього моменту. |
| Baseline strategy | Детермінована контрольна стратегія, що використовується як порівняльний рубіж для learned candidates. |
| `BUY` / `SELL` / `HOLD` | Спрощені action labels для заряджання, розряджання або утримання. У thesis evidence вони є research/action-label representation, а не безпосередньою командою інвертору. |
| `CHARGE` / `DISCHARGE` | Фізичні напрями зміни енергії батареї; в schedule rows можуть відповідати signed power, але не дорівнюють market trade без gatekeeper і clearing semantics. |
| Candidate family | Група feasible schedules, створених однаковим правилом або профілем. |
| Candidate library | Набір feasible schedule candidates, які проходять однаковий strict LP/oracle scoring. |
| Cleared Trade | Підтверджена ринкова алокація після клірингу; у поточній роботі не генерується як live market result. |
| Data leakage | Некоректне використання майбутніх або final-holdout даних у train/selection features. |
| Digital twin | Деталізована модель фізичного стану батареї; у поточному MVP використовується спрощений feasibility-and-economics preview, а повний digital twin лишається roadmap. |
| Dispatch Command | Фізична команда обладнанню; не створюється поточними offline evidence packets. |
| Evidence packet | Локальний відтворюваний пакет результатів: JSON/Markdown/CSV, manifests, checks і claim-boundary metadata. |
| Final holdout | Останні validation anchors, які використовуються лише для scoring, а не для training або selector choice. |
| Feasible schedule | Charge/discharge/hold schedule, який задовольняє LP constraints: SOC, power limits, efficiency та related economics. |
| Forecast candidate | Модель або правило, що генерує forecast input для LP; саме по собі не є strategy promotion. |
| Governance-only source | Джерело або feature lane, яке описане та перевіряється, але ще не має права входити в training. |
| Horizon-aware regret-weighted calibration | Prior-only корекція forecast bias окремо за horizon step, зважена за downstream regret минулих anchors. |
| Market coupling | Ринкова інтеграція між зонами; у роботі є майбутнім exogenous-feature напрямом, а не поточним training input. |
| Market execution | Подання заявки, кліринг або фізичне виконання в реальному ринку; поточні results цього не роблять. |
| `market_execution_enabled=false` | Claim-boundary flag, який прямо фіксує, що evidence row не дозволяє live trading або dispatch. |
| Offline Strategy Promotion | Thesis-facing стан, коли candidate пройшов offline/read-model gate, але strict fallback, no-live-execution boundary і human/operator review залишаються чинними. |
| Operator preview | Пояснюваний read-model surface для перегляду рекомендацій та evidence; не є market execution console. |
| Oracle benchmark | Offline perfect-foresight evaluator, який використовує realized prices лише після факту для верхньої межі value і regret. |
| Prior-only feature | Ознака, доступна до anchor і дозволена для train/selection; не містить final-holdout actuals. |
| Proposed Bid | Повна ринкова заявка; у поточному thesis evidence не створюється як live market artifact. |
| Read model | API/dashboard представлення вже матеріалізованих evidence rows; не змінює strategy logic. |
| Regret | Втрачена цінність відносно oracle LP: різниця між oracle value і value вибраного schedule. |
| Rolling-origin backtest | Temporal validation protocol, у якому кожний anchor оцінюється з використанням тільки попередньо доступної інформації. |
| Rolling robustness | Перевірка того, що candidate не перемагає лише в одному latest holdout, а проходить кілька rolling validation windows. |
| Schedule | Погодинна послідовність charge/discharge/hold або signed power decisions для BESS. |
| Schedule/Value Learner V2 | Prior-only selector між feasible LP-scored schedule candidates; перший стійкий schedule/value challenger. |
| Schedule/Value Learner V2+ | Розширений schedule/value selector із багатшою candidate library; поточний основний підтверджений Offline Strategy Promotion result. |
| Strict LP/oracle evaluator | Єдиний final scoring contour, який оцінює schedules проти realized prices і oracle value без послаблення promotion rule. |
| Thesis-grade evidence | Evidence slice з observed provenance, достатнім coverage, no-leakage protocol і чіткою claim boundary. |
| `V2`, `V2+`, `V3`, `V4`, `V5` | Послідовні версії schedule/value або candidate-value DFL experiments. Номер версії не є академічним методом сам по собі; він позначає локальну ітерацію evidence path. |
| V2+ fallback | Conservative selection rule: якщо новий candidate не має достатнього prior evidence, система повертається до frozen Ukrainian-only V2+. |

## Технічні назви, що не перекладаються

У тексті збережено деякі англомовні назви, оскільки вони є назвами моделей,
asset keys, API endpoints або evidence flags. Наприклад:
`nbeatsx_official_global_panel_horizon_calibrated_v1`,
`dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame`,
`/dashboard/dfl-schedule-value-production-gate`,
`market_execution_enabled=false`. Такі назви подаються в моноширинному форматі
та не перекладаються, щоб не порушити відтворюваність між текстом диплома,
кодом, Dagster assets і локальними evidence packets.
