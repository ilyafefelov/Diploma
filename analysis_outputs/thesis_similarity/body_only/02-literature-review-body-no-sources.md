Розділ 2. Огляд предметної області та джерел

2.1. Український DAM і роль BESS

Ринок на добу наперед задає погодинні price signals, які можуть бути використані для планування заряду й розряду BESS. Для України така задача має практичну вагу через потребу в гнучкості, інтеграції відновлюваної генерації, локальному балансуванні та зменшенні вартості пікових годин. Ринкова специфіка описується не тільки загальноєвропейською логікою electricity market design, а й локальними правилами, price caps, тарифами та енергетичною політикою України (European Commission, 2026 [[34]](#source-34), [[35]](#source-35); NEURC, 2026 [[36]](#source-36); JSC Market Operator, 2026 [[37]](#source-37); Ministry of Economy of Ukraine, 2024 [[40]](#source-40), [[41]](#source-41)).

Водночас академічна модель має обережну межу: наявність historical price rows не перетворює систему на контур подання заявок і не означає, що timestamp publication evidence вже готовий для market-executable сценаріїв. Ця межа узгоджується з європейським контекстом синхронізації та майбутнього market-coupling, але не підміняє український DAM evidence layer (ENTSO-E, 2022 [[42]](#source-42); ACER, 2025 [[43]](#source-43)).

Європейські роботи про BESS utilization, revenue stacking і energy/reserve bidding корисні як ширший market-design контекст, однак у роботі вони не використовуються як доказ готовності українського live execution (Hu et al., 2022 [[12]](#source-12); Li et al., 2024 [[13]](#source-13)).

Контекст українського застосування наведено на рисунку 2.1.

![Рисунок 2.1. Контекст BESS/DAM для українського застосування](assets/compact-fig-2-1-ukraine-context.png)

Рисунок 2.1. Контекст BESS/DAM для українського застосування

Рисунок 2.1 показує, що практична цінність виникає на перетині DAM signal, BESS asset constraints і operator needs. Дипломний MVP охоплює рекомендаційний preview і відтворювану evidence chain; production trading потребує окремої юридичної, технічної і source-readiness інфраструктури.

2.2. Чому forecast-only оцінка недостатня

Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливість строгих EPF-бенчмарків і сильних baseline, а Elmachtoub and Grigas (2022) демонструють, що статистична якість прогнозу не завжди збігається з якістю downstream optimization decision [[14]](#source-14), [[4]](#source-4).

У BESS arbitrage важливі не всі помилки однаково: помилка біля локального піку або провалу може коштувати значно більше, ніж така сама абсолютна похибка у стабільній середині доби. Крім того, schedule value залежить від SOC path, efficiency losses і обмеження потужності. Саме тому storage-specific DFL роботи Sang et al. (2022/2023) та Mandi et al. (2024) пропонують оцінювати моделі через regret/value або decision loss, а не лише через forecast-only loss [[29]](#source-29), [[28]](#source-28).

Тому в роботі forecast layer розглядається як джерело candidate schedules, а не як остаточна відповідь. NBEATSx і TFT мають сенс лише тоді, коли їхні сигнали після LP scoring зменшують regret. Це також пояснює, чому raw neural forecast може бути слабшим за conservative baseline, але calibrated або schedule/value selected variants можуть стати корисними.

2.3. Ландшафт методів

Ландшафт методів, використаних у роботі, наведено на рисунку 2.2.

![Рисунок 2.2. Ландшафт методів: forecast, LP, regret і governance](assets/compact-fig-2-2-method-landscape.png)

Рисунок 2.2. Ландшафт методів: forecast, LP, regret і governance

Рисунок 2.2 групує методи у чотири шари. Forecasting дає price signal; optimization перетворює його на feasible schedule; decision-quality оцінює результат через regret/value; governance визначає, які source rows і claims можна використовувати. Така структура ближча до прикладної evidence system, ніж до традиційної таблиці forecast metrics.

2.4. Стислий literature/source matrix

Огляд джерел у роботі не подається як довгий paper-by-paper summary. Для практичної дипломної роботи важливіше показати, яка частина літератури і технічних джерел підтримує конкретний design choice. Матрицю напрямів наведено в таблиці 2.1.

Таблиця 2.1. Матриця літературних і технічних напрямів

| Напрям | В роботі | Поза scope |
| --- | --- | --- |
| EPF / time-series forecasting | NBEATSx/TFT; downstream regret [[2]](#source-2), [[6]](#source-6), [[14]](#source-14) | SOTA за MAE/RMSE без LP/regret |
| Storage optimization | LP schedule із SOC/power/efficiency [[51]](#source-51), [[10]](#source-10), [[5]](#source-5) | Settlement/live dispatch |
| DFL / predict-then-optimize | Regret/value як decision-quality criterion [[4]](#source-4), [[29]](#source-29), [[28]](#source-28) | Full differentiable controller |
| Offline RL / DT | Research-shadow sequence policy [[7]](#source-7), [[32]](#source-32) | DT/LAVA promotion/live policy |
| Ukrainian market governance | DAM preview і source-readiness blockers [[36]](#source-36), [[37]](#source-37), [[43]](#source-43) | Inferred receipts/submission claims |

Таблиця 2.1 показує, що робота поєднує electricity price forecasting, storage optimization, DFL/predict-then-optimize і market governance. Водночас вона свідомо не заявляє full settlement, deployed DT або full differentiable controller. Це скорочує текст і робить межі дослідження прозорими.

2.5. Storage optimization і LP як контрольний контур

Для BESS schedule LP є природним базовим інструментом: він може задати баланс енергії, обмеження SOC, потужність заряду/розряду, efficiency і простий економічний objective. Park et al. (2017) прямо формулюють short-term ESS scheduling через лінійні обмеження SOC, power range, energy limits та efficiency [[51]](#source-51). Hesse et al. (2019), Maheshwari et al. (2020), Kumtepeli et al. (2020) і Cao et al. (2020) показують, що storage dispatch може бути розширений degradation-aware або learning-based компонентами, але це підвищує вимоги до параметризації батареї та якості даних [[10]](#source-10), [[11]](#source-11), [[45]](#source-45), [[46]](#source-46).

У роботі LP використовується не як тема окремого математичного дослідження, а як deterministic evaluator, який дає однаковий scoring contour для всіх forecast і selector candidates. Це важливо, бо порівняння моделей без спільного optimization contour може бути несправедливим. Vykhodtsev et al. (2022) додатково пояснюють, що спрощене power-energy представлення батареї є придатним для techno-economic studies, але не замінює повний electrochemical digital twin [[9]](#source-9).

Strict LP/oracle evaluator виконує дві ролі. По-перше, він оцінює value вибраного schedule. По-друге, він обчислює oracle reference, тобто найкращу offline value при realized prices. Різниця між oracle value і selected value стає regret. Такий підхід дозволяє говорити не "модель має меншу похибку", а "schedule втрачає менше економічної цінності відносно oracle". Економічні припущення про storage cost/performance підтримуються NREL Storage Futures та NREL ATB матеріалами [[52]](#source-52), [[53]](#source-53).

2.6. NBEATSx, TFT і probabilistic forecasting

NBEATSx у роботі має статус forecast-layer choice/adaptor. Olivares et al. (2023) описують NBEATSx як neural basis expansion model з exogenous variables для electricity price forecasting [[2]](#source-2). Це релевантно для DAM/BESS, бо цінові ряди мають денні, тижневі, сезонні та регуляторні компоненти, а також реагують на погодні й системні фактори. Проте сам факт використання NBEATSx не є доказом кращого arbitrage result; його output перевіряється через LP/regret.

Temporal Fusion Transformer, запропонований Lim et al. (2021), є другим важливим forecast-layer напрямом [[6]](#source-6). TFT підтримує multi-horizon forecasting, static covariates, known future inputs, observed historical inputs, variable selection та attention-based interpretability. Probabilistic TFT та інші сучасні deep-learning EPF роботи розширюють цей напрям у бік uncertainty-aware forecasting, quantile loss і exogenous-aware transformer architectures (Jiang et al., 2024 [[3]](#source-3); Wang et al., 2024 [[15]](#source-15); Yu et al., 2026 [[16]](#source-16); Jin and Blanco-Encomienda, 2026 [[60]](#source-60)).

Окремий висновок із сучасної EPF-літератури стосується foundation models і temporal hierarchy. PriceFM, THieF, TSFM leakage studies, TFMAdapter і Reverso показують активний розвиток time-series foundation-model напряму, але ці джерела у роботі мають статус future/research context, а не підставу для зміни поточного headline result [[17]](#source-17), [[18]](#source-18), [[19]](#source-19), [[20]](#source-20), [[21]](#source-21). Current evidence залишається прив'язаною до українського DAM benchmark і V2/V2+ schedule/value selector.

2.7. Decision-focused learning і практична межа

Decision-Focused Learning у повному сенсі може означати навчання моделі через differentiable optimization layer або task-aware surrogate loss. Smart Predict-then-Optimize у Elmachtoub and Grigas (2022) формалізує проблему розриву між prediction loss і downstream objective [[4]](#source-4). Mandi et al. (2024) систематизують DFL як напрям, у якому learning model і constrained optimization problem поєднуються через decision loss, differentiable optimization, surrogate gradients або gradient-free methods [[28]](#source-28).

Для storage arbitrage ця логіка особливо важлива, бо рішення є multistage: charge/discharge у поточну годину змінює SOC і майбутні feasible actions. Sang et al. (2022/2023) безпосередньо пов'язують electricity price prediction з ESS arbitrage regret, Persak and Anjos (2024) підкреслюють multistage structure, а Yi, Alghumayjan, and Xu (2024) показують perturbed DFL для strategic energy storage [[29]](#source-29), [[30]](#source-30), [[31]](#source-31).

Distributional RL для energy arbitrage розглядається як risk-sensitive future context, а не як поточний production-facing метод рекомендаційного preview (Madahi et al., 2024 [[22]](#source-22)).

У роботі цей напрям використовується обережно. Головний результат є schedule/value selector з regret-aware evaluation, а не повний differentiable predict-then-bid stack. Диференційовані оптимізаційні шари Agrawal et al. (2019) та OptNet Amos and Kolter (2017) підтримують roadmap, але не доводять наявність deployed DFL controller [[8]](#source-8), [[54]](#source-54). Predict-then-bid framework Yi et al. (2025) також використовується як цільова дослідницька траєкторія, а не як опис поточного market-execution стану [[1]](#source-1).

2.8. Decision Transformer як research-shadow

Decision Transformer є привабливим напрямом для sequence-policy tasks, бо він може умовлювати дії на return-to-go та історію станів. Chen et al. (2021) формулюють Decision Transformer як return-conditioned sequence modeling для offline reinforcement learning [[7]](#source-7). Bhargava et al. (2023) додатково показують, що переваги DT залежать від якості, обсягу й структури offline trajectories [[32]](#source-32). Практичну implementation reference для майбутніх експериментів дає офіційна документація Hugging Face Transformers [[33]](#source-33).

У поточній роботі DT має тільки research-shadow статус. V13 source-readiness gate не дозволяє трактувати DT або LAVA як production-ready, доки немає explicit DAM publication receipts і достатньої кількості permitted training rows. Тому DT використовується для перевірки майбутньої архітектурної можливості, а не для headline result.

2.9. Роль українських джерел і source governance

Окремий аспект огляду - джерельна дисципліна. У задачах енергетичного арбітражу дані не є нейтральним фоном: timestamp, publication timing, timezone normalization і provenance визначають, чи має модель право бачити конкретну ознаку. Якщо зовнішній ряд доступний лише після decision time, його використання як train або selection feature створює leakage. Якщо publication receipt не підтверджено, не можна описувати систему як market-ready, навіть якщо локальний backtest виглядає переконливо.

У роботі тому використано обережний словник. DAM rows можуть бути частиною recommendation preview, але explicit DAM publication receipts потрібні для сильніших source-readiness claims. EU/Poland/ENTSO-E контексти можуть бути корисними як governance або shadow lanes, але вони не перетворюються автоматично на українські training targets. Такий підхід спирається на офіційні джерела про дані й ринкову інтеграцію: ENTSO-E Transparency Platform, OPSD, Nord Pool, Ember, ACER, European Commission і Ukrainian market/operator sources [[23]](#source-23), [[24]](#source-24), [[25]](#source-25), [[26]](#source-26), [[27]](#source-27), [[34]](#source-34), [[35]](#source-35), [[36]](#source-36), [[37]](#source-37), [[43]](#source-43).

Для forecast layer зовнішні ряди можуть бути корисними як exogenous covariates, але лише після licensing, timezone/DST, currency, market-rule, publication-time availability і domain-shift checks. Роботи Li and Becker (2021), Redhu and Bremdal (2023) та Mascarenhas et al. (2025) підтримують ідею neighboring-zone або cross-border features для EPF, а документація Nixtla пояснює технічну підтримку exogenous variables у NeuralForecast/NBEATSx [[47]](#source-47), [[48]](#source-48), [[49]](#source-49), [[50]](#source-50).

Погодні ознаки також потребують явного source contract: для цього релевантні документації Open-Meteo Forecast API та Historical Weather API, але ці джерела не скасовують вимоги перевіряти availability timing і leakage risk [[38]](#source-38), [[39]](#source-39).

2.10. Інженерна evidence system і MLOps-контекст

У прикладній роботі академічна якість залежить не лише від вибору моделі, а й від відтворюваності evidence pipeline. Dagster software-defined assets підтримують materialization, lineage і asset checks [[55]](#source-55). MLflow tracking використовується як джерело експериментальних run records і метрик [[56]](#source-56). FastAPI дає read-model surface для операторського preview, а не execution endpoint [[57]](#source-57). Pydantic strict mode підтримує deterministic validation і boundary discipline [[58]](#source-58). Medallion architecture використовується як термінологічна основа для Bronze/Silver/Gold data layers [[59]](#source-59).

Governance-контекст також включає AI Act regulatory framework як загальну рамку для data quality, human oversight, logging і risk management [[44]](#source-44). У роботі ці джерела не доводять market readiness, але підтримують engineering design: результати мають бути перевірюваними, відтворюваними й явно обмеженими.

2.11. Decision-aware evaluation як відповідь на слабкість forecast leaderboard

Forecast leaderboard зазвичай ранжує моделі за середньою помилкою. У storage arbitrage такий leaderboard може бути оманливим. Модель, що трохи краще передбачає середні години, може не давати кращого рішення, якщо вона помиляється в годинах з найбільшим arbitrage spread. Навпаки, модель із неідеальним MAE може мати корисний порядок піків і провалів, якщо LP schedule використовує саме цей порядок.

Тому decision-aware evaluation у роботі не є додатковою прикрасою. Вона є відповіддю на центральну слабкість forecast-only підходу, описану в EPF benchmarking, PTO і storage-specific DFL літературі (Lago et al., 2021 [[14]](#source-14); Elmachtoub and Grigas, 2022 [[4]](#source-4); Sang et al., 2022/2023 [[29]](#source-29)). Regret/value оцінка відповідає на питання, яке реально важить для BESS owner: скільки економічної цінності втрачено відносно oracle і чи стало рішення кращим за conservative fallback.

2.12. Висновок до розділу 2

Огляд показує, що науково коректна постановка задачі для BESS arbitrage не зводиться до forecast leaderboard. Найбільш релевантний шлях для дипломного MVP - це evidence-based contour, де прогноз, LP optimization, regret/value metrics і source governance перевіряються разом. EPF-моделі на кшталт NBEATSx і TFT є обґрунтованими forecast candidates, але їхня цінність має підтверджуватися через downstream LP scheduling, net value і regret (Olivares et al., 2023 [[2]](#source-2); Lim et al., 2021 [[6]](#source-6); Lago et al., 2021 [[14]](#source-14)).

Для роботи з цього випливають три методологічні вимоги. Перша: forecast candidates оцінюються через downstream LP/regret. Друга: source governance є явною, особливо для V13 і зовнішніх джерел. Третя: negative evidence залишається в тексті, бо саме вона показує, що система не просуває weak challengers. Ці вимоги формують методологію розділу 3 (Mandi et al., 2024 [[28]](#source-28); Yi et al., 2025 [[1]](#source-1); Chen et al., 2021 [[7]](#source-7)).

