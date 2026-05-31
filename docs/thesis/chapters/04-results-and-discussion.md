Розділ 4. Результати та обговорення

4.1. Scope результатів

Результати належать до DAM/IDM hourly recommendation preview та offline/read-model evidence. DAM/V2+ лишається primary evaluated packet, а IDM належить до product/read-model preview capability без market execution. Усі числові твердження нижче походять з наявних evidence packets і не є новими експериментами. Межу scope наведено в таблиці 4.1.

Таблиця 4.1. Scope результатів і межі інтерпретації

| Scope item | Поточний статус | Інтерпретація |
| --- | --- | --- |
| Market | DAM/V2+ evaluated packet; DAM/IDM hourly preview capability | Not live IDM bid, settlement або market coupling execution |
| Evidence | Offline/read-model packets | Результати відтворюються з repo artifacts |
| Safety | market_execution_enabled=false | No ProposedBid, ClearedTrade або DispatchCommand |
| V13 | Receipts blocked; safe-switch staged support 20/20 | DT/LAVA training remains blocked |

Таблиця 4.1 задає рамку для всього розділу. Headline result може бути практично корисним для operator preview, але він не відкриває live market execution. Це особливо важливо для V13, де safe-switch staged support уже може бути достатнім для частини локальної evidence, але explicit OREE DAM/IDM source/publication evidence for preview залишається blocker для DT/LAVA training, а market-submission receipts залишаються окремим execution blocker.

4.2. Headline V2/V2+ результат

Головний результат отримано не raw forecast, а Schedule/Value Learner V2+, який вибирає feasible schedule candidate за downstream value. Порівняння наведено в таблиці 4.2.

Таблиця 4.2. Headline comparison для strict, V2, V2+ та raw reference

| Comparator | Mean regret, UAH | Median regret, UAH | Висновок |
| --- | --- | --- | --- |
| strict_reference | 310.58 | 198.39 | Контрольний leakage-free baseline |
| Schedule/Value Learner V2 | 206.37 | 96.02 | Перший стійкий improvement |
| Schedule/Value Learner V2+ | 174.77 | 67.30 | Headline offline strategy result |
| raw_reference | 622.25 | 290.22 | Raw forecast alone is insufficient |

Таблиця 4.2 показує, що V2+ має mean regret 174.77 UAH і median regret 67.30 UAH. Це краще за strict_reference 310.58 / 198.39 UAH і краще за V2 206.37 / 96.02 UAH. Raw reference у цьому slice має значно гірший mean regret, що підтверджує головну тезу: прогноз сам по собі не є достатнім; потрібний schedule/value decision layer.

Ту саму різницю візуально наведено на рисунку 4.1.

![Рисунок 4.1. Драбина mean regret для strict, V2 та V2+](assets/compact-fig-4-1-regret-ladder.png)

Рисунок 4.1. Драбина mean regret для strict, V2 та V2+

Рисунок 4.1 робить headline result зрозумілим без довгого переліку проміжних експериментів. Lower regret означає меншу втрату economic value відносно oracle. V2+ знижує mean regret на 43.73% проти strict baseline і на 15.31% проти frozen V2.

Ключову result card наведено на рисунку 4.2.

![Рисунок 4.2. Підсумковий V2+ result card](assets/compact-fig-4-2-v2plus-card.png)

Рисунок 4.2. Підсумковий V2+ result card

Рисунок 4.2 підкреслює, що result має три частини: lower regret, lower median regret і rolling robustness 4 / 4. Саме поєднання цих критеріїв дозволяє назвати результат offline/read-model Strategy Promotion. Водночас safety boundary залишається незмінною.

4.3. Чому V2+ сильніший за raw forecast

Raw NBEATSx і TFT можуть мати корисні price signals, але BESS schedule залежить від розташування екстремумів, spread shape, SOC path, terminal SOC pressure, throughput і фізичних constraints. Тому прогнозний ряд не є кінцевим рішенням. Він стає корисним лише тоді, коли перетворюється на feasible charge/discharge schedule і оцінюється через regret/value. V2+ працює саме на цьому рівні: він порівнює candidate schedules, а не тільки прогнозні ряди.

Архітектура V2+ є decision stack з чотирьох частин. Перший шар - price-context/source layer: для published preview price source є official OREE row, а horizon-calibrated NBEATSx/TFT використовуються як scenario або historical forecast evidence для unpublished/replay cases. Жоден із цих signals не просувається напряму як ринкова дія. Другий шар - candidate library: поруч із frozen V2 додаються deterministic schedule families, які закривають failure modes попереднього рішення. До них належать perturbation around price extrema, robust spread penalty, small timing shifts around strict_similar_day, temporal block reconciliation і terminal SOC target. Третій шар - prior-only selector: він використовує train/prior anchors, prior family regret, forecast spread, forecast objective value, throughput, degradation proxy, SOC slack і candidate identity, але не бачить final-holdout regret до моменту scoring. Четвертий шар - unchanged strict LP/oracle evaluator: кожний selected schedule оцінюється тим самим contour, що й strict, raw, V2, DFL shadow і DT shadow. Саме ця комбінація дає V2+ перевагу: модель не намагається "вгадати вже опубліковану ціну краще за офіційне джерело", а вибирає той feasible schedule, який має кращу decision-value evidence.

Практично це означає, що V2+ не конкурує з forecast model як ще один forecast. Він стоїть після forecast і перед operator preview. Якщо forecast помиляється у рівні ціни, але зберігає корисний shape, V2+ може вибрати schedule family, яка зменшує regret. Якщо forecast дає привабливий, але ризиковий schedule, selector або fallback залишають V2+/strict contour без просування слабшого challenger. Тому головний результат виникає не з isolated forecast accuracy, а з поєднання candidate diversity, prior-only вибору, V2 fallback і однакового LP/oracle scoring на 5 tenants, 90 tenant-anchors і 4 rolling windows.

Схему архітектурної різниці між підходами наведено на рисунку 4.3.

![Рисунок 4.3. Архітектурне порівняння raw, strict, V2+, DFL і DT](assets/compact-fig-4-3-architecture-comparison.png)

Рисунок 4.3. Архітектурне порівняння raw, strict, V2+, DFL і DT

Рисунок 4.3 показує, що raw forecast зупиняється на price vector, strict_similar_day дає безпечний, але негнучкий fallback, а V2+ додає schedule-family search і conservative prior-only switching. DFL і Decision Transformer мають потенційно сильнішу форму навчання, але в поточному evidence set вони не мають достатнього prior-safe signal, щоб замінити V2+ без деградації.

Raw reference програє найчіткіше. Його mean regret становить 622.25 UAH, а median regret - 290.22 UAH, бо price forecast не дорівнює battery decision. Навіть якщо forecast вловлює загальний рівень ціни, LP може отримати неправильний порядок charge/discharge годин, terminal SOC pressure або надмірний throughput. V2+ зменшує цю проблему через schedule-level selection: він оцінює не абстрактну точність ряду, а downstream value після фізичних та економічних constraints.

Strict baseline програє V2+ з іншої причини. Strict_similar_day є leakage-free, простим і стабільним, тому він залишається корисним fallback і comparator. Але його архітектура фіксована: вона не бачить нові feasible schedule families і не використовує prior evidence для вибору між ними. Через це strict_reference має mean regret 310.58 UAH проти 174.77 UAH у V2+. Аналітично це не "провал" strict, а межа deterministic fallback: він добре захищає від overfitting, але не використовує підтверджені можливості schedule/value selection.

DFL-напрям у роботі не відкинуто, але його межу уточнено. Ранні candidate-value DFL v3, residual/DT bridge і regret-aware selector були корисними як negative evidence: вони перевіряли, чи можна навчити prior-only replacement rule поверх V2+. DFL v2 matched V2+ на рівні mean regret 174.77 UAH, але не створив replacement improvement, а перший conservative regret-aware run зробив 0 / 90 non-V2+ switches. Після цього було навчено corrected DT/V2+ safe-switch shadow: замість candidate-index accuracy він оптимізує \(\Delta r_{\mathrm{V2+}}\) (`regret_delta_vs_v2_plus_uah`) і abstains до V2+, якщо predicted improvement слабкий або tail-risk guard не проходить. На frozen final-holdout packet цей shadow знизив mean regret до 168.16 UAH проти 174.77 UAH у V2+, median regret до 61.71 UAH проти 67.30 UAH, зробив 4 / 90 non-V2+ switches, відновив 3 / 15 observed safe-switch opportunities і не мав tail-risk losses. Це positive research evidence, але ще не default replacement: `promotion_gate_passed=false`, `market_execution_enabled=false`, а V2+ залишається fallback.

Decision Transformer у поточному evidence set треба описувати як research lane з двома різними етапами. Негативним був ранній proxy-експеримент: candidate-index / schedule-family classifier з cross-entropy objective оптимізував відтворення teacher labels, а не LP/oracle regret напряму. В apples-to-apples comparison цей raw DT selected mean regret становив 460.30 UAH, бо модель обрала 65 rows schedule_value_learner_v2_reference і 25 rows raw_reference, але 0 rows V2+. Corrected shadow змінив постановку задачі: він навчається на regret/value delta відносно V2+, використовує V2+ як fallback і дозволяє switch лише у рідкісних безпечних випадках. Саме тому новий результат практично кращий за V2+ на цьому packet, але академічна межа лишається консервативною: це manual research diagnostic, не raw BUY/SELL/HOLD controller, не dashboard/API default і не DT/LAVA promotion. Додатково V13 source-readiness не дозволяє описувати DT/LAVA як готовий training або execution contour.

Кількісне порівняння підходів наведено на рисунку 4.4.

![Рисунок 4.4. Порівняння mean regret для raw, DT, strict, V2 і V2+](assets/compact-fig-4-4-method-regret-comparison.png)

Рисунок 4.4. Порівняння mean regret для raw, DT, strict, V2 і V2+

Рисунок 4.4 підкреслює головну логіку результату: нижчий regret має не найскладніша модель за назвою, а той підхід, який краще поєднує feasible schedules, prior-only selection і strict LP/oracle evaluation. V2+ перемагає в поточному evidence set саме як conservative decision layer, а не як claim про універсальну перевагу над усіма майбутніми DFL або DT варіантами.

У практичній інтерпретації оператор отримує не "сирий прогноз", а пояснення: який schedule обрано, який regret очікується за backtest evidence, чому fallback не гірший і які gates пройдено. Така структура робить dashboard корисним для українського BESS owner, бо він бачить decision evidence, а не лише графік ціни.

4.4. Robustness і negative evidence

Після V2+ були перевірені додаткові candidate-value та plateau-breaking кроки. Їхній підсумок наведено в таблиці 4.3.

Таблиця 4.3. Robustness і plateau diagnostics після V2+

| Challenger | Observed result | Gate status |
| --- | --- | --- |
| V3 candidate-value | Додав value labels, але не дав robust improvement | Not promoted |
| V4 plateau breaker | Діагностував failure-mode candidates | Not promoted |
| V5 point-in-time repair | Покращив protocol clarity, але не замінив V2+ | Not promoted |
| DT/V2+ safe-switch selector | 168.16 UAH mean regret; 4 / 90 non-V2+ switches; 3 / 15 safe-switch opportunities recovered | Positive shadow evidence; V2+ remains confirmed offline comparator/evidence |

Таблиця 4.3 показує, що подальші candidate-value кроки спочатку не дали safe replacement of V2+, але corrected safe-switch shadow знайшов рідкісні безпечні обходи V2+. Це не змінює default strategy автоматично: система демонструє здатність показати positive shadow evidence і водночас зупинити promotion, доки окремий gate не дозволить default switch. Для дипломної роботи важливо показати не лише success path, а й disciplined non-promotion.

Пояснення плато наведено на рисунку 4.5.

![Рисунок 4.5. Діагностика плато V3/V4/V5](assets/compact-fig-4-3-plateau.png)

Рисунок 4.5. Діагностика плато V3/V4/V5

Рисунок 4.5 фіксує логіку gate-based evidence. Якщо новий selector не має стійкого prior-only сигналу, abstention і fallback до V2+ є правильним рішенням. Якщо corrected selector знаходить невелику кількість safe switches, це можна показувати як research diagnostic, але не як production/default switch. Це зменшує ризик перебільшення claims і формує реалістичний roadmap майбутніх досліджень.

4.5. Shadow evidence і непідтверджені challengers

Після headline V2+ були перевірені додаткові гілки, які могли б пояснити або потенційно покращити результат. Їхній підсумок наведено в таблиці 4.4.

Таблиця 4.4. Shadow evidence, near-miss results і blockers

| Shadow lane | Підтверджений результат | Рішення gate |
| --- | --- | --- |
| TFT quantile | Best TFT V2+ row: 225.47 UAH mean regret і 121.00 UAH median regret | Гірше за frozen V2+ 174.77 / 67.30 UAH; blocked |
| Poland lag24 richer | 177.34 UAH mean regret, 39.46 UAH median regret | Median кращий, але mean гірший за V2+ на 2.58 UAH; not promoted |
| Poland prior-only veto | 167.05 UAH mean regret, 55.97 UAH median regret; 34 challenger rows і 56 fallback rows | Near-miss: improvement 4.41% нижче порогу 5%; not promoted |
| DT/V2+ safe-switch selector | 168.16 UAH mean regret проти V2+ 174.77 UAH; 4 / 90 non-V2+ switches; 3 / 15 safe-switch opportunities recovered; 0 tail-risk losses | Positive shadow evidence; V2+ remains confirmed offline comparator/evidence |
| DT apples-to-apples | 460.30 UAH mean regret; +285.53 UAH проти V2+ | Research-shadow only; not promoted |
| V13 acquisition | Safe-switch support validated, але explicit OREE DAM/IDM source/publication evidence for preview missing | dt_lava_ready=false; permits_model_training=false |

Таблиця 4.4 важлива не менше за headline. Вона показує, що система не просуває model family тільки через сучасну назву або локально привабливий сигнал. Poland prior-only veto був near-miss, а DT/V2+ safe-switch selector став першим corrected shadow, який показав невелике regret improvement без tail-risk losses. Водночас він лишається manual diagnostic, бо explicit promotion gate не додано і `promotion_gate_passed=false`. Отже, V2+ не замінюється, але roadmap уже має конкретний напрям: навчати модель шукати рідкісні safe switches, а не копіювати V2+ або raw action labels.

Межу TFT наведено на рисунку 4.6.

![Рисунок 4.6. TFT як complementary schedule expert](assets/compact-fig-4-4-tft.png)

Рисунок 4.6. TFT як complementary schedule expert

Рисунок 4.6 показує, що TFT не потрібно описувати як провал. Він дає uncertainty/diversity signal і може бути корисним у наступних ітераціях. Однак у поточному evidence set він не перемагає frozen Ukrainian-only V2+, тому його статус - complementary shadow lane.

Межу Decision Transformer наведено на рисунку 4.7.

![Рисунок 4.7. Decision Transformer shadow boundary](assets/compact-fig-4-5-dt.png)

Рисунок 4.7. Decision Transformer shadow boundary

Рисунок 4.7 відділяє working sequence-policy pipeline від deployed controller. Ранній apples-to-apples DT classifier дав regret 460.30 UAH і тому залишився negative evidence. Corrected DT/V2+ safe-switch shadow уже показав кращий final-holdout regret, ніж V2+ (168.16 проти 174.77 UAH), але тільки як offline/manual research diagnostic. Додатково V13 не дозволяє DT/LAVA training claims, доки explicit OREE DAM/IDM source/publication evidence for preview залишається blocker.

4.6. Шлях експериментів

Експериментальний шлях у роботі можна стисло подати як послідовність контрольованих gates. Спочатку strict_similar_day задає leakage-free baseline. Далі raw NBEATSx/TFT forecast adapters створюють price signals, але не вважаються рішенням самі по собі. Потім LP contour перетворює forecast або selector output на feasible schedule, а regret/value scoring порівнює цей schedule з oracle. Після цього V2 і V2+ перевіряються на 5 tenants, 90 tenant-anchors і 4 rolling windows. Цю послідовність наведено на рисунку 4.8.

![Рисунок 4.8. Експериментальний шлях від baseline до operator preview](assets/compact-fig-4-6-experiment-path.png)

Рисунок 4.8. Експериментальний шлях від baseline до operator preview

Рисунок 4.8 показує, що кожний етап додає не "красивішу модель", а новий рівень доказовості. Raw forecast стає корисним тільки після LP scoring; V2+ стає headline лише після rolling robustness; shadow lanes залишаються diagnostics, якщо не проходять gate.

Цей шлях пояснює, чому результат не зводиться до одного числа. V2 довів корисність schedule/value selection проти strict baseline. V2+ став headline, бо одночасно знизив mean regret, median regret і пройшов rolling robustness. Shadow lanes після цього мали роль challengers: вони могли замінити V2+ лише за умови prior-only evidence, non-degradation і достатнього improvement. Corrected DT/V2+ safe-switch shadow дав таке локальне improvement на frozen packet, але ще не має promotion gate, source-readiness closure і default-switch policy; тому він є candidate for future promotion, а не поточним replacement.

4.7. Що можна впевнено показувати оператору

На поточному етапі оператору можна показувати не market command, а evidence-backed preview. До такого preview належать DAM/IDM hourly recommendation rows, official-row або forecast-scenario context, regret/value comparison, V2+ versus strict/raw comparators, gate status, V13 readiness і shadow diagnostics. Кожний елемент має читатися як пояснення рішення, а не як ProposedBid, ClearedTrade або DispatchCommand.

Склад такого operator-facing preview наведено на рисунку 4.9.

![Рисунок 4.9. Що можна впевнено показувати оператору](assets/compact-fig-4-7-operator-preview.png)

Рисунок 4.9. Що можна впевнено показувати оператору

Рисунок 4.9 відділяє дозволені read-model елементи від недозволених execution objects. Оператор може бачити schedule preview, regret/value comparison і gate status, але система не формує market-submittable bid або dispatch command.

У показаному materialized packet operator-preview частина має 24 hourly DAM rows, зокрема 1 BUY, 2 SELL і 21 HOLD. Той самий read-model contract підтримує IDM hourly recommendation preview rows, але не live IDM bids. Якщо DAM/IDM row уже published, LP використовує official row first; якщо target ще unpublished, ML forecast scenarios можуть бути лише scenario input до LP. Такий розподіл не є слабкістю: HOLD може бути правильним рішенням, якщо spread, SOC або risk не підтримують charge/discharge. Саме тому dashboard має оцінюватися через regret/value і gate status, а не через кількість активних BUY/SELL action labels.

4.8. Практична інтерпретація для України

Практичний результат полягає не в тому, що система торгує, а в тому, що вона допомагає оператору приймати evidence-informed decisions. Для українського BESS owner це корисно з трьох причин. По-перше, DAM/IDM hourly preview можна демонструвати без regulatory overreach. По-друге, кожний result row має audit trail і може бути пояснений через regret/value. По-третє, система явно показує blockers: source/publication evidence, V13, DT/LAVA і market execution не змішуються з current MVP.

Прапорець market_execution_enabled=false у цьому контексті є частиною safety case. Він означає, що навіть підтверджений offline result не перетворюється автоматично на live market action. Щоб змінити цю межу, потрібні source/publication evidence, operational responsibility, legal review, market integration і monitoring; market-submission receipts залишаються окремими для execution contour. Тому current practical suitability означає operator-facing evidence preview, а не готовність до live trading.

4.9. Обмеження результатів

Головні обмеження результатів залишаються явними. Результат не охоплює live bid submission, settlement reconciliation, balancing market, physical inverter dispatch або повний electrochemical digital twin. LP contour є достатнім для однакового feasibility/value evaluation, але не замінює production asset-management model. External source lanes, зокрема Poland lag24 / prior-only veto і V13, показують потенціал, але не скасовують source-governance blockers.

Ці обмеження не зменшують цінність розділу, бо вони визначають чесну межу claims. V2+ можна описувати як strongest offline/read-model strategy result у наявному evidence set. Його не можна описувати як універсальну гарантію прибутку, market-ready bid engine або deployed DT/DFL controller. Така межа робить результат захищеним академічно й придатним для практичного roadmap.

4.10. Висновок до розділу 4

Головний результат роботи - V2+ schedule/value selector з mean regret 174.77 UAH, median regret 67.30 UAH, improvement 43.73% проти strict і 15.31% проти V2, а також rolling robustness 4 / 4. Corrected DT/V2+ safe-switch shadow показує практично важливий follow-up: mean regret 168.16 UAH, median regret 61.71 UAH, 4 / 90 safe switches, 3 / 15 recovered opportunities і 0 tail-risk losses. Але він не змінює headline/default без окремого promotion gate: V2+ є confirmed offline schedule-value evidence/comparator, dashboard/API показують DT shadow лише manual diagnostic, а operator preview path використовує official OREE row + deterministic LP. Підсумкова межа залишається незмінною: DAM/IDM hourly recommendation preview, offline/read-model evidence і no market execution.
