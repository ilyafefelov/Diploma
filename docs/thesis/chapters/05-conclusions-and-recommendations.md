Розділ 5. Висновки та рекомендації

5.1. Підсумок виконаних завдань

У роботі побудовано практичну evidence system для DAM recommendation preview у задачі BESS-арбітражу. Основна ідея полягала в тому, щоб оцінювати не лише прогноз ціни, а downstream decision value після LP schedule optimization. Це дозволило перейти від forecast-only порівнянь до regret/value evidence, яка краще відповідає реальній економічній задачі оператора BESS.

Підсумок цілей і результатів наведено в таблиці 5.1.

Таблиця 5.1. Цілі роботи та отримані результати

| Мета | Підсумок | Практичне значення |
| --- | --- | --- |
| Оцінити DAM BESS рішення | Regret/value введено як головний критерій | Рішення оцінюються економічно, не тільки forecast-only |
| Побудувати MVP | Dagster/FastAPI/dashboard read model працює як evidence surface | Оператор бачить preview без live execution |
| Перевірити ML/DFL потенціал | V2+ став default, corrected DT/V2+ safe-switch shadow дав 168.16 UAH mean regret | Система показує improvement, але не overclaim-ить promotion |
| Зберегти безпеку | market_execution_enabled=false і V13 blockers явні | Підхід придатний для академічної демонстрації в Україні |

Таблиця 5.1 показує, що основні завдання виконано в межах безпечного scope. Робота не заявляє live market execution, але дає defendable academic MVP: source-backed offline evidence, dashboard/read-model preview, strict LP/oracle scoring і пояснювану межу V13.

5.2. Основні висновки

Перший висновок: для BESS arbitrage forecast-only метрики недостатні. Практична якість визначається тим, скільки economic value schedule втрачає відносно oracle LP. Тому regret/value має бути центральним критерієм, а MAE/RMSE - допоміжним forecast diagnostics.

Другий висновок: Schedule/Value Learner V2+ є найсильнішим підтвердженим результатом у наявному evidence set. Його mean regret 174.77 UAH, median regret 67.30 UAH і rolling robustness 4 / 4 формують достатню основу для offline/read-model Strategy Promotion, але не для market execution.

Третій висновок: shadow evidence є важливою частиною академічної якості. TFT, Poland/TFT, raw DT classifier і V3/V4/V5 diagnostics не були приховані або подані як перемоги. Corrected DT/V2+ safe-switch shadow показав невелике regret improvement без tail-risk losses, але також показав правильну межу: positive diagnostic не дорівнює default promotion.

Четвертий висновок: для української практики найціннішою є не автономність, а контрольована прозорість. Operator preview може допомогти зрозуміти schedule logic, але market_execution_enabled=false має залишатися явним до проходження джерельних, юридичних і операційних gates.

5.3. Рекомендації для впровадження

Практичний roadmap наведено на рисунку 5.1.

![Рисунок 5.1. Практичний roadmap впровадження для України](assets/compact-fig-5-1-roadmap.png)

Рисунок 5.1. Практичний roadmap впровадження для України

Рисунок 5.1 показує послідовний шлях: спочатку evidence and preview, потім source receipts і point-in-time context, після цього shadow challengers під strict LP/oracle, і лише значно пізніше supervised execution gate. Такий порядок зменшує ризик overclaiming і робить систему придатною для поступового використання в українському контексті.

V2+ залишається default preview strategy, а TFT/DT/Poland lanes - shadow diagnostics. Corrected DT/V2+ safe-switch selector є найперспективнішим candidate для майбутнього promotion, бо вже покращив mean regret на 3.78% проти V2+ на frozen packet, але потребує окремого promotion gate, source-readiness closure і повторної validation. Подальша робота зосереджується на explicit DAM publication receipts, richer point-in-time context, перевірці null coverage у зовнішніх features і окремому legal/operational design для market-submittable contours. До цього моменту жодний dashboard або API endpoint не описується як trading console.

5.4. Підсумковий висновок

Робота демонструє практично придатний і академічно чесний шлях від українського DAM price signal до BESS recommendation preview. Головна цінність полягає в disciplined evidence: результат є коротшим, зрозумілішим і краще обмеженим, ніж перелік усіх дослідницьких траєкторій. Claim boundary залишається незмінною: DAM recommendation preview, offline evidence, no market execution.


5.5. Обмеження роботи

Основне обмеження полягає в тому, що результат не є market-executable system. У роботі немає live bid submission, settlement reconciliation, physical dispatch integration або юридичного процесу участі на ринку. Це не недолік реалізації, а свідома межа. Для market execution потрібні інші gates, відповідальність і source receipts, яких поточний academic MVP не заявляє.

Друге обмеження - спрощення фізики батареї. LP contour враховує основні feasibility constraints, але не є повним electrochemical digital twin. Для практичного preview цього достатньо, а повний production asset management потребує детальнішої SOH/degradation моделі й перевірки на measured telemetry.

Третє обмеження - статус зовнішніх джерел. TFT, Poland/TFT і V13 acquisition lanes показують потенціал, але не проходять достатні gates для headline promotion. Тому вони залишаються roadmap evidence, а не результатом, який змінює default strategy.

5.6. Напрями подальшого розвитку

Перший напрям - завершення explicit DAM publication receipt lane. Це розблокує сильніші source-readiness claims і дозволить безпечніше розглядати DT/LAVA або інші sequence-policy approaches. Другий напрям - винести corrected DT/V2+ safe-switch selector у формальний promotion gate: frozen strict/oracle scoring, повторна final-holdout validation, no-tail-risk condition і explicit default-switch policy. Третій напрям - покращення point-in-time context для candidate-value learners і контрольоване розширення TFT/Poland feature space після repair null coverage і rolling validation.

Окремий напрям розвитку - operator experience. Dashboard пояснює не тільки що рекомендовано, а й чому рекомендація не є market command. Для практичного використання це може бути важливіше за додавання ще однієї моделі: оператору потрібна довіра, auditability і зрозуміла межа відповідальності.

5.7. Завершальний практичний висновок

Коротка інтерпретація роботи така: система вже корисна як академічний і операторський preview, але ще не є trading system. Саме ця чесність робить результат обґрунтованим. Він показує реальну інженерну придатність для України, не перебільшуючи рівень ринкової готовності.


5.8. Пілотне використання після завершення роботи

Робота може бути основою для controlled pilot. У такому контурі market_execution_enabled=false зберігається, source-readiness та monitoring поступово покращуються, shadow validation виконується на новіших даних без зміни default strategy, а operator decisions порівнюються з preview для оцінки корисності пояснень.

Execution integration належить до наступного етапу. Навіть тоді execution залишається окремим bounded context, із власними schema, permissions, audit logs і rollback procedures. Поточна робота корисна саме тим, що не змішує ці етапи.

5.9. Підсумкова академічна позиція

Академічна позиція роботи полягає в контрольованій практичності. Вона не є суто теоретичною, бо має працюючий MVP і реальні evidence artifacts. Вона не є неконтрольованим стартап-прототипом, бо кожний сильний claim має gate і boundary. Вона не є повним research paper про DT або DFL, бо DT/DFL напрями залишені в чесному status research-shadow / future work.

Саме така позиція робить текст придатним для пояснювальної записки: він демонструє розуміння предметної області, інженерних обмежень і академічної відповідальності за claims.


5.10. Відповідь на дослідницьке питання

Дослідницьке питання можна сформулювати так: чи можна побудувати практично придатну, безпечну й evidence-backed систему DAM recommendation preview для BESS в Україні, де якість оцінюється через decision value? Відповідь роботи - так, у межі offline/read-model evidence. V2+ демонструє кращий regret/value результат, а архітектура показує, як цей результат подати оператору без переходу до market execution.

Друга частина відповіді не менш важлива: не всі advanced AI lanes готові до default promotion. TFT, Poland/TFT і raw DT classifier є корисними diagnostics; corrected DT/V2+ safe-switch shadow уже дає positive regret evidence, але залишається manual research source до окремого promotion gate. V13 source-readiness лишається gate. Тому підсумковий результат не перебільшує AI capability, а показує контрольовану практичну цінність.
