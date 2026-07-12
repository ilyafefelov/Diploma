Розділ 5. Висновки та рекомендації

> **Post-defense correction (2026-07-12).** The 168.16 UAH artifact historically
> named `dt_v2_plus` is random forest trained on an exact mirrored packet, not a
> Decision Transformer or OOS result. The separate HF transformer-backbone
> diagnostic is also non-independent. See
> [ERRATA_MODEL_LINEAGE_2026-07-12.md](../ERRATA_MODEL_LINEAGE_2026-07-12.md).

5.1. Підсумок виконаних завдань

У роботі побудовано практичну evidence system для DAM/IDM hourly recommendation preview у задачі BESS-арбітражу. Основна ідея полягала в тому, щоб оцінювати не лише прогноз ціни, а downstream decision value після LP schedule optimization. DAM/V2+ залишається primary evaluated thesis evidence, тоді як read-model capability підтримує DAM і IDM preview під тією самою no-execution межею. Це дозволило перейти від forecast-only порівнянь до regret/value evidence, яка краще відповідає реальній економічній задачі оператора BESS.

Підсумок цілей і результатів наведено в таблиці 5.1.

Таблиця 5.1. Цілі роботи та отримані результати

| Мета | Підсумок | Практичне значення |
| --- | --- | --- |
| Оцінити DAM BESS рішення | Regret/value введено як головний критерій | Рішення оцінюються економічно, не тільки forecast-only |
| Побудувати MVP | Dagster/FastAPI/dashboard read model працює як evidence surface | Оператор бачить preview без live execution |
| Перевірити ML/DFL потенціал | V2+ став confirmed offline comparator/evidence; історичний `dt_v2_plus` RF дав 168.16 UAH лише на exact-mirror packet; HF scorer і 32-day readiness audit залишаються окремими diagnostics | Система зберігає model lineage і не overclaim-ить OOS performance або promotion |
| Зберегти безпеку | market_execution_enabled=false і V13 blockers явні | Підхід придатний для академічної демонстрації в Україні |

З таблиці 5.1 видно, що основні завдання виконано в межах безпечного scope. Робота не заявляє live market execution, але дає defendable academic MVP: source-backed offline evidence, dashboard/read-model preview, strict LP/oracle scoring і пояснювану межу V13.

5.2. Основні висновки

Перший висновок: у задачі BESS arbitrage головним є не прогноз як такий, а якість рішення після перетворення price signal у feasible schedule. Forecast-only метрики залишаються корисними diagnostics, але вони не відповідають на питання, скільки economic value втрачено відносно oracle LP. Саме тому regret/value, rolling robustness і gate status є центральними метриками роботи. Це зміщує фокус із "яка модель точніше передбачає ціну" на "яка архітектура стабільно створює кращі decision outcomes".

Другий висновок: найкращий підтверджений baseline у роботі - не найскладніша neural model, а V2+ schedule/value selector. Його mean regret 174.77 UAH, median regret 67.30 UAH і rolling robustness 4 / 4 показують, що conservative candidate selection із V2 fallback краще відповідає на ринкову форму українського DAM slice, ніж raw forecast або unconstrained classifier. Це важливий академічний результат: у прикладних energy systems простий, контрольований decision layer може бути сильнішим за модель із більш сучасною назвою.

Третій висновок: learned-challenger лінії показали важливість fallback, abstention і чіткої model lineage, але не дали незалежного model-performance evidence. Raw HF DT candidate-index smoke має 460.30 UAH на mirrored packet. Історичний `dt_v2_plus` результат 168.16 UAH створено random forest, а не transformer, і всі його чотири profile-row switches належать одній delivery date. HF value-aligned `DecisionTransformerModel`-backbone scorer має 158.71 UAH mirrored-packet diagnostic та окремий read-model readiness audit без realized regret. Коректний урок полягає у необхідності constrained candidate scoring, deterministic guards і незалежної prospective validation, а не у доведеній перевазі transformer architecture.

Четвертий висновок: найбільш продуктивні архітектурні прийоми - safe-switch framing, value-aligned candidate library, tail-risk guard, deterministic safety projection і source-backed-only policy. Вони працюють разом: candidate library дає моделі економічно змістовні варіанти, scorer шукає improvement проти V2+, tail-risk guard блокує "прибуткові, але небезпечні" schedules, deterministic projection не дозволяє ML score порушити фізику батареї, а source-backed policy забороняє dashboard малювати фейкові ціни. Саме ця комбінація пояснює, чому HF value-aligned shadow став сильнішим demo/challenger path, ніж попередні raw DT або forecast-only alternatives.

П'ятий висновок: для українського DAM/IDM контексту контрольована прозорість цінніша за передчасну автономність. Оператору потрібні 24 hourly rows, source mode, guard diagnostics, schedule/value comparison і зрозуміле пояснення abstention; йому не потрібна прихована market order system у дипломному MVP. Тому `market_execution_enabled=false`, no `ProposedBid`, no market payload і V13/source-readiness blockers є не слабкістю, а частиною науково й інженерно чесного design.

Шостий висновок: майбутній шлях до near-LP neural optimizer проходить не через просту заміну LP на HF. Реалістичний розвиток: розширити candidate library, зібрати teacher LP/MIP schedules, навчити distillation model передбачати schedule/action vectors, додати projection/repair layer і перевіряти результат проти frozen LP oracle на source-ready data. Тільки після цього можна говорити про LP surrogate; у поточній роботі HF value-aligned shadow коректно називати live operator-preview challenger, а не production optimizer.

5.3. Рекомендації для впровадження

Практичний roadmap наведено на рисунку 5.1.

![Рисунок 5.1. Roadmap від thesis evidence до supervised execution gate](assets/compact-fig-5-1-roadmap.png)

Рисунок 5.1. Roadmap від thesis evidence до supervised execution gate

Рисунок 5.1 окреслює послідовний шлях: спочатку evidence and preview, потім OREE DAM/IDM source/publication evidence і point-in-time context, після цього shadow challengers під strict LP/oracle, і лише значно пізніше supervised execution gate. Такий порядок зменшує ризик overclaiming і робить систему придатною для поступового використання в українському контексті.

V2+ залишається confirmed offline schedule-value evidence/comparator, а TFT/DT/Poland lanes - shadow diagnostics. Operator preview path використовує official OREE row + deterministic LP для default/baseline preview; HF value-aligned shadow окремо показує, що transformer-backbone scorer може працювати як manually selected read-model source без LP у HF request path. Історичний `dt_v2_plus` random forest не є candidate for promotion на підставі 168.16 UAH, оскільки training/evaluation rows exact-mirrored і всі чотири switches припадають на одну delivery date. Майбутній challenger потребує frozen pre-evaluation protocol, genuinely later period, source-readiness closure і повторної multi-tenant validation. Подальша робота зосереджується на explicit OREE DAM/IDM source/publication evidence for preview, richer point-in-time context, перевірці null coverage у зовнішніх features, LP-distillation/repair layer і окремому legal/operational design для market-submittable contours. До цього моменту жодний dashboard або API endpoint не описується як trading console.

5.4. Підсумковий висновок

Робота демонструє практично придатний і академічно чесний шлях від українського DAM price signal до BESS recommendation preview, розширений до DAM/IDM hourly recommendation preview як product/read-model capability. Головна цінність полягає в disciplined evidence: результат є коротшим, зрозумілішим і краще обмеженим, ніж перелік усіх дослідницьких траєкторій. HF value-aligned shadow показує, як advanced sequence-model evidence можна включити в живий dashboard без production overreach: source-backed context, LP-free candidates, HF scoring, deterministic gates і no-execution flags. Claim boundary залишається незмінною: DAM/IDM hourly recommendation preview, offline/read-model evidence, no market execution.


5.5. Обмеження роботи

Основне обмеження полягає в тому, що результат не є market-executable system. У роботі немає live bid submission, settlement reconciliation, physical dispatch integration або юридичного процесу участі на ринку. Це не недолік реалізації, а свідома межа. Для market execution потрібні інші gates, відповідальність, source/publication evidence і market-submission receipts, яких поточний academic MVP не заявляє.

Друге обмеження - спрощення фізики батареї. LP contour враховує основні feasibility constraints, але не є повним electrochemical digital twin. Для практичного preview цього достатньо, а повний production asset management потребує детальнішої SOH/degradation моделі й перевірки на measured telemetry.

Третє обмеження - tenant-specific operational preferences. У конфігурації вже є location, battery metrics, load profile і consumption schedule для різних типів клієнтів, але поточний LP contour не трактує user reserved hours, робочі години або індивідуальні preferences як жорсткі schedule constraints. Вони використовуються як tenant/load context і readiness surface, тоді як default schedule optimization залишається price-driven під SOC, power, efficiency і degradation constraints. Для production contour ці preferences мають бути формалізовані як per-hour availability masks, підвищений minimum SOC для critical loads або penalty terms у objective.

Четверте обмеження - статус зовнішніх джерел. TFT, Poland lag24 / prior-only veto і V13 acquisition lanes показують потенціал, але не проходять достатні gates для headline promotion. Тому вони залишаються roadmap evidence, а не результатом, який змінює default strategy.

П'яте обмеження - HF value-aligned shadow не є математично повним optimizer. Він ранжує скінченну candidate library, а не розв'язує весь простір допустимих hourly schedules. Тому його не можна називати LP replacement in production. Щоб рухатися в цьому напрямі, потрібні teacher LP/MIP datasets, constraint-aware decoding або projection/repair layer, apples-to-apples validation проти frozen LP oracle і source-readiness closure для training data.

5.6. Напрями подальшого розвитку

Перший напрям - завершення explicit OREE DAM/IDM source/publication evidence lane for preview; market-submission receipts remain separate для execution contour. Це розблокує сильніші source-readiness claims і дозволить безпечніше розглядати DT/LAVA або інші sequence-policy approaches. Другий напрям - створити новий challenger protocol замість promotion історичного RF packet: frozen estimator identity, features, thresholds і strict/oracle scoring до початку genuinely later evaluation period, no-tail-risk condition і explicit default-switch policy. Третій напрям - покращення point-in-time context для candidate-value learners і контрольоване розширення TFT/Poland feature space після repair null coverage і rolling validation. Четвертий напрям - додати user-specific operational constraints: reserved hours, load-priority windows, tenant preference profiles і critical-load SOC reserve мають входити в LP або repair layer як формальні constraints, а не як неявні UI labels. П'ятий напрям - розвивати HF value-aligned shadow у near-optimizer research path: розширити candidate library, зібрати teacher LP/MIP schedules, навчити distillation model передбачати schedule/action vectors, додати deterministic projection/repair і порівнювати результат проти LP на frozen windows без зняття `market_execution_enabled=false`.

Окремий напрям розвитку - operator experience. Dashboard пояснює не тільки що рекомендовано, а й чому рекомендація не є market command. Для практичного використання це може бути важливіше за додавання ще однієї моделі: оператору потрібна довіра, auditability і зрозуміла межа відповідальності.

5.7. Завершальний практичний висновок

Коротка інтерпретація роботи така: система вже корисна як академічний і операторський preview, але ще не є trading system. Саме ця чесність робить результат обґрунтованим. Він показує реальну інженерну придатність для України, не перебільшуючи рівень ринкової готовності.


5.8. Пілотне використання після завершення роботи

Робота може бути основою для controlled pilot. У такому контурі market_execution_enabled=false зберігається, source-readiness та monitoring поступово покращуються, shadow validation виконується на новіших даних без зміни default strategy, а operator decisions порівнюються з preview для оцінки корисності пояснень.

Для такого pilot HF value-aligned shadow можна використовувати як read-only supervised challenger. Оператор може вручну обрати його в dashboard і побачити 24 hourly rows або explicit blocked/abstained reason для DAM/IDM latest/today/tomorrow/day+2. Це корисно для демонстрації й збору feedback, але не створює bid, не викликає market payload і не замінює V2+ default comparator.

Execution integration належить до наступного етапу. Навіть тоді execution залишається окремим bounded context, із власними schema, permissions, audit logs і rollback procedures. Поточна робота корисна саме тим, що не змішує ці етапи.

5.9. Підсумкова академічна позиція

Академічна позиція роботи полягає в контрольованій практичності. Вона не є суто теоретичною, бо має працюючий MVP і реальні evidence artifacts. Вона не є неконтрольованим стартап-прототипом, бо кожний сильний claim має gate і boundary. Вона не є повним research paper про DT або DFL, бо DT/DFL напрями залишені в чесному status research-shadow / future work.

Саме така позиція робить текст придатним для пояснювальної записки: він демонструє розуміння предметної області, інженерних обмежень і академічної відповідальності за claims.


5.10. Відповідь на дослідницьке питання

Дослідницьке питання можна сформулювати так: чи можна побудувати практично придатну, безпечну й evidence-backed систему DAM/IDM hourly recommendation preview для BESS в Україні, де якість оцінюється через decision value? Відповідь роботи - так, у межі offline/read-model evidence. V2+ демонструє кращий regret/value результат для primary DAM evidence, а архітектура показує, як подати оператору DAM або IDM hourly recommendation preview без переходу до market execution.

Друга частина відповіді не менш важлива: advanced AI lanes не готові до default promotion. TFT, Poland lag24 / prior-only veto і raw DT classifier є diagnostics; історичний `dt_v2_plus` RF є exact-mirror construction diagnostic, а не positive OOS evidence. HF value-aligned shadow показує шлях для transformer-backbone read-model demo: candidate scoring + abstention + deterministic gates + source-backed dashboard data flow, але його 158.71 UAH також не є independent holdout result. V13 source-readiness лишається gate. Тому підсумковий результат не перебільшує AI capability, а показує контрольовану практичну цінність і реалістичний напрям розвитку.
