Розділ 1. Вступ

1.1. Актуальність теми

Українська енергосистема у 2026 році потребує інструментів, які допомагають працювати з волатильністю, дефіцитом маневровості та зростанням ролі систем накопичення енергії. BESS може переносити енергію між годинами з різною ціною, але практична цінність такого переносу виникає лише тоді, коли рішення одночасно враховує ринковий сигнал, фізичні обмеження батареї, часову доступність даних і операційну безпеку. Сам прогноз ціни не є достатнім результатом: модель може мати прийнятну forecast error, але створити schedule з поганим SOC timing або низькою економічною цінністю.

Тому в роботі розглянуто не задачу автономної ринкової торгівлі, а задачу побудови evidence system для DAM recommendation preview. Така система пояснює оператору, чому певний hourly schedule є доцільним, які дані були доступні до моменту рішення, як результат порівнюється з conservative baseline і чому він не перетворюється автоматично на market-submittable bid. Ця межа важлива академічно й практично: без неї дипломний результат легко перебільшити до live trading або deployed Decision Transformer control, хоча наявна доказова база цього не дозволяє.

Логіку переходу від проблеми до рішення наведено на рисунку 1.1.

![Рисунок 1.1. Карта переходу від проблеми українського DAM до operator preview](assets/compact-fig-1-1-problem-solution.png)

Рисунок 1.1. Карта переходу від проблеми українського DAM до operator preview

Рисунок 1.1 узагальнює центральну ідею роботи. Ціновий сигнал DAM стає корисним лише після deterministic optimization і strict evidence gate. Саме тому головний внесок полягає не в додаванні ще однієї нейронної моделі, а в побудові чесного контуру, де forecast, schedule optimization, regret evaluation і source-governance працюють разом.

1.2. Проблема, що вирішується

Проблема має три рівні. Перший рівень - ринковий: погодинні ціни DAM можуть створювати можливість арбітражу, але реальна можливість залежить від spread shape, local price regime і доступності source-backed publication evidence. Другий рівень - інженерний: BESS не можна розглядати як абстрактний фінансовий актив, бо schedule має проходити SOC, power, efficiency і degradation-aware обмеження. Третій рівень - науковий: оцінювати модель за MAE/RMSE недостатньо, якщо рішення використовується для storage arbitrage; потрібна decision-aware метрика, що вимірює regret або value після optimization.

У роботі ці три рівні зведено в один offline/read-model contour. Price forecasts та candidate schedules оцінюються через однаковий strict LP/oracle evaluator. Learned або heuristic candidates можуть бути корисними лише тоді, коли вони знижують regret проти frozen baseline, не погіршують median behavior, проходять rolling robustness і не порушують safety/source gates. Якщо challenger не проходить gate, це не приховується: негативна evidence залишається частиною наукового результату й пояснює, чому система не просуває слабший метод.

Межу системи наведено на рисунку 1.2.

![Рисунок 1.2. Межа системи: evidence та preview без market execution](assets/compact-fig-1-2-boundary.png)

Рисунок 1.2. Межа системи: evidence та preview без market execution

Рисунок 1.2 показує, що результат належить до allowed evidence: offline materialization, read-model dashboard, operator preview і reproducible packets. Заблокована зона містить ProposedBid, ClearedTrade, DispatchCommand, deployed DT/LAVA і будь-який claim, який потребує explicit market submission readiness. Така межа зберігає практичну корисність без переходу до невиправданого market execution.

1.3. Мета і завдання

Мета роботи - спроєктувати, реалізувати та оцінити evidence system для DAM recommendation preview у задачі BESS-арбітражу в Україні, де якість моделей оцінюється за downstream decision value, а не лише за forecast-only метриками. Практична мета полягає в тому, щоб оператор або власник BESS отримував зрозумілу картину: які hourly recommendations пропонуються, чим вони кращі або гірші за conservative baseline, які gates пройдено і чому система не подає заявку на ринок.

Для досягнення цієї мети виконано такі завдання: сформовано market/problem framing для українського DAM; побудовано pipeline source snapshots -> normalized panel -> forecast/candidates -> strict LP/oracle scoring -> dashboard/read model; визначено метрики regret/value та rolling robustness; перевірено Schedule/Value Learner V2 і V2+ проти strict_similar_day; проаналізовано TFT, Poland/TFT і DT/V2+ safe-switch shadow як дослідницькі, але не default/execution гілки; зафіксовано V13 source-readiness blockers і прапорець market_execution_enabled=false.

1.4. Об'єкт, предмет і межі дослідження

Об'єктом є система прийняття рішень для BESS у DAM delivery-day planning scenario. Предметом є методологія побудови й оцінювання forecast-to-schedule candidates через strict LP/oracle contour та decision-aware regret/value metrics. У роботі не досліджується live market bidding, settlement, balancing-market participation, physical inverter dispatch або юридична процедура участі на ринку. Такі елементи потребують окремих operational gates, credentials, signed submissions, explicit DAM publication receipts і відповідальності оператора.

Межа market_execution_enabled=false проходить через усю роботу. Вона означає, що навіть коли offline evidence показує кращий regret, система не створює market order payload. Це дає можливість академічно оцінити практичну придатність методу для України без небезпечного змішування демонстраційного preview з торговою системою.

1.5. Наукова новизна і практична цінність

Наукова новизна полягає у фокусі на decision-quality evidence для BESS arbitrage: forecast candidates оцінюються не ізольовано, а через LP schedule та regret відносно oracle value. Додатково робота демонструє, як negative evidence, corrected safe-switch shadow evidence або blocked V13 acquisition можуть бути академічно коректними artifacts, якщо вони запобігають overclaiming. Практична цінність для України полягає в тому, що system design може бути використаний як консервативний операторський preview: він показує економічну логіку рішення, залишає audit trail і не потребує market-submission credentials для academic MVP.

1.6. Структура роботи

Розділ 2 стисло розглядає ринковий та методологічний контекст. Розділ 3 описує методологію, математичні вирази, evidence boundaries і архітектуру. Розділ 4 подає головні результати V2/V2+, robustness і shadow evidence. Розділ 5 формулює висновки й рекомендації. Детальні glossary, API traceability, довгі evidence manifests і shadow diagnostics винесено в додатки, щоб основна частина залишалася зосередженою на main story.


1.7. Практичний контекст для українського оператора

Практичний користувач такої системи не починає з питання, яка neural architecture є наймоднішою. Його перше питання простіше: чи можна довіряти рекомендації, коли ціна завтра може суттєво відрізнятися від історичного патерну, а батарея має реальні обмеження? Тому в роботі акцент зроблено на прозорості контуру. Оператор має бачити не лише "купити" або "продати", а й те, що recommendation прийшла з відтворюваного evidence packet, пройшла deterministic checks і не є ринковою заявкою.

Для України це особливо важливо через різницю між академічною демонстрацією та промисловою інтеграцією. У промисловому контурі потрібні credentials, юридична відповідальність, receipt evidence, settlement reconciliation і диспетчерські процедури. У дипломному контурі інженерний підхід демонструє чесне ставлення до таких меж: без explicit source publication receipts система не маскує blocker красивим dashboard, а навіть corrected DT/V2+ shadow з кращим regret не називається контролером або default strategy без окремого promotion gate.

Таке формулювання робить роботу прикладною, але не ризиковою. Вона не обіцяє автономної торгівлі; вона показує, як можна прийти до неї поступово: спочатку offline evidence, потім operator trust, потім source readiness, і лише після цього execution gate. У цьому сенсі результат корисний не тільки як кодовий MVP, а як методологічний шаблон для подібних українських energy-tech систем.

1.8. Відмінність між науковим результатом і інженерним артефактом

Науковий результат у роботі - це не просто наявність dashboard або факт, що pipeline запускається. Науковий результат полягає в доведенні, що schedule/value selector може зменшити downstream regret у строгому порівнянні з baseline, а також у доведенні меж, де інші candidates не проходять gate. Інженерний артефакт підтримує цей результат: Dagster materialization, FastAPI read model, Pydantic contracts і dashboard роблять evidence відтворюваною та зрозумілою.

Це розмежування допомагає уникнути двох крайнощів. Перша крайність - описати роботу як чистий software project без наукової оцінки. Друга - описати її як research paper без практичного MVP. У роботі обрано середній шлях: практичний проєкт має науково обґрунтований evaluation contour, а дослідницька частина має робочий інженерний носій.
