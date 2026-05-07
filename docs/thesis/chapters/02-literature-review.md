# Розділ 2. Огляд літератури

> Перший submission-ready варіант розділу 2 для подання керівнику. Текст уже придатний як аналітична основа пояснювальної записки, Week 1 report і supervisor review; надалі bibliographic base може розширюватися разом із розвитком експериментів, але цей розділ більше не є тимчасовою чернеткою.

## 2.1. Вступ: чому огляд літератури для цього проєкту не може бути вузьким

Автономний енергоарбітраж для battery energy storage systems (BESS) лежить на перетині кількох напрямів: прогнозування цін електроенергії, оптимізація торгових рішень, урахування фізичних обмежень батареї, оцінювання деградації та побудова відтворюваної MLOps-інфраструктури. Для цієї дипломної роботи недостатньо лише перерахувати моделі або статті. Важливо показати, як кожна група підходів впливає на архітектурні рішення: чому поточний MVP обмежено baseline-рішенням, чому фінальна цільова версія передбачає DFL-контур, і чому між ними потрібен окремий demo-stage.

Проєкт має інженерний формат, але опирається на дослідницьку логіку. Поточний підтверджений результат у репозиторії відповідає demo-ready MVP для Level 1: погодинний ринок DAM, канонічна валюта UAH, strict similar-day baseline forecast, LP-baseline strategy, projected battery state preview, окрема деградаційна складова в економіці та операторська dashboard-поверхня для контролю й пояснення результату. Паралельно зафіксовано цільову еволюцію до Decision-Focused Learning (DFL), де модель навчається не лише на точності forecast, а на якості фінального економічного рішення. Саме тому огляд літератури має не просто описати state of the art, а пояснити поетапну траєкторію розвитку системи.

## 2.2. Практичний ринковий контекст і state of practice для BESS

Якщо дивитися не лише на академічні роботи, а й на типову практику побудови BESS-рішень, то state of practice зазвичай спирається на кілька інженерно зрозумілих блоків: збір телеметрії, прогнозування короткострокових цін або навантаження, rule-based або optimization-based scheduling, операторський dashboard та шар safety constraints перед фізичним виконанням. Тобто ринкова практика не починає з повного end-to-end learned policy. Вона майже завжди починається з керованого baseline-контуру, який можна протестувати, аудіювати і пояснити бізнес- або операційному користувачу.

У випадку цієї дипломної роботи така логіка є принципово важливою. Завдання полягає не в тому, щоб одразу побудувати найскладнішу можливу модель, а в тому, щоб створити систему, яка є одночасно технічно відтворюваною, дослідницьки осмисленою і придатною до поетапної демонстрації. Саме тому поточний MVP свідомо обмежено Level 1 Market Scope: лише погодинний DAM, без одночасного охоплення IDM, balancing або multi-venue selection. Ця межа узгоджується з принципом Sequential Complexity, зафіксованим у [CONTEXT.md](../../../CONTEXT.md), і знижує ризик того, що проблема нестабільності інженерного пайплайну буде помилково інтерпретована як слабкість дослідницької ідеї.

Практично це означає, що demo-stage у проєкті не є «урізаною версією фінальної системи» у негативному сенсі. Навпаки, він виконує окрему методологічну роль: доводить, що контрольний контур, операторська поверхня та базові read models працюють коректно до того, як у систему буде додано складніші learned-компоненти.

## 2.3. Енергоарбітраж BESS як задача оптимізації

У літературі енергоарбітраж для BESS найчастіше формулюється як задача вибору моментів заряду та розряду для максимізації економічного результату за наявності обмежень на потужність, ємність, ККД та часову структуру цін. У найпростішому формулюванні це задача лінійної або змішаної оптимізації. Сильна сторона такого підходу полягає в прозорості: зв’язок між вхідними даними, обмеженнями та рекомендацією залишається зрозумілим.

Нова група джерел додає до цієї картини критично важливий проміжний шар між простими LP-постановками та повноцінним digital twin. Vykhodtsev et al. показують, що в багатьох techno-economic studies батарея досі подається через спрощене power-energy representation, яке добре працює для системного планування, але слабко відтворює реальні фізичні механізми старіння. Hesse et al. і Maheshwari et al. рухаються далі: вони намагаються вбудувати ageing- та efficiency-aware логіку безпосередньо в dispatch optimization, зокрема через MILP або кусочно-лінійні апроксимації нелінійної деградації. Це робить економічну оцінку реалістичнішою, але одразу підвищує обчислювальну вартість і ускладнює використання таких моделей у багатокроковому ринковому середовищі.

Для проєкту це критично з двох причин. По-перше, baseline-стратегія має бути придатною як контрольна група, з якою можна порівнювати складніші підходи. По-друге, вона має бути достатньо стабільною для supervisor demo. Саме тому в поточному репозиторії центральну роль відіграє LP-baseline, а не навчувана policy. Такий baseline не претендує на остаточний research contribution, але створює необхідну точку відліку для оцінювання regret та подальшого переходу до DFL.

Робота Yi et al. про predict-then-bid для енергоарбітражу BESS підсилює цю логіку: реальна задача не зводиться до самого forecast. Вона включає торгове рішення, реакцію ринку, ефект клірингу та підсумковий фінансовий результат. Отже, простий LP-baseline не суперечить дослідницькій новизні цієї роботи, а є необхідним першим рівнем, без якого складніший DFL-контур було б важко коректно інтерпретувати.

## 2.4. Forecasting state of the art: від наївного baseline до NBEATSx і TFT

Жодна стратегія арбітражу не може працювати без оцінки майбутньої цінової траєкторії. У літературі з forecasting електроенергії особливо релевантними для цього проєкту є два напрями: декомпозиційні моделі на кшталт NBEATSx і attention-based моделі на кшталт Temporal Fusion Transformer (TFT).

NBEATSx, згідно з Olivares et al., сильний тим, що поєднує часову декомпозицію з екзогенними змінними. Для ринку електроенергії це корисно, бо ціни мають денні та тижневі патерни, а також реагують на зовнішні чинники. Сильна сторона NBEATSx полягає в якості представлення часової форми ціни. Обмеження таке саме, як і для інших forecasting-моделей: кращий forecast ще не гарантує кращого економічного рішення, якщо downstream objective живе окремо.

TFT є важливим кандидатом для подальшого розвитку, оскільки працює з множиною ознак, attention-механізмом і probabilistic forecasting. Для волатильних ринків це особливо цінно, оскільки дозволяє переходити від single-point forecast до інтервальної та attribution-aware оцінки. У контексті диплома TFT логічно виступає як сильніший forecast layer для майбутнього Target Strategy.

Додатково важливо, що оригінальна робота Lim et al. про Temporal Fusion Transformers підкреслює не лише якість multi-horizon forecasting, а й інтерпретованість через variable selection та attention-based attribution. Для цього проєкту це суттєво, оскільки фінальна версія повинна не лише прогнозувати, а й пояснювати оператору, чому саме прогноз набув певної форми.

Окремо слід розмежувати weather-aware forecasting і dashboard-level weather explanation. У поточному MVP операторський `weather_bias` є пояснювальним read model: він показує, як хмарність, опади, вологість, температурне відхилення, ефективна сонячна радіація та вітер можуть бути асоційовані з uplift у ціні. Це не є причинною моделлю ціноутворення і не є входом у LP-dispatch. Таке обмеження узгоджується з Lago et al., які підкреслюють потребу у строгих benchmark-порівняннях для electricity price forecasting. Натомість NBEATSx, TFT і TimeXer підтримують правильний цільовий напрям: weather та інші exogenous variables повинні входити в forecast layer, а вже валідований прогноз ціни має передаватися в LP.

Оновлений Week 4 source refresh додає до цієї логіки ще два корисні орієнтири. PriceFM показує, що electricity price forecasting у Європі дедалі більше рухається в бік domain-specific foundation models, які враховують cross-region залежності, екзогенні фактори та probabilistic output. THieF, зі свого боку, підкреслює, що hourly DAM forecast не обов'язково має розглядатися ізольовано: узгодження прогнозів на рівні hourly products, block products і baseload може покращувати практичну якість прогнозування. Для цього диплома обидва джерела є аргументами на користь майбутнього forecast-layer розвитку, але не підставою змінювати поточний MVP або робити SOTA claim без decision-value benchmark.

Водночас поточний Level 1 MVP навмисно не стартує з нейромережевого forecast. Він використовує Strict Similar-Day Rule, тобто простий deployable baseline forecast. Це рішення обґрунтоване не слабкістю амбіції, а правильною постановкою експерименту: перший baseline має перевіряти архітектуру системи, а не приховувати її слабкі місця за складністю моделі. Таким чином, state of the art forecasting у цій роботі відіграє роль цільового орієнтира, а не стартового deliverable.

## 2.5. Predict-then-Optimize, regret і перехід до Decision-Focused Learning

Класичний Predict-then-Optimize (PTO) підхід передбачає окремий етап навчання forecast-моделі і окремий етап оптимізації рішення. Для багатьох прикладних задач це сильний baseline. Однак робота Elmachtoub and Grigas показує, що мінімізація звичайної statistical forecast error не завжди узгоджується з якістю кінцевого рішення. Якщо система оптимізується для прибутку, то тренувальний сигнал має бути ближчим до decision quality, ніж до абстрактних статистичних метрик.

Саме тому в цій дипломній роботі важливим поняттям стає regret. У поточному проєкті regret розглядається як втрачений прибуток відносно офлайн oracle benchmark. Уже на рівні MVP regret потрібен не лише як дослідницька метрика, а як спосіб пояснити, чому baseline є лише контрольним контуром, а не кінцевою відповіддю.

Yi et al. у predict-then-bid framework переносять цю логіку на задачу енергоарбітражу для BESS і показують, що для реального торгового середовища потрібно вбудовувати market response і clearing effect у decision loop. Сильна сторона DFL-підходу полягає в прямому зв’язку між прогнозом і підсумковим фінансовим результатом. Головне обмеження — складність навчання через недиференційованість клірингу, високу ціну експериментів і необхідність чіткого розділення симульованих та observed результатів.
Водночас важливо зафіксувати конкретне обмеження роботи Yi et al. (2025) щодо фізичного шару. Автори використовують спрощену лінійну модель деградації, де degradation cost є фіксованим штрафом за throughput без урахування нелінійних фізичних ефектів: зростання SEI-шару, асиметрія глибини розряду, capacity fade залежно від C-rate і температури. Для математичної елегантності DFL-контуру це виправдане спрощення, оскільки зберігає диференційованість objective function через `cvxpylayers`. Але для реальних BESS із глибоким циклуванням і багаторічним горизонтом це спрощення може накопичувати систематичну похибку в оцінці true asset cost — саме той ризик, який описують Hesse et al. та Maheshwari et al. Цей диплом планує зробити крок уперед у цьому напрямі: Silver Layer поточного MVP підтримує throughput-based degradation proxy з capex-based anchor, що вже відтворює economic wear-per-cycle реалістичніше, ніж довільна константа. Цільова версія передбачає розширення до cycle-depth-aware або temperature-aware degradation parametrization, обґрунтованої джерелами з фізичного моделювання.
Технічно важливим підґрунтям для такого переходу є роботи з differentiable optimization, зокрема Differentiable Convex Optimization Layers. Вони показують, що convex optimization problems можна вбудовувати як differentiable layers у ширший learning pipeline. Для цього диплома це важливо не як прямий готовий модуль для ринку, а як доказ того, що перехід від LP-baseline до більш тісно інтегрованого learning-and-optimization contour має реалістичну математичну і програмну основу.

Для дипломного проєкту з цього випливає практичний висновок: DFL є цільовою архітектурою, але не стартовим MVP. Спочатку потрібно побудувати стабільний LP-baseline, зафіксувати market and safety contracts, налагодити regret logging, а вже після цього переходити до differentiable clearing або surrogate-based навчання. Така послідовність мінімізує ризик того, що будь-яка помилка у результаті буде неоднозначно пояснюватися одночасно слабким кодом, нестабільними даними та ще не перевіреним ML-підходом.

Цю межу підсилює сучасна дискусія про evaluation leakage у time-series foundation models. Meyer et al. описують ризик завищеної оцінки якості, коли benchmark не контролює sample overlap або temporal overlap між тренувальним і тестовим матеріалом. Для цієї роботи це означає, що навіть потенційно сильніші foundation/adaptation підходи, такі як PriceFM, TFMAdapter або Reverso, мають спочатку пройти той самий rolling-origin protocol: кожний anchor бачить тільки минуле, LP отримує forecast candidate, а висновок робиться за realized decision value та oracle regret. Саме тому наступний крок після Week 3 - regret-weighted calibration і selector evidence, а не оголошення повного DFL.

## 2.6. Деградація батареї, Equivalent Full Cycle і Digital Twin

Енергоарбітраж не можна коректно оцінювати лише через gross market value. Надмірна кількість циклів заряду та розряду збільшує знос батареї і змінює реальну економіку рішення. Отже, коректна стратегія повинна враховувати не лише дохід, а й degradation cost.

Робота Grimaldi et al. про прибутковість арбітражу з урахуванням dynamic efficiency та degradation підтримує саме цю логіку: штраф за деградацію потрібно включати безпосередньо в objective function. Це запобігає ситуації, коли стратегія демонструє високий короткостроковий прибуток ціною неприйнятного довгострокового зносу активу. Водночас для академічної коректності важливо не змішувати загальний висновок статті з конкретною параметризацією штрафу за цикл у поточному MVP: demo-значення в репозиторії коректніше описувати як public-source capex-throughput proxy для preview model, а не як універсальну константу зі статті.

Саме тут нові джерела суттєво посилюють логіку диплома. Огляд Vykhodtsev et al. дозволяє прямо показати, що між battery physics і операційною оптимізацією існує структурний розрив: повні електрохімічні моделі занадто важкі для повсякденного dispatch-планування, а надто грубі лінійні моделі ризикують спотворити економіку активу. Hesse et al. і Maheshwari et al. демонструють проміжний шлях: ageing-aware dispatch, де деградація враховується всередині оптимізації, але ціною складнішого MILP або нелінійних/апроксимованих постановок.

У поточному MVP ця логіка реалізована через throughput-based degradation penalty в UAH. Коректніше описувати її як feasibility-and-economics preview model, а не як physical battery simulation. Вона навмисно спрощує батарею до керованого погодинного state-transition контуру: враховуються SOC-обмеження, ліміт потужності, спрощений ККД та економічно інтерпретований degradation penalty, прив'язаний до енергетичного throughput. Для current demo battery `10 MWh` proxy параметризується як `210 USD/kWh` (видимий capex anchor у Grimaldi et al.) + `15-year lifetime` і `~1 cycle/day` (NREL ATB) + `43.9129 UAH/USD` (НБУ, `04.05.2026`), що дає `16,843.3 UAH/cycle` або `842.2 UAH/MWh throughput`. Це ще не повний digital twin і не фізично найглибша модель батареї, але вже достатній крок до економічно осмисленого baseline. Важливо, що деградація тут не є post-hoc KPI. Вона включена в decision objective.

Водночас повна модель деградації значно складніша за лінійний штраф. Цільова research-версія проєкту орієнтується на розширення цього шару через більш фізично правдоподібні моделі та Digital Twin. Саме тут з’являється місце для P2D-моделей, SEI-аналізу та точнішого розрахунку SOH/SOC. Але для раннього MVP простіший Equivalent Full Cycle-based penalty є виправданим компромісом: він достатньо простий для LP-baseline і водночас захищає систему від економічно хибних рекомендацій.

## 2.7. Оркестрація, MLOps і operator-facing surfaces як частина інженерної новизни

Окремий блок літератури і практики стосується не самої стратегії, а інфраструктури, без якої неможливо отримати відтворюваний інженерний результат. Для дипломної роботи цього типу важливо не лише запропонувати модель, а й показати pipeline, який відтворює дані, проміжні артефакти, запуск baseline та фіксацію метрик.

У поточному проєкті таку роль виконує Dagster як orchestrator of software-defined assets, MLflow як механізм фіксації експериментальних результатів і regret-metrics, FastAPI як control-plane layer і Nuxt dashboard як operator-facing explanation surface. Цей блок не є основним предметом наукової новизни, але він критичний для того, щоб диплом не перетворився на набір несинхронізованих ноутбуків або ad-hoc скриптів.

Особливо важливо, що demo-stage у цьому проєкті вже має окрему operator surface. Це означає, що між backend-логікою і presentation layer проведено явну межу: dashboard показує read models і recommendation preview, але не видає себе за market execution engine. Така межа є не лише UX-рішенням, а й методологічним guardrail для диплома.

## 2.8. Поточний MVP, demo-stage і фінальна planned version

Станом на поточний етап проєкт має три логічні рівні, які не можна змішувати в одному наративі.

Перший рівень — це реалізований MVP baseline. Він включає Bronze -> Gold asset chain, strict data contracts, LP-baseline для погодинного DAM, throughput-based degradation penalty, regret logging та projected battery state preview. Це той рівень, який уже можна перевіряти, тестувати та демонструвати як стабільний інженерний контур.

Другий рівень — demo-stage operator surface. Він додає FastAPI read models, backend-owned operator status, same-origin dashboard proxy та візуальні поверхні для weather control і baseline recommendation preview. Його завдання не в тому, щоб симулювати повний ринок, а в тому, щоб дати керівнику і майбутньому читачу диплома зрозумілий інтерфейс, через який видно зв’язок між даними, обмеженнями та recommendation logic.

Окремо нові джерела корисні для правильного ринкового позиціювання фінальної версії. Hu et al. показують, що в більшості європейських ринків чистого енергоарбітражу часто недостатньо для привабливої окупності BESS, а значну роль відіграє revenue stacking через поєднання кількох сервісів і ринків. Це добре узгоджується з поточною архітектурною логікою диплома: DAM-only baseline є свідомим MVP-обмеженням, а multi-venue intelligence належить до наступної фази. Li et al. додатково підсилюють цей висновок уже на рівні конкретної постановки storage bidding, де temporal-aware DRL працює для одночасної участі в energy та contingency reserve markets. Для цього проєкту це означає: DRL/DFL не варто передчасно видавати за поточний deliverable, але саме вони мають сенс як напрям розвитку після стабілізації baseline і demo-stage.

Третій рівень — фінальна planned version. Вона передбачає перехід до predict-then-bid DFL architecture: сильніший forecasting layer на базі NBEATSx/TFT, differentiable or surrogate clearing, learned strategy layer на кшталт Decision Transformer, багаторинкове розширення, глибший digital twin і стримувальний Pydantic Gatekeeper між market intent та physical execution. Цей рівень є цільовим дослідницьким внеском, але він не повинен бути виданий за поточний deliverable.

Зокрема, Decision Transformer цікавий для цього проєкту тим, що формулює decision-making як conditional sequence modeling, де політика генерується з урахуванням desired return, минулих станів і дій. Для енергоарбітражу це привабливо, бо дозволяє описати послідовність BUY/SELL/HOLD або bid-oriented decisions як траєкторію, оптимізовану під економічний результат, а не лише під локальну помилку прогнозу.

Саме таке трирівневе розрізнення робить диплом логічним: MVP доводить інженерну здійсненність, demo-stage доводить пояснюваність і готовність до supervisor review, а final planned version формує напрям наукової новизни.

## 2.9. Роль MCP та agent tooling у цьому дипломі

Окремо варто зазначити, що в поточному репозиторії використовується MCP- та agent-based tooling як допоміжний інструментарій для research workflow, збору джерел, структурування документації й пришвидшення інженерної ітерації. Проте MCP не є предметом основної наукової новизни цього диплома.

Для коректного академічного позиціювання важливо підкреслити: дипломна робота присвячена автономному енергоарбітражу BESS, а MCP/agent tooling виступає лише supporting layer для продуктивнішої розробки та документування. Тобто в тексті диплома доцільно згадувати MCP лише як інженерний workflow-інструмент, а не як центральний результат роботи.

## 2.10. Як огляд літератури обґрунтовує вибір архітектури диплома

Сукупність джерел показує, що окремі частини задачі вже добре досліджені, але рідко зводяться в єдиний узгоджений контур, де прогноз, ринкова логіка, degradation-aware objective, operator surface і відтворювана оркестрація працюють разом. Класичний PTO підходить як сильний baseline, але не розв’язує проблему decision alignment. Сильні forecasting-моделі дають кращу оцінку цін, але самі по собі не оптимізують фінансовий результат. DFL-підходи потенційно зменшують розрив між prediction quality і decision quality, але різко підвищують інженерну складність. Digital twin підсилює фізичну правдоподібність, але збільшує обсяг моделювання і вимоги до даних.

Саме ця прогалина визначає логіку даного диплома. На першому етапі будується стабільний, перевірюваний LP-baseline для погодинного DAM у UAH. На другому етапі цей baseline стає demo-ready control surface для supervisor review. На третьому етапі baseline слугує контрольною групою для переходу до DFL-стратегії, яка має оптимізувати regret без порушення ринкових контрактів і safety constraints. Таким чином, проєкт не намагається одразу розв’язати всю задачу одним великим стрибком, а формує послідовну дослідницько-інженерну траєкторію, придатну як для MVP, так і для фінальної пояснювальної записки.

## 2.11. Що означає "повністю завершити LP baseline" у межах цього диплома

Огляд літератури для цього проєкту важливий не лише тим, що обґрунтовує майбутній DFL-напрям. Він також пояснює, що саме означає "довести baseline до завершеного стану". Для цього диплома завершений LP baseline не дорівнює фінальній target architecture. Це інше поняття: stable control contour, який уже не змінює щотижня свої базові контракти і придатний як чесний benchmark для всіх наступних дослідницьких кроків.

Практично це означає завершення п'яти речей:

1. **Зафіксований Level 1 scope.** Baseline лишається DAM-only, hourly, UAH-native і спирається на strict similar-day forecast як детермінований forecast baseline, а не на неявно змішану multi-market logic.
2. **Єдині contracts і economics.** Однакові `BatteryPhysicalMetrics`, throughput-based degradation proxy, Pydantic safety semantics і market naming мають використовуватися в assets, API, dashboard та документації без локального drift.
3. **Замкнений preview path.** Шлях `forecast -> LP solve -> projected SOC -> validated preview -> operator read model -> regret logging` має бути відтворюваним і технічно стабільним як у локальній перевірці, так і в supervisor demo.
4. **Перевірюваний validation package.** Для baseline мають існувати focused tests, build/materialization checks і достатній набір артефактів, який доводить, що мова йде про інженерний контур, а не про разовий demo-script.
5. **Freeze як control group.** Після стабілізації baseline треба перестати постійно змінювати його objective і contracts, щоб він справді став контрольною групою для stronger forecast layer, DFL, richer battery modelling і multi-market extensions.

Після цього перехід далі теж має бути поетапним. Спочатку baseline використовується як незмінний benchmark. Далі поверх нього можна ставити сильніший forecast layer на базі NBEATSx або TFT, добудовувати bid/clearing semantics і лише потім переходити до differentiable або surrogate-based DFL contour. Саме така послідовність випливає з розглянутих джерел: forecasting literature підказує, як посилити prediction layer; PTO/DFL literature підказує, як зблизити prediction quality і decision quality; battery-degradation literature підказує, як обережно нарощувати фізичну правдоподібність, не руйнуючи відтворюваність усього pipeline.

Таким чином, literature review обґрунтовує не лише кінцеву ціль диплома, а й сам спосіб руху до неї: спочатку завершений LP baseline, потім stronger learned components, потім comparison by regret and safety, а не навпаки.

## 2.13. Deep-research уточнення: empirical benchmark перед SOTA claims

Оновлений deep-research review підтверджує, що поточна архітектура диплома є сильною, але також фіксує головну академічну слабкість: значна частина історичного market/weather шару в поточному MVP може бути synthetic або demo-oriented. Тому твердження про ефективність для українського ринку не повинні спиратися лише на demo materialization або live-current overlay.

Для фінальної версії пояснювальної записки це означає зміну акценту. SOTA forecasting models, такі як NBEATSx і TFT, мають оцінюватися не лише за MAE/RMSE, а через rolling-origin strategy backtest: на кожному anchor використовується тільки минула інформація, модель прогнозує наступний горизонт, LP формує feasible schedule, а результат оцінюється за realized DAM prices, net UAH value, degradation penalty, throughput, EFC і oracle regret.

Таке уточнення не послаблює DFL-напрям. Навпаки, воно робить його більш захищеним: DFL pilot має порівнюватися з уже стабільним real-data benchmark, а не замінювати собою benchmark. Отже, дослідницька новизна диплома має будуватися у послідовності `real-data benchmark -> forecast decision-value comparison -> robustness -> DFL pilot`.

Week 4 source refresh фіксує цю послідовність як академічний guardrail. Нові джерела про electricity-price foundation models, temporal hierarchy forecasting і TSFM leakage не змінюють поточний implemented scope. Вони уточнюють, чому Week 4 calibration має бути оформлена як calibration/selector evidence: вона перевіряє, чи можуть NBEATSx/TFT-похідні candidates стати кориснішими за strict similar-day control після regret-aware correction, але ще не є ні live market strategy, ні full DFL.

Окремо варто зафіксувати European dataset bridge. ENTSO-E Transparency Platform, Open Power System Data, OPSD time series, Nord Pool Data Portal і Ember API є корисними як майбутні джерела для зовнішньої валідації, market-coupling context і порівняння з європейськими day-ahead даними. Але для поточного доказового шару вони не є implemented data sources. Week 3/DFL-panel evidence залишається українським: OREE DAM, tenant-aware Open-Meteo, UAH-native regret і той самий rolling-origin LP/oracle protocol. Таке розмежування важливе, щоб не змішати дослідницький roadmap з фактично матеріалізованими доказами.

## 2.14. Джерела, використані в поточній версії розділу

1. Yi et al. A Decision-Focused Predict-then-Bid Framework for Energy Storage Arbitrage. DOI: 10.48550/arXiv.2505.01551.
2. Olivares et al. Neural basis expansion analysis with exogenous variables: Forecasting electricity prices with NBEATSx. DOI: 10.1016/j.ijforecast.2022.03.001.
3. Jiang et al. Probabilistic electricity price forecasting based on penalized temporal fusion transformer. DOI: 10.1002/for.3084.
4. Elmachtoub and Grigas. Smart "Predict, then Optimize". DOI: 10.1287/mnsc.2020.3922.
5. Grimaldi et al. Profitability of energy arbitrage net profit for grid-scale battery energy storage considering dynamic efficiency and degradation using a linear, mixed-integer linear, and mixed-integer non-linear optimization approach. DOI: 10.1016/j.est.2024.112380.
6. Lim et al. Temporal Fusion Transformers for Interpretable Multi-horizon Time Series Forecasting. DOI: 10.48550/arXiv.1912.09363.
7. Chen et al. Decision Transformer: Reinforcement Learning via Sequence Modeling. DOI: 10.48550/arXiv.2106.01345.
8. Agrawal et al. Differentiable Convex Optimization Layers. DOI: 10.48550/arXiv.1910.12430.
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

Для першої submission-ready версії розділу локальна paper-base і source map вже зібрані в [docs/technical/papers/README.md](../../../technical/papers/README.md), а частина PDF-джерел збережена в [docs/technical/papers/2505.01551-predict-then-bid.pdf](../../../technical/papers/2505.01551-predict-then-bid.pdf), [docs/technical/papers/2104.05522-nbeatsx.pdf](../../../technical/papers/2104.05522-nbeatsx.pdf) та локальному архіві `docs/thesis/sources/`. Week 4 metadata-first intake зафіксовано в [docs/thesis/sources/week4-research-ingestion.md](../sources/week4-research-ingestion.md).
