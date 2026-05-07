# Autonomous Energy Arbitrage

Цей контекст описує домен автономного енергоарбітражу для BESS на ринках України 2026. Він фіксує канонічну мову для ринкових намірів, результатів клірингу та фізичного виконання, щоб модель, оркестрація і gatekeeper не змішували різні стадії одного процесу.

## Language

**Proposed Bid**:
Повна ринкова заявка для одного торгового інтервалу на одному **Market Venue**, яку модель формує до клірингу як впорядкований набір ціново-обсягових сегментів.
_Avoid_: ProposedTrade, trade, command, single segment

**Market Venue**:
Конкретний ринковий майданчик, правила якого визначають прайс-кеп, часовий інтервал і логіку валідації заявки.
_Avoid_: market portfolio, trading system, dispatch target

**Bid Curve**:
Структурне представлення **Proposed Bid** як монотонних ціново-обсягових сегментів для купівлі або продажу енергії.
_Avoid_: single trade, dispatch, action, interval command

**Bid Segment**:
Атомарна пара «ціна-обсяг-напрям» у складі **Bid Curve** для одного торгового інтервалу.
_Avoid_: whole bid, cleared trade, dispatch command

**Bid Quantity**:
Кількість у **Bid Segment**, виражена як середня потужність у MW протягом торгового інтервалу.
_Avoid_: interval energy block, direct MWh quantity, instantaneous spike

**Trading Interval Duration**:
Тривалість торгового інтервалу, яка перетворює **Bid Quantity** з MW у MWh для settlement, regret та feasibility-перевірок.
_Avoid_: optional metadata, implicit guess, market-agnostic duration

**Bid Gatekeeper**:
Перший рівень контролю, що блокує ринкові заявки, які батарея не зможе фізично виконати на момент клірингу.
_Avoid_: dispatch gatekeeper, runtime safety check

**Cleared Trade**:
Підтверджена алокація для конкретної BESS, яку агрегатор повертає після портфельного клірингу та внутрішнього розподілу обсягу.
_Avoid_: bid, dispatch, executed command, portfolio trade

**Cleared Segment Allocation**:
Детальний результат клірингу для окремого сегмента заявки, що фіксує запропонований і фактично прийнятий обсяг у межах одного інтервалу.
_Avoid_: whole trade, dispatch command, aggregated regret

**Cleared Trade Provenance**:
Позначка походження в **Cleared Trade**, яка вказує, чи результат отримано з диференційованої симуляції (`simulated`), чи з фактичного settlement від агрегатора (`observed`).
_Avoid_: separate domain types, hidden source, implicit mode

**Uniform Settlement Price**:
Єдина ринкова ціна клірингу для одного venue та інтервалу, яка застосовується до всіх акцептованих сегментів у межах **Cleared Trade**.
_Avoid_: per-segment settlement price, hidden pricing rule, blended average price

**Interval Degradation Penalty**:
Частка вартості деградації батареї, яка нараховується на конкретний торговий інтервал пропорційно енергетичному throughput активу.
_Avoid_: post-hoc KPI only, non-differentiable wear cost, fixed trade fee

**Equivalent Full Cycle (EFC)**:
Міра деградаційного навантаження, що визначається як сумарний абсолютний throughput заряду і розряду, поділений на подвійну номінальну ємність батареї.
_Avoid_: raw cycle count, discharge-only cycle, opaque wear score

**Baseline Strategy**:
Детермінований LP-алгоритм арбітражу, який слугує гарантованим MVP першого рівня та контрольною групою для порівняння з навчуваними стратегіями.
_Avoid_: provisional hack, optional fallback, neural policy

**Target Strategy**:
Навчувана DFL-стратегія, яка замінює baseline після побудови стабільного LP-контуру і оптимізує regret через диференційований market clearing.
_Avoid_: initial MVP, ad-hoc heuristic, pure LP solver

**Neural Forecast Silver Layer**:
Дослідницький Silver-шар прогнозування, який створює model-ready часові фічі та SOTA-inspired прогнози цін через NBEATSx-style і TFT-style моделі. Він існує поруч із **Level 1 Naive Forecast** і не замінює production baseline, доки не пройде regret-aware порівняння.
_Avoid_: baseline replacement, dashboard contract, market execution, unverified production forecast

**NBEATSx Forecast**:
Прогноз цін DAM, побудований на NBEATSx-style декомпозиції історичних цін із використанням екзогенних календарних і weather-aware ознак. У межах диплома це research forecast candidate для порівняння з **Level 1 Naive Forecast**.
_Avoid_: embeddings, dispatch policy, oracle forecast, guaranteed SOTA reproduction

**TFT Forecast**:
Прогноз цін DAM, побудований на TFT-style multi-horizon model із явними вагами вибору ознак для пояснення впливу календарних, weather-aware та lagged-price сигналів. У межах диплома це interpretable research forecast candidate, а не самостійна market strategy.
_Avoid_: opaque transformer output, dispatch command, market bid, dashboard-only feature

**Forecast Experiment Run**:
Відтворюваний MLflow/Dagster запуск forecast-кандидата на фіксованому Silver data slice, який фіксує метрики, маніфест і прогнозні рядки для порівняння.
_Avoid_: production deployment, dispatch validation, one-off notebook result

**Registered Forecast Candidate**:
MLflow Model Registry версія frozen forecast artifact для NBEATSx/TFT comparison, яка відтворює збережені прогнозні рядки, але не є **Target Strategy** або **Dispatch Command**.
_Avoid_: production policy, market execution model, retrainable SOTA claim

**Forecast Strategy Evaluation**:
Gold-шар порівняння, у якому **Level 1 Naive Forecast**, **NBEATSx Forecast** і **TFT Forecast** проходять через однаковий LP-контур, після чого їхні рішення оцінюються проти **Oracle Benchmark** за UAH value, degradation penalty і regret.
_Avoid_: production bidding, direct dispatch, forecast-only metric, Proposed Bid generation

**Real-Data Research Benchmark**:
Thesis-grade evaluation dataset and protocol built from observed Ukraine DAM history, timestamp-aligned weather, tenant battery assumptions, explicit provenance, effective-dated market constraints, FX assumptions, and market participation costs. It is the required empirical base before making strong claims about NBEATSx/TFT, DFL, or Ukraine market performance.
_Avoid_: synthetic demo history, live-current overlay only, architecture demo, SOTA claim without backtest

**Research Defense Dashboard**:
Thesis-facing dashboard mode that explains benchmark evidence, model claim boundaries, regret diagnostics, and research readiness without presenting simulated or pilot outputs as operational trading decisions.
_Avoid_: operator product dashboard, live trading console, production control plane, marketing dashboard

**Operator Product Dashboard**:
Operator-facing product demo surface that shows selected market, battery, telemetry, and planning metrics as operational status while preserving backend claim boundaries around simulated or research-only outputs.
_Avoid_: thesis-only evidence board, raw research notebook, unrestricted trading console, oracle dashboard

**Operator Decision Evidence Panel**:
Підрозділ **Operator Product Dashboard**, який показує live-read-model порівняння фізичного SOC, planning SOC, regret benchmark, forecast-dispatch diagnostics і grid/weather context для операторського рішення. Це evidence surface, а не генератор **Proposed Bid** чи **Dispatch Command**.
_Avoid_: live trading panel, oracle control panel, hidden model promotion, research-only defense page

**Operator Decision Readiness Strip**:
Компактний preflight-блок усередині **Operator Decision Evidence Panel**, який показує readiness фізичного SOC, planning SOC, grid context і source freshness перед тим, як оператор розглядатиме LP schedule як candidate dispatch review.
_Avoid_: market approval, bid validation, dispatch authorization, replacing **Bid Gatekeeper**

**Rolling-Origin Strategy Backtest**:
Evaluation protocol where each anchor uses only past data to build features/forecasts, solves the strategy for the next horizon, executes/scored only feasible decisions against later realized prices, and compares the result with **Oracle Benchmark**.
_Avoid_: hindsight training, one-off demo materialization, random train/test split across time, forecast-only leaderboard

**Regret-Weighted Forecast Calibration**:
DFL-inspired diagnostic layer that estimates a price-bias correction from prior-anchor forecast errors weighted by downstream regret, then re-evaluates corrected forecasts through the same strict LP/oracle scorer. It is not **Target Strategy** and not full differentiable DFL.
_Avoid_: claiming trained DFL, replacing strict baseline, using future anchors, dashboard production default

**Horizon-Aware Regret-Weighted Forecast Calibration**:
DFL-inspired diagnostic layer that estimates one regret-weighted price-bias correction per forecast horizon step from prior anchors, then re-evaluates corrected NBEATSx/TFT forecasts through the same strict LP/oracle scorer. It can support a value-oriented DFL argument, but is still not **Full Differentiable DFL**.
_Avoid_: claiming end-to-end trained DFL, using future anchors, replacing **Strict Similar-Day Rule** as dashboard default without live validation

**Full Differentiable DFL**:
Future **Target Strategy** training loop where a forecast model is optimized through a relaxed differentiable storage or bidding layer, then evaluated with the strict LP/simulator. It requires a separate no-leakage training protocol and must not be claimed from bias-calibration results.
_Avoid_: regret-weighted post-hoc calibration, oracle dispatch imitation, unconstrained neural action, final evaluation on relaxed constraints

**DFL Training Example**:
Research-only row that packages one tenant/anchor/model decision instance for later DFL experiments: point-in-time forecast inputs, realized evaluation labels, LP dispatch vectors, oracle/regret values, degradation/throughput economics, provenance, and claim-boundary flags.
_Avoid_: live bid, production policy input, oracle forecast, dashboard contract

**Promotion Gate**:
Conservative research gate that blocks a forecast, calibration, selector, or DFL candidate from being described as improved control unless it beats the frozen **Strict Similar-Day Rule** on same-scope regret and safety evidence.
_Avoid_: automatic deployment, dashboard default switch, forecast-only leaderboard, weak model promotion

**Effective-Dated Market Constraint**:
Regulatory or market parameter whose value depends on the delivery/decision date, such as DAM/IDM/Balancing price caps, operator transaction tariffs, or fixed participation fees.
_Avoid_: timeless constant, hidden config, dashboard-only annotation, post-hoc correction

**Market Participation Cost**:
Explicit UAH-denominated cost of participating in a market venue, including per-MWh operator tariffs, fixed software fees when relevant, taxes/VAT treatment if modeled, and other settlement-side costs separate from **Interval Degradation Penalty**.
_Avoid_: degradation penalty, spread capture, ignored fee, implicit transaction cost

**M3DT-Inspired Research Strategy**:
Дослідницька multi-client стратегія на основі ідеї Mixture-of-Expert Decision Transformer, де різні симульовані BESS-клієнти трактуються як пов'язані offline RL tasks. У межах диплома це SOTA-inspired experiment після стабілізації baseline, а не повне відтворення M3DT-паперу і не production market execution engine.
_Avoid_: current MVP, full M3DT reproduction, guaranteed SOTA result, dispatch engine

**Baseline Forecast**:
Простий deployable прогноз цін, доступний у момент прийняття рішення і достатній для запуску **Baseline Strategy** end-to-end у реальному часі.
_Avoid_: hindsight price, offline oracle, research-only upper bound

**Level 1 Naive Forecast**:
Сезонний slot-based naive прогноз для **Baseline Strategy**, який оцінює кожен майбутній слот за історично аналогічним часовим слотом замість повторення останньої ціни.
_Avoid_: flat persistence, hindsight oracle, constant-price horizon

**Price Shape Preservation**:
Вимога до baseline-прогнозу зберігати часову форму цінової кривої, щоб LP-арбітраж бачив спреди між дешевими і дорогими слотами.
_Avoid_: spread-free forecast, flat price vector, shape collapse

**Strict Similar-Day Rule**:
Канонічне правило **Level 1 Naive Forecast**, за яким кожен майбутній слот копіює один історично аналогічний слот без усереднення: $P_t = P_{t-24h}$ для Tue-Fri і $P_t = P_{t-168h}$ для Mon, Sat, Sun.
_Avoid_: rolling average, smoothed seasonal naive, tuned window baseline

**Baseline Rolling Horizon**:
Політика виконання **Baseline Strategy**, за якою LP-оптимізація регулярно перераховується на фіксований горизонт уперед, але до виконання передається лише перша команда поточного кроку.
_Avoid_: one-shot day plan, static schedule, full-horizon dispatch dump

**Level 1 Market Scope**:
Обсяг ринку для першого MVP, обмежений ринком на добу наперед (**DAM**) з погодинною резолюцією 60 хвилин.
_Avoid_: multi-venue baseline, 15-minute baseline, balancing-first MVP

**Sequential Complexity**:
Рівневий принцип розробки, за яким багаторинковість, 15-хвилинна резолюція і складніші стратегії додаються лише після стабілізації Level 1 baseline.
_Avoid_: all-at-once architecture, simultaneous venue expansion, premature stacking

**Canonical Currency**:
Канонічна грошова одиниця для цін, доходу, штрафів деградації, regret і settlement у всіх production-контрактах системи.
_Avoid_: mixed EUR/UAH accounting, display-only currency conversion, strategy-specific money units

**Economic Contract Consistency**:
Правило, за яким **Baseline Strategy** і **Target Strategy** використовують однакову грошову одиницю та сумісну економічну семантику objective, reward і settlement.
_Avoid_: Level 1 in EUR and Level 2 in UAH, incompatible reward scales, post-hoc currency remapping

**Oracle Benchmark**:
Офлайн perfect-foresight еталон, який використовує майбутні фактичні ціни лише для верхньої межі прибутку та аналізу regret.
_Avoid_: live forecast, deployable baseline, production strategy

**Dispatch Command**:
Фізична команда на заряд, розряд або утримання, яку система може передати обладнанню після ринкового результату та safety-перевірок.
_Avoid_: trade, bid, order

**Aggregator**:
Зовнішній ринковий учасник, який збирає **Proposed Bid** від багатьох BESS, формує портфельну заявку та повертає активу його підтверджену алокацію.
_Avoid_: local asset, market venue, inverter controller

**Settlement**:
Процес перетворення портфельного ринкового результату на підтверджену алокацію та фінансовий результат конкретної BESS.
_Avoid_: market venue, bid submission, dispatch

**Projected Battery State**:
Проміжний прогнозований стан BESS на початок цільового торгового інтервалу, який використовується для побудови консервативного конверта можливостей.
_Avoid_: live telemetry, current state, dispatch snapshot, final bid constraint

**Feasibility-and-Economics Preview Model**:
Спрощене погодинне представлення батареї, яке оцінює feasible power, projected SOC і деградаційний penalty для baseline-планування та operator preview без претензії на повний цифровий двійник.
_Avoid_: full digital twin, electrochemical battery simulation, hidden degradation function

**Bid Feasibility Envelope**:
Набір консервативних меж енергії, потужності та SOC для конкретного торгового інтервалу, що визначає безпечну зону формування **Proposed Bid**.
_Avoid_: point forecast, dispatch telemetry, single SOC estimate

**Bid Deadband**:
Ціновий розрив між BUY- і SELL-кривими в одному **Proposed Bid**, який гарантує, що одна ринкова ціна не активує обидва напрями одночасно.
_Avoid_: overlapping bid sides, ambiguous direction, self-crossing curve

**Battery Telemetry**:
Сирий або майже реальний стан батареї з інвертора та суміжних сенсорів, який використовується безпосередньо перед фізичним виконанням.
_Avoid_: forecast state, bid envelope, market intent

**Venue-Aware Selection**:
Процес, у якому стратегія оцінює кілька ринкових майданчиків, але для одного фізичного інтервалу активу обирає лише один виконуваний результат.
_Avoid_: concurrent multi-venue stacking, portfolio reconciliation

**Result Isolation**:
Правило MVP, за яким один торговий інтервал для однієї BESS породжує не більше одного **Cleared Trade** і не більше одного **Dispatch Command**.
_Avoid_: concurrent commitments, multi-dispatch interval

**No Bid**:
Стан відсутності ринкової заявки для конкретного venue та інтервалу після провалу валідації на стадії **Proposed Bid**.
_Avoid_: market hold, rejected dispatch, partial trade

**HOLD**:
Фізична команда нульової потужності, яку система видає для захисту обладнання після провалу валідації на стадії **Dispatch Command**.
_Avoid_: market bid, no bid, cleared trade

## Relationships

- A **Proposed Bid** produces zero or one **Cleared Trade**
- A **Proposed Bid** is represented as a **Bid Curve**
- A **Proposed Bid** belongs to exactly one **Market Venue**
- An **Aggregator** receives **Proposed Bid** objects from multiple BESS assets
- A **Bid Curve** contains one or more **Bid Segment** entries
- A **Bid Segment** uses **Bid Quantity** in MW
- **Trading Interval Duration** converts **Bid Quantity** in MW into interval energy in MWh
- A **Proposed Bid** stores its own **Trading Interval Duration** and validates it against **Market Venue** rules
- A two-sided **Proposed Bid** enforces **Bid Deadband** so that `max(BUY price) < min(SELL price)`
- A **Cleared Trade** stores its own historical **Trading Interval Duration** for replay, audit, and regret calculation
- A **Cleared Trade** contains one or more **Cleared Segment Allocation** entries
- A **Cleared Trade** is produced by **Settlement** after portfolio clearing and allocation
- A **Cleared Trade** carries **Cleared Trade Provenance** to distinguish training surrogates from settlement truth
- A **Cleared Trade** with `simulated` provenance is used for gradient-based training in the differentiable clearing layer
- A **Cleared Trade** with `observed` provenance is used for evaluation, settlement accounting, and digital-twin updates
- A **Cleared Trade** uses one **Uniform Settlement Price** for all accepted segment allocations in its venue and interval
- **Equivalent Full Cycle (EFC)** is computed as $(|E_{charge}| + |E_{discharge}|) / (2 \cdot Capacity)$
- **Interval Degradation Penalty** is computed from **Equivalent Full Cycle (EFC)** and contributes directly to the DFL objective
- **Baseline Strategy** provides the first guaranteed end-to-end MVP and the control group for later comparisons
- **Target Strategy** is introduced after the **Baseline Strategy** is working and reuses the same market and safety contracts
- **Neural Forecast Silver Layer** produces **NBEATSx Forecast** and **TFT Forecast** candidates beside the **Level 1 Naive Forecast**
- **NBEATSx Forecast** and **TFT Forecast** can feed future **Target Strategy** experiments after evaluation against the baseline
- A **Forecast Experiment Run** can produce one **Registered Forecast Candidate** in MLflow Model Registry
- A **Registered Forecast Candidate** is evidence for research comparison, not a **Target Strategy** or **Dispatch Command**
- **Forecast Strategy Evaluation** connects the **Neural Forecast Silver Layer** to Gold by routing forecast candidates through the same **Baseline Strategy** LP and scoring the resulting decisions against the **Oracle Benchmark**
- **Forecast Strategy Evaluation** is evaluation evidence, not **Proposed Bid** generation or production market submission
- **Real-Data Research Benchmark** upgrades **Forecast Strategy Evaluation** from demo evidence to thesis evidence by replacing synthetic history with observed market/weather inputs and explicit cost/constraint assumptions
- A **Research Defense Dashboard** presents **Real-Data Research Benchmark**, **Forecast Strategy Evaluation**, and DFL/DT readiness evidence without producing **Proposed Bid**, **Cleared Trade**, or **Dispatch Command** outputs
- An **Operator Product Dashboard** may reuse selected **Research Defense Dashboard** metrics, but must present them as operational status or research diagnostics according to provenance
- An **Operator Decision Evidence Panel** inside the **Operator Product Dashboard** may show regret and forecast diagnostics, but those remain read-model evidence until a **Bid Gatekeeper** produces a valid **Proposed Bid** and later dispatch safety checks produce a **Dispatch Command**
- **Rolling-Origin Strategy Backtest** is the canonical experiment design for comparing **Level 1 Naive Forecast**, **NBEATSx Forecast**, **TFT Forecast**, and later **Target Strategy**
- **Regret-Weighted Forecast Calibration** consumes **Real-Data Research Benchmark** outputs and produces corrected forecast candidates for another **Forecast Strategy Evaluation**
- **Regret-Weighted Forecast Calibration** is a diagnostic predecessor to **Full Differentiable DFL**, not a replacement for **Target Strategy**
- **Horizon-Aware Regret-Weighted Forecast Calibration** deepens **Regret-Weighted Forecast Calibration** by preserving forecast-horizon structure before LP scoring
- **Horizon-Aware Regret-Weighted Forecast Calibration** may beat the strict control on one aggregate metric, but remains a research diagnostic until live-available inputs and strict validation prove it is stable
- **Full Differentiable DFL** must be trained with only pre-anchor information and must still be scored through strict LP/simulator evaluation
- **Effective-Dated Market Constraint** constrains **Market Venue** validation and benchmark scoring for the relevant delivery date
- **Market Participation Cost** contributes to net UAH decision value alongside **Interval Degradation Penalty**
- **M3DT-Inspired Research Strategy** is a candidate **Target Strategy** evaluated on simulated client tasks after the baseline is stable
- **Baseline Forecast** is the live-available forecast input used by **Baseline Strategy**
- **Level 1 Naive Forecast** is the canonical first implementation of **Baseline Forecast**
- **Price Shape Preservation** is required so **Baseline Strategy** can detect arbitrage spreads
- **Strict Similar-Day Rule** is the canonical rule used by **Level 1 Naive Forecast**
- **Baseline Rolling Horizon** repeatedly re-optimizes **Baseline Strategy** while executing only the first command each cycle
- **Level 1 Market Scope** limits **Baseline Strategy** to hourly **DAM** decisions
- **Sequential Complexity** defers IDM, balancing, and finer-grained intervals to later strategy levels
- **Canonical Currency** is UAH from Level 1 onward
- **Economic Contract Consistency** requires **Baseline Strategy** and **Target Strategy** to share the same UAH-native economic units
- **Oracle Benchmark** is offline-only and never used for live bidding or dispatch decisions
- A **Projected Battery State** is derived from **Battery Telemetry**, forecasts, and existing commitments
- **Feasibility-and-Economics Preview Model** uses **Projected Battery State** and **Interval Degradation Penalty** for baseline planning and operator-facing preview
- A **Bid Feasibility Envelope** is derived from **Projected Battery State**
- **Venue-Aware Selection** chooses at most one executable outcome for a BESS in a physical interval
- A **Cleared Trade** produces zero or one **Dispatch Command**
- **Result Isolation** constrains one BESS interval to at most one **Cleared Trade** and one **Dispatch Command**
- A **Bid Gatekeeper** validates a **Proposed Bid** before market submission
- A **Bid Gatekeeper** uses **Bid Feasibility Envelope** for feasibility checks
- A failed **Proposed Bid** validation produces **No Bid** for that venue and interval
- A **Dispatch Command** is validated against **Battery Telemetry** immediately before execution
- A failed **Dispatch Command** validation produces **HOLD**
- A **Dispatch Command** must remain within the battery's safe operating envelope

## Example dialogue

> **Dev:** "Якщо модель згенерувала **Proposed Bid** на купівлю, ми одразу заряджаємо батарею?"
> **Domain expert:** "Ні. Спочатку ринок або приймає, або відхиляє заявку. Лише **Cleared Trade** може породити **Dispatch Command**, і тільки після safety-перевірки."

> **Dev:** "Gold Layer повертає одну дію чи повну **Bid Curve**?"
> **Domain expert:** "Для ринку потрібна **Bid Curve**. Окремий сегмент може бути частиною **Proposed Bid**, але не замінює всю заявку."

> **Dev:** "Що є атомарною сутністю: **Proposed Bid** чи **Bid Segment**?"
> **Domain expert:** "Для ринку й Dagster атомарною бізнес-сутністю є **Proposed Bid** на інтервал. **Bid Segment** є лише складовим елементом цієї заявки."

> **Dev:** "Одна заявка може одночасно належати DAM і balancing?"
> **Domain expert:** "Ні. Один **Proposed Bid** належить рівно одному **Market Venue**. Портфельна стратегія може породжувати багато заявок, але не змішує кілька venue в одному об'єкті."

> **Dev:** "Для BESS < 5 МВт хто взаємодіє з ринком безпосередньо?"
> **Domain expert:** "Не локальний актив. **Aggregator** подає портфельну заявку на ринок, а окрема BESS отримує лише свою підтверджену **Cleared Trade** через **Settlement**."

> **Dev:** "Чи може одна батарея мати два одночасні виконувані ринкові результати в одному інтервалі?"
> **Domain expert:** "Не в межах цього MVP. **Venue-Aware Selection** оцінює кілька venue, але **Result Isolation** дозволяє лише один **Cleared Trade** і один **Dispatch Command** на інтервал."

> **Dev:** "На який стан дивиться **Bid Gatekeeper**: поточний чи прогнозований?"
> **Domain expert:** "На **Projected Battery State** для цільового інтервалу. **Battery Telemetry** лишається джерелом істини лише для фінального dispatch safety check."

> **Dev:** "Що саме отримує **Bid Gatekeeper**: точковий прогноз чи консервативний конверт?"
> **Domain expert:** "Не точковий прогноз. Він отримує **Bid Feasibility Envelope**, побудований з **Projected Battery State**, щоб гарантувати виконуваність заявки навіть за несприятливого сценарію."

> **Dev:** "Що означає quantity у **Bid Segment**: енергію чи потужність?"
> **Domain expert:** "Це **Bid Quantity** у MW як середня потужність на інтервал. Енергія для перевірок і settlement виводиться через **Trading Interval Duration**."

> **Dev:** "Чи можуть BUY- і SELL-сегменти в одній заявці перекриватися за ціною?"
> **Domain expert:** "Ні. Якщо **Proposed Bid** містить обидві сторони, він має дотримуватися **Bid Deadband**, щоб одна ринкова ціна не активувала одночасно заряд і розряд."

> **Dev:** "Duration треба щоразу відновлювати з календаря venue?"
> **Domain expert:** "Ні. **Proposed Bid** і **Cleared Trade** зберігають власний історичний **Trading Interval Duration**, але Gatekeeper додатково перевіряє його проти правил відповідного **Market Venue**."

> **Dev:** "Після клірингу ми зберігаємо лише усереднений результат чи деталізацію по сегментах?"
> **Domain expert:** "Зберігаємо **Cleared Trade** як контейнер інтервалу з переліком **Cleared Segment Allocation**. Агреговані метрики виводяться з цієї деталізації, а не замінюють її."

> **Dev:** "Що є джерелом істини для навчання і оцінки: один і той самий результат клірингу?"
> **Domain expert:** "Ні. Це одна канонічна сутність **Cleared Trade**, але з різним **Cleared Trade Provenance**: `simulated` для differentiable training і `observed` для evaluation та settlement."

> **Dev:** "За якою ціною сетляться акцептовані сегменти: кожен за своєю чи за спільною?"
> **Domain expert:** "У межах MVP діє **Uniform Settlement Price**: всі акцептовані сегменти одного venue та інтервалу сетляться за єдиною ринковою ціною клірингу."

> **Dev:** "Як враховуємо деградацію в objective: як KPI чи як penalty?"
> **Domain expert:** "Як **Interval Degradation Penalty**, прямо всередині objective. Для цього використовуємо **Equivalent Full Cycle (EFC)**, щоб симетрично штрафувати і заряд, і розряд у кожному інтервалі."

> **Dev:** "Що є обов'язковим MVP для перших тижнів: LP чи одразу learned policy?"
> **Domain expert:** "Спочатку обов'язково працює **Baseline Strategy** як LP-пайплайн. Лише після цього додається **Target Strategy** як навчуваний DFL-шар поверх уже стабільного контуру."

> **Dev:** "Чи можна використовувати hindsight prices в baseline?"
> **Domain expert:** "Ні. Для живого MVP використовується **Baseline Forecast**, доступний на момент рішення. **Oracle Benchmark** існує лише офлайн для верхньої межі прибутку та аналізу regret."

> **Dev:** "Який саме naive forecast є канонічним для Level 1: плоска персистенція чи seasonal slot-based?"
> **Domain expert:** "Для Level 1 використовується **Level 1 Naive Forecast** як seasonal slot-based / similar-day rule, тому що **Price Shape Preservation** важливе для роботи LP-baseline."

> **Dev:** "Чи baseline-прогноз має бути строгим копіюванням аналогічного дня чи згладженим average?"
> **Domain expert:** "Використовуємо **Strict Similar-Day Rule** без усереднення, щоб baseline лишався методологічно наївним і прозорим."

> **Dev:** "Як baseline виконує план: весь день одразу чи з переоптимізацією?"
> **Domain expert:** "Через **Baseline Rolling Horizon**: LP щоразу оптимізує весь горизонт, але система виконує лише першу команду і потім переобчислює план на нових даних."

> **Dev:** "Чи Level 1 baseline вже має вибирати між DAM, IDM і balancing?"
> **Domain expert:** "Ні. **Level 1 Market Scope** обмежений погодинним **DAM**, а багаторинковість додається пізніше за принципом **Sequential Complexity**."

## Flagged ambiguities

- "battery simulation" раніше використовувалося і для спрощеного hourly preview, і для повного physical model; resolved: поточний MVP використовує **Feasibility-and-Economics Preview Model**, а повний digital twin є planned work.

> **Dev:** "Що є fallback після провалу валідації: один універсальний HOLD?"
> **Domain expert:** "Ні. На ринковій стадії це **No Bid**, бо не існує HOLD-заявки. На фізичній стадії це **HOLD** як нульова команда інвертору."

## Flagged ambiguities

- "trade" раніше вживався одночасно для ринкового наміру, результату клірингу та фізичної команди — resolved: це три різні сутності: **Proposed Bid**, **Cleared Trade** і **Dispatch Command**.
- "action" раніше натякало на безпосередню команду інвертору — resolved: ринковий вихід Gold Layer є **Proposed Bid** або **Bid Curve**, а не **Dispatch Command**.
- "bid" раніше могло означати і всю заявку, і один сегмент — resolved: повна заявка на інтервал є **Proposed Bid**, а її елемент є **Bid Segment**.
- "market" раніше міг означати одночасно venue і портфель усіх venue — resolved: конкретний майданчик є **Market Venue**, а мульти-ринкова стратегія залишається композицією окремих **Proposed Bid**.
- "cleared trade" раніше звучало як прямий результат біржі для однієї батареї — resolved: для малих активів це підтверджена алокація, яку актив отримує від **Aggregator** через **Settlement**.
- "revenue stacking" могло означати одночасні фізичні зобов'язання на кількох venue — resolved: у межах MVP це **Venue-Aware Selection**, а не concurrent stacking; діє правило **Result Isolation**.
- "battery state" раніше змішував прогнозований і фактичний стани — resolved: для feasibility використовується **Bid Feasibility Envelope**, який будується з **Projected Battery State**, а для фінального safety check використовується **Battery Telemetry**.
- "bid volume" раніше змішував потужність і енергію — resolved: quantity сегмента є **Bid Quantity** у MW, а енергія обчислюється через **Trading Interval Duration**.
- "fallback" раніше змішував ринкову і фізичну реакцію системи — resolved: для невалідної заявки використовується **No Bid**, а для небезпечного виконання використовується **HOLD**.
- "duration" раніше виглядала як зовнішня властивість ринку — resolved: історичний **Trading Interval Duration** зберігається всередині **Proposed Bid** і **Cleared Trade**, але валідовується проти **Market Venue**.
- "cleared result" раніше міг означати лише усереднений обсяг і ціну — resolved: канонічний **Cleared Trade** зберігає деталізацію через **Cleared Segment Allocation**, а агрегати є похідними.
- "cleared trade" раніше сприймався як єдине джерело істини без джерела походження — resolved: це одна доменна сутність **Cleared Trade** з явним **Cleared Trade Provenance**, де `simulated` використовується для training, а `observed` для evaluation та settlement.
- "two-sided bid" раніше допускав перетин BUY і SELL за ціною — resolved: двостороння заявка має дотримуватися **Bid Deadband**, інакше вона невалідна.
- "settlement price" раніше міг виглядати як сегментний атрибут — resolved: у межах MVP використовується одна **Uniform Settlement Price** на весь **Cleared Trade**.
- "degradation cost" раніше міг жити лише в звітності — resolved: використовується **Interval Degradation Penalty**, обчислений через **Equivalent Full Cycle (EFC)**, і входить безпосередньо в objective.
- "MVP strategy" раніше змішувала baseline і цільову learned policy — resolved: перший гарантований рівень є **Baseline Strategy**, а нейромережева модель є окремою **Target Strategy** для наступного етапу.
- "embedding" раніше могло звучати як окремий representation-learning slice — resolved: для поточного Silver-шару йдеться про **NBEATSx Forecast**, а не про embedding-first модель.
- "TFT" може змішувати forecast і strategy — resolved: **TFT Forecast** є interpretable прогнозним кандидатом у Silver, але не **Proposed Bid** і не **Dispatch Command**.
- "model registry" може звучати як production deployment — resolved: у цьому slice це **Registered Forecast Candidate** для відтворюваності MLflow/Dagster експериментів, а не **Target Strategy**.
- "connect Silver to Gold" може звучати як прямий перехід від прогнозу до ринкової заявки — resolved: поточний Gold-зв'язок є **Forecast Strategy Evaluation**, тобто LP/regret comparison, а не **Proposed Bid** або **Dispatch Command**.
- "real data" може звучати як live overlay поверх synthetic history — resolved: для thesis-grade claims потрібен **Real-Data Research Benchmark**, де історія DAM/weather є observed або clearly provenance-marked.
- "backtest" може звучати як будь-який replay — resolved: канонічний експеримент є **Rolling-Origin Strategy Backtest**, де майбутні ціни не доступні forecast/model layer до scoring.
- "price cap" раніше звучав як статична константа — resolved: price caps, tariffs і related market rules є **Effective-Dated Market Constraint**, особливо перед IDM/Balancing expansion.
- "profit" раніше міг означати gross spread мінус degradation only — resolved: thesis-grade net value також має враховувати **Market Participation Cost**.
- "M3DT" може звучати як обіцянка повного відтворення SOTA paper або production-моделі — resolved: у межах диплома це **M3DT-Inspired Research Strategy** для симульованих multi-client experiments після baseline, а не поточний MVP.
- "baseline forecast" раніше змішував live naive forecast і perfect foresight — resolved: live MVP використовує **Baseline Forecast**, а hindsight використовується лише як **Oracle Benchmark** офлайн.
- "naive forecast" раніше змішував flat persistence і seasonal slot-based baseline — resolved: канонічний Level 1 baseline є **Level 1 Naive Forecast** з **Price Shape Preservation**, а не плоска персистенція.
- "seasonal naive" раніше міг означати і strict copy, і smoothed average — resolved: канонічний baseline використовує **Strict Similar-Day Rule** без усереднення.
- "baseline execution" раніше виглядало як статичний денний план — resolved: baseline працює через **Baseline Rolling Horizon**, а не через one-shot schedule.
- "Level 1 scope" раніше міг тягнути за собою одразу кілька venue і 15-хвилинні інтервали — resolved: **Level 1 Market Scope** обмежений погодинним **DAM**, а розширення йде через **Sequential Complexity**.
- "dashboard" може означати і thesis-demo evidence surface, і operator-facing control surface — resolved: наступний UI slice є **Research Defense Dashboard**, не production/live-trading dashboard.
- "final product demo dashboard" відрізняється від thesis evidence surface — resolved: production-style demo surface є **Operator Product Dashboard**, який може reuse metrics, але не може маскувати pilot/simulated outputs as live controls.
