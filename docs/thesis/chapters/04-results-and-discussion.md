# 4. Результати експериментів та обговорення

## 4.1. Мета експериментального оцінювання

Експериментальна частина роботи перевіряє не лише якість прогнозу ціни, а
кінцеву якість рішення для BESS-арбітражу. Тому основною метрикою є не MAE або
RMSE, а regret відносно oracle LP, тобто різниця між значенням рішення, яке
можна було б отримати з фактичними цінами, і значенням рішення, прийнятого на
основі доступного перед рішенням прогнозу або селектора.

У всіх експериментах `strict_similar_day` використовується як заморожений
контрольний компаратор. Він не є нейронною моделлю: це leakage-free правило, яке
копіює історично схожий день, після чого schedule оцінюється тим самим strict
LP/oracle evaluator. Таким чином, порівняння з ним є консервативним контрольним
рубежем для Level 1 MVP.

Поточна claims boundary є такою:

- система демонструє offline/read-model Strategy Promotion evidence;
- `market_execution_enabled=false` в усіх evidence packets;
- результат не є live trading, не є deployed Decision Transformer control і не
  є твердженням про повну перевагу raw neural forecast;
- зовнішні європейські джерела, зокрема ENTSO-E, OPSD, Ember, Nord Pool,
  PriceFM і THieF, залишаються governance-only і не входять до тренування цього
  результату.

## 4.2. Експериментальна драбина

Експерименти були побудовані як послідовна драбина від простого контролю до
decision-aware schedule selection:

1. `strict_similar_day` - заморожений контрольний компаратор.
2. Raw neural forecasts - компактні та official NBEATSx/TFT прогнози,
   strict-scored через LP/oracle.
3. Horizon-aware regret-weighted calibration - prior-only корекція прогнозів за
   горизонтами, де попередні помилки мали більшу вартість у regret.
4. Feature-aware selectors - prior-only селектори, що враховують price regime,
   spread volatility, rank instability, calendar, weather/load context і
   tenant-level failure clusters.
5. Schedule/Value Learner V2 - decision-value selector, який вибирає один із
   feasible LP-scored schedule candidates, а не оцінює модель лише за помилкою
   forecast.

Ця драбина показала важливий методологічний висновок: у задачі BESS arbitrage
raw neural forecast може мати корисний сигнал, але цей сигнал стає цінним лише
після перетворення у decision-value вибір schedule.

## 4.3. Дані та область доказів

Головний зафіксований результат побудований на 365-anchor Ukrainian backfill
panel:

| Поле | Значення |
|---|---|
| Джерела | OREE DAM prices, Open-Meteo/weather, tenant load/config context |
| Кількість tenants | 5 canonical tenants |
| Кількість anchors | 365 rolling-origin anchors |
| Final holdout | 18 latest anchors per tenant/source |
| Final validation coverage | 90 tenant-anchors per source model |
| Market venue | DAM |
| Currency | UAH |
| Claim scope | Offline Strategy Promotion only |

Пакет доказів збережений у
`data/research_runs/week3_official_global_panel_365_strategy_promotion/`.
Він містить registry JSON/Markdown, attempt manifest, monitor snapshot,
promotion gate frame і окремий trace summary для Schedule/Value Learner V2.

## 4.4. Raw official NBEATSx як важливий, але недостатній сигнал

Official global-panel NBEATSx був потрібний для чеснішого тесту, ніж компактні
in-repo smoke models. Однак результат не слід інтерпретувати як доведення raw
forecast superiority. У проміжних і ранніх офіційних запусках raw forecasts
програвали `strict_similar_day` за strict LP/oracle regret. Це узгоджується з
методологією decision-focused learning: оптимізація BESS schedule залежить від
рангу екстремумів, spread shape, SOC path і degradation-adjusted value, а не
лише від середньої похибки прогнозу.

Тому офіційний neural forecast у підсумковому evidence packet використовується
як джерело candidate schedules, але не як автоматичний controller.

## 4.5. Результат Schedule/Value Learner V2

Перший стійкий результат отримано не raw NBEATSx, а Schedule/Value Learner V2
поверх official global-panel schedule candidates. Цей шар вибирає schedule
family на основі prior/train anchors, а final holdout використовується лише для
scoring.

365-anchor registry показав:

| Source model | Strict mean / median regret, UAH | Learner mean / median regret, UAH | Mean improvement vs strict | Rolling strict passes |
|---|---:|---:|---:|---:|
| `nbeatsx_official_global_panel_v1` | 310.583 / 198.386 | 225.437 / 109.692 | 27.41% | 4 / 4 |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 310.583 / 198.386 | 206.367 / 96.021 | 33.55% | 4 / 4 |

Обидва source-specific challengers пройшли conservative Offline Strategy
Promotion gate: mean regret покращився більше ніж на 5%, median regret не
погіршився, rolling robustness пройшов 4 з 4 вікон, coverage є thesis-grade, а
`market_execution_enabled=false`.

## 4.6. Traceability Schedule/Value Learner V2

Перед наступним V3/DT експериментом було зафіксовано, які саме weight-profile
selections зробив Schedule/Value Learner V2. Повний trace збережений у
`dfl_schedule_value_learner_v2_trace_summary.md` у 365-anchor evidence packet.

Ключові властивості trace:

- early train anchors мають 9 candidate schedules per anchor;
- після появи residual candidate доступно до 10 candidate schedules per anchor;
- final holdout містить 900 schedules per source model;
- calibrated official NBEATSx вибрав profile `prior_regret_value` для всіх
  п'яти tenants;
- raw official global-panel NBEATSx вибрав різні prior-only profiles:
  `prior_regret_value`, `spread_value` і `strict_guarded_prior_value`.

Final selected family counts показують, що learner не просто копіював strict
control. Для calibrated official NBEATSx він вибрав 16 strict-control, 19
strict-prior-residual і 55 strict-raw-blend schedules. Для raw official
global-panel NBEATSx він вибрав 19 strict-control, 7 strict-prior-residual і 64
strict-raw-blend schedules. Отже, improvement виник з prior-only вибору між
feasible schedule families, а не з live-підглядання у final holdout.

## 4.7. Результат Schedule/Value Learner V2+

Після фіксації V2 було виконано V2+ експеримент. Він не замінює строгий
LP/oracle evaluator і не вводить зовнішні європейські training rows. Його зміна
обмежена candidate library: додаються prior-safe schedule variants навколо
залишкових failure modes, а selector може перейти з frozen V2 на V2+ лише тоді,
коли prior/train anchors показують non-degrading improvement.

Latest-holdout comparison packet
`week3_official_global_panel_schedule_value_v2_plus_comparison` показав:

| Source model | Strict mean regret, UAH | Frozen V2 mean regret, UAH | V2+ mean regret, UAH | Improvement vs strict | Improvement vs V2 |
|---|---:|---:|---:|---:|---:|
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 310.58 | 206.37 | 174.77 | 43.73% | 15.31% |
| `nbeatsx_official_global_panel_v1` | 310.58 | 225.44 | 193.36 | 37.74% | 14.23% |

Rolling robustness replay over four 18-anchor windows also passed:

| Source model | Rolling windows passed | Interpretation |
|---|---:|---|
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 4 / 4 | V2+ beats both `strict_similar_day` and frozen V2 |
| `nbeatsx_official_global_panel_v1` | 4 / 4 | V2+ beats both `strict_similar_day` and frozen V2 |

Therefore the strongest current thesis result is Schedule/Value Learner V2+ on
the 365-anchor Ukrainian panel. The calibrated official NBEATSx source is the
preferred headline because it has the lowest latest-holdout mean regret. The
claim remains Offline Strategy Promotion only: no live market execution, no
dashboard/API default switch, and no claim that raw neural forecasting alone is
superior to `strict_similar_day`.

## 4.8. Інтерпретація результату

Результат підтримує тезу про практичну цінність decision-aware pipeline:
нейронний forecast сам по собі не був достатнім, але schedule/value layer
перетворив прогнозний сигнал у кращий LP-feasible decision. Для дипломної
архітектури це означає, що фінальний controller має бути default-fallback
системою:

- `strict_similar_day` залишається fallback і контрольним comparator;
- Schedule/Value Learner V2+ може бути promoted лише в offline/read-model
  evidence stack;
- Pydantic Gatekeeper і strict LP/oracle evaluator залишаються
  deterministic safety layers;
- live execution не активується цим результатом.

## 4.9. Що не підтверджено поточними експериментами

Поточні експерименти не доводять такі твердження:

- raw NBEATSx або TFT стабільно кращі за `strict_similar_day`;
- Decision Transformer уже є deployed controller;
- market-coupling exogenous features з ЄС входили у training або пояснюють
  поточний V2+ результат;
- система готова до реального виконання угод на ринку.

Ці обмеження є важливими для академічної чесності роботи. Вони не зменшують
цінність отриманого результату, але визначають його точну область застосування:
offline/read-model Strategy Promotion evidence для Ukrainian DAM BESS arbitrage.

## 4.10. Наступний напрямок після V2+

Оскільки V2+ уже покращив frozen V2 і пройшов rolling robustness, наступна
робота не повинна бути ще одним малим selector/ranker experiment. Доцільні два
напрями:

- governed market-coupling ablation: додати лише point-in-time approved
  сусідні market features і порівняти Ukrainian-only V2+ з
  Ukrainian-plus-governed-features без послаблення strict LP/oracle gate;
- true DFL/DT bridge: навчити decision-aligned або trajectory model, який
  повинен перевершити V2+ і behavior-cloning/selector baselines, а не просто
  повторити вже знайдений schedule/value rule.

V2+ залишається current thesis headline evidence до появи сильнішого результату
за тим самим conservative gate. Європейські market-coupling sources не входять у
поточний результат і можуть бути використані лише після перевірки publication
time, timezone/DST, currency normalization, licensing, market-rule mapping і
domain-shift ризику.

Після фіксації V2+ додано окремий governed market-coupling ablation path. Його
роль полягає не в тому, щоб заднім числом пояснити V2+ через європейські дані, а
в тому, щоб перевірити нову гіпотезу: чи покращить point-in-time approved
neighbor-market feature український V2+ baseline. Якщо governance неповний, цей
path повинен повертати `blocked_by_governance` і не запускати market-coupled
training variant.

Матеріалізований ablation packet підтвердив саме цей governance outcome:
`blocked_by_governance` для обох official global-panel NBEATSx source paths,
approved external feature columns відсутні, market-coupled B variant не
тренувався, а `market_execution_enabled=false`. Отже, поточний headline result
залишається українським V2+ evidence, а ENTSO-E/neighbor-market джерела
залишаються тільки governance/readiness шаром до завершення publication-time,
timezone/DST, FX, licensing, market-rule та domain-shift перевірок.

Додатковий Poland-specific governance run уточнив цей висновок. Новий
`entsoe_poland_feature_governance_frame` звів перший external lane до одного
можливого point-in-time feature column:
`entsoe_pl_day_ahead_price_uah_mwh`. Пакет
`week3_dfl_entsoe_poland_feature_ablation_v1` знову показав
`blocked_by_governance`, approved feature columns відсутні, а B-training не
запускався. На відміну від попереднього generic blocker, цей пакет уже називає
конкретні відсутні докази: ENTSO-E token/source-backed sample,
publication-time evidence, prior-known EUR/UAH FX, timezone/DST, licensing,
market-rule mapping, domain-shift validation і temporal availability. Це
означає, що наступна робота над market coupling є governance/data-acquisition
задачею, а не новим selector experiment.

Після цього також зафіксовано compact DFL/DT bridge result. У ньому residual
DFL, tiny offline Decision Transformer, behavior cloning і fallback
порівнювалися не лише зі `strict_similar_day`, а з поточним українським V2+
baseline. Результат є негативним, але корисним: compact bridge не перевершив
V2+ за mean regret без погіршення median regret. Тому він не стає headline
result і не змінює claim boundary.

Академічна інтерпретація цього результату така: відмова compact bridge не
спростовує DFL/DT як напрям. Вона показує, що старий compact candidate path є
слабшим за official global-panel V2+ schedule/value learner. Тому було додано
official V2+-teacher bridge path, де teacher trajectories формуються з official
V2+ та oracle-style schedule evidence, а final holdout залишається тільки для
scoring.

Цей official bridge також не перевершив V2+. Для calibrated official NBEATSx
V2+ mean regret становив 174.77 UAH, тоді як residual/DT challenger дав 367.70
UAH. Для raw official NBEATSx V2+ mean regret становив 193.36 UAH, тоді як
residual/DT challenger дав 328.51 UAH. Behavior cloning також залишився гіршим
за V2+. Отже, поточний висновок стає сильнішим: V2+ залишається thesis
headline, а наступний DFL/DT крок потребує кращої trajectory objective або
багатшого teacher/candidate design, а не простого tiny DT поверх наявних
траєкторій.

Додатковий failure audit для official bridge сформував 720 analysis-only rows.
Найбільший клас помилок - `candidate_family_collapse`: 351 rows, або 48.75%.
Це означає, що residual DFL / offline DT / fallback часто вибирали одну й ту
саму schedule-family, переважно `strict_raw_blend_v2`, замість того щоб
навчитися, коли V2+ змінює family/profile. Інші класи: behavior cloning
слабший за V2+ selector (142 rows), weak trajectory objective (135 rows) і
bad teacher target (92 rows), де strict already near-oracle або сильніший за
V2+. Тому наступний академічно чистий DFL v2 напрям - pairwise schedule-value
ranking або return-conditioned schedule-family selector, а не більший DT з тим
самим action-imitation objective.

Цей напрям реалізовано як окремий pairwise schedule-value DFL v2 slice. Він
працює не як deployed Decision Transformer, а як prior-only selector: на
train/prior anchors він порівнює feasible schedule families попарно за
value/regret, вибирає одну family тільки за наявності non-degradation signal
проти frozen V2+, інакше повертається до V2+. Final holdout використовується
лише для strict LP/oracle scoring. Отже, новий експеримент перевіряє саме
гіпотезу з failure audit: чи можна зменшити `candidate_family_collapse` через
кращу schedule-value objective, не послаблюючи V2+ fallback і не роблячи
market-execution claim.

Матеріалізація цього slice показала важливий негативний результат. Asset check
пройшов, evidence packet було збережено локально у
`data/research_runs/week3_dfl_schedule_value_dfl_v2_comparison/`, але gate
залишився `diagnostic_pass_replacement_blocked`: для calibrated official
NBEATSx DFL v2 повторив V2+ mean regret 174.77 UAH і median regret 67.30 UAH,
тобто improvement проти V2+ дорівнював 0.00%. Для raw official NBEATSx DFL v2
так само повторив V2+ mean regret 193.36 UAH. Отже, поточний DFL v2 objective
є валідним diagnostic evidence, але не замінює V2+ як thesis headline.

Наступний експеримент визначено як Candidate-Value DFL v3. На відміну від DFL
v2, він не вибирає одну family для всіх final anchors і не запускає ще один
малий Decision Transformer. Він розширює candidate library навколо реальних
failure modes - strict-neighborhood, SOC terminal target, peak/trough timing,
uncertainty/risk, degradation-price sweeps, train-only oracle-neighborhood
diagnostics, а також дві prior-template сім'ї. `prior_best_family_template_v3`
переносить середній forecast-vector delta між raw forecast і найкращим prior
feasible schedule, а `prior_oracle_residual_template_v3` переносить середній
raw-vs-actual residual з train-selection anchors. На final holdout ці сімейства
використовують тільки prior templates, тоді як actual prices залишаються лише
для strict scoring.

Окремий label panel,
`dfl_official_global_panel_candidate_value_label_panel_v3_frame`, розділяє
`selector_feature_*` prior-safe inputs і `label_*` realized value/regret labels.
На цьому panel тренується `learned_linear_candidate_value_v3`: невеликий
ridge-style candidate-level value scorer, який оцінює повні candidate schedules
за prior family regret, forecast spread, LP objective value, throughput,
degradation penalty, SOC slack і candidate-family intercepts. На відміну від
попередніх fixed profile selectors, тут справді підбираються ваги value scorer
на train/prior rows. Final-holdout labels не використовуються для weights,
profile selection або fallback decision.

Матеріалізований результат Candidate-Value DFL v3 також не замінив V2+. У
Dagster run `2dcdb48d-70b0-44f5-99b8-b8b5d4d58057` label-panel, strict
benchmark і failure-audit checks пройшли, але gate залишився
`diagnostic_pass_replacement_blocked`. Для calibrated official global-panel
NBEATSx V3 повторив V2+ mean regret `174.77` UAH і median regret `67.30` UAH;
для raw official global-panel NBEATSx V3 повторив V2+ mean regret `193.36` UAH
і median regret `68.89` UAH. Отже, candidate-level value scorer підтвердив
силу V2+ fallback, але не знайшов schedule, який надійно покращує V2+ на final
holdout. V2+ залишається thesis headline Offline Strategy Promotion evidence.

Failure audit пояснив, чому нові prior-template schedules не перемогли V2+
достатньо часто. На final holdout `prior_best_family_template_v3` мав mean
regret `605.71` UAH для calibrated NBEATSx і `689.66` UAH для raw NBEATSx;
`prior_oracle_residual_template_v3` мав відповідно `627.08` UAH і `729.69` UAH.
Win rate цих сімейств проти V2+ становив лише `4.44%`, `13.33%`, `5.56%` і
`7.78%` залежно від source row. Тому diagnosis - не нестача кількості
candidate schedules сама по собі, а `template_not_competitive_vs_v2_plus`:
середні історичні residual/delta шаблони іноді допомагають окремим anchors,
але в середньому гірше переносяться між price regimes, ніж уже знайдений
V2+ schedule/value blend.

Додатковий scratch, non-persisted zero-threshold probe показав, що послаблення
fallback могло б покращити raw NBEATSx source з `193.36` до `185.62` UAH mean
regret. Це не було promoted, тому що результат усе ще гірший за calibrated
V2+ headline (`174.77` UAH), а train/prior signal є надто слабким для
консервативного thesis gate.

На основі цього результату додано Plateau-Breaker / Candidate-Value DFL v4
slice. Його мета - не одразу запускати більший DT, а спершу розділити причини
плато: відсутність кращого candidate schedule, помилка scorer-а, або надто
консервативний fallback. Далі V4 перевіряє data/context gaps на 365-anchor
панелі та додає сильніші schedule families: quantile/risk, block peak,
terminal SOC reserve, spread-volatility robust, tenant-specific
degradation/throughput sweep і train-only oracle-neighborhood diagnostics.

Матеріалізований V4 run `0c57f795-3b5b-4106-ad9d-0776294a1eb4` пройшов
label-panel і strict-benchmark evidence checks, але також не замінив V2+. У
strict LP/oracle benchmark було `720` rows: для calibrated official NBEATSx V4
повторив V2+ mean regret `174.77` UAH, а для raw official NBEATSx повторив V2+
mean regret `193.36` UAH. Отже, improvement проти V2+ знову дорівнював
`0.00%`.

Новий внесок V4 полягає в кращій діагностиці плато. Autopsy показав, що для
calibrated NBEATSx `71 / 90` final-holdout tenant-anchor rows мають причину
`candidate_not_better`, а `19 / 90` - `fallback_too_conservative`. Для raw
NBEATSx відповідні числа становлять `48 / 90` і `42 / 90`. Pre-fallback raw
candidate scoring міг знизити mean regret до `190.59` UAH проти raw V2+
`193.36` UAH, але цей результат усе ще гірший за calibrated V2+ `174.77` UAH і
не мав достатнього prior/train evidence для promotion. Data-quality audit також
показав, що Ukrainian DAM history і regret-cluster alignment готові, але
weather/load context, calendar/event context і publication-time availability
мають gaps. Тому V2+ залишається thesis headline, а наступне покращення має
починатися з point-in-time context і нових schedule shapes, не з більшого DT
над тим самим objective.

## 4.11. Supervisor-facing evidence

Для короткої зустрічі з керівником підготовлено progress package:
`docs/thesis/weekly-reports/week4/progress-meeting-2026-05-13/`.

Пакет включає:

- короткий progress brief;
- speaker notes для 5-хвилинної презентації;
- графіки та інфографіку strategy ladder;
- feature audit Schedule/Value Learner V2;
- presentation deck package.

Цей пакет використовується як supervisor-facing summary, тоді як 365-anchor
registry є machine-checkable evidence packet для відтворення результатів.
