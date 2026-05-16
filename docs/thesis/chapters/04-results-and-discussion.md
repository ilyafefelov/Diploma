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
- market-coupling exogenous features з ЄС уже покращили український результат;
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
