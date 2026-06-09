# Speaker Notes: Supervisor Progress Meeting, 2026-05-13

## 1. Title / current status

Короткий меседж: після Demo 1 система перейшла від operator-facing MVP-прев'ю до відтворюваного research evidence path. Найсильніший результат зараз формулюється як **Offline Strategy Promotion**: schedule/value learner може бути offline/read-model challenger за `strict_similar_day`, але не є live execution.

Потрібно одразу сказати межу: немає live trading claim, немає deployed Decision Transformer, немає dashboard/API default switch.

## 2. Week 2 baseline

На Week 2 вже був наскрізний контур: OREE/Open-Meteo/tenant config входять у Dagster, далі `strict_similar_day` дає forecast, LP solver будує dispatch, FastAPI/Nuxt показують operator preview.

Слабке місце тоді: це ще не було evidence, що neural або DFL-шар кращий. Це був правильний MVP, але не promotion-grade experiment.

## 3. Strategy ladder: from baseline to schedule/value learning

Цей слайд пояснює різницю між усіма підходами в одній картинці.

`strict_similar_day` - це frozen control: історично схожий день, LP dispatch, без neural learning. Raw NBEATSx/TFT - це neural forecast, але він ще часто програє по decision value. Horizon-aware calibration робить prior-only correction по годинах горизонту, де помилка коштувала більше regret. Feature-aware selector додає price regime, weather/load context і tenant behavior. Schedule/Value Learner V2 вже вибирає не "красивий forecast", а feasible LP schedule з кращою UAH value.

Головний меседж: перемога прийшла не від raw forecast superiority, а від decision-aware schedule selection.

## 4. New evidence pipeline

Ми змінили питання. Не "який forecast красивіший", а "який schedule дає менший regret у гривнях після strict LP/oracle scoring".

Пайплайн зараз: observed panel -> official global-panel model -> candidate schedule library -> schedule/value learner -> strict promotion gate. Це ближче до DFL, бо оптимізаційна цінність стоїть в центрі.

## 5. Data shape

Головні числа: 5 tenants, 365 rolling anchors, `58,190 x 60` official global-panel training frame, 1,825 strict rows, 90 latest validation tenant-anchors per source.

Важливо: current promoted evidence is Ukrainian-only. EU/neighbor-market rows are not in training yet.

## 6. Raw official models failed

Raw official NBEATSx and horizon-calibrated NBEATSx still lose to `strict_similar_day` over the full 365-anchor panel.

Mean regret:

- `strict_similar_day`: 431.52 UAH.
- raw global-panel NBEATSx: 708.14 UAH.
- horizon-calibrated NBEATSx: 602.51 UAH.

Academic interpretation: this is not a raw forecast superiority result. It supports the DFL/PTO literature idea that forecast accuracy alone is not enough for arbitrage.

## 7. Exogenous/context features helped

The AFL audit showed that weather/load context fixed a blind spot. It covered 1,560 AFL rows across five tenants and two compact models. After adding prior-only context, the blocker became decision value and ranking, not missing context.

Feature-aware selectors improved strongly versus raw neural schedules:

- NBEATSx selector: 299.73 mean regret, 63.15% better than raw NBEATSx, 4.79% better than strict.
- TFT selector: 299.19 mean regret, 70.19% better than raw TFT, 4.96% better than strict.

That was useful, but it narrowly missed the conservative 5% strict-control gate. The point is not "features magically solved it"; the point is "prior-only context made selector behavior much closer to promotion."

## 8. Schedule/value learner won

The schedule/value learner is the first promotion-grade result.

On the 365-anchor Ukrainian panel latest holdout:

- raw-source learner: 225.44 mean regret vs strict 310.58, improvement 27.41%.
- calibrated-source learner: 206.37 mean regret vs strict 310.58, improvement 33.55%.
- rolling robustness: 4 of 4 strict-control windows.

This means the schedule/value layer can be described as a robust offline challenger, while `strict_similar_day` remains fallback.

## 9. Schedule/Value Learner V2 architecture and metrics

Це детальніша схема того, що саме стало promotion-grade evidence.

Кроки такі: official NBEATSx дає price forecast; candidate library генерує багато feasible LP schedules; кожен schedule має LP value, SOC path, throughput і degradation proxy; prior-only learner ранжує ці schedules без final-holdout actuals; потім strict LP/oracle gate робить фінальний scoring.

Числа на слайді важливі: для raw global-panel NBEATSx strict mean regret був 310.58, learner mean regret став 225.44, тобто 27.41% improvement. Для calibrated NBEATSx learner mean regret став 206.37, тобто 33.55% improvement. Rolling robustness - 4/4 windows.

Це все ще offline/read-model evidence only: `market_execution=false`, `strict_similar_day` залишається fallback, live trading не заявляється.

## 10. Schedule/Value Learner V2 feature map

Тут важливий нюанс термінології: фраза не "weights learned offline", а **"weight profile selected offline from prior anchors"**.

Це точно відповідає коду. Learner не робить gradient descent і не навчає ваги як neural model. Він має кілька фіксованих scoring profiles, проганяє їх на train-selection anchors, вибирає профіль з найменшим prior regret, а потім застосовує цей профіль до final holdout.

Це слабше за повний DFL, але сильніше й безпечніше для дипломного evidence gate: результат зрозумілий, prior-only, reproducible, і не використовує final actuals для вибору профілю.

## 11. Market-coupling blocked

ENTSO-E, OPSD, Ember, Nord Pool, PriceFM, and THieF are in the research/source map and feature-governance route, but they are not training inputs yet.

The blockers are publication time, timezone/DST alignment, prior EUR/UAH FX normalization, market-rule mapping, licensing, and domain-shift validation. This is deliberate: it prevents leakage and overclaiming.

## 12. Diploma meaning / next work

Final message: the project now has a defensible path from Week 2 MVP to DFL-style offline evidence.

What is done:

- observed Ukrainian panel;
- strict no-leakage evaluation;
- official global-panel NBEATSx evidence;
- schedule/value learner that beats `strict_similar_day` under strict LP/oracle gate.

What remains:

- no live execution;
- no deployed DT;
- market-coupling still gated;
- next work is GPU/HF offload, more UA backfill if available, and only then stronger DFL/DT experiments.

## 13. External feature boundary

Проста відповідь на питання про ENTSO-E, OPSD, Ember, Nord Pool, PriceFM і THieF: ні, ці джерела ще не потрапили в training або promoted result.

Вони вже представлені в source map і market-coupling governance route, але всі шість зовнішніх feature candidates мають `training_use_allowed=false`. У поточному 365-anchor result використані тільки Ukrainian OREE DAM, Open-Meteo/weather context, tenant load/configuration context і strict LP/oracle scoring.

Це важливо для захисту: майбутні EU/neighbor signals можуть бути корисними, але зараз проєкт не приписує їм отриманий результат.

## 14. Horizon-aware regret-weighted calibration

Пояснення простою мовою: raw neural forecast може помилятися на різних годинах горизонту. Але для батареї не всі години однаково важливі. Помилка в годину, де треба зарядитися або продати, може коштувати більше UAH regret, ніж помилка в нудну годину.

Horizon-aware regret-weighted calibration бере тільки prior anchors, дивиться де помилки forecast у минулому давали найбільший LP regret, і робить horizon-specific correction. Це покращило обидва neural candidates порівняно з raw variants, але на Dnipro 90-anchor preview вони все ще не стали стабільнішою заміною `strict_similar_day`.

## 15. Remaining failures are decision/ranking failures

Це означає, що модель часто не просто трохи помиляється в ціні. Вона помиляється у порядку важливих годин: які години дешеві для charge, які дорогі для discharge, і чи достатній spread для profitable cycle після efficiency/degradation.

Тому LP-value failure 80.23% означає: schedule, який вийшов із forecast, часто має гіршу UAH value, ніж strict control. Rank/extrema failure 64.83% означає: модель часто не ловить правильні high/low price hours. Spread-shape failure 55.19% означає: forecast shape не дає правильний arbitrage spread.

## 16. Why selectors almost passed, and schedule/value cleared the gate

Feature-aware selectors були першим доказом, що prior-only context має цінність. Вони вибирали правила з урахуванням price regime, spread volatility, rank stability, weather/load context і tenant behavior. Вони сильно покращили raw compact neural schedules: на 63.15% для NBEATSx і 70.19% для TFT.

Але вони були ще надто простими: вони майже дійшли до strict gate, 4.79% і 4.96% improvement vs strict, але поріг був 5%.

Schedule/value learner пішов на крок ближче до DFL: він не просто питає "чи forecast схожий", а вибирає серед feasible LP schedules за schedule features: forecast spread, LP objective, throughput, degradation, SOC slack і prior family regret. На 365-anchor panel це вже дало 27.41% і 33.55% improvement vs strict та 4/4 rolling strict-control passes.
