Перелік умовних позначень, скорочень і термінів

Цей розділ залишено коротким, щоб пояснювальна записка не втрачала фокус основної частини. Повний словник технічних назв, evidence flags, endpoint names і довгих дослідницьких термінів перенесено в додаток. Нижче наведено лише ті скорочення, які використовуються для читання розділів 1-5.

BESS - Battery Energy Storage System, система накопичення енергії на батареях.
DAM - Day-Ahead Market, ринок на добу наперед; primary evaluated research scope і headline result у роботі.
IDM - Intraday Market / ВДР, внутрішньодобовий ринок; у цій роботі доступний як повноцінний hourly recommendation preview/read-model lane. Live 15-minute bid, settlement, ProposedBid і market submission залишаються заблокованими.
DFL - Decision-Focused Learning, підхід до оцінювання моделей за downstream decision value.
DT - Decision Transformer, offline sequence-policy напрям, який у роботі має статус research-shadow.
EPF - Electricity Price Forecasting, прогнозування цін електроенергії.
LP - Linear Programming, лінійне програмування для побудови feasible schedule.
MAE / RMSE - forecast-only метрики, які не є головним критерієм promotion.
NBEATSx - time-series forecast adapter, який може створювати price-scenario candidates для unpublished targets.
SOC - State of Charge, стан заряду батареї.
SOH - State of Health, стан здоров'я батареї; повний digital-twin шар лишається roadmap.
TFT - Temporal Fusion Transformer, quantile forecasting model для shadow/complementarity evidence.
UAH - українська гривня, валюта regret/value у головних результатах.
V13 - source-readiness/acquisition gate; не modeling slice і не дозвіл на DT/LAVA.

Ключові межі:
market_execution_enabled=false - явний прапорець, що забороняє трактувати preview як live trading.
Operator preview - read-model поверхня для перегляду рекомендацій, а не market execution console.
Regret - втрачена цінність відносно oracle LP.
strict_similar_day - frozen leakage-free baseline і fallback comparator.
V2+ - підтверджений schedule/value selector у межах offline/read-model evidence.
HF value-aligned shadow - manually selected safe-switch shadow preview, який ранжує LP-free candidate schedules через Hugging Face scorer і deterministic gates; не V13 training, не production controller і не market execution.
Tail-risk guard - обмеження, що блокує candidate schedule, якщо predicted downside risk перевищує safe cap.
Guarded abstention - стан, коли shadow model свідомо повертається до HOLD/V2+ fallback замість non-HOLD preview.
