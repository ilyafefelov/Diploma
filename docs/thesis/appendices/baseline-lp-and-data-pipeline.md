# Додаток Б. LP-контур та поточний data pipeline

Додаток Б пояснює, як у роботі реалізовано deterministic LP-контур для BESS-арбітражу. Його призначення - показати, що operator preview і offline evaluator побудовані не як автономний trading agent, а як контрольований read-model контур: price context надходить у LP, LP формує physically feasible schedule, далі schedule проходить deterministic gatekeeper і використовується для value/regret evidence. Поточна межа залишається незмінною: no market execution, no proposed bid, no settlement, no dispatch command.

Б.1. Роль LP у системі

Таблиця Б.1. Компоненти decision pipeline і роль LP

| Компонент | Реалізація | Роль у роботі | Межа інтерпретації |
|---|---|---|---|
| Price context | Official OREE DAM/IDM rows, strict_similar_day, NBEATSx/TFT scenarios | Формує hourly price vector для LP або replay/evaluation | Не є market order і не є гарантією execution-ready ціни |
| LP schedule preview | `HourlyDamBaselineSolver` | Перетворює price vector і battery constraints у feasible charge/discharge schedule | Deterministic optimizer, не ML policy |
| Schedule gatekeeper | API physical-constraint checks та Pydantic contracts | Перевіряє SOC/power envelope і блокує infeasible preview | Gatekeeper не надсилає bid/order payload |
| Oracle/regret evaluator | strict LP/oracle scoring rows | Рахує decision value і regret для порівняння моделей | Offline/read-model evidence, не live settlement |
| ML/DFL/DT layers | V2+, DFL diagnostics, DT/HF shadow | Ранжують candidates або пропонують guarded safe-switch | Research/advisor layer з abstention/fallback |

Б.2. Математична постановка LP

Для кожної години \(t=0,\ldots,T-1\) LP вибирає:

```text
c_t = charge power, MW
d_t = discharge power, MW
e_t = stored energy / SOC, MWh
```

Параметри:

```text
p_t = published або scenario price для DAM/IDM, UAH/MWh
Delta t = 1 hour
C = battery capacity, MWh
P_max = max charge/discharge power, MW
eta_c = charge efficiency
eta_d = discharge efficiency
s_min, s_max = SOC bounds as fractions of C
gamma = degradation cost per MWh throughput, UAH/MWh
```

У коді `eta_c = eta_d = sqrt(round_trip_efficiency)`, а degradation coefficient визначається як:

```text
gamma = degradation_cost_per_cycle_uah / (2 * C)
```

Objective максимізує degradation-adjusted market value:

```text
maximize sum_t [
  p_t * (d_t - c_t) * Delta t
  - gamma * (c_t + d_t) * Delta t
]
```

Лінійні battery constraints:

```text
e_0 = initial_soc_fraction * C

e_{t+1} = e_t
          + c_t * eta_c * Delta t
          - d_t * Delta t / eta_d

s_min * C <= e_t <= s_max * C

0 <= c_t <= P_max
0 <= d_t <= P_max
```

Signed schedule, який бачить operator-facing read model:

```text
net_power_t = d_t - c_t
```

Якщо `net_power_t > 0`, schedule означає discharge/sell preview; якщо `net_power_t < 0`, charge/buy preview; якщо `net_power_t = 0`, HOLD. Важливо, що це read-model semantics: навіть коли перша година подається як recommendation preview, система не створює market-submittable `ProposedBid`.

Б.3. Відповідність коду та документації

Таблиця Б.2. Code-level verification LP-контуру

| Питання | Поточна реалізація | Де перевірено |
|---|---|---|
| Який solver використовується? | `cvxpy.Problem(objective, constraints)` у `HourlyDamBaselineSolver` | `src/smart_arbitrage/assets/gold/baseline_solver.py` |
| Який horizon? | Default `planning_horizon_hours=24`, interval `60` minutes | `BaselineSolverConfig` |
| Де врахована efficiency? | У SOC transition: charge додає `c_t * eta_c`, discharge віднімає `d_t / eta_d` | `_solve_schedule` |
| Де врахована degradation penalty? | Throughput penalty `gamma * (c_t + d_t) * Delta t` | `BatteryPhysicalMetrics.degradation_cost_per_mwh_throughput_uah` |
| Чи є binary/MILP decision? | Ні, поточний Level 1 contour є LP без binary unit-commitment змінних | `charge_mw`, `discharge_mw`, `soc_mwh` як continuous variables |
| Чи є market execution? | Ні, API повертає preview/read model і `market_execution_enabled=false` | `/dashboard/operator-recommendation` |

Б.3.1. Code summary LP solver

Основна реалізація LP solver знаходиться у `src/smart_arbitrage/assets/gold/baseline_solver.py` і зосереджена навколо класу `HourlyDamBaselineSolver`. Конфігурація `BaselineSolverConfig` задає default horizon `24` години, `commit_interval_hours=1`, hourly interval `60` minutes і дозволені market venues `DAM` та `IDM`. Якщо horizon або interval некоректні, конфігурація зупиняється через `ValueError`, тому solver не переходить у неявний режим з іншою часовою сіткою.

Solver має два входи. Перший шлях, `solve_next_dispatch`, сам будує strict similar-day forecast з price history: для Tuesday-Friday використовується price source з `t-24h`, для інших днів - `t-168h`; для цього потрібно щонайменше `168` hourly observations. Другий шлях, `solve_dispatch_from_forecast`, приймає вже підготовлений список `BaselineForecastPoint`; саме цей шлях використовують operator/API surfaces, коли price context уже зібрано з official OREE DAM/IDM row або forecast-store scenario.

У `_solve_schedule` solver створює три `cvxpy` variables: non-negative `charge_mw`, non-negative `discharge_mw` і `soc_mwh` довжиною `horizon + 1`. Objective максимізує суму `market_value_uah - degradation_penalty_uah`, де market value рахується як `price * (discharge_mw - charge_mw) * dt`, а degradation penalty - як throughput penalty з `BatteryPhysicalMetrics.degradation_cost_per_mwh_throughput_uah`. Constraints фіксують початковий SOC, enforce SOC transition з efficiency, тримають SOC у межах `soc_min_fraction` / `soc_max_fraction` і обмежують charge/discharge power через `max_power_mw`.

Після `cvxpy.Problem(...).solve()` код приймає тільки `OPTIMAL` або `OPTIMAL_INACCURATE`; інші statuses піднімають `RuntimeError`. Результат перетворюється на список `BaselineSchedulePoint`, де кожна година містить forecast price, charge/discharge MW, SOC before/after, throughput, degradation penalty, gross market value і net objective value. Signed operator schedule визначається як `net_power_mw = discharge_mw - charge_mw`: додатне значення означає discharge/sell preview, від'ємне - charge/buy preview, нульове - hold.

API-шар у `api/main.py` не трактує цей output як executable bid. `/dashboard/baseline-lp-preview` резолвить tenant battery metrics, starting SOC і official OREE/forecast price context, викликає `solve_dispatch_from_forecast`, запускає projected SOC simulation і формує response з forecast rows, recommendation schedule, bid-style preview rows та economics. У response явно виставлено `market_execution_enabled=false`, `market_order_payload_emitted=false` і `preview_only=true`, тому навіть перша година з `committed_dispatch` лишається read-model preview, а не market-submittable `ProposedBid`.

Поточне тестове покриття перевіряє solver не лише ізольовано, а й у pipeline context. `tests/optimization/test_degradation_accounting.py` підтверджує, що LP degradation penalty узгоджена з throughput/EFC формулою. `tests/dfl/test_relaxed_dispatch.py` порівнює relaxed differentiable dispatch із strict LP fixture. `tests/strategy/test_forecast_strategy_evaluation.py` проганяє strict similar-day, NBEATSx і TFT forecast candidates через однаковий LP/oracle regret contour. `tests/api/test_main.py` перевіряє API behavior baseline preview, включно з використанням fresh hourly telemetry SOC.

Б.4. SOC handling

Таблиця Б.3. Джерела starting SOC

| Source path | Умова | Використання |
|---|---|---|
| `telemetry_live` | Є live telemetry row | Operator recommendation path використовує clamped current SOC з high confidence |
| `hourly_snapshot` | Є fresh hourly snapshot | Operator recommendation path використовує clamped `soc_close` з medium confidence |
| `telemetry_projected` | Є stale snapshot | SOC projection враховує перший tenant load/PV row і позначається як review-required |
| `tenant_default` | Немає достатньої telemetry | Використовується configured `initial_soc_fraction`; preview має low-confidence warning |
| baseline preview fallback | Fresh hourly snapshot або tenant default | `/dashboard/baseline-lp-preview` використовує спрощений baseline SOC resolution |

Б.5. Відокремлення LP від ML і weather signal

LP є optimizer, а не learner: він не тренує параметри і не вчиться на даних. ML-шар може бути upstream, коли NBEATSx/TFT створюють price scenario, або downstream/advisory, коли V2+/DFL/DT/HF shadow ранжують feasible candidates. Але physical meaning LP не змінюється: кожний candidate має пройти той самий deterministic schedule/value contour.

Dashboard weather signal `weather_bias` не є прямим LP input. Він пояснює можливий weather-associated price uplift у market-pulse UI, але не повинен описуватися як causal control signal. Коректний майбутній шлях такий:

```text
weather + market history + calendar
  -> validated weather-aware price forecast
  -> deterministic LP preview schedule
  -> realized-value / oracle-regret benchmark
```

Б.6. Академічна інтерпретація

LP-контур відповідає price-taking storage-arbitrage постановці: charge при нижчих цінах, discharge при вищих цінах, з урахуванням SOC, power limit, efficiency losses і degradation proxy. Він потрібен у роботі не для автоматичного bidding, а для чесного downstream evaluator: forecast або selector вважається корисним лише тоді, коли після однакового LP/scoring path він зменшує regret або підвищує decision value без порушення safety/source boundaries.
