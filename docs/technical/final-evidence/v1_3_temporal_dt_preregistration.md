# v1.3 temporal Decision Transformer preregistration

Status: **frozen protocol; execution blocked by V13 source readiness**.

## Research question

On a causal, time-ordered BESS episode corpus, does a return-conditioned
Decision Transformer improve strict realized-price regret over the frozen LP
teacher and the forecast-loss / hybrid DFL comparators without using realized
price, reward, return-to-go, candidate value, regret, or target action in its
policy state?

## Dataset and state contract

The only eligible input is the v1.3 causal temporal corpus:

- 1,825 chronological episodes, 24 steps each, across five configured BESS
  profiles and 365 anchors per profile;
- state inputs: p10/p50/p90 forecasts, Ukrainian lag-24 price, weather,
  calendar, governed Poland lag-24, and prior SOC;
- action target: strict-LP teacher signed dispatch at the same horizon step;
- labels available after delivery only: realized price, interval reward,
  degradation penalty, and return-to-go.

Forbidden state fields include actual price, any realized reward/return-to-go,
oracle value, regret, schedule value, candidate index, candidate family, and
the action being predicted. Evaluation must fail closed if any forbidden field
appears in the state tensor.

## Chronological protocol

For each tenant, use the frozen 319 / 28 / 18 anchor split:

1. train only on the first 319 episodes;
2. select checkpoint, context length, RTG target scale, and all early-stopping
   decisions only on the next 28 episodes;
3. freeze the selected model and evaluate once on the final 18 future episodes.

Episodes may not be split within an anchor, and no same-date profile outcomes
may be treated as independent market dates. The final test consists of 18
distinct market dates and five profile outcomes per date.

## Model and training

- model: return-conditioned temporal transformer, two layers, hidden dimension
  32, two heads, context length 24;
- input state is the fixed causal state contract above; previous actions are
  shifted teacher actions during training and generated autoregressively at
  evaluation;
- train for at most 200 epochs with early stopping selected from validation
  action MSE; seed set is `20260713`, `20260714`, and `20260715`;
- target action is a signed MW dispatch, projected through the existing
  deterministic SOC/power gate before strict scoring;
- RTG is normalized using training episodes only and is never reconstructed
  from test actuals during policy inference.

## Evaluation and claims

For every frozen test prediction, use the same tenant battery capacity, power,
SOC bounds, efficiency, degradation cost, starting SOC, and terminal-SOC policy
as the strict LP evaluator. Score actions against realized prices only after
projection. Report mean regret, median regret, per-date paired deltas, solver
status, projection failure count, and a date-cluster bootstrap interval.

The primary comparison is DT versus its forecast/LP teacher on the same test
episodes. DT versus released V2+ is a separate system-level comparison; it may
not replace V2+ unless that stricter gate passes. A non-improvement, equality,
or harm is reportable negative evidence.

## Hard gate

This protocol must not train, tune, or score a temporal DT while the official
V13 source-family gate has `ready_rows=0/5` and
`permits_model_training=false`. The current Poland-context corpus is
experimental and `dt_training_eligible=false`; it is preparation evidence only.
`market_execution_enabled=false` remains mandatory regardless of result.
