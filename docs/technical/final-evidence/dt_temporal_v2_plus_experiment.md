# Time-Separated Return-Conditioned DT vs Frozen V2+

Date: 2026-07-13

## Question

Does a genuine return-conditioned Decision Transformer, or the same backbone
trained with a decision-aware regret/value ranking objective, improve on frozen
Schedule/Value Learner V2+ when training and evaluation windows are genuinely
different?

## Protocol

- Backbone: Hugging Face `DecisionTransformerModel`, hidden size 64, two
  layers, two heads, context length 4.
- Inputs: point-in-time forecast/battery/tenant/candidate-value state features.
- Actions: feasible candidate index / schedule family, not hourly
  BUY/SELL/HOLD.
- Return conditioning: nonzero regret-delta return-to-go; every run has nonzero
  action targets and nonzero RTG values.
- Objectives: candidate-index cross entropy and cross entropy plus
  decision-aware regret/value ranking.
- Sources: raw and horizon-calibrated official global-panel NBEATSx.
- Temporal protocols: evaluation window 1 trains on 4/3/2, window 2 on 4/3,
  and window 3 on 4.
- Stability: seeds 42, 2026, and 7; three epochs fixed before the suite.
- Selection: frozen 20 UAH predicted-improvement threshold, 0.5 family
  tail-risk cap, V2+ fallback.
- Evaluator: the unchanged strict LP/realized-price oracle contour.
- Independence: zero model-input-plus-target candidate overlap in every
  protocol.

This is a real trained return-conditioned DT candidate policy. The
decision-aware objective is DFL-style candidate ranking, but it is not a full
differentiable price-forecast -> storage-optimization -> market-clearing stack.

## Results

| Objective | Runs | Better than V2+ | Exact V2+ ties | Worse than V2+ | Mean delta vs V2+ across runs |
|---|---:|---:|---:|---:|---:|
| Candidate-index cross entropy | 18 | 0 | 15 | 3 | +2.42 UAH |
| Decision-aware regret/value ranking | 18 | 0 | 18 | 0 | 0.00 UAH |
| Total | 36 | 0 | 33 | 3 | — |

The three harmful cross-entropy runs were:

| Source / window / seed | DT minus V2+ | Switches | Switch wins | Switch losses | Tail losses |
|---|---:|---:|---:|---:|---:|
| Calibrated / 1 / 42 | +10.12 UAH | 5 | 0 | 5 | 2 |
| Raw / 1 / 42 | +12.28 UAH | 7 | 0 | 7 | 3 |
| Raw / 3 / 7 | +21.09 UAH | 9 | 0 | 3 | 3 |

The remaining switch rows in the raw-window-3 case are exact V2+ ties. No
cross-entropy switch in a harmful run produced an observed win. The
decision-aware model selected the V2+ family directly or abstained to it; it
never made a non-V2+ switch in the primary suite.

## Epoch sensitivity

The three-epoch configuration is the primary predeclared suite. A diagnostic
ten-epoch sensitivity was then run after inspecting those results. Calibrated
window 1 at seed 42 and four raw-source window/seed combinations all returned
to exact V2+ ties at ten epochs. This does not turn the sensitivity into a
positive result. It shows that the imitation policy's harmful switches are
training-horizon sensitive rather than a stable learned advantage.

## Why the result looks like this

1. **V2+ is already a strong fallback.** It is a prior-only schedule/value
   selector, not a naive forecast, so a challenger needs a reliable conditional
   safe-switch signal rather than better token imitation.
2. **Cross entropy optimizes the wrong success criterion.** Reproducing a
   candidate label can lower classification loss while choosing schedules with
   worse realized LP/oracle regret.
3. **The decision-aware loss learns the conservative answer.** On the available
   point-in-time context it finds no repeatable reason to leave V2+, so a tie is
   correct negative evidence, not a model improvement.
4. **The useful switch signal is sparse and regime-dependent.** Each evaluation
   window contains 18 market dates shared by five profiles. Seeds test optimizer
   stability; they do not create independent market episodes.
5. **More transformer capacity cannot manufacture information.** The earlier
   RF mirror result disappeared under temporal separation, and the honest DT
   suite reaches the same substantive conclusion: candidate opportunities exist
   in hindsight, but are not predicted robustly from the current prior context.

## Decision

Do not promote the DT, do not replace V2+, and do not describe this as full
differentiable DFL. The experiment is publishable negative evidence because it
uses real return conditioning, a decision-aware comparator, and zero temporal
content overlap.

Before a full DFL experiment, close the V13 source-readiness blocker (currently
`ready_rows=0/5` because explicit OREE DAM/IDM publication evidence is still
missing), then pre-register a separate differentiable pipeline:

```text
point-in-time price/context model
-> differentiable relaxed storage optimizer
-> realized value/regret + degradation loss
-> frozen strict LP/oracle evaluation on later dates
```

DT can be tested as an optional sequence backbone inside that study, but DFL
and DT must remain separate ablations. `market_execution_enabled=false` remains
mandatory.

Compact machine-readable evidence:
`runs/dt_temporal_v2_plus/temporal_suite_summary.json` and
`runs/dt_temporal_v2_plus/temporal_suite_rows.csv`.
