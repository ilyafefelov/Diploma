# Version 1.2 Differentiable DFL Preregistration

Status: frozen before the v1.2 differentiable-model runs on 2026-07-13.

## Purpose and claim boundary

This protocol tests whether a price-correction model trained through a
differentiable storage-dispatch layer improves later realized decision value.
It does not revise the defended thesis or the immutable v1.1 correction. It is
a research-shadow experiment with `market_execution_enabled=false`.

The experiment is not a full predict-then-bid controller. It contains a price
prediction/correction model, a differentiable storage optimizer, and a frozen
strict LP/oracle evaluator. It does not model strategic market clearing,
market submission, or settlement. Until V13 source readiness passes, every
result remains non-promotable and must report
`promotable_v13_permitted_training_rows=0`.

## Frozen comparators

1. Schedule/Value Learner V2+ remains the primary result and fallback.
2. The raw point-in-time NBEATSx and TFT paths are prediction baselines.
3. The existing time-separated Decision Transformer suite is retained as
   negative sequence-policy evidence and is not relabeled as DFL.
4. The historical artifact `dt_v2_plus` remains identified as a random forest;
   its exact-mirror result is excluded from model selection.

## Data and temporal protocols

- Inputs are point-in-time forecast vectors and prior-known calendar/profile
  context. Realized prices are labels only.
- Evaluation uses the same zero-content-overlap rolling protocols as the
  post-defense DT suite: evaluation window 1 may train on windows 4, 3, and 2;
  evaluation window 2 may train on windows 4 and 3; evaluation window 3 may
  train on window 4.
- Both calibrated and raw NBEATSx source lanes are evaluated separately.
- Seeds are fixed at `42`, `2026`, and `7`.
- Hyperparameters and checkpoint selection may use training data and an
  earlier inner validation split only. No evaluation-window outcome may select
  a model, threshold, epoch, seed, or reported subgroup.
- Exact candidate/input content overlap between training and evaluation must
  be zero. A failing overlap audit invalidates the affected run.

## Models and ablations

The frozen suite contains:

1. identity/raw forecast with the same relaxed storage layer;
2. a small feed-forward residual price corrector;
3. a transformer-encoder residual price corrector using hourly tokens;
4. forecast-loss training for each trainable architecture;
5. decision-focused training for each trainable architecture, backpropagating
   realized storage value through `cvxpylayers`;
6. strict LP/oracle rescoring of all produced schedules and comparison with
   V2+ on the identical evaluation rows.

The relaxed layer enforces charge/discharge power limits, SOC bounds,
split round-trip efficiency, degradation cost, and terminal SOC equality. A
small convex regularizer may be used for numerical uniqueness. Strict scoring,
not relaxed loss, is the final authority.

## Primary outcomes

The primary model outcome is paired mean realized-regret delta versus V2+ on
each genuinely later evaluation protocol. A negative delta means improvement.
The suite also reports median regret, beneficial/tie/harmful run counts, and a
date-cluster bootstrap interval where the date denominator permits it.

Promotion is not supported unless all of the following hold without selecting
a favorable seed or subgroup after inspection:

- lower mean regret than V2+ in every declared evaluation protocol for both
  source lanes;
- no increase in tail-loss count;
- zero physical/safety violations under strict evaluation;
- stable direction across all three seeds;
- V13 source readiness and its separate execution boundary are satisfied.

Failure of any condition is publishable negative evidence, not a reason to
change the protocol.

## Secondary transformer signals

The following are declared secondary and cannot be presented as control-value
improvement:

- price MAE and rank correlation;
- relaxed decision loss and relaxed realized value;
- candidate/schedule ranking accuracy where a common candidate set exists;
- tail-loss discrimination and calibration where both classes exist;
- gradient stability and feasibility rate;
- transformer-minus-feed-forward deltas under the same objective and split.

A positive transformer signal is reportable only when the metric was declared
above, is calculated over the full declared slice, and its limitation is stated
next to it. Training loss alone is not evidence of generalization.

## V13 source gate

Public OREE/PXS probing is allowed to establish availability, but retrieval
time, HTTP `Date`, a market-rule deadline, and first-seen observation time must
not be converted into `source_publication_timestamp`. The gate may close only
with explicit row-level publication evidence accepted by the V13 contract,
for example a valid authenticated and source-signed XMtrade/SCMO export. If
credentials or such an export are unavailable, the paper must say that the
public acquisition path remains externally blocked.

## Release rule

The next paper artifact is v1.2. Version v1.1 and the defended thesis remain
immutable archival records. V1.2 must include the preregistered protocol,
machine-readable run summaries, negative results, and any positive secondary
signal with its exact denominator and claim boundary.
