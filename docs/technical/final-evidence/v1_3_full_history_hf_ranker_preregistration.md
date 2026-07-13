# v1.3 Full-History HF Candidate Ranker Preregistration

Status: frozen before materializing the full-history candidate panel and before
training this ranker.

## Question

Can a prior-only transformer candidate ranker reduce strict LP/oracle regret
relative to a frozen V2+ reference in its fixed candidate universe, while
preserving V2+ fallback and tail-risk controls?

This is a **value-aligned transformer candidate ranker**, not a Decision
Transformer policy: candidate rows are a set to rank, and actions, rewards,
and returns-to-go are not used as policy tokens.

## Data and split

- Each tenant/source must have at least 293 earlier decision anchors before the
  validation block; 18-day packets are rejected by code.
- The last 28 anchors before the frozen future test block select threshold and
  all model hyperparameters.
- The future test block is strictly later than train and validation. It is not
  used to select epoch, seed, threshold, candidate subset, or reported source.
- Each anchor includes V2+ fallback plus a fixed 18-token candidate universe.
  It excludes candidate models unavailable for the entire 365-anchor panel;
  this availability filter and its resulting internal V2+ baseline are reported
  explicitly. A masked candidate-set model is required before testing the
  omitted intermittent candidates.
- The released V1.2 V2+ result (174.7684 UAH calibrated-source mean regret)
  remains a separate complete-library reference. A gain over the fixed-universe
  baseline is not a claim of improvement over that released result.

## Inputs and targets

Inputs may contain only prior-known forecast, calendar, tenant battery,
candidate-dispatch, degradation, and governed exogenous-context features. The
implementation reuses the `expanded_prior_context_v1` leakage guard.

Training labels are candidate regret delta versus V2+ and tail-risk status.
They may be used only for earlier train/validation anchors. Outcome fields,
including actual-price overlap, realized value, regret, and return-to-go, are
not model inputs.

The initial frozen training configuration is Hugging Face
`DecisionTransformerModel` used as a candidate-set encoder, hidden size 64,
two layers, two attention heads, seed 20260713, Adam learning rate 0.003, and
80 epochs. Threshold grid: 0, 5, 10, 20, and 50 UAH. This configuration is
selected before the first full-history test run; later changes require a new
preregistration and a new untouched test block.

## Selection and success criteria

- The frozen ranker predicts each candidate's regret delta and tail risk.
- A candidate may replace fixed-universe V2+ only if it beats the threshold selected on the
  validation block and passes configured tail-risk and deterministic safety
  gates; otherwise the output is V2+.
- Primary outcome: paired date-clustered mean regret delta versus fixed-universe
  V2+ on the
  future test block.
- Promotion requires a minimum 5% calibrated-source mean-regret improvement,
  no increased tail-loss count, zero strict safety violations, and the same
  direction on the raw-source sensitivity lane. Any failure remains research
  evidence only.

`market_execution_enabled=false` and all V13 promotion restrictions remain in
force.
