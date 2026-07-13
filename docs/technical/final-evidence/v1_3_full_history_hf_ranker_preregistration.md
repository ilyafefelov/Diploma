# v1.3 Full-History HF Candidate Ranker Preregistration

Status: frozen before materializing the full-history candidate panel and before
training this ranker.

## Question

Can a prior-only transformer candidate ranker reduce strict LP/oracle regret
relative to frozen V2+ while preserving V2+ fallback and tail-risk controls?

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
- Each anchor includes V2+ fallback plus a fixed candidate universe spanning
  Ukrainian V2+ families, governed Poland/TFT candidates where available, and
  preregistered robust/stochastic schedule candidates. Missing candidates must
  be represented as unavailable, not silently dropped after outcomes are seen.

## Inputs and targets

Inputs may contain only prior-known forecast, calendar, tenant battery,
candidate-dispatch, degradation, and governed exogenous-context features. The
implementation reuses the `expanded_prior_context_v1` leakage guard.

Training labels are candidate regret delta versus V2+ and tail-risk status.
They may be used only for earlier train/validation anchors. Outcome fields,
including actual-price overlap, realized value, regret, and return-to-go, are
not model inputs.

## Selection and success criteria

- The frozen ranker predicts each candidate's regret delta and tail risk.
- A candidate may replace V2+ only if it beats the threshold selected on the
  validation block and passes configured tail-risk and deterministic safety
  gates; otherwise the output is V2+.
- Primary outcome: paired date-clustered mean regret delta versus V2+ on the
  future test block.
- Promotion requires a minimum 5% calibrated-source mean-regret improvement,
  no increased tail-loss count, zero strict safety violations, and the same
  direction on the raw-source sensitivity lane. Any failure remains research
  evidence only.

`market_execution_enabled=false` and all V13 promotion restrictions remain in
force.
