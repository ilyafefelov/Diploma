# Version 1.3 Evidence Correction: Temporal DT Claim Boundary

Date: 2026-07-13

## Status

Version 1.3 is a new evidence release. It does not alter the defended thesis,
the immutable v1.1 correction, or the v1.2 archive.

Machine-readable audit:
`arxiv/evidence/lineage/v1_3_evidence_audit.json`.

## Correction

The v1.2 temporal DT suite is reclassified as an **invalid/non-causal
candidate-list diagnostic**. It must not be cited as a time-separated,
return-conditioned Decision Transformer policy result or as negative evidence
about a genuine DT policy.

The audit established two independent defects in the legacy packet:

1. Its sequence key was `(tenant_id, source_model_name, anchor_timestamp)`;
   tokens were candidate rows within one anchor, sorted by candidate index. It
   was not a time-ordered state/action trajectory.
2. Its state tensor included outcome-derived fields (`forecast_*_actual_overlap`,
   `schedule_value`, `regret_delta_vs_v2_plus`, and realized return-to-go), and
   it included the candidate-index target as a state feature. Zero content
   overlap between train and evaluation does not repair this point-in-time
   causality failure.

The stored 33 ties and three harmful runs therefore remain reproducible
artifacts, but their only permitted scope is:

> legacy candidate-list diagnostic, not causal DT policy evidence

## Unchanged evidence

The differentiable v1.2 suite remains valid only for its exact tested model:
a small residual MLP/one-layer transformer price corrector trained for six
epochs on 12--36 distinct fit dates, with a terminal-SOC-equality relaxed
training layer and an unconstrained-terminal-SOC strict evaluator. Its result
is negative for that small-data, mismatched-contract implementation. It is not
evidence that aligned DFL or transformer forecasting cannot improve V2+.

The V2+ retrospective result is unchanged: 174.77 UAH mean regret for the
calibrated source and 193.36 UAH for the raw source. It remains descriptive
retrospective evidence, not a prospective confirmation.

## v1.3 successor protocol

The next model path is a full-history, prior-only HF value-aligned candidate
ranker with V2+ fallback. It must train on all anchors available before each
decision period, choose its switch threshold on a strictly earlier validation
block, and evaluate on a frozen future date block. The follow-up aligned DFL
and true-DT studies are separate preregistered protocols; neither may reuse
the legacy candidate-list packet as policy evidence.

`market_execution_enabled=false` remains mandatory.
