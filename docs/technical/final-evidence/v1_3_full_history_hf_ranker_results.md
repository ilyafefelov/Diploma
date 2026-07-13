# v1.3 Full-History HF Candidate Ranker: Frozen Future-Test Result

Date: 2026-07-13

## Protocol actually run

This run uses the preregistered full-history protocol in
`v1_3_full_history_hf_ranker_preregistration.md`: 319 prior training anchors,
28 prior validation anchors, and 18 later future-test anchors for each of five
configured BESS profiles. The Hugging Face `DecisionTransformerModel` is used
only as a candidate-set encoder; it is not a temporal Decision Transformer
policy. Threshold selection occurred on validation only. The frozen test never
selected a threshold, architecture, seed, or candidate set.

The ranker evaluates an 18-token complete candidate universe. Its internal
V2+ reference is therefore **not** the published complete-library V2+ result
(174.7684 UAH calibrated-source mean regret). The complete-library headline
remains unchanged and is not superseded by this experiment.

## Results

| Source lane | Frozen test threshold | Internal V2+ mean regret | Ranker mean regret | Difference | Switches | Wins / losses |
|---|---:|---:|---:|---:|---:|---:|
| Calibrated | 0 UAH | 182.3037 UAH | 206.5037 UAH | +24.2000 UAH | 37 / 90 | 5 / 27 |
| Raw sensitivity | 0 UAH | 191.6432 UAH | 194.0300 UAH | +2.3868 UAH | 1 / 90 | 0 / 1 |

The calibrated lane is materially worse than its compatible frozen V2+
baseline. The raw sensitivity lane is also worse. Consequently this model does
not pass the preregistered 5% improvement, tail-risk, or cross-lane promotion
criteria. The safe operational result is the fallback: no model output is
promoted, and `market_execution_enabled=false` remains unchanged.

## Interpretation

This is valid negative evidence for this exact full-history HF candidate-set
encoder, feature set, candidate universe, and frozen protocol. It does **not**
show that transformers, DFL, or temporally valid Decision Transformers cannot
help. The next distinct experiment is aligned DFL: the same transformer must
be trained under forecast loss and under a validation-selected hybrid decision
loss, with matching storage contracts. A true DT experiment remains deferred
until a causal trajectory corpus exists.

## Reproducibility record

The ignored local run directory is
`.tmp_runtime/v1_3_full_history_hf_ranker_full_80/`. The runner is
`scripts/run_full_history_hf_candidate_ranker.py`; the split and safety
contracts are covered by `tests/dfl/test_full_history_hf_candidate_ranker.py`.
The machine-readable result summary is
`arxiv/evidence/lineage/v1_3_full_history_hf_ranker_result.json`.
