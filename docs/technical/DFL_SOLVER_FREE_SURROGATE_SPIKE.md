# DFL Solver-Free Surrogate Spike

Date: 2026-05-17

This note records how to use the Solver-Free-DFL research direction without
destabilizing the current thesis evidence stack. The target is narrow: improve
or explain the current Schedule/Value Learner V2+ result under the unchanged
strict LP/oracle gate. If the spike cannot do that, it stays a source-backed
future-work note.

Claim boundary: this is offline/read-model research planning only. It is not
full DFL, not a deployed Decision Transformer controller, not live market
execution, and not a replacement for `strict_similar_day` or V2+ evidence.
`market_execution_enabled=false` remains mandatory.

## Current Baseline To Beat Or Explain

The current thesis-facing comparator is the official global-panel
Schedule/Value Learner V2+ evidence:

| Source | Strict mean regret | Frozen V2 mean regret | V2+ mean regret | V2+ improvement vs strict | V2+ improvement vs V2 |
|---|---:|---:|---:|---:|---:|
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 310.58 | 206.37 | 174.77 | 43.73% | 15.31% |
| `nbeatsx_official_global_panel_v1` | 310.58 | 225.44 | 193.36 | 37.74% | 14.23% |

Both sources passed 4 / 4 rolling robustness windows. The calibrated source is
the current thesis headline. Any solver-free surrogate work must compare against
that state, not only against raw forecasts, compact candidates, or frozen V2.

Primary local anchors:

- [DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS.md](DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS.md)
- [DFL_OFFICIAL_SCHEDULE_VALUE_PROMOTION.md](DFL_OFFICIAL_SCHEDULE_VALUE_PROMOTION.md)
- [RESEARCH_INTEGRATION_PLAN.md](RESEARCH_INTEGRATION_PLAN.md)

## Source-Backed Idea

The useful external idea is not the whole reference codebase. It is the training
principle from Solver-Free Decision-Focused Learning for Linear Optimization
Problems:

- paper: [arXiv:2505.22224](https://arxiv.org/abs/2505.22224)
- code: [ML-KULeuven/Solver-Free-DFL](https://github.com/ML-KULeuven/Solver-Free-DFL)

The paper targets linear optimization DFL and reduces training cost by avoiding
an optimizer call during every loss evaluation. It does this by using the
geometry of the feasible LP polytope and comparing a ground-truth optimal
solution against adjacent vertices.

For this project, do not copy that stack directly:

- do not add Gurobi as a thesis dependency for this spike;
- do not introduce PyEPO unless a later implementation decision explicitly needs
  it;
- do not switch the repo to Python 3.13 to match the reference experiments;
- do not add D4RL, MuJoCo, or generic Decision Transformer benchmark scaffolds;
- do not weaken strict LP/oracle final scoring.

## BESS Mapping

The project already has a V2+ schedule candidate library. That gives us a safer
project-native analogue of "adjacent vertices":

| Solver-Free-DFL concept | Project-local analogue |
|---|---|
| Predicted LP cost coefficients | Forecast price vector and schedule-scoring features |
| Ground-truth optimal solution | Oracle / strict LP scored best candidate for the anchor |
| Adjacent vertices | Deterministic schedule-neighbor families in the V2+ candidate library |
| Solver-free training signal | Prior-only surrogate margin over schedule neighbors |
| Final decision quality | Strict LP/oracle UAH regret on final holdout and rolling windows |

This is an analogy, not a mathematical claim that the existing V2+ candidate
families are exact LP-polytope adjacent vertices. The thesis-safe term should be
**schedule-neighbor surrogate**, not "implemented Solver-Free-DFL", unless a
later implementation proves the exact LP adjacency relation.

## Spike Hypotheses

The spike is useful only if it answers at least one of these questions:

1. Can a prior-only schedule-neighbor surrogate select a candidate that improves
   mean regret versus V2+ without median degradation?
2. If it cannot improve V2+, can it explain where V2+ still loses value by
   ranking the strict-best or near-best candidate families on final-holdout
   anchors?
3. Can the surrogate reduce the need for expensive inner training or repeated
   solver calls while preserving the same strict LP/oracle evidence gate?

## Evidence Design

Use existing evidence before adding code:

1. Start from the exported V2+ packet:
   `data/research_runs/week3_official_global_panel_schedule_value_v2_plus_comparison/`.
2. Use `dfl_schedule_value_regret_decomposition_frame` and
   `dfl_schedule_candidate_library_v2_plus_frame` as the first data sources.
3. Compute surrogate diagnostics offline from prior/train-selection rows only.
4. Score final-holdout rows only with the unchanged strict LP/oracle evaluator.
5. Attach rolling-window diagnostics before making any thesis-facing claim.

Minimum diagnostic metrics:

- candidate-family rank correlation between surrogate score and strict regret;
- top-1 and top-3 coverage of the strict-best candidate family;
- mean and median regret for surrogate-selected candidates versus V2+;
- regret-decomposition coverage of V2+ failure modes;
- rolling-window pass count versus V2+ and `strict_similar_day`;
- `not_full_dfl=true`, `not_market_execution=true`, and
  `market_execution_enabled=false`.

## Promotion Or Future-Work Rule

Promote the spike into implementation only if it passes one of these gates:

| Outcome | Requirement | Documentation result |
|---|---|---|
| Improvement | Beats V2+ mean regret by at least 5%, does not worsen median regret, preserves 4 / 4 rolling robustness, and keeps zero safety violations | Candidate implementation issue or next-slice plan |
| Explanation | Does not beat V2+, but identifies a stable V2+ failure mode and ranks the strict-best or near-best schedule family well enough to guide a concrete next candidate-library change | Evidence note plus targeted follow-up |
| Negative | Does not beat V2+ and does not explain remaining regret | Keep as future work only |

No result may be described as full end-to-end DFL, Solver-Free-DFL deployment,
Decision Transformer control, or market execution unless a separate future slice
implements and verifies those claims directly.

## Archived Outcome: 2026-05-17

The spike was implemented and run as a local proof, then archived as a negative
result rather than promoted.

Runtime archive:

- `.tmp_runtime/solver_free_surrogate_real_run/summary.md`
- `.tmp_runtime/solver_free_surrogate_real_run/summary.json`
- `.tmp_runtime/solver_free_surrogate_improvement_scratch/summary.md`
- `.tmp_runtime/solver_free_surrogate_improvement_scratch/summary.json`
- `.tmp_runtime/solver_free_surrogate_improvement_scratch/ridge_narrow_summary.json`

Real V2+ candidate-library run:

- input candidate-library rows: 65,560;
- solver-free proof rows: 10 tenant/source rows;
- strict LP/oracle comparison rows: 900;
- all rows preserved `not_full_dfl=true`, `not_market_execution=true`,
  `market_execution_enabled=false`, and zero safety violations;
- calibrated source V2+ mean regret: 174.77 UAH;
- calibrated source solver-free surrogate mean regret: 174.77 UAH;
- raw source V2+ mean regret: 193.36 UAH;
- raw source solver-free surrogate mean regret: 193.36 UAH.

The selector fell back to V2+ for every tenant/source row. A follow-up scratch
sweep tried 79 deterministic prior-only selector variants and 24 ridge scorer
variants. No variant met the 5% mean-regret improvement gate without median
degradation. The best no-fallback final-only opportunities were small and did
not have strong train-selection evidence.

Decision: do not promote this as thesis improvement evidence, do not add a
Dagster asset/API/dashboard surface, and do not keep a tracked solver-free
surrogate code path until new candidate-library families or stronger prior
features exist. Solver-Free-DFL remains source-backed future work only.

## Implementation Boundary

If this becomes code, keep the first implementation doc-driven and additive:

- likely module: `src/smart_arbitrage/dfl/solver_free_surrogate.py`;
- likely tests: `tests/dfl/test_solver_free_surrogate.py`;
- likely docs update: this file plus
  [RESEARCH_INTEGRATION_PLAN.md](RESEARCH_INTEGRATION_PLAN.md);
- likely asset only after the pure function is proven:
  `dfl_schedule_value_solver_free_surrogate_frame`;
- no API/dashboard default switch;
- no dependency changes unless the pure Polars/Python diagnostic is insufficient.

The first code slice should be RED/GREEN around small fixture rows that prove:

- final-holdout rows cannot influence surrogate fitting;
- V2+ remains fallback when the surrogate has weak prior evidence;
- strict LP/oracle regret remains the only promotion metric;
- all output rows preserve the offline/read-model claim flags.
