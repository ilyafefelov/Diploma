# DT V2+ Rule-Distillation Shadow Plan

## Summary
Add a separate manual research shadow that trains DT logits to recover the existing V2+ selector rule, instead of letting raw DT argmax drift toward the weaker V2 reference. This is distillation evidence only: V2+ remains the default comparator/fallback, no promotion, no execution, no `ProposedBid`.

Use `/tdd` vertical slices: one failing behavior test, minimal implementation, then the next behavior.

## Key Changes
- Add a new DT objective kind: `v2_plus_rule_distillation`.
- Keep existing `decision_aware_regret_value_ranking` unchanged.
- In the V2+ strict-row teacher adapter, add explicit selector labels:
  - `label_is_v2_plus_selected_candidate`
  - `label_v2_plus_selected_candidate_index`
  - `label_v2_plus_selected_candidate_id`
  - `label_v2_plus_rule_distillation_target=true` only for `selection_role=schedule_value_learner_v2_plus`.
- In sequence tensors, add a target mask over candidate positions so the model learns “which candidate in this anchor set is V2+,” not “each row’s own candidate index.”
- Add a listwise distillation loss over candidate self-scores:
  - score each candidate as `logits[position, candidate_index]`
  - softmax over candidates in the anchor set
  - target is the V2+ selected candidate position
  - default weights for this objective: `distillation_weight=1.0`, `cross_entropy_weight=0.0`, `decision_aware_ranking_weight=0.0`.

## Implementation Details
- Add metrics to DT evaluation:
  - `v2_plus_rule_recovery_rate`
  - `raw_distilled_argmax_mean_regret_uah`
  - `raw_distilled_argmax_median_regret_uah`
  - `raw_distilled_argmax_minus_v2_plus_mean_regret_uah`
  - `raw_distilled_argmax_win_loss_tie_vs_v2_plus`
  - keep existing regret/value and boundary metrics.
- Add a new packet script path:
  - output dir: `data/research_runs/week3_dt_v2_plus_distillation_shadow_current/`
  - summary: `dt_v2_plus_distillation_summary.json/md`
  - rows: `dt_v2_plus_distillation_selected_rows.csv`
- Add a Dagster manual asset under `gold_dfl_training`:
  - `dfl_dt_v2_plus_distillation_shadow_frame`
  - tags/metadata must keep `market_execution_enabled=false`, `dt_lava_ready=false`, `permits_model_training=false`, `promotion_gate_passed=false`.
- Add optional diagnostic preview source:
  - `preview_source_id="dt_v2_plus_distillation_shadow"`
  - API must reject artifacts containing `market_execution_enabled=true`, `dt_lava_ready=true`, `permits_model_training=true`, or promoted rows.
  - Dashboard label: `DT V2+ distillation shadow`; show as diagnostic, never default.

## Test Plan
- RED 1: teacher adapter marks exactly one V2+ distillation target per `(tenant, source_model, anchor_timestamp, split_name)` candidate set.
- RED 2: sequence packet exports the V2+ target mask and target candidate ids without changing candidate feasibility masks.
- RED 3: `run_dt_research_shadow_smoke(objective_kind="v2_plus_rule_distillation")` reports recovery metrics and keeps all execution/promotion flags false.
- RED 4: on synthetic V2+ strict rows, distillation raw argmax selects `schedule_value_learner_v2_plus` and matches V2+ regret.
- Regression: existing decision-aware DT tests still pass and keep conservative V2+ fallback behavior.
- API/dashboard tests: new source is available as non-default diagnostic and boundary validation rejects unsafe artifacts.
- Verification commands:
  - `.\.venv\Scripts\python.exe -m pytest tests/dfl/test_dt_research_shadow.py -q`
  - `.\.venv\Scripts\python.exe -m pytest tests/assets/test_dfl_research_assets.py::test_dfl_research_assets_are_registered -q`
  - `.\.venv\Scripts\python.exe -m pytest tests/api/test_main.py -k shadow_preview -q`
  - `npm -C dashboard exec -- vitest run app/utils/operatorShadowPreview.test.ts`
  - `.\.venv\Scripts\python.exe -m ruff check src tests api scripts`
  - `.\.venv\Scripts\python.exe -m mypy src/smart_arbitrage/dfl/dt_research_shadow.py src/smart_arbitrage/assets/gold/dfl_research.py`

## Assumptions
- This first implementation is a recoverability/distillation smoke using the current V2+ strict-row packet and mirrored research-shadow train rows. It must not claim out-of-sample promotion.
- Acceptance for the materialized smoke packet: `v2_plus_rule_recovery_rate >= 0.95`, raw distilled argmax mean regret within `1e-6 UAH` of V2+ if recovery is perfect, and all boundary flags false.
- V2+ remains the production/default comparator until a later explicit promotion gate proves a non-V2+ DT residual beats V2+ under frozen strict LP/oracle.
