# DFL V11 Lower-Tail-Risk Candidate Gate + DT/LAVA Bridge

## Purpose

V10 proved that oracle-template schedules did not transfer safely: generated
final-holdout templates became tail-risk instead of robust wins. V11 therefore
does **not** add another selector over the same risky candidate universe. It
uses the Ukrainian source-backed context gate first, then creates only bounded,
lower-tail-risk candidate schedules and strict-scores them before any DT/LAVA
candidate-index policy is allowed.

Frozen comparator remains Ukrainian-only calibrated V2+:

- mean regret: `174.77` UAH;
- median regret: `67.30` UAH;
- rolling robustness: `4 / 4`;
- `market_execution_enabled=false`.

## Assets

The V11 path is additive in `gold_dfl_training`:

- `dfl_lower_tail_risk_candidate_library_v11_frame`
- `dfl_lower_tail_risk_candidate_v11_strict_rescore_frame`
- `dfl_lower_tail_risk_candidate_value_teacher_label_panel_v11_frame`
- `dfl_candidate_value_regret_surrogate_v11_frame`
- `dfl_candidate_value_v11_strict_lp_benchmark_frame`
- `dfl_v11_lava_dt_candidate_policy_frame`
- `dfl_v11_lava_dt_comparison_frame`

Config:

- `configs/real_data_dfl_ua_context_acquisition_v11_precondition_week3.yaml`

## Materialized Result

Dagster run `c2cf573c-6da2-42bd-80fe-26500f745a0d` materialized the V11
path after the Ukrainian context acquisition gate was ready.

- `dfl_lower_tail_risk_candidate_library_v11_frame`: `23,499` candidate rows,
  including `4,300` V11 lower-tail-risk generated rows.
- `dfl_lower_tail_risk_candidate_value_teacher_label_panel_v11_frame`:
  `23,499` strict-scored teacher rows.
- `dfl_candidate_value_v11_strict_lp_benchmark_frame`: V11 candidate-value
  selector matched V2+ at `174.77` UAH mean regret and `67.30` UAH median regret.
- `dfl_v11_lava_dt_comparison_frame`: candidate-value V11, behavior cloning,
  and DT/LAVA candidate-index policy all fell back to V2+ and therefore matched
  V2+ at `174.77 / 67.30` UAH with zero safety violations and
  `market_execution_enabled=false`.

The blocker is explicit: each tenant has only `2-7` prior material safe-switch
examples, below the configured `20` examples required before DT/LAVA candidate
index training is allowed. V11 therefore remains valid diagnostic evidence, not
a promoted policy.

## Candidate Families

V11 uses source-backed context readiness as a hard precondition. If the
Ukrainian context gate is not ready for an anchor, V11 emits no generated
candidate for that anchor.

When ready, it creates three bounded families from V2+ and strict fallback:

- `v2_terminal_reserve_clip` - reduces late-window discharge exposure to protect
  terminal SOC;
- `v2_volatility_cap_clip` - halves V2+ dispatch exposure when tail-risk is the
  main concern;
- `strict_guard_blend` - blends V2+ with strict fallback rather than importing
  oracle-template shapes.

These are feasible schedule candidates, not forecast rows. Every generated row
starts as `pending_strict_rescore` and cannot be used by a selector until the
unchanged strict LP/oracle labels are rebuilt.

## DT/LAVA Boundary

The DT/LAVA bridge in this slice predicts a candidate index / schedule family
choice, not raw hourly BUY/SELL/HOLD actions. It is blocked unless the V11
teacher panel contains enough prior/train non-tail-risk material safe switches.

Promotion still requires:

- mean regret at least `5%` better than V2+;
- median regret not worse than `67.30` UAH;
- rolling robustness `4 / 4`;
- zero safety violations;
- `market_execution_enabled=false`.

If the V11 teacher labels are still sparse, the correct result is diagnostic:
V2+ remains the thesis headline and the next improvement must come from better
Ukrainian context or safer feasible schedule design.

## Literature Link

Decision-focused storage papers support optimizing downstream regret/value
rather than raw forecast error. That does not imply that a Decision Transformer
should be trained on weak or tail-risk trajectories. For this project, DT/LAVA
is only valid after the candidate/value label layer shows transferable,
prior-safe switch examples.
