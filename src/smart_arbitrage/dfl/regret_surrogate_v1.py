"""Learning-limit audit and regret-surrogate DFL v1.

This slice does not add another raw DT policy. It first checks whether the
current feasible candidate universe has enough oracle-switch upside to justify
learning. If there is signal, it trains a conservative candidate-value scorer
that predicts regret delta versus corrected calibrated V2+ and falls back to
V2+ on weak or high-tail-risk cases.
"""

from __future__ import annotations

from datetime import datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)

LEARNING_LIMIT_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_v2_plus_learning_limit_audit_not_full_dfl"
)
EXPANDED_TEACHER_LABEL_CLAIM_SCOPE: Final[str] = (
    "dfl_expanded_schedule_value_teacher_labels_not_full_dfl"
)
REGRET_SURROGATE_FORECAST_CORRECTION_CLAIM_SCOPE: Final[str] = (
    "dfl_regret_surrogate_forecast_correction_not_full_dfl"
)
REGRET_SURROGATE_CANDIDATE_VALUE_CLAIM_SCOPE: Final[str] = (
    "dfl_regret_surrogate_candidate_value_not_full_dfl"
)
REGRET_SURROGATE_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_regret_surrogate_strict_lp_gate_not_full_dfl"
)
REGRET_SURROGATE_ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "dfl_regret_surrogate_rolling_robustness_not_full_dfl"
)
REGRET_SURROGATE_CONTEXT_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_regret_surrogate_safe_switch_context_audit_not_full_dfl"
)
REGRET_SURROGATE_TEACHER_V2_CLAIM_SCOPE: Final[str] = (
    "dfl_regret_surrogate_teacher_context_v2_not_full_dfl"
)
REGRET_SURROGATE_CONTEXTUAL_CANDIDATE_VALUE_CLAIM_SCOPE: Final[str] = (
    "dfl_regret_surrogate_contextual_candidate_value_v2_not_full_dfl"
)
REGRET_SURROGATE_CONTEXTUAL_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_regret_surrogate_contextual_strict_lp_gate_not_full_dfl"
)
REGRET_SURROGATE_CONTEXTUAL_ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "dfl_regret_surrogate_contextual_rolling_robustness_not_full_dfl"
)
REGRET_SURROGATE_SPARSE_FEATURE_CONTRACT_CLAIM_SCOPE: Final[str] = (
    "dfl_sparse_safe_switch_feature_contract_not_full_dfl"
)
REGRET_SURROGATE_SPARSE_OPPORTUNITY_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_sparse_safe_switch_opportunity_audit_not_full_dfl"
)
REGRET_SURROGATE_SPARSE_CANDIDATE_LIBRARY_CLAIM_SCOPE: Final[str] = (
    "dfl_sparse_safe_switch_candidate_library_v6_not_full_dfl"
)
REGRET_SURROGATE_SPARSE_TEACHER_CLAIM_SCOPE: Final[str] = (
    "dfl_sparse_safe_switch_teacher_v6_not_full_dfl"
)
REGRET_SURROGATE_SPARSE_MODEL_CLAIM_SCOPE: Final[str] = (
    "dfl_sparse_safe_switch_abstention_model_v6_not_full_dfl"
)
REGRET_SURROGATE_SPARSE_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_sparse_safe_switch_strict_lp_gate_not_full_dfl"
)
REGRET_SURROGATE_SPARSE_ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "dfl_sparse_safe_switch_rolling_robustness_not_full_dfl"
)
REGRET_SURROGATE_BACKFILL_REQUIREMENTS_CLAIM_SCOPE: Final[str] = (
    "dfl_v2_plus_opportunity_backfill_requirements_not_full_dfl"
)
REGRET_SURROGATE_BACKFILLED_CONTEXT_V7_CLAIM_SCOPE: Final[str] = (
    "dfl_backfilled_context_feature_panel_v7_not_full_dfl"
)
REGRET_SURROGATE_FEASIBLE_CANDIDATE_LIBRARY_V7_CLAIM_SCOPE: Final[str] = (
    "dfl_feasible_schedule_candidate_library_v7_not_full_dfl"
)
REGRET_SURROGATE_TEACHER_V7_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_teacher_v7_not_full_dfl"
)
REGRET_SURROGATE_MODEL_V7_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_regret_surrogate_v7_not_full_dfl"
)
REGRET_SURROGATE_STRICT_V7_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_v7_strict_lp_gate_not_full_dfl"
)
REGRET_SURROGATE_ROBUSTNESS_V7_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_v7_rolling_robustness_not_full_dfl"
)
REGRET_SURROGATE_UA_CONTEXT_BACKFILL_V8_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_backfilled_feature_panel_v8_not_full_dfl"
)
REGRET_SURROGATE_UA_CANDIDATE_LIBRARY_V8_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_feasible_candidate_library_v8_not_full_dfl"
)
REGRET_SURROGATE_UA_CANDIDATE_STRICT_RESCORE_V8_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_candidate_v8_strict_rescore_not_full_dfl"
)
REGRET_SURROGATE_UA_TEACHER_V8_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_candidate_value_teacher_v8_not_full_dfl"
)
REGRET_SURROGATE_MODEL_V8_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_regret_surrogate_v8_not_full_dfl"
)
REGRET_SURROGATE_STRICT_V8_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_v8_strict_lp_gate_not_full_dfl"
)
REGRET_SURROGATE_ROBUSTNESS_V8_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_v8_rolling_robustness_not_full_dfl"
)
REGRET_SURROGATE_V8_FALSE_POSITIVE_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_v8_false_positive_tail_risk_audit_not_full_dfl"
)
REGRET_SURROGATE_V8_PRUNED_FAMILY_PLAN_CLAIM_SCOPE: Final[str] = (
    "dfl_v8_pruned_candidate_family_plan_not_full_dfl"
)
REGRET_SURROGATE_V8_PRUNED_CANDIDATE_LIBRARY_CLAIM_SCOPE: Final[str] = (
    "dfl_v8_pruned_candidate_library_not_full_dfl"
)
REGRET_SURROGATE_V8_PRUNED_TEACHER_CLAIM_SCOPE: Final[str] = (
    "dfl_v8_pruned_candidate_value_teacher_not_full_dfl"
)
REGRET_SURROGATE_V8_PRUNED_SELECTOR_CLAIM_SCOPE: Final[str] = (
    "dfl_v8_pruned_candidate_value_selector_not_full_dfl"
)
REGRET_SURROGATE_V8_PRUNED_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_v8_pruned_candidate_value_strict_lp_gate_not_full_dfl"
)
REGRET_SURROGATE_UA_PRIOR_CONTEXT_V9_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_prior_context_backfilled_feature_panel_v9_not_full_dfl"
)
REGRET_SURROGATE_UA_NON_TAIL_RISK_CANDIDATE_LIBRARY_V9_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_non_tail_risk_candidate_library_v9_not_full_dfl"
)
REGRET_SURROGATE_UA_NON_TAIL_RISK_STRICT_RESCORE_V9_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_non_tail_risk_candidate_v9_strict_rescore_not_full_dfl"
)
REGRET_SURROGATE_UA_NON_TAIL_RISK_TEACHER_V9_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_non_tail_risk_candidate_value_teacher_v9_not_full_dfl"
)

REGRET_SURROGATE_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_regret_surrogate_strict_lp_benchmark"
)
REGRET_SURROGATE_MODEL_NAME: Final[str] = "dfl_regret_surrogate_candidate_value_v1"
REGRET_SURROGATE_CONTEXTUAL_MODEL_NAME: Final[str] = (
    "dfl_regret_surrogate_contextual_candidate_value_v2"
)
REGRET_SURROGATE_SELECTION_ROLE: Final[str] = "regret_surrogate_candidate_value"
REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE: Final[str] = (
    "regret_surrogate_contextual_candidate_value"
)
REGRET_SURROGATE_CONTEXTUAL_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_regret_surrogate_contextual_strict_lp_benchmark"
)
REGRET_SURROGATE_SPARSE_SAFE_SWITCH_MODEL_NAME: Final[str] = (
    "dfl_sparse_safe_switch_abstention_model_v6"
)
REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE: Final[str] = (
    "sparse_safe_switch_abstention_v6"
)
REGRET_SURROGATE_SPARSE_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_sparse_safe_switch_strict_lp_benchmark"
)
REGRET_SURROGATE_CANDIDATE_VALUE_V7_MODEL_NAME: Final[str] = (
    "dfl_candidate_value_regret_surrogate_v7"
)
REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE: Final[str] = (
    "candidate_value_regret_surrogate_v7"
)
REGRET_SURROGATE_CANDIDATE_VALUE_V7_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_candidate_value_v7_strict_lp_benchmark"
)
REGRET_SURROGATE_CANDIDATE_VALUE_V8_MODEL_NAME: Final[str] = (
    "dfl_candidate_value_regret_surrogate_v8"
)
REGRET_SURROGATE_CANDIDATE_VALUE_V8_SELECTION_ROLE: Final[str] = (
    "candidate_value_regret_surrogate_v8"
)
REGRET_SURROGATE_CANDIDATE_VALUE_V8_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_candidate_value_v8_strict_lp_benchmark"
)
REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_MODEL_NAME: Final[str] = (
    "dfl_v8_pruned_candidate_value_selector"
)
REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_SELECTION_ROLE: Final[str] = (
    "v8_pruned_candidate_value_selector"
)
REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_v8_pruned_candidate_value_strict_lp_benchmark"
)
V2_PLUS_REFERENCE_ROLE: Final[str] = "schedule_value_learner_v2_plus_reference"
STRICT_REFERENCE_ROLE: Final[str] = "strict_reference"

_REFERENCE_CANDIDATE_SOURCES: Final[frozenset[str]] = frozenset(
    {"v2_plus_default", "strict_fallback"}
)
_V2_PLUS_CANDIDATE_SOURCE: Final[str] = "v2_plus_default"
_STRICT_CANDIDATE_SOURCE: Final[str] = "strict_fallback"
_DEFAULT_ALLOWED_CANDIDATE_SOURCES: Final[tuple[str, ...]] = (
    "oracle_gap_candidate",
    "poland_shadow_candidate",
    "tft_shadow_candidate",
    "ua_context_candidate",
    "lava_candidate",
)
_V7_GENERATED_CANDIDATE_SOURCE: Final[str] = "v7_generated_candidate"
_V7_ALLOWED_CANDIDATE_SOURCES: Final[tuple[str, ...]] = (
    _V7_GENERATED_CANDIDATE_SOURCE,
    *_DEFAULT_ALLOWED_CANDIDATE_SOURCES,
)
_V8_GENERATED_CANDIDATE_SOURCE: Final[str] = "ua_context_v8_generated_candidate"
_V8_ALLOWED_CANDIDATE_SOURCES: Final[tuple[str, ...]] = (
    _V8_GENERATED_CANDIDATE_SOURCE,
    _V7_GENERATED_CANDIDATE_SOURCE,
    *_DEFAULT_ALLOWED_CANDIDATE_SOURCES,
)
_V9_GENERATED_CANDIDATE_SOURCE: Final[str] = "ua_context_v9_generated_candidate"

_REQUIRED_CANDIDATE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_source",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "generated_at",
        "split_name",
        "horizon_hours",
        "eligible_for_final_selection",
        "forecast_price_uah_mwh_vector",
        "actual_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "v2_plus_baseline_regret_uah",
        "label_regret_delta_vs_v2_plus_uah",
        "label_safe_switch_win",
        "label_tail_risk_loss",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "safety_violation_count",
        "evaluation_payload",
        "target_label_space",
        "raw_hourly_action_imitation",
        "market_execution_enabled",
    }
)
_REQUIRED_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "learning_limit_failure_mode",
        "candidate_universe_can_beat_v2_plus_gate",
        "recommended_next_branch",
        "market_execution_enabled",
    }
)
_REQUIRED_SCORER_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "selected_final_candidate_keys",
        "fallback_final_anchor_keys",
        "predicted_final_candidate_deltas",
        "predicted_final_tail_risk_probabilities",
        "market_execution_enabled",
        "raw_hourly_action_imitation",
    }
)


def build_dfl_v2_plus_learning_limit_audit_frame(
    candidate_feature_panel_frame: pl.DataFrame,
    *,
    min_oracle_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    tail_risk_delta_uah: float = 150.0,
) -> pl.DataFrame:
    """Audit whether the candidate universe can theoretically beat V2+."""

    _validate_candidate_panel(candidate_feature_panel_frame)
    if min_oracle_improvement_ratio_vs_v2_plus < 0.0:
        raise ValueError(
            "min_oracle_improvement_ratio_vs_v2_plus must not be negative."
        )
    if tail_risk_delta_uah <= 0.0:
        raise ValueError("tail_risk_delta_uah must be positive.")

    rows = list(candidate_feature_panel_frame.iter_rows(named=True))
    grouped = _group_by_anchor(rows)
    oracle_regrets: list[float] = []
    v2_regrets: list[float] = []
    anchor_summaries: list[dict[str, Any]] = []
    profile_stats = _profile_stats(_train_rows(rows))
    for anchor_key, anchor_rows in sorted(grouped.items()):
        baseline = _baseline_row(anchor_rows, anchor_key=anchor_key)
        candidates = _eligible_challengers(anchor_rows)
        best = min(
            [baseline, *candidates],
            key=lambda row: (
                float(row["regret_uah"]),
                str(row["candidate_source"]),
                str(row["candidate_family"]),
                str(row["candidate_model_name"]),
            ),
        )
        baseline_regret = float(baseline["regret_uah"])
        best_regret = float(best["regret_uah"])
        oracle_regrets.append(best_regret)
        v2_regrets.append(baseline_regret)
        best_profile = _profile_key(best)
        best_stats = profile_stats.get(best_profile, {})
        better_exists = best_regret < baseline_regret
        failure_mode = _learning_limit_failure_mode(
            better_exists=better_exists,
            best_source=str(best["candidate_source"]),
            best_delta=best_regret - baseline_regret,
            best_stats=best_stats,
            tail_risk_delta_uah=tail_risk_delta_uah,
        )
        anchor_summaries.append(
            {
                "tenant_id": anchor_key[0],
                "source_model_name": anchor_key[1],
                "anchor_timestamp": anchor_key[2],
                "split_name": str(baseline["split_name"]),
                "v2_plus_regret_uah": baseline_regret,
                "oracle_best_available_regret_uah": best_regret,
                "oracle_best_candidate_source": str(best["candidate_source"]),
                "oracle_best_candidate_family": str(best["candidate_family"]),
                "oracle_best_candidate_model_name": str(best["candidate_model_name"]),
                "oracle_switch_delta_vs_v2_plus_uah": best_regret - baseline_regret,
                "better_candidate_available": better_exists,
                "better_candidate_source_count": len(
                    [
                        row
                        for row in candidates
                        if float(row["regret_uah"]) < baseline_regret
                    ]
                ),
                "learning_limit_failure_mode": failure_mode,
                "profile_prior_safe_win_count": int(
                    best_stats.get("safe_win_count", 0)
                ),
                "profile_prior_tail_loss_count": int(
                    best_stats.get("tail_loss_count", 0)
                ),
                "profile_prior_mean_delta_uah": float(
                    best_stats.get("mean_delta_uah", 0.0)
                ),
            }
        )
    v2_mean = mean(v2_regrets)
    oracle_mean = mean(oracle_regrets)
    upper_bound_improvement = _improvement_ratio(v2_mean, oracle_mean)
    can_beat = upper_bound_improvement >= min_oracle_improvement_ratio_vs_v2_plus
    output_rows: list[dict[str, Any]] = []
    for row in anchor_summaries:
        copied = dict(row)
        copied.update(
            {
                "v2_plus_mean_regret_uah": v2_mean,
                "oracle_best_available_mean_regret_uah": oracle_mean,
                "oracle_upper_bound_improvement_ratio_vs_v2_plus": (
                    upper_bound_improvement
                ),
                "candidate_universe_can_beat_v2_plus_gate": can_beat,
                "recommended_next_branch": (
                    "regret_surrogate_dfl" if can_beat else "data_or_candidate_backfill"
                ),
                "claim_scope": LEARNING_LIMIT_AUDIT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        output_rows.append(copied)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp"]
    )


def build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame(
    candidate_feature_panel_frame: pl.DataFrame,
    learning_limit_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Publish expanded candidate-value teacher labels for regret-surrogate DFL."""

    _validate_candidate_panel(candidate_feature_panel_frame)
    _require_columns(
        learning_limit_audit_frame,
        _REQUIRED_AUDIT_COLUMNS,
        frame_name="learning_limit_audit_frame",
    )
    audit_by_anchor = {
        _anchor_key(row): row
        for row in learning_limit_audit_frame.iter_rows(named=True)
    }
    feature_names = _selector_feature_columns(candidate_feature_panel_frame)
    if not feature_names:
        raise ValueError("teacher label panel requires selector_feature_* columns.")
    rows: list[dict[str, Any]] = []
    for row in candidate_feature_panel_frame.iter_rows(named=True):
        key = _anchor_key(row)
        audit = audit_by_anchor.get(key)
        if audit is None:
            raise ValueError(f"missing learning-limit audit row for {key}.")
        delta = float(row["label_regret_delta_vs_v2_plus_uah"])
        copied = dict(row)
        copied.update(
            {
                "teacher_panel_version": "expanded_schedule_value_teacher_v1",
                "selected_feature_names": feature_names,
                "learning_limit_failure_mode": str(
                    audit["learning_limit_failure_mode"]
                ),
                "candidate_universe_can_beat_v2_plus_gate": bool(
                    audit["candidate_universe_can_beat_v2_plus_gate"]
                ),
                "recommended_next_branch": str(audit["recommended_next_branch"]),
                "is_training_row": (
                    str(row["split_name"]) != "final_holdout"
                    and bool(row["eligible_for_final_selection"])
                    and str(row["candidate_source"]) != _STRICT_CANDIDATE_SOURCE
                ),
                "label_expected_regret_delta_vs_v2_plus_uah": delta,
                "label_regret_surrogate_loss_weight": _loss_weight(
                    delta,
                    bool(row["label_tail_risk_loss"]),
                ),
                "target_label_space": "schedule_candidate_value_delta",
                "raw_hourly_action_imitation": False,
                "claim_scope": EXPANDED_TEACHER_LABEL_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        rows.append(copied)
    panel = pl.DataFrame(rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_teacher_panel(panel)
    return panel


def build_dfl_regret_surrogate_forecast_correction_v1_frame(
    expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    min_prior_safe_win_count: int = 1,
    min_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_predicted_tail_risk_probability: float = 0.25,
    allowed_candidate_sources: tuple[str, ...] = _DEFAULT_ALLOWED_CANDIDATE_SOURCES,
    use_cuda_if_available: bool = True,
) -> pl.DataFrame:
    """Fit a conservative regret-delta surrogate using train/prior rows only."""

    _validate_teacher_panel(expanded_schedule_value_teacher_label_panel_v1_frame)
    _validate_scorer_config(
        tenant_ids=tenant_ids,
        source_model_names=source_model_names,
        min_prior_safe_win_count=min_prior_safe_win_count,
        min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
        allowed_candidate_sources=allowed_candidate_sources,
    )
    rows = list(
        expanded_schedule_value_teacher_label_panel_v1_frame.iter_rows(named=True)
    )
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in source_model_names:
            scope_rows = [
                row
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            ]
            output_rows.append(
                _fit_scope_surrogate(
                    scope_rows,
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    min_prior_safe_win_count=min_prior_safe_win_count,
                    min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
                    min_predicted_improvement_uah=min_predicted_improvement_uah,
                    max_predicted_tail_risk_probability=(
                        max_predicted_tail_risk_probability
                    ),
                    allowed_candidate_sources=set(allowed_candidate_sources),
                    use_cuda_if_available=use_cuda_if_available,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def build_dfl_regret_surrogate_candidate_value_v1_frame(
    expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    regret_surrogate_forecast_correction_v1_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve final candidate choices from the fitted regret-surrogate scorer."""

    _validate_teacher_panel(expanded_schedule_value_teacher_label_panel_v1_frame)
    _validate_scorer_frame(regret_surrogate_forecast_correction_v1_frame)
    candidate_by_key = {
        _candidate_key(row): row
        for row in expanded_schedule_value_teacher_label_panel_v1_frame.iter_rows(
            named=True
        )
    }
    rows: list[dict[str, Any]] = []
    for scorer_row in regret_surrogate_forecast_correction_v1_frame.iter_rows(
        named=True
    ):
        selected_keys = [
            str(value) for value in scorer_row["selected_final_candidate_keys"]
        ]
        selected = [candidate_by_key[key] for key in selected_keys]
        rows.append(
            {
                "tenant_id": str(scorer_row["tenant_id"]),
                "source_model_name": str(scorer_row["source_model_name"]),
                "learner_model_name": REGRET_SURROGATE_MODEL_NAME,
                "target_label_space": "schedule_candidate_value_delta",
                "selected_final_candidate_keys": selected_keys,
                "fallback_final_anchor_keys": [
                    str(value) for value in scorer_row["fallback_final_anchor_keys"]
                ],
                "selected_final_candidate_count": len(selected_keys),
                "fallback_final_anchor_count": len(
                    scorer_row["fallback_final_anchor_keys"]
                ),
                "selected_final_family_counts": _family_counts(selected),
                "selected_final_candidate_source_counts": _source_counts(selected),
                "predicted_final_candidate_deltas": dict(
                    scorer_row["predicted_final_candidate_deltas"]
                ),
                "predicted_final_tail_risk_probabilities": dict(
                    scorer_row["predicted_final_tail_risk_probabilities"]
                ),
                "fallback_to_v2_plus": bool(scorer_row["fallback_to_v2_plus"]),
                "uses_v2_plus_anchor_fallback": bool(
                    scorer_row["uses_v2_plus_anchor_fallback"]
                ),
                "claim_scope": REGRET_SURROGATE_CANDIDATE_VALUE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
                "raw_hourly_action_imitation": False,
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def build_dfl_regret_surrogate_strict_lp_benchmark_frame(
    expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    regret_surrogate_candidate_value_v1_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict, V2+, and regret-surrogate rows under strict LP/oracle scoring."""

    _validate_teacher_panel(expanded_schedule_value_teacher_label_panel_v1_frame)
    _require_columns(
        regret_surrogate_candidate_value_v1_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "selected_final_candidate_keys",
                "fallback_final_anchor_keys",
                "market_execution_enabled",
            }
        ),
        frame_name="regret_surrogate_candidate_value_v1_frame",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        expanded_schedule_value_teacher_label_panel_v1_frame
    )
    panel_rows = list(
        expanded_schedule_value_teacher_label_panel_v1_frame.iter_rows(named=True)
    )
    candidate_by_key = {_candidate_key(row): row for row in panel_rows}
    v2_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in panel_rows:
        if str(row["split_name"]) != "final_holdout":
            continue
        source = str(row["candidate_source"])
        if source == _STRICT_CANDIDATE_SOURCE:
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=STRICT_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
        elif source == _V2_PLUS_CANDIDATE_SOURCE:
            v2_by_anchor[_anchor_key_string(row)] = row
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=V2_PLUS_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
    for scorer_row in regret_surrogate_candidate_value_v1_frame.iter_rows(named=True):
        for candidate_key in scorer_row["selected_final_candidate_keys"]:
            output_rows.append(
                _benchmark_row(
                    candidate_by_key[str(candidate_key)],
                    selection_role=REGRET_SURROGATE_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            fallback = v2_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"missing V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _benchmark_row(
                    fallback,
                    selection_role=REGRET_SURROGATE_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def build_dfl_regret_surrogate_rolling_robustness_frame(
    expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    min_prior_safe_win_count: int = 1,
    min_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_predicted_tail_risk_probability: float = 0.25,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    allowed_candidate_sources: tuple[str, ...] = _DEFAULT_ALLOWED_CANDIDATE_SOURCES,
    use_cuda_if_available: bool = True,
) -> pl.DataFrame:
    """Replay regret-surrogate selection over prior-only rolling windows."""

    _validate_teacher_panel(expanded_schedule_value_teacher_label_panel_v1_frame)
    if validation_window_count <= 0:
        raise ValueError("validation_window_count must be positive.")
    if validation_anchor_count <= 0:
        raise ValueError("validation_anchor_count must be positive.")
    rows = list(
        expanded_schedule_value_teacher_label_panel_v1_frame.iter_rows(named=True)
    )
    output_rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        anchors = sorted(
            {
                _datetime_value(row["anchor_timestamp"])
                for row in rows
                if str(row["source_model_name"]) == source_model_name
            }
        )
        source_window_rows: list[dict[str, Any]] = []
        for window_index in range(validation_window_count):
            end = len(anchors) - window_index * validation_anchor_count
            start = end - validation_anchor_count
            if start < 0:
                break
            validation_anchors = tuple(anchors[start:end])
            prior_anchors = tuple(anchors[:start])
            if len(prior_anchors) < min_prior_anchors_before_window:
                continue
            window_frame = _window_teacher_panel(
                rows,
                source_model_name=source_model_name,
                prior_anchors=set(prior_anchors),
                validation_anchors=set(validation_anchors),
            )
            scorer = build_dfl_regret_surrogate_forecast_correction_v1_frame(
                window_frame,
                tenant_ids=tenant_ids,
                source_model_names=(source_model_name,),
                min_prior_safe_win_count=min_prior_safe_win_count,
                min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_predicted_tail_risk_probability=(
                    max_predicted_tail_risk_probability
                ),
                allowed_candidate_sources=allowed_candidate_sources,
                use_cuda_if_available=use_cuda_if_available,
            )
            candidate_value = build_dfl_regret_surrogate_candidate_value_v1_frame(
                window_frame,
                scorer,
            )
            source_window_rows.append(
                _rolling_summary_row(
                    window_frame,
                    candidate_value,
                    source_model_name=source_model_name,
                    window_index=window_index,
                    validation_anchors=validation_anchors,
                    prior_anchors=prior_anchors,
                    min_mean_regret_improvement_ratio_vs_v2_plus=(
                        min_mean_regret_improvement_ratio_vs_v2_plus
                    ),
                )
            )
        pass_count = sum(
            1 for row in source_window_rows if bool(row["rolling_window_passed"])
        )
        diagnostic_count = sum(
            1 for row in source_window_rows if bool(row["diagnostic_window_passed"])
        )
        for row in source_window_rows:
            row["passing_window_count_for_source"] = pass_count
            row["diagnostic_window_count_for_source"] = diagnostic_count
            row["robust_regret_surrogate_challenger"] = (
                pass_count >= validation_window_count
            )
            row["diagnostic_signal_learnable"] = diagnostic_count >= min(
                validation_window_count,
                3,
            )
            row["production_promote"] = False
        output_rows.extend(source_window_rows)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "window_index"]
    )


def build_dfl_regret_surrogate_safe_switch_context_audit_frame(
    expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    *,
    material_switch_delta_uah: float = 25.0,
    high_v2_regret_uah: float = 500.0,
    high_forecast_spread_uah_mwh: float = 10_000.0,
    min_material_schedule_distance: float = 0.02,
    min_context_prior_safe_win_count: int = 1,
    min_context_prior_mean_improvement_uah: float = 1.0,
    max_context_tail_risk_probability: float = 0.25,
) -> pl.DataFrame:
    """Audit whether final safe switches have prior-supported contexts."""

    _validate_teacher_panel(expanded_schedule_value_teacher_label_panel_v1_frame)
    _validate_context_config(
        material_switch_delta_uah=material_switch_delta_uah,
        high_v2_regret_uah=high_v2_regret_uah,
        high_forecast_spread_uah_mwh=high_forecast_spread_uah_mwh,
        min_material_schedule_distance=min_material_schedule_distance,
        min_context_prior_safe_win_count=min_context_prior_safe_win_count,
        min_context_prior_mean_improvement_uah=min_context_prior_mean_improvement_uah,
        max_context_tail_risk_probability=max_context_tail_risk_probability,
    )
    rows = list(
        expanded_schedule_value_teacher_label_panel_v1_frame.iter_rows(named=True)
    )
    context_stats = _context_profile_stats(
        [
            row
            for row in rows
            if bool(row["is_training_row"])
            and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
        ],
        high_v2_regret_uah=high_v2_regret_uah,
        high_forecast_spread_uah_mwh=high_forecast_spread_uah_mwh,
        min_material_schedule_distance=min_material_schedule_distance,
    )
    output_rows: list[dict[str, Any]] = []
    for anchor_key, anchor_rows in sorted(_group_by_anchor(rows).items()):
        baseline = _baseline_row(anchor_rows, anchor_key=anchor_key)
        material_candidates = [
            row
            for row in _eligible_challengers(anchor_rows)
            if float(row["label_regret_delta_vs_v2_plus_uah"])
            <= -material_switch_delta_uah
        ]
        best = (
            min(
                material_candidates,
                key=lambda row: (
                    float(row["regret_uah"]),
                    str(row["candidate_source"]),
                    str(row["candidate_family"]),
                    str(row["candidate_model_name"]),
                ),
            )
            if material_candidates
            else None
        )
        profile = (
            _safe_switch_context_profile_key(
                best,
                high_v2_regret_uah=high_v2_regret_uah,
                high_forecast_spread_uah_mwh=high_forecast_spread_uah_mwh,
                min_material_schedule_distance=min_material_schedule_distance,
            )
            if best is not None
            else ""
        )
        stats = context_stats.get(profile, {})
        failure_mode = _safe_switch_context_failure_mode(
            material_safe_switch_available=best is not None,
            stats=stats,
            min_context_prior_safe_win_count=min_context_prior_safe_win_count,
            min_context_prior_mean_improvement_uah=(
                min_context_prior_mean_improvement_uah
            ),
            max_context_tail_risk_probability=max_context_tail_risk_probability,
        )
        output_rows.append(
            {
                "tenant_id": anchor_key[0],
                "source_model_name": anchor_key[1],
                "anchor_timestamp": anchor_key[2],
                "split_name": str(baseline["split_name"]),
                "v2_plus_regret_uah": float(baseline["regret_uah"]),
                "material_safe_switch_available": best is not None,
                "material_safe_switch_candidate_count": len(material_candidates),
                "best_material_candidate_key": _candidate_key(best)
                if best is not None
                else "",
                "best_material_candidate_source": str(best["candidate_source"])
                if best is not None
                else "",
                "best_material_candidate_family": str(best["candidate_family"])
                if best is not None
                else "",
                "best_material_candidate_model_name": str(best["candidate_model_name"])
                if best is not None
                else "",
                "best_material_candidate_regret_uah": float(best["regret_uah"])
                if best is not None
                else float(baseline["regret_uah"]),
                "best_material_delta_vs_v2_plus_uah": float(
                    best["label_regret_delta_vs_v2_plus_uah"]
                )
                if best is not None
                else 0.0,
                "safe_switch_context_profile_key": profile,
                "prior_context_support_count": int(stats.get("row_count", 0)),
                "prior_context_safe_win_count": int(stats.get("safe_win_count", 0)),
                "prior_context_tail_loss_count": int(stats.get("tail_loss_count", 0)),
                "prior_context_mean_delta_uah": float(stats.get("mean_delta_uah", 0.0)),
                "prior_context_tail_risk_probability": float(
                    stats.get("tail_risk_probability", 0.0)
                ),
                "safe_switch_context_failure_mode": failure_mode,
                "recommended_next_branch": _context_recommended_next_branch(
                    failure_mode
                ),
                "claim_scope": REGRET_SURROGATE_CONTEXT_AUDIT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp"]
    )


def build_dfl_regret_surrogate_teacher_label_panel_v2_frame(
    expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    regret_surrogate_safe_switch_context_audit_frame: pl.DataFrame,
    *,
    material_switch_delta_uah: float = 25.0,
    high_v2_regret_uah: float = 500.0,
    high_forecast_spread_uah_mwh: float = 10_000.0,
    min_material_schedule_distance: float = 0.02,
) -> pl.DataFrame:
    """Enrich V1 teacher rows with prior-supported safe-switch context labels."""

    _validate_teacher_panel(expanded_schedule_value_teacher_label_panel_v1_frame)
    _require_columns(
        regret_surrogate_safe_switch_context_audit_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "safe_switch_context_failure_mode",
                "recommended_next_branch",
                "market_execution_enabled",
            }
        ),
        frame_name="regret_surrogate_safe_switch_context_audit_frame",
    )
    if regret_surrogate_safe_switch_context_audit_frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError("safe-switch context audit refuses market execution.")
    rows = list(
        expanded_schedule_value_teacher_label_panel_v1_frame.iter_rows(named=True)
    )
    context_stats = _context_profile_stats(
        [
            row
            for row in rows
            if bool(row["is_training_row"])
            and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
        ],
        high_v2_regret_uah=high_v2_regret_uah,
        high_forecast_spread_uah_mwh=high_forecast_spread_uah_mwh,
        min_material_schedule_distance=min_material_schedule_distance,
    )
    audit_by_anchor = {
        _anchor_key(row): row
        for row in regret_surrogate_safe_switch_context_audit_frame.iter_rows(
            named=True
        )
    }
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        key = _anchor_key(row)
        audit = audit_by_anchor.get(key)
        if audit is None:
            raise ValueError(f"missing safe-switch context audit row for {key}.")
        profile = _safe_switch_context_profile_key(
            row,
            high_v2_regret_uah=high_v2_regret_uah,
            high_forecast_spread_uah_mwh=high_forecast_spread_uah_mwh,
            min_material_schedule_distance=min_material_schedule_distance,
        )
        stats = context_stats.get(profile, {})
        delta = float(row["label_regret_delta_vs_v2_plus_uah"])
        material_safe = (
            str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
            and delta <= -material_switch_delta_uah
        )
        copied = dict(row)
        copied.update(
            {
                "teacher_panel_version": "safe_switch_context_teacher_v2",
                "safe_switch_context_profile_key": profile,
                "selector_feature_context_prior_support_count": float(
                    stats.get("row_count", 0)
                ),
                "selector_feature_context_prior_safe_win_rate": float(
                    stats.get("safe_win_probability", 0.0)
                ),
                "selector_feature_context_prior_tail_risk_probability": float(
                    stats.get("tail_risk_probability", 0.0)
                ),
                "selector_feature_context_prior_mean_delta_uah": float(
                    stats.get("mean_delta_uah", 0.0)
                ),
                "label_context_material_safe_switch": material_safe,
                "label_context_tail_risk_loss": bool(row["label_tail_risk_loss"]),
                "label_context_switch_class": _context_switch_class(
                    material_safe=material_safe,
                    tail_loss=bool(row["label_tail_risk_loss"]),
                ),
                "diagnostic_anchor_safe_switch_context_failure_mode": str(
                    audit["safe_switch_context_failure_mode"]
                ),
                "diagnostic_anchor_recommended_next_branch": str(
                    audit["recommended_next_branch"]
                ),
                "diagnostic_prior_context_support_count": int(
                    stats.get("row_count", 0)
                ),
                "diagnostic_prior_context_safe_win_count": int(
                    stats.get("safe_win_count", 0)
                ),
                "diagnostic_prior_context_tail_loss_count": int(
                    stats.get("tail_loss_count", 0)
                ),
                "claim_scope": REGRET_SURROGATE_TEACHER_V2_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        output_rows.append(copied)
    panel = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_context_teacher_panel(panel)
    return panel


def build_dfl_regret_surrogate_contextual_candidate_value_v2_frame(
    regret_surrogate_teacher_label_panel_v2_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    min_context_prior_support_count: int = 1,
    min_context_prior_safe_win_count: int = 1,
    min_context_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_context_tail_risk_probability: float = 0.25,
    allowed_candidate_sources: tuple[str, ...] = _DEFAULT_ALLOWED_CANDIDATE_SOURCES,
) -> pl.DataFrame:
    """Select final candidates only from prior-supported safe-switch contexts."""

    _validate_context_teacher_panel(regret_surrogate_teacher_label_panel_v2_frame)
    _validate_contextual_selector_config(
        tenant_ids=tenant_ids,
        source_model_names=source_model_names,
        min_context_prior_support_count=min_context_prior_support_count,
        min_context_prior_safe_win_count=min_context_prior_safe_win_count,
        min_context_prior_mean_improvement_uah=(min_context_prior_mean_improvement_uah),
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_context_tail_risk_probability=max_context_tail_risk_probability,
        allowed_candidate_sources=allowed_candidate_sources,
    )
    rows = list(regret_surrogate_teacher_label_panel_v2_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in source_model_names:
            scope_rows = [
                row
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            ]
            output_rows.append(
                _fit_scope_contextual_surrogate(
                    scope_rows,
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    min_context_prior_support_count=min_context_prior_support_count,
                    min_context_prior_safe_win_count=min_context_prior_safe_win_count,
                    min_context_prior_mean_improvement_uah=(
                        min_context_prior_mean_improvement_uah
                    ),
                    min_predicted_improvement_uah=min_predicted_improvement_uah,
                    max_context_tail_risk_probability=(
                        max_context_tail_risk_probability
                    ),
                    allowed_candidate_sources=set(allowed_candidate_sources),
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame(
    regret_surrogate_teacher_label_panel_v2_frame: pl.DataFrame,
    regret_surrogate_contextual_candidate_value_v2_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score contextual regret-surrogate V2 against frozen V2+."""

    _validate_context_teacher_panel(regret_surrogate_teacher_label_panel_v2_frame)
    _require_columns(
        regret_surrogate_contextual_candidate_value_v2_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "selected_final_candidate_keys",
                "fallback_final_anchor_keys",
                "market_execution_enabled",
            }
        ),
        frame_name="regret_surrogate_contextual_candidate_value_v2_frame",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        regret_surrogate_teacher_label_panel_v2_frame
    )
    panel_rows = list(
        regret_surrogate_teacher_label_panel_v2_frame.iter_rows(named=True)
    )
    candidate_by_key = {_candidate_key(row): row for row in panel_rows}
    v2_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in panel_rows:
        if str(row["split_name"]) != "final_holdout":
            continue
        source = str(row["candidate_source"])
        if source == _STRICT_CANDIDATE_SOURCE:
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=STRICT_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=REGRET_SURROGATE_CONTEXTUAL_STRICT_LP_STRATEGY_KIND,
                    challenger_model_name=REGRET_SURROGATE_CONTEXTUAL_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_CONTEXTUAL_STRICT_CLAIM_SCOPE,
                )
            )
        elif source == _V2_PLUS_CANDIDATE_SOURCE:
            v2_by_anchor[_anchor_key_string(row)] = row
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=V2_PLUS_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=REGRET_SURROGATE_CONTEXTUAL_STRICT_LP_STRATEGY_KIND,
                    challenger_model_name=REGRET_SURROGATE_CONTEXTUAL_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_CONTEXTUAL_STRICT_CLAIM_SCOPE,
                )
            )
    for scorer_row in regret_surrogate_contextual_candidate_value_v2_frame.iter_rows(
        named=True
    ):
        for candidate_key in scorer_row["selected_final_candidate_keys"]:
            output_rows.append(
                _benchmark_row(
                    candidate_by_key[str(candidate_key)],
                    selection_role=REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=REGRET_SURROGATE_CONTEXTUAL_STRICT_LP_STRATEGY_KIND,
                    challenger_model_name=REGRET_SURROGATE_CONTEXTUAL_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_CONTEXTUAL_STRICT_CLAIM_SCOPE,
                )
            )
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            fallback = v2_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"missing V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _benchmark_row(
                    fallback,
                    selection_role=REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=REGRET_SURROGATE_CONTEXTUAL_STRICT_LP_STRATEGY_KIND,
                    challenger_model_name=REGRET_SURROGATE_CONTEXTUAL_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_CONTEXTUAL_STRICT_CLAIM_SCOPE,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def build_dfl_regret_surrogate_contextual_rolling_robustness_frame(
    expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    material_switch_delta_uah: float = 25.0,
    high_v2_regret_uah: float = 500.0,
    high_forecast_spread_uah_mwh: float = 10_000.0,
    min_material_schedule_distance: float = 0.02,
    min_context_prior_support_count: int = 1,
    min_context_prior_safe_win_count: int = 1,
    min_context_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_context_tail_risk_probability: float = 0.25,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    allowed_candidate_sources: tuple[str, ...] = _DEFAULT_ALLOWED_CANDIDATE_SOURCES,
) -> pl.DataFrame:
    """Replay contextual safe-switch selection over prior-only windows."""

    _validate_teacher_panel(expanded_schedule_value_teacher_label_panel_v1_frame)
    if validation_window_count <= 0:
        raise ValueError("validation_window_count must be positive.")
    if validation_anchor_count <= 0:
        raise ValueError("validation_anchor_count must be positive.")
    rows = list(
        expanded_schedule_value_teacher_label_panel_v1_frame.iter_rows(named=True)
    )
    output_rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        anchors = sorted(
            {
                _datetime_value(row["anchor_timestamp"])
                for row in rows
                if str(row["source_model_name"]) == source_model_name
            }
        )
        source_window_rows: list[dict[str, Any]] = []
        for window_index in range(validation_window_count):
            end = len(anchors) - window_index * validation_anchor_count
            start = end - validation_anchor_count
            if start < 0:
                break
            validation_anchors = tuple(anchors[start:end])
            prior_anchors = tuple(anchors[:start])
            if len(prior_anchors) < min_prior_anchors_before_window:
                continue
            window_frame = _window_teacher_panel(
                rows,
                source_model_name=source_model_name,
                prior_anchors=set(prior_anchors),
                validation_anchors=set(validation_anchors),
            )
            audit = build_dfl_regret_surrogate_safe_switch_context_audit_frame(
                window_frame,
                material_switch_delta_uah=material_switch_delta_uah,
                high_v2_regret_uah=high_v2_regret_uah,
                high_forecast_spread_uah_mwh=high_forecast_spread_uah_mwh,
                min_material_schedule_distance=min_material_schedule_distance,
                min_context_prior_safe_win_count=min_context_prior_safe_win_count,
                min_context_prior_mean_improvement_uah=(
                    min_context_prior_mean_improvement_uah
                ),
                max_context_tail_risk_probability=max_context_tail_risk_probability,
            )
            teacher_v2 = build_dfl_regret_surrogate_teacher_label_panel_v2_frame(
                window_frame,
                audit,
                material_switch_delta_uah=material_switch_delta_uah,
                high_v2_regret_uah=high_v2_regret_uah,
                high_forecast_spread_uah_mwh=high_forecast_spread_uah_mwh,
                min_material_schedule_distance=min_material_schedule_distance,
            )
            candidate_value = (
                build_dfl_regret_surrogate_contextual_candidate_value_v2_frame(
                    teacher_v2,
                    tenant_ids=tenant_ids,
                    source_model_names=(source_model_name,),
                    min_context_prior_support_count=min_context_prior_support_count,
                    min_context_prior_safe_win_count=min_context_prior_safe_win_count,
                    min_context_prior_mean_improvement_uah=(
                        min_context_prior_mean_improvement_uah
                    ),
                    min_predicted_improvement_uah=min_predicted_improvement_uah,
                    max_context_tail_risk_probability=(
                        max_context_tail_risk_probability
                    ),
                    allowed_candidate_sources=allowed_candidate_sources,
                )
            )
            row = _rolling_summary_row(
                teacher_v2,
                candidate_value,
                source_model_name=source_model_name,
                window_index=window_index,
                validation_anchors=validation_anchors,
                prior_anchors=prior_anchors,
                min_mean_regret_improvement_ratio_vs_v2_plus=(
                    min_mean_regret_improvement_ratio_vs_v2_plus
                ),
            )
            row["claim_scope"] = REGRET_SURROGATE_CONTEXTUAL_ROBUSTNESS_CLAIM_SCOPE
            source_window_rows.append(row)
        pass_count = sum(
            1 for row in source_window_rows if bool(row["rolling_window_passed"])
        )
        diagnostic_count = sum(
            1 for row in source_window_rows if bool(row["diagnostic_window_passed"])
        )
        for row in source_window_rows:
            row["passing_window_count_for_source"] = pass_count
            row["diagnostic_window_count_for_source"] = diagnostic_count
            row["robust_regret_surrogate_challenger"] = (
                pass_count >= validation_window_count
            )
            row["diagnostic_signal_learnable"] = diagnostic_count >= min(
                validation_window_count,
                3,
            )
            row["production_promote"] = False
        output_rows.extend(source_window_rows)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "window_index"]
    )


def build_dfl_sparse_safe_switch_feature_contract_audit_frame(
    frame: pl.DataFrame,
) -> pl.DataFrame:
    """Audit that selector features are prior-only inputs, not realized labels."""

    selector_features = _selector_feature_columns(frame)
    blocked = [
        column
        for column in selector_features
        if _selector_feature_name_is_blocked(column)
    ]
    selected_feature_names: list[str] = []
    if "selected_feature_names" in frame.columns and not frame.is_empty():
        for value in frame["selected_feature_names"].to_list():
            if isinstance(value, list):
                selected_feature_names.extend(str(item) for item in value)
    blocked_selected = [
        name
        for name in sorted(set(selected_feature_names))
        if _selector_feature_name_is_blocked(name)
    ]
    return pl.DataFrame(
        [
            {
                "feature_contract_passed": not blocked and not blocked_selected,
                "selector_feature_count": len(selector_features),
                "blocked_selector_feature_names": blocked,
                "blocked_selected_feature_names": blocked_selected,
                "claim_scope": REGRET_SURROGATE_SPARSE_FEATURE_CONTRACT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        ],
        infer_schema_length=None,
    )


def build_dfl_sparse_safe_switch_candidate_library_v6_frame(
    regret_surrogate_teacher_label_panel_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prepare V6 schedule candidates without fabricating final-holdout labels."""

    _validate_context_teacher_panel(regret_surrogate_teacher_label_panel_v2_frame)
    feature_contract = build_dfl_sparse_safe_switch_feature_contract_audit_frame(
        regret_surrogate_teacher_label_panel_v2_frame
    )
    if not bool(feature_contract["feature_contract_passed"].item()):
        raise ValueError(
            "sparse safe-switch candidate library refuses selector-feature leakage."
        )
    rows: list[dict[str, Any]] = []
    for row in regret_surrogate_teacher_label_panel_v2_frame.iter_rows(named=True):
        source = str(row["candidate_source"])
        copied = dict(row)
        copied.update(
            {
                "candidate_library_version": "sparse_safe_switch_v6",
                "candidate_schedule_class": _sparse_candidate_schedule_class(row),
                "oracle_neighborhood_train_only": (
                    source == "oracle_gap_candidate"
                    and str(row["split_name"]) == "train_selection"
                ),
                "eligible_for_final_selection_v6": bool(
                    row["eligible_for_final_selection"]
                )
                and not (
                    source == "oracle_gap_candidate"
                    and str(row["split_name"]) == "final_holdout"
                ),
                "claim_scope": REGRET_SURROGATE_SPARSE_CANDIDATE_LIBRARY_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        rows.append(copied)
    frame = pl.DataFrame(rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_sparse_candidate_library(frame)
    return frame


def build_dfl_sparse_safe_switch_opportunity_audit_frame(
    sparse_safe_switch_candidate_library_v6_frame: pl.DataFrame,
    *,
    material_switch_delta_uah: float = 25.0,
    max_prior_neighbor_distance: float = 1.5,
    min_neighbor_safe_win_count: int = 1,
    max_neighbor_tail_risk_probability: float = 0.25,
    nearest_neighbor_count: int = 5,
) -> pl.DataFrame:
    """Classify sparse safe-switch opportunities using nearest prior anchors."""

    _validate_sparse_candidate_library(sparse_safe_switch_candidate_library_v6_frame)
    _validate_sparse_neighbor_config(
        material_switch_delta_uah=material_switch_delta_uah,
        max_prior_neighbor_distance=max_prior_neighbor_distance,
        min_neighbor_safe_win_count=min_neighbor_safe_win_count,
        max_neighbor_tail_risk_probability=max_neighbor_tail_risk_probability,
        nearest_neighbor_count=nearest_neighbor_count,
    )
    rows = list(sparse_safe_switch_candidate_library_v6_frame.iter_rows(named=True))
    prior_rows = _sparse_prior_candidate_rows(rows)
    prior_rows_by_group: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for prior in prior_rows:
        prior_rows_by_group.setdefault(_sparse_neighbor_group_key(prior), []).append(
            prior
        )
    feature_names = _sparse_distance_feature_names(
        sparse_safe_switch_candidate_library_v6_frame
    )
    output_rows: list[dict[str, Any]] = []
    for anchor_key, anchor_rows in sorted(_group_by_anchor(rows).items()):
        baseline = _baseline_row(anchor_rows, anchor_key=anchor_key)
        challengers = _sparse_eligible_challengers(anchor_rows)
        material_candidates = [
            row
            for row in challengers
            if float(row["label_regret_delta_vs_v2_plus_uah"])
            <= -material_switch_delta_uah
        ]
        tail_candidates = [
            row for row in challengers if bool(row["label_tail_risk_loss"])
        ]
        best = (
            min(
                material_candidates,
                key=lambda row: (
                    float(row["regret_uah"]),
                    str(row["candidate_source"]),
                    str(row["candidate_family"]),
                    str(row["candidate_model_name"]),
                ),
            )
            if material_candidates
            else None
        )
        neighbor_stats = (
            _nearest_prior_neighbor_stats_from_candidates(
                best,
                prior_rows=prior_rows_by_group.get(
                    _sparse_neighbor_group_key(best), []
                ),
                feature_names=feature_names,
                nearest_neighbor_count=nearest_neighbor_count,
            )
            if best is not None
            else _empty_neighbor_stats()
        )
        opportunity_class = _sparse_opportunity_class(
            material_candidate_available=best is not None,
            tail_risk_candidate_count=len(tail_candidates),
            neighbor_stats=neighbor_stats,
            max_prior_neighbor_distance=max_prior_neighbor_distance,
            min_neighbor_safe_win_count=min_neighbor_safe_win_count,
            max_neighbor_tail_risk_probability=max_neighbor_tail_risk_probability,
        )
        output_rows.append(
            {
                "tenant_id": anchor_key[0],
                "source_model_name": anchor_key[1],
                "anchor_timestamp": anchor_key[2],
                "split_name": str(baseline["split_name"]),
                "v2_plus_regret_uah": float(baseline["regret_uah"]),
                "material_switch_delta_uah": material_switch_delta_uah,
                "material_candidate_available": best is not None,
                "material_candidate_count": len(material_candidates),
                "tail_risk_candidate_count": len(tail_candidates),
                "best_material_candidate_key": _candidate_key(best)
                if best is not None
                else "",
                "best_material_candidate_source": str(best["candidate_source"])
                if best is not None
                else "",
                "best_material_candidate_family": str(best["candidate_family"])
                if best is not None
                else "",
                "best_material_candidate_regret_uah": float(best["regret_uah"])
                if best is not None
                else float(baseline["regret_uah"]),
                "best_material_delta_vs_v2_plus_uah": float(
                    best["label_regret_delta_vs_v2_plus_uah"]
                )
                if best is not None
                else 0.0,
                "nearest_prior_safe_switch_distance": float(
                    neighbor_stats["nearest_safe_distance"]
                ),
                "nearest_prior_any_candidate_distance": float(
                    neighbor_stats["nearest_any_distance"]
                ),
                "neighbor_support_count": int(neighbor_stats["neighbor_count"]),
                "neighbor_safe_win_count": int(neighbor_stats["safe_win_count"]),
                "neighbor_tail_risk_count": int(neighbor_stats["tail_risk_count"]),
                "neighbor_tail_risk_probability": float(
                    neighbor_stats["tail_risk_probability"]
                ),
                "neighbor_mean_delta_uah": float(neighbor_stats["mean_delta_uah"]),
                "sparse_opportunity_class": opportunity_class,
                "recommended_next_branch": _sparse_recommended_next_branch(
                    opportunity_class
                ),
                "claim_scope": REGRET_SURROGATE_SPARSE_OPPORTUNITY_AUDIT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp"]
    )


def build_dfl_sparse_safe_switch_teacher_label_panel_v6_frame(
    sparse_safe_switch_candidate_library_v6_frame: pl.DataFrame,
    sparse_safe_switch_opportunity_audit_frame: pl.DataFrame,
    *,
    material_switch_delta_uah: float = 25.0,
    max_prior_neighbor_distance: float = 1.5,
    nearest_neighbor_count: int = 5,
) -> pl.DataFrame:
    """Attach distance-based prior support labels for the V6 abstaining selector."""

    _validate_sparse_candidate_library(sparse_safe_switch_candidate_library_v6_frame)
    _require_columns(
        sparse_safe_switch_opportunity_audit_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "sparse_opportunity_class",
                "recommended_next_branch",
                "market_execution_enabled",
            }
        ),
        frame_name="sparse_safe_switch_opportunity_audit_frame",
    )
    if sparse_safe_switch_opportunity_audit_frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError(
            "sparse safe-switch opportunity audit refuses market execution."
        )
    if material_switch_delta_uah <= 0.0:
        raise ValueError("material_switch_delta_uah must be positive.")
    if max_prior_neighbor_distance < 0.0:
        raise ValueError("max_prior_neighbor_distance must not be negative.")
    if nearest_neighbor_count < 1:
        raise ValueError("nearest_neighbor_count must be at least 1.")

    rows = list(sparse_safe_switch_candidate_library_v6_frame.iter_rows(named=True))
    prior_rows = _sparse_prior_candidate_rows(rows)
    prior_rows_by_group: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for prior in prior_rows:
        prior_rows_by_group.setdefault(_sparse_neighbor_group_key(prior), []).append(
            prior
        )
    feature_names = _sparse_distance_feature_names(
        sparse_safe_switch_candidate_library_v6_frame
    )
    audit_by_anchor = {
        _anchor_key(row): row
        for row in sparse_safe_switch_opportunity_audit_frame.iter_rows(named=True)
    }
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        audit = audit_by_anchor.get(_anchor_key(row))
        if audit is None:
            raise ValueError(
                f"missing sparse opportunity audit row for {_anchor_key(row)}."
            )
        source = str(row["candidate_source"])
        neighbor_stats = (
            _nearest_prior_neighbor_stats_from_candidates(
                row,
                prior_rows=prior_rows_by_group.get(_sparse_neighbor_group_key(row), []),
                feature_names=feature_names,
                nearest_neighbor_count=nearest_neighbor_count,
            )
            if str(row["split_name"]) == "final_holdout"
            and source not in _REFERENCE_CANDIDATE_SOURCES
            and bool(row.get("eligible_for_final_selection_v6", True))
            else _empty_neighbor_stats()
        )
        delta = float(row["label_regret_delta_vs_v2_plus_uah"])
        material_safe = (
            source not in _REFERENCE_CANDIDATE_SOURCES
            and delta <= -material_switch_delta_uah
        )
        copied = dict(row)
        feature_list = list(copied.get("selected_feature_names", []))
        for feature_name in (
            "selector_feature_nearest_prior_safe_switch_distance",
            "selector_feature_neighbor_safe_win_count",
            "selector_feature_neighbor_tail_risk_probability",
            "selector_feature_neighbor_mean_delta_uah",
        ):
            if feature_name not in feature_list:
                feature_list.append(feature_name)
        copied.update(
            {
                "teacher_panel_version": "sparse_safe_switch_teacher_v6",
                "selected_feature_names": sorted(feature_list),
                "selector_feature_nearest_prior_safe_switch_distance": float(
                    neighbor_stats["nearest_safe_distance"]
                ),
                "selector_feature_nearest_prior_any_candidate_distance": float(
                    neighbor_stats["nearest_any_distance"]
                ),
                "selector_feature_neighbor_support_count": float(
                    neighbor_stats["neighbor_count"]
                ),
                "selector_feature_neighbor_safe_win_count": float(
                    neighbor_stats["safe_win_count"]
                ),
                "selector_feature_neighbor_tail_risk_probability": float(
                    neighbor_stats["tail_risk_probability"]
                ),
                "selector_feature_neighbor_mean_delta_uah": float(
                    neighbor_stats["mean_delta_uah"]
                ),
                "selector_feature_has_prior_neighbor_support": float(
                    float(neighbor_stats["nearest_safe_distance"])
                    <= max_prior_neighbor_distance
                ),
                "label_sparse_material_safe_switch": material_safe,
                "label_sparse_tail_risk_loss": bool(row["label_tail_risk_loss"]),
                "label_sparse_opportunity_class": str(
                    audit["sparse_opportunity_class"]
                ),
                "diagnostic_sparse_recommended_next_branch": str(
                    audit["recommended_next_branch"]
                ),
                "claim_scope": REGRET_SURROGATE_SPARSE_TEACHER_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        output_rows.append(copied)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_sparse_teacher_panel(frame)
    return frame


def build_dfl_sparse_safe_switch_abstention_model_v6_frame(
    sparse_safe_switch_teacher_label_panel_v6_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    max_prior_neighbor_distance: float = 1.5,
    min_neighbor_safe_win_count: int = 1,
    min_predicted_improvement_uah: float = 1.0,
    max_neighbor_tail_risk_probability: float = 0.25,
    allowed_candidate_sources: tuple[str, ...] = _DEFAULT_ALLOWED_CANDIDATE_SOURCES,
) -> pl.DataFrame:
    """Select distance-supported candidates; abstain to V2+ when evidence is weak."""

    _validate_sparse_teacher_panel(sparse_safe_switch_teacher_label_panel_v6_frame)
    _validate_sparse_selector_config(
        tenant_ids=tenant_ids,
        source_model_names=source_model_names,
        max_prior_neighbor_distance=max_prior_neighbor_distance,
        min_neighbor_safe_win_count=min_neighbor_safe_win_count,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_neighbor_tail_risk_probability=max_neighbor_tail_risk_probability,
        allowed_candidate_sources=allowed_candidate_sources,
    )
    rows = list(sparse_safe_switch_teacher_label_panel_v6_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in source_model_names:
            scope_rows = [
                row
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            ]
            output_rows.append(
                _fit_scope_sparse_safe_switch(
                    scope_rows,
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    max_prior_neighbor_distance=max_prior_neighbor_distance,
                    min_neighbor_safe_win_count=min_neighbor_safe_win_count,
                    min_predicted_improvement_uah=min_predicted_improvement_uah,
                    max_neighbor_tail_risk_probability=(
                        max_neighbor_tail_risk_probability
                    ),
                    allowed_candidate_sources=set(allowed_candidate_sources),
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def build_dfl_sparse_safe_switch_strict_lp_benchmark_frame(
    sparse_safe_switch_teacher_label_panel_v6_frame: pl.DataFrame,
    sparse_safe_switch_abstention_model_v6_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score V6 sparse safe-switch selections against V2+."""

    _validate_sparse_teacher_panel(sparse_safe_switch_teacher_label_panel_v6_frame)
    _validate_scorer_frame(sparse_safe_switch_abstention_model_v6_frame)
    resolved_generated_at = generated_at or _latest_generated_at(
        sparse_safe_switch_teacher_label_panel_v6_frame
    )
    panel_rows = list(
        sparse_safe_switch_teacher_label_panel_v6_frame.iter_rows(named=True)
    )
    candidate_by_key = {_candidate_key(row): row for row in panel_rows}
    v2_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in panel_rows:
        if str(row["split_name"]) != "final_holdout":
            continue
        source = str(row["candidate_source"])
        if source == _STRICT_CANDIDATE_SOURCE:
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=STRICT_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_SPARSE_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_SPARSE_SAFE_SWITCH_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_SPARSE_STRICT_CLAIM_SCOPE,
                )
            )
        elif source == _V2_PLUS_CANDIDATE_SOURCE:
            v2_by_anchor[_anchor_key_string(row)] = row
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=V2_PLUS_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_SPARSE_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_SPARSE_SAFE_SWITCH_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_SPARSE_STRICT_CLAIM_SCOPE,
                )
            )
    for scorer_row in sparse_safe_switch_abstention_model_v6_frame.iter_rows(
        named=True
    ):
        for candidate_key in scorer_row["selected_final_candidate_keys"]:
            output_rows.append(
                _benchmark_row(
                    candidate_by_key[str(candidate_key)],
                    selection_role=REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_SPARSE_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_SPARSE_SAFE_SWITCH_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_SPARSE_STRICT_CLAIM_SCOPE,
                )
            )
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            fallback = v2_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"missing V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _benchmark_row(
                    fallback,
                    selection_role=REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_SPARSE_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_SPARSE_SAFE_SWITCH_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_SPARSE_STRICT_CLAIM_SCOPE,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def build_dfl_sparse_safe_switch_rolling_robustness_frame(
    sparse_safe_switch_candidate_library_v6_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    material_switch_delta_uah: float = 25.0,
    max_prior_neighbor_distance: float = 1.5,
    min_neighbor_safe_win_count: int = 1,
    min_predicted_improvement_uah: float = 1.0,
    max_neighbor_tail_risk_probability: float = 0.25,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    allowed_candidate_sources: tuple[str, ...] = _DEFAULT_ALLOWED_CANDIDATE_SOURCES,
) -> pl.DataFrame:
    """Replay V6 sparse safe-switch selection over prior-only rolling windows."""

    _validate_sparse_candidate_library(sparse_safe_switch_candidate_library_v6_frame)
    if validation_window_count <= 0:
        raise ValueError("validation_window_count must be positive.")
    if validation_anchor_count <= 0:
        raise ValueError("validation_anchor_count must be positive.")
    rows = list(sparse_safe_switch_candidate_library_v6_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        anchors = sorted(
            {
                _datetime_value(row["anchor_timestamp"])
                for row in rows
                if str(row["source_model_name"]) == source_model_name
            }
        )
        source_window_rows: list[dict[str, Any]] = []
        for window_index in range(validation_window_count):
            end = len(anchors) - window_index * validation_anchor_count
            start = end - validation_anchor_count
            if start < 0:
                break
            validation_anchors = tuple(anchors[start:end])
            prior_anchors = tuple(anchors[:start])
            if len(prior_anchors) < min_prior_anchors_before_window:
                continue
            window_frame = _window_teacher_panel(
                rows,
                source_model_name=source_model_name,
                prior_anchors=set(prior_anchors),
                validation_anchors=set(validation_anchors),
            )
            library = build_dfl_sparse_safe_switch_candidate_library_v6_frame(
                window_frame
            )
            audit = build_dfl_sparse_safe_switch_opportunity_audit_frame(
                library,
                material_switch_delta_uah=material_switch_delta_uah,
                max_prior_neighbor_distance=max_prior_neighbor_distance,
                min_neighbor_safe_win_count=min_neighbor_safe_win_count,
                max_neighbor_tail_risk_probability=(max_neighbor_tail_risk_probability),
            )
            teacher_v6 = build_dfl_sparse_safe_switch_teacher_label_panel_v6_frame(
                library,
                audit,
                material_switch_delta_uah=material_switch_delta_uah,
                max_prior_neighbor_distance=max_prior_neighbor_distance,
            )
            model = build_dfl_sparse_safe_switch_abstention_model_v6_frame(
                teacher_v6,
                tenant_ids=tenant_ids,
                source_model_names=(source_model_name,),
                max_prior_neighbor_distance=max_prior_neighbor_distance,
                min_neighbor_safe_win_count=min_neighbor_safe_win_count,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_neighbor_tail_risk_probability=(max_neighbor_tail_risk_probability),
                allowed_candidate_sources=allowed_candidate_sources,
            )
            source_window_rows.append(
                _sparse_rolling_summary_row(
                    teacher_v6,
                    model,
                    source_model_name=source_model_name,
                    window_index=window_index,
                    validation_anchors=validation_anchors,
                    prior_anchors=prior_anchors,
                    min_mean_regret_improvement_ratio_vs_v2_plus=(
                        min_mean_regret_improvement_ratio_vs_v2_plus
                    ),
                )
            )
        pass_count = sum(
            1 for row in source_window_rows if bool(row["rolling_window_passed"])
        )
        diagnostic_count = sum(
            1 for row in source_window_rows if bool(row["diagnostic_window_passed"])
        )
        for row in source_window_rows:
            row["passing_window_count_for_source"] = pass_count
            row["diagnostic_window_count_for_source"] = diagnostic_count
            row["robust_sparse_safe_switch_challenger"] = (
                pass_count >= validation_window_count
            )
            row["diagnostic_signal_learnable"] = diagnostic_count >= min(
                validation_window_count,
                3,
            )
            row["production_promote"] = False
        output_rows.extend(source_window_rows)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "window_index"]
    )


def build_dfl_v2_plus_opportunity_backfill_requirements_frame(
    sparse_safe_switch_candidate_library_v6_frame: pl.DataFrame,
    sparse_safe_switch_opportunity_audit_frame: pl.DataFrame,
    *,
    material_switch_delta_uah: float = 25.0,
    min_prior_material_examples_for_dt: int = 20,
    min_oracle_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    high_forecast_spread_uah_mwh: float = 2_000.0,
    min_material_schedule_distance: float = 0.05,
) -> pl.DataFrame:
    """Decide whether V2+ misses need data backfill, candidates, or DT/LAVA."""

    _validate_sparse_candidate_library(sparse_safe_switch_candidate_library_v6_frame)
    _require_columns(
        sparse_safe_switch_opportunity_audit_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "sparse_opportunity_class",
                "material_candidate_available",
                "tail_risk_candidate_count",
                "market_execution_enabled",
            }
        ),
        frame_name="sparse_safe_switch_opportunity_audit_frame",
    )
    if material_switch_delta_uah <= 0.0:
        raise ValueError("material_switch_delta_uah must be positive.")
    if min_prior_material_examples_for_dt < 0:
        raise ValueError("min_prior_material_examples_for_dt must not be negative.")
    if min_oracle_improvement_ratio_vs_v2_plus < 0.0:
        raise ValueError(
            "min_oracle_improvement_ratio_vs_v2_plus must not be negative."
        )
    if high_forecast_spread_uah_mwh <= 0.0:
        raise ValueError("high_forecast_spread_uah_mwh must be positive.")
    if min_material_schedule_distance < 0.0:
        raise ValueError("min_material_schedule_distance must not be negative.")
    if sparse_safe_switch_opportunity_audit_frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError("opportunity backfill refuses market-execution evidence.")

    rows = list(sparse_safe_switch_candidate_library_v6_frame.iter_rows(named=True))
    grouped = _group_by_anchor(rows)
    audit_by_anchor = {
        _anchor_key(row): row
        for row in sparse_safe_switch_opportunity_audit_frame.iter_rows(named=True)
    }
    v2_regrets: list[float] = []
    selector_safe_oracle_regrets: list[float] = []
    prior_material_example_count = 0
    for anchor_rows in grouped.values():
        baseline = _baseline_row(anchor_rows, anchor_key=_anchor_key(anchor_rows[0]))
        challengers = _sparse_eligible_challengers(anchor_rows)
        material_challengers = [
            row
            for row in challengers
            if float(row["label_regret_delta_vs_v2_plus_uah"])
            <= -material_switch_delta_uah
        ]
        if str(baseline["split_name"]) != "final_holdout":
            prior_material_example_count += len(material_challengers)
        best = min(
            [baseline, *material_challengers],
            key=lambda row: (
                float(row["regret_uah"]),
                str(row["candidate_source"]),
                str(row["candidate_family"]),
                str(row["candidate_model_name"]),
            ),
        )
        v2_regrets.append(float(baseline["regret_uah"]))
        selector_safe_oracle_regrets.append(float(best["regret_uah"]))
    oracle_improvement = _improvement_ratio(
        mean(v2_regrets),
        mean(selector_safe_oracle_regrets),
    )

    output_rows: list[dict[str, Any]] = []
    for anchor_key, anchor_rows in sorted(grouped.items()):
        baseline = _baseline_row(anchor_rows, anchor_key=anchor_key)
        audit = audit_by_anchor.get(anchor_key)
        if audit is None:
            raise ValueError(f"missing sparse opportunity audit row for {anchor_key}.")
        strict_best, strict_reference_available = _strict_reference_row(
            anchor_rows,
            baseline=baseline,
        )
        strict_delta = float(strict_best["regret_uah"]) - float(baseline["regret_uah"])
        strict_material_win = strict_delta <= -material_switch_delta_uah
        challengers = _sparse_eligible_challengers(anchor_rows)
        material_candidate_count = sum(
            1
            for row in challengers
            if float(row["label_regret_delta_vs_v2_plus_uah"])
            <= -material_switch_delta_uah
        )
        weather_ready = _numeric_feature(
            baseline,
            "selector_feature_weather_load_context_ready",
        )
        calendar_ready = _numeric_feature(
            baseline,
            "selector_feature_calendar_publication_context_ready",
        )
        grid_ready = _numeric_feature(
            baseline,
            "selector_feature_grid_event_context_ready",
        )
        terminal_soc_pressure = (
            abs(
                _numeric_feature(
                    baseline, "selector_feature_terminal_soc_delta_fraction"
                )
            )
            >= min_material_schedule_distance
        )
        spread_regime_high = (
            _numeric_feature(baseline, "selector_feature_forecast_spread_uah_mwh")
            >= high_forecast_spread_uah_mwh
        )
        peak_trough_timing_pressure = any(
            _numeric_feature(row, "selector_feature_schedule_distance_from_v2_plus")
            >= min_material_schedule_distance
            for row in challengers
        )
        context_missing = any(
            value < 1.0 for value in (weather_ready, calendar_ready, grid_ready)
        )
        sparse_class = str(audit["sparse_opportunity_class"])
        tail_risk_dominated = (
            sparse_class == "tail_risk_dominated"
            or int(audit["tail_risk_candidate_count"]) > 0
        )
        candidate_family_gap = material_candidate_count == 0 and strict_material_win
        decision = _v7_backfill_decision(
            sparse_opportunity_class=sparse_class,
            context_missing=context_missing,
            candidate_family_gap=candidate_family_gap,
            strict_material_win=strict_material_win,
            prior_material_example_count=prior_material_example_count,
            selector_safe_oracle_improvement_ratio=oracle_improvement,
            min_prior_material_examples_for_dt=min_prior_material_examples_for_dt,
            min_oracle_improvement_ratio_vs_v2_plus=(
                min_oracle_improvement_ratio_vs_v2_plus
            ),
        )
        output_rows.append(
            {
                "tenant_id": anchor_key[0],
                "source_model_name": anchor_key[1],
                "anchor_timestamp": anchor_key[2],
                "split_name": str(baseline["split_name"]),
                "v2_plus_regret_uah": float(baseline["regret_uah"]),
                "strict_control_best_regret_uah": float(strict_best["regret_uah"]),
                "strict_control_reference_available": strict_reference_available,
                "strict_control_delta_vs_v2_plus_uah": strict_delta,
                "material_switch_delta_uah": material_switch_delta_uah,
                "material_non_reference_candidate_count": material_candidate_count,
                "diagnostic_strict_control_material_local_win": strict_material_win,
                "diagnostic_selector_safe_oracle_improvement_ratio_vs_v2_plus": (
                    oracle_improvement
                ),
                "diagnostic_prior_material_safe_switch_examples": (
                    prior_material_example_count
                ),
                "missing_weather_load_context": weather_ready < 1.0,
                "missing_calendar_event_context": calendar_ready < 1.0,
                "missing_grid_event_context": grid_ready < 1.0,
                "missing_prior_context": context_missing,
                "candidate_family_gap": candidate_family_gap,
                "terminal_soc_pressure": terminal_soc_pressure,
                "spread_regime_high": spread_regime_high,
                "peak_trough_timing_pressure": peak_trough_timing_pressure,
                "tail_risk_dominance": tail_risk_dominated,
                "sparse_opportunity_class": sparse_class,
                "opportunity_backfill_decision": decision,
                "claim_scope": REGRET_SURROGATE_BACKFILL_REQUIREMENTS_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp"]
    )


def build_dfl_backfilled_context_feature_panel_v7_frame(
    sparse_safe_switch_candidate_library_v6_frame: pl.DataFrame,
    v2_plus_opportunity_backfill_requirements_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Attach prior-only context missingness features for V7 candidate value learning."""

    _validate_sparse_candidate_library(sparse_safe_switch_candidate_library_v6_frame)
    _validate_v7_backfill_requirements_frame(
        v2_plus_opportunity_backfill_requirements_frame
    )
    requirements_by_anchor = {
        _anchor_key(row): row
        for row in v2_plus_opportunity_backfill_requirements_frame.iter_rows(named=True)
    }
    output_rows: list[dict[str, Any]] = []
    for row in sparse_safe_switch_candidate_library_v6_frame.iter_rows(named=True):
        requirement = requirements_by_anchor.get(_anchor_key(row))
        if requirement is None:
            raise ValueError(
                f"missing V7 backfill requirement row for {_anchor_key(row)}."
            )
        weather_ready = _numeric_feature(
            row,
            "selector_feature_weather_load_context_ready",
        )
        calendar_ready = _numeric_feature(
            row,
            "selector_feature_calendar_publication_context_ready",
        )
        grid_ready = _numeric_feature(row, "selector_feature_grid_event_context_ready")
        context_missing_count = sum(
            1 for value in (weather_ready, calendar_ready, grid_ready) if value < 1.0
        )
        feature_list = list(row.get("selected_feature_names", []))
        for feature_name in (
            "selector_feature_v7_context_missing_count",
            "selector_feature_v7_context_ready",
            "selector_feature_v7_schedule_distance",
            "selector_feature_v7_terminal_soc_pressure",
            "selector_feature_v7_spread_regime_high",
        ):
            if feature_name not in feature_list:
                feature_list.append(feature_name)
        copied = dict(row)
        copied.update(
            {
                "context_feature_panel_version": "backfilled_context_v7",
                "selected_feature_names": sorted(feature_list),
                "selector_feature_v7_context_missing_count": float(
                    context_missing_count
                ),
                "selector_feature_v7_context_ready": float(context_missing_count == 0),
                "selector_feature_v7_schedule_distance": _numeric_feature(
                    row,
                    "selector_feature_schedule_distance_from_v2_plus",
                ),
                "selector_feature_v7_terminal_soc_pressure": float(
                    bool(requirement["terminal_soc_pressure"])
                ),
                "selector_feature_v7_spread_regime_high": float(
                    bool(requirement["spread_regime_high"])
                ),
                "diagnostic_opportunity_backfill_decision": str(
                    requirement["opportunity_backfill_decision"]
                ),
                "diagnostic_candidate_family_gap": bool(
                    requirement["candidate_family_gap"]
                ),
                "diagnostic_strict_control_material_local_win": bool(
                    requirement["diagnostic_strict_control_material_local_win"]
                ),
                "claim_scope": REGRET_SURROGATE_BACKFILLED_CONTEXT_V7_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        output_rows.append(copied)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_sparse_candidate_library(frame)
    return frame


def build_dfl_feasible_schedule_candidate_library_v7_frame(
    backfilled_context_feature_panel_v7_frame: pl.DataFrame,
    v2_plus_opportunity_backfill_requirements_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Create V7 feasible candidates around V2+ misses without using final labels."""

    _validate_sparse_candidate_library(backfilled_context_feature_panel_v7_frame)
    _validate_v7_backfill_requirements_frame(
        v2_plus_opportunity_backfill_requirements_frame
    )
    requirements_by_anchor = {
        _anchor_key(row): row
        for row in v2_plus_opportunity_backfill_requirements_frame.iter_rows(named=True)
    }
    output_rows: list[dict[str, Any]] = []
    grouped = _group_by_anchor(
        list(backfilled_context_feature_panel_v7_frame.iter_rows(named=True))
    )
    for anchor_key, anchor_rows in sorted(grouped.items()):
        requirement = requirements_by_anchor.get(anchor_key)
        if requirement is None:
            raise ValueError(f"missing V7 backfill requirement row for {anchor_key}.")
        for row in anchor_rows:
            copied = dict(row)
            copied["eligible_for_final_selection_v7"] = bool(
                copied.get("eligible_for_final_selection_v6", True)
            )
            copied["claim_scope"] = (
                REGRET_SURROGATE_FEASIBLE_CANDIDATE_LIBRARY_V7_CLAIM_SCOPE
            )
            output_rows.append(copied)
        v2_row = _baseline_row(anchor_rows, anchor_key=anchor_key)
        strict_row, _ = _strict_reference_row(anchor_rows, baseline=v2_row)
        generated_specs = _v7_generated_candidate_specs(
            v2_row=v2_row,
            strict_row=strict_row,
            requirement=requirement,
        )
        output_rows.extend(generated_specs)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v7_candidate_library(frame)
    return frame


def build_dfl_ua_context_backfilled_feature_panel_v8_frame(
    feasible_schedule_candidate_library_v7_frame: pl.DataFrame,
    ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
    v2_plus_opportunity_backfill_requirements_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Merge source-backed Ukrainian prior context onto V7 schedule candidates."""

    _validate_v7_candidate_library(feasible_schedule_candidate_library_v7_frame)
    _validate_v7_backfill_requirements_frame(
        v2_plus_opportunity_backfill_requirements_frame
    )
    _require_columns(
        ua_context_oracle_gap_feature_panel_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "market_execution_enabled",
            }
        ),
        frame_name="UA context feature panel",
    )
    if ua_context_oracle_gap_feature_panel_frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError("V8 UA context backfill refuses market execution.")

    context_by_anchor: dict[tuple[str, str, datetime], dict[str, Any]] = {}
    for context_row in ua_context_oracle_gap_feature_panel_frame.iter_rows(named=True):
        key = _anchor_key(context_row)
        context_by_anchor.setdefault(key, context_row)
    requirements_by_anchor = {
        _anchor_key(row): row
        for row in v2_plus_opportunity_backfill_requirements_frame.iter_rows(named=True)
    }

    output_rows: list[dict[str, Any]] = []
    for row in feasible_schedule_candidate_library_v7_frame.iter_rows(named=True):
        key = _anchor_key(row)
        requirement = requirements_by_anchor.get(key)
        if requirement is None:
            raise ValueError(f"missing V8 backfill requirement row for {key}.")
        matching_context_row: dict[str, Any] | None = context_by_anchor.get(key)
        forecast = _float_vector(row["forecast_price_uah_mwh_vector"])
        peak_index, trough_index = _peak_trough_indices(forecast)
        blockers = _ua_context_blockers(matching_context_row, requirement)
        copied = dict(row)
        copied.update(
            {
                "feature_panel_version": "ua_context_backfill_v8",
                "selector_feature_ua_publication_context_ready": _context_feature(
                    matching_context_row,
                    "selector_feature_publication_time_ready",
                ),
                "selector_feature_ua_weather_load_context_ready": _context_feature(
                    matching_context_row,
                    "selector_feature_weather_load_context_ready",
                ),
                "selector_feature_ua_grid_event_context_ready": _context_feature(
                    matching_context_row,
                    "selector_feature_grid_event_context_ready",
                ),
                "selector_feature_ua_context_ready": 1.0 if not blockers else 0.0,
                "selector_feature_ua_peak_hour_index": float(peak_index),
                "selector_feature_ua_trough_hour_index": float(trough_index),
                "selector_feature_ua_peak_trough_distance_hours": float(
                    abs(peak_index - trough_index)
                ),
                "selector_feature_ua_forecast_spread_uah_mwh": (
                    max(forecast) - min(forecast) if forecast else 0.0
                ),
                "selector_feature_ua_morning_evening_spread_skew": (
                    _block_mean(forecast, range(17, 23))
                    - _block_mean(forecast, range(6, 11))
                ),
                "selector_feature_ua_terminal_soc_pressure": float(
                    bool(requirement["terminal_soc_pressure"])
                ),
                "selector_feature_ua_strict_local_rescue_hint": float(
                    bool(requirement["diagnostic_strict_control_material_local_win"])
                ),
                "diagnostic_ua_context_blockers": blockers,
                "diagnostic_v8_backfill_decision": str(
                    requirement["opportunity_backfill_decision"]
                ),
                "training_source_scope": (
                    "ukrainian_only_oree_open_meteo_tenant_grid"
                ),
                "claim_scope": REGRET_SURROGATE_UA_CONTEXT_BACKFILL_V8_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        output_rows.append(copied)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v8_context_panel(frame)
    return frame


def build_dfl_ua_context_feasible_schedule_candidate_library_v8_frame(
    ua_context_backfilled_feature_panel_v8_frame: pl.DataFrame,
    v2_plus_opportunity_backfill_requirements_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Add Ukrainian-context schedule candidates that require later strict rescore."""

    _validate_v8_context_panel(ua_context_backfilled_feature_panel_v8_frame)
    _validate_v7_backfill_requirements_frame(
        v2_plus_opportunity_backfill_requirements_frame
    )
    requirements_by_anchor = {
        _anchor_key(row): row
        for row in v2_plus_opportunity_backfill_requirements_frame.iter_rows(named=True)
    }
    output_rows: list[dict[str, Any]] = []
    grouped = _group_by_anchor(
        list(ua_context_backfilled_feature_panel_v8_frame.iter_rows(named=True))
    )
    for anchor_key, anchor_rows in sorted(grouped.items()):
        requirement = requirements_by_anchor.get(anchor_key)
        if requirement is None:
            raise ValueError(f"missing V8 backfill requirement row for {anchor_key}.")
        for row in anchor_rows:
            copied = dict(row)
            copied["eligible_for_final_selection_v8"] = bool(
                copied.get("eligible_for_final_selection_v7", True)
            )
            copied["candidate_value_label_status"] = copied.get(
                "candidate_value_label_status",
                "strict_scored_existing_candidate",
            )
            copied["diagnostic_requires_strict_rescore"] = False
            output_rows.append(copied)
        v2_row = _baseline_row(anchor_rows, anchor_key=anchor_key)
        strict_row, _ = _strict_reference_row(anchor_rows, baseline=v2_row)
        output_rows.extend(
            _v8_generated_candidate_specs(
                v2_row=v2_row,
                strict_row=strict_row,
                requirement=requirement,
            )
        )
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v8_candidate_library(frame)
    return frame


def build_dfl_ua_context_candidate_v8_strict_rescore_frame(
    ua_context_feasible_schedule_candidate_library_v8_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict-score V8 explicit schedules against actual prices and oracle value."""

    _validate_v8_candidate_library(
        ua_context_feasible_schedule_candidate_library_v8_frame
    )
    output_rows: list[dict[str, Any]] = []
    for row in ua_context_feasible_schedule_candidate_library_v8_frame.iter_rows(
        named=True
    ):
        if str(row["candidate_source"]) == _V8_GENERATED_CANDIDATE_SOURCE:
            output_rows.append(_rescore_v8_generated_candidate(row))
        else:
            copied = dict(row)
            copied["candidate_value_label_status"] = copied.get(
                "candidate_value_label_status",
                "strict_scored_existing_candidate",
            )
            copied["diagnostic_requires_strict_rescore"] = False
            copied["strict_rescore_version"] = "existing_candidate_score_reused"
            output_rows.append(copied)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v8_strict_rescore_frame(frame)
    return frame


def build_dfl_ua_context_candidate_value_teacher_label_panel_v8_frame(
    ua_context_candidate_v8_strict_rescore_frame: pl.DataFrame,
    v2_plus_opportunity_backfill_requirements_frame: pl.DataFrame,
    *,
    material_switch_delta_uah: float = 25.0,
    max_prior_neighbor_distance: float = 1.5,
    nearest_neighbor_count: int = 5,
) -> pl.DataFrame:
    """Rebuild V8 candidate-value labels after strict schedule rescore."""

    _validate_v8_strict_rescore_frame(ua_context_candidate_v8_strict_rescore_frame)
    _validate_v7_backfill_requirements_frame(
        v2_plus_opportunity_backfill_requirements_frame
    )
    if material_switch_delta_uah <= 0.0:
        raise ValueError("material_switch_delta_uah must be positive.")
    if max_prior_neighbor_distance < 0.0:
        raise ValueError("max_prior_neighbor_distance must not be negative.")
    if nearest_neighbor_count < 1:
        raise ValueError("nearest_neighbor_count must be at least 1.")
    rows = list(ua_context_candidate_v8_strict_rescore_frame.iter_rows(named=True))
    prior_rows = _v8_prior_candidate_rows(rows)
    prior_rows_by_group: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for prior in prior_rows:
        prior_rows_by_group.setdefault(_sparse_neighbor_group_key(prior), []).append(
            prior
        )
    feature_names = _sparse_distance_feature_names(
        ua_context_candidate_v8_strict_rescore_frame
    )
    requirements_by_anchor = {
        _anchor_key(row): row
        for row in v2_plus_opportunity_backfill_requirements_frame.iter_rows(named=True)
    }
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        requirement = requirements_by_anchor.get(_anchor_key(row))
        if requirement is None:
            raise ValueError(
                f"missing V8 backfill requirement row for {_anchor_key(row)}."
            )
        source = str(row["candidate_source"])
        eligible = bool(row.get("eligible_for_final_selection_v8", True))
        neighbor_stats = (
            _nearest_prior_neighbor_stats_from_candidates(
                row,
                prior_rows=prior_rows_by_group.get(_sparse_neighbor_group_key(row), []),
                feature_names=feature_names,
                nearest_neighbor_count=nearest_neighbor_count,
            )
            if str(row["split_name"]) == "final_holdout"
            and source not in _REFERENCE_CANDIDATE_SOURCES
            and eligible
            else _empty_neighbor_stats()
        )
        delta = float(row["label_regret_delta_vs_v2_plus_uah"])
        material_safe = (
            source not in _REFERENCE_CANDIDATE_SOURCES
            and eligible
            and delta <= -material_switch_delta_uah
        )
        feature_list = list(row.get("selected_feature_names", []))
        for feature_name in (
            "selector_feature_v8_nearest_prior_safe_switch_distance",
            "selector_feature_v8_neighbor_safe_win_count",
            "selector_feature_v8_neighbor_tail_risk_probability",
            "selector_feature_v8_neighbor_mean_delta_uah",
        ):
            if feature_name not in feature_list:
                feature_list.append(feature_name)
        copied = dict(row)
        copied.update(
            {
                "teacher_panel_version": "candidate_value_teacher_v8",
                "selected_feature_names": sorted(feature_list),
                "selector_feature_v8_nearest_prior_safe_switch_distance": float(
                    neighbor_stats["nearest_safe_distance"]
                ),
                "selector_feature_v8_nearest_prior_any_candidate_distance": float(
                    neighbor_stats["nearest_any_distance"]
                ),
                "selector_feature_v8_neighbor_support_count": float(
                    neighbor_stats["neighbor_count"]
                ),
                "selector_feature_v8_neighbor_safe_win_count": float(
                    neighbor_stats["safe_win_count"]
                ),
                "selector_feature_v8_neighbor_tail_risk_probability": float(
                    neighbor_stats["tail_risk_probability"]
                ),
                "selector_feature_v8_neighbor_mean_delta_uah": float(
                    neighbor_stats["mean_delta_uah"]
                ),
                "selector_feature_v8_has_prior_neighbor_support": float(
                    float(neighbor_stats["nearest_safe_distance"])
                    <= max_prior_neighbor_distance
                ),
                "label_v8_material_safe_switch": material_safe,
                "label_v8_tail_risk_loss": bool(row["label_tail_risk_loss"]),
                "label_v8_opportunity_backfill_decision": str(
                    requirement["opportunity_backfill_decision"]
                ),
                "diagnostic_v8_candidate_family_gap": bool(
                    requirement["candidate_family_gap"]
                ),
                "diagnostic_v8_strict_control_material_local_win": bool(
                    requirement["diagnostic_strict_control_material_local_win"]
                ),
                "claim_scope": REGRET_SURROGATE_UA_TEACHER_V8_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        output_rows.append(copied)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v8_teacher_panel(frame)
    return frame


def build_dfl_candidate_value_regret_surrogate_v8_frame(
    candidate_value_teacher_label_panel_v8_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    max_prior_neighbor_distance: float = 1.5,
    min_neighbor_safe_win_count: int = 1,
    min_predicted_improvement_uah: float = 1.0,
    max_neighbor_tail_risk_probability: float = 0.25,
    allowed_candidate_sources: tuple[str, ...] = _V8_ALLOWED_CANDIDATE_SOURCES,
    min_prior_material_safe_switch_examples_for_dt: int = 20,
) -> pl.DataFrame:
    """Train a conservative V8 selector over strict-rescored UA context schedules."""

    _validate_v8_teacher_panel(candidate_value_teacher_label_panel_v8_frame)
    _validate_sparse_selector_config(
        tenant_ids=tenant_ids,
        source_model_names=source_model_names,
        max_prior_neighbor_distance=max_prior_neighbor_distance,
        min_neighbor_safe_win_count=min_neighbor_safe_win_count,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_neighbor_tail_risk_probability=max_neighbor_tail_risk_probability,
        allowed_candidate_sources=allowed_candidate_sources,
    )
    if min_prior_material_safe_switch_examples_for_dt < 0:
        raise ValueError(
            "min_prior_material_safe_switch_examples_for_dt must not be negative."
        )
    rows = list(candidate_value_teacher_label_panel_v8_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in source_model_names:
            scope_rows = [
                row
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            ]
            prior_material_count = sum(
                1
                for row in scope_rows
                if bool(row.get("is_training_row", False))
                and bool(row.get("label_v8_material_safe_switch", False))
            )
            fitted = _fit_scope_v8_candidate_value(
                scope_rows,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                max_prior_neighbor_distance=max_prior_neighbor_distance,
                min_neighbor_safe_win_count=min_neighbor_safe_win_count,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_neighbor_tail_risk_probability=(max_neighbor_tail_risk_probability),
                allowed_candidate_sources=set(allowed_candidate_sources),
            )
            if prior_material_count < min_prior_material_safe_switch_examples_for_dt:
                fallback_keys = _final_anchor_keys(scope_rows)
                fitted.update(
                    {
                        "selected_final_candidate_keys": [],
                        "fallback_final_anchor_keys": fallback_keys,
                        "selected_final_candidate_count": 0,
                        "fallback_final_anchor_count": len(fallback_keys),
                        "selected_final_family_counts": {},
                        "selected_final_candidate_source_counts": {},
                        "fallback_to_v2_plus": True,
                        "uses_v2_plus_anchor_fallback": bool(fallback_keys),
                        "abstention_reason": "insufficient_prior_material_examples",
                    }
                )
            fitted.update(
                {
                    "learner_model_name": REGRET_SURROGATE_CANDIDATE_VALUE_V8_MODEL_NAME,
                    "selection_policy": (
                        "nearest_prior_strict_rescored_candidate_value_v8"
                    ),
                    "claim_scope": REGRET_SURROGATE_MODEL_V8_CLAIM_SCOPE,
                    "prior_material_safe_switch_example_count": prior_material_count,
                    "dt_lava_ready": (
                        prior_material_count
                        >= min_prior_material_safe_switch_examples_for_dt
                    ),
                }
            )
            output_rows.append(fitted)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def build_dfl_candidate_value_v8_strict_lp_benchmark_frame(
    candidate_value_teacher_label_panel_v8_frame: pl.DataFrame,
    candidate_value_regret_surrogate_v8_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score V8 candidate-value selections against corrected V2+."""

    _validate_v8_teacher_panel(candidate_value_teacher_label_panel_v8_frame)
    _validate_scorer_frame(candidate_value_regret_surrogate_v8_frame)
    resolved_generated_at = generated_at or _latest_generated_at(
        candidate_value_teacher_label_panel_v8_frame
    )
    panel_rows = list(
        candidate_value_teacher_label_panel_v8_frame.iter_rows(named=True)
    )
    candidate_by_key = {_candidate_key(row): row for row in panel_rows}
    v2_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in panel_rows:
        if str(row["split_name"]) != "final_holdout":
            continue
        source = str(row["candidate_source"])
        if source == _STRICT_CANDIDATE_SOURCE:
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=STRICT_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_CANDIDATE_VALUE_V8_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_CANDIDATE_VALUE_V8_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_STRICT_V8_CLAIM_SCOPE,
                )
            )
        elif source == _V2_PLUS_CANDIDATE_SOURCE:
            v2_by_anchor[_anchor_key_string(row)] = row
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=V2_PLUS_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_CANDIDATE_VALUE_V8_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_CANDIDATE_VALUE_V8_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_STRICT_V8_CLAIM_SCOPE,
                )
            )
    for scorer_row in candidate_value_regret_surrogate_v8_frame.iter_rows(named=True):
        for candidate_key in scorer_row["selected_final_candidate_keys"]:
            output_rows.append(
                _benchmark_row(
                    candidate_by_key[str(candidate_key)],
                    selection_role=REGRET_SURROGATE_CANDIDATE_VALUE_V8_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_CANDIDATE_VALUE_V8_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_CANDIDATE_VALUE_V8_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_STRICT_V8_CLAIM_SCOPE,
                )
            )
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            fallback = v2_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"missing V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _benchmark_row(
                    fallback,
                    selection_role=REGRET_SURROGATE_CANDIDATE_VALUE_V8_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_CANDIDATE_VALUE_V8_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_CANDIDATE_VALUE_V8_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_STRICT_V8_CLAIM_SCOPE,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def build_dfl_v8_false_positive_tail_risk_audit_frame(
    candidate_value_teacher_label_panel_v8_frame: pl.DataFrame,
    candidate_value_regret_surrogate_v8_frame: pl.DataFrame,
    *,
    false_positive_delta_uah: float = 0.0,
    material_switch_delta_uah: float = 25.0,
    tail_risk_delta_uah: float = 150.0,
    prune_tail_risk_probability_threshold: float = 0.50,
) -> pl.DataFrame:
    """Diagnose V8 selected-switch false positives and prior tail-risk families."""

    _validate_v8_teacher_panel(candidate_value_teacher_label_panel_v8_frame)
    _validate_scorer_frame(candidate_value_regret_surrogate_v8_frame)
    if material_switch_delta_uah <= 0.0:
        raise ValueError("material_switch_delta_uah must be positive.")
    if tail_risk_delta_uah <= 0.0:
        raise ValueError("tail_risk_delta_uah must be positive.")
    if not 0.0 <= prune_tail_risk_probability_threshold <= 1.0:
        raise ValueError("prune_tail_risk_probability_threshold must be in [0, 1].")

    selected_keys: set[str] = set()
    predicted_delta: dict[str, float] = {}
    predicted_tail: dict[str, float] = {}
    for scorer_row in candidate_value_regret_surrogate_v8_frame.iter_rows(named=True):
        selected_keys.update(str(key) for key in scorer_row["selected_final_candidate_keys"])
        predicted_delta.update(
            {
                str(key): float(value)
                for key, value in dict(
                    scorer_row["predicted_final_candidate_deltas"]
                ).items()
                if value is not None
            }
        )
        predicted_tail.update(
            {
                str(key): float(value)
                for key, value in dict(
                    scorer_row["predicted_final_tail_risk_probabilities"]
                ).items()
                if value is not None
            }
        )

    panel_rows = list(
        candidate_value_teacher_label_panel_v8_frame.iter_rows(named=True)
    )
    candidate_rows = [
        row
        for row in panel_rows
        if str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
    ]
    candidate_by_key = {_candidate_key(row): row for row in candidate_rows}
    missing_selected = sorted(key for key in selected_keys if key not in candidate_by_key)
    if missing_selected:
        raise ValueError(
            "V8 false-positive audit missing selected candidate rows: "
            + ", ".join(missing_selected[:3])
        )

    groups: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = {}
    for row in candidate_rows:
        groups.setdefault(_v8_family_group_key(row), []).append(row)

    audit_rows: list[dict[str, Any]] = []
    group_summaries: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for key, rows in sorted(groups.items()):
        summary = _v8_candidate_family_audit_row(
            key,
            rows,
            selected_keys=selected_keys,
            false_positive_delta_uah=false_positive_delta_uah,
            material_switch_delta_uah=material_switch_delta_uah,
            tail_risk_delta_uah=tail_risk_delta_uah,
            prune_tail_risk_probability_threshold=(
                prune_tail_risk_probability_threshold
            ),
        )
        group_summaries[key] = summary
        audit_rows.append(summary)

    for candidate_key in sorted(selected_keys):
        row = candidate_by_key[candidate_key]
        group_key = _v8_family_group_key(row)
        family_summary = group_summaries[group_key]
        delta = float(row["label_regret_delta_vs_v2_plus_uah"])
        false_positive_class = _v8_false_positive_class(
            row,
            false_positive_delta_uah=false_positive_delta_uah,
            material_switch_delta_uah=material_switch_delta_uah,
            tail_risk_delta_uah=tail_risk_delta_uah,
        )
        selected_action = _v8_selected_switch_next_action(
            false_positive_class,
            prior_pruned=bool(family_summary["prior_pruned_for_next_training"]),
        )
        audit_rows.append(
            {
                **_v8_audit_boundary_fields(
                    claim_scope=REGRET_SURROGATE_V8_FALSE_POSITIVE_AUDIT_CLAIM_SCOPE
                ),
                "audit_row_type": "selected_switch",
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
                "candidate_source": str(row["candidate_source"]),
                "candidate_family": str(row["candidate_family"]),
                "candidate_model_name": str(row["candidate_model_name"]),
                "candidate_key": candidate_key,
                "false_positive_class": false_positive_class,
                "recommended_next_action": selected_action,
                "prior_candidate_count": int(family_summary["prior_candidate_count"]),
                "prior_safe_win_count": int(family_summary["prior_safe_win_count"]),
                "prior_tail_risk_loss_count": int(
                    family_summary["prior_tail_risk_loss_count"]
                ),
                "prior_tail_risk_probability": float(
                    family_summary["prior_tail_risk_probability"]
                ),
                "prior_mean_delta_uah": float(family_summary["prior_mean_delta_uah"]),
                "final_candidate_count": int(family_summary["final_candidate_count"]),
                "final_safe_win_count": int(family_summary["final_safe_win_count"]),
                "final_tail_risk_loss_count": int(
                    family_summary["final_tail_risk_loss_count"]
                ),
                "selected_final_count": int(family_summary["selected_final_count"]),
                "selected_false_positive_count": int(
                    family_summary["selected_false_positive_count"]
                ),
                "selected_tail_risk_loss_count": int(
                    family_summary["selected_tail_risk_loss_count"]
                ),
                "selected_mean_delta_uah": float(
                    family_summary["selected_mean_delta_uah"]
                ),
                "selected_candidate_delta_uah": delta,
                "selected_predicted_delta_uah": predicted_delta.get(candidate_key, 0.0),
                "selected_predicted_tail_risk_probability": predicted_tail.get(
                    candidate_key, 1.0
                ),
                "prior_pruned_for_next_training": bool(
                    family_summary["prior_pruned_for_next_training"]
                ),
                "diagnostic_backfill_required": (
                    selected_action == "backfill_ukrainian_prior_context"
                ),
            }
        )

    return pl.DataFrame(audit_rows, infer_schema_length=None).sort(
        [
            "audit_row_type",
            "source_model_name",
            "tenant_id",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
            "anchor_timestamp",
        ]
    )


def build_dfl_v8_pruned_candidate_family_plan_frame(
    v8_false_positive_tail_risk_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Convert the V8 audit into a leakage-safe prune/backfill plan."""

    _require_columns(
        v8_false_positive_tail_risk_audit_frame,
        frozenset(
            {
                "audit_row_type",
                "tenant_id",
                "source_model_name",
                "candidate_source",
                "candidate_family",
                "candidate_model_name",
                "prior_pruned_for_next_training",
                "recommended_next_action",
                "prior_tail_risk_probability",
                "selected_false_positive_count",
                "selected_tail_risk_loss_count",
                "market_execution_enabled",
            }
        ),
        frame_name="V8 false-positive tail-risk audit frame",
    )
    if v8_false_positive_tail_risk_audit_frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError("V8 pruned family plan refuses market execution rows.")

    rows: list[dict[str, Any]] = []
    family_rows = v8_false_positive_tail_risk_audit_frame.filter(
        pl.col("audit_row_type") == "candidate_family"
    )
    for row in family_rows.iter_rows(named=True):
        prior_pruned = bool(row["prior_pruned_for_next_training"])
        recommended_action = str(row["recommended_next_action"])
        allowed = recommended_action in {
            "keep_candidate_family",
            "monitor_candidate_family",
        }
        if prior_pruned:
            if int(row["prior_tail_risk_loss_count"]) > int(row["prior_safe_win_count"]):
                blocked_reason = "prior_tail_risk_dominates_safe_wins"
            else:
                blocked_reason = "prior_tail_risk_probability_exceeds_threshold"
        elif recommended_action == "backfill_ukrainian_prior_context":
            blocked_reason = "needs_stronger_prior_context_before_training"
        else:
            blocked_reason = "none"
        rows.append(
            {
                **_v8_audit_boundary_fields(
                    claim_scope=REGRET_SURROGATE_V8_PRUNED_FAMILY_PLAN_CLAIM_SCOPE
                ),
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "candidate_source": str(row["candidate_source"]),
                "candidate_family": str(row["candidate_family"]),
                "candidate_model_name": str(row["candidate_model_name"]),
                "allowed_for_next_selector_training": allowed,
                "prior_pruned_for_next_training": prior_pruned,
                "recommended_next_action": recommended_action,
                "blocked_reason": blocked_reason,
                "prior_candidate_count": int(row["prior_candidate_count"]),
                "prior_safe_win_count": int(row["prior_safe_win_count"]),
                "prior_tail_risk_loss_count": int(row["prior_tail_risk_loss_count"]),
                "prior_tail_risk_probability": float(
                    row["prior_tail_risk_probability"]
                ),
                "selected_false_positive_count": int(
                    row["selected_false_positive_count"]
                ),
                "selected_tail_risk_loss_count": int(
                    row["selected_tail_risk_loss_count"]
                ),
                "diagnostic_backfill_required": (
                    recommended_action == "backfill_ukrainian_prior_context"
                ),
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def build_dfl_v8_pruned_candidate_library_frame(
    candidate_value_teacher_label_panel_v8_frame: pl.DataFrame,
    v8_pruned_candidate_family_plan_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Remove prior-risk candidate families while preserving strict/V2+ fallback rows."""

    _validate_v8_teacher_panel(candidate_value_teacher_label_panel_v8_frame)
    _require_columns(
        v8_pruned_candidate_family_plan_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "candidate_source",
                "candidate_family",
                "candidate_model_name",
                "allowed_for_next_selector_training",
                "market_execution_enabled",
            }
        ),
        frame_name="V8 pruned candidate family plan frame",
    )
    if v8_pruned_candidate_family_plan_frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError("V8 pruned candidate library refuses market execution rows.")

    blocked_profiles = {
        (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            str(row["candidate_source"]),
            str(row["candidate_family"]),
            str(row["candidate_model_name"]),
        )
        for row in v8_pruned_candidate_family_plan_frame.iter_rows(named=True)
        if not bool(row["allowed_for_next_selector_training"])
    }
    kept_rows: list[dict[str, Any]] = []
    for row in candidate_value_teacher_label_panel_v8_frame.iter_rows(named=True):
        copied = dict(row)
        source = str(copied["candidate_source"])
        if (
            source not in _REFERENCE_CANDIDATE_SOURCES
            and _v8_family_group_key(copied) in blocked_profiles
        ):
            continue
        copied.update(
            {
                "candidate_family_pruned_for_next_selector": False,
                "v8_pruned_candidate_library_version": "v8_tail_risk_pruned_v1",
                "claim_scope": REGRET_SURROGATE_V8_PRUNED_CANDIDATE_LIBRARY_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
                "raw_hourly_action_imitation": False,
            }
        )
        kept_rows.append(copied)

    frame = pl.DataFrame(kept_rows, infer_schema_length=None)
    _validate_v8_teacher_panel(frame)
    return frame.sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def build_dfl_v8_pruned_candidate_value_teacher_label_panel_frame(
    v8_pruned_candidate_library_frame: pl.DataFrame,
    *,
    material_switch_delta_uah: float = 25.0,
    max_prior_neighbor_distance: float = 1.5,
    nearest_neighbor_count: int = 5,
) -> pl.DataFrame:
    """Rebuild candidate-value labels and neighbor support after V8 family pruning."""

    _validate_v8_teacher_panel(v8_pruned_candidate_library_frame)
    if material_switch_delta_uah <= 0.0:
        raise ValueError("material_switch_delta_uah must be positive.")
    if max_prior_neighbor_distance < 0.0:
        raise ValueError("max_prior_neighbor_distance must not be negative.")
    if nearest_neighbor_count < 1:
        raise ValueError("nearest_neighbor_count must be at least 1.")

    rows = list(v8_pruned_candidate_library_frame.iter_rows(named=True))
    prior_rows = _v8_prior_candidate_rows(rows)
    prior_rows_by_group: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for prior in prior_rows:
        prior_rows_by_group.setdefault(_sparse_neighbor_group_key(prior), []).append(
            prior
        )
    feature_names = _sparse_distance_feature_names(v8_pruned_candidate_library_frame)

    output_rows: list[dict[str, Any]] = []
    for row in rows:
        source = str(row["candidate_source"])
        eligible = bool(row.get("eligible_for_final_selection_v8", True))
        neighbor_stats = (
            _nearest_prior_neighbor_stats_from_candidates(
                row,
                prior_rows=prior_rows_by_group.get(_sparse_neighbor_group_key(row), []),
                feature_names=feature_names,
                nearest_neighbor_count=nearest_neighbor_count,
            )
            if str(row["split_name"]) == "final_holdout"
            and source not in _REFERENCE_CANDIDATE_SOURCES
            and eligible
            else _empty_neighbor_stats()
        )
        delta = float(row["label_regret_delta_vs_v2_plus_uah"])
        material_safe = (
            source not in _REFERENCE_CANDIDATE_SOURCES
            and eligible
            and delta <= -material_switch_delta_uah
        )
        feature_list = list(row.get("selected_feature_names", []))
        for feature_name in (
            "selector_feature_v8_pruned_nearest_prior_safe_switch_distance",
            "selector_feature_v8_pruned_neighbor_safe_win_count",
            "selector_feature_v8_pruned_neighbor_tail_risk_probability",
            "selector_feature_v8_pruned_neighbor_mean_delta_uah",
        ):
            if feature_name not in feature_list:
                feature_list.append(feature_name)
        copied = dict(row)
        copied.update(
            {
                "teacher_panel_version": "candidate_value_teacher_v8_pruned",
                "selected_feature_names": sorted(feature_list),
                "selector_feature_v8_pruned_nearest_prior_safe_switch_distance": float(
                    neighbor_stats["nearest_safe_distance"]
                ),
                "selector_feature_v8_pruned_nearest_prior_any_candidate_distance": float(
                    neighbor_stats["nearest_any_distance"]
                ),
                "selector_feature_v8_pruned_neighbor_support_count": float(
                    neighbor_stats["neighbor_count"]
                ),
                "selector_feature_v8_pruned_neighbor_safe_win_count": float(
                    neighbor_stats["safe_win_count"]
                ),
                "selector_feature_v8_pruned_neighbor_tail_risk_probability": float(
                    neighbor_stats["tail_risk_probability"]
                ),
                "selector_feature_v8_pruned_neighbor_mean_delta_uah": float(
                    neighbor_stats["mean_delta_uah"]
                ),
                "selector_feature_v8_pruned_has_prior_neighbor_support": float(
                    float(neighbor_stats["nearest_safe_distance"])
                    <= max_prior_neighbor_distance
                ),
                "label_v8_pruned_material_safe_switch": material_safe,
                "label_v8_pruned_tail_risk_loss": bool(row["label_tail_risk_loss"]),
                "claim_scope": REGRET_SURROGATE_V8_PRUNED_TEACHER_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
                "raw_hourly_action_imitation": False,
            }
        )
        output_rows.append(copied)

    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v8_pruned_teacher_panel(frame)
    return frame


def build_dfl_v8_pruned_candidate_value_selector_frame(
    v8_pruned_candidate_value_teacher_label_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    max_prior_neighbor_distance: float = 1.5,
    min_neighbor_safe_win_count: int = 1,
    min_predicted_improvement_uah: float = 1.0,
    max_neighbor_tail_risk_probability: float = 0.25,
    allowed_candidate_sources: tuple[str, ...] = _V8_ALLOWED_CANDIDATE_SOURCES,
    min_prior_material_safe_switch_examples_for_dt: int = 20,
) -> pl.DataFrame:
    """Train a conservative selector only after V8 candidate-family pruning."""

    _validate_v8_pruned_teacher_panel(
        v8_pruned_candidate_value_teacher_label_panel_frame
    )
    _validate_sparse_selector_config(
        tenant_ids=tenant_ids,
        source_model_names=source_model_names,
        max_prior_neighbor_distance=max_prior_neighbor_distance,
        min_neighbor_safe_win_count=min_neighbor_safe_win_count,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_neighbor_tail_risk_probability=max_neighbor_tail_risk_probability,
        allowed_candidate_sources=allowed_candidate_sources,
    )
    if min_prior_material_safe_switch_examples_for_dt < 0:
        raise ValueError(
            "min_prior_material_safe_switch_examples_for_dt must not be negative."
        )

    rows = list(
        v8_pruned_candidate_value_teacher_label_panel_frame.iter_rows(named=True)
    )
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in source_model_names:
            scope_rows = [
                row
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            ]
            prior_material_count = sum(
                1
                for row in scope_rows
                if bool(row.get("is_training_row", False))
                and bool(row.get("label_v8_pruned_material_safe_switch", False))
            )
            fitted = _fit_scope_v8_pruned_candidate_value(
                scope_rows,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                max_prior_neighbor_distance=max_prior_neighbor_distance,
                min_neighbor_safe_win_count=min_neighbor_safe_win_count,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_neighbor_tail_risk_probability=(max_neighbor_tail_risk_probability),
                allowed_candidate_sources=set(allowed_candidate_sources),
            )
            if prior_material_count < min_prior_material_safe_switch_examples_for_dt:
                fallback_keys = _final_anchor_keys(scope_rows)
                fitted.update(
                    {
                        "selected_final_candidate_keys": [],
                        "fallback_final_anchor_keys": fallback_keys,
                        "selected_final_candidate_count": 0,
                        "fallback_final_anchor_count": len(fallback_keys),
                        "selected_final_family_counts": {},
                        "selected_final_candidate_source_counts": {},
                        "fallback_to_v2_plus": True,
                        "uses_v2_plus_anchor_fallback": bool(fallback_keys),
                        "abstention_reason": (
                            "insufficient_prior_material_examples_after_pruning"
                        ),
                    }
                )
            fitted.update(
                {
                    "learner_model_name": (
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_MODEL_NAME
                    ),
                    "selection_policy": "nearest_prior_pruned_candidate_value_v8",
                    "claim_scope": REGRET_SURROGATE_V8_PRUNED_SELECTOR_CLAIM_SCOPE,
                    "prior_material_safe_switch_example_count": prior_material_count,
                    "dt_lava_ready": (
                        prior_material_count
                        >= min_prior_material_safe_switch_examples_for_dt
                    ),
                }
            )
            output_rows.append(fitted)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def build_dfl_v8_pruned_candidate_value_strict_lp_benchmark_frame(
    v8_pruned_candidate_value_teacher_label_panel_frame: pl.DataFrame,
    v8_pruned_candidate_value_selector_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score the pruned V8 selector against corrected V2+."""

    _validate_v8_pruned_teacher_panel(
        v8_pruned_candidate_value_teacher_label_panel_frame
    )
    _validate_scorer_frame(v8_pruned_candidate_value_selector_frame)
    resolved_generated_at = generated_at or _latest_generated_at(
        v8_pruned_candidate_value_teacher_label_panel_frame
    )
    panel_rows = list(
        v8_pruned_candidate_value_teacher_label_panel_frame.iter_rows(named=True)
    )
    candidate_by_key = {_candidate_key(row): row for row in panel_rows}
    v2_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in panel_rows:
        if str(row["split_name"]) != "final_holdout":
            continue
        source = str(row["candidate_source"])
        if source == _STRICT_CANDIDATE_SOURCE:
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=STRICT_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_MODEL_NAME
                    ),
                    claim_scope=REGRET_SURROGATE_V8_PRUNED_STRICT_CLAIM_SCOPE,
                )
            )
        elif source == _V2_PLUS_CANDIDATE_SOURCE:
            v2_by_anchor[_anchor_key_string(row)] = row
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=V2_PLUS_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_MODEL_NAME
                    ),
                    claim_scope=REGRET_SURROGATE_V8_PRUNED_STRICT_CLAIM_SCOPE,
                )
            )
    for scorer_row in v8_pruned_candidate_value_selector_frame.iter_rows(named=True):
        for candidate_key in scorer_row["selected_final_candidate_keys"]:
            output_rows.append(
                _benchmark_row(
                    candidate_by_key[str(candidate_key)],
                    selection_role=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_SELECTION_ROLE
                    ),
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_MODEL_NAME
                    ),
                    claim_scope=REGRET_SURROGATE_V8_PRUNED_STRICT_CLAIM_SCOPE,
                )
            )
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            fallback = v2_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"missing V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _benchmark_row(
                    fallback,
                    selection_role=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_SELECTION_ROLE
                    ),
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=(
                        REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_MODEL_NAME
                    ),
                    claim_scope=REGRET_SURROGATE_V8_PRUNED_STRICT_CLAIM_SCOPE,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def build_dfl_ua_prior_context_backfilled_feature_panel_v9_frame(
    v8_pruned_candidate_value_teacher_label_panel_frame: pl.DataFrame,
    *,
    min_prior_context_neighbor_count: int = 3,
) -> pl.DataFrame:
    """Add stronger prior-only Ukrainian context support features before V9."""

    _validate_v8_pruned_teacher_panel(
        v8_pruned_candidate_value_teacher_label_panel_frame
    )
    if min_prior_context_neighbor_count < 1:
        raise ValueError("min_prior_context_neighbor_count must be at least 1.")
    rows = list(
        v8_pruned_candidate_value_teacher_label_panel_frame.iter_rows(named=True)
    )
    training_rows = [
        row
        for row in rows
        if bool(row.get("is_training_row", False))
        and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
    ]
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        prior_rows = [
            prior
            for prior in training_rows
            if str(prior["tenant_id"]) == str(row["tenant_id"])
            and str(prior["source_model_name"]) == str(row["source_model_name"])
            and _datetime_value(prior["anchor_timestamp"])
            < _datetime_value(row["anchor_timestamp"])
            and _v9_context_bucket(prior) == _v9_context_bucket(row)
        ]
        stats = _v9_prior_context_stats(prior_rows)
        blockers = _v9_prior_context_blockers(
            stats,
            min_prior_context_neighbor_count=min_prior_context_neighbor_count,
        )
        feature_list = list(row.get("selected_feature_names", []))
        for feature_name in (
            "selector_feature_v9_prior_context_neighbor_count",
            "selector_feature_v9_prior_context_safe_win_rate",
            "selector_feature_v9_prior_context_tail_risk_rate",
            "selector_feature_v9_prior_context_mean_delta_uah",
        ):
            if feature_name not in feature_list:
                feature_list.append(feature_name)
        copied = dict(row)
        copied.update(
            {
                "feature_panel_version": "ua_prior_context_backfill_v9",
                "selected_feature_names": sorted(feature_list),
                "selector_feature_v9_prior_context_neighbor_count": float(
                    stats["neighbor_count"]
                ),
                "selector_feature_v9_prior_context_safe_win_rate": float(
                    stats["safe_win_rate"]
                ),
                "selector_feature_v9_prior_context_tail_risk_rate": float(
                    stats["tail_risk_rate"]
                ),
                "selector_feature_v9_prior_context_mean_delta_uah": float(
                    stats["mean_delta_uah"]
                ),
                "selector_feature_v9_prior_context_ready": float(not blockers),
                "diagnostic_v9_prior_context_blockers": blockers,
                "training_source_scope": (
                    "ukrainian_only_oree_open_meteo_tenant_grid_prior_context"
                ),
                "claim_scope": REGRET_SURROGATE_UA_PRIOR_CONTEXT_V9_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
                "raw_hourly_action_imitation": False,
            }
        )
        output_rows.append(copied)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v9_context_panel(frame)
    return frame


def build_dfl_ua_non_tail_risk_candidate_library_v9_frame(
    ua_prior_context_backfilled_feature_panel_v9_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Generate bounded feasible schedules with no extra throughput over V2+."""

    _validate_v9_context_panel(ua_prior_context_backfilled_feature_panel_v9_frame)
    output_rows: list[dict[str, Any]] = []
    grouped = _group_by_anchor(
        list(ua_prior_context_backfilled_feature_panel_v9_frame.iter_rows(named=True))
    )
    for anchor_key, anchor_rows in sorted(grouped.items()):
        for row in anchor_rows:
            copied = dict(row)
            copied["eligible_for_final_selection_v9"] = bool(
                copied.get("eligible_for_final_selection_v8", True)
            )
            copied["diagnostic_requires_strict_rescore"] = False
            output_rows.append(copied)
        v2_row = _baseline_row(anchor_rows, anchor_key=anchor_key)
        output_rows.extend(_v9_generated_candidate_specs(v2_row=v2_row))
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v9_candidate_library(frame)
    return frame


def build_dfl_ua_non_tail_risk_candidate_v9_strict_rescore_frame(
    ua_non_tail_risk_candidate_library_v9_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict-score V9 generated schedules with the unchanged oracle evaluator."""

    _validate_v9_candidate_library(ua_non_tail_risk_candidate_library_v9_frame)
    output_rows: list[dict[str, Any]] = []
    for row in ua_non_tail_risk_candidate_library_v9_frame.iter_rows(named=True):
        if str(row["candidate_source"]) == _V9_GENERATED_CANDIDATE_SOURCE:
            output_rows.append(_rescore_v9_generated_candidate(row))
        else:
            copied = dict(row)
            copied["diagnostic_requires_strict_rescore"] = False
            copied["strict_rescore_version"] = copied.get(
                "strict_rescore_version",
                "existing_candidate_score_reused",
            )
            output_rows.append(copied)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v9_strict_rescore_frame(frame)
    return frame


def build_dfl_ua_non_tail_risk_candidate_value_teacher_label_panel_v9_frame(
    ua_non_tail_risk_candidate_v9_strict_rescore_frame: pl.DataFrame,
    *,
    material_switch_delta_uah: float = 25.0,
    max_prior_neighbor_distance: float = 1.5,
    nearest_neighbor_count: int = 5,
) -> pl.DataFrame:
    """Rebuild candidate-value labels after V9 non-tail-risk strict rescore."""

    _validate_v9_strict_rescore_frame(
        ua_non_tail_risk_candidate_v9_strict_rescore_frame
    )
    if material_switch_delta_uah <= 0.0:
        raise ValueError("material_switch_delta_uah must be positive.")
    if max_prior_neighbor_distance < 0.0:
        raise ValueError("max_prior_neighbor_distance must not be negative.")
    if nearest_neighbor_count < 1:
        raise ValueError("nearest_neighbor_count must be at least 1.")

    rows = list(
        ua_non_tail_risk_candidate_v9_strict_rescore_frame.iter_rows(named=True)
    )
    prior_rows = _v9_prior_candidate_rows(rows)
    prior_rows_by_group: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for prior in prior_rows:
        prior_rows_by_group.setdefault(_sparse_neighbor_group_key(prior), []).append(
            prior
        )
    feature_names = _sparse_distance_feature_names(
        ua_non_tail_risk_candidate_v9_strict_rescore_frame
    )
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        source = str(row["candidate_source"])
        eligible = bool(row.get("eligible_for_final_selection_v9", True))
        neighbor_stats = (
            _nearest_prior_neighbor_stats_from_candidates(
                row,
                prior_rows=prior_rows_by_group.get(_sparse_neighbor_group_key(row), []),
                feature_names=feature_names,
                nearest_neighbor_count=nearest_neighbor_count,
            )
            if str(row["split_name"]) == "final_holdout"
            and source not in _REFERENCE_CANDIDATE_SOURCES
            and eligible
            else _empty_neighbor_stats()
        )
        delta = float(row["label_regret_delta_vs_v2_plus_uah"])
        tail_risk_loss = bool(row["label_tail_risk_loss"])
        material_safe = (
            source not in _REFERENCE_CANDIDATE_SOURCES
            and eligible
            and delta <= -material_switch_delta_uah
        )
        generated_tail_risk_rejected = (
            source == _V9_GENERATED_CANDIDATE_SOURCE and tail_risk_loss
        )
        feature_list = list(row.get("selected_feature_names", []))
        for feature_name in (
            "selector_feature_v9_nearest_prior_safe_switch_distance",
            "selector_feature_v9_neighbor_safe_win_count",
            "selector_feature_v9_neighbor_tail_risk_probability",
            "selector_feature_v9_neighbor_mean_delta_uah",
        ):
            if feature_name not in feature_list:
                feature_list.append(feature_name)
        copied = dict(row)
        copied.update(
            {
                "teacher_panel_version": "candidate_value_teacher_v9_non_tail_risk",
                "selected_feature_names": sorted(feature_list),
                "selector_feature_v9_nearest_prior_safe_switch_distance": float(
                    neighbor_stats["nearest_safe_distance"]
                ),
                "selector_feature_v9_nearest_prior_any_candidate_distance": float(
                    neighbor_stats["nearest_any_distance"]
                ),
                "selector_feature_v9_neighbor_support_count": float(
                    neighbor_stats["neighbor_count"]
                ),
                "selector_feature_v9_neighbor_safe_win_count": float(
                    neighbor_stats["safe_win_count"]
                ),
                "selector_feature_v9_neighbor_tail_risk_probability": float(
                    neighbor_stats["tail_risk_probability"]
                ),
                "selector_feature_v9_neighbor_mean_delta_uah": float(
                    neighbor_stats["mean_delta_uah"]
                ),
                "selector_feature_v9_has_prior_neighbor_support": float(
                    float(neighbor_stats["nearest_safe_distance"])
                    <= max_prior_neighbor_distance
                ),
                "label_v9_material_safe_switch": material_safe,
                "label_v9_tail_risk_loss": tail_risk_loss,
                "diagnostic_v9_tail_risk_rejected": generated_tail_risk_rejected,
                "eligible_for_next_selector_training_v9": (
                    source not in _REFERENCE_CANDIDATE_SOURCES
                    and bool(row.get("is_training_row", False))
                    and eligible
                    and not tail_risk_loss
                ),
                "claim_scope": (
                    REGRET_SURROGATE_UA_NON_TAIL_RISK_TEACHER_V9_CLAIM_SCOPE
                ),
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
                "raw_hourly_action_imitation": False,
            }
        )
        output_rows.append(copied)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_v9_teacher_panel(frame)
    return frame


def build_dfl_candidate_value_v8_rolling_robustness_frame(
    ua_context_candidate_v8_strict_rescore_frame: pl.DataFrame,
    v2_plus_opportunity_backfill_requirements_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    material_switch_delta_uah: float = 25.0,
    max_prior_neighbor_distance: float = 1.5,
    min_neighbor_safe_win_count: int = 1,
    min_predicted_improvement_uah: float = 1.0,
    max_neighbor_tail_risk_probability: float = 0.25,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    allowed_candidate_sources: tuple[str, ...] = _V8_ALLOWED_CANDIDATE_SOURCES,
    min_prior_material_safe_switch_examples_for_dt: int = 20,
) -> pl.DataFrame:
    """Replay V8 candidate-value selection over prior-only rolling windows."""

    _validate_v8_strict_rescore_frame(ua_context_candidate_v8_strict_rescore_frame)
    _validate_v7_backfill_requirements_frame(
        v2_plus_opportunity_backfill_requirements_frame
    )
    if validation_window_count <= 0:
        raise ValueError("validation_window_count must be positive.")
    if validation_anchor_count <= 0:
        raise ValueError("validation_anchor_count must be positive.")
    rows = list(ua_context_candidate_v8_strict_rescore_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        anchors = sorted(
            {
                _datetime_value(row["anchor_timestamp"])
                for row in rows
                if str(row["source_model_name"]) == source_model_name
            }
        )
        source_window_rows: list[dict[str, Any]] = []
        for window_index in range(validation_window_count):
            end = len(anchors) - window_index * validation_anchor_count
            start = end - validation_anchor_count
            if start < 0:
                break
            validation_anchors = tuple(anchors[start:end])
            prior_anchors = tuple(anchors[:start])
            if len(prior_anchors) < min_prior_anchors_before_window:
                continue
            window_frame = _window_teacher_panel(
                rows,
                source_model_name=source_model_name,
                prior_anchors=set(prior_anchors),
                validation_anchors=set(validation_anchors),
            )
            teacher_v8 = build_dfl_ua_context_candidate_value_teacher_label_panel_v8_frame(
                window_frame,
                v2_plus_opportunity_backfill_requirements_frame,
                material_switch_delta_uah=material_switch_delta_uah,
                max_prior_neighbor_distance=max_prior_neighbor_distance,
            )
            model = build_dfl_candidate_value_regret_surrogate_v8_frame(
                teacher_v8,
                tenant_ids=tenant_ids,
                source_model_names=(source_model_name,),
                max_prior_neighbor_distance=max_prior_neighbor_distance,
                min_neighbor_safe_win_count=min_neighbor_safe_win_count,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_neighbor_tail_risk_probability=(max_neighbor_tail_risk_probability),
                allowed_candidate_sources=allowed_candidate_sources,
                min_prior_material_safe_switch_examples_for_dt=(
                    min_prior_material_safe_switch_examples_for_dt
                ),
            )
            summary = _sparse_rolling_summary_row(
                teacher_v8,
                model,
                source_model_name=source_model_name,
                window_index=window_index,
                validation_anchors=validation_anchors,
                prior_anchors=prior_anchors,
                min_mean_regret_improvement_ratio_vs_v2_plus=(
                    min_mean_regret_improvement_ratio_vs_v2_plus
                ),
            )
            summary["claim_scope"] = REGRET_SURROGATE_ROBUSTNESS_V8_CLAIM_SCOPE
            summary["selection_policy"] = (
                "nearest_prior_strict_rescored_candidate_value_v8"
            )
            source_window_rows.append(summary)
        pass_count = sum(
            1 for row in source_window_rows if bool(row["rolling_window_passed"])
        )
        diagnostic_count = sum(
            1 for row in source_window_rows if bool(row["diagnostic_window_passed"])
        )
        for row in source_window_rows:
            row["passing_window_count_for_source"] = pass_count
            row["diagnostic_window_count_for_source"] = diagnostic_count
            row["robust_candidate_value_v8_challenger"] = (
                pass_count >= validation_window_count
            )
            row["diagnostic_signal_learnable"] = diagnostic_count >= min(
                validation_window_count,
                3,
            )
            row["production_promote"] = False
        output_rows.extend(source_window_rows)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "window_index"]
    )


def build_dfl_candidate_value_teacher_label_panel_v7_frame(
    feasible_schedule_candidate_library_v7_frame: pl.DataFrame,
    v2_plus_opportunity_backfill_requirements_frame: pl.DataFrame,
    *,
    material_switch_delta_uah: float = 25.0,
    max_prior_neighbor_distance: float = 1.5,
    nearest_neighbor_count: int = 5,
) -> pl.DataFrame:
    """Attach V7 teacher labels and nearest-prior support features."""

    _validate_v7_candidate_library(feasible_schedule_candidate_library_v7_frame)
    _validate_v7_backfill_requirements_frame(
        v2_plus_opportunity_backfill_requirements_frame
    )
    if material_switch_delta_uah <= 0.0:
        raise ValueError("material_switch_delta_uah must be positive.")
    if max_prior_neighbor_distance < 0.0:
        raise ValueError("max_prior_neighbor_distance must not be negative.")
    if nearest_neighbor_count < 1:
        raise ValueError("nearest_neighbor_count must be at least 1.")
    rows = list(feasible_schedule_candidate_library_v7_frame.iter_rows(named=True))
    prior_rows = _sparse_prior_candidate_rows(rows)
    prior_rows_by_group: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for prior in prior_rows:
        prior_rows_by_group.setdefault(_sparse_neighbor_group_key(prior), []).append(
            prior
        )
    feature_names = _sparse_distance_feature_names(
        feasible_schedule_candidate_library_v7_frame
    )
    requirements_by_anchor = {
        _anchor_key(row): row
        for row in v2_plus_opportunity_backfill_requirements_frame.iter_rows(named=True)
    }
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        requirement = requirements_by_anchor.get(_anchor_key(row))
        if requirement is None:
            raise ValueError(
                f"missing V7 backfill requirement row for {_anchor_key(row)}."
            )
        source = str(row["candidate_source"])
        eligible = bool(row.get("eligible_for_final_selection_v7", True))
        neighbor_stats = (
            _nearest_prior_neighbor_stats_from_candidates(
                row,
                prior_rows=prior_rows_by_group.get(_sparse_neighbor_group_key(row), []),
                feature_names=feature_names,
                nearest_neighbor_count=nearest_neighbor_count,
            )
            if str(row["split_name"]) == "final_holdout"
            and source not in _REFERENCE_CANDIDATE_SOURCES
            and eligible
            else _empty_neighbor_stats()
        )
        delta = float(row["label_regret_delta_vs_v2_plus_uah"])
        material_safe = (
            source not in _REFERENCE_CANDIDATE_SOURCES
            and eligible
            and delta <= -material_switch_delta_uah
        )
        feature_list = list(row.get("selected_feature_names", []))
        for feature_name in (
            "selector_feature_nearest_prior_safe_switch_distance",
            "selector_feature_neighbor_safe_win_count",
            "selector_feature_neighbor_tail_risk_probability",
            "selector_feature_neighbor_mean_delta_uah",
        ):
            if feature_name not in feature_list:
                feature_list.append(feature_name)
        copied = dict(row)
        copied.update(
            {
                "teacher_panel_version": "candidate_value_teacher_v7",
                "selected_feature_names": sorted(feature_list),
                "selector_feature_nearest_prior_safe_switch_distance": float(
                    neighbor_stats["nearest_safe_distance"]
                ),
                "selector_feature_nearest_prior_any_candidate_distance": float(
                    neighbor_stats["nearest_any_distance"]
                ),
                "selector_feature_neighbor_support_count": float(
                    neighbor_stats["neighbor_count"]
                ),
                "selector_feature_neighbor_safe_win_count": float(
                    neighbor_stats["safe_win_count"]
                ),
                "selector_feature_neighbor_tail_risk_probability": float(
                    neighbor_stats["tail_risk_probability"]
                ),
                "selector_feature_neighbor_mean_delta_uah": float(
                    neighbor_stats["mean_delta_uah"]
                ),
                "selector_feature_has_prior_neighbor_support": float(
                    float(neighbor_stats["nearest_safe_distance"])
                    <= max_prior_neighbor_distance
                ),
                "label_sparse_material_safe_switch": material_safe,
                "label_sparse_opportunity_class": str(
                    requirement["opportunity_backfill_decision"]
                ),
                "label_v7_material_safe_switch": material_safe,
                "label_v7_tail_risk_loss": bool(row["label_tail_risk_loss"]),
                "label_v7_opportunity_backfill_decision": str(
                    requirement["opportunity_backfill_decision"]
                ),
                "diagnostic_v7_candidate_family_gap": bool(
                    requirement["candidate_family_gap"]
                ),
                "diagnostic_v7_strict_control_material_local_win": bool(
                    requirement["diagnostic_strict_control_material_local_win"]
                ),
                "claim_scope": REGRET_SURROGATE_TEACHER_V7_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        output_rows.append(copied)
    frame = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_sparse_teacher_panel(frame)
    _validate_v7_teacher_panel(frame)
    return frame


def build_dfl_candidate_value_regret_surrogate_v7_frame(
    candidate_value_teacher_label_panel_v7_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    max_prior_neighbor_distance: float = 1.5,
    min_neighbor_safe_win_count: int = 1,
    min_predicted_improvement_uah: float = 1.0,
    max_neighbor_tail_risk_probability: float = 0.25,
    allowed_candidate_sources: tuple[str, ...] = _V7_ALLOWED_CANDIDATE_SOURCES,
    min_prior_material_safe_switch_examples_for_dt: int = 20,
) -> pl.DataFrame:
    """Train a conservative V7 candidate-value selector with V2+ fallback."""

    _validate_v7_teacher_panel(candidate_value_teacher_label_panel_v7_frame)
    _validate_sparse_selector_config(
        tenant_ids=tenant_ids,
        source_model_names=source_model_names,
        max_prior_neighbor_distance=max_prior_neighbor_distance,
        min_neighbor_safe_win_count=min_neighbor_safe_win_count,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_neighbor_tail_risk_probability=max_neighbor_tail_risk_probability,
        allowed_candidate_sources=allowed_candidate_sources,
    )
    if min_prior_material_safe_switch_examples_for_dt < 0:
        raise ValueError(
            "min_prior_material_safe_switch_examples_for_dt must not be negative."
        )
    rows = list(candidate_value_teacher_label_panel_v7_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in source_model_names:
            scope_rows = [
                row
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            ]
            prior_material_count = sum(
                1
                for row in scope_rows
                if bool(row.get("is_training_row", False))
                and bool(row.get("label_v7_material_safe_switch", False))
            )
            fitted = _fit_scope_sparse_safe_switch(
                scope_rows,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                max_prior_neighbor_distance=max_prior_neighbor_distance,
                min_neighbor_safe_win_count=min_neighbor_safe_win_count,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_neighbor_tail_risk_probability=(max_neighbor_tail_risk_probability),
                allowed_candidate_sources=set(allowed_candidate_sources),
            )
            if prior_material_count < min_prior_material_safe_switch_examples_for_dt:
                fallback_keys = _final_anchor_keys(scope_rows)
                fitted.update(
                    {
                        "selected_final_candidate_keys": [],
                        "fallback_final_anchor_keys": fallback_keys,
                        "selected_final_candidate_count": 0,
                        "fallback_final_anchor_count": len(fallback_keys),
                        "selected_final_family_counts": {},
                        "selected_final_candidate_source_counts": {},
                        "fallback_to_v2_plus": True,
                        "uses_v2_plus_anchor_fallback": bool(fallback_keys),
                        "abstention_reason": "insufficient_prior_material_examples",
                    }
                )
            fitted.update(
                {
                    "learner_model_name": REGRET_SURROGATE_CANDIDATE_VALUE_V7_MODEL_NAME,
                    "selection_policy": ("nearest_prior_candidate_value_backfill_v7"),
                    "claim_scope": REGRET_SURROGATE_MODEL_V7_CLAIM_SCOPE,
                    "prior_material_safe_switch_example_count": prior_material_count,
                    "dt_lava_ready": (
                        prior_material_count
                        >= min_prior_material_safe_switch_examples_for_dt
                    ),
                }
            )
            output_rows.append(fitted)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def build_dfl_candidate_value_v7_strict_lp_benchmark_frame(
    candidate_value_teacher_label_panel_v7_frame: pl.DataFrame,
    candidate_value_regret_surrogate_v7_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score V7 candidate-value selections against corrected V2+."""

    _validate_v7_teacher_panel(candidate_value_teacher_label_panel_v7_frame)
    _validate_scorer_frame(candidate_value_regret_surrogate_v7_frame)
    resolved_generated_at = generated_at or _latest_generated_at(
        candidate_value_teacher_label_panel_v7_frame
    )
    panel_rows = list(
        candidate_value_teacher_label_panel_v7_frame.iter_rows(named=True)
    )
    candidate_by_key = {_candidate_key(row): row for row in panel_rows}
    v2_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in panel_rows:
        if str(row["split_name"]) != "final_holdout":
            continue
        source = str(row["candidate_source"])
        if source == _STRICT_CANDIDATE_SOURCE:
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=STRICT_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_CANDIDATE_VALUE_V7_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_CANDIDATE_VALUE_V7_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_STRICT_V7_CLAIM_SCOPE,
                )
            )
        elif source == _V2_PLUS_CANDIDATE_SOURCE:
            v2_by_anchor[_anchor_key_string(row)] = row
            output_rows.append(
                _benchmark_row(
                    row,
                    selection_role=V2_PLUS_REFERENCE_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_CANDIDATE_VALUE_V7_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_CANDIDATE_VALUE_V7_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_STRICT_V7_CLAIM_SCOPE,
                )
            )
    for scorer_row in candidate_value_regret_surrogate_v7_frame.iter_rows(named=True):
        for candidate_key in scorer_row["selected_final_candidate_keys"]:
            output_rows.append(
                _benchmark_row(
                    candidate_by_key[str(candidate_key)],
                    selection_role=REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_CANDIDATE_VALUE_V7_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_CANDIDATE_VALUE_V7_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_STRICT_V7_CLAIM_SCOPE,
                )
            )
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            fallback = v2_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"missing V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _benchmark_row(
                    fallback,
                    selection_role=REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                    strategy_kind=(
                        REGRET_SURROGATE_CANDIDATE_VALUE_V7_STRICT_LP_STRATEGY_KIND
                    ),
                    challenger_model_name=REGRET_SURROGATE_CANDIDATE_VALUE_V7_MODEL_NAME,
                    claim_scope=REGRET_SURROGATE_STRICT_V7_CLAIM_SCOPE,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def build_dfl_candidate_value_v7_rolling_robustness_frame(
    feasible_schedule_candidate_library_v7_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    material_switch_delta_uah: float = 25.0,
    max_prior_neighbor_distance: float = 1.5,
    min_neighbor_safe_win_count: int = 1,
    min_predicted_improvement_uah: float = 1.0,
    max_neighbor_tail_risk_probability: float = 0.25,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    allowed_candidate_sources: tuple[str, ...] = _V7_ALLOWED_CANDIDATE_SOURCES,
    min_prior_material_safe_switch_examples_for_dt: int = 20,
) -> pl.DataFrame:
    """Replay V7 candidate-value selection over prior-only rolling windows."""

    _validate_v7_candidate_library(feasible_schedule_candidate_library_v7_frame)
    if validation_window_count <= 0:
        raise ValueError("validation_window_count must be positive.")
    if validation_anchor_count <= 0:
        raise ValueError("validation_anchor_count must be positive.")
    rows = list(feasible_schedule_candidate_library_v7_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        anchors = sorted(
            {
                _datetime_value(row["anchor_timestamp"])
                for row in rows
                if str(row["source_model_name"]) == source_model_name
            }
        )
        source_window_rows: list[dict[str, Any]] = []
        for window_index in range(validation_window_count):
            end = len(anchors) - window_index * validation_anchor_count
            start = end - validation_anchor_count
            if start < 0:
                break
            validation_anchors = tuple(anchors[start:end])
            prior_anchors = tuple(anchors[:start])
            if len(prior_anchors) < min_prior_anchors_before_window:
                continue
            window_frame = _window_teacher_panel(
                rows,
                source_model_name=source_model_name,
                prior_anchors=set(prior_anchors),
                validation_anchors=set(validation_anchors),
            )
            audit = build_dfl_sparse_safe_switch_opportunity_audit_frame(
                window_frame,
                material_switch_delta_uah=material_switch_delta_uah,
                max_prior_neighbor_distance=max_prior_neighbor_distance,
                min_neighbor_safe_win_count=min_neighbor_safe_win_count,
                max_neighbor_tail_risk_probability=(max_neighbor_tail_risk_probability),
            )
            requirements = build_dfl_v2_plus_opportunity_backfill_requirements_frame(
                window_frame,
                audit,
                material_switch_delta_uah=material_switch_delta_uah,
                min_prior_material_examples_for_dt=(
                    min_prior_material_safe_switch_examples_for_dt
                ),
                min_oracle_improvement_ratio_vs_v2_plus=(
                    min_mean_regret_improvement_ratio_vs_v2_plus
                ),
            )
            teacher_v7 = build_dfl_candidate_value_teacher_label_panel_v7_frame(
                window_frame,
                requirements,
                material_switch_delta_uah=material_switch_delta_uah,
                max_prior_neighbor_distance=max_prior_neighbor_distance,
            )
            model = build_dfl_candidate_value_regret_surrogate_v7_frame(
                teacher_v7,
                tenant_ids=tenant_ids,
                source_model_names=(source_model_name,),
                max_prior_neighbor_distance=max_prior_neighbor_distance,
                min_neighbor_safe_win_count=min_neighbor_safe_win_count,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_neighbor_tail_risk_probability=(max_neighbor_tail_risk_probability),
                allowed_candidate_sources=allowed_candidate_sources,
                min_prior_material_safe_switch_examples_for_dt=(
                    min_prior_material_safe_switch_examples_for_dt
                ),
            )
            summary = _sparse_rolling_summary_row(
                teacher_v7,
                model,
                source_model_name=source_model_name,
                window_index=window_index,
                validation_anchors=validation_anchors,
                prior_anchors=prior_anchors,
                min_mean_regret_improvement_ratio_vs_v2_plus=(
                    min_mean_regret_improvement_ratio_vs_v2_plus
                ),
            )
            summary["claim_scope"] = REGRET_SURROGATE_ROBUSTNESS_V7_CLAIM_SCOPE
            summary["selection_policy"] = "nearest_prior_candidate_value_backfill_v7"
            source_window_rows.append(summary)
        pass_count = sum(
            1 for row in source_window_rows if bool(row["rolling_window_passed"])
        )
        diagnostic_count = sum(
            1 for row in source_window_rows if bool(row["diagnostic_window_passed"])
        )
        for row in source_window_rows:
            row["passing_window_count_for_source"] = pass_count
            row["diagnostic_window_count_for_source"] = diagnostic_count
            row["robust_candidate_value_v7_challenger"] = (
                pass_count >= validation_window_count
            )
            row["diagnostic_signal_learnable"] = diagnostic_count >= min(
                validation_window_count,
                3,
            )
            row["production_promote"] = False
        output_rows.extend(source_window_rows)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "window_index"]
    )


def evaluate_dfl_regret_surrogate_gate(
    strict_frame: pl.DataFrame,
    *,
    min_validation_tenant_anchor_count: int = 90,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Require regret-surrogate DFL to beat corrected V2+ before promotion."""

    _require_columns(
        strict_frame,
        frozenset(
            {
                "tenant_id",
                "selection_role",
                "anchor_timestamp",
                "regret_uah",
                "safety_violation_count",
                "not_market_execution",
                "market_execution_enabled",
            }
        ),
        frame_name="regret_surrogate_strict_frame",
    )
    summaries = _role_summaries(strict_frame)
    selected = summaries.get(REGRET_SURROGATE_SELECTION_ROLE)
    v2_plus = summaries.get(V2_PLUS_REFERENCE_ROLE)
    strict = summaries.get(STRICT_REFERENCE_ROLE)
    validation_count = _tenant_anchor_count(
        strict_frame.filter(pl.col("selection_role") == REGRET_SURROGATE_SELECTION_ROLE)
    )
    failures: list[str] = []
    if selected is None:
        failures.append("missing regret-surrogate rows")
    if v2_plus is None:
        failures.append("missing corrected V2+ reference rows")
    if strict is None:
        failures.append("missing strict reference rows")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            "regret-surrogate validation tenant-anchor count below required "
            f"{min_validation_tenant_anchor_count}"
        )
    if failures or selected is None or v2_plus is None or strict is None:
        return PromotionGateResult(
            False,
            "blocked",
            "; ".join(failures),
            {"role_summaries": summaries, "market_execution_enabled": False},
        )
    improvement_vs_v2 = _improvement_ratio(
        float(v2_plus["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    improvement_vs_strict = _improvement_ratio(
        float(strict["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    median_not_worse = float(selected["median_regret_uah"]) <= float(
        v2_plus["median_regret_uah"]
    )
    safety_ok = int(selected["safety_violation_count"]) == 0
    no_market_execution = _no_market_execution(strict_frame)
    passed = (
        improvement_vs_v2 >= min_mean_regret_improvement_ratio_vs_v2_plus
        and improvement_vs_strict >= min_mean_regret_improvement_ratio_vs_strict
        and median_not_worse
        and safety_ok
        and no_market_execution
    )
    diagnostic = improvement_vs_v2 > 0.0 and median_not_worse and safety_ok
    return PromotionGateResult(
        passed,
        "offline_strategy_challenger" if passed else "blocked",
        (
            "Regret-surrogate DFL v1 passed the frozen V2+ gate."
            if passed
            else "Regret-surrogate DFL v1 did not beat frozen V2+."
        ),
        {
            "role_summaries": summaries,
            "validation_tenant_anchor_count": validation_count,
            "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2,
            "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
            "median_not_worse_vs_v2_plus": median_not_worse,
            "safety_ok": safety_ok,
            "diagnostic_signal_passed": diagnostic,
            "market_execution_enabled": False,
            "production_promote": False,
        },
    )


def _fit_scope_surrogate(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
    min_prior_safe_win_count: int,
    min_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_predicted_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
    use_cuda_if_available: bool,
) -> dict[str, Any]:
    train_rows = [
        row
        for row in rows
        if bool(row["is_training_row"])
        and bool(row["eligible_for_final_selection"])
        and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
    ]
    final_rows = [
        row
        for row in rows
        if str(row["split_name"]) == "final_holdout"
        and bool(row["eligible_for_final_selection"])
    ]
    if not rows:
        raise ValueError(f"{tenant_id}/{source_model_name} has no teacher rows.")
    if not train_rows:
        raise ValueError(
            f"{tenant_id}/{source_model_name} regret surrogate needs train rows."
        )
    if not final_rows:
        raise ValueError(
            f"{tenant_id}/{source_model_name} regret surrogate needs final rows."
        )
    challenger_train = [
        row
        for row in train_rows
        if str(row["candidate_source"]) in allowed_candidate_sources
    ]
    if not challenger_train:
        raise ValueError(
            f"{tenant_id}/{source_model_name} regret surrogate needs challenger train rows."
        )
    profile_stats = _profile_stats(challenger_train)
    allowed_profiles = sorted(
        profile
        for profile, stats in profile_stats.items()
        if int(stats["safe_win_count"]) >= min_prior_safe_win_count
        and float(stats["mean_delta_uah"]) <= -min_prior_mean_improvement_uah
        and float(stats["tail_risk_probability"]) <= max_predicted_tail_risk_probability
    )
    selected_final, fallback_keys, predicted_delta, predicted_tail = (
        _select_final_candidates(
            final_rows,
            profile_stats=profile_stats,
            allowed_profiles=set(allowed_profiles),
            allowed_candidate_sources=allowed_candidate_sources,
            min_predicted_improvement_uah=min_predicted_improvement_uah,
            max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
        )
    )
    model_backend, model_device = _runtime_metadata(use_cuda_if_available)
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "learner_model_name": REGRET_SURROGATE_MODEL_NAME,
        "target_label_space": "schedule_candidate_value_delta",
        "selected_model_backend": model_backend,
        "torch_device": model_device,
        "allowed_candidate_sources": sorted(allowed_candidate_sources),
        "allowed_schedule_candidate_profiles": allowed_profiles,
        "schedule_candidate_profile_prior_stats": profile_stats,
        "selected_feature_names": _selector_feature_columns(pl.DataFrame(rows)),
        "min_predicted_improvement_uah": min_predicted_improvement_uah,
        "max_predicted_tail_risk_probability": max_predicted_tail_risk_probability,
        "fallback_to_v2_plus": not selected_final,
        "uses_v2_plus_anchor_fallback": bool(fallback_keys),
        "selector_gate_blocker": (
            "regret_surrogate_candidate_selected"
            if selected_final
            else "no_prior_safe_regret_surrogate_profile"
        ),
        "train_anchor_count": _anchor_count(train_rows),
        "final_holdout_anchor_count": _anchor_count(final_rows),
        "selected_final_candidate_keys": [
            _candidate_key(row) for row in selected_final
        ],
        "fallback_final_anchor_keys": fallback_keys,
        "predicted_final_candidate_deltas": predicted_delta,
        "predicted_final_tail_risk_probabilities": predicted_tail,
        "claim_scope": REGRET_SURROGATE_FORECAST_CORRECTION_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "raw_hourly_action_imitation": False,
    }


def _fit_scope_contextual_surrogate(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
    min_context_prior_support_count: int,
    min_context_prior_safe_win_count: int,
    min_context_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_context_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
) -> dict[str, Any]:
    if not rows:
        raise ValueError(
            f"{tenant_id}/{source_model_name} has no context teacher rows."
        )
    train_rows = [
        row
        for row in rows
        if bool(row["is_training_row"])
        and bool(row["eligible_for_final_selection"])
        and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
    ]
    final_rows = [
        row
        for row in rows
        if str(row["split_name"]) == "final_holdout"
        and bool(row["eligible_for_final_selection"])
    ]
    if not train_rows:
        raise ValueError(
            f"{tenant_id}/{source_model_name} contextual surrogate needs train rows."
        )
    if not final_rows:
        raise ValueError(
            f"{tenant_id}/{source_model_name} contextual surrogate needs final rows."
        )
    context_stats = _context_profile_stats_from_v2(train_rows)
    selected_final, fallback_keys, predicted_delta, predicted_tail = (
        _select_final_contextual_candidates(
            final_rows,
            context_stats=context_stats,
            allowed_candidate_sources=allowed_candidate_sources,
            min_context_prior_support_count=min_context_prior_support_count,
            min_context_prior_safe_win_count=min_context_prior_safe_win_count,
            min_context_prior_mean_improvement_uah=(
                min_context_prior_mean_improvement_uah
            ),
            min_predicted_improvement_uah=min_predicted_improvement_uah,
            max_context_tail_risk_probability=max_context_tail_risk_probability,
        )
    )
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "learner_model_name": REGRET_SURROGATE_CONTEXTUAL_MODEL_NAME,
        "target_label_space": "schedule_candidate_contextual_value_delta",
        "allowed_candidate_sources": sorted(allowed_candidate_sources),
        "context_profile_prior_stats": context_stats,
        "min_context_prior_support_count": min_context_prior_support_count,
        "min_context_prior_safe_win_count": min_context_prior_safe_win_count,
        "min_predicted_improvement_uah": min_predicted_improvement_uah,
        "max_context_tail_risk_probability": max_context_tail_risk_probability,
        "fallback_to_v2_plus": not selected_final,
        "uses_v2_plus_anchor_fallback": bool(fallback_keys),
        "selector_gate_blocker": (
            "contextual_regret_surrogate_candidate_selected"
            if selected_final
            else "no_prior_supported_safe_switch_context"
        ),
        "train_anchor_count": _anchor_count(train_rows),
        "final_holdout_anchor_count": _anchor_count(final_rows),
        "selected_final_candidate_keys": [
            _candidate_key(row) for row in selected_final
        ],
        "fallback_final_anchor_keys": fallback_keys,
        "selected_final_candidate_count": len(selected_final),
        "fallback_final_anchor_count": len(fallback_keys),
        "selected_final_family_counts": _family_counts(selected_final),
        "selected_final_candidate_source_counts": _source_counts(selected_final),
        "predicted_final_candidate_deltas": predicted_delta,
        "predicted_final_tail_risk_probabilities": predicted_tail,
        "claim_scope": REGRET_SURROGATE_CONTEXTUAL_CANDIDATE_VALUE_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "raw_hourly_action_imitation": False,
    }


def _select_final_contextual_candidates(
    final_rows: list[dict[str, Any]],
    *,
    context_stats: dict[str, dict[str, Any]],
    allowed_candidate_sources: set[str],
    min_context_prior_support_count: int,
    min_context_prior_safe_win_count: int,
    min_context_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_context_tail_risk_probability: float,
) -> tuple[list[dict[str, Any]], list[str], dict[str, float], dict[str, float]]:
    selected: list[dict[str, Any]] = []
    fallback_keys: list[str] = []
    predicted_delta: dict[str, float] = {}
    predicted_tail: dict[str, float] = {}
    for anchor, anchor_rows in sorted(_rows_by_datetime_anchor(final_rows).items()):
        candidates: list[tuple[dict[str, Any], float, float]] = []
        for row in anchor_rows:
            source = str(row["candidate_source"])
            if source not in allowed_candidate_sources:
                continue
            profile = str(row["safe_switch_context_profile_key"])
            stats = context_stats.get(profile)
            if stats is None:
                continue
            support = int(stats["row_count"])
            safe_wins = int(stats["safe_win_count"])
            delta = float(stats["mean_delta_uah"])
            tail = float(stats["tail_risk_probability"])
            predicted_delta[_candidate_key(row)] = delta
            predicted_tail[_candidate_key(row)] = tail
            if (
                support >= min_context_prior_support_count
                and safe_wins >= min_context_prior_safe_win_count
                and delta <= -min_context_prior_mean_improvement_uah
                and delta <= -min_predicted_improvement_uah
                and tail <= max_context_tail_risk_probability
            ):
                candidates.append((row, delta, tail))
        if not candidates:
            first = anchor_rows[0]
            fallback_keys.append(
                _anchor_key_from_parts(
                    str(first["tenant_id"]),
                    str(first["source_model_name"]),
                    anchor,
                )
            )
            continue
        selected.append(
            min(
                candidates,
                key=lambda item: (
                    item[2],
                    item[1],
                    float(
                        item[0].get(
                            "selector_feature_schedule_distance_from_v2_plus", 0.0
                        )
                    ),
                    str(item[0]["candidate_family"]),
                    str(item[0]["candidate_model_name"]),
                ),
            )[0]
        )
    return selected, fallback_keys, predicted_delta, predicted_tail


def _select_final_candidates(
    final_rows: list[dict[str, Any]],
    *,
    profile_stats: dict[str, dict[str, Any]],
    allowed_profiles: set[str],
    allowed_candidate_sources: set[str],
    min_predicted_improvement_uah: float,
    max_predicted_tail_risk_probability: float,
) -> tuple[list[dict[str, Any]], list[str], dict[str, float], dict[str, float]]:
    selected: list[dict[str, Any]] = []
    fallback_keys: list[str] = []
    predicted_delta: dict[str, float] = {}
    predicted_tail: dict[str, float] = {}
    for anchor, anchor_rows in sorted(_rows_by_datetime_anchor(final_rows).items()):
        candidates: list[tuple[dict[str, Any], float, float]] = []
        for row in anchor_rows:
            source = str(row["candidate_source"])
            profile = _profile_key(row)
            if (
                source not in allowed_candidate_sources
                or profile not in allowed_profiles
            ):
                continue
            stats = profile_stats[profile]
            delta = float(stats["mean_delta_uah"])
            tail = float(stats["tail_risk_probability"])
            predicted_delta[_candidate_key(row)] = delta
            predicted_tail[_candidate_key(row)] = tail
            if (
                delta <= -min_predicted_improvement_uah
                and tail <= max_predicted_tail_risk_probability
            ):
                candidates.append((row, delta, tail))
        if not candidates:
            first = anchor_rows[0]
            fallback_keys.append(
                _anchor_key_from_parts(
                    str(first["tenant_id"]),
                    str(first["source_model_name"]),
                    anchor,
                )
            )
            continue
        selected.append(
            min(
                candidates,
                key=lambda item: (
                    item[2],
                    item[1],
                    float(
                        item[0].get(
                            "selector_feature_schedule_distance_from_v2_plus", 0.0
                        )
                    ),
                    str(item[0]["candidate_family"]),
                    str(item[0]["candidate_model_name"]),
                ),
            )[0]
        )
    return selected, fallback_keys, predicted_delta, predicted_tail


def _learning_limit_failure_mode(
    *,
    better_exists: bool,
    best_source: str,
    best_delta: float,
    best_stats: dict[str, Any],
    tail_risk_delta_uah: float,
) -> str:
    if not better_exists:
        return "no_better_candidate"
    if best_source == _V2_PLUS_CANDIDATE_SOURCE:
        return "no_better_candidate"
    if (
        best_delta >= tail_risk_delta_uah
        or int(best_stats.get("tail_loss_count", 0)) > 0
    ):
        return "tail_risk_too_high"
    if int(best_stats.get("safe_win_count", 0)) < 1:
        return "better_candidate_unlearnable"
    return "selector_too_conservative"


def _rolling_summary_row(
    window_frame: pl.DataFrame,
    candidate_value: pl.DataFrame,
    *,
    source_model_name: str,
    window_index: int,
    validation_anchors: tuple[datetime, ...],
    prior_anchors: tuple[datetime, ...],
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
) -> dict[str, Any]:
    rows = list(window_frame.iter_rows(named=True))
    selected_keys: set[str] = set()
    fallback_anchor_keys: set[str] = set()
    for row in candidate_value.iter_rows(named=True):
        selected_keys.update(
            str(value) for value in row["selected_final_candidate_keys"]
        )
        fallback_anchor_keys.update(
            str(value) for value in row["fallback_final_anchor_keys"]
        )
    v2_regrets: list[float] = []
    selected_regrets: list[float] = []
    for anchor in validation_anchors:
        anchor_rows = [
            row
            for row in rows
            if _datetime_value(row["anchor_timestamp"]) == anchor
            and str(row["candidate_source"]) in {_V2_PLUS_CANDIDATE_SOURCE}
        ]
        for v2_row in anchor_rows:
            v2_regrets.append(float(v2_row["regret_uah"]))
            anchor_key = _anchor_key_from_parts(
                str(v2_row["tenant_id"]),
                str(v2_row["source_model_name"]),
                anchor,
            )
            if anchor_key in fallback_anchor_keys:
                selected_regrets.append(float(v2_row["regret_uah"]))
                continue
            selected_match = [
                row
                for row in rows
                if _candidate_key(row) in selected_keys
                and _datetime_value(row["anchor_timestamp"]) == anchor
                and str(row["tenant_id"]) == str(v2_row["tenant_id"])
            ]
            if selected_match:
                selected_regrets.append(float(selected_match[0]["regret_uah"]))
            else:
                selected_regrets.append(float(v2_row["regret_uah"]))
    v2_mean = mean(v2_regrets)
    selected_mean = mean(selected_regrets)
    improvement = _improvement_ratio(v2_mean, selected_mean)
    selected_median = median(selected_regrets)
    v2_median = median(v2_regrets)
    passed = (
        improvement >= min_mean_regret_improvement_ratio_vs_v2_plus
        and selected_median <= v2_median
    )
    return {
        "source_model_name": source_model_name,
        "window_index": window_index,
        "validation_anchor_count": len(validation_anchors),
        "minimum_prior_anchor_count_before_window": len(prior_anchors),
        "validation_anchor_start": validation_anchors[0],
        "validation_anchor_end": validation_anchors[-1],
        "selected_mean_regret_uah": selected_mean,
        "v2_plus_mean_regret_uah": v2_mean,
        "selected_median_regret_uah": selected_median,
        "v2_plus_median_regret_uah": v2_median,
        "mean_regret_improvement_ratio_vs_v2_plus": improvement,
        "rolling_window_passed": passed,
        "diagnostic_window_passed": improvement > 0.0 and selected_median <= v2_median,
        "claim_scope": REGRET_SURROGATE_ROBUSTNESS_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _window_teacher_panel(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    prior_anchors: set[datetime],
    validation_anchors: set[datetime],
) -> pl.DataFrame:
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        if str(row["source_model_name"]) != source_model_name:
            continue
        anchor = _datetime_value(row["anchor_timestamp"])
        if anchor in prior_anchors:
            copied = dict(row)
            copied["split_name"] = "train_selection"
            copied["is_training_row"] = (
                bool(row["eligible_for_final_selection"])
                and str(row["candidate_source"]) != _STRICT_CANDIDATE_SOURCE
            )
            output_rows.append(copied)
        elif anchor in validation_anchors:
            copied = dict(row)
            copied["split_name"] = "final_holdout"
            copied["is_training_row"] = False
            if (
                str(copied["candidate_source"]) == "oracle_gap_candidate"
                or bool(copied.get("oracle_neighborhood_train_only", False))
            ):
                copied["eligible_for_final_selection"] = False
                copied["eligible_for_final_selection_v6"] = False
                copied["eligible_for_final_selection_v7"] = False
            output_rows.append(copied)
    return pl.DataFrame(output_rows, infer_schema_length=None)


def _benchmark_row(
    row: dict[str, Any],
    *,
    selection_role: str,
    generated_at: datetime,
    strategy_kind: str = REGRET_SURROGATE_STRICT_LP_STRATEGY_KIND,
    challenger_model_name: str = REGRET_SURROGATE_MODEL_NAME,
    claim_scope: str = REGRET_SURROGATE_STRICT_CLAIM_SCOPE,
) -> dict[str, Any]:
    payload = dict(row["evaluation_payload"])
    payload.update(
        {
            "selection_role": selection_role,
            "regret_surrogate_role": selection_role,
            "claim_boundary": "offline_strategy_promotion_only",
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:{row['source_model_name']}:"
            f"{_datetime_value(row['anchor_timestamp']).isoformat()}:"
            f"{selection_role}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": str(row["source_model_name"]),
        "forecast_model_name": _forecast_model_name_for_role(
            row,
            selection_role,
            challenger_model_name=challenger_model_name,
        ),
        "selection_role": selection_role,
        "strategy_kind": strategy_kind,
        "market_venue": "DAM",
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _starting_soc_fraction(row),
        "starting_soc_source": "regret_surrogate_teacher_panel",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": _committed_action(row),
        "committed_power_mw": _first_dispatch_power(row),
        "rank_by_regret": 1,
        "safety_violation_count": int(row["safety_violation_count"]),
        "evaluation_payload": payload,
        "claim_scope": claim_scope,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _forecast_model_name_for_role(
    row: dict[str, Any],
    selection_role: str,
    *,
    challenger_model_name: str,
) -> str:
    if selection_role in {
        REGRET_SURROGATE_SELECTION_ROLE,
        REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE,
        REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE,
        REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE,
        REGRET_SURROGATE_CANDIDATE_VALUE_V8_SELECTION_ROLE,
    }:
        return challenger_model_name
    if selection_role == V2_PLUS_REFERENCE_ROLE:
        return "schedule_value_learner_v2_plus_reference"
    if selection_role == STRICT_REFERENCE_ROLE:
        return CONTROL_MODEL_NAME
    return str(row["candidate_model_name"])


def _starting_soc_fraction(row: dict[str, Any]) -> float:
    soc = row.get("soc_fraction_vector")
    if isinstance(soc, list) and soc:
        return float(soc[0])
    return float(row.get("starting_soc_fraction", 0.5))


def _first_dispatch_power(row: dict[str, Any]) -> float:
    dispatch = row.get("dispatch_mw_vector")
    if isinstance(dispatch, list) and dispatch:
        return float(dispatch[0])
    return float(row.get("committed_power_mw", 0.0))


def _committed_action(row: dict[str, Any]) -> str:
    power = _first_dispatch_power(row)
    if power > 0.0:
        return "DISCHARGE"
    if power < 0.0:
        return "CHARGE"
    return "HOLD"


def _profile_stats(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row["candidate_source"]) in _REFERENCE_CANDIDATE_SOURCES:
            continue
        grouped.setdefault(_profile_key(row), []).append(row)
    stats: dict[str, dict[str, Any]] = {}
    for profile, profile_rows in grouped.items():
        deltas = [
            float(row["label_regret_delta_vs_v2_plus_uah"]) for row in profile_rows
        ]
        tail_count = sum(1 for row in profile_rows if bool(row["label_tail_risk_loss"]))
        safe_count = sum(
            1 for row in profile_rows if bool(row["label_safe_switch_win"])
        )
        stats[profile] = {
            "candidate_source": str(profile_rows[0]["candidate_source"]),
            "candidate_family": str(profile_rows[0]["candidate_family"]),
            "candidate_model_name": str(profile_rows[0]["candidate_model_name"]),
            "row_count": len(profile_rows),
            "safe_win_count": safe_count,
            "tail_loss_count": tail_count,
            "mean_delta_uah": mean(deltas),
            "median_delta_uah": median(deltas),
            "tail_risk_probability": tail_count / len(profile_rows),
            "safe_win_probability": safe_count / len(profile_rows),
        }
    return stats


def _context_profile_stats(
    rows: list[dict[str, Any]],
    *,
    high_v2_regret_uah: float,
    high_forecast_spread_uah_mwh: float,
    min_material_schedule_distance: float,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row["candidate_source"]) in _REFERENCE_CANDIDATE_SOURCES:
            continue
        grouped.setdefault(
            _safe_switch_context_profile_key(
                row,
                high_v2_regret_uah=high_v2_regret_uah,
                high_forecast_spread_uah_mwh=high_forecast_spread_uah_mwh,
                min_material_schedule_distance=min_material_schedule_distance,
            ),
            [],
        ).append(row)
    return _stats_for_grouped_context_rows(grouped)


def _context_profile_stats_from_v2(
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row["candidate_source"]) in _REFERENCE_CANDIDATE_SOURCES:
            continue
        grouped.setdefault(str(row["safe_switch_context_profile_key"]), []).append(row)
    return _stats_for_grouped_context_rows(grouped)


def _stats_for_grouped_context_rows(
    grouped: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    for profile, profile_rows in grouped.items():
        deltas = [
            float(row["label_regret_delta_vs_v2_plus_uah"]) for row in profile_rows
        ]
        tail_count = sum(1 for row in profile_rows if bool(row["label_tail_risk_loss"]))
        safe_count = sum(
            1 for row in profile_rows if bool(row["label_safe_switch_win"])
        )
        stats[profile] = {
            "candidate_source": str(profile_rows[0]["candidate_source"]),
            "candidate_family": str(profile_rows[0]["candidate_family"]),
            "row_count": len(profile_rows),
            "safe_win_count": safe_count,
            "tail_loss_count": tail_count,
            "mean_delta_uah": mean(deltas),
            "median_delta_uah": median(deltas),
            "tail_risk_probability": tail_count / len(profile_rows),
            "safe_win_probability": safe_count / len(profile_rows),
        }
    return stats


def _safe_switch_context_profile_key(
    row: dict[str, Any],
    *,
    high_v2_regret_uah: float,
    high_forecast_spread_uah_mwh: float,
    min_material_schedule_distance: float,
) -> str:
    weekend = int(
        round(
            _numeric_feature(
                row, "selector_feature_anchor_is_weekend", "selector_feature_weekend"
            )
        )
    )
    grid_ready = int(
        round(_numeric_feature(row, "selector_feature_grid_event_context_ready"))
    )
    high_v2 = int(
        float(row.get("v2_plus_baseline_regret_uah", 0.0)) >= high_v2_regret_uah
    )
    high_spread = int(
        _numeric_feature(row, "selector_feature_forecast_spread_uah_mwh")
        >= high_forecast_spread_uah_mwh
    )
    material_distance = int(
        _numeric_feature(row, "selector_feature_schedule_distance_from_v2_plus")
        >= min_material_schedule_distance
    )
    return "|".join(
        [
            str(row["candidate_source"]),
            str(row["candidate_family"]),
            f"weekend={weekend}",
            f"grid={grid_ready}",
            f"high_v2={high_v2}",
            f"high_spread={high_spread}",
            f"material_distance={material_distance}",
        ]
    )


def _numeric_feature(row: dict[str, Any], *names: str) -> float:
    for name in names:
        value = row.get(name)
        if value is not None:
            return float(value)
    return 0.0


def _safe_switch_context_failure_mode(
    *,
    material_safe_switch_available: bool,
    stats: dict[str, Any],
    min_context_prior_safe_win_count: int,
    min_context_prior_mean_improvement_uah: float,
    max_context_tail_risk_probability: float,
) -> str:
    if not material_safe_switch_available:
        return "no_material_safe_switch"
    if not stats or int(stats.get("row_count", 0)) <= 0:
        return "context_without_prior_support"
    if (
        float(stats.get("tail_risk_probability", 1.0))
        > max_context_tail_risk_probability
    ):
        return "context_prior_tail_risk"
    if int(stats.get("safe_win_count", 0)) < min_context_prior_safe_win_count:
        return "context_prior_weak_support"
    if (
        float(stats.get("mean_delta_uah", 0.0))
        > -min_context_prior_mean_improvement_uah
    ):
        return "context_prior_weak_improvement"
    return "context_supported_safe_switch"


def _context_recommended_next_branch(failure_mode: str) -> str:
    if failure_mode == "context_supported_safe_switch":
        return "contextual_regret_surrogate_v2"
    if failure_mode == "context_without_prior_support":
        return "data_context_backfill"
    if failure_mode in {"context_prior_tail_risk", "context_prior_weak_support"}:
        return "tail_risk_feature_repair"
    if failure_mode == "context_prior_weak_improvement":
        return "candidate_value_label_repair"
    return "keep_v2_plus"


def _context_switch_class(*, material_safe: bool, tail_loss: bool) -> str:
    if tail_loss:
        return "tail_risk_loss"
    if material_safe:
        return "material_safe_switch"
    return "neutral_or_loss"


def _selector_feature_name_is_blocked(name: str) -> bool:
    lowered = name.lower()
    blocked_terms = (
        "actual",
        "diagnostic",
        "final_regret",
        "label",
        "oracle",
        "post_anchor",
        "realized",
    )
    return any(term in lowered for term in blocked_terms)


def _sparse_candidate_schedule_class(row: dict[str, Any]) -> str:
    source = str(row["candidate_source"])
    family = str(row["candidate_family"]).lower()
    if source == _V2_PLUS_CANDIDATE_SOURCE:
        return "v2_plus_fallback"
    if source == _STRICT_CANDIDATE_SOURCE:
        return "strict_control"
    if source == "oracle_gap_candidate":
        return "oracle_neighborhood_train_diagnostic"
    if "terminal" in family or "reserve" in family:
        return "soc_terminal_reserve"
    if "tft" in source or "quantile" in family:
        return "tft_poland_risk_reduced"
    if "poland" in source:
        return "poland_disagreement_risk_reduced"
    if "peak" in family or "trough" in family or "shift" in family:
        return "v2_plus_peak_trough_neighborhood"
    if "degradation" in family or "throughput" in family:
        return "throughput_degradation_sweep"
    return "candidate_value_neighbor"


def _sparse_prior_candidate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if bool(row.get("is_training_row", False))
        and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
        and bool(row.get("eligible_for_final_selection_v6", True))
    ]


def _v8_prior_candidate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if bool(row.get("is_training_row", False))
        and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
        and bool(row.get("eligible_for_final_selection_v8", True))
    ]


def _sparse_eligible_challengers(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if bool(
            row.get(
                "eligible_for_final_selection_v6", row["eligible_for_final_selection"]
            )
        )
        and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
    ]


def _sparse_neighbor_group_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row["source_model_name"]),
        str(row["candidate_source"]),
        str(row["candidate_family"]),
    )


def _sparse_distance_feature_names(frame: pl.DataFrame) -> tuple[str, ...]:
    preferred = [
        "selector_feature_schedule_distance_from_v2_plus",
        "selector_feature_total_throughput_delta_mwh",
        "selector_feature_terminal_soc_delta_fraction",
        "selector_feature_forecast_spread_uah_mwh",
        "selector_feature_total_degradation_penalty_uah",
        "selector_feature_poland_shadow_candidate",
        "selector_feature_tft_shadow_candidate",
        "selector_feature_weather_load_context_ready",
        "selector_feature_calendar_publication_context_ready",
        "selector_feature_grid_event_context_ready",
        "selector_feature_hour_of_day",
        "selector_feature_weekend",
    ]
    names = [
        name
        for name in preferred
        if name in frame.columns and not _selector_feature_name_is_blocked(name)
    ]
    if not names:
        names = [
            name
            for name in _selector_feature_columns(frame)
            if not _selector_feature_name_is_blocked(name)
        ]
    return tuple(names)


def _nearest_prior_neighbor_stats(
    row: dict[str, Any],
    *,
    prior_rows: list[dict[str, Any]],
    feature_names: tuple[str, ...],
    nearest_neighbor_count: int,
) -> dict[str, Any]:
    candidates = [
        prior
        for prior in prior_rows
        if _sparse_neighbor_group_key(prior) == _sparse_neighbor_group_key(row)
    ]
    return _nearest_prior_neighbor_stats_from_candidates(
        row,
        prior_rows=candidates,
        feature_names=feature_names,
        nearest_neighbor_count=nearest_neighbor_count,
    )


def _nearest_prior_neighbor_stats_from_candidates(
    row: dict[str, Any],
    *,
    prior_rows: list[dict[str, Any]],
    feature_names: tuple[str, ...],
    nearest_neighbor_count: int,
) -> dict[str, Any]:
    candidates = prior_rows
    if not candidates or not feature_names:
        return _empty_neighbor_stats()
    distances = sorted(
        (
            _sparse_feature_distance(row, prior, feature_names),
            _candidate_key(prior),
            prior,
        )
        for prior in candidates
    )
    nearest = [prior for _, _, prior in distances[:nearest_neighbor_count]]
    safe_distances = [
        distance
        for distance, _, prior in distances
        if bool(prior.get("label_context_material_safe_switch", False))
        or bool(prior.get("label_sparse_material_safe_switch", False))
        or bool(prior.get("label_safe_switch_win", False))
    ]
    deltas = [float(prior["label_regret_delta_vs_v2_plus_uah"]) for prior in nearest]
    tail_count = sum(1 for prior in nearest if bool(prior["label_tail_risk_loss"]))
    safe_count = sum(
        1
        for prior in nearest
        if bool(prior.get("label_context_material_safe_switch", False))
        or bool(prior.get("label_sparse_material_safe_switch", False))
        or bool(prior.get("label_safe_switch_win", False))
    )
    return {
        "nearest_any_distance": float(distances[0][0]),
        "nearest_safe_distance": float(safe_distances[0])
        if safe_distances
        else float("inf"),
        "neighbor_count": len(nearest),
        "safe_win_count": safe_count,
        "tail_risk_count": tail_count,
        "tail_risk_probability": tail_count / len(nearest),
        "mean_delta_uah": mean(deltas),
        "median_delta_uah": median(deltas),
    }


def _empty_neighbor_stats() -> dict[str, Any]:
    return {
        "nearest_any_distance": float("inf"),
        "nearest_safe_distance": float("inf"),
        "neighbor_count": 0,
        "safe_win_count": 0,
        "tail_risk_count": 0,
        "tail_risk_probability": 1.0,
        "mean_delta_uah": 0.0,
        "median_delta_uah": 0.0,
    }


def _v9_context_bucket(row: dict[str, Any]) -> tuple[str, str]:
    hour = int(_numeric_feature(row, "selector_feature_hour_of_day"))
    if hour < 6:
        block = "night"
    elif hour < 12:
        block = "morning"
    elif hour < 18:
        block = "afternoon"
    else:
        block = "evening"
    spread = _numeric_feature(row, "selector_feature_forecast_spread_uah_mwh")
    spread_regime = "high_spread" if spread >= 2500.0 else "low_spread"
    return block, spread_regime


def _v9_prior_context_stats(prior_rows: list[dict[str, Any]]) -> dict[str, float]:
    if not prior_rows:
        return {
            "neighbor_count": 0.0,
            "safe_win_rate": 0.0,
            "tail_risk_rate": 1.0,
            "mean_delta_uah": 0.0,
        }
    safe_count = sum(
        1
        for row in prior_rows
        if bool(row.get("label_v8_pruned_material_safe_switch", False))
        or bool(row.get("label_safe_switch_win", False))
    )
    tail_count = sum(1 for row in prior_rows if bool(row.get("label_tail_risk_loss")))
    deltas = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in prior_rows]
    return {
        "neighbor_count": float(len(prior_rows)),
        "safe_win_rate": safe_count / len(prior_rows),
        "tail_risk_rate": tail_count / len(prior_rows),
        "mean_delta_uah": mean(deltas),
    }


def _v9_prior_context_blockers(
    stats: dict[str, float],
    *,
    min_prior_context_neighbor_count: int,
) -> list[str]:
    blockers: list[str] = []
    if stats["neighbor_count"] < min_prior_context_neighbor_count:
        blockers.append("missing_v9_prior_context_neighbors")
    if stats["tail_risk_rate"] > 0.25:
        blockers.append("v9_prior_context_tail_risk_high")
    return blockers


def _v9_prior_candidate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if bool(row.get("is_training_row", False))
        and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
        and bool(row.get("eligible_for_final_selection_v9", True))
    ]


def _sparse_feature_distance(
    left: dict[str, Any],
    right: dict[str, Any],
    feature_names: tuple[str, ...],
) -> float:
    squared = 0.0
    for name in feature_names:
        left_value = _scaled_sparse_feature_value(left, name)
        right_value = _scaled_sparse_feature_value(right, name)
        squared += (left_value - right_value) ** 2
    return squared**0.5


def _scaled_sparse_feature_value(row: dict[str, Any], name: str) -> float:
    value = _numeric_feature(row, name)
    if name.endswith("hour_of_day"):
        return value / 24.0
    if "forecast_spread" in name:
        return value / 10_000.0
    if "degradation_penalty" in name:
        return value / 1_000.0
    return value


def _sparse_opportunity_class(
    *,
    material_candidate_available: bool,
    tail_risk_candidate_count: int,
    neighbor_stats: dict[str, Any],
    max_prior_neighbor_distance: float,
    min_neighbor_safe_win_count: int,
    max_neighbor_tail_risk_probability: float,
) -> str:
    if not material_candidate_available:
        if tail_risk_candidate_count > 0:
            return "tail_risk_dominated"
        return "no_material_candidate"
    if (
        float(neighbor_stats["nearest_safe_distance"]) <= max_prior_neighbor_distance
        and int(neighbor_stats["safe_win_count"]) >= min_neighbor_safe_win_count
        and float(neighbor_stats["tail_risk_probability"])
        <= max_neighbor_tail_risk_probability
    ):
        return "material_candidate_prior_supported"
    return "material_candidate_no_prior_neighbor"


def _sparse_recommended_next_branch(opportunity_class: str) -> str:
    if opportunity_class == "material_candidate_prior_supported":
        return "abstaining_safe_switch_v6"
    if opportunity_class == "material_candidate_no_prior_neighbor":
        return "ukrainian_context_backfill"
    if opportunity_class == "tail_risk_dominated":
        return "tail_risk_candidate_repair"
    return "keep_v2_plus"


def _v7_backfill_decision(
    *,
    sparse_opportunity_class: str,
    context_missing: bool,
    candidate_family_gap: bool,
    strict_material_win: bool,
    prior_material_example_count: int,
    selector_safe_oracle_improvement_ratio: float,
    min_prior_material_examples_for_dt: int,
    min_oracle_improvement_ratio_vs_v2_plus: float,
) -> str:
    if (
        prior_material_example_count >= min_prior_material_examples_for_dt
        and selector_safe_oracle_improvement_ratio
        >= min_oracle_improvement_ratio_vs_v2_plus
        and sparse_opportunity_class == "material_candidate_prior_supported"
    ):
        return "dt_ready"
    if (
        context_missing
        or sparse_opportunity_class == "material_candidate_no_prior_neighbor"
    ):
        return "backfill_needed"
    if candidate_family_gap or strict_material_win:
        return "candidate_generation_needed"
    if sparse_opportunity_class == "tail_risk_dominated":
        return "candidate_generation_needed"
    return "stop_modeling_current_candidate_space"


def _v7_generated_candidate_specs(
    *,
    v2_row: dict[str, Any],
    strict_row: dict[str, Any],
    requirement: dict[str, Any],
) -> list[dict[str, Any]]:
    del requirement
    specs = [
        (
            strict_row,
            "strict_guarded_rescue_v7",
            "strict_guarded_rescue_v7",
            "strict_guarded_rescue",
            0.0,
        ),
        (
            v2_row,
            "v2_plus_peak_trough_neighborhood_v7",
            "v2_plus_peak_trough_shift_v7",
            "v2_plus_peak_trough_neighborhood",
            0.08,
        ),
        (
            v2_row,
            "terminal_soc_reserve_v7",
            "terminal_soc_reserve_v7",
            "soc_terminal_reserve",
            0.04,
        ),
        (
            v2_row,
            "spread_volatility_robust_v7",
            "spread_volatility_robust_v7",
            "spread_volatility_robust",
            0.03,
        ),
        (
            v2_row,
            "morning_evening_block_v7",
            "morning_evening_block_v7",
            "morning_evening_block",
            0.06,
        ),
        (
            v2_row,
            "throughput_degradation_sweep_v7",
            "throughput_degradation_sweep_v7",
            "throughput_degradation_sweep",
            0.02,
        ),
    ]
    return [
        _copy_v7_generated_candidate(
            source_row=row,
            candidate_family=family,
            candidate_model_name=model_name,
            candidate_schedule_class=schedule_class,
            extra_schedule_distance=extra_distance,
        )
        for row, family, model_name, schedule_class, extra_distance in specs
    ]


def _copy_v7_generated_candidate(
    *,
    source_row: dict[str, Any],
    candidate_family: str,
    candidate_model_name: str,
    candidate_schedule_class: str,
    extra_schedule_distance: float,
) -> dict[str, Any]:
    copied = dict(source_row)
    regret = float(copied["regret_uah"])
    baseline = float(copied["v2_plus_baseline_regret_uah"])
    delta = regret - baseline
    source_payload = dict(copied["evaluation_payload"])
    source_payload.update(
        {
            "candidate_source": _V7_GENERATED_CANDIDATE_SOURCE,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "generated_from_candidate_source": str(source_row["candidate_source"]),
        }
    )
    copied.update(
        {
            "candidate_source": _V7_GENERATED_CANDIDATE_SOURCE,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "candidate_library_version": "candidate_value_v7",
            "candidate_schedule_class": candidate_schedule_class,
            "eligible_for_final_selection": True,
            "eligible_for_final_selection_v6": True,
            "eligible_for_final_selection_v7": True,
            "is_training_row": str(source_row["split_name"]) != "final_holdout",
            "oracle_neighborhood_train_only": False,
            "generated_from_candidate_source": str(source_row["candidate_source"]),
            "selector_feature_schedule_distance_from_v2_plus": (
                _numeric_feature(
                    source_row,
                    "selector_feature_schedule_distance_from_v2_plus",
                )
                + extra_schedule_distance
            ),
            "label_regret_delta_vs_v2_plus_uah": delta,
            "label_safe_switch_win": delta < 0.0,
            "label_tail_risk_loss": delta >= 150.0,
            "label_best_candidate_family": candidate_family
            if delta < 0.0
            else "frozen_v2_plus",
            "label_best_candidate_model_name": candidate_model_name
            if delta < 0.0
            else "schedule_value_learner_v2_plus",
            "label_is_anchor_best_candidate": delta < 0.0,
            "evaluation_payload": source_payload,
            "target_label_space": "schedule_candidate_value_v7",
            "raw_hourly_action_imitation": False,
            "claim_scope": REGRET_SURROGATE_FEASIBLE_CANDIDATE_LIBRARY_V7_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _v8_generated_candidate_specs(
    *,
    v2_row: dict[str, Any],
    strict_row: dict[str, Any],
    requirement: dict[str, Any],
) -> list[dict[str, Any]]:
    del requirement
    specs = [
        (
            v2_row,
            "ua_peak_trough_shift_v8",
            "ua_context_peak_trough_shift_v8",
            "ua_context_peak_trough_shift",
            "peak_trough",
        ),
        (
            v2_row,
            "ua_terminal_reserve_v8",
            "ua_context_terminal_reserve_v8",
            "ua_terminal_soc_reserve",
            "terminal_reserve",
        ),
        (
            v2_row,
            "ua_morning_evening_block_v8",
            "ua_context_morning_evening_block_v8",
            "ua_morning_evening_block",
            "morning_evening_block",
        ),
        (
            v2_row,
            "ua_tail_risk_clipped_v8",
            "ua_context_tail_risk_clipped_v8",
            "ua_tail_risk_clipped",
            "tail_risk_clipped",
        ),
        (
            strict_row,
            "ua_strict_blend_rescue_v8",
            "ua_context_strict_blend_rescue_v8",
            "ua_strict_blend_rescue",
            "strict_blend",
        ),
    ]
    return [
        _copy_v8_generated_candidate(
            source_row=row,
            v2_row=v2_row,
            candidate_family=family,
            candidate_model_name=model_name,
            candidate_schedule_class=schedule_class,
            schedule_rule=schedule_rule,
        )
        for row, family, model_name, schedule_class, schedule_rule in specs
    ]


def _copy_v8_generated_candidate(
    *,
    source_row: dict[str, Any],
    v2_row: dict[str, Any],
    candidate_family: str,
    candidate_model_name: str,
    candidate_schedule_class: str,
    schedule_rule: str,
) -> dict[str, Any]:
    copied = dict(source_row)
    dispatch = _v8_dispatch_vector(v2_row, source_row=source_row, rule=schedule_rule)
    soc = _soc_from_dispatch(v2_row, dispatch)
    throughput = sum(abs(value) for value in dispatch)
    degradation = float(v2_row.get("total_degradation_penalty_uah", 0.0)) * (
        throughput / max(sum(abs(value) for value in _float_vector(v2_row["dispatch_mw_vector"])), 1e-9)
    )
    payload = dict(copied["evaluation_payload"])
    payload.update(
        {
            "candidate_source": _V8_GENERATED_CANDIDATE_SOURCE,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "dispatch_mw": dispatch,
            "soc_fraction": soc,
            "generated_from_candidate_source": str(source_row["candidate_source"]),
            "requires_strict_rescore": True,
        }
    )
    copied.update(
        {
            "candidate_source": _V8_GENERATED_CANDIDATE_SOURCE,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "candidate_library_version": "ua_context_candidate_value_v8",
            "candidate_schedule_class": candidate_schedule_class,
            "eligible_for_final_selection": True,
            "eligible_for_final_selection_v6": False,
            "eligible_for_final_selection_v7": False,
            "eligible_for_final_selection_v8": True,
            "is_training_row": str(source_row["split_name"]) != "final_holdout",
            "oracle_neighborhood_train_only": False,
            "dispatch_mw_vector": dispatch,
            "soc_fraction_vector": soc,
            "total_throughput_mwh": throughput,
            "total_degradation_penalty_uah": degradation,
            "selector_feature_schedule_distance_from_v2_plus": _schedule_distance(
                _float_vector(v2_row["dispatch_mw_vector"]),
                dispatch,
            ),
            "selector_feature_total_throughput_delta_mwh": throughput
            - float(v2_row.get("total_throughput_mwh", throughput)),
            "selector_feature_terminal_soc_delta_fraction": soc[-1]
            - _float_vector(v2_row["soc_fraction_vector"])[-1],
            "candidate_value_label_status": "pending_strict_rescore",
            "diagnostic_requires_strict_rescore": True,
            "diagnostic_generated_schedule_rule": schedule_rule,
            "generated_from_candidate_source": str(source_row["candidate_source"]),
            "label_regret_delta_vs_v2_plus_uah": 0.0,
            "label_safe_switch_win": False,
            "label_tail_risk_loss": False,
            "label_best_candidate_family": "pending_strict_rescore",
            "label_best_candidate_model_name": "pending_strict_rescore",
            "label_is_anchor_best_candidate": False,
            "evaluation_payload": payload,
            "target_label_space": "schedule_candidate_value_v8",
            "raw_hourly_action_imitation": False,
            "claim_scope": REGRET_SURROGATE_UA_CANDIDATE_LIBRARY_V8_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _rescore_v8_generated_candidate(row: dict[str, Any]) -> dict[str, Any]:
    actual_prices = _float_vector(row["actual_price_uah_mwh_vector"])
    forecast_prices = _float_vector(row["forecast_price_uah_mwh_vector"])
    dispatch = _float_vector(row["dispatch_mw_vector"])
    if len(actual_prices) != len(dispatch):
        raise ValueError("V8 strict rescore needs aligned actual and dispatch vectors.")
    degradation = float(row["total_degradation_penalty_uah"])
    decision_value = _schedule_value_uah(
        prices=actual_prices,
        dispatch=dispatch,
        degradation_penalty_uah=degradation,
    )
    forecast_objective = _schedule_value_uah(
        prices=forecast_prices,
        dispatch=dispatch,
        degradation_penalty_uah=degradation,
    )
    oracle_value = float(row["oracle_value_uah"])
    regret = max(0.0, oracle_value - decision_value)
    regret_ratio = regret / abs(oracle_value) if abs(oracle_value) > 1e-9 else 0.0
    baseline_regret = float(row["v2_plus_baseline_regret_uah"])
    delta = regret - baseline_regret
    payload = _v8_rescore_payload(
        row,
        decision_value_uah=decision_value,
        forecast_objective_value_uah=forecast_objective,
        regret_uah=regret,
    )
    copied = dict(row)
    copied.update(
        {
            "decision_value_uah": decision_value,
            "forecast_objective_value_uah": forecast_objective,
            "regret_uah": regret,
            "regret_ratio": regret_ratio,
            "label_regret_delta_vs_v2_plus_uah": delta,
            "label_safe_switch_win": delta < 0.0,
            "label_tail_risk_loss": delta >= 150.0,
            "label_best_candidate_family": str(row["candidate_family"])
            if delta < 0.0
            else "frozen_v2_plus",
            "label_best_candidate_model_name": str(row["candidate_model_name"])
            if delta < 0.0
            else "schedule_value_learner_v2_plus",
            "label_is_anchor_best_candidate": delta < 0.0,
            "candidate_value_label_status": "strict_rescored_v8_candidate",
            "diagnostic_requires_strict_rescore": False,
            "strict_rescore_version": "ua_context_v8_direct_schedule_score_v1",
            "evaluation_payload": payload,
            "claim_scope": REGRET_SURROGATE_UA_CANDIDATE_STRICT_RESCORE_V8_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _schedule_value_uah(
    *,
    prices: list[float],
    dispatch: list[float],
    degradation_penalty_uah: float,
) -> float:
    return sum(price * power for price, power in zip(prices, dispatch, strict=True)) - (
        degradation_penalty_uah
    )


def _v8_rescore_payload(
    row: dict[str, Any],
    *,
    decision_value_uah: float,
    forecast_objective_value_uah: float,
    regret_uah: float,
) -> dict[str, Any]:
    payload = (
        dict(row["evaluation_payload"])
        if isinstance(row.get("evaluation_payload"), dict)
        else {}
    )
    dispatch = _float_vector(row["dispatch_mw_vector"])
    soc = _float_vector(row["soc_fraction_vector"])
    horizon = payload.get("horizon")
    if isinstance(horizon, list):
        updated_horizon: list[dict[str, Any]] = []
        for index, point in enumerate(horizon):
            updated = dict(point) if isinstance(point, dict) else {}
            if index < len(dispatch):
                updated["net_power_mw"] = dispatch[index]
            if index < len(soc):
                updated["soc_fraction"] = soc[index]
            updated_horizon.append(updated)
        payload["horizon"] = updated_horizon
    payload.update(
        {
            "strict_rescore_version": "ua_context_v8_direct_schedule_score_v1",
            "candidate_value_label_status": "strict_rescored_v8_candidate",
            "requires_strict_rescore": False,
            "decision_value_uah": decision_value_uah,
            "forecast_objective_value_uah": forecast_objective_value_uah,
            "regret_uah": regret_uah,
            "claim_scope": REGRET_SURROGATE_UA_CANDIDATE_STRICT_RESCORE_V8_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return payload


def _v8_dispatch_vector(
    v2_row: dict[str, Any],
    *,
    source_row: dict[str, Any],
    rule: str,
) -> list[float]:
    base = _float_vector(v2_row["dispatch_mw_vector"])
    strict = _float_vector(source_row["dispatch_mw_vector"])
    forecast = _float_vector(v2_row["forecast_price_uah_mwh_vector"])
    if not base:
        return []
    limit = max(max(abs(value) for value in base), 0.1)
    peak_index, trough_index = _peak_trough_indices(forecast)
    dispatch = list(base)
    if rule == "peak_trough":
        dispatch = [0.0 for _ in base]
        dispatch[trough_index] = -limit
        dispatch[peak_index] = limit
    elif rule == "terminal_reserve":
        midpoint = max(1, len(base) // 2)
        dispatch = [
            value * 0.5 if index >= midpoint and value > 0.0 else value
            for index, value in enumerate(base)
        ]
    elif rule == "morning_evening_block":
        dispatch = [0.0 for _ in base]
        low_block = _best_block_index(forecast, range(6, 11), prefer_high=False)
        high_block = _best_block_index(forecast, range(17, 23), prefer_high=True)
        dispatch[low_block] = -limit
        dispatch[high_block] = limit
    elif rule == "tail_risk_clipped":
        dispatch = [value * 0.5 for value in base]
    elif rule == "strict_blend":
        dispatch = [
            (base_value + strict_value) / 2.0
            for base_value, strict_value in zip(base, strict, strict=False)
        ]
    return [_clip(value, -limit, limit) for value in dispatch]


def _v9_generated_candidate_specs(*, v2_row: dict[str, Any]) -> list[dict[str, Any]]:
    specs = [
        (
            "ua_v2_plus_same_energy_peak_trough_v9",
            "ua_context_same_energy_peak_trough_v9",
            "same_energy_peak_trough",
        ),
        (
            "ua_v2_plus_peak_trough_guarded_v9",
            "ua_context_peak_trough_guarded_v9",
            "same_energy_peak_trough_guarded",
        ),
        (
            "ua_v2_plus_peak_trough_minimal_v9",
            "ua_context_peak_trough_minimal_v9",
            "same_energy_peak_trough_minimal",
        ),
    ]
    return [
        _copy_v9_generated_candidate(
            v2_row=v2_row,
            candidate_family=family,
            candidate_model_name=model_name,
            schedule_rule=rule,
        )
        for family, model_name, rule in specs
    ]


def _copy_v9_generated_candidate(
    *,
    v2_row: dict[str, Any],
    candidate_family: str,
    candidate_model_name: str,
    schedule_rule: str,
) -> dict[str, Any]:
    copied = dict(v2_row)
    dispatch = _v9_dispatch_vector(v2_row, rule=schedule_rule)
    soc = _soc_from_dispatch(v2_row, dispatch)
    throughput = sum(abs(value) for value in dispatch)
    base_throughput = max(float(v2_row.get("total_throughput_mwh", throughput)), 1e-9)
    degradation = float(v2_row.get("total_degradation_penalty_uah", 0.0)) * (
        throughput / base_throughput
    )
    payload = dict(copied["evaluation_payload"])
    payload.update(
        {
            "candidate_source": _V9_GENERATED_CANDIDATE_SOURCE,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "dispatch_mw": dispatch,
            "soc_fraction": soc,
            "generated_from_candidate_source": str(v2_row["candidate_source"]),
            "requires_strict_rescore": True,
            "non_tail_risk_generation_rule": schedule_rule,
        }
    )
    copied.update(
        {
            "candidate_source": _V9_GENERATED_CANDIDATE_SOURCE,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "candidate_library_version": "ua_non_tail_risk_candidate_value_v9",
            "candidate_schedule_class": schedule_rule,
            "eligible_for_final_selection": True,
            "eligible_for_final_selection_v6": False,
            "eligible_for_final_selection_v7": False,
            "eligible_for_final_selection_v8": False,
            "eligible_for_final_selection_v9": True,
            "is_training_row": str(v2_row["split_name"]) != "final_holdout",
            "oracle_neighborhood_train_only": False,
            "dispatch_mw_vector": dispatch,
            "soc_fraction_vector": soc,
            "total_throughput_mwh": throughput,
            "total_degradation_penalty_uah": degradation,
            "selector_feature_schedule_distance_from_v2_plus": _schedule_distance(
                _float_vector(v2_row["dispatch_mw_vector"]),
                dispatch,
            ),
            "selector_feature_total_throughput_delta_mwh": throughput
            - float(v2_row.get("total_throughput_mwh", throughput)),
            "selector_feature_terminal_soc_delta_fraction": soc[-1]
            - _float_vector(v2_row["soc_fraction_vector"])[-1],
            "candidate_value_label_status": "pending_strict_rescore",
            "diagnostic_requires_strict_rescore": True,
            "diagnostic_generated_schedule_rule": schedule_rule,
            "generated_from_candidate_source": str(v2_row["candidate_source"]),
            "label_regret_delta_vs_v2_plus_uah": 0.0,
            "label_safe_switch_win": False,
            "label_tail_risk_loss": False,
            "label_best_candidate_family": "pending_strict_rescore",
            "label_best_candidate_model_name": "pending_strict_rescore",
            "label_is_anchor_best_candidate": False,
            "evaluation_payload": payload,
            "target_label_space": "schedule_candidate_value_v9",
            "raw_hourly_action_imitation": False,
            "claim_scope": (
                REGRET_SURROGATE_UA_NON_TAIL_RISK_CANDIDATE_LIBRARY_V9_CLAIM_SCOPE
            ),
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _v9_dispatch_vector(v2_row: dict[str, Any], *, rule: str) -> list[float]:
    base = _float_vector(v2_row["dispatch_mw_vector"])
    forecast = _float_vector(v2_row["forecast_price_uah_mwh_vector"])
    if not base:
        return []
    throughput = sum(abs(value) for value in base)
    limit = max(max(abs(value) for value in base), 0.1)
    dispatch = list(base)
    if rule in {
        "same_energy_peak_trough",
        "same_energy_peak_trough_guarded",
        "same_energy_peak_trough_minimal",
    }:
        peak_index, trough_index = _peak_trough_indices(forecast)
        if peak_index != trough_index and throughput > 0.0:
            magnitude = min(limit, throughput / 2.0)
            if rule == "same_energy_peak_trough_guarded":
                magnitude *= 0.75
            elif rule == "same_energy_peak_trough_minimal":
                magnitude *= 0.50
            dispatch = [0.0 for _ in base]
            dispatch[trough_index] = -magnitude
            dispatch[peak_index] = magnitude
    elif rule == "terminal_guard":
        midpoint = max(1, len(base) // 2)
        dispatch = [
            value * 0.75 if index >= midpoint and value > 0.0 else value
            for index, value in enumerate(base)
        ]
    elif rule == "soft_clip":
        dispatch = [value * 0.75 for value in base]
    return [_clip(value, -limit, limit) for value in dispatch]


def _rescore_v9_generated_candidate(row: dict[str, Any]) -> dict[str, Any]:
    copied = _rescore_v8_generated_candidate(row)
    payload = (
        dict(copied["evaluation_payload"])
        if isinstance(copied.get("evaluation_payload"), dict)
        else {}
    )
    payload.update(
        {
            "strict_rescore_version": (
                "ua_context_v9_non_tail_risk_direct_schedule_score_v1"
            ),
            "candidate_value_label_status": "strict_rescored_v9_candidate",
            "claim_scope": (
                REGRET_SURROGATE_UA_NON_TAIL_RISK_STRICT_RESCORE_V9_CLAIM_SCOPE
            ),
        }
    )
    copied.update(
        {
            "candidate_value_label_status": "strict_rescored_v9_candidate",
            "strict_rescore_version": (
                "ua_context_v9_non_tail_risk_direct_schedule_score_v1"
            ),
            "evaluation_payload": payload,
            "claim_scope": (
                REGRET_SURROGATE_UA_NON_TAIL_RISK_STRICT_RESCORE_V9_CLAIM_SCOPE
            ),
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _soc_from_dispatch(source_row: dict[str, Any], dispatch: list[float]) -> list[float]:
    source_soc = _float_vector(source_row["soc_fraction_vector"])
    current = source_soc[0] if source_soc else 0.5
    soc: list[float] = []
    for value in dispatch:
        current = _clip(current - value * 0.05, 0.0, 1.0)
        soc.append(current)
    return soc


def _peak_trough_indices(values: list[float]) -> tuple[int, int]:
    if not values:
        return 0, 0
    peak_index = max(range(len(values)), key=lambda index: values[index])
    trough_index = min(range(len(values)), key=lambda index: values[index])
    return peak_index, trough_index


def _best_block_index(
    values: list[float],
    hours: range,
    *,
    prefer_high: bool,
) -> int:
    if not values:
        return 0
    candidates = [hour % len(values) for hour in hours if values]
    if not candidates:
        candidates = list(range(len(values)))
    return (
        max(candidates, key=lambda index: values[index])
        if prefer_high
        else min(candidates, key=lambda index: values[index])
    )


def _block_mean(values: list[float], hours: range) -> float:
    if not values:
        return 0.0
    selected = [values[hour % len(values)] for hour in hours]
    return mean(selected) if selected else 0.0


def _schedule_distance(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    width = min(len(left), len(right))
    return sum(abs(left[index] - right[index]) for index in range(width)) / width


def _clip(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def _float_vector(value: Any) -> list[float]:
    if isinstance(value, list | tuple):
        return [float(item) for item in value]
    return []


def _context_feature(row: dict[str, Any] | None, name: str) -> float:
    if row is None:
        return 0.0
    return _numeric_feature(row, name)


def _ua_context_blockers(
    context_row: dict[str, Any] | None,
    requirement: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if context_row is None:
        blockers.append("missing_ua_context_panel_row")
    else:
        raw = context_row.get("diagnostic_context_blockers", [])
        if isinstance(raw, list | tuple):
            blockers.extend(str(value) for value in raw)
        elif raw:
            blockers.append(str(raw))
        if _context_feature(context_row, "selector_feature_publication_time_ready") < 1.0:
            blockers.append("missing_publication_time")
        if _context_feature(context_row, "selector_feature_weather_load_context_ready") < 1.0:
            blockers.append("missing_weather_load_context")
        if _context_feature(context_row, "selector_feature_grid_event_context_ready") < 1.0:
            blockers.append("missing_grid_event_context")
    if bool(requirement["missing_prior_context"]):
        blockers.append("v7_missing_prior_context")
    return sorted(set(blockers))


def _final_anchor_keys(rows: list[dict[str, Any]]) -> list[str]:
    return [
        _anchor_key_from_parts(
            str(first["tenant_id"]),
            str(first["source_model_name"]),
            anchor,
        )
        for anchor, anchor_rows in sorted(
            _rows_by_datetime_anchor(
                [row for row in rows if str(row["split_name"]) == "final_holdout"]
            ).items()
        )
        for first in anchor_rows[:1]
    ]


def _fit_scope_sparse_safe_switch(
    scope_rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
    max_prior_neighbor_distance: float,
    min_neighbor_safe_win_count: int,
    min_predicted_improvement_uah: float,
    max_neighbor_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
) -> dict[str, Any]:
    final_rows = [
        row for row in scope_rows if str(row["split_name"]) == "final_holdout"
    ]
    selected, fallback_keys, predicted_delta, predicted_tail = (
        _select_sparse_final_candidates(
            final_rows,
            max_prior_neighbor_distance=max_prior_neighbor_distance,
            min_neighbor_safe_win_count=min_neighbor_safe_win_count,
            min_predicted_improvement_uah=min_predicted_improvement_uah,
            max_neighbor_tail_risk_probability=max_neighbor_tail_risk_probability,
            allowed_candidate_sources=allowed_candidate_sources,
        )
    )
    material_final_count = sum(
        1
        for row in final_rows
        if str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
        and float(row["label_regret_delta_vs_v2_plus_uah"]) < 0.0
    )
    if selected:
        abstention_reason = "selected_prior_supported_candidates"
    elif material_final_count > 0:
        abstention_reason = "no_prior_neighbor_support"
    else:
        abstention_reason = "no_material_candidate"
    selected_keys = [_candidate_key(row) for row in selected]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "learner_model_name": REGRET_SURROGATE_SPARSE_SAFE_SWITCH_MODEL_NAME,
        "target_label_space": "schedule_candidate_index",
        "selected_final_candidate_keys": selected_keys,
        "fallback_final_anchor_keys": fallback_keys,
        "selected_final_candidate_count": len(selected_keys),
        "fallback_final_anchor_count": len(fallback_keys),
        "selected_final_family_counts": _family_counts(selected),
        "selected_final_candidate_source_counts": _source_counts(selected),
        "predicted_final_candidate_deltas": predicted_delta,
        "predicted_final_tail_risk_probabilities": predicted_tail,
        "fallback_to_v2_plus": len(selected_keys) == 0,
        "uses_v2_plus_anchor_fallback": bool(fallback_keys),
        "abstention_reason": abstention_reason,
        "selection_policy": "nearest_prior_abstaining_safe_switch_v6",
        "claim_scope": REGRET_SURROGATE_SPARSE_MODEL_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "raw_hourly_action_imitation": False,
    }


def _fit_scope_v8_candidate_value(
    scope_rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
    max_prior_neighbor_distance: float,
    min_neighbor_safe_win_count: int,
    min_predicted_improvement_uah: float,
    max_neighbor_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
) -> dict[str, Any]:
    final_rows = [
        row for row in scope_rows if str(row["split_name"]) == "final_holdout"
    ]
    selected, fallback_keys, predicted_delta, predicted_tail = (
        _select_v8_final_candidates(
            final_rows,
            max_prior_neighbor_distance=max_prior_neighbor_distance,
            min_neighbor_safe_win_count=min_neighbor_safe_win_count,
            min_predicted_improvement_uah=min_predicted_improvement_uah,
            max_neighbor_tail_risk_probability=max_neighbor_tail_risk_probability,
            allowed_candidate_sources=allowed_candidate_sources,
        )
    )
    material_final_count = sum(
        1
        for row in final_rows
        if str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
        and bool(row.get("eligible_for_final_selection_v8", True))
        and float(row["label_regret_delta_vs_v2_plus_uah"]) < 0.0
    )
    if selected:
        abstention_reason = "selected_prior_supported_v8_candidates"
    elif material_final_count > 0:
        abstention_reason = "no_prior_neighbor_support"
    else:
        abstention_reason = "no_material_candidate"
    selected_keys = [_candidate_key(row) for row in selected]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "learner_model_name": REGRET_SURROGATE_CANDIDATE_VALUE_V8_MODEL_NAME,
        "target_label_space": "schedule_candidate_value_v8",
        "selected_final_candidate_keys": selected_keys,
        "fallback_final_anchor_keys": fallback_keys,
        "selected_final_candidate_count": len(selected_keys),
        "fallback_final_anchor_count": len(fallback_keys),
        "selected_final_family_counts": _family_counts(selected),
        "selected_final_candidate_source_counts": _source_counts(selected),
        "predicted_final_candidate_deltas": predicted_delta,
        "predicted_final_tail_risk_probabilities": predicted_tail,
        "fallback_to_v2_plus": len(selected_keys) == 0,
        "uses_v2_plus_anchor_fallback": bool(fallback_keys),
        "abstention_reason": abstention_reason,
        "selection_policy": "nearest_prior_strict_rescored_candidate_value_v8",
        "claim_scope": REGRET_SURROGATE_MODEL_V8_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "raw_hourly_action_imitation": False,
    }


def _fit_scope_v8_pruned_candidate_value(
    scope_rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
    max_prior_neighbor_distance: float,
    min_neighbor_safe_win_count: int,
    min_predicted_improvement_uah: float,
    max_neighbor_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
) -> dict[str, Any]:
    final_rows = [
        row for row in scope_rows if str(row["split_name"]) == "final_holdout"
    ]
    selected, fallback_keys, predicted_delta, predicted_tail = (
        _select_v8_pruned_final_candidates(
            final_rows,
            max_prior_neighbor_distance=max_prior_neighbor_distance,
            min_neighbor_safe_win_count=min_neighbor_safe_win_count,
            min_predicted_improvement_uah=min_predicted_improvement_uah,
            max_neighbor_tail_risk_probability=max_neighbor_tail_risk_probability,
            allowed_candidate_sources=allowed_candidate_sources,
        )
    )
    material_final_count = sum(
        1
        for row in final_rows
        if str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
        and bool(row.get("eligible_for_final_selection_v8", True))
        and float(row["label_regret_delta_vs_v2_plus_uah"]) < 0.0
    )
    if selected:
        abstention_reason = "selected_prior_supported_pruned_v8_candidates"
    elif material_final_count > 0:
        abstention_reason = "no_prior_neighbor_support_after_pruning"
    else:
        abstention_reason = "no_material_candidate_after_pruning"
    selected_keys = [_candidate_key(row) for row in selected]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "learner_model_name": REGRET_SURROGATE_V8_PRUNED_CANDIDATE_VALUE_MODEL_NAME,
        "target_label_space": "schedule_candidate_value_v8_pruned",
        "selected_final_candidate_keys": selected_keys,
        "fallback_final_anchor_keys": fallback_keys,
        "selected_final_candidate_count": len(selected_keys),
        "fallback_final_anchor_count": len(fallback_keys),
        "selected_final_family_counts": _family_counts(selected),
        "selected_final_candidate_source_counts": _source_counts(selected),
        "predicted_final_candidate_deltas": predicted_delta,
        "predicted_final_tail_risk_probabilities": predicted_tail,
        "fallback_to_v2_plus": len(selected_keys) == 0,
        "uses_v2_plus_anchor_fallback": bool(fallback_keys),
        "abstention_reason": abstention_reason,
        "selection_policy": "nearest_prior_pruned_candidate_value_v8",
        "claim_scope": REGRET_SURROGATE_V8_PRUNED_SELECTOR_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "raw_hourly_action_imitation": False,
    }


def _select_v8_pruned_final_candidates(
    final_rows: list[dict[str, Any]],
    *,
    max_prior_neighbor_distance: float,
    min_neighbor_safe_win_count: int,
    min_predicted_improvement_uah: float,
    max_neighbor_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
) -> tuple[list[dict[str, Any]], list[str], dict[str, float], dict[str, float]]:
    selected: list[dict[str, Any]] = []
    fallback_keys: list[str] = []
    predicted_delta: dict[str, float] = {}
    predicted_tail: dict[str, float] = {}
    for anchor, anchor_rows in sorted(_rows_by_datetime_anchor(final_rows).items()):
        candidates: list[tuple[dict[str, Any], float, float, float]] = []
        for row in anchor_rows:
            source = str(row["candidate_source"])
            if source not in allowed_candidate_sources:
                continue
            if not bool(row.get("eligible_for_final_selection_v8", True)):
                continue
            nearest = float(
                row.get(
                    "selector_feature_v8_pruned_nearest_prior_safe_switch_distance",
                    float("inf"),
                )
            )
            safe_count = int(
                float(
                    row.get("selector_feature_v8_pruned_neighbor_safe_win_count", 0.0)
                )
            )
            tail = float(
                row.get(
                    "selector_feature_v8_pruned_neighbor_tail_risk_probability", 1.0
                )
            )
            delta = float(
                row.get("selector_feature_v8_pruned_neighbor_mean_delta_uah", 0.0)
            )
            predicted_delta[_candidate_key(row)] = delta
            predicted_tail[_candidate_key(row)] = tail
            if (
                nearest <= max_prior_neighbor_distance
                and safe_count >= min_neighbor_safe_win_count
                and tail <= max_neighbor_tail_risk_probability
                and delta <= -min_predicted_improvement_uah
            ):
                candidates.append((row, delta, tail, nearest))
        if not candidates:
            first = anchor_rows[0]
            fallback_keys.append(
                _anchor_key_from_parts(
                    str(first["tenant_id"]),
                    str(first["source_model_name"]),
                    anchor,
                )
            )
            continue
        selected.append(
            min(
                candidates,
                key=lambda item: (
                    item[2],
                    item[1],
                    item[3],
                    str(item[0]["candidate_family"]),
                    str(item[0]["candidate_model_name"]),
                ),
            )[0]
        )
    return selected, fallback_keys, predicted_delta, predicted_tail


def _select_v8_final_candidates(
    final_rows: list[dict[str, Any]],
    *,
    max_prior_neighbor_distance: float,
    min_neighbor_safe_win_count: int,
    min_predicted_improvement_uah: float,
    max_neighbor_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
) -> tuple[list[dict[str, Any]], list[str], dict[str, float], dict[str, float]]:
    selected: list[dict[str, Any]] = []
    fallback_keys: list[str] = []
    predicted_delta: dict[str, float] = {}
    predicted_tail: dict[str, float] = {}
    for anchor, anchor_rows in sorted(_rows_by_datetime_anchor(final_rows).items()):
        candidates: list[tuple[dict[str, Any], float, float, float]] = []
        for row in anchor_rows:
            source = str(row["candidate_source"])
            if source not in allowed_candidate_sources:
                continue
            if not bool(row.get("eligible_for_final_selection_v8", True)):
                continue
            nearest = float(
                row.get(
                    "selector_feature_v8_nearest_prior_safe_switch_distance",
                    float("inf"),
                )
            )
            safe_count = int(
                float(row.get("selector_feature_v8_neighbor_safe_win_count", 0.0))
            )
            tail = float(
                row.get("selector_feature_v8_neighbor_tail_risk_probability", 1.0)
            )
            delta = float(row.get("selector_feature_v8_neighbor_mean_delta_uah", 0.0))
            predicted_delta[_candidate_key(row)] = delta
            predicted_tail[_candidate_key(row)] = tail
            if (
                nearest <= max_prior_neighbor_distance
                and safe_count >= min_neighbor_safe_win_count
                and tail <= max_neighbor_tail_risk_probability
                and delta <= -min_predicted_improvement_uah
            ):
                candidates.append((row, delta, tail, nearest))
        if not candidates:
            first = anchor_rows[0]
            fallback_keys.append(
                _anchor_key_from_parts(
                    str(first["tenant_id"]),
                    str(first["source_model_name"]),
                    anchor,
                )
            )
            continue
        selected.append(
            min(
                candidates,
                key=lambda item: (
                    item[2],
                    item[1],
                    item[3],
                    str(item[0]["candidate_family"]),
                    str(item[0]["candidate_model_name"]),
                ),
            )[0]
        )
    return selected, fallback_keys, predicted_delta, predicted_tail


def _select_sparse_final_candidates(
    final_rows: list[dict[str, Any]],
    *,
    max_prior_neighbor_distance: float,
    min_neighbor_safe_win_count: int,
    min_predicted_improvement_uah: float,
    max_neighbor_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
) -> tuple[list[dict[str, Any]], list[str], dict[str, float], dict[str, float]]:
    selected: list[dict[str, Any]] = []
    fallback_keys: list[str] = []
    predicted_delta: dict[str, float] = {}
    predicted_tail: dict[str, float] = {}
    for anchor, anchor_rows in sorted(_rows_by_datetime_anchor(final_rows).items()):
        candidates: list[tuple[dict[str, Any], float, float, float]] = []
        for row in anchor_rows:
            source = str(row["candidate_source"])
            if source not in allowed_candidate_sources:
                continue
            if not bool(row.get("eligible_for_final_selection_v6", True)):
                continue
            nearest = float(
                row.get(
                    "selector_feature_nearest_prior_safe_switch_distance", float("inf")
                )
            )
            safe_count = int(
                float(row.get("selector_feature_neighbor_safe_win_count", 0.0))
            )
            tail = float(
                row.get("selector_feature_neighbor_tail_risk_probability", 1.0)
            )
            delta = float(row.get("selector_feature_neighbor_mean_delta_uah", 0.0))
            predicted_delta[_candidate_key(row)] = delta
            predicted_tail[_candidate_key(row)] = tail
            if (
                nearest <= max_prior_neighbor_distance
                and safe_count >= min_neighbor_safe_win_count
                and tail <= max_neighbor_tail_risk_probability
                and delta <= -min_predicted_improvement_uah
            ):
                candidates.append((row, delta, tail, nearest))
        if not candidates:
            first = anchor_rows[0]
            fallback_keys.append(
                _anchor_key_from_parts(
                    str(first["tenant_id"]),
                    str(first["source_model_name"]),
                    anchor,
                )
            )
            continue
        selected.append(
            min(
                candidates,
                key=lambda item: (
                    item[2],
                    item[1],
                    item[3],
                    str(item[0]["candidate_family"]),
                    str(item[0]["candidate_model_name"]),
                ),
            )[0]
        )
    return selected, fallback_keys, predicted_delta, predicted_tail


def _sparse_rolling_summary_row(
    window_frame: pl.DataFrame,
    model_frame: pl.DataFrame,
    *,
    source_model_name: str,
    window_index: int,
    validation_anchors: tuple[datetime, ...],
    prior_anchors: tuple[datetime, ...],
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
) -> dict[str, Any]:
    row = _rolling_summary_row(
        window_frame,
        model_frame,
        source_model_name=source_model_name,
        window_index=window_index,
        validation_anchors=validation_anchors,
        prior_anchors=prior_anchors,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            min_mean_regret_improvement_ratio_vs_v2_plus
        ),
    )
    row["claim_scope"] = REGRET_SURROGATE_SPARSE_ROBUSTNESS_CLAIM_SCOPE
    return row


def _role_summaries(strict_frame: pl.DataFrame) -> dict[str, dict[str, float]]:
    summaries: dict[str, dict[str, float]] = {}
    for role in sorted(str(value) for value in strict_frame["selection_role"].unique()):
        role_frame = strict_frame.filter(pl.col("selection_role") == role)
        summaries[role] = {
            "row_count": float(role_frame.height),
            "mean_regret_uah": _float_stat(role_frame["regret_uah"].mean()),
            "median_regret_uah": _float_stat(role_frame["regret_uah"].median()),
            "safety_violation_count": _float_stat(
                role_frame["safety_violation_count"].sum()
            ),
        }
    return summaries


def _group_by_anchor(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str, datetime], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_anchor_key(row), []).append(row)
    return grouped


def _rows_by_datetime_anchor(
    rows: list[dict[str, Any]],
) -> dict[datetime, list[dict[str, Any]]]:
    grouped: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_datetime_value(row["anchor_timestamp"]), []).append(row)
    return grouped


def _train_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if str(row["split_name"]) != "final_holdout"]


def _eligible_challengers(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if bool(row["eligible_for_final_selection"])
        and str(row["candidate_source"]) not in _REFERENCE_CANDIDATE_SOURCES
    ]


def _baseline_row(
    rows: list[dict[str, Any]],
    *,
    anchor_key: tuple[str, str, datetime],
) -> dict[str, Any]:
    matches = [
        row for row in rows if str(row["candidate_source"]) == _V2_PLUS_CANDIDATE_SOURCE
    ]
    if not matches:
        raise ValueError(f"missing V2+ fallback row for {anchor_key}.")
    return min(matches, key=lambda row: float(row["regret_uah"]))


def _strict_reference_row(
    rows: list[dict[str, Any]],
    *,
    baseline: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    matches = [
        row for row in rows if str(row["candidate_source"]) == _STRICT_CANDIDATE_SOURCE
    ]
    if matches:
        return min(matches, key=lambda row: float(row["regret_uah"])), True
    strict_model_matches = [
        row for row in rows if str(row["candidate_model_name"]) == "strict_similar_day"
    ]
    if strict_model_matches:
        return min(strict_model_matches, key=lambda row: float(row["regret_uah"])), True
    return baseline, False


def _anchor_key(row: dict[str, Any]) -> tuple[str, str, datetime]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _anchor_key_string(row: dict[str, Any]) -> str:
    return _anchor_key_from_parts(
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _anchor_key_from_parts(
    tenant_id: str,
    source_model_name: str,
    anchor: datetime,
) -> str:
    return "|".join([tenant_id, source_model_name, anchor.isoformat()])


def _candidate_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"]).isoformat(),
            str(row["candidate_source"]),
            str(row["candidate_family"]),
            str(row["candidate_model_name"]),
        ]
    )


def _profile_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row["candidate_source"]),
            str(row["candidate_family"]),
            str(row["candidate_model_name"]),
        ]
    )


def _v8_family_group_key(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        str(row["candidate_source"]),
        str(row["candidate_family"]),
        str(row["candidate_model_name"]),
    )


def _v8_audit_boundary_fields(*, claim_scope: str) -> dict[str, Any]:
    return {
        "claim_scope": claim_scope,
        "target_label_space": "schedule_candidate_value_v8",
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "raw_hourly_action_imitation": False,
    }


def _v8_candidate_family_audit_row(
    group_key: tuple[str, str, str, str, str],
    rows: list[dict[str, Any]],
    *,
    selected_keys: set[str],
    false_positive_delta_uah: float,
    material_switch_delta_uah: float,
    tail_risk_delta_uah: float,
    prune_tail_risk_probability_threshold: float,
) -> dict[str, Any]:
    tenant_id, source_model_name, candidate_source, candidate_family, model_name = (
        group_key
    )
    prior_rows = [row for row in rows if str(row["split_name"]) != "final_holdout"]
    final_rows = [row for row in rows if str(row["split_name"]) == "final_holdout"]
    selected_rows = [row for row in rows if _candidate_key(row) in selected_keys]
    prior_safe = sum(
        1
        for row in prior_rows
        if _v8_is_material_safe_switch(
            row, material_switch_delta_uah=material_switch_delta_uah
        )
    )
    prior_tail = sum(
        1
        for row in prior_rows
        if _v8_is_tail_risk_loss(row, tail_risk_delta_uah=tail_risk_delta_uah)
    )
    final_safe = sum(
        1
        for row in final_rows
        if _v8_is_material_safe_switch(
            row, material_switch_delta_uah=material_switch_delta_uah
        )
    )
    final_tail = sum(
        1
        for row in final_rows
        if _v8_is_tail_risk_loss(row, tail_risk_delta_uah=tail_risk_delta_uah)
    )
    selected_false_positive = sum(
        1
        for row in selected_rows
        if float(row["label_regret_delta_vs_v2_plus_uah"]) > false_positive_delta_uah
    )
    selected_tail = sum(
        1
        for row in selected_rows
        if _v8_is_tail_risk_loss(row, tail_risk_delta_uah=tail_risk_delta_uah)
    )
    prior_tail_probability = (
        float(prior_tail) / float(len(prior_rows)) if prior_rows else 0.0
    )
    prior_pruned = bool(
        prior_rows
        and (
            prior_tail_probability >= prune_tail_risk_probability_threshold
            or (prior_tail > prior_safe and prior_tail > 0)
        )
    )
    selected_deltas = [
        float(row["label_regret_delta_vs_v2_plus_uah"]) for row in selected_rows
    ]
    prior_deltas = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in prior_rows]
    recommended_action = _v8_family_next_action(
        prior_pruned=prior_pruned,
        selected_false_positive_count=selected_false_positive,
        selected_tail_risk_loss_count=selected_tail,
        final_safe_win_count=final_safe,
        prior_safe_win_count=prior_safe,
    )
    return {
        **_v8_audit_boundary_fields(
            claim_scope=REGRET_SURROGATE_V8_FALSE_POSITIVE_AUDIT_CLAIM_SCOPE
        ),
        "audit_row_type": "candidate_family",
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "anchor_timestamp": None,
        "candidate_source": candidate_source,
        "candidate_family": candidate_family,
        "candidate_model_name": model_name,
        "candidate_key": "",
        "false_positive_class": "candidate_family_summary",
        "recommended_next_action": recommended_action,
        "prior_candidate_count": len(prior_rows),
        "prior_safe_win_count": prior_safe,
        "prior_tail_risk_loss_count": prior_tail,
        "prior_tail_risk_probability": prior_tail_probability,
        "prior_mean_delta_uah": mean(prior_deltas) if prior_deltas else 0.0,
        "final_candidate_count": len(final_rows),
        "final_safe_win_count": final_safe,
        "final_tail_risk_loss_count": final_tail,
        "selected_final_count": len(selected_rows),
        "selected_false_positive_count": selected_false_positive,
        "selected_tail_risk_loss_count": selected_tail,
        "selected_mean_delta_uah": mean(selected_deltas) if selected_deltas else 0.0,
        "selected_candidate_delta_uah": 0.0,
        "selected_predicted_delta_uah": 0.0,
        "selected_predicted_tail_risk_probability": 0.0,
        "prior_pruned_for_next_training": prior_pruned,
        "diagnostic_backfill_required": (
            recommended_action == "backfill_ukrainian_prior_context"
        ),
    }


def _v8_is_material_safe_switch(
    row: dict[str, Any],
    *,
    material_switch_delta_uah: float,
) -> bool:
    return bool(row.get("label_v8_material_safe_switch", False)) or (
        float(row["label_regret_delta_vs_v2_plus_uah"])
        <= -material_switch_delta_uah
    )


def _v8_is_tail_risk_loss(
    row: dict[str, Any],
    *,
    tail_risk_delta_uah: float,
) -> bool:
    return bool(row.get("label_v8_tail_risk_loss", False)) or (
        float(row["label_regret_delta_vs_v2_plus_uah"]) >= tail_risk_delta_uah
    )


def _v8_false_positive_class(
    row: dict[str, Any],
    *,
    false_positive_delta_uah: float,
    material_switch_delta_uah: float,
    tail_risk_delta_uah: float,
) -> str:
    delta = float(row["label_regret_delta_vs_v2_plus_uah"])
    if delta > false_positive_delta_uah:
        if _v8_is_tail_risk_loss(row, tail_risk_delta_uah=tail_risk_delta_uah):
            return "v8_false_positive_tail_risk_loss"
        return "v8_false_positive_weak_loss"
    if delta <= -material_switch_delta_uah:
        return "v8_true_positive_safe_switch"
    return "v8_neutral_or_small_delta_switch"


def _v8_family_next_action(
    *,
    prior_pruned: bool,
    selected_false_positive_count: int,
    selected_tail_risk_loss_count: int,
    final_safe_win_count: int,
    prior_safe_win_count: int,
) -> str:
    if prior_pruned:
        return "prune_candidate_family"
    if selected_false_positive_count > 0 or selected_tail_risk_loss_count > 0:
        return "backfill_ukrainian_prior_context"
    if final_safe_win_count > 0 and prior_safe_win_count == 0:
        return "backfill_ukrainian_prior_context"
    if prior_safe_win_count > 0:
        return "keep_candidate_family"
    return "monitor_candidate_family"


def _v8_selected_switch_next_action(
    false_positive_class: str,
    *,
    prior_pruned: bool,
) -> str:
    if prior_pruned:
        return "prune_candidate_family"
    if false_positive_class.startswith("v8_false_positive"):
        return "backfill_ukrainian_prior_context"
    if false_positive_class == "v8_true_positive_safe_switch":
        return "keep_candidate_family"
    return "monitor_candidate_family"


def _selector_feature_columns(frame: pl.DataFrame) -> list[str]:
    return sorted(
        column for column in frame.columns if column.startswith("selector_feature_")
    )


def _loss_weight(delta: float, tail_loss: bool) -> float:
    if tail_loss:
        return 2.0 + min(abs(delta), 500.0) / 250.0
    if delta < 0.0:
        return 1.0 + min(abs(delta), 500.0) / 500.0
    return 0.75


def _family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["candidate_family"])
        counts[key] = counts.get(key, 0) + 1
    return counts


def _source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["candidate_source"])
        counts[key] = counts.get(key, 0) + 1
    return counts


def _anchor_count(rows: list[dict[str, Any]]) -> int:
    return len({_anchor_key(row) for row in rows})


def _tenant_anchor_count(frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    return frame.select(["tenant_id", "anchor_timestamp"]).unique().height


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    value = frame["generated_at"].max()
    return _datetime_value(value)


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"expected datetime-like value, got {type(value)!r}")


def _improvement_ratio(baseline: float, challenger: float) -> float:
    if baseline <= 0.0:
        return 0.0
    return (baseline - challenger) / baseline


def _float_stat(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def _no_market_execution(frame: pl.DataFrame) -> bool:
    return not bool(frame.select(pl.col("market_execution_enabled").any()).item())


def _runtime_metadata(use_cuda_if_available: bool) -> tuple[str, str]:
    try:
        import torch
    except ImportError:
        return "profile_regret_surrogate_torch_unavailable", "cpu"
    if use_cuda_if_available and torch.cuda.is_available():
        return "profile_regret_surrogate_torch_cuda_metadata", "cuda"
    return "profile_regret_surrogate_torch_cpu_metadata", "cpu"


def _validate_candidate_panel(frame: pl.DataFrame) -> None:
    _require_columns(frame, _REQUIRED_CANDIDATE_COLUMNS, frame_name="candidate panel")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("regret-surrogate candidate panel refuses market execution.")
    if frame.select(pl.col("raw_hourly_action_imitation").any()).item():
        raise ValueError("regret-surrogate DFL does not imitate raw hourly actions.")
    if _V2_PLUS_CANDIDATE_SOURCE not in set(frame["candidate_source"].to_list()):
        raise ValueError("candidate panel requires V2+ fallback rows.")
    if _STRICT_CANDIDATE_SOURCE not in set(frame["candidate_source"].to_list()):
        raise ValueError("candidate panel requires strict fallback rows.")


def _validate_teacher_panel(frame: pl.DataFrame) -> None:
    _validate_candidate_panel(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "is_training_row",
                "teacher_panel_version",
                "selected_feature_names",
                "label_expected_regret_delta_vs_v2_plus_uah",
            }
        ),
        frame_name="teacher panel",
    )
    final_training = frame.filter(pl.col("split_name") == "final_holdout").select(
        pl.col("is_training_row").any()
    )
    if final_training.item():
        raise ValueError("final-holdout rows cannot be training rows.")


def _validate_context_teacher_panel(frame: pl.DataFrame) -> None:
    _validate_teacher_panel(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "safe_switch_context_profile_key",
                "label_context_material_safe_switch",
                "diagnostic_anchor_safe_switch_context_failure_mode",
                "selector_feature_context_prior_support_count",
                "selector_feature_context_prior_safe_win_rate",
                "selector_feature_context_prior_tail_risk_probability",
                "selector_feature_context_prior_mean_delta_uah",
            }
        ),
        frame_name="regret surrogate context teacher panel",
    )


def _validate_sparse_candidate_library(frame: pl.DataFrame) -> None:
    _validate_context_teacher_panel(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "candidate_library_version",
                "candidate_schedule_class",
                "eligible_for_final_selection_v6",
                "oracle_neighborhood_train_only",
            }
        ),
        frame_name="sparse safe-switch candidate library",
    )
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError(
            "sparse safe-switch candidate library refuses market execution."
        )


def _validate_sparse_teacher_panel(frame: pl.DataFrame) -> None:
    _validate_sparse_candidate_library(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "label_sparse_material_safe_switch",
                "label_sparse_opportunity_class",
                "selector_feature_nearest_prior_safe_switch_distance",
                "selector_feature_neighbor_safe_win_count",
                "selector_feature_neighbor_tail_risk_probability",
                "selector_feature_neighbor_mean_delta_uah",
            }
        ),
        frame_name="sparse safe-switch teacher panel",
    )


def _validate_v7_backfill_requirements_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "split_name",
                "opportunity_backfill_decision",
                "candidate_family_gap",
                "diagnostic_strict_control_material_local_win",
                "terminal_soc_pressure",
                "spread_regime_high",
                "market_execution_enabled",
            }
        ),
        frame_name="V7 opportunity backfill requirements frame",
    )
    allowed = {
        "backfill_needed",
        "candidate_generation_needed",
        "dt_ready",
        "stop_modeling_current_candidate_space",
    }
    actual = set(
        str(value) for value in frame["opportunity_backfill_decision"].to_list()
    )
    unknown = actual.difference(allowed)
    if unknown:
        raise ValueError(f"unknown V7 backfill decisions: {sorted(unknown)}")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V7 backfill requirements refuse market execution.")


def _validate_v7_candidate_library(frame: pl.DataFrame) -> None:
    _validate_sparse_candidate_library(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "eligible_for_final_selection_v7",
                "candidate_schedule_class",
                "target_label_space",
            }
        ),
        frame_name="V7 feasible schedule candidate library",
    )
    if frame.filter(
        (pl.col("candidate_source") == "oracle_gap_candidate")
        & (pl.col("split_name") == "final_holdout")
        & pl.col("eligible_for_final_selection_v7")
    ).height:
        raise ValueError("V7 oracle-neighborhood diagnostics must be train-only.")


def _validate_v8_context_panel(frame: pl.DataFrame) -> None:
    _validate_v7_candidate_library(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "selector_feature_ua_context_ready",
                "selector_feature_ua_peak_hour_index",
                "selector_feature_ua_trough_hour_index",
                "selector_feature_ua_morning_evening_spread_skew",
                "diagnostic_ua_context_blockers",
                "training_source_scope",
            }
        ),
        frame_name="V8 Ukrainian context backfilled feature panel",
    )
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V8 Ukrainian context backfill refuses market execution.")


def _validate_v8_candidate_library(frame: pl.DataFrame) -> None:
    _validate_v8_context_panel(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "eligible_for_final_selection_v8",
                "candidate_value_label_status",
                "diagnostic_requires_strict_rescore",
            }
        ),
        frame_name="V8 Ukrainian feasible schedule candidate library",
    )
    if frame.filter(
        (pl.col("candidate_source") == _V8_GENERATED_CANDIDATE_SOURCE)
        & (pl.col("candidate_value_label_status") != "pending_strict_rescore")
    ).height:
        raise ValueError("V8 generated candidates must wait for strict rescore labels.")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V8 Ukrainian candidate library refuses market execution.")


def _validate_v8_strict_rescore_frame(frame: pl.DataFrame) -> None:
    _validate_v8_context_panel(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "eligible_for_final_selection_v8",
                "candidate_value_label_status",
                "diagnostic_requires_strict_rescore",
                "strict_rescore_version",
            }
        ),
        frame_name="V8 Ukrainian candidate strict rescore frame",
    )
    if frame.filter(
        (pl.col("candidate_source") == _V8_GENERATED_CANDIDATE_SOURCE)
        & (pl.col("candidate_value_label_status") != "strict_rescored_v8_candidate")
    ).height:
        raise ValueError("V8 generated candidates must be strict-rescored.")
    if frame.filter(
        (pl.col("candidate_source") == _V8_GENERATED_CANDIDATE_SOURCE)
        & pl.col("diagnostic_requires_strict_rescore")
    ).height:
        raise ValueError("V8 strict-rescored candidates cannot require rescore.")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V8 strict rescore refuses market execution.")


def _validate_v8_teacher_panel(frame: pl.DataFrame) -> None:
    _validate_v8_strict_rescore_frame(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "teacher_panel_version",
                "label_v8_material_safe_switch",
                "label_v8_tail_risk_loss",
                "label_v8_opportunity_backfill_decision",
                "selector_feature_v8_nearest_prior_safe_switch_distance",
                "selector_feature_v8_neighbor_safe_win_count",
                "selector_feature_v8_neighbor_tail_risk_probability",
                "selector_feature_v8_neighbor_mean_delta_uah",
            }
        ),
        frame_name="V8 Ukrainian candidate-value teacher label panel",
    )
    final_training = frame.filter(pl.col("split_name") == "final_holdout").select(
        pl.col("is_training_row").any()
    )
    if final_training.item():
        raise ValueError("V8 final-holdout rows cannot be training rows.")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V8 teacher label panel refuses market execution.")


def _validate_v8_pruned_teacher_panel(frame: pl.DataFrame) -> None:
    _validate_v8_teacher_panel(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "v8_pruned_candidate_library_version",
                "label_v8_pruned_material_safe_switch",
                "label_v8_pruned_tail_risk_loss",
                "selector_feature_v8_pruned_nearest_prior_safe_switch_distance",
                "selector_feature_v8_pruned_neighbor_safe_win_count",
                "selector_feature_v8_pruned_neighbor_tail_risk_probability",
                "selector_feature_v8_pruned_neighbor_mean_delta_uah",
            }
        ),
        frame_name="V8 pruned candidate-value teacher label panel",
    )
    if frame.filter(pl.col("teacher_panel_version") != "candidate_value_teacher_v8_pruned").height:
        raise ValueError("V8 pruned teacher rows must use the pruned teacher version.")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V8 pruned teacher label panel refuses market execution.")


def _validate_v9_context_panel(frame: pl.DataFrame) -> None:
    _validate_v8_teacher_panel(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "v8_pruned_candidate_library_version",
                "label_v8_pruned_material_safe_switch",
                "label_v8_pruned_tail_risk_loss",
                "selector_feature_v8_pruned_nearest_prior_safe_switch_distance",
                "selector_feature_v8_pruned_neighbor_safe_win_count",
                "selector_feature_v8_pruned_neighbor_tail_risk_probability",
                "selector_feature_v8_pruned_neighbor_mean_delta_uah",
            }
        ),
        frame_name="V9 Ukrainian prior context backfill panel",
    )
    _require_columns(
        frame,
        frozenset(
            {
                "selector_feature_v9_prior_context_neighbor_count",
                "selector_feature_v9_prior_context_safe_win_rate",
                "selector_feature_v9_prior_context_tail_risk_rate",
                "selector_feature_v9_prior_context_mean_delta_uah",
                "selector_feature_v9_prior_context_ready",
                "diagnostic_v9_prior_context_blockers",
            }
        ),
        frame_name="V9 Ukrainian prior context backfill panel",
    )
    if frame.filter(pl.col("feature_panel_version") != "ua_prior_context_backfill_v9").height:
        raise ValueError("V9 context rows must use the V9 feature panel version.")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V9 prior context panel refuses market execution.")


def _validate_v9_candidate_library(frame: pl.DataFrame) -> None:
    _validate_v9_context_panel(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "eligible_for_final_selection_v9",
                "candidate_value_label_status",
                "diagnostic_requires_strict_rescore",
            }
        ),
        frame_name="V9 non-tail-risk candidate library",
    )
    if frame.filter(
        (pl.col("candidate_source") == _V9_GENERATED_CANDIDATE_SOURCE)
        & (pl.col("candidate_value_label_status") != "pending_strict_rescore")
    ).height:
        raise ValueError("V9 generated candidates must start pending strict rescore.")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V9 candidate library refuses market execution.")


def _validate_v9_strict_rescore_frame(frame: pl.DataFrame) -> None:
    _validate_v9_context_panel(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "eligible_for_final_selection_v9",
                "candidate_value_label_status",
                "diagnostic_requires_strict_rescore",
                "strict_rescore_version",
            }
        ),
        frame_name="V9 non-tail-risk strict rescore frame",
    )
    if frame.filter(
        (pl.col("candidate_source") == _V9_GENERATED_CANDIDATE_SOURCE)
        & (pl.col("candidate_value_label_status") != "strict_rescored_v9_candidate")
    ).height:
        raise ValueError("V9 generated candidates must be strict-rescored.")
    if frame.filter(
        (pl.col("candidate_source") == _V9_GENERATED_CANDIDATE_SOURCE)
        & pl.col("diagnostic_requires_strict_rescore")
    ).height:
        raise ValueError("V9 strict-rescored candidates cannot require rescore.")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V9 strict rescore refuses market execution.")


def _validate_v9_teacher_panel(frame: pl.DataFrame) -> None:
    _validate_v9_strict_rescore_frame(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "label_v9_material_safe_switch",
                "label_v9_tail_risk_loss",
                "selector_feature_v9_nearest_prior_safe_switch_distance",
                "selector_feature_v9_neighbor_safe_win_count",
                "selector_feature_v9_neighbor_tail_risk_probability",
                "selector_feature_v9_neighbor_mean_delta_uah",
                "diagnostic_v9_tail_risk_rejected",
                "eligible_for_next_selector_training_v9",
            }
        ),
        frame_name="V9 non-tail-risk candidate-value teacher panel",
    )
    if frame.filter(
        pl.col("teacher_panel_version") != "candidate_value_teacher_v9_non_tail_risk"
    ).height:
        raise ValueError("V9 teacher rows must use the V9 teacher version.")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V9 teacher label panel refuses market execution.")


def _validate_v7_teacher_panel(frame: pl.DataFrame) -> None:
    _validate_v7_candidate_library(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "label_v7_material_safe_switch",
                "label_v7_tail_risk_loss",
                "label_v7_opportunity_backfill_decision",
                "selector_feature_nearest_prior_safe_switch_distance",
                "selector_feature_neighbor_safe_win_count",
                "selector_feature_neighbor_tail_risk_probability",
                "selector_feature_neighbor_mean_delta_uah",
            }
        ),
        frame_name="V7 candidate value teacher panel",
    )


def _validate_scorer_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, _REQUIRED_SCORER_COLUMNS, frame_name="scorer frame")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("regret-surrogate scorer refuses market execution.")
    if frame.select(pl.col("raw_hourly_action_imitation").any()).item():
        raise ValueError("regret-surrogate scorer does not imitate raw hourly actions.")


def _validate_scorer_config(
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    min_prior_safe_win_count: int,
    min_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_predicted_tail_risk_probability: float,
    allowed_candidate_sources: tuple[str, ...],
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must not be empty.")
    if not source_model_names:
        raise ValueError("source_model_names must not be empty.")
    if min_prior_safe_win_count < 1:
        raise ValueError("min_prior_safe_win_count must be at least 1.")
    if min_prior_mean_improvement_uah < 0.0:
        raise ValueError("min_prior_mean_improvement_uah must not be negative.")
    if min_predicted_improvement_uah < 0.0:
        raise ValueError("min_predicted_improvement_uah must not be negative.")
    if not 0.0 <= max_predicted_tail_risk_probability <= 1.0:
        raise ValueError("max_predicted_tail_risk_probability must be between 0 and 1.")
    if not allowed_candidate_sources:
        raise ValueError("allowed_candidate_sources must not be empty.")


def _validate_sparse_neighbor_config(
    *,
    material_switch_delta_uah: float,
    max_prior_neighbor_distance: float,
    min_neighbor_safe_win_count: int,
    max_neighbor_tail_risk_probability: float,
    nearest_neighbor_count: int,
) -> None:
    if material_switch_delta_uah <= 0.0:
        raise ValueError("material_switch_delta_uah must be positive.")
    if max_prior_neighbor_distance < 0.0:
        raise ValueError("max_prior_neighbor_distance must not be negative.")
    if min_neighbor_safe_win_count < 1:
        raise ValueError("min_neighbor_safe_win_count must be at least 1.")
    if not 0.0 <= max_neighbor_tail_risk_probability <= 1.0:
        raise ValueError("max_neighbor_tail_risk_probability must be between 0 and 1.")
    if nearest_neighbor_count < 1:
        raise ValueError("nearest_neighbor_count must be at least 1.")


def _validate_sparse_selector_config(
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    max_prior_neighbor_distance: float,
    min_neighbor_safe_win_count: int,
    min_predicted_improvement_uah: float,
    max_neighbor_tail_risk_probability: float,
    allowed_candidate_sources: tuple[str, ...],
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must not be empty.")
    if not source_model_names:
        raise ValueError("source_model_names must not be empty.")
    if max_prior_neighbor_distance < 0.0:
        raise ValueError("max_prior_neighbor_distance must not be negative.")
    if min_neighbor_safe_win_count < 1:
        raise ValueError("min_neighbor_safe_win_count must be at least 1.")
    if min_predicted_improvement_uah < 0.0:
        raise ValueError("min_predicted_improvement_uah must not be negative.")
    if not 0.0 <= max_neighbor_tail_risk_probability <= 1.0:
        raise ValueError("max_neighbor_tail_risk_probability must be between 0 and 1.")
    if not allowed_candidate_sources:
        raise ValueError("allowed_candidate_sources must not be empty.")


def _validate_context_config(
    *,
    material_switch_delta_uah: float,
    high_v2_regret_uah: float,
    high_forecast_spread_uah_mwh: float,
    min_material_schedule_distance: float,
    min_context_prior_safe_win_count: int,
    min_context_prior_mean_improvement_uah: float,
    max_context_tail_risk_probability: float,
) -> None:
    if material_switch_delta_uah <= 0.0:
        raise ValueError("material_switch_delta_uah must be positive.")
    if high_v2_regret_uah <= 0.0:
        raise ValueError("high_v2_regret_uah must be positive.")
    if high_forecast_spread_uah_mwh <= 0.0:
        raise ValueError("high_forecast_spread_uah_mwh must be positive.")
    if min_material_schedule_distance < 0.0:
        raise ValueError("min_material_schedule_distance must not be negative.")
    if min_context_prior_safe_win_count < 1:
        raise ValueError("min_context_prior_safe_win_count must be at least 1.")
    if min_context_prior_mean_improvement_uah < 0.0:
        raise ValueError("min_context_prior_mean_improvement_uah must not be negative.")
    if not 0.0 <= max_context_tail_risk_probability <= 1.0:
        raise ValueError("max_context_tail_risk_probability must be between 0 and 1.")


def _validate_contextual_selector_config(
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    min_context_prior_support_count: int,
    min_context_prior_safe_win_count: int,
    min_context_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_context_tail_risk_probability: float,
    allowed_candidate_sources: tuple[str, ...],
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must not be empty.")
    if not source_model_names:
        raise ValueError("source_model_names must not be empty.")
    if min_context_prior_support_count < 1:
        raise ValueError("min_context_prior_support_count must be at least 1.")
    if min_context_prior_safe_win_count < 1:
        raise ValueError("min_context_prior_safe_win_count must be at least 1.")
    if min_context_prior_mean_improvement_uah < 0.0:
        raise ValueError("min_context_prior_mean_improvement_uah must not be negative.")
    if min_predicted_improvement_uah < 0.0:
        raise ValueError("min_predicted_improvement_uah must not be negative.")
    if not 0.0 <= max_context_tail_risk_probability <= 1.0:
        raise ValueError("max_context_tail_risk_probability must be between 0 and 1.")
    if not allowed_candidate_sources:
        raise ValueError("allowed_candidate_sources must not be empty.")


def _require_columns(
    frame: pl.DataFrame,
    required: frozenset[str],
    *,
    frame_name: str,
) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} missing required columns: {missing}")


__all__ = [
    "REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE",
    "REGRET_SURROGATE_CONTEXTUAL_STRICT_LP_STRATEGY_KIND",
    "REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE",
    "REGRET_SURROGATE_CANDIDATE_VALUE_V7_STRICT_LP_STRATEGY_KIND",
    "REGRET_SURROGATE_CANDIDATE_VALUE_V8_SELECTION_ROLE",
    "REGRET_SURROGATE_CANDIDATE_VALUE_V8_STRICT_LP_STRATEGY_KIND",
    "REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE",
    "REGRET_SURROGATE_SPARSE_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND",
    "REGRET_SURROGATE_SELECTION_ROLE",
    "REGRET_SURROGATE_STRICT_LP_STRATEGY_KIND",
    "STRICT_REFERENCE_ROLE",
    "V2_PLUS_REFERENCE_ROLE",
    "build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame",
    "build_dfl_regret_surrogate_contextual_candidate_value_v2_frame",
    "build_dfl_regret_surrogate_contextual_rolling_robustness_frame",
    "build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame",
    "build_dfl_regret_surrogate_candidate_value_v1_frame",
    "build_dfl_regret_surrogate_forecast_correction_v1_frame",
    "build_dfl_regret_surrogate_rolling_robustness_frame",
    "build_dfl_regret_surrogate_safe_switch_context_audit_frame",
    "build_dfl_regret_surrogate_strict_lp_benchmark_frame",
    "build_dfl_regret_surrogate_teacher_label_panel_v2_frame",
    "build_dfl_v2_plus_learning_limit_audit_frame",
    "build_dfl_v2_plus_opportunity_backfill_requirements_frame",
    "build_dfl_backfilled_context_feature_panel_v7_frame",
    "build_dfl_feasible_schedule_candidate_library_v7_frame",
    "build_dfl_ua_context_backfilled_feature_panel_v8_frame",
    "build_dfl_ua_context_candidate_v8_strict_rescore_frame",
    "build_dfl_ua_context_candidate_value_teacher_label_panel_v8_frame",
    "build_dfl_ua_context_feasible_schedule_candidate_library_v8_frame",
    "build_dfl_candidate_value_teacher_label_panel_v7_frame",
    "build_dfl_candidate_value_regret_surrogate_v7_frame",
    "build_dfl_candidate_value_regret_surrogate_v8_frame",
    "build_dfl_candidate_value_v7_rolling_robustness_frame",
    "build_dfl_candidate_value_v7_strict_lp_benchmark_frame",
    "build_dfl_candidate_value_v8_rolling_robustness_frame",
    "build_dfl_candidate_value_v8_strict_lp_benchmark_frame",
    "build_dfl_v8_false_positive_tail_risk_audit_frame",
    "build_dfl_v8_pruned_candidate_library_frame",
    "build_dfl_v8_pruned_candidate_family_plan_frame",
    "build_dfl_v8_pruned_candidate_value_selector_frame",
    "build_dfl_v8_pruned_candidate_value_strict_lp_benchmark_frame",
    "build_dfl_v8_pruned_candidate_value_teacher_label_panel_frame",
    "build_dfl_ua_non_tail_risk_candidate_library_v9_frame",
    "build_dfl_ua_non_tail_risk_candidate_v9_strict_rescore_frame",
    "build_dfl_ua_non_tail_risk_candidate_value_teacher_label_panel_v9_frame",
    "build_dfl_ua_prior_context_backfilled_feature_panel_v9_frame",
    "build_dfl_sparse_safe_switch_abstention_model_v6_frame",
    "build_dfl_sparse_safe_switch_candidate_library_v6_frame",
    "build_dfl_sparse_safe_switch_feature_contract_audit_frame",
    "build_dfl_sparse_safe_switch_opportunity_audit_frame",
    "build_dfl_sparse_safe_switch_rolling_robustness_frame",
    "build_dfl_sparse_safe_switch_strict_lp_benchmark_frame",
    "build_dfl_sparse_safe_switch_teacher_label_panel_v6_frame",
    "evaluate_dfl_regret_surrogate_gate",
]
