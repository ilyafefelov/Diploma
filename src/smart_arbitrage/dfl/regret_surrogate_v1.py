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

REGRET_SURROGATE_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_regret_surrogate_strict_lp_benchmark"
)
REGRET_SURROGATE_MODEL_NAME: Final[str] = (
    "dfl_regret_surrogate_candidate_value_v1"
)
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
        raise ValueError("min_oracle_improvement_ratio_vs_v2_plus must not be negative.")
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
                    [row for row in candidates if float(row["regret_uah"]) < baseline_regret]
                ),
                "learning_limit_failure_mode": failure_mode,
                "profile_prior_safe_win_count": int(best_stats.get("safe_win_count", 0)),
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
                    "regret_surrogate_dfl"
                    if can_beat
                    else "data_or_candidate_backfill"
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
        _anchor_key(row): row for row in learning_limit_audit_frame.iter_rows(named=True)
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
        selected_keys = [str(value) for value in scorer_row["selected_final_candidate_keys"]]
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
                "prior_context_mean_delta_uah": float(
                    stats.get("mean_delta_uah", 0.0)
                ),
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
        min_context_prior_mean_improvement_uah=(
            min_context_prior_mean_improvement_uah
        ),
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
    panel_rows = list(regret_surrogate_teacher_label_panel_v2_frame.iter_rows(named=True))
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
        row for row in train_rows if str(row["candidate_source"]) in allowed_candidate_sources
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
        "selected_final_candidate_keys": [_candidate_key(row) for row in selected_final],
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
        raise ValueError(f"{tenant_id}/{source_model_name} has no context teacher rows.")
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
        "selected_final_candidate_keys": [_candidate_key(row) for row in selected_final],
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
            if source not in allowed_candidate_sources or profile not in allowed_profiles:
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
    if best_delta >= tail_risk_delta_uah or int(best_stats.get("tail_loss_count", 0)) > 0:
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
        selected_keys.update(str(value) for value in row["selected_final_candidate_keys"])
        fallback_anchor_keys.update(str(value) for value in row["fallback_final_anchor_keys"])
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
        deltas = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in profile_rows]
        tail_count = sum(1 for row in profile_rows if bool(row["label_tail_risk_loss"]))
        safe_count = sum(1 for row in profile_rows if bool(row["label_safe_switch_win"]))
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
        deltas = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in profile_rows]
        tail_count = sum(1 for row in profile_rows if bool(row["label_tail_risk_loss"]))
        safe_count = sum(1 for row in profile_rows if bool(row["label_safe_switch_win"]))
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
    weekend = int(round(_numeric_feature(row, "selector_feature_anchor_is_weekend", "selector_feature_weekend")))
    grid_ready = int(round(_numeric_feature(row, "selector_feature_grid_event_context_ready")))
    high_v2 = int(float(row.get("v2_plus_baseline_regret_uah", 0.0)) >= high_v2_regret_uah)
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
    if float(stats.get("tail_risk_probability", 1.0)) > max_context_tail_risk_probability:
        return "context_prior_tail_risk"
    if int(stats.get("safe_win_count", 0)) < min_context_prior_safe_win_count:
        return "context_prior_weak_support"
    if float(stats.get("mean_delta_uah", 0.0)) > -min_context_prior_mean_improvement_uah:
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
    matches = [row for row in rows if str(row["candidate_source"]) == _V2_PLUS_CANDIDATE_SOURCE]
    if not matches:
        raise ValueError(f"missing V2+ fallback row for {anchor_key}.")
    return min(matches, key=lambda row: float(row["regret_uah"]))


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


def _selector_feature_columns(frame: pl.DataFrame) -> list[str]:
    return sorted(column for column in frame.columns if column.startswith("selector_feature_"))


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
        raise ValueError(
            "max_predicted_tail_risk_probability must be between 0 and 1."
        )
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
        raise ValueError(
            "min_context_prior_mean_improvement_uah must not be negative."
        )
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
        raise ValueError(
            "min_context_prior_mean_improvement_uah must not be negative."
        )
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
    "evaluate_dfl_regret_surrogate_gate",
]
