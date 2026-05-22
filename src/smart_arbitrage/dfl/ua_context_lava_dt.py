"""UA-context DT/LAVA candidate-index policy.

This module keeps the DT/LAVA bridge decision-safe: it trains on feasible
schedule candidates and predicts candidate-index/family switches, not raw
hourly BUY/SELL/HOLD actions. V2+ remains the fallback and comparator.
"""

from __future__ import annotations

from datetime import datetime
from statistics import mean, median
from typing import Any, Final, cast

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl.promotion_gate import (
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
)

UA_CONTEXT_LAVA_TEACHER_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_lava_teacher_not_full_dfl"
)
UA_CONTEXT_LAVA_SEQUENCE_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_lava_sequence_training_not_full_dfl"
)
UA_CONTEXT_LAVA_POLICY_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_lava_candidate_policy_not_full_dfl"
)
UA_CONTEXT_LAVA_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_lava_strict_lp_gate_not_full_dfl"
)
UA_CONTEXT_LAVA_ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_lava_rolling_robustness_not_full_dfl"
)

UA_CONTEXT_LAVA_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_ua_context_lava_strict_lp_benchmark"
)
UA_CONTEXT_LAVA_MODEL_NAME: Final[str] = "dfl_ua_context_lava_candidate_index_v1"
UA_CONTEXT_LAVA_SELECTION_ROLE: Final[str] = "ua_context_lava_candidate_policy"
UA_CONTEXT_LAVA_BEHAVIOR_CLONING_SELECTION_ROLE: Final[str] = (
    "ua_context_lava_behavior_cloning_reference"
)
V2_PLUS_REFERENCE_ROLE: Final[str] = "schedule_value_learner_v2_plus_reference"
STRICT_REFERENCE_ROLE: Final[str] = "strict_reference"

_REQUIRED_TEACHER_INPUT_COLUMNS: Final[frozenset[str]] = frozenset(
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
        "label_regret_delta_vs_v2_plus_uah",
        "label_safe_switch_win",
        "label_tail_risk_loss",
        "forecast_price_uah_mwh_vector",
        "actual_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "safety_violation_count",
        "evaluation_payload",
    }
)
_REQUIRED_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "selection_role",
        "anchor_timestamp",
        "generated_at",
        "horizon_hours",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "safety_violation_count",
        "evaluation_payload",
    }
)
_CORE_POLICY_FEATURES: Final[tuple[str, ...]] = (
    "selector_feature_schedule_distance_from_v2_plus",
    "selector_feature_total_throughput_delta_mwh",
    "selector_feature_terminal_soc_delta_fraction",
    "selector_feature_forecast_spread_uah_mwh",
    "selector_feature_total_degradation_penalty_uah",
    "selector_feature_weather_load_context_ready",
    "selector_feature_calendar_publication_context_ready",
    "selector_feature_grid_event_context_ready",
    "selector_feature_hour_of_day",
    "selector_feature_weekend",
)
_FALLBACK_SOURCES: Final[frozenset[str]] = frozenset(
    {"v2_plus_default", "strict_fallback"}
)


def build_dfl_ua_context_lava_teacher_frame(
    ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
    lava_tail_risk_avoidance_label_frame: pl.DataFrame | None = None,
    *,
    tail_risk_delta_uah: float = 150.0,
) -> pl.DataFrame:
    """Build candidate-index teacher rows from UA context oracle-gap evidence."""

    _require_columns(
        ua_context_oracle_gap_feature_panel_frame,
        _REQUIRED_TEACHER_INPUT_COLUMNS,
        frame_name="ua_context_oracle_gap_feature_panel_frame",
    )
    if tail_risk_delta_uah <= 0.0:
        raise ValueError("tail_risk_delta_uah must be positive.")
    lava_lookup = _lava_tail_risk_lookup(lava_tail_risk_avoidance_label_frame)
    rows: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in ua_context_oracle_gap_feature_panel_frame.iter_rows(named=True):
        anchor = _datetime_value(row["anchor_timestamp"])
        grouped.setdefault((str(row["tenant_id"]), str(row["source_model_name"]), anchor), []).append(row)
    for key, anchor_rows in grouped.items():
        ordered = sorted(
            anchor_rows,
            key=lambda row: (
                _candidate_priority(row),
                str(row["candidate_source"]),
                str(row["candidate_family"]),
                str(row["candidate_model_name"]),
            ),
        )
        for index, row in enumerate(ordered):
            candidate_key = _candidate_key(row)
            lava_class = lava_lookup.get(candidate_key)
            delta = float(row["label_regret_delta_vs_v2_plus_uah"])
            candidate_source = str(row["candidate_source"])
            eligible = bool(row["eligible_for_final_selection"])
            teacher_class = _teacher_class(
                row,
                delta=delta,
                lava_tail_risk_class=lava_class,
                tail_risk_delta_uah=tail_risk_delta_uah,
            )
            copied = dict(row)
            copied.update(
                {
                    "teacher_candidate_key": candidate_key,
                    "teacher_candidate_index": index,
                    "teacher_anchor_candidate_count": len(ordered),
                    "teacher_schedule_candidate_class": teacher_class,
                    "teacher_target_family": str(row["candidate_family"]),
                    "teacher_target_source": candidate_source,
                    "teacher_return_to_go_delta_uah": max(0.0, -delta),
                    "teacher_tail_risk_penalty_uah": max(0.0, delta),
                    "teacher_tail_risk_probability_target": (
                        1.0 if teacher_class == "avoid_tail_risk_neighbor" else 0.0
                    ),
                    "teacher_loss_weight": _teacher_loss_weight(teacher_class, delta),
                    "is_training_row": (
                        str(row["split_name"]) != "final_holdout"
                        and eligible
                        and teacher_class != "oracle_neighbor_diagnostic"
                    ),
                    "lava_tail_risk_avoidance_class": lava_class or "",
                    "target_label_space": "ua_context_schedule_candidate_index",
                    "raw_hourly_action_imitation": False,
                    "claim_scope": UA_CONTEXT_LAVA_TEACHER_CLAIM_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "market_execution_enabled": False,
                }
            )
            rows.append(copied)
    return pl.DataFrame(rows, infer_schema_length=None).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "teacher_candidate_index",
        ]
    )


def build_dfl_ua_context_lava_sequence_training_frame(
    ua_context_lava_teacher_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Add sequence and return-to-go metadata for candidate-index training."""

    _validate_teacher_frame(ua_context_lava_teacher_frame)
    rows = list(ua_context_lava_teacher_frame.iter_rows(named=True))
    anchor_positions: dict[tuple[str, str, datetime], int] = {}
    for tenant_id, source_model_name in sorted(
        {(str(row["tenant_id"]), str(row["source_model_name"])) for row in rows}
    ):
        anchors = sorted(
            {
                _datetime_value(row["anchor_timestamp"])
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            }
        )
        anchor_positions.update(
            {
                (tenant_id, source_model_name, anchor): index
                for index, anchor in enumerate(anchors)
            }
        )
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"]),
        )
        copied = dict(row)
        copied.update(
            {
                "sequence_position": anchor_positions[key],
                "dt_return_to_go_uah": float(row["teacher_return_to_go_delta_uah"]),
                "dt_tail_risk_target": float(
                    row["teacher_tail_risk_probability_target"]
                ),
                "dt_candidate_index_target": int(row["teacher_candidate_index"]),
                "dt_candidate_family_target": str(row["teacher_target_family"]),
                "target_label_space": "ua_context_schedule_candidate_index",
                "raw_hourly_action_imitation": False,
                "claim_scope": UA_CONTEXT_LAVA_SEQUENCE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        output_rows.append(copied)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "tenant_id",
            "source_model_name",
            "sequence_position",
            "teacher_candidate_index",
        ]
    )


def build_dfl_ua_context_lava_candidate_policy_frame(
    ua_context_lava_sequence_training_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    min_prior_safe_win_count: int = 2,
    max_prior_tail_loss_count: int = 0,
    min_prior_precision: float = 0.75,
    min_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_predicted_tail_risk_probability: float = 0.25,
    allowed_candidate_sources: tuple[str, ...] = (),
    hard_blocked_candidate_families: tuple[str, ...] = (
        "rank_extrema_perturbation_v2_plus",
    ),
    torch_hidden_size: int = 8,
    torch_max_epochs: int = 25,
    use_cuda_if_available: bool = True,
    random_seed: int = 23,
) -> pl.DataFrame:
    """Train a deterministic Torch candidate-index policy with V2+ fallback."""

    _validate_sequence_frame(ua_context_lava_sequence_training_frame)
    _validate_policy_config(
        tenant_ids=tenant_ids,
        source_model_names=source_model_names,
        min_prior_safe_win_count=min_prior_safe_win_count,
        max_prior_tail_loss_count=max_prior_tail_loss_count,
        min_prior_precision=min_prior_precision,
        min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
        torch_hidden_size=torch_hidden_size,
        torch_max_epochs=torch_max_epochs,
    )
    rows = list(ua_context_lava_sequence_training_frame.iter_rows(named=True))
    allowed_sources = set(allowed_candidate_sources)
    hard_blocked = set(hard_blocked_candidate_families)
    output_rows: list[dict[str, Any]] = []
    feature_names = _feature_columns(ua_context_lava_sequence_training_frame)
    for tenant_id in tenant_ids:
        for source_model_name in source_model_names:
            scope_rows = [
                row
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            ]
            if not scope_rows:
                continue
            train_rows = [
                row
                for row in scope_rows
                if bool(row["is_training_row"]) and bool(row["eligible_for_final_selection"])
            ]
            final_rows = [
                row
                for row in scope_rows
                if str(row["split_name"]) == "final_holdout"
                and bool(row["eligible_for_final_selection"])
            ]
            if not train_rows:
                raise ValueError(f"{tenant_id}/{source_model_name} needs train rows.")
            if not final_rows:
                raise ValueError(f"{tenant_id}/{source_model_name} needs final rows.")
            resolved_sources = _allowed_sources(train_rows, allowed_sources)
            challenger_train_rows = [
                row
                for row in train_rows
                if str(row["candidate_source"]) in resolved_sources
                and str(row["candidate_family"]) not in hard_blocked
            ]
            if not challenger_train_rows:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} needs challenger train rows."
                )
            profile_stats = _profile_stats(
                train_rows,
                allowed_candidate_sources=resolved_sources,
                hard_blocked_candidate_families=hard_blocked,
            )
            allowed_profiles = sorted(
                profile
                for profile, stats in profile_stats.items()
                if int(stats["safe_win_count"]) >= min_prior_safe_win_count
                and int(stats["tail_loss_count"]) <= max_prior_tail_loss_count
                and float(stats["safe_precision"]) >= min_prior_precision
                and float(stats["mean_prior_delta_uah"]) <= -min_prior_mean_improvement_uah
                and not bool(stats["hard_blocked"])
            )
            model = _fit_torch_candidate_policy(
                challenger_train_rows,
                feature_names=feature_names,
                torch_hidden_size=torch_hidden_size,
                torch_max_epochs=torch_max_epochs,
                use_cuda_if_available=use_cuda_if_available,
                random_seed=random_seed,
            )
            selected, fallback_keys, predicted = _select_final_candidates(
                final_rows,
                model=model,
                feature_names=feature_names,
                profile_stats=profile_stats,
                allowed_profiles=set(allowed_profiles),
                allowed_candidate_sources=resolved_sources,
                hard_blocked_candidate_families=hard_blocked,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
            )
            bc_selected, bc_fallback = _behavior_clone_final_candidates(
                train_rows,
                final_rows,
                allowed_candidate_sources=resolved_sources,
                hard_blocked_candidate_families=hard_blocked,
            )
            output_rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": UA_CONTEXT_LAVA_MODEL_NAME,
                    "target_label_space": "ua_context_schedule_candidate_index",
                    "raw_hourly_action_imitation": False,
                    "selected_policy_type": "torch_candidate_index_policy_v1",
                    "selected_feature_names": feature_names,
                    "torch_hidden_size": torch_hidden_size,
                    "torch_max_epochs": torch_max_epochs,
                    "torch_device": str(model["device"]),
                    "random_seed": random_seed,
                    "allowed_schedule_candidate_profiles": allowed_profiles,
                    "blocked_schedule_candidate_profiles": sorted(
                        profile
                        for profile, stats in profile_stats.items()
                        if profile not in allowed_profiles or bool(stats["hard_blocked"])
                    ),
                    "schedule_candidate_profile_prior_stats": profile_stats,
                    "fallback_to_v2_plus": not selected,
                    "uses_v2_plus_anchor_fallback": bool(fallback_keys),
                    "selector_gate_blocker": (
                        "ua_context_lava_candidate_selected"
                        if selected
                        else "no_prior_safe_ua_context_lava_profile"
                    ),
                    "allowed_candidate_sources": sorted(resolved_sources),
                    "hard_blocked_candidate_families": sorted(hard_blocked),
                    "min_predicted_improvement_uah": min_predicted_improvement_uah,
                    "max_predicted_tail_risk_probability": (
                        max_predicted_tail_risk_probability
                    ),
                    "train_anchor_count": _anchor_count(train_rows),
                    "final_holdout_anchor_count": _anchor_count(final_rows),
                    "fallback_final_anchor_keys": fallback_keys,
                    "selected_final_candidate_keys": [
                        _candidate_key(row) for row in selected
                    ],
                    "behavior_clone_final_candidate_keys": [
                        _candidate_key(row) for row in bc_selected
                    ],
                    "behavior_clone_fallback_anchor_keys": bc_fallback,
                    "selected_final_family_counts": _family_counts(selected, fallback_keys),
                    "selected_final_candidate_source_counts": _source_counts(
                        selected,
                        fallback_keys,
                    ),
                    "predicted_final_candidate_deltas": {
                        _candidate_key(row): float(
                            row["predicted_regret_delta_vs_v2_plus_uah"]
                        )
                        for row in predicted
                    },
                    "predicted_final_tail_risk_probabilities": {
                        _candidate_key(row): float(row["predicted_tail_risk_probability"])
                        for row in predicted
                    },
                    "claim_scope": UA_CONTEXT_LAVA_POLICY_CLAIM_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "market_execution_enabled": False,
                }
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["tenant_id", "source_model_name"]
    )


def build_dfl_ua_context_lava_strict_lp_benchmark_frame(
    ua_context_lava_sequence_training_frame: pl.DataFrame,
    ua_context_lava_candidate_policy_frame: pl.DataFrame,
    frozen_v2_plus_strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score UA-context DT/LAVA candidate-index policy against V2+."""

    _validate_sequence_frame(ua_context_lava_sequence_training_frame)
    _validate_policy_frame(ua_context_lava_candidate_policy_frame)
    _require_columns(
        frozen_v2_plus_strict_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="frozen_v2_plus_strict_frame",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        ua_context_lava_sequence_training_frame
    )
    candidate_rows = list(ua_context_lava_sequence_training_frame.iter_rows(named=True))
    source_models = {str(row["source_model_name"]) for row in candidate_rows}
    candidate_by_key = {_candidate_key(row): row for row in candidate_rows}
    v2_reference_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in frozen_v2_plus_strict_frame.iter_rows(named=True):
        if str(row["source_model_name"]) not in source_models:
            continue
        if str(row["selection_role"]) not in {
            "strict_reference",
            "schedule_value_learner_v2_plus",
        }:
            continue
        role = (
            STRICT_REFERENCE_ROLE
            if str(row["selection_role"]) == "strict_reference"
            else V2_PLUS_REFERENCE_ROLE
        )
        if (
            role == V2_PLUS_REFERENCE_ROLE
            and str(row["source_model_name"]) == baseline_source_model_name
        ):
            v2_reference_by_anchor[_anchor_key(row)] = row
        output_rows.append(
            _reference_row(row, selection_role=role, generated_at=resolved_generated_at)
        )
    for policy_row in ua_context_lava_candidate_policy_frame.iter_rows(named=True):
        for key in policy_row["selected_final_candidate_keys"]:
            output_rows.append(
                _candidate_benchmark_row(
                    candidate_by_key[str(key)],
                    policy_row=policy_row,
                    selection_role=UA_CONTEXT_LAVA_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
        for anchor_key in policy_row["fallback_final_anchor_keys"]:
            fallback = v2_reference_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"Missing V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _fallback_benchmark_row(
                    fallback,
                    policy_row=policy_row,
                    selection_role=UA_CONTEXT_LAVA_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
        for key in policy_row["behavior_clone_final_candidate_keys"]:
            output_rows.append(
                _candidate_benchmark_row(
                    candidate_by_key[str(key)],
                    policy_row=policy_row,
                    selection_role=UA_CONTEXT_LAVA_BEHAVIOR_CLONING_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
        for anchor_key in policy_row["behavior_clone_fallback_anchor_keys"]:
            fallback = v2_reference_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"Missing V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _fallback_benchmark_row(
                    fallback,
                    policy_row=policy_row,
                    selection_role=UA_CONTEXT_LAVA_BEHAVIOR_CLONING_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"]
    )


def build_dfl_ua_context_lava_rolling_robustness_frame(
    ua_context_lava_sequence_training_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    min_prior_safe_win_count: int = 2,
    max_prior_tail_loss_count: int = 0,
    min_prior_precision: float = 0.75,
    min_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_predicted_tail_risk_probability: float = 0.25,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    allowed_candidate_sources: tuple[str, ...] = (),
    hard_blocked_candidate_families: tuple[str, ...] = (
        "rank_extrema_perturbation_v2_plus",
    ),
    torch_hidden_size: int = 8,
    torch_max_epochs: int = 25,
    use_cuda_if_available: bool = True,
    random_seed: int = 23,
) -> pl.DataFrame:
    """Replay the UA-context candidate-index policy in prior-only windows."""

    _validate_sequence_frame(ua_context_lava_sequence_training_frame)
    rows = list(ua_context_lava_sequence_training_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        anchors = sorted(
            {
                _datetime_value(row["anchor_timestamp"])
                for row in rows
                if str(row["source_model_name"]) == source_model_name
            }
        )
        for window_index in range(validation_window_count):
            end = len(anchors) - window_index * validation_anchor_count
            start = end - validation_anchor_count
            if start < 0:
                break
            validation_anchors = tuple(anchors[start:end])
            prior_anchors = tuple(anchor for anchor in anchors[:start])
            if len(prior_anchors) < min_prior_anchors_before_window:
                continue
            window_frame = _window_sequence_frame(
                rows,
                source_model_name=source_model_name,
                prior_anchors=set(prior_anchors),
                validation_anchors=set(validation_anchors),
            )
            policy = build_dfl_ua_context_lava_candidate_policy_frame(
                window_frame,
                tenant_ids=tenant_ids,
                source_model_names=(source_model_name,),
                min_prior_safe_win_count=min_prior_safe_win_count,
                max_prior_tail_loss_count=max_prior_tail_loss_count,
                min_prior_precision=min_prior_precision,
                min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_predicted_tail_risk_probability=(
                    max_predicted_tail_risk_probability
                ),
                allowed_candidate_sources=allowed_candidate_sources,
                hard_blocked_candidate_families=hard_blocked_candidate_families,
                torch_hidden_size=torch_hidden_size,
                torch_max_epochs=torch_max_epochs,
                use_cuda_if_available=use_cuda_if_available,
                random_seed=random_seed,
            )
            output_rows.append(
                _rolling_summary_row(
                    window_frame,
                    policy,
                    source_model_name=source_model_name,
                    window_index=window_index,
                    validation_anchors=validation_anchors,
                    prior_anchors=prior_anchors,
                    min_mean_regret_improvement_ratio_vs_v2_plus=(
                        min_mean_regret_improvement_ratio_vs_v2_plus
                    ),
                    min_mean_regret_improvement_ratio_vs_strict=(
                        min_mean_regret_improvement_ratio_vs_strict
                    ),
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "window_index"]
    )


def evaluate_dfl_ua_context_lava_gate(
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
    """Gate UA-context DT/LAVA candidate policy against V2+ and strict."""

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
        frame_name="ua_context_lava_strict_frame",
    )
    summaries = _role_summaries(strict_frame)
    selected = summaries.get(UA_CONTEXT_LAVA_SELECTION_ROLE)
    v2_plus = summaries.get(V2_PLUS_REFERENCE_ROLE)
    strict = summaries.get(STRICT_REFERENCE_ROLE)
    failures: list[str] = []
    validation_count = _tenant_anchor_count(
        strict_frame.filter(pl.col("selection_role") == UA_CONTEXT_LAVA_SELECTION_ROLE)
    )
    if selected is None:
        failures.append("missing UA-context DT/LAVA policy rows")
    if v2_plus is None:
        failures.append("missing V2+ reference rows")
    if strict is None:
        failures.append("missing strict reference rows")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            "UA-context DT/LAVA validation tenant-anchor count below required "
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
        "passed" if passed else "blocked",
        (
            "UA-context DT/LAVA candidate policy passed the frozen V2+ gate."
            if passed
            else "UA-context DT/LAVA candidate policy did not beat frozen V2+."
        ),
        {
            "role_summaries": summaries,
            "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2,
            "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
            "median_not_worse_vs_v2_plus": median_not_worse,
            "safety_ok": safety_ok,
            "diagnostic_signal_passed": diagnostic,
            "market_execution_enabled": False,
        },
    )


def _teacher_class(
    row: dict[str, Any],
    *,
    delta: float,
    lava_tail_risk_class: str | None,
    tail_risk_delta_uah: float,
) -> str:
    if str(row["candidate_source"]) == "v2_plus_default":
        return "fallback_v2_plus"
    if not bool(row["eligible_for_final_selection"]):
        return "oracle_neighbor_diagnostic"
    if lava_tail_risk_class == "tail_risk_switch" or delta >= tail_risk_delta_uah:
        return "avoid_tail_risk_neighbor"
    if lava_tail_risk_class == "safe_switch_win" or bool(row["label_safe_switch_win"]):
        return "safe_schedule_candidate"
    if delta < 0.0:
        return "safe_schedule_candidate"
    return "neutral_schedule_candidate"


def _teacher_loss_weight(teacher_class: str, delta: float) -> float:
    if teacher_class == "safe_schedule_candidate":
        return 1.0 + min(abs(delta), 500.0) / 500.0
    if teacher_class == "avoid_tail_risk_neighbor":
        return 1.5 + min(abs(delta), 500.0) / 250.0
    if teacher_class == "fallback_v2_plus":
        return 0.75
    return 0.5


def _fit_torch_candidate_policy(
    rows: list[dict[str, Any]],
    *,
    feature_names: list[str],
    torch_hidden_size: int,
    torch_max_epochs: int,
    use_cuda_if_available: bool,
    random_seed: int,
) -> dict[str, Any]:
    try:
        import torch
    except ImportError as exc:  # pragma: no cover - torch is a project dependency.
        raise RuntimeError("Torch is required for UA-context DT/LAVA policy.") from exc
    torch.manual_seed(random_seed)
    try:
        torch.use_deterministic_algorithms(True, warn_only=True)
    except TypeError:
        torch.use_deterministic_algorithms(True)
    device = torch.device(
        "cuda" if use_cuda_if_available and torch.cuda.is_available() else "cpu"
    )
    x_values = [[_feature_value(row, name) for name in feature_names] for row in rows]
    y_delta = [[float(row["label_regret_delta_vs_v2_plus_uah"])] for row in rows]
    y_tail = [[float(row["teacher_tail_risk_probability_target"])] for row in rows]
    x_tensor = torch.tensor(x_values, dtype=torch.float32, device=device)
    mean_tensor = torch.mean(x_tensor, dim=0, keepdim=True)
    std_tensor = torch.std(x_tensor, dim=0, keepdim=True, unbiased=False)
    std_tensor = torch.where(std_tensor < 1e-6, torch.ones_like(std_tensor), std_tensor)
    x_norm = (x_tensor - mean_tensor) / std_tensor
    delta_tensor = torch.tensor(y_delta, dtype=torch.float32, device=device)
    tail_tensor = torch.tensor(y_tail, dtype=torch.float32, device=device)
    model = torch.nn.Sequential(
        torch.nn.Linear(len(feature_names), torch_hidden_size),
        torch.nn.ReLU(),
        torch.nn.Linear(torch_hidden_size, 2),
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.03, weight_decay=1e-3)
    for _ in range(torch_max_epochs):
        optimizer.zero_grad(set_to_none=True)
        output = model(x_norm)
        delta_loss = torch.nn.functional.mse_loss(output[:, :1], delta_tensor)
        tail_loss = torch.nn.functional.binary_cross_entropy_with_logits(
            output[:, 1:2],
            tail_tensor,
        )
        loss = delta_loss + tail_loss
        loss.backward()
        optimizer.step()
    with torch.no_grad():
        train_output = model(x_norm)
        train_delta = train_output[:, :1].detach().cpu().flatten().tolist()
        train_tail = torch.sigmoid(train_output[:, 1:2]).detach().cpu().flatten().tolist()
    return {
        "model": model,
        "feature_means": {
            feature_names[index]: float(mean_tensor.detach().cpu()[0, index].item())
            for index in range(len(feature_names))
        },
        "feature_scales": {
            feature_names[index]: float(std_tensor.detach().cpu()[0, index].item())
            for index in range(len(feature_names))
        },
        "feature_names": feature_names,
        "device": device,
        "train_predicted_delta_mean": mean(train_delta) if train_delta else 0.0,
        "train_predicted_tail_mean": mean(train_tail) if train_tail else 0.0,
    }


def _predict_candidate(
    row: dict[str, Any],
    *,
    model: dict[str, Any],
    feature_names: list[str],
    profile_stats: dict[str, dict[str, Any]],
) -> tuple[float, float]:
    import torch

    values = []
    for name in feature_names:
        mean_value = float(model["feature_means"].get(name, 0.0))
        scale_value = float(model["feature_scales"].get(name, 1.0)) or 1.0
        values.append((_feature_value(row, name) - mean_value) / scale_value)
    with torch.no_grad():
        tensor = torch.tensor([values], dtype=torch.float32, device=model["device"])
        output = model["model"](tensor)
        predicted_delta = float(output[0, 0].detach().cpu().item())
        predicted_tail = float(torch.sigmoid(output[0, 1]).detach().cpu().item())
    profile = profile_stats.get(_profile_key(row), {})
    profile_delta = float(profile.get("mean_prior_delta_uah", predicted_delta))
    profile_tail = float(
        profile.get(
            "smoothed_prior_tail_risk_probability",
            1.0 - float(profile.get("safe_precision", 0.0)),
        )
    )
    blended_delta = (predicted_delta + profile_delta) / 2.0
    blended_tail = max(profile_tail, predicted_tail * 0.25)
    return blended_delta, max(0.0, min(1.0, blended_tail))


def _select_final_candidates(
    final_rows: list[dict[str, Any]],
    *,
    model: dict[str, Any],
    feature_names: list[str],
    profile_stats: dict[str, dict[str, Any]],
    allowed_profiles: set[str],
    allowed_candidate_sources: set[str],
    hard_blocked_candidate_families: set[str],
    min_predicted_improvement_uah: float,
    max_predicted_tail_risk_probability: float,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    selected: list[dict[str, Any]] = []
    fallback_keys: list[str] = []
    predicted_rows: list[dict[str, Any]] = []
    for anchor, anchor_rows in sorted(_rows_by_anchor(final_rows).items()):
        scored: list[tuple[dict[str, Any], float, float]] = []
        for row in anchor_rows:
            if str(row["candidate_source"]) not in allowed_candidate_sources:
                continue
            if str(row["candidate_family"]) in hard_blocked_candidate_families:
                continue
            if _profile_key(row) not in allowed_profiles:
                continue
            predicted_delta, predicted_tail = _predict_candidate(
                row,
                model=model,
                feature_names=feature_names,
                profile_stats=profile_stats,
            )
            copied = dict(row)
            copied["predicted_regret_delta_vs_v2_plus_uah"] = predicted_delta
            copied["predicted_tail_risk_probability"] = predicted_tail
            predicted_rows.append(copied)
            scored.append((row, predicted_delta, predicted_tail))
        switchable = [
            (row, predicted_delta, predicted_tail)
            for row, predicted_delta, predicted_tail in scored
            if predicted_delta <= -min_predicted_improvement_uah
            and predicted_tail <= max_predicted_tail_risk_probability
        ]
        if not switchable:
            fallback_keys.append(_anchor_key_from_parts(str(anchor[0]), str(anchor[1]), anchor[2]))
            continue
        selected.append(
            min(
                switchable,
                key=lambda item: (
                    item[2],
                    item[1],
                    _feature_value(item[0], "selector_feature_schedule_distance_from_v2_plus"),
                    str(item[0]["candidate_family"]),
                    str(item[0]["candidate_model_name"]),
                ),
            )[0]
        )
    return selected, fallback_keys, predicted_rows


def _behavior_clone_final_candidates(
    train_rows: list[dict[str, Any]],
    final_rows: list[dict[str, Any]],
    *,
    allowed_candidate_sources: set[str],
    hard_blocked_candidate_families: set[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    safe_profiles = [
        _profile_key(row)
        for row in train_rows
        if str(row["teacher_schedule_candidate_class"]) == "safe_schedule_candidate"
        and str(row["candidate_source"]) in allowed_candidate_sources
        and str(row["candidate_family"]) not in hard_blocked_candidate_families
    ]
    if not safe_profiles:
        return [], [
            _anchor_key_from_parts(str(anchor[0]), str(anchor[1]), anchor[2])
            for anchor in sorted(_rows_by_anchor(final_rows))
        ]
    target_profile = max(sorted(set(safe_profiles)), key=safe_profiles.count)
    selected: list[dict[str, Any]] = []
    fallback: list[str] = []
    for anchor, anchor_rows in sorted(_rows_by_anchor(final_rows).items()):
        matches = [row for row in anchor_rows if _profile_key(row) == target_profile]
        if not matches:
            fallback.append(_anchor_key_from_parts(str(anchor[0]), str(anchor[1]), anchor[2]))
            continue
        selected.append(
            min(
                matches,
                key=lambda row: (
                    _feature_value(row, "selector_feature_schedule_distance_from_v2_plus"),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected, fallback


def _rolling_summary_row(
    window_frame: pl.DataFrame,
    policy: pl.DataFrame,
    *,
    source_model_name: str,
    window_index: int,
    validation_anchors: tuple[datetime, ...],
    prior_anchors: tuple[datetime, ...],
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    min_mean_regret_improvement_ratio_vs_strict: float,
) -> dict[str, Any]:
    rows = list(window_frame.iter_rows(named=True))
    policy_rows = list(policy.iter_rows(named=True))
    by_key = {_candidate_key(row): row for row in rows}
    v2_by_anchor = {
        _anchor_key(row): row
        for row in rows
        if str(row["candidate_source"]) == "v2_plus_default"
        and str(row["split_name"]) == "final_holdout"
    }
    selected_rows: list[dict[str, Any]] = []
    for policy_row in policy_rows:
        for key in policy_row["selected_final_candidate_keys"]:
            selected_rows.append(by_key[str(key)])
        for anchor_key in policy_row["fallback_final_anchor_keys"]:
            selected_rows.append(v2_by_anchor[str(anchor_key)])
    strict_rows = [
        row
        for row in rows
        if str(row["split_name"]) == "final_holdout"
        and str(row["candidate_family"]) == v2.CANDIDATE_FAMILY_STRICT
    ]
    v2_rows = list(v2_by_anchor.values())
    strict_mean = _mean_regret(strict_rows)
    v2_mean = _mean_regret(v2_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    v2_median = _median_regret(v2_rows)
    selected_median = _median_regret(selected_rows)
    improvement_vs_v2 = _improvement_ratio(v2_mean, selected_mean)
    improvement_vs_strict = _improvement_ratio(strict_mean, selected_mean)
    median_not_worse = selected_median <= v2_median
    passed = (
        improvement_vs_v2 >= min_mean_regret_improvement_ratio_vs_v2_plus
        and improvement_vs_strict >= min_mean_regret_improvement_ratio_vs_strict
        and median_not_worse
    )
    return {
        "source_model_name": source_model_name,
        "selection_role": UA_CONTEXT_LAVA_SELECTION_ROLE,
        "window_index": window_index,
        "validation_anchor_count_per_tenant": len(validation_anchors),
        "validation_tenant_anchor_count": _anchor_count(selected_rows),
        "minimum_prior_anchor_count_before_window": len(prior_anchors),
        "strict_mean_regret_uah": strict_mean,
        "v2_plus_mean_regret_uah": v2_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "v2_plus_median_regret_uah": v2_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "median_not_worse_vs_v2_plus": median_not_worse,
        "rolling_window_passed": passed,
        "diagnostic_window_passed": improvement_vs_v2 > 0.0 and median_not_worse,
        "fallback_row_count": sum(
            len(row["fallback_final_anchor_keys"]) for row in policy_rows
        ),
        "selected_candidate_source_counts": _source_counts(selected_rows, []),
        "validation_window_anchor_start": min(validation_anchors).isoformat(),
        "validation_window_anchor_end": max(validation_anchors).isoformat(),
        "target_label_space": "ua_context_schedule_candidate_index",
        "raw_hourly_action_imitation": False,
        "claim_scope": UA_CONTEXT_LAVA_ROBUSTNESS_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _window_sequence_frame(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    prior_anchors: set[datetime],
    validation_anchors: set[datetime],
) -> pl.DataFrame:
    output: list[dict[str, Any]] = []
    for row in rows:
        if str(row["source_model_name"]) != source_model_name:
            continue
        anchor = _datetime_value(row["anchor_timestamp"])
        if anchor in prior_anchors:
            copied = dict(row)
            copied["split_name"] = "train_selection"
            copied["is_training_row"] = (
                bool(row["eligible_for_final_selection"])
                and str(row["teacher_schedule_candidate_class"]) != "oracle_neighbor_diagnostic"
            )
            output.append(copied)
        elif anchor in validation_anchors:
            copied = dict(row)
            copied["split_name"] = "final_holdout"
            copied["is_training_row"] = False
            output.append(copied)
    return pl.DataFrame(output, infer_schema_length=None)


def _profile_stats(
    rows: list[dict[str, Any]],
    *,
    allowed_candidate_sources: set[str],
    hard_blocked_candidate_families: set[str],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row["candidate_source"]) not in allowed_candidate_sources:
            continue
        grouped.setdefault(_profile_key(row), []).append(row)
    output: dict[str, dict[str, Any]] = {}
    for profile, profile_rows in grouped.items():
        safe_count = sum(
            str(row["teacher_schedule_candidate_class"]) == "safe_schedule_candidate"
            for row in profile_rows
        )
        tail_count = sum(
            str(row["teacher_schedule_candidate_class"]) == "avoid_tail_risk_neighbor"
            for row in profile_rows
        )
        safe_precision = safe_count / max(1, safe_count + tail_count)
        deltas = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in profile_rows]
        output[profile] = {
            "row_count": len(profile_rows),
            "safe_win_count": safe_count,
            "tail_loss_count": tail_count,
            "safe_precision": safe_precision,
            "smoothed_prior_tail_risk_probability": (tail_count + 1.0)
            / (safe_count + tail_count + 2.0),
            "mean_prior_delta_uah": mean(deltas) if deltas else 0.0,
            "candidate_source": str(profile_rows[0]["candidate_source"]),
            "candidate_family": str(profile_rows[0]["candidate_family"]),
            "hard_blocked": str(profile_rows[0]["candidate_family"]) in hard_blocked_candidate_families,
        }
    return output


def _allowed_sources(
    train_rows: list[dict[str, Any]],
    configured_sources: set[str],
) -> set[str]:
    if configured_sources:
        return configured_sources
    return {
        str(row["candidate_source"])
        for row in train_rows
        if str(row["candidate_source"]) not in _FALLBACK_SOURCES
    }


def _feature_columns(frame: pl.DataFrame) -> list[str]:
    candidates = [
        column
        for column in frame.columns
        if column.startswith("selector_feature_") and frame[column].dtype.is_numeric()
    ]
    ordered = [column for column in _CORE_POLICY_FEATURES if column in candidates]
    for column in sorted(candidates):
        if column not in ordered:
            ordered.append(column)
    if not ordered:
        raise ValueError("UA-context LAVA policy needs selector_feature_* columns.")
    return ordered


def _feature_value(row: dict[str, Any], name: str) -> float:
    value = row.get(name, 0.0)
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _candidate_priority(row: dict[str, Any]) -> tuple[int, float]:
    source = str(row["candidate_source"])
    if source == "v2_plus_default":
        return (0, 0.0)
    if str(row["candidate_family"]) == v2.CANDIDATE_FAMILY_STRICT:
        return (1, 0.0)
    return (2, float(row["regret_uah"]))


def _lava_tail_risk_lookup(frame: pl.DataFrame | None) -> dict[str, str]:
    if frame is None or frame.is_empty():
        return {}
    rows = frame.iter_rows(named=True)
    lookup: dict[str, str] = {}
    for row in rows:
        key = row.get("teacher_candidate_key")
        if not key:
            key = _candidate_key(row)
        value = row.get("tail_risk_avoidance_class") or row.get(
            "teacher_schedule_neighbor_class"
        )
        if value:
            lookup[str(key)] = str(value)
    return lookup


def _candidate_key(row: dict[str, Any]) -> str:
    anchor = _datetime_value(row["anchor_timestamp"])
    return (
        f"{row['tenant_id']}|{row['source_model_name']}|{anchor.isoformat()}|"
        f"{row['candidate_source']}|{row['candidate_family']}|"
        f"{row['candidate_model_name']}"
    )


def _anchor_key(row: dict[str, Any]) -> str:
    return _anchor_key_from_parts(
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _anchor_key_from_parts(tenant_id: str, source_model_name: str, anchor: datetime) -> str:
    return f"{tenant_id}|{source_model_name}|{anchor.isoformat()}"


def _profile_key(row: dict[str, Any]) -> str:
    return f"{row['candidate_source']}|{row['candidate_family']}"


def _rows_by_anchor(rows: list[dict[str, Any]]) -> dict[tuple[str, str, datetime], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                _datetime_value(row["anchor_timestamp"]),
            ),
            [],
        ).append(row)
    return grouped


def _reference_row(
    row: dict[str, Any],
    *,
    selection_role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    copied = dict(row)
    payload = dict(_payload(row))
    payload.update(
        {
            "selection_role": selection_role,
            "ua_context_lava_role": selection_role,
            "claim_scope": UA_CONTEXT_LAVA_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    copied.update(
        {
            "selection_role": selection_role,
            "strategy_kind": UA_CONTEXT_LAVA_STRICT_LP_STRATEGY_KIND,
            "generated_at": generated_at,
            "claim_scope": UA_CONTEXT_LAVA_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "evaluation_payload": payload,
        }
    )
    return copied


def _candidate_benchmark_row(
    row: dict[str, Any],
    *,
    policy_row: dict[str, Any],
    selection_role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(_payload(row))
    key = _candidate_key(row)
    payload.update(
        {
            "selection_role": selection_role,
            "ua_context_lava_role": selection_role,
            "ua_context_lava_selected": True,
            "selected_candidate_key": key,
            "selected_policy_type": str(policy_row["selected_policy_type"]),
            "predicted_regret_delta_vs_v2_plus_uah": dict(
                policy_row["predicted_final_candidate_deltas"]
            ).get(key),
            "predicted_tail_risk_probability": dict(
                policy_row["predicted_final_tail_risk_probabilities"]
            ).get(key),
            "claim_scope": UA_CONTEXT_LAVA_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:ua-context-lava:{selection_role}:"
            f"{row['source_model_name']}:{row['candidate_family']}:"
            f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": str(row["source_model_name"]),
        "forecast_model_name": str(policy_row["learner_model_name"]),
        "strategy_kind": UA_CONTEXT_LAVA_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _first_or_default(row["soc_fraction_vector"], 0.5),
        "starting_soc_source": "ua_context_lava_sequence_training_frame",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": v2._committed_action(row),
        "committed_power_mw": abs(_first_or_default(row["dispatch_mw_vector"], 0.0)),
        "rank_by_regret": 1,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": int(row["safety_violation_count"]),
        "selection_role": selection_role,
        "selected_candidate_family": str(row["candidate_family"]),
        "selected_candidate_model_name": str(row["candidate_model_name"]),
        "fallback_to_v2_plus": False,
        "claim_scope": UA_CONTEXT_LAVA_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "evaluation_payload": payload,
    }


def _fallback_benchmark_row(
    row: dict[str, Any],
    *,
    policy_row: dict[str, Any],
    selection_role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    copied = _reference_row(row, selection_role=selection_role, generated_at=generated_at)
    payload = dict(copied["evaluation_payload"])
    payload.update(
        {
            "ua_context_lava_selected": False,
            "fallback_to_corrected_v2_plus": True,
            "selector_gate_blocker": str(policy_row["selector_gate_blocker"]),
        }
    )
    copied.update(
        {
            "forecast_model_name": str(policy_row["learner_model_name"]),
            "fallback_to_v2_plus": True,
            "evaluation_payload": payload,
        }
    )
    return copied


def _role_summaries(frame: pl.DataFrame) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for role in sorted(frame["selection_role"].unique().to_list()):
        role_frame = frame.filter(pl.col("selection_role") == role)
        summaries[str(role)] = {
            "rows": role_frame.height,
            "tenant_anchor_count": _tenant_anchor_count(role_frame),
            "mean_regret_uah": float(cast(float, role_frame["regret_uah"].mean())),
            "median_regret_uah": float(cast(float, role_frame["regret_uah"].median())),
            "safety_violation_count": int(role_frame["safety_violation_count"].sum()),
        }
    return summaries


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    return mean([float(row["regret_uah"]) for row in rows]) if rows else float("inf")


def _median_regret(rows: list[dict[str, Any]]) -> float:
    return median([float(row["regret_uah"]) for row in rows]) if rows else float("inf")


def _improvement_ratio(baseline: float, selected: float) -> float:
    if baseline <= 0.0 or baseline == float("inf"):
        return 0.0
    return (baseline - selected) / baseline


def _anchor_count(rows: list[dict[str, Any]]) -> int:
    return len(
        {
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                _datetime_value(row["anchor_timestamp"]),
            )
            for row in rows
        }
    )


def _tenant_anchor_count(frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    return frame.select(["tenant_id", "anchor_timestamp"]).unique().height


def _source_counts(rows: list[dict[str, Any]], fallback_keys: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["candidate_source"])
        counts[key] = counts.get(key, 0) + 1
    if fallback_keys:
        counts["frozen_v2_plus_fallback"] = len(fallback_keys)
    return counts


def _family_counts(rows: list[dict[str, Any]], fallback_keys: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["candidate_family"])
        counts[key] = counts.get(key, 0) + 1
    if fallback_keys:
        counts["frozen_v2_plus_fallback"] = len(fallback_keys)
    return counts


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload", {})
    return dict(payload) if isinstance(payload, dict) else {}


def _first_or_default(value: Any, default: float) -> float:
    if isinstance(value, (list, tuple)) and value:
        return float(value[0])
    return default


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    values = [_datetime_value(value) for value in frame["generated_at"].to_list()]
    return max(values)


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"Expected datetime-compatible value, got {type(value)!r}.")


def _no_market_execution(frame: pl.DataFrame) -> bool:
    if "market_execution_enabled" not in frame.columns:
        return False
    return not any(bool(value) for value in frame["market_execution_enabled"].to_list())


def _require_columns(frame: pl.DataFrame, required: frozenset[str], *, frame_name: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} missing required columns: {missing}")


def _validate_teacher_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        _REQUIRED_TEACHER_INPUT_COLUMNS
        | frozenset(
            {
                "teacher_candidate_key",
                "teacher_candidate_index",
                "teacher_schedule_candidate_class",
                "teacher_return_to_go_delta_uah",
                "teacher_tail_risk_probability_target",
                "target_label_space",
                "raw_hourly_action_imitation",
                "is_training_row",
            }
        ),
        frame_name="ua_context_lava_teacher_frame",
    )
    if any(bool(value) for value in frame["raw_hourly_action_imitation"].to_list()):
        raise ValueError("UA-context LAVA teacher frame must not imitate raw actions.")


def _validate_sequence_frame(frame: pl.DataFrame) -> None:
    _validate_teacher_frame(frame)
    _require_columns(
        frame,
        frozenset(
            {
                "sequence_position",
                "dt_return_to_go_uah",
                "dt_tail_risk_target",
                "dt_candidate_index_target",
            }
        ),
        frame_name="ua_context_lava_sequence_training_frame",
    )


def _validate_policy_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "learner_model_name",
                "selected_final_candidate_keys",
                "fallback_final_anchor_keys",
                "behavior_clone_final_candidate_keys",
                "behavior_clone_fallback_anchor_keys",
                "raw_hourly_action_imitation",
                "market_execution_enabled",
            }
        ),
        frame_name="ua_context_lava_candidate_policy_frame",
    )
    if any(bool(value) for value in frame["raw_hourly_action_imitation"].to_list()):
        raise ValueError("UA-context LAVA policy must not imitate raw actions.")


def _validate_policy_config(
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    min_prior_safe_win_count: int,
    max_prior_tail_loss_count: int,
    min_prior_precision: float,
    min_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_predicted_tail_risk_probability: float,
    torch_hidden_size: int,
    torch_max_epochs: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must not be empty.")
    if not source_model_names:
        raise ValueError("source_model_names must not be empty.")
    if min_prior_safe_win_count < 1:
        raise ValueError("min_prior_safe_win_count must be at least 1.")
    if max_prior_tail_loss_count < 0:
        raise ValueError("max_prior_tail_loss_count must not be negative.")
    if not 0.0 <= min_prior_precision <= 1.0:
        raise ValueError("min_prior_precision must be between 0 and 1.")
    if min_prior_mean_improvement_uah < 0.0:
        raise ValueError("min_prior_mean_improvement_uah must not be negative.")
    if min_predicted_improvement_uah < 0.0:
        raise ValueError("min_predicted_improvement_uah must not be negative.")
    if not 0.0 <= max_predicted_tail_risk_probability <= 1.0:
        raise ValueError("max_predicted_tail_risk_probability must be between 0 and 1.")
    if torch_hidden_size < 1:
        raise ValueError("torch_hidden_size must be positive.")
    if torch_max_epochs < 1:
        raise ValueError("torch_max_epochs must be positive.")


__all__ = [
    "UA_CONTEXT_LAVA_BEHAVIOR_CLONING_SELECTION_ROLE",
    "UA_CONTEXT_LAVA_SELECTION_ROLE",
    "UA_CONTEXT_LAVA_STRICT_LP_STRATEGY_KIND",
    "build_dfl_ua_context_lava_candidate_policy_frame",
    "build_dfl_ua_context_lava_rolling_robustness_frame",
    "build_dfl_ua_context_lava_sequence_training_frame",
    "build_dfl_ua_context_lava_strict_lp_benchmark_frame",
    "build_dfl_ua_context_lava_teacher_frame",
    "evaluate_dfl_ua_context_lava_gate",
]
