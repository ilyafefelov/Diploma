"""Credentialless DT research-shadow sequence data and smoke training.

This module intentionally separates offline academic research from promotable
V13 training. It can use chronological delivery-time splits while publication
receipts are unverified, but every artifact remains non-promotable and
non-executable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
import ast
import importlib.util
import json
from pathlib import Path
from typing import Any, Final

import numpy as np
import polars as pl
import torch
from torch import nn

from smart_arbitrage.decision_transformer.policy import DecisionTransformerPolicy

DT_RESEARCH_SHADOW_SEQUENCE_CLAIM_SCOPE: Final[str] = (
    "dt_research_shadow_sequence_dataset_not_promotable_not_market_execution"
)
DT_RESEARCH_SHADOW_SMOKE_CLAIM_SCOPE: Final[str] = (
    "dt_research_shadow_transformer_smoke_not_promotable_not_market_execution"
)
DT_RESEARCH_SHADOW_EVALUATION_CLAIM_SCOPE: Final[str] = (
    "dt_research_shadow_evaluation_packet_not_promotable_not_market_execution"
)
SPLIT_STRATEGY: Final[str] = "chronological_delivery_timestamp"
REQUIRED_FORECAST_FAMILIES: Final[tuple[str, ...]] = ("nbeatsx", "tft")
SEQUENCE_NPZ_NAME: Final[str] = "dt_research_shadow_sequences.npz"
SEQUENCE_SUMMARY_JSON_NAME: Final[str] = "dt_research_shadow_sequence_summary.json"
SEQUENCE_VALIDATION_JSON_NAME: Final[str] = "dt_research_shadow_sequence_validation.json"
SMOKE_SUMMARY_JSON_NAME: Final[str] = "dt_research_shadow_smoke_summary.json"
SELECTED_PREVIEW_JSON_NAME: Final[str] = (
    "dt_research_shadow_selected_schedule_preview.json"
)
EVALUATION_SUMMARY_JSON_NAME: Final[str] = (
    "dt_research_shadow_evaluation_summary.json"
)
EVALUATION_VALIDATION_JSON_NAME: Final[str] = (
    "dt_research_shadow_evaluation_validation.json"
)
OBJECTIVE_KIND_CROSS_ENTROPY: Final[str] = "cross_entropy_candidate_index"
OBJECTIVE_KIND_DECISION_AWARE: Final[str] = (
    "decision_aware_regret_value_ranking"
)
OBJECTIVE_KIND_V2_PLUS_RULE_DISTILLATION: Final[str] = (
    "v2_plus_rule_distillation"
)
LOSS_FUNCTION_DECISION_AWARE: Final[str] = (
    "cross_entropy_candidate_index_plus_decision_aware_regret_value_ranking"
)
LOSS_FUNCTION_V2_PLUS_RULE_DISTILLATION: Final[str] = (
    "v2_plus_rule_distillation_listwise"
)
DEFAULT_OBJECTIVE_KIND: Final[str] = OBJECTIVE_KIND_DECISION_AWARE
DEFAULT_CROSS_ENTROPY_WEIGHT: Final[float] = 1.0
DEFAULT_DECISION_AWARE_RANKING_WEIGHT: Final[float] = 1.0
DEFAULT_DISTILLATION_WEIGHT: Final[float] = 1.0
DEFAULT_MIN_PREDICTED_IMPROVEMENT_UAH: Final[float] = 50.0
DEFAULT_MAX_FAMILY_TAIL_RISK_PROBABILITY: Final[float] = 0.5

STATE_FEATURE_NAMES: Final[tuple[str, ...]] = (
    "forecast_vector_mean_scaled",
    "forecast_spread_scaled",
    "forecast_top_k_actual_overlap",
    "forecast_bottom_k_actual_overlap",
    "forecast_family_nbeatsx_flag",
    "forecast_family_tft_flag",
    "soc_vector_last",
    "soc_min_slack_fraction",
    "terminal_soc_delta_fraction",
    "throughput_delta_mwh",
    "degradation_penalty_scaled",
    "schedule_value_scaled",
    "regret_delta_vs_v2_plus_scaled",
    "return_to_go_scaled",
    "candidate_index_scaled",
    "candidate_count_scaled",
    "v13_training_permission_flag",
    "publication_receipt_verified_flag",
    "tenant_hash_scaled",
    "source_hash_scaled",
)
STATE_CONTEXT_GROUP_FEATURES: Final[dict[str, tuple[str, ...]]] = {
    "forecast_context": (
        "forecast_vector_mean_scaled",
        "forecast_spread_scaled",
        "forecast_top_k_actual_overlap",
        "forecast_bottom_k_actual_overlap",
        "forecast_family_nbeatsx_flag",
        "forecast_family_tft_flag",
    ),
    "battery_soc_context": (
        "soc_vector_last",
        "soc_min_slack_fraction",
        "terminal_soc_delta_fraction",
        "throughput_delta_mwh",
        "degradation_penalty_scaled",
    ),
    "tenant_context": (
        "tenant_hash_scaled",
        "source_hash_scaled",
    ),
    "candidate_value_regret_context": (
        "schedule_value_scaled",
        "regret_delta_vs_v2_plus_scaled",
        "return_to_go_scaled",
        "candidate_index_scaled",
        "candidate_count_scaled",
    ),
    "gate_context": (
        "v13_training_permission_flag",
        "publication_receipt_verified_flag",
    ),
}
REQUIRED_STATE_CONTEXT_GROUPS: Final[tuple[str, ...]] = (
    "forecast_context",
    "battery_soc_context",
    "tenant_context",
    "candidate_value_regret_context",
    "gate_context",
)
RETURN_TO_GO_TARGET: Final[str] = (
    "negative_regret_delta_vs_v2_plus_or_strict_reference"
)
ACTION_TARGET_VALUE_COLUMNS: Final[tuple[str, ...]] = (
    "dt_candidate_id_target",
    "dt_candidate_index_target",
    "dt_schedule_family_target",
)
REQUIRED_TEACHER_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "forecast_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
        "schedule_value_uah",
        "regret_uah",
        "regret_delta_vs_v2_plus_uah",
        "return_to_go_regret_target_uah",
        "dt_candidate_index_target",
        "dt_candidate_id_target",
        "dt_schedule_family_target",
        "dt_action_target_contract",
        "v2_plus_role",
        "v13_training_permission_gate_passed",
        "permitted_model_training_row",
        "permits_model_training",
        "training_blocker",
        "promotion_gate_passed",
        "market_execution_gate_passed",
        "raw_hourly_action_imitation",
        "not_full_dfl",
        "not_market_execution",
        "market_execution_enabled",
    }
)
CANDIDATE_LIBRARY_RESEARCH_SHADOW_SOURCE_KIND: Final[str] = (
    "credentialless_candidate_library_research_shadow_adapter"
)
CANDIDATE_LIBRARY_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "candidate_family",
        "candidate_model_name",
        "forecast_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
        "decision_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "forecast_spread_uah_mwh",
        "forecast_top_k_actual_overlap",
        "forecast_bottom_k_actual_overlap",
        "soc_min_slack_fraction",
        "safety_violation_count",
        "total_throughput_mwh",
        "total_degradation_penalty_uah",
        "not_full_dfl",
        "not_market_execution",
        "market_execution_enabled",
    }
)
V2_PLUS_STRICT_ROLE_TO_FAMILY: Final[dict[str, str]] = {
    "raw_reference": "raw_reference",
    "schedule_value_learner_v2_plus": "schedule_value_learner_v2_plus",
    "schedule_value_learner_v2_reference": "schedule_value_learner_v2_reference",
    "strict_reference": "strict_reference",
}
V2_PLUS_STRICT_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "anchor_timestamp",
        "generated_at",
        "horizon_hours",
        "starting_soc_fraction",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "selection_role",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
        "evaluation_payload",
    }
)
V2_PLUS_REGRET_DECOMPOSITION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "best_candidate_family",
        "best_candidate_model_name",
        "best_candidate_regret_uah",
        "regret_gap_v2_to_best_candidate_uah",
    }
)


def build_dt_research_shadow_teacher_rows_from_candidate_library(
    *,
    candidate_library_frame: pl.DataFrame,
    source_kind: str = CANDIDATE_LIBRARY_RESEARCH_SHADOW_SOURCE_KIND,
    max_rows: int | None = None,
) -> pl.DataFrame:
    """Adapt credentialless candidate-library rows into DT shadow teacher rows.

    These rows are useful only for offline research-shadow state/action context.
    They never become V13-permitted training rows while publication receipts and
    source-readiness gates remain blocked.
    """

    if not source_kind:
        raise ValueError("source_kind must be non-empty.")
    if max_rows is not None and max_rows <= 0:
        raise ValueError("max_rows must be positive when provided.")
    frame = _normalized_candidate_library_rows(candidate_library_frame)
    if max_rows is not None:
        frame = frame.head(max_rows)

    adapted_rows: list[dict[str, Any]] = []
    for _, group in frame.group_by(
        ["tenant_id", "source_model_name", "anchor_timestamp"],
        maintain_order=True,
    ):
        group_rows = list(
            group.sort(["candidate_family", "candidate_model_name"]).iter_rows(
                named=True,
            )
        )
        reference_regret, reference_name = _candidate_library_reference_regret(
            group_rows
        )
        candidate_count = len(group_rows)
        for candidate_index, row in enumerate(group_rows):
            family = _non_empty_text(row.get("candidate_family"), "candidate_library")
            candidate_model = _non_empty_text(row.get("candidate_model_name"), family)
            regret = _float(row.get("regret_uah"))
            regret_delta = (
                regret - reference_regret if reference_regret is not None else regret
            )
            schedule_value = _float(row.get("decision_value_uah"))
            soc_vector = _vector(row.get("soc_fraction_vector"))
            adapted = dict(row)
            adapted.update(
                {
                    "schedule_value_uah": schedule_value,
                    "regret_delta_vs_v2_plus_uah": regret_delta,
                    "return_to_go_regret_target_uah": -regret_delta,
                    "dt_candidate_index_target": candidate_index,
                    "dt_candidate_id_target": _candidate_library_candidate_id(
                        row=row,
                        candidate_index=candidate_index,
                        family=family,
                        candidate_model=candidate_model,
                    ),
                    "dt_schedule_family_target": family,
                    "dfl_input_contract": (
                        "calibrated_forecasts_tenant_soc_context_feasible_candidate_schedules"
                    ),
                    "dfl_target_contract": (
                        "best_candidate_schedule_value_regret_delta_vs_v2_plus"
                    ),
                    "dt_input_contract": (
                        "research_shadow_sequence_forecast_battery_tenant_candidate_value_return_to_go"
                    ),
                    "dt_action_target_contract": "candidate_id_or_schedule_family",
                    "v2_plus_role": "teacher_comparator_fallback",
                    "v13_training_permission_gate_passed": False,
                    "v13_blocking_context_families": (
                        "explicit_dam_publication_receipts"
                    ),
                    "permitted_model_training_row": False,
                    "permits_model_training": False,
                    "training_blocker": (
                        "v13_training_permission_gate_blocked_research_shadow_candidate_library_adapter"
                    ),
                    "promotion_gate_passed": False,
                    "market_execution_gate_passed": False,
                    "raw_hourly_action_imitation": False,
                    "not_deployed_dt_control": True,
                    "research_shadow_source_kind": source_kind,
                    "research_shadow_reward_reference": reference_name,
                    "adapted_from_candidate_library": True,
                    "publication_receipt_verified": False,
                    "source_publication_timestamp_available": False,
                    "market_availability_claim": False,
                    "research_shadow_not_promotable": True,
                    "teacher_anchor_candidate_count": candidate_count,
                    "selector_feature_forecast_spread_uah_mwh": _float(
                        row.get("forecast_spread_uah_mwh")
                    ),
                    "selector_feature_terminal_soc_delta_fraction": (
                        soc_vector[-1] - soc_vector[0] if len(soc_vector) >= 2 else 0.0
                    ),
                    "selector_feature_total_throughput_delta_mwh": _float(
                        row.get("total_throughput_mwh")
                    ),
                    "selector_feature_total_degradation_penalty_uah": _float(
                        row.get("total_degradation_penalty_uah")
                    ),
                    "market_execution_enabled": False,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
            adapted_rows.append(adapted)
    return pl.DataFrame(adapted_rows)


def build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows(
    *,
    strict_rows_frame: pl.DataFrame,
    regret_decomposition_frame: pl.DataFrame | None = None,
    mirror_training_offset_days: int = 365,
) -> pl.DataFrame:
    """Adapt V2+ strict-row evidence into an apples-to-apples DT shadow table.

    The exported V2+ strict rows are a final-holdout comparison packet, not a
    full historical training table. To let the DT smoke path train while keeping
    the real final-holdout V2+ controls intact, this adapter creates mirrored
    non-promotable train rows dated before the final-holdout rows. The result is
    a comparator-aligned smoke packet, not out-of-sample promotion evidence.
    """

    if mirror_training_offset_days <= 0:
        raise ValueError("mirror_training_offset_days must be positive.")
    frame = _normalized_v2_plus_strict_rows(strict_rows_frame)
    best_labels = _best_available_label_lookup(regret_decomposition_frame)
    role_to_index = {
        role: index for index, role in enumerate(V2_PLUS_STRICT_ROLE_TO_FAMILY)
    }
    adapted_rows: list[dict[str, Any]] = []
    for _, group in frame.group_by(
        ["tenant_id", "source_model_name", "anchor_timestamp"],
        maintain_order=True,
    ):
        rows = list(group.sort("selection_role").iter_rows(named=True))
        v2_row = _role_row(rows, "schedule_value_learner_v2_plus")
        if v2_row is None:
            raise ValueError("V2+ strict rows are missing schedule_value_learner_v2_plus.")
        v2_regret = _float(v2_row.get("regret_uah"))
        candidate_count = len(rows)
        for row in rows:
            role = str(row["selection_role"])
            if role not in V2_PLUS_STRICT_ROLE_TO_FAMILY:
                continue
            anchor = row["anchor_timestamp"]
            if not isinstance(anchor, datetime):
                raise ValueError("anchor_timestamp must parse as datetime.")
            for split_name, anchor_timestamp in (
                (
                    "train_selection",
                    anchor - timedelta(days=mirror_training_offset_days),
                ),
                ("final_holdout", anchor),
            ):
                v2_plus_selected_candidate_index = role_to_index[
                    "schedule_value_learner_v2_plus"
                ]
                v2_plus_selected_candidate_id = _v2_plus_strict_candidate_id(
                    row=v2_row,
                    anchor_timestamp=anchor_timestamp,
                    family=V2_PLUS_STRICT_ROLE_TO_FAMILY[
                        "schedule_value_learner_v2_plus"
                    ],
                    candidate_index=v2_plus_selected_candidate_index,
                )
                adapted_rows.append(
                    _v2_plus_strict_teacher_row(
                        row=row,
                        split_name=split_name,
                        anchor_timestamp=anchor_timestamp,
                        candidate_index=role_to_index[role],
                        candidate_count=candidate_count,
                        family=V2_PLUS_STRICT_ROLE_TO_FAMILY[role],
                        v2_regret=v2_regret,
                        best_label=best_labels.get(_anchor_key(row)),
                        v2_plus_selected_candidate_index=v2_plus_selected_candidate_index,
                        v2_plus_selected_candidate_id=v2_plus_selected_candidate_id,
                    )
                )
    if not adapted_rows:
        raise ValueError("No V2+ strict rows could be adapted for DT shadow.")
    return pl.DataFrame(adapted_rows)


def build_dt_research_shadow_sequence_packet(
    *,
    teacher_rows_frame: pl.DataFrame,
    run_slug: str,
    context_length: int = 8,
    max_sequences: int | None = None,
) -> dict[str, Any]:
    """Build the metadata packet for credentialless DT research-shadow tensors."""

    if context_length <= 0:
        raise ValueError("context_length must be positive.")
    frame = _normalized_teacher_rows(teacher_rows_frame)
    sequences = _sequence_groups(frame, max_sequences=max_sequences)
    split_metadata = _split_metadata(frame)
    dataset_summary = _dataset_summary(
        frame,
        sequence_count=len(sequences),
        context_length=context_length,
    )
    state_contract = _state_feature_contract()
    reward_contract = _reward_target_contract(frame)
    return {
        "run_slug": run_slug,
        "claim_scope": DT_RESEARCH_SHADOW_SEQUENCE_CLAIM_SCOPE,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "state_feature_names": list(STATE_FEATURE_NAMES),
        "dt_state_feature_contract": state_contract,
        "dt_reward_target_contract": reward_contract,
        "context_length": context_length,
        "split_metadata": split_metadata,
        "dataset_summary": dataset_summary,
        "claim_boundary": {
            "minimal_dfl_substrate": True,
            "candidate_schedule_rows": True,
            "strict_lp_oracle_regret_value": True,
            "forecast_context_required_families": list(REQUIRED_FORECAST_FAMILIES),
            "forecast_context_coverage_required_for_full_dt_prototype": True,
            "v2_plus_role": "teacher_comparator_fallback",
            "action_target": "candidate_index_or_schedule_family",
            "action_target_value_columns": list(ACTION_TARGET_VALUE_COLUMNS),
            "action_target_excludes_raw_hourly_buy_sell_hold": True,
            "raw_hourly_buy_sell_hold_action_target": False,
            "publication_receipt_verified": False,
            "source_publication_timestamp_available": False,
            "market_availability_claim": False,
            "research_shadow_not_promotable": True,
            "dt_promotion_gate_passed": False,
            "market_execution_enabled": False,
        },
        "attached_artifacts": {
            "sequence_npz": SEQUENCE_NPZ_NAME,
            "summary_json": SEQUENCE_SUMMARY_JSON_NAME,
            "validation_json": SEQUENCE_VALIDATION_JSON_NAME,
        },
    }


def validate_dt_research_shadow_sequence_packet(
    packet: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the research-shadow sequence packet boundary."""

    failures: list[str] = []
    gate_results: dict[str, dict[str, Any]] = {}
    if packet.get("claim_scope") != DT_RESEARCH_SHADOW_SEQUENCE_CLAIM_SCOPE:
        failures.append("invalid_claim_scope")
    if _contains_market_execution_enabled_true(packet):
        failures.append("nested_market_execution_enabled_true")

    summary = _mapping(packet.get("dataset_summary"))
    split = _mapping(packet.get("split_metadata"))
    boundary = _mapping(packet.get("claim_boundary"))
    state_contract = _mapping(packet.get("dt_state_feature_contract"))
    reward_contract = _mapping(packet.get("dt_reward_target_contract"))
    _add_gate(
        gate_results,
        failures,
        "chronological_split",
        []
        if split.get("split_strategy") == SPLIT_STRATEGY
        and split.get("chronological_split_passed") is True
        else ["chronological_split_not_passed"],
    )
    _add_gate(
        gate_results,
        failures,
        "research_shadow_training_rows",
        []
        if _int(summary.get("research_shadow_training_rows")) > 0
        else ["research_shadow_training_rows_missing"],
    )
    _add_gate(
        gate_results,
        failures,
        "promotable_v13_blocked",
        []
        if _int(summary.get("promotable_v13_permitted_training_rows")) == 0
        and summary.get("v13_training_permission_gate_passed") is False
        and boundary.get("research_shadow_not_promotable") is True
        else ["promotable_v13_training_not_blocked"],
    )
    _add_gate(
        gate_results,
        failures,
        "no_market_execution",
        ["nested_market_execution_enabled_true"]
        if _contains_market_execution_enabled_true(packet)
        else [],
    )
    _add_gate(
        gate_results,
        failures,
        "action_target_contract",
        []
        if boundary.get("action_target") == "candidate_index_or_schedule_family"
        and _sequence_equal(
            boundary.get("action_target_value_columns"),
            list(ACTION_TARGET_VALUE_COLUMNS),
        )
        and boundary.get("action_target_excludes_raw_hourly_buy_sell_hold") is True
        and boundary.get("raw_hourly_buy_sell_hold_action_target") is False
        else ["invalid_action_target_contract"],
    )
    _add_gate(
        gate_results,
        failures,
        "state_feature_contract",
        []
        if state_contract.get("state_contract_passed") is True
        and _sequence_equal(
            state_contract.get("missing_state_context_groups"),
            [],
        )
        else ["state_feature_contract_not_passed"],
    )
    _add_gate(
        gate_results,
        failures,
        "reward_target_contract",
        []
        if reward_contract.get("reward_contract_passed") is True
        and reward_contract.get("return_to_go_target") == RETURN_TO_GO_TARGET
        and reward_contract.get("market_execution_enabled") is False
        else ["reward_target_contract_not_passed"],
    )
    return {
        "claim_scope": "dt_research_shadow_sequence_validation_not_market_execution",
        "passed": not failures,
        "failures": failures,
        "gate_results": gate_results,
        "market_execution_enabled": False,
    }


def validate_dt_research_shadow_evaluation_packet(
    packet: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the standalone DT research-shadow evaluation packet."""

    failures: list[str] = []
    gate_results: dict[str, dict[str, Any]] = {}
    metrics = _mapping(packet.get("evaluation_metrics"))
    deltas = _mapping(packet.get("regret_value_deltas"))
    if packet.get("claim_scope") != DT_RESEARCH_SHADOW_EVALUATION_CLAIM_SCOPE:
        failures.append("invalid_claim_scope")
    if _contains_market_execution_enabled_true(packet):
        failures.append("nested_market_execution_enabled_true")
    _add_gate(
        gate_results,
        failures,
        "regret_value_metrics",
        []
        if packet.get("primary_metric") == "regret_value_vs_strict_v2_plus_behavior"
        and packet.get("accuracy_is_secondary") is True
        and _required_float_metrics_present(
            metrics,
            (
                "dt_selected_mean_regret_uah",
                "dt_selected_mean_value_uah",
                "strict_mean_regret_uah",
                "strict_mean_value_uah",
                "v2_plus_mean_regret_uah",
                "v2_plus_mean_value_uah",
                "behavior_cloning_mean_regret_uah",
                "behavior_cloning_mean_value_uah",
            ),
        )
        and _required_float_metrics_present(
            deltas,
            (
                "dt_minus_strict_regret_uah",
                "dt_minus_v2_plus_regret_uah",
                "dt_minus_behavior_cloning_regret_uah",
                "dt_minus_strict_value_uah",
                "dt_minus_v2_plus_value_uah",
                "dt_minus_behavior_cloning_value_uah",
            ),
        )
        else ["regret_value_metrics_missing_or_invalid"],
    )
    _add_gate(
        gate_results,
        failures,
        "comparison_controls",
        []
        if _sequence_equal(
            packet.get("comparison_controls"),
            [
                "strict_lp_oracle_reference",
                "schedule_value_learner_v2_plus_teacher_comparator_fallback",
                "behavior_cloning_majority_family_reference",
            ],
        )
        else ["comparison_controls_invalid"],
    )
    _add_gate(
        gate_results,
        failures,
        "deterministic_safety_projection",
        []
        if packet.get("deterministic_safety_projection_passed") is True
        else ["deterministic_safety_projection_not_passed"],
    )
    _add_gate(
        gate_results,
        failures,
        "candidate_feasibility_mask",
        []
        if packet.get("candidate_feasibility_mask_attached") is True
        and _float(packet.get("infeasible_action_prediction_count")) == 0.0
        else ["candidate_feasibility_mask_missing_or_invalid"],
    )
    _add_gate(
        gate_results,
        failures,
        "non_promotion",
        []
        if packet.get("dt_promotion_gate_passed") is False
        and packet.get("research_shadow_not_promotable") is True
        else ["dt_promotion_gate_not_blocked"],
    )
    _add_gate(
        gate_results,
        failures,
        "no_market_execution",
        ["nested_market_execution_enabled_true"]
        if _contains_market_execution_enabled_true(packet)
        else [],
    )
    return {
        "claim_scope": "dt_research_shadow_evaluation_validation_not_market_execution",
        "passed": not failures,
        "failures": failures,
        "gate_results": gate_results,
        "market_execution_enabled": False,
    }


def write_dt_research_shadow_sequence_packet(
    *,
    output_dir: str | Path,
    packet: Mapping[str, Any],
    teacher_rows_frame: pl.DataFrame,
) -> dict[str, Path]:
    """Write DT research-shadow sequence tensors and packet metadata."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    context_length = _int(packet.get("context_length"))
    frame = _normalized_teacher_rows(teacher_rows_frame)
    arrays = _sequence_arrays(frame, context_length=context_length)
    npz_path = output_path / SEQUENCE_NPZ_NAME
    np.savez(npz_path, **arrays)
    summary_path = output_path / SEQUENCE_SUMMARY_JSON_NAME
    validation_path = output_path / SEQUENCE_VALIDATION_JSON_NAME
    summary_path.write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    validation_path.write_text(
        json.dumps(
            validate_dt_research_shadow_sequence_packet(packet),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "sequence_npz": npz_path,
        "summary_json": summary_path,
        "validation_json": validation_path,
    }


def run_dt_research_shadow_smoke(
    *,
    sequence_npz_path: str | Path,
    output_dir: str | Path,
    max_epochs: int = 1,
    hidden_dim: int = 32,
    num_layers: int = 1,
    num_heads: int = 2,
    learning_rate: float = 0.01,
    seed: int = 20260525,
    model_backbone: str = "auto",
    objective_kind: str = DEFAULT_OBJECTIVE_KIND,
    cross_entropy_weight: float = DEFAULT_CROSS_ENTROPY_WEIGHT,
    decision_aware_ranking_weight: float = DEFAULT_DECISION_AWARE_RANKING_WEIGHT,
    distillation_weight: float = DEFAULT_DISTILLATION_WEIGHT,
    min_predicted_improvement_uah: float = DEFAULT_MIN_PREDICTED_IMPROVEMENT_UAH,
    max_family_tail_risk_probability: float = (
        DEFAULT_MAX_FAMILY_TAIL_RISK_PROBABILITY
    ),
) -> dict[str, Path]:
    """Train/evaluate a tiny DT research shadow with conservative V2+ fallback."""

    if max_epochs <= 0:
        raise ValueError("max_epochs must be positive for the research-shadow smoke.")
    if cross_entropy_weight < 0.0:
        raise ValueError("cross_entropy_weight must be non-negative.")
    if decision_aware_ranking_weight < 0.0:
        raise ValueError("decision_aware_ranking_weight must be non-negative.")
    if distillation_weight < 0.0:
        raise ValueError("distillation_weight must be non-negative.")
    if min_predicted_improvement_uah < 0.0:
        raise ValueError("min_predicted_improvement_uah must be non-negative.")
    if not 0.0 <= max_family_tail_risk_probability <= 1.0:
        raise ValueError("max_family_tail_risk_probability must be in [0, 1].")
    if objective_kind not in {
        OBJECTIVE_KIND_CROSS_ENTROPY,
        OBJECTIVE_KIND_DECISION_AWARE,
        OBJECTIVE_KIND_V2_PLUS_RULE_DISTILLATION,
    }:
        raise ValueError(
            "objective_kind must be one of: "
            f"{OBJECTIVE_KIND_CROSS_ENTROPY}, {OBJECTIVE_KIND_DECISION_AWARE}, "
            f"{OBJECTIVE_KIND_V2_PLUS_RULE_DISTILLATION}."
        )
    if (
        objective_kind == OBJECTIVE_KIND_V2_PLUS_RULE_DISTILLATION
        and cross_entropy_weight == DEFAULT_CROSS_ENTROPY_WEIGHT
        and decision_aware_ranking_weight == DEFAULT_DECISION_AWARE_RANKING_WEIGHT
        and distillation_weight == DEFAULT_DISTILLATION_WEIGHT
    ):
        cross_entropy_weight = 0.0
        decision_aware_ranking_weight = 0.0

    objective_config = _training_objective_config(
        objective_kind=objective_kind,
        cross_entropy_weight=cross_entropy_weight,
        decision_aware_ranking_weight=decision_aware_ranking_weight,
        distillation_weight=distillation_weight,
    )
    selection_policy = _selection_policy_config(
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
    )

    torch.manual_seed(seed)
    npz = np.load(sequence_npz_path, allow_pickle=True)
    states = torch.tensor(npz["states"], dtype=torch.float32)
    actions = torch.tensor(npz["actions"], dtype=torch.long)
    returns_to_go = torch.tensor(npz["returns_to_go"], dtype=torch.float32)
    candidate_mask = torch.tensor(npz["candidate_mask"], dtype=torch.bool)
    action_feasibility_mask = torch.tensor(
        npz["action_feasibility_mask"],
        dtype=torch.bool,
    )
    split_names = npz["split_names"].astype(str)
    action_dim = int(npz["action_dim"].item())
    _ensure_action_targets_feasible(
        actions=actions,
        candidate_mask=candidate_mask,
        action_feasibility_mask=action_feasibility_mask,
    )

    train_indices = np.flatnonzero(split_names == "train")
    eval_indices = np.flatnonzero(split_names == "evaluation")
    if len(train_indices) == 0:
        raise ValueError("DT research-shadow smoke requires train sequences.")
    if len(eval_indices) == 0:
        eval_indices = train_indices
    hf_available, hf_reason = _hf_decision_transformer_status()
    selected_backbone, backbone_reason = _select_model_backbone(
        requested=model_backbone,
        hf_available=hf_available,
        hf_reason=hf_reason,
    )

    if selected_backbone == "huggingface_decision_transformer_model":
        model = _build_hf_decision_transformer_model(
            state_dim=states.shape[-1],
            action_dim=action_dim,
            hidden_dim=hidden_dim,
            context_length=states.shape[1],
            num_layers=num_layers,
            num_heads=num_heads,
        )
    else:
        model = DecisionTransformerPolicy(
            state_dim=states.shape[-1],
            action_dim=action_dim,
            hidden_dim=hidden_dim,
            context_length=states.shape[1],
            num_layers=num_layers,
            num_heads=num_heads,
        )
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    loss_function = nn.CrossEntropyLoss(ignore_index=-100)
    previous_actions = _previous_action_one_hot(actions, action_dim=action_dim)
    train_loss_first = 0.0
    train_loss_last = 0.0
    train_ce_loss_first = 0.0
    train_ce_loss_last = 0.0
    train_decision_aware_loss_first = 0.0
    train_decision_aware_loss_last = 0.0
    train_distillation_loss_first = 0.0
    train_distillation_loss_last = 0.0
    model.train()
    for epoch in range(max_epochs):
        optimizer.zero_grad()
        logits = _dt_policy_logits(
            model=model,
            selected_backbone=selected_backbone,
            states=states[train_indices],
            previous_actions=previous_actions[train_indices],
            returns_to_go=returns_to_go[train_indices],
        )
        logits = _mask_infeasible_action_logits(
            logits,
            action_feasibility_mask[train_indices],
        )
        loss = loss_function(
            logits.reshape(-1, action_dim),
            _masked_action_targets(
                actions[train_indices],
                candidate_mask[train_indices],
            ).reshape(-1),
        )
        decision_aware_ranking_loss = torch.zeros_like(loss)
        distillation_rule_loss = torch.zeros_like(loss)
        if objective_kind == OBJECTIVE_KIND_DECISION_AWARE:
            decision_aware_ranking_loss = _decision_aware_ranking_loss(
                logits=logits,
                actions=actions[train_indices],
                candidate_mask=candidate_mask[train_indices],
                candidate_regret=torch.tensor(
                    npz["candidate_regret_uah"][train_indices],
                    dtype=torch.float32,
                ),
                candidate_value=torch.tensor(
                    npz["candidate_value_uah"][train_indices],
                    dtype=torch.float32,
                ),
                is_v2_plus=torch.tensor(
                    npz["is_v2_plus_reference"][train_indices],
                    dtype=torch.bool,
                ),
            )
        if objective_kind == OBJECTIVE_KIND_V2_PLUS_RULE_DISTILLATION:
            distillation_rule_loss = _v2_plus_rule_distillation_loss(
                logits=logits,
                actions=actions[train_indices],
                candidate_mask=candidate_mask[train_indices],
                target_mask=torch.tensor(
                    npz["v2_plus_rule_distillation_target_mask"][train_indices],
                    dtype=torch.bool,
                ),
            )
        weighted_loss = (
            cross_entropy_weight * loss
            + decision_aware_ranking_weight * decision_aware_ranking_loss
            + distillation_weight * distillation_rule_loss
        )
        if epoch == 0:
            train_loss_first = float(weighted_loss.detach().item())
            train_ce_loss_first = float(loss.detach().item())
            train_decision_aware_loss_first = float(
                decision_aware_ranking_loss.detach().item()
            )
            train_distillation_loss_first = float(
                distillation_rule_loss.detach().item()
            )
        train_loss_last = float(weighted_loss.detach().item())
        train_ce_loss_last = float(loss.detach().item())
        train_decision_aware_loss_last = float(
            decision_aware_ranking_loss.detach().item()
        )
        train_distillation_loss_last = float(
            distillation_rule_loss.detach().item()
        )
        weighted_loss.backward()
        optimizer.step()
    model.eval()
    with torch.no_grad():
        train_logits = _dt_policy_logits(
            model=model,
            selected_backbone=selected_backbone,
            states=states[train_indices],
            previous_actions=previous_actions[train_indices],
            returns_to_go=returns_to_go[train_indices],
        )
        train_logits = _mask_infeasible_action_logits(
            train_logits,
            action_feasibility_mask[train_indices],
        )
        eval_logits = _dt_policy_logits(
            model=model,
            selected_backbone=selected_backbone,
            states=states[eval_indices],
            previous_actions=previous_actions[eval_indices],
            returns_to_go=returns_to_go[eval_indices],
        )
        eval_logits = _mask_infeasible_action_logits(
            eval_logits,
            action_feasibility_mask[eval_indices],
        )
        eval_loss = loss_function(
            eval_logits.reshape(-1, action_dim),
            _masked_action_targets(
                actions[eval_indices],
                candidate_mask[eval_indices],
            ).reshape(-1),
        )
    eval_decision_aware_ranking_loss = torch.zeros_like(eval_loss)
    eval_distillation_loss = torch.zeros_like(eval_loss)
    if objective_kind == OBJECTIVE_KIND_DECISION_AWARE:
        eval_decision_aware_ranking_loss = _decision_aware_ranking_loss(
            logits=eval_logits,
            actions=actions[eval_indices],
            candidate_mask=candidate_mask[eval_indices],
            candidate_regret=torch.tensor(
                npz["candidate_regret_uah"][eval_indices],
                dtype=torch.float32,
            ),
            candidate_value=torch.tensor(
                npz["candidate_value_uah"][eval_indices],
                dtype=torch.float32,
            ),
            is_v2_plus=torch.tensor(
                npz["is_v2_plus_reference"][eval_indices],
                dtype=torch.bool,
            ),
        )
    if objective_kind == OBJECTIVE_KIND_V2_PLUS_RULE_DISTILLATION:
        eval_distillation_loss = _v2_plus_rule_distillation_loss(
            logits=eval_logits,
            actions=actions[eval_indices],
            candidate_mask=candidate_mask[eval_indices],
            target_mask=torch.tensor(
                npz["v2_plus_rule_distillation_target_mask"][eval_indices],
                dtype=torch.bool,
            ),
        )
    metrics, selected_rows = _evaluation_metrics_and_preview_rows(
        npz=npz,
        eval_indices=eval_indices,
        eval_logits=eval_logits.detach().cpu().numpy(),
        train_indices=train_indices,
        train_logits=train_logits.detach().cpu().numpy(),
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
    )
    metrics["eval_cross_entropy_loss"] = float(eval_loss.detach().item())
    metrics["eval_decision_aware_ranking_loss"] = float(
        eval_decision_aware_ranking_loss.detach().item()
    )
    metrics["eval_weighted_objective_loss"] = float(
        cross_entropy_weight * eval_loss.detach().item()
        + decision_aware_ranking_weight
        * eval_decision_aware_ranking_loss.detach().item()
        + distillation_weight * eval_distillation_loss.detach().item()
    )
    metrics["eval_distillation_loss"] = float(eval_distillation_loss.detach().item())
    selected_preview_packet = _dt_research_shadow_selected_preview_packet(
        selected_rows=selected_rows,
        metrics=metrics,
    )
    summary = {
        "claim_scope": DT_RESEARCH_SHADOW_SMOKE_CLAIM_SCOPE,
        "requested_model_backbone": model_backbone,
        "model_backbone": selected_backbone,
        "model_backbone_selection_reason": backbone_reason,
        "hf_transformers_available": hf_available,
        "hf_decision_transformer_available": hf_available,
        "hf_decision_transformer_status": hf_reason,
        "loss_function": (
            LOSS_FUNCTION_DECISION_AWARE
            if objective_kind == OBJECTIVE_KIND_DECISION_AWARE
            else (
                LOSS_FUNCTION_V2_PLUS_RULE_DISTILLATION
                if objective_kind == OBJECTIVE_KIND_V2_PLUS_RULE_DISTILLATION
                else OBJECTIVE_KIND_CROSS_ENTROPY
            )
        ),
        "training_objective": objective_config,
        "selection_policy": selection_policy,
        "dt_tensor_contract": {
            "states_shape": list(states.shape),
            "actions_shape": list(actions.shape),
            "candidate_id_targets_shape": list(npz["candidate_id_targets"].shape),
            "schedule_family_targets_shape": list(
                npz["schedule_family_targets"].shape
            ),
            "candidate_id_targets_attached": True,
            "schedule_family_targets_attached": True,
            "returns_to_go_shape": list(returns_to_go.shape),
            "candidate_mask_shape": list(npz["candidate_mask"].shape),
            "candidate_mask_attached": True,
            "feasibility_mask_name": "candidate_mask",
            "action_feasibility_mask_shape": list(npz["action_feasibility_mask"].shape),
            "action_feasibility_mask_attached": True,
            "action_feasibility_mask_applied_to_loss": True,
            "action_feasibility_mask_applied_to_eval": True,
            "state_contract_passed": bool(npz["state_contract_passed"].item()),
            "state_context_groups": [
                str(value) for value in npz["state_context_groups"].tolist()
            ],
            "reward_contract_passed": bool(npz["reward_contract_passed"].item()),
            "return_to_go_target": str(npz["return_to_go_target"].item()),
            "action_target": "candidate_index_or_schedule_family",
            "raw_hourly_buy_sell_hold_action_target": False,
            "market_execution_enabled": False,
        },
        "train_sequence_count": int(len(train_indices)),
        "evaluation_sequence_count": int(len(eval_indices)),
        "research_shadow_training_rows": int(npz["research_shadow_training_rows"].item()),
        "promotable_v13_permitted_training_rows": int(
            npz["promotable_v13_permitted_training_rows"].item()
        ),
        "train_loss_first": train_loss_first,
        "train_loss_last": train_loss_last,
        "train_cross_entropy_loss_first": train_ce_loss_first,
        "train_cross_entropy_loss_last": train_ce_loss_last,
        "train_decision_aware_ranking_loss_first": train_decision_aware_loss_first,
        "train_decision_aware_ranking_loss_last": train_decision_aware_loss_last,
        "train_distillation_loss_first": train_distillation_loss_first,
        "train_distillation_loss_last": train_distillation_loss_last,
        "comparison_controls": [
            "strict_lp_oracle_reference",
            "schedule_value_learner_v2_plus_teacher_comparator_fallback",
            "behavior_cloning_majority_family_reference",
        ],
        "evaluation_metrics": metrics,
        "deterministic_safety_projection_passed": bool(
            np.max(npz["candidate_safety_violation_count"]) == 0
        ),
        "dt_promotion_gate_passed": False,
        "market_execution_enabled": False,
    }
    evaluation_packet = _dt_research_shadow_evaluation_packet(summary)
    evaluation_validation = validate_dt_research_shadow_evaluation_packet(
        evaluation_packet
    )
    summary["evaluation_packet_summary"] = {
        "claim_scope": evaluation_packet["claim_scope"],
        "primary_metric": evaluation_packet["primary_metric"],
        "summary_json": EVALUATION_SUMMARY_JSON_NAME,
        "validation_json": EVALUATION_VALIDATION_JSON_NAME,
        "validation_passed": bool(evaluation_validation["passed"]),
        "market_execution_enabled": False,
    }
    summary["attached_artifacts"] = {
        "smoke_summary_json": SMOKE_SUMMARY_JSON_NAME,
        "evaluation_summary_json": EVALUATION_SUMMARY_JSON_NAME,
        "selected_preview_json": SELECTED_PREVIEW_JSON_NAME,
    }
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    summary_path = output_path / SMOKE_SUMMARY_JSON_NAME
    evaluation_path = output_path / EVALUATION_SUMMARY_JSON_NAME
    evaluation_validation_path = output_path / EVALUATION_VALIDATION_JSON_NAME
    selected_preview_path = output_path / SELECTED_PREVIEW_JSON_NAME
    summary_path.write_text(
        json.dumps(_jsonable(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    evaluation_path.write_text(
        json.dumps(_jsonable(evaluation_packet), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    evaluation_validation_path.write_text(
        json.dumps(
            evaluation_validation,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    selected_preview_path.write_text(
        json.dumps(_jsonable(selected_preview_packet), indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    return {
        "summary_json": summary_path,
        "evaluation_summary_json": evaluation_path,
        "evaluation_validation_json": evaluation_validation_path,
        "selected_preview_json": selected_preview_path,
    }


def _normalized_candidate_library_rows(frame: pl.DataFrame) -> pl.DataFrame:
    missing = sorted(CANDIDATE_LIBRARY_REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"DT research-shadow candidate library missing columns: {missing}")
    if frame.height == 0:
        raise ValueError("DT research-shadow candidate library cannot be empty.")
    if _frame_has_true(frame, "market_execution_enabled"):
        raise ValueError("DT research-shadow refuses market execution candidate rows.")
    if not _frame_all_true(frame, "not_full_dfl"):
        raise ValueError("Candidate-library rows must keep not_full_dfl=true.")
    if not _frame_all_true(frame, "not_market_execution"):
        raise ValueError("Candidate-library rows must keep not_market_execution=true.")
    return frame.with_columns(
        pl.col("anchor_timestamp").cast(pl.Datetime, strict=False),
        pl.col("split_name").cast(pl.String),
        pl.col("candidate_family").cast(pl.String),
        pl.col("candidate_model_name").cast(pl.String),
        pl.lit(False).alias("market_execution_enabled"),
        pl.lit(True).alias("not_full_dfl"),
        pl.lit(True).alias("not_market_execution"),
    ).sort(["tenant_id", "source_model_name", "anchor_timestamp"])


def _normalized_v2_plus_strict_rows(frame: pl.DataFrame) -> pl.DataFrame:
    missing = sorted(V2_PLUS_STRICT_REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"V2+ strict rows missing columns: {missing}")
    if frame.height == 0:
        raise ValueError("V2+ strict rows cannot be empty.")
    if _frame_has_true(frame, "market_execution_enabled"):
        raise ValueError("V2+ strict-row DT shadow refuses market execution rows.")
    if not _frame_all_true(frame, "not_full_dfl"):
        raise ValueError("V2+ strict rows must keep not_full_dfl=true.")
    if not _frame_all_true(frame, "not_market_execution"):
        raise ValueError("V2+ strict rows must keep not_market_execution=true.")
    allowed_roles = set(V2_PLUS_STRICT_ROLE_TO_FAMILY)
    adapted = frame.with_columns(
        pl.col("anchor_timestamp").cast(pl.Datetime, strict=False),
        pl.col("selection_role").cast(pl.String),
        pl.lit(False).alias("market_execution_enabled"),
        pl.lit(True).alias("not_full_dfl"),
        pl.lit(True).alias("not_market_execution"),
    ).filter(pl.col("selection_role").is_in(sorted(allowed_roles)))
    if adapted.height == 0:
        raise ValueError("V2+ strict rows contain no supported selection roles.")
    return adapted.sort(["tenant_id", "source_model_name", "anchor_timestamp"])


def _best_available_label_lookup(
    regret_decomposition_frame: pl.DataFrame | None,
) -> dict[tuple[str, str, str], dict[str, Any]]:
    if regret_decomposition_frame is None:
        return {}
    missing = sorted(
        V2_PLUS_REGRET_DECOMPOSITION_COLUMNS - set(regret_decomposition_frame.columns)
    )
    if missing:
        raise ValueError(f"V2+ regret decomposition rows missing columns: {missing}")
    lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    frame = regret_decomposition_frame.with_columns(
        pl.col("anchor_timestamp").cast(pl.Datetime, strict=False),
    )
    for row in frame.iter_rows(named=True):
        lookup[_anchor_key(row)] = {
            "best_available_candidate_family": _non_empty_text(
                row.get("best_candidate_family"),
                "unknown_best_candidate",
            ),
            "best_available_candidate_model_name": _non_empty_text(
                row.get("best_candidate_model_name"),
                "unknown_best_candidate_model",
            ),
            "best_available_candidate_regret_uah": _float(
                row.get("best_candidate_regret_uah")
            ),
            "best_available_regret_gap_vs_v2_plus_uah": _float(
                row.get("regret_gap_v2_to_best_candidate_uah")
            ),
        }
    return lookup


def _v2_plus_strict_teacher_row(
    *,
    row: Mapping[str, Any],
    split_name: str,
    anchor_timestamp: datetime,
    candidate_index: int,
    candidate_count: int,
    family: str,
    v2_regret: float,
    best_label: Mapping[str, Any] | None,
    v2_plus_selected_candidate_index: int,
    v2_plus_selected_candidate_id: str,
) -> dict[str, Any]:
    payload = _json_mapping(row.get("evaluation_payload"))
    horizon = _list_value(payload.get("horizon"))
    forecast_vector = [
        _float(_mapping(item).get("forecast_price_uah_mwh")) for item in horizon
    ]
    actual_vector = [
        _float(_mapping(item).get("actual_price_uah_mwh")) for item in horizon
    ]
    dispatch_vector = [_float(_mapping(item).get("net_power_mw")) for item in horizon]
    if not forecast_vector:
        forecast_vector = [_float(row.get("forecast_objective_value_uah"))]
    if not actual_vector:
        actual_vector = [_float(row.get("oracle_value_uah"))]
    if not dispatch_vector:
        dispatch_vector = [0.0 for _ in forecast_vector]
    starting_soc = _float(row.get("starting_soc_fraction"), fallback=0.52)
    soc_vector = [starting_soc for _ in forecast_vector]
    forecast_diagnostics = _mapping(payload.get("forecast_diagnostics"))
    regret = _float(row.get("regret_uah"))
    regret_delta = regret - v2_regret
    label = dict(best_label or {})
    candidate_model_name = _non_empty_text(row.get("forecast_model_name"), family)
    label_is_v2_plus_selected_candidate = (
        family == "schedule_value_learner_v2_plus"
    )
    return {
        "tenant_id": _non_empty_text(row.get("tenant_id"), "unknown_tenant"),
        "source_model_name": _non_empty_text(
            row.get("source_model_name"),
            "unknown_source",
        ),
        "anchor_timestamp": anchor_timestamp,
        "generated_at": row.get("generated_at"),
        "split_name": split_name,
        "horizon_hours": _int(row.get("horizon_hours")),
        "candidate_family": family,
        "candidate_model_name": candidate_model_name,
        "forecast_price_uah_mwh_vector": forecast_vector,
        "actual_price_uah_mwh_vector": actual_vector,
        "dispatch_mw_vector": dispatch_vector,
        "soc_fraction_vector": soc_vector,
        "decision_value_uah": _float(row.get("decision_value_uah")),
        "forecast_objective_value_uah": _float(row.get("forecast_objective_value_uah")),
        "oracle_value_uah": _float(row.get("oracle_value_uah")),
        "regret_uah": regret,
        "regret_ratio": _float(row.get("regret_ratio")),
        "total_degradation_penalty_uah": _float(
            row.get("total_degradation_penalty_uah")
        ),
        "total_throughput_mwh": _float(row.get("total_throughput_mwh")),
        "forecast_spread_uah_mwh": _spread(forecast_vector),
        "actual_spread_uah_mwh": _spread(actual_vector),
        "forecast_top_k_actual_overlap": _float(
            forecast_diagnostics.get("top_k_price_recall")
        ),
        "forecast_bottom_k_actual_overlap": _float(
            payload.get("forecast_bottom_k_actual_overlap")
        ),
        "soc_min_slack_fraction": min(
            abs(starting_soc - 0.05),
            abs(0.95 - starting_soc),
        ),
        "safety_violation_count": _int(row.get("safety_violation_count")),
        "data_quality_tier": _non_empty_text(row.get("data_quality_tier"), "unknown"),
        "observed_coverage_ratio": _float(row.get("observed_coverage_ratio")),
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": "dt_v2_plus_strict_rows_apples_to_apples_shadow",
        "candidate_library_version": "v2_plus_strict_rows_export",
        "candidate_source": "v2_plus_strict_rows_apples_to_apples_adapter",
        "eligible_for_final_selection": False,
        "analysis_only": True,
        "label_regret_delta_vs_v2_plus_uah": regret_delta,
        "label_beats_v2_plus": regret_delta < 0.0,
        "label_is_v2_plus_selected_candidate": label_is_v2_plus_selected_candidate,
        "label_v2_plus_selected_candidate_index": v2_plus_selected_candidate_index,
        "label_v2_plus_selected_candidate_id": v2_plus_selected_candidate_id,
        "label_v2_plus_rule_distillation_target": (
            label_is_v2_plus_selected_candidate
        ),
        "market_execution_enabled": False,
        "label_safe_switch_win": regret_delta < 0.0,
        "label_tail_risk_loss": False,
        "raw_hourly_action_imitation": False,
        "teacher_candidate_key": _v2_plus_strict_candidate_id(
            row=row,
            anchor_timestamp=anchor_timestamp,
            family=family,
            candidate_index=candidate_index,
        ),
        "teacher_candidate_index": candidate_index,
        "teacher_anchor_candidate_count": candidate_count,
        "teacher_schedule_candidate_class": family,
        "teacher_target_family": family,
        "teacher_target_source": "real_v2_plus_strict_rows",
        "teacher_return_to_go_delta_uah": -regret_delta,
        "teacher_tail_risk_penalty_uah": 0.0,
        "teacher_tail_risk_probability_target": 0.0,
        "teacher_loss_weight": 1.0,
        "is_training_row": split_name == "train_selection",
        "lava_tail_risk_avoidance_class": "not_lava_training",
        "target_label_space": "candidate_index_or_schedule_family",
        "sequence_position": candidate_index,
        "dt_return_to_go_uah": -regret_delta,
        "dt_tail_risk_target": 0.0,
        "dt_candidate_index_target": candidate_index,
        "dt_candidate_family_target": family,
        "dt_candidate_id_target": _v2_plus_strict_candidate_id(
            row=row,
            anchor_timestamp=anchor_timestamp,
            family=family,
            candidate_index=candidate_index,
        ),
        "dt_schedule_family_target": family,
        "return_to_go_regret_target_uah": -regret_delta,
        "regret_delta_vs_v2_plus_uah": regret_delta,
        "schedule_value_uah": _float(row.get("decision_value_uah")),
        "dfl_input_contract": (
            "v2_plus_strict_rows_forecast_battery_candidate_schedule_context"
        ),
        "dfl_target_contract": "best_candidate_schedule_value_regret_delta_vs_v2_plus",
        "dt_input_contract": (
            "v2_plus_strict_rows_sequence_forecast_candidate_value_return_to_go"
        ),
        "dt_action_target_contract": "candidate_id_or_schedule_family",
        "v2_plus_role": "real_schedule_value_learner_v2_plus_comparator",
        "v13_training_permission_gate_passed": False,
        "v13_blocking_context_families": "explicit_dam_publication_receipts",
        "permitted_model_training_row": False,
        "permits_model_training": False,
        "training_blocker": "apples_to_apples_research_shadow_not_v13_training",
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "not_deployed_dt_control": True,
        "research_shadow_source_kind": (
            "v2_plus_strict_rows_mirrored_training_adapter"
        ),
        "research_shadow_reward_reference": "real_v2_plus_strict_rows_comparator",
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "market_availability_claim": False,
        "research_shadow_not_promotable": True,
        **label,
    }


def _role_row(
    rows: Sequence[Mapping[str, Any]],
    selection_role: str,
) -> Mapping[str, Any] | None:
    for row in rows:
        if str(row.get("selection_role")) == selection_role:
            return row
    return None


def _anchor_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        _non_empty_text(row.get("tenant_id"), ""),
        _non_empty_text(row.get("source_model_name"), ""),
        _iso_datetime(row.get("anchor_timestamp")),
    )


def _json_mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return {}


def _spread(values: Sequence[float]) -> float:
    return max(values) - min(values) if values else 0.0


def _v2_plus_strict_candidate_id(
    *,
    row: Mapping[str, Any],
    anchor_timestamp: datetime,
    family: str,
    candidate_index: int,
) -> str:
    return "|".join(
        [
            _non_empty_text(row.get("tenant_id"), "unknown_tenant"),
            _non_empty_text(row.get("source_model_name"), "unknown_source"),
            _iso_datetime(anchor_timestamp),
            family,
            str(candidate_index),
        ]
    )


def _dt_research_shadow_evaluation_packet(
    smoke_summary: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = _mapping(smoke_summary.get("evaluation_metrics"))
    dt_regret = _float(metrics.get("dt_selected_mean_regret_uah"))
    strict_regret = _float(metrics.get("strict_mean_regret_uah"))
    v2_regret = _float(metrics.get("v2_plus_mean_regret_uah"))
    behavior_regret = _float(metrics.get("behavior_cloning_mean_regret_uah"))
    dt_value = _float(metrics.get("dt_selected_mean_value_uah"))
    strict_value = _float(metrics.get("strict_mean_value_uah"))
    v2_value = _float(metrics.get("v2_plus_mean_value_uah"))
    behavior_value = _float(metrics.get("behavior_cloning_mean_value_uah"))
    return {
        "claim_scope": DT_RESEARCH_SHADOW_EVALUATION_CLAIM_SCOPE,
        "primary_metric": "regret_value_vs_strict_v2_plus_behavior",
        "accuracy_is_secondary": True,
        "comparison_controls": [
            str(control)
            for control in _list_value(smoke_summary.get("comparison_controls"))
        ],
        "evaluation_metrics": dict(metrics),
        "regret_value_deltas": {
            "dt_minus_strict_regret_uah": dt_regret - strict_regret,
            "dt_minus_v2_plus_regret_uah": dt_regret - v2_regret,
            "dt_minus_behavior_cloning_regret_uah": dt_regret - behavior_regret,
            "dt_minus_strict_value_uah": dt_value - strict_value,
            "dt_minus_v2_plus_value_uah": dt_value - v2_value,
            "dt_minus_behavior_cloning_value_uah": dt_value - behavior_value,
        },
        "deterministic_safety_projection_passed": bool(
            smoke_summary.get("deterministic_safety_projection_passed", False)
        ),
        "candidate_mask_attached": bool(
            _mapping(smoke_summary.get("dt_tensor_contract")).get(
                "candidate_mask_attached",
                False,
            )
        ),
        "candidate_feasibility_mask_attached": bool(
            _mapping(smoke_summary.get("dt_tensor_contract")).get(
                "action_feasibility_mask_attached",
                False,
            )
        ),
        "infeasible_action_prediction_count": int(
            _float(metrics.get("infeasible_action_prediction_count"))
        ),
        "action_target": "candidate_index_or_schedule_family",
        "raw_hourly_buy_sell_hold_action_target": False,
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "market_availability_claim": False,
        "research_shadow_not_promotable": True,
        "dt_promotion_gate_passed": False,
        "market_execution_enabled": False,
    }


def _hf_decision_transformer_status() -> tuple[bool, str]:
    if importlib.util.find_spec("transformers") is None:
        return False, "transformers_not_installed"
    try:
        from transformers import (
            DecisionTransformerConfig as _DecisionTransformerConfig,
            DecisionTransformerModel as _DecisionTransformerModel,
        )
    except Exception as exc:  # pragma: no cover - depends on optional package state.
        return (
            False,
            f"transformers_decision_transformer_unavailable:{exc.__class__.__name__}",
        )
    del _DecisionTransformerConfig, _DecisionTransformerModel
    return True, "hf_decision_transformer_importable"


def _select_model_backbone(
    *,
    requested: str,
    hf_available: bool,
    hf_reason: str,
) -> tuple[str, str]:
    normalized = requested.strip().casefold()
    if normalized not in {"auto", "local", "hf"}:
        raise ValueError("model_backbone must be one of: auto, local, hf.")
    if normalized == "local":
        return "local_dt_compatible_transformer_classifier", "local_requested"
    if normalized == "hf":
        if not hf_available:
            raise ValueError(
                f"Hugging Face DecisionTransformer requested but unavailable: {hf_reason}"
            )
        return "huggingface_decision_transformer_model", "hf_requested"
    if hf_available:
        return "huggingface_decision_transformer_model", "hf_available_auto_selected"
    return "local_dt_compatible_transformer_classifier", hf_reason


def _build_hf_decision_transformer_model(
    *,
    state_dim: int,
    action_dim: int,
    hidden_dim: int,
    context_length: int,
    num_layers: int,
    num_heads: int,
) -> nn.Module:
    from transformers import (
        DecisionTransformerConfig,
        DecisionTransformerModel,
    )

    config = DecisionTransformerConfig(
        state_dim=state_dim,
        act_dim=action_dim,
        hidden_size=hidden_dim,
        max_ep_len=context_length,
        n_layer=num_layers,
        n_head=num_heads,
        action_tanh=False,
    )
    return DecisionTransformerModel(config)


def _dt_policy_logits(
    *,
    model: nn.Module,
    selected_backbone: str,
    states: torch.Tensor,
    previous_actions: torch.Tensor,
    returns_to_go: torch.Tensor,
) -> torch.Tensor:
    if selected_backbone == "huggingface_decision_transformer_model":
        batch_size, context_length = states.shape[:2]
        timesteps = torch.arange(
            context_length,
            device=states.device,
            dtype=torch.long,
        ).unsqueeze(0).expand(batch_size, -1)
        attention_mask = torch.ones(
            (batch_size, context_length),
            device=states.device,
            dtype=torch.long,
        )
        rewards = torch.zeros(
            (batch_size, context_length, 1),
            device=states.device,
            dtype=states.dtype,
        )
        output = model(
            states=states,
            actions=previous_actions,
            rewards=rewards,
            returns_to_go=returns_to_go,
            timesteps=timesteps,
            attention_mask=attention_mask,
            return_dict=True,
        )
        return output.action_preds
    return model(
        states=states,
        actions=previous_actions,
        returns_to_go=returns_to_go,
    )


def _normalized_teacher_rows(frame: pl.DataFrame) -> pl.DataFrame:
    missing = sorted(REQUIRED_TEACHER_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"DT research-shadow teacher rows missing columns: {missing}")
    if _frame_has_true(frame, "market_execution_enabled"):
        raise ValueError("DT research-shadow refuses market execution rows.")
    if _frame_has_true(frame, "market_execution_gate_passed"):
        raise ValueError("DT research-shadow refuses market execution gates.")
    if _frame_has_true(frame, "promotion_gate_passed"):
        raise ValueError("DT research-shadow refuses promoted DT rows.")
    if _frame_has_true(frame, "raw_hourly_action_imitation"):
        raise ValueError("DT research-shadow refuses raw hourly action imitation.")
    if not _frame_all_true(frame, "not_full_dfl"):
        raise ValueError("DT research-shadow rows must keep not_full_dfl=true.")
    if not _frame_all_true(frame, "not_market_execution"):
        raise ValueError("DT research-shadow rows must keep not_market_execution=true.")
    if frame.height == 0:
        raise ValueError("DT research-shadow teacher rows cannot be empty.")
    return frame.with_columns(
        pl.col("anchor_timestamp").cast(pl.Datetime, strict=False),
        pl.lit(SPLIT_STRATEGY).alias("split_strategy"),
        pl.lit(False).alias("publication_receipt_verified"),
        pl.lit(False).alias("source_publication_timestamp_available"),
        pl.lit(False).alias("market_availability_claim"),
        pl.lit(True).alias("research_shadow_not_promotable"),
        (pl.col("split_name") == "train_selection").alias("research_shadow_training_row"),
        pl.col("permitted_model_training_row").alias(
            "promotable_v13_permitted_training_row"
        ),
    ).sort(["tenant_id", "source_model_name", "anchor_timestamp", "dt_candidate_index_target"])


def _state_feature_contract() -> dict[str, Any]:
    state_features = set(STATE_FEATURE_NAMES)
    missing_by_group = {
        group: [feature for feature in features if feature not in state_features]
        for group, features in STATE_CONTEXT_GROUP_FEATURES.items()
    }
    present_groups = [
        group
        for group in REQUIRED_STATE_CONTEXT_GROUPS
        if not missing_by_group.get(group)
    ]
    missing_groups = [
        group
        for group in REQUIRED_STATE_CONTEXT_GROUPS
        if missing_by_group.get(group)
    ]
    return {
        "state_contract_passed": not missing_groups,
        "state_dim": len(STATE_FEATURE_NAMES),
        "state_feature_names": list(STATE_FEATURE_NAMES),
        "state_feature_groups": {
            group: list(features)
            for group, features in STATE_CONTEXT_GROUP_FEATURES.items()
        },
        "required_state_context_groups": list(REQUIRED_STATE_CONTEXT_GROUPS),
        "present_state_context_groups": present_groups,
        "missing_state_context_groups": missing_groups,
        "market_execution_enabled": False,
    }


def _reward_target_contract(frame: pl.DataFrame) -> dict[str, Any]:
    reward_reference_values = {"v2_plus_teacher_comparator_fallback"}
    if "research_shadow_reward_reference" in frame.columns:
        reward_reference_values.update(
            text
            for value in frame["research_shadow_reward_reference"].unique()
            if (text := _non_empty_text(value, ""))
            and text.casefold() not in {"none", "null"}
        )
    return {
        "reward_contract_passed": True,
        "return_to_go_target": RETURN_TO_GO_TARGET,
        "return_to_go_column": "return_to_go_regret_target_uah",
        "schedule_value_column": "schedule_value_uah",
        "regret_delta_column": "regret_delta_vs_v2_plus_uah",
        "schedule_value_available": "schedule_value_uah" in frame.columns,
        "regret_delta_available": "regret_delta_vs_v2_plus_uah" in frame.columns,
        "value_metric_available": "schedule_value_uah" in frame.columns,
        "reward_reference_values": sorted(reward_reference_values),
        "uses_market_submission_receipts": False,
        "market_execution_enabled": False,
    }


def _candidate_library_reference_regret(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[float | None, str]:
    v2_regret = _candidate_library_family_regret(rows, "frozen_v2_plus_fallback")
    if v2_regret is not None:
        return v2_regret, "v2_plus_candidate_reference"
    strict_regret = _candidate_library_family_regret(rows, "strict_control")
    if strict_regret is not None:
        return strict_regret, "strict_control_fallback_no_v2_plus_candidate"
    return None, "candidate_regret_no_reference_fallback"


def _candidate_library_family_regret(
    rows: Sequence[Mapping[str, Any]],
    family: str,
) -> float | None:
    for row in rows:
        if str(row.get("candidate_family")) == family:
            return _float(row.get("regret_uah"))
    return None


def _candidate_library_candidate_id(
    *,
    row: Mapping[str, Any],
    candidate_index: int,
    family: str,
    candidate_model: str,
) -> str:
    return "|".join(
        [
            _non_empty_text(row.get("tenant_id"), "unknown_tenant"),
            _non_empty_text(row.get("source_model_name"), "unknown_source"),
            _iso_datetime(row.get("anchor_timestamp")),
            family,
            candidate_model,
            str(candidate_index),
        ]
    )


def _dataset_summary(
    frame: pl.DataFrame,
    *,
    sequence_count: int,
    context_length: int,
) -> dict[str, Any]:
    research_shadow_training_rows = frame.filter(
        pl.col("research_shadow_training_row")
    ).height
    promotable_rows = frame.filter(pl.col("promotable_v13_permitted_training_row")).height
    coverage = _forecast_family_coverage(frame)
    return {
        "available_teacher_rows": frame.height,
        "train_selection_rows": frame.filter(pl.col("split_name") == "train_selection").height,
        "research_shadow_training_rows": research_shadow_training_rows,
        "promotable_v13_permitted_training_rows": promotable_rows,
        "blocked_promotable_training_rows": frame.height - promotable_rows,
        "sequence_count": sequence_count,
        "context_length": context_length,
        "tenant_count": frame["tenant_id"].n_unique(),
        "source_model_count": frame["source_model_name"].n_unique(),
        "source_model_names": sorted(
            str(value) for value in frame["source_model_name"].unique().to_list()
        ),
        **coverage,
        "candidate_family_count": frame["dt_schedule_family_target"].n_unique(),
        "v13_training_permission_gate_passed": bool(
            frame.select(pl.col("v13_training_permission_gate_passed").all()).item()
        ),
        "dt_promotion_gate_passed": False,
        "market_execution_enabled": False,
    }


def _split_metadata(frame: pl.DataFrame) -> dict[str, Any]:
    return {
        "split_strategy": SPLIT_STRATEGY,
        "chronological_split_passed": _chronological_split_passed(frame),
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "market_availability_claim": False,
        "research_shadow_not_promotable": True,
        "market_execution_enabled": False,
    }


def _chronological_split_passed(frame: pl.DataFrame) -> bool:
    for _, group in frame.group_by(["tenant_id", "source_model_name"]):
        train = group.filter(pl.col("split_name") == "train_selection")
        evaluation = group.filter(pl.col("split_name") != "train_selection")
        if train.height == 0:
            return False
        if evaluation.height == 0:
            continue
        train_max = _datetime_series_value(train["anchor_timestamp"].max())
        evaluation_min = _datetime_series_value(evaluation["anchor_timestamp"].min())
        if train_max is None or evaluation_min is None:
            return False
        if train_max >= evaluation_min:
            return False
    return True


def _sequence_arrays(frame: pl.DataFrame, *, context_length: int) -> dict[str, Any]:
    sequences = _sequence_groups(frame)
    coverage = _forecast_family_coverage(frame)
    action_dim = max(_int(frame["dt_candidate_index_target"].max()) + 1, 1)
    family_names = sorted(str(value) for value in frame["dt_schedule_family_target"].unique())
    family_to_id = {family: index for index, family in enumerate(family_names)}
    states = np.zeros((len(sequences), context_length, len(STATE_FEATURE_NAMES)), dtype=np.float32)
    actions = np.full((len(sequences), context_length), -100, dtype=np.int64)
    candidate_id_targets = np.full(
        (len(sequences), context_length),
        "",
        dtype=object,
    )
    schedule_family_targets = np.full(
        (len(sequences), context_length),
        "",
        dtype=object,
    )
    returns_to_go = np.zeros((len(sequences), context_length, 1), dtype=np.float32)
    candidate_mask = np.zeros((len(sequences), context_length), dtype=bool)
    action_feasibility_mask = np.zeros(
        (len(sequences), context_length, action_dim),
        dtype=bool,
    )
    candidate_regret = np.full((len(sequences), context_length), np.nan, dtype=np.float32)
    candidate_value = np.full((len(sequences), context_length), np.nan, dtype=np.float32)
    candidate_regret_delta = np.full(
        (len(sequences), context_length),
        np.nan,
        dtype=np.float32,
    )
    candidate_tail_risk = np.zeros((len(sequences), context_length), dtype=bool)
    candidate_family_ids = np.full((len(sequences), context_length), -1, dtype=np.int64)
    is_v2_plus = np.zeros((len(sequences), context_length), dtype=bool)
    is_strict = np.zeros((len(sequences), context_length), dtype=bool)
    v2_plus_rule_target_mask = np.zeros((len(sequences), context_length), dtype=bool)
    v2_plus_rule_target_candidate_id = np.full(
        (len(sequences), context_length),
        "",
        dtype=object,
    )
    safety_counts = np.zeros((len(sequences), context_length), dtype=np.int64)
    split_names: list[str] = []
    anchor_timestamps: list[str] = []

    for sequence_index, rows in enumerate(sequences):
        rows = rows[:context_length]
        feasible_actions = [
            int(row["dt_candidate_index_target"])
            for row in rows
            if 0 <= int(row["dt_candidate_index_target"]) < action_dim
        ]
        split_names.append(
            "train"
            if all(str(row["split_name"]) == "train_selection" for row in rows)
            else "evaluation"
        )
        anchor_timestamps.append(_iso_datetime(rows[0]["anchor_timestamp"]))
        for position, row in enumerate(rows):
            states[sequence_index, position, :] = np.array(_state_features(row), dtype=np.float32)
            action = int(row["dt_candidate_index_target"])
            actions[sequence_index, position] = action
            candidate_id_targets[sequence_index, position] = str(
                row["dt_candidate_id_target"]
            )
            family = str(row["dt_schedule_family_target"])
            schedule_family_targets[sequence_index, position] = family
            returns_to_go[sequence_index, position, 0] = (
                _float(row["return_to_go_regret_target_uah"]) / 10_000.0
            )
            candidate_mask[sequence_index, position] = True
            action_feasibility_mask[sequence_index, position, feasible_actions] = True
            candidate_regret[sequence_index, position] = _float(row["regret_uah"])
            candidate_value[sequence_index, position] = _float(row["schedule_value_uah"])
            candidate_regret_delta[sequence_index, position] = _float(
                row.get("regret_delta_vs_v2_plus_uah")
            )
            candidate_tail_risk[sequence_index, position] = (
                _bool(row.get("label_tail_risk_loss"))
                or _int(row.get("safety_violation_count")) > 0
            )
            is_v2_plus_target = _bool(
                row.get("label_v2_plus_rule_distillation_target")
            ) or _bool(row.get("label_is_v2_plus_selected_candidate"))
            if is_v2_plus_target:
                v2_plus_rule_target_mask[sequence_index, position] = True
                v2_plus_rule_target_candidate_id[sequence_index, position] = str(
                    row.get("dt_candidate_id_target", "")
                )
            candidate_family_ids[sequence_index, position] = family_to_id[family]
            is_v2_plus[sequence_index, position] = _is_v2_plus_family(family)
            is_strict[sequence_index, position] = _is_strict_family(family)
            safety_counts[sequence_index, position] = _int(row.get("safety_violation_count"))

    return {
        "states": states,
        "actions": actions,
        "candidate_id_targets": candidate_id_targets,
        "schedule_family_targets": schedule_family_targets,
        "action_target_value_columns": np.array(
            ACTION_TARGET_VALUE_COLUMNS,
            dtype=object,
        ),
        "returns_to_go": returns_to_go,
        "candidate_mask": candidate_mask,
        "action_feasibility_mask": action_feasibility_mask,
        "candidate_regret_uah": candidate_regret,
        "candidate_value_uah": candidate_value,
        "candidate_regret_delta_vs_v2_plus_uah": candidate_regret_delta,
        "candidate_tail_risk_label": candidate_tail_risk,
        "v2_plus_rule_distillation_target_mask": v2_plus_rule_target_mask,
        "v2_plus_rule_distillation_target_candidate_id": v2_plus_rule_target_candidate_id,
        "candidate_family_ids": candidate_family_ids,
        "candidate_family_names": np.array(family_names, dtype=object),
        "is_v2_plus_reference": is_v2_plus,
        "is_strict_reference": is_strict,
        "candidate_safety_violation_count": safety_counts,
        "split_names": np.array(split_names, dtype=object),
        "anchor_timestamps": np.array(anchor_timestamps, dtype=object),
        "action_dim": np.array(action_dim, dtype=np.int64),
        "state_feature_names": np.array(STATE_FEATURE_NAMES, dtype=object),
        "state_context_groups": np.array(REQUIRED_STATE_CONTEXT_GROUPS, dtype=object),
        "state_contract_passed": np.array(True),
        "reward_contract_passed": np.array(True),
        "return_to_go_target": np.array(RETURN_TO_GO_TARGET, dtype=object),
        "forecast_context_required_families": np.array(
            coverage["forecast_context_required_families"],
            dtype=object,
        ),
        "forecast_context_present_families": np.array(
            coverage["forecast_context_present_families"],
            dtype=object,
        ),
        "forecast_context_missing_families": np.array(
            coverage["forecast_context_missing_families"],
            dtype=object,
        ),
        "forecast_context_coverage_passed": np.array(
            bool(coverage["forecast_context_coverage_passed"])
        ),
        "forecast_context_coverage_status": np.array(
            str(coverage["forecast_context_coverage_status"]),
            dtype=object,
        ),
        "split_strategy": np.array(SPLIT_STRATEGY, dtype=object),
        "publication_receipt_verified": np.array(False),
        "source_publication_timestamp_available": np.array(False),
        "market_availability_claim": np.array(False),
        "research_shadow_not_promotable": np.array(True),
        "research_shadow_training_rows": np.array(
            int(frame.filter(pl.col("research_shadow_training_row")).height),
            dtype=np.int64,
        ),
        "promotable_v13_permitted_training_rows": np.array(
            int(frame.filter(pl.col("promotable_v13_permitted_training_row")).height),
            dtype=np.int64,
        ),
        "market_execution_enabled": np.array(False),
    }


def _sequence_groups(
    frame: pl.DataFrame,
    *,
    max_sequences: int | None = None,
) -> list[list[dict[str, Any]]]:
    groups: list[list[dict[str, Any]]] = []
    for _, group in frame.group_by(
        ["tenant_id", "source_model_name", "anchor_timestamp"],
        maintain_order=True,
    ):
        rows = list(group.sort("dt_candidate_index_target").iter_rows(named=True))
        if rows:
            groups.append(rows)
        if max_sequences is not None and len(groups) >= max_sequences:
            break
    return groups


def _state_features(row: Mapping[str, Any]) -> list[float]:
    forecast_vector = _vector(row.get("forecast_price_uah_mwh_vector"))
    soc_vector = _vector(row.get("soc_fraction_vector"))
    forecast_family = _forecast_family(str(row.get("source_model_name", "")))
    return [
        _mean(forecast_vector) / 10_000.0,
        _float(row.get("forecast_spread_uah_mwh")) / 10_000.0,
        _float(row.get("forecast_top_k_actual_overlap")),
        _float(row.get("forecast_bottom_k_actual_overlap")),
        1.0 if forecast_family == "nbeatsx" else 0.0,
        1.0 if forecast_family == "tft" else 0.0,
        (soc_vector[-1] if soc_vector else 0.5),
        _float(row.get("soc_min_slack_fraction")),
        _float(row.get("selector_feature_terminal_soc_delta_fraction")),
        _float(row.get("selector_feature_total_throughput_delta_mwh")),
        _float(row.get("selector_feature_total_degradation_penalty_uah")) / 1_000.0,
        _float(row.get("schedule_value_uah")) / 10_000.0,
        _float(row.get("regret_delta_vs_v2_plus_uah")) / 10_000.0,
        _float(row.get("return_to_go_regret_target_uah")) / 10_000.0,
        _float(row.get("dt_candidate_index_target")) / 10.0,
        _float(row.get("teacher_anchor_candidate_count"), fallback=2.0) / 10.0,
        1.0 if bool(row.get("v13_training_permission_gate_passed")) else 0.0,
        0.0,
        _stable_hash_scaled(str(row.get("tenant_id", ""))),
        _stable_hash_scaled(str(row.get("source_model_name", ""))),
    ]


def _forecast_family_coverage(frame: pl.DataFrame) -> dict[str, Any]:
    present = sorted(
        {
            family
            for source_model_name in frame["source_model_name"].unique().to_list()
            if (family := _forecast_family(str(source_model_name)))
        }
    )
    missing = [family for family in REQUIRED_FORECAST_FAMILIES if family not in present]
    status = (
        "complete_nbeatsx_tft"
        if not missing
        else f"partial_missing_{'_'.join(missing)}"
    )
    return {
        "forecast_context_required_families": list(REQUIRED_FORECAST_FAMILIES),
        "forecast_context_present_families": present,
        "forecast_context_missing_families": missing,
        "forecast_context_coverage_passed": not missing,
        "forecast_context_coverage_status": status,
        "forecast_context_coverage_required_for_full_dt_prototype": True,
    }


def _forecast_family(source_model_name: str) -> str:
    normalized = source_model_name.casefold()
    if "nbeats" in normalized:
        return "nbeatsx"
    if "tft" in normalized:
        return "tft"
    return ""


def _is_v2_plus_family(family: str) -> bool:
    return family in {
        "frozen_v2_plus_fallback",
        "schedule_value_learner_v2_plus",
    }


def _is_strict_family(family: str) -> bool:
    return family in {"strict_control", "strict_reference"}


def _previous_action_one_hot(actions: torch.Tensor, *, action_dim: int) -> torch.Tensor:
    one_hot = torch.zeros((*actions.shape, action_dim), dtype=torch.float32)
    valid = actions >= 0
    if valid.any():
        one_hot[valid] = nn.functional.one_hot(
            actions[valid],
            num_classes=action_dim,
        ).to(torch.float32)
    shifted = torch.zeros_like(one_hot)
    if one_hot.shape[1] > 1:
        shifted[:, 1:, :] = one_hot[:, :-1, :]
    return shifted


def _ensure_action_targets_feasible(
    *,
    actions: torch.Tensor,
    candidate_mask: torch.Tensor,
    action_feasibility_mask: torch.Tensor,
) -> None:
    valid_positions = candidate_mask & (actions >= 0)
    if not bool(valid_positions.any()):
        raise ValueError("DT research-shadow smoke requires valid action targets.")
    feasible_target_flags = torch.zeros_like(valid_positions, dtype=torch.bool)
    feasible_rows = action_feasibility_mask[valid_positions]
    target_classes = actions[valid_positions].unsqueeze(1)
    feasible_target_flags[valid_positions] = feasible_rows.gather(
        1,
        target_classes,
    ).squeeze(1)
    if bool((valid_positions & ~feasible_target_flags).any()):
        raise ValueError("DT research-shadow action targets must be feasible.")


def _masked_action_targets(
    actions: torch.Tensor,
    candidate_mask: torch.Tensor,
) -> torch.Tensor:
    targets = actions.clone()
    targets[~candidate_mask] = -100
    return targets


def _mask_infeasible_action_logits(
    logits: torch.Tensor,
    action_feasibility_mask: torch.Tensor,
) -> torch.Tensor:
    return logits.masked_fill(~action_feasibility_mask, -1_000_000_000.0)


def _training_objective_config(
    *,
    objective_kind: str,
    cross_entropy_weight: float,
    decision_aware_ranking_weight: float,
    distillation_weight: float,
) -> dict[str, Any]:
    return {
        "objective_kind": objective_kind,
        "cross_entropy_weight": float(cross_entropy_weight),
        "decision_aware_ranking_weight": float(decision_aware_ranking_weight),
        "distillation_weight": float(distillation_weight),
        "action_target": "candidate_index_or_schedule_family",
        "raw_hourly_buy_sell_hold_action_target": False,
        "market_execution_enabled": False,
    }


def _selection_policy_config(
    *,
    min_predicted_improvement_uah: float,
    max_family_tail_risk_probability: float,
) -> dict[str, Any]:
    return {
        "selection_method": "conservative_v2_plus_fallback_shadow_selector",
        "v2_plus_default_fallback": True,
        "min_predicted_improvement_uah": float(min_predicted_improvement_uah),
        "max_family_tail_risk_probability": float(max_family_tail_risk_probability),
        "market_execution_enabled": False,
    }


def _decision_aware_ranking_loss(
    *,
    logits: torch.Tensor,
    actions: torch.Tensor,
    candidate_mask: torch.Tensor,
    candidate_regret: torch.Tensor,
    candidate_value: torch.Tensor,
    is_v2_plus: torch.Tensor,
) -> torch.Tensor:
    losses: list[torch.Tensor] = []
    for sequence_index in range(logits.shape[0]):
        valid_positions = torch.nonzero(candidate_mask[sequence_index], as_tuple=False).reshape(-1)
        if valid_positions.numel() == 0:
            continue
        action_ids = actions[sequence_index, valid_positions]
        valid_action_flags = action_ids >= 0
        if not bool(valid_action_flags.any()):
            continue
        valid_positions = valid_positions[valid_action_flags]
        action_ids = action_ids[valid_action_flags]
        candidate_scores = logits[sequence_index, valid_positions, action_ids]
        regrets = candidate_regret[sequence_index, valid_positions]
        values = candidate_value[sequence_index, valid_positions]
        v2_positions = valid_positions[
            is_v2_plus[sequence_index, valid_positions]
        ]
        if v2_positions.numel() > 0:
            v2_position = int(v2_positions[0].item())
            reference_regret = candidate_regret[sequence_index, v2_position]
            reference_value = candidate_value[sequence_index, v2_position]
        else:
            finite_regrets = regrets[torch.isfinite(regrets)]
            finite_values = values[torch.isfinite(values)]
            reference_regret = (
                finite_regrets.min()
                if finite_regrets.numel() > 0
                else torch.tensor(0.0, dtype=logits.dtype, device=logits.device)
            )
            reference_value = (
                finite_values.max()
                if finite_values.numel() > 0
                else torch.tensor(0.0, dtype=logits.dtype, device=logits.device)
            )
        improvement = reference_regret - regrets
        value_gain = values - reference_value
        utility = improvement + 0.25 * value_gain
        utility = torch.nan_to_num(utility, nan=0.0, posinf=0.0, neginf=0.0)
        target = torch.softmax(utility / 25.0, dim=0)
        log_probs = torch.log_softmax(candidate_scores, dim=0)
        losses.append(-(target * log_probs).sum())
    if not losses:
        return torch.zeros((), dtype=logits.dtype, device=logits.device)
    return torch.stack(losses).mean()


def _v2_plus_rule_distillation_loss(
    *,
    logits: torch.Tensor,
    actions: torch.Tensor,
    candidate_mask: torch.Tensor,
    target_mask: torch.Tensor,
) -> torch.Tensor:
    losses: list[torch.Tensor] = []
    for sequence_index in range(logits.shape[0]):
        valid_positions = torch.nonzero(
            candidate_mask[sequence_index],
            as_tuple=False,
        ).reshape(-1)
        if valid_positions.numel() == 0:
            continue
        action_ids = actions[sequence_index, valid_positions]
        valid_action_flags = action_ids >= 0
        if not bool(valid_action_flags.any()):
            continue
        valid_positions = valid_positions[valid_action_flags]
        action_ids = action_ids[valid_action_flags]
        candidate_scores = logits[sequence_index, valid_positions, action_ids]
        target_positions = torch.nonzero(
            target_mask[sequence_index, valid_positions],
            as_tuple=False,
        ).reshape(-1)
        if target_positions.numel() != 1:
            continue
        target_relative_index = int(target_positions[0].item())
        log_probs = torch.log_softmax(candidate_scores, dim=0)
        losses.append(-log_probs[target_relative_index])
    if not losses:
        return torch.zeros((), dtype=logits.dtype, device=logits.device)
    return torch.stack(losses).mean()


def _evaluation_metrics_and_preview_rows(
    *,
    npz: np.lib.npyio.NpzFile,
    eval_indices: np.ndarray,
    eval_logits: np.ndarray,
    train_indices: np.ndarray,
    train_logits: np.ndarray,
    min_predicted_improvement_uah: float,
    max_family_tail_risk_probability: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    selected_rows, diagnostics = _conservative_preview_rows(
        npz=npz,
        eval_indices=eval_indices,
        eval_logits=eval_logits,
        train_indices=train_indices,
        train_logits=train_logits,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
    )
    dt_regrets = [float(row["dt_selected_regret_uah"]) for row in selected_rows]
    dt_values = [float(row["dt_selected_value_uah"]) for row in selected_rows]
    v2_regrets = [float(row["v2_plus_regret_uah"]) for row in selected_rows]
    v2_values = [float(row["v2_plus_value_uah"]) for row in selected_rows]
    strict_regrets = [float(row["strict_regret_uah"]) for row in selected_rows]
    strict_values = [float(row["strict_value_uah"]) for row in selected_rows]

    candidate_regret = npz["candidate_regret_uah"][eval_indices]
    candidate_value = npz["candidate_value_uah"][eval_indices]
    family_ids = npz["candidate_family_ids"]
    train_family_ids = family_ids[npz["split_names"].astype(str) == "train"]
    behavior_family_id = _majority_family_id(train_family_ids)

    bc_regrets: list[float] = []
    bc_values: list[float] = []
    for index in range(len(eval_indices)):
        behavior_mask = np.equal(family_ids[eval_indices[index]], behavior_family_id)
        bc_regrets.append(
            _reference_regret(
                candidate_regret[index],
                behavior_mask,
            )
        )
        bc_values.append(_reference_value(candidate_value[index], behavior_mask))
    switch_rows = [
        row
        for row in selected_rows
        if not bool(row["abstained_to_v2_plus"])
        and not _is_v2_plus_family(str(row["selected_schedule_family"]))
    ]
    switch_regret_deltas = [float(row["regret_vs_v2_plus_uah"]) for row in switch_rows]
    raw_distilled_regrets = [
        float(row["raw_selected_regret_uah"]) for row in selected_rows
    ]
    raw_distilled_values = [
        float(row["raw_selected_value_uah"]) for row in selected_rows
    ]
    raw_distilled_regret_deltas = [
        float(row["raw_selected_regret_uah"] - row["v2_plus_regret_uah"])
        for row in selected_rows
    ]
    raw_distilled_win_count = int(sum(delta < 0.0 for delta in raw_distilled_regret_deltas))
    raw_distilled_loss_count = int(sum(delta > 0.0 for delta in raw_distilled_regret_deltas))
    raw_distilled_tie_count = int(sum(delta == 0.0 for delta in raw_distilled_regret_deltas))
    recovery_target_rows = [
        row
        for row in selected_rows
        if str(row.get("v2_plus_rule_target_candidate_id", "")).strip()
    ]
    recovery_count = sum(
        1
        for row in recovery_target_rows
        if bool(row.get("raw_selected_matches_v2_plus_rule_target"))
    )
    metrics = {
        "dt_selected_mean_regret_uah": _safe_mean(dt_regrets),
        "dt_selected_mean_value_uah": _safe_mean(dt_values),
        "dt_selected_median_regret_uah": _safe_median(dt_regrets),
        "dt_selected_median_value_uah": _safe_median(dt_values),
        "v2_plus_mean_regret_uah": _safe_mean(v2_regrets),
        "v2_plus_mean_value_uah": _safe_mean(v2_values),
        "v2_plus_median_regret_uah": _safe_median(v2_regrets),
        "v2_plus_median_value_uah": _safe_median(v2_values),
        "strict_mean_regret_uah": _safe_mean(strict_regrets),
        "strict_mean_value_uah": _safe_mean(strict_values),
        "strict_median_regret_uah": _safe_median(strict_regrets),
        "strict_median_value_uah": _safe_median(strict_values),
        "behavior_cloning_mean_regret_uah": _safe_mean(bc_regrets),
        "behavior_cloning_mean_value_uah": _safe_mean(bc_values),
        "behavior_cloning_median_regret_uah": _safe_median(bc_regrets),
        "behavior_cloning_median_value_uah": _safe_median(bc_values),
        "non_v2_plus_switch_count": float(len(switch_rows)),
        "abstention_count": float(
            sum(1 for row in selected_rows if bool(row["abstained_to_v2_plus"]))
        ),
        "switch_win_count": float(sum(delta < 0.0 for delta in switch_regret_deltas)),
        "switch_loss_count": float(sum(delta > 0.0 for delta in switch_regret_deltas)),
        "switch_tie_count": float(sum(delta == 0.0 for delta in switch_regret_deltas)),
        "switch_mean_regret_delta_uah": _safe_mean(switch_regret_deltas),
        "infeasible_action_prediction_count": float(
            diagnostics["infeasible_action_predictions"]
        ),
        "accuracy_secondary": float(
            diagnostics["correct"] / diagnostics["total"]
        )
        if diagnostics["total"]
        else 0.0,
        "v2_plus_rule_recovery_rate": float(recovery_count / len(recovery_target_rows))
        if recovery_target_rows
        else 0.0,
        "raw_distilled_argmax_mean_regret_uah": _safe_mean(raw_distilled_regrets),
        "raw_distilled_argmax_median_regret_uah": _safe_median(raw_distilled_regrets),
        "raw_distilled_argmax_mean_value_uah": _safe_mean(raw_distilled_values),
        "raw_distilled_argmax_minus_v2_plus_mean_regret_uah": _safe_mean(
            raw_distilled_regret_deltas
        ),
        "raw_distilled_argmax_win_loss_tie_vs_v2_plus": {
            "wins": raw_distilled_win_count,
            "losses": raw_distilled_loss_count,
            "ties": raw_distilled_tie_count,
        },
    }
    return metrics, selected_rows


def _dt_research_shadow_selected_preview_packet(
    *,
    selected_rows: list[dict[str, Any]],
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "claim_scope": (
            "dt_research_shadow_selected_schedule_preview_not_promotable_not_market_execution"
        ),
        "selection_method": (
            "conservative_v2_plus_fallback_by_predicted_improvement_and_tail_risk"
        ),
        "preview_rows": selected_rows,
        "evaluation_metrics": dict(metrics),
        "action_target": "candidate_index_or_schedule_family",
        "raw_hourly_buy_sell_hold_action_target": False,
        "publication_receipt_verified": False,
        "research_shadow_not_promotable": True,
        "dt_promotion_gate_passed": False,
        "market_execution_enabled": False,
    }


def _conservative_preview_rows(
    *,
    npz: np.lib.npyio.NpzFile,
    eval_indices: np.ndarray,
    eval_logits: np.ndarray,
    train_indices: np.ndarray,
    train_logits: np.ndarray,
    min_predicted_improvement_uah: float,
    max_family_tail_risk_probability: float,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    action_labels = npz["actions"][eval_indices]
    candidate_mask = npz["candidate_mask"][eval_indices]
    action_feasibility_mask = npz["action_feasibility_mask"][eval_indices]
    candidate_ids = npz["candidate_id_targets"][eval_indices].astype(str)
    schedule_families = npz["schedule_family_targets"][eval_indices].astype(str)
    candidate_regret = npz["candidate_regret_uah"][eval_indices]
    candidate_value = npz["candidate_value_uah"][eval_indices]
    candidate_safety = npz["candidate_safety_violation_count"][eval_indices]
    is_v2_plus = npz["is_v2_plus_reference"][eval_indices]
    is_strict = npz["is_strict_reference"][eval_indices]
    v2_plus_target_mask = (
        npz["v2_plus_rule_distillation_target_mask"][eval_indices]
        if "v2_plus_rule_distillation_target_mask" in npz.files
        else np.zeros_like(candidate_mask, dtype=bool)
    )
    v2_plus_target_candidate_id = (
        npz["v2_plus_rule_distillation_target_candidate_id"][eval_indices].astype(str)
        if "v2_plus_rule_distillation_target_candidate_id" in npz.files
        else np.full(candidate_ids.shape, "", dtype=object).astype(str)
    )
    anchor_timestamps = npz["anchor_timestamps"].astype(str)
    tail_risk_probabilities = _family_tail_risk_probabilities(
        npz=npz,
        train_indices=train_indices,
    )
    margin_slope, margin_intercept = _margin_to_improvement_calibration(
        npz=npz,
        sequence_indices=train_indices,
        logits=train_logits,
    )
    rows: list[dict[str, Any]] = []
    diagnostics = {"correct": 0, "total": 0, "infeasible_action_predictions": 0}
    for local_index, sequence_index in enumerate(eval_indices):
        valid_positions = np.flatnonzero(candidate_mask[local_index])
        if len(valid_positions) == 0:
            continue
        predicted_token_classes = np.argmax(eval_logits[local_index], axis=-1)
        for position in valid_positions:
            diagnostics["total"] += 1
            predicted_class = int(predicted_token_classes[position])
            if not bool(action_feasibility_mask[local_index, position, predicted_class]):
                diagnostics["infeasible_action_predictions"] += 1
            if predicted_class == int(action_labels[local_index, position]):
                diagnostics["correct"] += 1
        candidate_score_rows: list[tuple[int, float]] = []
        for position in valid_positions:
            action_class = int(action_labels[local_index, position])
            if action_class < 0:
                continue
            candidate_score_rows.append(
                (int(position), float(eval_logits[local_index, position, action_class]))
            )
        if not candidate_score_rows:
            continue
        raw_selected_position = max(candidate_score_rows, key=lambda row: row[1])[0]
        raw_selected_score = dict(candidate_score_rows)[raw_selected_position]
        raw_selected_regret = float(candidate_regret[local_index, raw_selected_position])
        raw_selected_value = float(candidate_value[local_index, raw_selected_position])
        v2_plus_target_positions = np.flatnonzero(
            candidate_mask[local_index] & v2_plus_target_mask[local_index]
        )
        v2_plus_target_position = (
            int(v2_plus_target_positions[0]) if len(v2_plus_target_positions) else None
        )
        target_candidate_id = (
            str(v2_plus_target_candidate_id[local_index, v2_plus_target_position])
            if v2_plus_target_position is not None
            else ""
        )
        raw_selected_matches_target = (
            v2_plus_target_position is not None
            and raw_selected_position == v2_plus_target_position
        )
        v2_positions = np.flatnonzero(
            candidate_mask[local_index] & is_v2_plus[local_index]
        )
        v2_position = int(v2_positions[0]) if len(v2_positions) else raw_selected_position
        v2_candidate_id = str(candidate_ids[local_index, v2_position])
        v2_regret = float(candidate_regret[local_index, v2_position])
        v2_value = float(candidate_value[local_index, v2_position])
        v2_score = dict(candidate_score_rows).get(v2_position, raw_selected_score)
        predicted_improvement = (
            margin_slope * (raw_selected_score - v2_score) + margin_intercept
            if raw_selected_position != v2_position
            else 0.0
        )
        raw_selected_family = str(schedule_families[local_index, raw_selected_position])
        family_tail_risk_probability = tail_risk_probabilities.get(raw_selected_family, 0.0)
        tail_risk_guard_passed = (
            family_tail_risk_probability <= max_family_tail_risk_probability
            and int(candidate_safety[local_index, raw_selected_position]) <= 0
        )
        abstained_to_v2_plus = False
        abstention_reason = ""
        selected_position = raw_selected_position
        if raw_selected_position != v2_position:
            if not tail_risk_guard_passed:
                abstained_to_v2_plus = True
                abstention_reason = "tail_risk_guard_blocked"
                selected_position = v2_position
            elif predicted_improvement < min_predicted_improvement_uah:
                abstained_to_v2_plus = True
                abstention_reason = "predicted_improvement_below_threshold"
                selected_position = v2_position

        selected_candidate_id = str(candidate_ids[local_index, selected_position])
        selected_regret = float(candidate_regret[local_index, selected_position])
        selected_value = float(candidate_value[local_index, selected_position])
        strict_regret = _reference_regret(
            candidate_regret[local_index],
            is_strict[local_index],
        )
        strict_value = _reference_value(
            candidate_value[local_index],
            is_strict[local_index],
        )
        rows.append(
            {
                "tenant_id": _candidate_id_part(selected_candidate_id, 0),
                "source_model_name": _candidate_id_part(selected_candidate_id, 1),
                "anchor_timestamp": str(anchor_timestamps[sequence_index]),
                "selected_candidate_id": selected_candidate_id,
                "selected_schedule_family": str(
                    schedule_families[local_index, selected_position]
                ),
                "selected_candidate_index": int(
                    action_labels[local_index, selected_position]
                ),
                "selected_candidate_position": int(selected_position),
                "raw_selected_candidate_id": str(
                    candidate_ids[local_index, raw_selected_position]
                ),
                "raw_selected_schedule_family": raw_selected_family,
                "raw_selected_candidate_index": int(
                    action_labels[local_index, raw_selected_position]
                ),
                "raw_selected_regret_uah": raw_selected_regret,
                "raw_selected_value_uah": raw_selected_value,
                "raw_selected_minus_v2_plus_regret_uah": (
                    raw_selected_regret - v2_regret
                ),
                "raw_selected_minus_v2_plus_value_uah": (
                    raw_selected_value - v2_value
                ),
                "v2_plus_rule_target_candidate_id": target_candidate_id,
                "v2_plus_rule_target_candidate_index": (
                    int(action_labels[local_index, v2_plus_target_position])
                    if v2_plus_target_position is not None
                    else None
                ),
                "raw_selected_matches_v2_plus_rule_target": bool(
                    raw_selected_matches_target
                ),
                "v2_plus_candidate_id": v2_candidate_id,
                "dt_selected_regret_uah": selected_regret,
                "dt_selected_value_uah": selected_value,
                "v2_plus_regret_uah": v2_regret,
                "v2_plus_value_uah": v2_value,
                "strict_regret_uah": strict_regret,
                "strict_value_uah": strict_value,
                "selected_minus_v2_plus_regret_uah": selected_regret - v2_regret,
                "selected_minus_v2_plus_value_uah": selected_value - v2_value,
                "regret_vs_v2_plus_uah": selected_regret - v2_regret,
                "regret_vs_strict_uah": selected_regret - strict_regret,
                "value_vs_v2_plus_uah": selected_value - v2_value,
                "value_vs_strict_uah": selected_value - strict_value,
                "predicted_improvement_vs_v2_plus_uah": float(predicted_improvement),
                "abstained_to_v2_plus": bool(abstained_to_v2_plus),
                "abstention_reason": abstention_reason,
                "family_tail_risk_probability": float(family_tail_risk_probability),
                "tail_risk_guard_passed": bool(tail_risk_guard_passed),
                "candidate_safety_violation_count": int(
                    candidate_safety[local_index, selected_position]
                ),
                "publication_receipt_verified": False,
                "research_shadow_not_promotable": True,
                "dt_promotion_gate_passed": False,
                "market_execution_enabled": False,
            }
        )
    return rows, diagnostics


def _family_tail_risk_probabilities(
    *,
    npz: np.lib.npyio.NpzFile,
    train_indices: np.ndarray,
) -> dict[str, float]:
    if len(train_indices) == 0:
        return {}
    families = npz["schedule_family_targets"][train_indices].astype(str)
    if "candidate_tail_risk_label" in npz.files:
        labels = npz["candidate_tail_risk_label"][train_indices]
    else:
        safety = npz["candidate_safety_violation_count"][train_indices]
        labels = np.array(safety > 0, dtype=bool)
    mask = npz["candidate_mask"][train_indices]
    counts: dict[str, int] = {}
    positives: dict[str, int] = {}
    for sequence_index in range(families.shape[0]):
        for position in range(families.shape[1]):
            if not bool(mask[sequence_index, position]):
                continue
            family = str(families[sequence_index, position])
            counts[family] = counts.get(family, 0) + 1
            if bool(labels[sequence_index, position]):
                positives[family] = positives.get(family, 0) + 1
    return {
        family: float(positives.get(family, 0) / count)
        for family, count in counts.items()
        if count > 0
    }


def _margin_to_improvement_calibration(
    *,
    npz: np.lib.npyio.NpzFile,
    sequence_indices: np.ndarray,
    logits: np.ndarray,
) -> tuple[float, float]:
    if len(sequence_indices) == 0:
        return 0.0, 0.0
    action_labels = npz["actions"][sequence_indices]
    candidate_mask = npz["candidate_mask"][sequence_indices]
    is_v2_plus = npz["is_v2_plus_reference"][sequence_indices]
    candidate_regret = npz["candidate_regret_uah"][sequence_indices]
    margins: list[float] = []
    improvements: list[float] = []
    for sequence_local_index in range(len(sequence_indices)):
        valid_positions = np.flatnonzero(candidate_mask[sequence_local_index])
        if len(valid_positions) == 0:
            continue
        candidate_scores: list[tuple[int, float]] = []
        for position in valid_positions:
            action_class = int(action_labels[sequence_local_index, position])
            if action_class < 0:
                continue
            candidate_scores.append(
                (
                    int(position),
                    float(logits[sequence_local_index, position, action_class]),
                )
            )
        if not candidate_scores:
            continue
        raw_position = max(candidate_scores, key=lambda row: row[1])[0]
        v2_positions = np.flatnonzero(
            candidate_mask[sequence_local_index] & is_v2_plus[sequence_local_index]
        )
        if len(v2_positions) == 0:
            continue
        v2_position = int(v2_positions[0])
        score_lookup = dict(candidate_scores)
        raw_score = score_lookup.get(raw_position)
        v2_score = score_lookup.get(v2_position)
        if raw_score is None or v2_score is None:
            continue
        raw_regret = float(candidate_regret[sequence_local_index, raw_position])
        v2_regret = float(candidate_regret[sequence_local_index, v2_position])
        margins.append(raw_score - v2_score)
        improvements.append(v2_regret - raw_regret)
    if not margins:
        return 0.0, 0.0
    margin_array = np.array(margins, dtype=np.float64)
    improvement_array = np.array(improvements, dtype=np.float64)
    margin_mean = float(np.mean(margin_array))
    improvement_mean = float(np.mean(improvement_array))
    margin_var = float(np.var(margin_array))
    if margin_var < 1e-9:
        return 0.0, improvement_mean
    covariance = float(
        np.mean((margin_array - margin_mean) * (improvement_array - improvement_mean))
    )
    slope = covariance / margin_var
    intercept = improvement_mean - slope * margin_mean
    return float(slope), float(intercept)


def _candidate_id_part(candidate_id: str, index: int) -> str:
    parts = candidate_id.split("|")
    if index < len(parts):
        return parts[index]
    return ""


def _majority_family_id(family_ids: np.ndarray) -> int:
    values = [int(value) for value in family_ids.reshape(-1).tolist() if int(value) >= 0]
    if not values:
        return -1
    return max(sorted(set(values)), key=values.count)


def _reference_regret(regrets: np.ndarray, mask: np.ndarray) -> float:
    return _reference_metric(regrets, mask)


def _reference_value(values: np.ndarray, mask: np.ndarray) -> float:
    return _reference_metric(values, mask)


def _reference_metric(values: np.ndarray, mask: np.ndarray) -> float:
    positions = np.flatnonzero(mask & np.isfinite(values))
    if len(positions) == 0:
        finite = values[np.isfinite(values)]
        return float(finite[0]) if len(finite) else 0.0
    return float(values[int(positions[0])])


def _safe_mean(values: list[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _safe_median(values: list[float]) -> float:
    return float(np.median(np.array(values, dtype=np.float64))) if values else 0.0


def _add_gate(
    gate_results: dict[str, dict[str, Any]],
    failures: list[str],
    gate_name: str,
    gate_failures: list[str],
) -> None:
    gate_results[gate_name] = {
        "passed": not gate_failures,
        "failures": gate_failures,
        "market_execution_enabled": False,
    }
    failures.extend(f"{gate_name}:{failure}" for failure in gate_failures)


def _frame_has_true(frame: pl.DataFrame, column: str) -> bool:
    return column in frame.columns and bool(frame.select(pl.col(column).any()).item())


def _frame_all_true(frame: pl.DataFrame, column: str) -> bool:
    return column in frame.columns and bool(frame.select(pl.col(column).all()).item())


def _contains_market_execution_enabled_true(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key == "market_execution_enabled" and bool(item):
                return True
            if _contains_market_execution_enabled_true(item):
                return True
        return False
    if isinstance(value, list | tuple):
        return any(_contains_market_execution_enabled_true(item) for item in value)
    return False


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence_equal(value: object, expected: Sequence[object]) -> bool:
    if isinstance(value, list | tuple):
        return list(value) == list(expected)
    return False


def _list_value(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _required_float_metrics_present(
    metrics: Mapping[str, Any],
    required_names: Sequence[str],
) -> bool:
    return all(_float_or_none(metrics.get(name)) is not None for name in required_names)


def _vector(value: object) -> list[float]:
    if value is None:
        return []
    if isinstance(value, list | tuple):
        return [_float(item) for item in value]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = ast.literal_eval(text)
        if isinstance(parsed, list | tuple):
            return [_float(item) for item in parsed]
    return []


def _float(value: object, *, fallback: float = 0.0) -> float:
    if value is None:
        return fallback
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return fallback
    return fallback


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value != 0
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes"}
    return False


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _non_empty_text(value: object, fallback: str) -> str:
    text = str(value).strip() if value is not None else ""
    return text if text else fallback


def _int(value: object) -> int:
    if value is None or isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _datetime_series_value(value: object) -> datetime | None:
    return value if isinstance(value, datetime) else None


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _stable_hash_scaled(value: str) -> float:
    if not value:
        return 0.0
    return float(sum(ord(char) for char in value) % 1000) / 1000.0


def _iso_datetime(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _jsonable(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    return value
