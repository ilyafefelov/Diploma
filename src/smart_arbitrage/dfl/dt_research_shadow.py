"""Credentialless DT research-shadow sequence data and smoke training.

This module intentionally separates offline academic research from promotable
V13 training. It can use chronological delivery-time splits while publication
receipts are unverified, but every artifact remains non-promotable and
non-executable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
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
) -> dict[str, Path]:
    """Train/evaluate a tiny local DT-compatible candidate-index classifier."""

    if max_epochs <= 0:
        raise ValueError("max_epochs must be positive for the research-shadow smoke.")
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
        if epoch == 0:
            train_loss_first = float(loss.detach().item())
        train_loss_last = float(loss.detach().item())
        loss.backward()
        optimizer.step()
    model.eval()
    with torch.no_grad():
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
    metrics = _evaluation_metrics(
        npz=npz,
        eval_indices=eval_indices,
        eval_logits=eval_logits.detach().cpu().numpy(),
    )
    metrics["eval_cross_entropy_loss"] = float(eval_loss.detach().item())
    selected_preview_packet = _dt_research_shadow_selected_preview_packet(
        npz=npz,
        eval_indices=eval_indices,
        eval_logits=eval_logits.detach().cpu().numpy(),
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
        "loss_function": "cross_entropy_candidate_index",
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
    candidate_family_ids = np.full((len(sequences), context_length), -1, dtype=np.int64)
    is_v2_plus = np.zeros((len(sequences), context_length), dtype=bool)
    is_strict = np.zeros((len(sequences), context_length), dtype=bool)
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
            candidate_family_ids[sequence_index, position] = family_to_id[family]
            is_v2_plus[sequence_index, position] = family == "frozen_v2_plus_fallback"
            is_strict[sequence_index, position] = family == "strict_control"
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


def _evaluation_metrics(
    *,
    npz: np.lib.npyio.NpzFile,
    eval_indices: np.ndarray,
    eval_logits: np.ndarray,
) -> dict[str, float]:
    action_labels = npz["actions"][eval_indices]
    candidate_mask = npz["candidate_mask"][eval_indices]
    action_feasibility_mask = npz["action_feasibility_mask"][eval_indices]
    candidate_regret = npz["candidate_regret_uah"][eval_indices]
    candidate_value = npz["candidate_value_uah"][eval_indices]
    family_ids = npz["candidate_family_ids"]
    is_v2_plus = npz["is_v2_plus_reference"][eval_indices]
    is_strict = npz["is_strict_reference"][eval_indices]
    train_family_ids = family_ids[npz["split_names"].astype(str) == "train"]
    behavior_family_id = _majority_family_id(train_family_ids)

    dt_regrets: list[float] = []
    dt_values: list[float] = []
    v2_regrets: list[float] = []
    v2_values: list[float] = []
    strict_regrets: list[float] = []
    strict_values: list[float] = []
    bc_regrets: list[float] = []
    bc_values: list[float] = []
    correct = 0
    total = 0
    infeasible_predictions = 0
    for index in range(len(eval_indices)):
        valid_positions = np.flatnonzero(candidate_mask[index])
        if len(valid_positions) == 0:
            continue
        predicted_token_classes = np.argmax(eval_logits[index], axis=-1)
        for position in valid_positions:
            total += 1
            predicted_class = int(predicted_token_classes[position])
            if not bool(action_feasibility_mask[index, position, predicted_class]):
                infeasible_predictions += 1
            if predicted_token_classes[position] == action_labels[index, position]:
                correct += 1
        candidate_scores = [
            eval_logits[index, position, int(action_labels[index, position])]
            for position in valid_positions
            if int(action_labels[index, position]) >= 0
        ]
        selected_position = int(valid_positions[int(np.argmax(candidate_scores))])
        dt_regrets.append(float(candidate_regret[index, selected_position]))
        dt_values.append(float(candidate_value[index, selected_position]))
        v2_regrets.append(_reference_regret(candidate_regret[index], is_v2_plus[index]))
        v2_values.append(_reference_value(candidate_value[index], is_v2_plus[index]))
        strict_regrets.append(_reference_regret(candidate_regret[index], is_strict[index]))
        strict_values.append(_reference_value(candidate_value[index], is_strict[index]))
        behavior_mask = np.equal(family_ids[eval_indices[index]], behavior_family_id)
        bc_regrets.append(
            _reference_regret(
                candidate_regret[index],
                behavior_mask,
            )
        )
        bc_values.append(_reference_value(candidate_value[index], behavior_mask))
    return {
        "dt_selected_mean_regret_uah": _safe_mean(dt_regrets),
        "dt_selected_mean_value_uah": _safe_mean(dt_values),
        "v2_plus_mean_regret_uah": _safe_mean(v2_regrets),
        "v2_plus_mean_value_uah": _safe_mean(v2_values),
        "strict_mean_regret_uah": _safe_mean(strict_regrets),
        "strict_mean_value_uah": _safe_mean(strict_values),
        "behavior_cloning_mean_regret_uah": _safe_mean(bc_regrets),
        "behavior_cloning_mean_value_uah": _safe_mean(bc_values),
        "infeasible_action_prediction_count": float(infeasible_predictions),
        "accuracy_secondary": float(correct / total) if total else 0.0,
    }


def _dt_research_shadow_selected_preview_packet(
    *,
    npz: np.lib.npyio.NpzFile,
    eval_indices: np.ndarray,
    eval_logits: np.ndarray,
    metrics: Mapping[str, float],
) -> dict[str, Any]:
    return {
        "claim_scope": (
            "dt_research_shadow_selected_schedule_preview_not_promotable_not_market_execution"
        ),
        "selection_method": (
            "highest_feasible_dt_candidate_logit_over_evaluation_sequence_candidates"
        ),
        "preview_rows": _dt_research_shadow_selected_preview_rows(
            npz=npz,
            eval_indices=eval_indices,
            eval_logits=eval_logits,
        ),
        "evaluation_metrics": dict(metrics),
        "action_target": "candidate_index_or_schedule_family",
        "raw_hourly_buy_sell_hold_action_target": False,
        "publication_receipt_verified": False,
        "research_shadow_not_promotable": True,
        "dt_promotion_gate_passed": False,
        "market_execution_enabled": False,
    }


def _dt_research_shadow_selected_preview_rows(
    *,
    npz: np.lib.npyio.NpzFile,
    eval_indices: np.ndarray,
    eval_logits: np.ndarray,
) -> list[dict[str, Any]]:
    action_labels = npz["actions"][eval_indices]
    candidate_mask = npz["candidate_mask"][eval_indices]
    candidate_ids = npz["candidate_id_targets"][eval_indices].astype(str)
    schedule_families = npz["schedule_family_targets"][eval_indices].astype(str)
    candidate_regret = npz["candidate_regret_uah"][eval_indices]
    candidate_value = npz["candidate_value_uah"][eval_indices]
    candidate_safety = npz["candidate_safety_violation_count"][eval_indices]
    anchor_timestamps = npz["anchor_timestamps"].astype(str)
    is_v2_plus = npz["is_v2_plus_reference"][eval_indices]
    is_strict = npz["is_strict_reference"][eval_indices]
    rows: list[dict[str, Any]] = []
    for local_index, sequence_index in enumerate(eval_indices):
        valid_positions = np.flatnonzero(candidate_mask[local_index])
        if len(valid_positions) == 0:
            continue
        candidate_scores = [
            eval_logits[local_index, position, int(action_labels[local_index, position])]
            for position in valid_positions
            if int(action_labels[local_index, position]) >= 0
        ]
        if not candidate_scores:
            continue
        selected_position = int(valid_positions[int(np.argmax(candidate_scores))])
        selected_candidate_id = str(candidate_ids[local_index, selected_position])
        selected_regret = float(candidate_regret[local_index, selected_position])
        selected_value = float(candidate_value[local_index, selected_position])
        v2_regret = _reference_regret(
            candidate_regret[local_index],
            is_v2_plus[local_index],
        )
        v2_value = _reference_value(candidate_value[local_index], is_v2_plus[local_index])
        strict_regret = _reference_regret(
            candidate_regret[local_index],
            is_strict[local_index],
        )
        strict_value = _reference_value(candidate_value[local_index], is_strict[local_index])
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
                "selected_candidate_position": selected_position,
                "dt_selected_regret_uah": selected_regret,
                "dt_selected_value_uah": selected_value,
                "v2_plus_regret_uah": v2_regret,
                "v2_plus_value_uah": v2_value,
                "strict_regret_uah": strict_regret,
                "strict_value_uah": strict_value,
                "regret_vs_v2_plus_uah": selected_regret - v2_regret,
                "regret_vs_strict_uah": selected_regret - strict_regret,
                "value_vs_v2_plus_uah": selected_value - v2_value,
                "value_vs_strict_uah": selected_value - strict_value,
                "candidate_safety_violation_count": int(
                    candidate_safety[local_index, selected_position]
                ),
                "publication_receipt_verified": False,
                "research_shadow_not_promotable": True,
                "dt_promotion_gate_passed": False,
                "market_execution_enabled": False,
            }
        )
    return rows


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
