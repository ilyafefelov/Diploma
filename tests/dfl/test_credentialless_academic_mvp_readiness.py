from __future__ import annotations

import json
from typing import TypedDict

from smart_arbitrage.dfl.credentialless_academic_mvp_readiness import (
    build_credentialless_academic_mvp_readiness_summary,
    validate_credentialless_academic_mvp_readiness_summary,
    write_credentialless_academic_mvp_readiness_packet,
)


class _DtResearchShadowKwargs(TypedDict):
    dt_research_shadow_sequence_summary: dict[str, object]
    dt_research_shadow_smoke_summary: dict[str, object]
    dt_research_shadow_evaluation_validation: dict[str, object]


def test_credentialless_academic_mvp_passes_preview_and_prototype_gates() -> None:
    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        dt_research_shadow_sequence_summary=_dt_research_shadow_sequence_summary(),
        dt_research_shadow_smoke_summary=_dt_research_shadow_smoke_summary(),
        dt_research_shadow_evaluation_validation=(
            _dt_research_shadow_evaluation_validation()
        ),
    )

    assert summary["claim_scope"] == (
        "credentialless_academic_mvp_readiness_not_market_execution"
    )
    assert summary["academic_mvp_gate_passed"] is True
    assert summary["operator_preview_gate"]["passed"] is True
    assert summary["operator_preview_gate"]["proposed_bid_status"] == (
        "not_emitted_operator_preview"
    )
    assert summary["operator_preview_gate"]["source_governance_label"] == (
        "receipt-gated for market submission"
    )
    assert summary["operator_preview_gate"]["bid_recommendation_preview_rows"] == 1
    assert summary["operator_preview_gate"]["bid_recommendation_preview_status"] == (
        "non_submittable_dam_preview"
    )
    bid_preview_summary = summary["operator_preview_gate"]["bid_preview_summary"]
    assert bid_preview_summary == {
        "claim_scope": "dam_delivery_day_bid_recommendation_preview_not_market_submission",
        "row_count": 1,
        "buy_rows": 1,
        "sell_rows": 0,
        "hold_rows": 0,
        "total_buy_mwh": 0.1,
        "total_sell_mwh": 0.0,
        "max_quantity_mw": 0.1,
        "indicative_buy_notional_uah": 150.0,
        "indicative_sell_notional_uah": 0.0,
        "has_buy_or_sell_recommendation": True,
        "preview_only": True,
        "market_order_payload_emitted": False,
        "proposed_bid_emitted": False,
        "read_model_boundary": "operator_preview_no_market_submission",
        "market_execution_enabled": False,
    }
    assert summary["source_governance"]["scmo_credentials_required_for_diploma_mvp"] is False
    assert summary["source_governance"]["v13_explicit_receipts_gate_passed"] is False
    assert summary["source_governance"]["market_submission_receipt_gate_status"] == (
        "blocked_external_access"
    )
    assert summary["source_governance"]["public_credentialless_source_observed"] is True
    assert summary["source_governance"]["publication_receipt_verified"] is False
    assert summary["source_governance"]["source_publication_timestamp_available"] is False
    assert summary["source_governance"]["market_availability_claim"] is False
    assert summary["source_governance"]["source_governance_evidence_status"] == (
        "public_credentialless_source_observed_receipt_not_verified"
    )
    assert summary["source_governance"]["academic_mvp_source_governance_passed"] is True
    assert summary["dt_lava_prototype_gate"]["passed_for_academic_mvp"] is True
    assert summary["dt_lava_prototype_gate"]["lava_npz_smoke_validation"] == {
        "configured": True,
        "claim_scope": "lava_npz_margin_smoke_packet_validation_not_market_execution",
        "validation_passed": True,
        "artifact_hashes_valid": True,
        "metrics_valid": True,
        "aggregate_valid": True,
        "npz_contract_valid": True,
        "baseline_comparison_valid": True,
        "baseline_comparison_ready": True,
        "promotion_gate": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }
    assert summary["dt_lava_teacher_contract_gate"]["passed_for_academic_mvp"] is True
    assert summary["dt_lava_teacher_contract_gate"]["permitted_model_training_rows"] == 0
    assert summary["dt_lava_teacher_contract_gate"]["target_label_space"] == (
        "candidate_index_or_schedule_family"
    )
    assert summary["dt_lava_teacher_contract_gate"]["teacher_packet_validation"] == {
        "configured": True,
        "claim_scope": "v13_dt_lava_teacher_packet_validation_not_market_execution",
        "passed": True,
        "candidate_schedule_teacher_contract_passed": True,
        "training_permission_consistency_passed": True,
        "promotion_execution_blocked_passed": True,
        "no_market_execution_passed": True,
        "market_execution_enabled": False,
    }
    prototype_contract = summary["prototype_contract"]
    assert prototype_contract["claim_scope"] == (
        "credentialless_dfl_dt_prototype_contract_not_market_execution"
    )
    assert prototype_contract["product_boundary"] == (
        "dam_delivery_day_operator_recommendation_preview"
    )
    assert prototype_contract["dfl_input_groups"] == [
        "forecast_context",
        "battery_soc_context",
        "tenant_context",
        "candidate_schedule_context",
    ]
    assert prototype_contract["dfl_target_groups"] == ["value_return_targets"]
    assert prototype_contract["dt_action_target_contract"] == (
        "candidate_id_or_schedule_family"
    )
    assert prototype_contract["raw_hourly_action_imitation"] is False
    assert prototype_contract["v2_plus_role"] == "teacher_comparator_fallback"
    assert prototype_contract["evaluation_contract"] == {
        "primary_metric": "strict_lp_oracle_regret_value_vs_v2_plus",
        "required_controls": [
            "strict_reference",
            "schedule_value_learner_v2_plus_reference",
            "filtered_behavior_cloning_reference",
        ],
        "required_controls_present": True,
        "behavior_cloning_control_present": True,
        "deterministic_safety_projection_required": True,
        "deterministic_safety_projection_passed": True,
        "market_execution_enabled": False,
    }
    teacher_contract_summary = summary["dt_lava_teacher_contract_gate"][
        "teacher_contract_summary"
    ]
    assert teacher_contract_summary["required_dfl_input_groups_present"] is True
    assert teacher_contract_summary["required_dfl_target_groups_present"] is True
    assert teacher_contract_summary["required_dt_input_groups_present"] is True
    assert teacher_contract_summary["training_permission_status"] == (
        "blocked_until_v13_source_readiness"
    )
    assert teacher_contract_summary["v2_plus_role"] == "teacher_comparator_fallback"
    assert teacher_contract_summary["raw_hourly_action_imitation"] is False
    assert summary["offline_challenger_gate"]["passed_for_academic_mvp"] is True
    assert summary["offline_challenger_gate"]["promotion_gate_passed"] is False
    assert summary["offline_challenger_gate"]["offline_challenger_packet_validation"] == {
        "configured": True,
        "claim_scope": (
            "v13_dt_lava_offline_challenger_packet_validation_not_market_execution"
        ),
        "passed": True,
        "strict_control_comparison_passed": True,
        "deterministic_safety_projection_passed": True,
        "non_promotion_execution_boundary_passed": True,
        "no_market_execution_passed": True,
        "market_execution_enabled": False,
    }
    control_summary = summary["offline_challenger_gate"]["control_comparison_summary"]
    assert control_summary["required_control_roles_present"] is True
    assert control_summary["behavior_cloning_control_present"] is True
    assert control_summary["validation_tenant_anchor_count"] == 90
    assert control_summary["source_model_summaries"][0]["v2_plus_mean_regret_uah"] == (
        180.0
    )
    scorecard = summary["prototype_evidence_scorecard"]
    assert scorecard == {
        "claim_scope": "credentialless_dfl_dt_prototype_evidence_scorecard_not_market_execution",
        "scorecard_passed_for_academic_mvp": True,
        "operator_bid_preview_rows": 1,
        "operator_bid_preview_has_buy_or_sell": True,
        "lava_npz_validation_passed": True,
        "lava_npz_baseline_comparison_ready": True,
        "teacher_rows": 3921,
        "teacher_train_selection_rows": 3741,
        "teacher_permitted_model_training_rows": 0,
        "dt_action_target_contract": "candidate_id_or_schedule_family",
        "offline_challenger_evidence_passed": True,
        "offline_challenger_promotion_gate_passed": False,
        "offline_challenger_decision": "blocked",
        "strict_v2_plus_behavior_cloning_controls_present": True,
        "deterministic_safety_projection_passed": True,
        "validation_tenant_anchor_count": 90,
        "best_observed_challenger_role": "offline_dt_reference",
        "best_observed_mean_regret_improvement_ratio_vs_v2_plus": 0.0,
        "v2_plus_role": "teacher_comparator_fallback",
        "market_submission_ready": False,
        "permits_model_training": False,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }
    passport = summary["gate_passport"]
    assert passport["operator_preview_gate"]["passed"] is True
    assert passport["dam_bid_recommendation_preview_gate"]["passed"] is True
    assert passport["dam_bid_recommendation_preview_gate"]["buy_rows"] == 1
    assert passport["dam_bid_recommendation_preview_gate"]["total_buy_mwh"] == 0.1
    assert passport["academic_source_governance_gate"] == {
        "passed": True,
        "status": "passed",
        "claim_scope": "credentialless_academic_mvp_source_governance",
        "public_credentialless_source_observed": True,
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "market_availability_claim": False,
        "market_execution_enabled": False,
    }
    assert passport["dt_lava_prototype_ci_smoke_gate"]["passed"] is True
    assert passport["lava_npz_smoke_packet_validation_gate"] == {
        "passed": True,
        "status": "passed",
        "claim_scope": "lava_npz_margin_smoke_packet_validation_not_market_execution",
        "artifact_hashes_valid": True,
        "metrics_valid": True,
        "aggregate_valid": True,
        "npz_contract_valid": True,
        "baseline_comparison_valid": True,
        "permits_model_training": False,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }
    assert passport["dfl_dt_prototype_contract_gate"]["passed"] is True
    assert passport["v13_gated_teacher_contract_gate"]["passed"] is True
    assert (
        passport["v13_gated_teacher_contract_gate"]["teacher_packet_validation_passed"]
        is True
    )
    assert passport["offline_challenger_non_promotion_gate"]["passed"] is True
    assert (
        passport["offline_challenger_non_promotion_gate"][
            "offline_challenger_packet_validation_passed"
        ]
        is True
    )
    assert passport["prototype_evidence_scorecard_gate"] == {
        "passed": True,
        "status": "passed",
        "claim_scope": "credentialless_dfl_dt_prototype_evidence_scorecard_not_market_execution",
        "operator_bid_preview_rows": 1,
        "teacher_train_selection_rows": 3741,
        "validation_tenant_anchor_count": 90,
        "permits_model_training": False,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }
    assert passport["market_execution_safety_gate"]["passed"] is True
    assert passport["market_submission_receipt_gate"] == {
        "passed": False,
        "status": "blocked_external_access",
        "claim_scope": "market_submission_grade_receipt_readiness",
        "required_for_academic_mvp": False,
        "market_execution_enabled": False,
    }
    assert passport["dt_lava_training_promotion_gate"]["passed"] is False
    assert passport["dt_lava_training_promotion_gate"]["status"] == (
        "blocked_until_v13_source_readiness"
    )
    assert passport["market_execution_gate"]["status"] == "out_of_scope"
    assert summary["market_submission_ready"] is False
    assert summary["market_execution_enabled"] is False


def test_credentialless_academic_mvp_rejects_failed_teacher_packet_validation() -> None:
    teacher_validation = _teacher_validation()
    teacher_validation["passed"] = False
    gate_results = teacher_validation["gate_results"]
    assert isinstance(gate_results, dict)
    no_market_execution = gate_results["no_market_execution"]
    assert isinstance(no_market_execution, dict)
    no_market_execution["passed"] = False

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=teacher_validation,
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )

    assert summary["academic_mvp_gate_passed"] is False
    assert summary["dt_lava_teacher_contract_gate"]["passed_for_academic_mvp"] is False
    assert "teacher_packet_validation_not_passed" in summary[
        "dt_lava_teacher_contract_gate"
    ]["failures"]
    assert "teacher_packet_validation_no_market_execution_not_passed" in summary[
        "dt_lava_teacher_contract_gate"
    ]["failures"]


def test_credentialless_academic_mvp_rejects_failed_offline_challenger_validation() -> None:
    challenger_validation = _offline_challenger_validation()
    challenger_validation["passed"] = False
    gate_results = challenger_validation["gate_results"]
    assert isinstance(gate_results, dict)
    safety_gate = gate_results["deterministic_safety_projection"]
    assert isinstance(safety_gate, dict)
    safety_gate["passed"] = False

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=challenger_validation,
        **_dt_research_shadow_kwargs(),
    )

    assert summary["academic_mvp_gate_passed"] is False
    assert summary["offline_challenger_gate"]["passed_for_academic_mvp"] is False
    assert "offline_challenger_packet_validation_not_passed" in summary[
        "offline_challenger_gate"
    ]["failures"]
    assert "offline_challenger_validation_deterministic_safety_projection_not_passed" in summary[
        "offline_challenger_gate"
    ]["failures"]


def test_credentialless_academic_mvp_rejects_operator_preview_bid_payload() -> None:
    operator_preview = _operator_preview()
    operator_preview["proposed_bid"] = {"price_uah_mwh": 1000.0}

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=operator_preview,
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )

    assert summary["academic_mvp_gate_passed"] is False
    assert summary["operator_preview_gate"]["passed"] is False
    assert "forbidden_payload_key:proposed_bid" in summary["operator_preview_gate"][
        "failures"
    ]
    assert summary["market_execution_enabled"] is False


def test_credentialless_academic_mvp_rejects_missing_bid_recommendation_preview() -> None:
    operator_preview = _operator_preview()
    operator_preview.pop("bid_recommendation_preview")

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=operator_preview,
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )

    assert summary["academic_mvp_gate_passed"] is False
    assert summary["operator_preview_gate"]["passed"] is False
    assert "bid_recommendation_preview_empty" in summary["operator_preview_gate"][
        "failures"
    ]


def test_credentialless_academic_mvp_rejects_nested_market_order_payload() -> None:
    operator_preview = _operator_preview()
    bid_preview = operator_preview["bid_recommendation_preview"]
    assert isinstance(bid_preview, list)
    bid_preview[0]["market_order_payload"] = {"quantity_mw": 0.1}

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=operator_preview,
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )

    assert summary["academic_mvp_gate_passed"] is False
    assert summary["operator_preview_gate"]["passed"] is False
    assert (
        "forbidden_payload_key:bid_recommendation_preview[0].market_order_payload"
        in summary["operator_preview_gate"]["failures"]
    )


def test_credentialless_academic_mvp_rejects_raw_hourly_dt_teacher_target() -> None:
    teacher_summary = _teacher_summary()
    claim_boundary = teacher_summary["claim_boundary"]
    assert isinstance(claim_boundary, dict)
    claim_boundary["target_label_space"] = "raw_hourly_buy_sell_hold"

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=teacher_summary,
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )

    assert summary["academic_mvp_gate_passed"] is False
    assert summary["dt_lava_teacher_contract_gate"]["passed_for_academic_mvp"] is False
    assert "invalid_dt_action_target:raw_hourly_buy_sell_hold" in summary[
        "dt_lava_teacher_contract_gate"
    ]["failures"]


def test_credentialless_academic_mvp_requires_v2_plus_teacher_comparator_fallback() -> None:
    teacher_summary = _teacher_summary()
    feature_contract = teacher_summary["feature_contract"]
    assert isinstance(feature_contract, dict)
    feature_contract["v2_plus_role"] = "feature_only"

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=teacher_summary,
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )

    assert summary["academic_mvp_gate_passed"] is False
    assert summary["dt_lava_teacher_contract_gate"]["passed_for_academic_mvp"] is False
    assert "invalid_v2_plus_role:feature_only" in summary[
        "dt_lava_teacher_contract_gate"
    ]["failures"]


def test_credentialless_academic_mvp_requires_complete_teacher_contract_summary() -> None:
    teacher_summary = _teacher_summary()
    contract_summary = teacher_summary["teacher_contract_summary"]
    assert isinstance(contract_summary, dict)
    contract_summary["required_dt_input_groups_present"] = False

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=teacher_summary,
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )

    assert summary["academic_mvp_gate_passed"] is False
    assert summary["dt_lava_teacher_contract_gate"]["passed_for_academic_mvp"] is False
    assert "teacher_contract_missing_required_dt_input_groups" in summary[
        "dt_lava_teacher_contract_gate"
    ]["failures"]


def test_credentialless_academic_mvp_exposes_phase_readiness_matrix() -> None:
    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )

    phase_readiness = summary["prototype_phase_readiness"]

    assert phase_readiness["claim_scope"] == (
        "credentialless_dfl_dt_prototype_phase_readiness_not_market_execution"
    )
    assert phase_readiness["market_execution_enabled"] is False
    assert phase_readiness["phase_0_v13_source_readiness"] == {
        "status": "blocked_market_submission_receipts",
        "explicit_receipts_gate_passed": False,
        "safe_switch_floor_passed": True,
        "ready_for_training": False,
        "required_for_academic_mvp": False,
        "market_execution_enabled": False,
    }
    assert phase_readiness["phase_1_lava_npz_smoke"]["status"] == (
        "passed_ci_smoke_not_promotion"
    )
    assert phase_readiness["phase_1_lava_npz_smoke"]["gate_passed"] is True
    assert phase_readiness["phase_2_v13_gated_teacher_contract"]["status"] == (
        "passed_contract_training_rows_gated"
    )
    assert phase_readiness["phase_2_v13_gated_teacher_contract"][
        "permitted_model_training_rows"
    ] == 0
    assert phase_readiness["phase_3_offline_challenger"]["status"] == (
        "passed_non_promotion_evidence"
    )
    assert phase_readiness["phase_3_offline_challenger"]["promotion_gate_passed"] is False
    assert phase_readiness["phase_4_full_schedule_dfl"]["status"] == (
        "future_work_not_started"
    )

    validation = validate_credentialless_academic_mvp_readiness_summary(summary)

    assert validation["passed"] is True
    assert validation["gate_results"]["prototype_phase_readiness"]["passed"] is True
    assert validation["gate_results"]["prototype_evidence_scorecard"]["passed"] is True
    assert validation["prototype_evidence_scorecard"]["scorecard_passed_for_academic_mvp"] is True
    assert validation["gate_results"]["lava_npz_smoke_packet_validation"]["passed"] is True


def test_credentialless_academic_mvp_exposes_dt_research_shadow_gate() -> None:
    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        dt_research_shadow_sequence_summary=_dt_research_shadow_sequence_summary(),
        dt_research_shadow_smoke_summary=_dt_research_shadow_smoke_summary(),
        dt_research_shadow_evaluation_validation=(
            _dt_research_shadow_evaluation_validation()
        ),
    )

    gate = summary["dt_research_shadow_gate"]
    passport_gate = summary["gate_passport"]["dt_research_shadow_smoke_gate"]
    validation = validate_credentialless_academic_mvp_readiness_summary(summary)

    assert gate["configured"] is True
    assert gate["passed_for_academic_mvp"] is True
    assert gate["split_strategy"] == "chronological_delivery_timestamp"
    assert gate["chronological_split_passed"] is True
    assert gate["available_teacher_rows"] == 3921
    assert gate["train_selection_rows"] == 3741
    assert gate["research_shadow_training_rows"] == 3741
    assert gate["promotable_v13_permitted_training_rows"] == 0
    assert gate["forecast_context_required_families"] == ["nbeatsx", "tft"]
    assert gate["forecast_context_present_families"] == ["nbeatsx"]
    assert gate["forecast_context_missing_families"] == ["tft"]
    assert gate["forecast_context_coverage_passed"] is False
    assert gate["forecast_context_coverage_status"] == "partial_missing_tft"
    assert gate["state_contract_passed"] is True
    assert gate["reward_contract_passed"] is True
    assert gate["action_feasibility_mask_attached"] is True
    assert gate["action_feasibility_mask_applied_to_loss"] is True
    assert gate["action_feasibility_mask_applied_to_eval"] is True
    assert gate["infeasible_action_prediction_count"] == 0
    assert gate["state_context_groups"] == [
        "forecast_context",
        "battery_soc_context",
        "tenant_context",
        "candidate_value_regret_context",
        "gate_context",
    ]
    assert gate["return_to_go_target"] == (
        "negative_regret_delta_vs_v2_plus_or_strict_reference"
    )
    assert gate["evaluation_packet_claim_scope"] == (
        "dt_research_shadow_evaluation_packet_not_promotable_not_market_execution"
    )
    assert gate["evaluation_packet_primary_metric"] == (
        "regret_value_vs_strict_v2_plus_behavior"
    )
    assert gate["evaluation_packet_validation_passed"] is True
    assert gate["evaluation_packet_validation_source"] == "sidecar_validation_json"
    assert gate["publication_receipt_verified"] is False
    assert gate["source_publication_timestamp_available"] is False
    assert gate["market_availability_claim"] is False
    assert gate["research_shadow_not_promotable"] is True
    assert gate["requested_model_backbone"] == "auto"
    assert gate["model_backbone"] == "local_dt_compatible_transformer_classifier"
    assert gate["model_backbone_selection_reason"] == "transformers_not_installed"
    assert gate["hf_decision_transformer_available"] is False
    assert gate["hf_decision_transformer_status"] == "transformers_not_installed"
    assert gate["promotion_blocker"] == (
        "explicit_dam_publication_receipts_missing_publication_receipt_not_verified"
    )
    assert gate["dt_promotion_gate_passed"] is False
    assert gate["market_execution_enabled"] is False
    assert gate["evaluation_metrics"]["v2_plus_mean_value_uah"] == 3375.0
    assert gate["evaluation_metrics"]["strict_mean_value_uah"] == 3600.0
    assert gate["evaluation_metrics"]["behavior_cloning_mean_value_uah"] == 3375.0
    assert passport_gate["passed"] is True
    assert passport_gate["research_shadow_training_rows"] == 3741
    assert passport_gate["promotable_v13_permitted_training_rows"] == 0
    assert passport_gate["model_backbone"] == "local_dt_compatible_transformer_classifier"
    assert passport_gate["model_backbone_selection_reason"] == "transformers_not_installed"
    assert validation["passed"] is True
    assert validation["gate_results"]["dt_research_shadow_gate"]["passed"] is True


def test_credentialless_academic_mvp_exposes_hf_dt_research_shadow_gate() -> None:
    sequence_summary = _dt_research_shadow_sequence_summary()
    dataset_summary = sequence_summary["dataset_summary"]
    assert isinstance(dataset_summary, dict)
    dataset_summary["forecast_context_present_families"] = ["nbeatsx", "tft"]
    dataset_summary["forecast_context_missing_families"] = []
    dataset_summary["forecast_context_coverage_passed"] = True
    dataset_summary["forecast_context_coverage_status"] = "complete_nbeatsx_tft"
    smoke_summary = _dt_research_shadow_smoke_summary()
    smoke_summary["requested_model_backbone"] = "hf"
    smoke_summary["model_backbone"] = "huggingface_decision_transformer_model"
    smoke_summary["model_backbone_selection_reason"] = "hf_requested"
    smoke_summary["hf_transformers_available"] = True
    smoke_summary["hf_decision_transformer_available"] = True
    smoke_summary["hf_decision_transformer_status"] = (
        "hf_decision_transformer_importable"
    )

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        dt_research_shadow_sequence_summary=sequence_summary,
        dt_research_shadow_smoke_summary=smoke_summary,
        dt_research_shadow_evaluation_validation=(
            _dt_research_shadow_evaluation_validation()
        ),
    )

    gate = summary["dt_research_shadow_gate"]
    passport_gate = summary["gate_passport"]["dt_research_shadow_smoke_gate"]
    validation = validate_credentialless_academic_mvp_readiness_summary(summary)

    assert gate["passed_for_academic_mvp"] is True
    assert gate["requested_model_backbone"] == "hf"
    assert gate["model_backbone"] == "huggingface_decision_transformer_model"
    assert gate["model_backbone_selection_reason"] == "hf_requested"
    assert gate["hf_decision_transformer_available"] is True
    assert gate["hf_decision_transformer_status"] == (
        "hf_decision_transformer_importable"
    )
    assert gate["forecast_context_coverage_passed"] is True
    assert gate["forecast_context_coverage_status"] == "complete_nbeatsx_tft"
    assert gate["promotable_v13_permitted_training_rows"] == 0
    assert gate["publication_receipt_verified"] is False
    assert gate["research_shadow_not_promotable"] is True
    assert gate["dt_promotion_gate_passed"] is False
    assert gate["market_execution_enabled"] is False
    assert passport_gate["model_backbone"] == "huggingface_decision_transformer_model"
    assert passport_gate["model_backbone_selection_reason"] == "hf_requested"
    assert passport_gate["market_execution_enabled"] is False
    assert validation["passed"] is True
    assert validation["gate_results"]["dt_research_shadow_gate"]["passed"] is True


def test_credentialless_academic_mvp_rejects_failed_dt_evaluation_validation() -> None:
    evaluation_validation = _dt_research_shadow_evaluation_validation()
    evaluation_validation["passed"] = False
    evaluation_validation["failures"] = ["regret_value_metrics:missing"]

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        dt_research_shadow_sequence_summary=_dt_research_shadow_sequence_summary(),
        dt_research_shadow_smoke_summary=_dt_research_shadow_smoke_summary(),
        dt_research_shadow_evaluation_validation=evaluation_validation,
    )

    gate = summary["dt_research_shadow_gate"]
    validation = validate_credentialless_academic_mvp_readiness_summary(summary)

    assert gate["passed_for_academic_mvp"] is False
    assert gate["evaluation_packet_validation_source"] == "sidecar_validation_json"
    assert "dt_research_shadow_evaluation_validation_not_passed" in gate["failures"]
    assert summary["academic_mvp_gate_passed"] is False
    assert validation["passed"] is False


def test_credentialless_academic_mvp_rejects_failed_lava_npz_validation() -> None:
    prototype_readiness = _dt_lava_prototype_readiness()
    lava_validation = prototype_readiness["lava_npz_smoke_validation"]
    assert isinstance(lava_validation, dict)
    lava_validation["validation_passed"] = False
    lava_validation["metrics_valid"] = False

    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=prototype_readiness,
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        dt_research_shadow_sequence_summary=_dt_research_shadow_sequence_summary(),
        dt_research_shadow_smoke_summary=_dt_research_shadow_smoke_summary(),
        dt_research_shadow_evaluation_validation=(
            _dt_research_shadow_evaluation_validation()
        ),
    )

    assert summary["academic_mvp_gate_passed"] is False
    assert summary["dt_lava_prototype_gate"]["passed_for_academic_mvp"] is False
    assert "lava_npz_smoke_packet_validation_not_passed" in summary[
        "dt_lava_prototype_gate"
    ]["failures"]
    assert "lava_npz_smoke_packet_metrics_invalid" in summary[
        "dt_lava_prototype_gate"
    ]["failures"]


def test_credentialless_academic_mvp_writer_emits_json_and_markdown(tmp_path) -> None:
    output_dir = tmp_path / "academic_mvp"

    packet = write_credentialless_academic_mvp_readiness_packet(
        output_dir=output_dir,
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        dt_research_shadow_sequence_summary=_dt_research_shadow_sequence_summary(),
        dt_research_shadow_smoke_summary=_dt_research_shadow_smoke_summary(),
        dt_research_shadow_evaluation_validation=(
            _dt_research_shadow_evaluation_validation()
        ),
    )

    summary = json.loads(
        (output_dir / "credentialless_academic_mvp_readiness_summary.json").read_text(
            encoding="utf-8"
        )
    )
    markdown = (
        output_dir / "credentialless_academic_mvp_readiness_summary.md"
    ).read_text(encoding="utf-8")
    validation = json.loads(
        (
            output_dir / "credentialless_academic_mvp_readiness_validation.json"
        ).read_text(encoding="utf-8")
    )

    assert packet["summary_json"].endswith(
        "credentialless_academic_mvp_readiness_summary.json"
    )
    assert packet["validation_json"].endswith(
        "credentialless_academic_mvp_readiness_validation.json"
    )
    assert summary["academic_mvp_gate_passed"] is True
    assert summary["gate_passport"]["market_execution_safety_gate"]["passed"] is True
    assert validation["passed"] is True
    assert validation["gate_results"]["dfl_dt_prototype_contract_gate"]["passed"] is True
    assert "SCMO credentials are not required for the diploma MVP" in markdown
    assert "Market execution safety gate passed" in markdown
    assert "DAM preview buy/sell MWh: `0.1` / `0.0`" in markdown
    assert "DFL/DT prototype contract: `True`" in markdown
    assert "Phase 0 V13 source readiness: `blocked_market_submission_receipts`" in markdown
    assert "Phase 3 offline challenger: `passed_non_promotion_evidence`" in markdown
    assert "Prototype evidence scorecard: `True`" in markdown
    assert "DT action target: `candidate_id_or_schedule_family`" in markdown
    assert "DT research-shadow rows: `3741`; promotable V13 rows: `0`" in markdown
    assert "DT forecast-context coverage: `partial_missing_tft`" in markdown
    assert "DT/V2+/strict/BC regret" in markdown
    assert "DT/V2+/strict/BC value" in markdown
    assert "Offline challenger control anchors: `90`" in markdown
    assert "market_execution_enabled=false" in markdown


def test_credentialless_academic_mvp_cli_materializes_packet(tmp_path) -> None:
    from scripts.materialize_credentialless_academic_mvp_readiness_packet import main

    operator_preview_path = _write_json(tmp_path / "operator_preview.json", _operator_preview())
    v13_summary_path = _write_json(
        tmp_path / "v13_summary.json",
        _v13_receipt_blocked_safe_switch_ready_summary(),
    )
    prototype_path = _write_json(
        tmp_path / "dt_lava_prototype.json",
        _dt_lava_prototype_readiness(),
    )
    teacher_path = _write_json(tmp_path / "teacher.json", _teacher_summary())
    teacher_validation_path = _write_json(
        tmp_path / "teacher_validation.json",
        _teacher_validation(),
    )
    challenger_path = _write_json(
        tmp_path / "challenger.json",
        _offline_challenger_summary(),
    )
    challenger_validation_path = _write_json(
        tmp_path / "challenger_validation.json",
        _offline_challenger_validation(),
    )
    dt_sequence_path = _write_json(
        tmp_path / "dt_research_shadow_sequence_summary.json",
        _dt_research_shadow_sequence_summary(),
    )
    dt_smoke_path = _write_json(
        tmp_path / "dt_research_shadow_smoke_summary.json",
        _dt_research_shadow_smoke_summary(),
    )
    _write_json(
        tmp_path / "dt_research_shadow_evaluation_validation.json",
        _dt_research_shadow_evaluation_validation(),
    )
    output_dir = tmp_path / "academic_mvp"

    exit_code = main(
        [
            "--operator-preview-json",
            str(operator_preview_path),
            "--v13-acquisition-summary-json",
            str(v13_summary_path),
            "--dt-lava-prototype-readiness-json",
            str(prototype_path),
            "--teacher-summary-json",
            str(teacher_path),
            "--teacher-validation-json",
            str(teacher_validation_path),
            "--offline-challenger-summary-json",
            str(challenger_path),
            "--offline-challenger-validation-json",
            str(challenger_validation_path),
            "--dt-research-shadow-sequence-summary-json",
            str(dt_sequence_path),
            "--dt-research-shadow-smoke-summary-json",
            str(dt_smoke_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    summary = json.loads(
        (output_dir / "credentialless_academic_mvp_readiness_summary.json").read_text(
            encoding="utf-8"
        )
    )
    validation = json.loads(
        (output_dir / "credentialless_academic_mvp_readiness_validation.json").read_text(
            encoding="utf-8"
        )
    )

    assert exit_code == 0
    assert summary["academic_mvp_gate_passed"] is True
    assert validation["passed"] is True
    assert summary["market_execution_enabled"] is False


def test_credentialless_academic_mvp_validator_accepts_prototype_packet() -> None:
    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )

    validation = validate_credentialless_academic_mvp_readiness_summary(summary)

    assert validation["passed"] is True
    assert validation["market_execution_enabled"] is False
    assert validation["gate_results"]["academic_mvp_gate"]["passed"] is True
    assert validation["gate_results"]["dfl_dt_prototype_contract_gate"]["passed"] is True
    assert validation["gate_results"]["market_submission_receipt_gate"]["passed"] is True
    assert validation["gate_results"]["market_execution_gate"]["passed"] is True
    assert validation["prototype_contract"]["dt_action_target_contract"] == (
        "candidate_id_or_schedule_family"
    )


def test_credentialless_academic_mvp_validator_rejects_promoted_execution_gate() -> None:
    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )
    market_execution_gate = summary["gate_passport"]["market_execution_gate"]
    assert isinstance(market_execution_gate, dict)
    market_execution_gate["passed"] = True

    validation = validate_credentialless_academic_mvp_readiness_summary(summary)

    assert validation["passed"] is False
    assert (
        "future_gate_passed_for_credentialless_scope:market_execution_gate"
        in validation["failures"]
    )


def test_credentialless_academic_mvp_validator_rejects_tampered_teacher_validation() -> None:
    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )
    teacher_validation = summary["dt_lava_teacher_contract_gate"][
        "teacher_packet_validation"
    ]
    assert isinstance(teacher_validation, dict)
    teacher_validation["passed"] = False

    validation = validate_credentialless_academic_mvp_readiness_summary(summary)

    assert validation["passed"] is False
    assert validation["gate_results"]["teacher_packet_validation"]["passed"] is False
    assert "teacher_packet_validation_not_passed" in validation["failures"]


def test_credentialless_academic_mvp_validator_cli_writes_evidence(tmp_path) -> None:
    from scripts.validate_credentialless_academic_mvp_readiness_packet import main

    output_dir = tmp_path / "academic_mvp"
    packet = write_credentialless_academic_mvp_readiness_packet(
        output_dir=output_dir,
        operator_preview=_operator_preview(),
        v13_acquisition_summary=_v13_receipt_blocked_safe_switch_ready_summary(),
        dt_lava_prototype_readiness=_dt_lava_prototype_readiness(),
        teacher_summary=_teacher_summary(),
        teacher_validation=_teacher_validation(),
        offline_challenger_summary=_offline_challenger_summary(),
        offline_challenger_validation=_offline_challenger_validation(),
        **_dt_research_shadow_kwargs(),
    )
    validation_path = tmp_path / "academic_mvp_validation.json"

    exit_code = main(
        [
            "--input",
            packet["summary_json"],
            "--output",
            str(validation_path),
        ]
    )

    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert validation["passed"] is True
    assert validation["gate_results"]["dfl_dt_prototype_contract_gate"]["passed"] is True


def _operator_preview() -> dict[str, object]:
    return {
        "tenant_id": "client_004_kharkiv_hospital",
        "selected_strategy_id": "strict_similar_day",
        "market_scope": "dam_hourly_planning_preview",
        "market_venue": "DAM",
        "interval_minutes": 60,
        "market_execution_enabled": False,
        "read_model_boundary": "operator_preview_no_market_submission",
        "market_gate_status": "not_evaluated_preview_only",
        "bid_eligibility_status": "not_applicable_no_proposed_bid",
        "proposed_bid_status": "not_emitted_operator_preview",
        "v13_readiness": {
            "gate_status": "data_acquisition_needed",
            "v13_candidate_generation_ready": False,
            "dt_lava_ready": False,
            "market_execution_enabled": False,
            "top_priority_blocker": "explicit_dam_publication_receipts",
            "source_governance_status": "receipt_gated_for_market_submission",
            "source_governance_label": "receipt-gated for market submission",
            "market_submission_receipt_gate_status": "blocked_external_access",
            "scmo_credentials_required_for_diploma_mvp": False,
            "scmo_credentials_required_for_market_submission_grade_receipts": True,
        },
        "recommendation_schedule": [
            {
                "interval_start": "2026-05-26T00:00:00",
                "recommendation": "charge",
                "projected_soc_after": 0.55,
            }
        ],
        "bid_recommendation_preview": [
            {
                "step_index": 0,
                "interval_start": "2026-05-26T00:00:00",
                "market_venue": "DAM",
                "side": "BUY",
                "operator_action": "charge",
                "quantity_mw": 0.1,
                "indicative_limit_price_uah_mwh": 1500.0,
                "preview_only": True,
                "market_execution_enabled": False,
                "market_order_payload_emitted": False,
                "proposed_bid_status": "not_emitted_operator_preview",
                "read_model_boundary": "operator_preview_no_market_submission",
            }
        ],
    }


def _write_json(path, payload: dict[str, object]) -> object:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _v13_receipt_blocked_safe_switch_ready_summary() -> dict[str, object]:
    return {
        "claim_boundary": {
            "market_execution_enabled": False,
            "not_market_execution": True,
            "dt_lava_still_gated": True,
        },
        "readiness_summary": {
            "ready_rows": 0,
            "readiness_rows": 5,
            "blocked_rows": 5,
            "readiness_decisions": ["data_acquisition_needed"],
            "v13_candidate_generation_ready": False,
            "max_prior_material_safe_switch_examples": 20,
            "min_safe_examples_required": 20,
        },
        "source_inventory_summary": {
            "blocked_required_sources": ["explicit_dam_publication_receipts"],
            "blocked_required_source_family_count": 1,
        },
        "safe_switch_deficit_summary": {
            "total_missing_examples": 0,
            "max_missing_examples": 0,
            "blocked_tenant_source_count": 0,
        },
        "receipt_source_lead_audit_summary": {
            "auth_blocked_count": 6,
            "candidate_receipt_source_found": False,
            "dataset_level_metadata_only_count": 2,
            "lead_count": 8,
            "validated_receipt_csv_ready": False,
            "market_execution_enabled": False,
        },
        "receipt_source_audit_summary": {
            "probe_count": 4,
            "candidate_receipt_source_found": False,
            "receipt_csv_generated": False,
            "market_execution_enabled": False,
        },
        "scmo_ws_security_preflight_summary": {
            "credential_material_ready": False,
            "signed_download_request_ready": False,
            "missing_env_vars": [
                "SCMO_USERNAME",
                "SCMO_PASSWORD",
                "SCMO_CLIENT_CERT_PEM",
                "SCMO_CLIENT_KEY_PEM",
                "SCMO_CLIENT_P12",
            ],
            "market_execution_enabled": False,
        },
        "v13_candidate_generation_ready": False,
    }


def _dt_lava_prototype_readiness() -> dict[str, object]:
    return {
        "claim_scope": "dt_lava_prototype_readiness_not_market_execution",
        "ci_smoke_ready": True,
        "dt_lava_prototype_gate_passed": True,
        "dt_lava_training_ready": False,
        "no_market_execution_safety_gate_passed": True,
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "lava_npz_smoke_validation": {
            "configured": True,
            "claim_scope": "lava_npz_margin_smoke_packet_validation_not_market_execution",
            "validation_passed": True,
            "artifact_hashes_valid": True,
            "metrics_valid": True,
            "aggregate_valid": True,
            "npz_contract_valid": True,
            "baseline_comparison_valid": True,
            "baseline_comparison_ready": True,
            "promotion_gate": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        },
        "gate_passport": {
            "lava_npz_smoke_packet_validation_gate": {
                "passed": True,
                "market_execution_enabled": False,
            },
            "v13_training_permission_gate": {
                "passed": False,
                "status": "blocked",
                "market_execution_enabled": False,
            },
            "no_market_execution_safety_gate": {
                "passed": True,
                "market_execution_enabled": False,
            },
        },
    }


def _teacher_summary() -> dict[str, object]:
    return {
        "claim_boundary": {
            "target_label_space": "candidate_index_or_schedule_family",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
        "dataset_summary": {
            "rows": 3921,
            "train_selection_rows": 3741,
            "permitted_model_training_rows": 0,
            "dt_lava_training_dataset_ready": False,
            "safe_switch_coverage_gate_passed": True,
            "v13_training_permission_gate_passed": False,
            "market_execution_enabled": False,
        },
        "feature_contract": {
            "v2_plus_role": "teacher_comparator_fallback",
            "dt_action_target_contract": "candidate_id_or_schedule_family",
        },
        "teacher_contract_summary": {
            "claim_scope": "candidate_schedule_teacher_contract_not_market_execution",
            "required_dfl_input_groups_present": True,
            "required_dfl_target_groups_present": True,
            "required_dt_input_groups_present": True,
            "dfl_input_groups": [
                "forecast_context",
                "battery_soc_context",
                "tenant_context",
                "candidate_schedule_context",
            ],
            "dfl_target_groups": ["value_return_targets"],
            "dt_input_groups": [
                "identity_context",
                "forecast_context",
                "battery_soc_context",
                "tenant_context",
                "candidate_schedule_context",
                "value_return_targets",
                "gate_context",
            ],
            "target_label_space": "candidate_index_or_schedule_family",
            "dt_action_target_contract": "candidate_id_or_schedule_family",
            "v2_plus_role": "teacher_comparator_fallback",
            "training_permission_status": "blocked_until_v13_source_readiness",
            "train_selection_rows": 3741,
            "permitted_model_training_rows": 0,
            "training_rows_blocked_by_v13_source_readiness": True,
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
        },
        "gate_passport": {
            "teacher_dataset_contract_gate": {
                "passed": True,
                "market_execution_enabled": False,
            },
            "safe_switch_coverage_gate": {
                "passed": True,
                "market_execution_enabled": False,
            },
            "v13_training_permission_gate": {
                "passed": False,
                "status": "blocked",
                "market_execution_enabled": False,
            },
        },
    }


def _teacher_validation() -> dict[str, object]:
    return {
        "claim_scope": "v13_dt_lava_teacher_packet_validation_not_market_execution",
        "passed": True,
        "failures": [],
        "gate_results": {
            "candidate_schedule_teacher_contract": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "training_permission_consistency": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "promotion_execution_blocked": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "no_market_execution": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
        },
        "market_execution_enabled": False,
    }


def _offline_challenger_summary() -> dict[str, object]:
    return {
        "claim_boundary": {
            "offline_challenger_only": True,
            "requires_v13_source_readiness": True,
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
        "gate": {
            "decision": "blocked",
            "passed": False,
            "metrics": {
                "offline_dt_lava_challenger_gate_passed": False,
                "teacher_dataset_ready": False,
                "deterministic_safety_projection_passed": True,
                "required_control_roles_present": True,
                "behavior_cloning_control_present": True,
                "bridge_evidence_passed": True,
                "bridge_gate_passed": False,
                "safe_switch_coverage_gate_passed": True,
                "v13_training_permission_gate_passed": False,
                "market_execution_enabled": False,
                "control_comparison_summary": {
                    "claim_scope": (
                        "strict_lp_oracle_control_comparison_not_market_execution"
                    ),
                    "required_control_roles": [
                        "filtered_behavior_cloning_reference",
                        "schedule_value_learner_v2_plus_reference",
                        "strict_reference",
                    ],
                    "required_control_roles_present": True,
                    "behavior_cloning_control_present": True,
                    "source_model_count": 1,
                    "validation_tenant_anchor_count": 90,
                    "best_observed_challenger_role": "offline_dt_reference",
                    "best_observed_source_model_name": (
                        "nbeatsx_official_global_panel_horizon_calibrated_v1"
                    ),
                    "best_observed_mean_regret_improvement_ratio_vs_v2_plus": 0.0,
                    "source_model_summaries": [
                        {
                            "source_model_name": (
                                "nbeatsx_official_global_panel_horizon_calibrated_v1"
                            ),
                            "tenant_count": 5,
                            "validation_tenant_anchor_count": 90,
                            "strict_mean_regret_uah": 310.0,
                            "strict_median_regret_uah": 310.0,
                            "v2_plus_mean_regret_uah": 180.0,
                            "v2_plus_median_regret_uah": 180.0,
                            "behavior_cloning_mean_regret_uah": 240.0,
                            "behavior_cloning_median_regret_uah": 240.0,
                            "challenger_summaries": [],
                            "market_execution_enabled": False,
                        }
                    ],
                    "market_execution_enabled": False,
                },
            },
        },
        "promotion_gate": {
            "offline_dt_lava_challenger_gate_passed": False,
            "production_promote": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        },
        "market_execution_enabled": False,
    }


def _offline_challenger_validation() -> dict[str, object]:
    return {
        "claim_scope": (
            "v13_dt_lava_offline_challenger_packet_validation_not_market_execution"
        ),
        "passed": True,
        "failures": [],
        "gate_results": {
            "packet_contract": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "strict_control_comparison": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "deterministic_safety_projection": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "non_promotion_execution_boundary": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "no_market_execution": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
        },
        "market_execution_enabled": False,
    }


def _dt_research_shadow_sequence_summary() -> dict[str, object]:
    return {
        "claim_scope": "dt_research_shadow_sequence_dataset_not_promotable_not_market_execution",
        "dt_state_feature_contract": {
            "state_contract_passed": True,
            "state_dim": 20,
            "required_state_context_groups": [
                "forecast_context",
                "battery_soc_context",
                "tenant_context",
                "candidate_value_regret_context",
                "gate_context",
            ],
            "present_state_context_groups": [
                "forecast_context",
                "battery_soc_context",
                "tenant_context",
                "candidate_value_regret_context",
                "gate_context",
            ],
            "missing_state_context_groups": [],
            "market_execution_enabled": False,
        },
        "dt_reward_target_contract": {
            "reward_contract_passed": True,
            "return_to_go_target": (
                "negative_regret_delta_vs_v2_plus_or_strict_reference"
            ),
            "schedule_value_available": True,
            "regret_delta_available": True,
            "value_metric_available": True,
            "market_execution_enabled": False,
        },
        "dataset_summary": {
            "available_teacher_rows": 3921,
            "train_selection_rows": 3741,
            "research_shadow_training_rows": 3741,
            "promotable_v13_permitted_training_rows": 0,
            "forecast_context_required_families": ["nbeatsx", "tft"],
            "forecast_context_present_families": ["nbeatsx"],
            "forecast_context_missing_families": ["tft"],
            "forecast_context_coverage_passed": False,
            "forecast_context_coverage_status": "partial_missing_tft",
            "v13_training_permission_gate_passed": False,
            "dt_promotion_gate_passed": False,
            "market_execution_enabled": False,
        },
        "split_metadata": {
            "split_strategy": "chronological_delivery_timestamp",
            "chronological_split_passed": True,
            "publication_receipt_verified": False,
            "source_publication_timestamp_available": False,
            "market_availability_claim": False,
            "research_shadow_not_promotable": True,
            "market_execution_enabled": False,
        },
        "claim_boundary": {
            "action_target": "candidate_index_or_schedule_family",
            "raw_hourly_buy_sell_hold_action_target": False,
            "v2_plus_role": "teacher_comparator_fallback",
            "forecast_context_required_families": ["nbeatsx", "tft"],
            "forecast_context_coverage_required_for_full_dt_prototype": True,
            "research_shadow_not_promotable": True,
            "dt_promotion_gate_passed": False,
            "market_execution_enabled": False,
        },
        "market_execution_enabled": False,
    }


def _dt_research_shadow_smoke_summary() -> dict[str, object]:
    return {
        "claim_scope": (
            "dt_research_shadow_transformer_smoke_not_promotable_not_market_execution"
        ),
        "requested_model_backbone": "auto",
        "model_backbone": "local_dt_compatible_transformer_classifier",
        "model_backbone_selection_reason": "transformers_not_installed",
        "hf_transformers_available": False,
        "hf_decision_transformer_available": False,
        "hf_decision_transformer_status": "transformers_not_installed",
        "loss_function": (
            "cross_entropy_candidate_index_plus_decision_aware_regret_value_ranking"
        ),
        "evaluation_packet_summary": {
            "claim_scope": (
                "dt_research_shadow_evaluation_packet_not_promotable_not_market_execution"
            ),
            "primary_metric": "regret_value_vs_strict_v2_plus_behavior",
            "summary_json": "dt_research_shadow_evaluation_summary.json",
            "validation_json": "dt_research_shadow_evaluation_validation.json",
            "validation_passed": True,
            "market_execution_enabled": False,
        },
        "dt_tensor_contract": {
            "state_contract_passed": True,
            "state_context_groups": [
                "forecast_context",
                "battery_soc_context",
                "tenant_context",
                "candidate_value_regret_context",
                "gate_context",
            ],
            "reward_contract_passed": True,
            "return_to_go_target": (
                "negative_regret_delta_vs_v2_plus_or_strict_reference"
            ),
            "candidate_mask_attached": True,
            "action_feasibility_mask_attached": True,
            "action_feasibility_mask_applied_to_loss": True,
            "action_feasibility_mask_applied_to_eval": True,
            "market_execution_enabled": False,
        },
        "train_sequence_count": 1735,
        "evaluation_sequence_count": 90,
        "research_shadow_training_rows": 3741,
        "promotable_v13_permitted_training_rows": 0,
        "deterministic_safety_projection_passed": True,
        "dt_promotion_gate_passed": False,
        "evaluation_metrics": {
            "dt_selected_mean_regret_uah": 310.58,
            "dt_selected_mean_value_uah": 3600.9,
            "v2_plus_mean_regret_uah": 627.04,
            "v2_plus_mean_value_uah": 3375.0,
            "strict_mean_regret_uah": 310.58,
            "strict_mean_value_uah": 3600.0,
            "behavior_cloning_mean_regret_uah": 627.04,
            "behavior_cloning_mean_value_uah": 3375.0,
            "infeasible_action_prediction_count": 0.0,
            "accuracy_secondary": 0.5,
        },
        "market_execution_enabled": False,
    }


def _dt_research_shadow_evaluation_validation() -> dict[str, object]:
    return {
        "claim_scope": "dt_research_shadow_evaluation_validation_not_market_execution",
        "passed": True,
        "failures": [],
        "gate_results": {
            "regret_value_metrics": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "comparison_controls": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "deterministic_safety_projection": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "non_promotion": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
            "no_market_execution": {
                "passed": True,
                "failures": [],
                "market_execution_enabled": False,
            },
        },
        "market_execution_enabled": False,
    }


def _dt_research_shadow_kwargs() -> _DtResearchShadowKwargs:
    return {
        "dt_research_shadow_sequence_summary": _dt_research_shadow_sequence_summary(),
        "dt_research_shadow_smoke_summary": _dt_research_shadow_smoke_summary(),
        "dt_research_shadow_evaluation_validation": (
            _dt_research_shadow_evaluation_validation()
        ),
    }
