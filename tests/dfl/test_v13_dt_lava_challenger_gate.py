from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.v13_dt_lava_challenger_gate import (
    evaluate_v13_dt_lava_offline_challenger_gate,
)
from smart_arbitrage.dfl.v13_dt_lava_teacher_export import (
    build_dfl_v13_dt_lava_teacher_packet,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODEL = "nbeatsx_official_global_panel_horizon_calibrated_v1"
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 24, 12)


def test_v13_dt_lava_offline_challenger_blocks_when_teacher_dataset_not_ready() -> None:
    packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-blocked",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=False),
    )

    gate = evaluate_v13_dt_lava_offline_challenger_gate(
        teacher_packet=packet,
        bridge_strict_frame=_bridge_strict_frame(offline_dt_regret=120.0),
        source_model_names=(SOURCE_MODEL,),
        min_validation_tenant_anchor_count=90,
    )

    assert gate.passed is False
    assert gate.decision == "blocked"
    assert "V13" in gate.description
    assert gate.metrics["teacher_dataset_ready"] is False
    assert gate.metrics["bridge_gate_passed"] is True
    assert gate.metrics["market_execution_enabled"] is False
    assert gate.metrics["market_execution_gate_passed"] is False


def test_v13_dt_lava_offline_challenger_passes_with_ready_teacher_and_bridge() -> None:
    packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-ready",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=True),
    )

    bridge = _bridge_strict_frame(offline_dt_regret=120.0)
    gate = evaluate_v13_dt_lava_offline_challenger_gate(
        teacher_packet=packet,
        bridge_strict_frame=bridge,
        source_model_names=(SOURCE_MODEL,),
        min_validation_tenant_anchor_count=90,
    )

    assert gate.passed is True
    assert gate.decision == "offline_dt_lava_challenger"
    assert gate.metrics["teacher_dataset_ready"] is True
    assert gate.metrics["bridge_gate_passed"] is True
    assert gate.metrics["required_control_roles_present"] is True
    assert gate.metrics["behavior_cloning_control_present"] is True
    assert gate.metrics["safe_switch_coverage_gate_passed"] is True
    assert gate.metrics["deterministic_safety_projection_passed"] is True
    assert gate.metrics["deterministic_safety_projection_expected_row_count"] == (
        bridge.height
    )
    assert gate.metrics["deterministic_safety_projection_row_count"] == bridge.height
    assert gate.metrics["deterministic_safety_projection_failed_row_count"] == 0
    assert gate.metrics["deterministic_safety_projection_missing_row_count"] == 0
    assert gate.metrics["deterministic_safety_projection_coverage_ratio"] == 1.0
    assert gate.metrics["best_challenger_role"] == "offline_dt_reference"
    assert gate.metrics["offline_dt_lava_challenger_gate_passed"] is True
    control_summary = gate.metrics["control_comparison_summary"]
    assert control_summary["claim_scope"] == (
        "strict_lp_oracle_control_comparison_not_market_execution"
    )
    assert control_summary["required_control_roles"] == [
        "filtered_behavior_cloning_reference",
        "schedule_value_learner_v2_plus_reference",
        "strict_reference",
    ]
    assert control_summary["required_control_roles_present"] is True
    assert control_summary["behavior_cloning_control_present"] is True
    assert control_summary["source_model_count"] == 1
    assert control_summary["validation_tenant_anchor_count"] == 90
    source_summary = control_summary["source_model_summaries"][0]
    assert source_summary["strict_mean_regret_uah"] == 310.0
    assert source_summary["v2_plus_mean_regret_uah"] == 180.0
    assert source_summary["behavior_cloning_mean_regret_uah"] == 240.0
    assert source_summary["market_execution_enabled"] is False
    assert gate.metrics["market_execution_enabled"] is False
    assert gate.metrics["market_execution_gate_passed"] is False
    assert gate.metrics["not_deployed_decision_transformer_control"] is True


def test_v13_dt_lava_offline_challenger_rejects_market_execution_claims() -> None:
    packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-ready",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=True),
    )
    packet["claim_boundary"]["market_execution_enabled"] = True

    gate = evaluate_v13_dt_lava_offline_challenger_gate(
        teacher_packet=packet,
        bridge_strict_frame=_bridge_strict_frame(offline_dt_regret=120.0),
        source_model_names=(SOURCE_MODEL,),
        min_validation_tenant_anchor_count=90,
    )

    assert gate.passed is False
    assert gate.decision == "blocked"
    assert "market execution" in gate.description
    assert gate.metrics["teacher_dataset_ready"] is True
    assert gate.metrics["bridge_gate_passed"] is True
    assert gate.metrics["offline_dt_lava_challenger_gate_passed"] is False
    assert gate.metrics["market_execution_enabled"] is False
    assert gate.metrics["market_execution_gate_passed"] is False


def test_v13_dt_lava_offline_challenger_requires_safety_projection() -> None:
    packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-ready",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=True),
    )
    bridge = _bridge_strict_frame(offline_dt_regret=120.0).with_columns(
        pl.when(pl.col("selection_role") == "offline_dt_reference")
        .then(pl.lit(False))
        .otherwise(pl.col("deterministic_safety_projection_passed"))
        .alias("deterministic_safety_projection_passed")
    )

    gate = evaluate_v13_dt_lava_offline_challenger_gate(
        teacher_packet=packet,
        bridge_strict_frame=bridge,
        source_model_names=(SOURCE_MODEL,),
        min_validation_tenant_anchor_count=90,
    )

    assert gate.passed is False
    assert gate.decision == "blocked"
    assert "deterministic safety projection" in gate.description
    assert gate.metrics["deterministic_safety_projection_passed"] is False
    assert gate.metrics["deterministic_safety_projection_expected_row_count"] == (
        bridge.height
    )
    assert gate.metrics["deterministic_safety_projection_failed_row_count"] > 0
    assert gate.metrics["offline_dt_lava_challenger_gate_passed"] is False
    assert gate.metrics["market_execution_enabled"] is False


def _teacher_contract_frame(*, v13_ready: bool) -> pl.DataFrame:
    return pl.DataFrame(
        [
            _teacher_contract_row(
                anchor=FIRST_ANCHOR,
                split_name="train_selection",
                training_blocker=(
                    "none" if v13_ready else "v13_training_permission_gate_blocked"
                ),
                permitted_model_training_row=v13_ready,
            ),
            _teacher_contract_row(
                anchor=FIRST_ANCHOR + timedelta(days=1),
                split_name="final_holdout",
                training_blocker=(
                    "final_holdout_scoring_only"
                    if v13_ready
                    else "v13_training_permission_gate_blocked"
                ),
                permitted_model_training_row=False,
            ),
        ]
    )


def _teacher_contract_row(
    *,
    anchor: datetime,
    split_name: str,
    training_blocker: str,
    permitted_model_training_row: bool,
) -> dict[str, object]:
    return {
        "tenant_id": "client_004_kharkiv_hospital",
        "source_model_name": SOURCE_MODEL,
        "anchor_timestamp": anchor,
        "split_name": split_name,
        "dt_candidate_id_target": (
            "client_004_kharkiv_hospital|"
            f"{SOURCE_MODEL}|{anchor.isoformat()}|safe-switch"
        ),
        "dt_candidate_index_target": 1,
        "dt_schedule_family_target": "safe_switch_v2_plus_neighbor",
        "return_to_go_regret_target_uah": 42.0,
        "regret_delta_vs_v2_plus_uah": -42.0,
        "schedule_value_uah": 958.0,
        "forecast_price_uah_mwh_vector": [1000.0, 1400.0],
        "dispatch_mw_vector": [0.25, -0.25],
        "soc_fraction_vector": [0.5, 0.58],
        "selector_feature_forecast_spread_uah_mwh": 400.0,
        "dfl_input_contract": (
            "calibrated_forecasts_tenant_soc_context_feasible_candidate_schedules"
        ),
        "dfl_target_contract": "best_candidate_schedule_value_regret_delta_vs_v2_plus",
        "dt_input_contract": (
            "v13_teacher_sequence_forecast_battery_tenant_candidate_value_return_to_go"
        ),
        "dt_action_target_contract": "candidate_id_or_schedule_family",
        "v2_plus_role": "teacher_comparator_fallback",
        "v13_training_permission_gate_passed": permitted_model_training_row,
        "v13_prior_material_safe_switch_example_count": 20,
        "v13_min_prior_material_safe_switch_examples_for_dt": 20,
        "v13_readiness_decision": (
            "v13_candidate_generation_ready"
            if permitted_model_training_row
            else "data_acquisition_needed"
        ),
        "v13_blocking_context_families": "none"
        if permitted_model_training_row
        else "explicit_dam_publication_receipts:blocked_missing_source",
        "permitted_model_training_row": permitted_model_training_row,
        "permits_model_training": permitted_model_training_row,
        "training_blocker": training_blocker,
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "raw_hourly_action_imitation": False,
        "not_full_dfl": True,
        "not_deployed_dt_control": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _bridge_strict_frame(*, offline_dt_regret: float) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    role_regrets = {
        "strict_reference": 310.0,
        "schedule_value_learner_v2_plus_reference": 180.0,
        "residual_dfl_reference": 220.0,
        "offline_dt_reference": offline_dt_regret,
        "filtered_behavior_cloning_reference": 240.0,
        "residual_dt_fallback_reference": 170.0,
    }
    for tenant_id in TENANTS:
        for anchor_index in range(18):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            for selection_role, regret in role_regrets.items():
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "source_model_name": SOURCE_MODEL,
                        "forecast_model_name": SOURCE_MODEL,
                        "strategy_kind": (
                            "dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark"
                        ),
                        "anchor_timestamp": anchor,
                        "generated_at": GENERATED_AT,
                        "regret_uah": regret,
                        "decision_value_uah": 1000.0 - regret,
                        "selection_role": selection_role,
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "safety_violation_count": 0,
                        "deterministic_safety_projection_passed": True,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                        "evaluation_payload": {
                            "market_execution_enabled": False,
                            "deterministic_safety_projection_passed": True,
                            "not_full_dfl": True,
                            "not_market_execution": True,
                        },
                        "selected_strategy_source": selection_role,
                        "claim_scope": "dfl_v2_plus_dfl_dt_bridge_not_full_dfl",
                    }
                )
    return pl.DataFrame(rows)
