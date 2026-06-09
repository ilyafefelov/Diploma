from __future__ import annotations

from datetime import datetime, timedelta
import json
import pickle

import polars as pl
import pytest

from smart_arbitrage.dfl.v13_dt_lava_teacher_export import (
    build_dfl_v13_dt_lava_teacher_packet,
    validate_dfl_v13_dt_lava_teacher_packet,
    write_dfl_v13_dt_lava_teacher_packet,
)


def test_v13_dt_lava_teacher_packet_exports_candidate_index_dataset(
    tmp_path,
) -> None:
    frame = _teacher_contract_frame(v13_ready=True)

    packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-ready",
        teacher_contract_frame=frame,
        dagster_run_id="run-v13-teacher",
        asset_check_status="passed",
    )
    export_dir = write_dfl_v13_dt_lava_teacher_packet(
        packet,
        output_root=tmp_path,
        teacher_contract_frame=frame,
    )

    summary = packet["dataset_summary"]
    gates = packet["gate_passport"]

    assert packet["phase"] == "phase_2_v13_gated_dt_lava_teacher_dataset"
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["claim_boundary"]["not_deployed_decision_transformer_control"] is True
    assert packet["claim_boundary"]["no_raw_hourly_action_imitation"] is True
    assert packet["feature_contract"]["dfl_input_contract"] == (
        "calibrated_forecasts_tenant_soc_context_feasible_candidate_schedules"
    )
    assert packet["feature_contract"]["dt_action_target_contract"] == (
        "candidate_id_or_schedule_family"
    )
    assert packet["feature_contract"]["v2_plus_role"] == "teacher_comparator_fallback"
    assert packet["feature_contract"]["architecture_recommendation"] == {
        "dfl_input": (
            "calibrated_nbeatsx_tft_forecasts_plus_tenant_soc_context_plus_"
            "feasible_candidate_schedules"
        ),
        "dfl_target": "best_candidate_schedule_value_regret_delta_vs_v2_plus",
        "dt_input": (
            "v13_passing_teacher_sequences_with_forecast_battery_tenant_"
            "candidate_value_return_to_go_regret"
        ),
        "dt_action_target": "candidate_id_or_schedule_family",
        "v2_plus_role": "teacher_comparator_fallback",
    }
    teacher_contract_summary = packet["teacher_contract_summary"]
    assert teacher_contract_summary["required_dfl_input_groups_present"] is True
    assert teacher_contract_summary["required_dfl_target_groups_present"] is True
    assert teacher_contract_summary["required_dt_input_groups_present"] is True
    assert teacher_contract_summary["target_label_space"] == (
        "candidate_index_or_schedule_family"
    )
    assert teacher_contract_summary["dt_action_target_contract"] == (
        "candidate_id_or_schedule_family"
    )
    assert teacher_contract_summary["v2_plus_role"] == "teacher_comparator_fallback"
    assert teacher_contract_summary["training_permission_status"] == (
        "ready_for_offline_training_benchmark"
    )
    assert teacher_contract_summary["raw_hourly_action_imitation"] is False
    assert teacher_contract_summary["market_execution_enabled"] is False
    assert "forecast_price_uah_mwh_vector" in (
        packet["feature_contract"]["feature_column_groups"]["forecast_context"]
    )
    assert "soc_fraction_vector" in (
        packet["feature_contract"]["feature_column_groups"]["battery_soc_context"]
    )
    assert "dispatch_mw_vector" in (
        packet["feature_contract"]["feature_column_groups"]["candidate_schedule_context"]
    )
    assert summary["rows"] == 2
    assert summary["permitted_model_training_rows"] == 1
    assert summary["final_holdout_scoring_rows"] == 1
    assert summary["safe_switch_covered_tenant_source_count"] == 1
    assert summary["safe_switch_coverage_gate_passed"] is True
    assert summary["market_execution_enabled"] is False
    assert gates["v13_training_permission_gate"]["passed"] is True
    assert gates["dt_lava_training_promotion_gate"]["status"] == "not_run"
    assert gates["market_execution_gate"]["status"] == "out_of_scope"

    persisted = json.loads(
        (export_dir / "dfl_v13_dt_lava_teacher_summary.json").read_text(
            encoding="utf-8"
        )
    )
    validation = json.loads(
        (export_dir / "dfl_v13_dt_lava_teacher_validation.json").read_text(
            encoding="utf-8"
        )
    )
    markdown = (export_dir / "dfl_v13_dt_lava_teacher_summary.md").read_text(
        encoding="utf-8"
    )
    rows_csv = export_dir / "dfl_v13_dt_lava_teacher_rows.csv"

    assert persisted["dataset_summary"]["permitted_model_training_rows"] == 1
    assert persisted["attached_artifacts"]["validation_json"] == (
        "dfl_v13_dt_lava_teacher_validation.json"
    )
    assert validation["claim_scope"] == (
        "v13_dt_lava_teacher_packet_validation_not_market_execution"
    )
    assert validation["passed"] is True
    assert validation["gate_results"]["candidate_schedule_teacher_contract"]["passed"] is True
    assert validation["gate_results"]["no_market_execution"]["passed"] is True
    assert validation["gate_results"]["promotion_execution_blocked"]["passed"] is True
    assert validation["market_execution_enabled"] is False
    assert rows_csv.exists()
    assert "candidate id / schedule-family targets" in markdown
    assert "V2+ role: `teacher_comparator_fallback`" in markdown
    assert "market_execution_enabled=false" in markdown


def test_v13_dt_lava_teacher_packet_cli_writes_export(tmp_path) -> None:
    from scripts.materialize_v13_dt_lava_teacher_packet import main

    teacher_pickle = tmp_path / "teacher_contract.pkl"
    with teacher_pickle.open("wb") as file:
        pickle.dump(_teacher_contract_frame(v13_ready=False), file)

    output_root = tmp_path / "runs"
    exit_code = main(
        [
            "--teacher-contract-pickle",
            str(teacher_pickle),
            "--output-root",
            str(output_root),
            "--run-slug",
            "v13-teacher-blocked",
        ]
    )

    assert exit_code == 0
    summary_path = (
        output_root
        / "v13-teacher-blocked"
        / "dfl_v13_dt_lava_teacher_summary.json"
    )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    validation_path = (
        output_root
        / "v13-teacher-blocked"
        / "dfl_v13_dt_lava_teacher_validation.json"
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))

    assert summary["dataset_summary"]["permitted_model_training_rows"] == 0
    assert summary["dataset_summary"]["safe_switch_coverage_gate_passed"] is True
    assert summary["teacher_contract_summary"]["permitted_model_training_rows"] == 0
    assert summary["teacher_contract_summary"]["training_permission_status"] == (
        "blocked_until_v13_source_readiness"
    )
    assert summary["gate_passport"]["v13_training_permission_gate"]["status"] == (
        "blocked"
    )
    assert summary["gate_passport"]["market_execution_gate"]["status"] == (
        "out_of_scope"
    )
    assert validation["passed"] is True
    assert validation["gate_results"]["training_permission_consistency"]["passed"] is True
    assert summary["claim_boundary"]["market_execution_enabled"] is False


def test_v13_dt_lava_teacher_packet_validation_rejects_market_execution() -> None:
    packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-invalid",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=False),
    )
    packet["claim_boundary"]["market_execution_enabled"] = True

    validation = validate_dfl_v13_dt_lava_teacher_packet(packet)

    assert validation["passed"] is False
    assert validation["gate_results"]["no_market_execution"]["passed"] is False
    assert "nested_market_execution_enabled_true" in validation["failures"]


def test_v13_dt_lava_teacher_packet_rejects_missing_phase2_context_group() -> None:
    frame = _teacher_contract_frame(v13_ready=True).drop("soc_fraction_vector")

    with pytest.raises(ValueError, match="battery_soc_context"):
        build_dfl_v13_dt_lava_teacher_packet(
            run_slug="v13-teacher-missing-soc",
            teacher_contract_frame=frame,
        )


def _teacher_contract_frame(*, v13_ready: bool) -> pl.DataFrame:
    anchor = datetime(2026, 1, 1, 23)
    return pl.DataFrame(
        [
            _teacher_contract_row(
                anchor=anchor,
                split_name="train_selection",
                training_blocker="none" if v13_ready else "v13_training_permission_gate_blocked",
                permitted_model_training_row=v13_ready,
            ),
            _teacher_contract_row(
                anchor=anchor + timedelta(days=1),
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
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "anchor_timestamp": anchor,
        "split_name": split_name,
        "dt_candidate_id_target": (
            "client_004_kharkiv_hospital|nbeatsx_official_global_panel_"
            f"horizon_calibrated_v1|{anchor.isoformat()}|safe-switch"
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
        "claim_scope": "v13_gated_dt_lava_teacher_contract_not_training_until_source_ready",
        "not_full_dfl": True,
        "not_deployed_dt_control": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }
