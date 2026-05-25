from __future__ import annotations

from datetime import datetime, timedelta
import json
import pickle

import polars as pl

from smart_arbitrage.dfl.v13_dt_lava_challenger_export import (
    build_v13_dt_lava_offline_challenger_packet,
    validate_v13_dt_lava_offline_challenger_packet,
    write_v13_dt_lava_offline_challenger_packet,
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
GENERATED_AT = datetime(2026, 5, 25, 12)


def test_v13_dt_lava_offline_challenger_packet_blocks_current_v13_gap(
    tmp_path,
) -> None:
    teacher_packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-blocked",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=False),
    )

    packet = build_v13_dt_lava_offline_challenger_packet(
        run_slug="v13-challenger-blocked",
        teacher_packet=teacher_packet,
        bridge_strict_frame=_bridge_strict_frame(offline_dt_regret=120.0),
        source_model_names=(SOURCE_MODEL,),
    )
    export_dir = write_v13_dt_lava_offline_challenger_packet(
        packet,
        output_root=tmp_path,
    )

    assert packet["phase"] == "phase_3_v13_gated_dt_lava_offline_challenger_gate"
    assert packet["gate"]["passed"] is False
    assert packet["gate"]["decision"] == "blocked"
    assert packet["gate"]["metrics"]["teacher_dataset_ready"] is False
    assert packet["gate"]["metrics"]["bridge_gate_passed"] is True
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["claim_boundary"]["not_deployed_decision_transformer_control"] is True
    assert packet["claim_boundary"]["no_dashboard_api_default_switch"] is True
    assert packet["promotion_gate"]["offline_dt_lava_challenger_gate_passed"] is False
    assert packet["promotion_gate"]["market_execution_gate_passed"] is False
    assert packet["promotion_gate"]["production_promote"] is False

    persisted = json.loads(
        (export_dir / "dfl_v13_dt_lava_offline_challenger_summary.json").read_text(
            encoding="utf-8"
        )
    )
    markdown = (
        export_dir / "dfl_v13_dt_lava_offline_challenger_summary.md"
    ).read_text(encoding="utf-8")
    metrics = json.loads(
        (export_dir / "dfl_v13_dt_lava_offline_challenger_metrics.json").read_text(
            encoding="utf-8"
        )
    )
    validation = json.loads(
        (
            export_dir / "dfl_v13_dt_lava_offline_challenger_validation.json"
        ).read_text(encoding="utf-8")
    )

    assert persisted["gate"]["metrics"]["market_execution_enabled"] is False
    assert persisted["attached_artifacts"]["validation_json"] == (
        "dfl_v13_dt_lava_offline_challenger_validation.json"
    )
    assert metrics["offline_dt_lava_challenger_gate_passed"] is False
    assert validation["claim_scope"] == (
        "v13_dt_lava_offline_challenger_packet_validation_not_market_execution"
    )
    assert validation["passed"] is True
    assert validation["gate_results"]["strict_control_comparison"]["passed"] is True
    assert validation["gate_results"]["deterministic_safety_projection"]["passed"] is True
    assert validation["gate_results"]["non_promotion_execution_boundary"]["passed"] is True
    assert validation["market_execution_enabled"] is False
    assert "V13 training permission" in markdown
    assert "market_execution_enabled=false" in markdown


def test_v13_dt_lava_offline_challenger_packet_can_pass_offline_only() -> None:
    teacher_packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-ready",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=True),
    )

    packet = build_v13_dt_lava_offline_challenger_packet(
        run_slug="v13-challenger-ready",
        teacher_packet=teacher_packet,
        bridge_strict_frame=_bridge_strict_frame(offline_dt_regret=120.0),
        source_model_names=(SOURCE_MODEL,),
    )

    assert packet["gate"]["passed"] is True
    assert packet["gate"]["decision"] == "offline_dt_lava_challenger"
    assert packet["promotion_gate"]["offline_dt_lava_challenger_gate_passed"] is True
    assert packet["promotion_gate"]["market_execution_gate_passed"] is False
    assert packet["promotion_gate"]["production_promote"] is False
    assert packet["claim_boundary"]["market_execution_enabled"] is False

    validation = validate_v13_dt_lava_offline_challenger_packet(packet)

    assert validation["passed"] is True
    assert validation["gate_results"]["non_promotion_execution_boundary"]["passed"] is True


def test_v13_dt_lava_offline_challenger_packet_cli_writes_export(tmp_path) -> None:
    from scripts.materialize_v13_dt_lava_offline_challenger_packet import main

    teacher_packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-blocked",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=False),
    )
    teacher_path = tmp_path / "teacher_summary.json"
    teacher_path.write_text(json.dumps(teacher_packet), encoding="utf-8")
    bridge_pickle = tmp_path / "bridge.pkl"
    with bridge_pickle.open("wb") as file:
        pickle.dump(_bridge_strict_frame(offline_dt_regret=120.0), file)

    exit_code = main(
        [
            "--teacher-summary-json",
            str(teacher_path),
            "--bridge-frame-pickle",
            str(bridge_pickle),
            "--output-root",
            str(tmp_path / "runs"),
            "--run-slug",
            "v13-challenger-cli",
            "--source-model-names-csv",
            SOURCE_MODEL,
        ]
    )

    assert exit_code == 0
    summary = json.loads(
        (
            tmp_path
            / "runs"
            / "v13-challenger-cli"
            / "dfl_v13_dt_lava_offline_challenger_summary.json"
        ).read_text(encoding="utf-8")
    )
    assert summary["gate"]["passed"] is False
    assert summary["promotion_gate"]["market_execution_gate_passed"] is False
    assert summary["claim_boundary"]["market_execution_enabled"] is False
    validation = json.loads(
        (
            tmp_path
            / "runs"
            / "v13-challenger-cli"
            / "dfl_v13_dt_lava_offline_challenger_validation.json"
        ).read_text(encoding="utf-8")
    )
    assert validation["passed"] is True


def test_v13_dt_lava_offline_challenger_cli_can_overlay_legacy_safety_projection(
    tmp_path,
) -> None:
    from scripts.materialize_v13_dt_lava_offline_challenger_packet import main

    teacher_packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-blocked",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=False),
    )
    teacher_path = tmp_path / "teacher_summary.json"
    teacher_path.write_text(json.dumps(teacher_packet), encoding="utf-8")
    bridge_pickle = tmp_path / "bridge.pkl"
    legacy_bridge = _bridge_strict_frame(offline_dt_regret=120.0).drop(
        "deterministic_safety_projection_passed"
    )
    with bridge_pickle.open("wb") as file:
        pickle.dump(legacy_bridge, file)

    exit_code = main(
        [
            "--teacher-summary-json",
            str(teacher_path),
            "--bridge-frame-pickle",
            str(bridge_pickle),
            "--output-root",
            str(tmp_path / "runs"),
            "--run-slug",
            "v13-challenger-cli",
            "--source-model-names-csv",
            SOURCE_MODEL,
            "--infer-deterministic-safety-projection-from-zero-violations",
        ]
    )

    assert exit_code == 0
    metrics = json.loads(
        (
            tmp_path
            / "runs"
            / "v13-challenger-cli"
            / "dfl_v13_dt_lava_offline_challenger_metrics.json"
        ).read_text(encoding="utf-8")
    )
    assert metrics["deterministic_safety_projection_passed"] is True
    assert metrics["deterministic_safety_projection_row_count"] == legacy_bridge.height
    assert metrics["market_execution_enabled"] is False


def test_v13_dt_lava_offline_challenger_validation_rejects_market_execution() -> None:
    teacher_packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug="v13-teacher-blocked",
        teacher_contract_frame=_teacher_contract_frame(v13_ready=False),
    )
    packet = build_v13_dt_lava_offline_challenger_packet(
        run_slug="v13-challenger-invalid",
        teacher_packet=teacher_packet,
        bridge_strict_frame=_bridge_strict_frame(offline_dt_regret=120.0),
        source_model_names=(SOURCE_MODEL,),
    )
    packet["claim_boundary"]["market_execution_enabled"] = True

    validation = validate_v13_dt_lava_offline_challenger_packet(packet)

    assert validation["passed"] is False
    assert validation["gate_results"]["no_market_execution"]["passed"] is False
    assert "nested_market_execution_enabled_true" in validation["failures"]


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
