from __future__ import annotations

import json
import pickle

import polars as pl

from smart_arbitrage.dfl.dt_lava_prototype_readiness import (
    build_dt_lava_prototype_readiness_summary,
    write_dt_lava_prototype_readiness_packet,
)


def test_dt_lava_prototype_readiness_reports_v13_and_candidate_blockers() -> None:
    summary = build_dt_lava_prototype_readiness_summary(
        v13_acquisition_summary=_blocked_v13_summary(),
        candidate_frame_pickle_path=None,
        materialization_blockers=(
            "dfl_official_global_panel_schedule_candidate_library_v2_frame",
        ),
    )

    blocker_codes = {blocker["code"] for blocker in summary["blockers"]}

    assert summary["claim_scope"] == "dt_lava_prototype_readiness_not_market_execution"
    assert summary["ci_smoke_ready"] is False
    assert summary["dt_lava_training_ready"] is False
    assert summary["promotion_gate_passed"] is False
    assert summary["market_execution_gate_passed"] is False
    assert summary["dt_lava_prototype_gate_passed"] is False
    assert summary["no_market_execution_safety_gate_passed"] is True
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False
    assert "candidate_frame_pickle_missing" in blocker_codes
    assert "explicit_dam_publication_receipts_blocked" in blocker_codes
    assert "safe_switch_examples_short" in blocker_codes
    assert "v13_candidate_generation_not_ready" in blocker_codes
    assert "materialization_input_missing" in blocker_codes


def test_dt_lava_prototype_readiness_accepts_real_candidate_frame_but_keeps_v13_gate(
    tmp_path,
) -> None:
    candidate_pickle = tmp_path / "dfl_lava_schedule_neighbor_candidate_frame.pkl"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame(), file)

    summary = build_dt_lava_prototype_readiness_summary(
        v13_acquisition_summary=_blocked_v13_summary(),
        candidate_frame_pickle_path=candidate_pickle,
        lava_npz_smoke_packet_validation=_valid_lava_npz_validation(),
    )

    blocker_codes = {blocker["code"] for blocker in summary["blockers"]}

    assert summary["ci_smoke_ready"] is True
    assert summary["lava_npz_smoke_validation"]["configured"] is True
    assert summary["lava_npz_smoke_validation"]["validation_passed"] is True
    assert (
        summary["gate_passport"]["lava_npz_smoke_packet_validation_gate"]["passed"]
        is True
    )
    assert summary["candidate_frame"]["row_count"] == 2
    assert summary["candidate_frame"]["npz_instance_count"] == 1
    assert "candidate_frame_pickle_missing" not in blocker_codes
    assert "v13_candidate_generation_not_ready" in blocker_codes
    assert summary["dt_lava_training_ready"] is False
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False


def test_dt_lava_readiness_blocks_invalid_lava_npz_validation(tmp_path) -> None:
    candidate_pickle = tmp_path / "dfl_lava_schedule_neighbor_candidate_frame.pkl"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame(), file)

    validation = _valid_lava_npz_validation()
    validation["artifact_hashes_valid"] = False

    summary = build_dt_lava_prototype_readiness_summary(
        v13_acquisition_summary=_blocked_v13_summary(),
        candidate_frame_pickle_path=candidate_pickle,
        lava_npz_smoke_packet_validation=validation,
    )

    blocker_codes = {blocker["code"] for blocker in summary["blockers"]}

    assert summary["ci_smoke_ready"] is False
    assert summary["lava_npz_smoke_validation"]["validation_passed"] is False
    assert "lava_npz_smoke_validation_failed" in blocker_codes
    assert (
        summary["gate_passport"]["lava_npz_smoke_packet_validation_gate"]["passed"]
        is False
    )
    assert summary["market_execution_enabled"] is False


def test_dt_lava_readiness_requires_lava_npz_validation_packet(tmp_path) -> None:
    candidate_pickle = tmp_path / "dfl_lava_schedule_neighbor_candidate_frame.pkl"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame(), file)

    summary = build_dt_lava_prototype_readiness_summary(
        v13_acquisition_summary=_blocked_v13_summary(),
        candidate_frame_pickle_path=candidate_pickle,
        lava_npz_smoke_packet_validation=None,
    )

    blocker_codes = {blocker["code"] for blocker in summary["blockers"]}

    assert summary["ci_smoke_ready"] is False
    assert summary["lava_npz_smoke_validation"]["configured"] is False
    assert summary["lava_npz_smoke_validation"]["validation_passed"] is False
    assert "lava_npz_smoke_validation_missing" in blocker_codes
    assert (
        summary["gate_passport"]["lava_npz_smoke_packet_validation_gate"]["passed"]
        is False
    )
    assert summary["market_execution_enabled"] is False


def test_dt_lava_readiness_reports_passed_upstream_offline_strategy_gate(
    tmp_path,
) -> None:
    candidate_pickle = tmp_path / "dfl_lava_schedule_neighbor_candidate_frame.pkl"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame(), file)

    summary = build_dt_lava_prototype_readiness_summary(
        v13_acquisition_summary=_blocked_v13_summary(),
        candidate_frame_pickle_path=candidate_pickle,
        lava_npz_smoke_packet_validation=_valid_lava_npz_validation(),
        offline_strategy_promotion_registry=_passed_strategy_registry(),
    )

    upstream = summary["offline_strategy_promotion"]

    assert upstream["configured"] is True
    assert upstream["evidence_passed"] is True
    assert upstream["promotion_gate_passed"] is True
    assert upstream["production_promote_count"] == 1
    assert upstream["promoted_source_model_names"] == [
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    ]
    gate_passport = summary["gate_passport"]

    assert summary["ci_smoke_ready"] is True
    assert summary["dt_lava_prototype_gate_passed"] is True
    assert summary["no_market_execution_safety_gate_passed"] is True
    assert summary["dt_lava_training_ready"] is False
    assert summary["promotion_gate_passed"] is False
    assert summary["market_execution_gate_passed"] is False
    assert summary["market_execution_enabled"] is False
    assert gate_passport["upstream_offline_strategy_promotion_gate"]["passed"] is True
    assert gate_passport["dt_lava_prototype_ci_smoke_gate"]["passed"] is True
    assert gate_passport["v13_training_permission_gate"]["passed"] is False
    assert gate_passport["dt_lava_training_promotion_gate"]["status"] == "not_run"
    assert gate_passport["no_market_execution_safety_gate"]["passed"] is True
    assert gate_passport["market_execution_gate"]["status"] == "out_of_scope"


def test_dt_lava_prototype_readiness_writer_emits_json_and_markdown(tmp_path) -> None:
    candidate_pickle = tmp_path / "dfl_lava_schedule_neighbor_candidate_frame.pkl"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame(), file)

    output_dir = tmp_path / "readiness"

    packet = write_dt_lava_prototype_readiness_packet(
        output_dir=output_dir,
        v13_acquisition_summary=_blocked_v13_summary(),
        candidate_frame_pickle_path=candidate_pickle,
        lava_npz_smoke_packet_validation=_valid_lava_npz_validation(),
    )

    summary_path = output_dir / "dt_lava_prototype_readiness_summary.json"
    markdown_path = output_dir / "dt_lava_prototype_readiness_summary.md"
    persisted = json.loads(summary_path.read_text(encoding="utf-8"))

    assert packet["summary_json"] == str(summary_path)
    assert packet["summary_markdown"] == str(markdown_path)
    assert persisted["ci_smoke_ready"] is True
    assert persisted["dt_lava_training_ready"] is False
    assert "market_execution_enabled=false" in markdown_path.read_text(encoding="utf-8")


def test_dt_lava_prototype_readiness_cli_writes_blocked_packet(tmp_path) -> None:
    from scripts.materialize_dt_lava_prototype_readiness_packet import main

    v13_summary_path = tmp_path / "v13_summary.json"
    v13_summary_path.write_text(
        json.dumps(_blocked_v13_summary()),
        encoding="utf-8",
    )
    output_dir = tmp_path / "readiness"
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(
        json.dumps(_passed_strategy_registry()),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--v13-acquisition-summary-json",
            str(v13_summary_path),
            "--offline-strategy-promotion-registry-json",
            str(registry_path),
            "--lava-npz-smoke-validation-json",
            str(_write_lava_npz_validation(tmp_path / "lava_validation.json")),
            "--output-dir",
            str(output_dir),
            "--materialization-blocker",
            "dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame",
        ]
    )

    assert exit_code == 0
    summary = json.loads(
        (output_dir / "dt_lava_prototype_readiness_summary.json").read_text(
            encoding="utf-8"
        )
    )
    blocker_codes = {blocker["code"] for blocker in summary["blockers"]}
    assert summary["ci_smoke_ready"] is False
    assert summary["market_execution_enabled"] is False
    assert summary["lava_npz_smoke_validation"]["configured"] is True
    assert summary["lava_npz_smoke_validation"]["validation_passed"] is True
    assert summary["offline_strategy_promotion"]["promotion_gate_passed"] is True
    assert "candidate_frame_pickle_missing" in blocker_codes
    assert "materialization_input_missing" in blocker_codes


def _blocked_v13_summary() -> dict[str, object]:
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
            "max_prior_material_safe_switch_examples": 7,
            "min_safe_examples_required": 20,
        },
        "source_inventory_summary": {
            "blocked_required_sources": [
                "explicit_dam_publication_receipts",
                "v12_safe_teacher_label_support",
            ],
            "blocked_required_source_family_count": 2,
        },
        "safe_switch_deficit_summary": {
            "total_missing_examples": 77,
            "max_missing_examples": 18,
            "blocked_tenant_source_count": 5,
        },
        "v13_candidate_generation_ready": False,
    }


def _passed_strategy_registry() -> dict[str, object]:
    return {
        "claim_boundary": {
            "claim_scope": (
                "dfl_schedule_value_production_gate_offline_strategy_not_market_execution"
            ),
            "market_execution_enabled": False,
            "not_full_dfl": True,
            "not_market_execution": True,
        },
        "summary": {
            "evidence_passed": True,
            "market_execution_enabled": False,
            "production_promote_count": 1,
            "promoted_source_model_names": [
                "nbeatsx_official_global_panel_horizon_calibrated_v1"
            ],
        },
        "source_model_rows": [
            {
                "source_model_name": (
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "production_promote": True,
                "promotion_blocker": "none",
                "market_execution_enabled": False,
            }
        ],
    }


def _valid_lava_npz_validation() -> dict[str, object]:
    return {
        "claim_scope": "lava_npz_margin_smoke_packet_validation_not_market_execution",
        "artifact_hashes_valid": True,
        "metrics_valid": True,
        "aggregate_valid": True,
        "npz_contract_valid": True,
        "baseline_comparison_valid": True,
        "baseline_comparison_ready": True,
        "baseline_selected_instance_count": 8,
        "strict_fallback_anchor_count": 8,
        "v2_plus_anchor_count": 8,
        "v13_acquisition_summary_attached": True,
        "v13_gate_status": "data_acquisition_needed",
        "promotion_gate": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _write_lava_npz_validation(path) -> object:
    path.write_text(json.dumps(_valid_lava_npz_validation()), encoding="utf-8")
    return path


def _candidate_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            _candidate_row(
                candidate_family="v2_plus_default",
                candidate_model_name="baseline",
                regret_uah=10.0,
                dispatch_mw_vector=[0.0, 0.25, -0.25],
            ),
            _candidate_row(
                candidate_family="poland_shadow_candidate",
                candidate_model_name="neighbor",
                regret_uah=12.0,
                dispatch_mw_vector=[0.0, 0.1, -0.1],
            ),
        ]
    )


def _candidate_row(
    *,
    candidate_family: str,
    candidate_model_name: str,
    regret_uah: float,
    dispatch_mw_vector: list[float],
) -> dict[str, object]:
    return {
        "tenant_id": "client_001_kyiv_mall",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "anchor_timestamp": "2026-01-01T23:00:00",
        "split_name": "train_selection",
        "eligible_for_final_selection": True,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "actual_price_uah_mwh_vector": [1000.0, 1200.0, 900.0],
        "dispatch_mw_vector": dispatch_mw_vector,
        "regret_uah": regret_uah,
        "market_execution_enabled": False,
        "selector_feature_schedule_distance_from_v2_plus": 0.0,
        "selector_feature_total_throughput_delta_mwh": 0.0,
        "selector_feature_terminal_soc_delta_fraction": 0.0,
        "selector_feature_forecast_spread_uah_mwh": 50.0,
        "selector_feature_total_degradation_penalty_uah": 1.0,
        "selector_feature_poland_shadow_candidate": (
            1.0 if candidate_family == "poland_shadow_candidate" else 0.0
        ),
        "selector_feature_oracle_train_diagnostic": 0.0,
    }
