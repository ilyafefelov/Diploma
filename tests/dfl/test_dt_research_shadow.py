from __future__ import annotations

from datetime import datetime, timedelta
import importlib.util
import json

import numpy as np
import polars as pl

from smart_arbitrage.dfl.dt_research_shadow import (
    build_dt_research_shadow_teacher_rows_from_candidate_library,
    build_dt_research_shadow_sequence_packet,
    run_dt_research_shadow_smoke,
    validate_dt_research_shadow_evaluation_packet,
    validate_dt_research_shadow_sequence_packet,
    write_dt_research_shadow_sequence_packet,
)
from scripts.materialize_dt_research_shadow_packet import (
    main as materialize_dt_research_shadow_packet,
)


def test_dt_research_shadow_sequence_dataset_splits_chronologically_without_promotion(
    tmp_path,
) -> None:
    frame = _teacher_rows()

    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=frame,
        run_slug="dt-shadow-test",
        context_length=3,
    )
    validation = validate_dt_research_shadow_sequence_packet(packet)
    paths = write_dt_research_shadow_sequence_packet(
        output_dir=tmp_path,
        packet=packet,
        teacher_rows_frame=frame,
    )

    summary = packet["dataset_summary"]
    split = packet["split_metadata"]
    boundary = packet["claim_boundary"]
    state_contract = packet["dt_state_feature_contract"]
    reward_contract = packet["dt_reward_target_contract"]

    assert "forecast_family_nbeatsx_flag" in packet["state_feature_names"]
    assert "forecast_family_tft_flag" in packet["state_feature_names"]
    assert state_contract["state_contract_passed"] is True
    assert state_contract["state_dim"] == len(packet["state_feature_names"])
    assert state_contract["required_state_context_groups"] == [
        "forecast_context",
        "battery_soc_context",
        "tenant_context",
        "candidate_value_regret_context",
        "gate_context",
    ]
    assert state_contract["missing_state_context_groups"] == []
    assert reward_contract["reward_contract_passed"] is True
    assert reward_contract["return_to_go_target"] == (
        "negative_regret_delta_vs_v2_plus_or_strict_reference"
    )
    assert reward_contract["schedule_value_available"] is True
    assert reward_contract["regret_delta_available"] is True
    assert reward_contract["market_execution_enabled"] is False
    assert summary["available_teacher_rows"] == 8
    assert summary["train_selection_rows"] == 6
    assert summary["research_shadow_training_rows"] == 6
    assert summary["promotable_v13_permitted_training_rows"] == 0
    assert summary["forecast_context_required_families"] == ["nbeatsx", "tft"]
    assert summary["forecast_context_present_families"] == ["nbeatsx"]
    assert summary["forecast_context_missing_families"] == ["tft"]
    assert summary["forecast_context_coverage_passed"] is False
    assert summary["forecast_context_coverage_status"] == "partial_missing_tft"
    assert summary["v13_training_permission_gate_passed"] is False
    assert summary["dt_promotion_gate_passed"] is False
    assert summary["market_execution_enabled"] is False
    assert boundary["forecast_context_required_families"] == ["nbeatsx", "tft"]
    assert boundary["forecast_context_coverage_required_for_full_dt_prototype"] is True
    assert split == {
        "split_strategy": "chronological_delivery_timestamp",
        "chronological_split_passed": True,
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "market_availability_claim": False,
        "research_shadow_not_promotable": True,
        "market_execution_enabled": False,
    }
    assert boundary["action_target"] == "candidate_index_or_schedule_family"
    assert boundary["raw_hourly_buy_sell_hold_action_target"] is False
    assert boundary["action_target_value_columns"] == [
        "dt_candidate_id_target",
        "dt_candidate_index_target",
        "dt_schedule_family_target",
    ]
    assert boundary["action_target_excludes_raw_hourly_buy_sell_hold"] is True
    assert boundary["v2_plus_role"] == "teacher_comparator_fallback"
    assert validation["passed"] is True
    assert validation["gate_results"]["chronological_split"]["passed"] is True
    assert validation["gate_results"]["research_shadow_training_rows"]["passed"] is True
    assert validation["gate_results"]["promotable_v13_blocked"]["passed"] is True
    assert validation["gate_results"]["state_feature_contract"]["passed"] is True
    assert validation["gate_results"]["reward_target_contract"]["passed"] is True

    npz = np.load(paths["sequence_npz"], allow_pickle=True)
    assert npz["states"].shape == (4, 3, len(packet["state_feature_names"]))
    assert npz["actions"].shape == (4, 3)
    assert npz["candidate_id_targets"].shape == (4, 3)
    assert npz["schedule_family_targets"].shape == (4, 3)
    assert str(npz["candidate_id_targets"][0, 0]).endswith("|frozen_v2_plus_fallback")
    assert npz["schedule_family_targets"][0].tolist() == [
        "frozen_v2_plus_fallback",
        "strict_control",
        "",
    ]
    assert npz["action_target_value_columns"].tolist() == [
        "dt_candidate_id_target",
        "dt_candidate_index_target",
        "dt_schedule_family_target",
    ]
    assert npz["returns_to_go"].shape == (4, 3, 1)
    assert npz["candidate_mask"].shape == (4, 3)
    assert npz["action_feasibility_mask"].shape == (4, 3, 2)
    assert npz["action_feasibility_mask"][0, 0].tolist() == [True, True]
    assert npz["action_feasibility_mask"][0, 1].tolist() == [True, True]
    assert npz["action_feasibility_mask"][0, 2].tolist() == [False, False]
    assert npz["split_names"].tolist() == ["train", "train", "train", "evaluation"]
    assert npz["forecast_context_required_families"].tolist() == ["nbeatsx", "tft"]
    assert npz["forecast_context_present_families"].tolist() == ["nbeatsx"]
    assert npz["forecast_context_missing_families"].tolist() == ["tft"]
    assert npz["forecast_context_coverage_passed"].item() is False
    assert npz["state_contract_passed"].item() is True
    assert npz["reward_contract_passed"].item() is True
    assert npz["state_context_groups"].tolist() == [
        "forecast_context",
        "battery_soc_context",
        "tenant_context",
        "candidate_value_regret_context",
        "gate_context",
    ]
    assert str(npz["return_to_go_target"].item()) == (
        "negative_regret_delta_vs_v2_plus_or_strict_reference"
    )
    assert npz["publication_receipt_verified"].item() is False
    assert npz["research_shadow_not_promotable"].item() is True
    assert npz["market_execution_enabled"].item() is False


def test_dt_research_shadow_sequence_dataset_detects_full_nbeatsx_tft_context() -> None:
    dual_source_rows = pl.concat(
        [
            _teacher_rows(),
            _teacher_rows().with_columns(
                pl.lit(
                    "tft_official_global_panel_v1_horizon_quantile_calibrated_v1"
                ).alias("source_model_name")
            ),
        ]
    )

    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=dual_source_rows,
        run_slug="dt-shadow-dual-source-test",
        context_length=3,
    )

    summary = packet["dataset_summary"]
    assert summary["source_model_count"] == 2
    assert summary["forecast_context_present_families"] == ["nbeatsx", "tft"]
    assert summary["forecast_context_missing_families"] == []
    assert summary["forecast_context_coverage_passed"] is True
    assert summary["forecast_context_coverage_status"] == "complete_nbeatsx_tft"


def test_dt_research_shadow_adapts_tft_candidate_library_as_non_promotable_context(
    tmp_path,
) -> None:
    adapted_tft_rows = build_dt_research_shadow_teacher_rows_from_candidate_library(
        candidate_library_frame=_tft_candidate_library_rows()
    )
    combined_rows = pl.concat(
        [_teacher_rows(), adapted_tft_rows],
        how="diagonal_relaxed",
    )

    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=combined_rows,
        run_slug="dt-shadow-adapted-tft-context-test",
        context_length=3,
    )
    validation = validate_dt_research_shadow_sequence_packet(packet)
    paths = write_dt_research_shadow_sequence_packet(
        output_dir=tmp_path,
        packet=packet,
        teacher_rows_frame=combined_rows,
    )

    assert adapted_tft_rows.height == 8
    assert adapted_tft_rows.select(pl.col("permitted_model_training_row").any()).item() is False
    assert (
        adapted_tft_rows.select(pl.col("v13_training_permission_gate_passed").any()).item()
        is False
    )
    assert adapted_tft_rows.select(pl.col("market_execution_enabled").any()).item() is False
    assert adapted_tft_rows.select(pl.col("not_market_execution").all()).item() is True
    assert adapted_tft_rows.select(pl.col("raw_hourly_action_imitation").any()).item() is False
    assert adapted_tft_rows["research_shadow_source_kind"].unique().to_list() == [
        "credentialless_candidate_library_research_shadow_adapter"
    ]

    summary = packet["dataset_summary"]
    reward_contract = packet["dt_reward_target_contract"]
    assert summary["available_teacher_rows"] == 16
    assert summary["train_selection_rows"] == 12
    assert summary["research_shadow_training_rows"] == 12
    assert summary["promotable_v13_permitted_training_rows"] == 0
    assert summary["forecast_context_present_families"] == ["nbeatsx", "tft"]
    assert summary["forecast_context_missing_families"] == []
    assert summary["forecast_context_coverage_passed"] is True
    assert summary["forecast_context_coverage_status"] == "complete_nbeatsx_tft"
    assert reward_contract["reward_reference_values"] == [
        "strict_control_fallback_no_v2_plus_candidate",
        "v2_plus_teacher_comparator_fallback",
    ]
    assert validation["passed"] is True

    npz = np.load(paths["sequence_npz"], allow_pickle=True)
    assert npz["forecast_context_coverage_passed"].item() is True
    assert npz["forecast_context_present_families"].tolist() == ["nbeatsx", "tft"]
    assert npz["promotable_v13_permitted_training_rows"].item() == 0
    assert npz["market_execution_enabled"].item() is False


def test_dt_research_shadow_smoke_trains_transformer_and_reports_regret_controls(
    tmp_path,
) -> None:
    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="dt-shadow-smoke",
        context_length=3,
    )
    paths = write_dt_research_shadow_sequence_packet(
        output_dir=tmp_path,
        packet=packet,
        teacher_rows_frame=_teacher_rows(),
    )

    smoke = run_dt_research_shadow_smoke(
        sequence_npz_path=paths["sequence_npz"],
        output_dir=tmp_path,
        model_backbone="local",
        max_epochs=2,
        hidden_dim=16,
        num_layers=1,
        num_heads=2,
        seed=7,
    )

    summary = json.loads(smoke["summary_json"].read_text(encoding="utf-8"))
    evaluation = json.loads(
        smoke["evaluation_summary_json"].read_text(encoding="utf-8")
    )
    evaluation_validation = json.loads(
        smoke["evaluation_validation_json"].read_text(encoding="utf-8")
    )

    assert summary["claim_scope"] == (
        "dt_research_shadow_transformer_smoke_not_promotable_not_market_execution"
    )
    assert summary["requested_model_backbone"] == "local"
    assert summary["model_backbone"] == "local_dt_compatible_transformer_classifier"
    assert summary["model_backbone_selection_reason"] == "local_requested"
    assert summary["loss_function"] == "cross_entropy_candidate_index"
    assert summary["dt_tensor_contract"]["states_shape"] == [4, 3, 20]
    assert summary["dt_tensor_contract"]["actions_shape"] == [4, 3]
    assert summary["dt_tensor_contract"]["candidate_id_targets_shape"] == [4, 3]
    assert summary["dt_tensor_contract"]["schedule_family_targets_shape"] == [4, 3]
    assert summary["dt_tensor_contract"]["candidate_id_targets_attached"] is True
    assert summary["dt_tensor_contract"]["schedule_family_targets_attached"] is True
    assert summary["dt_tensor_contract"]["returns_to_go_shape"] == [4, 3, 1]
    assert summary["dt_tensor_contract"]["candidate_mask_attached"] is True
    assert summary["dt_tensor_contract"]["action_feasibility_mask_attached"] is True
    assert summary["dt_tensor_contract"]["action_feasibility_mask_shape"] == [4, 3, 2]
    assert summary["dt_tensor_contract"]["action_feasibility_mask_applied_to_loss"] is True
    assert summary["dt_tensor_contract"]["action_feasibility_mask_applied_to_eval"] is True
    assert summary["dt_tensor_contract"]["state_contract_passed"] is True
    assert summary["dt_tensor_contract"]["reward_contract_passed"] is True
    assert summary["dt_tensor_contract"]["return_to_go_target"] == (
        "negative_regret_delta_vs_v2_plus_or_strict_reference"
    )
    assert summary["train_sequence_count"] == 3
    assert summary["evaluation_sequence_count"] == 1
    assert summary["research_shadow_training_rows"] == 6
    assert summary["promotable_v13_permitted_training_rows"] == 0
    assert summary["deterministic_safety_projection_passed"] is True
    assert summary["dt_promotion_gate_passed"] is False
    assert summary["market_execution_enabled"] is False
    assert summary["evaluation_metrics"]["dt_selected_mean_regret_uah"] >= 0.0
    assert summary["evaluation_metrics"]["dt_selected_mean_value_uah"] >= 0.0
    assert summary["evaluation_metrics"]["v2_plus_mean_regret_uah"] >= 0.0
    assert summary["evaluation_metrics"]["v2_plus_mean_value_uah"] >= 0.0
    assert summary["evaluation_metrics"]["strict_mean_regret_uah"] >= 0.0
    assert summary["evaluation_metrics"]["strict_mean_value_uah"] >= 0.0
    assert summary["evaluation_metrics"]["behavior_cloning_mean_regret_uah"] >= 0.0
    assert summary["evaluation_metrics"]["behavior_cloning_mean_value_uah"] >= 0.0
    assert summary["evaluation_metrics"]["infeasible_action_prediction_count"] == 0
    assert summary["evaluation_metrics"]["accuracy_secondary"] >= 0.0
    assert summary["comparison_controls"] == [
        "strict_lp_oracle_reference",
        "schedule_value_learner_v2_plus_teacher_comparator_fallback",
        "behavior_cloning_majority_family_reference",
    ]
    assert summary["evaluation_packet_summary"] == {
        "claim_scope": (
            "dt_research_shadow_evaluation_packet_not_promotable_not_market_execution"
        ),
        "primary_metric": "regret_value_vs_strict_v2_plus_behavior",
        "summary_json": "dt_research_shadow_evaluation_summary.json",
        "validation_json": "dt_research_shadow_evaluation_validation.json",
        "validation_passed": True,
        "market_execution_enabled": False,
    }
    assert summary["attached_artifacts"]["evaluation_summary_json"] == (
        "dt_research_shadow_evaluation_summary.json"
    )
    assert evaluation["claim_scope"] == (
        "dt_research_shadow_evaluation_packet_not_promotable_not_market_execution"
    )
    assert evaluation["primary_metric"] == "regret_value_vs_strict_v2_plus_behavior"
    assert evaluation["comparison_controls"] == summary["comparison_controls"]
    assert evaluation["evaluation_metrics"] == summary["evaluation_metrics"]
    assert evaluation["regret_value_deltas"]["dt_minus_strict_regret_uah"] == (
        summary["evaluation_metrics"]["dt_selected_mean_regret_uah"]
        - summary["evaluation_metrics"]["strict_mean_regret_uah"]
    )
    assert evaluation["regret_value_deltas"]["dt_minus_v2_plus_value_uah"] == (
        summary["evaluation_metrics"]["dt_selected_mean_value_uah"]
        - summary["evaluation_metrics"]["v2_plus_mean_value_uah"]
    )
    assert evaluation["deterministic_safety_projection_passed"] is True
    assert evaluation["dt_promotion_gate_passed"] is False
    assert evaluation["market_execution_enabled"] is False
    assert evaluation_validation["claim_scope"] == (
        "dt_research_shadow_evaluation_validation_not_market_execution"
    )
    assert evaluation_validation["passed"] is True
    assert evaluation_validation["failures"] == []
    assert evaluation_validation["gate_results"]["regret_value_metrics"]["passed"] is True
    assert evaluation_validation["gate_results"]["no_market_execution"]["passed"] is True


def test_dt_research_shadow_smoke_masks_infeasible_candidate_classes(
    tmp_path,
) -> None:
    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=_teacher_rows_with_missing_candidate_class(),
        run_slug="dt-shadow-feasibility-mask",
        context_length=3,
    )
    paths = write_dt_research_shadow_sequence_packet(
        output_dir=tmp_path,
        packet=packet,
        teacher_rows_frame=_teacher_rows_with_missing_candidate_class(),
    )
    npz = np.load(paths["sequence_npz"], allow_pickle=True)

    assert npz["action_dim"].item() == 3
    assert npz["action_feasibility_mask"].shape == (4, 3, 3)
    assert npz["action_feasibility_mask"][0, 0].tolist() == [True, False, True]
    assert npz["action_feasibility_mask"][0, 1].tolist() == [True, False, True]
    assert npz["action_feasibility_mask"][0, 2].tolist() == [False, False, False]

    smoke = run_dt_research_shadow_smoke(
        sequence_npz_path=paths["sequence_npz"],
        output_dir=tmp_path,
        model_backbone="local",
        max_epochs=1,
        hidden_dim=16,
        num_layers=1,
        num_heads=2,
        seed=37,
    )

    summary = json.loads(smoke["summary_json"].read_text(encoding="utf-8"))
    evaluation = json.loads(
        smoke["evaluation_summary_json"].read_text(encoding="utf-8")
    )
    evaluation_validation = json.loads(
        smoke["evaluation_validation_json"].read_text(encoding="utf-8")
    )

    assert summary["dt_tensor_contract"]["action_feasibility_mask_attached"] is True
    assert summary["dt_tensor_contract"]["action_feasibility_mask_shape"] == [4, 3, 3]
    assert summary["dt_tensor_contract"]["action_feasibility_mask_applied_to_loss"] is True
    assert summary["dt_tensor_contract"]["action_feasibility_mask_applied_to_eval"] is True
    assert summary["evaluation_metrics"]["infeasible_action_prediction_count"] == 0
    assert evaluation["candidate_feasibility_mask_attached"] is True
    assert evaluation["infeasible_action_prediction_count"] == 0
    assert evaluation_validation["gate_results"]["candidate_feasibility_mask"]["passed"] is True


def test_dt_research_shadow_evaluation_validation_rejects_market_execution(
    tmp_path,
) -> None:
    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="dt-shadow-eval-validation",
        context_length=3,
    )
    paths = write_dt_research_shadow_sequence_packet(
        output_dir=tmp_path,
        packet=packet,
        teacher_rows_frame=_teacher_rows(),
    )
    smoke = run_dt_research_shadow_smoke(
        sequence_npz_path=paths["sequence_npz"],
        output_dir=tmp_path,
        model_backbone="local",
        max_epochs=1,
        hidden_dim=16,
        num_layers=1,
        num_heads=2,
        seed=17,
    )
    evaluation = json.loads(
        smoke["evaluation_summary_json"].read_text(encoding="utf-8")
    )
    evaluation["market_execution_enabled"] = True

    validation = validate_dt_research_shadow_evaluation_packet(evaluation)

    assert validation["passed"] is False
    assert "nested_market_execution_enabled_true" in validation["failures"]
    assert validation["gate_results"]["no_market_execution"]["passed"] is False


def test_dt_research_shadow_smoke_reports_hf_backend_availability(tmp_path) -> None:
    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="dt-shadow-backbone",
        context_length=3,
    )
    paths = write_dt_research_shadow_sequence_packet(
        output_dir=tmp_path,
        packet=packet,
        teacher_rows_frame=_teacher_rows(),
    )

    smoke = run_dt_research_shadow_smoke(
        sequence_npz_path=paths["sequence_npz"],
        output_dir=tmp_path,
        max_epochs=1,
        hidden_dim=16,
        num_layers=1,
        num_heads=2,
        seed=11,
    )

    summary = json.loads(smoke["summary_json"].read_text(encoding="utf-8"))
    transformers_installed = importlib.util.find_spec("transformers") is not None

    assert summary["requested_model_backbone"] == "auto"
    assert summary["hf_decision_transformer_available"] is transformers_installed
    if transformers_installed:
        assert summary["model_backbone"] == "huggingface_decision_transformer_model"
        assert summary["model_backbone_selection_reason"] == "hf_available_auto_selected"
    else:
        assert summary["model_backbone"] == "local_dt_compatible_transformer_classifier"
        assert summary["model_backbone_selection_reason"] == "transformers_not_installed"
    assert summary["market_execution_enabled"] is False
    assert summary["dt_promotion_gate_passed"] is False


def test_dt_research_shadow_cli_merges_candidate_library_context(tmp_path) -> None:
    teacher_csv = tmp_path / "teacher_rows.csv"
    candidate_library_csv = tmp_path / "tft_candidate_library_rows.csv"
    output_dir = tmp_path / "packet"
    _csv_ready(_teacher_rows()).write_csv(teacher_csv)
    _csv_ready(_tft_candidate_library_rows()).write_csv(candidate_library_csv)

    exit_code = materialize_dt_research_shadow_packet(
        [
            "--teacher-rows-csv",
            str(teacher_csv),
            "--candidate-library-csv",
            str(candidate_library_csv),
            "--output-dir",
            str(output_dir),
            "--run-slug",
            "dt-shadow-cli-merge-test",
            "--context-length",
            "3",
            "--max-epochs",
            "1",
            "--hidden-dim",
            "16",
            "--num-layers",
            "1",
            "--num-heads",
            "2",
            "--seed",
            "9",
        ]
    )

    assert exit_code == 0
    sequence_summary = json.loads(
        (output_dir / "dt_research_shadow_sequence_summary.json").read_text(
            encoding="utf-8"
        )
    )
    smoke_summary = json.loads(
        (output_dir / "dt_research_shadow_smoke_summary.json").read_text(
            encoding="utf-8"
        )
    )
    evaluation_summary = json.loads(
        (output_dir / "dt_research_shadow_evaluation_summary.json").read_text(
            encoding="utf-8"
        )
    )
    evaluation_validation = json.loads(
        (output_dir / "dt_research_shadow_evaluation_validation.json").read_text(
            encoding="utf-8"
        )
    )
    assert sequence_summary["dataset_summary"]["available_teacher_rows"] == 16
    assert (
        sequence_summary["dataset_summary"]["forecast_context_coverage_status"]
        == "complete_nbeatsx_tft"
    )
    assert sequence_summary["dataset_summary"]["promotable_v13_permitted_training_rows"] == 0
    assert smoke_summary["research_shadow_training_rows"] == 12
    assert smoke_summary["promotable_v13_permitted_training_rows"] == 0
    assert smoke_summary["market_execution_enabled"] is False
    assert evaluation_summary["primary_metric"] == (
        "regret_value_vs_strict_v2_plus_behavior"
    )
    assert evaluation_summary["market_execution_enabled"] is False
    assert evaluation_validation["passed"] is True
    assert evaluation_validation["market_execution_enabled"] is False


def _teacher_rows() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 1, 1, 23)
    for anchor_index in range(4):
        split_name = "train_selection" if anchor_index < 3 else "final_holdout"
        anchor = start + timedelta(days=anchor_index)
        rows.extend(
            [
                _row(
                    anchor=anchor,
                    split_name=split_name,
                    candidate_index=0,
                    family="frozen_v2_plus_fallback",
                    regret=100.0 + anchor_index,
                    value=900.0 - anchor_index,
                    regret_delta=0.0,
                ),
                _row(
                    anchor=anchor,
                    split_name=split_name,
                    candidate_index=1,
                    family="strict_control",
                    regret=140.0 + anchor_index,
                    value=860.0 - anchor_index,
                    regret_delta=40.0,
                ),
            ]
        )
    return pl.DataFrame(rows)


def _teacher_rows_with_missing_candidate_class() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 1, 1, 23)
    for anchor_index in range(4):
        split_name = "train_selection" if anchor_index < 3 else "final_holdout"
        anchor = start + timedelta(days=anchor_index)
        rows.extend(
            [
                _row(
                    anchor=anchor,
                    split_name=split_name,
                    candidate_index=0,
                    family="frozen_v2_plus_fallback",
                    regret=100.0 + anchor_index,
                    value=900.0 - anchor_index,
                    regret_delta=0.0,
                ),
                _row(
                    anchor=anchor,
                    split_name=split_name,
                    candidate_index=2,
                    family="tail_risk_aware_schedule",
                    regret=90.0 + anchor_index,
                    value=910.0 - anchor_index,
                    regret_delta=-10.0,
                ),
            ]
        )
    return pl.DataFrame(rows)


def _csv_ready(frame: pl.DataFrame) -> pl.DataFrame:
    vector_columns = [
        "forecast_price_uah_mwh_vector",
        "actual_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
    ]
    return frame.with_columns(
        [
            pl.col(column)
            .map_elements(_json_vector, return_dtype=pl.String)
            .alias(column)
            for column in vector_columns
            if column in frame.columns
        ]
    )


def _json_vector(value: object) -> str:
    if isinstance(value, pl.Series):
        return json.dumps(value.to_list())
    return json.dumps(value)


def _tft_candidate_library_rows() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 1, 1, 23)
    for anchor_index in range(4):
        split_name = "train_selection" if anchor_index < 3 else "final_holdout"
        anchor = start + timedelta(days=anchor_index)
        rows.extend(
            [
                _candidate_library_row(
                    anchor=anchor,
                    split_name=split_name,
                    candidate_model_name="tft_strict_control",
                    family="strict_control",
                    regret=130.0 + anchor_index,
                    value=870.0 - anchor_index,
                ),
                _candidate_library_row(
                    anchor=anchor,
                    split_name=split_name,
                    candidate_model_name="tft_quantile_p50_schedule",
                    family="quantile_p50_schedule",
                    regret=95.0 + anchor_index,
                    value=905.0 - anchor_index,
                ),
            ]
        )
    return pl.DataFrame(rows)


def _candidate_library_row(
    *,
    anchor: datetime,
    split_name: str,
    candidate_model_name: str,
    family: str,
    regret: float,
    value: float,
) -> dict[str, object]:
    return {
        "tenant_id": "client_004_kharkiv_hospital",
        "source_model_name": "tft_official_global_panel_p50_quantile_calibrated_v1",
        "anchor_timestamp": anchor,
        "split_name": split_name,
        "horizon_hours": 24,
        "candidate_family": family,
        "candidate_model_name": candidate_model_name,
        "forecast_price_uah_mwh_vector": [1020.0, 1380.0],
        "actual_price_uah_mwh_vector": [1000.0, 1400.0],
        "dispatch_mw_vector": [0.2, -0.2],
        "soc_fraction_vector": [0.5, 0.62],
        "decision_value_uah": value,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "forecast_spread_uah_mwh": 360.0,
        "forecast_top_k_actual_overlap": 0.5,
        "forecast_bottom_k_actual_overlap": 0.5,
        "soc_min_slack_fraction": 0.22,
        "safety_violation_count": 0,
        "total_throughput_mwh": 0.4,
        "total_degradation_penalty_uah": 2.5,
        "market_execution_enabled": False,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _row(
    *,
    anchor: datetime,
    split_name: str,
    candidate_index: int,
    family: str,
    regret: float,
    value: float,
    regret_delta: float,
) -> dict[str, object]:
    return {
        "tenant_id": "client_004_kharkiv_hospital",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "anchor_timestamp": anchor,
        "split_name": split_name,
        "horizon_hours": 24,
        "forecast_price_uah_mwh_vector": [1000.0, 1400.0],
        "dispatch_mw_vector": [0.1, -0.1],
        "soc_fraction_vector": [0.5, 0.6],
        "decision_value_uah": value,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "forecast_spread_uah_mwh": 400.0,
        "forecast_top_k_actual_overlap": 0.5,
        "forecast_bottom_k_actual_overlap": 0.5,
        "soc_min_slack_fraction": 0.2,
        "safety_violation_count": 0,
        "selector_feature_forecast_spread_uah_mwh": 400.0,
        "selector_feature_terminal_soc_delta_fraction": 0.1,
        "selector_feature_total_throughput_delta_mwh": 0.0,
        "selector_feature_total_degradation_penalty_uah": 2.0,
        "market_execution_enabled": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "raw_hourly_action_imitation": False,
        "dt_candidate_index_target": candidate_index,
        "dt_candidate_id_target": f"{anchor.isoformat()}|{family}",
        "dt_schedule_family_target": family,
        "return_to_go_regret_target_uah": -regret_delta,
        "regret_delta_vs_v2_plus_uah": regret_delta,
        "schedule_value_uah": value,
        "dfl_input_contract": (
            "calibrated_forecasts_tenant_soc_context_feasible_candidate_schedules"
        ),
        "dfl_target_contract": "best_candidate_schedule_value_regret_delta_vs_v2_plus",
        "dt_input_contract": (
            "v13_teacher_sequence_forecast_battery_tenant_candidate_value_return_to_go"
        ),
        "dt_action_target_contract": "candidate_id_or_schedule_family",
        "v2_plus_role": "teacher_comparator_fallback",
        "v13_training_permission_gate_passed": False,
        "v13_blocking_context_families": "explicit_dam_publication_receipts",
        "permitted_model_training_row": False,
        "permits_model_training": False,
        "training_blocker": "v13_training_permission_gate_blocked",
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "not_deployed_dt_control": True,
    }
