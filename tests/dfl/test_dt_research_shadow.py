from __future__ import annotations

from datetime import datetime, timedelta
import importlib.util
import json

import numpy as np
import polars as pl
import torch

from smart_arbitrage.dfl.dt_research_shadow import (
    build_dt_research_shadow_teacher_rows_from_candidate_library,
    build_dt_research_shadow_teacher_rows_from_temporal_v2_plus_strict_rows,
    build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows,
    build_dt_research_shadow_sequence_packet,
    run_dt_research_shadow_smoke,
    validate_dt_research_shadow_evaluation_packet,
    validate_dt_research_shadow_sequence_packet,
    write_dt_research_shadow_sequence_packet,
)
from scripts.materialize_dt_research_shadow_packet import (
    main as materialize_dt_research_shadow_packet,
)
from scripts.materialize_dt_v2_plus_apples_to_apples_packet import (
    main as materialize_dt_v2_plus_apples_to_apples_packet,
)
from scripts.materialize_dt_v2_plus_distillation_shadow_packet import (
    main as materialize_dt_v2_plus_distillation_shadow_packet,
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
    selected_preview = json.loads(
        smoke["selected_preview_json"].read_text(encoding="utf-8")
    )

    assert summary["claim_scope"] == (
        "dt_research_shadow_transformer_smoke_not_promotable_not_market_execution"
    )
    assert summary["requested_model_backbone"] == "local"
    assert summary["model_backbone"] == "local_dt_compatible_transformer_classifier"
    assert summary["model_backbone_selection_reason"] == "local_requested"
    assert summary["loss_function"] == (
        "cross_entropy_candidate_index_plus_decision_aware_regret_value_ranking"
    )
    assert summary["training_objective"]["objective_kind"] == (
        "decision_aware_regret_value_ranking"
    )
    assert summary["training_objective"]["cross_entropy_weight"] == 1.0
    assert summary["training_objective"]["decision_aware_ranking_weight"] == 1.0
    assert summary["selection_policy"]["v2_plus_default_fallback"] is True
    assert summary["selection_policy"]["market_execution_enabled"] is False
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
    assert summary["evaluation_metrics"]["dt_selected_median_regret_uah"] >= 0.0
    assert summary["evaluation_metrics"]["v2_plus_median_regret_uah"] >= 0.0
    assert summary["evaluation_metrics"]["strict_median_regret_uah"] >= 0.0
    assert summary["evaluation_metrics"]["infeasible_action_prediction_count"] == 0
    assert summary["evaluation_metrics"]["abstention_count"] >= 0.0
    assert summary["evaluation_metrics"]["non_v2_plus_switch_count"] >= 0.0
    assert summary["evaluation_metrics"]["switch_win_count"] >= 0.0
    assert summary["evaluation_metrics"]["switch_loss_count"] >= 0.0
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
    assert summary["attached_artifacts"]["selected_preview_json"] == (
        "dt_research_shadow_selected_schedule_preview.json"
    )
    assert selected_preview["claim_scope"] == (
        "dt_research_shadow_selected_schedule_preview_not_promotable_not_market_execution"
    )
    assert selected_preview["action_target"] == "candidate_index_or_schedule_family"
    assert selected_preview["raw_hourly_buy_sell_hold_action_target"] is False
    assert selected_preview["market_execution_enabled"] is False
    assert selected_preview["dt_promotion_gate_passed"] is False
    assert selected_preview["research_shadow_not_promotable"] is True
    assert selected_preview["preview_rows"][0]["selected_candidate_id"]
    assert selected_preview["preview_rows"][0]["selected_schedule_family"]
    assert selected_preview["preview_rows"][0]["market_execution_enabled"] is False
    assert "abstained_to_v2_plus" in selected_preview["preview_rows"][0]
    assert "predicted_improvement_vs_v2_plus_uah" in selected_preview["preview_rows"][0]
    assert "tail_risk_guard_passed" in selected_preview["preview_rows"][0]
    assert "family_tail_risk_probability" in selected_preview["preview_rows"][0]
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


def test_dt_research_shadow_smoke_conservative_selector_abstains_to_v2_plus(
    tmp_path,
    monkeypatch,
) -> None:
    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="dt-shadow-conservative-selector",
        context_length=3,
    )
    paths = write_dt_research_shadow_sequence_packet(
        output_dir=tmp_path,
        packet=packet,
        teacher_rows_frame=_teacher_rows(),
    )

    import smart_arbitrage.dfl.dt_research_shadow as dt_shadow_module

    def _fake_policy_logits(**kwargs) -> torch.Tensor:
        states = kwargs["states"]
        action_dim = int(kwargs["previous_actions"].shape[-1])
        logits = torch.zeros(
            (states.shape[0], states.shape[1], action_dim),
            dtype=torch.float32,
        )
        if action_dim >= 2:
            logits[:, :, 1] = 12.0
            logits[:, :, 0] = 0.5
        return logits.requires_grad_()

    monkeypatch.setattr(dt_shadow_module, "_dt_policy_logits", _fake_policy_logits)

    smoke = run_dt_research_shadow_smoke(
        sequence_npz_path=paths["sequence_npz"],
        output_dir=tmp_path,
        model_backbone="local",
        max_epochs=1,
        hidden_dim=16,
        num_layers=1,
        num_heads=2,
        seed=11,
        min_predicted_improvement_uah=10_000.0,
        max_family_tail_risk_probability=0.01,
        objective_kind="decision_aware_regret_value_ranking",
    )

    selected_preview = json.loads(
        smoke["selected_preview_json"].read_text(encoding="utf-8")
    )
    assert selected_preview["preview_rows"]
    row = selected_preview["preview_rows"][0]
    assert row["abstained_to_v2_plus"] is True
    assert row["abstention_reason"] == "predicted_improvement_below_threshold"
    assert row["selected_schedule_family"] == "frozen_v2_plus_fallback"
    assert row["selected_candidate_id"] == row["v2_plus_candidate_id"]
    assert row["tail_risk_guard_passed"] is True
    assert row["market_execution_enabled"] is False


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


def test_dt_research_shadow_smoke_saves_loadable_non_promotable_checkpoint(
    tmp_path,
) -> None:
    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="dt-shadow-checkpoint",
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
        seed=23,
        model_backbone="local",
        save_checkpoint=True,
    )

    summary = json.loads(smoke["summary_json"].read_text(encoding="utf-8"))
    checkpoint = summary["checkpoint"]

    assert checkpoint["saved"] is True
    assert checkpoint["format"] == "torch_state_dict"
    assert checkpoint["load_smoke_passed"] is True
    assert checkpoint["market_execution_enabled"] is False
    assert checkpoint["dt_promotion_gate_passed"] is False
    assert checkpoint["permits_model_training"] is False
    assert smoke["checkpoint_dir"] == tmp_path / "dt_research_shadow_model_checkpoint"
    assert (smoke["checkpoint_dir"] / "model_checkpoint.pt").exists()
    assert (smoke["checkpoint_dir"] / "checkpoint_metadata.json").exists()


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
    selected_preview = json.loads(
        (output_dir / "dt_research_shadow_selected_schedule_preview.json").read_text(
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
    assert selected_preview["market_execution_enabled"] is False
    assert selected_preview["preview_rows"]
    assert selected_preview["preview_rows"][0]["selected_candidate_id"]
    assert evaluation_validation["passed"] is True
    assert evaluation_validation["market_execution_enabled"] is False


def test_dt_research_shadow_cli_can_persist_non_promotable_checkpoint(
    tmp_path,
) -> None:
    teacher_csv = tmp_path / "teacher_rows.csv"
    output_dir = tmp_path / "packet"
    _csv_ready(_teacher_rows()).write_csv(teacher_csv)

    exit_code = materialize_dt_research_shadow_packet(
        [
            "--teacher-rows-csv",
            str(teacher_csv),
            "--output-dir",
            str(output_dir),
            "--run-slug",
            "dt-shadow-cli-checkpoint-test",
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
            "29",
            "--model-backbone",
            "local",
            "--save-checkpoint",
        ]
    )

    smoke_summary = json.loads(
        (output_dir / "dt_research_shadow_smoke_summary.json").read_text(
            encoding="utf-8"
        )
    )

    assert exit_code == 0
    assert smoke_summary["checkpoint"]["saved"] is True
    assert smoke_summary["checkpoint"]["load_smoke_passed"] is True
    assert smoke_summary["checkpoint"]["market_execution_enabled"] is False
    assert smoke_summary["checkpoint"]["dt_promotion_gate_passed"] is False
    assert (
        output_dir / "dt_research_shadow_model_checkpoint" / "model_checkpoint.pt"
    ).exists()


def test_dt_research_shadow_adapts_real_v2_plus_strict_rows_for_apples_to_apples() -> None:
    teacher_rows = build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows(
        strict_rows_frame=_v2_plus_strict_rows(),
        regret_decomposition_frame=_v2_plus_regret_decomposition_rows(),
    )

    assert teacher_rows.height == 16
    assert set(teacher_rows["dt_schedule_family_target"].unique().to_list()) == {
        "raw_reference",
        "schedule_value_learner_v2_plus",
        "schedule_value_learner_v2_reference",
        "strict_reference",
    }
    assert set(teacher_rows["split_name"].unique().to_list()) == {
        "train_selection",
        "final_holdout",
    }
    assert teacher_rows.filter(pl.col("split_name") == "train_selection").height == 8
    assert teacher_rows.filter(pl.col("split_name") == "final_holdout").height == 8
    assert (
        teacher_rows.select(pl.col("market_execution_enabled").any()).item()
        is False
    )
    assert (
        teacher_rows.select(pl.col("promotion_gate_passed").any()).item()
        is False
    )
    assert teacher_rows["v2_plus_role"].unique().to_list() == [
        "real_schedule_value_learner_v2_plus_comparator"
    ]
    final_v2_mean = (
        teacher_rows.filter(
            (pl.col("split_name") == "final_holdout")
            & (pl.col("dt_schedule_family_target") == "schedule_value_learner_v2_plus")
        )["regret_uah"].mean()
    )
    assert final_v2_mean == 15.0
    assert "best_available_candidate_regret_uah" in teacher_rows.columns
    assert (
        teacher_rows.filter(
            pl.col("dt_schedule_family_target") == "schedule_value_learner_v2_plus"
        )["regret_delta_vs_v2_plus_uah"].to_list()
        == [0.0, 0.0, 0.0, 0.0]
    )


def test_dt_research_shadow_adapts_distinct_temporal_strict_rows_without_mirroring() -> None:
    evaluation_rows = _v2_plus_strict_rows()
    training_rows = evaluation_rows.with_columns(
        (pl.col("anchor_timestamp") - pl.duration(days=60)).alias("anchor_timestamp"),
        (pl.col("regret_uah") + 5.0).alias("regret_uah"),
        (pl.col("decision_value_uah") - 5.0).alias("decision_value_uah"),
    )

    teacher_rows = build_dt_research_shadow_teacher_rows_from_temporal_v2_plus_strict_rows(
        training_strict_rows_frame=training_rows,
        evaluation_strict_rows_frame=evaluation_rows,
    )

    train = teacher_rows.filter(pl.col("split_name") == "train_selection")
    evaluation = teacher_rows.filter(pl.col("split_name") == "final_holdout")
    assert train.height == 8
    assert evaluation.height == 8
    assert train["anchor_timestamp"].max() < evaluation["anchor_timestamp"].min()
    assert set(train["research_shadow_source_kind"]) == {
        "v2_plus_strict_rows_temporal_training_adapter"
    }
    assert set(evaluation["research_shadow_source_kind"]) == {
        "v2_plus_strict_rows_temporal_evaluation_adapter"
    }


def test_dt_research_shadow_marks_single_v2_plus_distillation_target_per_candidate_set() -> None:
    teacher_rows = build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows(
        strict_rows_frame=_v2_plus_strict_rows(),
        regret_decomposition_frame=_v2_plus_regret_decomposition_rows(),
    )

    grouped = teacher_rows.group_by(
        ["tenant_id", "source_model_name", "anchor_timestamp", "split_name"]
    ).agg(
        pl.col("label_v2_plus_rule_distillation_target")
        .cast(pl.Int64)
        .sum()
        .alias("target_count"),
        pl.col("label_v2_plus_selected_candidate_id")
        .n_unique()
        .alias("selected_candidate_id_count"),
    )
    assert grouped.height == 4
    assert grouped["target_count"].to_list() == [1, 1, 1, 1]
    assert grouped["selected_candidate_id_count"].to_list() == [1, 1, 1, 1]
    target_rows = teacher_rows.filter(pl.col("label_v2_plus_rule_distillation_target"))
    assert target_rows.height == 4
    assert target_rows["dt_schedule_family_target"].unique().to_list() == [
        "schedule_value_learner_v2_plus"
    ]
    assert (
        target_rows["label_v2_plus_selected_candidate_id"].to_list()
        == target_rows["dt_candidate_id_target"].to_list()
    )


def test_dt_research_shadow_sequence_packet_exports_v2_plus_distillation_targets(
    tmp_path,
) -> None:
    teacher_rows = build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows(
        strict_rows_frame=_v2_plus_strict_rows(),
        regret_decomposition_frame=_v2_plus_regret_decomposition_rows(),
    )
    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=teacher_rows,
        run_slug="dt-shadow-distillation-targets",
        context_length=4,
    )
    paths = write_dt_research_shadow_sequence_packet(
        output_dir=tmp_path,
        packet=packet,
        teacher_rows_frame=teacher_rows,
    )

    npz = np.load(paths["sequence_npz"], allow_pickle=True)
    target_mask = npz["v2_plus_rule_distillation_target_mask"]
    target_ids = npz["v2_plus_rule_distillation_target_candidate_id"].astype(str)
    candidate_ids = npz["candidate_id_targets"].astype(str)
    assert target_mask.shape == candidate_ids.shape
    assert target_ids.shape == candidate_ids.shape
    assert target_mask.sum(axis=1).tolist() == [1, 1, 1, 1]
    for sequence_index in range(target_mask.shape[0]):
        target_position = int(np.flatnonzero(target_mask[sequence_index])[0])
        assert (
            target_ids[sequence_index, target_position]
            == candidate_ids[sequence_index, target_position]
        )


def test_dt_v2_plus_apples_to_apples_cli_reports_real_v2_plus_control(
    tmp_path,
) -> None:
    strict_csv = tmp_path / "v2_plus_strict_rows.csv"
    regret_csv = tmp_path / "regret_decomposition.csv"
    output_dir = tmp_path / "packet"
    _v2_plus_strict_rows().write_csv(strict_csv)
    _v2_plus_regret_decomposition_rows().write_csv(regret_csv)

    exit_code = materialize_dt_v2_plus_apples_to_apples_packet(
        [
            "--strict-rows-csv",
            str(strict_csv),
            "--regret-decomposition-csv",
            str(regret_csv),
            "--output-dir",
            str(output_dir),
            "--run-slug",
            "dt-v2-plus-apples-test",
            "--source-model-name",
            "nbeatsx_official_global_panel_horizon_calibrated_v1",
            "--context-length",
            "4",
            "--max-epochs",
            "1",
            "--hidden-dim",
            "16",
            "--num-layers",
            "1",
            "--num-heads",
            "2",
            "--model-backbone",
            "local",
        ]
    )

    assert exit_code == 0
    summary = json.loads(
        (output_dir / "dt_v2_plus_apples_to_apples_summary.json").read_text(
            encoding="utf-8"
        )
    )
    controls = summary["final_holdout_controls"]
    assert controls["schedule_value_learner_v2_plus"]["mean_regret_uah"] == 15.0
    assert controls["strict_reference"]["mean_regret_uah"] == 30.0
    assert "raw_reference" in summary["candidate_set"]
    assert "schedule_value_learner_v2_reference" in summary["candidate_set"]
    assert summary["boundary"]["real_v2_plus_comparator"] is True
    assert summary["boundary"]["mirrored_training_rows"] is True
    assert summary["boundary"]["out_of_sample_generalization_claim"] is False
    assert summary["boundary"]["market_execution_enabled"] is False
    assert summary["dt_evaluation_metrics"]["v2_plus_mean_regret_uah"] == 15.0
    assert summary["best_available_label_summary"]["attached"] is True


def test_dt_research_shadow_smoke_reports_v2_plus_rule_distillation_metrics(
    tmp_path,
) -> None:
    teacher_rows = build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows(
        strict_rows_frame=_v2_plus_strict_rows(),
        regret_decomposition_frame=_v2_plus_regret_decomposition_rows(),
    )
    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=teacher_rows,
        run_slug="dt-shadow-distillation-smoke",
        context_length=4,
    )
    paths = write_dt_research_shadow_sequence_packet(
        output_dir=tmp_path,
        packet=packet,
        teacher_rows_frame=teacher_rows,
    )

    smoke = run_dt_research_shadow_smoke(
        sequence_npz_path=paths["sequence_npz"],
        output_dir=tmp_path,
        model_backbone="local",
        max_epochs=40,
        hidden_dim=16,
        num_layers=1,
        num_heads=2,
        seed=17,
        objective_kind="v2_plus_rule_distillation",
        cross_entropy_weight=0.0,
        decision_aware_ranking_weight=0.0,
        distillation_weight=1.0,
    )
    summary = json.loads(smoke["summary_json"].read_text(encoding="utf-8"))
    metrics = summary["evaluation_metrics"]
    assert summary["loss_function"] == "v2_plus_rule_distillation_listwise"
    assert summary["training_objective"]["objective_kind"] == "v2_plus_rule_distillation"
    assert summary["training_objective"]["cross_entropy_weight"] == 0.0
    assert summary["training_objective"]["decision_aware_ranking_weight"] == 0.0
    assert summary["training_objective"]["distillation_weight"] == 1.0
    assert summary["market_execution_enabled"] is False
    assert summary["dt_promotion_gate_passed"] is False
    assert metrics["v2_plus_rule_recovery_rate"] >= 0.95
    assert abs(metrics["raw_distilled_argmax_minus_v2_plus_mean_regret_uah"]) <= 1e-6
    assert metrics["raw_distilled_argmax_win_loss_tie_vs_v2_plus"]["losses"] == 0


def test_dt_v2_plus_distillation_shadow_cli_materializes_packet(
    tmp_path,
) -> None:
    strict_csv = tmp_path / "v2_plus_strict_rows.csv"
    regret_csv = tmp_path / "regret_decomposition.csv"
    output_dir = tmp_path / "packet"
    _v2_plus_strict_rows().write_csv(strict_csv)
    _v2_plus_regret_decomposition_rows().write_csv(regret_csv)

    exit_code = materialize_dt_v2_plus_distillation_shadow_packet(
        [
            "--strict-rows-csv",
            str(strict_csv),
            "--regret-decomposition-csv",
            str(regret_csv),
            "--output-dir",
            str(output_dir),
            "--run-slug",
            "dt-v2-plus-distillation-test",
            "--source-model-name",
            "nbeatsx_official_global_panel_horizon_calibrated_v1",
            "--context-length",
            "4",
            "--max-epochs",
            "10",
            "--hidden-dim",
            "16",
            "--num-layers",
            "1",
            "--num-heads",
            "2",
            "--seed",
            "19",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "dt_research_shadow_teacher_rows.csv").exists()
    assert (output_dir / "dt_v2_plus_distillation_teacher_rows.csv").exists()
    summary = json.loads(
        (output_dir / "dt_v2_plus_distillation_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["boundary"]["market_execution_enabled"] is False
    assert summary["boundary"]["dt_promotion_gate_passed"] is False
    assert summary["boundary"]["v2_plus_remains_default"] is True
    assert (
        summary["dt_evaluation_metrics"]["v2_plus_rule_recovery_rate"] >= 0.95
    )


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


def _v2_plus_strict_rows() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 4, 12, 23)
    role_regrets = {
        "raw_reference": [80.0, 70.0],
        "schedule_value_learner_v2_plus": [10.0, 20.0],
        "schedule_value_learner_v2_reference": [18.0, 24.0],
        "strict_reference": [35.0, 25.0],
    }
    for anchor_index in range(2):
        anchor = start + timedelta(days=anchor_index)
        for role, regrets in role_regrets.items():
            rows.append(
                {
                    "evaluation_id": f"test:{role}:{anchor_index}",
                    "tenant_id": "client_003_dnipro_factory",
                    "source_model_name": (
                        "nbeatsx_official_global_panel_horizon_calibrated_v1"
                    ),
                    "forecast_model_name": f"model_{role}",
                    "strategy_kind": "dfl_schedule_value_learner_v2_plus_strict_lp",
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "generated_at": "2026-05-15T17:53:14+00:00",
                    "horizon_hours": 24,
                    "starting_soc_fraction": 0.52,
                    "starting_soc_source": "schedule_candidate_library_v2_plus",
                    "decision_value_uah": 1000.0 - regrets[anchor_index],
                    "forecast_objective_value_uah": 900.0,
                    "oracle_value_uah": 1000.0,
                    "regret_uah": regrets[anchor_index],
                    "regret_ratio": regrets[anchor_index] / 1000.0,
                    "total_degradation_penalty_uah": 2.0,
                    "total_throughput_mwh": 0.4,
                    "committed_action": "HOLD",
                    "committed_power_mw": 0.0,
                    "rank_by_regret": 1,
                    "data_quality_tier": "thesis_grade",
                    "observed_coverage_ratio": 1.0,
                    "safety_violation_count": 0,
                    "selection_role": role,
                    "claim_scope": (
                        "dfl_schedule_value_learner_v2_plus_strict_lp_gate_not_full_dfl"
                    ),
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "evaluation_payload": json.dumps(
                        {
                            "forecast_diagnostics": {
                                "top_k_price_recall": 0.5,
                            },
                            "horizon": [
                                {
                                    "actual_price_uah_mwh": 1000.0,
                                    "forecast_price_uah_mwh": 900.0,
                                    "net_power_mw": 0.0,
                                },
                                {
                                    "actual_price_uah_mwh": 1400.0,
                                    "forecast_price_uah_mwh": 1300.0,
                                    "net_power_mw": 0.1,
                                },
                            ],
                        }
                    ),
                }
            )
    return pl.DataFrame(rows)


def _v2_plus_regret_decomposition_rows() -> pl.DataFrame:
    start = datetime(2026, 4, 12, 23)
    rows = []
    for anchor_index, best_regret in enumerate([10.0, 12.0]):
        rows.append(
            {
                "tenant_id": "client_003_dnipro_factory",
                "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
                "anchor_timestamp": start + timedelta(days=anchor_index),
                "split_name": "final_holdout",
                "best_candidate_family": (
                    "schedule_value_learner_v2_plus"
                    if anchor_index == 0
                    else "oracle_neighbor_diagnostic"
                ),
                "best_candidate_model_name": f"best_model_{anchor_index}",
                "best_candidate_regret_uah": best_regret,
                "regret_gap_v2_to_best_candidate_uah": (
                    0.0 if anchor_index == 0 else 8.0
                ),
                "claim_scope": "dfl_schedule_value_regret_decomposition_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
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
