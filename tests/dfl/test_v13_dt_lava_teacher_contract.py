from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.v13_dt_lava_teacher_contract import (
    build_dfl_v13_gated_dt_lava_teacher_contract_frame,
)


def test_v13_dt_lava_teacher_contract_blocks_training_until_v13_passes() -> None:
    contract = build_dfl_v13_gated_dt_lava_teacher_contract_frame(
        _sequence_training_frame(),
        _v13_readiness_frame(ready=False),
    )

    assert contract.height == 2
    assert contract["permitted_model_training_row"].to_list() == [False, False]
    assert contract["v13_training_permission_gate_passed"].to_list() == [False, False]
    assert contract["training_blocker"].to_list() == [
        "v13_training_permission_gate_blocked",
        "v13_training_permission_gate_blocked",
    ]
    assert set(contract["v2_plus_role"].unique().to_list()) == {
        "teacher_comparator_fallback"
    }
    assert set(contract["dt_action_target_contract"].unique().to_list()) == {
        "candidate_id_or_schedule_family"
    }
    assert set(contract["market_execution_enabled"].unique().to_list()) == {False}


def test_v13_dt_lava_teacher_contract_exposes_candidate_targets_when_v13_passes() -> (
    None
):
    contract = build_dfl_v13_gated_dt_lava_teacher_contract_frame(
        _sequence_training_frame(),
        _v13_readiness_frame(ready=True),
    )

    train_row = contract.filter(pl.col("split_name") == "train_selection").row(
        0,
        named=True,
    )
    final_row = contract.filter(pl.col("split_name") == "final_holdout").row(
        0,
        named=True,
    )

    assert train_row["permitted_model_training_row"] is True
    assert train_row["training_blocker"] == "none"
    assert train_row["dt_candidate_id_target"] == (
        "tenant_a|nbeatsx_official_global_panel_horizon_calibrated_v1|"
        "2026-01-01T23:00:00|poland_shadow_candidate|"
        "poland_safe_value_candidate|candidate_safe"
    )
    assert train_row["dt_candidate_index_target"] == 1
    assert train_row["dt_schedule_family_target"] == "poland_safe_value_candidate"
    assert train_row["dfl_target_contract"] == (
        "best_candidate_schedule_value_regret_delta_vs_v2_plus"
    )
    assert train_row["dt_input_contract"] == (
        "v13_teacher_sequence_forecast_battery_tenant_candidate_value_return_to_go"
    )
    assert train_row["return_to_go_regret_target_uah"] == 50.0
    assert train_row["regret_delta_vs_v2_plus_uah"] == -50.0
    assert train_row["schedule_value_uah"] == 950.0
    assert final_row["permitted_model_training_row"] is False
    assert final_row["training_blocker"] == "final_holdout_scoring_only"
    assert set(contract["raw_hourly_action_imitation"].unique().to_list()) == {False}
    assert set(contract["promotion_gate_passed"].unique().to_list()) == {False}
    assert set(contract["market_execution_enabled"].unique().to_list()) == {False}


def test_v13_dt_lava_teacher_contract_rejects_execution_claims() -> None:
    sequence = _sequence_training_frame().with_columns(
        pl.lit(True).alias("market_execution_enabled")
    )

    with pytest.raises(ValueError, match="market execution"):
        build_dfl_v13_gated_dt_lava_teacher_contract_frame(
            sequence,
            _v13_readiness_frame(ready=True),
        )


def test_v13_dt_lava_teacher_contract_requires_phase2_context_vectors() -> None:
    sequence = _sequence_training_frame().drop("forecast_price_uah_mwh_vector")

    with pytest.raises(ValueError, match="forecast_price_uah_mwh_vector"):
        build_dfl_v13_gated_dt_lava_teacher_contract_frame(
            sequence,
            _v13_readiness_frame(ready=True),
        )


def _sequence_training_frame() -> pl.DataFrame:
    anchor = datetime(2026, 1, 1, 23)
    return pl.DataFrame(
        [
            _sequence_row(
                anchor=anchor,
                split_name="train_selection",
                is_training_row=True,
                candidate_index=1,
            ),
            _sequence_row(
                anchor=anchor + timedelta(days=1),
                split_name="final_holdout",
                is_training_row=False,
                candidate_index=1,
            ),
        ]
    )


def _sequence_row(
    *,
    anchor: datetime,
    split_name: str,
    is_training_row: bool,
    candidate_index: int,
) -> dict[str, object]:
    candidate_key = (
        "tenant_a|nbeatsx_official_global_panel_horizon_calibrated_v1|"
        f"{anchor.isoformat()}|poland_shadow_candidate|"
        "poland_safe_value_candidate|candidate_safe"
    )
    return {
        "tenant_id": "tenant_a",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "anchor_timestamp": anchor,
        "split_name": split_name,
        "is_training_row": is_training_row,
        "teacher_candidate_key": candidate_key,
        "teacher_candidate_index": candidate_index,
        "dt_candidate_index_target": candidate_index,
        "dt_candidate_family_target": "poland_safe_value_candidate",
        "teacher_target_family": "poland_safe_value_candidate",
        "teacher_return_to_go_delta_uah": 50.0,
        "dt_return_to_go_uah": 50.0,
        "label_regret_delta_vs_v2_plus_uah": -50.0,
        "decision_value_uah": 950.0,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": 50.0,
        "forecast_price_uah_mwh_vector": [1000.0, 3000.0],
        "dispatch_mw_vector": [0.25, -0.25],
        "soc_fraction_vector": [0.5, 0.55],
        "selector_feature_forecast_spread_uah_mwh": 2000.0,
        "target_label_space": "ua_context_schedule_candidate_index",
        "raw_hourly_action_imitation": False,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _v13_readiness_frame(*, ready: bool) -> pl.DataFrame:
    blocker = "none" if ready else "explicit_dam_publication_receipts:blocked_missing_source"
    return pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": (
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "v13_candidate_generation_ready": ready,
                "readiness_decision": "v13_candidate_generation_ready"
                if ready
                else "data_acquisition_needed",
                "prior_material_safe_switch_example_count": 20 if ready else 7,
                "min_prior_material_safe_switch_examples_for_dt": 20,
                "blocking_context_families": blocker,
                "dt_lava_ready": False,
                "raw_hourly_action_imitation": False,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        ]
    )
