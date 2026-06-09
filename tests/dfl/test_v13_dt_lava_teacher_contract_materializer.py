from __future__ import annotations

import json
import pickle
from pathlib import Path

import polars as pl


def test_v13_teacher_contract_materializer_writes_blocked_contract(
    tmp_path: Path,
) -> None:
    from scripts.materialize_v13_dt_lava_teacher_contract_from_candidate_frame import (
        main,
    )

    candidate_pickle = tmp_path / "candidate.pkl"
    readiness_csv = tmp_path / "readiness.csv"
    output_pickle = tmp_path / "teacher_contract.pkl"
    summary_json = tmp_path / "summary.json"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame(), file)
    _readiness_frame(ready=False).write_csv(readiness_csv)

    exit_code = main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--readiness-csv",
            str(readiness_csv),
            "--output-pickle",
            str(output_pickle),
            "--summary-json",
            str(summary_json),
        ]
    )

    assert exit_code == 0
    with output_pickle.open("rb") as file:
        contract = pickle.load(file)
    summary = json.loads(summary_json.read_text(encoding="utf-8"))

    assert isinstance(contract, pl.DataFrame)
    assert contract.height == 2
    assert set(contract["dt_action_target_contract"].unique().to_list()) == {
        "candidate_id_or_schedule_family"
    }
    assert contract["permitted_model_training_row"].to_list() == [False, False]
    assert summary["contract_rows"] == 2
    assert summary["permitted_model_training_rows"] == 0
    assert summary["training_blocker_counts"] == {
        "v13_training_permission_gate_blocked": 2
    }
    assert summary["market_execution_enabled"] is False


def test_v13_teacher_contract_materializer_filters_to_readiness_pairs(
    tmp_path: Path,
) -> None:
    from scripts.materialize_v13_dt_lava_teacher_contract_from_candidate_frame import (
        main,
    )

    candidate_pickle = tmp_path / "candidate.pkl"
    readiness_csv = tmp_path / "readiness.csv"
    output_pickle = tmp_path / "teacher_contract.pkl"
    summary_json = tmp_path / "summary.json"
    with candidate_pickle.open("wb") as file:
        pickle.dump(
            pl.concat(
                [
                    _candidate_frame(),
                    _candidate_frame().with_columns(
                        pl.lit("untracked_model").alias("source_model_name")
                    ),
                ],
                how="vertical",
            ),
            file,
        )
    _readiness_frame(ready=False).write_csv(readiness_csv)

    exit_code = main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--readiness-csv",
            str(readiness_csv),
            "--output-pickle",
            str(output_pickle),
            "--summary-json",
            str(summary_json),
        ]
    )

    assert exit_code == 0
    summary = json.loads(summary_json.read_text(encoding="utf-8"))
    assert summary["candidate_rows"] == 4
    assert summary["contract_rows"] == 2
    assert summary["filtered_to_v13_readiness_pairs"] is True
    assert summary["dropped_non_v13_readiness_pair_rows"] == 2


def _candidate_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            _candidate_row(
                candidate_source="v2_plus_default",
                candidate_family="v2_plus_default",
                candidate_model_name="v2_plus_default",
                regret_delta=0.0,
                decision_value=900.0,
                regret=100.0,
            ),
            _candidate_row(
                candidate_source="schedule_neighbor",
                candidate_family="safe_neighbor",
                candidate_model_name="safe_neighbor_v1",
                regret_delta=-50.0,
                decision_value=950.0,
                regret=50.0,
            ),
        ]
    )


def _candidate_row(
    *,
    candidate_source: str,
    candidate_family: str,
    candidate_model_name: str,
    regret_delta: float,
    decision_value: float,
    regret: float,
) -> dict[str, object]:
    return {
        "tenant_id": "tenant_a",
        "source_model_name": "nbeatsx_silver_v0",
        "candidate_source": candidate_source,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": "2026-01-01T23:00:00",
        "generated_at": "2026-01-01T12:00:00",
        "split_name": "train_selection",
        "horizon_hours": 2,
        "eligible_for_final_selection": True,
        "label_regret_delta_vs_v2_plus_uah": regret_delta,
        "forecast_price_uah_mwh_vector": [1000.0, 3000.0],
        "actual_price_uah_mwh_vector": [1200.0, 2800.0],
        "dispatch_mw_vector": [0.25, -0.25],
        "soc_fraction_vector": [0.5, 0.55],
        "decision_value_uah": decision_value,
        "forecast_objective_value_uah": 925.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 1.0,
        "total_throughput_mwh": 0.5,
        "safety_violation_count": 0,
        "evaluation_payload": "{}",
        "raw_hourly_action_imitation": False,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _readiness_frame(*, ready: bool) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "nbeatsx_silver_v0",
                "v13_candidate_generation_ready": ready,
                "readiness_decision": "v13_candidate_generation_ready"
                if ready
                else "data_acquisition_needed",
                "prior_material_safe_switch_example_count": 20,
                "min_prior_material_safe_switch_examples_for_dt": 20,
                "blocking_context_families": "none"
                if ready
                else "explicit_dam_publication_receipts:blocked_missing_source",
                "market_execution_enabled": False,
            }
        ]
    )
