from __future__ import annotations

from datetime import datetime, timedelta
import json

import polars as pl

from scripts.materialize_hf_safe_switch_scorer_packet import (
    main as materialize_hf_safe_switch_scorer_packet,
)
from smart_arbitrage.dfl.hf_safe_switch_scorer import (
    CLAIM_SCOPE,
    build_hf_safe_switch_scorer_packet,
    write_hf_safe_switch_scorer_packet,
)


def test_hf_safe_switch_scorer_packet_preserves_research_boundary(tmp_path) -> None:
    packet = build_hf_safe_switch_scorer_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="hf-safe-switch-test",
        thresholds_uah=(0.0, 20.0),
        max_epochs=1,
        hidden_dim=8,
        num_layers=1,
        num_heads=1,
        output_dir=tmp_path,
        canonical_aggregate={
            "baseline_mean_regret": 174.7684,
            "mean_test_regret": 168.1566,
            "pass_level": "secondary",
        },
        save_checkpoint=True,
    )
    paths = write_hf_safe_switch_scorer_packet(output_dir=tmp_path, packet=packet)

    summary = packet["summary"]
    assert summary["claim_scope"] == CLAIM_SCOPE
    assert summary["model_backbone"] == "huggingface_decision_transformer_model"
    assert summary["dataset_summary"]["research_shadow_training_rows"] == 8
    assert summary["dataset_summary"]["promotable_v13_permitted_training_rows"] == 0
    assert summary["publication_receipt_verified"] is False
    assert summary["source_publication_timestamp_available"] is False
    assert summary["dt_promotion_gate_passed"] is False
    assert summary["promotion_gate_passed"] is False
    assert summary["market_execution_enabled"] is False
    assert summary["selection_config"]["max_predicted_tail_risk_probability"] == 0.5
    assert summary["checkpoint"]["saved"] is True
    assert summary["checkpoint"]["load_smoke_passed"] is True
    assert len(summary["threshold_results"]) == 2
    assert paths["summary_json"].exists()
    assert paths["threshold_metrics_csv"].exists()
    assert paths["selected_rows_threshold_0"].exists()


def test_hf_safe_switch_scorer_refuses_market_execution_rows() -> None:
    frame = _teacher_rows().with_columns(pl.lit(True).alias("market_execution_enabled"))

    try:
        build_hf_safe_switch_scorer_packet(
            teacher_rows_frame=frame,
            run_slug="bad",
            max_epochs=1,
        )
    except ValueError as exc:
        assert "market_execution_enabled" in str(exc)
    else:  # pragma: no cover - defensive test branch.
        raise AssertionError("expected market execution rows to be rejected")


def test_hf_safe_switch_scorer_cli_writes_packet(tmp_path) -> None:
    teacher_rows_csv = tmp_path / "teacher_rows.csv"
    aggregate_json = tmp_path / "aggregate.json"
    output_dir = tmp_path / "packet"
    _csv_ready(_teacher_rows()).write_csv(teacher_rows_csv)
    aggregate_json.write_text(
        json.dumps(
            {
                "baseline_mean_regret": 174.7684,
                "mean_test_regret": 168.1566,
                "pass_level": "secondary",
            }
        ),
        encoding="utf-8",
    )

    exit_code = materialize_hf_safe_switch_scorer_packet(
        [
            "--teacher-rows-csv",
            str(teacher_rows_csv),
            "--output-dir",
            str(output_dir),
            "--run-slug",
            "hf-safe-switch-cli-test",
            "--canonical-aggregate-json",
            str(aggregate_json),
            "--thresholds-uah",
            "0,20",
            "--max-epochs",
            "1",
            "--hidden-dim",
            "8",
            "--num-layers",
            "1",
            "--num-heads",
            "1",
        ]
    )

    assert exit_code == 0
    summary = json.loads(
        (output_dir / "hf_safe_switch_scorer_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["claim_scope"] == CLAIM_SCOPE
    assert summary["market_execution_enabled"] is False
    assert (output_dir / "hf_safe_switch_scorer_threshold_metrics.csv").exists()


def _teacher_rows() -> pl.DataFrame:
    start = datetime(2026, 4, 1, 23)
    rows: list[dict[str, object]] = []
    scenarios = [
        ("train_selection", 0, -60.0, 80.0, 110.0),
        ("train_selection", 1, 55.0, -30.0, 125.0),
        ("final_holdout", 2, -50.0, 85.0, 130.0),
        ("final_holdout", 3, 70.0, -25.0, 115.0),
    ]
    for split_name, day_offset, raw_delta, ref_delta, strict_delta in scenarios:
        anchor = start + timedelta(days=day_offset)
        candidate_count = 4
        for candidate_index, family, regret_delta in (
            (0, "raw_reference", raw_delta),
            (1, "schedule_value_learner_v2_plus", 0.0),
            (2, "schedule_value_learner_v2_plus_reference", ref_delta),
            (3, "strict_reference", strict_delta),
        ):
            rows.append(
                _row(
                    anchor=anchor,
                    split_name=split_name,
                    family=family,
                    candidate_index=candidate_index,
                    candidate_count=candidate_count,
                    regret=100.0 + regret_delta,
                    regret_delta=regret_delta,
                    forecast_spread=500.0 + 10.0 * day_offset,
                )
            )
    return pl.DataFrame(rows)


def _row(
    *,
    anchor: datetime,
    split_name: str,
    family: str,
    candidate_index: int,
    candidate_count: int,
    regret: float,
    regret_delta: float,
    forecast_spread: float,
) -> dict[str, object]:
    return {
        "tenant_id": "client_003_dnipro_factory",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "anchor_timestamp": anchor,
        "split_name": split_name,
        "horizon_hours": 24,
        "forecast_price_uah_mwh_vector": [1000.0, 1000.0 + forecast_spread],
        "dispatch_mw_vector": [0.1, -0.1],
        "soc_fraction_vector": [0.52, 0.58],
        "decision_value_uah": 1000.0 - regret,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "forecast_spread_uah_mwh": forecast_spread,
        "forecast_top_k_actual_overlap": 0.5,
        "forecast_bottom_k_actual_overlap": 0.5,
        "soc_min_slack_fraction": 0.2,
        "safety_violation_count": 0,
        "selector_feature_forecast_spread_uah_mwh": forecast_spread,
        "selector_feature_terminal_soc_delta_fraction": 0.06,
        "selector_feature_total_throughput_delta_mwh": 0.4,
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
        "schedule_value_uah": 1000.0 - regret,
        "dfl_input_contract": (
            "calibrated_forecasts_tenant_soc_context_feasible_candidate_schedules"
        ),
        "dfl_target_contract": "best_candidate_schedule_value_regret_delta_vs_v2_plus",
        "dt_input_contract": (
            "research_shadow_sequence_forecast_battery_tenant_candidate_value_return_to_go"
        ),
        "dt_action_target_contract": "candidate_id_or_schedule_family",
        "v2_plus_role": "real_schedule_value_learner_v2_plus_comparator",
        "v13_training_permission_gate_passed": False,
        "v13_blocking_context_families": "explicit_dam_publication_receipts",
        "permitted_model_training_row": False,
        "permits_model_training": False,
        "training_blocker": "apples_to_apples_research_shadow_not_v13_training",
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "not_deployed_dt_control": True,
        "research_shadow_source_kind": "v2_plus_strict_rows_mirrored_training_adapter",
        "research_shadow_reward_reference": "real_v2_plus_strict_rows_comparator",
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "market_availability_claim": False,
        "research_shadow_not_promotable": True,
        "teacher_anchor_candidate_count": candidate_count,
        "forecast_objective_value_uah": 1000.0,
    }


def _csv_ready(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        [
            pl.col(column)
            .map_elements(_json_vector, return_dtype=pl.String)
            .alias(column)
            for column in (
                "forecast_price_uah_mwh_vector",
                "dispatch_mw_vector",
                "soc_fraction_vector",
            )
        ]
    )


def _json_vector(value: object) -> str:
    if isinstance(value, pl.Series):
        return json.dumps(value.to_list())
    return json.dumps(value)
