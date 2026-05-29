from __future__ import annotations

from datetime import datetime, timedelta
import json

import polars as pl

from scripts.materialize_dt_v2_plus_promotion_evidence_packet import (
    main as materialize_dt_v2_plus_promotion_evidence_packet,
)
from smart_arbitrage.dfl.dt_v2_plus_promotion_evidence import (
    build_dt_v2_plus_promotion_evidence_packet,
)


def test_dt_v2_plus_promotion_evidence_passes_rare_safe_switches() -> None:
    teacher_rows, selected_rows = _promotion_evidence_rows(
        selected_deltas=[-40.0, 0.0, 0.0, 0.0],
    )

    packet = build_dt_v2_plus_promotion_evidence_packet(
        selected_rows,
        teacher_rows,
        run_slug="promotion-evidence-pass",
        min_final_holdout_anchor_count=4,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
        max_non_v2_plus_switch_rate=0.5,
    )

    summary = packet["summary"]
    gate = packet["gate_rows"].row(0, named=True)

    assert summary["gate"]["promotion_evidence_passed"] is True
    assert summary["boundary"]["v2_plus_remains_default"] is True
    assert summary["boundary"]["market_execution_enabled"] is False
    assert summary["boundary"]["promotion_gate_passed"] is False
    assert summary["boundary"]["strict_oracle_reference_is_runtime_input"] is False
    assert gate["promotion_evidence_passed"] is True
    assert gate["promotion_blocker"] == "none"
    assert gate["non_v2_plus_switch_count"] == 1
    assert gate["safe_switch_win_count"] == 1
    assert gate["mean_regret_improvement_ratio_vs_v2_plus"] == 0.1


def test_dt_v2_plus_promotion_evidence_blocks_tie_no_switch() -> None:
    teacher_rows, selected_rows = _promotion_evidence_rows(
        selected_deltas=[0.0, 0.0, 0.0, 0.0],
    )

    packet = build_dt_v2_plus_promotion_evidence_packet(
        selected_rows,
        teacher_rows,
        run_slug="promotion-evidence-tie",
        min_final_holdout_anchor_count=4,
    )

    summary = packet["summary"]
    gate = packet["gate_rows"].row(0, named=True)

    assert summary["gate"]["promotion_evidence_passed"] is False
    assert gate["promotion_blocker"] == "no_non_v2_plus_switches"
    assert gate["selector_minus_v2_plus_mean_regret_uah"] == 0.0
    assert gate["observed_safe_switch_opportunity_count"] == 1
    assert gate["recovered_safe_switch_opportunity_count"] == 0


def test_dt_v2_plus_promotion_evidence_blocks_tail_risk_loss() -> None:
    teacher_rows, selected_rows = _promotion_evidence_rows(
        selected_deltas=[-100.0, -100.0, 150.0, 0.0],
    )

    packet = build_dt_v2_plus_promotion_evidence_packet(
        selected_rows,
        teacher_rows,
        run_slug="promotion-evidence-tail-risk",
        min_final_holdout_anchor_count=4,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
        max_non_v2_plus_switch_rate=0.75,
        tail_risk_loss_threshold_uah=100.0,
        max_tail_risk_loss_count=0,
    )

    gate = packet["gate_rows"].row(0, named=True)

    assert gate["promotion_evidence_passed"] is False
    assert gate["promotion_blocker"] == "tail_risk_loss_detected"
    assert gate["tail_risk_loss_count"] == 1
    assert gate["mean_regret_improvement_ratio_vs_v2_plus"] == 0.125


def test_dt_v2_plus_promotion_evidence_refuses_executable_rows() -> None:
    teacher_rows, selected_rows = _promotion_evidence_rows(
        selected_deltas=[-40.0, 0.0, 0.0, 0.0],
    )
    unsafe_rows = selected_rows.with_columns(pl.lit(True).alias("market_execution_enabled"))

    try:
        build_dt_v2_plus_promotion_evidence_packet(
            unsafe_rows,
            teacher_rows,
            run_slug="unsafe",
            min_final_holdout_anchor_count=4,
        )
    except ValueError as exc:
        assert "market_execution_enabled=false" in str(exc)
    else:  # pragma: no cover - defensive branch.
        raise AssertionError("expected executable selected rows to be rejected")


def test_dt_v2_plus_promotion_evidence_cli_writes_packet(tmp_path) -> None:
    teacher_rows, selected_rows = _promotion_evidence_rows(
        selected_deltas=[-40.0, 0.0, 0.0, 0.0],
    )
    teacher_csv = tmp_path / "teacher_rows.csv"
    selected_csv = tmp_path / "selected_rows.csv"
    output_dir = tmp_path / "promotion_evidence"
    _csv_ready(teacher_rows).write_csv(teacher_csv)
    selected_rows.write_csv(selected_csv)

    exit_code = materialize_dt_v2_plus_promotion_evidence_packet(
        [
            "--teacher-rows-csv",
            str(teacher_csv),
            "--selected-rows-csv",
            str(selected_csv),
            "--output-dir",
            str(output_dir),
            "--run-slug",
            "promotion-evidence-cli",
            "--min-final-holdout-anchor-count",
            "4",
            "--min-mean-regret-improvement-ratio-vs-v2-plus",
            "0.05",
            "--max-non-v2-plus-switch-rate",
            "0.5",
        ]
    )

    assert exit_code == 0
    summary = json.loads(
        (output_dir / "dt_v2_plus_promotion_evidence_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["gate"]["promotion_evidence_passed"] is True
    assert (output_dir / "dt_v2_plus_promotion_evidence_gate_rows.csv").exists()
    assert (output_dir / "dt_v2_plus_promotion_evidence_selected_rows.csv").exists()
    assert (output_dir / "dt_v2_plus_promotion_evidence_summary.md").exists()


def _promotion_evidence_rows(
    *,
    selected_deltas: list[float],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    start = datetime(2026, 4, 1, 23)
    teacher_rows: list[dict[str, object]] = []
    selected_rows: list[dict[str, object]] = []
    for index, selected_delta in enumerate(selected_deltas):
        anchor = start + timedelta(days=index)
        v2_regret = 100.0
        opportunity_delta = (
            selected_delta
            if selected_delta != 0.0
            else (-40.0 if index == 0 else 120.0)
        )
        teacher_rows.append(
            _teacher_row(
                anchor=anchor,
                family="schedule_value_learner_v2_plus",
                candidate_index=0,
                regret=v2_regret,
                regret_delta=0.0,
            )
        )
        teacher_rows.append(
            _teacher_row(
                anchor=anchor,
                family="safe_challenger",
                candidate_index=1,
                regret=v2_regret + opportunity_delta,
                regret_delta=opportunity_delta,
            )
        )
        selected_family = (
            "schedule_value_learner_v2_plus"
            if selected_delta == 0.0
            else "safe_challenger"
        )
        selected_rows.append(
            _selected_row(
                anchor=anchor,
                family=selected_family,
                candidate_index=0 if selected_delta == 0.0 else 1,
                selected_regret=v2_regret + selected_delta,
                v2_regret=v2_regret,
                predicted_improvement=abs(selected_delta),
                abstained=selected_delta == 0.0,
            )
        )
    return pl.DataFrame(teacher_rows), pl.DataFrame(selected_rows)


def _teacher_row(
    *,
    anchor: datetime,
    family: str,
    candidate_index: int,
    regret: float,
    regret_delta: float,
) -> dict[str, object]:
    return {
        "tenant_id": "client_003_dnipro_factory",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "anchor_timestamp": anchor,
        "split_name": "final_holdout",
        "forecast_price_uah_mwh_vector": [1000.0, 1100.0],
        "actual_price_uah_mwh_vector": [1000.0, 1120.0],
        "dispatch_mw_vector": [0.1, -0.1],
        "soc_fraction_vector": [0.52, 0.58],
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "schedule_value_uah": 1000.0 - regret,
        "decision_value_uah": 1000.0 - regret,
        "regret_delta_vs_v2_plus_uah": regret_delta,
        "dt_candidate_index_target": candidate_index,
        "dt_candidate_id_target": (
            f"client_003_dnipro_factory|nbeatsx|{anchor.isoformat()}|{family}|"
            f"{candidate_index}"
        ),
        "dt_schedule_family_target": family,
        "safety_violation_count": 0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "permits_model_training": False,
        "raw_hourly_action_imitation": False,
    }


def _selected_row(
    *,
    anchor: datetime,
    family: str,
    candidate_index: int,
    selected_regret: float,
    v2_regret: float,
    predicted_improvement: float,
    abstained: bool,
) -> dict[str, object]:
    candidate_id = (
        f"client_003_dnipro_factory|nbeatsx|{anchor.isoformat()}|{family}|"
        f"{candidate_index}"
    )
    return {
        "tenant_id": "client_003_dnipro_factory",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "anchor_timestamp": anchor,
        "selected_candidate_id": candidate_id,
        "selected_candidate_index": candidate_index,
        "selected_schedule_family": family,
        "selected_regret_uah": selected_regret,
        "selected_value_uah": 1000.0 - selected_regret,
        "v2_plus_candidate_id": (
            "client_003_dnipro_factory|nbeatsx|"
            f"{anchor.isoformat()}|schedule_value_learner_v2_plus|0"
        ),
        "v2_plus_regret_uah": v2_regret,
        "v2_plus_value_uah": 1000.0 - v2_regret,
        "selected_minus_v2_plus_regret_uah": selected_regret - v2_regret,
        "selected_minus_v2_plus_value_uah": v2_regret - selected_regret,
        "predicted_regret_delta_vs_v2_plus_uah": -predicted_improvement,
        "predicted_improvement_vs_v2_plus_uah": predicted_improvement,
        "abstained_to_v2_plus": abstained,
        "abstention_reason": (
            "predicted_improvement_below_threshold"
            if abstained
            else "selected_predicted_regret_improvement"
        ),
        "family_tail_risk_probability": 0.0,
        "tail_risk_guard_passed": True,
        "research_shadow_not_promotable": True,
        "dt_lava_ready": False,
        "promotion_gate_passed": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_market_execution": True,
    }


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
