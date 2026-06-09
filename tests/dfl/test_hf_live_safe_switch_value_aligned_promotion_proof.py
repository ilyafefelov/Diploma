from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.materialize_hf_live_safe_switch_value_aligned_promotion_proof import (
    build_value_aligned_shadow_promotion_proof,
    write_value_aligned_shadow_promotion_proof,
)
from scripts.materialize_hf_live_safe_switch_value_aligned_shadow_demo_packet import (
    build_value_aligned_shadow_demo_packet,
    write_value_aligned_shadow_demo_packet,
)


def test_value_aligned_shadow_promotion_proof_passes_formal_gate(tmp_path: Path) -> None:
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()
    _write_audit_fixture(audit_dir)
    robustness_summary = tmp_path / "robustness_summary.json"
    canonical_aggregate = tmp_path / "aggregate.json"
    _write_json(
        robustness_summary,
        {
            "robustness_gate_passed": True,
            "selected_operating_threshold_uah": 100.0,
            "selected_threshold_metrics": {
                "all_seeds_beat_v2_plus_baseline": True,
                "seeds_beating_v2_plus_baseline_count": 5,
                "seed_count": 5,
                "selected_mean_regret_mean": 158.71,
                "mean_minus_v2_plus_baseline_uah": -16.06,
            },
            "canonical_comparison": {
                "mean_hf_mean_regret_uah": 158.71,
                "v2_plus_baseline_mean_regret_uah": 174.77,
                "mean_hf_minus_v2_plus_uah": -16.06,
            },
            "market_execution_enabled": False,
            "promotion_gate_passed": False,
        },
    )
    _write_json(
        canonical_aggregate,
        {
            "mean_test_regret": 168.16,
            "baseline_mean_regret": 174.77,
            "market_execution_enabled": False,
            "promotion_gate_passed": False,
        },
    )

    proof = build_value_aligned_shadow_promotion_proof(
        audit_dir=audit_dir,
        robustness_summary_json=robustness_summary,
        canonical_aggregate_json=canonical_aggregate,
        run_slug="value_aligned_shadow_promotion_test",
        min_source_backed_days=3.0,
    )

    assert proof["shadow_promotion_gate_passed"] is True
    assert proof["promotion_gate_scope"] == "value_aligned_shadow_candidate_library"
    assert proof["market_execution_enabled"] is False
    assert proof["production_market_promotion_gate_passed"] is False
    assert proof["gate_results"]["multi_day_nonfallback_switch_rate"]["passed"] is True
    assert proof["gate_results"]["zero_safety_failures"]["passed"] is True
    assert proof["gate_results"]["tail_risk_control"]["passed"] is True
    assert proof["gate_results"]["frozen_regret_vs_v2_plus"]["passed"] is True
    assert proof["selected_nonfallback_day_count"] == 2.0
    assert proof["value_aligned_switch_rate"] == 2 / 3

    paths = write_value_aligned_shadow_promotion_proof(output_dir=tmp_path / "proof", proof=proof)
    assert paths["promotion_gate_json"].exists()
    assert paths["promotion_gate_md"].exists()
    assert paths["selected_nonfallback_days_csv"].exists()
    assert "shadow_promotion_gate_passed" in paths["promotion_gate_md"].read_text(encoding="utf-8")
    selected_rows = list(csv.DictReader(paths["selected_nonfallback_days_csv"].open(newline="", encoding="utf-8")))
    assert [row["target_delivery_date"] for row in selected_rows] == [
        "2026-05-01",
        "2026-05-02",
    ]


def test_value_aligned_shadow_promotion_proof_blocks_when_tail_risk_increases(
    tmp_path: Path,
) -> None:
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()
    _write_audit_fixture(audit_dir, tail_delta=3.0)
    robustness_summary = tmp_path / "robustness_summary.json"
    canonical_aggregate = tmp_path / "aggregate.json"
    _write_json(
        robustness_summary,
        {
            "robustness_gate_passed": True,
            "selected_threshold_metrics": {
                "all_seeds_beat_v2_plus_baseline": True,
                "selected_mean_regret_mean": 158.71,
                "mean_minus_v2_plus_baseline_uah": -16.06,
            },
            "canonical_comparison": {
                "mean_hf_mean_regret_uah": 158.71,
                "v2_plus_baseline_mean_regret_uah": 174.77,
                "mean_hf_minus_v2_plus_uah": -16.06,
            },
        },
    )
    _write_json(canonical_aggregate, {"mean_test_regret": 168.16, "baseline_mean_regret": 174.77})

    proof = build_value_aligned_shadow_promotion_proof(
        audit_dir=audit_dir,
        robustness_summary_json=robustness_summary,
        canonical_aggregate_json=canonical_aggregate,
        run_slug="value_aligned_shadow_promotion_test",
        min_source_backed_days=3.0,
    )

    assert proof["shadow_promotion_gate_passed"] is False
    assert proof["gate_results"]["tail_risk_control"]["passed"] is False
    assert "tail_risk_control" in proof["blocking_reasons"]


def test_value_aligned_shadow_demo_packet_compares_latest_abstention_and_switch_day(
    tmp_path: Path,
) -> None:
    promotion_gate = {
        "shadow_promotion_gate_passed": True,
        "market_execution_enabled": False,
        "production_market_promotion_gate_passed": False,
    }
    packet = build_value_aligned_shadow_demo_packet(
        run_slug="value_aligned_shadow_demo_test",
        latest_response=_shadow_response(
            target_date="latest",
            family="schedule_value_learner_v2_plus",
            selected_value=0.0,
            predicted_delta=0.0,
        ),
        switch_day_response=_shadow_response(
            target_date="2026-05-02",
            family="strict_reference",
            selected_value=2765.7152,
            predicted_delta=-448.4634,
        ),
        promotion_gate=promotion_gate,
    )

    assert packet["demo_packet_passed"] is True
    assert packet["latest_case"]["selected_schedule_family"] == "schedule_value_learner_v2_plus"
    assert packet["switch_day_case"]["selected_schedule_family"] == "strict_reference"
    assert packet["switch_day_case"]["selected_candidate_estimated_value_uah"] == 2765.7152
    assert packet["market_execution_enabled"] is False
    assert packet["production_market_promotion_gate_passed"] is False

    paths = write_value_aligned_shadow_demo_packet(output_dir=tmp_path / "demo", packet=packet)
    assert paths["demo_packet_json"].exists()
    assert paths["demo_packet_md"].exists()
    assert paths["demo_cases_csv"].exists()
    assert "2026-05-02" in paths["demo_cases_csv"].read_text(encoding="utf-8")


def _write_audit_fixture(audit_dir: Path, *, tail_delta: float = -2.0) -> None:
    _write_json(
        audit_dir / "summary.json",
        {
            "claim_scope": "hf_live_safe_switch_selection_audit_shadow_not_promotable",
            "source_price_scope": "official_oree_observed_rows_only",
            "audit_passed_for_bundle_update": True,
            "candidate_library_update_gate_passed": True,
            "update_gate_threshold_uah": 100.0,
            "update_gate_baseline_template_grid_id": "default",
            "update_gate_candidate_template_grid_id": "candidate_library_value_aligned",
            "candidate_library_value_aligned_selected_value_improvement_uah": 125.0,
            "candidate_library_value_aligned_value_gap_ratio_vs_default": 0.4,
            "candidate_library_value_aligned_tail_failure_delta_count": tail_delta,
            "candidate_library_value_aligned_safety_failure_count": 0.0,
            "market_execution_enabled": False,
            "promotion_gate_passed": False,
            "dt_lava_ready": False,
        },
    )
    _write_csv(
        audit_dir / "threshold_summary.csv",
        [
            {
                "template_grid_id": "default",
                "threshold_uah": 100.0,
                "source_backed_day_count": 3.0,
                "switch_count": 1.0,
                "switch_rate": 1 / 3,
                "mean_selected_schedule_value_uah": 50.0,
                "mean_best_template_schedule_value_uah": 350.0,
                "mean_selected_vs_best_template_value_gap_uah": 300.0,
                "predicted_tail_guard_failed_count": 5.0,
                "safety_guard_failed_count": 0.0,
            },
            {
                "template_grid_id": "candidate_library_value_aligned",
                "threshold_uah": 100.0,
                "source_backed_day_count": 3.0,
                "switch_count": 2.0,
                "switch_rate": 2 / 3,
                "mean_selected_schedule_value_uah": 175.0,
                "mean_best_template_schedule_value_uah": 295.0,
                "mean_selected_vs_best_template_value_gap_uah": 120.0,
                "predicted_tail_guard_failed_count": 5.0 + tail_delta,
                "safety_guard_failed_count": 0.0,
            },
        ],
    )
    _write_csv(
        audit_dir / "candidate_scores.csv",
        [
            _candidate_score("2026-05-01", "schedule_value_learner_v2_plus", 0.0, 0.1, 0.0),
            _candidate_score("2026-05-01", "strict_reference", -120.0, 0.2, 500.0),
            _candidate_score("2026-05-02", "schedule_value_learner_v2_plus", 0.0, 0.1, 0.0),
            _candidate_score("2026-05-02", "raw_reference", -160.0, 0.2, 800.0),
            _candidate_score("2026-05-03", "schedule_value_learner_v2_plus", 0.0, 0.1, 0.0),
            _candidate_score("2026-05-03", "strict_reference", -50.0, 0.2, 300.0),
        ],
    )
    _write_csv(audit_dir / "template_summary.csv", [])


def _candidate_score(
    date: str,
    family: str,
    predicted_delta: float,
    predicted_tail: float,
    schedule_value: float,
) -> dict[str, object]:
    return {
        "target_delivery_date": date,
        "template_grid_id": "candidate_library_value_aligned",
        "candidate_family": family,
        "candidate_id": f"{date}|{family}",
        "predicted_regret_delta_vs_v2_plus_uah": predicted_delta,
        "predicted_tail_risk_probability": predicted_tail,
        "family_tail_risk_probability": 0.0,
        "schedule_value_uah": schedule_value,
        "total_throughput_mwh": 0.2 if family != "schedule_value_learner_v2_plus" else 0.0,
        "safety_violation_count": 0.0,
        "threshold_guard_passed": predicted_delta < -100.0,
        "predicted_tail_guard_passed": True,
        "family_tail_guard_passed": True,
        "safety_guard_passed": True,
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _shadow_response(
    *,
    target_date: str,
    family: str,
    selected_value: float,
    predicted_delta: float,
) -> dict[str, object]:
    return {
        "preview_source_id": "hf_live_safe_switch_value_aligned_shadow",
        "preview_status": "value_aligned_shadow_not_promoted",
        "selected_schedule_family": family,
        "target_delivery_window_start": target_date,
        "market_execution_enabled": False,
        "market_order_payload_emitted": False,
        "promotion_gate_passed": False,
        "dt_lava_ready": False,
        "comparison_metrics": {
            "selected_candidate_estimated_value_uah": selected_value,
            "predicted_regret_delta_vs_v2_plus_uah": predicted_delta,
            "candidate_template_grid_value_aligned": 1.0,
        },
        "recommendation_schedule": [{"regret_uah": None}],
    }
