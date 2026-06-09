from __future__ import annotations

from scripts.audit_hf_live_safe_switch_forecast_guard import (
    build_forecast_guard_audit_tables,
    write_forecast_guard_audit_outputs,
)


def test_forecast_guard_audit_writes_read_only_outputs(tmp_path) -> None:
    tables = build_forecast_guard_audit_tables(
        [
            _block(
                target_date="2026-06-02",
                market_venue="DAM",
                template_grid_id="candidate_library_value_aligned",
                selected_family="schedule_value_learner_v2_plus",
                scored_candidates=[
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.2, 0.0),
                    _candidate("strict_reference", -90.0, 0.61, 850.0),
                    _candidate("raw_reference", -80.0, 0.72, 1400.0),
                ],
            )
        ],
        default_threshold_uah=100.0,
    )

    paths = write_forecast_guard_audit_outputs(
        output_dir=tmp_path,
        run_slug="forecast_guard_audit_test",
        tenant_id="client_003_dnipro_factory",
        tables=tables,
    )

    assert paths["candidate_scores_csv"].exists()
    assert paths["forecast_guard_summary_csv"].exists()
    summary_text = paths["summary_json"].read_text(encoding="utf-8")
    assert '"market_execution_enabled": false' in summary_text
    assert '"promotion_gate_passed": false' in summary_text
    assert '"proposed_bid_emitted": false' in summary_text
    assert '"market_order_payload_emitted": false' in summary_text
    assert "forecast-date HF abstains correctly" in paths["summary_md"].read_text(encoding="utf-8")


def test_forecast_guard_audit_blocks_grid_update_when_no_forecast_switch_passes() -> None:
    tables = build_forecast_guard_audit_tables(
        [
            _block(
                target_date="2026-06-02",
                market_venue="DAM",
                template_grid_id="candidate_library_value_aligned",
                selected_family="schedule_value_learner_v2_plus",
                scored_candidates=[
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.2, 0.0),
                    _candidate("strict_reference", -90.0, 0.61, 850.0),
                    _candidate("raw_reference", -80.0, 0.72, 1400.0),
                ],
            ),
            _block(
                target_date="2026-06-02",
                market_venue="DAM",
                template_grid_id="candidate_library_forecast_guarded",
                selected_family="schedule_value_learner_v2_plus",
                scored_candidates=[
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.2, 0.0),
                    _candidate("strict_reference", -95.0, 0.58, 650.0),
                    _candidate("raw_reference", -70.0, 0.55, 900.0),
                ],
            ),
        ],
        default_threshold_uah=100.0,
    )

    assert tables["summary"]["forecast_candidate_library_update_gate_passed"] is False
    assert tables["summary"]["recommended_action"] == (
        "keep_current_forecast_hold_candidate_library_needs_redesign"
    )
    assert tables["summary"]["update_gate_failed_reason"] == "no_guard_passing_forecast_nonfallback"


def test_forecast_guard_audit_allows_manual_review_when_forecast_grid_passes() -> None:
    tables = build_forecast_guard_audit_tables(
        [
            _block(
                target_date="2026-06-02",
                market_venue="DAM",
                template_grid_id="candidate_library_value_aligned",
                selected_family="schedule_value_learner_v2_plus",
                scored_candidates=[
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.2, 0.0),
                    _candidate("strict_reference", -90.0, 0.61, 850.0),
                    _candidate("raw_reference", -80.0, 0.72, 1400.0),
                ],
            ),
            _block(
                target_date="2026-06-02",
                market_venue="DAM",
                template_grid_id="candidate_library_forecast_guarded",
                selected_family="strict_reference",
                scored_candidates=[
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.2, 0.0),
                    _candidate("strict_reference", -130.0, 0.32, 700.0),
                    _candidate("raw_reference", -70.0, 0.55, 900.0),
                ],
            ),
        ],
        default_threshold_uah=100.0,
    )

    assert tables["summary"]["forecast_candidate_library_update_gate_passed"] is True
    assert tables["summary"]["recommended_action"] == "manual_review_required_before_forecast_grid_update"
    assert tables["summary"]["candidate_library_forecast_guarded_switch_count"] == 1.0
    assert tables["summary"]["candidate_library_forecast_guarded_selected_value_improvement_uah"] > 0.0


def _block(
    *,
    target_date: str,
    market_venue: str,
    template_grid_id: str,
    selected_family: str,
    scored_candidates: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "target_delivery_date": target_date,
        "market_venue": market_venue,
        "template_grid_id": template_grid_id,
        "price_context_status": "pre_publication_forecast",
        "selected_schedule_family": selected_family,
        "selection_reason": (
            "guard_abstained_to_safe_fallback"
            if selected_family == "schedule_value_learner_v2_plus"
            else "predicted_guard_passed"
        ),
        "abstained_to_v2_plus": selected_family == "schedule_value_learner_v2_plus",
        "scored_candidates": scored_candidates,
    }


def _candidate(
    family: str,
    predicted_delta: float,
    predicted_tail: float,
    schedule_value: float,
) -> dict[str, object]:
    return {
        "dt_schedule_family_target": family,
        "dt_candidate_id_target": family,
        "dt_candidate_index_target": 1 if family == "schedule_value_learner_v2_plus" else 3,
        "predicted_regret_delta_vs_v2_plus_uah": predicted_delta,
        "predicted_tail_risk_probability": predicted_tail,
        "family_tail_risk_probability": 0.0,
        "schedule_value_uah": schedule_value,
        "total_throughput_mwh": 0.2,
        "safety_violation_count": 0,
        "template_clip_count": 0,
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "dt_lava_ready": False,
    }
