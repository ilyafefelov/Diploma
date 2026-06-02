from __future__ import annotations

from types import SimpleNamespace

from scripts.audit_hf_live_safe_switch_selection import (
    build_selection_audit_tables,
    validate_source_backed_price_context,
    write_selection_audit_outputs,
)
from smart_arbitrage.dfl.hf_live_safe_switch_preview import (
    SUPPORTED_TEMPLATE_GRIDS,
    template_grid_specs,
)


def test_hf_live_selection_audit_writes_read_only_outputs(tmp_path) -> None:
    tables = build_selection_audit_tables(
        [
            {
                "target_delivery_date": "2026-05-25",
                "template_grid_id": "default",
                "scored_candidates": [
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.1, 0.0),
                    _candidate("strict_reference", -110.0, 0.2, 900.0),
                    _candidate("raw_reference", -150.0, 0.8, 2400.0),
                ],
            }
        ],
        thresholds_uah=(100.0,),
        default_threshold_uah=100.0,
    )

    paths = write_selection_audit_outputs(
        output_dir=tmp_path,
        run_slug="hf_live_selection_audit_test",
        tenant_id="client_003_dnipro_factory",
        market_venue="DAM",
        thresholds_uah=(100.0,),
        tables=tables,
    )

    assert paths["candidate_scores_csv"].exists()
    assert paths["threshold_summary_csv"].exists()
    assert paths["template_summary_csv"].exists()
    summary = paths["summary_json"].read_text(encoding="utf-8")
    assert '"market_execution_enabled": false' in summary
    assert '"promotion_gate_passed": false' in summary
    assert '"dt_lava_ready": false' in summary
    assert '"proposed_bid_emitted": false' in summary
    assert '"market_order_payload_emitted": false' in summary
    assert "strict_reference" in paths["template_summary_csv"].read_text(encoding="utf-8")


def test_candidate_library_v2_grid_preserves_hf_family_names() -> None:
    specs = template_grid_specs("candidate_library_v2")
    hf_bundle_families = {
        "raw_reference",
        "schedule_value_learner_v2_plus",
        "schedule_value_learner_v2_reference",
        "strict_reference",
    }

    assert "candidate_library_v2" in SUPPORTED_TEMPLATE_GRIDS
    assert specs is not None
    assert set(specs).issubset(hf_bundle_families)
    assert specs["schedule_value_learner_v2_plus"].active_hour_count == 0
    assert specs["raw_reference"].power_fraction < 0.5
    assert specs["schedule_value_learner_v2_reference"].active_hour_count >= 3


def test_candidate_library_value_aligned_grid_targets_selected_value() -> None:
    specs = template_grid_specs("candidate_library_value_aligned")
    hf_bundle_families = {
        "raw_reference",
        "schedule_value_learner_v2_plus",
        "schedule_value_learner_v2_reference",
        "strict_reference",
    }

    assert "candidate_library_value_aligned" in SUPPORTED_TEMPLATE_GRIDS
    assert specs is not None
    assert set(specs).issubset(hf_bundle_families)
    assert specs["schedule_value_learner_v2_plus"].active_hour_count == 0
    assert specs["raw_reference"].active_hour_count == 3
    assert specs["schedule_value_learner_v2_reference"].power_fraction == 0.28
    assert specs["strict_reference"].active_hour_count == 4


def test_candidate_library_forecast_guarded_grid_preserves_hf_family_names() -> None:
    specs = template_grid_specs("candidate_library_forecast_guarded")
    hf_bundle_families = {
        "raw_reference",
        "schedule_value_learner_v2_plus",
        "schedule_value_learner_v2_reference",
        "strict_reference",
    }

    assert "candidate_library_forecast_guarded" in SUPPORTED_TEMPLATE_GRIDS
    assert specs is not None
    assert set(specs).issubset(hf_bundle_families)
    assert specs["schedule_value_learner_v2_plus"].active_hour_count == 0
    assert specs["raw_reference"].power_fraction < 0.28
    assert specs["schedule_value_learner_v2_reference"].power_fraction < 0.28
    assert specs["strict_reference"].active_hour_count <= 3


def test_hf_live_selection_audit_blocks_weak_high_gap_candidate_library() -> None:
    tables = build_selection_audit_tables(
        [
            {
                "target_delivery_date": "2026-05-25",
                "template_grid_id": "default",
                "scored_candidates": [
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.1, 0.0),
                    _candidate("strict_reference", -120.0, 0.2, 100.0),
                    _candidate("raw_reference", -10.0, 0.8, 1000.0),
                ],
            },
            {
                "target_delivery_date": "2026-05-25",
                "template_grid_id": "candidate_library_v2",
                "scored_candidates": [
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.1, 0.0),
                    _candidate("strict_reference", -130.0, 0.2, 150.0),
                    _candidate("raw_reference", -10.0, 0.8, 1000.0),
                ],
            },
        ],
        thresholds_uah=(100.0,),
        default_threshold_uah=100.0,
    )

    assert tables["summary"]["audit_passed_for_bundle_update"] is False
    assert (
        tables["summary"]["recommended_action"]
        == "keep_current_bundle_candidate_library_needs_redesign"
    )
    assert tables["summary"]["update_gate_failed_reason"] == "value_gap_not_substantially_reduced"
    assert tables["summary"]["candidate_library_v2_value_gap_ratio_vs_default"] > 0.75


def test_hf_live_selection_audit_allows_value_aligned_manual_review_gate() -> None:
    tables = build_selection_audit_tables(
        [
            {
                "target_delivery_date": "2026-05-25",
                "template_grid_id": "default",
                "scored_candidates": [
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.1, 0.0),
                    _candidate("strict_reference", -120.0, 0.2, 500.0),
                    _candidate("raw_reference", -10.0, 0.8, 2000.0),
                ],
            },
            {
                "target_delivery_date": "2026-05-25",
                "template_grid_id": "candidate_library_value_aligned",
                "scored_candidates": [
                    _candidate("schedule_value_learner_v2_plus", 0.0, 0.1, 0.0),
                    _candidate("strict_reference", -130.0, 0.2, 1400.0),
                    _candidate("raw_reference", -10.0, 0.7, 2000.0),
                ],
            },
        ],
        thresholds_uah=(100.0,),
        default_threshold_uah=100.0,
    )

    assert tables["summary"]["audit_passed_for_bundle_update"] is True
    assert (
        tables["summary"]["recommended_action"]
        == "manual_review_required_before_bundle_update"
    )
    assert tables["summary"]["update_gate_candidate_template_grid_id"] == (
        "candidate_library_value_aligned"
    )
    assert tables["summary"]["update_gate_failed_reason"] == ""
    assert tables["summary"]["candidate_library_value_aligned_selected_value_improvement_uah"] > 0


def test_hf_live_selection_audit_rejects_non_official_or_incomplete_context() -> None:
    validate_source_backed_price_context(
        SimpleNamespace(price_context_status="official_published", delivery_forecast=[object()] * 24)
    )

    try:
        validate_source_backed_price_context(
            SimpleNamespace(
                price_context_status="pre_publication_forecast",
                delivery_forecast=[object()] * 24,
            )
        )
    except ValueError as error:
        assert "official_published" in str(error)
    else:
        raise AssertionError("pre-publication contexts must be rejected")

    try:
        validate_source_backed_price_context(
            SimpleNamespace(price_context_status="official_published", delivery_forecast=[object()] * 23)
        )
    except ValueError as error:
        assert "24 hourly" in str(error)
    else:
        raise AssertionError("incomplete official contexts must be rejected")


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
    }
