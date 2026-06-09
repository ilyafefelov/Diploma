from __future__ import annotations

import csv
from pathlib import Path

from scripts.materialize_hf_live_safe_switch_shadow_demo_evidence import (
    build_hf_live_safe_switch_shadow_demo_evidence,
    write_hf_live_safe_switch_shadow_demo_evidence,
)


def test_hf_live_safe_switch_shadow_demo_evidence_writes_four_canonical_cases(
    tmp_path: Path,
) -> None:
    packet = build_hf_live_safe_switch_shadow_demo_evidence(
        run_slug="hf_shadow_demo_evidence_test",
        tenant_id="client_003_dnipro_factory",
        case_responses={
            "official_dam_proof": _shadow_response(
                market_venue="DAM",
                target_date="2026-05-02",
                family="strict_reference",
                non_hold_rows=4,
                selected_value=2765.71,
                metrics={"official_context_published": 1.0, "source_backed_price_context": 1.0},
            ),
            "forecast_dam_action": _shadow_response(
                market_venue="DAM",
                target_date="2026-06-02",
                family="schedule_value_learner_v2_reference",
                non_hold_rows=4,
                selected_value=318.86,
                metrics={
                    "forecast_context_pre_publication": 1.0,
                    "source_backed_price_context": 1.0,
                    "candidate_template_grid_forecast_guarded": 1.0,
                    "forecast_guard_audit_passed": 1.0,
                },
            ),
            "forecast_dam_abstention": _shadow_response(
                market_venue="DAM",
                target_date="2026-06-03",
                family="schedule_value_learner_v2_plus",
                non_hold_rows=0,
                selected_value=0.0,
                metrics={
                    "forecast_context_pre_publication": 1.0,
                    "source_backed_price_context": 1.0,
                    "forecast_guard_abstained_to_safe_fallback": 1.0,
                },
            ),
            "forecast_idm_abstention": _shadow_response(
                market_venue="IDM",
                target_date="2026-06-02",
                family="schedule_value_learner_v2_plus",
                non_hold_rows=0,
                selected_value=0.0,
                metrics={
                    "forecast_context_pre_publication": 1.0,
                    "source_backed_price_context": 1.0,
                    "forecast_guard_abstained_to_safe_fallback": 1.0,
                },
            ),
        },
    )

    assert packet["demo_evidence_passed"] is True
    assert packet["case_count"] == 4.0
    assert packet["nonfallback_case_count"] == 2.0
    assert packet["guarded_abstention_case_count"] == 2.0
    assert packet["market_execution_enabled"] is False
    assert packet["promotion_gate_passed"] is False
    assert packet["proposed_bid_emitted"] is False
    assert packet["market_order_payload_emitted"] is False
    assert [case["case_id"] for case in packet["cases"]] == [
        "official_dam_proof",
        "forecast_dam_action",
        "forecast_dam_abstention",
        "forecast_idm_abstention",
    ]

    paths = write_hf_live_safe_switch_shadow_demo_evidence(
        output_dir=tmp_path,
        packet=packet,
        response_payloads={
            case_id: response
            for case_id, response in {
                "official_dam_proof": _shadow_response(
                    market_venue="DAM",
                    target_date="2026-05-02",
                    family="strict_reference",
                    non_hold_rows=4,
                    selected_value=2765.71,
                    metrics={"official_context_published": 1.0, "source_backed_price_context": 1.0},
                ),
            }.items()
        },
    )

    assert paths["summary_json"].exists()
    assert paths["summary_md"].exists()
    assert paths["demo_cases_csv"].exists()
    assert paths["response_dir"].joinpath("official_dam_proof.json").exists()
    assert "Forecast DAM action" in paths["summary_md"].read_text(encoding="utf-8")
    rows = list(csv.DictReader(paths["demo_cases_csv"].open(newline="", encoding="utf-8")))
    assert len(rows) == 4
    assert rows[1]["case_id"] == "forecast_dam_action"


def test_hf_live_safe_switch_shadow_demo_evidence_blocks_market_payloads() -> None:
    response = _shadow_response(
        market_venue="DAM",
        target_date="2026-05-02",
        family="strict_reference",
        non_hold_rows=4,
        selected_value=2765.71,
        metrics={"official_context_published": 1.0, "source_backed_price_context": 1.0},
    )
    response["market_execution_enabled"] = True
    response["proposed_bid"] = {"venue": "DAM"}
    packet = build_hf_live_safe_switch_shadow_demo_evidence(
        run_slug="hf_shadow_demo_evidence_test",
        tenant_id="client_003_dnipro_factory",
        case_responses={"official_dam_proof": response},
    )

    assert packet["demo_evidence_passed"] is False
    assert packet["cases"][0]["case_passed"] is False
    assert packet["cases"][0]["market_execution_enabled"] is True
    assert packet["cases"][0]["prohibited_market_payload_present"] is True


def _shadow_response(
    *,
    market_venue: str,
    target_date: str,
    family: str,
    non_hold_rows: int,
    selected_value: float,
    metrics: dict[str, float],
) -> dict[str, object]:
    schedule = [
        _schedule_row(action="discharge" if index < non_hold_rows else "hold")
        for index in range(24)
    ]
    return {
        "preview_source_id": "hf_live_safe_switch_value_aligned_shadow",
        "preview_status": "value_aligned_shadow_not_promoted",
        "selected_schedule_family": family,
        "target_delivery_window_start": f"{target_date}T00:00:00",
        "market_venue": market_venue,
        "market_execution_enabled": False,
        "market_order_payload_emitted": False,
        "promotion_gate_passed": False,
        "dt_lava_ready": False,
        "comparison_metrics": {
            "selected_candidate_estimated_value_uah": selected_value,
            "predicted_regret_delta_vs_v2_plus_uah": -144.0 if family != "schedule_value_learner_v2_plus" else 0.0,
            **metrics,
        },
        "recommendation_schedule": schedule,
    }


def _schedule_row(*, action: str) -> dict[str, object]:
    power = 0.25 if action != "hold" else 0.0
    return {
        "action": action,
        "recommended_net_power_mw": power,
        "regret_uah": None,
        "market_execution_enabled": False,
        "market_order_payload_emitted": False,
        "proposed_bid_status": "not_emitted_operator_preview",
    }
