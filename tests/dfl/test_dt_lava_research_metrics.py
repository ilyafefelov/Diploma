from __future__ import annotations

import json

import pytest

from smart_arbitrage.dfl.dt_lava_research_metrics import (
    validate_dt_lava_research_metrics_payload,
)


def test_dt_lava_research_metrics_contract_accepts_blocked_research_smoke() -> None:
    payload = {
        "claim_scope": "dt_lava_research_smoke_not_market_execution",
        "tenant_id": "client_003_dnipro_factory",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "window_id": "2026-01-01T23:00:00",
        "seed": 13,
        "comparator_model_name": "schedule_value_learner_v2_plus",
        "candidate_model_name": "lava_schedule_neighbor_npz_smoke_v0",
        "mean_regret_uah": 121.5,
        "baseline_mean_regret_uah": 118.0,
        "v13_gate_status": "data_acquisition_needed",
        "v13_candidate_generation_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }

    normalized = validate_dt_lava_research_metrics_payload(payload)

    assert normalized["claim_scope"] == "dt_lava_research_smoke_not_market_execution"
    assert normalized["market_execution_enabled"] is False
    assert normalized["permits_model_training"] is False
    assert normalized["value_delta_vs_baseline_uah"] == pytest.approx(-3.5)


def test_dt_lava_research_metrics_contract_rejects_execution_or_premature_training() -> None:
    payload = {
        "claim_scope": "dt_lava_research_smoke_not_market_execution",
        "tenant_id": "client_003_dnipro_factory",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "window_id": "2026-01-01T23:00:00",
        "seed": 13,
        "comparator_model_name": "schedule_value_learner_v2_plus",
        "candidate_model_name": "lava_schedule_neighbor_npz_smoke_v0",
        "mean_regret_uah": 121.5,
        "baseline_mean_regret_uah": 118.0,
        "v13_gate_status": "data_acquisition_needed",
        "v13_candidate_generation_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": True,
        "market_execution_enabled": False,
    }

    with pytest.raises(ValueError, match="permits_model_training"):
        validate_dt_lava_research_metrics_payload(payload)

    payload["permits_model_training"] = False
    payload["market_execution_enabled"] = True
    with pytest.raises(ValueError, match="market_execution_enabled=false"):
        validate_dt_lava_research_metrics_payload(payload)


def test_dt_lava_research_metrics_cli_writes_normalized_payload(
    tmp_path,
) -> None:
    from scripts.validate_dt_lava_research_metrics import main

    raw_path = tmp_path / "raw_metrics.json"
    output_path = tmp_path / "normalized_metrics.json"
    raw_path.write_text(
        json.dumps(
            {
                "claim_scope": "dt_lava_research_smoke_not_market_execution",
                "tenant_id": "client_003_dnipro_factory",
                "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
                "window_id": "2026-01-01T23:00:00",
                "seed": 13,
                "comparator_model_name": "schedule_value_learner_v2_plus",
                "candidate_model_name": "lava_schedule_neighbor_npz_smoke_v0",
                "mean_regret_uah": 121.5,
                "baseline_mean_regret_uah": 118.0,
                "v13_gate_status": "data_acquisition_needed",
                "v13_candidate_generation_ready": False,
                "dt_lava_ready": False,
                "permits_model_training": False,
                "market_execution_enabled": False,
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(["--input", str(raw_path), "--output", str(output_path)])

    assert exit_code == 0
    normalized = json.loads(output_path.read_text(encoding="utf-8"))
    assert normalized["market_execution_enabled"] is False
    assert normalized["value_delta_vs_baseline_uah"] == pytest.approx(-3.5)
