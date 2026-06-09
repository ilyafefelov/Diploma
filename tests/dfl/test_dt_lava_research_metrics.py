from __future__ import annotations

import json

import pytest

from smart_arbitrage.dfl.dt_lava_research_metrics import (
    aggregate_dt_lava_research_metrics_payloads,
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


def test_dt_lava_research_metrics_aggregate_summarizes_research_smokes() -> None:
    first_payload = {
        "claim_scope": "lava_npz_margin_smoke_not_market_execution",
        "tenant_id": "client_001_kyiv_mall",
        "source_model_name": "lava_schedule_neighbor_npz_smoke_v0",
        "window_id": "window_0",
        "seed": 1,
        "comparator_model_name": "zero_adjacent_margin_violation_reference",
        "candidate_model_name": "lava_npz_margin_smoke_v0",
        "mean_regret_uah": 10.0,
        "baseline_mean_regret_uah": 0.0,
        "v13_gate_status": "data_acquisition_needed",
        "v13_candidate_generation_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }
    second_payload = {
        **first_payload,
        "window_id": "window_1",
        "seed": 2,
        "mean_regret_uah": 14.0,
    }

    aggregate = aggregate_dt_lava_research_metrics_payloads(
        [first_payload, second_payload]
    )

    assert aggregate["claim_scope"] == "dt_lava_research_metrics_aggregate_not_market_execution"
    assert aggregate["metric_count"] == 2
    assert aggregate["window_count"] == 2
    assert aggregate["seed_count"] == 2
    assert aggregate["mean_regret_uah"] == pytest.approx(12.0)
    assert aggregate["baseline_mean_regret_uah"] == pytest.approx(0.0)
    assert aggregate["value_delta_vs_baseline_uah"] == pytest.approx(-12.0)
    assert aggregate["v13_gate_statuses"] == ["data_acquisition_needed"]
    assert aggregate["v13_candidate_generation_ready"] is False
    assert aggregate["dt_lava_ready"] is False
    assert aggregate["permits_model_training"] is False
    assert aggregate["ci_smoke_only"] is True
    assert aggregate["promotion_gate"] is False
    assert aggregate["market_execution_enabled"] is False
    assert aggregate["by_window"]["window_0"]["mean_regret_uah"] == pytest.approx(10.0)
    assert aggregate["by_window"]["window_1"]["mean_regret_uah"] == pytest.approx(14.0)


def test_dt_lava_research_metrics_aggregate_cli_writes_summary(tmp_path) -> None:
    from scripts.aggregate_dt_lava_research_metrics import main

    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    payload = {
        "claim_scope": "lava_npz_margin_smoke_not_market_execution",
        "tenant_id": "client_001_kyiv_mall",
        "source_model_name": "lava_schedule_neighbor_npz_smoke_v0",
        "window_id": "window_0",
        "seed": 1,
        "comparator_model_name": "zero_adjacent_margin_violation_reference",
        "candidate_model_name": "lava_npz_margin_smoke_v0",
        "mean_regret_uah": 10.0,
        "baseline_mean_regret_uah": 0.0,
        "v13_gate_status": "data_acquisition_needed",
        "v13_candidate_generation_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }
    (metrics_dir / "window_0_metrics.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    (metrics_dir / "window_1_metrics.json").write_text(
        json.dumps({**payload, "window_id": "window_1", "seed": 2}),
        encoding="utf-8",
    )
    output_path = tmp_path / "aggregate.json"

    exit_code = main(
        [
            "--input-dir",
            str(metrics_dir),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    aggregate = json.loads(output_path.read_text(encoding="utf-8"))
    assert aggregate["metric_count"] == 2
    assert aggregate["promotion_gate"] is False
    assert aggregate["market_execution_enabled"] is False
    assert sorted(aggregate["by_window"]) == ["window_0", "window_1"]


def test_dt_lava_research_metrics_aggregate_cli_accepts_packet_directory(
    tmp_path,
) -> None:
    from scripts.aggregate_dt_lava_research_metrics import main

    packet_dir = tmp_path / "lava_npz_smoke"
    packet_dir.mkdir()
    payload = {
        "claim_scope": "lava_npz_margin_smoke_not_market_execution",
        "tenant_id": "client_001_kyiv_mall",
        "source_model_name": "lava_schedule_neighbor_npz_smoke_v0",
        "window_id": "window_0",
        "seed": 1,
        "comparator_model_name": "zero_adjacent_margin_violation_reference",
        "candidate_model_name": "lava_npz_margin_smoke_v0",
        "mean_regret_uah": 10.0,
        "baseline_mean_regret_uah": 0.0,
        "v13_gate_status": "data_acquisition_needed",
        "v13_candidate_generation_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }
    (packet_dir / "candidate_lava_margin_metrics.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    (packet_dir / "candidate_lava_smoke_summary.json").write_text(
        json.dumps(
            {
                "claim_scope": "lava_npz_margin_smoke_packet_not_market_execution",
                "row_count": 3,
                "market_execution_enabled": False,
            }
        ),
        encoding="utf-8",
    )
    (packet_dir / "lava_npz_margin_smoke_manifest.json").write_text(
        json.dumps(
            {
                "claim_scope": "lava_npz_margin_smoke_packet_not_market_execution",
                "ci_smoke_only": True,
                "market_execution_enabled": False,
            }
        ),
        encoding="utf-8",
    )
    output_path = packet_dir / "dt_lava_research_metrics_aggregate.json"
    output_path.write_text(
        json.dumps(
            {
                "claim_scope": "stale_aggregate_not_market_execution",
                "metric_count": 99,
                "market_execution_enabled": False,
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--input-dir",
            str(packet_dir),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    aggregate = json.loads(output_path.read_text(encoding="utf-8"))
    assert aggregate["metric_count"] == 1
    assert aggregate["by_window"]["window_0"]["metric_count"] == 1
    assert aggregate["promotion_gate"] is False
    assert aggregate["market_execution_enabled"] is False
