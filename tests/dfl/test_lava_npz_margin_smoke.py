from __future__ import annotations

import hashlib
import json
from pathlib import Path
import pickle

import numpy as np
import polars as pl
import pytest

from smart_arbitrage.dfl.lava_npz_margin_smoke import run_lava_npz_margin_smoke


def test_lava_npz_margin_smoke_emits_validated_research_metrics(
    tmp_path: Path,
) -> None:
    npz_path = tmp_path / "lava_smoke.npz"
    _write_margin_npz(npz_path)

    metrics = run_lava_npz_margin_smoke(
        npz_path,
        seed=13,
        tenant_id="lava_npz_smoke_panel",
        window_id="window_0",
    )

    assert metrics["claim_scope"] == "lava_npz_margin_smoke_not_market_execution"
    assert metrics["tenant_id"] == "lava_npz_smoke_panel"
    assert metrics["window_id"] == "window_0"
    assert metrics["seed"] == 13
    assert metrics["mean_regret_uah"] == pytest.approx(10.0)
    assert metrics["baseline_mean_regret_uah"] == pytest.approx(0.0)
    assert metrics["value_delta_vs_baseline_uah"] == pytest.approx(-10.0)
    assert metrics["lava_margin_violation_mean_uah"] == pytest.approx(10.0)
    assert metrics["lava_margin_violation_max_uah"] == pytest.approx(10.0)
    assert metrics["lava_adjacent_pair_count"] == 2
    assert metrics["npz_instance_count"] == 2
    assert metrics["v13_candidate_generation_ready"] is False
    assert metrics["dt_lava_ready"] is False
    assert metrics["permits_model_training"] is False
    assert metrics["market_execution_enabled"] is False


def test_lava_npz_margin_smoke_rejects_executable_npz(tmp_path: Path) -> None:
    npz_path = tmp_path / "bad_lava_smoke.npz"
    _write_margin_npz(npz_path, market_execution_enabled=True)

    with pytest.raises(ValueError, match="market_execution_enabled=false"):
        run_lava_npz_margin_smoke(npz_path)


def test_lava_npz_margin_smoke_rejects_missing_source_identity_vector(
    tmp_path: Path,
) -> None:
    npz_path = tmp_path / "bad_lava_smoke.npz"
    np.savez(
        npz_path,
        claim_scope=np.array("lava_npz_smoke_contract_not_market_execution"),
        feature_matrix=np.array([[0.2, 0.7]], dtype=float),
        cost_vector_matrix=np.array([[10.0, 0.0]], dtype=float),
        optimal_vertex_matrix=np.array([[1.0, 0.0]], dtype=float),
        adjacent_vertex_tensor=np.array([[[0.0, 1.0]]], dtype=float),
        adjacent_mask=np.array([[True]], dtype=bool),
        tenant_id_vector=np.array(["tenant_a"]),
        anchor_timestamp_vector=np.array(["2026-01-01T23:00:00"]),
        selected_candidate_model_name_vector=np.array(["candidate_a"]),
        v13_candidate_generation_ready=np.array(False),
        dt_lava_ready=np.array(False),
        permits_model_training=np.array(False),
        raw_hourly_action_imitation=np.array(False),
        market_execution_enabled=np.array(False),
    )

    with pytest.raises(ValueError, match="source_model_name_vector"):
        run_lava_npz_margin_smoke(npz_path)


def test_lava_npz_margin_smoke_cli_writes_metrics_json(tmp_path: Path) -> None:
    from scripts.run_lava_npz_margin_smoke import main

    npz_path = tmp_path / "lava_smoke.npz"
    output_path = tmp_path / "metrics.json"
    _write_margin_npz(npz_path)

    exit_code = main(
        [
            "--input",
            str(npz_path),
            "--output",
            str(output_path),
            "--seed",
            "7",
            "--window-id",
            "window_0",
        ]
    )

    assert exit_code == 0
    metrics = json.loads(output_path.read_text(encoding="utf-8"))
    assert metrics["candidate_model_name"] == "lava_npz_margin_smoke_v0"
    assert metrics["seed"] == 7
    assert metrics["market_execution_enabled"] is False


def test_lava_npz_margin_smoke_packet_cli_writes_research_artifacts(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)

    exit_code = main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--output-dir",
            str(output_dir),
            "--seed",
            "5",
            "--window-id",
            "window_0",
            "--max-instances",
            "2",
            "--max-neighbors",
            "1",
        ]
    )

    assert exit_code == 0
    manifest = json.loads(
        (output_dir / "lava_npz_margin_smoke_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["claim_scope"] == "lava_npz_margin_smoke_packet_not_market_execution"
    assert manifest["npz_path"].endswith("candidate_lava_smoke.npz")
    assert manifest["summary_json_path"].endswith("candidate_lava_smoke_summary.json")
    assert manifest["metrics_json_path"].endswith("candidate_lava_margin_metrics.json")
    assert manifest["aggregate_metrics_json_path"].endswith(
        "dt_lava_research_metrics_aggregate.json"
    )
    assert manifest["validation_summary_json_path"].endswith(
        "lava_npz_margin_smoke_packet_validation.json"
    )
    assert manifest["aggregate_metric_count"] == 1
    assert manifest["aggregate_promotion_gate"] is False
    assert manifest["baseline_comparison"]["baseline_comparison_ready"] is True
    assert manifest["baseline_comparison"]["strict_fallback_anchor_count"] == 1
    assert manifest["baseline_comparison"]["v2_plus_anchor_count"] == 1
    assert manifest["artifact_sha256"] == {
        "candidate_frame_pickle": _sha256(candidate_pickle),
        "npz": _sha256(output_dir / "candidate_lava_smoke.npz"),
        "summary_json": _sha256(output_dir / "candidate_lava_smoke_summary.json"),
        "metrics_json": _sha256(output_dir / "candidate_lava_margin_metrics.json"),
        "aggregate_metrics_json": _sha256(
            output_dir / "dt_lava_research_metrics_aggregate.json"
        ),
    }
    assert manifest["seed"] == 5
    assert manifest["window_id"] == "window_0"
    assert manifest["source_model_name"] == "lava_schedule_neighbor_npz_smoke_v0"
    assert manifest["v13_gate_status"] == "data_acquisition_needed"
    assert manifest["v13_candidate_generation_ready"] is False
    assert manifest["dt_lava_ready"] is False
    assert manifest["permits_model_training"] is False
    assert manifest["raw_hourly_action_imitation"] is False
    assert manifest["ci_smoke_only"] is True
    assert manifest["promotion_gate"] is False
    assert manifest["not_full_dfl"] is True
    assert manifest["not_market_execution"] is True
    assert manifest["market_execution_enabled"] is False

    metrics = json.loads(
        (output_dir / "candidate_lava_margin_metrics.json").read_text(
            encoding="utf-8"
        )
    )
    assert metrics["seed"] == 5
    assert metrics["baseline_comparison"]["baseline_comparison_ready"] is True
    assert metrics["market_execution_enabled"] is False
    assert (output_dir / "candidate_lava_smoke.npz").exists()

    aggregate = json.loads(
        (output_dir / "dt_lava_research_metrics_aggregate.json").read_text(
            encoding="utf-8"
        )
    )
    assert aggregate["claim_scope"] == "dt_lava_research_metrics_aggregate_not_market_execution"
    assert aggregate["metric_count"] == 1
    assert aggregate["ci_smoke_only"] is True
    assert aggregate["promotion_gate"] is False
    assert aggregate["permits_model_training"] is False
    assert aggregate["market_execution_enabled"] is False

    validation_summary = json.loads(
        (output_dir / "lava_npz_margin_smoke_packet_validation.json").read_text(
            encoding="utf-8"
        )
    )
    assert validation_summary["artifact_hashes_valid"] is True
    assert validation_summary["metrics_valid"] is True
    assert validation_summary["aggregate_valid"] is True
    assert validation_summary["baseline_comparison_valid"] is True
    assert validation_summary["baseline_comparison_ready"] is True
    assert validation_summary["strict_fallback_anchor_count"] == 1
    assert validation_summary["v2_plus_anchor_count"] == 1
    assert validation_summary["market_execution_enabled"] is False


def test_lava_npz_margin_smoke_packet_compares_strict_and_v2_plus_baselines(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)

    exit_code = main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--output-dir",
            str(output_dir),
            "--max-instances",
            "1",
            "--max-neighbors",
            "3",
        ]
    )

    assert exit_code == 0
    metrics = json.loads(
        (output_dir / "candidate_lava_margin_metrics.json").read_text(
            encoding="utf-8"
        )
    )
    comparison = metrics["baseline_comparison"]
    assert comparison["claim_scope"] == (
        "lava_npz_source_baseline_comparison_not_market_execution"
    )
    assert comparison["baseline_comparison_ready"] is True
    assert comparison["selected_instance_count"] == 1
    assert comparison["strict_fallback_anchor_count"] == 1
    assert comparison["v2_plus_anchor_count"] == 1
    assert comparison["candidate_mean_regret_uah"] == pytest.approx(20.0)
    assert comparison["strict_fallback_mean_regret_uah"] == pytest.approx(40.0)
    assert comparison["v2_plus_mean_regret_uah"] == pytest.approx(30.0)
    assert comparison["value_delta_vs_strict_fallback_uah"] == pytest.approx(20.0)
    assert comparison["value_delta_vs_v2_plus_uah"] == pytest.approx(10.0)
    assert comparison["mean_regret_improvement_ratio_vs_strict_fallback"] == (
        pytest.approx(0.5)
    )
    assert comparison["mean_regret_improvement_ratio_vs_v2_plus"] == pytest.approx(
        1.0 / 3.0
    )
    assert comparison["promotion_gate"] is False
    assert comparison["market_execution_enabled"] is False

    manifest = json.loads(
        (output_dir / "lava_npz_margin_smoke_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["baseline_comparison"] == comparison


def test_lava_npz_margin_smoke_packet_cli_attaches_v13_acquisition_summary(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    v13_summary_path = tmp_path / "v13_acquisition_summary.json"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)
    _write_v13_acquisition_summary(v13_summary_path)

    exit_code = main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--output-dir",
            str(output_dir),
            "--v13-acquisition-summary-json",
            str(v13_summary_path),
        ]
    )

    assert exit_code == 0
    manifest = json.loads(
        (output_dir / "lava_npz_margin_smoke_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["v13_acquisition_summary_json_path"] == str(v13_summary_path)
    assert manifest["artifact_sha256"]["v13_acquisition_summary_json"] == _sha256(
        v13_summary_path
    )
    assert manifest["v13_gate_status"] == "data_acquisition_needed"
    assert manifest["v13_acquisition_summary"] == {
        "blocked_rows": 5,
        "gate_status": "data_acquisition_needed",
        "market_execution_enabled": False,
        "max_prior_material_safe_switch_examples": 7,
        "min_safe_examples_required": 20,
        "readiness_rows": 5,
        "ready_rows": 0,
        "v13_candidate_generation_ready": False,
    }
    validation_summary = json.loads(
        (output_dir / "lava_npz_margin_smoke_packet_validation.json").read_text(
            encoding="utf-8"
        )
    )
    assert validation_summary["v13_acquisition_summary_attached"] is True
    assert validation_summary["v13_gate_status"] == "data_acquisition_needed"
    assert validation_summary["v13_acquisition_summary_json_path"] == str(
        v13_summary_path
    )
    assert validation_summary["v13_blocked_rows"] == 5
    assert validation_summary["v13_ready_rows"] == 0
    assert validation_summary["v13_readiness_rows"] == 5
    assert validation_summary["v13_max_prior_material_safe_switch_examples"] == 7
    assert validation_summary["v13_min_safe_examples_required"] == 20
    assert validation_summary["market_execution_enabled"] is False


def test_lava_npz_margin_smoke_packet_cli_rejects_conflicting_v13_summary_status(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    v13_summary_path = tmp_path / "v13_acquisition_summary.json"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)
    _write_v13_acquisition_summary(v13_summary_path)

    with pytest.raises(ValueError, match="v13-gate-status conflicts"):
        main(
            [
                "--candidate-frame-pickle",
                str(candidate_pickle),
                "--output-dir",
                str(output_dir),
                "--v13-acquisition-summary-json",
                str(v13_summary_path),
                "--v13-gate-status",
                "v13_candidate_generation_ready",
            ]
        )
    assert not (output_dir / "candidate_lava_margin_metrics.json").exists()
    assert not (output_dir / "lava_npz_margin_smoke_manifest.json").exists()


def test_lava_npz_margin_smoke_packet_cli_rejects_misleading_v13_ready_status(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)

    with pytest.raises(ValueError, match="cannot claim a ready V13 gate"):
        main(
            [
                "--candidate-frame-pickle",
                str(candidate_pickle),
                "--output-dir",
                str(output_dir),
                "--v13-gate-status",
                "v13_candidate_generation_ready",
            ]
        )
    assert not (output_dir / "candidate_lava_margin_metrics.json").exists()
    assert not (output_dir / "dt_lava_research_metrics_aggregate.json").exists()
    assert not (output_dir / "lava_npz_margin_smoke_packet_validation.json").exists()
    assert not (output_dir / "lava_npz_margin_smoke_manifest.json").exists()


def test_lava_npz_margin_smoke_packet_validator_accepts_hashed_packet(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main as packet_main
    from scripts.validate_lava_npz_margin_smoke_packet import main as validate_main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    validation_summary_path = tmp_path / "packet_validation_summary.json"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)
    packet_main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--output-dir",
            str(output_dir),
        ]
    )

    exit_code = validate_main(
        [
            "--manifest",
            str(output_dir / "lava_npz_margin_smoke_manifest.json"),
            "--output",
            str(validation_summary_path),
        ]
    )

    assert exit_code == 0
    validation_summary = json.loads(
        validation_summary_path.read_text(encoding="utf-8")
    )
    assert validation_summary["claim_scope"] == (
        "lava_npz_margin_smoke_packet_validation_not_market_execution"
    )
    assert validation_summary["artifact_hashes_valid"] is True
    assert validation_summary["metrics_valid"] is True
    assert validation_summary["aggregate_valid"] is True
    assert validation_summary["npz_contract_valid"] is True
    assert validation_summary["baseline_comparison_valid"] is True
    assert validation_summary["baseline_comparison_ready"] is True
    assert validation_summary["strict_fallback_anchor_count"] == 1
    assert validation_summary["v2_plus_anchor_count"] == 1
    assert validation_summary["promotion_gate"] is False
    assert validation_summary["permits_model_training"] is False
    assert validation_summary["market_execution_enabled"] is False


def test_lava_npz_margin_smoke_packet_validator_rejects_incomplete_baseline_comparison(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main as packet_main
    from scripts.validate_lava_npz_margin_smoke_packet import main as validate_main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)
    packet_main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--output-dir",
            str(output_dir),
            "--max-instances",
            "1",
        ]
    )

    metrics_path = output_dir / "candidate_lava_margin_metrics.json"
    manifest_path = output_dir / "lava_npz_margin_smoke_manifest.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    comparison = metrics["baseline_comparison"]
    comparison["baseline_comparison_ready"] = False
    comparison["strict_fallback_anchor_count"] = 0
    comparison["missing_strict_fallback_anchor_count"] = 1
    metrics_path.write_text(json.dumps(metrics), encoding="utf-8")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["baseline_comparison"] = comparison
    manifest["artifact_sha256"]["metrics_json"] = _sha256(metrics_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="baseline_comparison requires ready"):
        validate_main(
            [
                "--manifest",
                str(manifest_path),
                "--output",
                str(tmp_path / "packet_validation_summary.json"),
            ]
        )


def test_lava_npz_margin_smoke_packet_validator_rejects_tampered_artifact(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main as packet_main
    from scripts.validate_lava_npz_margin_smoke_packet import main as validate_main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)
    packet_main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--output-dir",
            str(output_dir),
        ]
    )
    metrics_path = output_dir / "candidate_lava_margin_metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    metrics["mean_regret_uah"] = metrics["mean_regret_uah"] + 1.0
    metrics_path.write_text(json.dumps(metrics), encoding="utf-8")

    with pytest.raises(ValueError, match="metrics_json SHA256 mismatch"):
        validate_main(
            [
                "--manifest",
                str(output_dir / "lava_npz_margin_smoke_manifest.json"),
                "--output",
                str(tmp_path / "packet_validation_summary.json"),
            ]
        )


def test_lava_npz_margin_smoke_packet_validator_rejects_tampered_v13_summary(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main as packet_main
    from scripts.validate_lava_npz_margin_smoke_packet import main as validate_main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    v13_summary_path = tmp_path / "v13_acquisition_summary.json"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)
    _write_v13_acquisition_summary(v13_summary_path)
    packet_main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--output-dir",
            str(output_dir),
            "--v13-acquisition-summary-json",
            str(v13_summary_path),
        ]
    )
    v13_payload = json.loads(v13_summary_path.read_text(encoding="utf-8"))
    v13_payload["readiness_summary"]["ready_rows"] = 5
    v13_summary_path.write_text(json.dumps(v13_payload), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="v13_acquisition_summary_json SHA256 mismatch",
    ):
        validate_main(
            [
                "--manifest",
                str(output_dir / "lava_npz_margin_smoke_manifest.json"),
                "--output",
                str(tmp_path / "packet_validation_summary.json"),
            ]
        )


def test_lava_npz_margin_smoke_packet_validator_rejects_tampered_v13_ready_claim(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main as packet_main
    from scripts.validate_lava_npz_margin_smoke_packet import main as validate_main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    v13_summary_path = tmp_path / "v13_acquisition_summary.json"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)
    _write_v13_acquisition_summary(v13_summary_path)
    packet_main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--output-dir",
            str(output_dir),
            "--v13-acquisition-summary-json",
            str(v13_summary_path),
        ]
    )
    manifest_path = output_dir / "lava_npz_margin_smoke_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["v13_candidate_generation_ready"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="v13_candidate_generation_ready does not match",
    ):
        validate_main(
            [
                "--manifest",
                str(manifest_path),
                "--output",
                str(tmp_path / "packet_validation_summary.json"),
            ]
        )


def test_lava_npz_margin_smoke_packet_validator_rejects_tampered_manifest_counts(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_margin_smoke_packet import main as packet_main
    from scripts.validate_lava_npz_margin_smoke_packet import main as validate_main

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    output_dir = tmp_path / "packet"
    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame_with_baselines(), file)
    packet_main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--output-dir",
            str(output_dir),
        ]
    )
    manifest_path = output_dir / "lava_npz_margin_smoke_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["npz_instance_count"] = int(manifest["npz_instance_count"]) + 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="npz_instance_count does not match"):
        validate_main(
            [
                "--manifest",
                str(manifest_path),
                "--output",
                str(tmp_path / "packet_validation_summary.json"),
            ]
        )


def _write_margin_npz(
    path: Path,
    *,
    market_execution_enabled: bool = False,
) -> None:
    np.savez(
        path,
        claim_scope=np.array("lava_npz_smoke_contract_not_market_execution"),
        feature_matrix=np.array([[0.2, 0.7], [0.4, 0.5]], dtype=float),
        cost_vector_matrix=np.array([[10.0, 0.0], [0.0, 10.0]], dtype=float),
        optimal_vertex_matrix=np.array([[1.0, 0.0], [0.0, 1.0]], dtype=float),
        adjacent_vertex_tensor=np.array(
            [
                [[0.0, 1.0]],
                [[1.0, 0.0]],
            ],
            dtype=float,
        ),
        adjacent_mask=np.array([[True], [True]], dtype=bool),
        tenant_id_vector=np.array(["tenant_a", "tenant_b"]),
        source_model_name_vector=np.array(["source_a", "source_b"]),
        anchor_timestamp_vector=np.array(
            ["2026-01-01T23:00:00", "2026-01-02T23:00:00"]
        ),
        selected_candidate_model_name_vector=np.array(["candidate_a", "candidate_b"]),
        v13_candidate_generation_ready=np.array(False),
        dt_lava_ready=np.array(False),
        permits_model_training=np.array(False),
        raw_hourly_action_imitation=np.array(False),
        market_execution_enabled=np.array(market_execution_enabled),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _candidate_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": "client_001_kyiv_mall",
                "source_model_name": "lava_schedule_neighbor_bridge_v1",
                "candidate_family": "safe_smoke_family",
                "candidate_model_name": "safe_train_candidate_a",
                "anchor_timestamp": "2026-01-01T23:00:00",
                "split_name": "train_selection",
                "eligible_for_final_selection": True,
                "actual_price_uah_mwh_vector": [1000.0, 4000.0],
                "dispatch_mw_vector": [0.25, -0.25],
                "regret_uah": 20.0,
                "selector_feature_schedule_distance_from_v2_plus": 0.25,
                "selector_feature_total_throughput_delta_mwh": 0.5,
                "selector_feature_terminal_soc_delta_fraction": 0.0125,
                "selector_feature_forecast_spread_uah_mwh": 3000.0,
                "selector_feature_total_degradation_penalty_uah": 2.5,
                "selector_feature_poland_shadow_candidate": 0.0,
                "selector_feature_oracle_train_diagnostic": 0.0,
                "market_execution_enabled": False,
            },
            {
                "tenant_id": "client_001_kyiv_mall",
                "source_model_name": "lava_schedule_neighbor_bridge_v1",
                "candidate_family": "safe_smoke_family",
                "candidate_model_name": "safe_train_candidate_b",
                "anchor_timestamp": "2026-01-01T23:00:00",
                "split_name": "train_selection",
                "eligible_for_final_selection": True,
                "actual_price_uah_mwh_vector": [1000.0, 4000.0],
                "dispatch_mw_vector": [0.0, 0.0],
                "regret_uah": 35.0,
                "selector_feature_schedule_distance_from_v2_plus": 0.0,
                "selector_feature_total_throughput_delta_mwh": 0.0,
                "selector_feature_terminal_soc_delta_fraction": 0.0,
                "selector_feature_forecast_spread_uah_mwh": 3000.0,
                "selector_feature_total_degradation_penalty_uah": 0.0,
                "selector_feature_poland_shadow_candidate": 0.0,
                "selector_feature_oracle_train_diagnostic": 0.0,
                "market_execution_enabled": False,
            },
        ]
    )


def _candidate_frame_with_baselines() -> pl.DataFrame:
    rows = _candidate_frame().to_dicts()
    base = rows[0]
    rows[0] = {
        **base,
        "candidate_family": "safe_smoke_family",
        "candidate_model_name": "safe_train_candidate_a",
        "regret_uah": 20.0,
        "dispatch_mw_vector": [0.25, -0.25],
    }
    rows[1] = {
        **base,
        "candidate_family": "neighbor_candidate",
        "candidate_model_name": "safe_train_candidate_b",
        "regret_uah": 35.0,
        "dispatch_mw_vector": [0.0, 0.0],
    }
    rows.append(
        {
            **base,
            "candidate_family": "strict_control",
            "candidate_model_name": "strict_similar_day",
            "regret_uah": 40.0,
            "dispatch_mw_vector": [0.1, -0.1],
        }
    )
    rows.append(
        {
            **base,
            "candidate_family": "frozen_v2_plus_fallback",
            "candidate_model_name": "schedule_value_learner_v2_plus",
            "regret_uah": 30.0,
            "dispatch_mw_vector": [0.2, -0.2],
        }
    )
    return pl.DataFrame(rows)


def _write_v13_acquisition_summary(
    path: Path,
    *,
    market_execution_enabled: bool = False,
) -> None:
    payload = {
        "claim_boundary": {
            "dt_lava_still_gated": True,
            "market_execution_enabled": market_execution_enabled,
            "not_deployed_decision_transformer_control": True,
            "not_full_dfl": True,
            "not_market_execution": True,
            "v13_stops_before_candidate_generation": True,
        },
        "readiness_summary": {
            "blocked_rows": 5,
            "max_prior_material_safe_switch_examples": 7,
            "min_safe_examples_required": 20,
            "readiness_decisions": ["data_acquisition_needed"],
            "readiness_rows": 5,
            "ready_rows": 0,
            "v13_candidate_generation_ready": False,
        },
        "acquisition_input_preflight_summary": {
            "data_acquisition_needed": True,
            "dt_lava_ready": False,
            "market_execution_enabled": market_execution_enabled,
            "permits_model_training": False,
            "v13_candidate_generation_ready": False,
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
