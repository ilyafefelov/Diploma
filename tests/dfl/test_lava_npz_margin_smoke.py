from __future__ import annotations

import json
from pathlib import Path

import numpy as np
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
        v13_candidate_generation_ready=np.array(False),
        dt_lava_ready=np.array(False),
        permits_model_training=np.array(False),
        raw_hourly_action_imitation=np.array(False),
        market_execution_enabled=np.array(market_execution_enabled),
    )
