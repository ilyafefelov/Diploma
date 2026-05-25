from __future__ import annotations

import json
from pathlib import Path
import pickle
from datetime import datetime

import numpy as np
import polars as pl
import pytest

from smart_arbitrage.dfl.lava_npz_smoke_contract import (
    validate_lava_npz_smoke_contract,
    write_lava_npz_smoke_artifact_from_candidate_frame,
)


def test_lava_npz_smoke_contract_accepts_blocked_research_artifact(
    tmp_path: Path,
) -> None:
    npz_path = tmp_path / "lava_smoke.npz"
    _write_lava_npz(npz_path)

    summary = validate_lava_npz_smoke_contract(npz_path)

    assert summary == {
        "claim_scope": "lava_npz_smoke_contract_not_market_execution",
        "instance_count": 2,
        "feature_count": 3,
        "decision_dimension": 4,
        "max_neighbor_count": 2,
        "valid_neighbor_count": 3,
        "v13_candidate_generation_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "raw_hourly_action_imitation": False,
        "market_execution_enabled": False,
    }


def test_lava_npz_smoke_contract_rejects_execution_and_premature_training(
    tmp_path: Path,
) -> None:
    npz_path = tmp_path / "market_execution.npz"
    _write_lava_npz(npz_path, market_execution_enabled=True)

    with pytest.raises(ValueError, match="market_execution_enabled=false"):
        validate_lava_npz_smoke_contract(npz_path)

    npz_path = tmp_path / "premature_training.npz"
    _write_lava_npz(npz_path, permits_model_training=True)

    with pytest.raises(ValueError, match="permits_model_training"):
        validate_lava_npz_smoke_contract(npz_path)


def test_lava_npz_smoke_contract_rejects_bad_shapes_or_empty_neighbors(
    tmp_path: Path,
) -> None:
    npz_path = tmp_path / "bad_shape.npz"
    _write_lava_npz(npz_path, adjacent_vertex_tensor=np.ones((2, 2, 3)))

    with pytest.raises(ValueError, match="adjacent_vertex_tensor"):
        validate_lava_npz_smoke_contract(npz_path)

    npz_path = tmp_path / "empty_neighbors.npz"
    _write_lava_npz(npz_path, adjacent_mask=np.zeros((2, 2), dtype=bool))

    with pytest.raises(ValueError, match="at least one valid adjacent"):
        validate_lava_npz_smoke_contract(npz_path)


def test_lava_npz_smoke_contract_cli_writes_summary(tmp_path: Path) -> None:
    from scripts.validate_lava_npz_smoke_contract import main

    npz_path = tmp_path / "lava_smoke.npz"
    summary_path = tmp_path / "summary.json"
    _write_lava_npz(npz_path)

    exit_code = main(["--input", str(npz_path), "--output", str(summary_path)])

    assert exit_code == 0
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["claim_scope"] == "lava_npz_smoke_contract_not_market_execution"
    assert summary["market_execution_enabled"] is False
    assert summary["valid_neighbor_count"] == 3


def test_lava_npz_smoke_artifact_is_sourced_from_train_candidate_evidence(
    tmp_path: Path,
) -> None:
    npz_path = tmp_path / "sourced_lava_smoke.npz"

    summary = write_lava_npz_smoke_artifact_from_candidate_frame(
        _candidate_frame(),
        npz_path,
        max_instances=2,
        max_neighbors=1,
    )

    assert summary["instance_count"] == 2
    assert summary["max_neighbor_count"] == 1
    assert summary["valid_neighbor_count"] == 2
    assert summary["market_execution_enabled"] is False
    with np.load(npz_path, allow_pickle=False) as artifact:
        assert artifact["tenant_id_vector"].tolist() == [
            "client_001_kyiv_mall",
            "client_002_lviv_office",
        ]
        assert artifact["selected_candidate_model_name_vector"].tolist() == [
            "safe_train_candidate_a",
            "safe_train_candidate_a",
        ]
        assert artifact["optimal_vertex_matrix"].shape == (2, 2)
        assert artifact["adjacent_vertex_tensor"].shape == (2, 1, 2)


def test_lava_npz_smoke_artifact_rejects_unsourced_or_executable_candidates(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="market execution"):
        write_lava_npz_smoke_artifact_from_candidate_frame(
            _candidate_frame(market_execution_enabled=True),
            tmp_path / "bad.npz",
        )

    with pytest.raises(ValueError, match="at least one eligible train anchor"):
        write_lava_npz_smoke_artifact_from_candidate_frame(
            _candidate_frame(single_candidate_per_anchor=True),
            tmp_path / "no_neighbors.npz",
        )


def test_lava_npz_smoke_artifact_cli_writes_npz_and_summary(
    tmp_path: Path,
) -> None:
    from scripts.materialize_lava_npz_smoke_artifact import main

    candidate_pickle_path = tmp_path / "candidate_frame.pkl"
    npz_path = tmp_path / "lava_smoke.npz"
    summary_path = tmp_path / "lava_smoke_summary.json"
    with candidate_pickle_path.open("wb") as file:
        pickle.dump(_candidate_frame(), file)

    exit_code = main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle_path),
            "--output-npz",
            str(npz_path),
            "--summary-json",
            str(summary_path),
            "--max-instances",
            "2",
            "--max-neighbors",
            "1",
        ]
    )

    assert exit_code == 0
    assert npz_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["claim_scope"] == "lava_npz_smoke_contract_not_market_execution"
    assert summary["valid_neighbor_count"] == 2
    assert summary["market_execution_enabled"] is False


def _write_lava_npz(
    path: Path,
    *,
    adjacent_vertex_tensor: np.ndarray | None = None,
    adjacent_mask: np.ndarray | None = None,
    market_execution_enabled: bool = False,
    permits_model_training: bool = False,
) -> None:
    np.savez(
        path,
        claim_scope=np.array("lava_npz_smoke_contract_not_market_execution"),
        feature_matrix=np.array(
            [
                [0.2, 0.7, 1.0],
                [0.4, 0.5, 0.0],
            ],
            dtype=float,
        ),
        cost_vector_matrix=np.array(
            [
                [11.0, 7.0, 5.0, 2.0],
                [8.0, 6.0, 4.0, 3.0],
            ],
            dtype=float,
        ),
        optimal_vertex_matrix=np.array(
            [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
            ],
            dtype=float,
        ),
        adjacent_vertex_tensor=adjacent_vertex_tensor
        if adjacent_vertex_tensor is not None
        else np.array(
            [
                [[0.0, 1.0, 0.0, 0.0], [0.0, 0.0, 1.0, 0.0]],
                [[1.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 1.0]],
            ],
            dtype=float,
        ),
        adjacent_mask=adjacent_mask
        if adjacent_mask is not None
        else np.array([[True, True], [True, False]], dtype=bool),
        tenant_id_vector=np.array(
            ["client_001_kyiv_mall", "client_002_lviv_office"]
        ),
        source_model_name_vector=np.array(
            ["lava_schedule_neighbor_bridge_v1", "lava_schedule_neighbor_bridge_v1"]
        ),
        anchor_timestamp_vector=np.array(
            ["2026-01-01T23:00:00", "2026-01-02T23:00:00"]
        ),
        selected_candidate_model_name_vector=np.array(
            ["safe_train_candidate_a", "safe_train_candidate_a"]
        ),
        v13_candidate_generation_ready=np.array(False),
        dt_lava_ready=np.array(False),
        permits_model_training=np.array(permits_model_training),
        raw_hourly_action_imitation=np.array(False),
        market_execution_enabled=np.array(market_execution_enabled),
    )


def _candidate_frame(
    *,
    market_execution_enabled: bool = False,
    single_candidate_per_anchor: bool = False,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_index, tenant_id in enumerate(
        ("client_001_kyiv_mall", "client_002_lviv_office")
    ):
        anchor = datetime(2026, 1, 1 + tenant_index, 23)
        rows.append(
            _candidate_row(
                tenant_id=tenant_id,
                anchor=anchor,
                split_name="train_selection",
                candidate_model_name="safe_train_candidate_a",
                regret_uah=20.0,
                dispatch=(0.25, -0.25),
                market_execution_enabled=market_execution_enabled,
            )
        )
        if not single_candidate_per_anchor:
            rows.append(
                _candidate_row(
                    tenant_id=tenant_id,
                    anchor=anchor,
                    split_name="train_selection",
                    candidate_model_name="safe_train_candidate_b",
                    regret_uah=35.0,
                    dispatch=(0.0, 0.0),
                    market_execution_enabled=market_execution_enabled,
                )
            )
        rows.append(
            _candidate_row(
                tenant_id=tenant_id,
                anchor=anchor,
                split_name="final_holdout",
                candidate_model_name="final_holdout_candidate",
                regret_uah=1.0,
                dispatch=(0.9, -0.9),
                market_execution_enabled=market_execution_enabled,
            )
        )
    return pl.DataFrame(rows)


def _candidate_row(
    *,
    tenant_id: str,
    anchor: datetime,
    split_name: str,
    candidate_model_name: str,
    regret_uah: float,
    dispatch: tuple[float, float],
    market_execution_enabled: bool,
) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "source_model_name": "lava_schedule_neighbor_bridge_v1",
        "candidate_family": "safe_smoke_family",
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": anchor,
        "split_name": split_name,
        "eligible_for_final_selection": True,
        "actual_price_uah_mwh_vector": [1000.0, 4000.0],
        "dispatch_mw_vector": list(dispatch),
        "regret_uah": regret_uah,
        "selector_feature_schedule_distance_from_v2_plus": abs(dispatch[0]),
        "selector_feature_total_throughput_delta_mwh": abs(dispatch[0])
        + abs(dispatch[1]),
        "selector_feature_terminal_soc_delta_fraction": dispatch[0] * 0.05,
        "selector_feature_forecast_spread_uah_mwh": 3000.0,
        "selector_feature_total_degradation_penalty_uah": 2.5,
        "selector_feature_poland_shadow_candidate": 0.0,
        "selector_feature_oracle_train_diagnostic": 0.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": market_execution_enabled,
    }
