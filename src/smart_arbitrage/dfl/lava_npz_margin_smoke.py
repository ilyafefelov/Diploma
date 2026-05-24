"""Deterministic margin diagnostics for validated LAVA NPZ smoke artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from smart_arbitrage.dfl.dt_lava_research_metrics import (
    validate_dt_lava_research_metrics_payload,
)
from smart_arbitrage.dfl.lava_npz_smoke_contract import (
    validate_lava_npz_smoke_contract,
)

LAVA_NPZ_MARGIN_SMOKE_CLAIM_SCOPE = "lava_npz_margin_smoke_not_market_execution"
LAVA_NPZ_MARGIN_SMOKE_CANDIDATE_MODEL = "lava_npz_margin_smoke_v0"
LAVA_NPZ_ZERO_MARGIN_COMPARATOR = "zero_adjacent_margin_violation_reference"


def run_lava_npz_margin_smoke(
    npz_path: str | Path,
    *,
    seed: int = 0,
    tenant_id: str = "lava_npz_smoke_panel",
    source_model_name: str = "lava_schedule_neighbor_npz_smoke_v0",
    window_id: str = "lava_npz_smoke_window",
    v13_gate_status: str = "data_acquisition_needed",
) -> dict[str, Any]:
    """Compute adjacent-vertex margin diagnostics and emit validated metrics."""

    path = Path(npz_path)
    summary = validate_lava_npz_smoke_contract(path)
    with np.load(path, allow_pickle=False) as artifact:
        costs = artifact["cost_vector_matrix"].astype(float, copy=False)
        optimal = artifact["optimal_vertex_matrix"].astype(float, copy=False)
        adjacent = artifact["adjacent_vertex_tensor"].astype(float, copy=False)
        adjacent_mask = artifact["adjacent_mask"].astype(bool, copy=False)

    optimal_objective = np.einsum("ij,ij->i", costs, optimal)
    adjacent_objective = np.einsum("ij,ikj->ik", costs, adjacent)
    margin_violations = np.maximum(optimal_objective[:, None] - adjacent_objective, 0.0)
    valid_margin_violations = margin_violations[adjacent_mask]
    if valid_margin_violations.size < 1:
        raise ValueError("LAVA NPZ margin smoke needs at least one adjacent pair.")

    mean_margin_violation = float(np.mean(valid_margin_violations))
    max_margin_violation = float(np.max(valid_margin_violations))
    normalized = validate_dt_lava_research_metrics_payload(
        {
            "claim_scope": LAVA_NPZ_MARGIN_SMOKE_CLAIM_SCOPE,
            "tenant_id": tenant_id,
            "source_model_name": source_model_name,
            "window_id": window_id,
            "seed": seed,
            "comparator_model_name": LAVA_NPZ_ZERO_MARGIN_COMPARATOR,
            "candidate_model_name": LAVA_NPZ_MARGIN_SMOKE_CANDIDATE_MODEL,
            "mean_regret_uah": mean_margin_violation,
            "baseline_mean_regret_uah": 0.0,
            "v13_gate_status": v13_gate_status,
            "v13_candidate_generation_ready": bool(
                summary["v13_candidate_generation_ready"]
            ),
            "dt_lava_ready": bool(summary["dt_lava_ready"]),
            "permits_model_training": bool(summary["permits_model_training"]),
            "market_execution_enabled": False,
        }
    )
    normalized.update(
        {
            "lava_margin_violation_mean_uah": mean_margin_violation,
            "lava_margin_violation_max_uah": max_margin_violation,
            "lava_adjacent_pair_count": int(valid_margin_violations.size),
            "npz_claim_scope": summary["claim_scope"],
            "npz_instance_count": summary["instance_count"],
            "npz_valid_neighbor_count": summary["valid_neighbor_count"],
            "raw_hourly_action_imitation": bool(summary["raw_hourly_action_imitation"]),
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return normalized


__all__ = [
    "LAVA_NPZ_MARGIN_SMOKE_CANDIDATE_MODEL",
    "LAVA_NPZ_MARGIN_SMOKE_CLAIM_SCOPE",
    "LAVA_NPZ_ZERO_MARGIN_COMPARATOR",
    "run_lava_npz_margin_smoke",
]
