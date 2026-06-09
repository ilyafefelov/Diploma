"""Deterministic margin diagnostics for validated LAVA NPZ smoke artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from smart_arbitrage.dfl.dt_lava_research_metrics import (
    validate_dt_lava_research_metrics_payload,
)
from smart_arbitrage.dfl.lava_npz_smoke_contract import (
    validate_lava_npz_smoke_contract,
)

LAVA_NPZ_MARGIN_SMOKE_CLAIM_SCOPE = "lava_npz_margin_smoke_not_market_execution"
LAVA_NPZ_MARGIN_SMOKE_CANDIDATE_MODEL = "lava_npz_margin_smoke_v0"
LAVA_NPZ_ZERO_MARGIN_COMPARATOR = "zero_adjacent_margin_violation_reference"
LAVA_NPZ_SOURCE_BASELINE_COMPARISON_CLAIM_SCOPE = (
    "lava_npz_source_baseline_comparison_not_market_execution"
)
STRICT_FALLBACK_FAMILY = "strict_control"
V2_PLUS_FALLBACK_FAMILY = "frozen_v2_plus_fallback"


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


def summarize_lava_npz_source_baseline_comparison(
    candidate_frame: pl.DataFrame,
    *,
    max_instances: int = 8,
) -> dict[str, Any]:
    """Compare selected NPZ smoke candidates with V2+ and strict fallback rows."""

    required_columns = {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "eligible_for_final_selection",
        "candidate_family",
        "candidate_model_name",
        "regret_uah",
        "market_execution_enabled",
    }
    missing = sorted(required_columns.difference(candidate_frame.columns))
    if missing:
        raise ValueError(
            "LAVA NPZ source baseline comparison missing columns: "
            f"{', '.join(missing)}"
        )
    if max_instances < 1:
        raise ValueError("max_instances must be at least 1.")
    if candidate_frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("LAVA NPZ source baseline comparison refuses market execution.")

    train_rows = [
        row
        for row in candidate_frame.sort(
            [
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "candidate_family",
                "candidate_model_name",
            ]
        ).iter_rows(named=True)
        if str(row["split_name"]) == "train_selection"
        and bool(row["eligible_for_final_selection"])
    ]
    rows_by_anchor: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in train_rows:
        rows_by_anchor.setdefault(_source_anchor_key(row), []).append(row)

    selected_rows: list[dict[str, Any]] = []
    strict_rows: list[dict[str, Any]] = []
    v2_plus_rows: list[dict[str, Any]] = []
    missing_strict_count = 0
    missing_v2_plus_count = 0
    for _anchor_key, anchor_rows in sorted(rows_by_anchor.items()):
        if len(selected_rows) >= max_instances:
            break
        if len(anchor_rows) < 2:
            continue
        selected = min(anchor_rows, key=_candidate_sort_key)
        selected_rows.append(selected)
        strict = _best_family_row(anchor_rows, STRICT_FALLBACK_FAMILY)
        v2_plus = _best_family_row(anchor_rows, V2_PLUS_FALLBACK_FAMILY)
        if strict is None:
            missing_strict_count += 1
        else:
            strict_rows.append(strict)
        if v2_plus is None:
            missing_v2_plus_count += 1
        else:
            v2_plus_rows.append(v2_plus)

    candidate_mean_regret = _mean_regret(selected_rows)
    strict_mean_regret = _mean_regret(strict_rows)
    v2_plus_mean_regret = _mean_regret(v2_plus_rows)
    strict_delta = _value_delta(
        baseline_mean_regret=strict_mean_regret,
        candidate_mean_regret=candidate_mean_regret,
        baseline_rows=strict_rows,
        candidate_rows=selected_rows,
    )
    v2_plus_delta = _value_delta(
        baseline_mean_regret=v2_plus_mean_regret,
        candidate_mean_regret=candidate_mean_regret,
        baseline_rows=v2_plus_rows,
        candidate_rows=selected_rows,
    )
    return {
        "claim_scope": LAVA_NPZ_SOURCE_BASELINE_COMPARISON_CLAIM_SCOPE,
        "selected_instance_count": len(selected_rows),
        "strict_fallback_family": STRICT_FALLBACK_FAMILY,
        "v2_plus_family": V2_PLUS_FALLBACK_FAMILY,
        "strict_fallback_anchor_count": len(strict_rows),
        "v2_plus_anchor_count": len(v2_plus_rows),
        "missing_strict_fallback_anchor_count": missing_strict_count,
        "missing_v2_plus_anchor_count": missing_v2_plus_count,
        "baseline_comparison_ready": bool(
            selected_rows
            and len(strict_rows) == len(selected_rows)
            and len(v2_plus_rows) == len(selected_rows)
            and missing_strict_count == 0
            and missing_v2_plus_count == 0
        ),
        "candidate_mean_regret_uah": candidate_mean_regret,
        "strict_fallback_mean_regret_uah": strict_mean_regret,
        "v2_plus_mean_regret_uah": v2_plus_mean_regret,
        "value_delta_vs_strict_fallback_uah": strict_delta,
        "value_delta_vs_v2_plus_uah": v2_plus_delta,
        "mean_regret_improvement_ratio_vs_strict_fallback": _improvement_ratio(
            baseline_mean_regret=strict_mean_regret,
            candidate_mean_regret=candidate_mean_regret,
            baseline_rows=strict_rows,
            candidate_rows=selected_rows,
        ),
        "mean_regret_improvement_ratio_vs_v2_plus": _improvement_ratio(
            baseline_mean_regret=v2_plus_mean_regret,
            candidate_mean_regret=candidate_mean_regret,
            baseline_rows=v2_plus_rows,
            candidate_rows=selected_rows,
        ),
        "promotion_gate": False,
        "permits_model_training": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _source_anchor_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        str(row["anchor_timestamp"]),
    )


def _candidate_sort_key(row: dict[str, Any]) -> tuple[float, str, str]:
    return (
        float(row["regret_uah"]),
        str(row["candidate_family"]),
        str(row["candidate_model_name"]),
    )


def _best_family_row(
    rows: list[dict[str, Any]],
    candidate_family: str,
) -> dict[str, Any] | None:
    family_rows = [
        row for row in rows if str(row["candidate_family"]) == candidate_family
    ]
    if not family_rows:
        return None
    return min(family_rows, key=_candidate_sort_key)


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    return float(sum(float(row["regret_uah"]) for row in rows) / len(rows))


def _value_delta(
    *,
    baseline_mean_regret: float,
    candidate_mean_regret: float,
    baseline_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
) -> float:
    if not baseline_rows or not candidate_rows:
        return 0.0
    return baseline_mean_regret - candidate_mean_regret


def _improvement_ratio(
    *,
    baseline_mean_regret: float,
    candidate_mean_regret: float,
    baseline_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
) -> float:
    if not baseline_rows or not candidate_rows or abs(baseline_mean_regret) < 1e-9:
        return 0.0
    return (baseline_mean_regret - candidate_mean_regret) / abs(baseline_mean_regret)


__all__ = [
    "LAVA_NPZ_MARGIN_SMOKE_CANDIDATE_MODEL",
    "LAVA_NPZ_MARGIN_SMOKE_CLAIM_SCOPE",
    "LAVA_NPZ_SOURCE_BASELINE_COMPARISON_CLAIM_SCOPE",
    "LAVA_NPZ_ZERO_MARGIN_COMPARATOR",
    "run_lava_npz_margin_smoke",
    "summarize_lava_npz_source_baseline_comparison",
]
