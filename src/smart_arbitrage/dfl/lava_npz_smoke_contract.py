"""Schema guard for tiny LAVA-style NPZ smoke artifacts.

This is intentionally a research-artifact contract, not a trainer. It lets a
future solver-free LAVA smoke reuse deterministic NPZ inputs while preserving
the current V13 and no-market-execution boundaries.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Final, cast

import numpy as np
import polars as pl

REQUIRED_LAVA_NPZ_SMOKE_ARRAYS: Final[frozenset[str]] = frozenset(
    {
        "claim_scope",
        "feature_matrix",
        "cost_vector_matrix",
        "optimal_vertex_matrix",
        "adjacent_vertex_tensor",
        "adjacent_mask",
        "v13_candidate_generation_ready",
        "dt_lava_ready",
        "permits_model_training",
        "raw_hourly_action_imitation",
        "market_execution_enabled",
    }
)
LAVA_NPZ_SMOKE_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "selector_feature_schedule_distance_from_v2_plus",
    "selector_feature_total_throughput_delta_mwh",
    "selector_feature_terminal_soc_delta_fraction",
    "selector_feature_forecast_spread_uah_mwh",
    "selector_feature_total_degradation_penalty_uah",
    "selector_feature_poland_shadow_candidate",
    "selector_feature_oracle_train_diagnostic",
)
_REQUIRED_LAVA_NPZ_SOURCE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "anchor_timestamp",
        "split_name",
        "eligible_for_final_selection",
        "candidate_family",
        "candidate_model_name",
        "actual_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "regret_uah",
        "market_execution_enabled",
        *LAVA_NPZ_SMOKE_FEATURE_COLUMNS,
    }
)


def write_lava_npz_smoke_artifact_from_candidate_frame(
    candidate_frame: pl.DataFrame,
    output_path: str | Path,
    *,
    max_instances: int = 8,
    max_neighbors: int = 4,
) -> dict[str, Any]:
    """Write a deterministic NPZ smoke artifact from schedule-neighbor candidates."""

    path = Path(output_path)
    arrays = build_lava_npz_smoke_artifact_arrays_from_candidate_frame(
        candidate_frame,
        max_instances=max_instances,
        max_neighbors=max_neighbors,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(path, **cast(dict[str, Any], arrays))
    return validate_lava_npz_smoke_contract(path)


def build_lava_npz_smoke_artifact_arrays_from_candidate_frame(
    candidate_frame: pl.DataFrame,
    *,
    max_instances: int = 8,
    max_neighbors: int = 4,
) -> dict[str, np.ndarray]:
    """Build NPZ arrays from existing train-only LAVA schedule-neighbor evidence."""

    _require_source_columns(candidate_frame)
    if max_instances < 1:
        raise ValueError("max_instances must be at least 1.")
    if max_neighbors < 1:
        raise ValueError("max_neighbors must be at least 1.")
    if candidate_frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("LAVA NPZ smoke source frame refuses market execution claims.")
    if "permits_model_training" in candidate_frame.columns and candidate_frame.select(
        pl.col("permits_model_training").any()
    ).item():
        raise ValueError("LAVA NPZ smoke source frame refuses model-training promotion.")

    source_rows = [
        row
        for row in candidate_frame.sort(
            ["tenant_id", "anchor_timestamp", "candidate_family", "candidate_model_name"]
        ).iter_rows(named=True)
        if str(row["split_name"]) == "train_selection"
        and bool(row["eligible_for_final_selection"])
    ]
    rows_by_anchor: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in source_rows:
        rows_by_anchor.setdefault(_anchor_key(row), []).append(row)

    feature_rows: list[list[float]] = []
    cost_rows: list[list[float]] = []
    optimal_rows: list[list[float]] = []
    adjacent_rows: list[list[list[float]]] = []
    adjacent_mask_rows: list[list[bool]] = []
    tenant_ids: list[str] = []
    anchor_timestamps: list[str] = []
    selected_candidate_names: list[str] = []
    decision_dimension: int | None = None

    for anchor_key, anchor_rows in sorted(rows_by_anchor.items()):
        if len(anchor_rows) < 2:
            continue
        best = min(anchor_rows, key=_candidate_sort_key)
        neighbors = [
            row
            for row in sorted(anchor_rows, key=lambda row: _neighbor_sort_key(row, best))
            if row is not best
        ][:max_neighbors]
        if not neighbors:
            continue

        feature_vector = [float(best[column]) for column in LAVA_NPZ_SMOKE_FEATURE_COLUMNS]
        cost_vector = _float_vector(best["actual_price_uah_mwh_vector"])
        optimal_vertex = _float_vector(best["dispatch_mw_vector"])
        if len(cost_vector) != len(optimal_vertex):
            raise ValueError("actual_price_uah_mwh_vector and dispatch_mw_vector widths must match.")
        if decision_dimension is None:
            decision_dimension = len(optimal_vertex)
        if len(optimal_vertex) != decision_dimension:
            raise ValueError("All dispatch_mw_vector widths must match.")

        padded_neighbors: list[list[float]] = []
        padded_mask: list[bool] = []
        for neighbor in neighbors:
            neighbor_vertex = _float_vector(neighbor["dispatch_mw_vector"])
            if len(neighbor_vertex) != decision_dimension:
                raise ValueError("All adjacent dispatch_mw_vector widths must match.")
            padded_neighbors.append(neighbor_vertex)
            padded_mask.append(True)
        while len(padded_neighbors) < max_neighbors:
            padded_neighbors.append([0.0] * decision_dimension)
            padded_mask.append(False)

        feature_rows.append(feature_vector)
        cost_rows.append(cost_vector)
        optimal_rows.append(optimal_vertex)
        adjacent_rows.append(padded_neighbors)
        adjacent_mask_rows.append(padded_mask)
        tenant_ids.append(anchor_key[0])
        anchor_timestamps.append(anchor_key[1])
        selected_candidate_names.append(str(best["candidate_model_name"]))
        if len(feature_rows) >= max_instances:
            break

    if decision_dimension is None or not feature_rows:
        raise ValueError(
            "LAVA NPZ smoke artifact needs at least one eligible train anchor with an adjacent candidate."
        )

    return {
        "claim_scope": np.array("lava_npz_smoke_contract_not_market_execution"),
        "feature_matrix": np.array(feature_rows, dtype=float),
        "cost_vector_matrix": np.array(cost_rows, dtype=float),
        "optimal_vertex_matrix": np.array(optimal_rows, dtype=float),
        "adjacent_vertex_tensor": np.array(adjacent_rows, dtype=float),
        "adjacent_mask": np.array(adjacent_mask_rows, dtype=bool),
        "tenant_id_vector": np.array(tenant_ids),
        "anchor_timestamp_vector": np.array(anchor_timestamps),
        "selected_candidate_model_name_vector": np.array(selected_candidate_names),
        "v13_candidate_generation_ready": np.array(False),
        "dt_lava_ready": np.array(False),
        "permits_model_training": np.array(False),
        "raw_hourly_action_imitation": np.array(False),
        "market_execution_enabled": np.array(False),
    }


def validate_lava_npz_smoke_contract(npz_path: str | Path) -> dict[str, Any]:
    """Validate a tiny LAVA-style NPZ and return a boundary-preserving summary."""

    path = Path(npz_path)
    with np.load(path, allow_pickle=False) as artifact:
        missing = sorted(REQUIRED_LAVA_NPZ_SMOKE_ARRAYS.difference(artifact.files))
        if missing:
            raise ValueError(f"Missing LAVA NPZ smoke arrays: {', '.join(missing)}")

        claim_scope = _string_scalar(artifact["claim_scope"], "claim_scope")
        if "not_market_execution" not in claim_scope:
            raise ValueError("claim_scope must include not_market_execution.")

        feature_matrix = _float_array_2d(
            artifact["feature_matrix"],
            "feature_matrix",
        )
        cost_vector_matrix = _float_array_2d(
            artifact["cost_vector_matrix"],
            "cost_vector_matrix",
        )
        optimal_vertex_matrix = _float_array_2d(
            artifact["optimal_vertex_matrix"],
            "optimal_vertex_matrix",
        )
        adjacent_vertex_tensor = _float_array_3d(
            artifact["adjacent_vertex_tensor"],
            "adjacent_vertex_tensor",
        )
        adjacent_mask = _bool_array_2d(artifact["adjacent_mask"], "adjacent_mask")

        v13_candidate_generation_ready = _bool_scalar(
            artifact["v13_candidate_generation_ready"],
            "v13_candidate_generation_ready",
        )
        dt_lava_ready = _bool_scalar(artifact["dt_lava_ready"], "dt_lava_ready")
        permits_model_training = _bool_scalar(
            artifact["permits_model_training"],
            "permits_model_training",
        )
        raw_hourly_action_imitation = _bool_scalar(
            artifact["raw_hourly_action_imitation"],
            "raw_hourly_action_imitation",
        )
        market_execution_enabled = _bool_scalar(
            artifact["market_execution_enabled"],
            "market_execution_enabled",
        )

    instance_count, feature_count = feature_matrix.shape
    cost_instance_count, decision_dimension = cost_vector_matrix.shape
    if cost_instance_count != instance_count:
        raise ValueError("cost_vector_matrix row count must match feature_matrix.")
    if optimal_vertex_matrix.shape != cost_vector_matrix.shape:
        raise ValueError("optimal_vertex_matrix shape must match cost_vector_matrix.")
    expected_adjacent_shape = (
        instance_count,
        adjacent_vertex_tensor.shape[1],
        decision_dimension,
    )
    if adjacent_vertex_tensor.shape != expected_adjacent_shape:
        raise ValueError(
            "adjacent_vertex_tensor shape must be "
            "(instance_count, max_neighbor_count, decision_dimension)."
        )
    if adjacent_mask.shape != adjacent_vertex_tensor.shape[:2]:
        raise ValueError("adjacent_mask shape must match adjacent_vertex_tensor first axes.")

    valid_neighbor_count = int(np.count_nonzero(adjacent_mask))
    if valid_neighbor_count < 1:
        raise ValueError("LAVA NPZ smoke artifact needs at least one valid adjacent vertex.")
    if raw_hourly_action_imitation:
        raise ValueError("LAVA NPZ smoke artifacts require raw_hourly_action_imitation=false.")
    if market_execution_enabled:
        raise ValueError("LAVA NPZ smoke artifacts require market_execution_enabled=false.")
    if permits_model_training and not (v13_candidate_generation_ready and dt_lava_ready):
        raise ValueError(
            "permits_model_training=true requires V13 candidate generation and DT/LAVA readiness."
        )

    return {
        "claim_scope": claim_scope,
        "instance_count": int(instance_count),
        "feature_count": int(feature_count),
        "decision_dimension": int(decision_dimension),
        "max_neighbor_count": int(adjacent_vertex_tensor.shape[1]),
        "valid_neighbor_count": valid_neighbor_count,
        "v13_candidate_generation_ready": v13_candidate_generation_ready,
        "dt_lava_ready": dt_lava_ready,
        "permits_model_training": permits_model_training,
        "raw_hourly_action_imitation": raw_hourly_action_imitation,
        "market_execution_enabled": False,
    }


def _float_array_2d(value: np.ndarray, name: str) -> np.ndarray:
    array = np.asarray(value)
    if array.ndim != 2:
        raise ValueError(f"{name} must be a 2D array.")
    try:
        numeric = array.astype(float, copy=False)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric.") from exc
    if not np.isfinite(numeric).all():
        raise ValueError(f"{name} must contain only finite values.")
    return numeric


def _float_array_3d(value: np.ndarray, name: str) -> np.ndarray:
    array = np.asarray(value)
    if array.ndim != 3:
        raise ValueError(f"{name} must be a 3D array.")
    try:
        numeric = array.astype(float, copy=False)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric.") from exc
    if not np.isfinite(numeric).all():
        raise ValueError(f"{name} must contain only finite values.")
    return numeric


def _bool_array_2d(value: np.ndarray, name: str) -> np.ndarray:
    array = np.asarray(value)
    if array.ndim != 2:
        raise ValueError(f"{name} must be a 2D boolean array.")
    if not np.issubdtype(array.dtype, np.bool_):
        raise ValueError(f"{name} must be a boolean array.")
    return array


def _bool_scalar(value: np.ndarray, name: str) -> bool:
    array = np.asarray(value)
    if array.shape != ():
        raise ValueError(f"{name} must be a scalar boolean.")
    scalar = array.item()
    if not isinstance(scalar, bool):
        raise ValueError(f"{name} must be a scalar boolean.")
    return scalar


def _string_scalar(value: np.ndarray, name: str) -> str:
    array = np.asarray(value)
    if array.shape != ():
        raise ValueError(f"{name} must be a scalar string.")
    scalar = array.item()
    if isinstance(scalar, bytes):
        scalar = scalar.decode("utf-8")
    if not isinstance(scalar, str) or not scalar.strip():
        raise ValueError(f"{name} must be a non-empty scalar string.")
    return scalar.strip()


def _require_source_columns(frame: pl.DataFrame) -> None:
    missing = sorted(_REQUIRED_LAVA_NPZ_SOURCE_COLUMNS.difference(frame.columns))
    if missing:
        raise ValueError(f"LAVA NPZ smoke source frame is missing columns: {missing}")


def _anchor_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row["tenant_id"]), str(row["anchor_timestamp"]))


def _candidate_sort_key(row: dict[str, Any]) -> tuple[float, str, str]:
    return (
        float(row["regret_uah"]),
        str(row["candidate_family"]),
        str(row["candidate_model_name"]),
    )


def _neighbor_sort_key(
    row: dict[str, Any],
    best: dict[str, Any],
) -> tuple[float, float, str, str]:
    return (
        abs(float(row["regret_uah"]) - float(best["regret_uah"])),
        float(row["regret_uah"]),
        str(row["candidate_family"]),
        str(row["candidate_model_name"]),
    )


def _float_vector(value: Any) -> list[float]:
    if isinstance(value, str):
        return [float(item.strip()) for item in value.split(",") if item.strip()]
    return [float(item) for item in value]


__all__ = [
    "LAVA_NPZ_SMOKE_FEATURE_COLUMNS",
    "REQUIRED_LAVA_NPZ_SMOKE_ARRAYS",
    "build_lava_npz_smoke_artifact_arrays_from_candidate_frame",
    "validate_lava_npz_smoke_contract",
    "write_lava_npz_smoke_artifact_from_candidate_frame",
]
