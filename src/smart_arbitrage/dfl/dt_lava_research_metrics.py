"""DT/LAVA research metrics contract guard.

The validator keeps future DT/LAVA and LAVA-style smoke outputs in the same
read-model boundary as V13: research metrics are useful evidence, but they must
not look like market execution or model-training promotion unless the V13 gate
actually permits it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

REQUIRED_DT_LAVA_RESEARCH_METRIC_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "claim_scope",
        "tenant_id",
        "source_model_name",
        "window_id",
        "seed",
        "comparator_model_name",
        "candidate_model_name",
        "mean_regret_uah",
        "baseline_mean_regret_uah",
        "v13_gate_status",
        "v13_candidate_generation_ready",
        "dt_lava_ready",
        "permits_model_training",
        "market_execution_enabled",
    }
)


def validate_dt_lava_research_metrics_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and normalize one DT/LAVA research metrics payload."""

    missing = sorted(REQUIRED_DT_LAVA_RESEARCH_METRIC_FIELDS - set(payload))
    if missing:
        raise ValueError(f"Missing DT/LAVA research metric fields: {', '.join(missing)}")

    claim_scope = _required_str(payload, "claim_scope")
    if "not_market_execution" not in claim_scope:
        raise ValueError("claim_scope must include not_market_execution.")

    market_execution_enabled = _required_bool(payload, "market_execution_enabled")
    if market_execution_enabled:
        raise ValueError("DT/LAVA research metrics require market_execution_enabled=false.")

    v13_candidate_generation_ready = _required_bool(
        payload,
        "v13_candidate_generation_ready",
    )
    dt_lava_ready = _required_bool(payload, "dt_lava_ready")
    permits_model_training = _required_bool(payload, "permits_model_training")
    if permits_model_training and not (
        v13_candidate_generation_ready and dt_lava_ready
    ):
        raise ValueError(
            "permits_model_training=true requires V13 candidate generation and DT/LAVA readiness."
        )

    mean_regret_uah = _required_float(payload, "mean_regret_uah")
    baseline_mean_regret_uah = _required_float(payload, "baseline_mean_regret_uah")

    return {
        "claim_scope": claim_scope,
        "tenant_id": _required_str(payload, "tenant_id"),
        "source_model_name": _required_str(payload, "source_model_name"),
        "window_id": _required_str(payload, "window_id"),
        "seed": _required_int(payload, "seed"),
        "comparator_model_name": _required_str(payload, "comparator_model_name"),
        "candidate_model_name": _required_str(payload, "candidate_model_name"),
        "mean_regret_uah": mean_regret_uah,
        "baseline_mean_regret_uah": baseline_mean_regret_uah,
        "value_delta_vs_baseline_uah": baseline_mean_regret_uah - mean_regret_uah,
        "v13_gate_status": _required_str(payload, "v13_gate_status"),
        "v13_candidate_generation_ready": v13_candidate_generation_ready,
        "dt_lava_ready": dt_lava_ready,
        "permits_model_training": permits_model_training,
        "market_execution_enabled": False,
    }


def _required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string.")
    return value.strip()


def _required_bool(payload: Mapping[str, Any], key: str) -> bool:
    value = payload[key]
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean.")
    return value


def _required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer.")
    return value


def _required_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be numeric.")
    return float(value)


__all__ = [
    "REQUIRED_DT_LAVA_RESEARCH_METRIC_FIELDS",
    "validate_dt_lava_research_metrics_payload",
]
