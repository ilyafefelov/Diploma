"""Validate a research-only LAVA NPZ margin-smoke packet manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from smart_arbitrage.dfl.dt_lava_research_metrics import (
    aggregate_dt_lava_research_metrics_payloads,
    validate_dt_lava_research_metrics_payload,
)
from smart_arbitrage.dfl.lava_npz_smoke_contract import (
    validate_lava_npz_smoke_contract,
)

VALIDATION_CLAIM_SCOPE = "lava_npz_margin_smoke_packet_validation_not_market_execution"
PACKET_CLAIM_SCOPE = "lava_npz_margin_smoke_packet_not_market_execution"
BASELINE_COMPARISON_CLAIM_SCOPE = "lava_npz_source_baseline_comparison_not_market_execution"
STRICT_FALLBACK_FAMILY = "strict_control"
V2_PLUS_FALLBACK_FAMILY = "frozen_v2_plus_fallback"
REQUIRED_PATH_FIELDS = (
    "candidate_frame_pickle",
    "npz_path",
    "summary_json_path",
    "metrics_json_path",
    "aggregate_metrics_json_path",
    "manifest_json_path",
)
REQUIRED_HASH_KEYS = {
    "candidate_frame_pickle": "candidate_frame_pickle",
    "npz": "npz_path",
    "summary_json": "summary_json_path",
    "metrics_json": "metrics_json_path",
    "aggregate_metrics_json": "aggregate_metrics_json_path",
}
V13_ACQUISITION_SUMMARY_PATH_FIELD = "v13_acquisition_summary_json_path"
V13_ACQUISITION_SUMMARY_HASH_KEY = "v13_acquisition_summary_json"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate a SHA256-bearing, research-only LAVA NPZ margin-smoke "
            "packet manifest."
        ),
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    manifest = _read_json_object(args.manifest)
    summary = validate_lava_npz_margin_smoke_packet_manifest(manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote LAVA NPZ margin-smoke packet validation summary to {args.output}")
    return 0


def validate_lava_npz_margin_smoke_packet_manifest(
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate a LAVA NPZ margin-smoke packet without promotion semantics."""

    _require_packet_boundary(manifest)
    paths = _packet_paths(manifest)
    _validate_artifact_hashes(manifest, paths)

    npz_summary = validate_lava_npz_smoke_contract(paths["npz_path"])
    _validate_manifest_v13_readiness_claim(manifest, npz_summary)
    stored_summary = _read_json_object(paths["summary_json_path"])
    if stored_summary != npz_summary:
        raise ValueError("summary_json does not match the validated NPZ contract.")

    metrics = _read_json_object(paths["metrics_json_path"])
    normalized_metrics = validate_dt_lava_research_metrics_payload(metrics)
    if normalized_metrics["market_execution_enabled"]:
        raise ValueError("metrics_json requires market_execution_enabled=false.")
    baseline_comparison = _validate_baseline_comparison(
        manifest=manifest,
        metrics=metrics,
    )

    stored_aggregate = _read_json_object(paths["aggregate_metrics_json_path"])
    recomputed_aggregate = aggregate_dt_lava_research_metrics_payloads([metrics])
    if stored_aggregate != recomputed_aggregate:
        raise ValueError("aggregate_metrics_json does not match recomputed metrics.")
    if stored_aggregate["market_execution_enabled"]:
        raise ValueError("aggregate_metrics_json requires market_execution_enabled=false.")
    _validate_manifest_summary_counts(
        manifest=manifest,
        npz_summary=npz_summary,
        metrics=metrics,
        aggregate=stored_aggregate,
    )

    v13_summary = _validate_optional_v13_acquisition_summary(manifest, paths, npz_summary)

    return {
        "claim_scope": VALIDATION_CLAIM_SCOPE,
        "packet_claim_scope": str(manifest["claim_scope"]),
        "artifact_hashes_valid": True,
        "npz_contract_valid": True,
        "metrics_valid": True,
        "aggregate_valid": True,
        "baseline_comparison_valid": True,
        "baseline_comparison_ready": True,
        "baseline_selected_instance_count": baseline_comparison[
            "selected_instance_count"
        ],
        "strict_fallback_anchor_count": baseline_comparison[
            "strict_fallback_anchor_count"
        ],
        "v2_plus_anchor_count": baseline_comparison["v2_plus_anchor_count"],
        "missing_strict_fallback_anchor_count": baseline_comparison[
            "missing_strict_fallback_anchor_count"
        ],
        "missing_v2_plus_anchor_count": baseline_comparison[
            "missing_v2_plus_anchor_count"
        ],
        "v13_acquisition_summary_attached": v13_summary is not None,
        "v13_acquisition_summary_json_path": (
            None
            if v13_summary is None
            else str(paths[V13_ACQUISITION_SUMMARY_PATH_FIELD])
        ),
        "v13_gate_status": (
            "data_acquisition_needed"
            if v13_summary is None
            else str(v13_summary["gate_status"])
        ),
        "v13_blocked_rows": (
            None if v13_summary is None else int(v13_summary["blocked_rows"])
        ),
        "v13_ready_rows": (
            None if v13_summary is None else int(v13_summary["ready_rows"])
        ),
        "v13_readiness_rows": (
            None if v13_summary is None else int(v13_summary["readiness_rows"])
        ),
        "v13_max_prior_material_safe_switch_examples": (
            None
            if v13_summary is None
            else int(v13_summary["max_prior_material_safe_switch_examples"])
        ),
        "v13_min_safe_examples_required": (
            None
            if v13_summary is None
            else int(v13_summary["min_safe_examples_required"])
        ),
        "npz_instance_count": int(npz_summary["instance_count"]),
        "npz_valid_neighbor_count": int(npz_summary["valid_neighbor_count"]),
        "aggregate_metric_count": int(stored_aggregate["metric_count"]),
        "v13_candidate_generation_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "promotion_gate": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _require_packet_boundary(manifest: Mapping[str, Any]) -> None:
    if manifest.get("claim_scope") != PACKET_CLAIM_SCOPE:
        raise ValueError("LAVA NPZ margin-smoke packet claim_scope is invalid.")
    for key in (
        "market_execution_enabled",
        "promotion_gate",
        "permits_model_training",
        "dt_lava_ready",
        "raw_hourly_action_imitation",
    ):
        if bool(manifest.get(key)):
            raise ValueError(f"LAVA NPZ margin-smoke packet requires {key}=false.")
    for key in ("ci_smoke_only", "not_full_dfl", "not_market_execution"):
        if not bool(manifest.get(key)):
            raise ValueError(f"LAVA NPZ margin-smoke packet requires {key}=true.")


def _packet_paths(manifest: Mapping[str, Any]) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for field in REQUIRED_PATH_FIELDS:
        value = manifest.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"LAVA NPZ margin-smoke packet missing path: {field}.")
        path = Path(value)
        if not path.exists():
            raise ValueError(f"LAVA NPZ margin-smoke packet path does not exist: {field}.")
        paths[field] = path
    v13_path_value = manifest.get(V13_ACQUISITION_SUMMARY_PATH_FIELD)
    if v13_path_value is not None:
        if not isinstance(v13_path_value, str) or not v13_path_value.strip():
            raise ValueError(
                "LAVA NPZ margin-smoke packet has an invalid "
                f"{V13_ACQUISITION_SUMMARY_PATH_FIELD}."
            )
        v13_path = Path(v13_path_value)
        if not v13_path.exists():
            raise ValueError(
                "LAVA NPZ margin-smoke packet path does not exist: "
                f"{V13_ACQUISITION_SUMMARY_PATH_FIELD}."
            )
        paths[V13_ACQUISITION_SUMMARY_PATH_FIELD] = v13_path
    return paths


def _validate_artifact_hashes(
    manifest: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> None:
    artifact_sha256 = manifest.get("artifact_sha256")
    if not isinstance(artifact_sha256, Mapping):
        raise ValueError("LAVA NPZ margin-smoke packet requires artifact_sha256.")
    for hash_key, path_key in REQUIRED_HASH_KEYS.items():
        expected_hash = artifact_sha256.get(hash_key)
        if not isinstance(expected_hash, str) or not expected_hash.strip():
            raise ValueError(f"LAVA NPZ margin-smoke packet missing {hash_key} SHA256.")
        actual_hash = _sha256_file(paths[path_key])
        if actual_hash != expected_hash:
            raise ValueError(f"{hash_key} SHA256 mismatch.")
    if V13_ACQUISITION_SUMMARY_PATH_FIELD in paths:
        expected_hash = artifact_sha256.get(V13_ACQUISITION_SUMMARY_HASH_KEY)
        if not isinstance(expected_hash, str) or not expected_hash.strip():
            raise ValueError(
                "LAVA NPZ margin-smoke packet missing "
                f"{V13_ACQUISITION_SUMMARY_HASH_KEY} SHA256."
            )
        actual_hash = _sha256_file(paths[V13_ACQUISITION_SUMMARY_PATH_FIELD])
        if actual_hash != expected_hash:
            raise ValueError(f"{V13_ACQUISITION_SUMMARY_HASH_KEY} SHA256 mismatch.")


def _validate_optional_v13_acquisition_summary(
    manifest: Mapping[str, Any],
    paths: Mapping[str, Path],
    npz_summary: Mapping[str, Any],
) -> dict[str, Any] | None:
    if V13_ACQUISITION_SUMMARY_PATH_FIELD not in paths:
        return None

    v13_summary = summarize_v13_acquisition_summary_payload(
        _read_json_object(paths[V13_ACQUISITION_SUMMARY_PATH_FIELD])
    )
    stored_summary = manifest.get("v13_acquisition_summary")
    if stored_summary != v13_summary:
        raise ValueError("v13_acquisition_summary does not match the attached JSON.")
    if bool(manifest["v13_candidate_generation_ready"]) != bool(
        v13_summary["v13_candidate_generation_ready"]
    ):
        raise ValueError(
            "v13_candidate_generation_ready does not match the attached V13 summary."
        )
    if str(manifest.get("v13_gate_status")) != str(v13_summary["gate_status"]):
        raise ValueError("v13_gate_status does not match the attached V13 summary.")
    if bool(v13_summary["v13_candidate_generation_ready"]) and not bool(
        npz_summary["v13_candidate_generation_ready"]
    ):
        raise ValueError(
            "V13 acquisition summary is ready but the NPZ contract reports "
            "v13_candidate_generation_ready=false."
        )
    return v13_summary


def _validate_manifest_v13_readiness_claim(
    manifest: Mapping[str, Any],
    npz_summary: Mapping[str, Any],
) -> None:
    value = manifest.get("v13_candidate_generation_ready")
    if not isinstance(value, bool):
        raise ValueError(
            "LAVA NPZ margin-smoke packet requires boolean "
            "v13_candidate_generation_ready."
        )
    if value != bool(npz_summary["v13_candidate_generation_ready"]):
        raise ValueError(
            "v13_candidate_generation_ready does not match the NPZ contract."
        )


def _validate_manifest_summary_counts(
    *,
    manifest: Mapping[str, Any],
    npz_summary: Mapping[str, Any],
    metrics: Mapping[str, Any],
    aggregate: Mapping[str, Any],
) -> None:
    expected_counts = {
        "npz_instance_count": int(npz_summary["instance_count"]),
        "npz_valid_neighbor_count": int(npz_summary["valid_neighbor_count"]),
        "lava_adjacent_pair_count": int(metrics["lava_adjacent_pair_count"]),
        "aggregate_metric_count": int(aggregate["metric_count"]),
    }
    for key, expected_value in expected_counts.items():
        actual_value = manifest.get(key)
        if isinstance(actual_value, bool) or not isinstance(actual_value, int | float):
            raise ValueError(f"LAVA NPZ margin-smoke packet requires numeric {key}.")
        if int(actual_value) != expected_value:
            raise ValueError(f"{key} does not match validated packet artifacts.")
    aggregate_promotion_gate = manifest.get("aggregate_promotion_gate")
    if not isinstance(aggregate_promotion_gate, bool):
        raise ValueError(
            "LAVA NPZ margin-smoke packet requires boolean aggregate_promotion_gate."
        )
    if aggregate_promotion_gate != bool(aggregate["promotion_gate"]):
        raise ValueError(
            "aggregate_promotion_gate does not match validated aggregate metrics."
        )


def _validate_baseline_comparison(
    *,
    manifest: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> dict[str, int]:
    manifest_comparison = manifest.get("baseline_comparison")
    metrics_comparison = metrics.get("baseline_comparison")
    if not isinstance(manifest_comparison, Mapping):
        raise ValueError("LAVA NPZ margin-smoke packet requires baseline_comparison.")
    if not isinstance(metrics_comparison, Mapping):
        raise ValueError("metrics_json requires baseline_comparison.")
    if manifest_comparison != metrics_comparison:
        raise ValueError(
            "manifest baseline_comparison does not match metrics baseline_comparison."
        )
    if str(manifest_comparison.get("claim_scope", "")) != BASELINE_COMPARISON_CLAIM_SCOPE:
        raise ValueError("baseline_comparison claim_scope is invalid.")
    if bool(manifest_comparison.get("market_execution_enabled", False)):
        raise ValueError("baseline_comparison requires market_execution_enabled=false.")
    if bool(manifest_comparison.get("permits_model_training", False)):
        raise ValueError("baseline_comparison requires permits_model_training=false.")
    if bool(manifest_comparison.get("promotion_gate", False)):
        raise ValueError("baseline_comparison requires promotion_gate=false.")
    if str(manifest_comparison.get("strict_fallback_family", "")) != STRICT_FALLBACK_FAMILY:
        raise ValueError("baseline_comparison requires strict_control fallback family.")
    if str(manifest_comparison.get("v2_plus_family", "")) != V2_PLUS_FALLBACK_FAMILY:
        raise ValueError(
            "baseline_comparison requires frozen_v2_plus_fallback family."
        )

    ready = manifest_comparison.get("baseline_comparison_ready")
    if not isinstance(ready, bool):
        raise ValueError("baseline_comparison requires boolean baseline_comparison_ready.")
    selected_count = _baseline_int_field(
        manifest_comparison,
        "selected_instance_count",
    )
    strict_count = _baseline_int_field(
        manifest_comparison,
        "strict_fallback_anchor_count",
    )
    v2_plus_count = _baseline_int_field(
        manifest_comparison,
        "v2_plus_anchor_count",
    )
    missing_strict_count = _baseline_int_field(
        manifest_comparison,
        "missing_strict_fallback_anchor_count",
    )
    missing_v2_plus_count = _baseline_int_field(
        manifest_comparison,
        "missing_v2_plus_anchor_count",
    )
    if (
        not ready
        or selected_count < 1
        or strict_count != selected_count
        or v2_plus_count != selected_count
        or missing_strict_count != 0
        or missing_v2_plus_count != 0
    ):
        raise ValueError(
            "baseline_comparison requires ready strict-control and V2+ fallback coverage."
        )
    return {
        "selected_instance_count": selected_count,
        "strict_fallback_anchor_count": strict_count,
        "v2_plus_anchor_count": v2_plus_count,
        "missing_strict_fallback_anchor_count": missing_strict_count,
        "missing_v2_plus_anchor_count": missing_v2_plus_count,
    }


def _baseline_int_field(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"baseline_comparison requires numeric {key}.")
    return int(value)


def summarize_v13_acquisition_summary_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Extract packet-safe V13 readiness metadata from an acquisition summary."""

    _require_v13_no_execution_boundary(payload)
    readiness = _required_mapping(payload, "readiness_summary")
    v13_ready = _bool_field(readiness, "v13_candidate_generation_ready")
    gate_status = _v13_gate_status(readiness, v13_ready=v13_ready)
    return {
        "blocked_rows": _int_field(readiness, "blocked_rows"),
        "gate_status": gate_status,
        "market_execution_enabled": False,
        "max_prior_material_safe_switch_examples": _int_field(
            readiness, "max_prior_material_safe_switch_examples"
        ),
        "min_safe_examples_required": _int_field(
            readiness, "min_safe_examples_required"
        ),
        "readiness_rows": _int_field(readiness, "readiness_rows"),
        "ready_rows": _int_field(readiness, "ready_rows"),
        "v13_candidate_generation_ready": v13_ready,
    }


def _require_v13_no_execution_boundary(payload: Mapping[str, Any]) -> None:
    for section_name in ("claim_boundary", "acquisition_input_preflight_summary"):
        section = payload.get(section_name)
        if isinstance(section, Mapping):
            if bool(section.get("market_execution_enabled", False)):
                raise ValueError(
                    "V13 acquisition summary requires market_execution_enabled=false."
                )
            if bool(section.get("permits_model_training", False)):
                raise ValueError(
                    "V13 acquisition summary requires permits_model_training=false."
                )
    claim_boundary = payload.get("claim_boundary")
    if isinstance(claim_boundary, Mapping) and claim_boundary.get("not_market_execution") is False:
        raise ValueError("V13 acquisition summary requires not_market_execution=true.")


def _v13_gate_status(readiness: Mapping[str, Any], *, v13_ready: bool) -> str:
    if v13_ready:
        return "v13_candidate_generation_ready"
    decisions = readiness.get("readiness_decisions")
    if isinstance(decisions, list):
        string_decisions = [
            str(decision)
            for decision in decisions
            if isinstance(decision, str) and decision.strip()
        ]
        if "data_acquisition_needed" in string_decisions:
            return "data_acquisition_needed"
        if string_decisions:
            return string_decisions[0]
    return "data_acquisition_needed"


def _required_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise ValueError(f"V13 acquisition summary requires {key}.")
    return value


def _int_field(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"V13 acquisition summary requires numeric {key}.")
    return int(value)


def _bool_field(payload: Mapping[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"V13 acquisition summary requires boolean {key}.")
    return value


def _read_json_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON payload must be an object: {path}")
    return value


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
