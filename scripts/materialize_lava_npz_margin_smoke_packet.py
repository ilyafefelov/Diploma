"""Export a research-only LAVA NPZ margin-smoke packet."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import pickle
import sys
from typing import Any, Sequence

import polars as pl

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from smart_arbitrage.dfl.dt_lava_research_metrics import (
    aggregate_dt_lava_research_metrics_payloads,
)
from smart_arbitrage.dfl.lava_npz_margin_smoke import (
    run_lava_npz_margin_smoke,
    summarize_lava_npz_source_baseline_comparison,
)
from smart_arbitrage.dfl.lava_npz_smoke_contract import (
    write_lava_npz_smoke_artifact_from_candidate_frame,
)
from scripts.validate_lava_npz_margin_smoke_packet import (
    V13_ACQUISITION_SUMMARY_HASH_KEY,
    validate_lava_npz_margin_smoke_packet_manifest,
    summarize_v13_acquisition_summary_payload,
)

PACKET_CLAIM_SCOPE = "lava_npz_margin_smoke_packet_not_market_execution"
DEFAULT_NPZ_NAME = "candidate_lava_smoke.npz"
DEFAULT_SUMMARY_NAME = "candidate_lava_smoke_summary.json"
DEFAULT_METRICS_NAME = "candidate_lava_margin_metrics.json"
DEFAULT_AGGREGATE_NAME = "dt_lava_research_metrics_aggregate.json"
DEFAULT_MANIFEST_NAME = "lava_npz_margin_smoke_manifest.json"
DEFAULT_VALIDATION_NAME = "lava_npz_margin_smoke_packet_validation.json"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create a research-only LAVA NPZ smoke packet from an existing "
            "schedule-neighbor candidate frame."
        ),
    )
    parser.add_argument("--candidate-frame-pickle", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--tenant-id", default="lava_npz_smoke_panel")
    parser.add_argument("--source-model-name", default="lava_schedule_neighbor_npz_smoke_v0")
    parser.add_argument("--window-id", default="lava_npz_smoke_window")
    parser.add_argument("--v13-gate-status", default=None)
    parser.add_argument("--v13-acquisition-summary-json", type=Path, default=None)
    parser.add_argument("--max-instances", type=int, default=8)
    parser.add_argument("--max-neighbors", type=int, default=4)
    args = parser.parse_args(argv)

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    npz_path = output_dir / DEFAULT_NPZ_NAME
    summary_path = output_dir / DEFAULT_SUMMARY_NAME
    metrics_path = output_dir / DEFAULT_METRICS_NAME
    aggregate_path = output_dir / DEFAULT_AGGREGATE_NAME
    manifest_path = output_dir / DEFAULT_MANIFEST_NAME
    validation_path = output_dir / DEFAULT_VALIDATION_NAME

    candidate_frame = _load_polars_frame(args.candidate_frame_pickle)
    summary = write_lava_npz_smoke_artifact_from_candidate_frame(
        candidate_frame,
        npz_path,
        max_instances=args.max_instances,
        max_neighbors=args.max_neighbors,
    )
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    v13_acquisition_summary = _load_optional_v13_acquisition_summary(
        args.v13_acquisition_summary_json
    )
    v13_gate_status = _resolve_v13_gate_status(
        requested_v13_gate_status=args.v13_gate_status,
        v13_acquisition_summary=v13_acquisition_summary,
    )
    _assert_v13_gate_status_matches_summary(summary, v13_gate_status)
    metrics = run_lava_npz_margin_smoke(
        npz_path,
        seed=args.seed,
        tenant_id=args.tenant_id,
        source_model_name=args.source_model_name,
        window_id=args.window_id,
        v13_gate_status=v13_gate_status,
    )
    baseline_comparison = summarize_lava_npz_source_baseline_comparison(
        candidate_frame,
        max_instances=args.max_instances,
    )
    metrics["baseline_comparison"] = baseline_comparison
    metrics_path.write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    aggregate = aggregate_dt_lava_research_metrics_payloads([metrics])
    aggregate_path.write_text(
        json.dumps(aggregate, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _assert_packet_boundary(summary, metrics, v13_gate_status)
    artifact_sha256 = {
        "candidate_frame_pickle": _sha256_file(args.candidate_frame_pickle),
        "npz": _sha256_file(npz_path),
        "summary_json": _sha256_file(summary_path),
        "metrics_json": _sha256_file(metrics_path),
        "aggregate_metrics_json": _sha256_file(aggregate_path),
    }
    if args.v13_acquisition_summary_json is not None:
        artifact_sha256[V13_ACQUISITION_SUMMARY_HASH_KEY] = _sha256_file(
            args.v13_acquisition_summary_json
        )
    manifest = {
        "claim_scope": PACKET_CLAIM_SCOPE,
        "candidate_frame_pickle": str(args.candidate_frame_pickle),
        "npz_path": str(npz_path),
        "summary_json_path": str(summary_path),
        "metrics_json_path": str(metrics_path),
        "aggregate_metrics_json_path": str(aggregate_path),
        "manifest_json_path": str(manifest_path),
        "validation_summary_json_path": str(validation_path),
        "seed": args.seed,
        "window_id": args.window_id,
        "tenant_id": args.tenant_id,
        "source_model_name": args.source_model_name,
        "v13_gate_status": v13_gate_status,
        "npz_instance_count": summary["instance_count"],
        "npz_valid_neighbor_count": summary["valid_neighbor_count"],
        "lava_adjacent_pair_count": metrics["lava_adjacent_pair_count"],
        "lava_margin_violation_mean_uah": metrics["lava_margin_violation_mean_uah"],
        "baseline_comparison": baseline_comparison,
        "aggregate_metric_count": aggregate["metric_count"],
        "aggregate_promotion_gate": aggregate["promotion_gate"],
        "artifact_sha256": artifact_sha256,
        "v13_candidate_generation_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "raw_hourly_action_imitation": False,
        "ci_smoke_only": True,
        "promotion_gate": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }
    if args.v13_acquisition_summary_json is not None:
        manifest["v13_acquisition_summary_json_path"] = str(
            args.v13_acquisition_summary_json
        )
        manifest["v13_acquisition_summary"] = v13_acquisition_summary
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    validation_summary = validate_lava_npz_margin_smoke_packet_manifest(manifest)
    validation_path.write_text(
        json.dumps(validation_summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote LAVA NPZ margin-smoke packet manifest to {manifest_path}")
    return 0


def _load_polars_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


def _load_optional_v13_acquisition_summary(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"V13 acquisition summary JSON must be an object: {path}")
    return summarize_v13_acquisition_summary_payload(payload)


def _resolve_v13_gate_status(
    *,
    requested_v13_gate_status: str | None,
    v13_acquisition_summary: dict[str, Any] | None,
) -> str:
    if v13_acquisition_summary is None:
        return (
            "data_acquisition_needed"
            if requested_v13_gate_status is None
            else requested_v13_gate_status
        )
    summary_gate_status = str(v13_acquisition_summary["gate_status"])
    if (
        requested_v13_gate_status is not None
        and requested_v13_gate_status != summary_gate_status
    ):
        raise ValueError(
            "v13-gate-status conflicts with the attached V13 acquisition summary."
        )
    return summary_gate_status


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assert_packet_boundary(
    summary: dict[str, Any],
    metrics: dict[str, Any],
    v13_gate_status: str,
) -> None:
    _assert_v13_gate_status_matches_summary(summary, v13_gate_status)
    for payload_name, payload in (("summary", summary), ("metrics", metrics)):
        if bool(payload["market_execution_enabled"]):
            raise ValueError(f"LAVA NPZ margin-smoke {payload_name} requires market_execution_enabled=false.")
        if bool(payload["permits_model_training"]):
            raise ValueError(f"LAVA NPZ margin-smoke {payload_name} requires permits_model_training=false.")
        if bool(payload["dt_lava_ready"]):
            raise ValueError(f"LAVA NPZ margin-smoke {payload_name} requires dt_lava_ready=false.")
    if bool(summary["raw_hourly_action_imitation"]):
        raise ValueError(
            "LAVA NPZ margin-smoke summary requires raw_hourly_action_imitation=false."
        )


def _assert_v13_gate_status_matches_summary(
    summary: dict[str, Any],
    v13_gate_status: str,
) -> None:
    if (
        v13_gate_status != "data_acquisition_needed"
        and not bool(summary["v13_candidate_generation_ready"])
    ):
        raise ValueError(
            "LAVA NPZ margin-smoke packet cannot claim a ready V13 gate "
            "while the NPZ contract reports v13_candidate_generation_ready=false."
        )


if __name__ == "__main__":
    raise SystemExit(main())
