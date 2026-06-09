from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys
from typing import Sequence

import polars as pl

from smart_arbitrage.dfl.v13_dt_lava_challenger_export import (
    V13_DT_LAVA_CHALLENGER_JSON_ARTIFACT_NAME,
    V13_DT_LAVA_CHALLENGER_MARKDOWN_ARTIFACT_NAME,
    V13_DT_LAVA_CHALLENGER_METRICS_ARTIFACT_NAME,
    V13_DT_LAVA_CHALLENGER_VALIDATION_ARTIFACT_NAME,
    build_v13_dt_lava_offline_challenger_packet,
    write_v13_dt_lava_offline_challenger_packet,
)

DEFAULT_RUN_SLUG = "week3_v13_dt_lava_offline_challenger_gate"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export a Phase 3 V13-gated DT/LAVA offline challenger packet. "
            "This evaluates only the offline strict LP/oracle gate and never "
            "enables market execution."
        )
    )
    parser.add_argument("--teacher-summary-json", type=Path, required=True)
    parser.add_argument("--bridge-frame-pickle", type=Path, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data") / "research_runs",
    )
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--source-model-names-csv", default="")
    parser.add_argument("--min-tenant-count", type=int, default=5)
    parser.add_argument("--min-validation-tenant-anchor-count", type=int, default=90)
    parser.add_argument(
        "--infer-deterministic-safety-projection-from-zero-violations",
        action="store_true",
        help=(
            "For legacy bridge frames that predate the explicit projection column, "
            "add deterministic_safety_projection_passed=safety_violation_count==0."
        ),
    )
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--asset-check-status", default=None)
    args = parser.parse_args(argv)

    teacher_packet = _load_json_mapping(args.teacher_summary_json)
    bridge_frame = _load_polars_frame(args.bridge_frame_pickle)
    if args.infer_deterministic_safety_projection_from_zero_violations:
        bridge_frame = _overlay_deterministic_safety_projection(bridge_frame)
    packet = build_v13_dt_lava_offline_challenger_packet(
        run_slug=args.run_slug,
        teacher_packet=teacher_packet,
        bridge_strict_frame=bridge_frame,
        source_model_names=_source_model_names(args.source_model_names_csv),
        min_tenant_count=args.min_tenant_count,
        min_validation_tenant_anchor_count=args.min_validation_tenant_anchor_count,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        asset_check_status=args.asset_check_status,
    )
    export_dir = write_v13_dt_lava_offline_challenger_packet(
        packet,
        output_root=args.output_root,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(
                export_dir / V13_DT_LAVA_CHALLENGER_JSON_ARTIFACT_NAME
            ),
            "summary_markdown": str(
                export_dir / V13_DT_LAVA_CHALLENGER_MARKDOWN_ARTIFACT_NAME
            ),
            "metrics_json": str(
                export_dir / V13_DT_LAVA_CHALLENGER_METRICS_ARTIFACT_NAME
            ),
            "validation_json": str(
                export_dir / V13_DT_LAVA_CHALLENGER_VALIDATION_ARTIFACT_NAME
            ),
            "gate_decision": packet["gate"]["decision"],
            "gate_passed": packet["gate"]["passed"],
            "offline_dt_lava_challenger_gate_passed": packet["promotion_gate"][
                "offline_dt_lava_challenger_gate_passed"
            ],
            "market_execution_gate_passed": packet["promotion_gate"][
                "market_execution_gate_passed"
            ],
            "market_execution_enabled": packet["claim_boundary"][
                "market_execution_enabled"
            ],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def _load_json_mapping(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain a JSON object.")
    return value


def _load_polars_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


def _source_model_names(value: str) -> tuple[str, ...] | None:
    names = tuple(item.strip() for item in value.split(",") if item.strip())
    return names or None


def _overlay_deterministic_safety_projection(frame: pl.DataFrame) -> pl.DataFrame:
    if "deterministic_safety_projection_passed" in frame.columns:
        return frame
    if "safety_violation_count" not in frame.columns:
        raise ValueError(
            "Cannot infer deterministic safety projection without "
            "safety_violation_count."
        )
    return frame.with_columns(
        (pl.col("safety_violation_count").cast(pl.Int64) == 0).alias(
            "deterministic_safety_projection_passed"
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
