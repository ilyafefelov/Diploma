from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.schedule_value_promotion_gate import (
    build_dfl_schedule_value_production_gate_registry,
    write_dfl_schedule_value_production_gate_registry,
)

DEFAULT_RUN_SLUG = "week3_dfl_schedule_value_production_gate"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a concise Schedule/Value Learner V2 production-gate registry."
    )
    parser.add_argument("--gate-frame-pickle", type=Path, required=True)
    parser.add_argument(
        "--output-root", type=Path, default=Path("data") / "research_runs"
    )
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--attempt-manifest", type=Path, default=None)
    parser.add_argument("--monitor-snapshot", type=Path, default=None)
    parser.add_argument("--learner-frame-pickle", type=Path, default=None)
    args = parser.parse_args()

    gate_frame = _load_polars_frame(args.gate_frame_pickle)
    learner_frame = (
        _load_polars_frame(args.learner_frame_pickle)
        if args.learner_frame_pickle is not None
        else None
    )
    registry = build_dfl_schedule_value_production_gate_registry(
        run_slug=args.run_slug,
        gate_frame=gate_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
    )
    export_dir = write_dfl_schedule_value_production_gate_registry(
        registry,
        output_root=args.output_root,
        run_slug=args.run_slug,
        attempt_manifest_path=args.attempt_manifest,
        monitor_snapshot_path=args.monitor_snapshot,
        learner_trace_frame=learner_frame,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "registry_json": str(
                export_dir / "dfl_schedule_value_production_gate_registry.json"
            ),
            "registry_markdown": str(
                export_dir / "dfl_schedule_value_production_gate_registry.md"
            ),
            "attempt_manifest": str(export_dir / "attempt_manifest.json")
            if args.attempt_manifest
            else None,
            "monitor_snapshot": str(export_dir / "resume-summary.json")
            if args.monitor_snapshot
            else None,
            "learner_trace_summary": str(
                export_dir / "dfl_schedule_value_learner_v2_trace_summary.json"
            )
            if args.learner_frame_pickle
            else None,
            "learner_trace_markdown": str(
                export_dir / "dfl_schedule_value_learner_v2_trace_summary.md"
            )
            if args.learner_frame_pickle
            else None,
            "production_promote_count": registry["summary"]["production_promote_count"],
            "promoted_source_model_names": registry["summary"][
                "promoted_source_model_names"
            ],
            "market_execution_enabled": registry["summary"]["market_execution_enabled"],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")


def _load_polars_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


if __name__ == "__main__":
    main()
