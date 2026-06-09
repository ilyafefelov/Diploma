from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path
import sys

import polars as pl

from smart_arbitrage.forecasting.poland_neighbor_snapshot_export import (
    build_entsoe_poland_governance_closure_packet,
    write_entsoe_poland_governance_closure_packet,
)

DEFAULT_RUN_SLUG = "week3_entsoe_poland_governance_closure"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export an ENTSO-E Poland hourly governance-closure evidence packet."
    )
    parser.add_argument("--snapshot-frame-pickle", type=Path, required=True)
    parser.add_argument("--hourly-feature-frame-pickle", type=Path, required=True)
    parser.add_argument("--governance-closure-frame-pickle", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    args = parser.parse_args()

    snapshot_frame = _load_polars_frame(args.snapshot_frame_pickle)
    hourly_feature_frame = _load_polars_frame(args.hourly_feature_frame_pickle)
    governance_closure_frame = _load_polars_frame(
        args.governance_closure_frame_pickle
    )
    packet = build_entsoe_poland_governance_closure_packet(
        snapshot_frame=snapshot_frame,
        hourly_feature_frame=hourly_feature_frame,
        governance_closure_frame=governance_closure_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
    )
    export_dir = write_entsoe_poland_governance_closure_packet(
        output_root=args.output_root,
        run_slug=args.run_slug,
        snapshot_frame=snapshot_frame,
        hourly_feature_frame=hourly_feature_frame,
        governance_closure_frame=governance_closure_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(
                export_dir / "entsoe_poland_governance_closure_summary.json"
            ),
            "summary_markdown": str(
                export_dir / "entsoe_poland_governance_closure_summary.md"
            ),
            "readiness_status": packet["governance_summary"]["readiness_status"],
            "blockers": packet["governance_summary"]["blockers"],
            "market_execution_enabled": packet["claim_boundary"][
                "market_execution_enabled"
            ],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def _load_polars_frame(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Evidence input not found: {path}")
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
