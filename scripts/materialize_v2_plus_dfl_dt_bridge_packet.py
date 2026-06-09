from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge_export import (
    build_dfl_v2_plus_dfl_dt_bridge_packet,
    write_dfl_v2_plus_dfl_dt_bridge_packet,
)

DEFAULT_RUN_SLUG = "week3_dfl_v2_plus_dfl_dt_bridge_negative_evidence"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a V2+-anchored residual DFL/offline DT bridge packet."
    )
    parser.add_argument("--bridge-frame-pickle", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--asset-check-status", default=None)
    args = parser.parse_args()

    strict_frame = _load_polars_frame(args.bridge_frame_pickle)
    packet = build_dfl_v2_plus_dfl_dt_bridge_packet(
        run_slug=args.run_slug,
        strict_frame=strict_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        asset_check_status=args.asset_check_status,
    )
    export_dir = write_dfl_v2_plus_dfl_dt_bridge_packet(
        packet,
        output_root=args.output_root,
        strict_frame=strict_frame,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(export_dir / "dfl_v2_plus_dfl_dt_bridge_summary.json"),
            "summary_markdown": str(
                export_dir / "dfl_v2_plus_dfl_dt_bridge_summary.md"
            ),
            "gate_decision": packet["gate"]["decision"],
            "gate_passed": packet["gate"]["passed"],
            "negative_evidence": packet["negative_evidence"],
            "market_execution_enabled": packet["claim_boundary"][
                "market_execution_enabled"
            ],
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
