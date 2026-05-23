from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_acquisition_export import (
    build_dfl_ua_context_v13_acquisition_packet,
    write_dfl_ua_context_v13_acquisition_packet,
)

DEFAULT_RUN_SLUG = "week3_dfl_ua_context_acquisition_v13"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a V13 Ukrainian context acquisition readiness packet."
    )
    parser.add_argument("--source-evidence-pickle", type=Path, default=None)
    parser.add_argument("--source-inventory-pickle", type=Path, required=True)
    parser.add_argument("--readiness-pickle", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--asset-check-status", default=None)
    args = parser.parse_args()

    source_evidence = (
        _load_polars_frame(args.source_evidence_pickle)
        if args.source_evidence_pickle is not None
        else None
    )
    source_inventory = _load_polars_frame(args.source_inventory_pickle)
    readiness = _load_polars_frame(args.readiness_pickle)
    packet = build_dfl_ua_context_v13_acquisition_packet(
        run_slug=args.run_slug,
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        acquisition_source_evidence_frame=source_evidence,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        asset_check_status=args.asset_check_status,
    )
    export_dir = write_dfl_ua_context_v13_acquisition_packet(
        packet,
        output_root=args.output_root,
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        acquisition_source_evidence_frame=source_evidence,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(
                export_dir / "dfl_ua_context_v13_acquisition_summary.json"
            ),
            "summary_markdown": str(
                export_dir / "dfl_ua_context_v13_acquisition_summary.md"
            ),
            "v13_candidate_generation_ready": packet[
                "v13_candidate_generation_ready"
            ],
            "readiness_decisions": packet["readiness_summary"][
                "readiness_decisions"
            ],
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
