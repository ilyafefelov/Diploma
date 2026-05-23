from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.ua_context_acquisition_export import (
    build_dfl_ua_context_backfill_readiness_packet,
    write_dfl_ua_context_backfill_readiness_packet,
)

DEFAULT_RUN_SLUG = "week3_dfl_ua_context_acquisition_v1"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a UA context acquisition readiness packet."
    )
    parser.add_argument("--source-inventory-pickle", type=Path, required=True)
    parser.add_argument("--dam-publication-pickle", type=Path, required=True)
    parser.add_argument("--weather-load-pv-pickle", type=Path, required=True)
    parser.add_argument("--grid-event-pickle", type=Path, required=True)
    parser.add_argument("--calendar-block-pickle", type=Path, required=True)
    parser.add_argument("--coverage-gate-pickle", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--asset-check-status", default=None)
    args = parser.parse_args()

    source_inventory = _load_polars_frame(args.source_inventory_pickle)
    dam_publication = _load_polars_frame(args.dam_publication_pickle)
    weather_load_pv = _load_polars_frame(args.weather_load_pv_pickle)
    grid_event = _load_polars_frame(args.grid_event_pickle)
    calendar_block = _load_polars_frame(args.calendar_block_pickle)
    coverage_gate = _load_polars_frame(args.coverage_gate_pickle)
    packet = build_dfl_ua_context_backfill_readiness_packet(
        run_slug=args.run_slug,
        source_inventory_frame=source_inventory,
        dam_publication_frame=dam_publication,
        weather_load_pv_frame=weather_load_pv,
        grid_event_frame=grid_event,
        calendar_block_frame=calendar_block,
        coverage_gate_frame=coverage_gate,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        asset_check_status=args.asset_check_status,
    )
    export_dir = write_dfl_ua_context_backfill_readiness_packet(
        packet,
        output_root=args.output_root,
        source_inventory_frame=source_inventory,
        dam_publication_frame=dam_publication,
        weather_load_pv_frame=weather_load_pv,
        grid_event_frame=grid_event,
        calendar_block_frame=calendar_block,
        coverage_gate_frame=coverage_gate,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(
                export_dir / "dfl_ua_context_backfill_readiness_summary.json"
            ),
            "summary_markdown": str(
                export_dir / "dfl_ua_context_backfill_readiness_summary.md"
            ),
            "v11_candidate_generation_ready": packet[
                "v11_candidate_generation_ready"
            ],
            "gate_decisions": packet["readiness_summary"][
                "context_backfill_gate_decisions"
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
