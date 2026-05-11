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
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    args = parser.parse_args()

    gate_frame = _load_gate_frame(args.gate_frame_pickle)
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
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "registry_json": str(export_dir / "dfl_schedule_value_production_gate_registry.json"),
            "registry_markdown": str(export_dir / "dfl_schedule_value_production_gate_registry.md"),
            "production_promote_count": registry["summary"]["production_promote_count"],
            "promoted_source_model_names": registry["summary"]["promoted_source_model_names"],
            "market_execution_enabled": registry["summary"]["market_execution_enabled"],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")


def _load_gate_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


if __name__ == "__main__":
    main()
