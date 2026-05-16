from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.schedule_value_learner_v2_plus_export import (
    build_dfl_schedule_value_learner_v2_plus_comparison_packet,
    write_dfl_schedule_value_learner_v2_plus_comparison_packet,
)

DEFAULT_RUN_SLUG = "week3_official_global_panel_schedule_value_v2_plus_comparison"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a Schedule/Value Learner V2+ comparison packet."
    )
    parser.add_argument("--strict-frame-pickle", type=Path, required=True)
    parser.add_argument("--learner-frame-pickle", type=Path, required=True)
    parser.add_argument("--regret-decomposition-pickle", type=Path, required=True)
    parser.add_argument("--rolling-robustness-pickle", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    args = parser.parse_args()

    strict_frame = _load_polars_frame(args.strict_frame_pickle)
    learner_frame = _load_polars_frame(args.learner_frame_pickle)
    regret_decomposition_frame = _load_polars_frame(args.regret_decomposition_pickle)
    rolling_robustness_frame = (
        _load_polars_frame(args.rolling_robustness_pickle)
        if args.rolling_robustness_pickle is not None
        else None
    )
    packet = build_dfl_schedule_value_learner_v2_plus_comparison_packet(
        run_slug=args.run_slug,
        strict_frame=strict_frame,
        learner_frame=learner_frame,
        regret_decomposition_frame=regret_decomposition_frame,
        rolling_robustness_frame=rolling_robustness_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
    )
    export_dir = write_dfl_schedule_value_learner_v2_plus_comparison_packet(
        packet,
        output_root=args.output_root,
        strict_frame=strict_frame,
        rolling_robustness_frame=rolling_robustness_frame,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "comparison_json": str(
                export_dir / "dfl_schedule_value_learner_v2_plus_comparison.json"
            ),
            "comparison_markdown": str(
                export_dir / "dfl_schedule_value_learner_v2_plus_comparison.md"
            ),
            "gate_decision": packet["gate"]["decision"],
            "best_source_model_name": packet["gate"]["metrics"][
                "best_source_model_name"
            ],
            "market_execution_enabled": packet["gate"]["metrics"][
                "market_execution_enabled"
            ],
            "rolling_robustness_attached": rolling_robustness_frame is not None,
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
