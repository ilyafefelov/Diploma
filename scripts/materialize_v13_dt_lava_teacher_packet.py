from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys
from typing import Sequence

import polars as pl

from smart_arbitrage.dfl.v13_dt_lava_teacher_export import (
    V13_DT_LAVA_TEACHER_JSON_ARTIFACT_NAME,
    V13_DT_LAVA_TEACHER_MARKDOWN_ARTIFACT_NAME,
    V13_DT_LAVA_TEACHER_VALIDATION_JSON_ARTIFACT_NAME,
    build_dfl_v13_dt_lava_teacher_packet,
    write_dfl_v13_dt_lava_teacher_packet,
)

DEFAULT_RUN_SLUG = "week3_v13_dt_lava_teacher_dataset"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export a V13-gated DT/LAVA teacher dataset packet. This writes "
            "candidate-index / schedule-family supervision evidence only; it "
            "does not train DT/LAVA or enable market execution."
        )
    )
    parser.add_argument("--teacher-contract-pickle", type=Path, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data") / "research_runs",
    )
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--asset-check-status", default=None)
    args = parser.parse_args(argv)

    teacher_contract_frame = _load_polars_frame(args.teacher_contract_pickle)
    packet = build_dfl_v13_dt_lava_teacher_packet(
        run_slug=args.run_slug,
        teacher_contract_frame=teacher_contract_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        asset_check_status=args.asset_check_status,
    )
    export_dir = write_dfl_v13_dt_lava_teacher_packet(
        packet,
        output_root=args.output_root,
        teacher_contract_frame=teacher_contract_frame,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(export_dir / V13_DT_LAVA_TEACHER_JSON_ARTIFACT_NAME),
            "summary_markdown": str(
                export_dir / V13_DT_LAVA_TEACHER_MARKDOWN_ARTIFACT_NAME
            ),
            "validation_json": str(
                export_dir / V13_DT_LAVA_TEACHER_VALIDATION_JSON_ARTIFACT_NAME
            ),
            "v13_training_permission_gate_passed": packet["dataset_summary"][
                "v13_training_permission_gate_passed"
            ],
            "permitted_model_training_rows": packet["dataset_summary"][
                "permitted_model_training_rows"
            ],
            "promotion_gate_passed": packet["dataset_summary"]["promotion_gate_passed"],
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
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
