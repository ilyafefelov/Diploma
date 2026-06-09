from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.v10_closure_export import (
    build_dfl_v10_tail_risk_transfer_closure_packet,
    write_dfl_v10_tail_risk_transfer_closure_packet,
)

DEFAULT_RUN_SLUG = "week3_dfl_v10_tail_risk_transfer_closure"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a V10 tail-risk transfer closure evidence packet."
    )
    parser.add_argument("--tail-risk-audit-pickle", type=Path, required=True)
    parser.add_argument("--learning-ceiling-pickle", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--asset-check-status", default=None)
    args = parser.parse_args()

    audit_frame = _load_polars_frame(args.tail_risk_audit_pickle)
    decision_frame = _load_polars_frame(args.learning_ceiling_pickle)
    packet = build_dfl_v10_tail_risk_transfer_closure_packet(
        run_slug=args.run_slug,
        tail_risk_audit_frame=audit_frame,
        learning_ceiling_decision_frame=decision_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        asset_check_status=args.asset_check_status,
    )
    export_dir = write_dfl_v10_tail_risk_transfer_closure_packet(
        packet,
        output_root=args.output_root,
        tail_risk_audit_frame=audit_frame,
        learning_ceiling_decision_frame=decision_frame,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(
                export_dir / "dfl_v10_tail_risk_transfer_closure_summary.json"
            ),
            "summary_markdown": str(
                export_dir / "dfl_v10_tail_risk_transfer_closure_summary.md"
            ),
            "negative_evidence": packet["negative_evidence"],
            "learning_ceiling_decision": packet["learning_ceiling_decision"][
                "v10_learning_ceiling_decision"
            ],
            "dt_lava_ready": packet["learning_ceiling_decision"]["dt_lava_ready"],
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
