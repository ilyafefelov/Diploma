"""Materialize offline DT/V2+ residual challenger promotion evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.dt_v2_plus_promotion_evidence import (  # noqa: E402
    build_dt_v2_plus_promotion_evidence_packet,
    write_dt_v2_plus_promotion_evidence_packet,
)

DEFAULT_RUN_SLUG = "week3_dt_v2_plus_promotion_evidence_current"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build offline promotion evidence for a residual DT selector over "
            "the frozen V2+ fallback. This never enables market execution."
        )
    )
    parser.add_argument("--selected-rows-csv", type=Path, required=True)
    parser.add_argument("--teacher-rows-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--source-model-name", default="")
    parser.add_argument("--min-final-holdout-anchor-count", type=int, default=90)
    parser.add_argument(
        "--min-mean-regret-improvement-ratio-vs-v2-plus",
        type=float,
        default=0.05,
    )
    parser.add_argument("--max-non-v2-plus-switch-rate", type=float, default=0.25)
    parser.add_argument("--tail-risk-loss-threshold-uah", type=float, default=150.0)
    parser.add_argument("--max-tail-risk-loss-count", type=int, default=0)
    args = parser.parse_args(argv)

    selected_rows = _read_csv(args.selected_rows_csv)
    teacher_rows = _read_csv(args.teacher_rows_csv)
    if args.source_model_name:
        selected_rows = selected_rows.filter(
            pl.col("source_model_name") == args.source_model_name
        )
        teacher_rows = teacher_rows.filter(
            pl.col("source_model_name") == args.source_model_name
        )
    packet = build_dt_v2_plus_promotion_evidence_packet(
        selected_rows,
        teacher_rows,
        run_slug=args.run_slug,
        min_final_holdout_anchor_count=args.min_final_holdout_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            args.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        max_non_v2_plus_switch_rate=args.max_non_v2_plus_switch_rate,
        tail_risk_loss_threshold_uah=args.tail_risk_loss_threshold_uah,
        max_tail_risk_loss_count=args.max_tail_risk_loss_count,
    )
    paths = write_dt_v2_plus_promotion_evidence_packet(
        output_dir=args.output_dir,
        packet=packet,
    )
    summary = packet["summary"]
    gate = summary["gate"]
    json.dump(
        {
            "summary_json": str(paths["summary_json"]),
            "summary_markdown": str(paths["summary_markdown"]),
            "gate_rows_csv": str(paths["gate_rows_csv"]),
            "selected_rows_csv": str(paths["selected_rows_csv"]),
            "safe_switch_opportunities_csv": str(
                paths["safe_switch_opportunities_csv"]
            ),
            "promotion_evidence_passed": gate["promotion_evidence_passed"],
            "promotion_blocker": gate["promotion_blocker"],
            "selector_minus_v2_plus_mean_regret_uah": gate[
                "selector_minus_v2_plus_mean_regret_uah"
            ],
            "non_v2_plus_switch_count": gate["non_v2_plus_switch_count"],
            "market_execution_enabled": False,
            "promotion_gate_passed": False,
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def _read_csv(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pl.read_csv(path, infer_schema_length=1000)


if __name__ == "__main__":
    raise SystemExit(main())
