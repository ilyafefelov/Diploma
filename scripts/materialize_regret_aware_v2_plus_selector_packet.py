"""Materialize a regret-aware V2+ fallback selector packet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys
from typing import Sequence

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.dt_research_shadow import (  # noqa: E402
    build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows,
)
from smart_arbitrage.dfl.regret_aware_v2_plus_selector import (  # noqa: E402
    FEATURE_SET_BASE,
    MODEL_KIND_WEIGHTED_RIDGE,
    build_regret_aware_v2_plus_selector_packet,
    write_regret_aware_v2_plus_selector_packet,
)

DEFAULT_SOURCE_MODEL_NAME = "nbeatsx_official_global_panel_horizon_calibrated_v1"
DEFAULT_RUN_SLUG = "week3_regret_aware_v2_plus_selector_current"
TEACHER_ROWS_CSV_NAME = "regret_aware_v2_plus_selector_teacher_rows.csv"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a non-promotable regret-aware candidate selector from the "
            "real V2+ apples-to-apples strict-row artifact or its teacher rows."
        )
    )
    parser.add_argument("--teacher-rows-csv", type=Path, default=None)
    parser.add_argument("--strict-rows-csv", type=Path, default=None)
    parser.add_argument("--regret-decomposition-csv", type=Path, default=None)
    parser.add_argument("--regret-decomposition-pickle", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--source-model-name", default=DEFAULT_SOURCE_MODEL_NAME)
    parser.add_argument("--min-predicted-improvement-uah", type=float, default=150.0)
    parser.add_argument("--tail-risk-loss-threshold-uah", type=float, default=150.0)
    parser.add_argument("--max-family-tail-risk-probability", type=float, default=0.5)
    parser.add_argument("--ridge-l2", type=float, default=10.0)
    parser.add_argument("--model-kind", default=MODEL_KIND_WEIGHTED_RIDGE)
    parser.add_argument("--feature-set", default=FEATURE_SET_BASE)
    args = parser.parse_args(argv)

    if args.teacher_rows_csv is None and args.strict_rows_csv is None:
        raise ValueError("Use --teacher-rows-csv or --strict-rows-csv.")
    if args.regret_decomposition_csv is not None and args.regret_decomposition_pickle is not None:
        raise ValueError(
            "Use only one of --regret-decomposition-csv or "
            "--regret-decomposition-pickle."
        )

    teacher_rows = _teacher_rows(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    _csv_ready(teacher_rows).write_csv(args.output_dir / TEACHER_ROWS_CSV_NAME)
    result = build_regret_aware_v2_plus_selector_packet(
        teacher_rows,
        run_slug=args.run_slug,
        min_predicted_improvement_uah=args.min_predicted_improvement_uah,
        tail_risk_loss_threshold_uah=args.tail_risk_loss_threshold_uah,
        max_family_tail_risk_probability=args.max_family_tail_risk_probability,
        ridge_l2=args.ridge_l2,
        model_kind=args.model_kind,
        feature_set=args.feature_set,
    )
    paths = write_regret_aware_v2_plus_selector_packet(
        output_dir=args.output_dir,
        result=result,
    )
    summary = result["summary"]
    json.dump(
        {
            "summary_json": str(paths["summary_json"]),
            "summary_markdown": str(paths["summary_markdown"]),
            "selected_rows_csv": str(paths["selected_rows_csv"]),
            "teacher_rows_csv": str(args.output_dir / TEACHER_ROWS_CSV_NAME),
            "selector_mean_regret_uah": summary["evaluation"][
                "selector_mean_regret_uah"
            ],
            "v2_plus_mean_regret_uah": summary["evaluation"][
                "v2_plus_mean_regret_uah"
            ],
            "selector_minus_v2_plus_mean_regret_uah": summary["evaluation"][
                "selector_minus_v2_plus_mean_regret_uah"
            ],
            "non_v2_plus_switch_count": summary["evaluation"][
                "non_v2_plus_switch_count"
            ],
            "abstention_count": summary["evaluation"]["abstention_count"],
            "model_kind": summary["model_kind"],
            "feature_set": summary["feature_set"],
            "market_execution_enabled": False,
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def _teacher_rows(args: argparse.Namespace) -> pl.DataFrame:
    if args.teacher_rows_csv is not None:
        frame = pl.read_csv(args.teacher_rows_csv, infer_schema_length=1000)
        return frame.filter(pl.col("source_model_name") == args.source_model_name)
    if args.strict_rows_csv is None:
        raise ValueError("--strict-rows-csv is required when teacher rows are absent.")
    strict_rows = pl.read_csv(args.strict_rows_csv, infer_schema_length=1000)
    strict_rows = strict_rows.filter(pl.col("source_model_name") == args.source_model_name)
    if strict_rows.is_empty():
        raise ValueError(f"No strict rows found for source model: {args.source_model_name}")
    regret_decomposition = None
    if args.regret_decomposition_csv is not None:
        regret_decomposition = pl.read_csv(
            args.regret_decomposition_csv,
            infer_schema_length=1000,
        ).filter(pl.col("source_model_name") == args.source_model_name)
    elif args.regret_decomposition_pickle is not None:
        regret_decomposition = _load_polars_frame(
            args.regret_decomposition_pickle
        ).filter(pl.col("source_model_name") == args.source_model_name)
    return build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows(
        strict_rows_frame=strict_rows,
        regret_decomposition_frame=regret_decomposition,
    )


def _load_polars_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


def _csv_ready(frame: pl.DataFrame) -> pl.DataFrame:
    vector_columns = [
        "forecast_price_uah_mwh_vector",
        "actual_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
    ]
    expressions = []
    for column in vector_columns:
        if column in frame.columns:
            expressions.append(
                pl.col(column)
                .map_elements(_json_text, return_dtype=pl.String)
                .alias(column)
            )
    return frame.with_columns(expressions) if expressions else frame


def _json_text(value: object) -> str:
    if isinstance(value, pl.Series):
        return json.dumps(value.to_list())
    return json.dumps(value)


if __name__ == "__main__":
    raise SystemExit(main())
