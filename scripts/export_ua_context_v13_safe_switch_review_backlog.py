"""Export a non-promotional V13 safe-switch review backlog."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_safe_switch_review_backlog import (
    DEFAULT_MATERIAL_LABEL_COLUMN,
    DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN,
    DEFAULT_TAIL_RISK_LABEL_COLUMN,
    build_dfl_ua_context_safe_switch_review_backlog_v13_frame,
    summarize_dfl_ua_context_safe_switch_review_backlog_v13_frame,
)

_TARGET_COLUMNS: tuple[str, ...] = (
    "acquisition_priority_rank",
    "tenant_id",
    "source_model_name",
    "current_prior_material_safe_switch_examples",
    "required_prior_material_safe_switch_examples",
    "target_new_prior_material_safe_switch_examples",
    "primary_blocking_source_family",
    "market_execution_enabled",
)
_OPTIONAL_CANDIDATE_COLUMNS: tuple[str, ...] = (
    "market_execution_enabled",
    "raw_hourly_action_imitation",
    "label_safe_switch_win",
    "candidate_family",
    "candidate_model_name",
    "label_regret_delta_vs_v2_plus_uah",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a Phase 0 V13 safe-switch review backlog from candidate "
            "safe-teacher rows and acquisition targets. The output is not "
            "training data and does not permit DT/LAVA or market execution."
        )
    )
    parser.add_argument("--candidate-rows-csv", type=Path, required=True)
    parser.add_argument("--acquisition-targets-csv", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument(
        "--material-label-column",
        default=DEFAULT_MATERIAL_LABEL_COLUMN,
    )
    parser.add_argument(
        "--tail-risk-label-column",
        default=DEFAULT_TAIL_RISK_LABEL_COLUMN,
    )
    parser.add_argument(
        "--source-evidence-timestamp-column",
        default=DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN,
    )
    parser.add_argument(
        "--max-review-rows-per-target",
        type=int,
        default=None,
    )
    args = parser.parse_args(argv)

    candidate_columns = _candidate_columns(
        args.candidate_rows_csv,
        material_label_column=args.material_label_column,
        tail_risk_label_column=args.tail_risk_label_column,
        source_evidence_timestamp_column=args.source_evidence_timestamp_column,
    )
    target_columns = _selected_columns(args.acquisition_targets_csv, _TARGET_COLUMNS)

    candidate_frame = (
        pl.scan_csv(
            args.candidate_rows_csv,
            infer_schema_length=0,
            try_parse_dates=False,
        )
        .select(candidate_columns)
        .collect()
    )
    acquisition_targets_frame = (
        pl.scan_csv(
            args.acquisition_targets_csv,
            infer_schema_length=0,
            try_parse_dates=False,
        )
        .select(target_columns)
        .collect()
    )
    backlog = build_dfl_ua_context_safe_switch_review_backlog_v13_frame(
        candidate_frame,
        acquisition_targets_frame,
        material_label_column=args.material_label_column,
        tail_risk_label_column=args.tail_risk_label_column,
        source_evidence_timestamp_column=args.source_evidence_timestamp_column,
        max_review_rows_per_target=args.max_review_rows_per_target,
    )
    summary = summarize_dfl_ua_context_safe_switch_review_backlog_v13_frame(backlog)
    summary.update(
        {
            "candidate_rows_csv": str(args.candidate_rows_csv),
            "acquisition_targets_csv": str(args.acquisition_targets_csv),
            "review_backlog_csv": str(args.output_csv),
            "candidate_can_satisfy_v13_without_validation": False,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        }
    )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_csv(args.output_csv)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(_json_ready(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote V13 safe-switch review backlog: {args.output_csv}")
    print(f"Wrote V13 safe-switch review backlog summary: {args.summary_json}")
    return 0


def _candidate_columns(
    path: Path,
    *,
    material_label_column: str,
    tail_risk_label_column: str,
    source_evidence_timestamp_column: str,
) -> list[str]:
    required = (
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        material_label_column,
        tail_risk_label_column,
        source_evidence_timestamp_column,
    )
    optional = tuple(
        column
        for column in _OPTIONAL_CANDIDATE_COLUMNS
        if column not in required
    )
    return _selected_columns(path, required + optional)


def _selected_columns(path: Path, requested: Sequence[str]) -> list[str]:
    header = _csv_header(path)
    missing = [column for column in requested if column not in header]
    if missing and set(missing).issubset(_OPTIONAL_CANDIDATE_COLUMNS):
        missing = []
    if missing:
        raise ValueError(f"{path} is missing required columns: {missing}")
    return [column for column in requested if column in header]


def _csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file)
        try:
            return next(reader)
        except StopIteration as error:
            raise ValueError(f"{path} is empty.") from error


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
