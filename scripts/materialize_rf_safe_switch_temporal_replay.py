"""Materialize a time-separated retrospective RF safe-switch replay."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys
from typing import Any, Sequence

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.rf_safe_switch_temporal_replay import (  # noqa: E402
    build_rf_safe_switch_temporal_replay_packet,
    write_rf_safe_switch_temporal_replay_packet,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (  # noqa: E402
    build_dfl_schedule_value_learner_v2_plus_rolling_strict_rows_frame,
)

DEFAULT_SOURCE_MODEL_NAME = "nbeatsx_official_global_panel_horizon_calibrated_v1"
DEFAULT_TENANT_IDS = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fit the RF safe-switch on earlier V2+ rolling windows and evaluate "
            "one distinct later retrospective window."
        )
    )
    parser.add_argument("--candidate-library-pickle", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--run-slug",
        default="rf_safe_switch_temporal_replay_2026_07_12",
    )
    parser.add_argument("--source-model-name", default=DEFAULT_SOURCE_MODEL_NAME)
    parser.add_argument("--tenant-ids", default=",".join(DEFAULT_TENANT_IDS))
    parser.add_argument("--training-window-indices", default="4,3,2")
    parser.add_argument("--evaluation-window-index", type=int, default=1)
    parser.add_argument("--validation-window-count", type=int, default=4)
    parser.add_argument("--validation-anchor-count", type=int, default=18)
    parser.add_argument("--min-prior-anchors-before-window", type=int, default=30)
    parser.add_argument("--seeds", default="42,2026,7")
    parser.add_argument("--min-predicted-improvement-uah", type=float, default=20.0)
    parser.add_argument("--tail-risk-loss-threshold-uah", type=float, default=150.0)
    parser.add_argument("--max-family-tail-risk-probability", type=float, default=0.5)
    parser.add_argument("--bootstrap-iterations", type=int, default=20_000)
    parser.add_argument("--bootstrap-block-length", type=int, default=3)
    parser.add_argument("--bootstrap-seed", type=int, default=20260712)
    args = parser.parse_args(argv)

    candidate_library = _load_polars_frame(args.candidate_library_pickle)
    tenant_ids = _csv_text_values(args.tenant_ids, field_name="tenant_ids")
    rolling_strict_rows = (
        build_dfl_schedule_value_learner_v2_plus_rolling_strict_rows_frame(
            candidate_library,
            tenant_ids=tenant_ids,
            forecast_model_names=(args.source_model_name,),
            validation_window_count=args.validation_window_count,
            validation_anchor_count=args.validation_anchor_count,
            min_prior_anchors_before_window=args.min_prior_anchors_before_window,
        )
    )
    packet = build_rf_safe_switch_temporal_replay_packet(
        rolling_strict_rows,
        run_slug=args.run_slug,
        source_model_name=args.source_model_name,
        training_window_indices=_csv_int_values(
            args.training_window_indices,
            field_name="training_window_indices",
        ),
        evaluation_window_index=args.evaluation_window_index,
        seeds=_csv_int_values(args.seeds, field_name="seeds"),
        min_predicted_improvement_uah=args.min_predicted_improvement_uah,
        tail_risk_loss_threshold_uah=args.tail_risk_loss_threshold_uah,
        max_family_tail_risk_probability=args.max_family_tail_risk_probability,
        bootstrap_iterations=args.bootstrap_iterations,
        bootstrap_block_length=args.bootstrap_block_length,
        bootstrap_seed=args.bootstrap_seed,
    )
    paths = write_rf_safe_switch_temporal_replay_packet(
        output_dir=args.output_dir,
        packet=packet,
    )
    json.dump(
        {
            "summary": packet["summary"],
            "artifacts": {key: str(path) for key, path in paths.items()},
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def _load_polars_frame(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("rb") as file:
        value: Any = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


def _csv_text_values(value: str, *, field_name: str) -> tuple[str, ...]:
    values = tuple(item.strip() for item in value.split(",") if item.strip())
    if not values:
        raise ValueError(f"{field_name} must contain at least one value.")
    return values


def _csv_int_values(value: str, *, field_name: str) -> tuple[int, ...]:
    text_values = _csv_text_values(value, field_name=field_name)
    try:
        return tuple(int(item) for item in text_values)
    except ValueError as exc:
        raise ValueError(f"{field_name} must contain comma-separated integers.") from exc


if __name__ == "__main__":
    raise SystemExit(main())
