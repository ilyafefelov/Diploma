"""Materialize time-separated return-conditioned DT and decision-aware DT runs."""

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

from smart_arbitrage.dfl.dt_research_shadow import (  # noqa: E402
    OBJECTIVE_KIND_CROSS_ENTROPY,
    OBJECTIVE_KIND_DECISION_AWARE,
)
from smart_arbitrage.dfl.dt_temporal_v2_plus_experiment import (  # noqa: E402
    run_dt_temporal_v2_plus_experiment,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (  # noqa: E402
    build_dfl_schedule_value_learner_v2_plus_rolling_strict_rows_frame,
)

DEFAULT_SOURCE_MODEL_NAMES = (
    "nbeatsx_official_global_panel_horizon_calibrated_v1",
    "nbeatsx_official_global_panel_v1",
)
DEFAULT_TENANT_IDS = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
DEFAULT_OBJECTIVES = (
    OBJECTIVE_KIND_CROSS_ENTROPY,
    OBJECTIVE_KIND_DECISION_AWARE,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Train return-conditioned DT candidate selectors on earlier rolling "
            "windows and compare them with frozen V2+ on later windows."
        )
    )
    parser.add_argument("--candidate-library-pickle", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--run-slug",
        default="dt_temporal_v2_plus_experiment_2026_07_13",
    )
    parser.add_argument(
        "--source-model-names",
        default=",".join(DEFAULT_SOURCE_MODEL_NAMES),
    )
    parser.add_argument("--tenant-ids", default=",".join(DEFAULT_TENANT_IDS))
    parser.add_argument("--evaluation-window-indices", default="1,2,3")
    parser.add_argument("--objective-kinds", default=",".join(DEFAULT_OBJECTIVES))
    parser.add_argument("--seeds", default="42,2026,7")
    parser.add_argument("--validation-window-count", type=int, default=4)
    parser.add_argument("--validation-anchor-count", type=int, default=18)
    parser.add_argument("--min-prior-anchors-before-window", type=int, default=30)
    parser.add_argument("--context-length", type=int, default=4)
    parser.add_argument("--max-epochs", type=int, default=10)
    parser.add_argument("--hidden-dim", type=int, default=64)
    parser.add_argument("--num-layers", type=int, default=2)
    parser.add_argument("--num-heads", type=int, default=2)
    parser.add_argument("--learning-rate", type=float, default=0.001)
    parser.add_argument("--min-predicted-improvement-uah", type=float, default=20.0)
    parser.add_argument("--max-family-tail-risk-probability", type=float, default=0.5)
    parser.add_argument("--tail-risk-loss-threshold-uah", type=float, default=150.0)
    parser.add_argument("--model-backbone", choices=("auto", "local", "hf"), default="hf")
    args = parser.parse_args(argv)

    source_model_names = _csv_text_values(
        args.source_model_names,
        field_name="source_model_names",
    )
    tenant_ids = _csv_text_values(args.tenant_ids, field_name="tenant_ids")
    candidate_library = _load_polars_frame(args.candidate_library_pickle)
    rolling_strict_rows = (
        build_dfl_schedule_value_learner_v2_plus_rolling_strict_rows_frame(
            candidate_library,
            tenant_ids=tenant_ids,
            forecast_model_names=source_model_names,
            validation_window_count=args.validation_window_count,
            validation_anchor_count=args.validation_anchor_count,
            min_prior_anchors_before_window=args.min_prior_anchors_before_window,
        )
    )
    result = run_dt_temporal_v2_plus_experiment(
        rolling_strict_rows,
        output_dir=args.output_dir,
        run_slug=args.run_slug,
        source_model_names=source_model_names,
        evaluation_window_indices=_csv_int_values(
            args.evaluation_window_indices,
            field_name="evaluation_window_indices",
        ),
        objective_kinds=_csv_text_values(
            args.objective_kinds,
            field_name="objective_kinds",
        ),
        seeds=_csv_int_values(args.seeds, field_name="seeds"),
        context_length=args.context_length,
        max_epochs=args.max_epochs,
        hidden_dim=args.hidden_dim,
        num_layers=args.num_layers,
        num_heads=args.num_heads,
        learning_rate=args.learning_rate,
        min_predicted_improvement_uah=args.min_predicted_improvement_uah,
        max_family_tail_risk_probability=args.max_family_tail_risk_probability,
        tail_risk_loss_threshold_uah=args.tail_risk_loss_threshold_uah,
        model_backbone=args.model_backbone,
    )
    json.dump(result["summary"], sys.stdout, indent=2)
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
    try:
        return tuple(
            int(item)
            for item in _csv_text_values(value, field_name=field_name)
        )
    except ValueError as exc:
        raise ValueError(f"{field_name} must contain comma-separated integers.") from exc


if __name__ == "__main__":
    raise SystemExit(main())
