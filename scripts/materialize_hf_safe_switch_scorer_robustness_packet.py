"""Materialize HF safe-switch scorer robustness evidence."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
from pathlib import Path
import sys
from typing import Any

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.hf_safe_switch_scorer_robustness import (  # noqa: E402
    DEFAULT_ROBUSTNESS_SEEDS,
    DEFAULT_ROBUSTNESS_THRESHOLDS_UAH,
    build_hf_safe_switch_scorer_robustness_packet,
    write_hf_safe_switch_scorer_robustness_packet,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run multi-seed robustness evidence for the HF safe-switch scorer. "
            "This remains research-shadow evidence, not V13 training or market execution."
        )
    )
    parser.add_argument("--teacher-rows-csv", type=Path, required=True)
    parser.add_argument(
        "--canonical-aggregate-json",
        type=Path,
        default=Path("runs/dt_v2_plus/aggregate.json"),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-slug", default="week5_hf_safe_switch_scorer_robustness")
    parser.add_argument("--seeds", default=_join_ints(DEFAULT_ROBUSTNESS_SEEDS))
    parser.add_argument(
        "--thresholds-uah",
        default=_join_floats(DEFAULT_ROBUSTNESS_THRESHOLDS_UAH),
    )
    parser.add_argument("--max-epochs", type=int, default=200)
    parser.add_argument("--hidden-dim", type=int, default=64)
    parser.add_argument("--num-layers", type=int, default=2)
    parser.add_argument("--num-heads", type=int, default=2)
    parser.add_argument("--learning-rate", type=float, default=0.003)
    parser.add_argument("--weight-decay", type=float, default=0.01)
    parser.add_argument("--regret-scale-uah", type=float, default=100.0)
    parser.add_argument("--safe-switch-extra-weight", type=float, default=30.0)
    parser.add_argument("--pairwise-margin-scaled", type=float, default=0.2)
    parser.add_argument("--max-predicted-tail-risk-probability", type=float, default=0.5)
    parser.add_argument("--max-family-tail-risk-probability", type=float, default=1.0)
    parser.add_argument("--tail-risk-loss-threshold-uah", type=float, default=150.0)
    parser.add_argument("--bootstrap-iterations", type=int, default=1000)
    parser.add_argument("--bootstrap-seed", type=int, default=20260601)
    parser.add_argument(
        "--save-checkpoints",
        action="store_true",
        help=(
            "Persist per-seed non-promotable HF checkpoints. This does not "
            "grant V13 training or market execution permission."
        ),
    )
    args = parser.parse_args(argv)

    teacher_rows = pl.read_csv(args.teacher_rows_csv, infer_schema_length=1000)
    canonical = _load_json(args.canonical_aggregate_json)
    packet = build_hf_safe_switch_scorer_robustness_packet(
        teacher_rows_frame=teacher_rows,
        run_slug=args.run_slug,
        seeds=_parse_ints(args.seeds),
        thresholds_uah=_parse_floats(args.thresholds_uah),
        max_epochs=args.max_epochs,
        hidden_dim=args.hidden_dim,
        num_layers=args.num_layers,
        num_heads=args.num_heads,
        learning_rate=args.learning_rate,
        weight_decay=args.weight_decay,
        regret_scale_uah=args.regret_scale_uah,
        safe_switch_extra_weight=args.safe_switch_extra_weight,
        pairwise_margin_scaled=args.pairwise_margin_scaled,
        max_predicted_tail_risk_probability=(
            args.max_predicted_tail_risk_probability
        ),
        max_family_tail_risk_probability=args.max_family_tail_risk_probability,
        tail_risk_loss_threshold_uah=args.tail_risk_loss_threshold_uah,
        canonical_aggregate=canonical,
        output_dir=args.output_dir,
        save_checkpoints=args.save_checkpoints,
        bootstrap_iterations=args.bootstrap_iterations,
        bootstrap_seed=args.bootstrap_seed,
    )
    paths = write_hf_safe_switch_scorer_robustness_packet(
        output_dir=args.output_dir,
        packet=packet,
    )
    summary = packet["summary"]
    result = {
        "robustness_summary_json": str(paths["robustness_summary_json"]),
        "robustness_threshold_metrics_csv": str(
            paths["robustness_threshold_metrics_csv"]
        ),
        "seed_metrics_csv": str(paths["seed_metrics_csv"]),
        "failure_slices_csv": str(paths["failure_slices_csv"]),
        "best_selected_rows_csv": str(paths["best_selected_rows_csv"]),
        "selected_operating_threshold_uah": summary[
            "selected_operating_threshold_uah"
        ],
        "robustness_gate_passed": summary["robustness_gate_passed"],
        "robustness_gate_reason": summary["robustness_gate_reason"],
        "canonical_comparison": summary["canonical_comparison"],
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "dt_promotion_gate_passed": False,
    }
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return data


def _parse_ints(raw: str) -> tuple[int, ...]:
    values = tuple(int(item.strip()) for item in raw.split(",") if item.strip())
    if not values:
        raise ValueError("integer list must contain at least one value.")
    return values


def _parse_floats(raw: str) -> tuple[float, ...]:
    values = tuple(float(item.strip()) for item in raw.split(",") if item.strip())
    if not values:
        raise ValueError("float list must contain at least one value.")
    return values


def _join_ints(values: Sequence[int]) -> str:
    return ",".join(str(value) for value in values)


def _join_floats(values: Sequence[float]) -> str:
    return ",".join(f"{value:g}" for value in values)


if __name__ == "__main__":
    raise SystemExit(main())
