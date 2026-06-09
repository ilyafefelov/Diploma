"""Materialize an HF safe-switch candidate scorer research-shadow packet."""

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

from smart_arbitrage.dfl.hf_safe_switch_scorer import (  # noqa: E402
    build_hf_safe_switch_scorer_packet,
    write_hf_safe_switch_scorer_packet,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Train/evaluate a Hugging Face DecisionTransformer candidate scorer "
            "on frozen safe-switch teacher rows. This is research-shadow evidence, "
            "not V13 training and not market execution."
        )
    )
    parser.add_argument("--teacher-rows-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-slug", default="week5_hf_safe_switch_scorer_current")
    parser.add_argument(
        "--canonical-aggregate-json",
        type=Path,
        default=Path("runs/dt_v2_plus/aggregate.json"),
    )
    parser.add_argument("--thresholds-uah", default="0,5,10,20,50")
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
    parser.add_argument("--max-family-tail-risk-probability", type=float, default=0.5)
    parser.add_argument("--tail-risk-loss-threshold-uah", type=float, default=150.0)
    parser.add_argument("--seed", type=int, default=20260525)
    parser.add_argument(
        "--save-checkpoint",
        action="store_true",
        help=(
            "Persist the non-promotable HF checkpoint and run a load smoke. "
            "This does not grant V13 training or market execution permission."
        ),
    )
    args = parser.parse_args(argv)

    teacher_rows = pl.read_csv(args.teacher_rows_csv, infer_schema_length=1000)
    canonical = _load_json(args.canonical_aggregate_json)
    packet = build_hf_safe_switch_scorer_packet(
        teacher_rows_frame=teacher_rows,
        run_slug=args.run_slug,
        thresholds_uah=_parse_thresholds(args.thresholds_uah),
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
        seed=args.seed,
        output_dir=args.output_dir,
        canonical_aggregate=canonical,
        save_checkpoint=args.save_checkpoint,
    )
    paths = write_hf_safe_switch_scorer_packet(
        output_dir=args.output_dir,
        packet=packet,
    )
    summary = packet["summary"]
    result = {
        "summary_json": str(paths["summary_json"]),
        "threshold_metrics_csv": str(paths["threshold_metrics_csv"]),
        "best_threshold_uah": summary["best_threshold_uah"],
        "best_selected_mean_regret_uah": summary["best_metrics"][
            "selected_mean_regret_uah"
        ],
        "best_non_v2_plus_switch_count": summary["best_metrics"][
            "non_v2_plus_switch_count"
        ],
        "best_switch_loss_count": summary["best_metrics"]["switch_loss_count"],
        "canonical_comparison": summary["canonical_comparison"],
        "checkpoint": summary["checkpoint"],
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


def _parse_thresholds(raw: str) -> tuple[float, ...]:
    values = tuple(float(item.strip()) for item in raw.split(",") if item.strip())
    if not values:
        raise ValueError("--thresholds-uah must contain at least one number.")
    return values


if __name__ == "__main__":
    raise SystemExit(main())
