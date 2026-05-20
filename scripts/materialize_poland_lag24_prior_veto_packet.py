from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from smart_arbitrage.dfl.poland_lag24_prior_veto import (
    PolandLag24PriorVetoConfig,
    build_poland_lag24_prior_veto_frame,
    build_poland_lag24_prior_veto_packet,
    write_poland_lag24_prior_veto_packet,
)

DEFAULT_RUN_SLUG = "week3_poland_lag24_prior_tail_risk_veto"


def main() -> None:
    args = _parse_args()
    audit_frame = pl.read_csv(args.tail_risk_audit_csv, infer_schema_length=1000)
    config = PolandLag24PriorVetoConfig(
        min_prior_rows=args.min_prior_rows,
        ridge_alpha=args.ridge_alpha,
        threshold_candidates=tuple(args.threshold_candidates),
        min_prior_selected_rows=args.min_prior_selected_rows,
        max_prior_selected_loss_delta_uah=args.max_prior_selected_loss_delta_uah,
        require_prior_mean_non_degradation=not args.allow_prior_mean_degradation,
        require_prior_median_non_degradation=not args.allow_prior_median_degradation,
        promotion_min_improvement_ratio=args.promotion_min_improvement_ratio,
    )
    veto_frame = build_poland_lag24_prior_veto_frame(audit_frame, config=config)
    packet = build_poland_lag24_prior_veto_packet(
        run_slug=args.run_slug,
        veto_frame=veto_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        config=config,
    )
    export_dir = write_poland_lag24_prior_veto_packet(
        packet,
        output_root=args.output_root,
        veto_frame=veto_frame,
    )
    summary = packet["summary"]
    print(f"Wrote Poland prior-veto packet to {export_dir}")
    print(
        "Rows={row_count} selected_poland={selected_challenger_rows} "
        "mean={selected_mean_regret_uah:.2f} "
        "improvement={mean_regret_improvement_ratio_vs_baseline:.2%} "
        "selector_is_prior_only={selector_is_prior_only}".format(
            selector_is_prior_only=packet["claim_boundary"]["selector_is_prior_only"],
            **summary,
        )
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export a prior-only tail-risk veto over Poland lag-24 schedules."
        )
    )
    parser.add_argument(
        "--tail-risk-audit-csv",
        type=Path,
        required=True,
        help="CSV produced by materialize_poland_lag24_tail_risk_packet.py.",
    )
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--output-root", type=Path, default=Path("data/research_runs"))
    parser.add_argument("--min-prior-rows", type=int, default=20)
    parser.add_argument("--ridge-alpha", type=float, default=100.0)
    parser.add_argument("--min-prior-selected-rows", type=int, default=1)
    parser.add_argument(
        "--max-prior-selected-loss-delta-uah",
        type=float,
        default=250.0,
    )
    parser.add_argument(
        "--promotion-min-improvement-ratio",
        type=float,
        default=0.05,
    )
    parser.add_argument(
        "--threshold-candidates",
        nargs="+",
        type=float,
        default=[
            -500.0,
            -300.0,
            -200.0,
            -150.0,
            -100.0,
            -75.0,
            -50.0,
            -30.0,
            -20.0,
            -10.0,
            0.0,
            10.0,
            20.0,
            30.0,
            50.0,
            75.0,
            100.0,
        ],
    )
    parser.add_argument(
        "--allow-prior-mean-degradation",
        action="store_true",
        help="Allow a prior threshold even if its prior mean is worse than fallback.",
    )
    parser.add_argument(
        "--allow-prior-median-degradation",
        action="store_true",
        help="Allow a prior threshold even if its prior median is worse than fallback.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
