from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
from typing import Any

import polars as pl

from smart_arbitrage.dfl.poland_lag24_tail_risk_audit import (
    build_poland_lag24_tail_risk_audit_frame,
    build_poland_lag24_tail_risk_packet,
    write_poland_lag24_tail_risk_packet,
)

DEFAULT_RUN_SLUG = "week3_poland_lag24_richer_tail_risk_audit"
DEFAULT_BASELINE_MODEL = (
    "dfl_schedule_value_learner_v2_plus_"
    "nbeatsx_official_global_panel_horizon_calibrated_v1"
)
DEFAULT_CHALLENGER_MODEL = (
    "dfl_schedule_value_learner_v2_plus_"
    "tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1"
)
DEFAULT_STRATEGY_KIND = "dfl_schedule_value_learner_v2_plus_strict_lp_benchmark"


def main() -> None:
    args = _parse_args()
    baseline_frame = pl.read_csv(args.baseline_strict_rows_csv, infer_schema_length=1000)
    challenger_frame = (
        pl.read_csv(args.challenger_strict_rows_csv, infer_schema_length=1000)
        if args.challenger_strict_rows_csv is not None
        else _load_challenger_rows_from_postgres(
            dsn=args.dsn or _strategy_dsn_from_env(),
            strategy_kind=args.strategy_kind,
            generated_at_iso=args.generated_at_iso,
            model_name=args.challenger_model_name,
        )
    )
    audit_frame = build_poland_lag24_tail_risk_audit_frame(
        baseline_frame=baseline_frame,
        challenger_frame=challenger_frame,
        baseline_model_name=args.baseline_model_name,
        challenger_model_name=args.challenger_model_name,
        tail_loss_quantile=args.tail_loss_quantile,
    )
    packet = build_poland_lag24_tail_risk_packet(
        run_slug=args.run_slug,
        audit_frame=audit_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
    )
    export_dir = write_poland_lag24_tail_risk_packet(
        packet,
        output_root=args.output_root,
        audit_frame=audit_frame,
    )
    summary = packet["summary"]
    print(f"Wrote Poland tail-risk audit packet to {export_dir}")
    print(
        "Rows={row_count} wins={wins} losses={losses} "
        "mean_delta={mean_delta_regret_uah:.2f} "
        "oracle_loss_avoidance_is_diagnostic_only={oracle_loss_avoidance_is_diagnostic_only}".format(
            **summary
        )
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export a tail-risk autopsy for the Poland lag-24 V2+ near miss."
        )
    )
    parser.add_argument(
        "--baseline-strict-rows-csv",
        type=Path,
        required=True,
        help="CSV with frozen Ukrainian-only V2+ strict rows.",
    )
    parser.add_argument(
        "--challenger-strict-rows-csv",
        type=Path,
        default=None,
        help=(
            "Optional CSV with Poland challenger strict rows. If omitted, the "
            "script queries Postgres using --generated-at-iso."
        ),
    )
    parser.add_argument(
        "--generated-at-iso",
        default=None,
        help="Postgres generated_at timestamp for challenger rows.",
    )
    parser.add_argument(
        "--strategy-kind",
        default=DEFAULT_STRATEGY_KIND,
        help="Strategy kind to query when challenger CSV is omitted.",
    )
    parser.add_argument(
        "--baseline-model-name",
        default=DEFAULT_BASELINE_MODEL,
        help="Frozen V2+ model row to compare against.",
    )
    parser.add_argument(
        "--challenger-model-name",
        default=DEFAULT_CHALLENGER_MODEL,
        help="Poland-enhanced challenger model row to audit.",
    )
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--output-root", type=Path, default=Path("data/research_runs"))
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--tail-loss-quantile", type=float, default=0.8)
    return parser.parse_args()


def _strategy_dsn_from_env() -> str:
    value = os.environ.get("SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN") or os.environ.get(
        "SMART_ARBITRAGE_MARKET_DATA_DSN"
    )
    if not value:
        raise SystemExit(
            "Set SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN or pass --dsn when "
            "--challenger-strict-rows-csv is omitted."
        )
    return value


def _load_challenger_rows_from_postgres(
    *,
    dsn: str,
    strategy_kind: str,
    generated_at_iso: str | None,
    model_name: str,
) -> pl.DataFrame:
    if not generated_at_iso:
        raise SystemExit(
            "--generated-at-iso is required when --challenger-strict-rows-csv is omitted."
        )
    generated_at = _parse_generated_at(generated_at_iso)
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    evaluation_id,
                    tenant_id,
                    forecast_model_name,
                    strategy_kind,
                    market_venue,
                    anchor_timestamp::text AS anchor_timestamp,
                    generated_at::text AS generated_at,
                    horizon_hours,
                    starting_soc_fraction,
                    starting_soc_source,
                    decision_value_uah,
                    forecast_objective_value_uah,
                    oracle_value_uah,
                    regret_uah,
                    regret_ratio,
                    total_degradation_penalty_uah,
                    total_throughput_mwh,
                    committed_action,
                    committed_power_mw,
                    rank_by_regret,
                    evaluation_payload::text AS evaluation_payload
                FROM forecast_strategy_evaluations
                WHERE strategy_kind = %s
                  AND generated_at = %s
                  AND forecast_model_name = %s
                ORDER BY tenant_id, anchor_timestamp
                """,
                (strategy_kind, generated_at, model_name),
            )
            rows: list[dict[str, Any]] = list(cursor.fetchall())
    if not rows:
        raise SystemExit(
            "No challenger rows found for "
            f"strategy_kind={strategy_kind}, generated_at={generated_at.isoformat()}, "
            f"model={model_name}."
        )
    return pl.DataFrame(rows)


def _parse_generated_at(raw_value: str) -> datetime:
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit("--generated-at-iso must be an ISO datetime.") from exc


if __name__ == "__main__":
    main()
