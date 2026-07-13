"""Materialize a causal temporal episode corpus from aligned full-context rows.

The LP teacher receives forecast p50 only. Realized prices are retained only as
post-delivery reward labels. This creates a research corpus; V13 still blocks
Decision Transformer training and market execution.
"""

from __future__ import annotations

import argparse
from datetime import timedelta
import json
from pathlib import Path
import pickle
from typing import Sequence

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import (
    BaselineForecastPoint,
    BaselineSolverConfig,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.decision_transformer.causal_episodes import (
    build_causal_temporal_episode_frame,
)
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    tenant_battery_defaults_from_registry,
)


def _teacher_seed_frame(context_frame: pl.DataFrame) -> pl.DataFrame:
    solver = HourlyDamBaselineSolver(BaselineSolverConfig(planning_horizon_hours=24))
    rows: list[dict[str, object]] = []
    for row in context_frame.iter_rows(named=True):
        anchor = row["anchor_timestamp"]
        if not hasattr(anchor, "isoformat"):
            raise TypeError("anchor_timestamp must be a datetime")
        forecast = [
            BaselineForecastPoint(
                forecast_timestamp=anchor + timedelta(hours=index + 1),
                source_timestamp=anchor,
                predicted_price_uah_mwh=float(price),
            )
            for index, price in enumerate(row["forecast_p50_uah_mwh"])
        ]
        defaults = tenant_battery_defaults_from_registry(str(row["tenant_id"]))
        solve = solver.solve_dispatch_from_forecast(
            forecast=forecast,
            battery_metrics=defaults.metrics,
            current_soc_fraction=float(row["starting_soc_fraction"]),
            anchor_timestamp=anchor,
            commit_reason="causal_temporal_episode_teacher_not_market_execution",
        )
        rows.append(
            {
                **row,
                "teacher_dispatch_mw_vector": [point.net_power_mw for point in solve.schedule],
                "teacher_soc_before_fraction_vector": [
                    point.soc_before_mwh / defaults.metrics.capacity_mwh for point in solve.schedule
                ],
                "teacher_soc_after_fraction_vector": [
                    point.soc_after_mwh / defaults.metrics.capacity_mwh for point in solve.schedule
                ],
                "teacher_degradation_penalty_uah_vector": [
                    point.degradation_penalty_uah for point in solve.schedule
                ],
                "teacher_solver_status": "strict_lp_optimal_or_optimal_inaccurate",
            }
        )
    return pl.DataFrame(rows)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args(argv)
    with args.input.open("rb") as handle:
        context_frame = pickle.load(handle)
    if not isinstance(context_frame, pl.DataFrame):
        raise TypeError("--input must contain a Polars DataFrame")
    episode_frame = build_causal_temporal_episode_frame(_teacher_seed_frame(context_frame))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("wb") as handle:
        pickle.dump(episode_frame, handle)
    summary = {
        "artifact": "causal_temporal_episode_corpus",
        "input_anchor_count": context_frame.height,
        "episode_count": episode_frame.select("episode_id").n_unique(),
        "row_count": episode_frame.height,
        "horizon_steps": episode_frame.group_by("episode_id").len().select("len").unique().to_series().to_list(),
        "state_contains_realized_price": "state_actual_price_uah_mwh" in episode_frame.columns,
        "dt_training_eligible": False,
        "market_execution_enabled": False,
        "claim_scope": "causal_temporal_episode_corpus_not_dt_training_or_market_execution",
    }
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
