"""Offline trajectory builders for Decision Transformer research."""

from __future__ import annotations

from typing import Any, Final

import polars as pl

REQUIRED_TRANSITION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "episode_id",
        "tenant_id",
        "market_venue",
        "scenario_index",
        "step_index",
        "interval_start",
        "state_soc_before",
        "state_soc_after",
        "state_soh",
        "feasible_net_power_mw",
        "market_price_uah_mwh",
        "reward_uah",
        "degradation_penalty_uah",
        "baseline_value_uah",
        "oracle_value_uah",
        "regret_uah",
    }
)


def build_decision_transformer_trajectory_frame(transition_frame: pl.DataFrame) -> pl.DataFrame:
    """Build return-conditioned offline trajectory rows from simulated dispatch transitions."""

    missing_columns = REQUIRED_TRANSITION_COLUMNS.difference(transition_frame.columns)
    if missing_columns:
        raise ValueError(f"transition_frame is missing required columns: {sorted(missing_columns)}")
    rows: list[dict[str, Any]] = []
    for _, episode_frame in transition_frame.sort(["episode_id", "step_index"]).group_by("episode_id", maintain_order=True):
        episode_rows = episode_frame.sort("step_index").iter_rows(named=True)
        materialized_rows = list(episode_rows)
        returns_to_go = _returns_to_go([float(row["reward_uah"]) for row in materialized_rows])
        for row, return_to_go in zip(materialized_rows, returns_to_go, strict=True):
            net_power_mw = float(row["feasible_net_power_mw"])
            rows.append(
                {
                    "episode_id": str(row["episode_id"]),
                    "tenant_id": str(row["tenant_id"]),
                    "market_venue": str(row["market_venue"]),
                    "scenario_index": int(row["scenario_index"]),
                    "step_index": int(row["step_index"]),
                    "interval_start": row["interval_start"],
                    "state_soc_before": float(row["state_soc_before"]),
                    "state_soc_after": float(row["state_soc_after"]),
                    "state_soh": float(row["state_soh"]),
                    "state_market_price_uah_mwh": float(row["market_price_uah_mwh"]),
                    "action_charge_mw": max(0.0, -net_power_mw),
                    "action_discharge_mw": max(0.0, net_power_mw),
                    "reward_uah": float(row["reward_uah"]),
                    "return_to_go_uah": return_to_go,
                    "degradation_penalty_uah": float(row["degradation_penalty_uah"]),
                    "baseline_value_uah": float(row["baseline_value_uah"]),
                    "oracle_value_uah": float(row["oracle_value_uah"]),
                    "regret_uah": float(row["regret_uah"]),
                    "academic_scope": "offline_dt_training_trajectory_not_live_policy",
                }
            )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["tenant_id", "episode_id", "step_index"])


def _returns_to_go(rewards: list[float]) -> list[float]:
    running_total = 0.0
    values: list[float] = []
    for reward in reversed(rewards):
        running_total += reward
        values.append(running_total)
    return list(reversed(values))
