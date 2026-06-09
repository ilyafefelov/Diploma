"""Paper-trading replay rows derived from simulated dispatch transitions."""

from __future__ import annotations

from typing import Final

import polars as pl

REQUIRED_TRANSITION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "episode_id",
        "tenant_id",
        "interval_start",
        "step_index",
        "state_soc_before",
        "state_soc_after",
        "feasible_net_power_mw",
        "market_price_uah_mwh",
        "reward_uah",
        "cleared_trade_provenance",
    }
)


def build_simulated_live_trading_frame(transition_frame: pl.DataFrame) -> pl.DataFrame:
    """Build a simulated live-trading replay frame for API/dashboard consumption later."""

    missing_columns = REQUIRED_TRANSITION_COLUMNS.difference(transition_frame.columns)
    if missing_columns:
        raise ValueError(f"transition_frame is missing required columns: {sorted(missing_columns)}")
    return (
        transition_frame
        .sort(["tenant_id", "episode_id", "step_index"])
        .with_columns(
            [
                pl.when(pl.col("feasible_net_power_mw") > 0.0)
                .then(pl.lit("SELL"))
                .when(pl.col("feasible_net_power_mw") < 0.0)
                .then(pl.lit("BUY"))
                .otherwise(pl.lit("HOLD"))
                .alias("proposed_trade_side"),
                pl.col("feasible_net_power_mw").abs().alias("proposed_quantity_mw"),
                pl.when(pl.col("cleared_trade_provenance") == "simulated")
                .then(pl.lit("accepted"))
                .otherwise(pl.lit("rejected"))
                .alias("gatekeeper_status"),
                pl.lit("simulated").alias("paper_trade_provenance"),
                pl.lit(None, dtype=pl.Utf8).alias("settlement_id"),
                pl.lit(
                    "Simulated paper-trading replay only; not connected to live market execution."
                ).alias("live_mode_warning"),
            ]
        )
        .select(
            [
                "episode_id",
                "tenant_id",
                "interval_start",
                "step_index",
                "state_soc_before",
                "state_soc_after",
                "proposed_trade_side",
                "proposed_quantity_mw",
                "feasible_net_power_mw",
                "market_price_uah_mwh",
                "reward_uah",
                "gatekeeper_status",
                "paper_trade_provenance",
                "settlement_id",
                "live_mode_warning",
            ]
        )
    )
