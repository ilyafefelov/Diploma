from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.decision_transformer.trajectories import build_decision_transformer_trajectory_frame
from smart_arbitrage.live.paper_trading import build_simulated_live_trading_frame
from smart_arbitrage.resources.simulated_trade_store import get_simulated_trade_store
from smart_arbitrage.training.simulated_trades import (
    SimulatedTradeTrainingConfig,
    build_simulated_trade_training_data,
)


class SimulatedTradeTrainingAssetConfig(dg.Config):
    """Configurable cap for the DAM simulated trade-training corpus."""

    max_anchors_per_tenant: int = 90
    scenarios_per_anchor: int = 8
    horizon_hours: int = 24
    seed: int = 20260504
    tenant_ids_csv: str = ""


@dg.asset(group_name="silver", tags={"medallion": "silver", "domain": "simulated_trade_training"})
def simulated_trade_silver_feature_frame(
    context,
    dam_price_history: pl.DataFrame,
) -> pl.DataFrame:
    """Silver DAM price frame prepared for simulated trade trajectory generation."""

    frame = (
        dam_price_history
        .select([column for column in ["timestamp", "price_uah_mwh"] if column in dam_price_history.columns])
        .drop_nulls()
        .unique(subset=["timestamp"], keep="last")
        .sort("timestamp")
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "scope": "silver_bridge_for_simulated_dam_training",
            "source_asset": "dam_price_history",
        },
    )
    return frame


@dg.asset(group_name="gold", tags={"medallion": "gold", "domain": "simulated_trade_training"})
def simulated_trade_training_frame(
    context,
    config: SimulatedTradeTrainingAssetConfig,
    simulated_trade_silver_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Simulated DAM state-action-reward-regret trajectories for later DFL/DT training."""

    training_result = build_simulated_trade_training_data(
        simulated_trade_silver_feature_frame,
        tenant_ids=_tenant_ids_from_csv(config.tenant_ids_csv),
        config=SimulatedTradeTrainingConfig(
            max_anchors_per_tenant=config.max_anchors_per_tenant,
            scenarios_per_anchor=config.scenarios_per_anchor,
            horizon_hours=config.horizon_hours,
            seed=config.seed,
        ),
    )
    get_simulated_trade_store().upsert_training_frames(
        episode_frame=training_result.episode_frame,
        transition_frame=training_result.transition_frame,
    )
    _add_metadata(
        context,
        {
            "episode_rows": training_result.episode_frame.height,
            "transition_rows": training_result.transition_frame.height,
            "tenant_count": training_result.transition_frame.select("tenant_id").n_unique() if training_result.transition_frame.height else 0,
            "market_venue": "DAM",
            "idm_scope": "schema_hook_only",
        },
    )
    return training_result.transition_frame


@dg.asset(group_name="gold", tags={"medallion": "gold", "domain": "decision_transformer"})
def decision_transformer_trajectory_frame(
    context,
    simulated_trade_training_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Gold offline DT trajectory rows from simulated dispatch transitions."""

    frame = build_decision_transformer_trajectory_frame(simulated_trade_training_frame)
    get_simulated_trade_store().upsert_decision_transformer_trajectory_frame(frame)
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "episode_count": frame.select("episode_id").n_unique() if frame.height else 0,
            "scope": "offline_dt_training_trajectory_not_live_policy",
        },
    )
    return frame


@dg.asset(group_name="gold", tags={"medallion": "gold", "domain": "paper_trading"})
def simulated_live_trading_frame(
    context,
    simulated_trade_training_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Gold simulated live-trading replay rows for backend/dashboard read models later."""

    frame = build_simulated_live_trading_frame(simulated_trade_training_frame)
    get_simulated_trade_store().upsert_simulated_live_trading_frame(frame)
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
            "scope": "simulated_paper_trading_not_market_execution",
        },
    )
    return frame


SIMULATED_TRADE_TRAINING_ASSETS = [
    simulated_trade_silver_feature_frame,
    simulated_trade_training_frame,
    decision_transformer_trajectory_frame,
    simulated_live_trading_frame,
]


def _tenant_ids_from_csv(value: str) -> list[str] | None:
    tenant_ids = [
        item.strip()
        for item in value.split(",")
        if item.strip()
    ]
    if not tenant_ids:
        return None
    return tenant_ids


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
    if context is not None:
        context.add_output_metadata(metadata)
