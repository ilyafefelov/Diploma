from typing import Any

import dagster as dg
import polars as pl

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


@dg.asset(group_name="gold")
def simulated_trade_training_frame(
    context,
    config: SimulatedTradeTrainingAssetConfig,
    dam_price_history: pl.DataFrame,
) -> pl.DataFrame:
    """Simulated DAM state-action-reward-regret trajectories for later DFL/DT training."""

    training_result = build_simulated_trade_training_data(
        dam_price_history,
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


SIMULATED_TRADE_TRAINING_ASSETS = [simulated_trade_training_frame]


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
