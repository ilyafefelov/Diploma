from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.decision_transformer.trajectories import build_decision_transformer_trajectory_frame
from smart_arbitrage.decision_transformer.policy_training import build_decision_transformer_policy_preview_frame
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


@dg.asset(
    group_name=taxonomy.SILVER_SIMULATED_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="silver",
        domain="simulated_trade_training",
        elt_stage="transform",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
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


@dg.asset(
    group_name=taxonomy.SILVER_DECISION_TRANSFORMER,
    tags=taxonomy.asset_tags(
        medallion="silver",
        domain="decision_transformer",
        elt_stage="transform",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def decision_transformer_forecast_context_silver(
    context,
    nbeatsx_price_forecast: pl.DataFrame,
    tft_price_forecast: pl.DataFrame,
) -> pl.DataFrame:
    """Silver NBEATSx/TFT forecast state exposed to DT trajectory construction."""

    frame = _build_decision_transformer_forecast_context(
        nbeatsx_price_forecast=nbeatsx_price_forecast,
        tft_price_forecast=tft_price_forecast,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "source_models": "nbeatsx_silver_v0,tft_silver_v0",
            "scope": "forecast_state_for_offline_dt_preview",
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_SIMULATED_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="simulated_trade_training",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
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


@dg.asset(
    group_name=taxonomy.GOLD_DECISION_TRANSFORMER,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="decision_transformer",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def decision_transformer_trajectory_frame(
    context,
    simulated_trade_training_frame: pl.DataFrame,
    decision_transformer_forecast_context_silver: pl.DataFrame,
) -> pl.DataFrame:
    """Gold offline DT trajectory rows from simulated dispatch transitions."""

    enriched_transition_frame = _attach_forecast_context_to_transitions(
        transition_frame=simulated_trade_training_frame,
        forecast_context_frame=decision_transformer_forecast_context_silver,
    )
    frame = build_decision_transformer_trajectory_frame(enriched_transition_frame)
    get_simulated_trade_store().upsert_decision_transformer_trajectory_frame(frame)
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "episode_count": frame.select("episode_id").n_unique() if frame.height else 0,
            "forecast_context_rows": decision_transformer_forecast_context_silver.height,
            "scope": "offline_dt_training_trajectory_not_live_policy",
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DECISION_TRANSFORMER,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="decision_transformer",
        elt_stage="publish",
        ml_stage="pilot",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def decision_transformer_policy_preview_frame(
    context,
    decision_transformer_trajectory_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Gold DT policy preview rows after strict action projection.

    This is an offline policy-evaluation read model, not market execution.
    """

    frame = build_decision_transformer_policy_preview_frame(decision_transformer_trajectory_frame)
    get_simulated_trade_store().upsert_decision_transformer_policy_preview_frame(frame)
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
            "constraint_violation_count": int(frame.select("constraint_violation").sum().item()) if frame.height else 0,
            "scope": "offline_dt_policy_preview_not_market_execution",
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_PAPER_TRADING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="paper_trading",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
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
    decision_transformer_forecast_context_silver,
    simulated_trade_training_frame,
    decision_transformer_trajectory_frame,
    decision_transformer_policy_preview_frame,
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


def _build_decision_transformer_forecast_context(
    *,
    nbeatsx_price_forecast: pl.DataFrame,
    tft_price_forecast: pl.DataFrame,
) -> pl.DataFrame:
    nbeatsx_context = _nbeatsx_forecast_context_frame(nbeatsx_price_forecast)
    tft_context = _tft_forecast_context_frame(tft_price_forecast)
    if nbeatsx_context.is_empty() and tft_context.is_empty():
        return _empty_decision_transformer_forecast_context()
    if nbeatsx_context.is_empty():
        joined = tft_context.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("nbeatsx_forecast_uah_mwh")
        )
    elif tft_context.is_empty():
        joined = nbeatsx_context.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias("tft_forecast_p50_uah_mwh"),
                pl.lit(None, dtype=pl.Float64).alias("forecast_uncertainty_uah_mwh"),
            ]
        )
    else:
        joined = nbeatsx_context.join(tft_context, on="interval_start", how="full", coalesce=True)
    return (
        joined
        .with_columns(
            [
                (pl.col("tft_forecast_p50_uah_mwh") - pl.col("nbeatsx_forecast_uah_mwh"))
                .alias("forecast_spread_uah_mwh"),
                pl.lit("compact_nbeatsx_tft_silver").alias("forecast_context_source"),
            ]
        )
        .select(
            [
                "interval_start",
                "nbeatsx_forecast_uah_mwh",
                "tft_forecast_p50_uah_mwh",
                "forecast_uncertainty_uah_mwh",
                "forecast_spread_uah_mwh",
                "forecast_context_source",
            ]
        )
        .sort("interval_start")
    )


def _nbeatsx_forecast_context_frame(forecast_frame: pl.DataFrame) -> pl.DataFrame:
    if forecast_frame.is_empty() or not {"forecast_timestamp", "predicted_price_uah_mwh"}.issubset(forecast_frame.columns):
        return pl.DataFrame(schema={"interval_start": pl.Datetime, "nbeatsx_forecast_uah_mwh": pl.Float64})
    return (
        forecast_frame
        .select(
            [
                pl.col("forecast_timestamp").alias("interval_start"),
                pl.col("predicted_price_uah_mwh").cast(pl.Float64).alias("nbeatsx_forecast_uah_mwh"),
            ]
        )
        .drop_nulls(subset=["interval_start"])
        .unique(subset=["interval_start"], keep="last")
        .sort("interval_start")
    )


def _tft_forecast_context_frame(forecast_frame: pl.DataFrame) -> pl.DataFrame:
    required_columns = {"forecast_timestamp", "predicted_price_p50_uah_mwh"}
    if forecast_frame.is_empty() or not required_columns.issubset(forecast_frame.columns):
        return pl.DataFrame(
            schema={
                "interval_start": pl.Datetime,
                "tft_forecast_p50_uah_mwh": pl.Float64,
                "forecast_uncertainty_uah_mwh": pl.Float64,
            }
        )
    uncertainty_expression = (
        pl.col("predicted_price_p90_uah_mwh") - pl.col("predicted_price_p10_uah_mwh")
        if {"predicted_price_p10_uah_mwh", "predicted_price_p90_uah_mwh"}.issubset(forecast_frame.columns)
        else pl.lit(None, dtype=pl.Float64)
    )
    return (
        forecast_frame
        .select(
            [
                pl.col("forecast_timestamp").alias("interval_start"),
                pl.col("predicted_price_p50_uah_mwh").cast(pl.Float64).alias("tft_forecast_p50_uah_mwh"),
                uncertainty_expression.cast(pl.Float64).alias("forecast_uncertainty_uah_mwh"),
            ]
        )
        .drop_nulls(subset=["interval_start"])
        .unique(subset=["interval_start"], keep="last")
        .sort("interval_start")
    )


def _attach_forecast_context_to_transitions(
    *,
    transition_frame: pl.DataFrame,
    forecast_context_frame: pl.DataFrame,
) -> pl.DataFrame:
    if transition_frame.is_empty() or forecast_context_frame.is_empty():
        return transition_frame
    context_columns = [
        "nbeatsx_forecast_uah_mwh",
        "tft_forecast_p50_uah_mwh",
        "forecast_uncertainty_uah_mwh",
        "forecast_spread_uah_mwh",
        "forecast_context_source",
    ]
    return (
        transition_frame
        .drop([column for column in context_columns if column in transition_frame.columns])
        .join(
            forecast_context_frame.select(["interval_start", *context_columns]),
            on="interval_start",
            how="left",
        )
    )


def _empty_decision_transformer_forecast_context() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "interval_start": pl.Datetime,
            "nbeatsx_forecast_uah_mwh": pl.Float64,
            "tft_forecast_p50_uah_mwh": pl.Float64,
            "forecast_uncertainty_uah_mwh": pl.Float64,
            "forecast_spread_uah_mwh": pl.Float64,
            "forecast_context_source": pl.Utf8,
        }
    )
