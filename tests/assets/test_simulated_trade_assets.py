from datetime import datetime
from typing import Any, cast

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.assets.gold.simulated_trades import (
    SIMULATED_TRADE_TRAINING_ASSETS,
    SimulatedTradeTrainingAssetConfig,
    decision_transformer_forecast_context_silver,
    decision_transformer_trajectory_frame,
    decision_transformer_policy_preview_frame,
    simulated_live_trading_frame,
    simulated_trade_silver_feature_frame,
    simulated_trade_training_frame,
)
from smart_arbitrage.defs import defs
from smart_arbitrage.resources.simulated_trade_store import InMemorySimulatedTradeStore


def test_simulated_trade_training_asset_persists_transition_frame(monkeypatch) -> None:
    store = InMemorySimulatedTradeStore()
    price_history = build_synthetic_market_price_history(
        history_hours=15 * 24,
        forecast_hours=24,
        now=datetime(2026, 5, 4, 12, 0),
    )
    monkeypatch.setattr("smart_arbitrage.assets.gold.simulated_trades.get_simulated_trade_store", lambda: store)
    silver_frame = cast(pl.DataFrame, simulated_trade_silver_feature_frame(None, price_history))

    transition_frame = cast(
        pl.DataFrame,
        simulated_trade_training_frame(
            None,
            SimulatedTradeTrainingAssetConfig(
                max_anchors_per_tenant=1,
                scenarios_per_anchor=1,
                horizon_hours=6,
                tenant_ids_csv="client_003_dnipro_factory",
            ),
            silver_frame,
        ),
    )

    assert transition_frame.height == 6
    assert store.episode_frame.height == 1
    assert store.transition_frame.height == 6

    forecast_context_frame = cast(
        pl.DataFrame,
        decision_transformer_forecast_context_silver(
            None,
            _nbeatsx_forecast_for_transition_frame(transition_frame),
            _tft_forecast_for_transition_frame(transition_frame),
        ),
    )
    trajectory_frame = cast(
        pl.DataFrame,
        decision_transformer_trajectory_frame(
            None,
            transition_frame,
            forecast_context_frame,
        ),
    )
    paper_frame = cast(pl.DataFrame, simulated_live_trading_frame(None, transition_frame))

    assert trajectory_frame.height == 6
    assert store.decision_transformer_trajectory_frame.height == 6
    assert "return_to_go_uah" in trajectory_frame.columns
    assert trajectory_frame.select("state_nbeatsx_forecast_uah_mwh").to_series().to_list() == [
        4100.0 + index for index in range(6)
    ]
    assert trajectory_frame.select("state_tft_forecast_uah_mwh").to_series().to_list() == [
        4300.0 + index for index in range(6)
    ]

    policy_preview_frame = cast(
        pl.DataFrame,
        decision_transformer_policy_preview_frame(None, trajectory_frame),
    )

    assert policy_preview_frame.height == 6
    assert store.decision_transformer_policy_preview_frame.height == 6
    assert policy_preview_frame.select("constraint_violation").to_series().to_list() == [
        False for _ in range(6)
    ]
    assert policy_preview_frame.select("readiness_status").to_series().unique().to_list() == [
        "ready_for_operator_preview"
    ]

    assert paper_frame.height == 6
    assert store.simulated_live_trading_frame.height == 6
    assert paper_frame.select("paper_trade_provenance").to_series().unique().to_list() == ["simulated"]


def test_simulated_trade_assets_use_medallion_tags_and_silver_bridge() -> None:
    tags_by_key = {
        asset_key.to_user_string(): tags
        for asset in SIMULATED_TRADE_TRAINING_ASSETS
        for asset_key, tags in asset.tags_by_key.items()
    }
    groups_by_key = {
        asset_key.to_user_string(): group
        for asset in SIMULATED_TRADE_TRAINING_ASSETS
        for asset_key, group in asset.group_names_by_key.items()
    }
    training_deps = {asset_key.to_user_string() for asset_key in simulated_trade_training_frame.dependency_keys}
    forecast_context_deps = {
        asset_key.to_user_string()
        for asset_key in decision_transformer_forecast_context_silver.dependency_keys
    }
    trajectory_deps = {
        asset_key.to_user_string()
        for asset_key in decision_transformer_trajectory_frame.dependency_keys
    }

    assert groups_by_key["simulated_trade_silver_feature_frame"] == "silver_simulated_training"
    assert tags_by_key["simulated_trade_silver_feature_frame"]["medallion"] == "silver"
    assert groups_by_key["decision_transformer_forecast_context_silver"] == "silver_decision_transformer"
    assert tags_by_key["decision_transformer_forecast_context_silver"]["medallion"] == "silver"
    assert groups_by_key["simulated_trade_training_frame"] == "gold_simulated_training"
    assert tags_by_key["simulated_trade_training_frame"]["medallion"] == "gold"
    assert "simulated_trade_silver_feature_frame" in training_deps
    assert "dam_price_history" not in training_deps
    assert forecast_context_deps == {"nbeatsx_price_forecast", "tft_price_forecast"}
    assert "decision_transformer_forecast_context_silver" in trajectory_deps


def test_simulated_trade_training_asset_is_registered() -> None:
    asset_keys = {asset.key.to_user_string() for asset in SIMULATED_TRADE_TRAINING_ASSETS}
    registered_asset_keys = {cast(Any, asset).key.to_user_string() for asset in defs.assets or []}

    assert {
        "simulated_trade_silver_feature_frame",
        "decision_transformer_forecast_context_silver",
        "simulated_trade_training_frame",
        "decision_transformer_trajectory_frame",
        "decision_transformer_policy_preview_frame",
        "simulated_live_trading_frame",
    }.issubset(asset_keys)
    assert asset_keys.issubset(registered_asset_keys)


def _nbeatsx_forecast_for_transition_frame(transition_frame: pl.DataFrame) -> pl.DataFrame:
    timestamps = transition_frame.select("interval_start").to_series().to_list()
    return transition_frame.select("interval_start").with_columns(
        [
            pl.Series("forecast_timestamp", timestamps),
            pl.Series("model_name", ["nbeatsx_silver_v0" for _ in timestamps]),
            pl.Series("predicted_price_uah_mwh", [4100.0 + index for index, _ in enumerate(timestamps)]),
        ]
    ).select(["forecast_timestamp", "model_name", "predicted_price_uah_mwh"])


def _tft_forecast_for_transition_frame(transition_frame: pl.DataFrame) -> pl.DataFrame:
    timestamps = transition_frame.select("interval_start").to_series().to_list()
    return transition_frame.select("interval_start").with_columns(
        [
            pl.Series("forecast_timestamp", timestamps),
            pl.Series("model_name", ["tft_silver_v0" for _ in timestamps]),
            pl.Series("predicted_price_p10_uah_mwh", [3900.0 + index for index, _ in enumerate(timestamps)]),
            pl.Series("predicted_price_p50_uah_mwh", [4300.0 + index for index, _ in enumerate(timestamps)]),
            pl.Series("predicted_price_p90_uah_mwh", [4700.0 + index for index, _ in enumerate(timestamps)]),
        ]
    ).select([
        "forecast_timestamp",
        "model_name",
        "predicted_price_p10_uah_mwh",
        "predicted_price_p50_uah_mwh",
        "predicted_price_p90_uah_mwh",
    ])
