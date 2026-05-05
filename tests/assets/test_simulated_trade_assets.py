from datetime import datetime

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.assets.gold.simulated_trades import (
    SIMULATED_TRADE_TRAINING_ASSETS,
    SimulatedTradeTrainingAssetConfig,
    decision_transformer_trajectory_frame,
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
    silver_frame = simulated_trade_silver_feature_frame(None, price_history)

    transition_frame = simulated_trade_training_frame(
        None,
        SimulatedTradeTrainingAssetConfig(
            max_anchors_per_tenant=1,
            scenarios_per_anchor=1,
            horizon_hours=6,
            tenant_ids_csv="client_003_dnipro_factory",
        ),
        silver_frame,
    )

    assert transition_frame.height == 6
    assert store.episode_frame.height == 1
    assert store.transition_frame.height == 6

    trajectory_frame = decision_transformer_trajectory_frame(None, transition_frame)
    paper_frame = simulated_live_trading_frame(None, transition_frame)

    assert trajectory_frame.height == 6
    assert store.decision_transformer_trajectory_frame.height == 6
    assert "return_to_go_uah" in trajectory_frame.columns
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

    assert groups_by_key["simulated_trade_silver_feature_frame"] == "silver"
    assert tags_by_key["simulated_trade_silver_feature_frame"]["medallion"] == "silver"
    assert groups_by_key["simulated_trade_training_frame"] == "gold"
    assert tags_by_key["simulated_trade_training_frame"]["medallion"] == "gold"
    assert "simulated_trade_silver_feature_frame" in training_deps
    assert "dam_price_history" not in training_deps


def test_simulated_trade_training_asset_is_registered() -> None:
    asset_keys = {asset.key.to_user_string() for asset in SIMULATED_TRADE_TRAINING_ASSETS}
    registered_asset_keys = {asset.key.to_user_string() for asset in defs.assets or []}

    assert {
        "simulated_trade_silver_feature_frame",
        "simulated_trade_training_frame",
        "decision_transformer_trajectory_frame",
        "simulated_live_trading_frame",
    }.issubset(asset_keys)
    assert asset_keys.issubset(registered_asset_keys)
