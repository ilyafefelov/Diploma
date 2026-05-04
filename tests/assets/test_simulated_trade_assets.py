from datetime import datetime

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.assets.gold.simulated_trades import (
    SIMULATED_TRADE_TRAINING_ASSETS,
    SimulatedTradeTrainingAssetConfig,
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

    transition_frame = simulated_trade_training_frame(
        None,
        SimulatedTradeTrainingAssetConfig(
            max_anchors_per_tenant=1,
            scenarios_per_anchor=1,
            horizon_hours=6,
            tenant_ids_csv="client_003_dnipro_factory",
        ),
        price_history,
    )

    assert transition_frame.height == 6
    assert store.episode_frame.height == 1
    assert store.transition_frame.height == 6


def test_simulated_trade_training_asset_is_registered() -> None:
    asset_keys = {asset.key.to_user_string() for asset in SIMULATED_TRADE_TRAINING_ASSETS}
    registered_asset_keys = {asset.key.to_user_string() for asset in defs.assets or []}

    assert {"simulated_trade_training_frame"}.issubset(asset_keys)
    assert asset_keys.issubset(registered_asset_keys)
