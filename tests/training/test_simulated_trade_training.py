from datetime import datetime

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN
from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.gatekeeper.schemas import MARKET_PRICE_CAPS_UAH_PER_MWH
from smart_arbitrage.training.simulated_trades import (
    SimulatedTradeTrainingConfig,
    build_simulated_trade_training_data,
)


def test_simulated_trade_training_data_is_reproducible_and_marks_simulated_trades() -> None:
    price_history = build_synthetic_market_price_history(
        history_hours=15 * 24,
        forecast_hours=24,
        now=datetime(2026, 5, 4, 12, 0),
    )
    config = SimulatedTradeTrainingConfig(
        max_anchors_per_tenant=2,
        scenarios_per_anchor=2,
        horizon_hours=6,
        seed=123,
    )

    first_result = build_simulated_trade_training_data(
        price_history,
        tenant_ids=["client_003_dnipro_factory"],
        config=config,
    )
    second_result = build_simulated_trade_training_data(
        price_history,
        tenant_ids=["client_003_dnipro_factory"],
        config=config,
    )

    assert first_result.transition_frame.equals(second_result.transition_frame)
    assert first_result.episode_frame.equals(second_result.episode_frame)
    assert first_result.episode_frame.height == 4
    assert first_result.transition_frame.height == 24
    assert set(first_result.transition_frame.select("market_venue").to_series().to_list()) == {"DAM"}
    assert set(first_result.transition_frame.select("cleared_trade_provenance").to_series().to_list()) == {"simulated"}
    assert first_result.transition_frame.select("regret_uah").min().item() >= 0.0


def test_simulated_trade_training_caps_dam_clearing_prices() -> None:
    price_history = build_synthetic_market_price_history(
        history_hours=15 * 24,
        forecast_hours=24,
        now=datetime(2026, 5, 4, 12, 0),
    ).with_columns(
        (pl.col(DEFAULT_PRICE_COLUMN) + 20_000.0).alias(DEFAULT_PRICE_COLUMN)
    )
    config = SimulatedTradeTrainingConfig(
        max_anchors_per_tenant=1,
        scenarios_per_anchor=1,
        horizon_hours=3,
        seed=123,
    )

    result = build_simulated_trade_training_data(
        price_history,
        tenant_ids=["client_003_dnipro_factory"],
        config=config,
    )

    dam_cap = MARKET_PRICE_CAPS_UAH_PER_MWH["DAM"]
    cleared_trades = result.transition_frame.select("cleared_trade").to_series().to_list()
    assert result.transition_frame.select("market_price_uah_mwh").max().item() <= dam_cap
    assert all(trade["market_clearing_price_uah_mwh"] <= dam_cap for trade in cleared_trades)
