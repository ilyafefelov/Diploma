from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import (
    build_synthetic_market_price_history,
)
from smart_arbitrage.assets.gold.baseline_solver import (
    DEFAULT_PRICE_COLUMN,
    DEFAULT_TIMESTAMP_COLUMN,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.assets.gold.forecast_strategy import (
    FORECAST_STRATEGY_GOLD_ASSETS,
    ForecastStrategyComparisonAssetConfig,
    forecast_strategy_comparison_frame,
)
from smart_arbitrage.defs import defs
from smart_arbitrage.resources.strategy_evaluation_store import (
    InMemoryStrategyEvaluationStore,
)


def _price_history() -> pl.DataFrame:
    return build_synthetic_market_price_history(
        history_hours=15 * 24,
        forecast_hours=24,
        now=datetime(2026, 5, 4, 12, 0),
    )


def _anchor_timestamp(price_history: pl.DataFrame) -> datetime:
    latest_timestamp = (
        price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1)
    )
    if not isinstance(latest_timestamp, datetime):
        raise TypeError("timestamp column must contain datetime values.")
    return latest_timestamp - timedelta(hours=24)


def _strict_forecast_frame(price_history: pl.DataFrame) -> pl.DataFrame:
    anchor_timestamp = _anchor_timestamp(price_history)
    historical_prices = price_history.filter(
        pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp
    )
    forecast = HourlyDamBaselineSolver().build_forecast(
        historical_prices,
        anchor_timestamp=anchor_timestamp,
    )
    return pl.DataFrame(
        {
            "forecast_timestamp": [point.forecast_timestamp for point in forecast],
            "source_timestamp": [point.source_timestamp for point in forecast],
            "predicted_price_uah_mwh": [
                point.predicted_price_uah_mwh for point in forecast
            ],
        }
    )


def _nbeatsx_forecast_frame(price_history: pl.DataFrame) -> pl.DataFrame:
    anchor_timestamp = _anchor_timestamp(price_history)
    actual_future = price_history.filter(
        pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp
    ).head(24)
    return pl.DataFrame(
        {
            "forecast_timestamp": actual_future.select(DEFAULT_TIMESTAMP_COLUMN)
            .to_series()
            .to_list(),
            "model_name": ["nbeatsx_silver_v0" for _ in range(actual_future.height)],
            "predicted_price_uah_mwh": actual_future.select(DEFAULT_PRICE_COLUMN)
            .to_series()
            .to_list(),
        }
    )


def _tft_forecast_frame(price_history: pl.DataFrame) -> pl.DataFrame:
    anchor_timestamp = _anchor_timestamp(price_history)
    actual_future = price_history.filter(
        pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp
    ).head(24)
    prices = [
        float(value)
        for value in actual_future.select(DEFAULT_PRICE_COLUMN).to_series().to_list()
    ]
    return pl.DataFrame(
        {
            "forecast_timestamp": actual_future.select(DEFAULT_TIMESTAMP_COLUMN)
            .to_series()
            .to_list(),
            "model_name": ["tft_silver_v0" for _ in range(actual_future.height)],
            "predicted_price_p50_uah_mwh": [price + 75.0 for price in prices],
        }
    )


def test_forecast_strategy_comparison_asset_persists_gold_frame(monkeypatch) -> None:
    store = InMemoryStrategyEvaluationStore()
    price_history = _price_history()
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.forecast_strategy.get_strategy_evaluation_store",
        lambda: store,
    )

    frame = forecast_strategy_comparison_frame(
        None,
        ForecastStrategyComparisonAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory"
        ),
        price_history,
        _strict_forecast_frame(price_history),
        _nbeatsx_forecast_frame(price_history),
        _tft_forecast_frame(price_history),
    )

    assert frame.height == 3
    assert store.evaluation_frame.height == 3
    assert set(frame.select("forecast_model_name").to_series().to_list()) == {
        "strict_similar_day",
        "nbeatsx_silver_v0",
        "tft_silver_v0",
    }
    assert frame.select("tenant_id").to_series().unique().to_list() == [
        "client_003_dnipro_factory"
    ]


def test_forecast_strategy_gold_asset_is_registered() -> None:
    asset_keys = {asset.key.to_user_string() for asset in FORECAST_STRATEGY_GOLD_ASSETS}
    registered_asset_keys = {asset.key.to_user_string() for asset in defs.assets or []}

    assert {"forecast_strategy_comparison_frame"}.issubset(asset_keys)
    assert asset_keys.issubset(registered_asset_keys)
