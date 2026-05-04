from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.assets.bronze.market_weather import (
    build_synthetic_market_price_history,
)
from smart_arbitrage.assets.gold.baseline_solver import (
    DEFAULT_PRICE_COLUMN,
    DEFAULT_TIMESTAMP_COLUMN,
    BaselineForecastPoint,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
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


def _strict_forecast_frame(
    price_history: pl.DataFrame, anchor_timestamp: datetime
) -> pl.DataFrame:
    historical_prices = price_history.filter(
        pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp
    )
    forecast = HourlyDamBaselineSolver().build_forecast(
        historical_prices,
        anchor_timestamp=anchor_timestamp,
    )
    return _forecast_frame("strict_similar_day", forecast)


def _actual_forecast_frame(
    price_history: pl.DataFrame, anchor_timestamp: datetime
) -> pl.DataFrame:
    actual_future = (
        price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp)
        .head(24)
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
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


def _tft_forecast_frame(
    price_history: pl.DataFrame, anchor_timestamp: datetime
) -> pl.DataFrame:
    actual_future = (
        price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp)
        .head(24)
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
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


def test_forecast_strategy_evaluation_compares_silver_forecasts_against_oracle() -> (
    None
):
    price_history = _price_history()
    anchor_timestamp = _anchor_timestamp(price_history)
    metrics = BatteryPhysicalMetrics(
        capacity_mwh=0.5,
        max_power_mw=0.25,
        round_trip_efficiency=0.92,
        degradation_cost_per_cycle_uah=120.0,
    )

    evaluation = evaluate_forecast_candidates_against_oracle(
        price_history=price_history,
        tenant_id="client_003_dnipro_factory",
        battery_metrics=metrics,
        starting_soc_fraction=0.5,
        starting_soc_source="tenant_default",
        anchor_timestamp=anchor_timestamp,
        candidates=[
            ForecastCandidate(
                model_name="strict_similar_day",
                forecast_frame=_strict_forecast_frame(price_history, anchor_timestamp),
                point_prediction_column="predicted_price_uah_mwh",
            ),
            ForecastCandidate(
                model_name="nbeatsx_silver_v0",
                forecast_frame=_actual_forecast_frame(price_history, anchor_timestamp),
                point_prediction_column="predicted_price_uah_mwh",
            ),
            ForecastCandidate(
                model_name="tft_silver_v0",
                forecast_frame=_tft_forecast_frame(price_history, anchor_timestamp),
                point_prediction_column="predicted_price_p50_uah_mwh",
            ),
        ],
    )

    assert evaluation.height == 3
    assert set(evaluation.select("forecast_model_name").to_series().to_list()) == {
        "strict_similar_day",
        "nbeatsx_silver_v0",
        "tft_silver_v0",
    }
    assert set(evaluation.select("strategy_kind").to_series().to_list()) == {
        "forecast_driven_lp"
    }
    assert set(evaluation.select("market_venue").to_series().to_list()) == {"DAM"}
    assert evaluation.select("oracle_value_uah").n_unique() == 1
    assert evaluation.select("regret_uah").min().item() >= -1e-9
    assert evaluation.filter(pl.col("forecast_model_name") == "nbeatsx_silver_v0").row(
        0, named=True
    )["regret_uah"] == pytest.approx(0.0, abs=1e-5)
    assert evaluation.select("rank_by_regret").min().item() == 1
    assert evaluation.select("total_degradation_penalty_uah").min().item() >= 0.0
    assert "evaluation_payload" in evaluation.columns


def _forecast_frame(
    model_name: str, forecast: list[BaselineForecastPoint]
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "forecast_timestamp": [point.forecast_timestamp for point in forecast],
            "model_name": [model_name for _ in forecast],
            "source_timestamp": [point.source_timestamp for point in forecast],
            "predicted_price_uah_mwh": [
                point.predicted_price_uah_mwh for point in forecast
            ],
        }
    )
