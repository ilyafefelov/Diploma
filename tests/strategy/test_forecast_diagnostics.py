from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
)


def test_forecast_diagnostics_are_persisted_beside_regret_without_changing_ranking() -> None:
    price_history = build_synthetic_market_price_history(
        history_hours=15 * 24,
        forecast_hours=24,
        now=datetime(2026, 5, 4, 12, 0),
    )
    anchor_timestamp = price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-25)
    future = price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp).head(24)
    actual_prices = [float(value) for value in future.select(DEFAULT_PRICE_COLUMN).to_series().to_list()]
    forecast_frame = pl.DataFrame(
        {
            "forecast_timestamp": future.select(DEFAULT_TIMESTAMP_COLUMN).to_series().to_list(),
            "predicted_price_p10_uah_mwh": [price - 150.0 for price in actual_prices],
            "predicted_price_p50_uah_mwh": [price + 100.0 for price in actual_prices],
            "predicted_price_p90_uah_mwh": [price + 250.0 for price in actual_prices],
        }
    )

    evaluation = evaluate_forecast_candidates_against_oracle(
        price_history=price_history,
        tenant_id="client_003_dnipro_factory",
        battery_metrics=BatteryPhysicalMetrics(
            capacity_mwh=0.5,
            max_power_mw=0.25,
            round_trip_efficiency=0.92,
            degradation_cost_per_cycle_uah=120.0,
        ),
        starting_soc_fraction=0.5,
        starting_soc_source="tenant_default",
        anchor_timestamp=anchor_timestamp,
        candidates=[
            ForecastCandidate(
                model_name="tft_silver_v0",
                forecast_frame=forecast_frame,
                point_prediction_column="predicted_price_p50_uah_mwh",
            )
        ],
    )

    payload = evaluation.row(0, named=True)["evaluation_payload"]
    diagnostics = payload["forecast_diagnostics"]
    assert diagnostics["mae_uah_mwh"] == pytest.approx(100.0)
    assert diagnostics["rmse_uah_mwh"] == pytest.approx(100.0)
    assert diagnostics["pinball_loss_p50_uah_mwh"] == pytest.approx(50.0)
    assert 0.0 <= diagnostics["directional_accuracy"] <= 1.0
    assert 0.0 <= diagnostics["top_k_price_recall"] <= 1.0
    assert evaluation.select("rank_by_regret").to_series().to_list() == [1]


def test_forecast_diagnostics_skip_missing_quantile_values() -> None:
    price_history = build_synthetic_market_price_history(
        history_hours=15 * 24,
        forecast_hours=24,
        now=datetime(2026, 5, 4, 12, 0),
    )
    anchor_timestamp = price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-25)
    future = price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp).head(24)
    actual_prices = [
        float(value)
        for value in future.select(DEFAULT_PRICE_COLUMN).to_series().to_list()
    ]
    forecast_frame = pl.DataFrame(
        {
            "forecast_timestamp": future.select(DEFAULT_TIMESTAMP_COLUMN)
            .to_series()
            .to_list(),
            "predicted_price_p10_uah_mwh": [None for _ in actual_prices],
            "predicted_price_p50_uah_mwh": [price + 100.0 for price in actual_prices],
            "predicted_price_p90_uah_mwh": [None for _ in actual_prices],
        }
    )

    evaluation = evaluate_forecast_candidates_against_oracle(
        price_history=price_history,
        tenant_id="client_003_dnipro_factory",
        battery_metrics=BatteryPhysicalMetrics(
            capacity_mwh=0.5,
            max_power_mw=0.25,
            round_trip_efficiency=0.92,
            degradation_cost_per_cycle_uah=120.0,
        ),
        starting_soc_fraction=0.5,
        starting_soc_source="tenant_default",
        anchor_timestamp=anchor_timestamp,
        candidates=[
            ForecastCandidate(
                model_name="tft_official_v0",
                forecast_frame=forecast_frame,
                point_prediction_column="predicted_price_p50_uah_mwh",
            )
        ],
    )

    diagnostics = evaluation.row(0, named=True)["evaluation_payload"][
        "forecast_diagnostics"
    ]
    assert diagnostics["pinball_loss_p50_uah_mwh"] == pytest.approx(50.0)
    assert "pinball_loss_p10_uah_mwh" not in diagnostics
    assert "pinball_loss_p90_uah_mwh" not in diagnostics


def test_forecast_diagnostics_do_not_depend_on_prices_after_scored_horizon() -> None:
    timestamps = [datetime(2026, 1, 1) + timedelta(hours=hour) for hour in range(220)]
    prices = [1000.0 + float(hour % 24) * 20.0 for hour in range(220)]
    price_history = pl.DataFrame(
        {
            DEFAULT_TIMESTAMP_COLUMN: timestamps,
            DEFAULT_PRICE_COLUMN: prices,
        }
    )
    anchor_timestamp = timestamps[190]
    forecast_timestamps = [anchor_timestamp + timedelta(hours=step + 1) for step in range(24)]
    forecast_frame = pl.DataFrame(
        {
            "forecast_timestamp": forecast_timestamps,
            "predicted_price_uah_mwh": [1200.0 for _ in forecast_timestamps],
        }
    )

    def _diagnostics(frame: pl.DataFrame) -> dict[str, float]:
        evaluation = evaluate_forecast_candidates_against_oracle(
            price_history=frame,
            tenant_id="client_003_dnipro_factory",
            battery_metrics=BatteryPhysicalMetrics(
                capacity_mwh=0.5,
                max_power_mw=0.25,
                round_trip_efficiency=0.92,
                degradation_cost_per_cycle_uah=120.0,
            ),
            starting_soc_fraction=0.5,
            starting_soc_source="tenant_default",
            anchor_timestamp=anchor_timestamp,
            candidates=[
                ForecastCandidate(
                    model_name="strict_similar_day",
                    forecast_frame=forecast_frame,
                    point_prediction_column="predicted_price_uah_mwh",
                )
            ],
        )
        return evaluation.row(0, named=True)["evaluation_payload"]["forecast_diagnostics"]

    control = _diagnostics(price_history)
    mutated = _diagnostics(
        price_history.with_columns(
            pl.when(pl.col(DEFAULT_TIMESTAMP_COLUMN) > forecast_timestamps[-1])
            .then(pl.col(DEFAULT_PRICE_COLUMN) + 99999.0)
            .otherwise(pl.col(DEFAULT_PRICE_COLUMN))
            .alias(DEFAULT_PRICE_COLUMN)
        )
    )

    assert mutated == control
