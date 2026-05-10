from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from smart_arbitrage.resources.forecast_store import InMemoryForecastStore, NullForecastStore


def _forecast_frame(*, start: datetime, values: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "forecast_timestamp": [
                start + timedelta(hours=step_index)
                for step_index in range(len(values))
            ],
            "predicted_price_uah_mwh": values,
            "predicted_price_p50_uah_mwh": values,
            "predicted_price_p10_uah_mwh": [value - 100.0 for value in values],
            "predicted_price_p90_uah_mwh": [value + 100.0 for value in values],
        }
    )


def test_in_memory_forecast_store_reads_latest_run_per_model() -> None:
    store = InMemoryForecastStore()
    start = datetime(2026, 5, 4, 18, tzinfo=UTC)

    store.upsert_forecast_run(
        model_name="nbeatsx_official_v0",
        forecast_frame=_forecast_frame(start=start, values=[1000.0, 1100.0]),
        point_prediction_column="predicted_price_uah_mwh",
    )
    store.upsert_forecast_run(
        model_name="nbeatsx_official_v0",
        forecast_frame=_forecast_frame(start=start, values=[2100.0, 2200.0]),
        point_prediction_column="predicted_price_uah_mwh",
    )
    store.upsert_forecast_run(
        model_name="tft_official_v0",
        forecast_frame=_forecast_frame(start=start, values=[3100.0, 3200.0]),
        point_prediction_column="predicted_price_p50_uah_mwh",
    )

    latest_frame = store.latest_forecast_observation_frame(
        model_names=["nbeatsx_official_v0", "tft_official_v0"],
        limit_per_model=2,
    )

    assert latest_frame.height == 4
    nbeatsx_values = (
        latest_frame
        .filter(pl.col("model_name") == "nbeatsx_official_v0")
        .sort("forecast_timestamp")
        .select("predicted_price_uah_mwh")
        .to_series()
        .to_list()
    )
    assert nbeatsx_values == [2100.0, 2200.0]
    assert latest_frame.select("prediction_payload").drop_nulls().height == 4


def test_null_forecast_store_returns_empty_latest_observations() -> None:
    latest_frame = NullForecastStore().latest_forecast_observation_frame(
        model_names=["nbeatsx_official_v0"],
    )

    assert latest_frame.height == 0
    assert {
        "run_id",
        "model_name",
        "generated_at",
        "forecast_timestamp",
        "predicted_price_uah_mwh",
        "prediction_payload",
    }.issubset(set(latest_frame.columns))
