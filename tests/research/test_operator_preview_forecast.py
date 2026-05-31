from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.research.operator_preview_forecast import (
    OperatorPreviewForecastBuilders,
    materialize_operator_preview_forecast_runs,
    operator_preview_forecast_model_names,
)
from smart_arbitrage.resources.forecast_store import InMemoryForecastStore
from smart_arbitrage.resources.market_data_store import (
    InMemoryMarketDataStore,
    market_price_observations_from_frame,
)


def test_operator_preview_forecast_model_names_are_venue_specific() -> None:
    assert operator_preview_forecast_model_names("DAM") == (
        "nbeatsx_official_v0",
        "tft_official_v0",
    )
    assert operator_preview_forecast_model_names("IDM") == (
        "nbeatsx_official_idm_v0",
        "tft_official_idm_v0",
    )


def test_materialize_operator_preview_forecasts_persists_idm_model_names() -> None:
    market_store = InMemoryMarketDataStore()
    market_store.upsert_market_prices(
        market_price_observations_from_frame(
            _source_backed_market_frame(
                market_venue="IDM",
                start_timestamp=datetime(2026, 5, 1),
                hours=31 * 24,
            )
        )
    )
    forecast_store = InMemoryForecastStore()

    result = materialize_operator_preview_forecast_runs(
        market_data_store=market_store,
        forecast_store=forecast_store,
        tenant_id="client_003_dnipro_factory",
        market_venue="IDM",
        forecast_start=datetime(2026, 6, 1),
        horizon_hours=72,
        builders=OperatorPreviewForecastBuilders(
            nbeatsx_builder=_fake_nbeatsx_forecast,
            tft_builder=_fake_tft_forecast,
        ),
    )

    assert set(result.run_ids) == {"nbeatsx_official_idm_v0", "tft_official_idm_v0"}
    assert result.source_history_rows == 31 * 24
    assert result.forecast_start == datetime(2026, 6, 1)
    assert result.forecast_end == datetime(2026, 6, 3, 23)
    latest_frame = forecast_store.latest_forecast_observation_frame(
        model_names=["nbeatsx_official_idm_v0", "tft_official_idm_v0"],
        limit_per_model=72,
    )
    assert latest_frame.height == 144
    assert set(latest_frame.select("model_name").to_series().to_list()) == {
        "nbeatsx_official_idm_v0",
        "tft_official_idm_v0",
    }
    assert latest_frame.select("forecast_timestamp").min().item() == datetime(2026, 6, 1)
    assert latest_frame.select("forecast_timestamp").max().item() == datetime(2026, 6, 3, 23)


def test_materialize_operator_preview_forecasts_refuses_synthetic_history() -> None:
    market_store = InMemoryMarketDataStore()
    synthetic_frame = _source_backed_market_frame(
        market_venue="DAM",
        start_timestamp=datetime(2026, 5, 1),
        hours=31 * 24,
    ).with_columns(
        [
            pl.lit("SYNTHETIC").alias("source"),
            pl.lit("synthetic").alias("source_kind"),
            pl.lit("synthetic://demo").alias("source_url"),
        ]
    )
    market_store.upsert_market_prices(market_price_observations_from_frame(synthetic_frame))

    with pytest.raises(ValueError, match="source-backed observed OREE"):
        materialize_operator_preview_forecast_runs(
            market_data_store=market_store,
            forecast_store=InMemoryForecastStore(),
            tenant_id="client_003_dnipro_factory",
            market_venue="DAM",
            forecast_start=datetime(2026, 6, 1),
            horizon_hours=72,
            builders=OperatorPreviewForecastBuilders(
                nbeatsx_builder=_fake_nbeatsx_forecast,
                tft_builder=_fake_tft_forecast,
            ),
        )


def test_materialize_operator_preview_forecasts_requires_contiguous_source_history() -> None:
    market_store = InMemoryMarketDataStore()
    market_store.upsert_market_prices(
        market_price_observations_from_frame(
            _source_backed_market_frame(
                market_venue="DAM",
                start_timestamp=datetime(2026, 5, 1),
                hours=(31 * 24) - 1,
            )
        )
    )

    with pytest.raises(ValueError, match="forecast_start must be the hour after"):
        materialize_operator_preview_forecast_runs(
            market_data_store=market_store,
            forecast_store=InMemoryForecastStore(),
            tenant_id="client_003_dnipro_factory",
            market_venue="DAM",
            forecast_start=datetime(2026, 6, 1),
            horizon_hours=72,
            builders=OperatorPreviewForecastBuilders(
                nbeatsx_builder=_fake_nbeatsx_forecast,
                tft_builder=_fake_tft_forecast,
            ),
        )


def _source_backed_market_frame(
    *,
    market_venue: str,
    start_timestamp: datetime,
    hours: int,
) -> pl.DataFrame:
    timestamps = [start_timestamp + timedelta(hours=hour_index) for hour_index in range(hours)]
    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "price_uah_mwh": [
                2300.0
                + (hour_index % 24) * 35.0
                + (420.0 if 18 <= hour_index % 24 <= 21 else 0.0)
                for hour_index in range(hours)
            ],
            "price_eur_mwh": [55.0 + (hour_index % 24) for hour_index in range(hours)],
            "volume_mwh": [900.0 + (hour_index % 10) for hour_index in range(hours)],
            "source": ["OREE_DATA_VIEW" for _ in range(hours)],
            "source_kind": ["observed" for _ in range(hours)],
            "source_url": ["https://www.oree.com.ua/index.php/data_view" for _ in range(hours)],
            "market_venue": [market_venue for _ in range(hours)],
            "market_zone": ["IPS" for _ in range(hours)],
            "market_timezone": ["Europe/Kyiv" for _ in range(hours)],
            "fetched_at": [datetime(2026, 6, 1) for _ in range(hours)],
            "price_spike": [False for _ in range(hours)],
            "low_volume": [False for _ in range(hours)],
        }
    )


def _fake_nbeatsx_forecast(training_frame: pl.DataFrame, *, horizon_hours: int, **_: object) -> pl.DataFrame:
    future_timestamps = _future_timestamps(training_frame, horizon_hours=horizon_hours)
    return pl.DataFrame(
        {
            "forecast_timestamp": future_timestamps,
            "predicted_price_uah_mwh": [3200.0 + hour_index for hour_index in range(horizon_hours)],
        }
    )


def _fake_tft_forecast(training_frame: pl.DataFrame, *, horizon_hours: int, **_: object) -> pl.DataFrame:
    future_timestamps = _future_timestamps(training_frame, horizon_hours=horizon_hours)
    values = [3100.0 + hour_index for hour_index in range(horizon_hours)]
    return pl.DataFrame(
        {
            "forecast_timestamp": future_timestamps,
            "predicted_price_uah_mwh": values,
            "predicted_price_p50_uah_mwh": [value + 5.0 for value in values],
            "predicted_price_p10_uah_mwh": [value - 100.0 for value in values],
            "predicted_price_p90_uah_mwh": [value + 100.0 for value in values],
        }
    )


def _future_timestamps(training_frame: pl.DataFrame, *, horizon_hours: int) -> list[datetime]:
    return (
        training_frame
        .filter(pl.col("is_forecast"))
        .sort("ds")
        .head(horizon_hours)
        .select("ds")
        .to_series()
        .to_list()
    )
