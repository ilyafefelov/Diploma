from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl

from smart_arbitrage.research.operator_preview_refresh import ensure_operator_preview_window
from smart_arbitrage.resources.forecast_store import InMemoryForecastStore
from smart_arbitrage.resources.market_data_store import (
    InMemoryMarketDataStore,
    market_price_observations_from_frame,
)


def test_ensure_operator_preview_window_refreshes_source_rows_before_materializing_weekly_cache() -> None:
    market_store = InMemoryMarketDataStore()
    market_store.upsert_market_prices(
        market_price_observations_from_frame(
            _source_backed_market_frame(
                market_venue="DAM",
                start_timestamp=datetime(2026, 5, 1),
                hours=15 * 24,
            )
        )
    )
    forecast_store = InMemoryForecastStore()

    result = ensure_operator_preview_window(
        market_data_store=market_store,
        forecast_store=forecast_store,
        tenant_id="client_003_dnipro_factory",
        market_venue="DAM",
        target_delivery_date=date(2026, 5, 24),
        source_history_fetcher=_fetch_source_window,
        nbeatsx_builder=_fake_forecast,
        tft_builder=_fake_forecast,
        cache_horizon_hours=168,
    )

    assert result.status == "materialized"
    assert result.market_execution_enabled is False
    assert result.read_model_boundary == "operator_preview_no_market_submission"
    assert result.source_refresh_rows == 8 * 24
    assert result.source_refresh_dates == tuple(
        (date(2026, 5, 15) + timedelta(days=day_index)).isoformat()
        for day_index in range(1, 9)
    )
    assert result.forecast_start == datetime(2026, 5, 24)
    assert result.forecast_horizon_end == datetime(2026, 5, 30, 23)
    assert result.horizon_hours == 168
    assert set(result.forecast_run_ids) == {"nbeatsx_official_v0", "tft_official_v0"}

    forecast_frame = forecast_store.forecast_observation_frame_for_window(
        model_names=["nbeatsx_official_v0", "tft_official_v0"],
        window_start=datetime(2026, 5, 24),
        window_end=datetime(2026, 5, 25),
        limit_per_model=24,
    )
    assert forecast_frame.height == 48
    assert set(forecast_frame.select("market_venue").to_series().to_list()) == {"DAM"}


def test_ensure_operator_preview_window_blocks_without_substitute_rows_when_source_refresh_fails() -> None:
    market_store = InMemoryMarketDataStore()
    market_store.upsert_market_prices(
        market_price_observations_from_frame(
            _source_backed_market_frame(
                market_venue="IDM",
                start_timestamp=datetime(2026, 5, 1),
                hours=15 * 24,
            )
        )
    )
    forecast_store = InMemoryForecastStore()

    result = ensure_operator_preview_window(
        market_data_store=market_store,
        forecast_store=forecast_store,
        tenant_id="client_003_dnipro_factory",
        market_venue="IDM",
        target_delivery_date=date(2026, 5, 24),
        source_history_fetcher=_missing_source_window,
        nbeatsx_builder=_fake_forecast,
        tft_builder=_fake_forecast,
        cache_horizon_hours=168,
    )

    assert result.status == "blocked_source_unavailable"
    assert "source-backed rows unavailable" in result.message
    assert "synthetic" not in result.message.lower()
    assert result.market_execution_enabled is False
    assert forecast_store.latest_forecast_observation_frame(
        model_names=["nbeatsx_official_idm_v0", "tft_official_idm_v0"],
        limit_per_model=24,
    ).is_empty()


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
            "price_uah_mwh": [2400.0 + (hour_index % 24) * 30.0 for hour_index in range(hours)],
            "price_eur_mwh": [60.0 + (hour_index % 24) for hour_index in range(hours)],
            "volume_mwh": [1000.0 for _ in range(hours)],
            "source": ["OREE_DATA_VIEW" for _ in range(hours)],
            "source_kind": ["observed" for _ in range(hours)],
            "source_url": ["https://www.oree.com.ua/index.php/pricectr/data_view" for _ in range(hours)],
            "market_venue": [market_venue for _ in range(hours)],
            "market_zone": ["IPS" for _ in range(hours)],
            "market_timezone": ["Europe/Kyiv" for _ in range(hours)],
            "fetched_at": [datetime(2026, 6, 1) for _ in range(hours)],
            "price_spike": [False for _ in range(hours)],
            "low_volume": [False for _ in range(hours)],
        }
    )


def _fetch_source_window(
    *,
    start_date: date,
    end_date: date,
    market_venue: str,
) -> pl.DataFrame:
    return _source_backed_market_frame(
        market_venue=market_venue,
        start_timestamp=datetime.combine(start_date, datetime.min.time()),
        hours=((end_date - start_date).days + 1) * 24,
    )


def _missing_source_window(
    *,
    start_date: date,
    end_date: date,
    market_venue: str,
) -> pl.DataFrame:
    del start_date, end_date, market_venue
    raise ValueError("source-backed rows unavailable from OREE")


def _fake_forecast(training_frame: pl.DataFrame, *, horizon_hours: int, **_: object) -> pl.DataFrame:
    timestamps = (
        training_frame
        .filter(pl.col("is_forecast"))
        .sort("ds")
        .head(horizon_hours)
        .select("ds")
        .to_series()
        .to_list()
    )
    return pl.DataFrame(
        {
            "forecast_timestamp": timestamps,
            "predicted_price_uah_mwh": [3000.0 + hour_index for hour_index in range(horizon_hours)],
        }
    )
