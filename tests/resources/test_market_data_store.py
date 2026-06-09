from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.resources.market_data_store import (
    InMemoryMarketDataStore,
    MarketPriceObservation,
    WeatherObservation,
)


def test_in_memory_market_data_store_reads_observed_market_window() -> None:
    store = InMemoryMarketDataStore()
    start = datetime(2026, 5, 1)
    store.upsert_market_prices(
        [
            _market_observation(
                timestamp=start,
                source_kind="observed",
                price_uah_mwh=2100.0,
            ),
            _market_observation(
                timestamp=start + timedelta(hours=1),
                source_kind="synthetic",
                price_uah_mwh=9999.0,
            ),
            _market_observation(
                timestamp=start + timedelta(hours=2),
                source_kind="observed",
                price_uah_mwh=2300.0,
            ),
        ]
    )

    frame = store.list_market_price_frame(
        market_venue="DAM",
        source_kind="observed",
        start_timestamp=start,
        end_timestamp=start + timedelta(hours=2),
    )

    assert frame.height == 2
    assert frame.select("source_kind").to_series().to_list() == ["observed", "observed"]
    assert frame.select("price_uah_mwh").to_series().to_list() == [2100.0, 2300.0]


def test_market_data_source_kind_accepts_derived_rows() -> None:
    observation = _market_observation(
        timestamp=datetime(2026, 5, 1),
        source_kind="derived",
        price_uah_mwh=2200.0,
    )

    assert observation.source_kind == "derived"


def test_in_memory_market_data_store_reads_tenant_weather_window() -> None:
    store = InMemoryMarketDataStore()
    start = datetime(2026, 5, 1)
    store.upsert_weather_observations(
        [
            _weather_observation(
                tenant_id="client_001_kyiv_mall",
                timestamp=start,
                source_kind="observed",
            ),
            _weather_observation(
                tenant_id="client_002_lviv_office",
                timestamp=start,
                source_kind="observed",
            ),
            _weather_observation(
                tenant_id="client_001_kyiv_mall",
                timestamp=start + timedelta(hours=1),
                source_kind="synthetic",
            ),
        ]
    )

    frame = store.list_weather_observation_frame(
        tenant_id="client_001_kyiv_mall",
        source_kind="observed",
        start_timestamp=start,
        end_timestamp=start + timedelta(hours=1),
    )

    assert frame.height == 1
    assert frame.row(0, named=True)["tenant_id"] == "client_001_kyiv_mall"
    assert frame.row(0, named=True)["source_kind"] == "observed"
    assert isinstance(frame, pl.DataFrame)


def _market_observation(
    *,
    timestamp: datetime,
    source_kind: str,
    price_uah_mwh: float,
) -> MarketPriceObservation:
    return MarketPriceObservation(
        timestamp=timestamp,
        price_uah_mwh=price_uah_mwh,
        price_eur_mwh=price_uah_mwh / 40.0,
        volume_mwh=1000.0,
        source=f"TEST_{source_kind.upper()}",
        source_kind=source_kind,
        source_url=f"test://{source_kind}",
        market_venue="DAM",
        market_zone="IPS",
        market_timezone="Europe/Kyiv",
        fetched_at=timestamp,
        price_spike=False,
        low_volume=False,
    )


def _weather_observation(
    *,
    tenant_id: str,
    timestamp: datetime,
    source_kind: str,
) -> WeatherObservation:
    return WeatherObservation(
        tenant_id=tenant_id,
        timestamp=timestamp,
        location_latitude=50.45,
        location_longitude=30.52,
        location_timezone="Europe/Kyiv",
        temperature=20.0,
        solar_radiation=300.0,
        wind_speed=4.0,
        cloudcover=30.0,
        precipitation=0.0,
        pressure=1010.0,
        humidity=60.0,
        source=f"TEST_{source_kind.upper()}",
        source_kind=source_kind,
        source_url=f"test://{source_kind}",
        fetched_at=timestamp,
    )
