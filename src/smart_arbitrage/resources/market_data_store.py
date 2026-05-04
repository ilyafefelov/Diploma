from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from functools import cache
import os
from typing import Any, Literal, Protocol, cast

import polars as pl
from pydantic import BaseModel, ConfigDict

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN


DEFAULT_TENANT_ID = "__default__"
SourceKind = Literal["observed", "synthetic"]


class MarketPriceObservation(BaseModel):
    model_config = ConfigDict(strict=True)

    timestamp: datetime
    price_uah_mwh: float
    price_eur_mwh: float
    volume_mwh: float
    source: str
    source_kind: SourceKind
    source_url: str
    market_venue: str
    market_zone: str
    market_timezone: str
    fetched_at: datetime
    price_spike: bool
    low_volume: bool


class WeatherObservation(BaseModel):
    model_config = ConfigDict(strict=True)

    tenant_id: str
    timestamp: datetime
    location_latitude: float
    location_longitude: float
    location_timezone: str
    temperature: float
    solar_radiation: float
    wind_speed: float
    cloudcover: float
    precipitation: float
    pressure: float
    humidity: float
    source: str
    source_kind: SourceKind
    source_url: str
    fetched_at: datetime


class MarketDataStore(Protocol):
    def upsert_market_prices(self, observations: Sequence[MarketPriceObservation]) -> None: ...

    def upsert_weather_observations(self, observations: Sequence[WeatherObservation]) -> None: ...


class NullMarketDataStore:
    def upsert_market_prices(self, observations: Sequence[MarketPriceObservation]) -> None:
        return None

    def upsert_weather_observations(self, observations: Sequence[WeatherObservation]) -> None:
        return None


class PostgresMarketDataStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._ensure_schema()

    def _connect(self) -> Any:
        from psycopg import connect

        return connect(self._dsn)

    def _ensure_schema(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS market_price_observations (
                        timestamp TIMESTAMP NOT NULL,
                        market_venue TEXT NOT NULL,
                        market_zone TEXT NOT NULL,
                        market_timezone TEXT NOT NULL,
                        price_uah_mwh DOUBLE PRECISION NOT NULL,
                        price_eur_mwh DOUBLE PRECISION NOT NULL,
                        volume_mwh DOUBLE PRECISION NOT NULL,
                        source TEXT NOT NULL,
                        source_kind TEXT NOT NULL,
                        source_url TEXT NOT NULL,
                        fetched_at TIMESTAMP NOT NULL,
                        price_spike BOOLEAN NOT NULL,
                        low_volume BOOLEAN NOT NULL,
                        PRIMARY KEY (timestamp, market_venue, market_zone, source)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS weather_observations (
                        tenant_id TEXT NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        location_latitude DOUBLE PRECISION NOT NULL,
                        location_longitude DOUBLE PRECISION NOT NULL,
                        location_timezone TEXT NOT NULL,
                        temperature DOUBLE PRECISION NOT NULL,
                        solar_radiation DOUBLE PRECISION NOT NULL,
                        wind_speed DOUBLE PRECISION NOT NULL,
                        cloudcover DOUBLE PRECISION NOT NULL,
                        precipitation DOUBLE PRECISION NOT NULL,
                        pressure DOUBLE PRECISION NOT NULL,
                        humidity DOUBLE PRECISION NOT NULL,
                        source TEXT NOT NULL,
                        source_kind TEXT NOT NULL,
                        source_url TEXT NOT NULL,
                        fetched_at TIMESTAMP NOT NULL,
                        PRIMARY KEY (
                            tenant_id,
                            timestamp,
                            location_latitude,
                            location_longitude,
                            source
                        )
                    )
                    """
                )
            connection.commit()

    def upsert_market_prices(self, observations: Sequence[MarketPriceObservation]) -> None:
        if not observations:
            return None

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO market_price_observations (
                        timestamp,
                        market_venue,
                        market_zone,
                        market_timezone,
                        price_uah_mwh,
                        price_eur_mwh,
                        volume_mwh,
                        source,
                        source_kind,
                        source_url,
                        fetched_at,
                        price_spike,
                        low_volume
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp, market_venue, market_zone, source)
                    DO UPDATE SET
                        market_timezone = EXCLUDED.market_timezone,
                        price_uah_mwh = EXCLUDED.price_uah_mwh,
                        price_eur_mwh = EXCLUDED.price_eur_mwh,
                        volume_mwh = EXCLUDED.volume_mwh,
                        source_kind = EXCLUDED.source_kind,
                        source_url = EXCLUDED.source_url,
                        fetched_at = EXCLUDED.fetched_at,
                        price_spike = EXCLUDED.price_spike,
                        low_volume = EXCLUDED.low_volume
                    """,
                    [_market_observation_values(observation) for observation in observations],
                )
            connection.commit()

    def upsert_weather_observations(self, observations: Sequence[WeatherObservation]) -> None:
        if not observations:
            return None

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO weather_observations (
                        tenant_id,
                        timestamp,
                        location_latitude,
                        location_longitude,
                        location_timezone,
                        temperature,
                        solar_radiation,
                        wind_speed,
                        cloudcover,
                        precipitation,
                        pressure,
                        humidity,
                        source,
                        source_kind,
                        source_url,
                        fetched_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (
                        tenant_id,
                        timestamp,
                        location_latitude,
                        location_longitude,
                        source
                    )
                    DO UPDATE SET
                        location_timezone = EXCLUDED.location_timezone,
                        temperature = EXCLUDED.temperature,
                        solar_radiation = EXCLUDED.solar_radiation,
                        wind_speed = EXCLUDED.wind_speed,
                        cloudcover = EXCLUDED.cloudcover,
                        precipitation = EXCLUDED.precipitation,
                        pressure = EXCLUDED.pressure,
                        humidity = EXCLUDED.humidity,
                        source_kind = EXCLUDED.source_kind,
                        source_url = EXCLUDED.source_url,
                        fetched_at = EXCLUDED.fetched_at
                    """,
                    [_weather_observation_values(observation) for observation in observations],
                )
            connection.commit()


def market_price_observations_from_frame(price_history: pl.DataFrame) -> list[MarketPriceObservation]:
    return [
        MarketPriceObservation(
            timestamp=_required_datetime(row, DEFAULT_TIMESTAMP_COLUMN),
            price_uah_mwh=_required_float(row, DEFAULT_PRICE_COLUMN),
            price_eur_mwh=_required_float(row, "price_eur_mwh"),
            volume_mwh=_required_float(row, "volume_mwh"),
            source=_required_text(row, "source"),
            source_kind=_required_source_kind(row),
            source_url=_required_text(row, "source_url"),
            market_venue=_required_text(row, "market_venue"),
            market_zone=_required_text(row, "market_zone"),
            market_timezone=_required_text(row, "market_timezone"),
            fetched_at=_required_datetime(row, "fetched_at"),
            price_spike=_required_bool(row, "price_spike"),
            low_volume=_required_bool(row, "low_volume"),
        )
        for row in price_history.iter_rows(named=True)
    ]


def weather_observations_from_frame(
    weather_frame: pl.DataFrame,
    *,
    tenant_id: str | None,
) -> list[WeatherObservation]:
    resolved_tenant_id = tenant_id if tenant_id is not None and tenant_id.strip() else DEFAULT_TENANT_ID
    return [
        WeatherObservation(
            tenant_id=resolved_tenant_id,
            timestamp=_required_datetime(row, DEFAULT_TIMESTAMP_COLUMN),
            location_latitude=_required_float(row, "location_latitude"),
            location_longitude=_required_float(row, "location_longitude"),
            location_timezone=_required_text(row, "location_timezone"),
            temperature=_required_float(row, "temperature"),
            solar_radiation=_required_float(row, "solar_radiation"),
            wind_speed=_required_float(row, "wind_speed"),
            cloudcover=_required_float(row, "cloudcover"),
            precipitation=_required_float(row, "precipitation"),
            pressure=_required_float(row, "pressure"),
            humidity=_required_float(row, "humidity"),
            source=_required_text(row, "source"),
            source_kind=_required_source_kind(row),
            source_url=_required_text(row, "source_url"),
            fetched_at=_required_datetime(row, "fetched_at"),
        )
        for row in weather_frame.iter_rows(named=True)
    ]


@cache
def get_market_data_store() -> MarketDataStore:
    dsn = os.getenv("SMART_ARBITRAGE_MARKET_DATA_DSN", "").strip()
    if not dsn:
        return NullMarketDataStore()

    return PostgresMarketDataStore(dsn)


def _market_observation_values(observation: MarketPriceObservation) -> tuple[object, ...]:
    return (
        observation.timestamp,
        observation.market_venue,
        observation.market_zone,
        observation.market_timezone,
        observation.price_uah_mwh,
        observation.price_eur_mwh,
        observation.volume_mwh,
        observation.source,
        observation.source_kind,
        observation.source_url,
        observation.fetched_at,
        observation.price_spike,
        observation.low_volume,
    )


def _weather_observation_values(observation: WeatherObservation) -> tuple[object, ...]:
    return (
        observation.tenant_id,
        observation.timestamp,
        observation.location_latitude,
        observation.location_longitude,
        observation.location_timezone,
        observation.temperature,
        observation.solar_radiation,
        observation.wind_speed,
        observation.cloudcover,
        observation.precipitation,
        observation.pressure,
        observation.humidity,
        observation.source,
        observation.source_kind,
        observation.source_url,
        observation.fetched_at,
    )


def _required_datetime(row: dict[str, object], key: str) -> datetime:
    value = _required_value(row, key)
    if not isinstance(value, datetime):
        raise TypeError(f"{key} must be a datetime.")
    return value


def _required_float(row: dict[str, object], key: str) -> float:
    value = _required_value(row, key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{key} must be numeric.")
    return float(value)


def _required_bool(row: dict[str, object], key: str) -> bool:
    value = _required_value(row, key)
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be a bool.")
    return value


def _required_text(row: dict[str, object], key: str) -> str:
    value = _required_value(row, key)
    if not isinstance(value, str):
        raise TypeError(f"{key} must be text.")
    cleaned_value = value.strip()
    if not cleaned_value:
        raise ValueError(f"{key} must not be empty.")
    return cleaned_value


def _required_source_kind(row: dict[str, object]) -> SourceKind:
    source_kind = _required_text(row, "source_kind")
    if source_kind not in {"observed", "synthetic"}:
        raise ValueError("source_kind must be observed or synthetic.")
    return cast(SourceKind, source_kind)


def _required_value(row: dict[str, object], key: str) -> object:
    if key not in row or row[key] is None:
        raise ValueError(f"{key} is required.")
    return row[key]
