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
SourceKind = Literal["observed", "synthetic", "derived"]


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

    def list_market_price_frame(
        self,
        *,
        market_venue: str | None = None,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame: ...

    def list_weather_observation_frame(
        self,
        *,
        tenant_id: str | None = None,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame: ...


class NullMarketDataStore:
    def upsert_market_prices(self, observations: Sequence[MarketPriceObservation]) -> None:
        return None

    def upsert_weather_observations(self, observations: Sequence[WeatherObservation]) -> None:
        return None

    def list_market_price_frame(
        self,
        *,
        market_venue: str | None = None,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame:
        return pl.DataFrame()

    def list_weather_observation_frame(
        self,
        *,
        tenant_id: str | None = None,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame:
        return pl.DataFrame()


class InMemoryMarketDataStore:
    def __init__(self) -> None:
        self.market_observations: list[MarketPriceObservation] = []
        self.weather_observations: list[WeatherObservation] = []

    def upsert_market_prices(self, observations: Sequence[MarketPriceObservation]) -> None:
        existing = {
            (
                observation.timestamp,
                observation.market_venue,
                observation.market_zone,
                observation.source,
            ): observation
            for observation in self.market_observations
        }
        for observation in observations:
            existing[
                (
                    observation.timestamp,
                    observation.market_venue,
                    observation.market_zone,
                    observation.source,
                )
            ] = observation
        self.market_observations = sorted(existing.values(), key=lambda item: item.timestamp)

    def upsert_weather_observations(self, observations: Sequence[WeatherObservation]) -> None:
        existing = {
            (
                observation.tenant_id,
                observation.timestamp,
                observation.location_latitude,
                observation.location_longitude,
                observation.source,
            ): observation
            for observation in self.weather_observations
        }
        for observation in observations:
            existing[
                (
                    observation.tenant_id,
                    observation.timestamp,
                    observation.location_latitude,
                    observation.location_longitude,
                    observation.source,
                )
            ] = observation
        self.weather_observations = sorted(
            existing.values(),
            key=lambda item: (item.tenant_id, item.timestamp),
        )

    def list_market_price_frame(
        self,
        *,
        market_venue: str | None = None,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame:
        observations = [
            observation
            for observation in self.market_observations
            if _matches_market_observation(
                observation,
                market_venue=market_venue,
                source_kind=source_kind,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        ]
        return market_price_observations_to_frame(observations)

    def list_weather_observation_frame(
        self,
        *,
        tenant_id: str | None = None,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame:
        observations = [
            observation
            for observation in self.weather_observations
            if _matches_weather_observation(
                observation,
                tenant_id=tenant_id,
                source_kind=source_kind,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        ]
        return weather_observations_to_frame(observations)


class PostgresMarketDataStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._ensure_schema()

    def _connect(self) -> Any:
        from psycopg import connect
        from psycopg.rows import dict_row

        return connect(self._dsn, row_factory=dict_row)

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

    def list_market_price_frame(
        self,
        *,
        market_venue: str | None = None,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame:
        where_clauses, params = _market_where_clauses(
            market_venue=market_venue,
            source_kind=source_kind,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        query = (
            """
            SELECT
                timestamp,
                price_uah_mwh,
                price_eur_mwh,
                volume_mwh,
                source,
                source_kind,
                source_url,
                market_venue,
                market_zone,
                market_timezone,
                fetched_at,
                price_spike,
                low_volume
            FROM market_price_observations
            """
            + _where_sql(where_clauses)
            + " ORDER BY timestamp"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
        return pl.DataFrame([dict(row) for row in rows])

    def list_weather_observation_frame(
        self,
        *,
        tenant_id: str | None = None,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame:
        where_clauses, params = _weather_where_clauses(
            tenant_id=tenant_id,
            source_kind=source_kind,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        query = (
            """
            SELECT
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
            FROM weather_observations
            """
            + _where_sql(where_clauses)
            + " ORDER BY tenant_id, timestamp"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
        return pl.DataFrame([dict(row) for row in rows])


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


def market_price_observations_to_frame(observations: Sequence[MarketPriceObservation]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                DEFAULT_TIMESTAMP_COLUMN: observation.timestamp,
                "price_uah_mwh": observation.price_uah_mwh,
                "price_eur_mwh": observation.price_eur_mwh,
                "volume_mwh": observation.volume_mwh,
                "source": observation.source,
                "source_kind": observation.source_kind,
                "source_url": observation.source_url,
                "market_venue": observation.market_venue,
                "market_zone": observation.market_zone,
                "market_timezone": observation.market_timezone,
                "fetched_at": observation.fetched_at,
                "price_spike": observation.price_spike,
                "low_volume": observation.low_volume,
            }
            for observation in observations
        ]
    )


def weather_observations_from_frame(
    weather_frame: pl.DataFrame,
    *,
    tenant_id: str | None,
) -> list[WeatherObservation]:
    resolved_tenant_id = tenant_id if tenant_id is not None and tenant_id.strip() else DEFAULT_TENANT_ID
    return [
        WeatherObservation(
            tenant_id=_weather_observation_tenant_id(row, resolved_tenant_id),
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


def weather_observations_to_frame(observations: Sequence[WeatherObservation]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": observation.tenant_id,
                DEFAULT_TIMESTAMP_COLUMN: observation.timestamp,
                "location_latitude": observation.location_latitude,
                "location_longitude": observation.location_longitude,
                "location_timezone": observation.location_timezone,
                "temperature": observation.temperature,
                "solar_radiation": observation.solar_radiation,
                "wind_speed": observation.wind_speed,
                "cloudcover": observation.cloudcover,
                "precipitation": observation.precipitation,
                "pressure": observation.pressure,
                "humidity": observation.humidity,
                "source": observation.source,
                "source_kind": observation.source_kind,
                "source_url": observation.source_url,
                "fetched_at": observation.fetched_at,
            }
            for observation in observations
        ]
    )


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
    if source_kind not in {"observed", "synthetic", "derived"}:
        raise ValueError("source_kind must be observed, synthetic, or derived.")
    return cast(SourceKind, source_kind)


def _weather_observation_tenant_id(row: dict[str, object], fallback_tenant_id: str) -> str:
    raw_tenant_id = row.get("tenant_id")
    if isinstance(raw_tenant_id, str) and raw_tenant_id.strip():
        return raw_tenant_id.strip()
    return fallback_tenant_id


def _required_value(row: dict[str, object], key: str) -> object:
    if key not in row or row[key] is None:
        raise ValueError(f"{key} is required.")
    return row[key]


def _matches_market_observation(
    observation: MarketPriceObservation,
    *,
    market_venue: str | None,
    source_kind: SourceKind | None,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> bool:
    if market_venue is not None and observation.market_venue != market_venue:
        return False
    if source_kind is not None and observation.source_kind != source_kind:
        return False
    return _timestamp_in_window(
        observation.timestamp,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )


def _matches_weather_observation(
    observation: WeatherObservation,
    *,
    tenant_id: str | None,
    source_kind: SourceKind | None,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> bool:
    if tenant_id is not None and observation.tenant_id != tenant_id:
        return False
    if source_kind is not None and observation.source_kind != source_kind:
        return False
    return _timestamp_in_window(
        observation.timestamp,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )


def _timestamp_in_window(
    timestamp: datetime,
    *,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> bool:
    if start_timestamp is not None and timestamp < start_timestamp:
        return False
    if end_timestamp is not None and timestamp > end_timestamp:
        return False
    return True


def _market_where_clauses(
    *,
    market_venue: str | None,
    source_kind: SourceKind | None,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> tuple[list[str], list[object]]:
    where_clauses: list[str] = []
    params: list[object] = []
    if market_venue is not None:
        where_clauses.append("market_venue = %s")
        params.append(market_venue)
    if source_kind is not None:
        where_clauses.append("source_kind = %s")
        params.append(source_kind)
    _append_timestamp_clauses(
        where_clauses,
        params,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return where_clauses, params


def _weather_where_clauses(
    *,
    tenant_id: str | None,
    source_kind: SourceKind | None,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> tuple[list[str], list[object]]:
    where_clauses: list[str] = []
    params: list[object] = []
    if tenant_id is not None:
        where_clauses.append("tenant_id = %s")
        params.append(tenant_id)
    if source_kind is not None:
        where_clauses.append("source_kind = %s")
        params.append(source_kind)
    _append_timestamp_clauses(
        where_clauses,
        params,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return where_clauses, params


def _append_timestamp_clauses(
    where_clauses: list[str],
    params: list[object],
    *,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> None:
    if start_timestamp is not None:
        where_clauses.append("timestamp >= %s")
        params.append(start_timestamp)
    if end_timestamp is not None:
        where_clauses.append("timestamp <= %s")
        params.append(end_timestamp)


def _where_sql(where_clauses: Sequence[str]) -> str:
    if not where_clauses:
        return ""
    return " WHERE " + " AND ".join(where_clauses)
