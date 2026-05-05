from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from functools import cache
import os
from typing import Any, Protocol, cast

import polars as pl
from pydantic import BaseModel, ConfigDict

from smart_arbitrage.resources.market_data_store import SourceKind

GRID_EVENT_DSN_ENV = "SMART_ARBITRAGE_GRID_EVENT_DSN"
MARKET_DATA_DSN_ENV = "SMART_ARBITRAGE_MARKET_DATA_DSN"


class GridEventObservation(BaseModel):
    model_config = ConfigDict(strict=True)

    post_id: str
    post_url: str
    published_at: datetime
    fetched_at: datetime
    raw_text: str
    source: str
    source_kind: SourceKind
    source_url: str
    energy_system_status: bool
    shelling_damage: bool
    outage_or_restriction: bool
    consumption_change: str
    solar_shift_advice: bool
    evening_saving_request: bool
    affected_oblasts: list[str]


class GridEventStore(Protocol):
    def upsert_grid_events(self, observations: Sequence[GridEventObservation]) -> None: ...

    def list_grid_event_frame(
        self,
        *,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame: ...


class NullGridEventStore:
    def upsert_grid_events(self, observations: Sequence[GridEventObservation]) -> None:
        return None

    def list_grid_event_frame(
        self,
        *,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame:
        return _empty_grid_event_frame()


class InMemoryGridEventStore:
    def __init__(self) -> None:
        self.observations: list[GridEventObservation] = []

    def upsert_grid_events(self, observations: Sequence[GridEventObservation]) -> None:
        existing = {observation.post_id: observation for observation in self.observations}
        for observation in observations:
            existing[observation.post_id] = observation
        self.observations = sorted(existing.values(), key=lambda item: item.published_at)

    def list_grid_event_frame(
        self,
        *,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame:
        observations = [
            observation
            for observation in self.observations
            if _matches_grid_event(
                observation,
                source_kind=source_kind,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        ]
        return grid_event_observations_to_frame(observations)


class PostgresGridEventStore:
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
                    CREATE TABLE IF NOT EXISTS grid_event_observations (
                        post_id TEXT PRIMARY KEY,
                        post_url TEXT NOT NULL,
                        published_at TIMESTAMP NOT NULL,
                        fetched_at TIMESTAMP NOT NULL,
                        raw_text TEXT NOT NULL,
                        source TEXT NOT NULL,
                        source_kind TEXT NOT NULL,
                        source_url TEXT NOT NULL,
                        energy_system_status BOOLEAN NOT NULL,
                        shelling_damage BOOLEAN NOT NULL,
                        outage_or_restriction BOOLEAN NOT NULL,
                        consumption_change TEXT NOT NULL,
                        solar_shift_advice BOOLEAN NOT NULL,
                        evening_saving_request BOOLEAN NOT NULL,
                        affected_oblasts JSONB NOT NULL
                    )
                    """
                )
            connection.commit()

    def upsert_grid_events(self, observations: Sequence[GridEventObservation]) -> None:
        if not observations:
            return None

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO grid_event_observations (
                        post_id,
                        post_url,
                        published_at,
                        fetched_at,
                        raw_text,
                        source,
                        source_kind,
                        source_url,
                        energy_system_status,
                        shelling_damage,
                        outage_or_restriction,
                        consumption_change,
                        solar_shift_advice,
                        evening_saving_request,
                        affected_oblasts
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (post_id)
                    DO UPDATE SET
                        post_url = EXCLUDED.post_url,
                        published_at = EXCLUDED.published_at,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_text = EXCLUDED.raw_text,
                        source = EXCLUDED.source,
                        source_kind = EXCLUDED.source_kind,
                        source_url = EXCLUDED.source_url,
                        energy_system_status = EXCLUDED.energy_system_status,
                        shelling_damage = EXCLUDED.shelling_damage,
                        outage_or_restriction = EXCLUDED.outage_or_restriction,
                        consumption_change = EXCLUDED.consumption_change,
                        solar_shift_advice = EXCLUDED.solar_shift_advice,
                        evening_saving_request = EXCLUDED.evening_saving_request,
                        affected_oblasts = EXCLUDED.affected_oblasts
                    """,
                    [_grid_event_values(observation) for observation in observations],
                )
            connection.commit()

    def list_grid_event_frame(
        self,
        *,
        source_kind: SourceKind | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
    ) -> pl.DataFrame:
        where_clauses, params = _grid_event_where_clauses(
            source_kind=source_kind,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        query = (
            """
            SELECT
                post_id,
                post_url,
                published_at,
                fetched_at,
                raw_text,
                source,
                source_kind,
                source_url,
                energy_system_status,
                shelling_damage,
                outage_or_restriction,
                consumption_change,
                solar_shift_advice,
                evening_saving_request,
                affected_oblasts
            FROM grid_event_observations
            """
            + _where_sql(where_clauses)
            + " ORDER BY published_at, post_id"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
        return pl.DataFrame([dict(row) for row in rows]) if rows else _empty_grid_event_frame()


def grid_event_observations_to_frame(observations: Sequence[GridEventObservation]) -> pl.DataFrame:
    if not observations:
        return _empty_grid_event_frame()
    return pl.DataFrame(
        [
            {
                "post_id": observation.post_id,
                "post_url": observation.post_url,
                "published_at": observation.published_at,
                "fetched_at": observation.fetched_at,
                "raw_text": observation.raw_text,
                "source": observation.source,
                "source_kind": observation.source_kind,
                "source_url": observation.source_url,
                "energy_system_status": observation.energy_system_status,
                "shelling_damage": observation.shelling_damage,
                "outage_or_restriction": observation.outage_or_restriction,
                "consumption_change": observation.consumption_change,
                "solar_shift_advice": observation.solar_shift_advice,
                "evening_saving_request": observation.evening_saving_request,
                "affected_oblasts": observation.affected_oblasts,
            }
            for observation in observations
        ]
    ).sort(["published_at", "post_id"])


@cache
def get_grid_event_store() -> GridEventStore:
    dsn = resolve_grid_event_store_dsn()
    if not dsn:
        return NullGridEventStore()
    return PostgresGridEventStore(dsn)


def resolve_grid_event_store_dsn() -> str:
    grid_event_dsn = os.getenv(GRID_EVENT_DSN_ENV, "").strip()
    if grid_event_dsn:
        return grid_event_dsn
    return os.getenv(MARKET_DATA_DSN_ENV, "").strip()


def _grid_event_values(observation: GridEventObservation) -> tuple[object, ...]:
    from psycopg.types.json import Jsonb

    return (
        observation.post_id,
        observation.post_url,
        observation.published_at,
        observation.fetched_at,
        observation.raw_text,
        observation.source,
        observation.source_kind,
        observation.source_url,
        observation.energy_system_status,
        observation.shelling_damage,
        observation.outage_or_restriction,
        observation.consumption_change,
        observation.solar_shift_advice,
        observation.evening_saving_request,
        Jsonb(observation.affected_oblasts),
    )


def _matches_grid_event(
    observation: GridEventObservation,
    *,
    source_kind: SourceKind | None,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> bool:
    if source_kind is not None and observation.source_kind != source_kind:
        return False
    if start_timestamp is not None and observation.published_at < start_timestamp:
        return False
    if end_timestamp is not None and observation.published_at > end_timestamp:
        return False
    return True


def _grid_event_where_clauses(
    *,
    source_kind: SourceKind | None,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> tuple[list[str], list[object]]:
    where_clauses: list[str] = []
    params: list[object] = []
    if source_kind is not None:
        where_clauses.append("source_kind = %s")
        params.append(source_kind)
    if start_timestamp is not None:
        where_clauses.append("published_at >= %s")
        params.append(start_timestamp)
    if end_timestamp is not None:
        where_clauses.append("published_at <= %s")
        params.append(end_timestamp)
    return where_clauses, params


def _where_sql(where_clauses: Sequence[str]) -> str:
    if not where_clauses:
        return ""
    return " WHERE " + " AND ".join(where_clauses)


def _empty_grid_event_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "post_id": pl.Utf8,
            "post_url": pl.Utf8,
            "published_at": pl.Datetime,
            "fetched_at": pl.Datetime,
            "raw_text": pl.Utf8,
            "source": pl.Utf8,
            "source_kind": pl.Utf8,
            "source_url": pl.Utf8,
            "energy_system_status": pl.Boolean,
            "shelling_damage": pl.Boolean,
            "outage_or_restriction": pl.Boolean,
            "consumption_change": pl.Utf8,
            "solar_shift_advice": pl.Boolean,
            "evening_saving_request": pl.Boolean,
            "affected_oblasts": pl.List(pl.Utf8),
        }
    )


def _required_source_kind(value: object) -> SourceKind:
    if value not in {"observed", "synthetic", "derived"}:
        raise ValueError("source_kind must be observed, synthetic, or derived.")
    return cast(SourceKind, value)


__all__ = [
    "GridEventObservation",
    "GridEventStore",
    "InMemoryGridEventStore",
    "NullGridEventStore",
    "PostgresGridEventStore",
    "get_grid_event_store",
    "grid_event_observations_to_frame",
    "resolve_grid_event_store_dsn",
]
