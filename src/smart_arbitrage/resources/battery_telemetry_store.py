from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from functools import cache
import json
import os
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics


SourceKind = Literal["observed", "synthetic"]
TelemetryFreshness = Literal["fresh", "stale"]
DEFAULT_RAW_TELEMETRY_INTERVAL_MINUTES = 5
DEFAULT_FRESH_MIN_OBSERVATIONS_PER_HOUR = 9
BATTERY_TELEMETRY_OBSERVATION_COLUMNS = (
    "tenant_id",
    "observed_at",
    "current_soc",
    "soh",
    "power_mw",
    "temperature_c",
    "source",
    "source_kind",
)
BATTERY_STATE_HOURLY_SNAPSHOT_COLUMNS = (
    "tenant_id",
    "snapshot_hour",
    "observation_count",
    "soc_open",
    "soc_close",
    "soc_mean",
    "soh_close",
    "power_mw_mean",
    "throughput_mwh",
    "efc_delta",
    "telemetry_freshness",
    "first_observed_at",
    "last_observed_at",
)


class BatteryTelemetryObservation(BaseModel):
    model_config = ConfigDict(strict=True)

    tenant_id: str = Field(min_length=1)
    observed_at: datetime
    current_soc: float = Field(ge=0.0, le=1.0)
    soh: float = Field(ge=0.0, le=1.0)
    power_mw: float
    temperature_c: float | None = None
    source: str = Field(min_length=1)
    source_kind: SourceKind
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class BatteryStateHourlySnapshot(BaseModel):
    model_config = ConfigDict(strict=True)

    tenant_id: str = Field(min_length=1)
    snapshot_hour: datetime
    observation_count: int = Field(ge=0)
    soc_open: float = Field(ge=0.0, le=1.0)
    soc_close: float = Field(ge=0.0, le=1.0)
    soc_mean: float = Field(ge=0.0, le=1.0)
    soh_close: float = Field(ge=0.0, le=1.0)
    power_mw_mean: float
    throughput_mwh: float = Field(ge=0.0)
    efc_delta: float = Field(ge=0.0)
    telemetry_freshness: TelemetryFreshness
    first_observed_at: datetime
    last_observed_at: datetime


class BatteryTelemetryStore(Protocol):
    def upsert_battery_telemetry(self, observations: Sequence[BatteryTelemetryObservation]) -> None: ...

    def upsert_hourly_snapshots(self, snapshots: Sequence[BatteryStateHourlySnapshot]) -> None: ...

    def list_battery_telemetry(self, *, tenant_id: str | None = None) -> list[BatteryTelemetryObservation]: ...

    def list_hourly_snapshots(self, *, tenant_id: str | None = None) -> list[BatteryStateHourlySnapshot]: ...

    def get_latest_battery_telemetry(self, *, tenant_id: str) -> BatteryTelemetryObservation | None: ...

    def get_latest_hourly_snapshot(self, *, tenant_id: str) -> BatteryStateHourlySnapshot | None: ...


class NullBatteryTelemetryStore:
    def upsert_battery_telemetry(self, observations: Sequence[BatteryTelemetryObservation]) -> None:
        return None

    def upsert_hourly_snapshots(self, snapshots: Sequence[BatteryStateHourlySnapshot]) -> None:
        return None

    def list_battery_telemetry(self, *, tenant_id: str | None = None) -> list[BatteryTelemetryObservation]:
        return []

    def list_hourly_snapshots(self, *, tenant_id: str | None = None) -> list[BatteryStateHourlySnapshot]:
        return []

    def get_latest_battery_telemetry(self, *, tenant_id: str) -> BatteryTelemetryObservation | None:
        return None

    def get_latest_hourly_snapshot(self, *, tenant_id: str) -> BatteryStateHourlySnapshot | None:
        return None


class InMemoryBatteryTelemetryStore:
    def __init__(self) -> None:
        self._observations: dict[tuple[str, datetime], BatteryTelemetryObservation] = {}
        self._snapshots: dict[tuple[str, datetime], BatteryStateHourlySnapshot] = {}

    def upsert_battery_telemetry(self, observations: Sequence[BatteryTelemetryObservation]) -> None:
        for observation in observations:
            self._observations[(observation.tenant_id, observation.observed_at)] = observation

    def upsert_hourly_snapshots(self, snapshots: Sequence[BatteryStateHourlySnapshot]) -> None:
        for snapshot in snapshots:
            self._snapshots[(snapshot.tenant_id, snapshot.snapshot_hour)] = snapshot

    def list_battery_telemetry(self, *, tenant_id: str | None = None) -> list[BatteryTelemetryObservation]:
        observations = list(self._observations.values())
        if tenant_id is not None:
            observations = [observation for observation in observations if observation.tenant_id == tenant_id]
        return sorted(observations, key=lambda observation: (observation.tenant_id, observation.observed_at))

    def list_hourly_snapshots(self, *, tenant_id: str | None = None) -> list[BatteryStateHourlySnapshot]:
        snapshots = list(self._snapshots.values())
        if tenant_id is not None:
            snapshots = [snapshot for snapshot in snapshots if snapshot.tenant_id == tenant_id]
        return sorted(snapshots, key=lambda snapshot: (snapshot.tenant_id, snapshot.snapshot_hour))

    def get_latest_battery_telemetry(self, *, tenant_id: str) -> BatteryTelemetryObservation | None:
        observations = self.list_battery_telemetry(tenant_id=tenant_id)
        if not observations:
            return None
        return observations[-1]

    def get_latest_hourly_snapshot(self, *, tenant_id: str) -> BatteryStateHourlySnapshot | None:
        snapshots = self.list_hourly_snapshots(tenant_id=tenant_id)
        if not snapshots:
            return None
        return snapshots[-1]


def build_hourly_battery_state_snapshots(
    observations: Sequence[BatteryTelemetryObservation],
    *,
    battery_metrics_by_tenant: dict[str, BatteryPhysicalMetrics],
    raw_interval_minutes: int = DEFAULT_RAW_TELEMETRY_INTERVAL_MINUTES,
    fresh_min_observations: int = DEFAULT_FRESH_MIN_OBSERVATIONS_PER_HOUR,
) -> list[BatteryStateHourlySnapshot]:
    if raw_interval_minutes <= 0:
        raise ValueError("raw_interval_minutes must be positive.")
    if fresh_min_observations <= 0:
        raise ValueError("fresh_min_observations must be positive.")

    grouped_observations: dict[tuple[str, datetime], list[BatteryTelemetryObservation]] = defaultdict(list)
    for observation in observations:
        snapshot_hour = _truncate_to_hour(observation.observed_at)
        grouped_observations[(observation.tenant_id, snapshot_hour)].append(observation)

    snapshots: list[BatteryStateHourlySnapshot] = []
    interval_hours = raw_interval_minutes / 60.0
    for (tenant_id, snapshot_hour), group in sorted(grouped_observations.items()):
        if tenant_id not in battery_metrics_by_tenant:
            raise ValueError(f"Missing battery metrics for tenant {tenant_id}.")
        ordered_group = sorted(group, key=lambda observation: observation.observed_at)
        metrics = battery_metrics_by_tenant[tenant_id]
        throughput_mwh = sum(abs(observation.power_mw) * interval_hours for observation in ordered_group)
        soc_values = [observation.current_soc for observation in ordered_group]
        power_values = [observation.power_mw for observation in ordered_group]
        snapshots.append(
            BatteryStateHourlySnapshot(
                tenant_id=tenant_id,
                snapshot_hour=snapshot_hour,
                observation_count=len(ordered_group),
                soc_open=ordered_group[0].current_soc,
                soc_close=ordered_group[-1].current_soc,
                soc_mean=sum(soc_values) / len(soc_values),
                soh_close=ordered_group[-1].soh,
                power_mw_mean=sum(power_values) / len(power_values),
                throughput_mwh=throughput_mwh,
                efc_delta=throughput_mwh / (2.0 * metrics.capacity_mwh),
                telemetry_freshness="fresh" if len(ordered_group) >= fresh_min_observations else "stale",
                first_observed_at=ordered_group[0].observed_at,
                last_observed_at=ordered_group[-1].observed_at,
            )
        )
    return snapshots


class PostgresBatteryTelemetryStore:
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
                    CREATE TABLE IF NOT EXISTS battery_telemetry_observations (
                        tenant_id TEXT NOT NULL,
                        observed_at TIMESTAMP NOT NULL,
                        current_soc DOUBLE PRECISION NOT NULL,
                        soh DOUBLE PRECISION NOT NULL,
                        power_mw DOUBLE PRECISION NOT NULL,
                        temperature_c DOUBLE PRECISION,
                        source TEXT NOT NULL,
                        source_kind TEXT NOT NULL,
                        raw_payload JSONB NOT NULL,
                        PRIMARY KEY (tenant_id, observed_at, source)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS battery_state_hourly_snapshots (
                        tenant_id TEXT NOT NULL,
                        snapshot_hour TIMESTAMP NOT NULL,
                        observation_count INTEGER NOT NULL,
                        soc_open DOUBLE PRECISION NOT NULL,
                        soc_close DOUBLE PRECISION NOT NULL,
                        soc_mean DOUBLE PRECISION NOT NULL,
                        soh_close DOUBLE PRECISION NOT NULL,
                        power_mw_mean DOUBLE PRECISION NOT NULL,
                        throughput_mwh DOUBLE PRECISION NOT NULL,
                        efc_delta DOUBLE PRECISION NOT NULL,
                        telemetry_freshness TEXT NOT NULL,
                        first_observed_at TIMESTAMP NOT NULL,
                        last_observed_at TIMESTAMP NOT NULL,
                        PRIMARY KEY (tenant_id, snapshot_hour)
                    )
                    """
                )
            connection.commit()

    def upsert_battery_telemetry(self, observations: Sequence[BatteryTelemetryObservation]) -> None:
        if not observations:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO battery_telemetry_observations (
                        tenant_id,
                        observed_at,
                        current_soc,
                        soh,
                        power_mw,
                        temperature_c,
                        source,
                        source_kind,
                        raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (tenant_id, observed_at, source)
                    DO UPDATE SET
                        current_soc = EXCLUDED.current_soc,
                        soh = EXCLUDED.soh,
                        power_mw = EXCLUDED.power_mw,
                        temperature_c = EXCLUDED.temperature_c,
                        source_kind = EXCLUDED.source_kind,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    [_observation_values(observation) for observation in observations],
                )
            connection.commit()

    def upsert_hourly_snapshots(self, snapshots: Sequence[BatteryStateHourlySnapshot]) -> None:
        if not snapshots:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO battery_state_hourly_snapshots (
                        tenant_id,
                        snapshot_hour,
                        observation_count,
                        soc_open,
                        soc_close,
                        soc_mean,
                        soh_close,
                        power_mw_mean,
                        throughput_mwh,
                        efc_delta,
                        telemetry_freshness,
                        first_observed_at,
                        last_observed_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, snapshot_hour)
                    DO UPDATE SET
                        observation_count = EXCLUDED.observation_count,
                        soc_open = EXCLUDED.soc_open,
                        soc_close = EXCLUDED.soc_close,
                        soc_mean = EXCLUDED.soc_mean,
                        soh_close = EXCLUDED.soh_close,
                        power_mw_mean = EXCLUDED.power_mw_mean,
                        throughput_mwh = EXCLUDED.throughput_mwh,
                        efc_delta = EXCLUDED.efc_delta,
                        telemetry_freshness = EXCLUDED.telemetry_freshness,
                        first_observed_at = EXCLUDED.first_observed_at,
                        last_observed_at = EXCLUDED.last_observed_at
                    """,
                    [_snapshot_values(snapshot) for snapshot in snapshots],
                )
            connection.commit()

    def list_battery_telemetry(self, *, tenant_id: str | None = None) -> list[BatteryTelemetryObservation]:
        where_clause = ""
        params: tuple[str, ...] = ()
        if tenant_id is not None:
            where_clause = "WHERE tenant_id = %s"
            params = (tenant_id,)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        tenant_id,
                        observed_at,
                        current_soc,
                        soh,
                        power_mw,
                        temperature_c,
                        source,
                        source_kind,
                        raw_payload
                    FROM battery_telemetry_observations
                    {where_clause}
                    ORDER BY tenant_id, observed_at
                    """,
                    params,
                )
                return [_observation_from_row(row) for row in cursor.fetchall()]

    def list_hourly_snapshots(self, *, tenant_id: str | None = None) -> list[BatteryStateHourlySnapshot]:
        where_clause = ""
        params: tuple[str, ...] = ()
        if tenant_id is not None:
            where_clause = "WHERE tenant_id = %s"
            params = (tenant_id,)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        tenant_id,
                        snapshot_hour,
                        observation_count,
                        soc_open,
                        soc_close,
                        soc_mean,
                        soh_close,
                        power_mw_mean,
                        throughput_mwh,
                        efc_delta,
                        telemetry_freshness,
                        first_observed_at,
                        last_observed_at
                    FROM battery_state_hourly_snapshots
                    {where_clause}
                    ORDER BY tenant_id, snapshot_hour
                    """,
                    params,
                )
                return [_snapshot_from_row(row) for row in cursor.fetchall()]

    def get_latest_battery_telemetry(self, *, tenant_id: str) -> BatteryTelemetryObservation | None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        tenant_id,
                        observed_at,
                        current_soc,
                        soh,
                        power_mw,
                        temperature_c,
                        source,
                        source_kind,
                        raw_payload
                    FROM battery_telemetry_observations
                    WHERE tenant_id = %s
                    ORDER BY observed_at DESC
                    LIMIT 1
                    """,
                    (tenant_id,),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return _observation_from_row(row)

    def get_latest_hourly_snapshot(self, *, tenant_id: str) -> BatteryStateHourlySnapshot | None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        tenant_id,
                        snapshot_hour,
                        observation_count,
                        soc_open,
                        soc_close,
                        soc_mean,
                        soh_close,
                        power_mw_mean,
                        throughput_mwh,
                        efc_delta,
                        telemetry_freshness,
                        first_observed_at,
                        last_observed_at
                    FROM battery_state_hourly_snapshots
                    WHERE tenant_id = %s
                    ORDER BY snapshot_hour DESC
                    LIMIT 1
                    """,
                    (tenant_id,),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return _snapshot_from_row(row)


def _truncate_to_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _observation_values(observation: BatteryTelemetryObservation) -> tuple[Any, ...]:
    return (
        observation.tenant_id,
        observation.observed_at,
        observation.current_soc,
        observation.soh,
        observation.power_mw,
        observation.temperature_c,
        observation.source,
        observation.source_kind,
        json.dumps(observation.raw_payload),
    )


def _snapshot_values(snapshot: BatteryStateHourlySnapshot) -> tuple[Any, ...]:
    return (
        snapshot.tenant_id,
        snapshot.snapshot_hour,
        snapshot.observation_count,
        snapshot.soc_open,
        snapshot.soc_close,
        snapshot.soc_mean,
        snapshot.soh_close,
        snapshot.power_mw_mean,
        snapshot.throughput_mwh,
        snapshot.efc_delta,
        snapshot.telemetry_freshness,
        snapshot.first_observed_at,
        snapshot.last_observed_at,
    )


def _observation_from_row(row: Sequence[Any]) -> BatteryTelemetryObservation:
    raw_payload = row[8]
    if not isinstance(raw_payload, dict):
        raw_payload = json.loads(str(raw_payload))
    return BatteryTelemetryObservation(
        tenant_id=str(row[0]),
        observed_at=_as_datetime(row[1]),
        current_soc=float(row[2]),
        soh=float(row[3]),
        power_mw=float(row[4]),
        temperature_c=None if row[5] is None else float(row[5]),
        source=str(row[6]),
        source_kind=_as_source_kind(row[7]),
        raw_payload=raw_payload,
    )


def _snapshot_from_row(row: Sequence[Any]) -> BatteryStateHourlySnapshot:
    return BatteryStateHourlySnapshot(
        tenant_id=str(row[0]),
        snapshot_hour=_as_datetime(row[1]),
        observation_count=int(row[2]),
        soc_open=float(row[3]),
        soc_close=float(row[4]),
        soc_mean=float(row[5]),
        soh_close=float(row[6]),
        power_mw_mean=float(row[7]),
        throughput_mwh=float(row[8]),
        efc_delta=float(row[9]),
        telemetry_freshness=_as_telemetry_freshness(row[10]),
        first_observed_at=_as_datetime(row[11]),
        last_observed_at=_as_datetime(row[12]),
    )


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value
    raise TypeError("database timestamp value must be a datetime.")


def _as_source_kind(value: Any) -> SourceKind:
    if value in {"observed", "synthetic"}:
        return value
    raise ValueError(f"Unsupported source_kind: {value}")


def _as_telemetry_freshness(value: Any) -> TelemetryFreshness:
    if value in {"fresh", "stale"}:
        return value
    raise ValueError(f"Unsupported telemetry_freshness: {value}")


def telemetry_observations_to_frame(observations: Iterable[BatteryTelemetryObservation]) -> Any:
    import polars as pl

    rows = [
        {
            "tenant_id": observation.tenant_id,
            "observed_at": observation.observed_at,
            "current_soc": observation.current_soc,
            "soh": observation.soh,
            "power_mw": observation.power_mw,
            "temperature_c": observation.temperature_c,
            "source": observation.source,
            "source_kind": observation.source_kind,
        }
        for observation in observations
    ]
    if not rows:
        return pl.DataFrame(schema={column_name: pl.Null for column_name in BATTERY_TELEMETRY_OBSERVATION_COLUMNS})
    return pl.DataFrame(rows).select(BATTERY_TELEMETRY_OBSERVATION_COLUMNS)


def hourly_snapshots_to_frame(snapshots: Iterable[BatteryStateHourlySnapshot]) -> Any:
    import polars as pl

    rows = [snapshot.model_dump() for snapshot in snapshots]
    if not rows:
        return pl.DataFrame(schema={column_name: pl.Null for column_name in BATTERY_STATE_HOURLY_SNAPSHOT_COLUMNS})
    return pl.DataFrame(rows).select(BATTERY_STATE_HOURLY_SNAPSHOT_COLUMNS)


@cache
def get_battery_telemetry_store() -> BatteryTelemetryStore:
    dsn = os.environ.get("SMART_ARBITRAGE_BATTERY_TELEMETRY_DSN") or os.environ.get("SMART_ARBITRAGE_MARKET_DATA_DSN")
    if dsn is None:
        return NullBatteryTelemetryStore()
    return PostgresBatteryTelemetryStore(dsn)
