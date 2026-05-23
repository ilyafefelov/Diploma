from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from functools import cache
import json
import os
from typing import Any, Literal, Protocol

CanonicalValidationOutcome = Literal["NO_BID", "HOLD"]


class ValidationStage(StrEnum):
    PROPOSED_BID = "proposed_bid"
    DISPATCH_COMMAND = "dispatch_command"


@dataclass(frozen=True, slots=True)
class ValidationFailureRecord:
    failure_id: str
    tenant_id: str
    validation_stage: ValidationStage
    contract_type: str
    canonical_outcome: CanonicalValidationOutcome
    venue: str | None
    interval_start: datetime | None
    duration_minutes: int | None
    failure_reason: str
    payload: dict[str, Any]
    created_at: datetime


class ValidationFailureStore(Protocol):
    def append_failure(self, record: ValidationFailureRecord) -> None: ...

    def latest_failure(
        self,
        *,
        tenant_id: str,
        validation_stage: ValidationStage | None = None,
    ) -> ValidationFailureRecord | None: ...

    def latest_failures(
        self,
        *,
        tenant_id: str,
        limit: int = 200,
    ) -> list[ValidationFailureRecord]: ...


class NullValidationFailureStore:
    def append_failure(self, record: ValidationFailureRecord) -> None:
        return None

    def latest_failure(
        self,
        *,
        tenant_id: str,
        validation_stage: ValidationStage | None = None,
    ) -> ValidationFailureRecord | None:
        return None

    def latest_failures(
        self,
        *,
        tenant_id: str,
        limit: int = 200,
    ) -> list[ValidationFailureRecord]:
        return []


class InMemoryValidationFailureStore:
    def __init__(self) -> None:
        self.records: list[ValidationFailureRecord] = []

    def append_failure(self, record: ValidationFailureRecord) -> None:
        self.records = [
            existing for existing in self.records if existing.failure_id != record.failure_id
        ]
        self.records.append(record)

    def latest_failure(
        self,
        *,
        tenant_id: str,
        validation_stage: ValidationStage | None = None,
    ) -> ValidationFailureRecord | None:
        rows = self._tenant_records(tenant_id=tenant_id, validation_stage=validation_stage)
        if not rows:
            return None
        return max(rows, key=lambda row: row.created_at)

    def latest_failures(
        self,
        *,
        tenant_id: str,
        limit: int = 200,
    ) -> list[ValidationFailureRecord]:
        rows = self._tenant_records(tenant_id=tenant_id, validation_stage=None)
        return sorted(rows, key=lambda row: row.created_at, reverse=True)[:limit]

    def _tenant_records(
        self,
        *,
        tenant_id: str,
        validation_stage: ValidationStage | None,
    ) -> list[ValidationFailureRecord]:
        return [
            record
            for record in self.records
            if record.tenant_id == tenant_id
            and (validation_stage is None or record.validation_stage == validation_stage)
        ]


class PostgresValidationFailureStore:
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
                    CREATE TABLE IF NOT EXISTS validation_failures (
                        failure_id TEXT PRIMARY KEY,
                        tenant_id TEXT NOT NULL,
                        validation_stage TEXT NOT NULL,
                        contract_type TEXT NOT NULL,
                        canonical_outcome TEXT NOT NULL,
                        venue TEXT,
                        interval_start TIMESTAMPTZ,
                        duration_minutes INTEGER,
                        failure_reason TEXT NOT NULL,
                        payload_json JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    )
                    """
                )
            connection.commit()

    def append_failure(self, record: ValidationFailureRecord) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO validation_failures (
                        failure_id,
                        tenant_id,
                        validation_stage,
                        contract_type,
                        canonical_outcome,
                        venue,
                        interval_start,
                        duration_minutes,
                        failure_reason,
                        payload_json,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (failure_id)
                    DO UPDATE SET
                        tenant_id = EXCLUDED.tenant_id,
                        validation_stage = EXCLUDED.validation_stage,
                        contract_type = EXCLUDED.contract_type,
                        canonical_outcome = EXCLUDED.canonical_outcome,
                        venue = EXCLUDED.venue,
                        interval_start = EXCLUDED.interval_start,
                        duration_minutes = EXCLUDED.duration_minutes,
                        failure_reason = EXCLUDED.failure_reason,
                        payload_json = EXCLUDED.payload_json,
                        created_at = EXCLUDED.created_at
                    """,
                    (
                        record.failure_id,
                        record.tenant_id,
                        record.validation_stage.value,
                        record.contract_type,
                        record.canonical_outcome,
                        record.venue,
                        record.interval_start,
                        record.duration_minutes,
                        record.failure_reason,
                        _payload_to_json(record.payload),
                        record.created_at,
                    ),
                )
            connection.commit()

    def latest_failure(
        self,
        *,
        tenant_id: str,
        validation_stage: ValidationStage | None = None,
    ) -> ValidationFailureRecord | None:
        rows = self.latest_failures(tenant_id=tenant_id, limit=500)
        for row in rows:
            if validation_stage is None or row.validation_stage == validation_stage:
                return row
        return None

    def latest_failures(
        self,
        *,
        tenant_id: str,
        limit: int = 200,
    ) -> list[ValidationFailureRecord]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        failure_id,
                        tenant_id,
                        validation_stage,
                        contract_type,
                        canonical_outcome,
                        venue,
                        interval_start,
                        duration_minutes,
                        failure_reason,
                        payload_json,
                        created_at
                    FROM validation_failures
                    WHERE tenant_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (tenant_id, limit),
                )
                rows = cursor.fetchall()

        return [_record_from_row(row) for row in rows]


def _record_from_row(row: dict[str, Any]) -> ValidationFailureRecord:
    payload = row["payload_json"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    return ValidationFailureRecord(
        failure_id=row["failure_id"],
        tenant_id=row["tenant_id"],
        validation_stage=ValidationStage(row["validation_stage"]),
        contract_type=row["contract_type"],
        canonical_outcome=row["canonical_outcome"],
        venue=row["venue"],
        interval_start=row["interval_start"],
        duration_minutes=row["duration_minutes"],
        failure_reason=row["failure_reason"],
        payload=payload,
        created_at=row["created_at"],
    )


def _payload_to_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=_json_default)


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


@cache
def get_validation_failure_store() -> ValidationFailureStore:
    dsn = os.getenv("SMART_ARBITRAGE_VALIDATION_FAILURE_DSN", "").strip()
    if not dsn:
        return NullValidationFailureStore()
    return PostgresValidationFailureStore(dsn)


__all__ = [
    "CanonicalValidationOutcome",
    "InMemoryValidationFailureStore",
    "NullValidationFailureStore",
    "PostgresValidationFailureStore",
    "ValidationFailureRecord",
    "ValidationFailureStore",
    "ValidationStage",
    "get_validation_failure_store",
]
