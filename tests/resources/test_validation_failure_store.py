from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from smart_arbitrage.resources.validation_failure_store import (
    InMemoryValidationFailureStore,
    PostgresValidationFailureStore,
    ValidationFailureRecord,
    ValidationStage,
)


class _RecordingCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.params: list[tuple[Any, ...] | None] = []

    def __enter__(self) -> _RecordingCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[Any, ...] | None = None) -> None:
        self.statements.append(statement)
        self.params.append(params)

    def fetchall(self) -> list[dict[str, Any]]:
        return []


class _RecordingConnection:
    def __init__(self) -> None:
        self.cursor_instance = _RecordingCursor()
        self.commit_count = 0

    def __enter__(self) -> _RecordingConnection:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self) -> _RecordingCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_count += 1


def test_postgres_validation_failure_store_creates_validation_failures_table(
    monkeypatch,
) -> None:
    connection = _RecordingConnection()
    monkeypatch.setattr(
        PostgresValidationFailureStore,
        "_connect",
        lambda self: connection,
    )

    PostgresValidationFailureStore("postgresql://example")

    schema_sql = "\n".join(connection.cursor_instance.statements)
    assert "CREATE TABLE IF NOT EXISTS validation_failures" in schema_sql
    assert "validation_stage TEXT NOT NULL" in schema_sql
    assert "canonical_outcome TEXT NOT NULL" in schema_sql
    assert "payload_json JSONB NOT NULL" in schema_sql
    assert connection.commit_count == 1


def test_in_memory_validation_failure_store_returns_latest_failure_by_stage() -> None:
    store = InMemoryValidationFailureStore()
    older_record = _record(
        failure_id="older",
        validation_stage=ValidationStage.DISPATCH_COMMAND,
        canonical_outcome="HOLD",
        created_at=datetime(2026, 5, 23, 10, tzinfo=UTC),
    )
    newer_record = _record(
        failure_id="newer",
        validation_stage=ValidationStage.PROPOSED_BID,
        canonical_outcome="NO_BID",
        created_at=datetime(2026, 5, 23, 11, tzinfo=UTC),
    )
    store.append_failure(older_record)
    store.append_failure(newer_record)

    assert store.latest_failure(tenant_id="tenant") == newer_record
    assert (
        store.latest_failure(
            tenant_id="tenant",
            validation_stage=ValidationStage.DISPATCH_COMMAND,
        )
        == older_record
    )


def _record(
    *,
    failure_id: str,
    validation_stage: ValidationStage,
    canonical_outcome,
    created_at: datetime,
) -> ValidationFailureRecord:
    return ValidationFailureRecord(
        failure_id=failure_id,
        tenant_id="tenant",
        validation_stage=validation_stage,
        contract_type="ProposedBid" if validation_stage == ValidationStage.PROPOSED_BID else "DispatchCommand",
        canonical_outcome=canonical_outcome,
        venue="DAM",
        interval_start=datetime(2026, 5, 24, 9, tzinfo=UTC),
        duration_minutes=60,
        failure_reason="blocked",
        payload={},
        created_at=created_at,
    )
