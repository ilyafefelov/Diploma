from __future__ import annotations

from typing import Any

from smart_arbitrage.resources.strategy_evaluation_store import (
    PostgresStrategyEvaluationStore,
)


class _RecordingCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def __enter__(self) -> _RecordingCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[Any, ...] | None = None) -> None:
        self.statements.append(statement)


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


def test_postgres_strategy_evaluation_schema_adds_latest_read_index(monkeypatch) -> None:
    connection = _RecordingConnection()
    monkeypatch.setattr(
        PostgresStrategyEvaluationStore,
        "_connect",
        lambda self: connection,
    )

    PostgresStrategyEvaluationStore("postgresql://example")

    schema_sql = "\n".join(connection.cursor_instance.statements)
    normalized_schema_sql = " ".join(schema_sql.split())
    assert "CREATE TABLE IF NOT EXISTS forecast_strategy_evaluations" in schema_sql
    assert "CREATE INDEX IF NOT EXISTS forecast_strategy_evaluations_latest_read_idx" in schema_sql
    assert (
        "tenant_id, strategy_kind, generated_at DESC, anchor_timestamp, "
        "rank_by_regret, forecast_model_name"
    ) in normalized_schema_sql
    assert connection.commit_count == 1
