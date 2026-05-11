from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import polars as pl

from smart_arbitrage.resources.strategy_evaluation_store import (
    InMemoryStrategyEvaluationStore,
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


def test_in_memory_strategy_store_normalizes_mixed_generated_at_timezones() -> None:
    store = InMemoryStrategyEvaluationStore()
    store.upsert_evaluation_frame(
        pl.DataFrame(
            {
                "evaluation_id": ["old"],
                "tenant_id": ["client_003_dnipro_factory"],
                "forecast_model_name": ["strict_similar_day"],
                "strategy_kind": ["dfl_trajectory_value_selector_strict_lp_benchmark"],
                "anchor_timestamp": [datetime(2026, 4, 12, 23)],
                "generated_at": [datetime(2026, 5, 5, 12)],
                "rank_by_regret": [1],
            }
        )
    )

    store.upsert_evaluation_frame(
        pl.DataFrame(
            {
                "evaluation_id": ["new"],
                "tenant_id": ["client_003_dnipro_factory"],
                "forecast_model_name": ["dfl_trajectory_value_selector_v1_tft_silver_v0"],
                "strategy_kind": ["dfl_trajectory_value_selector_strict_lp_benchmark"],
                "anchor_timestamp": [datetime(2026, 4, 13, 23)],
                "generated_at": [datetime(2026, 5, 6, 12, tzinfo=UTC)],
                "rank_by_regret": [2],
            }
        )
    )

    latest_frame = store.latest_strategy_kind_frame(
        tenant_id="client_003_dnipro_factory",
        strategy_kind="dfl_trajectory_value_selector_strict_lp_benchmark",
    )

    assert latest_frame.height == 1
    assert latest_frame["evaluation_id"].to_list() == ["new"]
    assert latest_frame.schema["generated_at"] == pl.Datetime("us")


def test_in_memory_strategy_store_reads_resumable_batch_by_generated_at() -> None:
    store = InMemoryStrategyEvaluationStore()
    batch_generated_at = datetime(2026, 5, 11, 12, tzinfo=UTC)
    other_generated_at = datetime(2026, 5, 11, 13, tzinfo=UTC)
    store.upsert_evaluation_frame(
        pl.DataFrame(
            {
                "evaluation_id": ["batch-a", "batch-b", "other"],
                "tenant_id": [
                    "client_003_dnipro_factory",
                    "client_004_kharkiv_hospital",
                    "client_003_dnipro_factory",
                ],
                "forecast_model_name": [
                    "nbeatsx_official_v0",
                    "tft_official_v0",
                    "nbeatsx_official_v0",
                ],
                "strategy_kind": [
                    "official_forecast_rolling_origin_benchmark",
                    "official_forecast_rolling_origin_benchmark",
                    "official_forecast_rolling_origin_benchmark",
                ],
                "anchor_timestamp": [
                    datetime(2026, 4, 1, 23),
                    datetime(2026, 4, 2, 23),
                    datetime(2026, 4, 3, 23),
                ],
                "generated_at": [batch_generated_at, batch_generated_at, other_generated_at],
                "rank_by_regret": [2, 3, 2],
            }
        )
    )

    batch_frame = store.strategy_kind_frame_for_generated_at(
        strategy_kind="official_forecast_rolling_origin_benchmark",
        generated_at=batch_generated_at,
    )

    assert batch_frame["evaluation_id"].to_list() == ["batch-a", "batch-b"]
    assert batch_frame.schema["generated_at"] == pl.Datetime("us")
