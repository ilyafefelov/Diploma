from __future__ import annotations

from functools import cache
import json
import os
from typing import Any, Protocol

import polars as pl


class StrategyEvaluationStore(Protocol):
    def upsert_evaluation_frame(self, evaluation_frame: pl.DataFrame) -> None: ...

    def latest_evaluation_frame(self, *, tenant_id: str) -> pl.DataFrame: ...

    def latest_real_data_benchmark_frame(self, *, tenant_id: str) -> pl.DataFrame: ...


class NullStrategyEvaluationStore:
    def upsert_evaluation_frame(self, evaluation_frame: pl.DataFrame) -> None:
        return None

    def latest_evaluation_frame(self, *, tenant_id: str) -> pl.DataFrame:
        return pl.DataFrame()

    def latest_real_data_benchmark_frame(self, *, tenant_id: str) -> pl.DataFrame:
        return pl.DataFrame()


class InMemoryStrategyEvaluationStore:
    def __init__(self) -> None:
        self.evaluation_frame = pl.DataFrame()

    def upsert_evaluation_frame(self, evaluation_frame: pl.DataFrame) -> None:
        self.evaluation_frame = _append_or_replace(
            self.evaluation_frame,
            evaluation_frame,
            subset=["evaluation_id", "tenant_id", "forecast_model_name"],
        )

    def latest_evaluation_frame(self, *, tenant_id: str) -> pl.DataFrame:
        return _latest_tenant_frame(
            self.evaluation_frame,
            tenant_id=tenant_id,
            strategy_kind="forecast_driven_lp",
        )

    def latest_real_data_benchmark_frame(self, *, tenant_id: str) -> pl.DataFrame:
        return _latest_tenant_frame(
            self.evaluation_frame,
            tenant_id=tenant_id,
            strategy_kind="real_data_rolling_origin_benchmark",
        )


class PostgresStrategyEvaluationStore:
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
					CREATE TABLE IF NOT EXISTS forecast_strategy_evaluations (
					    evaluation_id TEXT NOT NULL,
					    tenant_id TEXT NOT NULL,
					    forecast_model_name TEXT NOT NULL,
					    strategy_kind TEXT NOT NULL,
					    market_venue TEXT NOT NULL,
					    anchor_timestamp TIMESTAMP NOT NULL,
					    generated_at TIMESTAMPTZ NOT NULL,
					    horizon_hours INTEGER NOT NULL,
					    starting_soc_fraction DOUBLE PRECISION NOT NULL,
					    starting_soc_source TEXT NOT NULL,
					    decision_value_uah DOUBLE PRECISION NOT NULL,
					    forecast_objective_value_uah DOUBLE PRECISION NOT NULL,
					    oracle_value_uah DOUBLE PRECISION NOT NULL,
					    regret_uah DOUBLE PRECISION NOT NULL,
					    regret_ratio DOUBLE PRECISION NOT NULL,
					    total_degradation_penalty_uah DOUBLE PRECISION NOT NULL,
					    total_throughput_mwh DOUBLE PRECISION NOT NULL,
					    committed_action TEXT NOT NULL,
					    committed_power_mw DOUBLE PRECISION NOT NULL,
					    rank_by_regret INTEGER NOT NULL,
					    evaluation_payload JSONB NOT NULL,
					    PRIMARY KEY (evaluation_id, tenant_id, forecast_model_name)
					)
					"""
                )
            connection.commit()

    def upsert_evaluation_frame(self, evaluation_frame: pl.DataFrame) -> None:
        if evaluation_frame.height == 0:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
					INSERT INTO forecast_strategy_evaluations (
					    evaluation_id,
					    tenant_id,
					    forecast_model_name,
					    strategy_kind,
					    market_venue,
					    anchor_timestamp,
					    generated_at,
					    horizon_hours,
					    starting_soc_fraction,
					    starting_soc_source,
					    decision_value_uah,
					    forecast_objective_value_uah,
					    oracle_value_uah,
					    regret_uah,
					    regret_ratio,
					    total_degradation_penalty_uah,
					    total_throughput_mwh,
					    committed_action,
					    committed_power_mw,
					    rank_by_regret,
					    evaluation_payload
					)
					VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
					ON CONFLICT (evaluation_id, tenant_id, forecast_model_name)
					DO UPDATE SET
					    strategy_kind = EXCLUDED.strategy_kind,
					    market_venue = EXCLUDED.market_venue,
					    anchor_timestamp = EXCLUDED.anchor_timestamp,
					    generated_at = EXCLUDED.generated_at,
					    horizon_hours = EXCLUDED.horizon_hours,
					    starting_soc_fraction = EXCLUDED.starting_soc_fraction,
					    starting_soc_source = EXCLUDED.starting_soc_source,
					    decision_value_uah = EXCLUDED.decision_value_uah,
					    forecast_objective_value_uah = EXCLUDED.forecast_objective_value_uah,
					    oracle_value_uah = EXCLUDED.oracle_value_uah,
					    regret_uah = EXCLUDED.regret_uah,
					    regret_ratio = EXCLUDED.regret_ratio,
					    total_degradation_penalty_uah = EXCLUDED.total_degradation_penalty_uah,
					    total_throughput_mwh = EXCLUDED.total_throughput_mwh,
					    committed_action = EXCLUDED.committed_action,
					    committed_power_mw = EXCLUDED.committed_power_mw,
					    rank_by_regret = EXCLUDED.rank_by_regret,
					    evaluation_payload = EXCLUDED.evaluation_payload
					""",
                    [
                        _evaluation_values(row)
                        for row in evaluation_frame.iter_rows(named=True)
                    ],
                )
            connection.commit()

    def latest_evaluation_frame(self, *, tenant_id: str) -> pl.DataFrame:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
					SELECT *
					FROM forecast_strategy_evaluations
					WHERE tenant_id = %s
					  AND strategy_kind = 'forecast_driven_lp'
					  AND generated_at = (
					      SELECT max(generated_at)
					      FROM forecast_strategy_evaluations
					      WHERE tenant_id = %s
					        AND strategy_kind = 'forecast_driven_lp'
					  )
					ORDER BY rank_by_regret, forecast_model_name
					""",
                    (tenant_id, tenant_id),
                )
                rows = cursor.fetchall()
        return pl.DataFrame([_normalize_row(dict(row)) for row in rows])

    def latest_real_data_benchmark_frame(self, *, tenant_id: str) -> pl.DataFrame:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
					SELECT *
					FROM forecast_strategy_evaluations
					WHERE tenant_id = %s
					  AND strategy_kind = 'real_data_rolling_origin_benchmark'
					  AND generated_at = (
					      SELECT max(generated_at)
					      FROM forecast_strategy_evaluations
					      WHERE tenant_id = %s
					        AND strategy_kind = 'real_data_rolling_origin_benchmark'
					  )
					ORDER BY anchor_timestamp, rank_by_regret, forecast_model_name
					""",
                    (tenant_id, tenant_id),
                )
                rows = cursor.fetchall()
        return pl.DataFrame([_normalize_row(dict(row)) for row in rows])


def _append_or_replace(
    base_frame: pl.DataFrame, incoming_frame: pl.DataFrame, *, subset: list[str]
) -> pl.DataFrame:
    if incoming_frame.height == 0:
        return base_frame
    if base_frame.height == 0:
        return incoming_frame.clone()
    return pl.concat([base_frame, incoming_frame]).unique(subset=subset, keep="last")


def _latest_tenant_frame(
    frame: pl.DataFrame,
    *,
    tenant_id: str,
    strategy_kind: str,
) -> pl.DataFrame:
    if frame.height == 0:
        return pl.DataFrame()
    tenant_frame = frame.filter(
        (pl.col("tenant_id") == tenant_id)
        & (pl.col("strategy_kind") == strategy_kind)
    )
    if tenant_frame.height == 0:
        return pl.DataFrame()
    latest_generated_at = tenant_frame.select("generated_at").max().item()
    sort_columns = ["rank_by_regret", "forecast_model_name"]
    if strategy_kind == "real_data_rolling_origin_benchmark":
        sort_columns = ["anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    return tenant_frame.filter(pl.col("generated_at") == latest_generated_at).sort(sort_columns)


def _evaluation_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["evaluation_id"],
        row["tenant_id"],
        row["forecast_model_name"],
        row["strategy_kind"],
        row["market_venue"],
        row["anchor_timestamp"],
        row["generated_at"],
        row["horizon_hours"],
        row["starting_soc_fraction"],
        row["starting_soc_source"],
        row["decision_value_uah"],
        row["forecast_objective_value_uah"],
        row["oracle_value_uah"],
        row["regret_uah"],
        row["regret_ratio"],
        row["total_degradation_penalty_uah"],
        row["total_throughput_mwh"],
        row["committed_action"],
        row["committed_power_mw"],
        row["rank_by_regret"],
        json.dumps(row["evaluation_payload"], default=str),
    )


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    if isinstance(payload, str):
        row["evaluation_payload"] = json.loads(payload)
    return row


@cache
def get_strategy_evaluation_store() -> StrategyEvaluationStore:
    dsn = os.environ.get("SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN") or os.environ.get(
        "SMART_ARBITRAGE_MARKET_DATA_DSN"
    )
    if dsn is None:
        return NullStrategyEvaluationStore()
    return PostgresStrategyEvaluationStore(dsn)
