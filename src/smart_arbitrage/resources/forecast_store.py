from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from functools import cache
import json
import os
from typing import Any, Protocol
from uuid import uuid4

import polars as pl


_LATEST_FORECAST_OBSERVATION_SCHEMA: dict[str, Any] = {
    "run_id": pl.Utf8,
    "model_name": pl.Utf8,
    "generated_at": pl.Datetime,
    "forecast_timestamp": pl.Datetime,
    "predicted_price_uah_mwh": pl.Float64,
    "prediction_payload": pl.Utf8,
}


class ForecastStore(Protocol):
    def upsert_forecast_run(
        self,
        *,
        model_name: str,
        forecast_frame: pl.DataFrame,
        point_prediction_column: str,
    ) -> str: ...

    def latest_forecast_observation_frame(
        self,
        *,
        model_names: Sequence[str],
        limit_per_model: int = 24,
    ) -> pl.DataFrame: ...


class NullForecastStore:
    def upsert_forecast_run(
        self,
        *,
        model_name: str,
        forecast_frame: pl.DataFrame,
        point_prediction_column: str,
    ) -> str:
        return f"{model_name}:not_persisted"

    def latest_forecast_observation_frame(
        self,
        *,
        model_names: Sequence[str],
        limit_per_model: int = 24,
    ) -> pl.DataFrame:
        return _empty_latest_forecast_observation_frame()


class InMemoryForecastStore:
    def __init__(self) -> None:
        self.summary_frame = pl.DataFrame()
        self.observation_frame = pl.DataFrame()
        self._run_order_by_id: dict[str, int] = {}
        self._run_sequence = 0

    def upsert_forecast_run(
        self,
        *,
        model_name: str,
        forecast_frame: pl.DataFrame,
        point_prediction_column: str,
    ) -> str:
        run_id = _forecast_run_id(model_name)
        self._run_sequence += 1
        self._run_order_by_id[run_id] = self._run_sequence
        summary_frame = _summary_frame(
            run_id=run_id,
            model_name=model_name,
            forecast_frame=forecast_frame,
            point_prediction_column=point_prediction_column,
        )
        observation_frame = _observation_frame(
            run_id=run_id,
            model_name=model_name,
            forecast_frame=forecast_frame,
            point_prediction_column=point_prediction_column,
        )
        self.summary_frame = _append_or_replace(self.summary_frame, summary_frame, subset=["run_id"])
        self.observation_frame = _append_or_replace(
            self.observation_frame,
            observation_frame,
            subset=["run_id", "forecast_timestamp"],
        )
        return run_id

    def latest_forecast_observation_frame(
        self,
        *,
        model_names: Sequence[str],
        limit_per_model: int = 24,
    ) -> pl.DataFrame:
        if self.summary_frame.is_empty() or self.observation_frame.is_empty() or not model_names:
            return _empty_latest_forecast_observation_frame()
        rows: list[pl.DataFrame] = []
        for model_name in model_names:
            model_summaries = self.summary_frame.filter(pl.col("model_name") == model_name)
            if model_summaries.is_empty():
                continue
            latest_run_id = max(
                (str(run_id) for run_id in model_summaries.select("run_id").to_series().to_list()),
                key=lambda run_id: self._run_order_by_id.get(run_id, -1),
            )
            latest_summary = model_summaries.filter(pl.col("run_id") == latest_run_id).head(1)
            latest_row = latest_summary.row(0, named=True)
            model_rows = (
                self.observation_frame
                .filter(pl.col("run_id") == latest_row["run_id"])
                .sort("forecast_timestamp")
                .head(limit_per_model)
                .with_columns(pl.lit(latest_row["generated_at"]).alias("generated_at"))
                .select(list(_LATEST_FORECAST_OBSERVATION_SCHEMA))
            )
            if model_rows.height:
                rows.append(model_rows)
        if not rows:
            return _empty_latest_forecast_observation_frame()
        return pl.concat(rows, how="vertical").select(list(_LATEST_FORECAST_OBSERVATION_SCHEMA))


class PostgresForecastStore:
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
                    CREATE TABLE IF NOT EXISTS forecast_run_summaries (
                        run_id TEXT PRIMARY KEY,
                        model_name TEXT NOT NULL,
                        generated_at TIMESTAMP NOT NULL,
                        horizon_rows INTEGER NOT NULL,
                        min_prediction_uah_mwh DOUBLE PRECISION NOT NULL,
                        max_prediction_uah_mwh DOUBLE PRECISION NOT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS price_forecast_observations (
                        run_id TEXT NOT NULL,
                        model_name TEXT NOT NULL,
                        forecast_timestamp TIMESTAMP NOT NULL,
                        predicted_price_uah_mwh DOUBLE PRECISION NOT NULL,
                        prediction_payload JSONB NOT NULL,
                        PRIMARY KEY (run_id, forecast_timestamp)
                    )
                    """
                )
            connection.commit()

    def upsert_forecast_run(
        self,
        *,
        model_name: str,
        forecast_frame: pl.DataFrame,
        point_prediction_column: str,
    ) -> str:
        run_id = _forecast_run_id(model_name)
        summary = _summary_frame(
            run_id=run_id,
            model_name=model_name,
            forecast_frame=forecast_frame,
            point_prediction_column=point_prediction_column,
        )
        observations = _observation_frame(
            run_id=run_id,
            model_name=model_name,
            forecast_frame=forecast_frame,
            point_prediction_column=point_prediction_column,
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if summary.height:
                    cursor.executemany(
                        """
                        INSERT INTO forecast_run_summaries (
                            run_id,
                            model_name,
                            generated_at,
                            horizon_rows,
                            min_prediction_uah_mwh,
                            max_prediction_uah_mwh
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id)
                        DO UPDATE SET
                            model_name = EXCLUDED.model_name,
                            generated_at = EXCLUDED.generated_at,
                            horizon_rows = EXCLUDED.horizon_rows,
                            min_prediction_uah_mwh = EXCLUDED.min_prediction_uah_mwh,
                            max_prediction_uah_mwh = EXCLUDED.max_prediction_uah_mwh
                        """,
                        [_summary_values(row) for row in summary.iter_rows(named=True)],
                    )
                if observations.height:
                    cursor.executemany(
                        """
                        INSERT INTO price_forecast_observations (
                            run_id,
                            model_name,
                            forecast_timestamp,
                            predicted_price_uah_mwh,
                            prediction_payload
                        )
                        VALUES (%s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (run_id, forecast_timestamp)
                        DO UPDATE SET
                            model_name = EXCLUDED.model_name,
                            predicted_price_uah_mwh = EXCLUDED.predicted_price_uah_mwh,
                            prediction_payload = EXCLUDED.prediction_payload
                        """,
                        [_observation_values(row) for row in observations.iter_rows(named=True)],
                    )
            connection.commit()
        return run_id

    def latest_forecast_observation_frame(
        self,
        *,
        model_names: Sequence[str],
        limit_per_model: int = 24,
    ) -> pl.DataFrame:
        if not model_names:
            return _empty_latest_forecast_observation_frame()
        placeholders = ", ".join(["%s"] * len(model_names))
        query = f"""
            WITH latest_runs AS (
                SELECT DISTINCT ON (model_name)
                    run_id,
                    model_name,
                    generated_at
                FROM forecast_run_summaries
                WHERE model_name IN ({placeholders})
                ORDER BY model_name, generated_at DESC, run_id DESC
            ),
            ranked_observations AS (
                SELECT
                    observations.run_id,
                    observations.model_name,
                    latest_runs.generated_at,
                    observations.forecast_timestamp,
                    observations.predicted_price_uah_mwh,
                    observations.prediction_payload::text AS prediction_payload,
                    ROW_NUMBER() OVER (
                        PARTITION BY observations.model_name
                        ORDER BY observations.forecast_timestamp
                    ) AS row_number
                FROM price_forecast_observations observations
                JOIN latest_runs
                    ON latest_runs.run_id = observations.run_id
            )
            SELECT
                run_id,
                model_name,
                generated_at,
                forecast_timestamp,
                predicted_price_uah_mwh,
                prediction_payload
            FROM ranked_observations
            WHERE row_number <= %s
            ORDER BY model_name, forecast_timestamp
        """
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, [*model_names, limit_per_model])
                records = cursor.fetchall()
        if not records:
            return _empty_latest_forecast_observation_frame()
        return pl.DataFrame(
            records,
            schema=list(_LATEST_FORECAST_OBSERVATION_SCHEMA),
            orient="row",
        )


def _forecast_run_id(model_name: str) -> str:
    return f"{model_name}:{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}:{uuid4().hex[:8]}"


def _summary_frame(
    *,
    run_id: str,
    model_name: str,
    forecast_frame: pl.DataFrame,
    point_prediction_column: str,
) -> pl.DataFrame:
    if forecast_frame.height == 0:
        return pl.DataFrame()
    _validate_forecast_frame(forecast_frame, point_prediction_column=point_prediction_column)
    predictions = forecast_frame.select(point_prediction_column).to_series()
    min_prediction: Any = predictions.min()
    max_prediction: Any = predictions.max()
    return pl.DataFrame(
        {
            "run_id": [run_id],
            "model_name": [model_name],
            "generated_at": [datetime.now(UTC)],
            "horizon_rows": [forecast_frame.height],
            "min_prediction_uah_mwh": [float(min_prediction)],
            "max_prediction_uah_mwh": [float(max_prediction)],
        }
    )


def _observation_frame(
    *,
    run_id: str,
    model_name: str,
    forecast_frame: pl.DataFrame,
    point_prediction_column: str,
) -> pl.DataFrame:
    if forecast_frame.height == 0:
        return pl.DataFrame()
    _validate_forecast_frame(forecast_frame, point_prediction_column=point_prediction_column)
    rows: list[dict[str, Any]] = []
    for row in forecast_frame.iter_rows(named=True):
        rows.append(
            {
                "run_id": run_id,
                "model_name": model_name,
                "forecast_timestamp": row["forecast_timestamp"],
                "predicted_price_uah_mwh": float(row[point_prediction_column]),
                "prediction_payload": json.dumps(row, default=str),
            }
        )
    return pl.DataFrame(rows)


def _empty_latest_forecast_observation_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_LATEST_FORECAST_OBSERVATION_SCHEMA)


def _validate_forecast_frame(forecast_frame: pl.DataFrame, *, point_prediction_column: str) -> None:
    required_columns = {"forecast_timestamp", point_prediction_column}
    missing_columns = required_columns.difference(forecast_frame.columns)
    if missing_columns:
        raise ValueError(f"forecast frame is missing required columns: {sorted(missing_columns)}")


def _append_or_replace(base_frame: pl.DataFrame, incoming_frame: pl.DataFrame, *, subset: list[str]) -> pl.DataFrame:
    if incoming_frame.height == 0:
        return base_frame
    if base_frame.height == 0:
        return incoming_frame
    return pl.concat([base_frame, incoming_frame]).unique(subset=subset, keep="last")


def _summary_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["run_id"],
        row["model_name"],
        row["generated_at"],
        row["horizon_rows"],
        row["min_prediction_uah_mwh"],
        row["max_prediction_uah_mwh"],
    )


def _observation_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["run_id"],
        row["model_name"],
        row["forecast_timestamp"],
        row["predicted_price_uah_mwh"],
        row["prediction_payload"],
    )


@cache
def get_forecast_store() -> ForecastStore:
    dsn = os.environ.get("SMART_ARBITRAGE_FORECAST_DSN") or os.environ.get("SMART_ARBITRAGE_MARKET_DATA_DSN")
    if dsn is None:
        return NullForecastStore()
    return PostgresForecastStore(dsn)
