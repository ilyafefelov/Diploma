from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
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
    "market_venue": pl.Utf8,
    "training_cutoff": pl.Datetime,
    "feature_cutoff": pl.Datetime,
    "horizon_start": pl.Datetime,
    "horizon_end": pl.Datetime,
    "source_window_start": pl.Datetime,
    "source_window_end": pl.Datetime,
    "forecast_timestamp": pl.Datetime,
    "predicted_price_uah_mwh": pl.Float64,
    "prediction_payload": pl.Utf8,
}
_FORECAST_RUN_METADATA_COLUMNS: tuple[str, ...] = (
    "generated_at",
    "market_venue",
    "training_cutoff",
    "feature_cutoff",
    "horizon_start",
    "horizon_end",
    "source_window_start",
    "source_window_end",
)


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
                .with_columns(_summary_metadata_columns(latest_row))
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
                        market_venue TEXT,
                        training_cutoff TIMESTAMP,
                        feature_cutoff TIMESTAMP,
                        horizon_start TIMESTAMP,
                        horizon_end TIMESTAMP,
                        source_window_start TIMESTAMP,
                        source_window_end TIMESTAMP,
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
                for column_name, column_type in (
                    ("market_venue", "TEXT"),
                    ("training_cutoff", "TIMESTAMP"),
                    ("feature_cutoff", "TIMESTAMP"),
                    ("horizon_start", "TIMESTAMP"),
                    ("horizon_end", "TIMESTAMP"),
                    ("source_window_start", "TIMESTAMP"),
                    ("source_window_end", "TIMESTAMP"),
                ):
                    cursor.execute(
                        f"ALTER TABLE forecast_run_summaries ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
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
                            market_venue,
                            training_cutoff,
                            feature_cutoff,
                            horizon_start,
                            horizon_end,
                            source_window_start,
                            source_window_end,
                            horizon_rows,
                            min_prediction_uah_mwh,
                            max_prediction_uah_mwh
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id)
                        DO UPDATE SET
                            model_name = EXCLUDED.model_name,
                            generated_at = EXCLUDED.generated_at,
                            market_venue = EXCLUDED.market_venue,
                            training_cutoff = EXCLUDED.training_cutoff,
                            feature_cutoff = EXCLUDED.feature_cutoff,
                            horizon_start = EXCLUDED.horizon_start,
                            horizon_end = EXCLUDED.horizon_end,
                            source_window_start = EXCLUDED.source_window_start,
                            source_window_end = EXCLUDED.source_window_end,
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
                    generated_at,
                    market_venue,
                    training_cutoff,
                    feature_cutoff,
                    horizon_start,
                    horizon_end,
                    source_window_start,
                    source_window_end
                FROM forecast_run_summaries
                WHERE model_name IN ({placeholders})
                ORDER BY model_name, generated_at DESC, run_id DESC
            ),
            ranked_observations AS (
                SELECT
                    observations.run_id,
                    observations.model_name,
                    latest_runs.generated_at,
                    latest_runs.market_venue,
                    latest_runs.training_cutoff,
                    latest_runs.feature_cutoff,
                    latest_runs.horizon_start,
                    latest_runs.horizon_end,
                    latest_runs.source_window_start,
                    latest_runs.source_window_end,
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
                market_venue,
                training_cutoff,
                feature_cutoff,
                horizon_start,
                horizon_end,
                source_window_start,
                source_window_end,
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
    metadata = _forecast_run_metadata(
        model_name=model_name,
        forecast_frame=forecast_frame,
    )
    return pl.DataFrame(
        {
            "run_id": [run_id],
            "model_name": [model_name],
            **{column: [metadata[column]] for column in _FORECAST_RUN_METADATA_COLUMNS},
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


def _forecast_run_metadata(*, model_name: str, forecast_frame: pl.DataFrame) -> dict[str, Any]:
    forecast_timestamps = _frame_datetime_values(forecast_frame, "forecast_timestamp")
    if not forecast_timestamps:
        raise ValueError("forecast frame is missing forecast timestamps")
    horizon_start = min(forecast_timestamps)
    horizon_end = max(forecast_timestamps)
    default_cutoff = horizon_start - timedelta(hours=1)
    training_cutoff = _max_frame_datetime_value(forecast_frame, "training_cutoff") or default_cutoff
    feature_cutoff = _max_frame_datetime_value(forecast_frame, "feature_cutoff") or training_cutoff
    return {
        "generated_at": _max_frame_datetime_value(forecast_frame, "generated_at") or datetime.now(UTC),
        "market_venue": _frame_text_value(forecast_frame, "market_venue") or _infer_market_venue_from_model_name(model_name),
        "training_cutoff": training_cutoff,
        "feature_cutoff": feature_cutoff,
        "horizon_start": _min_frame_datetime_value(forecast_frame, "horizon_start") or horizon_start,
        "horizon_end": _max_frame_datetime_value(forecast_frame, "horizon_end") or horizon_end,
        "source_window_start": _min_frame_datetime_value(forecast_frame, "source_window_start") or training_cutoff,
        "source_window_end": _max_frame_datetime_value(forecast_frame, "source_window_end") or training_cutoff,
    }


def _summary_metadata_columns(row: dict[str, Any]) -> list[pl.Expr]:
    return [
        pl.lit(row["generated_at"], dtype=pl.Datetime).alias("generated_at"),
        pl.lit(row.get("market_venue"), dtype=pl.Utf8).alias("market_venue"),
        pl.lit(row.get("training_cutoff"), dtype=pl.Datetime).alias("training_cutoff"),
        pl.lit(row.get("feature_cutoff"), dtype=pl.Datetime).alias("feature_cutoff"),
        pl.lit(row.get("horizon_start"), dtype=pl.Datetime).alias("horizon_start"),
        pl.lit(row.get("horizon_end"), dtype=pl.Datetime).alias("horizon_end"),
        pl.lit(row.get("source_window_start"), dtype=pl.Datetime).alias("source_window_start"),
        pl.lit(row.get("source_window_end"), dtype=pl.Datetime).alias("source_window_end"),
    ]


def _frame_datetime_values(forecast_frame: pl.DataFrame, column_name: str) -> list[datetime]:
    if column_name not in forecast_frame.columns:
        return []
    values: list[datetime] = []
    for raw_value in forecast_frame.select(column_name).to_series().to_list():
        parsed_value = _optional_datetime(raw_value)
        if parsed_value is not None:
            values.append(parsed_value)
    return values


def _min_frame_datetime_value(forecast_frame: pl.DataFrame, column_name: str) -> datetime | None:
    values = _frame_datetime_values(forecast_frame, column_name)
    return min(values) if values else None


def _max_frame_datetime_value(forecast_frame: pl.DataFrame, column_name: str) -> datetime | None:
    values = _frame_datetime_values(forecast_frame, column_name)
    return max(values) if values else None


def _optional_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def _frame_text_value(forecast_frame: pl.DataFrame, column_name: str) -> str | None:
    if column_name not in forecast_frame.columns:
        return None
    for value in forecast_frame.select(column_name).to_series().to_list():
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text.upper()
    return None


def _infer_market_venue_from_model_name(model_name: str) -> str:
    return "IDM" if "_idm_" in model_name.lower() or model_name.lower().endswith("_idm_v0") else "DAM"


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
        row["market_venue"],
        row["training_cutoff"],
        row["feature_cutoff"],
        row["horizon_start"],
        row["horizon_end"],
        row["source_window_start"],
        row["source_window_end"],
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
