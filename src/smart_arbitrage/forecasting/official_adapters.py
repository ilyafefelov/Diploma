"""Optional official backend adapters for SOTA NBEATSx and TFT experiments."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Final

import polars as pl


OFFICIAL_FORECAST_COLUMNS: Final[tuple[str, ...]] = (
    "model_name",
    "model_family",
    "backend_name",
    "backend_status",
    "unique_id",
    "forecast_timestamp",
    "predicted_price_uah_mwh",
    "predicted_price_p10_uah_mwh",
    "predicted_price_p50_uah_mwh",
    "predicted_price_p90_uah_mwh",
    "prediction_interval_kind",
    "training_rows",
    "horizon_rows",
    "adapter_scope",
)

_OFFICIAL_FORECAST_SCHEMA: Final[dict[str, Any]] = {
    "model_name": pl.Utf8,
    "model_family": pl.Utf8,
    "backend_name": pl.Utf8,
    "backend_status": pl.Utf8,
    "unique_id": pl.Utf8,
    "forecast_timestamp": pl.Datetime,
    "predicted_price_uah_mwh": pl.Float64,
    "predicted_price_p10_uah_mwh": pl.Float64,
    "predicted_price_p50_uah_mwh": pl.Float64,
    "predicted_price_p90_uah_mwh": pl.Float64,
    "prediction_interval_kind": pl.Utf8,
    "training_rows": pl.Int64,
    "horizon_rows": pl.Int64,
    "adapter_scope": pl.Utf8,
}

_Importer = Callable[[str], object]


@dataclass(frozen=True)
class OfficialBackendStatus:
    backend_name: str
    package_name: str
    available: bool
    reason: str


class OfficialForecastAdapterError(RuntimeError):
    """Raised when an installed official backend cannot produce forecast rows."""


def inspect_official_forecast_backends(
    *,
    importer: _Importer = import_module,
) -> dict[str, OfficialBackendStatus]:
    """Probe optional official forecast packages without importing them at module import time."""

    return {
        "neuralforecast": _probe_backend(
            backend_name="neuralforecast",
            package_name="neuralforecast",
            importer=importer,
        ),
        "pytorch_forecasting": _probe_backend(
            backend_name="pytorch_forecasting",
            package_name="pytorch_forecasting",
            importer=importer,
        ),
        "lightning": _probe_backend(
            backend_name="lightning",
            package_name="lightning.pytorch",
            importer=importer,
        ),
    }


def build_official_nbeatsx_forecast(
    training_frame: pl.DataFrame,
    *,
    horizon_hours: int = 24,
    max_steps: int = 10,
    importer: _Importer = import_module,
) -> pl.DataFrame:
    """Train/predict with Nixtla NeuralForecast NBEATSx when the optional backend exists."""

    _validate_training_frame(training_frame)
    backend_status = inspect_official_forecast_backends(importer=importer)["neuralforecast"]
    if not backend_status.available:
        return _empty_forecast_frame()

    try:
        neuralforecast_module = importer("neuralforecast")
        neuralforecast_models = importer("neuralforecast.models")
        neural_forecast_cls = getattr(neuralforecast_module, "NeuralForecast")
        nbeatsx_cls = getattr(neuralforecast_models, "NBEATSx")
    except Exception as exc:  # pragma: no cover - depends on optional backend internals
        raise OfficialForecastAdapterError(f"unable to import official NBEATSx backend: {exc}") from exc

    train_frame = training_frame.filter(pl.col("is_train") & pl.col("y").is_not_null())
    future_frame = training_frame.filter(pl.col("is_forecast")).head(horizon_hours)
    if train_frame.is_empty() or future_frame.is_empty():
        return _empty_forecast_frame()

    feature_columns = _existing_feature_columns(training_frame)
    hist_exog = _csv_columns(training_frame, "historical_observed_feature_columns_csv")
    futr_exog = _csv_columns(training_frame, "known_future_feature_columns_csv")
    input_size = max(horizon_hours, min(168, train_frame.height))

    try:
        model = nbeatsx_cls(
            h=future_frame.height,
            input_size=input_size,
            futr_exog_list=[column for column in futr_exog if column in feature_columns],
            hist_exog_list=[column for column in hist_exog if column in feature_columns],
            max_steps=max_steps,
            random_seed=20260506,
            scaler_type="robust",
        )
        neural_forecast = neural_forecast_cls(models=[model], freq="h")
        neural_forecast.fit(df=train_frame.select(["unique_id", "ds", "y", *feature_columns]).to_pandas())
        prediction_frame = neural_forecast.predict(
            futr_df=future_frame.select(["unique_id", "ds", *feature_columns]).to_pandas()
        )
    except Exception as exc:  # pragma: no cover - requires optional backend training
        raise OfficialForecastAdapterError(f"official NBEATSx training failed: {exc}") from exc

    predictions = pl.from_pandas(prediction_frame)
    prediction_column = _prediction_column(predictions)
    return _official_forecast_frame(
        model_name="nbeatsx_official_v0",
        model_family="NBEATSx",
        backend_name="neuralforecast",
        unique_id=str(future_frame.select("unique_id").to_series().item(0)),
        timestamps=future_frame.select("ds").to_series().to_list(),
        point_predictions=predictions.select(prediction_column).to_series().to_list(),
        training_rows=train_frame.height,
        interval_kind="point",
    )


def build_official_tft_forecast(
    training_frame: pl.DataFrame,
    *,
    importer: _Importer = import_module,
) -> pl.DataFrame:
    """Expose the official TFT adapter contract.

    The implementation intentionally returns no model rows unless PyTorch Forecasting
    and Lightning are installed. The first production TFT training loop should be added
    as a separate slice because it changes dependency/runtime behavior substantially.
    """

    _validate_training_frame(training_frame)
    status = inspect_official_forecast_backends(importer=importer)
    if not status["pytorch_forecasting"].available or not status["lightning"].available:
        return _empty_forecast_frame()
    return _empty_forecast_frame()


def _probe_backend(*, backend_name: str, package_name: str, importer: _Importer) -> OfficialBackendStatus:
    try:
        importer(package_name)
    except ModuleNotFoundError:
        return OfficialBackendStatus(
            backend_name=backend_name,
            package_name=package_name,
            available=False,
            reason=f"{package_name} not installed; install with `uv sync --extra sota`",
        )
    except Exception as exc:
        return OfficialBackendStatus(
            backend_name=backend_name,
            package_name=package_name,
            available=False,
            reason=f"{package_name} import failed: {exc}",
        )
    return OfficialBackendStatus(
        backend_name=backend_name,
        package_name=package_name,
        available=True,
        reason="available",
    )


def _validate_training_frame(training_frame: pl.DataFrame) -> None:
    required_columns = {
        "unique_id",
        "ds",
        "y",
        "is_train",
        "is_forecast",
        "known_future_feature_columns_csv",
        "historical_observed_feature_columns_csv",
    }
    missing_columns = required_columns.difference(training_frame.columns)
    if missing_columns:
        raise ValueError(f"training_frame is missing required columns: {sorted(missing_columns)}")


def _existing_feature_columns(training_frame: pl.DataFrame) -> list[str]:
    excluded_columns = {
        "unique_id",
        "ds",
        "y",
        "split",
        "tenant_id",
        "market_venue",
        "is_train",
        "is_forecast",
        "sota_schema_version",
        "supported_backends_csv",
        "known_future_feature_columns_csv",
        "historical_observed_feature_columns_csv",
        "static_feature_columns_csv",
    }
    return [
        column
        for column in training_frame.columns
        if column not in excluded_columns and training_frame.schema[column].is_numeric()
    ]


def _csv_columns(training_frame: pl.DataFrame, column_name: str) -> list[str]:
    value = training_frame.select(column_name).to_series().item(0)
    if not isinstance(value, str):
        return []
    return [column.strip() for column in value.split(",") if column.strip()]


def _prediction_column(prediction_frame: pl.DataFrame) -> str:
    for column_name in prediction_frame.columns:
        if column_name not in {"unique_id", "ds"} and prediction_frame.schema[column_name].is_numeric():
            return column_name
    raise OfficialForecastAdapterError("official backend returned no numeric prediction column")


def _official_forecast_frame(
    *,
    model_name: str,
    model_family: str,
    backend_name: str,
    unique_id: str,
    timestamps: list[Any],
    point_predictions: list[Any],
    training_rows: int,
    interval_kind: str,
) -> pl.DataFrame:
    horizon_rows = min(len(timestamps), len(point_predictions))
    if horizon_rows == 0:
        return _empty_forecast_frame()

    values = [float(value) for value in point_predictions[:horizon_rows]]
    return pl.DataFrame(
        {
            "model_name": [model_name] * horizon_rows,
            "model_family": [model_family] * horizon_rows,
            "backend_name": [backend_name] * horizon_rows,
            "backend_status": ["trained"] * horizon_rows,
            "unique_id": [unique_id] * horizon_rows,
            "forecast_timestamp": timestamps[:horizon_rows],
            "predicted_price_uah_mwh": values,
            "predicted_price_p10_uah_mwh": [None] * horizon_rows,
            "predicted_price_p50_uah_mwh": values,
            "predicted_price_p90_uah_mwh": [None] * horizon_rows,
            "prediction_interval_kind": [interval_kind] * horizon_rows,
            "training_rows": [training_rows] * horizon_rows,
            "horizon_rows": [horizon_rows] * horizon_rows,
            "adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * horizon_rows,
        },
        schema=_OFFICIAL_FORECAST_SCHEMA,
    )


def _empty_forecast_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_OFFICIAL_FORECAST_SCHEMA)
