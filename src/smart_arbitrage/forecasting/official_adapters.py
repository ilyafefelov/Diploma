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
    random_seed: int = 20260506,
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

    feature_columns = _existing_feature_columns(training_frame)
    train_frame = (
        training_frame
        .filter(pl.col("is_train") & pl.col("y").is_not_null())
        .select(["unique_id", "ds", "y", *feature_columns])
        .pipe(_fill_numeric_feature_nulls, feature_columns=feature_columns)
        .drop_nulls(subset=["y"])
    )
    future_frame = (
        training_frame
        .filter(pl.col("is_forecast"))
        .head(horizon_hours)
        .select(["unique_id", "ds", *feature_columns])
        .with_columns([
            pl.col(column).fill_null(strategy="forward").fill_null(strategy="backward").fill_null(0.0)
            for column in feature_columns
        ])
    )
    if train_frame.is_empty() or future_frame.is_empty():
        return _empty_forecast_frame()

    hist_exog = _csv_columns(training_frame, "historical_observed_feature_columns_csv")
    futr_exog = _csv_columns(training_frame, "known_future_feature_columns_csv")
    input_size = _nbeatsx_input_size(
        training_rows=train_frame.height,
        horizon_rows=future_frame.height,
    )
    if input_size is None:
        return _empty_forecast_frame()

    try:
        model = nbeatsx_cls(
            h=future_frame.height,
            input_size=input_size,
            futr_exog_list=[column for column in futr_exog if column in feature_columns],
            hist_exog_list=[column for column in hist_exog if column in feature_columns],
            max_steps=max_steps,
            random_seed=random_seed,
            scaler_type="robust",
            logger=False,
            enable_progress_bar=False,
            enable_model_summary=False,
        )
        neural_forecast = neural_forecast_cls(models=[model], freq="h")
        neural_forecast.fit(df=train_frame.to_pandas())
        prediction_frame = neural_forecast.predict(
            futr_df=future_frame.to_pandas()
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
    horizon_hours: int = 24,
    max_epochs: int = 1,
    batch_size: int = 64,
    learning_rate: float = 0.01,
    hidden_size: int = 8,
    hidden_continuous_size: int = 4,
    importer: _Importer = import_module,
) -> pl.DataFrame:
    """Train/predict with PyTorch-Forecasting TFT when the optional backend exists."""

    _validate_training_frame(training_frame)
    status = inspect_official_forecast_backends(importer=importer)
    if not status["pytorch_forecasting"].available or not status["lightning"].available:
        return _empty_forecast_frame()

    try:
        pytorch_forecasting_module = importer("pytorch_forecasting")
        pytorch_forecasting_metrics = importer("pytorch_forecasting.metrics")
        lightning_module = importer("lightning.pytorch")
        time_series_dataset_cls = getattr(pytorch_forecasting_module, "TimeSeriesDataSet")
        temporal_fusion_transformer_cls = getattr(pytorch_forecasting_module, "TemporalFusionTransformer")
        quantile_loss_cls = getattr(pytorch_forecasting_metrics, "QuantileLoss")
        trainer_cls = getattr(lightning_module, "Trainer")
    except Exception as exc:  # pragma: no cover - depends on optional backend internals
        raise OfficialForecastAdapterError(f"unable to import official TFT backend: {exc}") from exc

    tft_frames = _tft_training_frames(training_frame, horizon_hours=horizon_hours)
    if tft_frames is None:
        return _empty_forecast_frame()
    train_frame, prediction_frame, feature_columns, known_future_columns, historical_observed_columns, encoder_length = tft_frames

    try:
        training_dataset = time_series_dataset_cls(
            train_frame.to_pandas(),
            time_idx="time_idx",
            target="y",
            group_ids=["unique_id"],
            max_encoder_length=encoder_length,
            max_prediction_length=horizon_hours,
            static_categoricals=["unique_id"],
            time_varying_known_reals=known_future_columns,
            time_varying_unknown_reals=["y", *historical_observed_columns],
            add_relative_time_idx=True,
            add_target_scales=True,
            add_encoder_length=True,
            allow_missing_timesteps=False,
        )
        prediction_dataset = time_series_dataset_cls.from_dataset(
            training_dataset,
            prediction_frame.to_pandas(),
            predict=True,
            stop_randomization=True,
        )
        train_loader = training_dataset.to_dataloader(train=True, batch_size=batch_size, num_workers=0)
        prediction_loader = prediction_dataset.to_dataloader(train=False, batch_size=batch_size, num_workers=0)
        model = temporal_fusion_transformer_cls.from_dataset(
            training_dataset,
            learning_rate=learning_rate,
            hidden_size=hidden_size,
            attention_head_size=1,
            dropout=0.1,
            hidden_continuous_size=hidden_continuous_size,
            output_size=3,
            loss=quantile_loss_cls(quantiles=[0.1, 0.5, 0.9]),
            log_interval=-1,
            reduce_on_plateau_patience=2,
        )
        trainer = trainer_cls(
            max_epochs=max_epochs,
            accelerator="cpu",
            devices=1,
            logger=False,
            enable_checkpointing=False,
            enable_progress_bar=False,
            enable_model_summary=False,
            num_sanity_val_steps=0,
            deterministic=True,
        )
        trainer.fit(model, train_dataloaders=train_loader)
        prediction_output = model.predict(prediction_loader, mode="quantiles", return_x=False)
    except Exception as exc:  # pragma: no cover - requires optional backend training
        raise OfficialForecastAdapterError(f"official TFT training failed: {exc}") from exc

    quantile_rows = _quantile_prediction_rows(prediction_output, horizon_hours=horizon_hours)
    return _official_quantile_forecast_frame(
        model_name="tft_official_v0",
        model_family="TFT",
        backend_name="pytorch_forecasting",
        unique_id=str(prediction_frame.select("unique_id").to_series().item(0)),
        timestamps=prediction_frame.tail(horizon_hours).select("ds").to_series().to_list(),
        quantile_rows=quantile_rows,
        training_rows=train_frame.height,
        interval_kind="quantile",
    )


def _tft_training_frames(
    training_frame: pl.DataFrame,
    *,
    horizon_hours: int,
) -> tuple[pl.DataFrame, pl.DataFrame, list[str], list[str], list[str], int] | None:
    feature_columns = _existing_feature_columns(training_frame)
    if not feature_columns:
        return None
    known_future_columns = [
        column
        for column in _csv_columns(training_frame, "known_future_feature_columns_csv")
        if column in feature_columns
    ]
    historical_observed_columns = [
        column
        for column in _csv_columns(training_frame, "historical_observed_feature_columns_csv")
        if column in feature_columns and column not in known_future_columns
    ]
    selected_columns = ["unique_id", "ds", "y", *feature_columns]
    train_frame = (
        training_frame
        .filter(pl.col("is_train") & pl.col("y").is_not_null())
        .select(selected_columns)
        .pipe(_fill_numeric_feature_nulls, feature_columns=feature_columns)
        .drop_nulls(subset=["y"])
    )
    future_frame = (
        training_frame
        .filter(pl.col("is_forecast"))
        .head(horizon_hours)
        .select(selected_columns)
        .pipe(_fill_numeric_feature_nulls, feature_columns=feature_columns)
    )
    if train_frame.is_empty() or future_frame.is_empty():
        return None
    encoder_length = _tft_encoder_length(training_rows=train_frame.height, horizon_rows=future_frame.height)
    if encoder_length is None:
        return None
    last_training_target = train_frame.select("y").tail(1).to_series().item()
    if last_training_target is None:
        return None
    combined_frame = (
        pl.concat(
            [
                train_frame.with_columns(pl.lit(True).alias("_is_train")),
                future_frame
                .with_columns(
                    [
                        pl.lit(float(last_training_target)).alias("y"),
                        pl.lit(False).alias("_is_train"),
                    ]
                ),
            ],
            how="vertical_relaxed",
        )
        .sort("ds")
        .with_row_index("time_idx")
        .with_columns(pl.col("time_idx").cast(pl.Int64))
    )
    indexed_train_frame = combined_frame.filter(pl.col("_is_train")).drop("_is_train")
    indexed_prediction_frame = (
        pl.concat(
            [
                indexed_train_frame.tail(encoder_length),
                combined_frame.filter(~pl.col("_is_train")).drop("_is_train"),
            ],
            how="vertical_relaxed",
        )
        .sort("time_idx")
    )
    return (
        indexed_train_frame,
        indexed_prediction_frame,
        feature_columns,
        known_future_columns,
        historical_observed_columns,
        encoder_length,
    )


def _fill_numeric_feature_nulls(frame: pl.DataFrame, *, feature_columns: list[str]) -> pl.DataFrame:
    return frame.with_columns(
        [
            pl.col(column).fill_null(strategy="forward").fill_null(strategy="backward").fill_null(0.0)
            for column in feature_columns
        ]
    )


def _tft_encoder_length(*, training_rows: int, horizon_rows: int) -> int | None:
    max_trainable_encoder_length = training_rows - horizon_rows - 1
    if max_trainable_encoder_length < 12:
        return None
    return min(72, max_trainable_encoder_length)


def _quantile_prediction_rows(prediction_output: object, *, horizon_hours: int) -> list[tuple[float, float, float]]:
    values = _nested_prediction_values(prediction_output)
    if not values:
        return []
    first_batch = values[0]
    rows: list[tuple[float, float, float]] = []
    for raw_row in first_batch[:horizon_hours]:
        if len(raw_row) < 3:
            continue
        rows.append((float(raw_row[0]), float(raw_row[1]), float(raw_row[2])))
    return rows


def _nested_prediction_values(prediction_output: object) -> list[list[list[float]]]:
    detach = getattr(prediction_output, "detach", None)
    if callable(detach):
        cpu = detach().cpu()
        return cpu.tolist()
    tolist = getattr(prediction_output, "tolist", None)
    if callable(tolist):
        return tolist()
    if isinstance(prediction_output, list):
        return prediction_output
    if isinstance(prediction_output, tuple) and prediction_output:
        return _nested_prediction_values(prediction_output[0])
    raise OfficialForecastAdapterError("official TFT backend returned unsupported prediction output")


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


def _nbeatsx_input_size(*, training_rows: int, horizon_rows: int) -> int | None:
    """Choose a smoke-safe input window that leaves at least one training window."""

    max_trainable_input_size = training_rows - horizon_rows - 1
    if max_trainable_input_size < 1:
        return None
    return min(168, max_trainable_input_size)


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


def _official_quantile_forecast_frame(
    *,
    model_name: str,
    model_family: str,
    backend_name: str,
    unique_id: str,
    timestamps: list[Any],
    quantile_rows: list[tuple[float, float, float]],
    training_rows: int,
    interval_kind: str,
) -> pl.DataFrame:
    horizon_rows = min(len(timestamps), len(quantile_rows))
    if horizon_rows == 0:
        return _empty_forecast_frame()

    p10_values = [float(row[0]) for row in quantile_rows[:horizon_rows]]
    p50_values = [float(row[1]) for row in quantile_rows[:horizon_rows]]
    p90_values = [float(row[2]) for row in quantile_rows[:horizon_rows]]
    return pl.DataFrame(
        {
            "model_name": [model_name] * horizon_rows,
            "model_family": [model_family] * horizon_rows,
            "backend_name": [backend_name] * horizon_rows,
            "backend_status": ["trained"] * horizon_rows,
            "unique_id": [unique_id] * horizon_rows,
            "forecast_timestamp": timestamps[:horizon_rows],
            "predicted_price_uah_mwh": p50_values,
            "predicted_price_p10_uah_mwh": p10_values,
            "predicted_price_p50_uah_mwh": p50_values,
            "predicted_price_p90_uah_mwh": p90_values,
            "prediction_interval_kind": [interval_kind] * horizon_rows,
            "training_rows": [training_rows] * horizon_rows,
            "horizon_rows": [horizon_rows] * horizon_rows,
            "adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * horizon_rows,
        },
        schema=_OFFICIAL_FORECAST_SCHEMA,
    )


def _empty_forecast_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_OFFICIAL_FORECAST_SCHEMA)
