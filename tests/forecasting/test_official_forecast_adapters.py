from typing import cast

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.forecasting.neural_features import build_neural_forecast_feature_frame
from smart_arbitrage.forecasting.official_adapters import (
    OFFICIAL_FORECAST_COLUMNS,
    build_official_nbeatsx_forecast,
    build_official_tft_forecast,
    inspect_official_forecast_backends,
)
from smart_arbitrage.forecasting.sota_training import build_sota_forecast_training_frame


def _training_frame() -> pl.DataFrame:
    price_history = build_synthetic_market_price_history(history_hours=240, forecast_hours=24)
    feature_frame = build_neural_forecast_feature_frame(price_history)
    return build_sota_forecast_training_frame(feature_frame, tenant_id="client_003_dnipro_factory")


def test_official_backend_probe_reports_unavailable_packages() -> None:
    def missing_importer(name: str) -> object:
        raise ModuleNotFoundError(name)

    status = inspect_official_forecast_backends(importer=missing_importer)

    assert status["neuralforecast"].available is False
    assert status["pytorch_forecasting"].available is False
    assert status["lightning"].available is False
    assert "not installed" in status["neuralforecast"].reason


def test_official_nbeatsx_adapter_returns_empty_readiness_frame_when_backend_missing() -> None:
    def missing_importer(name: str) -> object:
        raise ModuleNotFoundError(name)

    forecast = build_official_nbeatsx_forecast(_training_frame(), importer=missing_importer)

    assert forecast.height == 0
    assert forecast.columns == list(OFFICIAL_FORECAST_COLUMNS)


def test_official_nbeatsx_adapter_keeps_input_window_trainable_after_lag_drop() -> None:
    captured: dict[str, int] = {}

    class FakeNBEATSx:
        def __init__(self, **kwargs: object) -> None:
            horizon_rows = kwargs["h"]
            input_size = kwargs["input_size"]
            assert isinstance(horizon_rows, int)
            assert isinstance(input_size, int)
            captured["horizon_rows"] = horizon_rows
            captured["input_size"] = input_size
            captured["max_steps"] = cast(int, kwargs["max_steps"])
            captured["random_seed"] = cast(int, kwargs["random_seed"])
            assert kwargs["logger"] is False
            assert kwargs["enable_progress_bar"] is False
            assert kwargs["enable_model_summary"] is False

    class FakeNeuralForecast:
        def __init__(self, *, models: list[object], freq: str) -> None:
            self._model = models[0]
            assert freq == "h"
            assert isinstance(self._model, FakeNBEATSx)

        def fit(self, df: object) -> None:
            captured["training_rows"] = len(df)  # type: ignore[arg-type]
            assert captured["training_rows"] > captured["input_size"] + captured["horizon_rows"]

        def predict(self, *, futr_df: object) -> object:
            frame = futr_df[["unique_id", "ds"]].copy()  # type: ignore[index]
            frame["NBEATSx"] = [4100.0 + index for index in range(len(frame))]
            return frame

    class FakeNeuralForecastModule:
        NeuralForecast = FakeNeuralForecast

    class FakeNeuralForecastModels:
        NBEATSx = FakeNBEATSx

    def fake_importer(name: str) -> object:
        if name == "neuralforecast":
            return FakeNeuralForecastModule()
        if name == "neuralforecast.models":
            return FakeNeuralForecastModels()
        raise ModuleNotFoundError(name)

    forecast = build_official_nbeatsx_forecast(
        _training_frame(),
        max_steps=100,
        random_seed=20260509,
        importer=fake_importer,
    )

    assert forecast.height == 24
    assert forecast.select("backend_status").to_series().unique().to_list() == ["trained"]
    assert captured["input_size"] <= captured["training_rows"] - captured["horizon_rows"] - 1
    assert captured["max_steps"] == 100
    assert captured["random_seed"] == 20260509


def test_official_nbeatsx_adapter_fills_feature_nulls_before_training() -> None:
    captured: dict[str, int] = {}

    class FakeNBEATSx:
        def __init__(self, **kwargs: object) -> None:
            captured["input_size"] = cast(int, kwargs["input_size"])
            captured["horizon_rows"] = cast(int, kwargs["h"])

    class FakeNeuralForecast:
        def __init__(self, *, models: list[object], freq: str) -> None:
            assert isinstance(models[0], FakeNBEATSx)
            assert freq == "h"

        def fit(self, df: object) -> None:
            captured["training_rows"] = len(df)  # type: ignore[arg-type]
            assert captured["training_rows"] > 0
            assert int(df.isna().sum().sum()) == 0  # type: ignore[attr-defined]

        def predict(self, *, futr_df: object) -> object:
            frame = futr_df[["unique_id", "ds"]].copy()  # type: ignore[index]
            frame["NBEATSx"] = [4100.0 + index for index in range(len(frame))]
            return frame

    class FakeNeuralForecastModule:
        NeuralForecast = FakeNeuralForecast

    class FakeNeuralForecastModels:
        NBEATSx = FakeNBEATSx

    def fake_importer(name: str) -> object:
        if name == "neuralforecast":
            return FakeNeuralForecastModule()
        if name == "neuralforecast.models":
            return FakeNeuralForecastModels()
        raise ModuleNotFoundError(name)

    frame_with_minimum_history = (
        _training_frame()
        .filter(
            ((pl.col("split") == "train") & (pl.int_range(pl.len()) < 168))
            | (pl.col("split") == "forecast")
        )
        .with_columns(
            pl.when(pl.col("split") == "train")
            .then(None)
            .otherwise(pl.col("lag_168_price_uah_mwh"))
            .alias("lag_168_price_uah_mwh")
        )
    )

    forecast = build_official_nbeatsx_forecast(
        frame_with_minimum_history,
        max_steps=1,
        importer=fake_importer,
    )

    assert forecast.height == 24
    assert captured["training_rows"] == 168


def test_official_tft_adapter_returns_empty_readiness_frame_when_backend_missing() -> None:
    def missing_importer(name: str) -> object:
        raise ModuleNotFoundError(name)

    forecast = build_official_tft_forecast(_training_frame(), importer=missing_importer)

    assert forecast.height == 0
    assert forecast.columns == list(OFFICIAL_FORECAST_COLUMNS)


def test_official_tft_adapter_trains_quantile_smoke_when_backend_available() -> None:
    captured: dict[str, object] = {}

    class FakeTimeSeriesDataSet:
        def __init__(self, data: object, **kwargs: object) -> None:
            captured["training_rows"] = len(data)  # type: ignore[arg-type]
            captured.setdefault("max_prediction_length", kwargs["max_prediction_length"])
            captured.setdefault("time_varying_known_reals", kwargs["time_varying_known_reals"])
            captured.setdefault("time_varying_unknown_reals", kwargs["time_varying_unknown_reals"])

        @classmethod
        def from_dataset(
            cls,
            dataset: object,
            data: object,
            *,
            predict: bool,
            stop_randomization: bool,
        ) -> "FakeTimeSeriesDataSet":
            captured["prediction_rows"] = len(data)  # type: ignore[arg-type]
            assert predict is True
            assert stop_randomization is True
            return cls(data, max_prediction_length=24, time_varying_known_reals=[], time_varying_unknown_reals=[])

        def to_dataloader(self, *, train: bool, batch_size: int, num_workers: int) -> str:
            assert num_workers == 0
            captured[f"batch_size_{train}"] = batch_size
            return f"loader:{train}"

    class FakeTemporalFusionTransformer:
        @classmethod
        def from_dataset(cls, dataset: object, **kwargs: object) -> "FakeTemporalFusionTransformer":
            captured["output_size"] = kwargs["output_size"]
            captured["hidden_size"] = kwargs["hidden_size"]
            captured["hidden_continuous_size"] = kwargs["hidden_continuous_size"]
            captured["learning_rate"] = kwargs["learning_rate"]
            captured["loss"] = kwargs["loss"]
            return cls()

        def predict(self, data: object, *, mode: str, return_x: bool) -> list[list[list[float]]]:
            assert data == "loader:False"
            assert mode == "quantiles"
            assert return_x is False
            return [
                [
                    [4000.0 + step, 4100.0 + step, 4200.0 + step]
                    for step in range(24)
                ]
            ]

    class FakeQuantileLoss:
        def __init__(self, quantiles: list[float]) -> None:
            captured["quantiles"] = quantiles

    class FakeTrainer:
        def __init__(self, **kwargs: object) -> None:
            captured["trainer_max_epochs"] = kwargs["max_epochs"]
            assert kwargs["logger"] is False
            assert kwargs["enable_checkpointing"] is False
            assert kwargs["enable_progress_bar"] is False
            assert kwargs["enable_model_summary"] is False

        def fit(self, model: object, *, train_dataloaders: object) -> None:
            captured["fit_loader"] = train_dataloaders

    class FakePyTorchForecastingModule:
        TimeSeriesDataSet = FakeTimeSeriesDataSet
        TemporalFusionTransformer = FakeTemporalFusionTransformer

    class FakeMetricsModule:
        QuantileLoss = FakeQuantileLoss

    class FakeLightningModule:
        Trainer = FakeTrainer

    def fake_importer(name: str) -> object:
        if name == "pytorch_forecasting":
            return FakePyTorchForecastingModule()
        if name == "pytorch_forecasting.metrics":
            return FakeMetricsModule()
        if name == "lightning.pytorch":
            return FakeLightningModule()
        raise ModuleNotFoundError(name)

    forecast = build_official_tft_forecast(
        _training_frame(),
        max_epochs=15,
        batch_size=32,
        learning_rate=0.005,
        hidden_size=12,
        hidden_continuous_size=6,
        importer=fake_importer,
    )

    assert forecast.height == 24
    assert forecast.select("model_name").to_series().unique().to_list() == ["tft_official_v0"]
    assert forecast.select("backend_status").to_series().unique().to_list() == ["trained"]
    assert forecast.select("prediction_interval_kind").to_series().unique().to_list() == ["quantile"]
    assert forecast.select("predicted_price_p10_uah_mwh").to_series().item(0) == 4000.0
    assert forecast.select("predicted_price_p50_uah_mwh").to_series().item(0) == 4100.0
    assert forecast.select("predicted_price_p90_uah_mwh").to_series().item(0) == 4200.0
    assert captured["max_prediction_length"] == 24
    assert "y" in cast(list[str], captured["time_varying_unknown_reals"])
    assert captured["quantiles"] == [0.1, 0.5, 0.9]
    assert captured["output_size"] == 3
    assert captured["trainer_max_epochs"] == 15
    assert captured["batch_size_True"] == 32
    assert captured["batch_size_False"] == 32
    assert captured["hidden_size"] == 12
    assert captured["hidden_continuous_size"] == 6
    assert captured["learning_rate"] == 0.005
    assert captured["fit_loader"] == "loader:True"
