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


def test_official_tft_adapter_returns_empty_readiness_frame_when_backend_missing() -> None:
    def missing_importer(name: str) -> object:
        raise ModuleNotFoundError(name)

    forecast = build_official_tft_forecast(_training_frame(), importer=missing_importer)

    assert forecast.height == 0
    assert forecast.columns == list(OFFICIAL_FORECAST_COLUMNS)
