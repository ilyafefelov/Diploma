from datetime import datetime

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.assets.silver.neural_forecasts import (
	NEURAL_FORECAST_SILVER_ASSETS,
	nbeatsx_price_forecast,
	neural_forecast_feature_frame,
	tft_price_forecast,
)
from smart_arbitrage.forecasting.neural_features import (
	DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
	NEURAL_FORECAST_FEATURE_COLUMNS,
	build_neural_forecast_feature_frame,
)
from smart_arbitrage.forecasting.nbeatsx import build_nbeatsx_forecast
from smart_arbitrage.forecasting.tft import build_tft_forecast
from smart_arbitrage.defs import defs
from smart_arbitrage.resources.forecast_store import InMemoryForecastStore


def _synthetic_price_history() -> pl.DataFrame:
	return build_synthetic_market_price_history(
		history_hours=15 * 24,
		forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
		now=datetime(2026, 5, 4, 12, 0),
	)


def test_neural_forecast_feature_frame_marks_train_and_horizon_rows() -> None:
	price_history = _synthetic_price_history()

	feature_frame = build_neural_forecast_feature_frame(price_history)

	assert feature_frame.height == price_history.height
	assert set(NEURAL_FORECAST_FEATURE_COLUMNS).issubset(set(feature_frame.columns))
	assert feature_frame.filter(pl.col("split") == "forecast").height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert feature_frame.filter(pl.col("split") == "train").height >= 168
	assert feature_frame.select("lag_24_price_uah_mwh").drop_nulls().height > 0
	assert feature_frame.select("lag_168_price_uah_mwh").drop_nulls().height > 0


def test_neural_forecast_feature_frame_can_include_hourly_battery_state_features() -> None:
	price_history = _synthetic_price_history()
	timestamp = price_history.select("timestamp").to_series().item(-3)
	telemetry_frame = pl.DataFrame(
		{
			"tenant_id": ["client_003_dnipro_factory"],
			"snapshot_hour": [timestamp],
			"soc_close": [0.62],
			"soh_close": [0.961],
			"throughput_mwh": [0.08],
			"efc_delta": [0.08],
			"telemetry_freshness": ["fresh"],
		}
	)

	feature_frame = build_neural_forecast_feature_frame(
		price_history,
		battery_state_hourly_snapshots=telemetry_frame,
	)

	assert {"battery_soc", "battery_soh", "battery_throughput_mwh", "battery_efc_delta", "telemetry_is_fresh"}.issubset(
		set(NEURAL_FORECAST_FEATURE_COLUMNS)
	)
	matched_row = feature_frame.filter(pl.col("timestamp") == timestamp).row(0, named=True)
	assert matched_row["battery_soc"] == 0.62
	assert matched_row["battery_soh"] == 0.961
	assert matched_row["battery_throughput_mwh"] == 0.08
	assert matched_row["battery_efc_delta"] == 0.08
	assert matched_row["telemetry_is_fresh"] == 1.0


def test_nbeatsx_forecast_returns_hourly_uah_predictions() -> None:
	feature_frame = build_neural_forecast_feature_frame(_synthetic_price_history())

	forecast = build_nbeatsx_forecast(feature_frame)

	assert forecast.height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert forecast.select("model_name").to_series().unique().to_list() == ["nbeatsx_silver_v0"]
	assert forecast.select("predicted_price_uah_mwh").drop_nulls().height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert forecast.select("predicted_price_uah_mwh").min().item() > 0.0
	assert "trend_component_uah_mwh" in forecast.columns
	assert "exogenous_component_uah_mwh" in forecast.columns


def test_tft_forecast_returns_predictions_and_feature_weights() -> None:
	feature_frame = build_neural_forecast_feature_frame(_synthetic_price_history())

	forecast = build_tft_forecast(feature_frame)

	assert forecast.height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert forecast.select("model_name").to_series().unique().to_list() == ["tft_silver_v0"]
	assert forecast.select("predicted_price_p50_uah_mwh").drop_nulls().height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert forecast.select("predicted_price_p10_uah_mwh").min().item() > 0.0
	assert forecast.select("predicted_price_p90_uah_mwh").min().item() > forecast.select("predicted_price_p10_uah_mwh").min().item()
	assert "top_feature_name" in forecast.columns
	assert "top_feature_weight" in forecast.columns
	assert forecast.select("top_feature_weight").min().item() >= 0.0
	assert forecast.select("top_feature_weight").max().item() <= 1.0


def test_neural_forecast_silver_assets_are_registered_without_dashboard_contracts() -> None:
	asset_keys = {asset.key.to_user_string() for asset in NEURAL_FORECAST_SILVER_ASSETS}
	registered_asset_keys = {asset.key.to_user_string() for asset in defs.assets or []}

	assert {
		"neural_forecast_feature_frame",
		"nbeatsx_price_forecast",
		"tft_price_forecast",
	}.issubset(asset_keys)
	assert asset_keys.issubset(registered_asset_keys)


def test_neural_forecast_assets_materialize_dataframes() -> None:
	price_history = _synthetic_price_history()

	feature_asset_output = neural_forecast_feature_frame(None, price_history)
	nbeatsx_asset_output = nbeatsx_price_forecast(None, feature_asset_output)
	tft_asset_output = tft_price_forecast(None, feature_asset_output)

	assert feature_asset_output.filter(pl.col("split") == "forecast").height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert nbeatsx_asset_output.height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert tft_asset_output.height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS


def test_neural_forecast_assets_persist_model_runs(monkeypatch) -> None:
	forecast_store = InMemoryForecastStore()
	monkeypatch.setattr("smart_arbitrage.assets.silver.neural_forecasts.get_forecast_store", lambda: forecast_store)
	feature_frame = build_neural_forecast_feature_frame(_synthetic_price_history())

	nbeatsx_price_forecast(None, feature_frame)
	tft_price_forecast(None, feature_frame)

	assert forecast_store.summary_frame.height == 2
	assert forecast_store.observation_frame.height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS * 2
	assert set(forecast_store.summary_frame.select("model_name").to_series().to_list()) == {
		"nbeatsx_silver_v0",
		"tft_silver_v0",
	}
