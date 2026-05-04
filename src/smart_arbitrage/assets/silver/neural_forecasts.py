"""NBEATSx and TFT Silver forecast assets."""

from __future__ import annotations

import os
from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.forecasting.nbeatsx import build_nbeatsx_forecast
from smart_arbitrage.forecasting.neural_features import build_neural_forecast_feature_frame
from smart_arbitrage.forecasting.tft import build_tft_forecast
from smart_arbitrage.resources.forecast_store import get_forecast_store


@dg.asset(group_name="silver")
def neural_forecast_feature_frame(
	context,
	dam_price_history: pl.DataFrame,
	battery_state_hourly_silver=None,
) -> pl.DataFrame:
	"""Model-ready Silver feature frame for NBEATSx and TFT research forecasts."""

	feature_frame = build_neural_forecast_feature_frame(
		dam_price_history,
		battery_state_hourly_snapshots=battery_state_hourly_silver,
	)
	_add_metadata(
		context,
		{
			"rows": feature_frame.height,
			"train_rows": feature_frame.filter(pl.col("split") == "train").height,
			"forecast_rows": feature_frame.filter(pl.col("split") == "forecast").height,
			"feature_count": len([column for column in feature_frame.columns if column not in {"timestamp", "price_uah_mwh", "target_price_uah_mwh", "split"}]),
		},
	)
	return feature_frame


@dg.asset(group_name="silver")
def nbeatsx_price_forecast(
	context,
	neural_forecast_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
	"""NBEATSx-style DAM price forecast candidate for research comparison."""

	forecast = build_nbeatsx_forecast(neural_forecast_feature_frame)
	forecast_run_id = _persist_forecast_run(
		model_name="nbeatsx_silver_v0",
		forecast=forecast,
		point_prediction_column="predicted_price_uah_mwh",
	)
	_add_metadata(
		context,
		{
			"model_name": "nbeatsx_silver_v0",
			"forecast_run_id": forecast_run_id,
			"forecast_rows": forecast.height,
			"min_predicted_price_uah_mwh": forecast.select("predicted_price_uah_mwh").min().item(),
			"max_predicted_price_uah_mwh": forecast.select("predicted_price_uah_mwh").max().item(),
		},
	)
	return forecast


@dg.asset(group_name="silver")
def tft_price_forecast(
	context,
	neural_forecast_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
	"""TFT-style interpretable DAM price forecast candidate for research comparison."""

	forecast = build_tft_forecast(neural_forecast_feature_frame)
	forecast_run_id = _persist_forecast_run(
		model_name="tft_silver_v0",
		forecast=forecast,
		point_prediction_column="predicted_price_p50_uah_mwh",
	)
	_add_metadata(
		context,
		{
			"model_name": "tft_silver_v0",
			"forecast_run_id": forecast_run_id,
			"forecast_rows": forecast.height,
			"min_p50_price_uah_mwh": forecast.select("predicted_price_p50_uah_mwh").min().item(),
			"max_p50_price_uah_mwh": forecast.select("predicted_price_p50_uah_mwh").max().item(),
			"top_features": ", ".join(
				str(value)
				for value in forecast.select("top_feature_name").to_series().unique().to_list()
			),
		},
	)
	return forecast


NEURAL_FORECAST_SILVER_ASSETS = [
	neural_forecast_feature_frame,
	nbeatsx_price_forecast,
	tft_price_forecast,
]


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
	if context is not None:
		context.add_output_metadata(metadata)


def _persist_forecast_run(*, model_name: str, forecast: pl.DataFrame, point_prediction_column: str) -> str:
	forecast_run_id = get_forecast_store().upsert_forecast_run(
		model_name=model_name,
		forecast_frame=forecast,
		point_prediction_column=point_prediction_column,
	)
	_log_forecast_run_to_mlflow(
		model_name=model_name,
		forecast=forecast,
		point_prediction_column=point_prediction_column,
	)
	return forecast_run_id


def _log_forecast_run_to_mlflow(*, model_name: str, forecast: pl.DataFrame, point_prediction_column: str) -> None:
	mlflow = _try_import_mlflow()
	if mlflow is None or forecast.height == 0:
		return None
	tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
	if tracking_uri is not None:
		mlflow.set_tracking_uri(tracking_uri)
	mlflow.set_experiment("neural-price-forecasting")
	with mlflow.start_run(run_name=model_name):
		mlflow.log_param("model_name", model_name)
		mlflow.log_param("horizon_rows", forecast.height)
		mlflow.log_metric("min_prediction_uah_mwh", float(forecast.select(point_prediction_column).min().item()))
		mlflow.log_metric("max_prediction_uah_mwh", float(forecast.select(point_prediction_column).max().item()))
		mlflow.set_tag("forecast_layer", "silver")


def _try_import_mlflow() -> Any | None:
	try:
		import mlflow
	except ModuleNotFoundError:
		return None
	return mlflow
