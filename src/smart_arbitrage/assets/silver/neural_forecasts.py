"""NBEATSx and TFT Silver forecast assets."""

from __future__ import annotations

from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.forecasting.nbeatsx import build_nbeatsx_forecast
from smart_arbitrage.forecasting.neural_features import build_neural_forecast_feature_frame
from smart_arbitrage.forecasting.tft import build_tft_forecast


@dg.asset(group_name="silver")
def neural_forecast_feature_frame(
	context,
	dam_price_history: pl.DataFrame,
) -> pl.DataFrame:
	"""Model-ready Silver feature frame for NBEATSx and TFT research forecasts."""

	feature_frame = build_neural_forecast_feature_frame(dam_price_history)
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
	_add_metadata(
		context,
		{
			"model_name": "nbeatsx_silver_v0",
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
	_add_metadata(
		context,
		{
			"model_name": "tft_silver_v0",
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
