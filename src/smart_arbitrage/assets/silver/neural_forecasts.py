"""NBEATSx and TFT Silver forecast assets."""

import os
from datetime import date, datetime
from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.forecasting.afe import build_forecast_afe_feature_catalog_frame
from smart_arbitrage.forecasting.market_coupling_availability import (
	build_market_coupling_temporal_availability_frame,
)
from smart_arbitrage.forecasting.nbeatsx import build_nbeatsx_forecast
from smart_arbitrage.forecasting.neural_features import build_neural_forecast_feature_frame
from smart_arbitrage.forecasting.official_adapters import (
	build_official_global_panel_nbeatsx_forecast,
	build_official_nbeatsx_forecast,
	build_official_tft_forecast,
	inspect_official_forecast_backends,
)
from smart_arbitrage.forecasting.sota_training import build_sota_forecast_training_frame
from smart_arbitrage.forecasting.sota_training import build_official_global_panel_training_frame
from smart_arbitrage.forecasting.tft import build_tft_forecast
from smart_arbitrage.resources.forecast_store import get_forecast_store

MLFLOW_FORECAST_EXPERIMENT_NAME = "smart-arbitrage-forecast-research"
MLFLOW_FORECAST_MODEL_REGISTRY_NAMES = {
	"nbeatsx_silver_v0": "smart-arbitrage-nbeatsx-silver",
	"tft_silver_v0": "smart-arbitrage-tft-silver",
	"nbeatsx_official_v0": "smart-arbitrage-nbeatsx-official",
	"tft_official_v0": "smart-arbitrage-tft-official",
}


class NbeatsxOfficialForecastAssetConfig(dg.Config):
	"""CPU-safe official NBEATSx adapter controls."""

	horizon_hours: int = 24
	max_steps: int = 10
	random_seed: int = 20260506


class TftOfficialForecastAssetConfig(dg.Config):
	"""CPU-safe official TFT adapter controls."""

	horizon_hours: int = 24
	max_epochs: int = 1
	batch_size: int = 64
	learning_rate: float = 0.01
	hidden_size: int = 8
	hidden_continuous_size: int = 4


class OfficialGlobalPanelTrainingAssetConfig(dg.Config):
	"""Point-in-time global-panel official forecast training contract."""

	tenant_ids_csv: str = ""
	horizon_hours: int = 24
	temporal_scaler_type: str = "robust"


@dg.asset(
	group_name=taxonomy.SILVER_FORECAST_FEATURES,
	tags=taxonomy.asset_tags(
		medallion="silver",
		domain="forecasting",
		elt_stage="transform",
		ml_stage="feature_engineering",
		evidence_scope="research_only",
		backend="official_forecast_adapters",
		market_venue="DAM",
	),
)
def official_forecast_exogenous_governance_frame(context) -> pl.DataFrame:
	"""Feature-governance sidecar for official forecast training.

	The frame is intentionally metadata-only in this slice. External European
	market-coupling sources are visible in lineage, but remain blocked from
	training until licensing, timezone, currency, market-rule, temporal
	availability, and domain-shift controls are mapped.
	"""

	frame = build_market_coupling_temporal_availability_frame(
		build_forecast_afe_feature_catalog_frame()
	)
	_add_metadata(
		context,
		{
			"rows": frame.height,
			"source_count": frame.select("source_name").n_unique() if frame.height else 0,
			"training_allowed_rows": frame.filter(pl.col("training_use_allowed")).height,
			"scope": "official_forecast_exogenous_governance_not_training_data",
		},
	)
	return frame


@dg.asset(
	group_name=taxonomy.SILVER_FORECAST_FEATURES,
	tags=taxonomy.asset_tags(
		medallion="silver",
		domain="forecasting",
		elt_stage="transform",
		ml_stage="feature_engineering",
		evidence_scope="research_only",
		market_venue="DAM",
	),
)
def neural_forecast_feature_frame(
	context,
	dam_price_history: pl.DataFrame,
	battery_state_hourly_silver=None,
	grid_event_signal_silver=None,
	tenant_net_load_hourly_silver=None,
) -> pl.DataFrame:
	"""Model-ready Silver feature frame for NBEATSx and TFT research forecasts."""

	feature_frame = build_neural_forecast_feature_frame(
		dam_price_history,
		battery_state_hourly_snapshots=battery_state_hourly_silver,
		future_weather_mode="forecast_only",
		grid_event_signal_frame=grid_event_signal_silver,
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


@dg.asset(
	group_name=taxonomy.SILVER_FORECAST_FEATURES,
	tags=taxonomy.asset_tags(
		medallion="silver",
		domain="forecasting",
		elt_stage="transform",
		ml_stage="feature_engineering",
		evidence_scope="research_only",
		market_venue="DAM",
	),
)
def sota_forecast_training_frame(
	context,
	neural_forecast_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
	"""Backend-neutral Silver frame for full NeuralForecast/PyTorch-Forecasting experiments."""

	frame = build_sota_forecast_training_frame(
		neural_forecast_feature_frame,
		tenant_id="global_research_tenant",
	)
	_add_metadata(
		context,
		{
			"rows": frame.height,
			"train_rows": frame.filter(pl.col("split") == "train").height,
			"forecast_rows": frame.filter(pl.col("split") == "forecast").height,
			"schema_version": frame.select("sota_schema_version").to_series().item(0) if frame.height else "empty",
			"scope": "sota_backend_contract_not_trained_model",
		},
	)
	return frame


@dg.asset(
	group_name=taxonomy.SILVER_FORECAST_FEATURES,
	tags=taxonomy.asset_tags(
		medallion="silver",
		domain="forecasting",
		elt_stage="transform",
		ml_stage="feature_engineering",
		evidence_scope="research_only",
		backend="official_forecast_adapters",
		market_venue="DAM",
	),
)
def official_global_panel_training_frame(
	context,
	config: OfficialGlobalPanelTrainingAssetConfig,
	real_data_benchmark_silver_feature_frame: pl.DataFrame,
	official_forecast_exogenous_governance_frame: pl.DataFrame,
) -> pl.DataFrame:
	"""Point-in-time global panel for serious official NBEATSx/TFT evidence."""

	tenant_ids = _tenant_ids_from_csv(config.tenant_ids_csv, real_data_benchmark_silver_feature_frame)
	frame = build_official_global_panel_training_frame(
		real_data_benchmark_silver_feature_frame,
		tenant_ids=tenant_ids,
		horizon_hours=config.horizon_hours,
		temporal_scaler_type=config.temporal_scaler_type,
		market_coupling_availability_frame=official_forecast_exogenous_governance_frame,
	)
	_add_metadata(
		context,
		{
			"rows": frame.height,
			"tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
			"unique_id_count": frame.select("unique_id").n_unique() if frame.height else 0,
			"train_rows": frame.filter(pl.col("split") == "train").height,
			"forecast_rows": frame.filter(pl.col("split") == "forecast").height,
			"schema_version": frame.select("sota_schema_version").to_series().item(0) if frame.height else "empty",
			"temporal_scaler_type": config.temporal_scaler_type,
			"external_feature_training_status": frame.select("external_feature_training_status").to_series().item(0) if frame.height else "empty",
			"blocked_external_feature_columns_csv": frame.select("blocked_external_feature_columns_csv").to_series().item(0) if frame.height else "",
			"scope": "official_global_panel_feature_scaling_contract_not_trained_model",
		},
	)
	return frame


@dg.asset(
	group_name=taxonomy.SILVER_FORECAST_CANDIDATES,
	tags=taxonomy.asset_tags(
		medallion="silver",
		domain="forecasting",
		elt_stage="transform",
		ml_stage="forecasting",
		evidence_scope="research_only",
		market_venue="DAM",
	),
)
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


@dg.asset(
	group_name=taxonomy.SILVER_FORECAST_CANDIDATES,
	tags=taxonomy.asset_tags(
		medallion="silver",
		domain="forecasting",
		elt_stage="transform",
		ml_stage="forecasting",
		evidence_scope="research_only",
		market_venue="DAM",
	),
)
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


@dg.asset(
	group_name=taxonomy.SILVER_FORECAST_CANDIDATES,
	tags=taxonomy.asset_tags(
		medallion="silver",
		domain="forecasting",
		elt_stage="transform",
		ml_stage="forecasting",
		evidence_scope="research_only",
		backend="neuralforecast",
		market_venue="DAM",
	),
)
def nbeatsx_official_price_forecast(
	context,
	config: NbeatsxOfficialForecastAssetConfig,
	sota_forecast_training_frame: pl.DataFrame,
) -> pl.DataFrame:
	"""Official NeuralForecast NBEATSx candidate when optional SOTA dependencies exist."""

	forecast = build_official_nbeatsx_forecast(
		sota_forecast_training_frame,
		horizon_hours=config.horizon_hours,
		max_steps=config.max_steps,
		random_seed=config.random_seed,
	)
	forecast_run_id = (
		_persist_forecast_run(
			model_name="nbeatsx_official_v0",
			forecast=forecast,
			point_prediction_column="predicted_price_uah_mwh",
		)
		if forecast.height
		else "not_materialized"
	)
	backend_status = inspect_official_forecast_backends()["neuralforecast"]
	_add_metadata(
		context,
		{
			"model_name": "nbeatsx_official_v0",
			"forecast_run_id": forecast_run_id,
			"forecast_rows": forecast.height,
			"horizon_hours": config.horizon_hours,
			"max_steps": config.max_steps,
			"random_seed": config.random_seed,
			"backend_available": backend_status.available,
			"backend_status": backend_status.reason,
			"scope": "official_neuralforecast_adapter_not_live_strategy",
		},
	)
	return forecast


@dg.asset(
	group_name=taxonomy.SILVER_FORECAST_CANDIDATES,
	tags=taxonomy.asset_tags(
		medallion="silver",
		domain="forecasting",
		elt_stage="transform",
		ml_stage="forecasting",
		evidence_scope="research_only",
		backend="neuralforecast",
		market_venue="DAM",
	),
)
def nbeatsx_official_global_panel_price_forecast(
	context,
	config: NbeatsxOfficialForecastAssetConfig,
	official_global_panel_training_frame: pl.DataFrame,
) -> pl.DataFrame:
	"""Official Nixtla NBEATSx global-panel candidate for serious evidence."""

	forecast = build_official_global_panel_nbeatsx_forecast(
		official_global_panel_training_frame,
		horizon_hours=config.horizon_hours,
		max_steps=config.max_steps,
		random_seed=config.random_seed,
	)
	forecast_run_id = (
		_persist_forecast_run(
			model_name="nbeatsx_official_global_panel_v1",
			forecast=forecast,
			point_prediction_column="predicted_price_uah_mwh",
		)
		if forecast.height
		else "not_materialized"
	)
	backend_status = inspect_official_forecast_backends()["neuralforecast"]
	_add_metadata(
		context,
		{
			"model_name": "nbeatsx_official_global_panel_v1",
			"forecast_run_id": forecast_run_id,
			"forecast_rows": forecast.height,
			"unique_id_count": forecast.select("unique_id").n_unique() if forecast.height else 0,
			"horizon_hours": config.horizon_hours,
			"max_steps": config.max_steps,
			"random_seed": config.random_seed,
			"backend_available": backend_status.available,
			"backend_status": backend_status.reason,
			"scope": "official_global_panel_neuralforecast_adapter_not_live_strategy",
		},
	)
	return forecast


@dg.asset(
	group_name=taxonomy.SILVER_FORECAST_CANDIDATES,
	tags=taxonomy.asset_tags(
		medallion="silver",
		domain="forecasting",
		elt_stage="transform",
		ml_stage="forecasting",
		evidence_scope="research_only",
		backend="pytorch_forecasting",
		market_venue="DAM",
	),
)
def tft_official_price_forecast(
	context,
	config: TftOfficialForecastAssetConfig,
	sota_forecast_training_frame: pl.DataFrame,
) -> pl.DataFrame:
	"""Official PyTorch-Forecasting TFT candidate readiness asset."""

	forecast = build_official_tft_forecast(
		sota_forecast_training_frame,
		horizon_hours=config.horizon_hours,
		max_epochs=config.max_epochs,
		batch_size=config.batch_size,
		learning_rate=config.learning_rate,
		hidden_size=config.hidden_size,
		hidden_continuous_size=config.hidden_continuous_size,
	)
	forecast_run_id = (
		_persist_forecast_run(
			model_name="tft_official_v0",
			forecast=forecast,
			point_prediction_column="predicted_price_uah_mwh",
		)
		if forecast.height
		else "not_materialized"
	)
	backend_status = inspect_official_forecast_backends()
	_add_metadata(
		context,
		{
			"model_name": "tft_official_v0",
			"forecast_run_id": forecast_run_id,
			"forecast_rows": forecast.height,
			"horizon_hours": config.horizon_hours,
			"max_epochs": config.max_epochs,
			"batch_size": config.batch_size,
			"learning_rate": config.learning_rate,
			"hidden_size": config.hidden_size,
			"hidden_continuous_size": config.hidden_continuous_size,
			"pytorch_forecasting_available": backend_status["pytorch_forecasting"].available,
			"lightning_available": backend_status["lightning"].available,
			"pytorch_forecasting_status": backend_status["pytorch_forecasting"].reason,
			"lightning_status": backend_status["lightning"].reason,
			"scope": "official_pytorch_forecasting_adapter_not_live_strategy",
		},
	)
	return forecast


NEURAL_FORECAST_SILVER_ASSETS = [
	neural_forecast_feature_frame,
	sota_forecast_training_frame,
	official_forecast_exogenous_governance_frame,
	official_global_panel_training_frame,
	nbeatsx_price_forecast,
	tft_price_forecast,
	nbeatsx_official_price_forecast,
	nbeatsx_official_global_panel_price_forecast,
	tft_official_price_forecast,
]


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
	if context is not None:
		context.add_output_metadata(metadata)


def _tenant_ids_from_csv(value: str, frame: pl.DataFrame) -> tuple[str, ...]:
	tenant_ids = tuple(item.strip() for item in value.split(",") if item.strip())
	if tenant_ids:
		return tenant_ids
	if "tenant_id" not in frame.columns:
		raise ValueError("tenant_ids_csv is required when the input frame has no tenant_id column.")
	discovered = tuple(str(value) for value in frame.select("tenant_id").drop_nulls().unique().sort("tenant_id")["tenant_id"].to_list())
	if not discovered:
		raise ValueError("official global panel requires at least one tenant_id.")
	return discovered


def _persist_forecast_run(*, model_name: str, forecast: pl.DataFrame, point_prediction_column: str) -> str:
	forecast_run_id = get_forecast_store().upsert_forecast_run(
		model_name=model_name,
		forecast_frame=forecast,
		point_prediction_column=point_prediction_column,
	)
	_log_forecast_run_to_mlflow(
		model_name=model_name,
		forecast_run_id=forecast_run_id,
		forecast=forecast,
		point_prediction_column=point_prediction_column,
	)
	return forecast_run_id


def _log_forecast_run_to_mlflow(
	*,
	model_name: str,
	forecast_run_id: str,
	forecast: pl.DataFrame,
	point_prediction_column: str,
) -> None:
	mlflow = _try_import_mlflow()
	if mlflow is None or forecast.height == 0:
		return None
	tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
	if tracking_uri is None:
		return None
	mlflow.set_tracking_uri(tracking_uri)
	mlflow.set_experiment(MLFLOW_FORECAST_EXPERIMENT_NAME)
	metrics = _forecast_metrics(forecast, point_prediction_column=point_prediction_column)
	manifest = _forecast_manifest(
		model_name=model_name,
		forecast_run_id=forecast_run_id,
		forecast=forecast,
		point_prediction_column=point_prediction_column,
	)
	with mlflow.start_run(run_name=f"{model_name}:{forecast_run_id}") as active_run:
		mlflow.log_param("model_name", model_name)
		mlflow.log_param("forecast_run_id", forecast_run_id)
		mlflow.log_param("horizon_rows", forecast.height)
		mlflow.log_param("point_prediction_column", point_prediction_column)
		for metric_name, metric_value in metrics.items():
			mlflow.log_metric(metric_name, metric_value)
		mlflow.set_tag("forecast_layer", "silver")
		mlflow.set_tag("model_kind", "frozen_forecast_candidate")
		mlflow.set_tag("market_venue", "DAM")
		mlflow.log_dict(manifest, "forecast_manifest.json")
		mlflow.log_dict(_forecast_records(forecast), "forecast_rows.json")
		if _should_register_forecast_model():
			_register_frozen_forecast_candidate(
				mlflow=mlflow,
				model_name=model_name,
				forecast_run_id=forecast_run_id,
				active_run_id=active_run.info.run_id,
				forecast=forecast,
				point_prediction_column=point_prediction_column,
				manifest=manifest,
			)


def _forecast_metrics(forecast: pl.DataFrame, *, point_prediction_column: str) -> dict[str, float]:
	predictions = forecast.select(point_prediction_column).to_series()
	min_prediction = _series_min_float(predictions)
	max_prediction = _series_max_float(predictions)
	metrics = {
		"horizon_rows": float(forecast.height),
		"min_prediction_uah_mwh": min_prediction,
		"max_prediction_uah_mwh": max_prediction,
		"mean_prediction_uah_mwh": _series_mean_float(predictions),
		"prediction_spread_uah_mwh": max_prediction - min_prediction,
	}
	if {"predicted_price_p10_uah_mwh", "predicted_price_p90_uah_mwh"}.issubset(forecast.columns):
		interval_width = (
			forecast.select("predicted_price_p90_uah_mwh").to_series()
			- forecast.select("predicted_price_p10_uah_mwh").to_series()
		)
		if not interval_width.drop_nulls().is_empty():
			metrics["mean_prediction_interval_width_uah_mwh"] = _series_mean_float(interval_width)
	if "top_feature_weight" in forecast.columns:
		metrics["max_top_feature_weight"] = _series_max_float(forecast.select("top_feature_weight").to_series())
	return metrics


def _series_min_float(series: pl.Series) -> float:
	value: Any = series.min()
	if value is None:
		raise ValueError("series minimum is not available.")
	return float(value)


def _series_max_float(series: pl.Series) -> float:
	value: Any = series.max()
	if value is None:
		raise ValueError("series maximum is not available.")
	return float(value)


def _series_mean_float(series: pl.Series) -> float:
	value: Any = series.mean()
	if value is None:
		raise ValueError("series mean is not available.")
	return float(value)


def _forecast_manifest(
	*,
	model_name: str,
	forecast_run_id: str,
	forecast: pl.DataFrame,
	point_prediction_column: str,
) -> dict[str, Any]:
	return {
		"forecast_run_id": forecast_run_id,
		"model_name": model_name,
		"market_venue": "DAM",
		"forecast_layer": "silver",
		"model_kind": "frozen_forecast_candidate",
		"point_prediction_column": point_prediction_column,
		"horizon_rows": forecast.height,
		"forecast_columns": forecast.columns,
		"registered_model_name": MLFLOW_FORECAST_MODEL_REGISTRY_NAMES.get(model_name),
		"academic_scope": (
			"Research forecast candidate for comparison against the Level 1 Baseline Forecast; "
			"not a dispatch policy or market-execution contract."
		),
	}


def _forecast_records(forecast: pl.DataFrame) -> list[dict[str, Any]]:
	return [
		{
			column_name: _json_safe_value(value)
			for column_name, value in row.items()
		}
		for row in forecast.iter_rows(named=True)
	]


def _json_safe_value(value: Any) -> Any:
	if isinstance(value, datetime | date):
		return value.isoformat()
	return value


def _should_register_forecast_model() -> bool:
	return os.environ.get("SMART_ARBITRAGE_MLFLOW_REGISTER_MODELS", "true").lower() not in {"0", "false", "no"}


def _register_frozen_forecast_candidate(
	*,
	mlflow: Any,
	model_name: str,
	forecast_run_id: str,
	active_run_id: str,
	forecast: pl.DataFrame,
	point_prediction_column: str,
	manifest: dict[str, Any],
) -> None:
	registered_model_name = MLFLOW_FORECAST_MODEL_REGISTRY_NAMES.get(model_name)
	if registered_model_name is None:
		return None
	python_model_class = _static_forecast_pyfunc_model_class(mlflow)
	model_info = mlflow.pyfunc.log_model(
		name="forecast_pyfunc",
		python_model=python_model_class(
			forecast_rows=_forecast_records(forecast),
			point_prediction_column=point_prediction_column,
		),
		registered_model_name=registered_model_name,
		input_example=[{"request_id": "example"}],
		pip_requirements=["pandas"],
		metadata=manifest,
	)
	client = mlflow.MlflowClient()
	client.set_registered_model_tag(registered_model_name, "project", "smart-energy-arbitrage")
	client.set_registered_model_tag(registered_model_name, "forecast_layer", "silver")
	client.set_registered_model_tag(registered_model_name, "market_venue", "DAM")
	client.set_registered_model_tag(registered_model_name, "model_kind", "frozen_forecast_candidate")
	client.set_registered_model_tag(registered_model_name, "latest_forecast_run_id", forecast_run_id)
	client.set_registered_model_tag(registered_model_name, "latest_source_run_id", active_run_id)
	version = getattr(model_info, "registered_model_version", None)
	if version is not None:
		client.set_registered_model_tag(registered_model_name, "latest_registered_model_version", str(version))


def _static_forecast_pyfunc_model_class(mlflow: Any) -> type:
	class StaticForecastPyFuncModel(mlflow.pyfunc.PythonModel):
		def __init__(self, *, forecast_rows: list[dict[str, Any]], point_prediction_column: str) -> None:
			self._forecast_rows = forecast_rows
			self._point_prediction_column = point_prediction_column

		def predict(self, context, model_input, params=None):
			import pandas as pd

			frame = pd.DataFrame(self._forecast_rows)
			if params and params.get("point_predictions_only"):
				return frame[["forecast_timestamp", self._point_prediction_column]]
			return frame

	return StaticForecastPyFuncModel


def _try_import_mlflow() -> Any | None:
	try:
		import mlflow
	except ModuleNotFoundError:
		return None
	return mlflow
