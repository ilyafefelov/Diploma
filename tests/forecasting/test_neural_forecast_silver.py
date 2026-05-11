from datetime import UTC, datetime

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.assets.silver.neural_forecasts import (
	NEURAL_FORECAST_SILVER_ASSETS,
	NbeatsxOfficialForecastAssetConfig,
	OfficialGlobalPanelTrainingAssetConfig,
	TftOfficialForecastAssetConfig,
	_forecast_metrics,
	official_global_panel_training_frame,
	nbeatsx_official_global_panel_price_forecast,
	nbeatsx_official_price_forecast,
	nbeatsx_price_forecast,
	neural_forecast_feature_frame,
	official_forecast_exogenous_governance_frame,
	sota_forecast_training_frame,
	tft_official_price_forecast,
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


def test_neural_forecast_feature_frame_does_not_leak_future_realized_prices() -> None:
	price_history = _synthetic_price_history()
	control_features = build_neural_forecast_feature_frame(price_history)
	anchor_timestamp = (
		control_features
		.filter(pl.col("split") == "train")
		.select("timestamp")
		.to_series()
		.item(-1)
	)
	mutated_price_history = price_history.with_columns(
		pl.when(pl.col("timestamp") > anchor_timestamp)
		.then(pl.col("price_uah_mwh") + 50000.0)
		.otherwise(pl.col("price_uah_mwh"))
		.alias("price_uah_mwh")
	)

	mutated_features = build_neural_forecast_feature_frame(mutated_price_history)

	forecast_columns = ["timestamp", *NEURAL_FORECAST_FEATURE_COLUMNS]
	assert (
		control_features
		.filter(pl.col("split") == "forecast")
		.select(forecast_columns)
		.equals(
			mutated_features
			.filter(pl.col("split") == "forecast")
			.select(forecast_columns)
		)
	)
	assert (
		mutated_features
		.filter(pl.col("split") == "forecast")
		.select("target_price_uah_mwh")
		.null_count()
		.item()
		== DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	)


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


def test_neural_forecast_feature_frame_adds_market_regime_features() -> None:
	price_history = _synthetic_price_history()

	feature_frame = build_neural_forecast_feature_frame(price_history)

	assert {
		"market_price_cap_max",
		"market_price_cap_min",
		"days_since_regime_change",
		"is_price_cap_changed_recently",
	}.issubset(set(NEURAL_FORECAST_FEATURE_COLUMNS))
	latest_row = feature_frame.sort("timestamp").row(-1, named=True)
	assert latest_row["market_price_cap_max"] == 15000.0
	assert latest_row["market_price_cap_min"] == 10.0
	assert latest_row["days_since_regime_change"] >= 0.0


def test_forecast_only_weather_mode_masks_future_historical_weather() -> None:
	price_history = _synthetic_price_history().with_columns(
		[
			pl.lit(77.0).alias("weather_temperature"),
			pl.lit("observed").alias("weather_source_kind"),
		]
	)

	feature_frame = build_neural_forecast_feature_frame(
		price_history,
		future_weather_mode="forecast_only",
	)

	forecast_rows = feature_frame.filter(pl.col("split") == "forecast")
	assert forecast_rows.select("weather_temperature").to_series().unique().to_list() == [18.0]
	assert forecast_rows.select("weather_known_future_available").to_series().unique().to_list() == [0.0]


def test_forecast_only_weather_mode_keeps_future_forecast_weather() -> None:
	price_history = _synthetic_price_history().with_columns(
		[
			pl.lit(11.0).alias("weather_temperature"),
			pl.lit("forecast").alias("weather_source_kind"),
		]
	)

	feature_frame = build_neural_forecast_feature_frame(
		price_history,
		future_weather_mode="forecast_only",
	)

	forecast_rows = feature_frame.filter(pl.col("split") == "forecast")
	assert forecast_rows.select("weather_temperature").to_series().unique().to_list() == [11.0]
	assert forecast_rows.select("weather_known_future_available").to_series().unique().to_list() == [1.0]


def test_neural_forecast_feature_frame_normalizes_utc_battery_snapshot_timestamps() -> None:
	price_history = _synthetic_price_history()
	timestamp = price_history.select("timestamp").to_series().item(-5)
	telemetry_frame = pl.DataFrame(
		{
			"tenant_id": ["client_001_kyiv_mall"],
			"snapshot_hour": [timestamp.replace(tzinfo=UTC)],
			"soc_close": [0.71],
			"soh_close": [0.952],
			"throughput_mwh": [0.04],
			"efc_delta": [0.04],
			"telemetry_freshness": ["fresh"],
		}
	)

	feature_frame = build_neural_forecast_feature_frame(
		price_history,
		battery_state_hourly_snapshots=telemetry_frame,
	)

	matched_row = feature_frame.filter(pl.col("timestamp") == timestamp).row(0, named=True)
	assert matched_row["battery_soc"] == 0.71
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
		"sota_forecast_training_frame",
		"official_forecast_exogenous_governance_frame",
		"official_global_panel_training_frame",
		"nbeatsx_official_global_panel_price_forecast",
		"nbeatsx_price_forecast",
		"tft_price_forecast",
		"nbeatsx_official_price_forecast",
		"tft_official_price_forecast",
	}.issubset(asset_keys)
	assert asset_keys.issubset(registered_asset_keys)
	tags_by_key = {
		asset_key.to_user_string(): tags
		for asset in NEURAL_FORECAST_SILVER_ASSETS
		for asset_key, tags in asset.tags_by_key.items()
	}
	groups_by_key = {
		asset_key.to_user_string(): group
		for asset in NEURAL_FORECAST_SILVER_ASSETS
		for asset_key, group in asset.group_names_by_key.items()
	}
	assert tags_by_key["sota_forecast_training_frame"]["medallion"] == "silver"
	assert groups_by_key["neural_forecast_feature_frame"] == "silver_forecast_features"
	assert groups_by_key["sota_forecast_training_frame"] == "silver_forecast_features"
	assert groups_by_key["official_forecast_exogenous_governance_frame"] == "silver_forecast_features"
	assert groups_by_key["official_global_panel_training_frame"] == "silver_forecast_features"
	assert groups_by_key["nbeatsx_official_global_panel_price_forecast"] == "silver_forecast_candidates"
	assert groups_by_key["nbeatsx_price_forecast"] == "silver_forecast_candidates"
	assert groups_by_key["tft_price_forecast"] == "silver_forecast_candidates"


def test_official_global_panel_training_asset_materializes_multi_tenant_contract() -> None:
	price_history = _synthetic_price_history().with_columns(
		[
			pl.lit("client_003_dnipro_factory").alias("tenant_id"),
			pl.lit("observed").alias("source_kind"),
			pl.lit("forecast").alias("weather_source_kind"),
		]
	)

	governance = official_forecast_exogenous_governance_frame(None)

	frame = official_global_panel_training_frame(
		None,
		OfficialGlobalPanelTrainingAssetConfig(),
		price_history,
		governance,
	)

	assert frame.select("unique_id").to_series().unique().to_list() == [
		"client_003_dnipro_factory:DAM"
	]
	assert frame.select("sota_schema_version").to_series().unique().to_list() == [
		"official_global_panel_sota_v1"
	]
	assert frame.filter(pl.col("split") == "forecast").select("y").null_count().item() == (
		DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	)
	assert frame.select("target_scaler_fit_scope").to_series().unique().to_list() == [
		"train_rows_only_per_unique_id"
	]
	assert frame.select("external_feature_training_status").to_series().unique().to_list() == [
		"blocked_by_governance"
	]
	assert "entsoe_neighbor_day_ahead_price_context" in frame.select(
		"blocked_external_feature_columns_csv"
	).to_series().item(0)


def test_official_global_panel_nbeatsx_asset_uses_global_panel_contract(monkeypatch) -> None:
	training_frame = pl.DataFrame(
		{
			"unique_id": ["client_001_kyiv_mall:DAM", "client_001_kyiv_mall:DAM"],
			"ds": [datetime(2026, 5, 1, 1), datetime(2026, 5, 1, 2)],
			"y": [None, None],
			"split": ["forecast", "forecast"],
			"tenant_id": ["client_001_kyiv_mall", "client_001_kyiv_mall"],
			"market_venue": ["DAM", "DAM"],
			"is_train": [False, False],
			"is_forecast": [True, True],
			"sota_schema_version": ["official_global_panel_sota_v1", "official_global_panel_sota_v1"],
			"supported_backends_csv": ["neuralforecast_nbeatsx", "neuralforecast_nbeatsx"],
			"known_future_feature_columns_csv": ["weather_temperature", "weather_temperature"],
			"historical_observed_feature_columns_csv": ["lag_24_price_uah_mwh", "lag_24_price_uah_mwh"],
			"static_feature_columns_csv": ["tenant_id,market_venue", "tenant_id,market_venue"],
			"target_scaler_fit_scope": ["train_rows_only_per_unique_id", "train_rows_only_per_unique_id"],
			"temporal_scaler_type": ["robust", "robust"],
		}
	)
	captured: dict[str, object] = {}

	def fake_global_nbeatsx(frame: pl.DataFrame, **kwargs: object) -> pl.DataFrame:
		captured["schema_version"] = frame.select("sota_schema_version").to_series().item(0)
		captured.update(kwargs)
		return pl.DataFrame(
			{
				"model_name": ["nbeatsx_official_global_panel_v1", "nbeatsx_official_global_panel_v1"],
				"model_family": ["NBEATSx", "NBEATSx"],
				"backend_name": ["neuralforecast", "neuralforecast"],
				"backend_status": ["trained", "trained"],
				"unique_id": ["client_001_kyiv_mall:DAM", "client_001_kyiv_mall:DAM"],
				"forecast_timestamp": [datetime(2026, 5, 1, 1), datetime(2026, 5, 1, 2)],
				"predicted_price_uah_mwh": [4100.0, 4200.0],
				"predicted_price_p10_uah_mwh": [None, None],
				"predicted_price_p50_uah_mwh": [4100.0, 4200.0],
				"predicted_price_p90_uah_mwh": [None, None],
				"prediction_interval_kind": ["point", "point"],
				"training_rows": [100, 100],
				"horizon_rows": [2, 2],
				"adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * 2,
			}
		)

	def fake_backend_status() -> dict[str, object]:
		class Status:
			available = True
			reason = "fake_backend"

		return {
			"neuralforecast": Status(),
			"pytorch_forecasting": Status(),
			"lightning": Status(),
		}

	monkeypatch.setattr(
		"smart_arbitrage.assets.silver.neural_forecasts.build_official_global_panel_nbeatsx_forecast",
		fake_global_nbeatsx,
	)
	monkeypatch.setattr("smart_arbitrage.assets.silver.neural_forecasts.inspect_official_forecast_backends", fake_backend_status)

	forecast = nbeatsx_official_global_panel_price_forecast(
		None,
		NbeatsxOfficialForecastAssetConfig(max_steps=25, random_seed=20260511),
		training_frame,
	)

	assert forecast.select("model_name").to_series().unique().to_list() == [
		"nbeatsx_official_global_panel_v1"
	]
	assert captured["schema_version"] == "official_global_panel_sota_v1"
	assert captured["max_steps"] == 25
	assert captured["random_seed"] == 20260511


def test_neural_forecast_assets_materialize_dataframes(monkeypatch) -> None:
	price_history = _synthetic_price_history()
	captured: dict[str, object] = {}

	def fake_official_forecast(
		training_frame: pl.DataFrame,
		**kwargs: object,
	) -> pl.DataFrame:
		captured.update(kwargs)
		forecast_timestamps = (
			training_frame
			.filter(pl.col("split") == "forecast")
			.select("ds")
			.to_series()
			.to_list()
		)
		return pl.DataFrame(
			{
				"model_name": ["official_fake_v0"] * len(forecast_timestamps),
				"model_family": ["official_fake"] * len(forecast_timestamps),
				"backend_name": ["fake"] * len(forecast_timestamps),
				"backend_status": ["trained"] * len(forecast_timestamps),
				"unique_id": ["global_research_tenant"] * len(forecast_timestamps),
				"forecast_timestamp": forecast_timestamps,
				"predicted_price_uah_mwh": [4100.0] * len(forecast_timestamps),
				"predicted_price_p10_uah_mwh": [4000.0] * len(forecast_timestamps),
				"predicted_price_p50_uah_mwh": [4100.0] * len(forecast_timestamps),
				"predicted_price_p90_uah_mwh": [4200.0] * len(forecast_timestamps),
				"prediction_interval_kind": ["fake"] * len(forecast_timestamps),
				"training_rows": [training_frame.filter(pl.col("split") == "train").height] * len(forecast_timestamps),
				"horizon_rows": [len(forecast_timestamps)] * len(forecast_timestamps),
				"adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * len(forecast_timestamps),
			}
		)

	def fake_backend_status() -> dict[str, object]:
		class Status:
			available = True
			reason = "fake_backend"

		return {
			"neuralforecast": Status(),
			"pytorch_forecasting": Status(),
			"lightning": Status(),
		}

	monkeypatch.setattr("smart_arbitrage.assets.silver.neural_forecasts.build_official_nbeatsx_forecast", fake_official_forecast)
	monkeypatch.setattr("smart_arbitrage.assets.silver.neural_forecasts.build_official_tft_forecast", fake_official_forecast)
	monkeypatch.setattr("smart_arbitrage.assets.silver.neural_forecasts.inspect_official_forecast_backends", fake_backend_status)

	feature_asset_output = neural_forecast_feature_frame(None, price_history)
	sota_asset_output = sota_forecast_training_frame(None, feature_asset_output)
	nbeatsx_asset_output = nbeatsx_price_forecast(None, feature_asset_output)
	tft_asset_output = tft_price_forecast(None, feature_asset_output)
	official_nbeatsx_output = nbeatsx_official_price_forecast(
		None,
		NbeatsxOfficialForecastAssetConfig(max_steps=100, random_seed=20260509),
		sota_asset_output,
	)
	official_tft_output = tft_official_price_forecast(
		None,
		TftOfficialForecastAssetConfig(
			max_epochs=15,
			batch_size=32,
			learning_rate=0.005,
			hidden_size=12,
			hidden_continuous_size=6,
		),
		sota_asset_output,
	)

	assert feature_asset_output.filter(pl.col("split") == "forecast").height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert sota_asset_output.filter(pl.col("split") == "forecast").height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert nbeatsx_asset_output.height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert tft_asset_output.height == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert {"model_name", "backend_status", "forecast_timestamp", "predicted_price_uah_mwh"}.issubset(official_nbeatsx_output.columns)
	assert {"model_name", "backend_status", "forecast_timestamp", "predicted_price_uah_mwh"}.issubset(official_tft_output.columns)
	assert captured["max_steps"] == 100
	assert captured["random_seed"] == 20260509
	assert captured["max_epochs"] == 15
	assert captured["batch_size"] == 32
	assert captured["learning_rate"] == 0.005
	assert captured["hidden_size"] == 12
	assert captured["hidden_continuous_size"] == 6


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


def test_forecast_metrics_skip_empty_prediction_interval_width_for_point_forecasts() -> None:
	forecast = pl.DataFrame(
		{
			"forecast_timestamp": [datetime(2026, 5, 6, 14), datetime(2026, 5, 6, 15)],
			"predicted_price_uah_mwh": [4200.0, 4300.0],
			"predicted_price_p10_uah_mwh": [None, None],
			"predicted_price_p90_uah_mwh": [None, None],
		}
	)

	metrics = _forecast_metrics(forecast, point_prediction_column="predicted_price_uah_mwh")

	assert metrics["horizon_rows"] == 2.0
	assert metrics["mean_prediction_uah_mwh"] == 4250.0
	assert "mean_prediction_interval_width_uah_mwh" not in metrics


def test_neural_forecast_asset_registers_frozen_candidate_when_tracking_uri_is_set(monkeypatch) -> None:
	forecast_store = InMemoryForecastStore()
	fake_mlflow = _FakeMlflow()
	monkeypatch.setattr("smart_arbitrage.assets.silver.neural_forecasts.get_forecast_store", lambda: forecast_store)
	monkeypatch.setattr("smart_arbitrage.assets.silver.neural_forecasts._try_import_mlflow", lambda: fake_mlflow)
	monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow.test:5000")
	feature_frame = build_neural_forecast_feature_frame(_synthetic_price_history())

	nbeatsx_price_forecast(None, feature_frame)

	assert fake_mlflow.tracking_uri == "http://mlflow.test:5000"
	assert fake_mlflow.experiment_name == "smart-arbitrage-forecast-research"
	assert fake_mlflow.logged_params["model_name"] == "nbeatsx_silver_v0"
	assert fake_mlflow.logged_metrics["horizon_rows"] == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
	assert fake_mlflow.logged_models[0]["registered_model_name"] == "smart-arbitrage-nbeatsx-silver"
	assert fake_mlflow.logged_models[0]["metadata"]["model_kind"] == "frozen_forecast_candidate"
	assert fake_mlflow.registered_model_tags["smart-arbitrage-nbeatsx-silver"]["forecast_layer"] == "silver"
	assert fake_mlflow.registered_model_tags["smart-arbitrage-nbeatsx-silver"]["latest_forecast_run_id"]
	assert fake_mlflow.registered_model_tags["smart-arbitrage-nbeatsx-silver"]["latest_registered_model_version"] == "1"


class _FakeRunInfo:
	run_id = "fake-run-id"


class _FakeRun:
	info = _FakeRunInfo()


class _FakeRunContext:
	def __init__(self, fake_mlflow: "_FakeMlflow") -> None:
		self._fake_mlflow = fake_mlflow

	def __enter__(self) -> _FakeRun:
		self._fake_mlflow.started_runs += 1
		return _FakeRun()

	def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
		return None


class _FakeModelInfo:
	registered_model_version = "1"


class _FakePyFunc:
	PythonModel = object

	def __init__(self, fake_mlflow: "_FakeMlflow") -> None:
		self._fake_mlflow = fake_mlflow

	def log_model(self, **kwargs: object) -> _FakeModelInfo:
		self._fake_mlflow.logged_models.append(kwargs)
		return _FakeModelInfo()


class _FakeMlflowClient:
	def __init__(self, fake_mlflow: "_FakeMlflow") -> None:
		self._fake_mlflow = fake_mlflow

	def set_registered_model_tag(self, name: str, key: str, value: str) -> None:
		self._fake_mlflow.registered_model_tags.setdefault(name, {})[key] = value


class _FakeMlflow:
	def __init__(self) -> None:
		self.pyfunc = _FakePyFunc(self)
		self.tracking_uri: str | None = None
		self.experiment_name: str | None = None
		self.started_runs = 0
		self.logged_params: dict[str, object] = {}
		self.logged_metrics: dict[str, float] = {}
		self.logged_tags: dict[str, str] = {}
		self.logged_dicts: dict[str, object] = {}
		self.logged_models: list[dict[str, object]] = []
		self.registered_model_tags: dict[str, dict[str, str]] = {}

	def set_tracking_uri(self, tracking_uri: str) -> None:
		self.tracking_uri = tracking_uri

	def set_experiment(self, experiment_name: str) -> None:
		self.experiment_name = experiment_name

	def start_run(self, *, run_name: str) -> _FakeRunContext:
		self.logged_params["run_name"] = run_name
		return _FakeRunContext(self)

	def log_param(self, key: str, value: object) -> None:
		self.logged_params[key] = value

	def log_metric(self, key: str, value: float) -> None:
		self.logged_metrics[key] = value

	def set_tag(self, key: str, value: str) -> None:
		self.logged_tags[key] = value

	def log_dict(self, dictionary: object, artifact_file: str) -> None:
		self.logged_dicts[artifact_file] = dictionary

	def MlflowClient(self) -> _FakeMlflowClient:
		return _FakeMlflowClient(self)
