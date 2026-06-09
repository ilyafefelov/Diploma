"""Model-ready Silver features for NBEATSx-style and TFT-style forecasts."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final, Literal

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.forecasting.grid_event_signals import GRID_EVENT_FEATURE_COLUMNS, GRID_EVENT_FEATURE_DEFAULTS
from smart_arbitrage.market_rules import market_rule_features

DEFAULT_NEURAL_FORECAST_HORIZON_HOURS: Final[int] = 24
MIN_NEURAL_FORECAST_TRAIN_ROWS: Final[int] = 168
MODEL_VISIBLE_PRICE_COLUMN: Final[str] = "_model_visible_price_uah_mwh"
WEATHER_SOURCE_KIND_COLUMN: Final[str] = "weather_source_kind"
FutureWeatherMode = Literal["historical_benchmark", "forecast_only"]

WEATHER_FEATURE_DEFAULTS: Final[dict[str, float]] = {
	"weather_temperature": 18.0,
	"weather_wind_speed": 5.0,
	"weather_cloudcover": 50.0,
	"weather_precipitation": 0.0,
	"weather_effective_solar": 0.0,
	"weather_known_future_available": 1.0,
}

BATTERY_TELEMETRY_FEATURE_DEFAULTS: Final[dict[str, float]] = {
	"battery_soc": 0.5,
	"battery_soh": 0.97,
	"battery_throughput_mwh": 0.0,
	"battery_efc_delta": 0.0,
	"telemetry_is_fresh": 0.0,
}

MARKET_LIQUIDITY_FEATURE_DEFAULTS: Final[dict[str, float]] = {
	"market_volume_mwh": 1000.0,
	"market_low_volume": 0.0,
	"market_price_spike": 0.0,
}

NEURAL_FORECAST_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
	"hour_sin",
	"hour_cos",
	"weekday_sin",
	"weekday_cos",
	"is_weekend",
	"lag_24_price_uah_mwh",
	"lag_168_price_uah_mwh",
	"rolling_24h_mean_uah_mwh",
	"market_volume_mwh",
	"market_low_volume",
	"market_price_spike",
	"weather_temperature",
	"weather_wind_speed",
	"weather_cloudcover",
	"weather_precipitation",
	"weather_effective_solar",
	"weather_known_future_available",
	"battery_soc",
	"battery_soh",
	"battery_throughput_mwh",
	"battery_efc_delta",
	"telemetry_is_fresh",
	"market_price_cap_max",
	"market_price_cap_min",
	"market_regime_code",
	"days_since_regime_change",
	"is_price_cap_changed_recently",
	*GRID_EVENT_FEATURE_COLUMNS,
)


def build_neural_forecast_feature_frame(
	price_history: pl.DataFrame,
	*,
	battery_state_hourly_snapshots: pl.DataFrame | None = None,
	horizon_hours: int = DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
	timestamp_column: str = DEFAULT_TIMESTAMP_COLUMN,
	price_column: str = DEFAULT_PRICE_COLUMN,
	future_weather_mode: FutureWeatherMode = "historical_benchmark",
	grid_event_signal_frame: pl.DataFrame | None = None,
) -> pl.DataFrame:
	"""Build a full train/forecast feature frame for neural Silver forecasts."""

	if horizon_hours <= 0:
		raise ValueError("horizon_hours must be positive.")
	if future_weather_mode not in {"historical_benchmark", "forecast_only"}:
		raise ValueError("future_weather_mode must be historical_benchmark or forecast_only.")

	history = _prepare_price_history(
		price_history,
		timestamp_column=timestamp_column,
		price_column=price_column,
	)
	if history.height < MIN_NEURAL_FORECAST_TRAIN_ROWS + horizon_hours:
		raise ValueError("Neural forecast features require at least 168 train rows plus the forecast horizon.")

	anchor_timestamp = _resolve_anchor_timestamp(history, horizon_hours=horizon_hours, timestamp_column=timestamp_column)
	history = _join_battery_state_hourly_snapshots(
		history,
		battery_state_hourly_snapshots=battery_state_hourly_snapshots,
		timestamp_column=timestamp_column,
	)
	history = _ensure_weather_columns(history)
	history = _apply_future_weather_mode(
		history,
		anchor_timestamp=anchor_timestamp,
		timestamp_column=timestamp_column,
		future_weather_mode=future_weather_mode,
	)
	history = _ensure_battery_telemetry_columns(history)
	history = _ensure_market_liquidity_columns(history)
	history = _join_grid_event_signal_frame(
		history,
		grid_event_signal_frame=grid_event_signal_frame,
		timestamp_column=timestamp_column,
	)
	history = _ensure_grid_event_columns(history)
	history = _add_market_rule_features(history, timestamp_column=timestamp_column)
	history = _mask_future_prices_for_forecast_features(
		history,
		anchor_timestamp=anchor_timestamp,
		timestamp_column=timestamp_column,
		price_column=price_column,
	)
	feature_frame = (
		history
		.with_columns(
			[
				pl.col(timestamp_column).dt.hour().cast(pl.Float64).alias("_hour"),
				pl.col(timestamp_column).dt.weekday().cast(pl.Float64).alias("_weekday"),
				pl.col(MODEL_VISIBLE_PRICE_COLUMN).shift(24).alias("lag_24_price_uah_mwh"),
				pl.col(MODEL_VISIBLE_PRICE_COLUMN).shift(168).alias("lag_168_price_uah_mwh"),
				pl.col(MODEL_VISIBLE_PRICE_COLUMN).shift(1).rolling_mean(window_size=24, min_samples=1).alias("rolling_24h_mean_uah_mwh"),
			]
		)
		.with_columns(
			[
				((pl.col("_hour") / 24.0) * 2.0 * 3.141592653589793).sin().alias("hour_sin"),
				((pl.col("_hour") / 24.0) * 2.0 * 3.141592653589793).cos().alias("hour_cos"),
				((pl.col("_weekday") / 7.0) * 2.0 * 3.141592653589793).sin().alias("weekday_sin"),
				((pl.col("_weekday") / 7.0) * 2.0 * 3.141592653589793).cos().alias("weekday_cos"),
				pl.col(timestamp_column).dt.weekday().is_in([6, 7]).cast(pl.Float64).alias("is_weekend"),
				pl.when(pl.col(timestamp_column) > pl.lit(anchor_timestamp))
				.then(pl.lit("forecast"))
				.otherwise(pl.lit("train"))
				.alias("split"),
				pl.col(MODEL_VISIBLE_PRICE_COLUMN).alias("target_price_uah_mwh"),
			]
		)
		.with_columns(
			[
				pl.col(column_name).fill_null(default_value).cast(pl.Float64).alias(column_name)
				for column_name, default_value in {
					**WEATHER_FEATURE_DEFAULTS,
					**BATTERY_TELEMETRY_FEATURE_DEFAULTS,
					**MARKET_LIQUIDITY_FEATURE_DEFAULTS,
					**GRID_EVENT_FEATURE_DEFAULTS,
				}.items()
			]
		)
		.select(
			[
				pl.col(timestamp_column).alias("timestamp"),
				pl.col(MODEL_VISIBLE_PRICE_COLUMN).cast(pl.Float64).alias("price_uah_mwh"),
				"target_price_uah_mwh",
				"split",
				"market_regime_id",
				*NEURAL_FORECAST_FEATURE_COLUMNS,
			]
		)
		.sort("timestamp")
	)
	return feature_frame


def training_feature_frame(feature_frame: pl.DataFrame) -> pl.DataFrame:
	"""Return valid training rows with all neural features present."""

	_validate_feature_frame(feature_frame)
	return (
		feature_frame
		.filter(pl.col("split") == "train")
		.drop_nulls(subset=[*NEURAL_FORECAST_FEATURE_COLUMNS, "target_price_uah_mwh"])
		.sort("timestamp")
	)


def forecast_feature_frame(feature_frame: pl.DataFrame) -> pl.DataFrame:
	"""Return the forecast-horizon rows in timestamp order."""

	_validate_feature_frame(feature_frame)
	forecast_rows = feature_frame.filter(pl.col("split") == "forecast").sort("timestamp")
	if forecast_rows.height == 0:
		raise ValueError("feature_frame does not contain forecast rows.")
	return forecast_rows


def feature_matrix(frame: pl.DataFrame) -> list[list[float]]:
	return [
		[float(row[column_name]) for column_name in NEURAL_FORECAST_FEATURE_COLUMNS]
		for row in frame.select(NEURAL_FORECAST_FEATURE_COLUMNS).iter_rows(named=True)
	]


def target_vector(frame: pl.DataFrame) -> list[float]:
	return [float(value) for value in frame.select("target_price_uah_mwh").to_series().to_list()]


def timestamp_vector(frame: pl.DataFrame) -> list[datetime]:
	timestamps: list[datetime] = []
	for value in frame.select("timestamp").to_series().to_list():
		if not isinstance(value, datetime):
			raise TypeError("timestamp column must contain datetime values.")
		timestamps.append(value)
	return timestamps


def _prepare_price_history(
	price_history: pl.DataFrame,
	*,
	timestamp_column: str,
	price_column: str,
) -> pl.DataFrame:
	required_columns = {timestamp_column, price_column}
	missing_columns = required_columns.difference(price_history.columns)
	if missing_columns:
		raise ValueError(f"price_history is missing required columns: {sorted(missing_columns)}")
	history = (
		price_history
		.select(
			[
				timestamp_column,
				price_column,
				*[column for column in WEATHER_FEATURE_DEFAULTS if column in price_history.columns],
				*[column for column in [WEATHER_SOURCE_KIND_COLUMN] if column in price_history.columns],
				*[column for column in ("volume_mwh", "low_volume", "price_spike") if column in price_history.columns],
				*[column for column in MARKET_LIQUIDITY_FEATURE_DEFAULTS if column in price_history.columns],
				*[column for column in GRID_EVENT_FEATURE_DEFAULTS if column in price_history.columns],
			]
		)
		.drop_nulls(subset=[timestamp_column, price_column])
		.with_columns(pl.col(timestamp_column).dt.replace_time_zone(None).alias(timestamp_column))
		.sort(timestamp_column)
		.unique(subset=[timestamp_column], keep="last")
		.sort(timestamp_column)
	)
	if history.height == 0:
		raise ValueError("price_history must contain at least one non-null row.")
	return history


def _ensure_weather_columns(history: pl.DataFrame) -> pl.DataFrame:
	expressions: list[pl.Expr] = []
	for column_name, default_value in WEATHER_FEATURE_DEFAULTS.items():
		if column_name in history.columns:
			expressions.append(pl.col(column_name).fill_null(default_value).alias(column_name))
		else:
			expressions.append(pl.lit(default_value).alias(column_name))
	if WEATHER_SOURCE_KIND_COLUMN not in history.columns:
		expressions.append(pl.lit("unknown").alias(WEATHER_SOURCE_KIND_COLUMN))
	return history.with_columns(expressions)


def _ensure_market_liquidity_columns(history: pl.DataFrame) -> pl.DataFrame:
	expressions: list[pl.Expr] = []
	if "market_volume_mwh" in history.columns:
		expressions.append(pl.col("market_volume_mwh").fill_null(1000.0).cast(pl.Float64).alias("market_volume_mwh"))
	elif "volume_mwh" in history.columns:
		expressions.append(pl.col("volume_mwh").fill_null(1000.0).cast(pl.Float64).alias("market_volume_mwh"))
	else:
		expressions.append(pl.lit(1000.0).alias("market_volume_mwh"))

	if "market_low_volume" in history.columns:
		expressions.append(pl.col("market_low_volume").fill_null(0.0).cast(pl.Float64).alias("market_low_volume"))
	elif "low_volume" in history.columns:
		expressions.append(pl.col("low_volume").fill_null(False).cast(pl.Float64).alias("market_low_volume"))
	else:
		expressions.append(pl.lit(0.0).alias("market_low_volume"))

	if "market_price_spike" in history.columns:
		expressions.append(pl.col("market_price_spike").fill_null(0.0).cast(pl.Float64).alias("market_price_spike"))
	elif "price_spike" in history.columns:
		expressions.append(pl.col("price_spike").fill_null(False).cast(pl.Float64).alias("market_price_spike"))
	else:
		expressions.append(pl.lit(0.0).alias("market_price_spike"))
	return history.with_columns(expressions)


def _ensure_grid_event_columns(history: pl.DataFrame) -> pl.DataFrame:
	expressions: list[pl.Expr] = []
	for column_name, default_value in GRID_EVENT_FEATURE_DEFAULTS.items():
		if column_name in history.columns:
			expressions.append(pl.col(column_name).fill_null(default_value).cast(pl.Float64).alias(column_name))
		else:
			expressions.append(pl.lit(default_value).alias(column_name))
	return history.with_columns(expressions)


def _ensure_battery_telemetry_columns(history: pl.DataFrame) -> pl.DataFrame:
	expressions: list[pl.Expr] = []
	for column_name, default_value in BATTERY_TELEMETRY_FEATURE_DEFAULTS.items():
		if column_name in history.columns:
			expressions.append(pl.col(column_name).fill_null(default_value).alias(column_name))
		else:
			expressions.append(pl.lit(default_value).alias(column_name))
	return history.with_columns(expressions)


def _apply_future_weather_mode(
	history: pl.DataFrame,
	*,
	anchor_timestamp: datetime,
	timestamp_column: str,
	future_weather_mode: FutureWeatherMode,
) -> pl.DataFrame:
	if future_weather_mode == "historical_benchmark":
		return history.with_columns(pl.lit(1.0).alias("weather_known_future_available"))
	future_non_forecast_weather = (
		(pl.col(timestamp_column) > pl.lit(anchor_timestamp))
		& (pl.col(WEATHER_SOURCE_KIND_COLUMN) != "forecast")
	)
	return history.with_columns(
		[
			pl.when(future_non_forecast_weather)
			.then(pl.lit(default_value))
			.otherwise(pl.col(column_name))
			.alias(column_name)
			for column_name, default_value in WEATHER_FEATURE_DEFAULTS.items()
			if column_name != "weather_known_future_available"
		]
		+ [
			pl.when(pl.col(timestamp_column) <= pl.lit(anchor_timestamp))
			.then(pl.lit(1.0))
			.when(pl.col(WEATHER_SOURCE_KIND_COLUMN) == "forecast")
			.then(pl.lit(1.0))
			.otherwise(pl.lit(0.0))
			.alias("weather_known_future_available")
		]
	)


def _add_market_rule_features(history: pl.DataFrame, *, timestamp_column: str) -> pl.DataFrame:
	timestamps = history.select(timestamp_column).to_series().to_list()
	feature_rows = []
	for value in timestamps:
		if not isinstance(value, datetime):
			raise TypeError("timestamp column must contain datetime values.")
		feature_rows.append(market_rule_features(venue="DAM", timestamp=value))
	regime_ids = sorted({str(features["market_regime_id"]) for features in feature_rows})
	regime_codes = {regime_id: float(index) for index, regime_id in enumerate(regime_ids)}
	return history.with_columns(
		[
			pl.Series("market_price_cap_max", [float(features["market_price_cap_max"]) for features in feature_rows]),
			pl.Series("market_price_cap_min", [float(features["market_price_cap_min"]) for features in feature_rows]),
			pl.Series("market_regime_id", [str(features["market_regime_id"]) for features in feature_rows]),
			pl.Series("market_regime_code", [regime_codes[str(features["market_regime_id"])] for features in feature_rows]),
			pl.Series("days_since_regime_change", [float(features["days_since_regime_change"]) for features in feature_rows]),
			pl.Series(
				"is_price_cap_changed_recently",
				[float(features["is_price_cap_changed_recently"]) for features in feature_rows],
			),
		]
	)


def _mask_future_prices_for_forecast_features(
	history: pl.DataFrame,
	*,
	anchor_timestamp: datetime,
	timestamp_column: str,
	price_column: str,
) -> pl.DataFrame:
	return history.with_columns(
		pl.when(pl.col(timestamp_column) <= pl.lit(anchor_timestamp))
		.then(pl.col(price_column))
		.otherwise(pl.lit(None, dtype=pl.Float64))
		.alias(MODEL_VISIBLE_PRICE_COLUMN)
	)


def _join_battery_state_hourly_snapshots(
	history: pl.DataFrame,
	*,
	battery_state_hourly_snapshots: pl.DataFrame | None,
	timestamp_column: str,
) -> pl.DataFrame:
	if battery_state_hourly_snapshots is None or battery_state_hourly_snapshots.height == 0:
		return history
	required_columns = {
		"snapshot_hour",
		"soc_close",
		"soh_close",
		"throughput_mwh",
		"efc_delta",
		"telemetry_freshness",
	}
	missing_columns = required_columns.difference(battery_state_hourly_snapshots.columns)
	if missing_columns:
		raise ValueError(f"battery_state_hourly_snapshots is missing required columns: {sorted(missing_columns)}")
	battery_features = (
		battery_state_hourly_snapshots
		.select(
			[
				pl.col("snapshot_hour").dt.replace_time_zone(None).alias(timestamp_column),
				pl.col("soc_close").cast(pl.Float64).alias("battery_soc"),
				pl.col("soh_close").cast(pl.Float64).alias("battery_soh"),
				pl.col("throughput_mwh").cast(pl.Float64).alias("battery_throughput_mwh"),
				pl.col("efc_delta").cast(pl.Float64).alias("battery_efc_delta"),
				pl.when(pl.col("telemetry_freshness") == "fresh")
				.then(pl.lit(1.0))
				.otherwise(pl.lit(0.0))
				.alias("telemetry_is_fresh"),
			]
		)
		.group_by(timestamp_column)
		.agg(
			[
				pl.col("battery_soc").mean(),
				pl.col("battery_soh").mean(),
				pl.col("battery_throughput_mwh").sum(),
				pl.col("battery_efc_delta").sum(),
				pl.col("telemetry_is_fresh").max(),
			]
		)
	)
	return history.join(battery_features, on=timestamp_column, how="left")


def _join_grid_event_signal_frame(
	history: pl.DataFrame,
	*,
	grid_event_signal_frame: pl.DataFrame | None,
	timestamp_column: str,
) -> pl.DataFrame:
	if grid_event_signal_frame is None or grid_event_signal_frame.height == 0:
		return history
	required_columns = {timestamp_column, *GRID_EVENT_FEATURE_COLUMNS}
	missing_columns = required_columns.difference(grid_event_signal_frame.columns)
	if missing_columns:
		raise ValueError(f"grid_event_signal_frame is missing required columns: {sorted(missing_columns)}")
	aggregations: list[pl.Expr] = []
	for column_name in GRID_EVENT_FEATURE_COLUMNS:
		if column_name in {"days_since_grid_event", "event_source_freshness_hours"}:
			aggregations.append(pl.col(column_name).min())
		else:
			aggregations.append(pl.col(column_name).max())
	grid_features = (
		grid_event_signal_frame
		.select(
			[
				pl.col(timestamp_column).dt.replace_time_zone(None).alias(timestamp_column),
				*[pl.col(column_name).cast(pl.Float64).alias(column_name) for column_name in GRID_EVENT_FEATURE_COLUMNS],
			]
		)
		.group_by(timestamp_column)
		.agg(aggregations)
	)
	return history.join(grid_features, on=timestamp_column, how="left")


def _resolve_anchor_timestamp(history: pl.DataFrame, *, horizon_hours: int, timestamp_column: str) -> datetime:
	timestamp_values = history.select(timestamp_column).to_series().to_list()
	anchor_value = timestamp_values[-horizon_hours - 1]
	latest_value = timestamp_values[-1]
	if not isinstance(anchor_value, datetime) or not isinstance(latest_value, datetime):
		raise TypeError("timestamp column must contain datetime values.")
	expected_latest_timestamp = anchor_value + timedelta(hours=horizon_hours)
	if latest_value != expected_latest_timestamp:
		raise ValueError("Neural forecast feature frame requires contiguous hourly horizon rows.")
	return anchor_value


def _validate_feature_frame(feature_frame: pl.DataFrame) -> None:
	required_columns = {"timestamp", "target_price_uah_mwh", "split", *NEURAL_FORECAST_FEATURE_COLUMNS}
	missing_columns = required_columns.difference(feature_frame.columns)
	if missing_columns:
		raise ValueError(f"feature_frame is missing required columns: {sorted(missing_columns)}")
