from datetime import UTC, datetime

import polars as pl
import pytest

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.forecasting.grid_event_signals import (
	GRID_EVENT_FEATURE_COLUMNS,
	build_grid_event_signal_frame,
)
from smart_arbitrage.forecasting.neural_features import (
	DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
	NEURAL_FORECAST_FEATURE_COLUMNS,
	build_neural_forecast_feature_frame,
)
from smart_arbitrage.resources.grid_event_store import GridEventObservation, grid_event_observations_to_frame


def test_grid_event_signal_frame_uses_only_events_known_at_each_hour() -> None:
	price_history = pl.DataFrame(
		{
			"timestamp": [
				datetime(2026, 4, 30, 8),
				datetime(2026, 4, 30, 10),
			],
			"price_uah_mwh": [2000.0, 2100.0],
		}
	)
	events = grid_event_observations_to_frame(
		[
			_grid_event(
				published_at=datetime(2026, 4, 30, 7, tzinfo=UTC),
				affected_oblasts=["Kharkiv"],
			)
		]
	)

	signal_frame = build_grid_event_signal_frame(
		price_history=price_history,
		grid_events=events,
		tenant_ids=["client_004_kharkiv_hospital"],
	)

	before_event = signal_frame.filter(pl.col("timestamp") == datetime(2026, 4, 30, 8)).row(0, named=True)
	after_event = signal_frame.filter(pl.col("timestamp") == datetime(2026, 4, 30, 10)).row(0, named=True)
	assert before_event["grid_event_count_24h"] == 0.0
	assert before_event["tenant_region_affected"] == 0.0
	assert after_event["grid_event_count_24h"] == 1.0
	assert after_event["tenant_region_affected"] == 1.0
	assert after_event["outage_flag"] == 1.0
	assert after_event["national_grid_risk_score"] > 0.0


def test_grid_event_signal_frame_ignores_non_operational_posts_for_features() -> None:
	price_history = pl.DataFrame(
		{
			"timestamp": [datetime(2026, 5, 5, 12)],
			"price_uah_mwh": [2100.0],
		}
	)
	events = grid_event_observations_to_frame(
		[
			_grid_event(
				published_at=datetime(2026, 5, 5, 8, tzinfo=UTC),
				affected_oblasts=[],
				energy_system_status=False,
				shelling_damage=False,
				outage_or_restriction=False,
				evening_saving_request=False,
			)
		]
	)

	signal_frame = build_grid_event_signal_frame(
		price_history=price_history,
		grid_events=events,
		tenant_ids=["client_004_kharkiv_hospital"],
	)

	row = signal_frame.row(0, named=True)
	assert row["grid_event_count_24h"] == 0.0
	assert row["national_grid_risk_score"] == 0.0
	assert row["event_source_freshness_hours"] == 999.0


def test_neural_feature_frame_includes_grid_events_and_market_liquidity_features() -> None:
	price_history = build_synthetic_market_price_history(
		history_hours=15 * 24,
		forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
		now=datetime(2026, 5, 4, 12),
	).with_columns(
		[
			pl.lit(88.0).alias("volume_mwh"),
			pl.lit(True).alias("low_volume"),
			pl.lit(True).alias("price_spike"),
		]
	)
	signal_timestamp = price_history.select("timestamp").to_series().item(-3)
	grid_event_signal_frame = pl.DataFrame(
		{
			"tenant_id": ["client_004_kharkiv_hospital"],
			"timestamp": [signal_timestamp],
			"grid_event_count_24h": [2.0],
			"tenant_region_affected": [1.0],
			"national_grid_risk_score": [0.85],
			"days_since_grid_event": [0.05],
			"outage_flag": [1.0],
			"saving_request_flag": [1.0],
			"solar_shift_hint": [1.0],
			"event_source_freshness_hours": [1.25],
		}
	)

	feature_frame = build_neural_forecast_feature_frame(
		price_history,
		grid_event_signal_frame=grid_event_signal_frame,
	)

	assert set(GRID_EVENT_FEATURE_COLUMNS).issubset(set(NEURAL_FORECAST_FEATURE_COLUMNS))
	assert {"market_volume_mwh", "market_low_volume", "market_price_spike"}.issubset(
		set(NEURAL_FORECAST_FEATURE_COLUMNS)
	)
	matched_row = feature_frame.filter(pl.col("timestamp") == signal_timestamp).row(0, named=True)
	assert matched_row["grid_event_count_24h"] == pytest.approx(2.0)
	assert matched_row["tenant_region_affected"] == pytest.approx(1.0)
	assert matched_row["market_volume_mwh"] == pytest.approx(88.0)
	assert matched_row["market_low_volume"] == pytest.approx(1.0)
	assert matched_row["market_price_spike"] == pytest.approx(1.0)


def _grid_event(
	*,
	published_at: datetime,
	affected_oblasts: list[str],
	energy_system_status: bool = True,
	shelling_damage: bool = True,
	outage_or_restriction: bool = True,
	evening_saving_request: bool = True,
) -> GridEventObservation:
	return GridEventObservation(
		post_id="Ukrenergo/4914",
		post_url="https://t.me/Ukrenergo/4914",
		published_at=published_at,
		fetched_at=datetime(2026, 4, 30, 9, 5, tzinfo=UTC),
		raw_text="СТАН ЕНЕРГОСИСТЕМИ. Є знеструмлення.",
		source="UKRENERGO_TELEGRAM",
		source_kind="observed",
		source_url="https://t.me/s/Ukrenergo",
		energy_system_status=energy_system_status,
		shelling_damage=shelling_damage,
		outage_or_restriction=outage_or_restriction,
		consumption_change="unknown",
		solar_shift_advice=False,
		evening_saving_request=evening_saving_request,
		affected_oblasts=affected_oblasts,
	)
