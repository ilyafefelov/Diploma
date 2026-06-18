import json
from pathlib import Path
from typing import Any, cast
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from fastapi.testclient import TestClient
import polars as pl
import pytest

import api.main as api_main
from smart_arbitrage.resources.operator_status_store import (
	OperatorFlowStatus,
	OperatorFlowType,
	OperatorStatusRecord,
)
from smart_arbitrage.resources.validation_failure_store import (
	InMemoryValidationFailureStore,
	ValidationFailureRecord,
	ValidationStage,
)
from smart_arbitrage.resources.battery_telemetry_store import (
	BatteryStateHourlySnapshot,
	BatteryTelemetryObservation,
	InMemoryBatteryTelemetryStore,
)
from smart_arbitrage.resources.grid_event_store import GridEventObservation, InMemoryGridEventStore
from smart_arbitrage.resources.market_data_store import (
	InMemoryMarketDataStore,
	MarketPriceObservation,
	WeatherObservation,
)
from smart_arbitrage.resources.dfl_training_store import InMemoryDflTrainingStore
from smart_arbitrage.resources.forecast_store import InMemoryForecastStore
from smart_arbitrage.research.operator_preview_refresh import OperatorPreviewEnsureResult
from smart_arbitrage.resources.simulated_trade_store import InMemorySimulatedTradeStore
from smart_arbitrage.resources.strategy_evaluation_store import InMemoryStrategyEvaluationStore


class _MaterializeResult:
	def __init__(self, *, success: bool) -> None:
		self.success = success


class _FakeOperatorStatusStore:
	def __init__(self) -> None:
		self.records: dict[tuple[str, OperatorFlowType], OperatorStatusRecord] = {}

	def upsert_status(self, record: OperatorStatusRecord) -> None:
		self.records[(record.tenant_id, record.flow_type)] = record

	def get_status(self, *, tenant_id: str, flow_type: OperatorFlowType) -> OperatorStatusRecord | None:
		return self.records.get((tenant_id, flow_type))


@pytest.fixture
def fake_status_store(monkeypatch: pytest.MonkeyPatch) -> _FakeOperatorStatusStore:
	store = _FakeOperatorStatusStore()
	monkeypatch.setattr(api_main, "get_operator_status_store", lambda: store)
	return store


@pytest.fixture
def fake_validation_failure_store(
	monkeypatch: pytest.MonkeyPatch,
) -> InMemoryValidationFailureStore:
	store = InMemoryValidationFailureStore()
	monkeypatch.setattr(api_main, "get_validation_failure_store", lambda: store)
	return store


@pytest.fixture
def fake_battery_telemetry_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryBatteryTelemetryStore:
	store = InMemoryBatteryTelemetryStore()
	monkeypatch.setattr(api_main, "get_battery_telemetry_store", lambda: store)
	return store


@pytest.fixture
def fake_strategy_evaluation_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryStrategyEvaluationStore:
	store = InMemoryStrategyEvaluationStore()
	monkeypatch.setattr(api_main, "get_strategy_evaluation_store", lambda: store)
	return store


@pytest.fixture
def fake_dfl_training_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryDflTrainingStore:
	store = InMemoryDflTrainingStore()
	monkeypatch.setattr(api_main, "get_dfl_training_store", lambda: store)
	return store


@pytest.fixture
def fake_simulated_trade_store(monkeypatch: pytest.MonkeyPatch) -> InMemorySimulatedTradeStore:
	store = InMemorySimulatedTradeStore()
	monkeypatch.setattr(api_main, "get_simulated_trade_store", lambda: store)
	return store


@pytest.fixture
def fake_forecast_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryForecastStore:
	store = InMemoryForecastStore()
	monkeypatch.setattr(api_main, "get_forecast_store", lambda: store)
	return store


@pytest.fixture
def fake_market_data_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryMarketDataStore:
	store = InMemoryMarketDataStore()
	monkeypatch.setattr(api_main, "get_market_data_store", lambda: store)
	return store


@pytest.fixture
def client(fake_market_data_store: InMemoryMarketDataStore) -> TestClient:
	_seed_official_oree_dam_rows(fake_market_data_store)
	return TestClient(api_main.app)


@pytest.fixture
def fake_grid_event_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryGridEventStore:
	store = InMemoryGridEventStore()
	monkeypatch.setattr(api_main, "get_grid_event_store", lambda: store)
	return store


def _seed_official_oree_dam_rows(
	store: InMemoryMarketDataStore,
	*,
	start: datetime = datetime(2026, 5, 1, tzinfo=UTC),
	hours: int = 15 * 24,
	price_offset_uah_mwh: float = 0.0,
) -> None:
	store.upsert_market_prices(
		[
			MarketPriceObservation(
				timestamp=start + timedelta(hours=index),
				price_uah_mwh=2400.0
				+ price_offset_uah_mwh
				+ ((index % 24) * 42.0)
				+ (520.0 if 18 <= (index % 24) <= 21 else 0.0)
				- (180.0 if 0 <= (index % 24) <= 5 else 0.0),
				price_eur_mwh=50.0 + ((index % 24) * 0.9),
				volume_mwh=2100.0 + (index % 7) * 12.0,
				source="OREE_DATA_VIEW",
				source_kind="observed",
				source_url="https://www.oree.com.ua/index.php/pricectr/data_view",
				market_venue="DAM",
				market_zone="IPS",
				market_timezone="Europe/Kyiv",
				fetched_at=start + timedelta(hours=index, minutes=20),
				price_spike=False,
				low_volume=False,
			)
			for index in range(hours)
		]
	)


def _seed_official_oree_idm_rows(
	store: InMemoryMarketDataStore,
	*,
	start: datetime = datetime(2026, 5, 1, tzinfo=UTC),
	hours: int = 15 * 24,
) -> None:
	store.upsert_market_prices(
		[
			MarketPriceObservation(
				timestamp=start + timedelta(hours=index),
				price_uah_mwh=2550.0
				+ ((index % 24) * 38.0)
				+ (430.0 if 17 <= (index % 24) <= 22 else 0.0)
				- (140.0 if 0 <= (index % 24) <= 5 else 0.0),
				price_eur_mwh=54.0 + ((index % 24) * 0.8),
				volume_mwh=900.0 + (index % 7) * 9.0,
				source="OREE_DATA_VIEW",
				source_kind="observed",
				source_url="https://www.oree.com.ua/index.php/pricectr/data_view",
				market_venue="IDM",
				market_zone="IPS",
				market_timezone="Europe/Kyiv",
				fetched_at=start + timedelta(hours=index, minutes=25),
				price_spike=False,
				low_volume=False,
			)
			for index in range(hours)
		]
	)


def _forecast_frame(
	*,
	target_date: datetime,
	values: list[float],
	generated_at: datetime | None = None,
	market_venue: str = "DAM",
	training_cutoff: datetime | None = None,
) -> pl.DataFrame:
	resolved_training_cutoff = training_cutoff or target_date - timedelta(hours=1)
	resolved_generated_at = generated_at or resolved_training_cutoff
	horizon_end = target_date + timedelta(hours=len(values) - 1)
	return pl.DataFrame(
		{
			"forecast_timestamp": [
				target_date + timedelta(hours=hour)
				for hour in range(len(values))
			],
			"predicted_price_uah_mwh": values,
			"generated_at": [resolved_generated_at for _ in values],
			"market_venue": [market_venue for _ in values],
			"training_cutoff": [resolved_training_cutoff for _ in values],
			"feature_cutoff": [resolved_training_cutoff for _ in values],
			"horizon_start": [target_date for _ in values],
			"horizon_end": [horizon_end for _ in values],
			"source_window_start": [resolved_training_cutoff - timedelta(days=14) for _ in values],
			"source_window_end": [resolved_training_cutoff for _ in values],
		}
	)


def _install_fake_operator_preview_forecast_materializer(
	monkeypatch: pytest.MonkeyPatch,
	forecast_store: InMemoryForecastStore,
) -> list[dict[str, Any]]:
	calls: list[dict[str, Any]] = []

	def _fake_materialize_operator_preview_forecast_runs(
		*,
		market_data_store: object,
		forecast_store: InMemoryForecastStore,
		tenant_id: str,
		market_venue: str,
		forecast_start: datetime,
		horizon_hours: int = 72,
		**_: object,
	) -> SimpleNamespace:
		del market_data_store
		resolved_market_venue = market_venue.upper()
		calls.append(
			{
				"tenant_id": tenant_id,
				"market_venue": resolved_market_venue,
				"forecast_start": forecast_start,
				"horizon_hours": horizon_hours,
			}
		)
		model_names = (
			("nbeatsx_official_idm_v0", "tft_official_idm_v0")
			if resolved_market_venue == "IDM"
			else ("nbeatsx_official_v0", "tft_official_v0")
		)
		for model_index, model_name in enumerate(model_names):
			values = [
				2500.0
				+ model_index * 120.0
				+ hour * 18.0
				+ (3200.0 if hour % 24 in {18, 19, 20, 21} else 0.0)
				- (900.0 if hour % 24 in {2, 3, 4} else 0.0)
				for hour in range(horizon_hours)
			]
			forecast_store.upsert_forecast_run(
				model_name=model_name,
				forecast_frame=_forecast_frame(
					target_date=forecast_start,
					values=values,
					generated_at=forecast_start - timedelta(hours=1),
					market_venue=resolved_market_venue,
					training_cutoff=forecast_start - timedelta(hours=1),
				),
				point_prediction_column="predicted_price_uah_mwh",
			)
		return SimpleNamespace(
			market_venue=resolved_market_venue,
			forecast_start=forecast_start,
			horizon_hours=horizon_hours,
			market_execution_enabled=False,
		)

	monkeypatch.setattr(
		api_main,
		"materialize_operator_preview_forecast_runs",
		_fake_materialize_operator_preview_forecast_runs,
		raising=False,
	)
	return calls


def test_healthcheck_returns_ok(client: TestClient) -> None:
	response = client.get("/health")

	assert response.status_code == 200
	assert response.json() == {"status": "ok"}


def test_list_tenants_returns_known_registry_entry(client: TestClient) -> None:
	response = client.get("/tenants")

	assert response.status_code == 200
	response_payload = response.json()
	assert any(tenant["tenant_id"] == "client_002_lviv_office" for tenant in response_payload)


def test_run_config_endpoint_returns_resolved_location(
	client: TestClient,
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
	response = client.post(
		"/weather/run-config",
		json={
			"tenant_id": "client_002_lviv_office",
			"location_config_path": "simulations/tenants.yml",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_002_lviv_office"
	assert response_payload["resolved_location"] == {
		"latitude": 49.84,
		"longitude": 24.03,
		"timezone": "Europe/Kyiv",
	}
	assert response_payload["run_config"] == {
		"ops": {
			"weather_forecast_bronze": {
				"config": {
					"tenant_id": "client_002_lviv_office",
					"location_config_path": "simulations/tenants.yml",
				}
			}
		}
	}
	status_record = fake_status_store.get_status(
		tenant_id="client_002_lviv_office",
		flow_type=OperatorFlowType.WEATHER_CONTROL,
	)
	assert status_record is not None
	assert status_record.status == OperatorFlowStatus.PREPARED


def test_run_config_endpoint_returns_404_for_unknown_tenant(client: TestClient) -> None:
	response = client.post(
		"/weather/run-config",
		json={
			"tenant_id": "unknown_tenant",
		},
	)

	assert response.status_code == 404
	assert "unknown_tenant" in response.json()["detail"]


def test_materialize_endpoint_returns_selected_assets(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
	def fake_materialize(selected_assets: list[Any], *, run_config: dict[str, Any]) -> _MaterializeResult:
		assert [asset.key.path[-1] for asset in selected_assets] == [
			"weather_forecast_bronze",
			"dam_price_history",
		]
		assert run_config == {
			"ops": {
				"weather_forecast_bronze": {
					"config": {
						"tenant_id": "client_002_lviv_office",
						"location_config_path": "simulations/tenants.yml",
					}
				}
			}
		}
		return _MaterializeResult(success=True)

	monkeypatch.setattr(api_main.dg, "materialize", fake_materialize)

	response = client.post(
		"/weather/materialize",
		json={
			"tenant_id": "client_002_lviv_office",
			"include_price_history": True,
			"location_config_path": "simulations/tenants.yml",
		},
	)

	assert response.status_code == 200
	assert response.json() == {
		"tenant_id": "client_002_lviv_office",
		"selected_assets": ["weather_forecast_bronze", "dam_price_history"],
		"run_config": {
			"ops": {
				"weather_forecast_bronze": {
					"config": {
						"tenant_id": "client_002_lviv_office",
						"location_config_path": "simulations/tenants.yml",
					}
				}
			}
		},
		"resolved_location": {
			"latitude": 49.84,
			"longitude": 24.03,
			"timezone": "Europe/Kyiv",
		},
		"success": True,
	}
	status_record = fake_status_store.get_status(
		tenant_id="client_002_lviv_office",
		flow_type=OperatorFlowType.WEATHER_CONTROL,
	)
	assert status_record is not None
	assert status_record.status == OperatorFlowStatus.COMPLETED
	assert status_record.payload is not None
	assert status_record.payload["selected_assets"] == ["weather_forecast_bronze", "dam_price_history"]


def test_materialize_endpoint_returns_500_on_failed_materialization(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
	def fake_materialize(selected_assets: list[Any], *, run_config: dict[str, Any]) -> _MaterializeResult:
		return _MaterializeResult(success=False)

	monkeypatch.setattr(api_main.dg, "materialize", fake_materialize)

	response = client.post(
		"/weather/materialize",
		json={
			"tenant_id": "client_002_lviv_office",
			"include_price_history": False,
			"location_config_path": "simulations/tenants.yml",
		},
	)

	assert response.status_code == 500
	assert response.json() == {"detail": "Dagster materialization failed."}
	status_record = fake_status_store.get_status(
		tenant_id="client_002_lviv_office",
		flow_type=OperatorFlowType.WEATHER_CONTROL,
	)
	assert status_record is not None
	assert status_record.status == OperatorFlowStatus.FAILED
	assert status_record.last_error == "Dagster materialization failed."


def test_dashboard_signal_preview_returns_tenant_aware_series(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
	def fake_build_weather_forecast_window(*, start_timestamp: datetime, hours: int, weather_location: Any) -> pl.DataFrame:
		return pl.DataFrame(
			{
				api_main.DEFAULT_TIMESTAMP_COLUMN: [
					start_timestamp + timedelta(hours=index)
					for index in range(hours)
				],
				"temperature": [18.0 + (index * 0.9) for index in range(hours)],
				"wind_speed": [2.0 + (index * 0.15) for index in range(hours)],
				"cloudcover": [12.0 + (index * 3.5) for index in range(hours)],
				"precipitation": [0.0 if index < 8 else 0.8 for index in range(hours)],
				"humidity": [52.0 + (index * 1.2) for index in range(hours)],
				"effective_solar": [max(0.0, 420.0 - (index * 18.0)) for index in range(hours)],
				"source": ["OPEN_METEO" for _ in range(hours)],
			}
		)

	monkeypatch.setattr(api_main, "build_weather_forecast_window", fake_build_weather_forecast_window)

	response = client.get(
		"/dashboard/signal-preview",
		params={
			"tenant_id": "client_002_lviv_office",
			"location_config_path": "simulations/tenants.yml",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_002_lviv_office"
	assert len(response_payload["labels"]) == 6
	assert len(response_payload["label_timestamps"]) == 6
	assert response_payload["label_timestamps"][0].startswith("2026-")
	assert all(len(label) == 5 and label[2] == ":" for label in response_payload["labels"])
	assert response_payload["latest_price_timestamp"] is not None
	assert response_payload["forecast_window_start"] is not None
	assert response_payload["forecast_window_end"] is not None
	assert response_payload["timezone"] == "Europe/Kyiv"
	assert len(response_payload["market_price"]) == 6
	assert len(response_payload["weather_bias"]) == 6
	assert response_payload["weather_sources"] == ["OPEN_METEO"] * 6
	assert len(response_payload["charge_intent"]) == 6
	assert len(response_payload["regret"]) == 6
	assert min(response_payload["market_price"]) > 1000.0
	assert min(response_payload["weather_bias"]) >= 0.0
	assert len(set(response_payload["weather_bias"])) > 1
	assert max(abs(value) for value in response_payload["charge_intent"]) <= 2.5
	assert min(response_payload["regret"]) >= 80.0
	assert response_payload["resolved_location"] == {
		"latitude": 49.84,
		"longitude": 24.03,
		"timezone": "Europe/Kyiv",
	}
	status_record = fake_status_store.get_status(
		tenant_id="client_002_lviv_office",
		flow_type=OperatorFlowType.SIGNAL_PREVIEW,
	)
	assert status_record is not None
	assert status_record.status == OperatorFlowStatus.COMPLETED


def test_projected_battery_state_returns_hourly_trace_with_override(
	client: TestClient,
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
	response = client.post(
		"/dashboard/projected-battery-state",
		json={
			"tenant_id": "client_003_dnipro_factory",
			"current_soc_fraction": 0.5,
			"battery_metrics": {
				"capacity_mwh": 4.0,
				"max_power_mw": 2.0,
				"round_trip_efficiency": 0.81,
				"degradation_cost_per_cycle_uah": 40.0,
				"soc_min_fraction": 0.25,
				"soc_max_fraction": 0.75,
			},
			"schedule": [
				{"interval_start": "2026-05-01T06:00:00Z", "net_power_mw": 1.0},
				{"interval_start": "2026-05-01T07:00:00Z", "net_power_mw": -2.0},
				{"interval_start": "2026-05-01T08:00:00Z", "net_power_mw": 3.0},
			],
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["interval_minutes"] == 60
	assert response_payload["starting_soc_fraction"] == 0.5
	assert response_payload["total_throughput_mwh"] == pytest.approx(4.52, rel=1e-3)
	assert response_payload["total_degradation_penalty_uah"] == pytest.approx(22.6, rel=1e-3)
	assert [point["requested_net_power_mw"] for point in response_payload["trace"]] == [1.0, -2.0, 3.0]
	assert [point["feasible_net_power_mw"] for point in response_payload["trace"]] == pytest.approx([0.9, -2.0, 1.62], rel=1e-3)
	assert [point["soc_after_fraction"] for point in response_payload["trace"]] == pytest.approx([0.25, 0.7, 0.25], rel=1e-3)
	status_record = fake_status_store.get_status(
		tenant_id="client_003_dnipro_factory",
		flow_type=OperatorFlowType.BASELINE_LP,
	)
	assert status_record is not None
	assert status_record.status == OperatorFlowStatus.COMPLETED


def test_projected_battery_state_uses_tenant_registry_defaults(
	client: TestClient,
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
	response = client.post(
		"/dashboard/projected-battery-state",
		json={
			"tenant_id": "client_003_dnipro_factory",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["starting_soc_fraction"] == pytest.approx(0.5)
	assert response_payload["battery_metrics"]["capacity_mwh"] == pytest.approx(0.5)
	assert response_payload["battery_metrics"]["max_power_mw"] == pytest.approx(0.25)
	assert response_payload["battery_metrics"]["round_trip_efficiency"] == pytest.approx(0.92)
	assert response_payload["battery_metrics"]["soc_min_fraction"] == pytest.approx(0.05)
	assert response_payload["battery_metrics"]["soc_max_fraction"] == pytest.approx(0.95)
	assert len(response_payload["trace"]) == 6
	assert max(abs(point["feasible_net_power_mw"]) for point in response_payload["trace"]) <= 0.25
	assert all(
		point["degradation_penalty_uah"]
		== pytest.approx(
			point["throughput_mwh"]
			* (
				response_payload["battery_metrics"]["degradation_cost_per_cycle_uah"]
				/ (2.0 * response_payload["battery_metrics"]["capacity_mwh"])
			)
		)
		for point in response_payload["trace"]
	)
	status_record = fake_status_store.get_status(
		tenant_id="client_003_dnipro_factory",
		flow_type=OperatorFlowType.BASELINE_LP,
	)
	assert status_record is not None
	assert status_record.status == OperatorFlowStatus.COMPLETED


def test_battery_state_endpoint_returns_latest_telemetry_and_hourly_snapshot(
	client: TestClient,
	fake_battery_telemetry_store: InMemoryBatteryTelemetryStore,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	monkeypatch.setenv("MQTT_HOST", "mqtt1")
	monkeypatch.setenv("MQTT_PORT", "1883")
	latest_observed_at = datetime(2026, 5, 4, 11, 55, tzinfo=UTC)
	fake_battery_telemetry_store.upsert_battery_telemetry(
		[
			BatteryTelemetryObservation(
				tenant_id="client_003_dnipro_factory",
				observed_at=latest_observed_at,
				current_soc=0.62,
				soh=0.961,
				power_mw=-0.04,
				temperature_c=25.6,
				source="simulated_mqtt",
				source_kind="synthetic",
				raw_payload={"topic": "smart-arbitrage/client_003_dnipro_factory/battery/telemetry"},
			)
		]
	)
	fake_battery_telemetry_store.upsert_hourly_snapshots(
		[
			BatteryStateHourlySnapshot(
				tenant_id="client_003_dnipro_factory",
				snapshot_hour=datetime(2026, 5, 4, 11, tzinfo=UTC),
				observation_count=12,
				soc_open=0.58,
				soc_close=0.62,
				soc_mean=0.60,
				soh_close=0.961,
				power_mw_mean=-0.03,
				throughput_mwh=0.08,
				efc_delta=0.08,
				telemetry_freshness="fresh",
				first_observed_at=datetime(2026, 5, 4, 11, tzinfo=UTC),
				last_observed_at=latest_observed_at,
			)
		]
	)

	response = client.get(
		"/dashboard/battery-state",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["fallback_reason"] is None
	assert response_payload["latest_telemetry"]["current_soc"] == pytest.approx(0.62)
	assert response_payload["latest_telemetry"]["source"] == "simulated_mqtt"
	assert response_payload["hourly_snapshot"]["snapshot_hour"] == "2026-05-04T11:00:00Z"
	assert response_payload["hourly_snapshot"]["telemetry_freshness"] == "fresh"
	assert response_payload["telemetry_ingest_source"] == {
		"protocol": "mqtt",
		"broker_host": "mqtt1",
		"broker_port": 1883,
		"topic": "smart-arbitrage/client_003_dnipro_factory/battery/telemetry",
		"source_kind": "configured_ingest_path_not_connectivity_probe",
	}


def test_exogenous_signals_endpoint_returns_weather_and_grid_event_read_model(
	client: TestClient,
	fake_market_data_store: InMemoryMarketDataStore,
	fake_grid_event_store: InMemoryGridEventStore,
) -> None:
	fake_market_data_store.upsert_weather_observations(
		[
			WeatherObservation(
				tenant_id="client_004_kharkiv_hospital",
				timestamp=datetime(2026, 4, 30, 10, tzinfo=UTC),
				location_latitude=49.99,
				location_longitude=36.23,
				location_timezone="Europe/Kyiv",
				temperature=16.5,
				solar_radiation=220.0,
				wind_speed=5.2,
				cloudcover=70.0,
				precipitation=0.0,
				pressure=1012.0,
				humidity=64.0,
				source="OPEN_METEO_FORECAST",
				source_kind="observed",
				source_url="https://api.open-meteo.com/v1/forecast",
				fetched_at=datetime(2026, 4, 30, 9, 55, tzinfo=UTC),
			)
		]
	)
	fake_grid_event_store.upsert_grid_events(
		[
			GridEventObservation(
				post_id="Ukrenergo/4914",
				post_url="https://t.me/Ukrenergo/4914",
				published_at=datetime(2026, 4, 30, 9, tzinfo=UTC),
				fetched_at=datetime(2026, 4, 30, 9, 5, tzinfo=UTC),
				raw_text="СТАН ЕНЕРГОСИСТЕМИ. Є нові знеструмлення на Харківщині.",
				source="UKRENERGO_TELEGRAM",
				source_kind="observed",
				source_url="https://t.me/s/Ukrenergo",
				energy_system_status=True,
				shelling_damage=True,
				outage_or_restriction=True,
				consumption_change="unknown",
				solar_shift_advice=False,
				evening_saving_request=True,
				affected_oblasts=["Kharkiv"],
			),
			GridEventObservation(
				post_id="Ukrenergo/4932",
				post_url="https://t.me/Ukrenergo/4932",
				published_at=datetime(2026, 4, 30, 11, tzinfo=UTC),
				fetched_at=datetime(2026, 4, 30, 11, 5, tzinfo=UTC),
				raw_text="Запрошуємо на стажування Energy Hub від НЕК Укренерго.",
				source="UKRENERGO_TELEGRAM",
				source_kind="observed",
				source_url="https://t.me/s/Ukrenergo",
				energy_system_status=False,
				shelling_damage=False,
				outage_or_restriction=False,
				consumption_change="unknown",
				solar_shift_advice=False,
				evening_saving_request=False,
				affected_oblasts=[],
			)
		]
	)

	response = client.get(
		"/dashboard/exogenous-signals",
		params={"tenant_id": "client_004_kharkiv_hospital"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_004_kharkiv_hospital"
	assert response_payload["latest_weather"]["source"] == "OPEN_METEO_FORECAST"
	assert response_payload["latest_grid_event"]["post_id"] == "Ukrenergo/4914"
	assert response_payload["latest_grid_event"]["affected_oblasts"] == ["Kharkiv"]
	assert response_payload["tenant_region_affected"] is True
	assert response_payload["outage_flag"] is True
	assert response_payload["national_grid_risk_score"] > 0.0
	assert "https://t.me/s/Ukrenergo" in response_payload["source_urls"]


def test_baseline_lp_preview_uses_fresh_hourly_telemetry_soc(
	client: TestClient,
	fake_status_store: _FakeOperatorStatusStore,
	fake_battery_telemetry_store: InMemoryBatteryTelemetryStore,
) -> None:
	fake_battery_telemetry_store.upsert_hourly_snapshots(
		[
			BatteryStateHourlySnapshot(
				tenant_id="client_003_dnipro_factory",
				snapshot_hour=datetime(2026, 5, 4, 11, tzinfo=UTC),
				observation_count=12,
				soc_open=0.58,
				soc_close=0.62,
				soc_mean=0.60,
				soh_close=0.961,
				power_mw_mean=-0.03,
				throughput_mwh=0.08,
				efc_delta=0.08,
				telemetry_freshness="fresh",
				first_observed_at=datetime(2026, 5, 4, 11, tzinfo=UTC),
				last_observed_at=datetime(2026, 5, 4, 11, 55, tzinfo=UTC),
			)
		]
	)

	response = client.get(
		"/dashboard/baseline-lp-preview",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["starting_soc_fraction"] == pytest.approx(0.62)
	assert response_payload["starting_soc_source"] == "telemetry_hourly"
	assert response_payload["telemetry_freshness"]["telemetry_freshness"] == "fresh"


def test_operator_recommendation_projects_stale_soc_with_load_schedule_and_warns(
	client: TestClient,
	fake_battery_telemetry_store: InMemoryBatteryTelemetryStore,
) -> None:
	fake_battery_telemetry_store.upsert_hourly_snapshots(
		[
			BatteryStateHourlySnapshot(
				tenant_id="client_003_dnipro_factory",
				snapshot_hour=datetime(2026, 5, 4, 11, tzinfo=UTC),
				observation_count=4,
				soc_open=0.60,
				soc_close=0.58,
				soc_mean=0.59,
				soh_close=0.961,
				power_mw_mean=-0.02,
				throughput_mwh=0.04,
				efc_delta=0.04,
				telemetry_freshness="stale",
				first_observed_at=datetime(2026, 5, 4, 11, tzinfo=UTC),
				last_observed_at=datetime(2026, 5, 4, 11, 15, tzinfo=UTC),
			)
		]
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "strict_similar_day"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["selected_strategy_id"] == "strict_similar_day"
	assert response_payload["soc_source"] == "telemetry_projected"
	assert response_payload["review_required"] is True
	assert "stale telemetry" in " ".join(response_payload["readiness_warnings"]).lower()
	assert response_payload["value_vs_hold_uah"] == pytest.approx(response_payload["daily_value_uah"])
	assert response_payload["hold_baseline_value_uah"] == pytest.approx(0.0)
	assert response_payload["policy_mode"] == "official_oree_dam_row_lp_preview"
	assert response_payload["policy_readiness"] == "official_dam_row_ready"
	assert response_payload["selected_policy_id"] == "strict_similar_day"
	assert response_payload["policy_forecast_context_source"] == "not_applicable"
	assert response_payload["policy_forecast_context_row_count"] == 0
	assert response_payload["policy_forecast_context_coverage_ratio"] == pytest.approx(0.0)
	assert response_payload["policy_forecast_context_warning"] is None
	assert len(response_payload["value_gap_series"]) == len(response_payload["recommendation_schedule"])
	assert response_payload["forecast_model_series"] == []
	assert response_payload["load_forecast"][0]["reason_code"] in {"first_shift", "second_shift", "off_hours"}
	assert response_payload["available_strategies"][0]["strategy_id"] == "strict_similar_day"
	assert any(strategy["enabled"] is False for strategy in response_payload["available_strategies"] if strategy["strategy_id"] == "decision_transformer")


def test_operator_recommendation_exposes_dam_preview_boundary_metadata(client: TestClient) -> None:
	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "strict_similar_day"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["market_scope"] == "dam_hourly_planning_preview"
	assert response_payload["market_venue"] == "DAM"
	assert response_payload["interval_minutes"] == 60
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["read_model_boundary"] == "operator_preview_no_market_submission"
	assert response_payload["market_gate_status"] == "not_evaluated_preview_only"
	assert response_payload["bid_eligibility_status"] == "not_applicable_no_proposed_bid"
	assert response_payload["proposed_bid_status"] == "not_emitted_operator_preview"
	assert response_payload["v13_readiness"]["gate_status"] == "data_acquisition_needed"
	assert response_payload["v13_readiness"]["v13_candidate_generation_ready"] is False
	assert response_payload["v13_readiness"]["dt_lava_ready"] is False
	assert response_payload["v13_readiness"]["market_execution_enabled"] is False
	assert response_payload["v13_readiness"]["missing_safe_switch_examples"] == 77
	assert "oree_dam_publication_receipts_csv_path" in response_payload["v13_readiness"]["missing_required_inputs"]
	assert "explicit_dam_publication_receipts" in response_payload["v13_readiness"]["top_priority_blocker"]
	assert response_payload["v13_readiness"]["receipt_source_audit_probe_count"] == 5
	assert response_payload["v13_readiness"]["receipt_source_audit_months_probed"] == [
		"01.2026",
		"02.2026",
		"03.2026",
		"04.2026",
		"05.2026",
	]
	assert response_payload["v13_readiness"]["receipt_source_audit_candidate_found"] is False
	assert response_payload["v13_readiness"]["receipt_source_audit_csv_generated"] is False
	assert response_payload["v13_readiness"]["receipt_source_audit_all_probes_insufficient"] is True
	assert response_payload["v13_readiness"]["source_governance_status"] == "receipt_gated_for_market_submission"
	assert response_payload["v13_readiness"]["source_governance_label"] == "receipt-gated for market submission"
	assert response_payload["v13_readiness"]["market_submission_receipt_gate_status"] == "blocked_external_access"
	assert response_payload["v13_readiness"]["scmo_credentials_required_for_diploma_mvp"] is False
	assert (
		response_payload["v13_readiness"][
			"scmo_credentials_required_for_market_submission_grade_receipts"
		]
		is True
	)
	assert response_payload["v13_readiness"]["safe_switch_target_tenant_source_count"] == 5
	assert response_payload["v13_readiness"]["safe_switch_max_new_examples_required"] == 18
	assert response_payload["v13_readiness"]["safe_switch_acquisition_targets"][0] == {
		"acquisition_priority_rank": 1,
		"tenant_id": "client_004_kharkiv_hospital",
		"source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
		"current_prior_material_safe_switch_examples": 2,
		"required_prior_material_safe_switch_examples": 20,
		"target_new_prior_material_safe_switch_examples": 18,
		"required_evidence_kind": "train_prior_non_tail_risk_material_safe_switch_rows",
		"recommended_next_step": "acquire_ukrainian_context_and_backfill_safe_labels",
		"target_is_precondition_only": True,
		"market_execution_enabled": False,
	}
	assert response_payload["forecast_generated_at"] is None
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload
	assert len(response_payload["bid_recommendation_preview"]) == len(
		response_payload["recommendation_schedule"]
	)
	first_preview_bid = response_payload["bid_recommendation_preview"][0]
	first_schedule_point = response_payload["recommendation_schedule"][0]
	assert first_preview_bid["interval_start"] == first_schedule_point["interval_start"]
	assert first_preview_bid["market_venue"] == "DAM"
	assert first_preview_bid["preview_only"] is True
	assert first_preview_bid["market_execution_enabled"] is False
	assert first_preview_bid["market_order_payload_emitted"] is False
	assert first_preview_bid["proposed_bid_status"] == "not_emitted_operator_preview"
	assert first_preview_bid["read_model_boundary"] == "operator_preview_no_market_submission"
	assert first_preview_bid["side"] in {"BUY", "SELL", "HOLD"}
	assert first_preview_bid["quantity_mw"] == pytest.approx(
		abs(first_schedule_point["recommended_net_power_mw"])
	)
	assert first_preview_bid["indicative_limit_price_uah_mwh"] == pytest.approx(
		first_schedule_point["forecast_price_uah_mwh"]
	)
	assert "proposed_bid" not in first_preview_bid
	assert "market_order_payload" not in first_preview_bid

	target_start = datetime.fromisoformat(response_payload["target_delivery_window_start"])
	target_end = datetime.fromisoformat(response_payload["target_delivery_window_end"])
	anchor_timestamp = datetime.fromisoformat(response_payload["anchor_timestamp"])
	first_interval = datetime.fromisoformat(response_payload["recommendation_schedule"][0]["interval_start"])
	last_interval = datetime.fromisoformat(response_payload["recommendation_schedule"][-1]["interval_start"])
	assert target_start == first_interval
	assert target_end == last_interval + timedelta(hours=1)
	assert anchor_timestamp < target_start
	assert target_start.date() == (anchor_timestamp + timedelta(days=1)).date()
	assert target_start.hour == 0
	assert target_start.minute == 0
	assert target_start.second == 0
	assert {
		datetime.fromisoformat(point["interval_start"]).date()
		for point in response_payload["recommendation_schedule"]
	} == {target_start.date()}


def test_operator_recommendation_uses_latest_official_oree_delivery_row_without_synthetic_fallback(
	client: TestClient,
	fake_market_data_store: InMemoryMarketDataStore,
) -> None:
	start = datetime(2026, 5, 15, tzinfo=UTC)
	fake_market_data_store.upsert_market_prices(
		[
			MarketPriceObservation(
				timestamp=start + timedelta(hours=index),
				price_uah_mwh=9900.0 + index,
				price_eur_mwh=200.0 + index,
				volume_mwh=1.0,
				source="SYNTHETIC_DEMO_FALLBACK",
				source_kind="synthetic",
				source_url="synthetic://demo",
				market_venue="DAM",
				market_zone="IPS",
				market_timezone="Europe/Kyiv",
				fetched_at=start + timedelta(hours=index),
				price_spike=False,
				low_volume=False,
			)
			for index in range(24)
		]
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "strict_similar_day"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	expected_prices = [
		2400.0
		+ (hour * 42.0)
		+ (520.0 if 18 <= hour <= 21 else 0.0)
		- (180.0 if 0 <= hour <= 5 else 0.0)
		for hour in range(24)
	]
	assert [
		point["forecast_price_uah_mwh"]
		for point in response_payload["recommendation_schedule"]
	] == pytest.approx(expected_prices)
	assert response_payload["forecast_source"] == (
		"Official OREE published DAM delivery row routed through Level 1 LP preview"
	)
	assert response_payload["policy_mode"] == "official_oree_dam_row_lp_preview"
	assert response_payload["forecast_model_series"] == []
	assert "synthetic" not in json.dumps(response_payload).lower()


def test_operator_recommendation_blocks_when_official_oree_dam_rows_are_missing(
	client: TestClient,
	fake_market_data_store: InMemoryMarketDataStore,
) -> None:
	fake_market_data_store.market_observations.clear()

	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "strict_similar_day"},
	)

	assert response.status_code == 503
	assert "Official observed OREE DAM rows are required" in response.json()["detail"]
	assert "No substitute prices are rendered" in response.json()["detail"]


def test_operator_recommendation_supports_source_backed_idm_hourly_preview(
	client: TestClient,
	fake_market_data_store: InMemoryMarketDataStore,
) -> None:
	_seed_official_oree_idm_rows(fake_market_data_store)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_idm_v0",
			"market_venue": "IDM",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["market_venue"] == "IDM"
	assert response_payload["market_scope"] == "idm_hourly_planning_preview"
	assert response_payload["interval_minutes"] == 60
	assert response_payload["price_context_status"] == "official_published"
	assert response_payload["policy_mode"] == "official_oree_idm_row_lp_preview"
	assert response_payload["selected_strategy_id"] == "strict_similar_day"
	assert response_payload["forecast_source"] == "Official OREE published IDM delivery row routed through hourly LP preview"
	assert any(
		"Requested strategy nbeatsx_official_idm_v0 is unavailable" in warning
		for warning in response_payload["readiness_warnings"]
	)
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["proposed_bid_status"] == "not_emitted_operator_preview"
	assert all(point["market_venue"] == "IDM" for point in response_payload["bid_recommendation_preview"])
	assert len(response_payload["recommendation_schedule"]) == 24
	advisor = response_payload["decision_advisor"]
	assert advisor["advisor_source_id"] == "idm_policy_advisor"
	assert advisor["candidate_decision"] == "abstain_to_lp"
	assert advisor["market_execution_enabled"] is False
	assert advisor["market_order_payload_emitted"] is False
	assert advisor["promotion_gate_passed"] is False
	assert advisor["dt_lava_ready"] is False
	assert advisor["evidence_layers"] == ["NBEATSx", "TFT", "V2+", "AFL", "DFL", "DT"]


def test_operator_recommendation_uses_forecast_for_unpublished_target_delivery_date(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	target_date = datetime(2026, 5, 20)
	forecast_prices = [3100.0 + hour * 77.0 for hour in range(24)]
	tft_prices = [2500.0 + (900.0 if 17 <= hour <= 21 else 0.0) for hour in range(24)]
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=forecast_prices,
			generated_at=datetime(2026, 5, 19, 18, tzinfo=UTC),
			market_venue="DAM",
		),
		point_prediction_column="predicted_price_uah_mwh",
	)
	fake_forecast_store.upsert_forecast_run(
		model_name="tft_official_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=tft_prices,
			generated_at=datetime(2026, 5, 19, 18, tzinfo=UTC),
			market_venue="DAM",
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_v0",
			"market_venue": "DAM",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["market_venue"] == "DAM"
	assert response_payload["target_delivery_date"] == "2026-05-20"
	assert response_payload["price_context_status"] == "pre_publication_forecast"
	assert response_payload["policy_mode"] == "pre_publication_forecast_lp_preview"
	assert response_payload["policy_readiness"] == "forecast_context_ready_preview_only"
	assert response_payload["policy_forecast_context_source"] == "nbeatsx_official_v0"
	assert response_payload["policy_forecast_context_row_count"] == 24
	assert response_payload["policy_forecast_context_coverage_ratio"] == pytest.approx(1.0)
	assert response_payload["forecast_source"] == (
		"NBEATSx pre-publication forecast scenario routed through deterministic LP preview; "
		"official OREE DAM row is not published for the target delivery date"
	)
	assert [
		point["forecast_price_uah_mwh"]
		for point in response_payload["recommendation_schedule"]
	] == pytest.approx(forecast_prices)
	advisor = response_payload["decision_advisor"]
	assert advisor["advisor_source_id"] == "pre_publication_policy_advisor"
	assert advisor["candidate_decision"] == "abstain_to_forecast_lp"
	assert advisor["advisor_status"] == "forecast_scenario_ranked_abstained"
	assert advisor["comparison_metrics"]["forecast_scenario_candidate_count"] == pytest.approx(2.0)
	candidates = advisor["forecast_scenario_candidates"]
	assert [candidate["rank"] for candidate in candidates] == [1, 2]
	assert {candidate["model_name"] for candidate in candidates} == {"nbeatsx_official_v0", "tft_official_v0"}
	assert {candidate["schedule_family"] for candidate in candidates} == {"deterministic_lp_forecast_scenario"}
	assert {candidate["advisor_decision"] for candidate in candidates} == {"ranked_abstain_preview_only"}
	assert {candidate["score_source"] for candidate in candidates} == {
		"lp_schedule_value_regret_adapter_for_v2_plus_dfl_dt_advisor"
	}
	assert {candidate["gatekeeper_status"] for candidate in candidates} == {
		"passed_lp_physical_constraints_preview_only"
	}
	assert sum(candidate["selected_for_operator_preview"] for candidate in candidates) == 1
	assert all(candidate["market_execution_enabled"] is False for candidate in candidates)
	assert all(candidate["market_order_payload_emitted"] is False for candidate in candidates)
	assert advisor["market_execution_enabled"] is False
	assert advisor["market_order_payload_emitted"] is False
	assert advisor["promotion_gate_passed"] is False


def test_operator_recommendation_keeps_official_target_row_ahead_of_forecast(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	target_date = datetime(2026, 5, 15, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=pl.DataFrame(
			{
				"forecast_timestamp": [target_date + timedelta(hours=hour) for hour in range(24)],
				"predicted_price_uah_mwh": [9900.0 + hour for hour in range(24)],
			}
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_v0",
			"target_delivery_date": "2026-05-15",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["target_delivery_date"] == "2026-05-15"
	assert response_payload["price_context_status"] == "official_published"
	assert response_payload["policy_mode"] == "official_oree_dam_row_lp_preview"
	assert response_payload["recommendation_schedule"][0]["forecast_price_uah_mwh"] == pytest.approx(2220.0)
	assert response_payload["recommendation_schedule"][0]["forecast_price_uah_mwh"] != pytest.approx(9900.0)


def test_operator_recommendation_materializes_forecast_for_unpublished_target_from_source_history(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	materialize_calls = _install_fake_operator_preview_forecast_materializer(
		monkeypatch,
		fake_forecast_store,
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_v0",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["target_delivery_date"] == "2026-05-20"
	assert response_payload["price_context_status"] == "pre_publication_forecast"
	assert response_payload["policy_mode"] == "pre_publication_forecast_lp_preview"
	assert response_payload["policy_forecast_context_source"] == "nbeatsx_official_v0"
	assert response_payload["policy_forecast_context_row_count"] == 24
	assert materialize_calls == [
		{
			"tenant_id": "client_003_dnipro_factory",
			"market_venue": "DAM",
			"forecast_start": datetime(2026, 5, 16),
			"horizon_hours": 120,
		}
	]
	assert len(response_payload["recommendation_schedule"]) == 24
	assert any(
		point["recommended_net_power_mw"] != pytest.approx(0.0)
		for point in response_payload["recommendation_schedule"]
	)
	assert {point["side"] for point in response_payload["bid_recommendation_preview"]}.issuperset(
		{"BUY", "SELL"}
	)
	assert response_payload["market_execution_enabled"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_operator_recommendation_materializes_fast_source_backed_forecast_for_selected_date(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_v0",
			"market_venue": "DAM",
			"target_delivery_date": "2026-05-16",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["target_delivery_date"] == "2026-05-16"
	assert response_payload["price_context_status"] == "pre_publication_forecast"
	assert response_payload["policy_forecast_context_source"] == "nbeatsx_official_v0"
	assert len(response_payload["recommendation_schedule"]) == 24
	assert any(
		point["side"] in {"BUY", "SELL"}
		for point in response_payload["bid_recommendation_preview"]
	)
	forecast_frame = fake_forecast_store.latest_forecast_observation_frame(
		model_names=["nbeatsx_official_v0", "tft_official_v0"],
		limit_per_model=72,
	)
	assert forecast_frame.height == 144
	payload = json.loads(forecast_frame.select("prediction_payload").to_series().item(0))
	assert payload["adapter_scope"] == "source_backed_lag_operator_preview_not_market_execution"
	assert response_payload["market_execution_enabled"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_operator_preview_ensure_endpoint_returns_read_model_materialization_status(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	monkeypatch.setattr(
		api_main,
		"ensure_operator_preview_window",
		lambda **_: OperatorPreviewEnsureResult(
			tenant_id="client_003_dnipro_factory",
			market_venue="DAM",
			target_delivery_date=datetime(2026, 5, 24).date(),
			status="materialized",
			stage="forecast_materialization",
			message="source-backed forecast-store rows materialized for operator preview; market execution remains disabled",
			latest_observed_timestamp=datetime(2026, 5, 23, 23),
			forecast_start=datetime(2026, 5, 24),
			forecast_horizon_end=datetime(2026, 5, 30, 23),
			horizon_hours=168,
			source_refresh_rows=192,
			source_refresh_dates=tuple(f"2026-05-{day:02d}" for day in range(16, 24)),
			forecast_rows=336,
			forecast_run_ids={
				"nbeatsx_official_v0": "nbeatsx_official_v0:test",
				"tft_official_v0": "tft_official_v0:test",
			},
		),
		raising=False,
	)

	response = client.post(
		"/dashboard/operator-preview/ensure",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"market_venue": "DAM",
			"target_delivery_date": "2026-05-24",
		},
	)

	assert response.status_code == 200
	payload = response.json()
	assert payload["status"] == "materialized"
	assert payload["stage"] == "forecast_materialization"
	assert payload["source_refresh_rows"] == 192
	assert payload["forecast_rows"] == 336
	assert payload["market_execution_enabled"] is False
	assert payload["read_model_boundary"] == "operator_preview_no_market_submission"
	assert "proposed_bid" not in payload
	assert "market_order_payload" not in payload


def test_operator_recommendation_uses_source_refresh_when_target_exceeds_current_preview_horizon(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	ensure_calls: list[dict[str, Any]] = []

	def _fake_ensure_operator_preview_window(**kwargs: Any) -> OperatorPreviewEnsureResult:
		ensure_calls.append(
			{
				"tenant_id": kwargs["tenant_id"],
				"market_venue": kwargs["market_venue"],
				"target_delivery_date": kwargs["target_delivery_date"],
			}
		)
		forecast_start = datetime(2026, 5, 24)
		for model_name in ("nbeatsx_official_v0", "tft_official_v0"):
			fake_forecast_store.upsert_forecast_run(
				model_name=model_name,
				forecast_frame=_forecast_frame(
					target_date=forecast_start,
					values=[2700.0 + hour_index * 25.0 for hour_index in range(72)],
					generated_at=datetime(2026, 5, 23, 23),
					training_cutoff=datetime(2026, 5, 23, 23),
				),
				point_prediction_column="predicted_price_uah_mwh",
			)
		return OperatorPreviewEnsureResult(
			tenant_id=kwargs["tenant_id"],
			market_venue=kwargs["market_venue"],
			target_delivery_date=kwargs["target_delivery_date"],
			status="materialized",
			stage="forecast_materialization",
			message="source-backed forecast-store rows materialized for operator preview; market execution remains disabled",
			forecast_start=forecast_start,
			forecast_horizon_end=datetime(2026, 5, 26, 23),
			horizon_hours=72,
			forecast_rows=144,
			forecast_run_ids={
				"nbeatsx_official_v0": "nbeatsx_official_v0:test",
				"tft_official_v0": "tft_official_v0:test",
			},
		)

	monkeypatch.setattr(api_main, "ensure_operator_preview_window", _fake_ensure_operator_preview_window)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_v0",
			"market_venue": "DAM",
			"target_delivery_date": "2026-05-24",
		},
	)

	assert response.status_code == 200
	assert ensure_calls == [
		{
			"tenant_id": "client_003_dnipro_factory",
			"market_venue": "DAM",
			"target_delivery_date": datetime(2026, 5, 24).date(),
		}
	]
	response_payload = response.json()
	assert response_payload["price_context_status"] == "pre_publication_forecast"
	assert response_payload["target_delivery_date"] == "2026-05-24"
	assert response_payload["market_execution_enabled"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_operator_recommendation_uses_complete_forecast_run_for_selected_date(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	forecast_start = datetime(2026, 5, 16)
	target_delivery_date = "2026-05-20"
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=forecast_start,
			values=[
				2500.0
				+ hour * 18.0
				+ (3200.0 if hour % 24 in {18, 19, 20, 21} else 0.0)
				- (900.0 if hour % 24 in {2, 3, 4} else 0.0)
				for hour in range(120)
			],
			generated_at=datetime(2026, 5, 15, 23),
			training_cutoff=datetime(2026, 5, 15, 23),
		),
		point_prediction_column="predicted_price_uah_mwh",
	)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=forecast_start,
			values=[4200.0 + hour for hour in range(72)],
			generated_at=datetime(2026, 5, 16, 1),
			training_cutoff=datetime(2026, 5, 15, 23),
		),
		point_prediction_column="predicted_price_uah_mwh",
	)
	monkeypatch.setattr(
		api_main,
		"_try_materialize_pre_publication_forecast_rows",
		lambda **_: "materialization disabled for complete-window regression",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_v0",
			"market_venue": "DAM",
			"target_delivery_date": target_delivery_date,
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["target_delivery_date"] == target_delivery_date
	assert response_payload["price_context_status"] == "pre_publication_forecast"
	assert response_payload["policy_forecast_context_source"] == "nbeatsx_official_v0"
	assert len(response_payload["recommendation_schedule"]) == 24
	assert response_payload["recommendation_schedule"][0]["interval_start"] == "2026-05-20T00:00:00"
	assert any(
		point["side"] in {"BUY", "SELL"}
		for point in response_payload["bid_recommendation_preview"]
	)
	assert response_payload["market_execution_enabled"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_operator_recommendation_materializes_idm_forecast_for_unpublished_target_from_source_history(
	client: TestClient,
	fake_market_data_store: InMemoryMarketDataStore,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	_seed_official_oree_idm_rows(fake_market_data_store)
	materialize_calls = _install_fake_operator_preview_forecast_materializer(
		monkeypatch,
		fake_forecast_store,
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_idm_v0",
			"market_venue": "IDM",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["market_venue"] == "IDM"
	assert response_payload["target_delivery_date"] == "2026-05-20"
	assert response_payload["price_context_status"] == "pre_publication_forecast"
	assert response_payload["policy_forecast_context_source"] == "nbeatsx_official_idm_v0"
	assert materialize_calls == [
		{
			"tenant_id": "client_003_dnipro_factory",
			"market_venue": "IDM",
			"forecast_start": datetime(2026, 5, 16),
			"horizon_hours": 120,
		}
	]
	assert all(point["market_venue"] == "IDM" for point in response_payload["bid_recommendation_preview"])
	assert {point["side"] for point in response_payload["bid_recommendation_preview"]}.issuperset(
		{"BUY", "SELL"}
	)
	assert response_payload["market_execution_enabled"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_operator_recommendation_blocks_unpublished_target_without_source_history(
	fake_market_data_store: InMemoryMarketDataStore,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	del fake_market_data_store
	monkeypatch.setattr(
		api_main,
		"ensure_operator_preview_window",
		lambda **kwargs: OperatorPreviewEnsureResult(
			tenant_id=kwargs["tenant_id"],
			market_venue=kwargs["market_venue"],
			target_delivery_date=kwargs["target_delivery_date"],
			status="blocked_source_unavailable",
			stage="source_refresh",
			message="source-backed rows unavailable from OREE",
		),
	)
	monkeypatch.setattr(
		api_main,
		"materialize_operator_preview_forecast_runs",
		lambda *args, **kwargs: (_ for _ in ()).throw(
			AssertionError("forecast materialization requires source-backed OREE history first")
		),
		raising=False,
	)
	response = TestClient(api_main.app).get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_v0",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 503
	assert "pre-publication forecast rows are required" in response.json()["detail"]
	assert "synthetic" not in response.json()["detail"].lower()


def test_operator_recommendation_blocks_pre_publication_forecast_generated_after_delivery_start(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	target_date = datetime(2026, 6, 2, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=[4100.0 + hour for hour in range(24)],
			generated_at=target_date + timedelta(hours=1),
			market_venue="DAM",
			training_cutoff=target_date - timedelta(hours=1),
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_v0",
			"market_venue": "DAM",
			"target_delivery_date": "2026-06-02",
		},
	)

	assert response.status_code == 503
	assert "point-in-time forecast metadata rejected" in response.json()["detail"]


def test_operator_recommendation_uses_idm_forecast_for_unpublished_target_delivery_date(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	target_date = datetime(2026, 5, 20, tzinfo=UTC)
	forecast_prices = [4200.0 + hour * 33.0 for hour in range(24)]
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_idm_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=forecast_prices,
			generated_at=datetime(2026, 5, 19, 18, tzinfo=UTC),
			market_venue="IDM",
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_idm_v0",
			"market_venue": "IDM",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["market_venue"] == "IDM"
	assert response_payload["target_delivery_date"] == "2026-05-20"
	assert response_payload["price_context_status"] == "pre_publication_forecast"
	assert response_payload["policy_mode"] == "pre_publication_forecast_lp_preview"
	assert response_payload["policy_forecast_context_source"] == "nbeatsx_official_idm_v0"
	assert response_payload["forecast_source"] == (
		"NBEATSx pre-publication forecast scenario routed through deterministic LP preview; "
		"official OREE IDM row is not published for the target delivery date"
	)
	assert [
		point["forecast_price_uah_mwh"]
		for point in response_payload["recommendation_schedule"]
	] == pytest.approx(forecast_prices)
	assert response_payload["decision_advisor"]["candidate_decision"] == "abstain_to_forecast_lp"
	assert all(point["market_venue"] == "IDM" for point in response_payload["bid_recommendation_preview"])


def test_operator_recommendation_keeps_idm_forecast_evidence_venue_scoped(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	target_date = datetime(2026, 6, 2, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=[9100.0 + hour for hour in range(24)],
			generated_at=datetime(2026, 6, 1, 9, tzinfo=UTC),
			market_venue="DAM",
		),
		point_prediction_column="predicted_price_uah_mwh",
	)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_idm_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=[4200.0 + hour for hour in range(24)],
			generated_at=datetime(2026, 6, 1, 9, tzinfo=UTC),
			market_venue="IDM",
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "nbeatsx_official_idm_v0",
			"market_venue": "IDM",
			"target_delivery_date": "2026-06-02",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["policy_forecast_context_source"] == "nbeatsx_official_idm_v0"
	assert {series["model_name"] for series in response_payload["forecast_model_series"]} == {
		"nbeatsx_official_idm_v0"
	}
	assert all(
		"official OREE DAM row" not in option["reason"]
		for option in response_payload["available_strategies"]
	)


def test_baseline_lp_preview_supports_source_backed_idm_hourly_preview(
	client: TestClient,
	fake_market_data_store: InMemoryMarketDataStore,
) -> None:
	_seed_official_oree_idm_rows(fake_market_data_store)

	response = client.get(
		"/dashboard/baseline-lp-preview",
		params={"tenant_id": "client_003_dnipro_factory", "market_venue": "IDM"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["market_venue"] == "IDM"
	assert response_payload["market_scope"] == "idm_hourly_planning_preview"
	assert response_payload["interval_minutes"] == 60
	assert response_payload["market_execution_enabled"] is False
	assert all(point["market_venue"] == "IDM" for point in response_payload["bid_recommendation_preview"])
	assert len(response_payload["recommendation_schedule"]) == 24


def test_operator_recommendation_blocks_idm_without_source_backed_rows(
	client: TestClient,
) -> None:
	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "market_venue": "IDM"},
	)

	assert response.status_code == 503
	assert "Official observed OREE IDM rows are required" in response.json()["detail"]
	assert "No substitute prices are rendered" in response.json()["detail"]


def test_operator_default_remains_v2_plus_when_dt_shadow_preview_exists(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_shadow_preview_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"strategy_id": "schedule_value_learner_v2_plus",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["selected_strategy_id"] == "schedule_value_learner_v2_plus"
	assert response_payload["policy_mode"] == "official_oree_dam_row_lp_preview"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["proposed_bid_status"] == "not_emitted_operator_preview"
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_dt_shadow_recommendation_preview_expands_selected_candidate_without_promotion(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_shadow_preview_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={"tenant_id": "client_003_dnipro_factory", "preview_source": "dt_shadow"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "dt_shadow"
	assert response_payload["preview_status"] == "research_shadow_not_promoted"
	assert response_payload["selected_candidate_id"] == "dt-candidate-worse-than-v2"
	assert response_payload["selected_schedule_family"] == "dt_tail_risk_aware_schedule"
	assert response_payload["is_default_strategy"] is False
	assert response_payload["is_promoted_strategy"] is False
	assert response_payload["research_shadow_not_promotable"] is True
	assert response_payload["default_strategy_id"] == "schedule_value_learner_v2_plus"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["proposed_bid_status"] == "not_emitted_operator_preview"
	assert response_payload["market_order_payload_emitted"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload
	assert response_payload["comparison_metrics"]["dt_selected_mean_regret_uah"] > response_payload[
		"comparison_metrics"
	]["v2_plus_mean_regret_uah"]
	assert response_payload["comparison_metrics"]["dt_minus_v2_plus_regret_uah"] == pytest.approx(45.0)
	assert response_payload["recommendation_schedule"][0] == {
		"step_index": 0,
		"interval_start": "2026-05-06T00:00:00Z",
		"action": "discharge",
		"quantity_mw": 0.12,
		"recommended_net_power_mw": 0.12,
		"forecast_price_uah_mwh": 4300.0,
		"soc_before_fraction": 0.52,
		"soc_after_fraction": 0.47,
		"selected_candidate_id": "dt-candidate-worse-than-v2",
		"schedule_family": "dt_tail_risk_aware_schedule",
		"expected_value_uah": 700.0,
		"regret_uah": 245.0,
		"regret_vs_v2_plus_uah": 45.0,
		"regret_vs_strict_uah": 80.0,
		"value_vs_v2_plus_uah": -45.0,
		"value_vs_strict_uah": -80.0,
		"gate_status": "accepted_shadow_preview",
		"safety_status": "no_safety_violations_recorded",
		"market_execution_enabled": False,
		"market_order_payload_emitted": False,
		"proposed_bid_status": "not_emitted_operator_preview",
	}


def test_direct_dt_shadow_recommendation_preview_uses_direct_candidate_artifacts(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_shadow_preview_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"DT_DIRECT_CANDIDATE_SHADOW_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_DIRECT_CANDIDATE_SHADOW_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "dt_direct_candidate_shadow",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "dt_direct_candidate_shadow"
	assert response_payload["preview_source_label"] == "Direct DT Shadow"
	assert response_payload["preview_status"] == "direct_candidate_shadow_not_promoted"
	assert response_payload["selected_candidate_id"] == "dt-candidate-worse-than-v2"
	assert response_payload["selected_schedule_family"] == "dt_tail_risk_aware_schedule"
	assert response_payload["is_default_strategy"] is False
	assert response_payload["is_promoted_strategy"] is False
	assert response_payload["research_shadow_not_promotable"] is True
	assert response_payload["default_strategy_id"] == "schedule_value_learner_v2_plus"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["proposed_bid_status"] == "not_emitted_operator_preview"
	assert response_payload["market_order_payload_emitted"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert response_payload["dt_lava_ready"] is False
	assert response_payload["source_readiness_gate_passed"] is False
	assert "Direct DT Shadow" in response_payload["boundary_labels"]
	assert any(
		source["preview_source_id"] == "dt_direct_candidate_shadow"
		and source["is_promoted_strategy"] is False
		and source["market_execution_enabled"] is False
		for source in response_payload["available_preview_sources"]
	)
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_apples_to_apples_dt_shadow_preview_uses_real_v2_plus_artifacts(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_shadow_preview_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_APPLES_TO_APPLES_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_APPLES_TO_APPLES_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "dt_v2_plus_apples_to_apples_shadow",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "dt_v2_plus_apples_to_apples_shadow"
	assert response_payload["preview_source_label"] == "DT vs real V2+ Shadow"
	assert response_payload["preview_status"] == "apples_to_apples_not_promoted"
	assert response_payload["is_default_strategy"] is False
	assert response_payload["is_promoted_strategy"] is False
	assert response_payload["research_shadow_not_promotable"] is True
	assert response_payload["default_strategy_id"] == "schedule_value_learner_v2_plus"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert response_payload["dt_lava_ready"] is False
	assert response_payload["source_readiness_gate_passed"] is False
	assert "DT vs real V2+ Shadow" in response_payload["boundary_labels"]
	assert any(
		source["preview_source_id"] == "dt_v2_plus_apples_to_apples_shadow"
		and source["is_promoted_strategy"] is False
		and source["market_execution_enabled"] is False
		for source in response_payload["available_preview_sources"]
	)
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_dt_v2_plus_distillation_shadow_preview_uses_distillation_artifacts(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_shadow_preview_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_DISTILLATION_SHADOW_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_DISTILLATION_SHADOW_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "dt_v2_plus_distillation_shadow",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "dt_v2_plus_distillation_shadow"
	assert response_payload["preview_source_label"] == "DT V2+ distillation shadow"
	assert response_payload["preview_status"] == "distillation_diagnostic_not_promoted"
	assert response_payload["preview_only"] is True
	assert response_payload["is_default_strategy"] is False
	assert response_payload["is_promoted_strategy"] is False
	assert response_payload["research_shadow_not_promotable"] is True
	assert response_payload["default_strategy_id"] == "schedule_value_learner_v2_plus"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["proposed_bid_status"] == "not_emitted_operator_preview"
	assert response_payload["market_order_payload_emitted"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert response_payload["dt_lava_ready"] is False
	assert response_payload["source_readiness_gate_passed"] is False
	assert "DT V2+ distillation shadow" in response_payload["boundary_labels"]
	assert any(
		source["preview_source_id"] == "dt_v2_plus_distillation_shadow"
		and source["is_default_strategy"] is False
		and source["is_promoted_strategy"] is False
		and source["market_execution_enabled"] is False
		for source in response_payload["available_preview_sources"]
	)
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_dt_v2_plus_distillation_shadow_preview_rejects_promoted_rows(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_shadow_preview_fixture(tmp_path, teacher_promotion_gate_passed=True)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_DISTILLATION_SHADOW_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_DISTILLATION_SHADOW_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "dt_v2_plus_distillation_shadow",
		},
	)

	assert response.status_code == 500
	assert "must not contain promoted rows" in response.json()["detail"]


@pytest.mark.parametrize(
	("fixture_kwargs", "expected_detail"),
	[
		(
			{"packet_market_execution_enabled": True},
			"must keep market_execution_enabled=false",
		),
		(
			{"packet_dt_lava_ready": True},
			"must not enable dt_lava_ready",
		),
		(
			{"packet_permits_model_training": True},
			"must not permit model training",
		),
	],
)
def test_dt_v2_plus_distillation_shadow_preview_rejects_executable_packet_flags(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
	fixture_kwargs: dict[str, Any],
	expected_detail: str,
) -> None:
	_write_dt_shadow_preview_fixture(tmp_path, **fixture_kwargs)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_DISTILLATION_SHADOW_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_DISTILLATION_SHADOW_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "dt_v2_plus_distillation_shadow",
		},
	)

	assert response.status_code == 500
	assert expected_detail in response.json()["detail"]


def test_regret_aware_selector_shadow_preview_abstains_to_v2_plus_without_promotion(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_regret_aware_selector_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"REGRET_AWARE_V2_PLUS_SELECTOR_SELECTED_ROWS_CSV_PATH",
		tmp_path / "regret_aware_v2_plus_selector_selected_rows.csv",
	)
	monkeypatch.setattr(
		api_main,
		"REGRET_AWARE_V2_PLUS_SELECTOR_TEACHER_ROWS_CSV_PATH",
		tmp_path / "regret_aware_v2_plus_selector_teacher_rows.csv",
	)
	monkeypatch.setattr(
		api_main,
		"REGRET_AWARE_V2_PLUS_SELECTOR_SUMMARY_JSON_PATH",
		tmp_path / "regret_aware_v2_plus_selector_summary.json",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "regret_aware_v2_plus_selector_shadow",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "regret_aware_v2_plus_selector_shadow"
	assert response_payload["preview_source_label"] == "Regret-aware V2+ selector"
	assert response_payload["preview_status"] == "regret_aware_abstention_not_promoted"
	assert response_payload["selected_candidate_id"] == "v2-plus-candidate"
	assert response_payload["selected_schedule_family"] == "schedule_value_learner_v2_plus"
	assert response_payload["is_default_strategy"] is False
	assert response_payload["is_promoted_strategy"] is False
	assert response_payload["research_shadow_not_promotable"] is True
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert response_payload["dt_lava_ready"] is False
	assert response_payload["comparison_metrics"]["selector_mean_regret_uah"] == pytest.approx(174.77)
	assert response_payload["comparison_metrics"]["dt_selected_mean_regret_uah"] == pytest.approx(174.77)
	assert response_payload["comparison_metrics"]["dt_minus_v2_plus_regret_uah"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["non_v2_plus_switch_count"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["abstention_count"] == pytest.approx(90.0)
	assert response_payload["recommendation_schedule"][0]["regret_vs_v2_plus_uah"] == pytest.approx(0.0)
	assert response_payload["recommendation_schedule"][0]["schedule_family"] == "schedule_value_learner_v2_plus"
	assert any(
		source["preview_source_id"] == "regret_aware_v2_plus_selector_shadow"
		and source["is_promoted_strategy"] is False
		and source["market_execution_enabled"] is False
		for source in response_payload["available_preview_sources"]
	)
	assert "Selector abstained to V2+" in " ".join(response_payload["readiness_warnings"])
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_dt_v2_plus_safe_switch_selector_shadow_preview_uses_current_evidence(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_v2_plus_safe_switch_selector_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_SAFE_SWITCH_SELECTOR_SELECTED_ROWS_CSV_PATH",
		tmp_path / "regret_aware_v2_plus_selector_selected_rows.csv",
	)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_SAFE_SWITCH_SELECTOR_TEACHER_ROWS_CSV_PATH",
		tmp_path / "regret_aware_v2_plus_selector_teacher_rows.csv",
	)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_SAFE_SWITCH_SELECTOR_SUMMARY_JSON_PATH",
		tmp_path / "regret_aware_v2_plus_selector_summary.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_V2_PLUS_PROMOTION_EVIDENCE_SUMMARY_JSON_PATH",
		tmp_path / "dt_v2_plus_promotion_evidence_summary.json",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "dt_v2_plus_safe_switch_selector_shadow",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "dt_v2_plus_safe_switch_selector_shadow"
	assert response_payload["preview_source_label"] == "DT V2+ safe-switch selector"
	assert response_payload["preview_status"] == "safe_switch_evidence_not_promoted"
	assert response_payload["selected_candidate_id"] == "strict-candidate"
	assert response_payload["selected_schedule_family"] == "strict_reference"
	assert response_payload["is_default_strategy"] is False
	assert response_payload["is_promoted_strategy"] is False
	assert response_payload["research_shadow_not_promotable"] is True
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert response_payload["dt_lava_ready"] is False
	assert response_payload["comparison_metrics"]["selector_mean_regret_uah"] == pytest.approx(168.15664125116336)
	assert response_payload["comparison_metrics"]["dt_selected_mean_regret_uah"] == pytest.approx(168.15664125116336)
	assert response_payload["comparison_metrics"]["dt_minus_v2_plus_regret_uah"] == pytest.approx(-6.611757063998141)
	assert response_payload["comparison_metrics"]["non_v2_plus_switch_count"] == pytest.approx(4.0)
	assert response_payload["comparison_metrics"]["abstention_count"] == pytest.approx(86.0)
	assert response_payload["comparison_metrics"]["observed_safe_switch_opportunity_count"] == pytest.approx(15.0)
	assert response_payload["comparison_metrics"]["recovered_safe_switch_opportunity_count"] == pytest.approx(3.0)
	assert response_payload["comparison_metrics"]["safe_switch_win_count"] == pytest.approx(3.0)
	assert response_payload["comparison_metrics"]["tail_risk_loss_count"] == pytest.approx(0.0)
	assert response_payload["recommendation_schedule"][0]["regret_vs_v2_plus_uah"] == pytest.approx(-30.0)
	assert any(
		source["preview_source_id"] == "dt_v2_plus_safe_switch_selector_shadow"
		and source["is_promoted_strategy"] is False
		and source["market_execution_enabled"] is False
		for source in response_payload["available_preview_sources"]
	)
	warnings = " ".join(response_payload["readiness_warnings"])
	assert "Recovered 3 of 15" in warnings
	assert "V2+ remains confirmed offline comparator/evidence" in " ".join(response_payload["boundary_labels"])
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_hf_live_safe_switch_shadow_preview_uses_live_rows_without_lp(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: SimpleNamespace(
			checkpoint_path=str(checkpoint_path),
			candidate_families=(
				"raw_reference",
				"schedule_value_learner_v2_plus",
				"schedule_value_learner_v2_plus_reference",
				"strict_reference",
			),
		),
	)

	def _fake_score(*, bundle: object, candidate_rows: list[dict[str, object]]) -> dict[str, object]:
		del bundle
		assert len(candidate_rows) == 4
		selected = next(
			row
			for row in candidate_rows
			if row["dt_schedule_family_target"] == "strict_reference"
		)
		selected_value_uah = float(cast(float, selected["schedule_value_uah"]))
		return {
			"selected_candidate": selected,
			"selected_candidate_id": selected["dt_candidate_id_target"],
			"selected_schedule_family": selected["dt_schedule_family_target"],
			"selected_candidate_index": selected["dt_candidate_index_target"],
			"selected_schedule_value_uah": selected["schedule_value_uah"],
			"predicted_regret_delta_vs_v2_plus_uah": -140.0,
			"predicted_tail_risk_probability": 0.2,
			"abstained_to_v2_plus": False,
			"selection_reason": "predicted_guard_passed",
			"live_actual_regret_available": False,
			"selection_diagnostics": {
				"reported_selected_predicted_regret_delta_vs_v2_plus_uah": -140.0,
				"raw_selected_predicted_regret_delta_vs_v2_plus_uah": -140.0,
				"best_nonfallback_schedule_family": "strict_reference",
				"best_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -140.0,
				"best_nonfallback_predicted_tail_risk_probability": 0.2,
				"best_nonfallback_family_tail_risk_probability": 0.1,
				"best_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"best_safe_nonfallback_schedule_family": "strict_reference",
				"best_safe_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -140.0,
				"best_safe_nonfallback_predicted_tail_risk_probability": 0.2,
				"best_safe_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"best_value_schedule_family": "strict_reference",
				"best_template_schedule_value_uah": selected_value_uah,
				"selected_vs_best_template_value_gap_uah": 0.0,
				"eligible_nonfallback_candidate_count": 1.0,
				"threshold_guard_failed_count": 2.0,
				"predicted_tail_guard_failed_count": 0.0,
				"family_tail_guard_failed_count": 0.0,
				"safety_guard_failed_count": 0.0,
			},
			"scored_candidates": [
				{
					**row,
					"predicted_regret_delta_vs_v2_plus_uah": (
						-140.0
						if row["dt_schedule_family_target"] == "strict_reference"
						else 0.0
					),
					"predicted_tail_risk_probability": 0.2,
				}
				for row in candidate_rows
			],
		}

	def _fail_lp(*args: object, **kwargs: object) -> object:
		raise AssertionError("LP solver must not run for hf_live_safe_switch_shadow")

	monkeypatch.setattr(api_main, "score_hf_safe_switch_candidate_rows", _fake_score)
	monkeypatch.setattr(
		api_main.HourlyDamBaselineSolver,
		"solve_dispatch_from_forecast",
		_fail_lp,
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_shadow",
			"market_venue": "DAM",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "hf_live_safe_switch_shadow"
	assert response_payload["preview_status"] == "live_shadow_not_promoted"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert response_payload["dt_lava_ready"] is False
	assert response_payload["source_readiness_gate_passed"] is False
	assert response_payload["selected_schedule_family"] == "strict_reference"
	assert len(response_payload["recommendation_schedule"]) == 24
	assert response_payload["recommendation_schedule"][0]["regret_uah"] is None
	assert response_payload["comparison_metrics"]["hf_mean_regret_uah"] == pytest.approx(158.7121)
	assert response_payload["comparison_metrics"]["canonical_safe_switch_mean_regret_uah"] == pytest.approx(168.1566)
	assert response_payload["comparison_metrics"]["v2_plus_baseline_mean_regret_uah"] == pytest.approx(174.77)
	assert response_payload["comparison_metrics"]["selected_operating_threshold_uah"] == pytest.approx(100.0)
	assert response_payload["comparison_metrics"]["live_actual_regret_available"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["best_nonfallback_predicted_regret_delta_vs_v2_plus_uah"] == pytest.approx(-140.0)
	assert response_payload["comparison_metrics"]["threshold_margin_to_switch_uah"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["threshold_guard_failed_count"] == pytest.approx(2.0)
	assert response_payload["comparison_metrics"]["selected_vs_best_template_value_gap_uah"] == pytest.approx(0.0)
	assert "Best non-fallback HF candidate: strict_reference" in " ".join(
		response_payload["readiness_warnings"]
	)
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_hf_live_safe_switch_value_aligned_shadow_uses_value_aligned_templates_without_lp(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: SimpleNamespace(
			checkpoint_path=str(checkpoint_path),
			candidate_families=(
				"raw_reference",
				"schedule_value_learner_v2_plus",
				"schedule_value_learner_v2_reference",
				"strict_reference",
			),
		),
	)

	def _fake_score(*, bundle: object, candidate_rows: list[dict[str, object]]) -> dict[str, object]:
		del bundle
		strict = next(
			row
			for row in candidate_rows
			if row["dt_schedule_family_target"] == "strict_reference"
		)
		strict_dispatch_vector = cast(list[float], strict["dispatch_mw_vector"])
		strict_value_uah = float(cast(float, strict["schedule_value_uah"]))
		strict_dispatch = [
			float(value)
			for value in strict_dispatch_vector
			if abs(float(value)) > 1e-9
		]
		assert len(strict_dispatch) == 8
		assert strict["schedule_value_uah"] != 0.0
		return {
			"selected_candidate": strict,
			"selected_candidate_id": strict["dt_candidate_id_target"],
			"selected_schedule_family": strict["dt_schedule_family_target"],
			"selected_candidate_index": strict["dt_candidate_index_target"],
			"selected_schedule_value_uah": strict["schedule_value_uah"],
			"predicted_regret_delta_vs_v2_plus_uah": -135.0,
			"predicted_tail_risk_probability": 0.21,
			"abstained_to_v2_plus": False,
			"selection_reason": "predicted_guard_passed",
			"live_actual_regret_available": False,
			"selection_diagnostics": {
				"reported_selected_predicted_regret_delta_vs_v2_plus_uah": -135.0,
				"raw_selected_predicted_regret_delta_vs_v2_plus_uah": -135.0,
				"best_nonfallback_schedule_family": "strict_reference",
				"best_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -135.0,
				"best_nonfallback_predicted_tail_risk_probability": 0.21,
				"best_nonfallback_family_tail_risk_probability": 0.0,
				"best_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"best_safe_nonfallback_schedule_family": "strict_reference",
				"best_safe_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -135.0,
				"best_safe_nonfallback_predicted_tail_risk_probability": 0.21,
				"best_safe_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"best_value_schedule_family": "strict_reference",
				"best_template_schedule_value_uah": strict_value_uah,
				"selected_vs_best_template_value_gap_uah": 0.0,
				"eligible_nonfallback_candidate_count": 1.0,
				"threshold_guard_failed_count": 2.0,
				"predicted_tail_guard_failed_count": 0.0,
				"family_tail_guard_failed_count": 0.0,
				"safety_guard_failed_count": 0.0,
			},
			"scored_candidates": [
				{
					**row,
					"predicted_regret_delta_vs_v2_plus_uah": (
						-135.0
						if row["dt_schedule_family_target"] == "strict_reference"
						else 0.0
					),
					"predicted_tail_risk_probability": 0.21,
				}
				for row in candidate_rows
			],
		}

	def _fail_lp(*args: object, **kwargs: object) -> object:
		raise AssertionError("LP solver must not run for value-aligned HF live shadow")

	monkeypatch.setattr(api_main, "score_hf_safe_switch_candidate_rows", _fake_score)
	monkeypatch.setattr(
		api_main.HourlyDamBaselineSolver,
		"solve_dispatch_from_forecast",
		_fail_lp,
	)
	proof_path = tmp_path / "promotion_gate.json"
	proof_path.write_text(
		json.dumps(
			{
				"shadow_promotion_gate_passed": True,
				"source_backed_day_count": 32.0,
				"value_aligned_switch_rate": 0.625,
				"selected_nonfallback_day_count": 20.0,
				"hf_minus_v2_plus_mean_regret_uah": -16.057869824303538,
				"safety_failure_count": 0.0,
				"tail_failure_delta_vs_default_count": -16.0,
				"value_gap_ratio_vs_default": 0.39748498175945224,
				"market_execution_enabled": False,
				"production_market_promotion_gate_passed": False,
				"market_order_payload_emitted": False,
				"proposed_bid_emitted": False,
			}
		),
		encoding="utf-8",
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_VALUE_ALIGNED_PROMOTION_PROOF_JSON_PATH",
		proof_path,
		raising=False,
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_value_aligned_shadow",
			"market_venue": "DAM",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "hf_live_safe_switch_value_aligned_shadow"
	assert response_payload["preview_source_label"] == "HF live safe-switch value-aligned shadow"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert response_payload["dt_lava_ready"] is False
	assert response_payload["recommendation_schedule"][0]["regret_uah"] is None
	assert response_payload["comparison_metrics"]["candidate_template_grid_value_aligned"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["selected_candidate_estimated_value_uah"] == pytest.approx(
		response_payload["comparison_metrics"]["dt_selected_mean_value_uah"]
	)
	assert response_payload["comparison_metrics"]["best_template_estimated_value_uah"] == pytest.approx(
		response_payload["comparison_metrics"]["dt_selected_mean_value_uah"]
	)
	assert response_payload["comparison_metrics"]["value_aligned_selected_value_improvement_uah"] == pytest.approx(645.3912023143322)
	assert response_payload["comparison_metrics"]["value_aligned_value_gap_ratio_vs_default"] == pytest.approx(0.39748498175945224)
	assert response_payload["comparison_metrics"]["value_aligned_tail_failure_delta_count"] == pytest.approx(-16.0)
	assert response_payload["comparison_metrics"]["value_aligned_safety_failure_count"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["shadow_promotion_gate_available"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["shadow_promotion_gate_passed"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["shadow_promotion_source_backed_day_count"] == pytest.approx(32.0)
	assert response_payload["comparison_metrics"]["shadow_promotion_switch_rate"] == pytest.approx(0.625)
	assert response_payload["comparison_metrics"]["shadow_promotion_nonfallback_day_count"] == pytest.approx(20.0)
	assert response_payload["comparison_metrics"]["shadow_promotion_hf_minus_v2_plus_mean_regret_uah"] == pytest.approx(-16.057869824303538)
	assert response_payload["comparison_metrics"]["shadow_promotion_safety_failure_count"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["shadow_promotion_tail_failure_delta_count"] == pytest.approx(-16.0)
	assert response_payload["comparison_metrics"]["shadow_promotion_value_gap_ratio_vs_default"] == pytest.approx(0.39748498175945224)
	assert response_payload["artifact_paths"]["value_aligned_shadow_promotion_gate_json"] == str(proof_path)
	warnings = " ".join(response_payload["readiness_warnings"])
	assert "Value-aligned HF shadow" in warnings
	assert "Shadow promotion gate passed for demo candidate-library use" in warnings
	assert "candidate_library_value_aligned" in warnings
	assert "Shadow promotion gate passed for demo candidate-library use" in " ".join(
		response_payload["boundary_labels"]
	)
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_hf_live_safe_switch_value_aligned_shadow_materializes_forecast_for_selected_date_without_lp(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	materialize_calls = _install_fake_operator_preview_forecast_materializer(
		monkeypatch,
		fake_forecast_store,
	)
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_VALUE_ALIGNED_PROMOTION_PROOF_JSON_PATH",
		tmp_path / "missing_promotion_gate.json",
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_FORECAST_GUARD_AUDIT_SUMMARY_JSON_PATH",
		tmp_path / "missing_forecast_guard_summary.json",
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: SimpleNamespace(
			checkpoint_path=str(checkpoint_path),
			candidate_families=(
				"raw_reference",
				"schedule_value_learner_v2_plus",
				"schedule_value_learner_v2_reference",
				"strict_reference",
			),
		),
	)

	def _fake_score(*, bundle: object, candidate_rows: list[dict[str, object]]) -> dict[str, object]:
		del bundle
		strict = next(
			row
			for row in candidate_rows
			if row["dt_schedule_family_target"] == "strict_reference"
		)
		strict_value_uah = float(cast(float, strict["schedule_value_uah"]))
		return {
			"selected_candidate": strict,
			"selected_candidate_id": strict["dt_candidate_id_target"],
			"selected_schedule_family": strict["dt_schedule_family_target"],
			"selected_candidate_index": strict["dt_candidate_index_target"],
			"selected_schedule_value_uah": strict["schedule_value_uah"],
			"predicted_regret_delta_vs_v2_plus_uah": -135.0,
			"predicted_tail_risk_probability": 0.21,
			"abstained_to_v2_plus": False,
			"selection_reason": "predicted_guard_passed",
			"live_actual_regret_available": False,
			"selection_diagnostics": {
				"reported_selected_predicted_regret_delta_vs_v2_plus_uah": -135.0,
				"raw_selected_predicted_regret_delta_vs_v2_plus_uah": -135.0,
				"best_nonfallback_schedule_family": "strict_reference",
				"best_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -135.0,
				"best_nonfallback_predicted_tail_risk_probability": 0.21,
				"best_nonfallback_family_tail_risk_probability": 0.0,
				"best_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"best_safe_nonfallback_schedule_family": "strict_reference",
				"best_safe_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -135.0,
				"best_safe_nonfallback_predicted_tail_risk_probability": 0.21,
				"best_safe_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"best_value_schedule_family": "strict_reference",
				"best_template_schedule_value_uah": strict_value_uah,
				"selected_vs_best_template_value_gap_uah": 0.0,
				"eligible_nonfallback_candidate_count": 1.0,
				"threshold_guard_failed_count": 2.0,
				"predicted_tail_guard_failed_count": 0.0,
				"family_tail_guard_failed_count": 0.0,
				"safety_guard_failed_count": 0.0,
			},
			"scored_candidates": candidate_rows,
		}

	monkeypatch.setattr(api_main, "score_hf_safe_switch_candidate_rows", _fake_score)
	monkeypatch.setattr(
		api_main.HourlyDamBaselineSolver,
		"solve_dispatch_from_forecast",
		lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("LP solver must not run")),
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_value_aligned_shadow",
			"market_venue": "DAM",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "hf_live_safe_switch_value_aligned_shadow"
	assert response_payload["target_delivery_window_start"] == "2026-05-20T00:00:00"
	assert response_payload["target_delivery_window_end"] == "2026-05-21T00:00:00"
	assert response_payload["selected_schedule_family"] == "strict_reference"
	assert any(point["action"] != "hold" for point in response_payload["recommendation_schedule"])
	assert response_payload["comparison_metrics"]["forecast_context_pre_publication"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["source_backed_price_context_available"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["request_fallback_materialized"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["forecast_rows_loaded"] == pytest.approx(24.0)
	assert response_payload["comparison_metrics"]["market_order_payload_emitted"] == pytest.approx(0.0)
	assert materialize_calls == [
		{
			"tenant_id": "client_003_dnipro_factory",
			"market_venue": "DAM",
			"forecast_start": datetime(2026, 5, 16),
			"horizon_hours": 120,
		}
	]
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_hfdt_live_shadow_preview_uses_forecast_v2_plus_candidate_when_guard_abstains(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	target_date = datetime(2026, 5, 20, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=[
				9000.0,
				7600.0,
				6900.0,
				6500.0,
				6200.0,
				6100.0,
				11800.0,
				7200.0,
				4700.0,
				3600.0,
				1800.0,
				250.0,
				700.0,
				740.0,
				780.0,
				840.0,
				2800.0,
				7700.0,
				14900.0,
				15100.0,
				15000.0,
				14950.0,
				14980.0,
				14960.0,
			],
			generated_at=datetime(2026, 5, 19, 18, tzinfo=UTC),
			market_venue="DAM",
		),
		point_prediction_column="predicted_price_uah_mwh",
	)
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_FORECAST_GUARD_AUDIT_SUMMARY_JSON_PATH",
		tmp_path / "missing_forecast_guard_summary.json",
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: SimpleNamespace(
			checkpoint_path=str(checkpoint_path),
			candidate_families=(
				"raw_reference",
				"schedule_value_learner_v2_plus",
				"schedule_value_learner_v2_reference",
				"strict_reference",
			),
		),
	)

	def _fake_score(*, bundle: object, candidate_rows: list[dict[str, object]]) -> dict[str, object]:
		del bundle
		fallback = next(
			row
			for row in candidate_rows
			if row["dt_schedule_family_target"] == "schedule_value_learner_v2_plus"
		)
		dispatch = [float(value) for value in cast(list[float], fallback["dispatch_mw_vector"])]
		assert any(abs(value) > 1e-9 for value in dispatch)
		assert float(cast(float, fallback["schedule_value_uah"])) > 0.0
		return {
			"selected_candidate": fallback,
			"selected_candidate_id": fallback["dt_candidate_id_target"],
			"selected_schedule_family": fallback["dt_schedule_family_target"],
			"selected_candidate_index": fallback["dt_candidate_index_target"],
			"selected_schedule_value_uah": fallback["schedule_value_uah"],
			"predicted_regret_delta_vs_v2_plus_uah": 0.0,
			"predicted_tail_risk_probability": 0.21,
			"abstained_to_v2_plus": True,
			"selection_reason": "guard_abstained_to_safe_fallback",
			"live_actual_regret_available": False,
			"selection_diagnostics": {
				"reported_selected_predicted_regret_delta_vs_v2_plus_uah": 0.0,
				"raw_selected_predicted_regret_delta_vs_v2_plus_uah": 0.0,
				"best_nonfallback_schedule_family": "strict_reference",
				"best_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -85.0,
				"best_nonfallback_predicted_tail_risk_probability": 0.21,
				"best_nonfallback_family_tail_risk_probability": 0.0,
				"best_nonfallback_threshold_margin_to_switch_uah": -15.0,
				"best_safe_nonfallback_schedule_family": "strict_reference",
				"best_safe_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -85.0,
				"best_safe_nonfallback_predicted_tail_risk_probability": 0.21,
				"best_safe_nonfallback_threshold_margin_to_switch_uah": -15.0,
				"best_value_schedule_family": "strict_reference",
				"best_template_schedule_value_uah": float(cast(float, fallback["schedule_value_uah"])) + 100.0,
				"selected_vs_best_template_value_gap_uah": 100.0,
				"eligible_nonfallback_candidate_count": 0.0,
				"threshold_guard_failed_count": 3.0,
				"predicted_tail_guard_failed_count": 0.0,
				"family_tail_guard_failed_count": 0.0,
				"safety_guard_failed_count": 0.0,
			},
			"scored_candidates": candidate_rows,
		}

	monkeypatch.setattr(api_main, "score_hf_safe_switch_candidate_rows", _fake_score)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hfdt_live_shadow_preview",
			"market_venue": "DAM",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "hfdt_live_shadow_preview"
	assert response_payload["selected_schedule_family"] == "schedule_value_learner_v2_plus"
	assert any(point["action"] != "hold" for point in response_payload["recommendation_schedule"])
	assert response_payload["comparison_metrics"]["hfdt_live_shadow_preview"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["v2_plus_forecast_candidate_available"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["forecast_context_pre_publication"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["source_backed_price_context_available"] == pytest.approx(1.0)
	warnings = " ".join(response_payload["readiness_warnings"])
	assert "HFDT live shadow ranks source-backed forecast candidate rows" in warnings
	assert "V2+ forecast fallback candidate uses deterministic LP preview rows" in warnings
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert response_payload["dt_lava_ready"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_hf_live_safe_switch_value_aligned_shadow_accepts_same_day_forecast_refresh_without_lp(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	target_date = datetime(2026, 6, 2, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=[4100.0 + hour * 25.0 for hour in range(24)],
			generated_at=target_date + timedelta(hours=12),
			market_venue="DAM",
			training_cutoff=target_date + timedelta(hours=11),
		),
		point_prediction_column="predicted_price_uah_mwh",
	)
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_VALUE_ALIGNED_PROMOTION_PROOF_JSON_PATH",
		tmp_path / "missing_promotion_gate.json",
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_FORECAST_GUARD_AUDIT_SUMMARY_JSON_PATH",
		tmp_path / "missing_forecast_guard_summary.json",
		raising=False,
	)
	monkeypatch.setattr(api_main, "_operator_preview_local_date", lambda: target_date.date())
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: SimpleNamespace(
			checkpoint_path=str(checkpoint_path),
			candidate_families=(
				"raw_reference",
				"schedule_value_learner_v2_plus",
				"schedule_value_learner_v2_reference",
				"strict_reference",
			),
		),
	)

	def _fake_score(*, bundle: object, candidate_rows: list[dict[str, object]]) -> dict[str, object]:
		del bundle
		strict = next(
			row
			for row in candidate_rows
			if row["dt_schedule_family_target"] == "strict_reference"
		)
		return {
			"selected_candidate": strict,
			"selected_candidate_id": strict["dt_candidate_id_target"],
			"selected_schedule_family": strict["dt_schedule_family_target"],
			"selected_candidate_index": strict["dt_candidate_index_target"],
			"selected_schedule_value_uah": strict["schedule_value_uah"],
			"predicted_regret_delta_vs_v2_plus_uah": -135.0,
			"predicted_tail_risk_probability": 0.21,
			"abstained_to_v2_plus": False,
			"selection_reason": "predicted_guard_passed",
			"live_actual_regret_available": False,
			"selection_diagnostics": {
				"best_safe_nonfallback_schedule_family": "strict_reference",
				"best_safe_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -135.0,
				"best_safe_nonfallback_predicted_tail_risk_probability": 0.21,
				"best_safe_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"threshold_guard_failed_count": 2.0,
				"predicted_tail_guard_failed_count": 0.0,
				"family_tail_guard_failed_count": 0.0,
				"safety_guard_failed_count": 0.0,
			},
			"scored_candidates": candidate_rows,
		}

	monkeypatch.setattr(api_main, "score_hf_safe_switch_candidate_rows", _fake_score)
	monkeypatch.setattr(
		api_main.HourlyDamBaselineSolver,
		"solve_dispatch_from_forecast",
		lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("LP solver must not run")),
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_value_aligned_shadow",
			"market_venue": "DAM",
			"target_delivery_date": "2026-06-02",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["target_delivery_window_start"].startswith("2026-06-02T00:00:00")
	assert len(response_payload["recommendation_schedule"]) == 24
	assert response_payload["comparison_metrics"]["forecast_context_same_day_refresh"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["source_backed_price_context_available"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["request_fallback_materialized"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["forecast_rows_loaded"] == pytest.approx(24.0)
	assert response_payload["comparison_metrics"]["market_order_payload_emitted"] == pytest.approx(0.0)
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_hf_live_safe_switch_value_aligned_shadow_materializes_after_stale_invalid_forecast_metadata(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	target_date = datetime(2026, 5, 20)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=[4200.0 + hour * 10.0 for hour in range(24)],
			generated_at=target_date + timedelta(hours=12),
			market_venue="DAM",
			training_cutoff=target_date + timedelta(hours=11),
		),
		point_prediction_column="predicted_price_uah_mwh",
	)
	materialize_calls = _install_fake_operator_preview_forecast_materializer(
		monkeypatch,
		fake_forecast_store,
	)
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_VALUE_ALIGNED_PROMOTION_PROOF_JSON_PATH",
		tmp_path / "missing_promotion_gate.json",
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_FORECAST_GUARD_AUDIT_SUMMARY_JSON_PATH",
		tmp_path / "missing_forecast_guard_summary.json",
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: SimpleNamespace(
			checkpoint_path=str(checkpoint_path),
			candidate_families=(
				"raw_reference",
				"schedule_value_learner_v2_plus",
				"schedule_value_learner_v2_reference",
				"strict_reference",
			),
		),
	)

	def _fake_score(*, bundle: object, candidate_rows: list[dict[str, object]]) -> dict[str, object]:
		del bundle
		fallback = next(
			row
			for row in candidate_rows
			if row["dt_schedule_family_target"] == "schedule_value_learner_v2_plus"
		)
		return {
			"selected_candidate": fallback,
			"selected_candidate_id": fallback["dt_candidate_id_target"],
			"selected_schedule_family": fallback["dt_schedule_family_target"],
			"selected_candidate_index": fallback["dt_candidate_index_target"],
			"selected_schedule_value_uah": fallback["schedule_value_uah"],
			"predicted_regret_delta_vs_v2_plus_uah": 0.0,
			"predicted_tail_risk_probability": 0.2,
			"abstained_to_v2_plus": True,
			"selection_reason": "guard_abstained_to_safe_fallback",
			"live_actual_regret_available": False,
			"selection_diagnostics": {
				"threshold_guard_failed_count": 2.0,
				"predicted_tail_guard_failed_count": 0.0,
				"family_tail_guard_failed_count": 0.0,
				"safety_guard_failed_count": 0.0,
			},
			"scored_candidates": candidate_rows,
		}

	monkeypatch.setattr(api_main, "score_hf_safe_switch_candidate_rows", _fake_score)
	monkeypatch.setattr(
		api_main.HourlyDamBaselineSolver,
		"solve_dispatch_from_forecast",
		lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("LP solver must not run")),
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_value_aligned_shadow",
			"market_venue": "DAM",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["target_delivery_window_start"] == "2026-05-20T00:00:00"
	assert len(response_payload["recommendation_schedule"]) == 24
	assert response_payload["comparison_metrics"]["source_backed_price_context_available"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["request_fallback_materialized"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["forecast_rows_loaded"] == pytest.approx(24.0)
	assert response_payload["comparison_metrics"]["market_order_payload_emitted"] == pytest.approx(0.0)
	assert materialize_calls
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_hf_live_safe_switch_value_aligned_shadow_reports_missing_proof_without_blocking(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_VALUE_ALIGNED_PROMOTION_PROOF_JSON_PATH",
		tmp_path / "missing_promotion_gate.json",
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_FORECAST_GUARD_AUDIT_SUMMARY_JSON_PATH",
		tmp_path / "missing_forecast_guard_summary.json",
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: SimpleNamespace(
			checkpoint_path=str(checkpoint_path),
			candidate_families=(
				"raw_reference",
				"schedule_value_learner_v2_plus",
				"schedule_value_learner_v2_reference",
				"strict_reference",
			),
		),
	)

	def _fake_score(*, bundle: object, candidate_rows: list[dict[str, object]]) -> dict[str, object]:
		del bundle
		fallback = next(
			row
			for row in candidate_rows
			if row["dt_schedule_family_target"] == "schedule_value_learner_v2_plus"
		)
		return {
			"selected_candidate": fallback,
			"selected_candidate_id": fallback["dt_candidate_id_target"],
			"selected_schedule_family": fallback["dt_schedule_family_target"],
			"selected_candidate_index": fallback["dt_candidate_index_target"],
			"selected_schedule_value_uah": fallback["schedule_value_uah"],
			"predicted_regret_delta_vs_v2_plus_uah": 0.0,
			"predicted_tail_risk_probability": 0.2,
			"abstained_to_v2_plus": True,
			"selection_reason": "guard_abstained_to_safe_fallback",
			"live_actual_regret_available": False,
			"selection_diagnostics": {
				"best_template_schedule_value_uah": 100.0,
				"selected_vs_best_template_value_gap_uah": 100.0,
				"threshold_guard_failed_count": 2.0,
				"predicted_tail_guard_failed_count": 0.0,
				"family_tail_guard_failed_count": 0.0,
				"safety_guard_failed_count": 0.0,
			},
			"scored_candidates": candidate_rows,
		}

	monkeypatch.setattr(api_main, "score_hf_safe_switch_candidate_rows", _fake_score)
	monkeypatch.setattr(
		api_main.HourlyDamBaselineSolver,
		"solve_dispatch_from_forecast",
		lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("LP solver must not run")),
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_value_aligned_shadow",
			"market_venue": "DAM",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["comparison_metrics"]["shadow_promotion_gate_available"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["shadow_promotion_gate_passed"] == pytest.approx(0.0)
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert "Shadow promotion proof packet is not materialized" in " ".join(
		response_payload["readiness_warnings"]
	)


def test_hf_live_safe_switch_value_aligned_shadow_reports_forecast_guard_abstention(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_VALUE_ALIGNED_PROMOTION_PROOF_JSON_PATH",
		tmp_path / "missing_promotion_gate.json",
		raising=False,
	)
	forecast_audit_path = tmp_path / "forecast_guard_summary.json"
	forecast_audit_path.write_text(
		json.dumps(
			{
				"forecast_candidate_library_update_gate_passed": True,
				"update_gate_candidate_template_grid_id": "candidate_library_forecast_guarded",
				"candidate_library_forecast_guarded_switch_count": 1.0,
				"candidate_library_forecast_guarded_selected_value_improvement_uah": 79.7,
				"candidate_library_forecast_guarded_tail_failure_delta_count": -2.0,
				"candidate_library_forecast_guarded_safety_failure_count": 0.0,
				"market_execution_enabled": False,
				"promotion_gate_passed": False,
				"market_order_payload_emitted": False,
				"proposed_bid_emitted": False,
			}
		),
		encoding="utf-8",
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_FORECAST_GUARD_AUDIT_SUMMARY_JSON_PATH",
		forecast_audit_path,
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: SimpleNamespace(
			checkpoint_path=str(checkpoint_path),
			candidate_families=(
				"raw_reference",
				"schedule_value_learner_v2_plus",
				"schedule_value_learner_v2_reference",
				"strict_reference",
			),
		),
	)
	target_date = datetime(2026, 5, 20, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=[3100.0 + hour * 77.0 for hour in range(24)],
			generated_at=datetime(2026, 5, 19, 18, tzinfo=UTC),
			market_venue="DAM",
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	def _fake_score(*, bundle: object, candidate_rows: list[dict[str, object]]) -> dict[str, object]:
		del bundle
		fallback = next(
			row
			for row in candidate_rows
			if row["dt_schedule_family_target"] == "schedule_value_learner_v2_plus"
		)
		return {
			"selected_candidate": fallback,
			"selected_candidate_id": fallback["dt_candidate_id_target"],
			"selected_schedule_family": fallback["dt_schedule_family_target"],
			"selected_candidate_index": fallback["dt_candidate_index_target"],
			"selected_schedule_value_uah": fallback["schedule_value_uah"],
			"predicted_regret_delta_vs_v2_plus_uah": 0.0,
			"predicted_tail_risk_probability": 0.61,
			"abstained_to_v2_plus": True,
			"selection_reason": "guard_abstained_to_safe_fallback",
			"live_actual_regret_available": False,
			"selection_diagnostics": {
				"reported_selected_predicted_regret_delta_vs_v2_plus_uah": 0.0,
				"raw_selected_predicted_regret_delta_vs_v2_plus_uah": 0.0,
				"best_nonfallback_schedule_family": "strict_reference",
				"best_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -90.0,
				"best_nonfallback_predicted_tail_risk_probability": 0.61,
				"best_nonfallback_family_tail_risk_probability": 0.0,
				"best_nonfallback_threshold_margin_to_switch_uah": -10.0,
				"best_safe_nonfallback_schedule_family": "",
				"best_safe_nonfallback_predicted_regret_delta_vs_v2_plus_uah": 0.0,
				"best_safe_nonfallback_predicted_tail_risk_probability": 0.0,
				"best_safe_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"best_value_schedule_family": "strict_reference",
				"best_template_schedule_value_uah": 850.0,
				"selected_vs_best_template_value_gap_uah": 850.0,
				"eligible_nonfallback_candidate_count": 0.0,
				"threshold_guard_failed_count": 3.0,
				"predicted_tail_guard_failed_count": 2.0,
				"family_tail_guard_failed_count": 0.0,
				"safety_guard_failed_count": 0.0,
			},
			"scored_candidates": candidate_rows,
		}

	monkeypatch.setattr(api_main, "score_hf_safe_switch_candidate_rows", _fake_score)
	monkeypatch.setattr(
		api_main.HourlyDamBaselineSolver,
		"solve_dispatch_from_forecast",
		lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("LP solver must not run")),
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_value_aligned_shadow",
			"market_venue": "DAM",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["market_venue"] == "DAM"
	assert response_payload["selected_schedule_family"] == "schedule_value_learner_v2_plus"
	assert all(point["action"] == "hold" for point in response_payload["recommendation_schedule"])
	assert response_payload["comparison_metrics"]["forecast_context_pre_publication"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["candidate_template_grid_forecast_guarded"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["guard_abstained_to_safe_fallback"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["forecast_guard_abstained_to_safe_fallback"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["threshold_guard_failed_count"] == pytest.approx(3.0)
	assert response_payload["comparison_metrics"]["predicted_tail_guard_failed_count"] == pytest.approx(2.0)
	assert response_payload["comparison_metrics"]["safety_guard_failed_count"] == pytest.approx(0.0)
	warnings = " ".join(response_payload["readiness_warnings"])
	assert "target_delivery_date=2026-05-20 loaded from source-backed pre-publication forecast rows" in warnings
	assert "HF selected the guarded fallback schedule because non-fallback candidates failed threshold/tail-risk guards" in warnings
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_hf_live_safe_switch_value_aligned_shadow_uses_forecast_guarded_grid_after_audit(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_VALUE_ALIGNED_PROMOTION_PROOF_JSON_PATH",
		tmp_path / "missing_promotion_gate.json",
		raising=False,
	)
	forecast_audit_path = tmp_path / "forecast_guard_summary.json"
	forecast_audit_path.write_text(
		json.dumps(
			{
				"forecast_candidate_library_update_gate_passed": True,
				"update_gate_candidate_template_grid_id": "candidate_library_forecast_guarded",
				"candidate_library_forecast_guarded_switch_count": 1.0,
				"candidate_library_forecast_guarded_selected_value_improvement_uah": 79.7,
				"candidate_library_forecast_guarded_tail_failure_delta_count": -2.0,
				"candidate_library_forecast_guarded_safety_failure_count": 0.0,
				"market_execution_enabled": False,
				"promotion_gate_passed": False,
				"market_order_payload_emitted": False,
				"proposed_bid_emitted": False,
			}
		),
		encoding="utf-8",
	)
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_FORECAST_GUARD_AUDIT_SUMMARY_JSON_PATH",
		forecast_audit_path,
		raising=False,
	)
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: SimpleNamespace(
			checkpoint_path=str(checkpoint_path),
			candidate_families=(
				"raw_reference",
				"schedule_value_learner_v2_plus",
				"schedule_value_learner_v2_reference",
				"strict_reference",
			),
		),
	)
	target_date = datetime(2026, 5, 20, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=_forecast_frame(
			target_date=target_date,
			values=[3100.0 + hour * 77.0 for hour in range(24)],
			generated_at=datetime(2026, 5, 19, 18, tzinfo=UTC),
			market_venue="DAM",
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	def _fake_score(*, bundle: object, candidate_rows: list[dict[str, object]]) -> dict[str, object]:
		del bundle
		strict = next(
			row
			for row in candidate_rows
			if row["dt_schedule_family_target"] == "strict_reference"
		)
		strict_dispatch_vector = cast(list[float], strict["dispatch_mw_vector"])
		strict_value_uah = float(cast(float, strict["schedule_value_uah"]))
		strict_dispatch = [
			float(value)
			for value in strict_dispatch_vector
			if abs(float(value)) > 1e-9
		]
		assert len(strict_dispatch) == 6
		return {
			"selected_candidate": strict,
			"selected_candidate_id": strict["dt_candidate_id_target"],
			"selected_schedule_family": strict["dt_schedule_family_target"],
			"selected_candidate_index": strict["dt_candidate_index_target"],
			"selected_schedule_value_uah": strict["schedule_value_uah"],
			"predicted_regret_delta_vs_v2_plus_uah": -130.0,
			"predicted_tail_risk_probability": 0.32,
			"abstained_to_v2_plus": False,
			"selection_reason": "predicted_guard_passed",
			"live_actual_regret_available": False,
			"selection_diagnostics": {
				"reported_selected_predicted_regret_delta_vs_v2_plus_uah": -130.0,
				"raw_selected_predicted_regret_delta_vs_v2_plus_uah": -130.0,
				"best_nonfallback_schedule_family": "strict_reference",
				"best_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -130.0,
				"best_nonfallback_predicted_tail_risk_probability": 0.32,
				"best_nonfallback_family_tail_risk_probability": 0.0,
				"best_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"best_safe_nonfallback_schedule_family": "strict_reference",
				"best_safe_nonfallback_predicted_regret_delta_vs_v2_plus_uah": -130.0,
				"best_safe_nonfallback_predicted_tail_risk_probability": 0.32,
				"best_safe_nonfallback_threshold_margin_to_switch_uah": 0.0,
				"best_value_schedule_family": "strict_reference",
				"best_template_schedule_value_uah": strict_value_uah,
				"selected_vs_best_template_value_gap_uah": 0.0,
				"eligible_nonfallback_candidate_count": 1.0,
				"threshold_guard_failed_count": 2.0,
				"predicted_tail_guard_failed_count": 0.0,
				"family_tail_guard_failed_count": 0.0,
				"safety_guard_failed_count": 0.0,
			},
			"scored_candidates": candidate_rows,
		}

	monkeypatch.setattr(api_main, "score_hf_safe_switch_candidate_rows", _fake_score)
	monkeypatch.setattr(
		api_main.HourlyDamBaselineSolver,
		"solve_dispatch_from_forecast",
		lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("LP solver must not run")),
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_value_aligned_shadow",
			"market_venue": "DAM",
			"target_delivery_date": "2026-05-20",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["selected_schedule_family"] == "strict_reference"
	assert any(point["action"] != "hold" for point in response_payload["recommendation_schedule"])
	assert response_payload["comparison_metrics"]["candidate_template_grid_forecast_guarded"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["forecast_guard_audit_passed"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["forecast_context_pre_publication"] == pytest.approx(1.0)
	assert response_payload["artifact_paths"]["forecast_guard_audit_summary_json"] == str(forecast_audit_path)
	assert "candidate_library_forecast_guarded" in " ".join(response_payload["readiness_warnings"])
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_hf_live_safe_switch_shadow_preview_blocks_when_checkpoint_missing(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		tmp_path / "missing_checkpoint",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_shadow",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "hf_live_safe_switch_shadow"
	assert response_payload["preview_status"] == "blocked_missing_hf_live_inference_bundle"
	assert response_payload["recommendation_schedule"] == []
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False


def test_hf_live_safe_switch_value_aligned_shadow_blocks_when_price_context_missing(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	del fake_forecast_store
	checkpoint_dir = tmp_path / "hf_live_checkpoint"
	checkpoint_dir.mkdir()
	monkeypatch.setattr(
		api_main,
		"HF_LIVE_SAFE_SWITCH_INFERENCE_CHECKPOINT_DIR_PATH",
		checkpoint_dir,
	)
	monkeypatch.setattr(
		api_main,
		"load_hf_safe_switch_inference_bundle",
		lambda checkpoint_path: (_ for _ in ()).throw(
			AssertionError("HF bundle must not load before source-backed price context exists")
		),
	)
	monkeypatch.setattr(
		api_main,
		"score_hf_safe_switch_candidate_rows",
		lambda *args, **kwargs: (_ for _ in ()).throw(
			AssertionError("HF scorer must not run before source-backed price context exists")
		),
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "hf_live_safe_switch_value_aligned_shadow",
			"market_venue": "DAM",
			"target_delivery_date": "2030-01-01",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "hf_live_safe_switch_value_aligned_shadow"
	assert response_payload["preview_status"] == "blocked_missing_source_backed_price_context"
	assert response_payload["market_venue"] == "DAM"
	assert response_payload["target_delivery_window_start"] == "2030-01-01T00:00:00"
	assert response_payload["target_delivery_window_end"] == "2030-01-02T00:00:00"
	assert response_payload["recommendation_schedule"] == []
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert response_payload["promotion_gate_passed"] is False
	assert response_payload["dt_lava_ready"] is False
	assert response_payload["source_readiness_gate_passed"] is False
	assert response_payload["comparison_metrics"]["source_backed_price_context_available"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["forecast_context_pre_publication"] == pytest.approx(1.0)
	assert response_payload["comparison_metrics"]["request_fallback_materialized"] == pytest.approx(0.0)
	assert response_payload["comparison_metrics"]["forecast_rows_loaded"] == pytest.approx(0.0)
	warnings = " ".join(response_payload["readiness_warnings"])
	assert "selected date and venue were received" in warnings
	assert "No substitute prices are rendered" in warnings
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_regret_aware_selector_shadow_preview_rejects_market_execution_artifacts(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_regret_aware_selector_fixture(tmp_path, market_execution_enabled=True)
	monkeypatch.setattr(
		api_main,
		"REGRET_AWARE_V2_PLUS_SELECTOR_SELECTED_ROWS_CSV_PATH",
		tmp_path / "regret_aware_v2_plus_selector_selected_rows.csv",
	)
	monkeypatch.setattr(
		api_main,
		"REGRET_AWARE_V2_PLUS_SELECTOR_TEACHER_ROWS_CSV_PATH",
		tmp_path / "regret_aware_v2_plus_selector_teacher_rows.csv",
	)
	monkeypatch.setattr(
		api_main,
		"REGRET_AWARE_V2_PLUS_SELECTOR_SUMMARY_JSON_PATH",
		tmp_path / "regret_aware_v2_plus_selector_summary.json",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "regret_aware_v2_plus_selector_shadow",
		},
	)

	assert response.status_code == 500
	assert "market_execution_enabled=false" in response.json()["detail"]


def test_dt_shadow_recommendation_preview_projects_placeholder_soc(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_shadow_preview_fixture(
		tmp_path,
		soc_fraction_vector=[0.5, 0.5, 0.5, 0.5],
	)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={"tenant_id": "client_003_dnipro_factory", "preview_source": "dt_shadow"},
	)

	assert response.status_code == 200
	schedule = response.json()["recommendation_schedule"]
	assert schedule[0]["soc_before_fraction"] == pytest.approx(0.5)
	assert schedule[0]["soc_after_fraction"] < 0.5
	assert schedule[1]["soc_before_fraction"] == pytest.approx(schedule[0]["soc_after_fraction"])
	assert schedule[1]["soc_after_fraction"] > schedule[1]["soc_before_fraction"]
	assert schedule[2]["soc_before_fraction"] == pytest.approx(schedule[2]["soc_after_fraction"])


def test_dt_shadow_recommendation_preview_can_project_to_requested_delivery_window(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_shadow_preview_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "dt_shadow",
			"target_delivery_window_start": "2026-05-26T00:00:00",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	schedule = response_payload["recommendation_schedule"]
	assert response_payload["target_delivery_window_start"] == "2026-05-26T00:00:00"
	assert response_payload["target_delivery_window_end"] == "2026-05-26T03:00:00"
	assert [point["interval_start"] for point in schedule] == [
		"2026-05-26T00:00:00",
		"2026-05-26T01:00:00",
		"2026-05-26T02:00:00",
	]
	assert response_payload["anchor_timestamp"] == "2026-05-05T23:00:00Z"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert "Projected onto requested delivery-day window" in response_payload["boundary_labels"]
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_dt_shadow_recommendation_preview_projects_selected_target_delivery_date(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_dt_shadow_preview_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_SELECTED_PREVIEW_JSON_PATH",
		tmp_path / "dt_selected_preview.json",
	)
	monkeypatch.setattr(
		api_main,
		"DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_PATH",
		tmp_path / "teacher_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "dt_shadow",
			"target_delivery_date": "2026-05-26",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	schedule = response_payload["recommendation_schedule"]
	assert response_payload["target_delivery_window_start"] == "2026-05-26T00:00:00"
	assert response_payload["target_delivery_window_end"] == "2026-05-26T03:00:00"
	assert [point["interval_start"] for point in schedule] == [
		"2026-05-26T00:00:00",
		"2026-05-26T01:00:00",
		"2026-05-26T02:00:00",
	]
	assert response_payload["anchor_timestamp"] == "2026-05-05T23:00:00Z"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert "Projected onto requested delivery-day window" in response_payload["boundary_labels"]
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def test_poland_tft_shadow_recommendation_preview_projects_soc_when_artifact_omits_it(
	client: TestClient,
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
) -> None:
	_write_shadow_augmented_gate_fixture(tmp_path)
	monkeypatch.setattr(
		api_main,
		"TFT_SHADOW_AUGMENTED_GATE_ROWS_CSV_PATH",
		tmp_path / "tft_augmented_gate_rows.csv",
	)

	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={"tenant_id": "client_003_dnipro_factory", "preview_source": "poland_tft_shadow"},
	)

	assert response.status_code == 200
	schedule = response.json()["recommendation_schedule"]
	assert schedule[0]["soc_before_fraction"] == pytest.approx(0.5)
	assert schedule[0]["soc_after_fraction"] < 0.5
	assert schedule[1]["soc_before_fraction"] == pytest.approx(schedule[0]["soc_after_fraction"])
	assert schedule[1]["soc_after_fraction"] > schedule[1]["soc_before_fraction"]
	assert response.json()["market_execution_enabled"] is False
	assert "proposed_bid" not in response.json()
	assert "market_order_payload" not in response.json()


def test_blocked_v13_dt_lava_promoted_training_preview_stays_roadmap_only(
	client: TestClient,
) -> None:
	response = client.get(
		"/dashboard/shadow-recommendation-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"preview_source": "v13_dt_lava_promoted_training",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["preview_source_id"] == "v13_dt_lava_promoted_training"
	assert response_payload["preview_status"] == "blocked_source_readiness_roadmap"
	assert response_payload["recommendation_schedule"] == []
	assert response_payload["is_promoted_strategy"] is False
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["market_order_payload_emitted"] is False
	assert "proposed_bid" not in response_payload
	assert "market_order_payload" not in response_payload


def _write_dt_shadow_preview_fixture(
	tmp_path: Path,
	*,
	soc_fraction_vector: list[float] | None = None,
	packet_market_execution_enabled: bool = False,
	packet_dt_lava_ready: bool = False,
	packet_permits_model_training: bool = False,
	packet_promotion_gate_passed: bool = False,
	teacher_market_execution_enabled: bool = False,
	teacher_dt_lava_ready: bool = False,
	teacher_permits_model_training: bool = False,
	teacher_promotion_gate_passed: bool = False,
) -> None:
	selected_candidate_id = "dt-candidate-worse-than-v2"
	soc_values = soc_fraction_vector or [0.52, 0.47, 0.51, 0.51]
	(tmp_path / "dt_selected_preview.json").write_text(
		json.dumps(
			{
				"claim_scope": "dt_research_shadow_selected_schedule_preview_not_promotable_not_market_execution",
				"preview_rows": [
					{
						"tenant_id": "client_003_dnipro_factory",
						"anchor_timestamp": "2026-05-05T23:00:00+00:00",
						"selected_candidate_id": selected_candidate_id,
						"selected_schedule_family": "dt_tail_risk_aware_schedule",
						"selected_candidate_index": 7,
						"dt_selected_regret_uah": 245.0,
						"dt_selected_value_uah": 700.0,
						"v2_plus_regret_uah": 200.0,
						"v2_plus_value_uah": 745.0,
						"strict_regret_uah": 165.0,
						"strict_value_uah": 780.0,
						"behavior_cloning_regret_uah": 250.0,
						"behavior_cloning_value_uah": 695.0,
						"market_execution_enabled": packet_market_execution_enabled,
						"promotion_gate_passed": packet_promotion_gate_passed,
						"dt_promotion_gate_passed": packet_promotion_gate_passed,
						"dt_lava_ready": packet_dt_lava_ready,
						"permits_model_training": packet_permits_model_training,
						"research_shadow_not_promotable": True,
					}
				],
				"evaluation_metrics": {
					"dt_selected_mean_regret_uah": 245.0,
					"dt_selected_mean_value_uah": 700.0,
					"v2_plus_mean_regret_uah": 200.0,
					"v2_plus_mean_value_uah": 745.0,
					"strict_mean_regret_uah": 165.0,
					"strict_mean_value_uah": 780.0,
					"behavior_cloning_mean_regret_uah": 250.0,
					"behavior_cloning_mean_value_uah": 695.0,
				},
				"market_execution_enabled": packet_market_execution_enabled,
				"promotion_gate_passed": packet_promotion_gate_passed,
				"dt_promotion_gate_passed": packet_promotion_gate_passed,
				"dt_lava_ready": packet_dt_lava_ready,
				"permits_model_training": packet_permits_model_training,
				"research_shadow_not_promotable": True,
			}
		),
		encoding="utf-8",
	)
	pl.DataFrame(
		{
			"tenant_id": ["client_003_dnipro_factory"],
			"anchor_timestamp": [datetime(2026, 5, 5, 23, tzinfo=UTC)],
			"dt_candidate_id_target": [selected_candidate_id],
			"dt_schedule_family_target": ["dt_tail_risk_aware_schedule"],
			"candidate_model_name": ["dt_shadow_candidate_tail_risk_aware"],
			"horizon_hours": [3],
			"forecast_price_uah_mwh_vector": [json.dumps([4300.0, 1400.0, 2400.0])],
			"dispatch_mw_vector": [json.dumps([0.12, -0.05, 0.0])],
			"soc_fraction_vector": [json.dumps(soc_values)],
			"schedule_value_uah": [700.0],
			"decision_value_uah": [700.0],
			"regret_uah": [245.0],
			"regret_delta_vs_v2_plus_uah": [45.0],
			"oracle_value_uah": [945.0],
			"safety_violation_count": [0],
			"market_execution_enabled": [teacher_market_execution_enabled],
			"market_execution_gate_passed": [False],
			"promotion_gate_passed": [teacher_promotion_gate_passed],
			"dt_lava_ready": [teacher_dt_lava_ready],
			"permits_model_training": [teacher_permits_model_training],
			"not_market_execution": [True],
			"not_deployed_dt_control": [True],
		}
	).write_csv(tmp_path / "teacher_rows.csv")


def _write_regret_aware_selector_fixture(
	tmp_path: Path,
	*,
	market_execution_enabled: bool = False,
) -> None:
	selected_candidate_id = "v2-plus-candidate"
	(tmp_path / "regret_aware_v2_plus_selector_summary.json").write_text(
		json.dumps(
			{
				"boundary": {
					"market_execution_enabled": market_execution_enabled,
					"promotion_gate_passed": False,
					"dt_lava_ready": False,
					"research_shadow_not_promotable": True,
				},
				"evaluation": {
					"selector_mean_regret_uah": 174.77,
					"selector_mean_value_uah": 825.0,
					"selector_minus_v2_plus_mean_regret_uah": 0.0,
					"selector_minus_v2_plus_mean_value_uah": 0.0,
					"v2_plus_mean_regret_uah": 174.77,
					"v2_plus_mean_value_uah": 825.0,
					"non_v2_plus_switch_count": 0,
					"abstention_count": 90,
					"control_summary": {
						"strict_reference": {
							"mean_regret_uah": 310.58,
							"mean_value_uah": 700.0,
						}
					},
				},
				"market_execution_enabled": market_execution_enabled,
			}
		),
		encoding="utf-8",
	)
	pl.DataFrame(
		{
			"tenant_id": ["client_003_dnipro_factory"],
			"source_model_name": ["nbeatsx_official_global_panel_horizon_calibrated_v1"],
			"anchor_timestamp": [datetime(2026, 5, 5, 23, tzinfo=UTC)],
			"selected_candidate_id": [selected_candidate_id],
			"selected_candidate_index": [1],
			"selected_schedule_family": ["schedule_value_learner_v2_plus"],
			"selected_regret_uah": [174.77],
			"selected_value_uah": [825.0],
			"v2_plus_candidate_id": [selected_candidate_id],
			"v2_plus_regret_uah": [174.77],
			"v2_plus_value_uah": [825.0],
			"selected_minus_v2_plus_regret_uah": [0.0],
			"selected_minus_v2_plus_value_uah": [0.0],
			"predicted_regret_delta_vs_v2_plus_uah": [0.0],
			"predicted_improvement_vs_v2_plus_uah": [0.0],
			"abstained_to_v2_plus": [True],
			"abstention_reason": ["predicted_improvement_below_threshold"],
			"family_tail_risk_probability": [0.07],
			"tail_risk_guard_passed": [True],
			"research_shadow_not_promotable": [True],
			"dt_lava_ready": [False],
			"promotion_gate_passed": [False],
			"market_execution_enabled": [market_execution_enabled],
			"not_market_execution": [not market_execution_enabled],
		}
	).write_csv(tmp_path / "regret_aware_v2_plus_selector_selected_rows.csv")
	pl.DataFrame(
		{
			"tenant_id": ["client_003_dnipro_factory", "client_003_dnipro_factory"],
			"anchor_timestamp": [
				datetime(2026, 5, 5, 23, tzinfo=UTC),
				datetime(2026, 5, 5, 23, tzinfo=UTC),
			],
			"dt_candidate_id_target": [selected_candidate_id, "strict-candidate"],
			"dt_schedule_family_target": ["schedule_value_learner_v2_plus", "strict_reference"],
			"candidate_family": ["schedule_value_learner_v2_plus", "strict_reference"],
			"candidate_model_name": ["schedule_value_learner_v2_plus", "strict_reference"],
			"horizon_hours": [3, 3],
			"forecast_price_uah_mwh_vector": [
				json.dumps([4300.0, 1400.0, 2400.0]),
				json.dumps([4300.0, 1400.0, 2400.0]),
			],
			"dispatch_mw_vector": [json.dumps([0.12, -0.05, 0.0]), json.dumps([0.0, 0.0, 0.0])],
			"soc_fraction_vector": [json.dumps([0.52, 0.47, 0.51, 0.51]), json.dumps([0.52, 0.52, 0.52, 0.52])],
			"schedule_value_uah": [825.0, 700.0],
			"decision_value_uah": [825.0, 700.0],
			"regret_uah": [174.77, 310.58],
			"regret_delta_vs_v2_plus_uah": [0.0, 135.81],
			"oracle_value_uah": [999.77, 1010.58],
			"safety_violation_count": [0, 0],
			"market_execution_enabled": [market_execution_enabled, False],
			"market_execution_gate_passed": [False, False],
			"promotion_gate_passed": [False, False],
			"not_market_execution": [not market_execution_enabled, True],
			"not_deployed_dt_control": [True, True],
		}
	).write_csv(tmp_path / "regret_aware_v2_plus_selector_teacher_rows.csv")


def _write_dt_v2_plus_safe_switch_selector_fixture(tmp_path: Path) -> None:
	selected_candidate_id = "strict-candidate"
	v2_plus_candidate_id = "v2-plus-candidate"
	(tmp_path / "regret_aware_v2_plus_selector_summary.json").write_text(
		json.dumps(
			{
				"boundary": {
					"market_execution_enabled": False,
					"promotion_gate_passed": False,
					"dt_lava_ready": False,
					"research_shadow_not_promotable": True,
				},
				"evaluation": {
					"selector_mean_regret_uah": 168.15664125116336,
					"selector_mean_value_uah": 3743.327643562355,
					"selector_minus_v2_plus_mean_regret_uah": -6.611757063998141,
					"selector_minus_v2_plus_mean_value_uah": 6.611757063998084,
					"v2_plus_mean_regret_uah": 174.7683983151615,
					"v2_plus_mean_value_uah": 3736.715886498357,
					"non_v2_plus_switch_count": 4,
					"abstention_count": 86,
					"control_summary": {
						"strict_reference": {
							"mean_regret_uah": 310.58280814033515,
							"mean_value_uah": 3600.901476666783,
						}
					},
				},
				"market_execution_enabled": False,
			}
		),
		encoding="utf-8",
	)
	(tmp_path / "dt_v2_plus_promotion_evidence_summary.json").write_text(
		json.dumps(
			{
				"boundary": {
					"market_execution_enabled": False,
					"promotion_gate_passed": False,
					"dt_lava_ready": False,
					"permits_model_training": False,
					"promotion_evidence_passed": True,
				},
				"gate": {
					"observed_safe_switch_opportunity_count": 15,
					"recovered_safe_switch_opportunity_count": 3,
					"safe_switch_win_count": 3,
					"safe_switch_loss_count": 0,
					"safe_switch_tie_count": 1,
					"tail_risk_loss_count": 0,
					"max_switch_loss_uah": 0.0,
					"mean_regret_improvement_ratio_vs_v2_plus": 0.03783153663784855,
					"oracle_scored_final_holdout_row_count": 360,
					"selected_mean_regret_uah": 168.15664125116336,
					"v2_plus_mean_regret_uah": 174.7683983151615,
					"strict_reference_mean_regret_uah": 310.58280814033515,
				},
			}
		),
		encoding="utf-8",
	)
	pl.DataFrame(
		{
			"tenant_id": ["client_003_dnipro_factory"],
			"source_model_name": ["nbeatsx_official_global_panel_horizon_calibrated_v1"],
			"anchor_timestamp": [datetime(2026, 5, 5, 23, tzinfo=UTC)],
			"selected_candidate_id": [selected_candidate_id],
			"selected_candidate_index": [3],
			"selected_schedule_family": ["strict_reference"],
			"selected_regret_uah": [144.77],
			"selected_value_uah": [855.0],
			"v2_plus_candidate_id": [v2_plus_candidate_id],
			"v2_plus_regret_uah": [174.77],
			"v2_plus_value_uah": [825.0],
			"selected_minus_v2_plus_regret_uah": [-30.0],
			"selected_minus_v2_plus_value_uah": [30.0],
			"predicted_regret_delta_vs_v2_plus_uah": [-42.0],
			"predicted_improvement_vs_v2_plus_uah": [42.0],
			"abstained_to_v2_plus": [False],
			"abstention_reason": ["safe_switch_threshold_passed"],
			"family_tail_risk_probability": [0.34],
			"tail_risk_guard_passed": [True],
			"research_shadow_not_promotable": [True],
			"dt_lava_ready": [False],
			"promotion_gate_passed": [False],
			"market_execution_enabled": [False],
			"not_market_execution": [True],
		}
	).write_csv(tmp_path / "regret_aware_v2_plus_selector_selected_rows.csv")
	pl.DataFrame(
		{
			"tenant_id": ["client_003_dnipro_factory", "client_003_dnipro_factory"],
			"anchor_timestamp": [
				datetime(2026, 5, 5, 23, tzinfo=UTC),
				datetime(2026, 5, 5, 23, tzinfo=UTC),
			],
			"dt_candidate_id_target": [selected_candidate_id, v2_plus_candidate_id],
			"dt_schedule_family_target": ["strict_reference", "schedule_value_learner_v2_plus"],
			"candidate_family": ["strict_reference", "schedule_value_learner_v2_plus"],
			"candidate_model_name": ["strict_reference", "schedule_value_learner_v2_plus"],
			"horizon_hours": [3, 3],
			"forecast_price_uah_mwh_vector": [
				json.dumps([4300.0, 1400.0, 2400.0]),
				json.dumps([4300.0, 1400.0, 2400.0]),
			],
			"dispatch_mw_vector": [json.dumps([0.12, -0.05, 0.0]), json.dumps([0.10, -0.02, 0.0])],
			"soc_fraction_vector": [json.dumps([0.52, 0.47, 0.51, 0.51]), json.dumps([0.52, 0.48, 0.50, 0.50])],
			"schedule_value_uah": [855.0, 825.0],
			"decision_value_uah": [855.0, 825.0],
			"regret_uah": [144.77, 174.77],
			"regret_delta_vs_v2_plus_uah": [-30.0, 0.0],
			"oracle_value_uah": [999.77, 999.77],
			"safety_violation_count": [0, 0],
			"market_execution_enabled": [False, False],
			"market_execution_gate_passed": [False, False],
			"promotion_gate_passed": [False, False],
			"not_market_execution": [True, True],
			"not_deployed_dt_control": [True, True],
		}
	).write_csv(tmp_path / "regret_aware_v2_plus_selector_teacher_rows.csv")


def _write_shadow_augmented_gate_fixture(tmp_path: Path) -> None:
	pl.DataFrame(
		{
			"tenant_id": ["client_003_dnipro_factory"],
			"source_model_name": ["tft_official_global_panel_v1_horizon_quantile_calibrated_v1"],
			"anchor_timestamp": [datetime(2026, 4, 29, 23, tzinfo=UTC)],
			"evaluation_id": ["poland-shadow-diagnostic-candidate"],
			"selection_role": ["positive_not_promoted"],
			"regret_uah": [245.0],
			"decision_value_uah": [700.0],
			"oracle_value_uah": [945.0],
			"evaluation_payload": [
				json.dumps(
					{
						"horizon": [
							{
								"step_index": 0,
								"interval_start": "2026-04-30T00:00:00Z",
								"net_power_mw": 0.12,
								"forecast_price_uah_mwh": 4300.0,
							},
							{
								"step_index": 1,
								"interval_start": "2026-04-30T01:00:00Z",
								"net_power_mw": -0.05,
								"forecast_price_uah_mwh": 1400.0,
							},
							{
								"step_index": 2,
								"interval_start": "2026-04-30T02:00:00Z",
								"net_power_mw": 0.0,
								"forecast_price_uah_mwh": 2400.0,
							},
						]
					}
				)
			],
		}
	).write_csv(tmp_path / "tft_augmented_gate_rows.csv")


def _academic_mvp_readiness_packet() -> dict[str, Any]:
	return {
		"claim_scope": "credentialless_academic_mvp_readiness_not_market_execution",
		"generated_at": "2026-05-25T02:36:18+00:00",
		"academic_mvp_gate_passed": True,
		"operator_preview_gate": {
			"passed": True,
			"source_governance_label": "receipt-gated for market submission",
			"market_execution_enabled": False,
		},
		"source_governance": {
			"academic_mvp_source_governance_passed": True,
			"source_governance_evidence_status": (
				"public_credentialless_source_observed_receipt_not_verified"
			),
			"public_credentialless_source_observed": True,
			"credentialless_observation_count": 6,
			"candidate_receipt_source_found": False,
			"receipt_csv_generated": False,
			"publication_receipt_verified": False,
			"source_publication_timestamp_available": False,
			"market_availability_claim": False,
			"market_submission_receipt_gate_status": "blocked_external_access",
			"scmo_credentials_required_for_diploma_mvp": False,
			"market_execution_enabled": False,
		},
		"dt_lava_prototype_gate": {
			"passed_for_academic_mvp": True,
			"dt_lava_training_ready": False,
			"lava_npz_smoke_validation": {
				"configured": True,
				"claim_scope": "lava_npz_margin_smoke_packet_validation_not_market_execution",
				"validation_passed": True,
				"artifact_hashes_valid": True,
				"metrics_valid": True,
				"aggregate_valid": True,
				"npz_contract_valid": True,
				"baseline_comparison_valid": True,
				"baseline_comparison_ready": True,
				"promotion_gate": False,
				"permits_model_training": False,
				"market_execution_enabled": False,
			},
			"market_execution_enabled": False,
		},
		"dt_lava_teacher_contract_gate": {
			"passed_for_academic_mvp": True,
			"permitted_model_training_rows": 0,
			"target_label_space": "candidate_index_or_schedule_family",
			"teacher_packet_validation": {
				"configured": True,
				"claim_scope": "v13_dt_lava_teacher_packet_validation_not_market_execution",
				"passed": True,
				"candidate_schedule_teacher_contract_passed": True,
				"training_permission_consistency_passed": True,
				"promotion_execution_blocked_passed": True,
				"no_market_execution_passed": True,
				"market_execution_enabled": False,
			},
			"market_execution_enabled": False,
		},
		"offline_challenger_gate": {
			"passed_for_academic_mvp": True,
			"promotion_gate_passed": False,
			"offline_challenger_packet_validation": {
				"configured": True,
				"claim_scope": (
					"v13_dt_lava_offline_challenger_packet_validation_not_market_execution"
				),
				"passed": True,
				"strict_control_comparison_passed": True,
				"deterministic_safety_projection_passed": True,
				"non_promotion_execution_boundary_passed": True,
				"no_market_execution_passed": True,
				"market_execution_enabled": False,
			},
			"market_execution_enabled": False,
		},
		"dt_research_shadow_gate": {
			"passed_for_academic_mvp": True,
			"status": "passed_research_shadow_not_promotable",
			"claim_scope": "dt_research_shadow_not_promotable_not_market_execution",
			"available_teacher_rows": 3921,
			"train_selection_rows": 3741,
			"research_shadow_training_rows": 3741,
			"promotable_v13_permitted_training_rows": 0,
			"split_strategy": "chronological_delivery_timestamp",
			"chronological_split_passed": True,
			"publication_receipt_verified": False,
			"source_publication_timestamp_available": False,
			"market_availability_claim": False,
			"research_shadow_not_promotable": True,
			"dt_promotion_gate_passed": False,
			"market_execution_enabled": False,
		},
		"prototype_contract": {
			"claim_scope": "credentialless_dfl_dt_prototype_contract_not_market_execution",
			"product_boundary": "dam_delivery_day_operator_recommendation_preview",
			"dt_action_target_contract": "candidate_id_or_schedule_family",
			"v2_plus_role": "teacher_comparator_fallback",
			"raw_hourly_action_imitation": False,
			"evaluation_contract": {
				"required_controls_present": True,
				"behavior_cloning_control_present": True,
				"deterministic_safety_projection_passed": True,
				"market_execution_enabled": False,
			},
			"prototype_contract_gate_passed": True,
			"market_execution_enabled": False,
		},
		"prototype_evidence_scorecard": {
			"claim_scope": "credentialless_dfl_dt_prototype_evidence_scorecard_not_market_execution",
			"scorecard_passed_for_academic_mvp": True,
			"operator_bid_preview_rows": 24,
			"operator_bid_preview_has_buy_or_sell": True,
			"lava_npz_validation_passed": True,
			"lava_npz_baseline_comparison_ready": True,
			"teacher_rows": 3921,
			"teacher_train_selection_rows": 3741,
			"teacher_permitted_model_training_rows": 0,
			"dt_action_target_contract": "candidate_id_or_schedule_family",
			"offline_challenger_evidence_passed": True,
			"offline_challenger_promotion_gate_passed": False,
			"offline_challenger_decision": "blocked",
			"strict_v2_plus_behavior_cloning_controls_present": True,
			"deterministic_safety_projection_passed": True,
			"validation_tenant_anchor_count": 90,
			"best_observed_challenger_role": "offline_dt_reference",
			"best_observed_mean_regret_improvement_ratio_vs_v2_plus": 0.0,
			"v2_plus_role": "teacher_comparator_fallback",
			"market_submission_ready": False,
			"permits_model_training": False,
			"promotion_gate_passed": False,
			"market_execution_enabled": False,
		},
		"prototype_phase_readiness": {
			"claim_scope": "credentialless_dfl_dt_prototype_phase_readiness_not_market_execution",
			"phase_0_v13_source_readiness": {
				"status": "blocked_market_submission_receipts",
				"explicit_receipts_gate_passed": False,
				"safe_switch_floor_passed": True,
				"ready_for_training": False,
				"required_for_academic_mvp": False,
				"market_execution_enabled": False,
			},
			"phase_1_lava_npz_smoke": {
				"status": "passed_ci_smoke_not_promotion",
				"gate_passed": True,
				"market_execution_enabled": False,
			},
			"phase_2_v13_gated_teacher_contract": {
				"status": "passed_contract_training_rows_gated",
				"gate_passed": True,
				"permitted_model_training_rows": 0,
				"market_execution_enabled": False,
			},
			"phase_3_offline_challenger": {
				"status": "passed_non_promotion_evidence",
				"gate_passed_for_academic_mvp": True,
				"promotion_gate_passed": False,
				"market_execution_enabled": False,
			},
			"phase_4_full_schedule_dfl": {
				"status": "future_work_not_started",
				"gate_passed": False,
				"market_execution_enabled": False,
			},
			"market_execution_enabled": False,
		},
		"gate_passport": {
			"operator_preview_gate": {
				"passed": True,
				"status": "passed",
				"market_execution_enabled": False,
			},
			"dam_bid_recommendation_preview_gate": {
				"passed": True,
				"status": "passed",
				"market_execution_enabled": False,
			},
			"academic_source_governance_gate": {
				"passed": True,
				"status": "passed",
				"market_execution_enabled": False,
			},
			"market_submission_receipt_gate": {
				"passed": False,
				"status": "blocked_external_access",
				"required_for_academic_mvp": False,
				"market_execution_enabled": False,
			},
			"dt_lava_prototype_ci_smoke_gate": {
				"passed": True,
				"status": "passed",
				"market_execution_enabled": False,
			},
			"lava_npz_smoke_packet_validation_gate": {
				"passed": True,
				"status": "passed",
				"claim_scope": "lava_npz_margin_smoke_packet_validation_not_market_execution",
				"artifact_hashes_valid": True,
				"metrics_valid": True,
				"aggregate_valid": True,
				"npz_contract_valid": True,
				"baseline_comparison_valid": True,
				"permits_model_training": False,
				"promotion_gate_passed": False,
				"market_execution_enabled": False,
			},
			"dfl_dt_prototype_contract_gate": {
				"passed": True,
				"status": "passed",
				"market_execution_enabled": False,
			},
			"v13_gated_teacher_contract_gate": {
				"passed": True,
				"status": "passed",
				"teacher_packet_validation_passed": True,
				"permitted_model_training_rows": 0,
				"market_execution_enabled": False,
			},
			"offline_challenger_non_promotion_gate": {
				"passed": True,
				"status": "passed",
				"offline_challenger_packet_validation_passed": True,
				"promotion_gate_passed": False,
				"market_execution_enabled": False,
			},
			"dt_research_shadow_smoke_gate": {
				"passed": True,
				"status": "passed_research_shadow_not_promotable",
				"claim_scope": "dt_research_shadow_not_promotable_not_market_execution",
				"research_shadow_training_rows": 3741,
				"promotable_v13_permitted_training_rows": 0,
				"promotion_gate_passed": False,
				"market_execution_enabled": False,
			},
			"prototype_evidence_scorecard_gate": {
				"passed": True,
				"status": "passed",
				"claim_scope": "credentialless_dfl_dt_prototype_evidence_scorecard_not_market_execution",
				"operator_bid_preview_rows": 24,
				"teacher_train_selection_rows": 3741,
				"validation_tenant_anchor_count": 90,
				"permits_model_training": False,
				"promotion_gate_passed": False,
				"market_execution_enabled": False,
			},
			"dt_lava_training_promotion_gate": {
				"passed": False,
				"status": "blocked_until_v13_source_readiness",
				"required_for_academic_mvp": False,
				"market_execution_enabled": False,
			},
			"market_execution_safety_gate": {
				"passed": True,
				"status": "passed",
				"market_execution_enabled": False,
			},
			"market_execution_gate": {
				"passed": False,
				"status": "out_of_scope",
				"required_for_academic_mvp": False,
				"market_execution_enabled": False,
			},
		},
		"market_submission_ready": False,
		"market_execution_gate_passed": False,
		"promotion_gate_passed": False,
		"permits_model_training": False,
		"market_execution_enabled": False,
		"no_market_execution_safety_gate_passed": True,
		"next_gate": "credentialless_academic_mvp_ready_for_thesis_demo",
	}


def _academic_mvp_readiness_validation_packet() -> dict[str, Any]:
	return {
		"claim_scope": "credentialless_academic_mvp_readiness_validation_not_market_execution",
		"validated_at": "2026-05-25T04:39:48+00:00",
		"passed": True,
		"failures": [],
		"gate_results": {
			"academic_mvp_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"operator_preview_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"dam_bid_recommendation_preview_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"academic_source_governance_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"dt_lava_prototype_ci_smoke_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"dfl_dt_prototype_contract_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"v13_gated_teacher_contract_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"offline_challenger_non_promotion_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"prototype_evidence_scorecard_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"dt_research_shadow_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"market_execution_safety_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"market_submission_receipt_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"dt_lava_training_promotion_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"market_execution_gate": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"prototype_contract": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"prototype_phase_readiness": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"prototype_evidence_scorecard": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"lava_npz_smoke_packet_validation": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"teacher_packet_validation": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
			"offline_challenger_packet_validation": {
				"passed": True,
				"failures": [],
				"market_execution_enabled": False,
			},
		},
		"prototype_contract": {
			"claim_scope": "credentialless_dfl_dt_prototype_contract_not_market_execution",
			"dt_action_target_contract": "candidate_id_or_schedule_family",
			"prototype_contract_gate_passed": True,
			"market_execution_enabled": False,
		},
		"prototype_phase_readiness": {
			"claim_scope": "credentialless_dfl_dt_prototype_phase_readiness_not_market_execution",
			"phase_0_status": "blocked_market_submission_receipts",
			"phase_1_status": "passed_ci_smoke_not_promotion",
			"phase_2_status": "passed_contract_training_rows_gated",
			"phase_3_status": "passed_non_promotion_evidence",
			"phase_4_status": "future_work_not_started",
			"market_execution_enabled": False,
		},
		"prototype_evidence_scorecard": {
			"claim_scope": "credentialless_dfl_dt_prototype_evidence_scorecard_not_market_execution",
			"scorecard_passed_for_academic_mvp": True,
			"market_execution_enabled": False,
		},
		"market_execution_enabled": False,
	}


def test_academic_mvp_readiness_endpoint_exposes_non_execution_packet(
	client: TestClient,
	tmp_path,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	packet_path = tmp_path / "credentialless_academic_mvp_readiness_summary.json"
	packet_path.write_text(json.dumps(_academic_mvp_readiness_packet()), encoding="utf-8")
	validation_path = tmp_path / "credentialless_academic_mvp_readiness_validation.json"
	validation_path.write_text(
		json.dumps(_academic_mvp_readiness_validation_packet()),
		encoding="utf-8",
	)
	monkeypatch.setenv(api_main.ACADEMIC_MVP_PACKET_JSON_ENV, str(packet_path))

	response = client.get("/dashboard/academic-mvp-readiness")

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["academic_mvp_gate_passed"] is True
	assert response_payload["market_submission_ready"] is False
	assert response_payload["permits_model_training"] is False
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["operator_preview_gate"]["source_governance_label"] == (
		"receipt-gated for market submission"
	)
	assert response_payload["source_governance"]["market_submission_receipt_gate_status"] == (
		"blocked_external_access"
	)
	assert response_payload["source_governance"][
		"public_credentialless_source_observed"
	] is True
	assert response_payload["source_governance"]["publication_receipt_verified"] is False
	assert response_payload["source_governance"][
		"source_publication_timestamp_available"
	] is False
	assert response_payload["source_governance"]["market_availability_claim"] is False
	assert response_payload["dt_lava_teacher_contract_gate"]["permitted_model_training_rows"] == 0
	assert response_payload["prototype_contract"]["dt_action_target_contract"] == (
		"candidate_id_or_schedule_family"
	)
	assert response_payload["prototype_evidence_scorecard"][
		"scorecard_passed_for_academic_mvp"
	] is True
	assert response_payload["prototype_evidence_scorecard"][
		"teacher_permitted_model_training_rows"
	] == 0
	assert response_payload["prototype_phase_readiness"]["phase_0_v13_source_readiness"][
		"status"
	] == "blocked_market_submission_receipts"
	assert response_payload["prototype_phase_readiness"]["phase_3_offline_challenger"][
		"status"
	] == "passed_non_promotion_evidence"
	assert response_payload["prototype_phase_readiness"]["phase_4_full_schedule_dfl"][
		"status"
	] == "future_work_not_started"
	assert response_payload["gate_passport"]["dt_lava_prototype_ci_smoke_gate"]["passed"] is True
	assert response_payload["gate_passport"]["lava_npz_smoke_packet_validation_gate"][
		"passed"
	] is True
	assert response_payload["gate_passport"]["lava_npz_smoke_packet_validation_gate"][
		"permits_model_training"
	] is False
	assert response_payload["gate_passport"]["dfl_dt_prototype_contract_gate"]["passed"] is True
	assert response_payload["gate_passport"]["prototype_evidence_scorecard_gate"][
		"passed"
	] is True
	assert response_payload["gate_passport"]["prototype_evidence_scorecard_gate"][
		"teacher_train_selection_rows"
	] == 3741
	assert response_payload["dt_research_shadow_gate"]["research_shadow_training_rows"] == 3741
	assert response_payload["dt_research_shadow_gate"][
		"promotable_v13_permitted_training_rows"
	] == 0
	assert response_payload["dt_research_shadow_gate"]["split_strategy"] == (
		"chronological_delivery_timestamp"
	)
	assert response_payload["dt_research_shadow_gate"]["publication_receipt_verified"] is False
	assert response_payload["dt_research_shadow_gate"]["market_availability_claim"] is False
	assert response_payload["dt_research_shadow_gate"]["market_execution_enabled"] is False
	assert response_payload["gate_passport"]["dt_research_shadow_smoke_gate"]["passed"] is True
	assert response_payload["gate_passport"]["dt_research_shadow_smoke_gate"][
		"promotable_v13_permitted_training_rows"
	] == 0
	assert response_payload["gate_passport"]["market_execution_safety_gate"]["passed"] is True
	assert response_payload["gate_passport"]["market_submission_receipt_gate"]["status"] == (
		"blocked_external_access"
	)
	assert response_payload["gate_passport"]["market_submission_receipt_gate"][
		"required_for_academic_mvp"
	] is False
	assert response_payload["gate_passport"]["dt_lava_training_promotion_gate"]["status"] == (
		"blocked_until_v13_source_readiness"
	)
	assert response_payload["artifact_validation"]["passed"] is True
	assert response_payload["artifact_validation"]["gate_results"][
		"dfl_dt_prototype_contract_gate"
	]["passed"] is True
	assert response_payload["artifact_validation"]["gate_results"]["market_execution_gate"]["passed"] is True
	assert response_payload["artifact_validation"]["gate_results"][
		"prototype_phase_readiness"
	]["passed"] is True
	assert response_payload["artifact_validation"]["gate_results"][
		"prototype_evidence_scorecard"
	]["passed"] is True
	assert response_payload["artifact_validation"]["market_execution_enabled"] is False
	assert response_payload["artifact_validation_packet_path"] == str(validation_path)
	assert response_payload["source_packet_path"] == str(packet_path)


def test_academic_mvp_readiness_endpoint_rejects_failed_artifact_validation(
	client: TestClient,
	tmp_path,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	packet_path = tmp_path / "credentialless_academic_mvp_readiness_summary.json"
	packet_path.write_text(json.dumps(_academic_mvp_readiness_packet()), encoding="utf-8")
	validation_packet = _academic_mvp_readiness_validation_packet()
	validation_packet["passed"] = False
	validation_packet["failures"] = ["dfl_dt_prototype_contract_gate:not_passed"]
	validation_path = tmp_path / "credentialless_academic_mvp_readiness_validation.json"
	validation_path.write_text(json.dumps(validation_packet), encoding="utf-8")
	monkeypatch.setenv(api_main.ACADEMIC_MVP_PACKET_JSON_ENV, str(packet_path))

	response = client.get("/dashboard/academic-mvp-readiness")

	assert response.status_code == 500
	assert "validation artifact" in response.json()["detail"]


def test_academic_mvp_readiness_endpoint_rejects_missing_phase_validation_gate(
	client: TestClient,
	tmp_path,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	packet_path = tmp_path / "credentialless_academic_mvp_readiness_summary.json"
	packet_path.write_text(json.dumps(_academic_mvp_readiness_packet()), encoding="utf-8")
	validation_packet = _academic_mvp_readiness_validation_packet()
	validation_packet["gate_results"].pop("prototype_phase_readiness")
	validation_path = tmp_path / "credentialless_academic_mvp_readiness_validation.json"
	validation_path.write_text(json.dumps(validation_packet), encoding="utf-8")
	monkeypatch.setenv(api_main.ACADEMIC_MVP_PACKET_JSON_ENV, str(packet_path))

	response = client.get("/dashboard/academic-mvp-readiness")

	assert response.status_code == 500
	assert "prototype_phase_readiness" in response.json()["detail"]


def test_academic_mvp_readiness_endpoint_rejects_failed_required_passport_gate(
	client: TestClient,
	tmp_path,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	packet = _academic_mvp_readiness_packet()
	packet["gate_passport"]["v13_gated_teacher_contract_gate"]["passed"] = False
	packet_path = tmp_path / "credentialless_academic_mvp_readiness_summary.json"
	packet_path.write_text(json.dumps(packet), encoding="utf-8")
	monkeypatch.setenv(api_main.ACADEMIC_MVP_PACKET_JSON_ENV, str(packet_path))

	response = client.get("/dashboard/academic-mvp-readiness")

	assert response.status_code == 500
	assert "v13_gated_teacher_contract_gate" in response.json()["detail"]


def test_academic_mvp_readiness_endpoint_rejects_missing_scorecard_passport_gate(
	client: TestClient,
	tmp_path,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	packet = _academic_mvp_readiness_packet()
	packet["gate_passport"].pop("prototype_evidence_scorecard_gate")
	packet_path = tmp_path / "credentialless_academic_mvp_readiness_summary.json"
	packet_path.write_text(json.dumps(packet), encoding="utf-8")
	validation_path = tmp_path / "credentialless_academic_mvp_readiness_validation.json"
	validation_path.write_text(
		json.dumps(_academic_mvp_readiness_validation_packet()),
		encoding="utf-8",
	)
	monkeypatch.setenv(api_main.ACADEMIC_MVP_PACKET_JSON_ENV, str(packet_path))

	response = client.get("/dashboard/academic-mvp-readiness")

	assert response.status_code == 500
	assert "prototype_evidence_scorecard_gate" in response.json()["detail"]


def test_academic_mvp_readiness_endpoint_rejects_missing_dt_shadow_gate(
	client: TestClient,
	tmp_path,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	packet = _academic_mvp_readiness_packet()
	packet.pop("dt_research_shadow_gate")
	packet["gate_passport"].pop("dt_research_shadow_smoke_gate")
	packet_path = tmp_path / "credentialless_academic_mvp_readiness_summary.json"
	packet_path.write_text(json.dumps(packet), encoding="utf-8")
	validation_path = tmp_path / "credentialless_academic_mvp_readiness_validation.json"
	validation_path.write_text(
		json.dumps(_academic_mvp_readiness_validation_packet()),
		encoding="utf-8",
	)
	monkeypatch.setenv(api_main.ACADEMIC_MVP_PACKET_JSON_ENV, str(packet_path))

	response = client.get("/dashboard/academic-mvp-readiness")

	assert response.status_code == 500
	assert "dt_research_shadow" in response.json()["detail"]


def test_operator_recommendation_exposes_v2_plus_offline_strategy(
	client: TestClient,
) -> None:
	strict_response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "strict_similar_day"},
	)
	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "schedule_value_learner_v2_plus"},
	)

	assert strict_response.status_code == 200
	assert response.status_code == 200
	strict_payload = strict_response.json()
	response_payload = response.json()
	assert response_payload["selected_strategy_id"] == "schedule_value_learner_v2_plus"
	assert response_payload["policy_mode"] == "official_oree_dam_row_lp_preview"
	assert response_payload["selected_policy_id"] == "schedule_value_learner_v2_plus"
	assert "official oree published dam prices" in response_payload["policy_explanation"].lower()
	assert "offline/research context" in response_payload["policy_explanation"].lower()
	assert "strict fallback" not in response_payload["policy_explanation"].lower()
	assert "V2+" in response_payload["forecast_source"]
	assert "research evidence" in response_payload["forecast_source"].lower()
	assert [
		point["forecast_price_uah_mwh"]
		for point in response_payload["recommendation_schedule"]
	] == [
		point["forecast_price_uah_mwh"]
		for point in strict_payload["recommendation_schedule"]
	]
	v2_plus_option = next(
		strategy
		for strategy in response_payload["available_strategies"]
		if strategy["strategy_id"] == "schedule_value_learner_v2_plus"
	)
	assert v2_plus_option["enabled"] is True
	assert v2_plus_option["mean_regret_uah"] == pytest.approx(174.77)


def test_operator_recommendation_reports_dt_forecast_context_when_selected(
	client: TestClient,
	fake_simulated_trade_store: InMemorySimulatedTradeStore,
) -> None:
	fake_simulated_trade_store.upsert_decision_transformer_policy_preview_frame(
		pl.DataFrame(
			{
				"policy_run_id": ["dt-run-001", "dt-run-001"],
				"created_at": [
					datetime(2026, 5, 5, 12, tzinfo=UTC),
					datetime(2026, 5, 5, 12, tzinfo=UTC),
				],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"episode_id": ["episode-001", "episode-001"],
				"step_index": [0, 1],
				"interval_start": [
					datetime(2026, 5, 5, 0, tzinfo=UTC),
					datetime(2026, 5, 5, 1, tzinfo=UTC),
				],
				"state_nbeatsx_forecast_uah_mwh": [4100.0, 1700.0],
				"state_tft_forecast_uah_mwh": [4350.0, 1550.0],
				"value_gap_uah": [134.0, 714.0],
				"constraint_violation": [False, False],
				"readiness_status": [
					"ready_for_operator_preview",
					"ready_for_operator_preview",
				],
			}
		)
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "decision_transformer"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["selected_strategy_id"] == "strict_similar_day"
	assert response_payload["policy_mode"] == "official_oree_dam_row_lp_preview"
	assert response_payload["v13_readiness"]["dt_lava_ready"] is False
	assert response_payload["v13_readiness"]["gate_status"] == "data_acquisition_needed"
	dt_option = next(
		strategy
		for strategy in response_payload["available_strategies"]
		if strategy["strategy_id"] == "decision_transformer"
	)
	assert dt_option["enabled"] is False
	assert "V13 acquisition/source-readiness gate" in dt_option["reason"]
	assert any("Requested strategy decision_transformer is unavailable" in warning for warning in response_payload["readiness_warnings"])


def test_baseline_lp_preview_returns_tenant_aware_recommendation_read_model(
	client: TestClient,
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
	response = client.get(
		"/dashboard/baseline-lp-preview",
		params={
			"tenant_id": "client_003_dnipro_factory",
		},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["market_venue"] == "DAM"
	assert response_payload["interval_minutes"] == 60
	assert response_payload["market_scope"] == "dam_hourly_planning_preview"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["read_model_boundary"] == "operator_preview_no_market_submission"
	assert response_payload["market_gate_status"] == "not_evaluated_preview_only"
	assert response_payload["bid_eligibility_status"] == "not_applicable_no_proposed_bid"
	assert response_payload["proposed_bid_status"] == "not_emitted_operator_preview"
	assert response_payload["starting_soc_fraction"] == pytest.approx(0.5)
	assert response_payload["starting_soc_source"] == "tenant_default"
	assert response_payload["battery_metrics"]["capacity_mwh"] == pytest.approx(0.5)
	assert response_payload["battery_metrics"]["max_power_mw"] == pytest.approx(0.25)
	assert len(response_payload["forecast"]) == 24
	assert len(response_payload["recommendation_schedule"]) == 24
	assert len(response_payload["projected_state"]["trace"]) == 24
	assert "committed_dispatch" not in response_payload
	assert "proposed_bid" not in response_payload
	target_start = datetime.fromisoformat(response_payload["target_delivery_window_start"])
	target_end = datetime.fromisoformat(response_payload["target_delivery_window_end"])
	anchor_timestamp = datetime.fromisoformat(response_payload["anchor_timestamp"])
	first_interval = datetime.fromisoformat(response_payload["recommendation_schedule"][0]["interval_start"])
	last_interval = datetime.fromisoformat(response_payload["recommendation_schedule"][-1]["interval_start"])
	assert response_payload["forecast_generated_at"] is None
	assert target_start == first_interval
	assert target_end == last_interval + timedelta(hours=1)
	assert anchor_timestamp < target_start
	assert target_start.date() == (anchor_timestamp + timedelta(days=1)).date()
	assert target_start.hour == 0
	assert target_start.minute == 0
	assert target_start.second == 0
	assert {
		datetime.fromisoformat(point["interval_start"]).date()
		for point in response_payload["recommendation_schedule"]
	} == {target_start.date()}
	assert response_payload["economics"]["total_degradation_penalty_uah"] >= 0.0
	assert response_payload["economics"]["total_gross_market_value_uah"] != 0.0
	assert max(
		abs(point["recommended_net_power_mw"])
		for point in response_payload["recommendation_schedule"]
	) <= response_payload["battery_metrics"]["max_power_mw"] + 1e-6
	assert all(
		point["degradation_penalty_uah"]
		== pytest.approx(
			point["throughput_mwh"]
			* (
				response_payload["battery_metrics"]["degradation_cost_per_cycle_uah"]
				/ (2.0 * response_payload["battery_metrics"]["capacity_mwh"])
			)
		)
		for point in response_payload["recommendation_schedule"]
	)
	status_record = fake_status_store.get_status(
		tenant_id="client_003_dnipro_factory",
		flow_type=OperatorFlowType.BASELINE_LP,
	)
	assert status_record is not None
	assert status_record.status == OperatorFlowStatus.COMPLETED


def test_forecast_strategy_comparison_endpoint_returns_latest_gold_rows(
	client: TestClient,
	fake_strategy_evaluation_store: InMemoryStrategyEvaluationStore,
) -> None:
	fake_strategy_evaluation_store.upsert_evaluation_frame(
		pl.DataFrame(
			{
				"evaluation_id": ["eval-001", "eval-001", "eval-001"],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"forecast_model_name": [
					"strict_similar_day",
					"nbeatsx_silver_v0",
					"tft_silver_v0",
				],
				"strategy_kind": [
					"forecast_driven_lp",
					"forecast_driven_lp",
					"forecast_driven_lp",
				],
				"market_venue": ["DAM", "DAM", "DAM"],
				"anchor_timestamp": [
					datetime(2026, 5, 4, 20, tzinfo=UTC)
					for _ in range(3)
				],
				"generated_at": [
					datetime(2026, 5, 4, 20, 30, tzinfo=UTC)
					for _ in range(3)
				],
				"horizon_hours": [24, 24, 24],
				"starting_soc_fraction": [0.5, 0.5, 0.5],
				"starting_soc_source": [
					"tenant_default",
					"tenant_default",
					"tenant_default",
				],
				"decision_value_uah": [110.0, 125.0, 120.0],
				"forecast_objective_value_uah": [105.0, 124.0, 119.0],
				"oracle_value_uah": [130.0, 130.0, 130.0],
				"regret_uah": [20.0, 5.0, 10.0],
				"regret_ratio": [0.1538, 0.0385, 0.0769],
				"total_degradation_penalty_uah": [9.0, 10.0, 10.0],
				"total_throughput_mwh": [0.2, 0.25, 0.24],
				"committed_action": ["HOLD", "DISCHARGE", "DISCHARGE"],
				"committed_power_mw": [0.0, 0.12, 0.08],
				"rank_by_regret": [3, 1, 2],
				"evaluation_payload": [
					{"scope": "test"},
					{"scope": "test"},
					{"scope": "test"},
				],
			}
		)
	)

	response = client.get(
		"/dashboard/forecast-strategy-comparison",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["market_venue"] == "DAM"
	assert response_payload["evaluation_id"] == "eval-001"
	assert [row["forecast_model_name"] for row in response_payload["comparisons"]] == [
		"nbeatsx_silver_v0",
		"tft_silver_v0",
		"strict_similar_day",
	]
	assert response_payload["comparisons"][0]["rank_by_regret"] == 1
	assert "proposed_bid" not in response_payload
	assert "cleared_trade" not in response_payload
	assert "dispatch_command" not in response_payload


def test_real_data_benchmark_endpoint_returns_latest_summary_and_rows(
	client: TestClient,
	fake_strategy_evaluation_store: InMemoryStrategyEvaluationStore,
) -> None:
	generated_at = datetime(2026, 5, 4, 20, 30, tzinfo=UTC)
	fake_strategy_evaluation_store.upsert_evaluation_frame(
		pl.DataFrame(
			{
				"evaluation_id": ["bench-001", "bench-001", "bench-001"],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"forecast_model_name": [
					"strict_similar_day",
					"nbeatsx_silver_v0",
					"tft_silver_v0",
				],
				"strategy_kind": [
					"real_data_rolling_origin_benchmark",
					"real_data_rolling_origin_benchmark",
					"real_data_rolling_origin_benchmark",
				],
				"market_venue": ["DAM", "DAM", "DAM"],
				"anchor_timestamp": [
					datetime(2026, 5, 3, 20, tzinfo=UTC)
					for _ in range(3)
				],
				"generated_at": [generated_at for _ in range(3)],
				"horizon_hours": [24, 24, 24],
				"starting_soc_fraction": [0.5, 0.5, 0.5],
				"starting_soc_source": [
					"tenant_default",
					"tenant_default",
					"tenant_default",
				],
				"decision_value_uah": [110.0, 125.0, 120.0],
				"forecast_objective_value_uah": [105.0, 124.0, 119.0],
				"oracle_value_uah": [130.0, 130.0, 130.0],
				"regret_uah": [20.0, 5.0, 10.0],
				"regret_ratio": [0.1538, 0.0385, 0.0769],
				"total_degradation_penalty_uah": [9.0, 10.0, 10.0],
				"total_throughput_mwh": [0.2, 0.25, 0.24],
				"committed_action": ["HOLD", "DISCHARGE", "DISCHARGE"],
				"committed_power_mw": [0.0, 0.12, 0.08],
				"rank_by_regret": [3, 1, 2],
				"evaluation_payload": [
					{"data_quality_tier": "thesis_grade", "benchmark_kind": "real_data_rolling_origin"},
					{"data_quality_tier": "thesis_grade", "benchmark_kind": "real_data_rolling_origin"},
					{"data_quality_tier": "thesis_grade", "benchmark_kind": "real_data_rolling_origin"},
				],
			}
		)
	)

	response = client.get(
		"/dashboard/real-data-benchmark",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["data_quality_tier"] == "thesis_grade"
	assert response_payload["anchor_count"] == 1
	assert response_payload["model_count"] == 3
	assert response_payload["best_model_name"] == "nbeatsx_silver_v0"
	assert response_payload["mean_regret_uah"] == pytest.approx(35.0 / 3.0)
	assert [row["forecast_model_name"] for row in response_payload["rows"]] == [
		"nbeatsx_silver_v0",
		"tft_silver_v0",
		"strict_similar_day",
	]


def test_future_stack_preview_returns_nbeatsx_and_tft_series(
	client: TestClient,
	fake_strategy_evaluation_store: InMemoryStrategyEvaluationStore,
) -> None:
	generated_at = datetime(2026, 5, 4, 20, 30, tzinfo=UTC)
	anchor_timestamp = datetime(2026, 5, 3, 20, tzinfo=UTC)
	horizon = [
		{
			"step_index": 0,
			"interval_start": "2026-05-03T21:00:00+00:00",
			"forecast_price_uah_mwh": 1000.0,
			"actual_price_uah_mwh": 1010.0,
			"net_power_mw": -0.1,
		},
		{
			"step_index": 1,
			"interval_start": "2026-05-03T22:00:00+00:00",
			"forecast_price_uah_mwh": 1400.0,
			"actual_price_uah_mwh": 1395.0,
			"net_power_mw": 0.1,
		},
	]
	fake_strategy_evaluation_store.upsert_evaluation_frame(
		pl.DataFrame(
			{
				"evaluation_id": ["bench-001", "bench-001"],
				"tenant_id": ["client_003_dnipro_factory", "client_003_dnipro_factory"],
				"forecast_model_name": ["nbeatsx_silver_v0", "tft_silver_v0"],
				"strategy_kind": [
					"real_data_rolling_origin_benchmark",
					"real_data_rolling_origin_benchmark",
				],
				"market_venue": ["DAM", "DAM"],
				"anchor_timestamp": [anchor_timestamp, anchor_timestamp],
				"generated_at": [generated_at, generated_at],
				"horizon_hours": [24, 24],
				"starting_soc_fraction": [0.5, 0.5],
				"starting_soc_source": ["tenant_default", "tenant_default"],
				"decision_value_uah": [120.0, 125.0],
				"forecast_objective_value_uah": [119.0, 124.0],
				"oracle_value_uah": [130.0, 130.0],
				"regret_uah": [10.0, 5.0],
				"regret_ratio": [0.0769, 0.0385],
				"total_degradation_penalty_uah": [10.0, 10.0],
				"total_throughput_mwh": [0.25, 0.24],
				"committed_action": ["DISCHARGE", "DISCHARGE"],
				"committed_power_mw": [0.08, 0.08],
				"rank_by_regret": [2, 1],
				"evaluation_payload": [
					{
						"data_quality_tier": "thesis_grade",
						"horizon": horizon,
						"forecast_diagnostics": {"mae_uah_mwh": 8.0},
					},
					{
						"data_quality_tier": "thesis_grade",
						"horizon": horizon,
						"forecast_diagnostics": {
							"mae_uah_mwh": 5.0,
							"pinball_loss_p10_uah_mwh": 4.0,
							"pinball_loss_p50_uah_mwh": 3.0,
							"pinball_loss_p90_uah_mwh": 4.5,
						},
					},
				],
			}
		)
	)

	response = client.get(
		"/dashboard/future-stack-preview",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["backend_status"]["neuralforecast"] in {"available", "dependency_missing"}
	assert {series["model_name"] for series in response_payload["forecast_series"]} == {
		"nbeatsx_silver_v0",
		"tft_silver_v0",
	}
	assert len(response_payload["forecast_series"][0]["points"]) == 2
	assert response_payload["forecast_series"][1]["uncertainty_kind"] == "quantile_proxy"
	assert response_payload["runtime_acceleration"]["device_type"] in {"cpu", "cuda", "mps"}
	assert "torch" in response_payload["runtime_acceleration"]["backend"]
	assert response_payload["runtime_acceleration"]["recommended_scope"]


def test_future_stack_preview_prefers_persisted_forecast_store_rows(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	start = datetime(2026, 5, 4, 18, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=pl.DataFrame(
			{
				"forecast_timestamp": [start, start + timedelta(hours=1)],
				"predicted_price_uah_mwh": [4200.0, 4300.0],
				"predicted_price_p50_uah_mwh": [4200.0, 4300.0],
				"adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * 2,
			}
		),
		point_prediction_column="predicted_price_uah_mwh",
	)
	fake_forecast_store.upsert_forecast_run(
		model_name="tft_official_v0",
		forecast_frame=pl.DataFrame(
			{
				"forecast_timestamp": [start, start + timedelta(hours=1)],
				"predicted_price_uah_mwh": [4100.0, 4400.0],
				"predicted_price_p10_uah_mwh": [3900.0, 4200.0],
				"predicted_price_p50_uah_mwh": [4100.0, 4400.0],
				"predicted_price_p90_uah_mwh": [4300.0, 4600.0],
				"adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * 2,
			}
		),
		point_prediction_column="predicted_price_p50_uah_mwh",
	)

	response = client.get(
		"/dashboard/future-stack-preview",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["selected_forecast_model"] == "nbeatsx_official_v0"
	assert response_payload["forecast_window_start"] == "2026-05-04T18:00:00Z"
	assert response_payload["forecast_window_end"] == "2026-05-04T19:00:00Z"
	assert {series["model_name"] for series in response_payload["forecast_series"]} == {
		"nbeatsx_official_v0",
		"tft_official_v0",
	}
	official_tft = next(series for series in response_payload["forecast_series"] if series["model_name"] == "tft_official_v0")
	assert official_tft["source_status"] == "official"
	assert official_tft["uncertainty_kind"] == "quantile"
	assert official_tft["points"][0]["p10_price_uah_mwh"] == pytest.approx(3900.0)
	assert official_tft["out_of_dam_cap_rows"] == 0
	assert official_tft["quality_boundary"] == "smoke_values_inside_dam_cap_not_value_claim"


def test_future_stack_preview_flags_out_of_cap_official_forecast_rows(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	start = datetime(2026, 5, 4, 18, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=pl.DataFrame(
			{
				"forecast_timestamp": [start, start + timedelta(hours=1)],
				"predicted_price_uah_mwh": [-25.0, 52_000.0],
				"predicted_price_p50_uah_mwh": [-25.0, 52_000.0],
			}
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	response = client.get(
		"/dashboard/future-stack-preview",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	official_nbeatsx = response.json()["forecast_series"][0]
	assert official_nbeatsx["model_name"] == "nbeatsx_official_v0"
	assert official_nbeatsx["out_of_dam_cap_rows"] == 2
	assert official_nbeatsx["quality_boundary"] == "needs_calibration_before_value_claim"
	assert [point["price_cap_status"] for point in official_nbeatsx["points"]] == [
		"below_dam_cap",
		"above_dam_cap",
	]


def test_operator_recommendation_uses_persisted_nbeatsx_tft_forecast_series(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	start = datetime(2026, 5, 4, 18, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=pl.DataFrame(
			{
				"forecast_timestamp": [start],
				"predicted_price_uah_mwh": [4200.0],
				"predicted_price_p50_uah_mwh": [4200.0],
			}
		),
		point_prediction_column="predicted_price_uah_mwh",
	)
	fake_forecast_store.upsert_forecast_run(
		model_name="tft_official_v0",
		forecast_frame=pl.DataFrame(
			{
				"forecast_timestamp": [start],
				"predicted_price_uah_mwh": [4100.0],
				"predicted_price_p10_uah_mwh": [3900.0],
				"predicted_price_p50_uah_mwh": [4100.0],
				"predicted_price_p90_uah_mwh": [4300.0],
			}
		),
		point_prediction_column="predicted_price_p50_uah_mwh",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "strict_similar_day"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert {series["model_name"] for series in response_payload["forecast_model_series"]} == {
		"nbeatsx_official_v0",
		"tft_official_v0",
	}
	assert response_payload["forecast_model_series"][0]["source_status"] == "official"
	assert response_payload["forecast_model_series"][0]["points"][0]["forecast_price_uah_mwh"] == pytest.approx(4200.0)


def test_operator_recommendation_routes_selected_official_forecast_into_lp_preview(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	start = datetime(2026, 5, 4, 18, tzinfo=UTC)
	forecast_prices = [1800.0, 5200.0, 1900.0, 5300.0]
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=pl.DataFrame(
			{
				"forecast_timestamp": [
					start + timedelta(hours=index)
					for index in range(len(forecast_prices))
				],
				"predicted_price_uah_mwh": forecast_prices,
				"predicted_price_p50_uah_mwh": forecast_prices,
				"adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * len(forecast_prices),
			}
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "nbeatsx_official_v0"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["selected_strategy_id"] == "nbeatsx_official_v0"
	assert response_payload["policy_mode"] == "official_oree_dam_row_lp_preview"
	assert response_payload["forecast_source"] == (
		"Official OREE published DAM delivery row routed through Level 1 LP preview; "
		"NBEATSx remains forecast evidence"
	)
	assert response_payload["forecast_generated_at"] is not None
	assert response_payload["recommendation_schedule"][0]["forecast_price_uah_mwh"] == pytest.approx(2220.0)
	assert response_payload["recommendation_schedule"][1]["forecast_price_uah_mwh"] == pytest.approx(2262.0)
	assert response_payload["forecast_model_series"][0]["points"][0]["forecast_price_uah_mwh"] == pytest.approx(1800.0)


def test_operator_recommendation_blocks_out_of_cap_official_forecast_from_lp_preview(
	client: TestClient,
	fake_forecast_store: InMemoryForecastStore,
) -> None:
	start = datetime(2026, 5, 4, 18, tzinfo=UTC)
	fake_forecast_store.upsert_forecast_run(
		model_name="nbeatsx_official_v0",
		forecast_frame=pl.DataFrame(
			{
				"forecast_timestamp": [start, start + timedelta(hours=1)],
				"predicted_price_uah_mwh": [4200.0, 19_500.0],
				"predicted_price_p50_uah_mwh": [4200.0, 19_500.0],
				"adapter_scope": [
					"official_backend_forecast_candidate_not_live_strategy",
					"official_backend_forecast_candidate_not_live_strategy",
				],
			}
		),
		point_prediction_column="predicted_price_uah_mwh",
	)

	response = client.get(
		"/dashboard/operator-recommendation",
		params={"tenant_id": "client_003_dnipro_factory", "strategy_id": "nbeatsx_official_v0"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	nbeatsx_option = next(
		option
		for option in response_payload["available_strategies"]
		if option["strategy_id"] == "nbeatsx_official_v0"
	)
	assert nbeatsx_option["enabled"] is False
	assert nbeatsx_option["reason"] == "official forecast rows need calibration: 1 out-of-cap rows"
	assert response_payload["selected_strategy_id"] == "strict_similar_day"
	assert response_payload["policy_mode"] == "official_oree_dam_row_lp_preview"
	assert any(
		"Requested strategy nbeatsx_official_v0 is unavailable" in warning
		for warning in response_payload["readiness_warnings"]
	)
	assert response_payload["forecast_model_series"][0]["out_of_dam_cap_rows"] == 1


def test_calibrated_ensemble_benchmark_endpoint_returns_latest_gate_rows(
	client: TestClient,
	fake_strategy_evaluation_store: InMemoryStrategyEvaluationStore,
) -> None:
	generated_at = datetime(2026, 5, 4, 20, 30, tzinfo=UTC)
	fake_strategy_evaluation_store.upsert_evaluation_frame(
		pl.DataFrame(
			{
				"evaluation_id": ["calibrated-gate-001", "calibrated-gate-002"],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"forecast_model_name": [
					"calibrated_value_aware_ensemble_v0",
					"calibrated_value_aware_ensemble_v0",
				],
				"strategy_kind": [
					"calibrated_value_aware_ensemble_gate",
					"calibrated_value_aware_ensemble_gate",
				],
				"market_venue": ["DAM", "DAM"],
				"anchor_timestamp": [
					datetime(2026, 5, 3, 20, tzinfo=UTC),
					datetime(2026, 5, 4, 20, tzinfo=UTC),
				],
				"generated_at": [generated_at, generated_at],
				"horizon_hours": [24, 24],
				"starting_soc_fraction": [0.5, 0.5],
				"starting_soc_source": ["tenant_default", "tenant_default"],
				"decision_value_uah": [120.0, 110.0],
				"forecast_objective_value_uah": [119.0, 109.0],
				"oracle_value_uah": [130.0, 130.0],
				"regret_uah": [10.0, 20.0],
				"regret_ratio": [0.0769, 0.1538],
				"total_degradation_penalty_uah": [10.0, 9.0],
				"total_throughput_mwh": [0.25, 0.2],
				"committed_action": ["DISCHARGE", "HOLD"],
				"committed_power_mw": [0.08, 0.0],
				"rank_by_regret": [1, 1],
				"evaluation_payload": [
					{
						"data_quality_tier": "thesis_grade",
						"selected_model_name": "tft_horizon_regret_weighted_calibrated_v0",
					},
					{
						"data_quality_tier": "thesis_grade",
						"selected_model_name": "strict_similar_day",
					},
				],
			}
		)
	)

	response = client.get(
		"/dashboard/calibrated-ensemble-benchmark",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["data_quality_tier"] == "thesis_grade"
	assert response_payload["anchor_count"] == 2
	assert response_payload["model_count"] == 1
	assert response_payload["best_model_name"] == "calibrated_value_aware_ensemble_v0"
	assert response_payload["mean_regret_uah"] == pytest.approx(15.0)
	assert [row["evaluation_payload"]["selected_model_name"] for row in response_payload["rows"]] == [
		"tft_horizon_regret_weighted_calibrated_v0",
		"strict_similar_day",
	]


def test_risk_adjusted_value_gate_endpoint_returns_latest_gate_rows(
	client: TestClient,
	fake_strategy_evaluation_store: InMemoryStrategyEvaluationStore,
) -> None:
	generated_at = datetime(2026, 5, 4, 20, 30, tzinfo=UTC)
	fake_strategy_evaluation_store.upsert_evaluation_frame(
		pl.DataFrame(
			{
				"evaluation_id": ["risk-gate-001", "risk-gate-002"],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"forecast_model_name": [
					"risk_adjusted_value_gate_v0",
					"risk_adjusted_value_gate_v0",
				],
				"strategy_kind": [
					"risk_adjusted_value_gate",
					"risk_adjusted_value_gate",
				],
				"market_venue": ["DAM", "DAM"],
				"anchor_timestamp": [
					datetime(2026, 5, 3, 20, tzinfo=UTC),
					datetime(2026, 5, 4, 20, tzinfo=UTC),
				],
				"generated_at": [generated_at, generated_at],
				"horizon_hours": [24, 24],
				"starting_soc_fraction": [0.5, 0.5],
				"starting_soc_source": ["tenant_default", "tenant_default"],
				"decision_value_uah": [118.0, 116.0],
				"forecast_objective_value_uah": [117.0, 115.0],
				"oracle_value_uah": [130.0, 130.0],
				"regret_uah": [12.0, 14.0],
				"regret_ratio": [0.0923, 0.1077],
				"total_degradation_penalty_uah": [10.0, 9.5],
				"total_throughput_mwh": [0.25, 0.22],
				"committed_action": ["DISCHARGE", "HOLD"],
				"committed_power_mw": [0.08, 0.0],
				"rank_by_regret": [1, 1],
				"evaluation_payload": [
					{
						"data_quality_tier": "thesis_grade",
						"selected_model_name": "strict_similar_day",
						"selection_policy": "risk_adjusted_prior_anchor_regret_tail_and_win_rate",
					},
					{
						"data_quality_tier": "thesis_grade",
						"selected_model_name": "tft_horizon_regret_weighted_calibrated_v0",
						"selection_policy": "risk_adjusted_prior_anchor_regret_tail_and_win_rate",
					},
				],
			}
		)
	)

	response = client.get(
		"/dashboard/risk-adjusted-value-gate",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["data_quality_tier"] == "thesis_grade"
	assert response_payload["anchor_count"] == 2
	assert response_payload["model_count"] == 1
	assert response_payload["best_model_name"] == "risk_adjusted_value_gate_v0"
	assert response_payload["mean_regret_uah"] == pytest.approx(13.0)
	assert [row["evaluation_payload"]["selected_model_name"] for row in response_payload["rows"]] == [
		"strict_similar_day",
		"tft_horizon_regret_weighted_calibrated_v0",
	]


def test_forecast_dispatch_sensitivity_endpoint_returns_diagnostic_buckets(
	client: TestClient,
	fake_strategy_evaluation_store: InMemoryStrategyEvaluationStore,
) -> None:
	generated_at = datetime(2026, 5, 4, 20, 30, tzinfo=UTC)
	anchor_timestamp = datetime(2026, 5, 4, 20, tzinfo=UTC)
	fake_strategy_evaluation_store.upsert_evaluation_frame(
		pl.DataFrame(
			{
				"evaluation_id": ["sensitivity-001", "sensitivity-002"],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"forecast_model_name": [
					"strict_similar_day",
					"tft_horizon_regret_weighted_calibrated_v0",
				],
				"strategy_kind": [
					"horizon_regret_weighted_forecast_calibration_benchmark",
					"horizon_regret_weighted_forecast_calibration_benchmark",
				],
				"market_venue": ["DAM", "DAM"],
				"anchor_timestamp": [anchor_timestamp, anchor_timestamp],
				"generated_at": [generated_at, generated_at],
				"horizon_hours": [2, 2],
				"starting_soc_fraction": [0.5, 0.5],
				"starting_soc_source": ["tenant_default", "tenant_default"],
				"decision_value_uah": [118.0, 116.0],
				"forecast_objective_value_uah": [117.0, 115.0],
				"oracle_value_uah": [130.0, 130.0],
				"regret_uah": [12.0, 620.0],
				"regret_ratio": [0.0923, 4.7692],
				"total_degradation_penalty_uah": [10.0, 9.5],
				"total_throughput_mwh": [0.25, 0.22],
				"committed_action": ["DISCHARGE", "DISCHARGE"],
				"committed_power_mw": [0.08, 0.08],
				"rank_by_regret": [1, 2],
				"evaluation_payload": [
					{
						"data_quality_tier": "thesis_grade",
						"forecast_diagnostics": {"mae_uah_mwh": 50.0, "rmse_uah_mwh": 55.0},
						"horizon": [
							{
								"step_index": 0,
								"forecast_price_uah_mwh": 1000.0,
								"actual_price_uah_mwh": 1010.0,
								"net_power_mw": -0.1,
							},
							{
								"step_index": 1,
								"forecast_price_uah_mwh": 1400.0,
								"actual_price_uah_mwh": 1410.0,
								"net_power_mw": 0.1,
							},
						],
					},
					{
						"data_quality_tier": "thesis_grade",
						"forecast_diagnostics": {"mae_uah_mwh": 800.0, "rmse_uah_mwh": 850.0},
						"horizon": [
							{
								"step_index": 0,
								"forecast_price_uah_mwh": 1000.0,
								"actual_price_uah_mwh": 1000.0,
								"net_power_mw": -0.1,
							},
							{
								"step_index": 1,
								"forecast_price_uah_mwh": 1400.0,
								"actual_price_uah_mwh": 1050.0,
								"net_power_mw": 0.1,
							},
						],
					},
				],
			}
		)
	)

	response = client.get(
		"/dashboard/forecast-dispatch-sensitivity",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["anchor_count"] == 1
	assert response_payload["model_count"] == 2
	assert response_payload["row_count"] == 2
	assert [row["diagnostic_bucket"] for row in response_payload["rows"]] == [
		"low_regret",
		"spread_objective_mismatch",
	]
	assert {row["diagnostic_bucket"] for row in response_payload["bucket_summary"]} == {
		"low_regret",
		"spread_objective_mismatch",
	}
	assert response_payload["rows"][1]["dispatch_spread_error_uah_mwh"] == pytest.approx(350.0)


def test_dfl_relaxed_pilot_endpoint_returns_latest_rows(
	client: TestClient,
	fake_dfl_training_store: InMemoryDflTrainingStore,
) -> None:
	fake_dfl_training_store.upsert_relaxed_pilot_frame(
		pl.DataFrame(
			{
				"pilot_name": ["relaxed_lp_dfl_pilot_v0", "relaxed_lp_dfl_pilot_v0"],
				"evaluation_id": ["eval-001", "eval-002"],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"forecast_model_name": ["tft_silver_v0", "nbeatsx_silver_v0"],
				"anchor_timestamp": [
					datetime(2026, 5, 3, 20, tzinfo=UTC),
					datetime(2026, 5, 4, 20, tzinfo=UTC),
				],
				"horizon_hours": [24, 24],
				"relaxed_realized_value_uah": [105.0, 112.0],
				"relaxed_oracle_value_uah": [130.0, 130.0],
				"relaxed_regret_uah": [25.0, 18.0],
				"first_charge_mw": [0.0, 0.1],
				"first_discharge_mw": [0.2, 0.0],
				"academic_scope": [
					"differentiable_relaxed_lp_pilot_not_final_dfl",
					"differentiable_relaxed_lp_pilot_not_final_dfl",
				],
			}
		)
	)

	response = client.get(
		"/dashboard/dfl-relaxed-pilot",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["row_count"] == 2
	assert response_payload["mean_relaxed_regret_uah"] == pytest.approx(21.5)
	assert response_payload["academic_scope"] == "differentiable_relaxed_lp_pilot_not_final_dfl"
	assert [row["forecast_model_name"] for row in response_payload["rows"]] == [
		"tft_silver_v0",
		"nbeatsx_silver_v0",
	]


def test_dfl_schedule_value_production_gate_endpoint_returns_offline_boundary(
	client: TestClient,
	fake_dfl_training_store: InMemoryDflTrainingStore,
) -> None:
	generated_at = datetime(2026, 5, 10, 13, tzinfo=UTC)
	fake_dfl_training_store.upsert_schedule_value_production_gate_frame(
		pl.DataFrame(
			{
				"source_model_name": ["nbeatsx_silver_v0", "tft_silver_v0"],
				"tenant_count": [5, 5],
				"latest_validation_tenant_anchor_count": [90, 90],
				"latest_strict_mean_regret_uah": [314.8126598731152, 314.8126598731152],
				"latest_selected_mean_regret_uah": [258.2268805296927, 248.48758297808885],
				"latest_strict_median_regret_uah": [202.60626109078976, 202.60626109078976],
				"latest_selected_median_regret_uah": [132.6155094227787, 89.89137186765288],
				"latest_mean_regret_improvement_ratio_vs_strict": [
					0.17974429416602652,
					0.21068109815456143,
				],
				"latest_median_not_worse": [True, True],
				"latest_source_signal": [True, True],
				"rolling_window_count": [4, 4],
				"rolling_strict_pass_window_count": [4, 3],
				"rolling_development_pass_window_count": [4, 4],
				"robust_research_challenger": [True, True],
				"allowed_challenger": [
					"dfl_schedule_value_learner_v2_nbeatsx_silver_v0",
					"dfl_schedule_value_learner_v2_tft_silver_v0",
				],
				"fallback_strategy": [
					"strict_similar_day_default_fallback",
					"strict_similar_day_default_fallback",
				],
				"promotion_blocker": ["none", "none"],
				"production_promote": [True, True],
				"market_execution_enabled": [False, False],
				"claim_scope": [
					"dfl_schedule_value_production_gate_offline_strategy_not_market_execution",
					"dfl_schedule_value_production_gate_offline_strategy_not_market_execution",
				],
				"academic_scope": [
					"Offline/read-model default-fallback gate for the Schedule/Value Learner V2.",
					"Offline/read-model default-fallback gate for the Schedule/Value Learner V2.",
				],
				"not_full_dfl": [True, True],
				"not_market_execution": [True, True],
				"generated_at": [generated_at, generated_at],
			}
		)
	)

	response = client.get("/dashboard/dfl-schedule-value-production-gate")

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["row_count"] == 2
	assert response_payload["production_promote_count"] == 2
	assert response_payload["promoted_source_model_names"] == [
		"nbeatsx_silver_v0",
		"tft_silver_v0",
	]
	assert response_payload["fallback_strategy"] == "strict_similar_day_default_fallback"
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["claim_boundary"] == "offline_read_model_strategy_evidence_only_not_market_execution"
	assert "Offline Strategy Promotion" in response_payload["academic_scope"]
	assert "market execution remains disabled" in response_payload["academic_scope"]
	assert response_payload["rows"][0]["market_execution_enabled"] is False
	assert response_payload["rows"][0]["production_promote"] is True


def test_decision_transformer_trajectories_endpoint_returns_rows(
	client: TestClient,
	fake_simulated_trade_store: InMemorySimulatedTradeStore,
) -> None:
	fake_simulated_trade_store.upsert_decision_transformer_trajectory_frame(
		pl.DataFrame(
			{
				"episode_id": ["episode-001", "episode-001"],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"market_venue": ["DAM", "DAM"],
				"scenario_index": [0, 0],
				"step_index": [0, 1],
				"interval_start": [
					datetime(2026, 5, 5, 0, tzinfo=UTC),
					datetime(2026, 5, 5, 1, tzinfo=UTC),
				],
				"state_soc_before": [0.5, 0.45],
				"state_soc_after": [0.45, 0.55],
				"state_soh": [0.96, 0.96],
				"state_market_price_uah_mwh": [1400.0, 900.0],
				"action_charge_mw": [0.0, 0.1],
				"action_discharge_mw": [0.1, 0.0],
				"reward_uah": [120.0, -95.0],
				"return_to_go_uah": [25.0, -95.0],
				"degradation_penalty_uah": [4.0, 3.5],
				"baseline_value_uah": [20.0, 20.0],
				"oracle_value_uah": [40.0, 40.0],
				"regret_uah": [20.0, 20.0],
				"academic_scope": [
					"offline_dt_training_trajectory_not_live_policy",
					"offline_dt_training_trajectory_not_live_policy",
				],
			}
		)
	)

	response = client.get(
		"/dashboard/decision-transformer-trajectories",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["row_count"] == 2
	assert response_payload["episode_count"] == 1
	assert response_payload["academic_scope"] == "offline_dt_training_trajectory_not_live_policy"
	assert response_payload["rows"][0]["action_discharge_mw"] == pytest.approx(0.1)


def test_decision_policy_preview_endpoint_returns_ready_rows(
	client: TestClient,
	fake_simulated_trade_store: InMemorySimulatedTradeStore,
) -> None:
	fake_simulated_trade_store.upsert_decision_transformer_policy_preview_frame(
		pl.DataFrame(
			{
				"policy_run_id": ["dt-run-001", "dt-run-001"],
				"created_at": [
					datetime(2026, 5, 5, 12, tzinfo=UTC),
					datetime(2026, 5, 5, 12, tzinfo=UTC),
				],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"episode_id": ["episode-001", "episode-001"],
				"market_venue": ["DAM", "DAM"],
				"scenario_index": [0, 0],
				"step_index": [0, 1],
				"interval_start": [
					datetime(2026, 5, 5, 0, tzinfo=UTC),
					datetime(2026, 5, 5, 1, tzinfo=UTC),
				],
				"state_market_price_uah_mwh": [4200.0, 1600.0],
				"state_nbeatsx_forecast_uah_mwh": [4100.0, 1700.0],
				"state_tft_forecast_uah_mwh": [4350.0, 1550.0],
				"state_forecast_uncertainty_uah_mwh": [360.0, 220.0],
				"state_forecast_spread_uah_mwh": [250.0, -150.0],
				"projected_soc_before": [0.5, 0.45],
				"projected_soc_after": [0.45, 0.55],
				"raw_charge_mw": [0.0, 0.1],
				"raw_discharge_mw": [0.1, 0.0],
				"projected_charge_mw": [0.0, 0.1],
				"projected_discharge_mw": [0.1, 0.0],
				"projected_net_power_mw": [0.1, -0.1],
				"expected_policy_value_uah": [416.0, -164.0],
				"hold_value_uah": [0.0, 0.0],
				"value_vs_hold_uah": [416.0, -164.0],
				"oracle_value_uah": [550.0, 550.0],
				"value_gap_uah": [134.0, 714.0],
				"constraint_violation": [False, False],
				"gatekeeper_status": ["accepted", "accepted"],
				"inference_latency_ms": [0.4, 0.5],
				"policy_mode": [
					"decision_transformer_preview",
					"decision_transformer_preview",
				],
				"readiness_status": [
					"ready_for_operator_preview",
					"ready_for_operator_preview",
				],
				"model_name": [
					"decision_transformer_policy_v0",
					"decision_transformer_policy_v0",
				],
				"academic_scope": [
					"offline_dt_policy_preview_not_market_execution",
					"offline_dt_policy_preview_not_market_execution",
				],
			}
		)
	)

	response = client.get(
		"/dashboard/decision-policy-preview",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["policy_run_id"] == "dt-run-001"
	assert response_payload["policy_readiness"] == "ready_for_operator_preview"
	assert response_payload["live_policy_claim"] is False
	assert response_payload["market_execution_enabled"] is False
	assert response_payload["constraint_violation_count"] == 0
	assert response_payload["forecast_context_source"] == "nbeatsx_tft_forecast_context"
	assert response_payload["forecast_context_row_count"] == 2
	assert response_payload["forecast_context_coverage_ratio"] == pytest.approx(1.0)
	assert response_payload["forecast_context_warning"] is None
	assert response_payload["policy_state_features"] == [
		"SOC",
		"SOH",
		"market price",
		"NBEATSx forecast",
		"TFT forecast",
		"forecast uncertainty",
		"forecast spread",
		"time of day",
		"degradation penalty",
		"return target",
		"previous battery action",
	]
	assert response_payload["policy_value_interpretation"] == (
		"value_gap = oracle_value_uah - expected_policy_value_uah after deterministic projection"
	)
	assert response_payload["operator_boundary"] == "preview_only_requires_gatekeeper_and_operator_review"
	assert response_payload["rows"][0]["projected_net_power_mw"] == pytest.approx(0.1)
	assert response_payload["rows"][0]["projected_action_label"] == "discharge"
	assert response_payload["rows"][0]["projection_status"] == "accepted_without_projection"
	assert response_payload["rows"][0]["projection_adjustment_mw"] == pytest.approx(0.0)
	assert response_payload["rows"][0]["value_gap_ratio"] == pytest.approx(134.0 / 550.0)
	assert response_payload["rows"][0]["state_nbeatsx_forecast_uah_mwh"] == pytest.approx(4100.0)
	assert response_payload["rows"][0]["state_tft_forecast_uah_mwh"] == pytest.approx(4350.0)
	assert response_payload["rows"][0]["state_forecast_uncertainty_uah_mwh"] == pytest.approx(360.0)
	assert response_payload["rows"][0]["state_forecast_spread_uah_mwh"] == pytest.approx(250.0)


def test_simulated_live_trading_endpoint_returns_rows(
	client: TestClient,
	fake_simulated_trade_store: InMemorySimulatedTradeStore,
) -> None:
	fake_simulated_trade_store.upsert_simulated_live_trading_frame(
		pl.DataFrame(
			{
				"episode_id": ["episode-001", "episode-001"],
				"tenant_id": [
					"client_003_dnipro_factory",
					"client_003_dnipro_factory",
				],
				"interval_start": [
					datetime(2026, 5, 5, 0, tzinfo=UTC),
					datetime(2026, 5, 5, 1, tzinfo=UTC),
				],
				"step_index": [0, 1],
				"state_soc_before": [0.5, 0.45],
				"state_soc_after": [0.45, 0.55],
				"proposed_trade_side": ["SELL", "BUY"],
				"proposed_quantity_mw": [0.1, 0.1],
				"feasible_net_power_mw": [0.1, -0.1],
				"market_price_uah_mwh": [1400.0, 900.0],
				"reward_uah": [120.0, -95.0],
				"gatekeeper_status": ["accepted", "accepted"],
				"paper_trade_provenance": ["simulated", "simulated"],
				"settlement_id": [None, None],
				"live_mode_warning": [
					"simulated_paper_trade_not_market_execution",
					"simulated_paper_trade_not_market_execution",
				],
			}
		)
	)

	response = client.get(
		"/dashboard/simulated-live-trading",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	response_payload = response.json()
	assert response_payload["tenant_id"] == "client_003_dnipro_factory"
	assert response_payload["row_count"] == 2
	assert response_payload["simulated_only"] is True
	assert response_payload["rows"][0]["paper_trade_provenance"] == "simulated"
	assert response_payload["rows"][0]["settlement_id"] is None


def test_operator_status_endpoint_returns_persisted_record(
	client: TestClient,
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
	fake_status_store.upsert_status(
		OperatorStatusRecord(
			tenant_id="client_003_dnipro_factory",
			flow_type=OperatorFlowType.WEATHER_CONTROL,
			status=OperatorFlowStatus.COMPLETED,
			updated_at=datetime(2026, 4, 30, 3, 58, tzinfo=UTC),
			payload={"selected_assets": ["weather_forecast_bronze"]},
		)
	)

	response = client.get(
		"/dashboard/operator-status",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"flow_type": "weather_control",
		},
	)

	assert response.status_code == 200
	assert response.json() == {
		"tenant_id": "client_003_dnipro_factory",
		"flow_type": "weather_control",
		"status": "completed",
		"updated_at": "2026-04-30T03:58:00+00:00",
		"payload": {"selected_assets": ["weather_forecast_bronze"]},
		"last_error": None,
	}


def test_operator_status_endpoint_returns_404_for_unknown_record(
	client: TestClient,
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
	response = client.get(
		"/dashboard/operator-status",
		params={
			"tenant_id": "client_003_dnipro_factory",
			"flow_type": "signal_preview",
		},
	)

	assert response.status_code == 404
	assert response.json() == {"detail": "Operator flow status not found."}


def test_gatekeeper_validation_status_endpoint_returns_latest_no_bid_failure(
	client: TestClient,
	fake_validation_failure_store: InMemoryValidationFailureStore,
) -> None:
	fake_validation_failure_store.append_failure(
		ValidationFailureRecord(
			failure_id="failure-001",
			tenant_id="client_003_dnipro_factory",
			validation_stage=ValidationStage.PROPOSED_BID,
			contract_type="ProposedBid",
			canonical_outcome="NO_BID",
			venue="DAM",
			interval_start=datetime(2026, 5, 24, 9, tzinfo=UTC),
			duration_minutes=60,
			failure_reason="Bid segment price 16000.0 exceeds the DAM cap of 15000.0.",
			payload={"venue": "DAM"},
			created_at=datetime(2026, 5, 23, 12, tzinfo=UTC),
		)
	)

	response = client.get(
		"/dashboard/gatekeeper-validation-status",
		params={"tenant_id": "client_003_dnipro_factory"},
	)

	assert response.status_code == 200
	assert response.json() == {
		"tenant_id": "client_003_dnipro_factory",
		"status": "blocked",
		"validation_stage": "proposed_bid",
		"contract_type": "ProposedBid",
		"canonical_outcome": "NO_BID",
		"venue": "DAM",
		"interval_start": "2026-05-24T09:00:00Z",
		"duration_minutes": 60,
		"failure_reason": "Bid segment price 16000.0 exceeds the DAM cap of 15000.0.",
		"created_at": "2026-05-23T12:00:00Z",
		"no_bid_semantics": "market_stage_bid_not_submitted",
		"hold_semantics": "physical_dispatch_zero_power_after_market_stage",
		"latest_failure_id": "failure-001",
	}


def test_openapi_schema_exposes_endpoint_metadata(client: TestClient) -> None:
	response = client.get("/openapi.json")

	assert response.status_code == 200
	schema = response.json()
	assert schema["info"]["title"] == "Smart Energy Arbitrage API"
	assert schema["paths"]["/tenants"]["get"]["summary"] == "List weather-aware tenants"
	assert schema["paths"]["/weather/materialize"]["post"]["summary"] == "Materialize weather experiment assets"
	assert schema["paths"]["/dashboard/signal-preview"]["get"]["summary"] == "Build dashboard signal preview"
	assert schema["paths"]["/dashboard/operator-status"]["get"]["summary"] == "Get persisted operator flow status"
	assert schema["paths"]["/dashboard/projected-battery-state"]["post"]["summary"] == "Build projected battery state preview"
	assert schema["paths"]["/dashboard/battery-state"]["get"]["summary"] == "Get latest battery telemetry state"
	assert schema["paths"]["/dashboard/exogenous-signals"]["get"]["summary"] == "Get latest exogenous signals"
	assert schema["paths"]["/dashboard/baseline-lp-preview"]["get"]["summary"] == "Build baseline LP preview"
	assert schema["paths"]["/dashboard/forecast-strategy-comparison"]["get"]["summary"] == "Get forecast strategy comparison"
	assert schema["paths"]["/dashboard/real-data-benchmark"]["get"]["summary"] == "Get real-data benchmark"
	assert schema["paths"]["/dashboard/calibrated-ensemble-benchmark"]["get"]["summary"] == "Get calibrated ensemble benchmark"
	assert schema["paths"]["/dashboard/risk-adjusted-value-gate"]["get"]["summary"] == "Get risk-adjusted value gate"
	assert schema["paths"]["/dashboard/forecast-dispatch-sensitivity"]["get"]["summary"] == "Get forecast-dispatch sensitivity"
	assert schema["paths"]["/dashboard/dfl-relaxed-pilot"]["get"]["summary"] == "Get relaxed DFL pilot"
	assert schema["paths"]["/dashboard/decision-transformer-trajectories"]["get"]["summary"] == "Get Decision Transformer trajectories"
	assert schema["paths"]["/dashboard/decision-policy-preview"]["get"]["summary"] == "Get Decision Transformer policy preview"
	assert schema["paths"]["/dashboard/simulated-live-trading"]["get"]["summary"] == "Get simulated live trading"
	assert schema["paths"]["/dashboard/future-stack-preview"]["get"]["summary"] == "Get future forecast and policy stack preview"
	assert schema["paths"]["/dashboard/operator-recommendation"]["get"]["summary"] == "Get operator recommendation"
	assert schema["paths"]["/dashboard/gatekeeper-validation-status"]["get"]["summary"] == "Get Bid Gatekeeper validation status"
