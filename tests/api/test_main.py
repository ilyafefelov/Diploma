from typing import Any
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
import polars as pl
import pytest

import api.main as api_main
from smart_arbitrage.resources.operator_status_store import (
	OperatorFlowStatus,
	OperatorFlowType,
	OperatorStatusRecord,
)
from smart_arbitrage.resources.battery_telemetry_store import (
	BatteryStateHourlySnapshot,
	BatteryTelemetryObservation,
	InMemoryBatteryTelemetryStore,
)
from smart_arbitrage.resources.grid_event_store import GridEventObservation, InMemoryGridEventStore
from smart_arbitrage.resources.market_data_store import InMemoryMarketDataStore, WeatherObservation
from smart_arbitrage.resources.dfl_training_store import InMemoryDflTrainingStore
from smart_arbitrage.resources.forecast_store import InMemoryForecastStore
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
def client() -> TestClient:
	return TestClient(api_main.app)


@pytest.fixture
def fake_status_store(monkeypatch: pytest.MonkeyPatch) -> _FakeOperatorStatusStore:
	store = _FakeOperatorStatusStore()
	monkeypatch.setattr(api_main, "get_operator_status_store", lambda: store)
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
def fake_grid_event_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryGridEventStore:
	store = InMemoryGridEventStore()
	monkeypatch.setattr(api_main, "get_grid_event_store", lambda: store)
	return store


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
) -> None:
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
	assert response_payload["policy_mode"] == "baseline_lp_preview"
	assert response_payload["policy_readiness"] == "lp_control_ready"
	assert response_payload["selected_policy_id"] == "strict_similar_day"
	assert len(response_payload["value_gap_series"]) == len(response_payload["recommendation_schedule"])
	assert {
		series["model_name"]
		for series in response_payload["forecast_model_series"]
	}.issuperset({"nbeatsx_silver_v0", "tft_silver_v0"})
	assert response_payload["load_forecast"][0]["reason_code"] in {"first_shift", "second_shift", "off_hours"}
	assert response_payload["available_strategies"][0]["strategy_id"] == "strict_similar_day"
	assert any(strategy["enabled"] is False for strategy in response_payload["available_strategies"] if strategy["strategy_id"] == "decision_transformer")


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
	assert response_payload["starting_soc_fraction"] == pytest.approx(0.5)
	assert response_payload["starting_soc_source"] == "tenant_default"
	assert response_payload["battery_metrics"]["capacity_mwh"] == pytest.approx(0.5)
	assert response_payload["battery_metrics"]["max_power_mw"] == pytest.approx(0.25)
	assert len(response_payload["forecast"]) == 24
	assert len(response_payload["recommendation_schedule"]) == 24
	assert len(response_payload["projected_state"]["trace"]) == 24
	assert "committed_dispatch" not in response_payload
	assert "proposed_bid" not in response_payload
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
	assert {series["model_name"] for series in response_payload["forecast_series"]} == {
		"nbeatsx_official_v0",
		"tft_official_v0",
	}
	official_tft = next(series for series in response_payload["forecast_series"] if series["model_name"] == "tft_official_v0")
	assert official_tft["source_status"] == "official"
	assert official_tft["uncertainty_kind"] == "quantile"
	assert official_tft["points"][0]["p10_price_uah_mwh"] == pytest.approx(3900.0)


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
	assert response_payload["rows"][0]["projected_net_power_mw"] == pytest.approx(0.1)


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
