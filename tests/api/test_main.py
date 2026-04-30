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
	assert all(len(label) == 5 and label[2] == ":" for label in response_payload["labels"])
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
	assert len(response_payload["forecast"]) == 24
	assert len(response_payload["recommendation_schedule"]) == 24
	assert len(response_payload["projected_state"]["trace"]) == 24
	assert "committed_dispatch" not in response_payload
	assert "proposed_bid" not in response_payload
	assert response_payload["economics"]["total_degradation_penalty_uah"] >= 0.0
	assert response_payload["economics"]["total_gross_market_value_uah"] != 0.0
	status_record = fake_status_store.get_status(
		tenant_id="client_003_dnipro_factory",
		flow_type=OperatorFlowType.BASELINE_LP,
	)
	assert status_record is not None
	assert status_record.status == OperatorFlowStatus.COMPLETED


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
	assert schema["paths"]["/dashboard/baseline-lp-preview"]["get"]["summary"] == "Build baseline LP preview"