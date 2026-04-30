from typing import Any
from datetime import UTC, datetime

from fastapi.testclient import TestClient
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
	fake_status_store: _FakeOperatorStatusStore,
) -> None:
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
	assert response_payload["labels"] == ["06:00", "09:00", "12:00", "15:00", "18:00", "21:00"]
	assert len(response_payload["market_price"]) == 6
	assert len(response_payload["weather_bias"]) == 6
	assert len(response_payload["charge_intent"]) == 6
	assert len(response_payload["regret"]) == 6
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