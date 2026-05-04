from __future__ import annotations

from datetime import datetime
import json
from typing import Any

from smart_arbitrage.resources.battery_telemetry_store import BatteryTelemetryObservation


MQTT_TOPIC_PREFIX = "smart-arbitrage"
MQTT_TELEMETRY_SOURCE = "simulated_mqtt"


def battery_telemetry_topic(tenant_id: str) -> str:
    return f"{MQTT_TOPIC_PREFIX}/{tenant_id}/battery/telemetry"


def build_simulated_battery_telemetry_payload(
    *,
    tenant_id: str,
    observed_at: datetime,
    sequence: int,
    current_soc: float,
    soh: float,
    power_mw: float,
    temperature_c: float = 24.0,
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "observed_at": observed_at.isoformat(),
        "current_soc": current_soc,
        "soh": soh,
        "power_mw": power_mw,
        "temperature_c": temperature_c,
        "source": MQTT_TELEMETRY_SOURCE,
        "source_kind": "synthetic",
        "sequence": sequence,
    }


def battery_telemetry_observation_from_payload(
    *,
    topic: str,
    payload: dict[str, Any] | str | bytes,
) -> BatteryTelemetryObservation:
    payload_mapping = _payload_mapping(payload)
    tenant_id = str(payload_mapping.get("tenant_id") or _tenant_id_from_topic(topic))
    observed_at = _datetime_from_payload(payload_mapping.get("observed_at"))
    return BatteryTelemetryObservation(
        tenant_id=tenant_id,
        observed_at=observed_at,
        current_soc=_float_field(payload_mapping, "current_soc"),
        soh=_float_field(payload_mapping, "soh"),
        power_mw=_float_field(payload_mapping, "power_mw"),
        temperature_c=_optional_float_field(payload_mapping, "temperature_c"),
        source=str(payload_mapping.get("source", MQTT_TELEMETRY_SOURCE)),
        source_kind="synthetic",
        raw_payload=payload_mapping,
    )


def _payload_mapping(payload: dict[str, Any] | str | bytes) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    loaded_payload = json.loads(payload)
    if not isinstance(loaded_payload, dict):
        raise ValueError("MQTT telemetry payload must decode to a JSON object.")
    return loaded_payload


def _tenant_id_from_topic(topic: str) -> str:
    parts = topic.split("/")
    if len(parts) != 4 or parts[0] != MQTT_TOPIC_PREFIX or parts[2:] != ["battery", "telemetry"]:
        raise ValueError(f"Unsupported battery telemetry topic: {topic}")
    return parts[1]


def _datetime_from_payload(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise ValueError("observed_at must be an ISO datetime string.")
    return datetime.fromisoformat(value)


def _float_field(payload: dict[str, Any], field_name: str) -> float:
    if field_name not in payload:
        raise ValueError(f"{field_name} is required in MQTT telemetry payload.")
    return float(payload[field_name])


def _optional_float_field(payload: dict[str, Any], field_name: str) -> float | None:
    if field_name not in payload or payload[field_name] is None:
        return None
    return float(payload[field_name])
