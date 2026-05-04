from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import math
import os
import time
from typing import Any

import paho.mqtt.client as mqtt

from smart_arbitrage.assets.bronze.market_weather import (
    list_available_weather_tenants,
    resolve_tenant_registry_entry,
)
from smart_arbitrage.telemetry.mqtt import (
    battery_telemetry_topic,
    build_simulated_battery_telemetry_payload,
)


def main() -> None:
    mqtt_host = os.environ.get("MQTT_HOST", "localhost")
    mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))
    publish_seconds = float(os.environ.get("TELEMETRY_PUBLISH_SECONDS", "5"))
    publish_once = os.environ.get("TELEMETRY_PUBLISH_ONCE", "false").lower() == "true"
    tenant_ids = _tenant_ids()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="smart-arbitrage-simulated-publisher")
    client.connect(mqtt_host, mqtt_port, keepalive=60)
    sequence = 0
    observed_at = datetime.now(UTC).replace(second=0, microsecond=0)

    while True:
        for tenant_id in tenant_ids:
            payload = _payload_for_tenant(
                tenant_id=tenant_id,
                observed_at=observed_at,
                sequence=sequence,
            )
            client.publish(
                battery_telemetry_topic(tenant_id),
                json.dumps(payload),
                qos=1,
            )
        client.loop(timeout=1.0)
        if publish_once:
            break
        sequence += 1
        observed_at += timedelta(minutes=5)
        time.sleep(publish_seconds)

    client.disconnect()


def _tenant_ids() -> list[str]:
    configured_value = os.environ.get("TELEMETRY_TENANT_IDS", "")
    tenant_ids = [item.strip() for item in configured_value.split(",") if item.strip()]
    if tenant_ids:
        return tenant_ids
    return [
        str(tenant["tenant_id"])
        for tenant in list_available_weather_tenants()
        if tenant.get("tenant_id") is not None
    ]


def _payload_for_tenant(*, tenant_id: str, observed_at: datetime, sequence: int) -> dict[str, Any]:
    tenant_entry = resolve_tenant_registry_entry(tenant_id=tenant_id)
    energy_system = tenant_entry.get("energy_system")
    initial_soc = 0.52
    if isinstance(energy_system, dict):
        initial_soc = float(energy_system.get("initial_soc_fraction", initial_soc))
    soc_wave = 0.08 * math.sin((sequence / 12.0) * 2.0 * math.pi)
    current_soc = max(0.05, min(0.95, initial_soc + soc_wave))
    power_mw = 0.05 * math.sin(((sequence + 3) / 12.0) * 2.0 * math.pi)
    soh = max(0.90, 0.97 - sequence * 0.00002)
    return build_simulated_battery_telemetry_payload(
        tenant_id=tenant_id,
        observed_at=observed_at,
        sequence=sequence,
        current_soc=round(current_soc, 4),
        soh=round(soh, 4),
        power_mw=round(power_mw, 4),
        temperature_c=round(24.0 + 2.0 * math.sin(sequence / 24.0), 2),
    )


if __name__ == "__main__":
    main()
