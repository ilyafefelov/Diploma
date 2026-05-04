from datetime import UTC, datetime

from smart_arbitrage.telemetry.mqtt import (
    battery_telemetry_observation_from_payload,
    build_simulated_battery_telemetry_payload,
    battery_telemetry_topic,
)


def test_simulated_mqtt_payload_round_trips_to_battery_telemetry_observation() -> None:
    observed_at = datetime(2026, 5, 4, 12, 5, tzinfo=UTC)
    tenant_id = "client_003_dnipro_factory"

    payload = build_simulated_battery_telemetry_payload(
        tenant_id=tenant_id,
        observed_at=observed_at,
        sequence=7,
        current_soc=0.61,
        soh=0.962,
        power_mw=-0.04,
    )
    observation = battery_telemetry_observation_from_payload(
        topic=battery_telemetry_topic(tenant_id),
        payload=payload,
    )

    assert observation.tenant_id == tenant_id
    assert observation.observed_at == observed_at
    assert observation.current_soc == 0.61
    assert observation.soh == 0.962
    assert observation.power_mw == -0.04
    assert observation.source == "simulated_mqtt"
    assert observation.source_kind == "synthetic"
    assert observation.raw_payload["sequence"] == 7
