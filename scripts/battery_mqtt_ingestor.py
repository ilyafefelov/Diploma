from __future__ import annotations

import os

import paho.mqtt.client as mqtt

from smart_arbitrage.resources.battery_telemetry_store import get_battery_telemetry_store
from smart_arbitrage.telemetry.mqtt import battery_telemetry_observation_from_payload


def main() -> None:
    mqtt_host = os.environ.get("MQTT_HOST", "localhost")
    mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))
    topic_filter = os.environ.get("MQTT_BATTERY_TOPIC_FILTER", "smart-arbitrage/+/battery/telemetry")
    store = get_battery_telemetry_store()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="smart-arbitrage-battery-ingestor")

    def on_connect(client, userdata, flags, reason_code, properties) -> None:
        client.subscribe(topic_filter, qos=1)

    def on_message(client, userdata, message) -> None:
        observation = battery_telemetry_observation_from_payload(
            topic=message.topic,
            payload=message.payload,
        )
        store.upsert_battery_telemetry([observation])

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_host, mqtt_port, keepalive=60)
    client.loop_forever()


if __name__ == "__main__":
    main()
