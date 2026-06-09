from __future__ import annotations

from datetime import UTC, datetime
import json

from smart_arbitrage.resources.operator_status_store import _payload_to_json


def test_payload_to_json_serializes_nested_datetimes() -> None:
	payload_json = _payload_to_json(
		{
			"generated_at": datetime(2026, 5, 6, 10, 30, tzinfo=UTC),
			"points": [
				{
					"timestamp": datetime(2026, 5, 6, 11, 0, tzinfo=UTC),
					"value": 42,
				}
			],
		}
	)

	assert payload_json is not None
	decoded = json.loads(payload_json)
	assert decoded["generated_at"] == "2026-05-06T10:30:00+00:00"
	assert decoded["points"][0]["timestamp"] == "2026-05-06T11:00:00+00:00"
