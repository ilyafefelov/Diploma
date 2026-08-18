from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from pydantic import ValidationError

from smart_arbitrage.publication.bess_arbitrage_index import (
    DEFAULT_PUBLIC_BATTERY_PRESETS,
    PUBLIC_BESS_INDEX_CLAIM_BOUNDARY,
    PublicBessGovernanceBoundary,
    build_blocked_public_bess_arbitrage_index_payload,
    build_public_bess_arbitrage_index_payload,
    build_public_bess_arbitrage_history_payload,
    _as_float_list,
)


EXPECTED_GOVERNANCE_BOUNDARY = {
    "calculation_scope": "generic_physical_bess_analytical_read_model",
    "enforced_constraints": [
        "power_limit",
        "state_of_charge_bounds",
        "round_trip_efficiency",
        "terminal_soc_equals_initial_soc",
        "degradation_proxy",
    ],
    "not_modeled_constraints": [
        "tenant_operating_calendar",
        "maintenance_or_outage_availability",
        "live_battery_telemetry",
        "market_eligibility_and_execution",
    ],
    "operator_context_status": "facility_specific_context_not_in_public_index",
    "metadata_status": "declared_model_boundary_not_source_receipt",
    "schedule_status": "perfect_hindsight_analytical_schedule_not_dispatch_command",
}


def test_public_bess_index_solves_two_presets_without_market_execution() -> None:
    delivery_start = datetime(2026, 6, 15)
    prices = [
        {
            "timestamp": delivery_start + timedelta(hours=hour),
            "price_uah_mwh": 1800.0 if hour < 6 else 7800.0 if 17 <= hour <= 21 else 3100.0,
            "volume_mwh": 2400.0 + hour,
            "source_url": "https://www.oree.com.ua/index.php/control/results_mo/DAM",
        }
        for hour in range(24)
    ]

    payload = build_public_bess_arbitrage_index_payload(
        prices,
        generated_at=datetime(2026, 6, 16, 7, 30),
    )

    assert payload["market_execution_enabled"] is False
    assert payload["proposed_bid_status"] == "not_emitted"
    assert payload["claim_boundary"] == PUBLIC_BESS_INDEX_CLAIM_BOUNDARY
    assert payload["governance"] == EXPECTED_GOVERNANCE_BOUNDARY
    assert payload["source"]["delivery_date"] == "2026-06-15"
    assert payload["source"]["row_count"] == 24
    assert [preset["preset_id"] for preset in payload["presets"]] == [
        preset.preset_id for preset in DEFAULT_PUBLIC_BATTERY_PRESETS
    ]

    for preset in payload["presets"]:
        assert preset["metrics"]["net_value_uah"] > 0.0
        assert preset["metrics"]["normalized_uah_per_mwh_capacity"] > 0.0
        assert preset["metrics"]["equivalent_full_cycles"] > 0.0
        assert preset["metrics"]["final_soc_mwh"] == pytest.approx(
            preset["battery"]["initial_soc_mwh"],
            abs=1e-4,
        )
        assert len(preset["hourly_schedule"]) == 24
        assert max(abs(point["net_power_mw"]) for point in preset["hourly_schedule"]) <= (
            preset["battery"]["max_power_mw"] + 1e-6
        )


def test_blocked_public_bess_payload_keeps_the_same_declared_governance_boundary() -> None:
    payload = build_blocked_public_bess_arbitrage_index_payload(
        delivery_day=date(2026, 6, 15),
        generated_at=datetime(2026, 6, 16, 7, 30),
        error=RuntimeError("official source unavailable"),
    )

    assert payload["governance"] == EXPECTED_GOVERNANCE_BOUNDARY
    assert payload["generated_at"] == "2026-06-16T07:30:00+00:00"
    assert payload["market_execution_enabled"] is False
    assert payload["proposed_bid_status"] == "not_emitted"


def test_public_bess_governance_contract_rejects_unknown_constraints() -> None:
    with pytest.raises(ValidationError):
        PublicBessGovernanceBoundary.model_validate(
            {
                "enforced_constraints": (
                    "power_limit",
                    "unverified_market_constraint",
                ),
            }
        )


def test_public_bess_history_merges_latest_by_delivery_date_and_preset() -> None:
    latest_payload = {
        "generated_at": "2026-06-16T07:30:00+00:00",
        "source": {"delivery_date": "2026-06-15"},
        "presets": [
            {
                "preset_id": "bess_100kw_215kwh",
                "label": "100 kW / 215 kWh C&I pack",
                "metrics": {
                    "net_value_uah": 125.0,
                    "normalized_uah_per_mwh_capacity": 581.4,
                    "equivalent_full_cycles": 0.6,
                    "throughput_mwh": 0.258,
                },
            }
        ],
    }
    previous_history = {
        "rows": [
            {
                "delivery_date": "2026-06-15",
                "preset_id": "bess_100kw_215kwh",
                "label": "old",
                "net_value_uah": 1.0,
            },
            {
                "delivery_date": "2026-06-14",
                "preset_id": "bess_100kw_215kwh",
                "label": "100 kW / 215 kWh C&I pack",
                "net_value_uah": 99.0,
            },
        ]
    }

    history = build_public_bess_arbitrage_history_payload(
        latest_payload=latest_payload,
        previous_history=previous_history,
    )

    assert history["market_execution_enabled"] is False
    assert history["row_count"] == 2
    assert [row["delivery_date"] for row in history["rows"]] == [
        "2026-06-14",
        "2026-06-15",
    ]
    assert history["rows"][-1]["net_value_uah"] == 125.0


def test_public_bess_history_treats_non_mapping_source_as_missing() -> None:
    history = build_public_bess_arbitrage_history_payload(
        latest_payload={"generated_at": "2026-06-16T07:30:00+00:00", "source": "invalid", "presets": []},
    )

    assert history["row_count"] == 0


def test_solver_values_reject_text_before_iterable_coercion() -> None:
    with pytest.raises(TypeError, match="Expected iterable solver values"):
        _as_float_list("12", expected_length=2)
