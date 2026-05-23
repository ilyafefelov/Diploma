from __future__ import annotations

from datetime import UTC, datetime

from smart_arbitrage.gatekeeper.bid_observability import (
    validate_proposed_bid_or_no_bid,
)
from smart_arbitrage.gatekeeper.schemas import (
    BatteryPhysicalMetrics,
    BidFeasibilityEnvelope,
    BidSegment,
    NoBid,
)
from smart_arbitrage.resources.validation_failure_store import (
    InMemoryValidationFailureStore,
    ValidationStage,
)


def test_failed_proposed_bid_validation_records_no_bid_failure() -> None:
    interval_start = datetime(2026, 5, 24, 9, tzinfo=UTC)
    store = InMemoryValidationFailureStore()

    outcome = validate_proposed_bid_or_no_bid(
        tenant_id="client_003_dnipro_factory",
        proposed_bid_payload={
            "venue": "DAM",
            "interval_start": interval_start,
            "duration_minutes": 60,
            "segments": [
                BidSegment(
                    side="SELL",
                    segment_order=0,
                    price_uah_mwh=16_000.0,
                    quantity_mw=0.4,
                ).model_dump()
            ],
        },
        bid_feasibility_envelope=BidFeasibilityEnvelope(
            venue="DAM",
            interval_start=interval_start,
            duration_minutes=60,
            soc_floor=0.05,
            soc_ceiling=0.95,
            max_feasible_sell_mw=0.5,
            max_feasible_buy_mw=0.5,
            available_energy_for_sell_mwh=0.5,
            available_headroom_for_buy_mwh=0.5,
        ),
        battery_physical_metrics=BatteryPhysicalMetrics(
            capacity_mwh=1.0,
            max_power_mw=0.5,
            round_trip_efficiency=0.9,
            degradation_cost_per_cycle_uah=54.0,
        ),
        failure_store=store,
        created_at=datetime(2026, 5, 23, 12, tzinfo=UTC),
    )

    assert outcome.accepted_bid is None
    assert isinstance(outcome.fallback, NoBid)
    assert outcome.fallback.reason == "proposed_bid_validation_failed"
    assert outcome.failure_record is not None
    assert outcome.failure_record.validation_stage == ValidationStage.PROPOSED_BID
    assert outcome.failure_record.canonical_outcome == "NO_BID"
    assert outcome.failure_record.contract_type == "ProposedBid"
    assert "price" in outcome.failure_record.failure_reason

    latest = store.latest_failure(
        tenant_id="client_003_dnipro_factory",
        validation_stage=ValidationStage.PROPOSED_BID,
    )
    assert latest == outcome.failure_record
