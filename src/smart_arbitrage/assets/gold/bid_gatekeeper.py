from __future__ import annotations

from datetime import UTC, datetime

import dagster as dg
import polars as pl

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.gatekeeper.bid_observability import (
    HOLD_SEMANTICS,
    NO_BID_SEMANTICS,
    validate_proposed_bid_or_no_bid,
)
from smart_arbitrage.gatekeeper.schemas import (
    BatteryPhysicalMetrics,
    BidFeasibilityEnvelope,
    BidSegment,
)
from smart_arbitrage.resources.validation_failure_store import (
    ValidationFailureRecord,
    get_validation_failure_store,
)

BID_GATEKEEPER_TENANT_ID = "client_003_dnipro_factory"


@dg.asset(
    group_name=taxonomy.GOLD_MVP_GATEKEEPER,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="bid_gatekeeper",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="demo",
        market_venue="DAM",
    ),
)
def bid_gatekeeper_validation_failure_frame(
    context,
) -> pl.DataFrame:
    """Demo Bid Gatekeeper evidence row for a blocked ProposedBid -> No Bid outcome."""

    interval_start = datetime(2026, 5, 24, 9, tzinfo=UTC)
    envelope = BidFeasibilityEnvelope(
        venue="DAM",
        interval_start=interval_start,
        duration_minutes=60,
        soc_floor=0.05,
        soc_ceiling=0.95,
        max_feasible_sell_mw=0.5,
        max_feasible_buy_mw=0.5,
        available_energy_for_sell_mwh=0.5,
        available_headroom_for_buy_mwh=0.5,
    )
    physical_metrics = BatteryPhysicalMetrics(
        capacity_mwh=1.0,
        max_power_mw=0.5,
        round_trip_efficiency=0.9,
        degradation_cost_per_cycle_uah=54.0,
    )
    outcome = validate_proposed_bid_or_no_bid(
        tenant_id=BID_GATEKEEPER_TENANT_ID,
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
        bid_feasibility_envelope=envelope,
        battery_physical_metrics=physical_metrics,
        failure_store=get_validation_failure_store(),
        created_at=datetime(2026, 5, 23, 12, tzinfo=UTC),
    )
    if outcome.failure_record is None:
        raise ValueError("Bid Gatekeeper demo must emit a ProposedBid validation failure.")

    frame = _records_to_frame([outcome.failure_record])
    context.add_output_metadata(
        {
            "blocked_count": frame.height,
            "validation_stage": "proposed_bid",
            "contract_type": "ProposedBid",
            "canonical_outcome": "NO_BID",
            "no_bid_semantics": NO_BID_SEMANTICS,
            "hold_semantics": HOLD_SEMANTICS,
        }
    )
    return frame


@dg.asset_check(
    asset=bid_gatekeeper_validation_failure_frame,
    name="bid_gatekeeper_validation_failure_evidence",
    description="Checks blocked ProposedBid validation is surfaced as market-stage No Bid evidence.",
)
def bid_gatekeeper_validation_failure_evidence(
    bid_gatekeeper_validation_failure_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    frame = bid_gatekeeper_validation_failure_frame
    has_rows = frame.height > 0
    has_proposed_bid_stage = (
        has_rows and frame.select("validation_stage").to_series().to_list() == ["proposed_bid"]
    )
    has_no_bid_outcome = (
        has_rows and frame.select("canonical_outcome").to_series().to_list() == ["NO_BID"]
    )
    passed = has_rows and has_proposed_bid_stage and has_no_bid_outcome
    return dg.AssetCheckResult(
        passed=passed,
        description=(
            "Bid Gatekeeper validation failure evidence is present."
            if passed
            else "Bid Gatekeeper validation failure evidence is missing or misclassified."
        ),
        metadata={
            "blocked_count": frame.height,
            "canonical_outcome": "NO_BID" if has_no_bid_outcome else "unknown",
            "validation_stage": "proposed_bid" if has_proposed_bid_stage else "unknown",
            "no_bid_semantics": NO_BID_SEMANTICS,
            "hold_semantics": HOLD_SEMANTICS,
        },
    )


def _records_to_frame(records: list[ValidationFailureRecord]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "failure_id": record.failure_id,
                "tenant_id": record.tenant_id,
                "validation_stage": record.validation_stage.value,
                "contract_type": record.contract_type,
                "canonical_outcome": record.canonical_outcome,
                "venue": record.venue,
                "interval_start": record.interval_start,
                "duration_minutes": record.duration_minutes,
                "failure_reason": record.failure_reason,
                "created_at": record.created_at,
                "no_bid_semantics": NO_BID_SEMANTICS,
                "hold_semantics": HOLD_SEMANTICS,
            }
            for record in records
        ]
    )


BID_GATEKEEPER_GOLD_ASSETS = [bid_gatekeeper_validation_failure_frame]
BID_GATEKEEPER_ASSET_CHECKS = [bid_gatekeeper_validation_failure_evidence]

__all__ = [
    "BID_GATEKEEPER_ASSET_CHECKS",
    "BID_GATEKEEPER_GOLD_ASSETS",
    "bid_gatekeeper_validation_failure_evidence",
    "bid_gatekeeper_validation_failure_frame",
]
