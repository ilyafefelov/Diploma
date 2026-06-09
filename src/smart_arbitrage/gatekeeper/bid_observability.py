from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from typing import Any, Mapping

from pydantic import BaseModel

from smart_arbitrage.gatekeeper.schemas import (
    BatteryPhysicalMetrics,
    BidFeasibilityEnvelope,
    DispatchCommand,
    NoBid,
    ProposedBid,
)
from smart_arbitrage.resources.validation_failure_store import (
    ValidationFailureRecord,
    ValidationFailureStore,
    ValidationStage,
    get_validation_failure_store,
)

NO_BID_SEMANTICS = "market_stage_bid_not_submitted"
HOLD_SEMANTICS = "physical_dispatch_zero_power_after_market_stage"


@dataclass(frozen=True, slots=True)
class BidGatekeeperValidationOutcome:
    accepted_bid: ProposedBid | None
    fallback: NoBid | None
    failure_record: ValidationFailureRecord | None


def validate_proposed_bid_or_no_bid(
    *,
    tenant_id: str,
    proposed_bid_payload: Mapping[str, Any],
    bid_feasibility_envelope: BidFeasibilityEnvelope,
    battery_physical_metrics: BatteryPhysicalMetrics,
    failure_store: ValidationFailureStore | None = None,
    created_at: datetime | None = None,
) -> BidGatekeeperValidationOutcome:
    """Validate a market-stage ProposedBid and record a No Bid fallback on failure."""

    validation_time = created_at or datetime.now(tz=UTC)
    store = failure_store or get_validation_failure_store()
    context = {
        "bid_feasibility_envelope": bid_feasibility_envelope,
        "battery_physical_metrics": battery_physical_metrics,
    }

    try:
        accepted_bid = ProposedBid.model_validate(proposed_bid_payload, context=context)
    except (TypeError, ValueError) as error:
        failure_reason = str(error)
        fallback = NoBid(
            venue=bid_feasibility_envelope.venue,
            interval_start=bid_feasibility_envelope.interval_start,
            duration_minutes=bid_feasibility_envelope.duration_minutes,
            reason="proposed_bid_validation_failed",
        )
        payload_snapshot = _json_ready_payload(proposed_bid_payload)
        record = ValidationFailureRecord(
            failure_id=_failure_id(
                tenant_id=tenant_id,
                validation_stage=ValidationStage.PROPOSED_BID,
                created_at=validation_time,
                payload=payload_snapshot,
                failure_reason=failure_reason,
            ),
            tenant_id=tenant_id,
            validation_stage=ValidationStage.PROPOSED_BID,
            contract_type="ProposedBid",
            canonical_outcome="NO_BID",
            venue=bid_feasibility_envelope.venue,
            interval_start=bid_feasibility_envelope.interval_start,
            duration_minutes=bid_feasibility_envelope.duration_minutes,
            failure_reason=failure_reason,
            payload=payload_snapshot,
            created_at=validation_time,
        )
        store.append_failure(record)
        return BidGatekeeperValidationOutcome(
            accepted_bid=None,
            fallback=fallback,
            failure_record=record,
        )

    return BidGatekeeperValidationOutcome(
        accepted_bid=accepted_bid,
        fallback=None,
        failure_record=None,
    )


def record_dispatch_command_validation_failure(
    *,
    tenant_id: str,
    dispatch_command: DispatchCommand,
    failure_reason: str,
    failure_store: ValidationFailureStore | None = None,
    created_at: datetime | None = None,
) -> ValidationFailureRecord:
    """Record a physical-stage DispatchCommand failure that falls back to HOLD."""

    validation_time = created_at or datetime.now(tz=UTC)
    store = failure_store or get_validation_failure_store()
    payload_snapshot = _json_ready_payload(dispatch_command.model_dump())
    record = ValidationFailureRecord(
        failure_id=_failure_id(
            tenant_id=tenant_id,
            validation_stage=ValidationStage.DISPATCH_COMMAND,
            created_at=validation_time,
            payload=payload_snapshot,
            failure_reason=failure_reason,
        ),
        tenant_id=tenant_id,
        validation_stage=ValidationStage.DISPATCH_COMMAND,
        contract_type="DispatchCommand",
        canonical_outcome="HOLD",
        venue=None,
        interval_start=dispatch_command.interval_start,
        duration_minutes=dispatch_command.duration_minutes,
        failure_reason=failure_reason,
        payload=payload_snapshot,
        created_at=validation_time,
    )
    store.append_failure(record)
    return record


def _failure_id(
    *,
    tenant_id: str,
    validation_stage: ValidationStage,
    created_at: datetime,
    payload: dict[str, Any],
    failure_reason: str,
) -> str:
    fingerprint_payload = {
        "tenant_id": tenant_id,
        "validation_stage": validation_stage.value,
        "created_at": created_at.isoformat(),
        "payload": payload,
        "failure_reason": failure_reason,
    }
    fingerprint = json.dumps(fingerprint_payload, sort_keys=True, default=str)
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:32]


def _json_ready_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): _json_ready_value(value)
        for key, value in payload.items()
    }


def _json_ready_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return _json_ready_payload(value)
    if isinstance(value, list):
        return [_json_ready_value(item) for item in value]
    return value


__all__ = [
    "BidGatekeeperValidationOutcome",
    "HOLD_SEMANTICS",
    "NO_BID_SEMANTICS",
    "record_dispatch_command_validation_failure",
    "validate_proposed_bid_or_no_bid",
]
