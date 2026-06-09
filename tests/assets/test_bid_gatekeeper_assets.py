from __future__ import annotations

from datetime import UTC, datetime

import dagster as dg

from smart_arbitrage.assets import mvp_demo
from smart_arbitrage.assets.gold import bid_gatekeeper
from smart_arbitrage.assets.gold.baseline_solver import (
    BaselineForecastPoint,
    BaselineSchedulePoint,
    BaselineSolveResult,
)
from smart_arbitrage.assets.gold.bid_gatekeeper import (
    bid_gatekeeper_validation_failure_evidence,
    bid_gatekeeper_validation_failure_frame,
)
from smart_arbitrage.defs import defs
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics, DispatchCommand
from smart_arbitrage.resources.validation_failure_store import (
    InMemoryValidationFailureStore,
    ValidationStage,
)


def test_bid_gatekeeper_failure_asset_logs_no_bid_validation_failure(
    monkeypatch,
) -> None:
    store = InMemoryValidationFailureStore()
    monkeypatch.setattr(bid_gatekeeper, "get_validation_failure_store", lambda: store)

    frame = bid_gatekeeper_validation_failure_frame(dg.build_asset_context())

    assert frame.height == 1
    assert frame.row(0, named=True)["validation_stage"] == "proposed_bid"
    assert frame.row(0, named=True)["canonical_outcome"] == "NO_BID"
    assert frame.row(0, named=True)["contract_type"] == "ProposedBid"

    latest = store.latest_failure(
        tenant_id="client_003_dnipro_factory",
        validation_stage=ValidationStage.PROPOSED_BID,
    )
    assert latest is not None
    assert latest.canonical_outcome == "NO_BID"


def test_blocked_dispatch_demo_logs_hold_validation_failure(monkeypatch) -> None:
    store = InMemoryValidationFailureStore()
    monkeypatch.setattr(mvp_demo, "get_validation_failure_store", lambda: store)
    baseline_plan = _baseline_plan()
    metrics = BatteryPhysicalMetrics(
        capacity_mwh=1.0,
        max_power_mw=0.5,
        round_trip_efficiency=0.9,
        degradation_cost_per_cycle_uah=54.0,
    )

    command = mvp_demo.blocked_dispatch_command_demo(
        dg.build_asset_context(),
        baseline_plan,
        metrics,
    )

    latest = store.latest_failure(
        tenant_id="client_003_dnipro_factory",
        validation_stage=ValidationStage.DISPATCH_COMMAND,
    )
    assert command.action == "HOLD"
    assert latest is not None
    assert latest.contract_type == "DispatchCommand"
    assert latest.canonical_outcome == "HOLD"
    assert latest.validation_stage == ValidationStage.DISPATCH_COMMAND


def test_bid_gatekeeper_failure_asset_check_accepts_no_bid_evidence() -> None:
    frame = bid_gatekeeper_validation_failure_frame(dg.build_asset_context())

    result = bid_gatekeeper_validation_failure_evidence(frame)

    assert result.passed is True
    assert result.metadata["blocked_count"].value == 1
    assert result.metadata["canonical_outcome"].value == "NO_BID"


def test_bid_gatekeeper_asset_and_check_are_registered() -> None:
    asset_keys = {
        asset_key.to_user_string()
        for asset in defs.assets or []
        for asset_key in asset.keys
    }
    check_keys = {
        (check_key.asset_key.to_user_string(), check_key.name)
        for check_def in defs.asset_checks or []
        for check_key in check_def.check_keys
    }

    assert "bid_gatekeeper_validation_failure_frame" in asset_keys
    assert (
        "bid_gatekeeper_validation_failure_frame",
        "bid_gatekeeper_validation_failure_evidence",
    ) in check_keys


def _baseline_plan() -> BaselineSolveResult:
    interval_start = datetime(2026, 5, 24, 9, tzinfo=UTC)
    anchor_timestamp = datetime(2026, 5, 23, 12, tzinfo=UTC)
    return BaselineSolveResult(
        anchor_timestamp=anchor_timestamp,
        forecast=[
            BaselineForecastPoint(
                forecast_timestamp=interval_start,
                source_timestamp=anchor_timestamp,
                predicted_price_uah_mwh=1500.0,
            )
        ],
        schedule=[
            BaselineSchedulePoint(
                step_index=0,
                interval_start=interval_start,
                forecast_price_uah_mwh=1500.0,
                charge_mw=0.0,
                discharge_mw=0.0,
                soc_before_mwh=0.5,
                soc_after_mwh=0.5,
                throughput_mwh=0.0,
                degradation_penalty_uah=0.0,
                gross_market_value_uah=0.0,
                net_objective_value_uah=0.0,
            )
        ],
        committed_dispatch=DispatchCommand(
            interval_start=interval_start,
            duration_minutes=60,
            action="HOLD",
            power_mw=0.0,
        ),
    )
