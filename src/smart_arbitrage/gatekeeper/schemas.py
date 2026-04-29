"""Canonical market, clearing, and dispatch contracts for smart_arbitrage."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

MarketVenue = Literal["DAM", "IDM", "BALANCING"]
BidSide = Literal["BUY", "SELL"]
DispatchAction = Literal["CHARGE", "DISCHARGE", "HOLD"]
ClearedTradeProvenance = Literal["simulated", "observed"]

MARKET_PRICE_CAPS_UAH_PER_MWH: dict[MarketVenue, float] = {
    "DAM": 15000.0,
    "IDM": 15000.0,
    "BALANCING": 16000.0,
}

VENUE_ALLOWED_DURATION_MINUTES: dict[MarketVenue, set[int]] = {
    "DAM": {60},
    "IDM": {15},
    "BALANCING": {15},
}


class BatteryPhysicalMetrics(BaseModel):
    """Physical and economic battery constraints in canonical UAH units."""

    capacity_mwh: float = Field(gt=0.0)
    max_power_mw: float = Field(gt=0.0)
    round_trip_efficiency: float = Field(gt=0.0, le=1.0)
    degradation_cost_per_cycle_uah: float = Field(gt=0.0)
    soc_min_fraction: float = Field(default=0.05, ge=0.0, le=1.0)
    soc_max_fraction: float = Field(default=0.95, ge=0.0, le=1.0)

    model_config = ConfigDict(strict=True)

    @model_validator(mode="after")
    def validate_soc_window(self) -> Self:
        if self.soc_min_fraction >= self.soc_max_fraction:
            raise ValueError("soc_min_fraction must be lower than soc_max_fraction.")
        return self

    @property
    def degradation_cost_per_mwh_throughput_uah(self) -> float:
        return self.degradation_cost_per_cycle_uah / (2.0 * self.capacity_mwh)


class BatteryTelemetry(BaseModel):
    """Live battery telemetry used for the final dispatch safety check."""

    current_soc: float = Field(ge=0.0, le=1.0)
    soh: float = Field(ge=0.0, le=1.0)
    last_updated: datetime

    model_config = ConfigDict(strict=True)


class ProjectedBatteryState(BaseModel):
    """Projected battery state at the start of a target trading interval."""

    expected_soc: float = Field(ge=0.0, le=1.0)
    expected_soh: float = Field(ge=0.0, le=1.0)
    interval_start: datetime
    duration_minutes: int = Field(gt=0)

    model_config = ConfigDict(strict=True)


class BidFeasibilityEnvelope(BaseModel):
    """Conservative bid envelope for a single venue and interval."""

    venue: MarketVenue
    interval_start: datetime
    duration_minutes: int = Field(gt=0)
    soc_floor: float = Field(ge=0.0, le=1.0)
    soc_ceiling: float = Field(ge=0.0, le=1.0)
    max_feasible_sell_mw: float = Field(ge=0.0)
    max_feasible_buy_mw: float = Field(ge=0.0)
    available_energy_for_sell_mwh: float = Field(ge=0.0)
    available_headroom_for_buy_mwh: float = Field(ge=0.0)

    model_config = ConfigDict(strict=True)

    @model_validator(mode="after")
    def validate_envelope(self) -> Self:
        if self.soc_floor > self.soc_ceiling:
            raise ValueError("soc_floor must be lower than or equal to soc_ceiling.")
        _validate_duration(self.venue, self.duration_minutes)
        return self


class BidSegment(BaseModel):
    """One price-quantity segment of a bid curve."""

    side: BidSide
    segment_order: int = Field(ge=0)
    price_uah_mwh: float = Field(ge=0.0)
    quantity_mw: float = Field(gt=0.0)

    model_config = ConfigDict(strict=True)


class ProposedBid(BaseModel):
    """Canonical market bid for one venue and one trading interval."""

    venue: MarketVenue
    interval_start: datetime
    duration_minutes: int = Field(gt=0)
    segments: list[BidSegment] = Field(min_length=1)

    model_config = ConfigDict(strict=True)

    @model_validator(mode="after")
    def validate_bid(self, info: ValidationInfo) -> Self:
        _validate_duration(self.venue, self.duration_minutes)
        _validate_price_caps(self.venue, self.segments)
        _validate_curve_monotonicity(self.segments)
        _validate_deadband(self.segments)

        context = info.context or {}
        envelope = context.get("bid_feasibility_envelope")
        physical_metrics = context.get("battery_physical_metrics")

        if envelope is not None:
            if not isinstance(envelope, BidFeasibilityEnvelope):
                raise TypeError("bid_feasibility_envelope must be a BidFeasibilityEnvelope instance.")
            if envelope.venue != self.venue:
                raise ValueError("Bid envelope venue does not match the proposed bid venue.")
            if envelope.interval_start != self.interval_start:
                raise ValueError("Bid envelope interval_start does not match the proposed bid interval.")
            if envelope.duration_minutes != self.duration_minutes:
                raise ValueError("Bid envelope duration_minutes does not match the proposed bid duration.")
            _validate_against_envelope(self, envelope)

        if physical_metrics is not None:
            if not isinstance(physical_metrics, BatteryPhysicalMetrics):
                raise TypeError("battery_physical_metrics must be a BatteryPhysicalMetrics instance.")
            _validate_against_physical_limits(self, physical_metrics)

        return self


class NoBid(BaseModel):
    """Market-stage fallback when a bid cannot be submitted."""

    venue: MarketVenue
    interval_start: datetime
    duration_minutes: int = Field(gt=0)
    reason: str = Field(min_length=1)

    model_config = ConfigDict(strict=True)

    @model_validator(mode="after")
    def validate_no_bid(self) -> Self:
        _validate_duration(self.venue, self.duration_minutes)
        return self


class ClearedSegmentAllocation(BaseModel):
    """Clearing outcome for one offered bid segment."""

    side: BidSide
    segment_order: int = Field(ge=0)
    offered_price_uah_mwh: float = Field(ge=0.0)
    offered_quantity_mw: float = Field(gt=0.0)
    cleared_quantity_mw: float = Field(ge=0.0)

    model_config = ConfigDict(strict=True)

    @model_validator(mode="after")
    def validate_cleared_quantity(self) -> Self:
        if self.cleared_quantity_mw > self.offered_quantity_mw:
            raise ValueError("cleared_quantity_mw cannot exceed offered_quantity_mw.")
        return self


class ClearedTrade(BaseModel):
    """Canonical cleared trade with explicit provenance metadata."""

    provenance: ClearedTradeProvenance
    venue: MarketVenue
    interval_start: datetime
    duration_minutes: int = Field(gt=0)
    market_clearing_price_uah_mwh: float = Field(ge=0.0)
    allocations: list[ClearedSegmentAllocation] = Field(min_length=1)
    aggregator_settlement_id: str | None = None
    simulation_sigma: float | None = Field(default=None, ge=0.0)

    model_config = ConfigDict(strict=True)

    @model_validator(mode="after")
    def validate_cleared_trade(self) -> Self:
        _validate_duration(self.venue, self.duration_minutes)
        market_cap = MARKET_PRICE_CAPS_UAH_PER_MWH[self.venue]
        if self.market_clearing_price_uah_mwh > market_cap:
            raise ValueError("market_clearing_price_uah_mwh exceeds the venue price cap.")
        if self.provenance == "simulated" and self.aggregator_settlement_id is not None:
            raise ValueError("simulated cleared trades cannot carry aggregator_settlement_id.")
        if self.provenance == "observed" and self.simulation_sigma is not None:
            raise ValueError("observed cleared trades cannot carry simulation_sigma.")
        return self

    @property
    def net_quantity_mw(self) -> float:
        sell_quantity = sum(item.cleared_quantity_mw for item in self.allocations if item.side == "SELL")
        buy_quantity = sum(item.cleared_quantity_mw for item in self.allocations if item.side == "BUY")
        return sell_quantity - buy_quantity

    @property
    def net_energy_mwh(self) -> float:
        return self.net_quantity_mw * (self.duration_minutes / 60.0)

    @property
    def settlement_value_uah(self) -> float:
        return self.net_energy_mwh * self.market_clearing_price_uah_mwh


class DispatchCommand(BaseModel):
    """Physical dispatch command after market clearing and final safety checks."""

    interval_start: datetime
    duration_minutes: int = Field(gt=0)
    action: DispatchAction
    power_mw: float = Field(ge=0.0)
    reason: str | None = None

    model_config = ConfigDict(strict=True)

    @model_validator(mode="after")
    def validate_dispatch(self, info: ValidationInfo) -> Self:
        if self.action == "HOLD" and self.power_mw != 0.0:
            raise ValueError("HOLD dispatch commands must have zero power_mw.")
        if self.action != "HOLD" and self.power_mw <= 0.0:
            raise ValueError("Non-HOLD dispatch commands must have positive power_mw.")

        context = info.context or {}
        telemetry = context.get("battery_telemetry")
        physical_metrics = context.get("battery_physical_metrics")

        if physical_metrics is not None:
            if not isinstance(physical_metrics, BatteryPhysicalMetrics):
                raise TypeError("battery_physical_metrics must be a BatteryPhysicalMetrics instance.")
            if self.power_mw > physical_metrics.max_power_mw:
                raise ValueError("Dispatch power exceeds max_power_mw.")

        if telemetry is not None:
            if not isinstance(telemetry, BatteryTelemetry):
                raise TypeError("battery_telemetry must be a BatteryTelemetry instance.")
            if physical_metrics is None:
                raise ValueError("battery_physical_metrics context is required when validating against telemetry.")
            if self.action == "DISCHARGE" and telemetry.current_soc <= physical_metrics.soc_min_fraction:
                raise ValueError("Dispatch would violate the minimum SOC safety threshold.")
            if self.action == "CHARGE" and telemetry.current_soc >= physical_metrics.soc_max_fraction:
                raise ValueError("Dispatch would violate the maximum SOC safety threshold.")

        return self

    @classmethod
    def from_net_power(
        cls,
        *,
        interval_start: datetime,
        duration_minutes: int,
        net_power_mw: float,
        epsilon_mw: float = 1e-6,
        reason: str | None = None,
    ) -> Self:
        if abs(net_power_mw) <= epsilon_mw:
            return cls(
                interval_start=interval_start,
                duration_minutes=duration_minutes,
                action="HOLD",
                power_mw=0.0,
                reason=reason,
            )

        action: DispatchAction = "DISCHARGE" if net_power_mw > 0.0 else "CHARGE"
        return cls(
            interval_start=interval_start,
            duration_minutes=duration_minutes,
            action=action,
            power_mw=abs(net_power_mw),
            reason=reason,
        )


def _validate_duration(venue: MarketVenue, duration_minutes: int) -> None:
    allowed = VENUE_ALLOWED_DURATION_MINUTES[venue]
    if duration_minutes not in allowed:
        raise ValueError(
            f"duration_minutes={duration_minutes} is not allowed for venue {venue}. Allowed values: {sorted(allowed)}"
        )


def _validate_price_caps(venue: MarketVenue, segments: list[BidSegment]) -> None:
    market_cap = MARKET_PRICE_CAPS_UAH_PER_MWH[venue]
    for segment in segments:
        if segment.price_uah_mwh > market_cap:
            raise ValueError(
                f"Bid segment price {segment.price_uah_mwh} exceeds the {venue} cap of {market_cap}."
            )


def _validate_curve_monotonicity(segments: list[BidSegment]) -> None:
    grouped: dict[BidSide, list[BidSegment]] = {"BUY": [], "SELL": []}
    for segment in segments:
        grouped[segment.side].append(segment)

    buy_segments = sorted(grouped["BUY"], key=lambda item: item.segment_order)
    sell_segments = sorted(grouped["SELL"], key=lambda item: item.segment_order)

    _assert_unique_segment_order(buy_segments, "BUY")
    _assert_unique_segment_order(sell_segments, "SELL")

    for first, second in zip(buy_segments, buy_segments[1:]):
        if first.price_uah_mwh < second.price_uah_mwh:
            raise ValueError("BUY segment prices must be monotonically non-increasing by segment_order.")

    for first, second in zip(sell_segments, sell_segments[1:]):
        if first.price_uah_mwh > second.price_uah_mwh:
            raise ValueError("SELL segment prices must be monotonically non-decreasing by segment_order.")


def _assert_unique_segment_order(segments: list[BidSegment], side: BidSide) -> None:
    orders = [segment.segment_order for segment in segments]
    if len(orders) != len(set(orders)):
        raise ValueError(f"{side} segment_order values must be unique within one ProposedBid.")


def _validate_deadband(segments: list[BidSegment]) -> None:
    buy_prices = [segment.price_uah_mwh for segment in segments if segment.side == "BUY"]
    sell_prices = [segment.price_uah_mwh for segment in segments if segment.side == "SELL"]
    if buy_prices and sell_prices and max(buy_prices) >= min(sell_prices):
        raise ValueError("Two-sided bids must satisfy Bid Deadband: max(BUY price) < min(SELL price).")


def _validate_against_envelope(proposed_bid: ProposedBid, envelope: BidFeasibilityEnvelope) -> None:
    duration_hours = proposed_bid.duration_minutes / 60.0
    total_sell_mw = sum(segment.quantity_mw for segment in proposed_bid.segments if segment.side == "SELL")
    total_buy_mw = sum(segment.quantity_mw for segment in proposed_bid.segments if segment.side == "BUY")

    if total_sell_mw > envelope.max_feasible_sell_mw:
        raise ValueError("SELL quantity exceeds max_feasible_sell_mw in the bid envelope.")
    if total_buy_mw > envelope.max_feasible_buy_mw:
        raise ValueError("BUY quantity exceeds max_feasible_buy_mw in the bid envelope.")

    required_sell_energy_mwh = total_sell_mw * duration_hours
    required_buy_energy_mwh = total_buy_mw * duration_hours
    if required_sell_energy_mwh > envelope.available_energy_for_sell_mwh:
        raise ValueError("SELL energy exceeds available_energy_for_sell_mwh in the bid envelope.")
    if required_buy_energy_mwh > envelope.available_headroom_for_buy_mwh:
        raise ValueError("BUY energy exceeds available_headroom_for_buy_mwh in the bid envelope.")


def _validate_against_physical_limits(proposed_bid: ProposedBid, physical_metrics: BatteryPhysicalMetrics) -> None:
    total_sell_mw = sum(segment.quantity_mw for segment in proposed_bid.segments if segment.side == "SELL")
    total_buy_mw = sum(segment.quantity_mw for segment in proposed_bid.segments if segment.side == "BUY")
    if total_sell_mw > physical_metrics.max_power_mw:
        raise ValueError("SELL quantity exceeds max_power_mw.")
    if total_buy_mw > physical_metrics.max_power_mw:
        raise ValueError("BUY quantity exceeds max_power_mw.")