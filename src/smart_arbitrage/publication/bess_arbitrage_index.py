"""Public Ukraine BESS Arbitrage Index payload builder.

The public index is a static analytical read model: official observed prices in,
perfect-hindsight daily LP out. It does not emit bids, dispatch commands, or any
market-execution payload.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from math import sqrt
from typing import Any, Final


PUBLIC_BESS_INDEX_CLAIM_BOUNDARY: Final[str] = (
    "public_bess_arbitrage_index_not_market_execution"
)
PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY: Final[str] = (
    "public_forecast_challenge_not_market_execution"
)
OREE_DAM_RESULTS_URL: Final[str] = "https://www.oree.com.ua/index.php/control/results_mo/DAM"
DEFAULT_MARKET_VENUE: Final[str] = "DAM"
DEFAULT_MARKET_ZONE: Final[str] = "OES Ukraine"
DEFAULT_MARKET_TIMEZONE: Final[str] = "Europe/Kyiv"
DEFAULT_ROUND_TRIP_EFFICIENCY: Final[float] = 0.92
DEFAULT_INITIAL_SOC_FRACTION: Final[float] = 0.50
DEFAULT_SOC_MIN_FRACTION: Final[float] = 0.05
DEFAULT_SOC_MAX_FRACTION: Final[float] = 0.95
DEMO_BATTERY_CAPEX_USD_PER_KWH: Final[float] = 210.0
DEMO_USD_TO_UAH_RATE: Final[float] = 43.9129
DEMO_BATTERY_LIFETIME_YEARS: Final[int] = 15
DEMO_BATTERY_CYCLES_PER_DAY: Final[float] = 1.0


@dataclass(frozen=True, slots=True)
class PublicBatteryPreset:
    preset_id: str
    label: str
    capacity_mwh: float
    max_power_mw: float
    round_trip_efficiency: float = DEFAULT_ROUND_TRIP_EFFICIENCY
    initial_soc_fraction: float = DEFAULT_INITIAL_SOC_FRACTION
    soc_min_fraction: float = DEFAULT_SOC_MIN_FRACTION
    soc_max_fraction: float = DEFAULT_SOC_MAX_FRACTION

    @property
    def initial_soc_mwh(self) -> float:
        return self.capacity_mwh * self.initial_soc_fraction

    @property
    def soc_min_mwh(self) -> float:
        return self.capacity_mwh * self.soc_min_fraction

    @property
    def soc_max_mwh(self) -> float:
        return self.capacity_mwh * self.soc_max_fraction

    @property
    def degradation_cost_per_cycle_uah(self) -> float:
        replacement_cost_uah = (
            DEMO_BATTERY_CAPEX_USD_PER_KWH
            * self.capacity_mwh
            * 1000.0
            * DEMO_USD_TO_UAH_RATE
        )
        lifetime_cycles = DEMO_BATTERY_LIFETIME_YEARS * 365.0 * DEMO_BATTERY_CYCLES_PER_DAY
        return replacement_cost_uah / lifetime_cycles

    @property
    def degradation_cost_per_mwh_throughput_uah(self) -> float:
        return self.degradation_cost_per_cycle_uah / (2.0 * self.capacity_mwh)


@dataclass(frozen=True, slots=True)
class PublicPricePoint:
    timestamp: datetime
    price_uah_mwh: float
    volume_mwh: float | None = None
    source_url: str = OREE_DAM_RESULTS_URL


DEFAULT_PUBLIC_BATTERY_PRESETS: Final[tuple[PublicBatteryPreset, ...]] = (
    PublicBatteryPreset(
        preset_id="bess_100kw_215kwh",
        label="100 kW / 215 kWh C&I pack",
        capacity_mwh=0.215,
        max_power_mw=0.100,
    ),
    PublicBatteryPreset(
        preset_id="bess_500kw_1mwh",
        label="500 kW / 1 MWh C&I pack",
        capacity_mwh=1.000,
        max_power_mw=0.500,
    ),
)


def build_public_bess_arbitrage_index_payload(
    price_rows: Sequence[Mapping[str, Any] | PublicPricePoint],
    *,
    generated_at: datetime | None = None,
    presets: Sequence[PublicBatteryPreset] = DEFAULT_PUBLIC_BATTERY_PRESETS,
    market_venue: str = DEFAULT_MARKET_VENUE,
    market_zone: str = DEFAULT_MARKET_ZONE,
    market_timezone: str = DEFAULT_MARKET_TIMEZONE,
) -> dict[str, Any]:
    prices = _normalize_price_points(price_rows)
    delivery_day = _complete_delivery_date(prices)
    generated = _utc_iso(generated_at or datetime.now(UTC))
    preset_payloads = [
        _solve_preset_payload(prices=prices, preset=preset)
        for preset in presets
    ]
    return {
        "schema_version": "ukraine_bess_arbitrage_index.v1",
        "generated_at": generated,
        "market_venue": market_venue,
        "market_zone": market_zone,
        "market_timezone": market_timezone,
        "source": {
            "source_name": "OREE DAM hourly prices",
            "source_url": _dominant_source_url(prices),
            "delivery_date": delivery_day.isoformat(),
            "row_count": len(prices),
            "source_scope": "official_observed_hourly_prices_only",
            "source_status": "complete_24_hour_delivery_day",
        },
        "methodology": {
            "index_kind": "realized_perfect_hindsight_daily_dispatch",
            "objective": "maximize DAM arbitrage value after round-trip efficiency and degradation proxy",
            "optimization_grain": "hourly",
            "terminal_soc": "final_soc_equals_initial_soc",
            "baseline": "no_battery_dispatch",
            "degradation_proxy": (
                "210 USD/kWh replacement-cost proxy, 43.9129 UAH/USD, "
                "15 years, 1 cycle/day"
            ),
            "not_market_execution": True,
        },
        "presets": preset_payloads,
        "summary": _summary_payload(preset_payloads),
        "claim_boundary": PUBLIC_BESS_INDEX_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "proposed_bid_status": "not_emitted",
    }


def build_public_bess_arbitrage_history_payload(
    *,
    latest_payload: Mapping[str, Any],
    previous_history: Mapping[str, Any] | None = None,
    max_days: int = 90,
) -> dict[str, Any]:
    rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in (previous_history or {}).get("rows", []):
        if not isinstance(row, Mapping):
            continue
        delivery_date = str(row.get("delivery_date") or "")
        preset_id = str(row.get("preset_id") or "")
        if delivery_date and preset_id:
            rows_by_key[(delivery_date, preset_id)] = dict(row)

    delivery_date = str((latest_payload.get("source") or {}).get("delivery_date") or "")
    generated_at = str(latest_payload.get("generated_at") or "")
    for preset in latest_payload.get("presets", []):
        if not isinstance(preset, Mapping):
            continue
        metrics_value = preset.get("metrics")
        metrics: Mapping[str, Any] = metrics_value if isinstance(metrics_value, Mapping) else {}
        row = {
            "delivery_date": delivery_date,
            "generated_at": generated_at,
            "preset_id": str(preset.get("preset_id")),
            "label": str(preset.get("label")),
            "net_value_uah": _round(float(metrics.get("net_value_uah", 0.0))),
            "normalized_uah_per_mwh_capacity": _round(
                float(metrics.get("normalized_uah_per_mwh_capacity", 0.0))
            ),
            "equivalent_full_cycles": _round(float(metrics.get("equivalent_full_cycles", 0.0))),
            "throughput_mwh": _round(float(metrics.get("throughput_mwh", 0.0))),
            "claim_boundary": PUBLIC_BESS_INDEX_CLAIM_BOUNDARY,
            "market_execution_enabled": False,
        }
        rows_by_key[(delivery_date, row["preset_id"])] = row

    rows = sorted(rows_by_key.values(), key=lambda row: (row["delivery_date"], row["preset_id"]))
    if max_days > 0:
        delivery_dates = sorted({str(row["delivery_date"]) for row in rows})[-max_days:]
        rows = [row for row in rows if str(row["delivery_date"]) in delivery_dates]

    return {
        "schema_version": "ukraine_bess_arbitrage_index_history.v1",
        "generated_at": generated_at,
        "row_count": len(rows),
        "rows": rows,
        "claim_boundary": PUBLIC_BESS_INDEX_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "proposed_bid_status": "not_emitted",
    }


def _solve_preset_payload(
    *,
    prices: Sequence[PublicPricePoint],
    preset: PublicBatteryPreset,
) -> dict[str, Any]:
    cvxpy = _require_cvxpy()
    horizon = len(prices)
    charge_mw = cvxpy.Variable(horizon, nonneg=True)
    discharge_mw = cvxpy.Variable(horizon, nonneg=True)
    soc_mwh = cvxpy.Variable(horizon + 1)

    charge_efficiency = sqrt(preset.round_trip_efficiency)
    discharge_efficiency = sqrt(preset.round_trip_efficiency)
    price_vector = [point.price_uah_mwh for point in prices]
    throughput_mwh = charge_mw + discharge_mw
    market_value_uah = cvxpy.multiply(price_vector, discharge_mw - charge_mw)
    degradation_penalty_uah = preset.degradation_cost_per_mwh_throughput_uah * throughput_mwh

    objective = cvxpy.Maximize(cvxpy.sum(market_value_uah - degradation_penalty_uah))
    constraints = [
        soc_mwh[0] == preset.initial_soc_mwh,
        soc_mwh[horizon] == preset.initial_soc_mwh,
        soc_mwh[1:] == soc_mwh[:-1] + charge_mw * charge_efficiency - discharge_mw / discharge_efficiency,
        soc_mwh >= preset.soc_min_mwh,
        soc_mwh <= preset.soc_max_mwh,
        charge_mw <= preset.max_power_mw,
        discharge_mw <= preset.max_power_mw,
    ]
    problem = cvxpy.Problem(objective, constraints)
    problem.solve()
    if problem.status not in {cvxpy.OPTIMAL, cvxpy.OPTIMAL_INACCURATE}:
        raise RuntimeError(f"Public BESS index LP did not converge: status={problem.status}")

    charge_values = _as_float_list(charge_mw.value, horizon)
    discharge_values = _as_float_list(discharge_mw.value, horizon)
    soc_values = _as_float_list(soc_mwh.value, horizon + 1)
    schedule = [
        _schedule_point_payload(
            price=price,
            step_index=step_index,
            charge_mw=max(charge_values[step_index], 0.0),
            discharge_mw=max(discharge_values[step_index], 0.0),
            soc_before_mwh=soc_values[step_index],
            soc_after_mwh=soc_values[step_index + 1],
            degradation_cost_per_mwh_throughput_uah=preset.degradation_cost_per_mwh_throughput_uah,
        )
        for step_index, price in enumerate(prices)
    ]
    gross_value = sum(float(point["gross_market_value_uah"]) for point in schedule)
    degradation_penalty = sum(float(point["degradation_penalty_uah"]) for point in schedule)
    net_value = sum(float(point["net_value_uah"]) for point in schedule)
    throughput = sum(float(point["throughput_mwh"]) for point in schedule)
    equivalent_full_cycles = throughput / (2.0 * preset.capacity_mwh)
    return {
        "preset_id": preset.preset_id,
        "label": preset.label,
        "battery": {
            "capacity_mwh": _round(preset.capacity_mwh),
            "capacity_kwh": _round(preset.capacity_mwh * 1000.0),
            "max_power_mw": _round(preset.max_power_mw),
            "max_power_kw": _round(preset.max_power_mw * 1000.0),
            "duration_hours": _round(preset.capacity_mwh / preset.max_power_mw),
            "round_trip_efficiency": preset.round_trip_efficiency,
            "soc_min_fraction": preset.soc_min_fraction,
            "soc_max_fraction": preset.soc_max_fraction,
            "initial_soc_fraction": preset.initial_soc_fraction,
            "initial_soc_mwh": _round(preset.initial_soc_mwh),
            "degradation_cost_per_cycle_uah": _round(preset.degradation_cost_per_cycle_uah),
            "degradation_cost_per_mwh_throughput_uah": _round(
                preset.degradation_cost_per_mwh_throughput_uah
            ),
        },
        "metrics": {
            "gross_market_value_uah": _round(gross_value),
            "degradation_penalty_uah": _round(degradation_penalty),
            "net_value_uah": _round(net_value),
            "normalized_uah_per_mwh_capacity": _round(net_value / preset.capacity_mwh),
            "throughput_mwh": _round(throughput),
            "equivalent_full_cycles": _round(equivalent_full_cycles),
            "final_soc_mwh": _round(soc_values[-1]),
            "charge_hours": sum(1 for point in schedule if float(point["charge_mw"]) > 1e-5),
            "discharge_hours": sum(1 for point in schedule if float(point["discharge_mw"]) > 1e-5),
        },
        "hourly_schedule": schedule,
        "claim_boundary": PUBLIC_BESS_INDEX_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "proposed_bid_status": "not_emitted",
    }


def _schedule_point_payload(
    *,
    price: PublicPricePoint,
    step_index: int,
    charge_mw: float,
    discharge_mw: float,
    soc_before_mwh: float,
    soc_after_mwh: float,
    degradation_cost_per_mwh_throughput_uah: float,
) -> dict[str, Any]:
    throughput = charge_mw + discharge_mw
    gross_market_value = price.price_uah_mwh * (discharge_mw - charge_mw)
    degradation_penalty = degradation_cost_per_mwh_throughput_uah * throughput
    net_value = gross_market_value - degradation_penalty
    return {
        "step_index": step_index,
        "timestamp": _naive_iso(price.timestamp),
        "price_uah_mwh": _round(price.price_uah_mwh),
        "volume_mwh": _round(price.volume_mwh) if price.volume_mwh is not None else None,
        "charge_mw": _round(charge_mw),
        "discharge_mw": _round(discharge_mw),
        "net_power_mw": _round(discharge_mw - charge_mw),
        "soc_before_mwh": _round(soc_before_mwh),
        "soc_after_mwh": _round(soc_after_mwh),
        "throughput_mwh": _round(throughput),
        "gross_market_value_uah": _round(gross_market_value),
        "degradation_penalty_uah": _round(degradation_penalty),
        "net_value_uah": _round(net_value),
    }


def _summary_payload(presets: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not presets:
        return {
            "headline_preset_id": None,
            "headline_net_value_uah": 0.0,
            "headline_normalized_uah_per_mwh_capacity": 0.0,
        }
    headline = presets[0]
    return {
        "headline_preset_id": headline["preset_id"],
        "headline_net_value_uah": headline["metrics"]["net_value_uah"],
        "headline_normalized_uah_per_mwh_capacity": headline["metrics"][
            "normalized_uah_per_mwh_capacity"
        ],
        "preset_count": len(presets),
    }


def _normalize_price_points(
    rows: Sequence[Mapping[str, Any] | PublicPricePoint],
) -> list[PublicPricePoint]:
    points = [_price_point(row) for row in rows]
    if not points:
        raise ValueError("public BESS index requires price rows")
    ordered = sorted(points, key=lambda point: point.timestamp)
    unique_by_timestamp = {point.timestamp: point for point in ordered}
    if len(unique_by_timestamp) != len(ordered):
        raise ValueError("public BESS index requires one row per delivery hour")
    return list(unique_by_timestamp.values())


def _price_point(row: Mapping[str, Any] | PublicPricePoint) -> PublicPricePoint:
    if isinstance(row, PublicPricePoint):
        return row
    timestamp = _datetime_value(row.get("timestamp"))
    return PublicPricePoint(
        timestamp=timestamp,
        price_uah_mwh=_float_value(row.get("price_uah_mwh"), field_name="price_uah_mwh"),
        volume_mwh=_optional_float_value(row.get("volume_mwh")),
        source_url=str(row.get("source_url") or OREE_DAM_RESULTS_URL),
    )


def _complete_delivery_date(prices: Sequence[PublicPricePoint]) -> date:
    if len(prices) != 24:
        raise ValueError("public BESS index requires exactly 24 hourly delivery rows")
    delivery_dates = {point.timestamp.date() for point in prices}
    if len(delivery_dates) != 1:
        raise ValueError("public BESS index requires one delivery date per payload")
    delivery_day = next(iter(delivery_dates))
    expected = [datetime.combine(delivery_day, datetime.min.time()) + timedelta(hours=hour) for hour in range(24)]
    actual = [point.timestamp.replace(tzinfo=None, minute=0, second=0, microsecond=0) for point in prices]
    if actual != expected:
        raise ValueError("public BESS index requires consecutive midnight-to-23:00 hourly rows")
    return delivery_day


def _dominant_source_url(prices: Sequence[PublicPricePoint]) -> str:
    source_urls = [point.source_url for point in prices if point.source_url]
    return source_urls[0] if source_urls else OREE_DAM_RESULTS_URL


def _require_cvxpy() -> Any:
    try:
        import cvxpy
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "cvxpy is required to compute the public BESS arbitrage index."
        ) from error
    return cvxpy


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None, minute=value.minute, second=value.second, microsecond=value.microsecond)
    if isinstance(value, str) and value.strip():
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    raise TypeError("timestamp must be a datetime or ISO datetime string")


def _float_value(value: Any, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise TypeError(f"{field_name} must be numeric")
    numeric = float(str(value).replace(",", "."))
    if not numeric == numeric:
        raise ValueError(f"{field_name} must be finite")
    return numeric


def _optional_float_value(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return _float_value(value, field_name="volume_mwh")


def _as_float_list(values: object, expected_length: int) -> list[float]:
    if values is None:
        raise RuntimeError("Expected solver values, received None.")
    if hasattr(values, "tolist"):
        raw_values = values.tolist()
    elif isinstance(values, Iterable):
        raw_values = list(values)
    else:
        raise TypeError("Expected solver values to be iterable.")
    flattened = [float(item) for item in raw_values]
    if len(flattened) != expected_length:
        raise RuntimeError("Unexpected solver output length.")
    return flattened


def _round(value: float | int | None, digits: int = 6) -> float:
    if value is None:
        return 0.0
    rounded = round(float(value), digits)
    return 0.0 if abs(rounded) < 10 ** -digits else rounded


def _naive_iso(value: datetime) -> str:
    return value.replace(tzinfo=None).isoformat()


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC).isoformat()
    return value.astimezone(UTC).isoformat()
