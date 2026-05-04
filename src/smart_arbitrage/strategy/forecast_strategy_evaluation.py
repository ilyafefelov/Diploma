from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import resolve_tenant_registry_entry
from smart_arbitrage.assets.gold.baseline_solver import (
    DEFAULT_PRICE_COLUMN,
    DEFAULT_TIMESTAMP_COLUMN,
    LEVEL1_INTERVAL_MINUTES,
    LEVEL1_MARKET_VENUE,
    BaselineForecastPoint,
    BaselineSolveResult,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.assets.mvp_demo import (
    DEMO_BATTERY_CAPEX_USD_PER_KWH,
    DEMO_BATTERY_CYCLES_PER_DAY,
    DEMO_BATTERY_LIFETIME_YEARS,
    DEMO_USD_TO_UAH_RATE,
)
from smart_arbitrage.gatekeeper.schemas import (
    BatteryPhysicalMetrics,
    MARKET_PRICE_CAPS_UAH_PER_MWH,
)

FORECAST_DRIVEN_LP_STRATEGY_KIND = "forecast_driven_lp"


@dataclass(frozen=True, slots=True)
class ForecastCandidate:
    model_name: str
    forecast_frame: pl.DataFrame
    point_prediction_column: str


@dataclass(frozen=True, slots=True)
class ForecastStrategyTenantDefaults:
    metrics: BatteryPhysicalMetrics
    initial_soc_fraction: float


def evaluate_forecast_candidates_against_oracle(
    *,
    price_history: pl.DataFrame,
    tenant_id: str,
    battery_metrics: BatteryPhysicalMetrics,
    starting_soc_fraction: float,
    starting_soc_source: str,
    anchor_timestamp: datetime,
    candidates: list[ForecastCandidate],
    evaluation_id: str | None = None,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Evaluate Silver forecast candidates through the same LP and oracle benchmark."""

    if not candidates:
        raise ValueError("At least one forecast candidate is required.")
    if not 0.0 <= starting_soc_fraction <= 1.0:
        raise ValueError("starting_soc_fraction must be between 0.0 and 1.0.")

    resolved_evaluation_id = evaluation_id or _evaluation_id(
        tenant_id=tenant_id, anchor_timestamp=anchor_timestamp
    )
    resolved_generated_at = generated_at or datetime.now(UTC)
    candidate_points = {
        candidate.model_name: _forecast_points_from_candidate(candidate)
        for candidate in candidates
    }
    forecast_timestamps = _shared_forecast_timestamps(candidate_points)
    actual_prices = _actual_prices_by_timestamp(
        price_history, forecast_timestamps=forecast_timestamps
    )
    oracle_forecast = [
        BaselineForecastPoint(
            forecast_timestamp=forecast_timestamp,
            source_timestamp=forecast_timestamp,
            predicted_price_uah_mwh=actual_prices[forecast_timestamp],
        )
        for forecast_timestamp in forecast_timestamps
    ]
    solver = HourlyDamBaselineSolver()
    oracle_result = solver.solve_dispatch_from_forecast(
        forecast=oracle_forecast,
        battery_metrics=battery_metrics,
        current_soc_fraction=starting_soc_fraction,
        anchor_timestamp=anchor_timestamp,
        commit_reason="gold_oracle_strategy_evaluation",
    )
    oracle_value_uah = _actual_decision_value_uah(oracle_result, actual_prices)

    rows: list[dict[str, Any]] = []
    for model_name, forecast_points in candidate_points.items():
        solve_result = solver.solve_dispatch_from_forecast(
            forecast=forecast_points,
            battery_metrics=battery_metrics,
            current_soc_fraction=starting_soc_fraction,
            anchor_timestamp=anchor_timestamp,
            commit_reason=f"gold_forecast_strategy_evaluation:{model_name}",
        )
        decision_value_uah = _actual_decision_value_uah(solve_result, actual_prices)
        forecast_objective_value_uah = _forecast_objective_value_uah(solve_result)
        regret_uah = max(0.0, oracle_value_uah - decision_value_uah)
        regret_ratio = (
            regret_uah / abs(oracle_value_uah) if abs(oracle_value_uah) > 1e-9 else 0.0
        )
        rows.append(
            {
                "evaluation_id": resolved_evaluation_id,
                "tenant_id": tenant_id,
                "forecast_model_name": model_name,
                "strategy_kind": FORECAST_DRIVEN_LP_STRATEGY_KIND,
                "market_venue": LEVEL1_MARKET_VENUE,
                "anchor_timestamp": anchor_timestamp,
                "generated_at": resolved_generated_at,
                "horizon_hours": len(forecast_points),
                "starting_soc_fraction": starting_soc_fraction,
                "starting_soc_source": starting_soc_source,
                "decision_value_uah": decision_value_uah,
                "forecast_objective_value_uah": forecast_objective_value_uah,
                "oracle_value_uah": oracle_value_uah,
                "regret_uah": regret_uah,
                "regret_ratio": regret_ratio,
                "total_degradation_penalty_uah": sum(
                    point.degradation_penalty_uah for point in solve_result.schedule
                ),
                "total_throughput_mwh": sum(
                    point.throughput_mwh for point in solve_result.schedule
                ),
                "committed_action": solve_result.committed_dispatch.action,
                "committed_power_mw": solve_result.committed_dispatch.power_mw,
                "rank_by_regret": 0,
                "evaluation_payload": _evaluation_payload(
                    solve_result=solve_result, actual_prices=actual_prices
                ),
            }
        )

    for rank, row in enumerate(
        sorted(
            rows, key=lambda item: (item["regret_uah"], item["forecast_model_name"])
        ),
        start=1,
    ):
        row["rank_by_regret"] = rank

    return pl.DataFrame(rows).sort(["rank_by_regret", "forecast_model_name"])


def tenant_battery_defaults_from_registry(
    tenant_id: str,
) -> ForecastStrategyTenantDefaults:
    tenant_entry = resolve_tenant_registry_entry(tenant_id=tenant_id)
    energy_system = tenant_entry.get("energy_system")
    if not isinstance(energy_system, dict):
        raise ValueError(f"Tenant {tenant_id} is missing energy_system.")
    capacity_kwh = _positive_float(
        energy_system.get("battery_capacity_kwh"), field_name="battery_capacity_kwh"
    )
    max_power_kw = _positive_float(
        energy_system.get("battery_max_power_kw", capacity_kwh * 0.5),
        field_name="battery_max_power_kw",
    )
    metrics = BatteryPhysicalMetrics(
        capacity_mwh=capacity_kwh / 1000.0,
        max_power_mw=max_power_kw / 1000.0,
        round_trip_efficiency=_bounded_float(
            energy_system.get("round_trip_efficiency", 0.92),
            field_name="round_trip_efficiency",
            minimum=0.0,
            maximum=1.0,
        ),
        degradation_cost_per_cycle_uah=_degradation_cost_per_cycle_uah(
            energy_system=energy_system,
            capacity_kwh=capacity_kwh,
        ),
        soc_min_fraction=_bounded_float(
            energy_system.get("soc_min_fraction", 0.05),
            field_name="soc_min_fraction",
            minimum=0.0,
            maximum=1.0,
        ),
        soc_max_fraction=_bounded_float(
            energy_system.get("soc_max_fraction", 0.95),
            field_name="soc_max_fraction",
            minimum=0.0,
            maximum=1.0,
        ),
    )
    return ForecastStrategyTenantDefaults(
        metrics=metrics,
        initial_soc_fraction=_bounded_float(
            energy_system.get("initial_soc_fraction", 0.52),
            field_name="initial_soc_fraction",
            minimum=0.0,
            maximum=1.0,
        ),
    )


def _forecast_points_from_candidate(
    candidate: ForecastCandidate,
) -> list[BaselineForecastPoint]:
    required_columns = {"forecast_timestamp", candidate.point_prediction_column}
    missing_columns = required_columns.difference(candidate.forecast_frame.columns)
    if missing_columns:
        raise ValueError(
            f"{candidate.model_name} forecast is missing required columns: {sorted(missing_columns)}"
        )
    forecast_points: list[BaselineForecastPoint] = []
    for row in candidate.forecast_frame.sort("forecast_timestamp").iter_rows(
        named=True
    ):
        forecast_timestamp = row["forecast_timestamp"]
        if not isinstance(forecast_timestamp, datetime):
            raise TypeError("forecast_timestamp column must contain datetime values.")
        source_timestamp = row.get("source_timestamp", forecast_timestamp)
        if not isinstance(source_timestamp, datetime):
            source_timestamp = forecast_timestamp
        forecast_points.append(
            BaselineForecastPoint(
                forecast_timestamp=forecast_timestamp,
                source_timestamp=source_timestamp,
                predicted_price_uah_mwh=_dam_price(
                    float(row[candidate.point_prediction_column])
                ),
            )
        )
    if not forecast_points:
        raise ValueError(
            f"{candidate.model_name} forecast must contain at least one row."
        )
    return forecast_points


def _shared_forecast_timestamps(
    candidate_points: dict[str, list[BaselineForecastPoint]],
) -> list[datetime]:
    first_model_name = next(iter(candidate_points))
    reference_timestamps = [
        point.forecast_timestamp for point in candidate_points[first_model_name]
    ]
    for model_name, points in candidate_points.items():
        timestamps = [point.forecast_timestamp for point in points]
        if timestamps != reference_timestamps:
            raise ValueError(
                f"{model_name} forecast horizon does not align with {first_model_name}."
            )
    return reference_timestamps


def _actual_prices_by_timestamp(
    price_history: pl.DataFrame, *, forecast_timestamps: list[datetime]
) -> dict[datetime, float]:
    required_columns = {DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN}
    missing_columns = required_columns.difference(price_history.columns)
    if missing_columns:
        raise ValueError(
            f"price_history is missing required columns: {sorted(missing_columns)}"
        )
    price_rows = (
        price_history.select(DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN)
        .drop_nulls()
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
    price_by_timestamp = {
        row[DEFAULT_TIMESTAMP_COLUMN]: _dam_price(float(row[DEFAULT_PRICE_COLUMN]))
        for row in price_rows.iter_rows(named=True)
    }
    missing_timestamps = [
        timestamp
        for timestamp in forecast_timestamps
        if timestamp not in price_by_timestamp
    ]
    if missing_timestamps:
        raise ValueError(
            "price_history is missing actual prices for the forecast horizon."
        )
    return {
        timestamp: price_by_timestamp[timestamp] for timestamp in forecast_timestamps
    }


def _actual_decision_value_uah(
    result: BaselineSolveResult, actual_prices: dict[datetime, float]
) -> float:
    value = 0.0
    for point in result.schedule:
        actual_price = actual_prices[point.interval_start]
        value += (
            actual_price * point.net_power_mw * (LEVEL1_INTERVAL_MINUTES / 60.0)
        ) - point.degradation_penalty_uah
    return value


def _forecast_objective_value_uah(result: BaselineSolveResult) -> float:
    return sum(point.net_objective_value_uah for point in result.schedule)


def _evaluation_payload(
    *, solve_result: BaselineSolveResult, actual_prices: dict[datetime, float]
) -> dict[str, Any]:
    return {
        "academic_scope": (
            "Gold-layer forecast strategy evaluation: forecast candidates are routed through the same LP, "
            "then scored against realized horizon prices and an oracle benchmark. This is not bid submission."
        ),
        "committed_dispatch_preview": solve_result.committed_dispatch.model_dump(
            mode="json"
        ),
        "horizon": [
            {
                "step_index": point.step_index,
                "interval_start": point.interval_start.isoformat(),
                "forecast_price_uah_mwh": point.forecast_price_uah_mwh,
                "actual_price_uah_mwh": actual_prices[point.interval_start],
                "net_power_mw": point.net_power_mw,
                "degradation_penalty_uah": point.degradation_penalty_uah,
            }
            for point in solve_result.schedule
        ],
    }


def _evaluation_id(*, tenant_id: str, anchor_timestamp: datetime) -> str:
    return f"{tenant_id}:{anchor_timestamp.strftime('%Y%m%dT%H%M')}:{uuid4().hex[:8]}"


def _dam_price(value: float) -> float:
    return max(0.0, min(MARKET_PRICE_CAPS_UAH_PER_MWH["DAM"], value))


def _degradation_cost_per_cycle_uah(
    *, energy_system: dict[str, Any], capacity_kwh: float
) -> float:
    capex_usd_per_kwh = _positive_float(
        energy_system.get("battery_capex_usd_per_kwh", DEMO_BATTERY_CAPEX_USD_PER_KWH),
        field_name="battery_capex_usd_per_kwh",
    )
    lifetime_years = _positive_float(
        energy_system.get("battery_lifetime_years", DEMO_BATTERY_LIFETIME_YEARS),
        field_name="battery_lifetime_years",
    )
    cycles_per_day = _positive_float(
        energy_system.get("battery_cycles_per_day", DEMO_BATTERY_CYCLES_PER_DAY),
        field_name="battery_cycles_per_day",
    )
    return (
        capex_usd_per_kwh
        * capacity_kwh
        * DEMO_USD_TO_UAH_RATE
        / (lifetime_years * 365.0 * cycles_per_day)
    )


def _positive_float(value: Any, *, field_name: str) -> float:
    parsed_value = _float_value(value, field_name=field_name)
    if parsed_value <= 0.0:
        raise ValueError(f"{field_name} must be positive.")
    return parsed_value


def _bounded_float(
    value: Any, *, field_name: str, minimum: float, maximum: float
) -> float:
    parsed_value = _float_value(value, field_name=field_name)
    if not minimum <= parsed_value <= maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}.")
    return parsed_value


def _float_value(value: Any, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric.")
    try:
        return float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must be numeric.") from error
