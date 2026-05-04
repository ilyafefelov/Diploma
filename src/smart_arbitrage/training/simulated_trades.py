from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import random
from typing import Any, Literal

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import (
    list_available_weather_tenants,
    resolve_tenant_registry_entry,
)
from smart_arbitrage.assets.gold.baseline_solver import (
    DEFAULT_PRICE_COLUMN,
    DEFAULT_TIMESTAMP_COLUMN,
    BaselineForecastPoint,
    BaselineSolverConfig,
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
    BidSide,
    ClearedSegmentAllocation,
    ClearedTrade,
    MARKET_PRICE_CAPS_UAH_PER_MWH,
)
from smart_arbitrage.optimization.projected_battery_state import (
    ScheduledPowerPoint,
    simulate_projected_battery_state,
)


MarketScope = Literal["DAM"]
DAM_PRICE_CAP_UAH_MWH = MARKET_PRICE_CAPS_UAH_PER_MWH["DAM"]


@dataclass(frozen=True, slots=True)
class SimulatedTradeTrainingConfig:
    max_anchors_per_tenant: int = 90
    scenarios_per_anchor: int = 8
    horizon_hours: int = 24
    seed: int = 20260504
    market_venue: MarketScope = "DAM"
    scenario_sigma_fraction: float = 0.06

    def __post_init__(self) -> None:
        if self.max_anchors_per_tenant <= 0:
            raise ValueError("max_anchors_per_tenant must be positive.")
        if self.scenarios_per_anchor <= 0:
            raise ValueError("scenarios_per_anchor must be positive.")
        if self.horizon_hours <= 0:
            raise ValueError("horizon_hours must be positive.")
        if self.market_venue != "DAM":
            raise ValueError("This Level 1 simulated training generator currently supports DAM only.")
        if self.scenario_sigma_fraction < 0.0:
            raise ValueError("scenario_sigma_fraction cannot be negative.")


@dataclass(frozen=True, slots=True)
class SimulatedTradeTrainingResult:
    episode_frame: pl.DataFrame
    transition_frame: pl.DataFrame


def build_simulated_trade_training_data(
    price_history: pl.DataFrame,
    *,
    tenant_ids: list[str] | None = None,
    config: SimulatedTradeTrainingConfig | None = None,
) -> SimulatedTradeTrainingResult:
    resolved_config = config or SimulatedTradeTrainingConfig()
    history = _prepare_price_history(price_history)
    selected_tenant_ids = tenant_ids or _all_tenant_ids()
    episode_rows: list[dict[str, Any]] = []
    transition_rows: list[dict[str, Any]] = []

    for tenant_id in selected_tenant_ids:
        tenant_defaults = _tenant_battery_defaults(tenant_id)
        anchor_timestamps = _select_anchor_timestamps(history, config=resolved_config)
        solver = HourlyDamBaselineSolver(
            BaselineSolverConfig(planning_horizon_hours=resolved_config.horizon_hours)
        )
        for anchor_timestamp in anchor_timestamps:
            historical_prices = history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp)
            baseline_forecast = solver.build_forecast(
                historical_prices,
                anchor_timestamp=anchor_timestamp,
            )
            actual_future_prices = _actual_future_prices(
                history,
                anchor_timestamp=anchor_timestamp,
                horizon_hours=resolved_config.horizon_hours,
            )
            for scenario_index in range(resolved_config.scenarios_per_anchor):
                scenario_forecast = _scenario_forecast(
                    actual_future_prices,
                    seed=resolved_config.seed,
                    tenant_id=tenant_id,
                    anchor_timestamp=anchor_timestamp,
                    scenario_index=scenario_index,
                    sigma_fraction=resolved_config.scenario_sigma_fraction,
                )
                baseline_result = solver.solve_dispatch_from_forecast(
                    forecast=baseline_forecast,
                    battery_metrics=tenant_defaults.metrics,
                    current_soc_fraction=tenant_defaults.initial_soc_fraction,
                    anchor_timestamp=anchor_timestamp,
                    commit_reason="simulated_baseline_policy",
                )
                oracle_result = solver.solve_dispatch_from_forecast(
                    forecast=scenario_forecast,
                    battery_metrics=tenant_defaults.metrics,
                    current_soc_fraction=tenant_defaults.initial_soc_fraction,
                    anchor_timestamp=anchor_timestamp,
                    commit_reason="simulated_oracle_teacher",
                )
                episode_id = _episode_id(
                    tenant_id=tenant_id,
                    anchor_timestamp=anchor_timestamp,
                    scenario_index=scenario_index,
                )
                replay = simulate_projected_battery_state(
                    schedule=[
                        ScheduledPowerPoint(
                            interval_start=point.interval_start,
                            net_power_mw=point.net_power_mw,
                        )
                        for point in baseline_result.schedule
                    ],
                    battery_metrics=tenant_defaults.metrics,
                    starting_soc_fraction=tenant_defaults.initial_soc_fraction,
                )
                scenario_prices_by_timestamp = {
                    point.forecast_timestamp: point.predicted_price_uah_mwh
                    for point in scenario_forecast
                }
                rewards = [
                    scenario_prices_by_timestamp[point.interval_start] * trace_point.feasible_net_power_mw
                    - trace_point.degradation_penalty_uah
                    for point, trace_point in zip(baseline_result.schedule, replay.trace, strict=True)
                ]
                baseline_value_uah = sum(rewards)
                oracle_value_uah = sum(point.net_objective_value_uah for point in oracle_result.schedule)
                regret_uah = max(0.0, oracle_value_uah - baseline_value_uah)
                episode_rows.append(
                    {
                        "episode_id": episode_id,
                        "tenant_id": tenant_id,
                        "market_venue": resolved_config.market_venue,
                        "anchor_timestamp": anchor_timestamp,
                        "scenario_index": scenario_index,
                        "horizon_hours": resolved_config.horizon_hours,
                        "baseline_value_uah": baseline_value_uah,
                        "oracle_value_uah": oracle_value_uah,
                        "regret_uah": regret_uah,
                        "seed": resolved_config.seed,
                    }
                )
                for point, trace_point, reward_uah in zip(
                    baseline_result.schedule,
                    replay.trace,
                    rewards,
                    strict=True,
                ):
                    market_price_uah_mwh = scenario_prices_by_timestamp[point.interval_start]
                    cleared_trade = _simulated_cleared_trade(
                        interval_start=point.interval_start,
                        market_price_uah_mwh=market_price_uah_mwh,
                        feasible_net_power_mw=trace_point.feasible_net_power_mw,
                    )
                    transition_rows.append(
                        {
                            "episode_id": episode_id,
                            "tenant_id": tenant_id,
                            "market_venue": resolved_config.market_venue,
                            "scenario_index": scenario_index,
                            "step_index": point.step_index,
                            "interval_start": point.interval_start,
                            "state_soc_before": trace_point.soc_before_fraction,
                            "state_soc_after": trace_point.soc_after_fraction,
                            "state_soh": max(0.0, 0.97 - trace_point.throughput_mwh / (2.0 * tenant_defaults.metrics.capacity_mwh)),
                            "action": "DISCHARGE" if trace_point.feasible_net_power_mw > 0.0 else "CHARGE" if trace_point.feasible_net_power_mw < 0.0 else "HOLD",
                            "recommended_net_power_mw": point.net_power_mw,
                            "feasible_net_power_mw": trace_point.feasible_net_power_mw,
                            "market_price_uah_mwh": market_price_uah_mwh,
                            "reward_uah": reward_uah,
                            "degradation_penalty_uah": trace_point.degradation_penalty_uah,
                            "baseline_value_uah": baseline_value_uah,
                            "oracle_value_uah": oracle_value_uah,
                            "regret_uah": regret_uah,
                            "cleared_trade_provenance": cleared_trade.provenance,
                            "cleared_trade": cleared_trade.model_dump(mode="json"),
                        }
                    )

    return SimulatedTradeTrainingResult(
        episode_frame=_episode_frame(episode_rows),
        transition_frame=_transition_frame(transition_rows),
    )


@dataclass(frozen=True, slots=True)
class _TenantBatteryDefaults:
    metrics: BatteryPhysicalMetrics
    initial_soc_fraction: float


def _prepare_price_history(price_history: pl.DataFrame) -> pl.DataFrame:
    required_columns = {DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN}
    missing_columns = required_columns.difference(price_history.columns)
    if missing_columns:
        raise ValueError(f"price_history is missing required columns: {sorted(missing_columns)}")
    history = (
        price_history
        .select(DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN)
        .drop_nulls()
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
        .with_columns(
            pl.col(DEFAULT_PRICE_COLUMN)
            .clip(0.0, DAM_PRICE_CAP_UAH_MWH)
            .alias(DEFAULT_PRICE_COLUMN)
        )
    )
    if history.height < 168:
        raise ValueError("simulated trade training requires at least 168 hourly DAM observations.")
    return history


def _all_tenant_ids() -> list[str]:
    tenant_ids = [
        str(tenant["tenant_id"])
        for tenant in list_available_weather_tenants()
        if tenant.get("tenant_id") is not None
    ]
    if not tenant_ids:
        raise ValueError("No tenants are available for simulated trade training.")
    return tenant_ids


def _select_anchor_timestamps(
    history: pl.DataFrame,
    *,
    config: SimulatedTradeTrainingConfig,
) -> list[datetime]:
    timestamp_values = history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().to_list()
    valid_anchor_values = timestamp_values[167 : len(timestamp_values) - config.horizon_hours]
    anchors = [value for value in valid_anchor_values if isinstance(value, datetime)]
    if not anchors:
        raise ValueError("price_history does not contain enough future rows for simulated training anchors.")
    if len(anchors) <= config.max_anchors_per_tenant:
        return anchors
    start_index = len(anchors) - config.max_anchors_per_tenant
    return anchors[start_index:]


def _actual_future_prices(
    history: pl.DataFrame,
    *,
    anchor_timestamp: datetime,
    horizon_hours: int,
) -> pl.DataFrame:
    actual_future = (
        history
        .filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp)
        .head(horizon_hours)
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
    if actual_future.height != horizon_hours:
        raise ValueError("Each simulated episode requires one realized price per horizon step.")
    return actual_future


def _scenario_forecast(
    actual_future_prices: pl.DataFrame,
    *,
    seed: int,
    tenant_id: str,
    anchor_timestamp: datetime,
    scenario_index: int,
    sigma_fraction: float,
) -> list[BaselineForecastPoint]:
    rng = random.Random(f"{seed}:{tenant_id}:{anchor_timestamp.isoformat()}:{scenario_index}")
    forecast: list[BaselineForecastPoint] = []
    for row in actual_future_prices.iter_rows(named=True):
        base_price = float(row[DEFAULT_PRICE_COLUMN])
        if scenario_index == 0 or sigma_fraction == 0.0:
            scenario_price = base_price
        else:
            scenario_price = max(0.0, base_price * (1.0 + rng.gauss(0.0, sigma_fraction)))
        scenario_price = min(DAM_PRICE_CAP_UAH_MWH, scenario_price)
        forecast.append(
            BaselineForecastPoint(
                forecast_timestamp=row[DEFAULT_TIMESTAMP_COLUMN],
                source_timestamp=row[DEFAULT_TIMESTAMP_COLUMN],
                predicted_price_uah_mwh=scenario_price,
            )
        )
    return forecast


def _simulated_cleared_trade(
    *,
    interval_start: datetime,
    market_price_uah_mwh: float,
    feasible_net_power_mw: float,
) -> ClearedTrade:
    cleared_quantity_mw = abs(feasible_net_power_mw)
    side: BidSide = "SELL" if feasible_net_power_mw > 0.0 else "BUY"
    return ClearedTrade(
        provenance="simulated",
        venue="DAM",
        interval_start=interval_start,
        duration_minutes=60,
        market_clearing_price_uah_mwh=market_price_uah_mwh,
        allocations=[
            ClearedSegmentAllocation(
                side=side,
                segment_order=0,
                offered_price_uah_mwh=market_price_uah_mwh,
                offered_quantity_mw=max(cleared_quantity_mw, 1e-6),
                cleared_quantity_mw=cleared_quantity_mw,
            )
        ],
        simulation_sigma=0.0,
    )


def _tenant_battery_defaults(tenant_id: str) -> _TenantBatteryDefaults:
    tenant_entry = resolve_tenant_registry_entry(tenant_id=tenant_id)
    energy_system = tenant_entry.get("energy_system")
    if not isinstance(energy_system, dict):
        raise ValueError(f"Tenant {tenant_id} is missing energy_system.")
    capacity_kwh = _positive_float(energy_system.get("battery_capacity_kwh"), field_name="battery_capacity_kwh")
    max_power_kw = _positive_float(energy_system.get("battery_max_power_kw", capacity_kwh * 0.5), field_name="battery_max_power_kw")
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
    return _TenantBatteryDefaults(
        metrics=metrics,
        initial_soc_fraction=_bounded_float(
            energy_system.get("initial_soc_fraction", 0.52),
            field_name="initial_soc_fraction",
            minimum=0.0,
            maximum=1.0,
        ),
    )


def _degradation_cost_per_cycle_uah(*, energy_system: dict[str, Any], capacity_kwh: float) -> float:
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
    return capex_usd_per_kwh * capacity_kwh * DEMO_USD_TO_UAH_RATE / (lifetime_years * 365.0 * cycles_per_day)


def _positive_float(value: Any, *, field_name: str) -> float:
    parsed_value = _float_value(value, field_name=field_name)
    if parsed_value <= 0.0:
        raise ValueError(f"{field_name} must be positive.")
    return parsed_value


def _bounded_float(value: Any, *, field_name: str, minimum: float, maximum: float) -> float:
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


def _episode_id(*, tenant_id: str, anchor_timestamp: datetime, scenario_index: int) -> str:
    return f"{tenant_id}:{anchor_timestamp.strftime('%Y%m%dT%H%M')}:{scenario_index:03d}"


def _episode_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["tenant_id", "anchor_timestamp", "scenario_index"])


def _transition_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["tenant_id", "episode_id", "step_index"])
