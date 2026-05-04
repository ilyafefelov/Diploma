"""Hourly DAM baseline solver with strict similar-day forecast and rolling horizon LP."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Literal

import polars as pl

from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics, DispatchCommand

PriceColumn = Literal["price_uah_mwh"]

DEFAULT_PRICE_COLUMN: Final[PriceColumn] = "price_uah_mwh"
DEFAULT_TIMESTAMP_COLUMN: Final[str] = "timestamp"
WEEKDAY_SIMILAR_DAY_LAG_HOURS: Final[int] = 24
SPECIAL_DAY_SIMILAR_DAY_LAG_HOURS: Final[int] = 168
LEVEL1_MARKET_VENUE: Final[str] = "DAM"
LEVEL1_INTERVAL_MINUTES: Final[int] = 60


@dataclass(frozen=True, slots=True)
class BaselineSolverConfig:
    planning_horizon_hours: int = 24
    commit_interval_hours: int = 1
    interval_minutes: int = LEVEL1_INTERVAL_MINUTES
    market_venue: str = LEVEL1_MARKET_VENUE
    numerical_epsilon_mw: float = 1e-6
    solver_name: str | None = None

    def __post_init__(self) -> None:
        if self.planning_horizon_hours <= 0:
            raise ValueError("planning_horizon_hours must be positive.")
        if self.commit_interval_hours <= 0:
            raise ValueError("commit_interval_hours must be positive.")
        if self.interval_minutes != LEVEL1_INTERVAL_MINUTES:
            raise ValueError("Level 1 baseline currently supports only hourly DAM intervals.")
        if self.market_venue != LEVEL1_MARKET_VENUE:
            raise ValueError("Level 1 baseline currently supports only DAM.")


@dataclass(frozen=True, slots=True)
class BaselineForecastPoint:
    forecast_timestamp: datetime
    source_timestamp: datetime
    predicted_price_uah_mwh: float


@dataclass(frozen=True, slots=True)
class BaselineSchedulePoint:
    step_index: int
    interval_start: datetime
    forecast_price_uah_mwh: float
    charge_mw: float
    discharge_mw: float
    soc_before_mwh: float
    soc_after_mwh: float
    throughput_mwh: float
    degradation_penalty_uah: float
    gross_market_value_uah: float
    net_objective_value_uah: float

    @property
    def net_power_mw(self) -> float:
        return self.discharge_mw - self.charge_mw


@dataclass(frozen=True, slots=True)
class BaselineSolveResult:
    anchor_timestamp: datetime
    forecast: list[BaselineForecastPoint]
    schedule: list[BaselineSchedulePoint]
    committed_dispatch: DispatchCommand


class HourlyDamBaselineSolver:
    """Canonical Level 1 LP baseline for hourly DAM operation."""

    def __init__(self, config: BaselineSolverConfig | None = None) -> None:
        self._config = config or BaselineSolverConfig()

    @property
    def config(self) -> BaselineSolverConfig:
        return self._config

    def build_forecast(
        self,
        price_history: pl.DataFrame,
        *,
        anchor_timestamp: datetime | None = None,
        timestamp_column: str = DEFAULT_TIMESTAMP_COLUMN,
        price_column: PriceColumn = DEFAULT_PRICE_COLUMN,
    ) -> list[BaselineForecastPoint]:
        history = _prepare_price_history(price_history, timestamp_column=timestamp_column, price_column=price_column)
        if len(history) < SPECIAL_DAY_SIMILAR_DAY_LAG_HOURS:
            raise ValueError("Strict similar-day forecast requires at least 168 hourly observations.")

        timestamp_to_price = {
            row[timestamp_column]: float(row[price_column])
            for row in history.select(timestamp_column, price_column).iter_rows(named=True)
        }

        latest_timestamp = history.select(timestamp_column).to_series().item(-1)
        if not isinstance(latest_timestamp, datetime):
            raise TypeError("timestamp column must contain datetime values.")
        anchor = anchor_timestamp or latest_timestamp
        latest_price = float(history.select(price_column).to_series().item(-1))

        forecast: list[BaselineForecastPoint] = []
        for step_index in range(self.config.planning_horizon_hours):
            forecast_timestamp = anchor + timedelta(hours=step_index + 1)
            source_timestamp = _resolve_similar_day_timestamp(forecast_timestamp)
            if source_timestamp not in timestamp_to_price:
                raise ValueError(
                    f"Missing similar-day source timestamp {source_timestamp.isoformat()} required for strict naive forecast."
                )
            forecast.append(
                BaselineForecastPoint(
                    forecast_timestamp=forecast_timestamp,
                    source_timestamp=source_timestamp,
                    predicted_price_uah_mwh=float(timestamp_to_price.get(source_timestamp, latest_price)),
                )
            )
        return forecast

    def solve_next_dispatch(
        self,
        price_history: pl.DataFrame,
        *,
        battery_metrics: BatteryPhysicalMetrics,
        current_soc_fraction: float,
        anchor_timestamp: datetime | None = None,
        timestamp_column: str = DEFAULT_TIMESTAMP_COLUMN,
        price_column: PriceColumn = DEFAULT_PRICE_COLUMN,
    ) -> BaselineSolveResult:
        forecast = self.build_forecast(
            price_history,
            anchor_timestamp=anchor_timestamp,
            timestamp_column=timestamp_column,
            price_column=price_column,
        )
        return self.solve_dispatch_from_forecast(
            forecast=forecast,
            battery_metrics=battery_metrics,
            current_soc_fraction=current_soc_fraction,
            anchor_timestamp=anchor_timestamp,
            commit_reason="baseline_rolling_horizon_commit",
        )

    def solve_dispatch_from_forecast(
        self,
        *,
        forecast: list[BaselineForecastPoint],
        battery_metrics: BatteryPhysicalMetrics,
        current_soc_fraction: float,
        anchor_timestamp: datetime | None = None,
        commit_reason: str = "baseline_rolling_horizon_commit",
    ) -> BaselineSolveResult:
        schedule = self._solve_schedule(
            forecast=forecast,
            battery_metrics=battery_metrics,
            current_soc_fraction=current_soc_fraction,
        )

        first_step = schedule[0]
        committed_dispatch = DispatchCommand.from_net_power(
            interval_start=first_step.interval_start,
            duration_minutes=self.config.interval_minutes,
            net_power_mw=first_step.net_power_mw,
            epsilon_mw=self.config.numerical_epsilon_mw,
            reason=commit_reason,
        )
        resolved_anchor_timestamp = anchor_timestamp
        if resolved_anchor_timestamp is None:
            resolved_anchor_timestamp = forecast[0].forecast_timestamp - timedelta(hours=self.config.commit_interval_hours)
        return BaselineSolveResult(
            anchor_timestamp=resolved_anchor_timestamp,
            forecast=forecast,
            schedule=schedule,
            committed_dispatch=committed_dispatch,
        )

    def _solve_schedule(
        self,
        *,
        forecast: list[BaselineForecastPoint],
        battery_metrics: BatteryPhysicalMetrics,
        current_soc_fraction: float,
    ) -> list[BaselineSchedulePoint]:
        cvxpy = _require_cvxpy()

        if not 0.0 <= current_soc_fraction <= 1.0:
            raise ValueError("current_soc_fraction must be between 0.0 and 1.0.")

        horizon = len(forecast)
        if horizon == 0:
            raise ValueError("forecast must contain at least one horizon point.")

        dt_hours = self.config.interval_minutes / 60.0
        charge_efficiency = battery_metrics.round_trip_efficiency ** 0.5
        discharge_efficiency = battery_metrics.round_trip_efficiency ** 0.5
        initial_soc_mwh = current_soc_fraction * battery_metrics.capacity_mwh

        prices = [point.predicted_price_uah_mwh for point in forecast]

        charge_mw = cvxpy.Variable(horizon, nonneg=True)
        discharge_mw = cvxpy.Variable(horizon, nonneg=True)
        soc_mwh = cvxpy.Variable(horizon + 1)

        throughput_mwh = (charge_mw + discharge_mw) * dt_hours
        market_value_uah = cvxpy.multiply(prices, (discharge_mw - charge_mw) * dt_hours)
        degradation_penalty_uah = battery_metrics.degradation_cost_per_mwh_throughput_uah * throughput_mwh

        objective = cvxpy.Maximize(cvxpy.sum(market_value_uah - degradation_penalty_uah))

        constraints = [
            soc_mwh[0] == initial_soc_mwh,
            soc_mwh[1:] == soc_mwh[:-1] + (charge_mw * charge_efficiency * dt_hours) - (discharge_mw * dt_hours / discharge_efficiency),
            soc_mwh >= battery_metrics.soc_min_fraction * battery_metrics.capacity_mwh,
            soc_mwh <= battery_metrics.soc_max_fraction * battery_metrics.capacity_mwh,
            charge_mw <= battery_metrics.max_power_mw,
            discharge_mw <= battery_metrics.max_power_mw,
        ]

        problem = cvxpy.Problem(objective, constraints)
        solve_kwargs: dict[str, object] = {}
        if self.config.solver_name is not None:
            solve_kwargs["solver"] = self.config.solver_name
        problem.solve(**solve_kwargs)

        if problem.status not in {cvxpy.OPTIMAL, cvxpy.OPTIMAL_INACCURATE}:
            raise RuntimeError(f"Baseline LP solve did not converge: status={problem.status}")

        charge_values = _as_float_list(charge_mw.value, horizon)
        discharge_values = _as_float_list(discharge_mw.value, horizon)
        soc_values = _as_float_list(soc_mwh.value, horizon + 1)

        schedule: list[BaselineSchedulePoint] = []
        for step_index, forecast_point in enumerate(forecast):
            charge_value = max(charge_values[step_index], 0.0)
            discharge_value = max(discharge_values[step_index], 0.0)
            throughput_value = (charge_value + discharge_value) * dt_hours
            gross_market_value = forecast_point.predicted_price_uah_mwh * (discharge_value - charge_value) * dt_hours
            degradation_penalty = battery_metrics.degradation_cost_per_mwh_throughput_uah * throughput_value
            schedule.append(
                BaselineSchedulePoint(
                    step_index=step_index,
                    interval_start=forecast_point.forecast_timestamp,
                    forecast_price_uah_mwh=forecast_point.predicted_price_uah_mwh,
                    charge_mw=charge_value,
                    discharge_mw=discharge_value,
                    soc_before_mwh=soc_values[step_index],
                    soc_after_mwh=soc_values[step_index + 1],
                    throughput_mwh=throughput_value,
                    degradation_penalty_uah=degradation_penalty,
                    gross_market_value_uah=gross_market_value,
                    net_objective_value_uah=gross_market_value - degradation_penalty,
                )
            )

        return schedule


def _prepare_price_history(
    price_history: pl.DataFrame,
    *,
    timestamp_column: str,
    price_column: PriceColumn,
) -> pl.DataFrame:
    required_columns = {timestamp_column, price_column}
    missing = required_columns.difference(price_history.columns)
    if missing:
        raise ValueError(f"price_history is missing required columns: {sorted(missing)}")

    history = (
        price_history
        .select(timestamp_column, price_column)
        .drop_nulls()
        .sort(timestamp_column)
        .unique(subset=[timestamp_column], keep="last")
        .sort(timestamp_column)
    )
    if len(history) == 0:
        raise ValueError("price_history must contain at least one non-null row.")
    return history


def _resolve_similar_day_timestamp(target_timestamp: datetime) -> datetime:
    if target_timestamp.weekday() in {1, 2, 3, 4}:
        return target_timestamp - timedelta(hours=WEEKDAY_SIMILAR_DAY_LAG_HOURS)
    return target_timestamp - timedelta(hours=SPECIAL_DAY_SIMILAR_DAY_LAG_HOURS)


def _require_cvxpy() -> Any:
    try:
        import cvxpy
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "cvxpy is required to solve the Level 1 baseline LP. Install project dependencies before calling solve_next_dispatch()."
        ) from error
    return cvxpy


def _as_float_list(values: object, expected_length: int) -> list[float]:
    if values is None:
        raise RuntimeError("Expected solver values, received None.")
    if hasattr(values, "tolist"):
        raw_values = values.tolist()
    elif isinstance(values, Iterable):
        raw_values = list(values)
    else:
        raise RuntimeError("Expected iterable solver values.")
    flattened = [float(item) for item in raw_values]
    if len(flattened) != expected_length:
        raise RuntimeError("Unexpected solver output length.")
    return flattened
