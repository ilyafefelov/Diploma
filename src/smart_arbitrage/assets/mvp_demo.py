"""Week 2 MVP Dagster demo slice: DAM prices -> forecast -> LP plan -> gatekeeper -> MLflow."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final

import dagster as dg
import polars as pl
from pydantic import ValidationError

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.assets.bronze.market_weather import (
    build_demo_market_price_history,
    build_synthetic_market_price_history,
    enrich_market_price_history_with_weather,
    weather_forecast_bronze,
)
from smart_arbitrage.assets.gold.baseline_solver import (
    DEFAULT_PRICE_COLUMN,
    DEFAULT_TIMESTAMP_COLUMN,
    BaselineForecastPoint,
    BaselineSolveResult,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics, BatteryTelemetry, DispatchCommand
from smart_arbitrage.resources.market_data_store import (
    get_market_data_store,
    market_price_observations_from_frame,
)

DEMO_EXPERIMENT_NAME: Final[str] = "mvp-baseline-demo"
DEMO_HISTORY_HOURS: Final[int] = 15 * 24
DEMO_HORIZON_HOURS: Final[int] = 24
DEMO_BATTERY_CAPACITY_MWH: Final[float] = 10.0
DEMO_BATTERY_MAX_POWER_MW: Final[float] = 2.0
DEMO_BATTERY_ROUND_TRIP_EFFICIENCY: Final[float] = 0.95
DEMO_BATTERY_CAPEX_USD_PER_KWH: Final[float] = 210.0
DEMO_USD_TO_UAH_RATE: Final[float] = 43.9129
DEMO_BATTERY_LIFETIME_YEARS: Final[int] = 15
DEMO_BATTERY_CYCLES_PER_DAY: Final[float] = 1.0
DEMO_BATTERY_LIFETIME_CYCLES: Final[float] = (
    DEMO_BATTERY_LIFETIME_YEARS * 365 * DEMO_BATTERY_CYCLES_PER_DAY
)


def _derive_demo_battery_replacement_cost_uah(capacity_mwh: float) -> float:
    capacity_kwh = capacity_mwh * 1000.0
    return DEMO_BATTERY_CAPEX_USD_PER_KWH * capacity_kwh * DEMO_USD_TO_UAH_RATE


def _derive_demo_degradation_cost_per_cycle_uah(capacity_mwh: float) -> float:
    replacement_cost_uah = _derive_demo_battery_replacement_cost_uah(capacity_mwh)
    return replacement_cost_uah / DEMO_BATTERY_LIFETIME_CYCLES


DEMO_DEGRADATION_COST_PER_CYCLE_UAH: Final[float] = _derive_demo_degradation_cost_per_cycle_uah(
    DEMO_BATTERY_CAPACITY_MWH
)


@dg.asset(
    group_name=taxonomy.BRONZE_MARKET_DATA,
    tags=taxonomy.asset_tags(
        medallion="bronze",
        domain="mvp_demo_market",
        elt_stage="extract_load",
        ml_stage="source_data",
        evidence_scope="demo",
        market_venue="DAM",
    ),
)
def dam_price_history(
    context,
    weather_forecast_bronze: pl.DataFrame,
) -> pl.DataFrame:
    """Hourly DAM price history with live OREE overlay and synthetic fallback."""

    price_history = build_demo_market_price_history(
        history_hours=DEMO_HISTORY_HOURS,
        forecast_hours=DEMO_HORIZON_HOURS,
    )
    price_history = enrich_market_price_history_with_weather(
        price_history,
        weather_forecast_bronze,
    )
    anchor_timestamp = _resolve_demo_anchor(price_history)
    source_values = sorted(str(value) for value in price_history.select("source").unique().to_series().to_list())
    weather_source_values = sorted(
        str(value)
        for value in price_history.select("weather_source").drop_nulls().unique().to_series().to_list()
    )
    market_observations = market_price_observations_from_frame(price_history)
    get_market_data_store().upsert_market_prices(market_observations)
    context.add_output_metadata(
        {
            "source_values": ", ".join(source_values),
            "weather_source_values": ", ".join(weather_source_values),
            "rows": price_history.height,
            "market_observation_rows": len(market_observations),
            "start_timestamp": price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(0).isoformat(),
            "anchor_timestamp": anchor_timestamp.isoformat(),
            "end_timestamp": price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1).isoformat(),
        }
    )
    return price_history


@dg.asset(
    group_name=taxonomy.GOLD_MVP_BATTERY,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="mvp_demo_battery",
        elt_stage="publish",
        ml_stage="source_data",
        evidence_scope="demo",
    ),
)
def demo_battery_physical_metrics(context) -> BatteryPhysicalMetrics:
    """Canonical battery parameters for the week 2 MVP demo."""

    metrics = BatteryPhysicalMetrics(
        capacity_mwh=DEMO_BATTERY_CAPACITY_MWH,
        max_power_mw=DEMO_BATTERY_MAX_POWER_MW,
        round_trip_efficiency=DEMO_BATTERY_ROUND_TRIP_EFFICIENCY,
        degradation_cost_per_cycle_uah=DEMO_DEGRADATION_COST_PER_CYCLE_UAH,
    )
    context.add_output_metadata(
        {
            "capacity_mwh": metrics.capacity_mwh,
            "max_power_mw": metrics.max_power_mw,
            "round_trip_efficiency": metrics.round_trip_efficiency,
            "degradation_cost_per_cycle_uah": metrics.degradation_cost_per_cycle_uah,
            "degradation_cost_per_mwh_throughput_uah": metrics.degradation_cost_per_mwh_throughput_uah,
            "degradation_cost_derivation": (
                "210 USD/kWh * 10,000 kWh * 43.9129 UAH/USD / (15 * 365)"
            ),
            "battery_replacement_cost_uah": _derive_demo_battery_replacement_cost_uah(metrics.capacity_mwh),
            "capex_usd_per_kwh": DEMO_BATTERY_CAPEX_USD_PER_KWH,
            "usd_to_uah_rate": DEMO_USD_TO_UAH_RATE,
            "lifetime_cycles": DEMO_BATTERY_LIFETIME_CYCLES,
        }
    )
    return metrics


@dg.asset(
    group_name=taxonomy.GOLD_MVP_BATTERY,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="mvp_demo_battery",
        elt_stage="publish",
        ml_stage="source_data",
        evidence_scope="demo",
    ),
)
def demo_battery_telemetry(
    context,
    dam_price_history: pl.DataFrame,
) -> BatteryTelemetry:
    """Battery telemetry snapshot used for dispatch-time gatekeeping."""

    telemetry = BatteryTelemetry(
        current_soc=0.52,
        soh=0.97,
        last_updated=_resolve_demo_anchor(dam_price_history),
    )
    context.add_output_metadata(
        {
            "current_soc": telemetry.current_soc,
            "soh": telemetry.soh,
            "last_updated": telemetry.last_updated.isoformat(),
        }
    )
    return telemetry


@dg.asset(
    group_name=taxonomy.SILVER_FORECAST_CANDIDATES,
    tags=taxonomy.asset_tags(
        medallion="silver",
        domain="mvp_demo_forecast",
        elt_stage="transform",
        ml_stage="forecasting",
        evidence_scope="demo",
        market_venue="DAM",
    ),
)
def strict_similar_day_forecast(
    context,
    dam_price_history: pl.DataFrame,
) -> pl.DataFrame:
    """Canonical strict similar-day forecast used by the LP baseline."""

    anchor_timestamp = _resolve_demo_anchor(dam_price_history)
    historical_prices = _historical_prices_for_anchor(dam_price_history, anchor_timestamp)
    solver = HourlyDamBaselineSolver()
    forecast = solver.build_forecast(historical_prices, anchor_timestamp=anchor_timestamp)
    forecast_frame = pl.DataFrame(
        {
            "forecast_timestamp": [point.forecast_timestamp for point in forecast],
            "source_timestamp": [point.source_timestamp for point in forecast],
            "predicted_price_uah_mwh": [point.predicted_price_uah_mwh for point in forecast],
        }
    )
    context.add_output_metadata(
        {
            "forecast_method": "strict_similar_day",
            "horizon_hours": forecast_frame.height,
            "anchor_timestamp": anchor_timestamp.isoformat(),
            "first_forecast_timestamp": forecast_frame.select("forecast_timestamp").to_series().item(0).isoformat(),
        }
    )
    return forecast_frame


@dg.asset(
    group_name=taxonomy.GOLD_MVP_DISPATCH,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="mvp_demo_dispatch",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="demo",
        market_venue="DAM",
    ),
)
def baseline_dispatch_plan(
    context,
    strict_similar_day_forecast: pl.DataFrame,
    demo_battery_physical_metrics: BatteryPhysicalMetrics,
    demo_battery_telemetry: BatteryTelemetry,
) -> BaselineSolveResult:
    """Rolling-horizon LP plan that commits only the next dispatch command."""

    solver = HourlyDamBaselineSolver()
    forecast_points = _forecast_frame_to_points(strict_similar_day_forecast)
    result = solver.solve_dispatch_from_forecast(
        forecast=forecast_points,
        battery_metrics=demo_battery_physical_metrics,
        current_soc_fraction=demo_battery_telemetry.current_soc,
        anchor_timestamp=demo_battery_telemetry.last_updated,
        commit_reason="baseline_rolling_horizon_commit",
    )
    context.add_output_metadata(
        {
            "anchor_timestamp": result.anchor_timestamp.isoformat(),
            "committed_action": result.committed_dispatch.action,
            "committed_power_mw": result.committed_dispatch.power_mw,
            "planning_horizon_hours": len(result.schedule),
            "first_interval_net_objective_uah": result.schedule[0].net_objective_value_uah,
        }
    )
    return result


@dg.asset(
    group_name=taxonomy.GOLD_MVP_GATEKEEPER,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="mvp_demo_gatekeeper",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="demo",
        market_venue="DAM",
    ),
)
def validated_dispatch_command(
    context,
    baseline_dispatch_plan: BaselineSolveResult,
    demo_battery_physical_metrics: BatteryPhysicalMetrics,
    demo_battery_telemetry: BatteryTelemetry,
) -> DispatchCommand:
    """Validated inverter command for the next physical interval."""

    validated_command, validation_status, failure_reason = _validate_dispatch_or_hold(
        baseline_dispatch_plan.committed_dispatch,
        battery_telemetry=demo_battery_telemetry,
        battery_physical_metrics=demo_battery_physical_metrics,
    )
    metadata: dict[str, str | float] = {
        "validation_status": validation_status,
        "action": validated_command.action,
        "power_mw": validated_command.power_mw,
    }
    if failure_reason is not None:
        metadata["failure_reason"] = failure_reason
    context.add_output_metadata(metadata)
    return validated_command


@dg.asset(
    group_name=taxonomy.GOLD_MVP_GATEKEEPER,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="mvp_demo_gatekeeper",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="demo",
        market_venue="DAM",
    ),
)
def blocked_dispatch_command_demo(
    context,
    baseline_dispatch_plan: BaselineSolveResult,
    demo_battery_physical_metrics: BatteryPhysicalMetrics,
) -> DispatchCommand:
    """Intentional safety-failure demo: DISCHARGE at 3% SOC must degrade to HOLD."""

    invalid_command = DispatchCommand(
        interval_start=baseline_dispatch_plan.committed_dispatch.interval_start,
        duration_minutes=baseline_dispatch_plan.committed_dispatch.duration_minutes,
        action="DISCHARGE",
        power_mw=min(1.0, demo_battery_physical_metrics.max_power_mw),
        reason="intentional_low_soc_demo",
    )
    low_soc_telemetry = BatteryTelemetry(
        current_soc=0.03,
        soh=0.97,
        last_updated=baseline_dispatch_plan.anchor_timestamp,
    )
    safe_command, validation_status, failure_reason = _validate_dispatch_or_hold(
        invalid_command,
        battery_telemetry=low_soc_telemetry,
        battery_physical_metrics=demo_battery_physical_metrics,
    )
    metadata: dict[str, str | float] = {
        "validation_status": validation_status,
        "input_soc": low_soc_telemetry.current_soc,
        "output_action": safe_command.action,
        "output_power_mw": safe_command.power_mw,
    }
    if failure_reason is not None:
        metadata["failure_reason"] = failure_reason
    context.add_output_metadata(metadata)
    return safe_command


@dg.asset(
    group_name=taxonomy.GOLD_MVP_BENCHMARK,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="mvp_demo_benchmark",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="demo",
        market_venue="DAM",
    ),
)
def oracle_benchmark_metrics(
    context,
    dam_price_history: pl.DataFrame,
    baseline_dispatch_plan: BaselineSolveResult,
    demo_battery_physical_metrics: BatteryPhysicalMetrics,
    demo_battery_telemetry: BatteryTelemetry,
) -> dict:
    """Perfect-foresight benchmark and regret calculation for the LP baseline."""

    solver = HourlyDamBaselineSolver()
    actual_future_prices = _actual_future_prices(
        dam_price_history,
        anchor_timestamp=baseline_dispatch_plan.anchor_timestamp,
        horizon_hours=len(baseline_dispatch_plan.forecast),
    )
    oracle_forecast = [
        BaselineForecastPoint(
            forecast_timestamp=row[DEFAULT_TIMESTAMP_COLUMN],
            source_timestamp=row[DEFAULT_TIMESTAMP_COLUMN],
            predicted_price_uah_mwh=float(row[DEFAULT_PRICE_COLUMN]),
        )
        for row in actual_future_prices.iter_rows(named=True)
    ]
    oracle_result = solver.solve_dispatch_from_forecast(
        forecast=oracle_forecast,
        battery_metrics=demo_battery_physical_metrics,
        current_soc_fraction=demo_battery_telemetry.current_soc,
        anchor_timestamp=baseline_dispatch_plan.anchor_timestamp,
        commit_reason="oracle_perfect_foresight_commit",
    )

    baseline_net_value_uah = _sum_schedule_net_objective_uah(baseline_dispatch_plan)
    oracle_net_value_uah = _sum_schedule_net_objective_uah(oracle_result)
    regret_uah = oracle_net_value_uah - baseline_net_value_uah
    oracle_denominator = abs(oracle_net_value_uah)
    regret_ratio = regret_uah / oracle_denominator if oracle_denominator > 1e-9 else 0.0

    metrics: dict[str, float | str] = {
        "baseline_net_value_uah": baseline_net_value_uah,
        "oracle_net_value_uah": oracle_net_value_uah,
        "regret_uah": regret_uah,
        "regret_ratio": regret_ratio,
        "baseline_committed_action": baseline_dispatch_plan.committed_dispatch.action,
        "oracle_committed_action": oracle_result.committed_dispatch.action,
    }
    context.add_output_metadata(metrics)
    return metrics


@dg.asset(
    group_name=taxonomy.GOLD_MVP_BENCHMARK,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="mvp_demo_benchmark",
        elt_stage="publish",
        ml_stage="diagnostics",
        evidence_scope="demo",
        market_venue="DAM",
    ),
)
def baseline_regret_tracking(
    context,
    oracle_benchmark_metrics: dict,
    baseline_dispatch_plan: BaselineSolveResult,
) -> dict:
    """MLflow tracking hook for the baseline-vs-oracle regret metrics."""

    tracking_result = _log_regret_metrics_to_mlflow(
        anchor_timestamp=baseline_dispatch_plan.anchor_timestamp,
        metrics=oracle_benchmark_metrics,
    )
    materialized_result = {**oracle_benchmark_metrics, **tracking_result}
    context.add_output_metadata(materialized_result)
    return materialized_result


MVP_DEMO_ASSETS = [
    dam_price_history,
    weather_forecast_bronze,
    demo_battery_physical_metrics,
    demo_battery_telemetry,
    strict_similar_day_forecast,
    baseline_dispatch_plan,
    validated_dispatch_command,
    blocked_dispatch_command_demo,
    oracle_benchmark_metrics,
    baseline_regret_tracking,
]


def _build_simulated_dam_price_history() -> pl.DataFrame:
    return build_synthetic_market_price_history(
        history_hours=DEMO_HISTORY_HOURS,
        forecast_hours=DEMO_HORIZON_HOURS,
    )


def _resolve_demo_anchor(price_history: pl.DataFrame) -> datetime:
    latest_timestamp = price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1)
    if not isinstance(latest_timestamp, datetime):
        raise TypeError("Price history timestamp column must contain datetime values.")
    return latest_timestamp - timedelta(hours=DEMO_HORIZON_HOURS)


def _historical_prices_for_anchor(price_history: pl.DataFrame, anchor_timestamp: datetime) -> pl.DataFrame:
    historical_prices = price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp)
    if historical_prices.height < 168:
        raise ValueError("At least 168 hourly DAM observations are required before the anchor timestamp.")
    return historical_prices


def _actual_future_prices(price_history: pl.DataFrame, *, anchor_timestamp: datetime, horizon_hours: int) -> pl.DataFrame:
    actual_future = (
        price_history
        .filter(
            (pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp)
            & (pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp + timedelta(hours=horizon_hours))
        )
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
    if actual_future.height != horizon_hours:
        raise ValueError("The oracle benchmark requires exactly one observed price per horizon step.")
    return actual_future


def _forecast_frame_to_points(forecast_frame: pl.DataFrame) -> list[BaselineForecastPoint]:
    return [
        BaselineForecastPoint(
            forecast_timestamp=row["forecast_timestamp"],
            source_timestamp=row["source_timestamp"],
            predicted_price_uah_mwh=float(row["predicted_price_uah_mwh"]),
        )
        for row in forecast_frame.iter_rows(named=True)
    ]


def _validate_dispatch_or_hold(
    command: DispatchCommand,
    *,
    battery_telemetry: BatteryTelemetry,
    battery_physical_metrics: BatteryPhysicalMetrics,
) -> tuple[DispatchCommand, str, str | None]:
    try:
        validated_command = DispatchCommand.model_validate(
            command.model_dump(),
            context={
                "battery_telemetry": battery_telemetry,
                "battery_physical_metrics": battery_physical_metrics,
            },
        )
    except ValidationError as error:
        failure_reason = error.errors()[0]["msg"]
        return (
            DispatchCommand.from_net_power(
                interval_start=command.interval_start,
                duration_minutes=command.duration_minutes,
                net_power_mw=0.0,
                reason="gatekeeper_validation_failure",
            ),
            "blocked",
            failure_reason,
        )
    return validated_command, "passed", None


def _sum_schedule_net_objective_uah(result: BaselineSolveResult) -> float:
    return sum(point.net_objective_value_uah for point in result.schedule)


def _log_regret_metrics_to_mlflow(
    *,
    anchor_timestamp: datetime,
    metrics: dict,
) -> dict[str, str | bool]:
    mlflow = _try_import_mlflow()
    if mlflow is None:
        return {
            "mlflow_logged": False,
            "mlflow_tracking_uri": "not_configured",
            "mlflow_note": "Install mlflow to see the regret run in the MLflow UI.",
        }

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if tracking_uri is None:
        tracking_uri = f"file:{(Path.cwd() / 'mlruns').as_posix()}"
        mlflow.set_tracking_uri(tracking_uri)
    else:
        mlflow.set_tracking_uri(tracking_uri)

    mlflow.set_experiment(DEMO_EXPERIMENT_NAME)
    with mlflow.start_run(run_name=f"baseline-{anchor_timestamp.strftime('%Y%m%dT%H%M')}") as run:
        mlflow.log_params(
            {
                "market_venue": "DAM",
                "forecast_method": "strict_similar_day",
                "planning_horizon_hours": DEMO_HORIZON_HOURS,
                "currency": "UAH",
            }
        )
        numeric_metrics = {
            key: float(value)
            for key, value in metrics.items()
            if isinstance(value, (int, float))
        }
        mlflow.log_metrics(numeric_metrics)
        mlflow.set_tag("demo_scope", "week2_mvp")
        mlflow.set_tag("strategy", "lp_baseline")
        return {
            "mlflow_logged": True,
            "mlflow_tracking_uri": tracking_uri,
            "mlflow_run_id": run.info.run_id,
        }


def _try_import_mlflow() -> Any | None:
    try:
        import mlflow
    except ModuleNotFoundError:
        return None
    return mlflow
