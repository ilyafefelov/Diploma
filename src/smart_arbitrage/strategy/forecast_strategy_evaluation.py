from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
from smart_arbitrage.forecasting.nbeatsx import build_nbeatsx_forecast
from smart_arbitrage.forecasting.neural_features import (
    DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    build_neural_forecast_feature_frame,
)
from smart_arbitrage.forecasting.tft import build_tft_forecast
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
from smart_arbitrage.market_rules import market_rule_for_timestamp

FORECAST_DRIVEN_LP_STRATEGY_KIND = "forecast_driven_lp"
REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND = "real_data_rolling_origin_benchmark"


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
        forecast_diagnostics = _forecast_diagnostics(
            candidate=next(
                candidate
                for candidate in candidates
                if candidate.model_name == model_name
            ),
            actual_prices=actual_prices,
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
                    solve_result=solve_result,
                    actual_prices=actual_prices,
                    forecast_diagnostics=forecast_diagnostics,
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


def evaluate_rolling_origin_forecast_benchmark(
    *,
    price_history: pl.DataFrame,
    tenant_id: str,
    battery_metrics: BatteryPhysicalMetrics,
    starting_soc_fraction: float,
    starting_soc_source: str,
    anchor_timestamps: list[datetime],
    horizon_hours: int = DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    max_anchors: int = 90,
    evaluation_id: str | None = None,
    generated_at: datetime | None = None,
    require_observed_source_rows: bool = True,
) -> pl.DataFrame:
    """Run leakage-free rolling-origin forecast strategy evaluation."""

    if horizon_hours != DEFAULT_NEURAL_FORECAST_HORIZON_HOURS:
        raise ValueError("Real-data benchmark currently supports a 24-hour horizon.")
    if max_anchors <= 0:
        raise ValueError("max_anchors must be positive.")
    if not anchor_timestamps:
        raise ValueError("At least one anchor timestamp is required.")

    benchmark_history = _prepare_benchmark_price_history(
        price_history,
        require_observed_source_rows=require_observed_source_rows,
    )
    selected_anchors = sorted(anchor_timestamps)[-max_anchors:]
    resolved_generated_at = generated_at or datetime.now(UTC)
    resolved_evaluation_id = evaluation_id or _benchmark_evaluation_id(
        tenant_id=tenant_id,
        generated_at=resolved_generated_at,
    )
    data_quality_tier = _data_quality_tier(benchmark_history)

    rows: list[pl.DataFrame] = []
    for anchor_timestamp in selected_anchors:
        anchor_window = _benchmark_window_for_anchor(
            benchmark_history,
            anchor_timestamp=anchor_timestamp,
            horizon_hours=horizon_hours,
        )
        candidates = _rolling_origin_candidates(
            anchor_window,
            anchor_timestamp=anchor_timestamp,
        )
        evaluation = evaluate_forecast_candidates_against_oracle(
            price_history=anchor_window,
            tenant_id=tenant_id,
            battery_metrics=battery_metrics,
            starting_soc_fraction=starting_soc_fraction,
            starting_soc_source=starting_soc_source,
            anchor_timestamp=anchor_timestamp,
            candidates=candidates,
            evaluation_id=f"{resolved_evaluation_id}:{anchor_timestamp.strftime('%Y%m%dT%H%M')}",
            generated_at=resolved_generated_at,
        )
        rows.append(
            _with_benchmark_metadata(
                evaluation,
                data_quality_tier=data_quality_tier,
                observed_coverage_ratio=_observed_coverage_ratio(anchor_window),
                tenant_id=tenant_id,
                anchor_timestamp=anchor_timestamp,
                battery_metrics=battery_metrics,
            )
        )

    return pl.concat(rows, how="diagonal_relaxed").sort(
        ["anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


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


def _prepare_benchmark_price_history(
    price_history: pl.DataFrame,
    *,
    require_observed_source_rows: bool,
) -> pl.DataFrame:
    required_columns = {DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN}
    missing_columns = required_columns.difference(price_history.columns)
    if missing_columns:
        raise ValueError(
            f"price_history is missing required columns: {sorted(missing_columns)}"
        )
    benchmark_history = (
        price_history
        .drop_nulls(subset=[DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN])
        .sort(DEFAULT_TIMESTAMP_COLUMN)
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
    if benchmark_history.height == 0:
        raise ValueError("price_history must contain observed source rows.")
    if require_observed_source_rows:
        if "source_kind" not in benchmark_history.columns:
            raise ValueError("Benchmark price history must identify observed source rows.")
        non_observed_rows = benchmark_history.filter(pl.col("source_kind") != "observed")
        if non_observed_rows.height:
            raise ValueError("Benchmark price history must contain only observed source rows.")
    return benchmark_history


def _benchmark_window_for_anchor(
    price_history: pl.DataFrame,
    *,
    anchor_timestamp: datetime,
    horizon_hours: int,
) -> pl.DataFrame:
    window_end = anchor_timestamp + timedelta(hours=horizon_hours)
    window = price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= window_end)
    forecast_timestamps = [
        anchor_timestamp + timedelta(hours=step_index + 1)
        for step_index in range(horizon_hours)
    ]
    _actual_prices_by_timestamp(window, forecast_timestamps=forecast_timestamps)
    if window.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp).height < 168:
        raise ValueError("Rolling-origin benchmark requires at least 168 past observed rows before each anchor.")
    return window


def _rolling_origin_candidates(
    price_history: pl.DataFrame,
    *,
    anchor_timestamp: datetime,
) -> list[ForecastCandidate]:
    historical_prices = price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp)
    strict_forecast = HourlyDamBaselineSolver().build_forecast(
        historical_prices,
        anchor_timestamp=anchor_timestamp,
    )
    strict_frame = pl.DataFrame(
        {
            "forecast_timestamp": [point.forecast_timestamp for point in strict_forecast],
            "source_timestamp": [point.source_timestamp for point in strict_forecast],
            "predicted_price_uah_mwh": [point.predicted_price_uah_mwh for point in strict_forecast],
        }
    )
    feature_frame = build_neural_forecast_feature_frame(
        price_history,
        future_weather_mode="forecast_only",
    )
    nbeatsx_forecast = build_nbeatsx_forecast(feature_frame)
    tft_forecast = build_tft_forecast(feature_frame)
    return [
        ForecastCandidate(
            model_name="strict_similar_day",
            forecast_frame=strict_frame,
            point_prediction_column="predicted_price_uah_mwh",
        ),
        ForecastCandidate(
            model_name="nbeatsx_silver_v0",
            forecast_frame=nbeatsx_forecast,
            point_prediction_column="predicted_price_uah_mwh",
        ),
        ForecastCandidate(
            model_name="tft_silver_v0",
            forecast_frame=tft_forecast,
            point_prediction_column="predicted_price_p50_uah_mwh",
        ),
    ]


def _with_benchmark_metadata(
    evaluation: pl.DataFrame,
    *,
    data_quality_tier: str,
    observed_coverage_ratio: float,
    tenant_id: str,
    anchor_timestamp: datetime,
    battery_metrics: BatteryPhysicalMetrics,
) -> pl.DataFrame:
    payloads: list[dict[str, Any]] = []
    efc_values: list[float] = []
    for row in evaluation.iter_rows(named=True):
        payload = dict(row["evaluation_payload"])
        total_throughput_mwh = float(row["total_throughput_mwh"])
        efc_proxy = total_throughput_mwh / (2.0 * battery_metrics.capacity_mwh)
        payload.update(
            {
                "benchmark_kind": "real_data_rolling_origin",
                "academic_scope": (
                    "Real-data rolling-origin DAM benchmark: each anchor fits forecasts on past rows only, "
                    "routes candidates through the Level 1 LP, and scores feasible dispatch against realized prices."
                ),
                "data_quality_tier": data_quality_tier,
                "observed_coverage_ratio": observed_coverage_ratio,
                "tenant_id": tenant_id,
                "anchor_timestamp": anchor_timestamp.isoformat(),
                "efc_proxy": efc_proxy,
            }
        )
        payloads.append(payload)
        efc_values.append(efc_proxy)
    return evaluation.with_columns(
        [
            pl.lit(REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND).alias("strategy_kind"),
            pl.Series("evaluation_payload", payloads),
            pl.Series("efc_proxy", efc_values),
        ]
    )


def _data_quality_tier(price_history: pl.DataFrame) -> str:
    if "source_kind" not in price_history.columns:
        return "demo_grade"
    source_kinds = set(str(value) for value in price_history.select("source_kind").to_series().to_list())
    return "thesis_grade" if source_kinds == {"observed"} else "demo_grade"


def _observed_coverage_ratio(price_history: pl.DataFrame) -> float:
    if price_history.height == 0 or "source_kind" not in price_history.columns:
        return 0.0
    observed_rows = price_history.filter(pl.col("source_kind") == "observed").height
    return observed_rows / price_history.height


def _benchmark_evaluation_id(*, tenant_id: str, generated_at: datetime) -> str:
    return f"{tenant_id}:real-data:{generated_at.strftime('%Y%m%dT%H%M%S')}:{uuid4().hex[:8]}"


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


def _forecast_diagnostics(
    *,
    candidate: ForecastCandidate,
    actual_prices: dict[datetime, float],
) -> dict[str, float]:
    rows = list(candidate.forecast_frame.sort("forecast_timestamp").iter_rows(named=True))
    forecast_values: list[float] = []
    actual_values: list[float] = []
    cap_violation_count = 0
    for row in rows:
        forecast_timestamp = row["forecast_timestamp"]
        if not isinstance(forecast_timestamp, datetime):
            raise TypeError("forecast_timestamp column must contain datetime values.")
        raw_forecast_value = float(row[candidate.point_prediction_column])
        forecast_values.append(raw_forecast_value)
        actual_values.append(actual_prices[forecast_timestamp])
        rule = market_rule_for_timestamp(venue="DAM", timestamp=forecast_timestamp)
        if raw_forecast_value < rule.min_price_uah_mwh or raw_forecast_value > rule.max_price_uah_mwh:
            cap_violation_count += 1

    errors = [
        forecast_value - actual_value
        for forecast_value, actual_value in zip(forecast_values, actual_values)
    ]
    diagnostics = {
        "mae_uah_mwh": _mean([abs(error) for error in errors]),
        "rmse_uah_mwh": _mean([error**2 for error in errors]) ** 0.5,
        "smape": _smape(forecast_values=forecast_values, actual_values=actual_values),
        "directional_accuracy": _directional_accuracy(
            forecast_values=forecast_values,
            actual_values=actual_values,
        ),
        "spread_ranking_quality": _rank_correlation(
            forecast_values,
            actual_values,
        ),
        "top_k_price_recall": _top_k_price_recall(
            forecast_values=forecast_values,
            actual_values=actual_values,
            k=min(3, len(actual_values)),
        ),
        "mean_forecast_price_uah_mwh": _mean(forecast_values),
        "mean_actual_price_uah_mwh": _mean(actual_values),
        "price_cap_violation_count": float(cap_violation_count),
    }
    diagnostics.update(_pinball_losses(candidate=candidate, actual_prices=actual_prices))
    return diagnostics


def _pinball_losses(
    *,
    candidate: ForecastCandidate,
    actual_prices: dict[datetime, float],
) -> dict[str, float]:
    quantile_columns = {
        "pinball_loss_p10_uah_mwh": ("predicted_price_p10_uah_mwh", 0.1),
        "pinball_loss_p50_uah_mwh": ("predicted_price_p50_uah_mwh", 0.5),
        "pinball_loss_p90_uah_mwh": ("predicted_price_p90_uah_mwh", 0.9),
    }
    rows = list(candidate.forecast_frame.sort("forecast_timestamp").iter_rows(named=True))
    losses: dict[str, float] = {}
    for metric_name, (column_name, quantile) in quantile_columns.items():
        if column_name not in candidate.forecast_frame.columns:
            continue
        quantile_losses: list[float] = []
        for row in rows:
            forecast_timestamp = row["forecast_timestamp"]
            if not isinstance(forecast_timestamp, datetime):
                raise TypeError("forecast_timestamp column must contain datetime values.")
            error = actual_prices[forecast_timestamp] - float(row[column_name])
            quantile_losses.append(max(quantile * error, (quantile - 1.0) * error))
        losses[metric_name] = _mean(quantile_losses)
    return losses


def _smape(*, forecast_values: list[float], actual_values: list[float]) -> float:
    values: list[float] = []
    for forecast_value, actual_value in zip(forecast_values, actual_values):
        denominator = abs(forecast_value) + abs(actual_value)
        if denominator <= 1e-9:
            values.append(0.0)
        else:
            values.append((2.0 * abs(forecast_value - actual_value)) / denominator)
    return _mean(values)


def _directional_accuracy(*, forecast_values: list[float], actual_values: list[float]) -> float:
    if len(forecast_values) < 2:
        return 0.0
    matches = 0
    comparisons = 0
    for index in range(1, len(forecast_values)):
        forecast_direction = _sign(forecast_values[index] - forecast_values[index - 1])
        actual_direction = _sign(actual_values[index] - actual_values[index - 1])
        matches += 1 if forecast_direction == actual_direction else 0
        comparisons += 1
    return matches / comparisons if comparisons else 0.0


def _rank_correlation(forecast_values: list[float], actual_values: list[float]) -> float:
    if len(forecast_values) < 2:
        return 0.0
    forecast_ranks = _ordinal_ranks(forecast_values)
    actual_ranks = _ordinal_ranks(actual_values)
    forecast_mean = _mean(forecast_ranks)
    actual_mean = _mean(actual_ranks)
    numerator = sum(
        (forecast_rank - forecast_mean) * (actual_rank - actual_mean)
        for forecast_rank, actual_rank in zip(forecast_ranks, actual_ranks)
    )
    forecast_scale = sum((forecast_rank - forecast_mean) ** 2 for forecast_rank in forecast_ranks)
    actual_scale = sum((actual_rank - actual_mean) ** 2 for actual_rank in actual_ranks)
    denominator = (forecast_scale * actual_scale) ** 0.5
    if denominator <= 1e-9:
        return 0.0
    return numerator / denominator


def _top_k_price_recall(*, forecast_values: list[float], actual_values: list[float], k: int) -> float:
    if k <= 0:
        return 0.0
    forecast_top = set(_top_k_indices(forecast_values, k=k))
    actual_top = set(_top_k_indices(actual_values, k=k))
    return len(forecast_top.intersection(actual_top)) / k


def _top_k_indices(values: list[float], *, k: int) -> list[int]:
    return [
        index
        for index, _ in sorted(
            enumerate(values),
            key=lambda item: (item[1], -item[0]),
            reverse=True,
        )[:k]
    ]


def _ordinal_ranks(values: list[float]) -> list[float]:
    ranked = sorted(enumerate(values), key=lambda item: (item[1], item[0]))
    ranks = [0.0 for _ in values]
    for rank, (index, _) in enumerate(ranked, start=1):
        ranks[index] = float(rank)
    return ranks


def _sign(value: float) -> int:
    if value > 0.0:
        return 1
    if value < 0.0:
        return -1
    return 0


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _evaluation_payload(
    *,
    solve_result: BaselineSolveResult,
    actual_prices: dict[datetime, float],
    forecast_diagnostics: dict[str, float],
) -> dict[str, Any]:
    return {
        "academic_scope": (
            "Gold-layer forecast strategy evaluation: forecast candidates are routed through the same LP, "
            "then scored against realized horizon prices and an oracle benchmark. This is not bid submission."
        ),
        "committed_dispatch_preview": solve_result.committed_dispatch.model_dump(
            mode="json"
        ),
        "forecast_diagnostics": forecast_diagnostics,
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
