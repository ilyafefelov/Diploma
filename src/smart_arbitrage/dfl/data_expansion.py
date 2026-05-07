"""UA-first data expansion and strict LP action-label helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Final, Literal

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import BaselineForecastPoint, HourlyDamBaselineSolver
from smart_arbitrage.dfl.schemas import DFLActionLabelV1
from smart_arbitrage.strategy.forecast_strategy_evaluation import tenant_battery_defaults_from_registry

CONTROL_MODEL_NAME: Final[Literal["strict_similar_day"]] = "strict_similar_day"
ACTION_LABEL_CLAIM_SCOPE: Final[Literal["dfl_action_label_panel_not_full_dfl"]] = (
    "dfl_action_label_panel_not_full_dfl"
)
EUROPEAN_BRIDGE_CLAIM_SCOPE: Final[str] = "external_validation_roadmap_only"
REQUIRED_COVERAGE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "timestamp",
        "price_uah_mwh",
        "source_kind",
        "weather_source_kind",
    }
)
REQUIRED_EVALUATION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "evaluation_id",
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "market_venue",
        "anchor_timestamp",
        "generated_at",
        "horizon_hours",
        "starting_soc_fraction",
        "decision_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "evaluation_payload",
    }
)


def build_dfl_data_coverage_audit_frame(
    feature_frame: pl.DataFrame,
    benchmark_frame: pl.DataFrame | None = None,
    *,
    tenant_ids: tuple[str, ...],
    target_anchor_count_per_tenant: int = 90,
    required_past_hours: int = 168,
    horizon_hours: int = 24,
    require_observed_sources: bool = True,
) -> pl.DataFrame:
    """Audit tenant-specific observed OREE/Open-Meteo coverage and anchor eligibility."""

    _require_columns(feature_frame, REQUIRED_COVERAGE_COLUMNS, frame_name="feature_frame")
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if target_anchor_count_per_tenant <= 0:
        raise ValueError("target_anchor_count_per_tenant must be positive.")
    if required_past_hours <= 0:
        raise ValueError("required_past_hours must be positive.")
    if horizon_hours <= 0:
        raise ValueError("horizon_hours must be positive.")

    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        tenant_frame = (
            feature_frame
            .filter(pl.col("tenant_id") == tenant_id)
            .drop_nulls(subset=["timestamp", "price_uah_mwh"])
            .unique(subset=["timestamp"], keep="last")
            .sort("timestamp")
        )
        if tenant_frame.height == 0:
            rows.append(
                _empty_coverage_row(
                    tenant_id=tenant_id,
                    target_anchor_count_per_tenant=target_anchor_count_per_tenant,
                )
            )
            continue
        if require_observed_sources:
            _require_observed_oree_rows(tenant_frame, tenant_id=tenant_id)
            _require_observed_open_meteo_rows(tenant_frame, tenant_id=tenant_id)

        timestamps = _datetime_values(tenant_frame["timestamp"].to_list(), field_name="timestamp")
        first_timestamp = timestamps[0]
        last_timestamp = timestamps[-1]
        expected_hour_count = int((last_timestamp - first_timestamp).total_seconds() // 3600) + 1
        available_price_timestamps = set(timestamps)
        weather_timestamps = set(
            _datetime_values(
                tenant_frame
                .filter(pl.col("weather_source_kind") == "observed")
                .select("timestamp")
                .to_series()
                .to_list(),
                field_name="timestamp",
            )
        )
        eligible_anchors = _eligible_daily_anchors(
            available_price_timestamps=available_price_timestamps,
            weather_timestamps=weather_timestamps,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            required_past_hours=required_past_hours,
            horizon_hours=horizon_hours,
        )
        missing_price_hours = max(expected_hour_count - len(available_price_timestamps), 0)
        missing_weather_hours = max(expected_hour_count - len(weather_timestamps), 0)
        latest_benchmark = _latest_benchmark_summary(benchmark_frame, tenant_id=tenant_id)
        rows.append(
            {
                "tenant_id": tenant_id,
                "first_timestamp": first_timestamp,
                "last_timestamp": last_timestamp,
                "price_row_count": tenant_frame.height,
                "weather_observed_row_count": len(weather_timestamps),
                "expected_hour_count": expected_hour_count,
                "missing_price_hours": missing_price_hours,
                "missing_weather_hours": missing_weather_hours,
                "eligible_anchor_count": len(eligible_anchors),
                "target_anchor_count_per_tenant": target_anchor_count_per_tenant,
                "meets_target_anchor_count": len(eligible_anchors) >= target_anchor_count_per_tenant,
                "first_eligible_anchor_timestamp": eligible_anchors[0] if eligible_anchors else None,
                "last_eligible_anchor_timestamp": eligible_anchors[-1] if eligible_anchors else None,
                "latest_benchmark_generated_at": latest_benchmark["latest_generated_at"],
                "latest_benchmark_anchor_count": latest_benchmark["anchor_count"],
                "latest_benchmark_model_count": latest_benchmark["model_count"],
                "price_observed_coverage_ratio": _ratio(tenant_frame.height, expected_hour_count),
                "weather_observed_coverage_ratio": _ratio(len(weather_timestamps), expected_hour_count),
                "data_quality_tier": "thesis_grade"
                if missing_price_hours == 0 and missing_weather_hours == 0
                else "coverage_gap",
                "claim_scope": "ua_observed_dfl_data_coverage_audit",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows).sort("tenant_id")


def build_dfl_action_label_panel_frame(
    evaluation_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_holdout_anchor_count_per_tenant: int = 18,
    require_thesis_grade: bool = True,
) -> pl.DataFrame:
    """Build oracle action-label rows from strict LP/oracle benchmark evidence."""

    _require_columns(evaluation_frame, REQUIRED_EVALUATION_COLUMNS, frame_name="evaluation_frame")
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_holdout_anchor_count_per_tenant <= 0:
        raise ValueError("final_holdout_anchor_count_per_tenant must be positive.")

    rows: list[dict[str, Any]] = []
    strict_rows = _strict_rows_by_anchor(evaluation_frame)
    for tenant_id in tenant_ids:
        for forecast_model_name in forecast_model_names:
            source_rows = (
                evaluation_frame
                .filter(
                    (pl.col("tenant_id") == tenant_id)
                    & (pl.col("forecast_model_name") == forecast_model_name)
                )
                .sort("anchor_timestamp")
            )
            if source_rows.height == 0:
                raise ValueError(f"missing source rows for {tenant_id}/{forecast_model_name}")
            if source_rows.height <= final_holdout_anchor_count_per_tenant:
                raise ValueError(
                    "source rows must exceed final_holdout_anchor_count_per_tenant for "
                    f"{tenant_id}/{forecast_model_name}"
                )
            final_anchor_set = set(
                _datetime_values(
                    source_rows.tail(final_holdout_anchor_count_per_tenant)["anchor_timestamp"].to_list(),
                    field_name="anchor_timestamp",
                )
            )
            for source_row in source_rows.iter_rows(named=True):
                anchor_timestamp = _datetime_value(source_row["anchor_timestamp"], field_name="anchor_timestamp")
                strict_key = (tenant_id, anchor_timestamp)
                if strict_key not in strict_rows:
                    raise ValueError(
                        "Each tenant/anchor must include a strict_similar_day row before building DFL action labels."
                    )
                strict_row = strict_rows[strict_key]
                source_payload = _payload(source_row)
                strict_payload = _payload(strict_row)
                if require_thesis_grade:
                    _require_thesis_grade_observed(source_payload, row_kind="candidate")
                    _require_thesis_grade_observed(strict_payload, row_kind="strict_similar_day")
                is_final_holdout = anchor_timestamp in final_anchor_set
                rows.append(
                    _action_label_from_rows(
                        source_row=source_row,
                        source_payload=source_payload,
                        strict_row=strict_row,
                        strict_payload=strict_payload,
                        is_final_holdout=is_final_holdout,
                    ).model_dump(mode="python")
                )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["tenant_id", "forecast_model_name", "anchor_timestamp"])


def european_dataset_bridge_registry_frame() -> pl.DataFrame:
    """Return research-only external dataset candidates without allowing training use."""

    rows = [
        {
            "source_name": "ENTSO-E Transparency Platform",
            "source_url": "https://www.entsoe.eu/data/transparency-platform/",
            "status": "watch",
            "intended_use": "future_external_validation_and_market_coupling_context",
            "training_use_allowed": False,
            "not_ingested": True,
            "claim_scope": EUROPEAN_BRIDGE_CLAIM_SCOPE,
        },
        {
            "source_name": "Open Power System Data",
            "source_url": "https://data.open-power-system-data.org/time_series/",
            "status": "watch",
            "intended_use": "future_european_hourly_price_load_renewables_context",
            "training_use_allowed": False,
            "not_ingested": True,
            "claim_scope": EUROPEAN_BRIDGE_CLAIM_SCOPE,
        },
        {
            "source_name": "Ember API",
            "source_url": "https://ember-energy.org/data/api/",
            "status": "watch",
            "intended_use": "future_generation_demand_emissions_context",
            "training_use_allowed": False,
            "not_ingested": True,
            "claim_scope": EUROPEAN_BRIDGE_CLAIM_SCOPE,
        },
        {
            "source_name": "Nord Pool Data Portal",
            "source_url": "https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/",
            "status": "watch_restricted",
            "intended_use": "future_restricted_market_data_reference_only_if_access_is_approved",
            "training_use_allowed": False,
            "not_ingested": True,
            "claim_scope": EUROPEAN_BRIDGE_CLAIM_SCOPE,
        },
    ]
    return pl.DataFrame(rows)


def _action_label_from_rows(
    *,
    source_row: dict[str, Any],
    source_payload: dict[str, Any],
    strict_row: dict[str, Any],
    strict_payload: dict[str, Any],
    is_final_holdout: bool,
) -> DFLActionLabelV1:
    horizon_rows = _horizon_rows(source_payload, expected_horizon_hours=int(source_row["horizon_hours"]))
    strict_horizon_rows = _horizon_rows(strict_payload, expected_horizon_hours=int(strict_row["horizon_hours"]))
    interval_starts = [_datetime_value(row["interval_start"], field_name="interval_start") for row in horizon_rows]
    actual_prices = _float_vector(horizon_rows, "actual_price_uah_mwh")
    oracle_result = _oracle_result(
        tenant_id=str(source_row["tenant_id"]),
        anchor_timestamp=_datetime_value(source_row["anchor_timestamp"], field_name="anchor_timestamp"),
        interval_starts=interval_starts,
        actual_prices=actual_prices,
        starting_soc_fraction=float(source_row["starting_soc_fraction"]),
    )
    oracle_signed_dispatch = [point.net_power_mw for point in oracle_result.schedule]
    target_masks = _target_masks(oracle_signed_dispatch)
    return DFLActionLabelV1(
        action_label_id=_action_label_id(source_row),
        evaluation_id=str(source_row["evaluation_id"]),
        strict_baseline_evaluation_id=str(strict_row["evaluation_id"]),
        tenant_id=str(source_row["tenant_id"]),
        anchor_timestamp=_datetime_value(source_row["anchor_timestamp"], field_name="anchor_timestamp"),
        split_name="final_holdout" if is_final_holdout else "train_selection",
        is_final_holdout=is_final_holdout,
        horizon_start=interval_starts[0],
        horizon_end=interval_starts[-1],
        horizon_hours=int(source_row["horizon_hours"]),
        market_venue="DAM",
        currency="UAH",
        forecast_model_name=str(source_row["forecast_model_name"]),
        source_strategy_kind=str(source_row["strategy_kind"]),
        strict_baseline_forecast_model_name=CONTROL_MODEL_NAME,
        target_strategy_name="oracle_lp",
        forecast_price_vector_uah_mwh=_float_vector(horizon_rows, "forecast_price_uah_mwh"),
        actual_price_vector_uah_mwh=actual_prices,
        candidate_signed_dispatch_vector_mw=_float_vector(horizon_rows, "net_power_mw"),
        strict_baseline_signed_dispatch_vector_mw=_float_vector(strict_horizon_rows, "net_power_mw"),
        oracle_signed_dispatch_vector_mw=oracle_signed_dispatch,
        oracle_charge_mw_vector=[point.charge_mw for point in oracle_result.schedule],
        oracle_discharge_mw_vector=[point.discharge_mw for point in oracle_result.schedule],
        oracle_soc_before_mwh_vector=[point.soc_before_mwh for point in oracle_result.schedule],
        oracle_soc_after_mwh_vector=[point.soc_after_mwh for point in oracle_result.schedule],
        oracle_degradation_penalty_vector_uah=[point.degradation_penalty_uah for point in oracle_result.schedule],
        target_charge_mask=target_masks["charge"],
        target_discharge_mask=target_masks["discharge"],
        target_hold_mask=target_masks["hold"],
        candidate_net_value_uah=float(source_row["decision_value_uah"]),
        strict_baseline_net_value_uah=float(strict_row["decision_value_uah"]),
        oracle_net_value_uah=float(source_row["oracle_value_uah"]),
        candidate_regret_uah=float(source_row["regret_uah"]),
        strict_baseline_regret_uah=float(strict_row["regret_uah"]),
        regret_delta_vs_strict_baseline_uah=float(source_row["regret_uah"]) - float(strict_row["regret_uah"]),
        candidate_total_throughput_mwh=float(source_row["total_throughput_mwh"]),
        strict_baseline_total_throughput_mwh=float(strict_row["total_throughput_mwh"]),
        candidate_total_degradation_penalty_uah=float(source_row["total_degradation_penalty_uah"]),
        strict_baseline_total_degradation_penalty_uah=float(strict_row["total_degradation_penalty_uah"]),
        candidate_safety_violation_count=_safety_violation_count(source_payload),
        strict_baseline_safety_violation_count=_safety_violation_count(strict_payload),
        data_quality_tier="thesis_grade",
        observed_coverage_ratio=float(source_payload.get("observed_coverage_ratio", 0.0)),
        claim_scope=ACTION_LABEL_CLAIM_SCOPE,
        not_full_dfl=True,
        not_market_execution=True,
        generated_at=_datetime_value(source_row["generated_at"], field_name="generated_at"),
    )


def _oracle_result(
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
    interval_starts: list[datetime],
    actual_prices: list[float],
    starting_soc_fraction: float,
) -> Any:
    defaults = tenant_battery_defaults_from_registry(tenant_id)
    oracle_forecast = [
        BaselineForecastPoint(
            forecast_timestamp=interval_start,
            source_timestamp=interval_start,
            predicted_price_uah_mwh=actual_prices[index],
        )
        for index, interval_start in enumerate(interval_starts)
    ]
    return HourlyDamBaselineSolver().solve_dispatch_from_forecast(
        forecast=oracle_forecast,
        battery_metrics=defaults.metrics,
        current_soc_fraction=starting_soc_fraction,
        anchor_timestamp=anchor_timestamp,
        commit_reason="dfl_action_label_oracle_lp",
    )


def _eligible_daily_anchors(
    *,
    available_price_timestamps: set[datetime],
    weather_timestamps: set[datetime],
    first_timestamp: datetime,
    last_timestamp: datetime,
    required_past_hours: int,
    horizon_hours: int,
) -> list[datetime]:
    latest_anchor = last_timestamp - timedelta(hours=horizon_hours)
    earliest_anchor = first_timestamp + timedelta(hours=required_past_hours)
    anchors: list[datetime] = []
    candidate_anchor = latest_anchor
    while candidate_anchor >= earliest_anchor:
        required_window_timestamps = [
            candidate_anchor - timedelta(hours=required_past_hours - 1) + timedelta(hours=step_index)
            for step_index in range(required_past_hours + horizon_hours)
        ]
        if all(
            timestamp in available_price_timestamps and timestamp in weather_timestamps
            for timestamp in required_window_timestamps
        ):
            anchors.append(candidate_anchor)
        candidate_anchor -= timedelta(hours=24)
    return sorted(anchors)


def _latest_benchmark_summary(benchmark_frame: pl.DataFrame | None, *, tenant_id: str) -> dict[str, Any]:
    if benchmark_frame is None or benchmark_frame.height == 0:
        return {"latest_generated_at": None, "anchor_count": 0, "model_count": 0}
    if not {"tenant_id", "generated_at", "anchor_timestamp", "forecast_model_name"}.issubset(benchmark_frame.columns):
        return {"latest_generated_at": None, "anchor_count": 0, "model_count": 0}
    tenant_frame = benchmark_frame.filter(pl.col("tenant_id") == tenant_id)
    if tenant_frame.height == 0:
        return {"latest_generated_at": None, "anchor_count": 0, "model_count": 0}
    latest_generated_at = tenant_frame.select("generated_at").max().item()
    latest_frame = tenant_frame.filter(pl.col("generated_at") == latest_generated_at)
    return {
        "latest_generated_at": latest_generated_at,
        "anchor_count": latest_frame.select("anchor_timestamp").n_unique(),
        "model_count": latest_frame.select("forecast_model_name").n_unique(),
    }


def _strict_rows_by_anchor(evaluation_frame: pl.DataFrame) -> dict[tuple[str, datetime], dict[str, Any]]:
    rows: dict[tuple[str, datetime], dict[str, Any]] = {}
    for row in evaluation_frame.filter(pl.col("forecast_model_name") == CONTROL_MODEL_NAME).iter_rows(named=True):
        rows[(str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"))] = row
    return rows


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing_columns = required_columns.difference(frame.columns)
    if missing_columns:
        raise ValueError(f"{frame_name} is missing required columns: {sorted(missing_columns)}")


def _require_observed_oree_rows(frame: pl.DataFrame, *, tenant_id: str) -> None:
    non_observed = frame.filter(pl.col("source_kind") != "observed")
    if non_observed.height:
        raise ValueError(f"DFL data coverage audit requires observed OREE rows for tenant_id={tenant_id}.")


def _require_observed_open_meteo_rows(frame: pl.DataFrame, *, tenant_id: str) -> None:
    non_observed = frame.filter(
        pl.col("weather_source_kind").is_not_null() & (pl.col("weather_source_kind") != "observed")
    )
    if non_observed.height:
        raise ValueError(f"DFL data coverage audit requires observed Open-Meteo rows for tenant_id={tenant_id}.")


def _empty_coverage_row(
    *,
    tenant_id: str,
    target_anchor_count_per_tenant: int,
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "first_timestamp": None,
        "last_timestamp": None,
        "price_row_count": 0,
        "weather_observed_row_count": 0,
        "expected_hour_count": 0,
        "missing_price_hours": 0,
        "missing_weather_hours": 0,
        "eligible_anchor_count": 0,
        "target_anchor_count_per_tenant": target_anchor_count_per_tenant,
        "meets_target_anchor_count": False,
        "first_eligible_anchor_timestamp": None,
        "last_eligible_anchor_timestamp": None,
        "latest_benchmark_generated_at": None,
        "latest_benchmark_anchor_count": 0,
        "latest_benchmark_model_count": 0,
        "price_observed_coverage_ratio": 0.0,
        "weather_observed_coverage_ratio": 0.0,
        "data_quality_tier": "coverage_gap",
        "claim_scope": "ua_observed_dfl_data_coverage_audit",
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row["evaluation_payload"]
    if not isinstance(payload, dict):
        raise ValueError("evaluation_payload must be a mapping.")
    return payload


def _require_thesis_grade_observed(payload: dict[str, Any], *, row_kind: str) -> None:
    if str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade":
        raise ValueError(f"DFL action labels require thesis_grade {row_kind} rows.")
    if float(payload.get("observed_coverage_ratio", 0.0)) < 1.0:
        raise ValueError(f"DFL action labels require observed coverage ratio of 1.0 for {row_kind} rows.")


def _horizon_rows(payload: dict[str, Any], *, expected_horizon_hours: int) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        raise ValueError("evaluation_payload must include a horizon list.")
    rows = [item for item in horizon if isinstance(item, dict)]
    if len(rows) != expected_horizon_hours:
        raise ValueError("evaluation_payload horizon length must match horizon_hours.")
    required_keys = {
        "interval_start",
        "forecast_price_uah_mwh",
        "actual_price_uah_mwh",
        "net_power_mw",
    }
    for row in rows:
        missing_keys = required_keys.difference(row)
        if missing_keys:
            raise ValueError(f"evaluation_payload horizon row is missing keys: {sorted(missing_keys)}")
    return rows


def _float_vector(rows: list[dict[str, Any]], key: str) -> list[float]:
    return [float(row[key]) for row in rows]


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"{field_name} must be a datetime value.")


def _datetime_values(values: list[Any], *, field_name: str) -> list[datetime]:
    return sorted(_datetime_value(value, field_name=field_name) for value in values)


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _safety_violation_count(payload: dict[str, Any]) -> int:
    if "safety_violation_count" in payload:
        return int(payload["safety_violation_count"])
    violations = payload.get("safety_violations")
    if isinstance(violations, list):
        return len(violations)
    return 0


def _target_masks(signed_dispatch_vector_mw: list[float]) -> dict[str, list[int]]:
    charge: list[int] = []
    discharge: list[int] = []
    hold: list[int] = []
    for value in signed_dispatch_vector_mw:
        if value < -1e-6:
            charge.append(1)
            discharge.append(0)
            hold.append(0)
        elif value > 1e-6:
            charge.append(0)
            discharge.append(1)
            hold.append(0)
        else:
            charge.append(0)
            discharge.append(0)
            hold.append(1)
    return {"charge": charge, "discharge": discharge, "hold": hold}


def _action_label_id(row: dict[str, Any]) -> str:
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    return f"{row['tenant_id']}:{row['forecast_model_name']}:{anchor_timestamp.strftime('%Y%m%dT%H%M')}:action-label-v1"
