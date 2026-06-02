"""LP-free live candidate templates for HF safe-switch shadow preview."""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

import numpy as np

from smart_arbitrage.assets.gold.baseline_solver import BaselineForecastPoint
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics

LIVE_SAFE_SWITCH_CANDIDATE_FAMILIES: Final[tuple[str, ...]] = (
    "raw_reference",
    "schedule_value_learner_v2_plus",
    "schedule_value_learner_v2_plus_reference",
    "strict_reference",
)


@dataclass(frozen=True)
class LiveSafeSwitchTemplateSpec:
    """Deterministic LP-free dispatch template parameters."""

    active_hour_count: int
    power_fraction: float


DEFAULT_LIVE_SAFE_SWITCH_TEMPLATE_SPECS: Final[dict[str, LiveSafeSwitchTemplateSpec]] = {
    "raw_reference": LiveSafeSwitchTemplateSpec(active_hour_count=4, power_fraction=0.5),
    "schedule_value_learner_v2_plus": LiveSafeSwitchTemplateSpec(
        active_hour_count=0,
        power_fraction=0.0,
    ),
    "schedule_value_learner_v2_plus_reference": LiveSafeSwitchTemplateSpec(
        active_hour_count=3,
        power_fraction=0.35,
    ),
    "schedule_value_learner_v2_reference": LiveSafeSwitchTemplateSpec(
        active_hour_count=3,
        power_fraction=0.35,
    ),
    "strict_reference": LiveSafeSwitchTemplateSpec(active_hour_count=2, power_fraction=0.25),
}

DEFAULT_TEMPLATE_GRIDS: Final[tuple[str, ...]] = ("default", "conservative")
SUPPORTED_TEMPLATE_GRIDS: Final[tuple[str, ...]] = (
    *DEFAULT_TEMPLATE_GRIDS,
    "candidate_library_v2",
    "candidate_library_value_aligned",
    "candidate_library_forecast_guarded",
)


def template_grid_specs(
    template_grid_id: str,
) -> dict[str, LiveSafeSwitchTemplateSpec] | None:
    """Return an LP-free template-grid override for live HF audit/preview paths."""

    normalized = template_grid_id.strip().lower()
    if normalized == "default":
        return None
    if normalized == "conservative":
        return {
            "strict_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=2,
                power_fraction=0.2,
            ),
            "schedule_value_learner_v2_plus_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=2,
                power_fraction=0.25,
            ),
            "schedule_value_learner_v2_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=2,
                power_fraction=0.25,
            ),
        }
    if normalized == "candidate_library_v2":
        return {
            "raw_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=4,
                power_fraction=0.32,
            ),
            "schedule_value_learner_v2_plus": LiveSafeSwitchTemplateSpec(
                active_hour_count=0,
                power_fraction=0.0,
            ),
            "schedule_value_learner_v2_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=4,
                power_fraction=0.30,
            ),
            "strict_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=2,
                power_fraction=0.35,
            ),
        }
    if normalized == "candidate_library_value_aligned":
        return {
            "raw_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=3,
                power_fraction=0.28,
            ),
            "schedule_value_learner_v2_plus": LiveSafeSwitchTemplateSpec(
                active_hour_count=0,
                power_fraction=0.0,
            ),
            "schedule_value_learner_v2_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=3,
                power_fraction=0.28,
            ),
            "strict_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=4,
                power_fraction=0.30,
            ),
        }
    if normalized == "candidate_library_forecast_guarded":
        return {
            "raw_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=2,
                power_fraction=0.18,
            ),
            "schedule_value_learner_v2_plus": LiveSafeSwitchTemplateSpec(
                active_hour_count=2,
                power_fraction=0.12,
            ),
            "schedule_value_learner_v2_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=2,
                power_fraction=0.18,
            ),
            "strict_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=3,
                power_fraction=0.2,
            ),
        }
    raise ValueError(f"Unsupported template_grid: {template_grid_id!r}.")


def build_hf_live_safe_switch_candidate_rows(
    *,
    tenant_id: str,
    source_model_name: str,
    anchor_timestamp: datetime,
    forecast: Sequence[BaselineForecastPoint],
    battery_metrics: BatteryPhysicalMetrics,
    starting_soc_fraction: float,
    candidate_families: Sequence[str] = LIVE_SAFE_SWITCH_CANDIDATE_FAMILIES,
    template_specs: Mapping[str, LiveSafeSwitchTemplateSpec] | None = None,
) -> list[dict[str, Any]]:
    """Build deterministic, solver-free candidate rows for live HF scoring."""

    if not forecast:
        raise ValueError("forecast must contain at least one point.")
    prices = [float(point.predicted_price_uah_mwh) for point in forecast]
    horizon = len(prices)
    resolved_specs = dict(DEFAULT_LIVE_SAFE_SWITCH_TEMPLATE_SPECS)
    if template_specs is not None:
        resolved_specs.update({str(family): spec for family, spec in template_specs.items()})
    rows: list[dict[str, Any]] = []
    for candidate_index, family in enumerate(candidate_families):
        spec = resolved_specs.get(
            str(family),
            LiveSafeSwitchTemplateSpec(active_hour_count=0, power_fraction=0.0),
        )
        requested_dispatch = _template_dispatch(
            prices=prices,
            active_hour_count=spec.active_hour_count,
            max_power_mw=float(battery_metrics.max_power_mw) * spec.power_fraction,
        )
        dispatch, soc_values, clip_count = _safe_dispatch_from_requests(
            requested_dispatch=requested_dispatch,
            battery_metrics=battery_metrics,
            starting_soc_fraction=starting_soc_fraction,
        )
        throughput_mwh = float(sum(abs(value) for value in dispatch))
        degradation_uah = (
            throughput_mwh
            * float(battery_metrics.degradation_cost_per_mwh_throughput_uah)
        )
        forecast_objective_uah = float(
            sum(price * power for price, power in zip(prices, dispatch, strict=True))
            - degradation_uah
        )
        candidate_id = (
            f"{tenant_id}|{source_model_name}|{anchor_timestamp.isoformat()}|"
            f"{family}|live"
        )
        rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "anchor_timestamp": anchor_timestamp,
                "split_name": "live_shadow",
                "horizon_hours": horizon,
                "forecast_price_uah_mwh_vector": prices,
                "dispatch_mw_vector": dispatch,
                "soc_fraction_vector": soc_values,
                "forecast_spread_uah_mwh": float(max(prices) - min(prices)),
                "soc_min_slack_fraction": float(
                    min(soc_values) - float(battery_metrics.soc_min_fraction)
                ),
                "total_throughput_mwh": throughput_mwh,
                "total_degradation_penalty_uah": degradation_uah,
                "forecast_objective_value_uah": forecast_objective_uah,
                "schedule_value_uah": forecast_objective_uah,
                "decision_value_uah": forecast_objective_uah,
                "safety_violation_count": 0,
                "template_clip_count": clip_count,
                "dt_candidate_index_target": candidate_index,
                "dt_candidate_id_target": candidate_id,
                "dt_schedule_family_target": family,
                "teacher_anchor_candidate_count": len(candidate_families),
                "market_execution_enabled": False,
                "promotion_gate_passed": False,
                "market_execution_gate_passed": False,
                "dt_lava_ready": False,
                "permits_model_training": False,
                "not_market_execution": True,
                "research_shadow_not_promotable": True,
                "raw_hourly_action_imitation": False,
            }
        )
    return rows


def _template_dispatch(
    *,
    prices: Sequence[float],
    active_hour_count: int,
    max_power_mw: float,
) -> list[float]:
    if active_hour_count <= 0 or max_power_mw <= 0.0:
        return [0.0 for _ in prices]
    hour_count = min(active_hour_count, max(1, len(prices) // 2))
    ordered_hours = sorted(range(len(prices)), key=lambda index: prices[index])
    charge_hours = set(ordered_hours[:hour_count])
    discharge_hours = set(ordered_hours[-hour_count:])
    return [
        -max_power_mw
        if index in charge_hours
        else max_power_mw
        if index in discharge_hours
        else 0.0
        for index in range(len(prices))
    ]


def _safe_dispatch_from_requests(
    *,
    requested_dispatch: Sequence[float],
    battery_metrics: BatteryPhysicalMetrics,
    starting_soc_fraction: float,
) -> tuple[list[float], list[float], int]:
    if not 0.0 <= starting_soc_fraction <= 1.0:
        raise ValueError("starting_soc_fraction must be between 0.0 and 1.0.")
    capacity_mwh = float(battery_metrics.capacity_mwh)
    max_power_mw = float(battery_metrics.max_power_mw)
    soc_min_mwh = float(battery_metrics.soc_min_fraction) * capacity_mwh
    soc_max_mwh = float(battery_metrics.soc_max_fraction) * capacity_mwh
    charge_efficiency = float(battery_metrics.round_trip_efficiency) ** 0.5
    discharge_efficiency = float(battery_metrics.round_trip_efficiency) ** 0.5
    soc_mwh = float(np.clip(starting_soc_fraction * capacity_mwh, soc_min_mwh, soc_max_mwh))
    dispatch: list[float] = []
    soc_values: list[float] = [soc_mwh / capacity_mwh]
    clip_count = 0
    for requested_power in requested_dispatch:
        requested = float(requested_power)
        if requested < 0.0:
            desired_charge = min(abs(requested), max_power_mw)
            max_charge = max(0.0, (soc_max_mwh - soc_mwh) / charge_efficiency)
            charge = min(desired_charge, max_charge)
            if charge + 1e-9 < desired_charge:
                clip_count += 1
            net_power_mw = -charge
            soc_mwh += charge * charge_efficiency
        elif requested > 0.0:
            desired_discharge = min(requested, max_power_mw)
            max_discharge = max(0.0, (soc_mwh - soc_min_mwh) * discharge_efficiency)
            discharge = min(desired_discharge, max_discharge)
            if discharge + 1e-9 < desired_discharge:
                clip_count += 1
            net_power_mw = discharge
            soc_mwh -= discharge / discharge_efficiency
        else:
            net_power_mw = 0.0
        soc_mwh = float(np.clip(soc_mwh, soc_min_mwh, soc_max_mwh))
        dispatch.append(float(net_power_mw))
        soc_values.append(float(soc_mwh / capacity_mwh))
    return dispatch, soc_values, clip_count


__all__ = [
    "DEFAULT_TEMPLATE_GRIDS",
    "DEFAULT_LIVE_SAFE_SWITCH_TEMPLATE_SPECS",
    "LIVE_SAFE_SWITCH_CANDIDATE_FAMILIES",
    "LiveSafeSwitchTemplateSpec",
    "SUPPORTED_TEMPLATE_GRIDS",
    "build_hf_live_safe_switch_candidate_rows",
    "template_grid_specs",
]
