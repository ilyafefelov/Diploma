"""Real-data trajectory rows for residual DFL and offline DT research."""

from __future__ import annotations

from datetime import datetime
from math import cos, pi, sin
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.strict_challenger import (
    CANDIDATE_FAMILY_STRICT,
    _datetime_value,
    _float_list,
    _payload,
    _require_columns,
    _validate_library_frame,
)
from smart_arbitrage.dfl.strict_failure_features import REQUIRED_PRIOR_FEATURE_PANEL_COLUMNS
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_REAL_DATA_TRAJECTORY_DATASET_CLAIM_SCOPE: Final[str] = (
    "dfl_real_data_trajectory_dataset_not_full_dfl"
)
DFL_REAL_DATA_TRAJECTORY_DATASET_ACADEMIC_SCOPE: Final[str] = (
    "Step-level trajectory dataset for residual schedule/value DFL and tiny offline "
    "Decision Transformer research. Teacher labels are train-only; final-holdout rows "
    "are scoring labels only. This is not full DFL, not DT control, and not market execution."
)
REQUIRED_TRAJECTORY_DATASET_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "candidate_family",
        "candidate_model_name",
        "episode_id",
        "horizon_step",
        "split_name",
        "feature_forecast_price_uah_mwh",
        "action_signed_dispatch_mw",
        "label_reward_uah",
        "label_return_to_go_uah",
        "teacher_label_allowed",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_dfl_real_data_trajectory_dataset_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    prior_feature_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
) -> pl.DataFrame:
    """Expand schedule candidates into horizon-step trajectories with prior-only features."""

    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
    )
    _validate_library_frame(schedule_candidate_library_frame)
    _validate_prior_feature_panel(prior_feature_panel_frame)
    library_rows = [
        row
        for row in schedule_candidate_library_frame.iter_rows(named=True)
        if str(row["tenant_id"]) in tenant_ids
        and str(row["source_model_name"]) in forecast_model_names
    ]
    _validate_anchor_coverage(
        library_rows,
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
    )
    feature_lookup = _prior_feature_lookup(prior_feature_panel_frame)
    teacher_lookup = _teacher_lookup(library_rows)
    step_rows: list[dict[str, Any]] = []
    for row in sorted(
        library_rows,
        key=lambda item: (
            str(item["tenant_id"]),
            str(item["source_model_name"]),
            _datetime_value(item["anchor_timestamp"], field_name="anchor_timestamp"),
            str(item["candidate_model_name"]),
        ),
    ):
        step_rows.extend(
            _trajectory_step_rows(
                row,
                feature_lookup=feature_lookup,
                teacher_lookup=teacher_lookup,
            )
        )
    if not step_rows:
        return pl.DataFrame(schema={column: pl.Null for column in REQUIRED_TRAJECTORY_DATASET_COLUMNS})
    return pl.DataFrame(step_rows).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "candidate_model_name",
            "horizon_step",
        ]
    )


def validate_dfl_real_data_trajectory_dataset_evidence(
    trajectory_frame: pl.DataFrame,
    *,
    min_tenant_count: int = 5,
    min_final_holdout_rows: int = 90,
) -> EvidenceCheckOutcome:
    """Validate the trajectory dataset claim boundary and basic step coverage."""

    missing_columns = sorted(REQUIRED_TRAJECTORY_DATASET_COLUMNS.difference(trajectory_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"trajectory dataset is missing required columns: {missing_columns}",
            {"row_count": trajectory_frame.height},
        )
    rows = list(trajectory_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "trajectory dataset has no rows", {"row_count": 0})
    failures: list[str] = []
    tenant_count = len({str(row["tenant_id"]) for row in rows})
    if tenant_count < min_tenant_count:
        failures.append(f"tenant_count must be at least {min_tenant_count}; observed {tenant_count}")
    final_anchor_count = len(
        {
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
            )
            for row in rows
            if str(row["split_name"]) == "final_holdout"
        }
    )
    if final_anchor_count < min_final_holdout_rows:
        failures.append(
            f"final_holdout tenant-anchor count must be at least {min_final_holdout_rows}; "
            f"observed {final_anchor_count}"
        )
    if any(bool(row["teacher_label_allowed"]) for row in rows if str(row["split_name"]) == "final_holdout"):
        failures.append("final-holdout rows must not carry teacher labels")
    if any(not bool(row["not_full_dfl"]) for row in rows):
        failures.append("trajectory rows must remain not_full_dfl")
    if any(not bool(row["not_market_execution"]) for row in rows):
        failures.append("trajectory rows must remain not_market_execution")
    return EvidenceCheckOutcome(
        not failures,
        "Trajectory dataset evidence passed." if not failures else "; ".join(failures),
        {
            "row_count": trajectory_frame.height,
            "tenant_count": tenant_count,
            "final_holdout_tenant_anchor_count": final_anchor_count,
        },
    )


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")


def _validate_prior_feature_panel(frame: pl.DataFrame) -> None:
    if frame.height == 0:
        return
    _require_columns(
        frame,
        REQUIRED_PRIOR_FEATURE_PANEL_COLUMNS,
        frame_name="prior_feature_panel_frame",
    )
    for row in frame.iter_rows(named=True):
        if row.get("not_full_dfl") is not True:
            raise ValueError("prior feature panel rows must remain not_full_dfl=true")
        if row.get("not_market_execution") is not True:
            raise ValueError("prior feature panel rows must remain not_market_execution=true")


def _validate_anchor_coverage(
    rows: list[dict[str, Any]],
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
) -> None:
    grouped: dict[tuple[str, str], set[datetime]] = {}
    split_by_anchor: dict[tuple[str, str, datetime], set[str]] = {}
    strict_by_anchor: dict[tuple[str, str, datetime], int] = {}
    for row in rows:
        tenant_id = str(row["tenant_id"])
        source_model_name = str(row["source_model_name"])
        anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        key = (tenant_id, source_model_name, anchor_timestamp)
        split_by_anchor.setdefault(key, set()).add(str(row["split_name"]))
        if str(row["candidate_family"]) == CANDIDATE_FAMILY_STRICT:
            strict_by_anchor[key] = strict_by_anchor.get(key, 0) + 1
        if str(row["split_name"]) == "final_holdout":
            grouped.setdefault((tenant_id, source_model_name), set()).add(anchor_timestamp)
    if any(len(splits) > 1 for splits in split_by_anchor.values()):
        raise ValueError("train/final overlap is not allowed in trajectory dataset input")
    missing_strict = [key for key in split_by_anchor if strict_by_anchor.get(key, 0) == 0]
    if missing_strict:
        raise ValueError("trajectory dataset requires strict baseline rows for every anchor")
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            final_count = len(grouped.get((tenant_id, source_model_name), set()))
            if final_count != final_validation_anchor_count_per_tenant:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} final_holdout anchor count must be "
                    f"{final_validation_anchor_count_per_tenant}; observed {final_count}"
                )


def _prior_feature_lookup(frame: pl.DataFrame) -> dict[tuple[str, str, datetime], dict[str, Any]]:
    lookup: dict[tuple[str, str, datetime], dict[str, Any]] = {}
    if frame.height == 0:
        return lookup
    for row in frame.iter_rows(named=True):
        lookup[
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
            )
        ] = row
    return lookup


def _teacher_lookup(rows: list[dict[str, Any]]) -> dict[tuple[str, str, datetime], dict[str, Any]]:
    by_anchor: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in rows:
        if str(row["split_name"]) == "final_holdout":
            continue
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        by_anchor.setdefault(key, []).append(row)
    return {
        key: min(
            anchor_rows,
            key=lambda row: (
                float(row["regret_uah"]),
                0 if str(row["candidate_family"]) != CANDIDATE_FAMILY_STRICT else 1,
                str(row["candidate_model_name"]),
            ),
        )
        for key, anchor_rows in by_anchor.items()
    }


def _trajectory_step_rows(
    row: dict[str, Any],
    *,
    feature_lookup: dict[tuple[str, str, datetime], dict[str, Any]],
    teacher_lookup: dict[tuple[str, str, datetime], dict[str, Any]],
) -> list[dict[str, Any]]:
    tenant_id = str(row["tenant_id"])
    source_model_name = str(row["source_model_name"])
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    key = (tenant_id, source_model_name, anchor_timestamp)
    horizon_hours = int(row["horizon_hours"])
    forecast_prices = _float_list(row["forecast_price_uah_mwh_vector"], field_name="forecast_price_uah_mwh_vector")
    actual_prices = _float_list(row["actual_price_uah_mwh_vector"], field_name="actual_price_uah_mwh_vector")
    dispatch = _float_list(row["dispatch_mw_vector"], field_name="dispatch_mw_vector")
    soc = _float_list(row["soc_fraction_vector"], field_name="soc_fraction_vector")
    if not (
        len(forecast_prices) == len(actual_prices) == len(dispatch) == len(soc) == horizon_hours
    ):
        raise ValueError("trajectory dataset vector length must match horizon_hours")
    payload = _payload(row)
    degradation_penalties = _degradation_penalties(row, payload=payload, horizon_hours=horizon_hours)
    rewards = [
        (actual_prices[index] * dispatch[index]) - degradation_penalties[index]
        for index in range(horizon_hours)
    ]
    returns_to_go = _returns_to_go(rewards)
    split_name = str(row["split_name"])
    teacher_allowed = split_name != "final_holdout"
    teacher_row = teacher_lookup.get(key) if teacher_allowed else None
    feature_row = feature_lookup.get(key, {})
    episode_id = (
        f"{tenant_id}|{source_model_name}|{anchor_timestamp.isoformat()}|"
        f"{row['candidate_model_name']}"
    )
    step_rows: list[dict[str, Any]] = []
    for step_index in range(horizon_hours):
        hour = (anchor_timestamp.hour + step_index + 1) % 24
        step_rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "anchor_timestamp": anchor_timestamp,
                "generated_at": row.get("generated_at"),
                "candidate_family": str(row["candidate_family"]),
                "candidate_model_name": str(row["candidate_model_name"]),
                "episode_id": episode_id,
                "horizon_step": step_index,
                "horizon_hours": horizon_hours,
                "split_name": split_name,
                "feature_forecast_price_uah_mwh": forecast_prices[step_index],
                "feature_forecast_spread_uah_mwh": float(row["forecast_spread_uah_mwh"]),
                "feature_prior_anchor_count": float(
                    feature_row.get("selector_feature_prior_anchor_count", 0.0)
                ),
                "feature_prior_strict_mean_regret_uah": float(
                    feature_row.get("selector_feature_prior_strict_mean_regret_uah", 0.0)
                ),
                "feature_prior_raw_mean_regret_uah": float(
                    feature_row.get("selector_feature_prior_raw_mean_regret_uah", 0.0)
                ),
                "feature_prior_best_non_strict_mean_regret_uah": float(
                    feature_row.get("selector_feature_prior_best_non_strict_mean_regret_uah", 0.0)
                ),
                "feature_prior_price_spread_std_uah_mwh": float(
                    feature_row.get("selector_feature_prior_price_spread_std_uah_mwh", 0.0)
                ),
                "feature_prior_net_load_mean_mw": float(
                    feature_row.get("selector_feature_prior_net_load_mean_mw", 0.0)
                ),
                "feature_calendar_hour_sin": sin(2.0 * pi * hour / 24.0),
                "feature_calendar_hour_cos": cos(2.0 * pi * hour / 24.0),
                "state_soc_fraction": soc[step_index],
                "action_signed_dispatch_mw": dispatch[step_index],
                "label_actual_price_uah_mwh": actual_prices[step_index],
                "label_reward_uah": rewards[step_index],
                "label_return_to_go_uah": returns_to_go[step_index],
                "episode_decision_value_uah": float(row["decision_value_uah"]),
                "episode_oracle_value_uah": float(row["oracle_value_uah"]),
                "episode_regret_uah": float(row["regret_uah"]),
                "episode_total_throughput_mwh": float(row["total_throughput_mwh"]),
                "episode_total_degradation_penalty_uah": float(
                    row["total_degradation_penalty_uah"]
                ),
                "teacher_label_allowed": teacher_allowed,
                "teacher_candidate_family": str(teacher_row["candidate_family"])
                if teacher_row is not None
                else None,
                "teacher_candidate_model_name": str(teacher_row["candidate_model_name"])
                if teacher_row is not None
                else None,
                "teacher_regret_uah": float(teacher_row["regret_uah"])
                if teacher_row is not None
                else None,
                "data_quality_tier": str(row["data_quality_tier"]),
                "observed_coverage_ratio": float(row["observed_coverage_ratio"]),
                "safety_violation_count": int(row["safety_violation_count"]),
                "claim_scope": DFL_REAL_DATA_TRAJECTORY_DATASET_CLAIM_SCOPE,
                "academic_scope": DFL_REAL_DATA_TRAJECTORY_DATASET_ACADEMIC_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return step_rows


def _degradation_penalties(
    row: dict[str, Any],
    *,
    payload: dict[str, Any],
    horizon_hours: int,
) -> list[float]:
    horizon = payload.get("horizon")
    if isinstance(horizon, list) and len(horizon) == horizon_hours:
        penalties: list[float] = []
        for point in horizon:
            if isinstance(point, dict):
                penalties.append(float(point.get("degradation_penalty_uah", 0.0)))
            else:
                penalties.append(0.0)
        return penalties
    total_penalty = float(row["total_degradation_penalty_uah"])
    return [total_penalty / horizon_hours for _ in range(horizon_hours)]


def _returns_to_go(rewards: list[float]) -> list[float]:
    remaining = 0.0
    values: list[float] = []
    for reward in reversed(rewards):
        remaining += reward
        values.append(remaining)
    values.reverse()
    return values
