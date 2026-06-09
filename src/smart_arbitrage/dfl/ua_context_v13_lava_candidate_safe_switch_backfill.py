"""Backfill V13 safe-switch rows from source-observed LAVA candidates.

This is an acquisition helper, not a training or execution path. It mines
already-material candidate rows for train/prior switches that beat V2+ under
the strict value surface, then emits canonical V13 safe-switch examples only
when an OREE DAM source observation/download hash is available for the delivery
date.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_acquisition import (
    normalize_dfl_ua_context_safe_switch_examples_v13_frame,
)

CLAIM_SCOPE: Final[str] = (
    "v13_safe_switch_lava_candidate_backfill_not_model_training"
)
DEFAULT_MIN_REGRET_IMPROVEMENT_UAH: Final[float] = 25.0

_CANDIDATE_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "split_name",
        "eligible_for_final_selection",
        "label_regret_delta_vs_v2_plus_uah",
        "safety_violation_count",
        "market_execution_enabled",
    }
)
_OBSERVATION_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "delivery_date",
        "source_observed_at_utc",
        "download_url",
        "download_sha256",
    }
)
_TARGET_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "current_prior_material_safe_switch_examples",
        "required_prior_material_safe_switch_examples",
    }
)


def build_v13_safe_switch_examples_from_lava_candidates_frame(
    lava_candidate_frame: pl.DataFrame,
    oree_observation_frame: pl.DataFrame,
    acquisition_targets_frame: pl.DataFrame,
    existing_safe_switch_examples_frame: pl.DataFrame | None = None,
    *,
    min_regret_improvement_uah: float = DEFAULT_MIN_REGRET_IMPROVEMENT_UAH,
) -> pl.DataFrame:
    """Emit canonical V13 examples from eligible, source-observed candidates."""

    if min_regret_improvement_uah <= 0.0:
        raise ValueError("min_regret_improvement_uah must be positive.")
    _validate_inputs(
        lava_candidate_frame,
        oree_observation_frame,
        acquisition_targets_frame,
        existing_safe_switch_examples_frame,
    )
    target_specs = _target_specs(
        acquisition_targets_frame,
        existing_safe_switch_examples_frame,
    )
    if not target_specs:
        return normalize_dfl_ua_context_safe_switch_examples_v13_frame(pl.DataFrame())

    observations = _observation_lookup(oree_observation_frame)
    existing_keys = _existing_keys(existing_safe_switch_examples_frame)
    selected_rows: list[dict[str, Any]] = []
    selected_keys: set[tuple[str, str, datetime]] = set()
    candidates = _eligible_candidates(
        lava_candidate_frame,
        min_regret_improvement_uah=min_regret_improvement_uah,
    )
    by_target = _candidate_rows_by_target(candidates)

    for target in sorted(target_specs.values(), key=lambda item: item["priority_rank"]):
        key = (target["tenant_id"], target["source_model_name"])
        needed = int(target["needed"])
        if needed <= 0:
            continue
        target_rows = by_target.get(key, [])
        picked = 0
        for row in target_rows:
            anchor = _datetime_value(row["anchor_timestamp"])
            identity = (key[0], key[1], anchor)
            if identity in existing_keys or identity in selected_keys:
                continue
            observation = observations.get(anchor.date())
            if observation is None:
                continue
            selected_rows.append(_canonical_row(row, observation))
            selected_keys.add(identity)
            picked += 1
            if picked >= needed:
                break

    normalized = normalize_dfl_ua_context_safe_switch_examples_v13_frame(
        pl.DataFrame(selected_rows, infer_schema_length=None)
        if selected_rows
        else pl.DataFrame()
    )
    if normalized.is_empty():
        return normalized
    priority_frame = pl.DataFrame(
        [
            {
                "tenant_id": target["tenant_id"],
                "source_model_name": target["source_model_name"],
                "_priority_rank": target["priority_rank"],
            }
            for target in target_specs.values()
        ]
    )
    return (
        normalized.join(priority_frame, on=["tenant_id", "source_model_name"], how="left")
        .sort(["_priority_rank", "anchor_timestamp"])
        .drop("_priority_rank")
    )


def summarize_v13_safe_switch_lava_candidate_backfill(
    selected_examples_frame: pl.DataFrame,
    lava_candidate_frame: pl.DataFrame,
    oree_observation_frame: pl.DataFrame,
    acquisition_targets_frame: pl.DataFrame,
    existing_safe_switch_examples_frame: pl.DataFrame | None = None,
    *,
    min_regret_improvement_uah: float = DEFAULT_MIN_REGRET_IMPROVEMENT_UAH,
) -> dict[str, Any]:
    """Summarize selected rows, remaining deficits, and missing OREE observations."""

    _validate_inputs(
        lava_candidate_frame,
        oree_observation_frame,
        acquisition_targets_frame,
        existing_safe_switch_examples_frame,
    )
    target_specs = _target_specs(
        acquisition_targets_frame,
        existing_safe_switch_examples_frame,
    )
    observations = _observation_lookup(oree_observation_frame)
    existing_keys = _existing_keys(existing_safe_switch_examples_frame)
    candidates = _eligible_candidates(
        lava_candidate_frame,
        min_regret_improvement_uah=min_regret_improvement_uah,
    )
    selected = normalize_dfl_ua_context_safe_switch_examples_v13_frame(
        selected_examples_frame
    )
    selected_counts = _counts_by_target(selected)
    selected_keys = _frame_keys(selected)
    source_observed_candidate_rows = 0
    source_observed_candidate_keys: set[tuple[str, str, datetime]] = set()
    missing_observation_dates: set[str] = set()
    missing_observation_candidate_rows = 0
    missing_observation_candidate_keys: set[tuple[str, str, datetime]] = set()
    for row in candidates.iter_rows(named=True):
        key = (str(row["tenant_id"]), str(row["source_model_name"]))
        if key not in target_specs:
            continue
        if int(target_specs[key]["needed"]) <= 0:
            continue
        anchor = _datetime_value(row["anchor_timestamp"])
        identity = (key[0], key[1], anchor)
        if identity in existing_keys:
            continue
        if anchor.date() in observations:
            source_observed_candidate_keys.add(identity)
        else:
            missing_observation_candidate_keys.add(identity)
            missing_observation_dates.add(anchor.date().isoformat())
    source_observed_candidate_rows = len(source_observed_candidate_keys)
    missing_observation_candidate_rows = len(missing_observation_candidate_keys)
    next_observation_dates = _next_observation_dates(
        candidates,
        target_specs=target_specs,
        observations=observations,
        existing_keys=existing_keys | selected_keys,
        selected_counts=selected_counts,
    )

    target_rows: list[dict[str, Any]] = []
    remaining_missing = 0
    for target in sorted(target_specs.values(), key=lambda item: item["priority_rank"]):
        selected_count = selected_counts.get(
            (target["tenant_id"], target["source_model_name"]),
            0,
        )
        remaining = max(int(target["needed"]) - selected_count, 0)
        remaining_missing += remaining
        target_rows.append(
            {
                "tenant_id": target["tenant_id"],
                "source_model_name": target["source_model_name"],
                "current_prior_material_safe_switch_examples": target["current"],
                "existing_incremental_examples": target["existing_incremental"],
                "required_prior_material_safe_switch_examples": target["required"],
                "selected_backfill_rows": selected_count,
                "remaining_missing_after_backfill": remaining,
            }
        )

    return {
        "claim_scope": CLAIM_SCOPE,
        "candidate_rows": lava_candidate_frame.height,
        "eligible_candidate_rows": candidates.height,
        "source_observation_rows": oree_observation_frame.height,
        "source_observed_candidate_rows": source_observed_candidate_rows,
        "missing_observation_candidate_rows": missing_observation_candidate_rows,
        "missing_observation_delivery_dates": sorted(missing_observation_dates),
        "next_observation_delivery_dates": next_observation_dates,
        "selected_backfill_rows": selected.height,
        "remaining_missing_after_backfill": remaining_missing,
        "min_regret_improvement_uah": min_regret_improvement_uah,
        "tail_risk_rule": (
            "label_regret_delta_vs_v2_plus_uah <= -min_regret_improvement_uah "
            "and safety_violation_count == 0"
        ),
        "target_rows": target_rows,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _validate_inputs(
    lava_candidate_frame: pl.DataFrame,
    oree_observation_frame: pl.DataFrame,
    acquisition_targets_frame: pl.DataFrame,
    existing_safe_switch_examples_frame: pl.DataFrame | None,
) -> None:
    _require_columns(
        lava_candidate_frame,
        _CANDIDATE_REQUIRED_COLUMNS,
        frame_name="candidate frame",
    )
    _require_columns(
        oree_observation_frame,
        _OBSERVATION_REQUIRED_COLUMNS,
        frame_name="OREE observation frame",
    )
    _require_columns(
        acquisition_targets_frame,
        _TARGET_REQUIRED_COLUMNS,
        frame_name="acquisition targets frame",
    )
    _refuse_market_execution(lava_candidate_frame, frame_name="candidate frame")
    _refuse_market_execution(
        oree_observation_frame,
        frame_name="OREE observation frame",
    )
    if existing_safe_switch_examples_frame is not None:
        normalize_dfl_ua_context_safe_switch_examples_v13_frame(
            existing_safe_switch_examples_frame
        )


def _eligible_candidates(
    frame: pl.DataFrame,
    *,
    min_regret_improvement_uah: float,
) -> pl.DataFrame:
    return (
        frame.with_columns(
            pl.col("anchor_timestamp")
            .map_elements(_datetime_value, return_dtype=pl.Datetime)
            .alias("anchor_timestamp"),
            pl.col("eligible_for_final_selection")
            .map_elements(_bool_value, return_dtype=pl.Boolean)
            .alias("_eligible_for_final_selection"),
            pl.col("label_regret_delta_vs_v2_plus_uah")
            .cast(pl.Float64)
            .alias("_regret_delta"),
            pl.col("safety_violation_count").cast(pl.Int64).alias("_safety_violations"),
        )
        .filter(
            (pl.col("split_name") == "train_selection")
            & pl.col("_eligible_for_final_selection")
            & (pl.col("_regret_delta") <= -float(min_regret_improvement_uah))
            & (pl.col("_safety_violations") == 0)
        )
        .sort(
            [
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "_regret_delta",
                "candidate_family",
                "candidate_model_name",
            ]
        )
    )


def _candidate_rows_by_target(
    candidates: pl.DataFrame,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], dict[datetime, dict[str, Any]]] = {}
    for row in candidates.iter_rows(named=True):
        target_key = (str(row["tenant_id"]), str(row["source_model_name"]))
        anchor = _datetime_value(row["anchor_timestamp"])
        anchor_rows = grouped.setdefault(target_key, {})
        current = anchor_rows.get(anchor)
        if current is None or float(row["_regret_delta"]) < float(current["_regret_delta"]):
            anchor_rows[anchor] = row
    return {
        key: sorted(
            rows.values(),
            key=lambda row: (
                _datetime_value(row["anchor_timestamp"]),
                float(row["_regret_delta"]),
                str(row["candidate_family"]),
                str(row["candidate_model_name"]),
            ),
        )
        for key, rows in grouped.items()
    }


def _target_specs(
    targets: pl.DataFrame,
    existing: pl.DataFrame | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    existing_counts = _counts_by_target(
        normalize_dfl_ua_context_safe_switch_examples_v13_frame(existing)
        if existing is not None
        else pl.DataFrame()
    )
    specs: dict[tuple[str, str], dict[str, Any]] = {}
    for fallback_rank, row in enumerate(targets.iter_rows(named=True), start=1):
        tenant_id = str(row["tenant_id"])
        source_model_name = str(row["source_model_name"])
        key = (tenant_id, source_model_name)
        current = _int_value(row["current_prior_material_safe_switch_examples"])
        required = _int_value(row["required_prior_material_safe_switch_examples"])
        existing_count = existing_counts.get(key, 0)
        priority = _int_value(row.get("acquisition_priority_rank"), fallback_rank)
        specs[key] = {
            "tenant_id": tenant_id,
            "source_model_name": source_model_name,
            "current": current,
            "existing_incremental": existing_count,
            "required": required,
            "needed": max(required - current - existing_count, 0),
            "priority_rank": priority,
        }
    return specs


def _counts_by_target(frame: pl.DataFrame) -> dict[tuple[str, str], int]:
    if frame.is_empty():
        return {}
    return {
        (str(row["tenant_id"]), str(row["source_model_name"])): int(row["count"])
        for row in frame.group_by(["tenant_id", "source_model_name"])
        .agg(pl.len().alias("count"))
        .iter_rows(named=True)
    }


def _existing_keys(frame: pl.DataFrame | None) -> set[tuple[str, str, datetime]]:
    if frame is None:
        return set()
    normalized = normalize_dfl_ua_context_safe_switch_examples_v13_frame(frame)
    return {
        (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"]),
        )
        for row in normalized.iter_rows(named=True)
    }


def _frame_keys(frame: pl.DataFrame) -> set[tuple[str, str, datetime]]:
    if frame.is_empty():
        return set()
    return {
        (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"]),
        )
        for row in frame.iter_rows(named=True)
    }


def _next_observation_dates(
    candidates: pl.DataFrame,
    *,
    target_specs: Mapping[tuple[str, str], Mapping[str, Any]],
    observations: Mapping[date, Mapping[str, Any]],
    existing_keys: set[tuple[str, str, datetime]],
    selected_counts: Mapping[tuple[str, str], int],
) -> list[str]:
    by_target = _candidate_rows_by_target(candidates)
    dates: list[str] = []
    seen_dates: set[str] = set()
    for target in sorted(target_specs.values(), key=lambda item: item["priority_rank"]):
        key = (str(target["tenant_id"]), str(target["source_model_name"]))
        remaining = max(int(target["needed"]) - int(selected_counts.get(key, 0)), 0)
        if remaining <= 0:
            continue
        queued = 0
        for row in by_target.get(key, []):
            anchor = _datetime_value(row["anchor_timestamp"])
            identity = (key[0], key[1], anchor)
            if identity in existing_keys:
                continue
            if anchor.date() in observations:
                continue
            date_text = anchor.date().isoformat()
            if date_text not in seen_dates:
                dates.append(date_text)
                seen_dates.add(date_text)
            queued += 1
            if queued >= remaining:
                break
    return dates


def _observation_lookup(frame: pl.DataFrame) -> dict[date, Mapping[str, Any]]:
    observations: dict[date, Mapping[str, Any]] = {}
    sorted_frame = frame.sort(["delivery_date", "source_observed_at_utc"])
    for row in sorted_frame.iter_rows(named=True):
        delivery_day = _date_value(row["delivery_date"])
        if delivery_day in observations:
            continue
        if not str(row["source_observed_at_utc"]).strip():
            continue
        if not str(row["download_url"]).strip():
            continue
        if not str(row["download_sha256"]).strip():
            continue
        observations[delivery_day] = row
    return observations


def _canonical_row(
    candidate: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> dict[str, Any]:
    anchor = _datetime_value(candidate["anchor_timestamp"])
    sha256 = str(observation["download_sha256"])
    return {
        "tenant_id": str(candidate["tenant_id"]),
        "source_model_name": str(candidate["source_model_name"]),
        "anchor_timestamp": anchor.isoformat(timespec="microseconds"),
        "split_name": "train_selection",
        "source_evidence_timestamp": _datetime_value(
            observation["source_observed_at_utc"]
        ).isoformat(timespec="microseconds"),
        "label_v13_material_safe_switch": True,
        "label_v13_tail_risk_loss": False,
        "source_url": str(observation["download_url"]),
        "source_title": str(
            observation.get("source_title", "OREE PXS DAM downloadxlsx endpoint")
        ),
        "receipt_id": (
            "oree-lava-safe-switch:"
            f"{anchor.isoformat(timespec='microseconds')}:{sha256[:16]}"
        ),
        "market_execution_enabled": False,
    }


def _require_columns(frame: pl.DataFrame, columns: frozenset[str], *, frame_name: str) -> None:
    missing = sorted(columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")


def _refuse_market_execution(frame: pl.DataFrame, *, frame_name: str) -> None:
    if "market_execution_enabled" not in frame.columns:
        return
    normalized = frame.with_columns(
        pl.col("market_execution_enabled")
        .map_elements(_bool_value, return_dtype=pl.Boolean)
        .alias("_market_execution_enabled")
    )
    if normalized.filter(pl.col("_market_execution_enabled")).height:
        raise ValueError(f"{frame_name} contains market execution rows.")


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    else:
        text = str(value).strip()
        if not text:
            raise ValueError("datetime value is empty.")
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value).strip()).date()


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in {0, 1}:
            return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no", ""}:
        return False
    raise TypeError(f"Cannot convert {type(value).__name__} to bool.")


def _int_value(value: Any, default: int = 0) -> int:
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


__all__ = [
    "CLAIM_SCOPE",
    "DEFAULT_MIN_REGRET_IMPROVEMENT_UAH",
    "build_v13_safe_switch_examples_from_lava_candidates_frame",
    "summarize_v13_safe_switch_lava_candidate_backfill",
]
