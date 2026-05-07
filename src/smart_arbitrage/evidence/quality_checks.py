from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

import polars as pl

DNIPRO_TENANT_ID: Final[str] = "client_003_dnipro_factory"
EXPECTED_DNIPRO_ANCHORS: Final[int] = 90
RAW_FORECAST_MODEL_NAMES: Final[tuple[str, ...]] = (
    "strict_similar_day",
    "nbeatsx_silver_v0",
    "tft_silver_v0",
)
VALUE_AWARE_SELECTOR_MODEL_NAME: Final[str] = "value_aware_ensemble_v0"
HORIZON_CALIBRATION_MODEL_NAMES: Final[tuple[str, ...]] = (
    "strict_similar_day",
    "tft_silver_v0",
    "nbeatsx_silver_v0",
    "tft_horizon_regret_weighted_calibrated_v0",
    "nbeatsx_horizon_regret_weighted_calibrated_v0",
)
HORIZON_CALIBRATED_MODEL_NAMES: Final[tuple[str, ...]] = (
    "tft_horizon_regret_weighted_calibrated_v0",
    "nbeatsx_horizon_regret_weighted_calibrated_v0",
)


@dataclass(frozen=True)
class EvidenceCheckOutcome:
    passed: bool
    description: str
    metadata: dict[str, Any]


def validate_real_data_benchmark_evidence(
    frame: pl.DataFrame,
    *,
    tenant_id: str = DNIPRO_TENANT_ID,
    expected_anchor_count: int = EXPECTED_DNIPRO_ANCHORS,
) -> EvidenceCheckOutcome:
    required_columns = {
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "anchor_timestamp",
        "generated_at",
        "evaluation_payload",
    }
    failures = _missing_column_failures(frame, required_columns)
    tenant_frame = _tenant_frame(frame, tenant_id=tenant_id) if not failures else pl.DataFrame()
    if not failures and tenant_frame.height == 0:
        failures.append(f"tenant_id={tenant_id} has no benchmark rows")

    anchor_count = _n_unique(tenant_frame, "anchor_timestamp")
    model_names = _unique_strings(tenant_frame, "forecast_model_name")
    missing_models = _missing_values(model_names, RAW_FORECAST_MODEL_NAMES)
    data_quality_tiers = _data_quality_tiers(tenant_frame)
    observed_coverage_min = _observed_coverage_min(tenant_frame)
    generated_at_count = _n_unique(tenant_frame, "generated_at")
    leaky_horizon_rows = _leaky_horizon_row_count(tenant_frame)
    missing_model_anchor_pairs = _missing_model_anchor_pair_count(
        tenant_frame,
        expected_models=RAW_FORECAST_MODEL_NAMES,
    )

    if anchor_count != expected_anchor_count:
        failures.append(
            f"anchor_count must be {expected_anchor_count}; observed {anchor_count}"
        )
    if missing_models:
        failures.append(f"missing raw forecast models: {missing_models}")
    if missing_model_anchor_pairs:
        failures.append(
            f"missing {missing_model_anchor_pairs} model-anchor benchmark rows"
        )
    if data_quality_tiers != ["thesis_grade"]:
        failures.append("benchmark evidence must contain only thesis_grade rows")
    if observed_coverage_min < 1.0:
        failures.append("benchmark evidence must have observed coverage ratio of 1.0")
    if generated_at_count != 1:
        failures.append("benchmark evidence must represent one latest generated_at batch")
    if leaky_horizon_rows:
        failures.append("forecast horizon rows must start after each anchor timestamp")

    return _outcome(
        failures=failures,
        passed_description="Dnipro real-data benchmark evidence is thesis-grade and no-leakage.",
        metadata={
            "tenant_id": tenant_id,
            "anchor_count": anchor_count,
            "model_count": len(model_names),
            "model_names": model_names,
            "missing_models": missing_models,
            "missing_model_anchor_pairs": missing_model_anchor_pairs,
            "data_quality_tiers": data_quality_tiers,
            "observed_coverage_min": observed_coverage_min,
            "generated_at_count": generated_at_count,
            "leaky_horizon_rows": leaky_horizon_rows,
        },
    )


def validate_dfl_training_evidence(
    frame: pl.DataFrame,
    *,
    tenant_id: str = DNIPRO_TENANT_ID,
    expected_anchor_count: int = EXPECTED_DNIPRO_ANCHORS,
) -> EvidenceCheckOutcome:
    required_columns = {
        "tenant_id",
        "anchor_timestamp",
        "forecast_model_name",
        "strategy_kind",
        "data_quality_tier",
    }
    failures = _missing_column_failures(frame, required_columns)
    tenant_frame = _tenant_frame(frame, tenant_id=tenant_id) if not failures else pl.DataFrame()
    anchor_count = _n_unique(tenant_frame, "anchor_timestamp")
    model_names = _unique_strings(tenant_frame, "forecast_model_name")
    strategy_kinds = _unique_strings(tenant_frame, "strategy_kind")
    expected_models = (*RAW_FORECAST_MODEL_NAMES, VALUE_AWARE_SELECTOR_MODEL_NAME)
    expected_strategy_kinds = (
        "real_data_rolling_origin_benchmark",
        "value_aware_ensemble_gate",
    )
    missing_models = _missing_values(model_names, expected_models)
    missing_strategy_kinds = _missing_values(strategy_kinds, expected_strategy_kinds)
    data_quality_tiers = _unique_strings(tenant_frame, "data_quality_tier")
    market_execution_rows = _keyword_row_count(
        tenant_frame,
        columns=("strategy_kind", "forecast_model_name"),
        keyword="market_execution",
    )

    if tenant_frame.height == 0:
        failures.append(f"tenant_id={tenant_id} has no DFL training rows")
    if anchor_count != expected_anchor_count:
        failures.append(
            f"DFL training anchor_count must be {expected_anchor_count}; observed {anchor_count}"
        )
    if missing_models:
        failures.append(f"missing DFL training models: {missing_models}")
    if missing_strategy_kinds:
        failures.append(f"missing DFL training strategy kinds: {missing_strategy_kinds}")
    if data_quality_tiers != ["thesis_grade"]:
        failures.append("DFL training evidence must contain only thesis_grade rows")
    if market_execution_rows:
        failures.append("DFL training evidence must not include market execution rows")

    return _outcome(
        failures=failures,
        passed_description="DFL training evidence has raw and selector rows for Dnipro.",
        metadata={
            "tenant_id": tenant_id,
            "row_count": tenant_frame.height,
            "anchor_count": anchor_count,
            "model_count": len(model_names),
            "model_names": model_names,
            "missing_models": missing_models,
            "strategy_kinds": strategy_kinds,
            "missing_strategy_kinds": missing_strategy_kinds,
            "data_quality_tiers": data_quality_tiers,
            "market_execution_rows": market_execution_rows,
        },
    )


def validate_horizon_calibration_evidence(
    frame: pl.DataFrame,
    *,
    tenant_id: str = DNIPRO_TENANT_ID,
    expected_anchor_count: int = EXPECTED_DNIPRO_ANCHORS,
) -> EvidenceCheckOutcome:
    required_columns = {
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "anchor_timestamp",
        "generated_at",
        "evaluation_payload",
    }
    failures = _missing_column_failures(frame, required_columns)
    tenant_frame = _tenant_frame(frame, tenant_id=tenant_id) if not failures else pl.DataFrame()
    anchor_count = _n_unique(tenant_frame, "anchor_timestamp")
    model_names = _unique_strings(tenant_frame, "forecast_model_name")
    missing_models = _missing_values(model_names, HORIZON_CALIBRATION_MODEL_NAMES)
    data_quality_tiers = _data_quality_tiers(tenant_frame)
    generated_at_count = _n_unique(tenant_frame, "generated_at")
    leaky_horizon_rows = _leaky_horizon_row_count(tenant_frame)
    leaky_calibration_rows = _leaky_calibration_metadata_count(tenant_frame)
    missing_model_anchor_pairs = _missing_model_anchor_pair_count(
        tenant_frame,
        expected_models=HORIZON_CALIBRATION_MODEL_NAMES,
    )

    if anchor_count != expected_anchor_count:
        failures.append(
            f"horizon calibration anchor_count must be {expected_anchor_count}; observed {anchor_count}"
        )
    if missing_models:
        failures.append(f"missing horizon calibration models: {missing_models}")
    if missing_model_anchor_pairs:
        failures.append(
            f"missing {missing_model_anchor_pairs} model-anchor calibration rows"
        )
    if data_quality_tiers != ["thesis_grade"]:
        failures.append("horizon calibration evidence must contain only thesis_grade rows")
    if generated_at_count != 1:
        failures.append("horizon calibration evidence must represent one latest generated_at batch")
    if leaky_horizon_rows:
        failures.append("forecast horizon rows must start after each anchor timestamp")
    if leaky_calibration_rows:
        failures.append("calibration metadata appears to use future anchors")

    return _outcome(
        failures=failures,
        passed_description="Horizon calibration evidence uses prior-anchor metadata only.",
        metadata={
            "tenant_id": tenant_id,
            "anchor_count": anchor_count,
            "model_count": len(model_names),
            "model_names": model_names,
            "missing_models": missing_models,
            "missing_model_anchor_pairs": missing_model_anchor_pairs,
            "data_quality_tiers": data_quality_tiers,
            "generated_at_count": generated_at_count,
            "leaky_horizon_rows": leaky_horizon_rows,
            "leaky_rows": leaky_calibration_rows,
        },
    )


def validate_selector_evidence(
    frame: pl.DataFrame,
    *,
    expected_strategy_kind: str,
    expected_model_name: str,
    tenant_id: str = DNIPRO_TENANT_ID,
    expected_anchor_count: int = EXPECTED_DNIPRO_ANCHORS,
) -> EvidenceCheckOutcome:
    required_columns = {
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "anchor_timestamp",
        "generated_at",
        "evaluation_payload",
    }
    failures = _missing_column_failures(frame, required_columns)
    tenant_frame = _tenant_frame(frame, tenant_id=tenant_id) if not failures else pl.DataFrame()
    selector_frame = (
        tenant_frame.filter(
            (pl.col("strategy_kind") == expected_strategy_kind)
            & (pl.col("forecast_model_name") == expected_model_name)
        )
        if tenant_frame.height
        else tenant_frame
    )
    anchor_count = _n_unique(selector_frame, "anchor_timestamp")
    row_count = selector_frame.height
    data_quality_tiers = _data_quality_tiers(selector_frame)
    duplicate_anchor_count = _duplicate_anchor_count(selector_frame)
    generated_at_count = _n_unique(selector_frame, "generated_at")
    full_dfl_claim_rows = _full_dfl_claim_row_count(selector_frame)

    if row_count == 0:
        failures.append(f"{expected_model_name} selector rows are missing")
    if anchor_count != expected_anchor_count:
        failures.append(
            f"selector anchor_count must be {expected_anchor_count}; observed {anchor_count}"
        )
    if row_count != expected_anchor_count:
        failures.append(
            f"selector must have one row per anchor; observed {row_count} rows"
        )
    if duplicate_anchor_count:
        failures.append(f"selector has {duplicate_anchor_count} duplicate anchor rows")
    if data_quality_tiers != ["thesis_grade"]:
        failures.append("selector evidence must contain only thesis_grade rows")
    if generated_at_count != 1:
        failures.append("selector evidence must represent one latest generated_at batch")
    if full_dfl_claim_rows:
        failures.append("selector evidence must not claim full DFL")

    return _outcome(
        failures=failures,
        passed_description=f"{expected_model_name} selector has one row per Dnipro anchor.",
        metadata={
            "tenant_id": tenant_id,
            "strategy_kind": expected_strategy_kind,
            "model_name": expected_model_name,
            "row_count": row_count,
            "anchor_count": anchor_count,
            "duplicate_anchor_count": duplicate_anchor_count,
            "data_quality_tiers": data_quality_tiers,
            "generated_at_count": generated_at_count,
            "full_dfl_claim_rows": full_dfl_claim_rows,
        },
    )


def _outcome(
    *,
    failures: list[str],
    passed_description: str,
    metadata: dict[str, Any],
) -> EvidenceCheckOutcome:
    return EvidenceCheckOutcome(
        passed=not failures,
        description=passed_description if not failures else "; ".join(failures),
        metadata=metadata,
    )


def _missing_column_failures(frame: pl.DataFrame, required_columns: set[str]) -> list[str]:
    missing_columns = sorted(required_columns.difference(frame.columns))
    if not missing_columns:
        return []
    return [f"frame is missing required columns: {missing_columns}"]


def _tenant_frame(frame: pl.DataFrame, *, tenant_id: str) -> pl.DataFrame:
    return frame.filter(pl.col("tenant_id") == tenant_id)


def _n_unique(frame: pl.DataFrame, column_name: str) -> int:
    if frame.height == 0 or column_name not in frame.columns:
        return 0
    return int(frame.select(column_name).n_unique())


def _unique_strings(frame: pl.DataFrame, column_name: str) -> list[str]:
    if frame.height == 0 or column_name not in frame.columns:
        return []
    return sorted(str(value) for value in frame[column_name].unique().to_list())


def _missing_values(observed_values: list[str], expected_values: tuple[str, ...]) -> list[str]:
    observed = set(observed_values)
    return [value for value in expected_values if value not in observed]


def _data_quality_tiers(frame: pl.DataFrame) -> list[str]:
    tiers: set[str] = set()
    if frame.height == 0:
        return []
    if "data_quality_tier" in frame.columns:
        tiers.update(str(value) for value in frame["data_quality_tier"].drop_nulls().to_list())
    if "evaluation_payload" in frame.columns:
        for payload in _payloads(frame):
            tiers.add(str(payload.get("data_quality_tier", "demo_grade")))
    return sorted(tiers)


def _observed_coverage_min(frame: pl.DataFrame) -> float:
    values: list[float] = []
    if frame.height == 0:
        return 0.0
    if "observed_coverage_ratio" in frame.columns:
        values.extend(float(value) for value in frame["observed_coverage_ratio"].drop_nulls().to_list())
    if "source_kind" in frame.columns:
        source_kinds = _unique_strings(frame, "source_kind")
        values.append(1.0 if source_kinds == ["observed"] else 0.0)
    if "evaluation_payload" in frame.columns:
        for payload in _payloads(frame):
            values.append(float(payload.get("observed_coverage_ratio", 0.0)))
    return min(values) if values else 0.0


def _payloads(frame: pl.DataFrame) -> list[dict[str, Any]]:
    if frame.height == 0 or "evaluation_payload" not in frame.columns:
        return []
    payloads: list[dict[str, Any]] = []
    for value in frame["evaluation_payload"].to_list():
        if isinstance(value, dict):
            payloads.append(value)
    return payloads


def _payload_from_row(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("evaluation_payload")
    return value if isinstance(value, dict) else {}


def _leaky_horizon_row_count(frame: pl.DataFrame) -> int:
    count = 0
    if frame.height == 0 or "evaluation_payload" not in frame.columns:
        return count
    for row in frame.iter_rows(named=True):
        anchor_timestamp = _datetime_value(row.get("anchor_timestamp"))
        if anchor_timestamp is None:
            continue
        for horizon_row in _horizon_rows(_payload_from_row(row)):
            interval_start = _datetime_value(horizon_row.get("interval_start"))
            if interval_start is not None and interval_start <= anchor_timestamp:
                count += 1
    return count


def _horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        return []
    return [row for row in horizon if isinstance(row, dict)]


def _datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
    return None


def _missing_model_anchor_pair_count(
    frame: pl.DataFrame,
    *,
    expected_models: tuple[str, ...],
) -> int:
    if frame.height == 0:
        return len(expected_models)
    rows = {
        (
            _datetime_value(row["anchor_timestamp"]),
            str(row["forecast_model_name"]),
        )
        for row in frame.iter_rows(named=True)
    }
    anchors = {
        _datetime_value(value)
        for value in frame["anchor_timestamp"].to_list()
        if _datetime_value(value) is not None
    }
    missing_count = 0
    for anchor in anchors:
        for model_name in expected_models:
            if (anchor, model_name) not in rows:
                missing_count += 1
    return missing_count


def _leaky_calibration_metadata_count(frame: pl.DataFrame) -> int:
    if frame.height == 0:
        return 0
    anchor_values = {
        value
        for value in (_datetime_value(value) for value in frame["anchor_timestamp"].to_list())
        if value is not None
    }
    anchors = sorted(anchor_values)
    prior_count_by_anchor = {anchor: index for index, anchor in enumerate(anchors)}
    leaky_rows = 0
    for row in frame.iter_rows(named=True):
        model_name = str(row["forecast_model_name"])
        if model_name not in HORIZON_CALIBRATED_MODEL_NAMES:
            continue
        anchor_timestamp = _datetime_value(row["anchor_timestamp"])
        if anchor_timestamp is None:
            continue
        payload = _payload_from_row(row)
        prior_anchor_count = _int_or_none(payload.get("prior_anchor_count"))
        calibration_window_anchor_count = _int_or_none(
            payload.get("calibration_window_anchor_count")
        )
        available_prior_anchors = prior_count_by_anchor.get(anchor_timestamp, 0)
        if prior_anchor_count is None:
            leaky_rows += 1
            continue
        if prior_anchor_count > available_prior_anchors:
            leaky_rows += 1
            continue
        if (
            calibration_window_anchor_count is not None
            and calibration_window_anchor_count > prior_anchor_count
        ):
            leaky_rows += 1
    return leaky_rows


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _duplicate_anchor_count(frame: pl.DataFrame) -> int:
    if frame.height == 0:
        return 0
    duplicate_rows = (
        frame.group_by("anchor_timestamp")
        .agg(pl.len().alias("rows"))
        .filter(pl.col("rows") > 1)
    )
    return duplicate_rows.height


def _full_dfl_claim_row_count(frame: pl.DataFrame) -> int:
    count = 0
    for payload in _payloads(frame):
        scope = str(payload.get("academic_scope", "")).lower()
        if "full dfl" in scope and "not full dfl" not in scope:
            count += 1
        if "full differentiable dfl" in scope and "not full differentiable dfl" not in scope:
            count += 1
    return count


def _keyword_row_count(
    frame: pl.DataFrame,
    *,
    columns: tuple[str, ...],
    keyword: str,
) -> int:
    if frame.height == 0:
        return 0
    count = 0
    for row in frame.iter_rows(named=True):
        if any(keyword in str(row.get(column, "")).lower() for column in columns):
            count += 1
    return count
