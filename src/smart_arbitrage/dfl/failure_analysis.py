"""DFL action-classifier failure diagnostics for strict LP/oracle evidence."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import (
    DFL_ACTION_LABEL_MODEL_NAMES,
    DFL_ACTION_LABEL_TENANT_IDS,
    EvidenceCheckOutcome,
)


FAILURE_ANALYSIS_CLAIM_SCOPE: Final[str] = (
    "dfl_action_classifier_failure_analysis_not_full_dfl"
)
FAILURE_ANALYSIS_CONCLUSION: Final[str] = (
    "blocked_static_action_classifier_not_decision_value_optimized"
)
PLAIN_CLASSIFIER_VARIANT: Final[str] = "plain_majority"
VALUE_AWARE_CLASSIFIER_VARIANT: Final[str] = "value_aware_weighted_majority"

_REQUIRED_ACTION_LABEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "split_name",
        "is_final_holdout",
        "horizon_hours",
        "actual_price_vector_uah_mwh",
        "target_charge_mask",
        "target_discharge_mask",
        "target_hold_mask",
        "candidate_safety_violation_count",
        "strict_baseline_safety_violation_count",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
    }
)
_REQUIRED_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "anchor_timestamp",
        "horizon_hours",
        "decision_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "evaluation_payload",
    }
)
_EXPECTED_VARIANTS: Final[tuple[str, str]] = (
    PLAIN_CLASSIFIER_VARIANT,
    VALUE_AWARE_CLASSIFIER_VARIANT,
)


@dataclass(frozen=True, slots=True)
class _VariantSpec:
    classifier_variant: str
    candidate_prefix: str


@dataclass(frozen=True, slots=True)
class _ProjectionRows:
    candidates_by_key: dict[tuple[str, str, datetime], dict[str, Any]]
    strict_by_key: dict[tuple[str, str, datetime], dict[str, Any]]


def build_dfl_action_classifier_failure_analysis_frame(
    action_label_frame: pl.DataFrame,
    plain_strict_lp_frame: pl.DataFrame,
    value_aware_strict_lp_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Explain why action-classifier probes are blocked by strict LP/oracle scoring."""

    _validate_action_label_frame(action_label_frame)
    specs = (
        _VariantSpec(
            classifier_variant=PLAIN_CLASSIFIER_VARIANT,
            candidate_prefix="dfl_action_classifier_v0_",
        ),
        _VariantSpec(
            classifier_variant=VALUE_AWARE_CLASSIFIER_VARIANT,
            candidate_prefix="dfl_value_aware_action_classifier_v1_",
        ),
    )
    frames_by_variant = {
        PLAIN_CLASSIFIER_VARIANT: plain_strict_lp_frame,
        VALUE_AWARE_CLASSIFIER_VARIANT: value_aware_strict_lp_frame,
    }
    projection_rows_by_variant = {
        spec.classifier_variant: _projection_rows(
            frames_by_variant[spec.classifier_variant],
            spec=spec,
        )
        for spec in specs
    }

    final_labels = action_label_frame.filter(pl.col("split_name") == "final_holdout")
    summary_rows: list[dict[str, Any]] = []
    for spec in specs:
        projection_rows = projection_rows_by_variant[spec.classifier_variant]
        for tenant_id in _unique_strings(final_labels, "tenant_id"):
            for source_model_name in _unique_strings(final_labels, "forecast_model_name"):
                group = final_labels.filter(
                    (pl.col("tenant_id") == tenant_id)
                    & (pl.col("forecast_model_name") == source_model_name)
                )
                if group.height == 0:
                    continue
                summary_rows.append(
                    _summary_row(
                        group,
                        tenant_id=tenant_id,
                        source_model_name=source_model_name,
                        spec=spec,
                        projection_rows=projection_rows,
                    )
                )

    if not summary_rows:
        raise ValueError("failure analysis requires final_holdout action-label rows")
    return _with_variant_comparison(pl.DataFrame(summary_rows)).sort(
        ["classifier_variant", "tenant_id", "source_model_name"]
    )


def validate_dfl_action_classifier_failure_analysis_evidence(
    frame: pl.DataFrame,
    *,
    expected_tenant_ids: tuple[str, ...] = DFL_ACTION_LABEL_TENANT_IDS,
    expected_model_names: tuple[str, ...] = DFL_ACTION_LABEL_MODEL_NAMES,
    expected_variants: tuple[str, ...] = _EXPECTED_VARIANTS,
    minimum_final_holdout_tenant_anchors_per_source_model: int = 90,
) -> EvidenceCheckOutcome:
    """Validate classifier-failure diagnostics are supervisor-ready evidence."""

    required_columns = {
        "tenant_id",
        "source_model_name",
        "classifier_variant",
        "final_holdout_anchor_count",
        "label_hour_count",
        "confusion_count_total",
        "data_quality_tier",
        "observed_coverage_ratio",
        "uses_final_holdout_for_training",
        "train_final_overlap_count",
        "not_full_dfl",
        "not_market_execution",
        "claim_scope",
    }
    failures = _missing_column_failures(frame, required_columns)
    tenant_ids = _unique_strings(frame, "tenant_id") if not failures else []
    source_model_names = _unique_strings(frame, "source_model_name") if not failures else []
    variants = _unique_strings(frame, "classifier_variant") if not failures else []
    missing_tenants = _missing_values(tenant_ids, expected_tenant_ids)
    missing_models = _missing_values(source_model_names, expected_model_names)
    missing_variants = _missing_values(variants, expected_variants)
    final_holdout_by_model = (
        _final_holdout_tenant_anchors_by_source_model(
            frame,
            expected_variants=expected_variants,
        )
        if not failures
        else {}
    )
    under_coverage_models = sorted(
        model_name
        for model_name, final_count in final_holdout_by_model.items()
        if final_count < minimum_final_holdout_tenant_anchors_per_source_model
    )
    data_quality_tiers = _unique_strings(frame, "data_quality_tier") if not failures else []
    observed_coverage_min = _min_float(frame, "observed_coverage_ratio") if not failures else 0.0
    claim_flag_failure_rows = _claim_flag_failure_count(frame) if not failures else 0
    final_holdout_training_rows = (
        frame.filter(pl.col("uses_final_holdout_for_training")).height if not failures else 0
    )
    train_final_overlap_count = _sum_int(frame, "train_final_overlap_count") if not failures else 0
    confusion_total_mismatch_rows = _confusion_total_mismatch_count(frame) if not failures else 0

    if frame.height == 0:
        failures.append("DFL classifier failure analysis has no rows")
    if missing_tenants:
        failures.append(f"missing failure-analysis tenants: {missing_tenants}")
    if missing_models:
        failures.append(f"missing failure-analysis source models: {missing_models}")
    if missing_variants:
        failures.append(f"missing failure-analysis classifier variants: {missing_variants}")
    if under_coverage_models:
        failures.append(
            "each source model must have at least "
            f"{minimum_final_holdout_tenant_anchors_per_source_model} final-holdout tenant-anchors"
        )
    if data_quality_tiers != ["thesis_grade"]:
        failures.append("failure analysis must contain only thesis_grade rows")
    if observed_coverage_min < 1.0:
        failures.append("failure analysis requires observed coverage ratio of 1.0")
    if claim_flag_failure_rows:
        failures.append("failure analysis must remain not_full_dfl and not_market_execution")
    if final_holdout_training_rows:
        failures.append("failure analysis must not use final holdout for training")
    if train_final_overlap_count:
        failures.append("failure analysis train/final splits must not overlap")
    if confusion_total_mismatch_rows:
        failures.append("confusion totals must equal label-hour totals")

    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "DFL classifier failure analysis is no-leakage research evidence."
            if not failures
            else "; ".join(failures)
        ),
        metadata={
            "row_count": frame.height,
            "tenant_count": len(tenant_ids),
            "source_model_count": len(source_model_names),
            "variant_count": len(variants),
            "tenant_ids": tenant_ids,
            "source_model_names": source_model_names,
            "classifier_variants": variants,
            "missing_tenants": missing_tenants,
            "missing_models": missing_models,
            "missing_variants": missing_variants,
            "final_holdout_tenant_anchors_by_source_model": final_holdout_by_model,
            "under_coverage_models": under_coverage_models,
            "data_quality_tiers": data_quality_tiers,
            "observed_coverage_min": observed_coverage_min,
            "claim_flag_failure_rows": claim_flag_failure_rows,
            "final_holdout_training_rows": final_holdout_training_rows,
            "train_final_overlap_count": train_final_overlap_count,
            "confusion_total_mismatch_rows": confusion_total_mismatch_rows,
        },
    )


def _validate_action_label_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, _REQUIRED_ACTION_LABEL_COLUMNS, frame_name="action_label_frame")
    if frame.height == 0:
        raise ValueError("action_label_frame must contain rows")
    if frame.filter(pl.col("data_quality_tier") != "thesis_grade").height:
        raise ValueError("failure analysis requires thesis_grade action-label rows")
    if frame.filter(pl.col("observed_coverage_ratio") < 1.0).height:
        raise ValueError("failure analysis requires observed coverage ratio of 1.0")
    if frame.filter(~pl.col("not_full_dfl")).height:
        raise ValueError("failure analysis rows must remain not_full_dfl")
    if frame.filter(~pl.col("not_market_execution")).height:
        raise ValueError("failure analysis rows must remain not_market_execution")
    if frame.filter(
        (pl.col("candidate_safety_violation_count") != 0)
        | (pl.col("strict_baseline_safety_violation_count") != 0)
    ).height:
        raise ValueError("failure analysis requires zero safety violations")
    if _train_final_overlap_count(frame):
        raise ValueError("failure analysis action-label splits overlap")
    if frame.filter(pl.col("split_name") == "final_holdout").height == 0:
        raise ValueError("failure analysis requires final_holdout rows")


def _projection_rows(frame: pl.DataFrame, *, spec: _VariantSpec) -> _ProjectionRows:
    _require_columns(frame, _REQUIRED_STRICT_COLUMNS, frame_name=f"{spec.classifier_variant}_frame")
    candidates_by_key: dict[tuple[str, str, datetime], dict[str, Any]] = {}
    strict_by_key: dict[tuple[str, str, datetime], dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        payload = _payload(row)
        source_model_name = str(payload.get("source_forecast_model_name", ""))
        anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        key = (str(row["tenant_id"]), source_model_name, anchor_timestamp)
        forecast_model_name = str(row["forecast_model_name"])
        if forecast_model_name == "strict_similar_day":
            strict_by_key[key] = row
        elif forecast_model_name == f"{spec.candidate_prefix}{source_model_name}":
            _validate_projection_payload(row, spec=spec)
            candidates_by_key[key] = row
    if not candidates_by_key:
        raise ValueError(f"{spec.classifier_variant} frame has no classifier candidate rows")
    return _ProjectionRows(candidates_by_key=candidates_by_key, strict_by_key=strict_by_key)


def _validate_projection_payload(row: dict[str, Any], *, spec: _VariantSpec) -> None:
    payload = _payload(row)
    if payload.get("data_quality_tier") != "thesis_grade":
        raise ValueError(f"{spec.classifier_variant} requires thesis_grade strict rows")
    if float(payload.get("observed_coverage_ratio", 0.0)) < 1.0:
        raise ValueError(f"{spec.classifier_variant} requires observed coverage ratio of 1.0")
    if payload.get("not_full_dfl") is not True:
        raise ValueError(f"{spec.classifier_variant} rows must remain not_full_dfl")
    if payload.get("not_market_execution") is not True:
        raise ValueError(f"{spec.classifier_variant} rows must remain not_market_execution")
    if payload.get("uses_final_holdout_for_training") is True:
        raise ValueError(f"{spec.classifier_variant} must not use final_holdout for training")
    predicted_labels = _string_list(payload.get("predicted_action_labels"))
    if len(predicted_labels) != int(row["horizon_hours"]):
        raise ValueError(f"{spec.classifier_variant} predicted labels must match horizon_hours")


def _summary_row(
    group: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    spec: _VariantSpec,
    projection_rows: _ProjectionRows,
) -> dict[str, Any]:
    confusion_counts: dict[str, int] = defaultdict(int)
    regret_weighted_confusion: dict[str, float] = defaultdict(float)
    label_hour_count = 0
    true_active_count = 0
    predicted_active_count = 0
    active_match_count = 0
    missed_charge_hours = 0
    missed_discharge_hours = 0
    false_active_hours = 0
    top_price_rank_miss_count = 0
    bottom_price_rank_miss_count = 0
    candidate_regrets: list[float] = []
    strict_regrets: list[float] = []
    candidate_values: list[float] = []
    strict_values: list[float] = []
    uses_final_holdout_for_training = False

    for action_row in group.iter_rows(named=True):
        anchor_timestamp = _datetime_value(action_row["anchor_timestamp"], field_name="anchor_timestamp")
        key = (tenant_id, source_model_name, anchor_timestamp)
        if key not in projection_rows.candidates_by_key:
            raise ValueError(
                f"missing classifier candidate row for {tenant_id}/{source_model_name}/{anchor_timestamp}"
            )
        if key not in projection_rows.strict_by_key:
            raise ValueError(
                f"missing strict_similar_day row for {tenant_id}/{source_model_name}/{anchor_timestamp}"
            )
        candidate_row = projection_rows.candidates_by_key[key]
        strict_row = projection_rows.strict_by_key[key]
        payload = _payload(candidate_row)
        predicted_labels = _string_list(payload.get("predicted_action_labels"))
        true_labels = _true_labels(action_row)
        actual_prices = _float_list(action_row["actual_price_vector_uah_mwh"])
        horizon_hours = int(action_row["horizon_hours"])
        if len(predicted_labels) != horizon_hours or len(true_labels) != horizon_hours:
            raise ValueError("failure analysis label vectors must match horizon_hours")
        if len(actual_prices) != horizon_hours:
            raise ValueError("failure analysis actual price vectors must match horizon_hours")

        candidate_regret = float(candidate_row["regret_uah"])
        candidate_regrets.append(candidate_regret)
        strict_regrets.append(float(strict_row["regret_uah"]))
        candidate_values.append(float(candidate_row["decision_value_uah"]))
        strict_values.append(float(strict_row["decision_value_uah"]))
        uses_final_holdout_for_training = (
            uses_final_holdout_for_training
            or payload.get("uses_final_holdout_for_training") is True
        )

        top_indices = set(_top_price_indices(actual_prices, count=min(3, horizon_hours)))
        bottom_indices = set(_bottom_price_indices(actual_prices, count=min(3, horizon_hours)))
        per_hour_regret = candidate_regret / horizon_hours if horizon_hours else 0.0
        for step_index, (true_label, predicted_label) in enumerate(zip(true_labels, predicted_labels)):
            confusion_key = f"{true_label}->{predicted_label}"
            confusion_counts[confusion_key] += 1
            regret_weighted_confusion[confusion_key] += per_hour_regret
            label_hour_count += 1
            true_active = true_label in {"charge", "discharge"}
            predicted_active = predicted_label in {"charge", "discharge"}
            true_active_count += int(true_active)
            predicted_active_count += int(predicted_active)
            active_match_count += int(true_active and predicted_label == true_label)
            missed_charge_hours += int(true_label == "charge" and predicted_label != "charge")
            missed_discharge_hours += int(true_label == "discharge" and predicted_label != "discharge")
            false_active_hours += int(true_label == "hold" and predicted_active)
            top_price_rank_miss_count += int(step_index in top_indices and predicted_label != "discharge")
            bottom_price_rank_miss_count += int(step_index in bottom_indices and predicted_label != "charge")

    mean_candidate_regret = _mean(candidate_regrets)
    mean_strict_regret = _mean(strict_regrets)
    mean_candidate_value = _mean(candidate_values)
    mean_strict_value = _mean(strict_values)
    return {
        "row_scope": "tenant_model",
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "classifier_variant": spec.classifier_variant,
        "final_holdout_anchor_count": _n_unique(group, "anchor_timestamp"),
        "label_hour_count": label_hour_count,
        "confusion_count_total": sum(confusion_counts.values()),
        "confusion_counts_json": _json_dumps(confusion_counts),
        "regret_weighted_confusion_uah_json": _json_dumps(regret_weighted_confusion),
        "active_precision": _safe_ratio(active_match_count, predicted_active_count),
        "active_recall": _safe_ratio(active_match_count, true_active_count),
        "missed_charge_hours": missed_charge_hours,
        "missed_discharge_hours": missed_discharge_hours,
        "false_active_hours": false_active_hours,
        "top_price_rank_miss_count": top_price_rank_miss_count,
        "bottom_price_rank_miss_count": bottom_price_rank_miss_count,
        "mean_candidate_regret_uah": mean_candidate_regret,
        "mean_strict_regret_uah": mean_strict_regret,
        "mean_regret_gap_vs_strict_uah": mean_candidate_regret - mean_strict_regret,
        "mean_candidate_net_value_uah": mean_candidate_value,
        "mean_strict_net_value_uah": mean_strict_value,
        "mean_value_loss_vs_strict_uah": mean_strict_value - mean_candidate_value,
        "mean_soc_path_value_loss_uah": mean_strict_value - mean_candidate_value,
        "plain_variant_mean_regret_uah": None,
        "value_aware_variant_mean_regret_uah": None,
        "regret_delta_vs_plain_uah": None,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "uses_final_holdout_for_training": uses_final_holdout_for_training,
        "train_final_overlap_count": 0,
        "analysis_conclusion": FAILURE_ANALYSIS_CONCLUSION,
        "claim_scope": FAILURE_ANALYSIS_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _with_variant_comparison(frame: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        peer_rows = frame.filter(
            (pl.col("tenant_id") == row["tenant_id"])
            & (pl.col("source_model_name") == row["source_model_name"])
        )
        plain = peer_rows.filter(pl.col("classifier_variant") == PLAIN_CLASSIFIER_VARIANT)
        value_aware = peer_rows.filter(pl.col("classifier_variant") == VALUE_AWARE_CLASSIFIER_VARIANT)
        plain_regret = (
            float(plain.select("mean_candidate_regret_uah").item()) if plain.height else None
        )
        value_aware_regret = (
            float(value_aware.select("mean_candidate_regret_uah").item())
            if value_aware.height
            else None
        )
        rows.append(
            {
                **row,
                "plain_variant_mean_regret_uah": plain_regret,
                "value_aware_variant_mean_regret_uah": value_aware_regret,
                "regret_delta_vs_plain_uah": float(row["mean_candidate_regret_uah"]) - plain_regret
                if plain_regret is not None
                else None,
            }
        )
    return pl.DataFrame(rows)


def _true_labels(row: dict[str, Any]) -> list[str]:
    charge_mask = _int_list(row["target_charge_mask"])
    discharge_mask = _int_list(row["target_discharge_mask"])
    hold_mask = _int_list(row["target_hold_mask"])
    horizon_hours = int(row["horizon_hours"])
    if {len(charge_mask), len(discharge_mask), len(hold_mask)} != {horizon_hours}:
        raise ValueError("failure analysis target masks must match horizon_hours")
    labels: list[str] = []
    for step_index in range(horizon_hours):
        if charge_mask[step_index] + discharge_mask[step_index] + hold_mask[step_index] != 1:
            raise ValueError("failure analysis target masks must be one-hot")
        if charge_mask[step_index]:
            labels.append("charge")
        elif discharge_mask[step_index]:
            labels.append("discharge")
        else:
            labels.append("hold")
    return labels


def _final_holdout_tenant_anchors_by_source_model(
    frame: pl.DataFrame,
    *,
    expected_variants: tuple[str, ...],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for source_model_name in _unique_strings(frame, "source_model_name"):
        variant_counts: list[int] = []
        for variant in expected_variants:
            variant_rows = frame.filter(
                (pl.col("source_model_name") == source_model_name)
                & (pl.col("classifier_variant") == variant)
            )
            variant_counts.append(_sum_int(variant_rows, "final_holdout_anchor_count"))
        counts[source_model_name] = min(variant_counts) if variant_counts else 0
    return counts


def _confusion_total_mismatch_count(frame: pl.DataFrame) -> int:
    return sum(
        1
        for row in frame.iter_rows(named=True)
        if int(row.get("confusion_count_total", -1)) != int(row.get("label_hour_count", -2))
    )


def _claim_flag_failure_count(frame: pl.DataFrame) -> int:
    return sum(
        1
        for row in frame.iter_rows(named=True)
        if row.get("not_full_dfl") is not True
        or row.get("not_market_execution") is not True
        or row.get("claim_scope") != FAILURE_ANALYSIS_CLAIM_SCOPE
    )


def _train_final_overlap_count(frame: pl.DataFrame) -> int:
    splits_by_key: dict[tuple[str, str, datetime], set[str]] = {}
    for row in frame.iter_rows(named=True):
        anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        key = (str(row["tenant_id"]), str(row["forecast_model_name"]), anchor_timestamp)
        splits_by_key.setdefault(key, set()).add(str(row["split_name"]))
    return sum(
        1
        for split_names in splits_by_key.values()
        if {"train_selection", "final_holdout"}.issubset(split_names)
    )


def _top_price_indices(values: list[float], *, count: int) -> list[int]:
    return [
        index
        for index, _ in sorted(
            enumerate(values),
            key=lambda item: item[1],
            reverse=True,
        )[:count]
    ]


def _bottom_price_indices(values: list[float], *, count: int) -> list[int]:
    return [
        index
        for index, _ in sorted(
            enumerate(values),
            key=lambda item: item[1],
        )[:count]
    ]


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing required columns: {missing_columns}")


def _missing_column_failures(frame: pl.DataFrame, required_columns: set[str]) -> list[str]:
    missing_columns = sorted(required_columns.difference(frame.columns))
    return [f"frame is missing required columns: {missing_columns}"] if missing_columns else []


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("evaluation_payload")
    return value if isinstance(value, dict) else {}


def _unique_strings(frame: pl.DataFrame, column_name: str) -> list[str]:
    if frame.height == 0 or column_name not in frame.columns:
        return []
    return sorted(str(value) for value in frame[column_name].unique().to_list())


def _missing_values(observed_values: list[str], expected_values: tuple[str, ...]) -> list[str]:
    observed = set(observed_values)
    return [value for value in expected_values if value not in observed]


def _n_unique(frame: pl.DataFrame, column_name: str) -> int:
    if frame.height == 0 or column_name not in frame.columns:
        return 0
    return int(frame.select(column_name).n_unique())


def _sum_int(frame: pl.DataFrame, column_name: str) -> int:
    if frame.height == 0 or column_name not in frame.columns:
        return 0
    return sum(int(value) for value in frame[column_name].drop_nulls().to_list())


def _min_float(frame: pl.DataFrame, column_name: str) -> float:
    if frame.height == 0 or column_name not in frame.columns:
        return 0.0
    values = [float(value) for value in frame[column_name].drop_nulls().to_list()]
    return min(values) if values else 0.0


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _safe_ratio(numerator: int, denominator: int) -> float:
    return float(numerator) / denominator if denominator else 0.0


def _json_dumps(value: dict[str, int] | dict[str, float]) -> str:
    return json.dumps(dict(sorted(value.items())), sort_keys=True)


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
    raise ValueError(f"{field_name} must be a datetime or ISO datetime string")


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _float_list(value: Any) -> list[float]:
    if not isinstance(value, list):
        return []
    return [float(item) for item in value]


def _int_list(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    return [int(item) for item in value]
