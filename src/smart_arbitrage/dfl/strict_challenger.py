"""Strict-control challenger diagnostics for DFL schedule evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

DFL_PIPELINE_INTEGRITY_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_pipeline_integrity_audit_not_full_dfl"
)
DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_candidate_library_v2_not_full_dfl"
)
DFL_NON_STRICT_UPPER_BOUND_CLAIM_SCOPE: Final[str] = (
    "dfl_non_strict_oracle_upper_bound_not_full_dfl"
)
DFL_STRICT_BASELINE_AUTOPSY_CLAIM_SCOPE: Final[str] = (
    "dfl_strict_baseline_autopsy_not_full_dfl"
)
DFL_SCHEDULE_LIBRARY_V2_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only strict-control challenger schedule library. This is not full DFL, "
    "not Decision Transformer control, and not market execution."
)

CANDIDATE_FAMILY_STRICT: Final[str] = "strict_control"
CANDIDATE_FAMILY_RAW: Final[str] = "raw_source"
CANDIDATE_FAMILY_BLEND_V2: Final[str] = "strict_raw_blend_v2"
CANDIDATE_FAMILY_PRIOR_RESIDUAL_V2: Final[str] = "strict_prior_residual_v2"
RANKER_SELECTION_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "prior_family_mean_regret_uah",
    "forecast_spread_uah_mwh",
    "total_degradation_penalty_uah",
    "total_throughput_mwh",
    "soc_min_slack_fraction",
)
ANALYSIS_ONLY_SCHEDULE_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "actual_spread_uah_mwh",
    "forecast_top_k_actual_overlap",
    "forecast_bottom_k_actual_overlap",
    "peak_index_abs_error",
    "trough_index_abs_error",
)
REQUIRED_LIBRARY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "split_name",
        "horizon_hours",
        "forecast_price_uah_mwh_vector",
        "actual_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "forecast_spread_uah_mwh",
        "safety_violation_count",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
        "evaluation_payload",
    }
)
REQUIRED_BENCHMARK_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "horizon_hours",
        "evaluation_payload",
    }
)
REQUIRED_UPPER_BOUND_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "selected_candidate_family",
        "selected_candidate_model_name",
        "strict_regret_uah",
        "best_non_strict_regret_uah",
        "non_strict_upper_bound_beats_strict",
        "candidate_family_count",
        "data_quality_tier",
        "observed_coverage_ratio",
        "safety_violation_count",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_dfl_pipeline_integrity_audit_frame(
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Summarize point-in-time and feature-boundary checks for DFL selectors."""

    _require_columns(
        real_data_rolling_origin_benchmark_frame,
        REQUIRED_BENCHMARK_COLUMNS,
        frame_name="real_data_rolling_origin_benchmark_frame",
    )
    _validate_library_frame(schedule_candidate_library_frame)
    forbidden_overlap = sorted(
        set(RANKER_SELECTION_FEATURE_COLUMNS).intersection(ANALYSIS_ONLY_SCHEDULE_FEATURE_COLUMNS)
    )
    leaky_benchmark_rows = _leaky_horizon_row_count(real_data_rolling_origin_benchmark_frame)
    leaky_library_rows = _leaky_horizon_row_count(schedule_candidate_library_frame)
    source_model_names = sorted(
        str(value)
        for value in schedule_candidate_library_frame["source_model_name"].unique().to_list()
    )
    failures: list[str] = []
    if forbidden_overlap:
        failures.append("ranker selection features include actual-derived diagnostics")
    if leaky_benchmark_rows or leaky_library_rows:
        failures.append("forecast horizon rows must start after anchor_timestamp")
    return pl.DataFrame(
        [
            {
                "audit_name": "dfl_pipeline_integrity_audit",
                "passed": not failures,
                "failure_reasons": failures,
                "market_anchor_count": _n_unique(real_data_rolling_origin_benchmark_frame, "anchor_timestamp"),
                "tenant_anchor_count": _tenant_anchor_count(real_data_rolling_origin_benchmark_frame),
                "source_model_count": len(source_model_names),
                "source_model_names": source_model_names,
                "ranker_selection_feature_columns": list(RANKER_SELECTION_FEATURE_COLUMNS),
                "analysis_only_schedule_feature_columns": list(ANALYSIS_ONLY_SCHEDULE_FEATURE_COLUMNS),
                "forbidden_ranker_feature_overlap_count": len(forbidden_overlap),
                "forbidden_ranker_feature_overlap": forbidden_overlap,
                "leaky_horizon_rows": leaky_benchmark_rows + leaky_library_rows,
                "benchmark_rows": real_data_rolling_origin_benchmark_frame.height,
                "schedule_candidate_rows": schedule_candidate_library_frame.height,
                "claim_scope": DFL_PIPELINE_INTEGRITY_AUDIT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        ]
    )


def build_dfl_schedule_candidate_library_v2_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    blend_weights: tuple[float, ...] = (0.25, 0.5, 0.75),
    residual_min_prior_anchors: int = 3,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Add prior-only strict/raw blend and strict residual schedule candidates."""

    _validate_library_frame(schedule_candidate_library_frame)
    if not blend_weights:
        raise ValueError("blend_weights must contain at least one value.")
    if any(not 0.0 <= weight <= 1.0 for weight in blend_weights):
        raise ValueError("blend_weights must be between 0.0 and 1.0.")
    if residual_min_prior_anchors < 0:
        raise ValueError("residual_min_prior_anchors must not be negative.")

    resolved_generated_at = generated_at or datetime.now(UTC)
    rows = [_source_library_row(row) for row in schedule_candidate_library_frame.iter_rows(named=True)]
    grouped_rows = _rows_by_tenant_source_anchor(schedule_candidate_library_frame)
    for key in sorted(grouped_rows, key=lambda item: (item[0], item[1], item[2])):
        tenant_id, source_model_name, anchor_timestamp = key
        anchor_rows = grouped_rows[key]
        strict_row = _single_candidate_family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
        raw_row = _single_candidate_family_row(anchor_rows, CANDIDATE_FAMILY_RAW)
        for blend_weight in blend_weights:
            rows.append(
                _evaluated_candidate_row(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_BLEND_V2,
                    candidate_model_name=_blend_model_name(source_model_name, blend_weight=blend_weight),
                    forecast_prices=_blend_vectors(
                        _float_list(strict_row["forecast_price_uah_mwh_vector"], field_name="strict forecast"),
                        _float_list(raw_row["forecast_price_uah_mwh_vector"], field_name="raw forecast"),
                        raw_weight=blend_weight,
                    ),
                    generated_at=resolved_generated_at,
                    metadata={
                        "blend_weight_raw": blend_weight,
                        "prior_residual_anchor_count": 0,
                        "no_leakage_prior_only": True,
                    },
                )
            )
        residual = _prior_residual_vector(
            schedule_candidate_library_frame,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
            anchor_timestamp=anchor_timestamp,
        )
        if residual.prior_anchor_count >= residual_min_prior_anchors:
            rows.append(
                _evaluated_candidate_row(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_PRIOR_RESIDUAL_V2,
                    candidate_model_name=_prior_residual_model_name(source_model_name),
                    forecast_prices=[
                        strict_price + residual_value
                        for strict_price, residual_value in zip(
                            _float_list(strict_row["forecast_price_uah_mwh_vector"], field_name="strict forecast"),
                            residual.residual_vector,
                            strict=True,
                        )
                    ],
                    generated_at=resolved_generated_at,
                    metadata={
                        "prior_residual_anchor_count": residual.prior_anchor_count,
                        "no_leakage_prior_only": True,
                    },
                )
            )
    return pl.DataFrame(rows).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "candidate_family", "candidate_model_name"]
    )


def build_dfl_non_strict_oracle_upper_bound_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    min_final_holdout_tenant_anchor_count_per_source_model: int = 90,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Pick the best final-holdout non-strict candidate per tenant/source/anchor."""

    _validate_library_frame(schedule_candidate_library_frame, require_thesis_grade=False)
    resolved_generated_at = generated_at or datetime.now(UTC)
    final_frame = schedule_candidate_library_frame.filter(pl.col("split_name") == "final_holdout")
    rows: list[dict[str, Any]] = []
    grouped_rows = _rows_by_tenant_source_anchor(final_frame)
    for key in sorted(grouped_rows, key=lambda item: (item[0], item[1], item[2])):
        tenant_id, source_model_name, anchor_timestamp = key
        anchor_rows = grouped_rows[key]
        strict_row = _single_candidate_family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
        non_strict_rows = [
            row for row in anchor_rows if str(row["candidate_family"]) != CANDIDATE_FAMILY_STRICT
        ]
        if not non_strict_rows:
            raise ValueError(f"missing non-strict candidates for {tenant_id}/{source_model_name}/{anchor_timestamp}")
        best_row = min(
            non_strict_rows,
            key=lambda row: (
                float(row["regret_uah"]),
                _candidate_family_sort_index(str(row["candidate_family"])),
                str(row["candidate_model_name"]),
            ),
        )
        rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "anchor_timestamp": anchor_timestamp,
                "generated_at": resolved_generated_at,
                "selected_candidate_family": str(best_row["candidate_family"]),
                "selected_candidate_model_name": str(best_row["candidate_model_name"]),
                "strict_candidate_model_name": str(strict_row["candidate_model_name"]),
                "strict_regret_uah": float(strict_row["regret_uah"]),
                "best_non_strict_regret_uah": float(best_row["regret_uah"]),
                "strict_gap_to_best_non_strict_uah": float(strict_row["regret_uah"]) - float(best_row["regret_uah"]),
                "non_strict_upper_bound_beats_strict": float(best_row["regret_uah"]) < float(strict_row["regret_uah"]),
                "candidate_family_count": len(non_strict_rows),
                "min_final_holdout_tenant_anchor_count_per_source_model": (
                    min_final_holdout_tenant_anchor_count_per_source_model
                ),
                "data_quality_tier": str(best_row["data_quality_tier"]),
                "observed_coverage_ratio": float(best_row["observed_coverage_ratio"]),
                "safety_violation_count": int(best_row["safety_violation_count"]) + int(strict_row["safety_violation_count"]),
                "claim_scope": DFL_NON_STRICT_UPPER_BOUND_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "evaluation_payload": {
                    "claim_scope": DFL_NON_STRICT_UPPER_BOUND_CLAIM_SCOPE,
                    "source_forecast_model_name": source_model_name,
                    "selected_candidate_family": str(best_row["candidate_family"]),
                    "selected_candidate_model_name": str(best_row["candidate_model_name"]),
                    "strict_regret_uah": float(strict_row["regret_uah"]),
                    "best_non_strict_regret_uah": float(best_row["regret_uah"]),
                    "not_full_dfl": True,
                    "not_market_execution": True,
                },
            }
        )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id", "anchor_timestamp"])


def build_dfl_strict_baseline_autopsy_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    dfl_non_strict_oracle_upper_bound_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Describe strict-similar-day failure opportunities against non-strict candidates."""

    _validate_library_frame(schedule_candidate_library_frame)
    _require_columns(
        dfl_non_strict_oracle_upper_bound_frame,
        REQUIRED_UPPER_BOUND_COLUMNS,
        frame_name="dfl_non_strict_oracle_upper_bound_frame",
    )
    strict_regrets = [
        float(value)
        for value in dfl_non_strict_oracle_upper_bound_frame["strict_regret_uah"].to_list()
    ]
    high_regret_threshold = _quantile(strict_regrets, 0.75)
    rows: list[dict[str, Any]] = []
    for row in dfl_non_strict_oracle_upper_bound_frame.iter_rows(named=True):
        strict_regret = float(row["strict_regret_uah"])
        gap = float(row["strict_gap_to_best_non_strict_uah"])
        upper_bound_beats_strict = bool(row["non_strict_upper_bound_beats_strict"])
        rows.append(
            {
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "anchor_timestamp": _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
                "strict_regret_uah": strict_regret,
                "best_non_strict_regret_uah": float(row["best_non_strict_regret_uah"]),
                "strict_gap_to_best_non_strict_uah": gap,
                "strict_high_regret_threshold_uah": high_regret_threshold,
                "strict_high_regret_flag": strict_regret >= high_regret_threshold,
                "selected_candidate_family": str(row["selected_candidate_family"]),
                "selected_candidate_model_name": str(row["selected_candidate_model_name"]),
                "candidate_family_count": int(row["candidate_family_count"]),
                "recommended_next_action": (
                    "train_selector_to_detect_strict_failure"
                    if upper_bound_beats_strict
                    else "expand_data_or_candidate_library"
                ),
                "claim_scope": DFL_STRICT_BASELINE_AUTOPSY_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id", "anchor_timestamp"])


def validate_dfl_non_strict_upper_bound_evidence(
    frame: pl.DataFrame,
    *,
    expected_tenant_ids: tuple[str, ...] | None = None,
    expected_source_model_names: tuple[str, ...] | None = None,
    minimum_validation_tenant_anchor_count_per_source_model: int = 90,
    expected_final_holdout_anchors_per_tenant_model: int = 18,
) -> EvidenceCheckOutcome:
    """Validate non-strict upper-bound coverage and strict-control challenge value."""

    failures = _missing_column_failures(frame, REQUIRED_UPPER_BOUND_COLUMNS)
    tenant_ids = _unique_strings(frame, "tenant_id") if not failures else []
    source_model_names = _unique_strings(frame, "source_model_name") if not failures else []
    if expected_tenant_ids is not None:
        missing_tenants = [tenant_id for tenant_id in expected_tenant_ids if tenant_id not in set(tenant_ids)]
        if missing_tenants:
            failures.append(f"missing upper-bound tenants: {missing_tenants}")
    if expected_source_model_names is not None:
        missing_models = [
            model_name for model_name in expected_source_model_names if model_name not in set(source_model_names)
        ]
        if missing_models:
            failures.append(f"missing upper-bound source models: {missing_models}")
    group_counts = _group_anchor_counts(frame) if not failures else {}
    minimum_group_count = min(group_counts.values()) if group_counts else 0
    validation_counts = _validation_tenant_anchor_count_by_source(frame) if not failures else {}
    minimum_validation_count = min(validation_counts.values()) if validation_counts else 0
    data_quality_tiers = _unique_strings(frame, "data_quality_tier") if not failures else []
    observed_coverage_min = _min_float(frame, "observed_coverage_ratio") if not failures else 0.0
    safety_violation_rows = _positive_int_row_count(frame, "safety_violation_count") if not failures else 0
    claim_flag_failures = _claim_flag_failure_count(frame) if not failures else 0
    strict_mean = _mean_column(frame, "strict_regret_uah") if not failures else 0.0
    best_non_strict_mean = _mean_column(frame, "best_non_strict_regret_uah") if not failures else 0.0
    upper_bound_improvement_ratio = _improvement_ratio(strict_mean, best_non_strict_mean)

    if frame.height == 0:
        failures.append("non-strict upper-bound frame has no rows")
    if minimum_group_count < expected_final_holdout_anchors_per_tenant_model:
        failures.append(
            "each tenant/source model must have exactly the final-holdout coverage; "
            f"observed minimum {minimum_group_count}"
        )
    if minimum_validation_count < minimum_validation_tenant_anchor_count_per_source_model:
        failures.append(
            "validation tenant-anchor coverage is below "
            f"{minimum_validation_tenant_anchor_count_per_source_model}; observed {minimum_validation_count}"
        )
    if data_quality_tiers != ["thesis_grade"]:
        failures.append("non-strict upper-bound evidence must contain only thesis_grade rows")
    if observed_coverage_min < 1.0:
        failures.append("non-strict upper-bound evidence requires observed coverage ratio of 1.0")
    if safety_violation_rows:
        failures.append("non-strict upper-bound evidence requires zero safety violations")
    if claim_flag_failures:
        failures.append("non-strict upper-bound evidence must remain not_full_dfl and not_market_execution")
    if upper_bound_improvement_ratio <= 0.0:
        failures.append("non-strict oracle upper bound does not beat strict_similar_day")

    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Non-strict oracle upper bound can challenge strict_similar_day."
            if not failures
            else "; ".join(failures)
        ),
        metadata={
            "row_count": frame.height,
            "tenant_count": len(tenant_ids),
            "source_model_count": len(source_model_names),
            "tenant_ids": tenant_ids,
            "source_model_names": source_model_names,
            "minimum_final_holdout_anchors_per_tenant_model": minimum_group_count,
            "validation_tenant_anchor_count_per_source_model": minimum_validation_count,
            "data_quality_tiers": data_quality_tiers,
            "observed_coverage_min": observed_coverage_min,
            "safety_violation_rows": safety_violation_rows,
            "claim_flag_failure_rows": claim_flag_failures,
            "strict_mean_regret_uah": strict_mean,
            "best_non_strict_mean_regret_uah": best_non_strict_mean,
            "upper_bound_improvement_ratio_vs_strict": upper_bound_improvement_ratio,
        },
    )


class _PriorResidual:
    def __init__(self, *, prior_anchor_count: int, residual_vector: list[float]) -> None:
        self.prior_anchor_count = prior_anchor_count
        self.residual_vector = residual_vector


def _source_library_row(row: dict[str, Any]) -> dict[str, Any]:
    copied = dict(row)
    payload = dict(_payload(row))
    payload.update(
        {
            "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_CLAIM_SCOPE,
            "candidate_library_version": "v2_source",
            "blend_weight_raw": None,
            "prior_residual_anchor_count": None,
            "no_leakage_prior_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    copied["claim_scope"] = DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_CLAIM_SCOPE
    copied["candidate_library_version"] = "v2_source"
    copied["evaluation_payload"] = payload
    return copied


def _evaluated_candidate_row(
    reference_row: dict[str, Any],
    *,
    source_model_name: str,
    candidate_family: str,
    candidate_model_name: str,
    forecast_prices: list[float],
    generated_at: datetime,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    tenant_id = str(reference_row["tenant_id"])
    anchor_timestamp = _datetime_value(reference_row["anchor_timestamp"], field_name="anchor_timestamp")
    tenant_defaults = tenant_battery_defaults_from_registry(tenant_id)
    evaluation = evaluate_forecast_candidates_against_oracle(
        price_history=_price_history_from_row(reference_row),
        tenant_id=tenant_id,
        battery_metrics=tenant_defaults.metrics,
        starting_soc_fraction=_starting_soc_fraction(reference_row),
        starting_soc_source="schedule_candidate_library_v2",
        anchor_timestamp=anchor_timestamp,
        candidates=[
            ForecastCandidate(
                model_name=candidate_model_name,
                forecast_frame=_forecast_frame_from_prices(
                    reference_row,
                    forecast_prices=forecast_prices,
                ),
                point_prediction_column="predicted_price_uah_mwh",
            )
        ],
        evaluation_id=f"{tenant_id}:schedule-library-v2:{candidate_model_name}:{anchor_timestamp:%Y%m%dT%H%M}",
        generated_at=generated_at,
    )
    evaluated = evaluation.row(0, named=True)
    payload = dict(_payload(evaluated))
    payload.update(
        {
            **metadata,
            "candidate_family": candidate_family,
            "source_forecast_model_name": source_model_name,
            "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_CLAIM_SCOPE,
            "academic_scope": DFL_SCHEDULE_LIBRARY_V2_ACADEMIC_SCOPE,
            "data_quality_tier": str(reference_row["data_quality_tier"]),
            "observed_coverage_ratio": float(reference_row["observed_coverage_ratio"]),
            "safety_violation_count": int(reference_row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return _candidate_row_from_evaluation(
        evaluated,
        payload=payload,
        source_model_name=source_model_name,
        candidate_family=candidate_family,
        split_name=str(reference_row["split_name"]),
        generated_at=generated_at,
    )


def _candidate_row_from_evaluation(
    row: dict[str, Any],
    *,
    payload: dict[str, Any],
    source_model_name: str,
    candidate_family: str,
    split_name: str,
    generated_at: datetime,
) -> dict[str, Any]:
    horizon = _horizon_rows(payload)
    horizon_hours = int(row["horizon_hours"])
    _validate_horizon_length(horizon, horizon_hours=horizon_hours)
    forecast_prices = _float_vector(horizon, key="forecast_price_uah_mwh")
    actual_prices = _float_vector(horizon, key="actual_price_uah_mwh")
    dispatch = _float_vector(horizon, key="net_power_mw", default=0.0)
    soc = _float_vector(horizon, key="soc_fraction", default=float(row["starting_soc_fraction"]))
    return {
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": str(row["forecast_model_name"]),
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        "generated_at": generated_at,
        "split_name": split_name,
        "horizon_hours": horizon_hours,
        "forecast_price_uah_mwh_vector": forecast_prices,
        "actual_price_uah_mwh_vector": actual_prices,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": soc,
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "forecast_spread_uah_mwh": _spread(forecast_prices),
        "actual_spread_uah_mwh": _spread(actual_prices),
        "forecast_top_k_actual_overlap": _top_k_overlap(forecast_prices, actual_prices, largest=True),
        "forecast_bottom_k_actual_overlap": _top_k_overlap(forecast_prices, actual_prices, largest=False),
        "peak_index_abs_error": float(
            abs(_extreme_index(forecast_prices, largest=True) - _extreme_index(actual_prices, largest=True))
        ),
        "trough_index_abs_error": float(
            abs(_extreme_index(forecast_prices, largest=False) - _extreme_index(actual_prices, largest=False))
        ),
        "soc_min_slack_fraction": min(min(soc), 1.0 - max(soc)) if soc else 0.0,
        "prior_family_mean_regret_uah": float(row["regret_uah"]),
        "safety_violation_count": int(payload.get("safety_violation_count", 0)),
        "data_quality_tier": str(payload.get("data_quality_tier", "demo_grade")),
        "observed_coverage_ratio": float(payload.get("observed_coverage_ratio", 0.0)),
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_CLAIM_SCOPE,
        "candidate_library_version": "v2_generated",
        "evaluation_payload": payload,
    }


def _prior_residual_vector(
    frame: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    anchor_timestamp: datetime,
) -> _PriorResidual:
    train_rows = frame.filter(
        (pl.col("tenant_id") == tenant_id)
        & (pl.col("source_model_name") == source_model_name)
        & (pl.col("split_name") == "train_selection")
        & (pl.col("anchor_timestamp") < anchor_timestamp)
    )
    grouped_rows = _rows_by_tenant_source_anchor(train_rows)
    residual_vectors: list[list[float]] = []
    for rows in grouped_rows.values():
        strict_row = _single_candidate_family_row(rows, CANDIDATE_FAMILY_STRICT)
        raw_row = _single_candidate_family_row(rows, CANDIDATE_FAMILY_RAW)
        strict_forecast = _float_list(strict_row["forecast_price_uah_mwh_vector"], field_name="strict forecast")
        raw_forecast = _float_list(raw_row["forecast_price_uah_mwh_vector"], field_name="raw forecast")
        residual_vectors.append([raw - strict for strict, raw in zip(strict_forecast, raw_forecast, strict=True)])
    if not residual_vectors:
        return _PriorResidual(prior_anchor_count=0, residual_vector=[])
    horizon_length = len(residual_vectors[0])
    return _PriorResidual(
        prior_anchor_count=len(residual_vectors),
        residual_vector=[
            mean(vector[index] for vector in residual_vectors)
            for index in range(horizon_length)
        ],
    )


def _blend_vectors(strict_forecast: list[float], raw_forecast: list[float], *, raw_weight: float) -> list[float]:
    if len(strict_forecast) != len(raw_forecast):
        raise ValueError("strict and raw forecast vectors must have matching lengths.")
    return [
        ((1.0 - raw_weight) * strict_value) + (raw_weight * raw_value)
        for strict_value, raw_value in zip(strict_forecast, raw_forecast, strict=True)
    ]


def _forecast_frame_from_prices(row: dict[str, Any], *, forecast_prices: list[float]) -> pl.DataFrame:
    horizon = _horizon_rows(_payload(row))
    if len(horizon) != len(forecast_prices):
        raise ValueError("forecast vector length must match horizon length.")
    return pl.DataFrame(
        {
            "forecast_timestamp": [_datetime_value(point["interval_start"], field_name="interval_start") for point in horizon],
            "predicted_price_uah_mwh": forecast_prices,
        }
    )


def _price_history_from_row(row: dict[str, Any]) -> pl.DataFrame:
    horizon = _horizon_rows(_payload(row))
    return pl.DataFrame(
        {
            DEFAULT_TIMESTAMP_COLUMN: [
                _datetime_value(point["interval_start"], field_name="interval_start")
                for point in horizon
            ],
            DEFAULT_PRICE_COLUMN: [float(point["actual_price_uah_mwh"]) for point in horizon],
        }
    )


def _starting_soc_fraction(row: dict[str, Any]) -> float:
    soc_values = _float_list(row["soc_fraction_vector"], field_name="soc_fraction_vector")
    return min(1.0, max(0.0, soc_values[0]))


def _rows_by_tenant_source_anchor(frame: pl.DataFrame) -> dict[tuple[str, str, datetime], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in frame.iter_rows(named=True):
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        grouped.setdefault(key, []).append(row)
    return grouped


def _single_candidate_family_row(rows: list[dict[str, Any]], candidate_family: str) -> dict[str, Any]:
    matches = [row for row in rows if str(row["candidate_family"]) == candidate_family]
    if not matches:
        raise ValueError(f"missing {candidate_family} candidate row")
    return min(matches, key=lambda row: str(row["candidate_model_name"]))


def _validate_library_frame(frame: pl.DataFrame, *, require_thesis_grade: bool = True) -> None:
    _require_columns(frame, REQUIRED_LIBRARY_COLUMNS, frame_name="schedule_candidate_library_frame")
    for row in frame.iter_rows(named=True):
        horizon_hours = int(row["horizon_hours"])
        for column_name in (
            "forecast_price_uah_mwh_vector",
            "actual_price_uah_mwh_vector",
            "dispatch_mw_vector",
            "soc_fraction_vector",
        ):
            if len(_float_list(row[column_name], field_name=column_name)) != horizon_hours:
                raise ValueError(f"{column_name} vector length must match horizon_hours")
        if require_thesis_grade and str(row["data_quality_tier"]) != "thesis_grade":
            raise ValueError("schedule candidate library requires thesis_grade rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("schedule candidate library requires observed coverage ratio of 1.0")
        if int(row["safety_violation_count"]):
            raise ValueError("schedule candidate library requires zero safety violations")
        if row.get("not_full_dfl") is not True:
            raise ValueError("schedule candidate library requires not_full_dfl=true")
        if row.get("not_market_execution") is not True:
            raise ValueError("schedule candidate library requires not_market_execution=true")
    split_by_key: dict[tuple[str, str, datetime], set[str]] = {}
    for row in frame.iter_rows(named=True):
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        split_by_key.setdefault(key, set()).add(str(row["split_name"]))
    if any(len(splits) > 1 for splits in split_by_key.values()):
        raise ValueError("train/final overlap is not allowed in schedule candidate library")


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing required columns: {missing_columns}")


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing_columns = sorted(required_columns.difference(frame.columns))
    return [f"frame is missing required columns: {missing_columns}"] if missing_columns else []


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    if not isinstance(payload, dict):
        raise ValueError("evaluation_payload must be a dict")
    return payload


def _horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list) or not horizon:
        raise ValueError("evaluation_payload.horizon must be a non-empty list")
    if not all(isinstance(point, dict) for point in horizon):
        raise ValueError("evaluation_payload.horizon must contain dict rows")
    return horizon


def _validate_horizon_length(horizon: list[dict[str, Any]], *, horizon_hours: int) -> None:
    if len(horizon) != horizon_hours:
        raise ValueError(f"vector length must match horizon_hours; observed {len(horizon)} vs {horizon_hours}")


def _float_vector(horizon: list[dict[str, Any]], *, key: str, default: float | None = None) -> list[float]:
    values: list[float] = []
    for point in horizon:
        value = point.get(key, default)
        if value is None:
            raise ValueError(f"horizon point is missing {key}")
        values.append(float(value))
    return values


def _float_list(value: object, *, field_name: str) -> list[float]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list.")
    return [float(item) for item in value]


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO datetime.") from exc
        return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
    raise ValueError(f"{field_name} must be a datetime.")


def _leaky_horizon_row_count(frame: pl.DataFrame) -> int:
    count = 0
    if "evaluation_payload" not in frame.columns or "anchor_timestamp" not in frame.columns:
        return count
    for row in frame.iter_rows(named=True):
        anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        payload = row.get("evaluation_payload")
        if not isinstance(payload, dict):
            continue
        horizon = payload.get("horizon")
        if not isinstance(horizon, list):
            continue
        for point in horizon:
            if not isinstance(point, dict) or "interval_start" not in point:
                continue
            interval_start = _datetime_value(point["interval_start"], field_name="interval_start")
            if interval_start <= anchor_timestamp:
                count += 1
    return count


def _tenant_anchor_count(frame: pl.DataFrame) -> int:
    if frame.height == 0:
        return 0
    return len(
        {
            (str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"))
            for row in frame.iter_rows(named=True)
        }
    )


def _n_unique(frame: pl.DataFrame, column_name: str) -> int:
    if frame.height == 0 or column_name not in frame.columns:
        return 0
    return int(frame.select(column_name).n_unique())


def _unique_strings(frame: pl.DataFrame, column_name: str) -> list[str]:
    if frame.height == 0 or column_name not in frame.columns:
        return []
    return sorted(str(value) for value in frame[column_name].unique().to_list())


def _group_anchor_counts(frame: pl.DataFrame) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], set[datetime]] = {}
    for row in frame.iter_rows(named=True):
        key = (str(row["tenant_id"]), str(row["source_model_name"]))
        counts.setdefault(key, set()).add(_datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"))
    return {key: len(values) for key, values in counts.items()}


def _validation_tenant_anchor_count_by_source(frame: pl.DataFrame) -> dict[str, int]:
    counts: dict[str, set[tuple[str, datetime]]] = {}
    for row in frame.iter_rows(named=True):
        source_model_name = str(row["source_model_name"])
        counts.setdefault(source_model_name, set()).add(
            (
                str(row["tenant_id"]),
                _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
            )
        )
    return {source_model_name: len(values) for source_model_name, values in counts.items()}


def _min_float(frame: pl.DataFrame, column_name: str) -> float:
    if frame.height == 0 or column_name not in frame.columns:
        return 0.0
    return min(float(value) for value in frame[column_name].drop_nulls().to_list())


def _positive_int_row_count(frame: pl.DataFrame, column_name: str) -> int:
    if frame.height == 0 or column_name not in frame.columns:
        return 0
    return sum(1 for value in frame[column_name].to_list() if int(value) > 0)


def _claim_flag_failure_count(frame: pl.DataFrame) -> int:
    if frame.height == 0:
        return 0
    return sum(
        1
        for row in frame.iter_rows(named=True)
        if row.get("not_full_dfl") is not True or row.get("not_market_execution") is not True
    )


def _mean_column(frame: pl.DataFrame, column_name: str) -> float:
    if frame.height == 0 or column_name not in frame.columns:
        return 0.0
    return mean(float(value) for value in frame[column_name].to_list())


def _improvement_ratio(baseline: float, candidate: float) -> float:
    if abs(baseline) < 1e-9:
        return 0.0
    return (baseline - candidate) / abs(baseline)


def _quantile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, max(0, int(round((len(sorted_values) - 1) * quantile))))
    return sorted_values[index]


def _spread(values: list[float]) -> float:
    return max(values) - min(values) if values else 0.0


def _top_k_overlap(forecast: list[float], actual: list[float], *, largest: bool) -> float:
    if not forecast or not actual:
        return 0.0
    k = max(1, min(3, len(forecast), len(actual)))
    forecast_indexes = set(_sorted_indexes(forecast, largest=largest)[:k])
    actual_indexes = set(_sorted_indexes(actual, largest=largest)[:k])
    return len(forecast_indexes.intersection(actual_indexes)) / float(k)


def _sorted_indexes(values: list[float], *, largest: bool) -> list[int]:
    return sorted(range(len(values)), key=lambda index: values[index], reverse=largest)


def _extreme_index(values: list[float], *, largest: bool) -> int:
    if not values:
        return 0
    return _sorted_indexes(values, largest=largest)[0]


def _candidate_family_sort_index(candidate_family: str) -> int:
    family_order = (
        CANDIDATE_FAMILY_RAW,
        "forecast_perturbation",
        CANDIDATE_FAMILY_BLEND_V2,
        CANDIDATE_FAMILY_PRIOR_RESIDUAL_V2,
    )
    try:
        return family_order.index(candidate_family)
    except ValueError:
        return len(family_order)


def _blend_model_name(source_model_name: str, *, blend_weight: float) -> str:
    token = f"{blend_weight:.2f}".replace(".", "p")
    return f"dfl_schedule_library_v2_blend_raw_{token}_{source_model_name}"


def _prior_residual_model_name(source_model_name: str) -> str:
    return f"dfl_schedule_library_v2_prior_residual_{source_model_name}"
