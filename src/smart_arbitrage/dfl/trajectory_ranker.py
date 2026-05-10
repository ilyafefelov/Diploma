"""Feature-ranker evidence over feasible strict-LP-scored schedules."""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

DFL_SCHEDULE_CANDIDATE_LIBRARY_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_candidate_library_not_full_dfl"
)
DFL_TRAJECTORY_FEATURE_RANKER_CLAIM_SCOPE: Final[str] = (
    "dfl_trajectory_feature_ranker_v1_not_full_dfl"
)
DFL_TRAJECTORY_FEATURE_RANKER_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_trajectory_feature_ranker_v1_strict_lp_gate_not_full_dfl"
)
DFL_TRAJECTORY_FEATURE_RANKER_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_trajectory_feature_ranker_strict_lp_benchmark"
)
DFL_TRAJECTORY_FEATURE_RANKER_PREFIX: Final[str] = "dfl_trajectory_feature_ranker_v1_"
DFL_FORECAST_PERTURBATION_PREFIX: Final[str] = "dfl_forecast_perturbation_v1"
DFL_TRAJECTORY_FEATURE_RANKER_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only feature ranker over feasible LP-scored schedules. It is not full DFL, "
    "not Decision Transformer control, and not market execution."
)

CANDIDATE_FAMILY_STRICT: Final[str] = "strict_control"
CANDIDATE_FAMILY_RAW: Final[str] = "raw_source"
CANDIDATE_FAMILY_PERTURBATION: Final[str] = "forecast_perturbation"
FINAL_ONLY_FAMILIES: Final[frozenset[str]] = frozenset(
    {"panel_v2", "decision_target_v3", "action_target_v4"}
)
REFERENCE_FAMILY_ORDER: Final[tuple[str, ...]] = (
    CANDIDATE_FAMILY_STRICT,
    CANDIDATE_FAMILY_RAW,
    CANDIDATE_FAMILY_PERTURBATION,
    "panel_v2",
    "decision_target_v3",
    "action_target_v4",
)
DEFAULT_PERTURB_SPREAD_SCALE_GRID: Final[tuple[float, ...]] = (0.9, 1.1)
DEFAULT_PERTURB_MEAN_SHIFT_GRID_UAH_MWH: Final[tuple[float, ...]] = (-250.0, 250.0)

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
        "starting_soc_source",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "committed_action",
        "committed_power_mw",
        "rank_by_regret",
        "evaluation_payload",
    }
)
REQUIRED_TRAJECTORY_VALUE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "split_name",
        "horizon_hours",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "prior_selection_mean_regret_uah",
        "data_quality_tier",
        "observed_coverage_ratio",
        "safety_violation_count",
        "not_full_dfl",
        "not_market_execution",
        "evaluation_payload",
    }
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
        "prior_family_mean_regret_uah",
        "safety_violation_count",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
        "evaluation_payload",
    }
)
REQUIRED_RANKER_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "selected_weight_profile_name",
        "final_holdout_tenant_anchor_count",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)

WEIGHT_PROFILES: Final[tuple[dict[str, float | str], ...]] = (
    {
        "name": "prior_regret_only",
        "prior_family_mean_regret_uah": 1.0,
        "forecast_spread_uah_mwh": 0.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.0,
        "soc_min_slack_fraction": 0.0,
    },
    {
        "name": "spread_value",
        "prior_family_mean_regret_uah": 0.0,
        "forecast_spread_uah_mwh": -1.0,
        "total_degradation_penalty_uah": 1.0,
        "total_throughput_mwh": 0.0,
        "soc_min_slack_fraction": -25.0,
    },
    {
        "name": "prior_spread_value",
        "prior_family_mean_regret_uah": 0.5,
        "forecast_spread_uah_mwh": -0.05,
        "total_degradation_penalty_uah": 1.0,
        "total_throughput_mwh": 0.0,
        "soc_min_slack_fraction": -10.0,
    },
)


def trajectory_feature_ranker_model_name(source_model_name: str) -> str:
    """Return the v1 feature-ranker model name for a raw source model."""

    return f"{DFL_TRAJECTORY_FEATURE_RANKER_PREFIX}{source_model_name}"


def build_dfl_schedule_candidate_library_frame(
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    dfl_trajectory_value_candidate_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    perturb_spread_scale_grid: tuple[float, ...] = DEFAULT_PERTURB_SPREAD_SCALE_GRID,
    perturb_mean_shift_grid_uah_mwh: tuple[float, ...] = DEFAULT_PERTURB_MEAN_SHIFT_GRID_UAH_MWH,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Build feasible schedule candidates for prior-only trajectory/value ranking."""

    _require_columns(
        real_data_rolling_origin_benchmark_frame,
        REQUIRED_EVALUATION_COLUMNS,
        frame_name="real_data_rolling_origin_benchmark_frame",
    )
    _require_columns(
        dfl_trajectory_value_candidate_panel_frame,
        REQUIRED_TRAJECTORY_VALUE_COLUMNS,
        frame_name="dfl_trajectory_value_candidate_panel_frame",
    )
    _validate_common_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
    )
    if not perturb_spread_scale_grid:
        raise ValueError("perturb_spread_scale_grid must contain at least one value.")
    if not perturb_mean_shift_grid_uah_mwh:
        raise ValueError("perturb_mean_shift_grid_uah_mwh must contain at least one value.")

    resolved_generated_at = generated_at or datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = _source_rows(
                real_data_rolling_origin_benchmark_frame,
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
            )
            final_anchors = set(_latest_anchors(source_rows, count=final_validation_anchor_count_per_tenant))
            control_by_anchor = _control_rows_by_anchor(
                real_data_rolling_origin_benchmark_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            for source_row in source_rows:
                anchor_timestamp = _datetime_value(source_row["anchor_timestamp"], field_name="anchor_timestamp")
                split_name = "final_holdout" if anchor_timestamp in final_anchors else "train_selection"
                control_row = control_by_anchor.get(anchor_timestamp)
                if control_row is None:
                    raise ValueError(
                        "missing strict_similar_day row for schedule candidate library "
                        f"{tenant_id}/{source_model_name}/{anchor_timestamp.isoformat()}"
                    )
                rows.append(
                    _candidate_row_from_evaluation_row(
                        control_row,
                        source_model_name=source_model_name,
                        candidate_family=CANDIDATE_FAMILY_STRICT,
                        split_name=split_name,
                        generated_at=resolved_generated_at,
                    )
                )
                rows.append(
                    _candidate_row_from_evaluation_row(
                        source_row,
                        source_model_name=source_model_name,
                        candidate_family=CANDIDATE_FAMILY_RAW,
                        split_name=split_name,
                        generated_at=resolved_generated_at,
                    )
                )
                rows.extend(
                    _perturbation_rows(
                        source_row,
                        source_model_name=source_model_name,
                        split_name=split_name,
                        perturb_spread_scale_grid=perturb_spread_scale_grid,
                        perturb_mean_shift_grid_uah_mwh=perturb_mean_shift_grid_uah_mwh,
                        generated_at=resolved_generated_at,
                    )
                )
            rows.extend(
                _final_only_candidate_rows(
                    dfl_trajectory_value_candidate_panel_frame,
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    final_anchors=final_anchors,
                    generated_at=resolved_generated_at,
                )
            )
    return _with_prior_family_scores(pl.DataFrame(rows)).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "candidate_family", "candidate_model_name"]
    )


def build_dfl_trajectory_feature_ranker_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    min_final_holdout_tenant_anchor_count_per_source_model: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> pl.DataFrame:
    """Select a feature-scoring profile using train-selection anchors only."""

    _validate_library_frame(schedule_candidate_library_frame)
    _validate_common_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=1,
    )
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = _library_rows(
                schedule_candidate_library_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            train_rows = [row for row in source_rows if row["split_name"] == "train_selection"]
            final_rows = [row for row in source_rows if row["split_name"] == "final_holdout"]
            if not train_rows:
                raise ValueError(f"missing train-selection schedule candidates for {tenant_id}/{source_model_name}")
            final_anchor_count = len(_tenant_anchor_set(final_rows))
            if final_anchor_count * len(tenant_ids) < min_final_holdout_tenant_anchor_count_per_source_model:
                raise ValueError(
                    "final-holdout tenant-anchor count must be at least "
                    f"{min_final_holdout_tenant_anchor_count_per_source_model}; "
                    f"observed {final_anchor_count * len(tenant_ids)}"
                )
            profile = _select_weight_profile(train_rows)
            selected_final_rows = _select_rows_by_score(final_rows, profile=profile)
            rows.append(
                _ranker_row(
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    profile=profile,
                    train_rows=train_rows,
                    selected_train_rows=_select_rows_by_score(train_rows, profile=profile),
                    final_rows=final_rows,
                    selected_final_rows=selected_final_rows,
                    min_final_holdout_tenant_anchor_count_per_source_model=(
                        min_final_holdout_tenant_anchor_count_per_source_model
                    ),
                )
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_trajectory_feature_ranker_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    trajectory_feature_ranker_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict/raw/ranker rows for the feature-ranker promotion gate."""

    _validate_library_frame(schedule_candidate_library_frame)
    _require_columns(
        trajectory_feature_ranker_frame,
        REQUIRED_RANKER_COLUMNS,
        frame_name="trajectory_feature_ranker_frame",
    )
    resolved_generated_at = generated_at or datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for ranker_row in trajectory_feature_ranker_frame.iter_rows(named=True):
        tenant_id = str(ranker_row["tenant_id"])
        source_model_name = str(ranker_row["source_model_name"])
        profile = _weight_profile_by_name(str(ranker_row["selected_weight_profile_name"]))
        source_rows = _library_rows(
            schedule_candidate_library_frame,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
        )
        final_rows = [row for row in source_rows if row["split_name"] == "final_holdout"]
        selected_rows_by_anchor = {
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
            for row in _select_rows_by_score(final_rows, profile=profile)
        }
        for anchor_timestamp in sorted(selected_rows_by_anchor):
            strict_row = _single_family_row(
                final_rows,
                anchor_timestamp=anchor_timestamp,
                candidate_family=CANDIDATE_FAMILY_STRICT,
            )
            raw_row = _single_family_row(
                final_rows,
                anchor_timestamp=anchor_timestamp,
                candidate_family=CANDIDATE_FAMILY_RAW,
            )
            selected_row = selected_rows_by_anchor[anchor_timestamp]
            rows.append(
                _strict_benchmark_row(
                    strict_row,
                    source_model_name=source_model_name,
                    ranker_row=ranker_row,
                    generated_at=resolved_generated_at,
                    as_ranker=False,
                )
            )
            rows.append(
                _strict_benchmark_row(
                    raw_row,
                    source_model_name=source_model_name,
                    ranker_row=ranker_row,
                    generated_at=resolved_generated_at,
                    as_ranker=False,
                )
            )
            rows.append(
                _strict_benchmark_row(
                    selected_row,
                    source_model_name=source_model_name,
                    ranker_row=ranker_row,
                    generated_at=resolved_generated_at,
                    as_ranker=True,
                )
            )
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name", "anchor_timestamp", "forecast_model_name"])


def evaluate_dfl_trajectory_feature_ranker_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate feature-ranker development evidence and production promotion readiness."""

    _require_columns(strict_frame, REQUIRED_EVALUATION_COLUMNS, frame_name="strict_frame")
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(False, "blocked", "trajectory feature-ranker strict frame has no rows", {})
    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    for source_model_name in source_names:
        source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
        if not source_rows:
            failures.append(f"{source_model_name} has no trajectory feature-ranker rows")
            continue
        summary, summary_failures = _selector_gate_summary(
            source_rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    if not summaries:
        return PromotionGateResult(False, "blocked", "; ".join(failures), {})

    production_passing = [summary for summary in summaries if summary["production_gate_passed"]]
    development_passing = [summary for summary in summaries if summary["development_gate_passed"]]
    best = max(summaries, key=lambda summary: float(summary["mean_regret_improvement_ratio_vs_strict"]))
    metrics = {
        "best_source_model_name": best["source_model_name"],
        "tenant_count": best["tenant_count"],
        "validation_tenant_anchor_count": best["validation_tenant_anchor_count"],
        "strict_mean_regret_uah": best["strict_mean_regret_uah"],
        "raw_mean_regret_uah": best["raw_mean_regret_uah"],
        "selected_mean_regret_uah": best["selected_mean_regret_uah"],
        "strict_median_regret_uah": best["strict_median_regret_uah"],
        "selected_median_regret_uah": best["selected_median_regret_uah"],
        "mean_regret_improvement_ratio_vs_strict": best["mean_regret_improvement_ratio_vs_strict"],
        "mean_regret_improvement_ratio_vs_raw": best["mean_regret_improvement_ratio_vs_raw"],
        "development_gate_passed": bool(development_passing),
        "production_gate_passed": bool(production_passing),
        "passing_source_model_names": [str(summary["source_model_name"]) for summary in production_passing],
        "model_summaries": summaries,
    }
    if production_passing and not failures:
        return PromotionGateResult(True, "promote", "trajectory feature ranker passes strict LP/oracle gate", metrics)
    if development_passing:
        return PromotionGateResult(
            False,
            "diagnostic_pass_production_blocked",
            "trajectory feature ranker improves over raw neural schedules but remains blocked versus "
            f"{control_model_name}: " + "; ".join(failures),
            metrics,
        )
    description = "; ".join(failures) if failures else "trajectory feature ranker has no development improvement"
    return PromotionGateResult(False, "blocked", description, metrics)


def _candidate_row_from_evaluation_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    candidate_family: str,
    split_name: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(_payload(row))
    _require_payload_provenance(payload)
    horizon = _horizon_rows(payload)
    horizon_hours = int(row["horizon_hours"])
    _validate_horizon_length(horizon, horizon_hours=horizon_hours)
    forecast_prices = _float_vector(horizon, keys=("forecast_price_uah_mwh",))
    actual_prices = _float_vector(horizon, keys=("actual_price_uah_mwh",))
    dispatch = _float_vector(horizon, keys=("net_power_mw",), default=0.0)
    soc = _float_vector(horizon, keys=("soc_fraction",), default=float(row["starting_soc_fraction"]))
    payload.update(
        {
            "candidate_family": candidate_family,
            "source_forecast_model_name": source_model_name,
            "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_CLAIM_SCOPE,
            "academic_scope": DFL_TRAJECTORY_FEATURE_RANKER_ACADEMIC_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
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
        "peak_index_abs_error": float(abs(_extreme_index(forecast_prices, largest=True) - _extreme_index(actual_prices, largest=True))),
        "trough_index_abs_error": float(abs(_extreme_index(forecast_prices, largest=False) - _extreme_index(actual_prices, largest=False))),
        "soc_min_slack_fraction": min(min(soc), 1.0 - max(soc)) if soc else 0.0,
        "prior_family_mean_regret_uah": float(row["regret_uah"]),
        "safety_violation_count": _safety_violation_count(payload),
        "data_quality_tier": str(payload.get("data_quality_tier", "demo_grade")),
        "observed_coverage_ratio": float(payload.get("observed_coverage_ratio", 0.0)),
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_CLAIM_SCOPE,
        "evaluation_payload": payload,
    }


def _perturbation_rows(
    source_row: dict[str, Any],
    *,
    source_model_name: str,
    split_name: str,
    perturb_spread_scale_grid: tuple[float, ...],
    perturb_mean_shift_grid_uah_mwh: tuple[float, ...],
    generated_at: datetime,
) -> list[dict[str, Any]]:
    payload = _payload(source_row)
    _require_payload_provenance(payload)
    anchor_timestamp = _datetime_value(source_row["anchor_timestamp"], field_name="anchor_timestamp")
    raw_forecast_prices = _float_vector(_horizon_rows(payload), keys=("forecast_price_uah_mwh",))
    tenant_defaults = tenant_battery_defaults_from_registry(str(source_row["tenant_id"]))
    rows: list[dict[str, Any]] = []
    for spread_scale in perturb_spread_scale_grid:
        for mean_shift in perturb_mean_shift_grid_uah_mwh:
            model_name = _perturbation_model_name(
                source_model_name,
                spread_scale=spread_scale,
                mean_shift_uah_mwh=mean_shift,
            )
            evaluation = evaluate_forecast_candidates_against_oracle(
                price_history=_price_history_from_payload(payload, anchor_timestamp=anchor_timestamp),
                tenant_id=str(source_row["tenant_id"]),
                battery_metrics=tenant_defaults.metrics,
                starting_soc_fraction=float(source_row["starting_soc_fraction"]),
                starting_soc_source=str(source_row["starting_soc_source"]),
                anchor_timestamp=anchor_timestamp,
                candidates=[
                    ForecastCandidate(
                        model_name=model_name,
                        forecast_frame=_forecast_frame_from_prices(
                            payload,
                            anchor_timestamp=anchor_timestamp,
                            forecast_prices=_perturbed_prices(
                                raw_forecast_prices,
                                spread_scale=spread_scale,
                                mean_shift_uah_mwh=mean_shift,
                            ),
                        ),
                        point_prediction_column="predicted_price_uah_mwh",
                    )
                ],
                evaluation_id=(
                    f"{source_row['tenant_id']}:schedule-library-perturbation:"
                    f"{source_model_name}:{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
                ),
                generated_at=generated_at,
            )
            evaluated_row = evaluation.row(0, named=True)
            evaluated_payload = dict(_payload(evaluated_row))
            evaluated_payload.update(
                {
                    "data_quality_tier": payload.get("data_quality_tier", "thesis_grade"),
                    "observed_coverage_ratio": payload.get("observed_coverage_ratio", 1.0),
                    "safety_violation_count": _safety_violation_count(payload),
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "source_forecast_model_name": source_model_name,
                }
            )
            evaluated_row["evaluation_payload"] = evaluated_payload
            rows.append(
                _candidate_row_from_evaluation_row(
                    evaluated_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_PERTURBATION,
                    split_name=split_name,
                    generated_at=generated_at,
                )
            )
    return rows


def _final_only_candidate_rows(
    frame: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    final_anchors: set[datetime],
    generated_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_rows = frame.filter(
        (pl.col("tenant_id") == tenant_id)
        & (pl.col("source_model_name") == source_model_name)
        & (pl.col("candidate_family").is_in(list(FINAL_ONLY_FAMILIES)))
    )
    for row in source_rows.iter_rows(named=True):
        anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        if anchor_timestamp not in final_anchors:
            continue
        candidate_row = _candidate_row_from_candidate_panel_row(row, generated_at=generated_at)
        rows.append(candidate_row)
    missing_families = FINAL_ONLY_FAMILIES.difference({str(row["candidate_family"]) for row in rows})
    if missing_families:
        raise ValueError(
            f"missing final-only schedule families for {tenant_id}/{source_model_name}: {sorted(missing_families)}"
        )
    return rows


def _candidate_row_from_candidate_panel_row(row: dict[str, Any], *, generated_at: datetime) -> dict[str, Any]:
    payload = dict(_payload(row))
    _require_payload_provenance(payload)
    horizon = _horizon_rows(payload)
    horizon_hours = int(row["horizon_hours"])
    _validate_horizon_length(horizon, horizon_hours=horizon_hours)
    forecast_prices = _float_vector(horizon, keys=("forecast_price_uah_mwh",))
    actual_prices = _float_vector(horizon, keys=("actual_price_uah_mwh",))
    dispatch = _float_vector(horizon, keys=("net_power_mw",), default=0.0)
    soc = _float_vector(horizon, keys=("soc_fraction",), default=0.5)
    payload.update(
        {
            "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_CLAIM_SCOPE,
            "academic_scope": DFL_TRAJECTORY_FEATURE_RANKER_ACADEMIC_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": str(row["source_model_name"]),
        "candidate_family": str(row["candidate_family"]),
        "candidate_model_name": str(row["candidate_model_name"]),
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        "generated_at": generated_at,
        "split_name": "final_holdout",
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
        "peak_index_abs_error": float(abs(_extreme_index(forecast_prices, largest=True) - _extreme_index(actual_prices, largest=True))),
        "trough_index_abs_error": float(abs(_extreme_index(forecast_prices, largest=False) - _extreme_index(actual_prices, largest=False))),
        "soc_min_slack_fraction": min(min(soc), 1.0 - max(soc)) if soc else 0.0,
        "prior_family_mean_regret_uah": float(row["prior_selection_mean_regret_uah"]),
        "safety_violation_count": int(row["safety_violation_count"]),
        "data_quality_tier": str(row["data_quality_tier"]),
        "observed_coverage_ratio": float(row["observed_coverage_ratio"]),
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_CLAIM_SCOPE,
        "evaluation_payload": payload,
    }


def _with_prior_family_scores(frame: pl.DataFrame) -> pl.DataFrame:
    train_rows = frame.filter(pl.col("split_name") == "train_selection")
    if train_rows.height == 0:
        return frame
    prior_scores = (
        train_rows
        .group_by(["tenant_id", "source_model_name", "candidate_family"])
        .agg(pl.mean("regret_uah").alias("computed_prior_family_mean_regret_uah"))
    )
    joined = frame.join(prior_scores, on=["tenant_id", "source_model_name", "candidate_family"], how="left")
    return joined.with_columns(
        pl.coalesce(
            ["computed_prior_family_mean_regret_uah", "prior_family_mean_regret_uah"]
        ).alias("prior_family_mean_regret_uah")
    ).drop("computed_prior_family_mean_regret_uah")


def _validate_library_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_LIBRARY_COLUMNS, frame_name="schedule_candidate_library_frame")
    for row in frame.iter_rows(named=True):
        horizon_hours = int(row["horizon_hours"])
        for column in (
            "forecast_price_uah_mwh_vector",
            "actual_price_uah_mwh_vector",
            "dispatch_mw_vector",
            "soc_fraction_vector",
        ):
            if len(_float_list(row[column], field_name=column)) != horizon_hours:
                raise ValueError(f"vector length must match horizon_hours for {column}")
        if str(row["data_quality_tier"]) != "thesis_grade":
            raise ValueError("schedule candidate library requires thesis_grade rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("schedule candidate library requires observed coverage ratio of 1.0")
        if int(row["safety_violation_count"]):
            raise ValueError("schedule candidate library requires zero safety violations")
        if not bool(row["not_full_dfl"]):
            raise ValueError("schedule candidate library requires not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("schedule candidate library requires not_market_execution=true")
    split_by_anchor: dict[tuple[str, str, datetime], set[str]] = {}
    for row in frame.iter_rows(named=True):
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        split_by_anchor.setdefault(key, set()).add(str(row["split_name"]))
    if any(len(splits) > 1 for splits in split_by_anchor.values()):
        raise ValueError("train/final overlap is not allowed in schedule candidate library")


def _ranker_row(
    *,
    tenant_id: str,
    source_model_name: str,
    profile: dict[str, float | str],
    train_rows: list[dict[str, Any]],
    selected_train_rows: list[dict[str, Any]],
    final_rows: list[dict[str, Any]],
    selected_final_rows: list[dict[str, Any]],
    min_final_holdout_tenant_anchor_count_per_source_model: int,
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "ranker_model_name": trajectory_feature_ranker_model_name(source_model_name),
        "selected_weight_profile_name": str(profile["name"]),
        "weight_prior_family_mean_regret_uah": float(profile["prior_family_mean_regret_uah"]),
        "weight_forecast_spread_uah_mwh": float(profile["forecast_spread_uah_mwh"]),
        "weight_total_degradation_penalty_uah": float(profile["total_degradation_penalty_uah"]),
        "weight_total_throughput_mwh": float(profile["total_throughput_mwh"]),
        "weight_soc_min_slack_fraction": float(profile["soc_min_slack_fraction"]),
        "train_selection_anchor_count": len(_anchor_set(train_rows)),
        "final_holdout_anchor_count": len(_anchor_set(final_rows)),
        "final_holdout_tenant_anchor_count": len(_tenant_anchor_set(final_rows)),
        "min_final_holdout_tenant_anchor_count_per_source_model": (
            min_final_holdout_tenant_anchor_count_per_source_model
        ),
        "selected_train_mean_regret_uah": _mean_regret(selected_train_rows),
        "selected_final_mean_regret_uah": _mean_regret(selected_final_rows),
        "selected_final_median_regret_uah": _median_regret(selected_final_rows),
        "selected_family_counts": _family_counts(selected_final_rows),
        "claim_scope": DFL_TRAJECTORY_FEATURE_RANKER_CLAIM_SCOPE,
        "academic_scope": DFL_TRAJECTORY_FEATURE_RANKER_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _select_weight_profile(train_rows: list[dict[str, Any]]) -> dict[str, float | str]:
    return min(
        WEIGHT_PROFILES,
        key=lambda profile: (
            _mean_regret(_select_rows_by_score(train_rows, profile=profile)),
            str(profile["name"]),
        ),
    )


def _select_rows_by_score(
    rows: list[dict[str, Any]],
    *,
    profile: dict[str, float | str],
) -> list[dict[str, Any]]:
    by_anchor: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        by_anchor.setdefault(_datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"), []).append(row)
    selected: list[dict[str, Any]] = []
    for anchor_rows in by_anchor.values():
        selected.append(
            min(
                anchor_rows,
                key=lambda row: (
                    _ranker_score(row, profile=profile),
                    _family_sort_index(str(row["candidate_family"])),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return sorted(selected, key=lambda row: _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"))


def _ranker_score(row: dict[str, Any], *, profile: dict[str, float | str]) -> float:
    return (
        float(profile["prior_family_mean_regret_uah"]) * float(row["prior_family_mean_regret_uah"])
        + float(profile["forecast_spread_uah_mwh"]) * float(row["forecast_spread_uah_mwh"])
        + float(profile["total_degradation_penalty_uah"]) * float(row["total_degradation_penalty_uah"])
        + float(profile["total_throughput_mwh"]) * float(row["total_throughput_mwh"])
        + float(profile["soc_min_slack_fraction"]) * float(row.get("soc_min_slack_fraction", 0.0))
    )


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    ranker_row: dict[str, Any],
    generated_at: datetime,
    as_ranker: bool,
) -> dict[str, Any]:
    payload = dict(_payload(row))
    candidate_family = str(row["candidate_family"])
    forecast_model_name = (
        trajectory_feature_ranker_model_name(source_model_name)
        if as_ranker
        else str(row["candidate_model_name"])
    )
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    payload.update(
        {
            "strict_gate_kind": "dfl_trajectory_feature_ranker_strict_lp",
            "source_forecast_model_name": source_model_name,
            "ranker_model_name": trajectory_feature_ranker_model_name(source_model_name),
            "ranker_selected_weight_profile_name": str(ranker_row["selected_weight_profile_name"]),
            "ranker_row_candidate_family": candidate_family,
            "ranker_row_candidate_model_name": str(row["candidate_model_name"]),
            "ranker_row_role": "ranker" if as_ranker else "reference",
            "claim_scope": DFL_TRAJECTORY_FEATURE_RANKER_STRICT_CLAIM_SCOPE,
            "academic_scope": DFL_TRAJECTORY_FEATURE_RANKER_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:trajectory-feature-ranker:{source_model_name}:"
            f"{'ranker' if as_ranker else 'reference'}:{candidate_family}:"
            f"{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_TRAJECTORY_FEATURE_RANKER_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _first_or_default(row["soc_fraction_vector"], default=0.5),
        "starting_soc_source": "schedule_candidate_library",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": _committed_action(row),
        "committed_power_mw": abs(_first_or_default(row["dispatch_mw_vector"], default=0.0)),
        "rank_by_regret": 1,
        "evaluation_payload": payload,
    }


def _selector_gate_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    control_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    selected_model_name = trajectory_feature_ranker_model_name(source_model_name)
    strict_rows = [row for row in rows if row["forecast_model_name"] == control_model_name]
    raw_rows = [row for row in rows if row["forecast_model_name"] == source_model_name]
    selected_rows = [row for row in rows if row["forecast_model_name"] == selected_model_name]
    strict_anchors = _tenant_anchor_set(strict_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    selected_anchors = _tenant_anchor_set(selected_rows)
    if strict_anchors != raw_anchors or strict_anchors != selected_anchors:
        failures.append(f"{source_model_name} strict/raw/ranker rows must cover matching tenant-anchor sets")
    tenant_count = len({tenant_id for tenant_id, _ in selected_anchors})
    validation_count = len(selected_anchors)
    if tenant_count < min_tenant_count:
        failures.append(f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    failures.extend(_provenance_failures([*strict_rows, *raw_rows, *selected_rows]))
    strict_mean = _mean_regret(strict_rows)
    raw_mean = _mean_regret(raw_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    selected_median = _median_regret(selected_rows)
    improvement_vs_raw = _improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = _improvement_ratio(strict_mean, selected_mean)
    development_passed = validation_count >= min_validation_tenant_anchor_count and improvement_vs_raw > 0.0
    production_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio
        and selected_median <= strict_median
        and not failures
    )
    if selected_rows and strict_rows and improvement_vs_strict < min_mean_regret_improvement_ratio:
        failures.append(
            f"{source_model_name} mean regret improvement vs {control_model_name} must be at least "
            f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
        )
    if selected_rows and strict_rows and selected_median > strict_median:
        failures.append(
            f"{source_model_name} median regret must not be worse than {control_model_name}; "
            f"observed ranker={selected_median:.2f}, strict={strict_median:.2f}"
        )
    return {
        "source_model_name": source_model_name,
        "ranker_model_name": selected_model_name,
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "development_gate_passed": development_passed,
        "production_gate_passed": production_passed,
        "failures": failures,
    }, failures


def _source_rows(frame: pl.DataFrame, *, tenant_id: str, forecast_model_name: str) -> list[dict[str, Any]]:
    rows = (
        frame
        .filter((pl.col("tenant_id") == tenant_id) & (pl.col("forecast_model_name") == forecast_model_name))
        .sort("anchor_timestamp")
        .iter_rows(named=True)
    )
    result = list(rows)
    if not result:
        raise ValueError(f"missing benchmark rows for {tenant_id}/{forecast_model_name}")
    return result


def _control_rows_by_anchor(
    frame: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
) -> dict[datetime, dict[str, Any]]:
    control_rows = list(
        frame
        .filter((pl.col("tenant_id") == tenant_id) & (pl.col("forecast_model_name") == CONTROL_MODEL_NAME))
        .sort("anchor_timestamp")
        .iter_rows(named=True)
    )
    if not control_rows:
        raise ValueError(f"missing strict_similar_day rows for {tenant_id}")
    by_anchor: dict[datetime, dict[str, Any]] = {}
    for row in control_rows:
        anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        if anchor_timestamp not in by_anchor or _source_model_name(row) == source_model_name:
            by_anchor[anchor_timestamp] = row
    return by_anchor


def _latest_anchors(rows: list[dict[str, Any]], *, count: int) -> list[datetime]:
    anchors = sorted({_datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") for row in rows})
    if len(anchors) < count:
        raise ValueError(f"not enough anchors for final holdout; observed {len(anchors)}, expected {count}")
    return anchors[-count:]


def _library_rows(frame: pl.DataFrame, *, tenant_id: str, source_model_name: str) -> list[dict[str, Any]]:
    rows = [
        row
        for row in frame.iter_rows(named=True)
        if row["tenant_id"] == tenant_id and row["source_model_name"] == source_model_name
    ]
    if not rows:
        raise ValueError(f"missing schedule candidate rows for {tenant_id}/{source_model_name}")
    return rows


def _single_family_row(
    rows: list[dict[str, Any]],
    *,
    anchor_timestamp: datetime,
    candidate_family: str,
) -> dict[str, Any]:
    matches = [
        row
        for row in rows
        if row["candidate_family"] == candidate_family
        and _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") == anchor_timestamp
    ]
    if not matches:
        raise ValueError(f"missing {candidate_family} row for {anchor_timestamp.isoformat()}")
    return matches[0]


def _forecast_frame_from_prices(
    payload: dict[str, Any],
    *,
    anchor_timestamp: datetime,
    forecast_prices: list[float],
) -> pl.DataFrame:
    horizon = _horizon_rows(payload)
    if len(forecast_prices) != len(horizon):
        raise ValueError("perturbation forecast vector length must match horizon length")
    return pl.DataFrame(
        {
            "forecast_timestamp": [
                _future_interval_start(point, anchor_timestamp=anchor_timestamp)
                for point in horizon
            ],
            "predicted_price_uah_mwh": forecast_prices,
        }
    )


def _price_history_from_payload(payload: dict[str, Any], *, anchor_timestamp: datetime) -> pl.DataFrame:
    horizon = _horizon_rows(payload)
    return pl.DataFrame(
        {
            DEFAULT_TIMESTAMP_COLUMN: [
                _future_interval_start(point, anchor_timestamp=anchor_timestamp)
                for point in horizon
            ],
            DEFAULT_PRICE_COLUMN: [
                float(point["actual_price_uah_mwh"])
                for point in horizon
            ],
        }
    )


def _perturbed_prices(
    raw_forecast_prices: list[float],
    *,
    spread_scale: float,
    mean_shift_uah_mwh: float,
) -> list[float]:
    raw_mean = mean(raw_forecast_prices)
    return [
        raw_mean + spread_scale * (price - raw_mean) + mean_shift_uah_mwh
        for price in raw_forecast_prices
    ]


def _perturbation_model_name(
    source_model_name: str,
    *,
    spread_scale: float,
    mean_shift_uah_mwh: float,
) -> str:
    return (
        f"{DFL_FORECAST_PERTURBATION_PREFIX}_spread_{_token(spread_scale)}"
        f"_shift_{_token(mean_shift_uah_mwh)}_{source_model_name}"
    )


def _token(value: float) -> str:
    prefix = "m" if value < 0 else "p"
    return f"{prefix}{abs(value):.2f}".replace(".", "p")


def _weight_profile_by_name(name: str) -> dict[str, float | str]:
    for profile in WEIGHT_PROFILES:
        if profile["name"] == name:
            return profile
    raise ValueError(f"unknown ranker weight profile: {name}")


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("evaluation_payload")
    if not isinstance(value, dict):
        raise ValueError("evaluation_payload must be a dict")
    return value


def _horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list) or not horizon:
        raise ValueError("evaluation_payload.horizon must be a non-empty list")
    if not all(isinstance(row, dict) for row in horizon):
        raise ValueError("evaluation_payload.horizon must contain dict rows")
    return horizon


def _validate_horizon_length(horizon: list[dict[str, Any]], *, horizon_hours: int) -> None:
    if len(horizon) != horizon_hours:
        raise ValueError(f"vector length must match horizon_hours; observed {len(horizon)} vs {horizon_hours}")


def _float_vector(
    horizon: list[dict[str, Any]],
    *,
    keys: tuple[str, ...],
    default: float | None = None,
) -> list[float]:
    values: list[float] = []
    for point in horizon:
        value = None
        for key in keys:
            if key in point:
                value = point[key]
                break
        if value is None:
            if default is None:
                raise ValueError(f"horizon point is missing one of {keys}")
            value = default
        values.append(float(value))
    return values


def _float_list(value: object, *, field_name: str) -> list[float]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list.")
    return [float(item) for item in value]


def _future_interval_start(point: dict[str, Any], *, anchor_timestamp: datetime) -> datetime:
    raw_value = point.get("interval_start")
    if isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, str):
        return datetime.fromisoformat(raw_value)
    return anchor_timestamp


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO datetime.") from exc
    raise ValueError(f"{field_name} must be a datetime.")


def _source_model_name(row: dict[str, Any]) -> str:
    if "source_model_name" in row and row["source_model_name"]:
        return str(row["source_model_name"])
    payload = _payload(row)
    return str(payload.get("source_forecast_model_name", ""))


def _require_payload_provenance(payload: dict[str, Any]) -> None:
    if str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade":
        raise ValueError("schedule candidate library requires thesis_grade rows")
    if float(payload.get("observed_coverage_ratio", 0.0)) < 1.0:
        raise ValueError("schedule candidate library requires observed coverage ratio of 1.0")
    if _safety_violation_count(payload):
        raise ValueError("schedule candidate library requires zero safety violations")
    if payload.get("not_full_dfl") is False:
        raise ValueError("schedule candidate library requires not_full_dfl=true")
    if payload.get("not_market_execution") is False:
        raise ValueError("schedule candidate library requires not_market_execution=true")


def _safety_violation_count(payload: dict[str, Any]) -> int:
    if "safety_violation_count" in payload:
        value = payload["safety_violation_count"]
        return 0 if value is None else int(value)
    safety_violations = payload.get("safety_violations")
    if isinstance(safety_violations, list):
        return len(safety_violations)
    return 0


def _spread(values: list[float]) -> float:
    return max(values) - min(values) if values else 0.0


def _top_k_overlap(forecast_values: list[float], actual_values: list[float], *, largest: bool) -> float:
    k = min(3, len(forecast_values), len(actual_values))
    if k <= 0:
        return 0.0
    forecast_indices = set(_top_k_indices(forecast_values, k=k, largest=largest))
    actual_indices = set(_top_k_indices(actual_values, k=k, largest=largest))
    return len(forecast_indices.intersection(actual_indices)) / k


def _top_k_indices(values: list[float], *, k: int, largest: bool) -> list[int]:
    return [
        index
        for index, _ in sorted(
            enumerate(values),
            key=lambda item: (item[1], -item[0]),
            reverse=largest,
        )[:k]
    ]


def _extreme_index(values: list[float], *, largest: bool) -> int:
    if not values:
        return 0
    return max(range(len(values)), key=values.__getitem__) if largest else min(range(len(values)), key=values.__getitem__)


def _first_or_default(value: object, *, default: float) -> float:
    values = _float_list(value, field_name="vector")
    return values[0] if values else default


def _committed_action(row: dict[str, Any]) -> str:
    committed_power = _first_or_default(row["dispatch_mw_vector"], default=0.0)
    if committed_power > 0.0:
        return "DISCHARGE"
    if committed_power < 0.0:
        return "CHARGE"
    return "HOLD"


def _family_sort_index(candidate_family: str) -> int:
    if candidate_family in REFERENCE_FAMILY_ORDER:
        return REFERENCE_FAMILY_ORDER.index(candidate_family)
    return len(REFERENCE_FAMILY_ORDER)


def _family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        family = str(row["candidate_family"])
        counts[family] = counts.get(family, 0) + 1
    return dict(sorted(counts.items()))


def _anchor_set(rows: list[dict[str, Any]]) -> set[datetime]:
    return {_datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") for row in rows}


def _tenant_anchor_set(rows: list[dict[str, Any]]) -> set[tuple[str, datetime]]:
    return {
        (str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"))
        for row in rows
    }


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    return mean(float(row["regret_uah"]) for row in rows) if rows else 0.0


def _median_regret(rows: list[dict[str, Any]]) -> float:
    return median(float(row["regret_uah"]) for row in rows) if rows else 0.0


def _improvement_ratio(baseline_mean: float, candidate_mean: float) -> float:
    return (baseline_mean - candidate_mean) / abs(baseline_mean) if abs(baseline_mean) > 1e-9 else 0.0


def _provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    payloads = [_payload(row) for row in rows]
    if any(str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade" for payload in payloads):
        failures.append("trajectory feature-ranker promotion requires thesis_grade evidence")
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        failures.append("trajectory feature-ranker promotion requires observed coverage ratio of 1.0")
    safety_violation_count = sum(_safety_violation_count(payload) for payload in payloads)
    if safety_violation_count:
        failures.append(f"trajectory feature-ranker promotion requires zero safety violations; observed {safety_violation_count}")
    if any(not bool(payload.get("not_full_dfl", False)) for payload in payloads):
        failures.append("trajectory feature-ranker promotion evidence must remain not_full_dfl")
    if any(not bool(payload.get("not_market_execution", False)) for payload in payloads):
        failures.append("trajectory feature-ranker promotion evidence must remain not_market_execution")
    return failures


def _validate_common_config(
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


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing required columns: {missing_columns}")
