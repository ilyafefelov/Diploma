"""Prior-only schedule/value learner v3 research challenger."""

from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_SCHEDULE_VALUE_LEARNER_V3_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_learner_v3_not_full_dfl"
)
DFL_SCHEDULE_VALUE_LEARNER_V3_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_learner_v3_strict_lp_gate_not_full_dfl"
)
DFL_SCHEDULE_VALUE_LEARNER_V3_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_schedule_value_learner_v3_strict_lp_benchmark"
)
DFL_SCHEDULE_VALUE_LEARNER_V3_PREFIX: Final[str] = "dfl_schedule_value_learner_v3_"
DFL_SCHEDULE_VALUE_LEARNER_V3_PROFILE_NAME: Final[str] = "ridge_regret_ranker_v3"
DFL_SCHEDULE_VALUE_LEARNER_V3_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only ridge-style schedule/value ranker over feasible LP-scored schedules. "
    "This is additive DFL research evidence, not full DFL, not Decision Transformer "
    "control, and not market execution."
)

DEFAULT_V3_FEATURE_NAMES: Final[tuple[str, ...]] = (
    "prior_family_mean_regret_uah",
    "forecast_spread_uah_mwh",
    "forecast_objective_value_uah",
    "total_degradation_penalty_uah",
    "total_throughput_mwh",
    "soc_min_slack_fraction",
)

REQUIRED_MODEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "learner_model_name",
        "selected_weight_profile_name",
        "selected_feature_names",
        "selected_feature_weights",
        "selected_feature_stats",
        "train_anchor_count",
        "final_holdout_anchor_count",
        "selected_train_mean_regret_uah",
        "selected_final_mean_regret_uah",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "strategy_kind",
        "anchor_timestamp",
        "generated_at",
        "regret_uah",
        "selection_role",
        "evaluation_payload",
    }
)


def schedule_value_learner_v3_model_name(source_model_name: str) -> str:
    """Return the stable DFL v3 schedule/value learner model name."""

    return f"{DFL_SCHEDULE_VALUE_LEARNER_V3_PREFIX}{source_model_name}"


def build_dfl_schedule_value_learner_v3_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    feature_names: tuple[str, ...] = DEFAULT_V3_FEATURE_NAMES,
    ridge_regularization: float = 1.0,
) -> pl.DataFrame:
    """Fit a tiny prior-only value ranker and select final schedules."""

    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        feature_names=feature_names,
        ridge_regularization=ridge_regularization,
    )
    v2._validate_library_frame(schedule_candidate_library_frame)
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = v2._library_rows(
                schedule_candidate_library_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            train_rows = [
                row
                for row in source_rows
                if str(row["split_name"]) == "train_selection"
            ]
            final_rows = [
                row for row in source_rows if str(row["split_name"]) == "final_holdout"
            ]
            final_anchor_count = len(v2._anchor_set(final_rows))
            if final_anchor_count != final_validation_anchor_count_per_tenant:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} final-holdout tenant-anchor count must be "
                    f"{final_validation_anchor_count_per_tenant}; observed {final_anchor_count}"
                )
            if not train_rows:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} schedule/value learner v3 needs train rows"
                )

            model = _fit_ridge_ranker(
                train_rows,
                feature_names=feature_names,
                ridge_regularization=ridge_regularization,
            )
            selected_train_rows = _select_rows_by_model(train_rows, model=model)
            selected_final_rows = _select_rows_by_model(final_rows, model=model)
            strict_train_rows = v2._selected_family_rows(
                train_rows, v2.CANDIDATE_FAMILY_STRICT
            )
            raw_train_rows = v2._selected_family_rows(
                train_rows, v2.CANDIDATE_FAMILY_RAW
            )
            strict_final_rows = v2._selected_family_rows(
                final_rows, v2.CANDIDATE_FAMILY_STRICT
            )
            raw_final_rows = v2._selected_family_rows(
                final_rows, v2.CANDIDATE_FAMILY_RAW
            )
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": schedule_value_learner_v3_model_name(
                        source_model_name
                    ),
                    "selected_weight_profile_name": DFL_SCHEDULE_VALUE_LEARNER_V3_PROFILE_NAME,
                    "selected_feature_names": list(feature_names),
                    "selected_feature_weights": model["weights"],
                    "selected_feature_stats": model["feature_stats"],
                    "train_anchor_count": len(v2._anchor_set(train_rows)),
                    "final_holdout_anchor_count": final_anchor_count,
                    "final_holdout_tenant_anchor_count": final_anchor_count
                    * len(tenant_ids),
                    "strict_train_mean_regret_uah": v2._mean_regret(strict_train_rows),
                    "raw_train_mean_regret_uah": v2._mean_regret(raw_train_rows),
                    "selected_train_mean_regret_uah": v2._mean_regret(
                        selected_train_rows
                    ),
                    "strict_train_median_regret_uah": v2._median_regret(
                        strict_train_rows
                    ),
                    "selected_train_median_regret_uah": v2._median_regret(
                        selected_train_rows
                    ),
                    "strict_final_mean_regret_uah": v2._mean_regret(strict_final_rows),
                    "raw_final_mean_regret_uah": v2._mean_regret(raw_final_rows),
                    "selected_final_mean_regret_uah": v2._mean_regret(
                        selected_final_rows
                    ),
                    "strict_final_median_regret_uah": v2._median_regret(
                        strict_final_rows
                    ),
                    "selected_final_median_regret_uah": v2._median_regret(
                        selected_final_rows
                    ),
                    "selected_train_family_counts": v2._family_counts(
                        selected_train_rows
                    ),
                    "selected_final_family_counts": v2._family_counts(
                        selected_final_rows
                    ),
                    "train_mean_regret_improvement_ratio_vs_strict": v2._improvement_ratio(
                        v2._mean_regret(strict_train_rows),
                        v2._mean_regret(selected_train_rows),
                    ),
                    "final_mean_regret_improvement_ratio_vs_strict": v2._improvement_ratio(
                        v2._mean_regret(strict_final_rows),
                        v2._mean_regret(selected_final_rows),
                    ),
                    "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V3_CLAIM_SCOPE,
                    "academic_scope": DFL_SCHEDULE_VALUE_LEARNER_V3_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_schedule_value_learner_v3_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    learner_v3_frame: pl.DataFrame,
    learner_v2_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict/raw/v2/v3 rows for the schedule-value DFL v3 gate."""

    v2._validate_library_frame(schedule_candidate_library_frame)
    _validate_learner_v3_frame(learner_v3_frame)
    v2._validate_learner_frame(learner_v2_frame)
    resolved_generated_at = generated_at or v2._latest_generated_at(
        schedule_candidate_library_frame
    )
    rows: list[dict[str, Any]] = []
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    v2_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_frame.iter_rows(named=True)
    }
    for learner_row in learner_v3_frame.iter_rows(named=True):
        tenant_id = str(learner_row["tenant_id"])
        source_model_name = str(learner_row["source_model_name"])
        v2_learner_row = v2_rows.get((tenant_id, source_model_name))
        if v2_learner_row is None:
            raise ValueError(
                f"missing v2 learner row for {tenant_id}/{source_model_name}"
            )

        final_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and str(row["split_name"]) == "final_holdout"
        ]
        v3_model = _model_from_learner_row(learner_row)
        selected_v3_by_anchor = {
            v2._datetime_value(
                row["anchor_timestamp"], field_name="anchor_timestamp"
            ): row
            for row in _select_rows_by_model(final_rows, model=v3_model)
        }
        selected_v2_by_anchor = {
            v2._datetime_value(
                row["anchor_timestamp"], field_name="anchor_timestamp"
            ): row
            for row in v2._select_rows_by_score(
                final_rows,
                profile=v2._profile_by_name(
                    str(v2_learner_row["selected_weight_profile_name"])
                ),
            )
        }
        for anchor_timestamp in sorted(selected_v3_by_anchor):
            anchor_rows = [
                row
                for row in final_rows
                if v2._datetime_value(
                    row["anchor_timestamp"], field_name="anchor_timestamp"
                )
                == anchor_timestamp
            ]
            strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
            raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
            v2_row = selected_v2_by_anchor[anchor_timestamp]
            v3_row = selected_v3_by_anchor[anchor_timestamp]
            rows.extend(
                [
                    _strict_benchmark_row(
                        strict_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="strict_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row(
                        raw_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="raw_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row(
                        v2_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v2_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row(
                        v3_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v3",
                        generated_at=resolved_generated_at,
                    ),
                ]
            )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"]
    )


def validate_dfl_schedule_value_learner_v3_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate structural schedule/value learner v3 evidence without requiring promotion."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"schedule/value learner v3 evidence is missing required columns: {missing_columns}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False, "schedule/value learner v3 evidence has no rows", {"row_count": 0}
        )
    source_names = source_model_names or tuple(
        sorted({_source_model_name(row) for row in rows})
    )
    failures: list[str] = []
    summaries: list[dict[str, Any]] = []
    for source_model_name in source_names:
        summary, summary_failures = _gate_summary(
            rows,
            source_model_name=source_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
            include_promotion_failures=False,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    metadata = {
        "row_count": strict_frame.height,
        "source_model_count": len(source_names),
        "source_model_names": list(source_names),
        "model_summaries": summaries,
    }
    return EvidenceCheckOutcome(
        not failures,
        "Schedule/value learner v3 evidence has valid coverage and claim boundaries."
        if not failures
        else "; ".join(failures),
        metadata,
    )


def evaluate_dfl_schedule_value_learner_v3_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate the schedule/value learner v3 strict LP/oracle gate."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return PromotionGateResult(
            False,
            "blocked",
            f"schedule/value learner v3 strict frame is missing required columns: {missing_columns}",
            {},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(
            False, "blocked", "schedule/value learner v3 strict frame has no rows", {}
        )
    source_names = source_model_names or tuple(
        sorted({_source_model_name(row) for row in rows})
    )
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    for source_model_name in source_names:
        summary, summary_failures = _gate_summary(
            rows,
            source_model_name=source_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
            include_promotion_failures=True,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    production_passing = [
        summary for summary in summaries if summary["production_gate_passed"]
    ]
    development_passing = [
        summary for summary in summaries if summary["development_gate_passed"]
    ]
    best = max(
        summaries,
        key=lambda summary: float(summary["mean_regret_improvement_ratio_vs_strict"]),
    )
    metrics = {
        "best_source_model_name": best["source_model_name"],
        "tenant_count": best["tenant_count"],
        "validation_tenant_anchor_count": best["validation_tenant_anchor_count"],
        "strict_mean_regret_uah": best["strict_mean_regret_uah"],
        "raw_mean_regret_uah": best["raw_mean_regret_uah"],
        "v2_mean_regret_uah": best["v2_mean_regret_uah"],
        "selected_mean_regret_uah": best["selected_mean_regret_uah"],
        "strict_median_regret_uah": best["strict_median_regret_uah"],
        "selected_median_regret_uah": best["selected_median_regret_uah"],
        "mean_regret_improvement_ratio_vs_strict": best[
            "mean_regret_improvement_ratio_vs_strict"
        ],
        "mean_regret_improvement_ratio_vs_raw": best[
            "mean_regret_improvement_ratio_vs_raw"
        ],
        "mean_regret_improvement_ratio_vs_v2": best[
            "mean_regret_improvement_ratio_vs_v2"
        ],
        "development_gate_passed": bool(development_passing),
        "production_gate_passed": bool(production_passing),
        "market_execution_enabled": False,
        "passing_source_model_names": [
            str(summary["source_model_name"]) for summary in production_passing
        ],
        "model_summaries": summaries,
    }
    if production_passing and not failures:
        return PromotionGateResult(
            True,
            "promote",
            "schedule/value learner v3 passes strict LP/oracle gate",
            metrics,
        )
    if development_passing:
        return PromotionGateResult(
            False,
            "diagnostic_pass_production_blocked",
            "schedule/value learner v3 improves over raw neural schedules but remains blocked versus "
            f"{CONTROL_MODEL_NAME} or frozen V2 evidence: " + "; ".join(failures),
            metrics,
        )
    return PromotionGateResult(
        False,
        "blocked",
        "; ".join(failures)
        if failures
        else "schedule/value learner v3 has no development improvement",
        metrics,
    )


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
    feature_names: tuple[str, ...],
    ridge_regularization: float,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
    if not feature_names:
        raise ValueError("feature_names must contain at least one feature.")
    if ridge_regularization <= 0.0:
        raise ValueError("ridge_regularization must be positive.")


def _validate_learner_v3_frame(frame: pl.DataFrame) -> None:
    v2._require_columns(
        frame, REQUIRED_MODEL_COLUMNS, frame_name="schedule_value_learner_v3_frame"
    )
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != DFL_SCHEDULE_VALUE_LEARNER_V3_CLAIM_SCOPE:
            raise ValueError(
                "schedule/value learner v3 frame has an unexpected claim_scope"
            )
        if not bool(row["not_full_dfl"]):
            raise ValueError(
                "schedule/value learner v3 rows must keep not_full_dfl=true"
            )
        if not bool(row["not_market_execution"]):
            raise ValueError(
                "schedule/value learner v3 rows must keep not_market_execution=true"
            )


def _fit_ridge_ranker(
    train_rows: list[dict[str, Any]],
    *,
    feature_names: tuple[str, ...],
    ridge_regularization: float,
) -> dict[str, Any]:
    feature_values = {
        feature_name: [float(row.get(feature_name, 0.0)) for row in train_rows]
        for feature_name in feature_names
    }
    feature_stats = {
        feature_name: _feature_stats(values)
        for feature_name, values in feature_values.items()
    }
    matrix: list[list[float]] = []
    targets: list[float] = []
    for row in train_rows:
        matrix.append(
            _feature_vector(
                row, feature_names=feature_names, feature_stats=feature_stats
            )
        )
        targets.append(float(row["regret_uah"]))
    weights_vector = _ridge_solve(
        matrix, targets, ridge_regularization=ridge_regularization
    )
    weights = {"intercept": weights_vector[0]}
    for feature_name, weight in zip(feature_names, weights_vector[1:], strict=True):
        weights[feature_name] = weight
    return {
        "feature_names": list(feature_names),
        "feature_stats": feature_stats,
        "weights": weights,
    }


def _feature_stats(values: list[float]) -> dict[str, float]:
    feature_mean = mean(values) if values else 0.0
    if len(values) < 2:
        return {"mean": feature_mean, "scale": 1.0}
    variance = mean([(value - feature_mean) ** 2 for value in values])
    scale = variance**0.5
    if scale <= 1e-9:
        scale = 1.0
    return {"mean": feature_mean, "scale": scale}


def _feature_vector(
    row: dict[str, Any],
    *,
    feature_names: tuple[str, ...],
    feature_stats: dict[str, dict[str, float]],
) -> list[float]:
    vector = [1.0]
    for feature_name in feature_names:
        stats = feature_stats[feature_name]
        raw_value = float(row.get(feature_name, 0.0))
        vector.append((raw_value - stats["mean"]) / stats["scale"])
    return vector


def _ridge_solve(
    matrix: list[list[float]],
    targets: list[float],
    *,
    ridge_regularization: float,
) -> list[float]:
    width = len(matrix[0])
    normal_matrix = [[0.0 for _ in range(width)] for _ in range(width)]
    rhs = [0.0 for _ in range(width)]
    for row, target in zip(matrix, targets, strict=True):
        for i in range(width):
            rhs[i] += row[i] * target
            for j in range(width):
                normal_matrix[i][j] += row[i] * row[j]
    for i in range(1, width):
        normal_matrix[i][i] += ridge_regularization
    return _solve_linear_system(normal_matrix, rhs)


def _solve_linear_system(matrix: list[list[float]], rhs: list[float]) -> list[float]:
    size = len(rhs)
    augmented = [row.copy() + [rhs[index]] for index, row in enumerate(matrix)]
    for pivot_index in range(size):
        pivot_row_index = max(
            range(pivot_index, size),
            key=lambda row_index: abs(augmented[row_index][pivot_index]),
        )
        if abs(augmented[pivot_row_index][pivot_index]) <= 1e-12:
            augmented[pivot_index][pivot_index] += 1e-6
            pivot_row_index = pivot_index
        augmented[pivot_index], augmented[pivot_row_index] = (
            augmented[pivot_row_index],
            augmented[pivot_index],
        )
        pivot = augmented[pivot_index][pivot_index]
        for column_index in range(pivot_index, size + 1):
            augmented[pivot_index][column_index] /= pivot
        for row_index in range(size):
            if row_index == pivot_index:
                continue
            factor = augmented[row_index][pivot_index]
            for column_index in range(pivot_index, size + 1):
                augmented[row_index][column_index] -= (
                    factor * augmented[pivot_index][column_index]
                )
    return [augmented[index][size] for index in range(size)]


def _select_rows_by_model(
    rows: list[dict[str, Any]], *, model: dict[str, Any]
) -> list[dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    for anchor_timestamp, anchor_rows in sorted(v2._rows_by_anchor(rows).items()):
        if not any(
            str(row["candidate_family"]) == v2.CANDIDATE_FAMILY_STRICT
            for row in anchor_rows
        ):
            raise ValueError(
                f"missing strict_control row for {anchor_timestamp.isoformat()}"
            )
        selected_rows.append(
            min(
                anchor_rows,
                key=lambda row: (
                    _score_row(row, model=model),
                    v2._family_sort_index(str(row["candidate_family"])),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _model_from_learner_row(row: dict[str, Any]) -> dict[str, Any]:
    feature_names = tuple(
        str(feature_name) for feature_name in row["selected_feature_names"]
    )
    return {
        "feature_names": list(feature_names),
        "feature_stats": dict(row["selected_feature_stats"]),
        "weights": dict(row["selected_feature_weights"]),
    }


def _score_row(row: dict[str, Any], *, model: dict[str, Any]) -> float:
    feature_names = tuple(str(feature_name) for feature_name in model["feature_names"])
    feature_stats = dict(model["feature_stats"])
    weights = dict(model["weights"])
    score = float(weights.get("intercept", 0.0))
    for feature_name in feature_names:
        stats = dict(feature_stats[feature_name])
        raw_value = float(row.get(feature_name, 0.0))
        scaled_value = (raw_value - float(stats["mean"])) / float(stats["scale"])
        score += float(weights.get(feature_name, 0.0)) * scaled_value
    return score


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(v2._payload(row))
    learner_model_name = schedule_value_learner_v3_model_name(source_model_name)
    forecast_model_name = _forecast_model_name_for_role(
        row,
        source_model_name=source_model_name,
        role=role,
    )
    anchor_timestamp = v2._datetime_value(
        row["anchor_timestamp"], field_name="anchor_timestamp"
    )
    payload.update(
        {
            "strict_gate_kind": "dfl_schedule_value_learner_v3_strict_lp",
            "source_forecast_model_name": source_model_name,
            "learner_model_name": learner_model_name,
            "selected_weight_profile_name": str(
                learner_row["selected_weight_profile_name"]
            ),
            "selected_feature_names": list(learner_row["selected_feature_names"]),
            "selected_feature_weights": dict(learner_row["selected_feature_weights"]),
            "selected_feature_stats": dict(learner_row["selected_feature_stats"]),
            "selector_row_candidate_family": str(row["candidate_family"]),
            "selector_row_candidate_model_name": str(row["candidate_model_name"]),
            "selector_row_role": role,
            "selection_role": role,
            "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V3_STRICT_CLAIM_SCOPE,
            "academic_scope": DFL_SCHEDULE_VALUE_LEARNER_V3_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": int(row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:schedule-value-learner-v3:{source_model_name}:"
            f"{role}:{row['candidate_family']}:{anchor_timestamp:%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V3_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": v2._first_or_default(
            row["soc_fraction_vector"], default=0.5
        ),
        "starting_soc_source": "schedule_candidate_library_v2",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": v2._committed_action(row),
        "committed_power_mw": abs(
            v2._first_or_default(row["dispatch_mw_vector"], default=0.0)
        ),
        "rank_by_regret": 1,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": int(row["safety_violation_count"]),
        "selection_role": role,
        "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V3_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": payload,
    }


def _forecast_model_name_for_role(
    row: dict[str, Any],
    *,
    source_model_name: str,
    role: str,
) -> str:
    if role == "schedule_value_learner_v3":
        return schedule_value_learner_v3_model_name(source_model_name)
    if role == "schedule_value_learner_v2_reference":
        return v2.schedule_value_learner_v2_model_name(source_model_name)
    return str(row["candidate_model_name"])


def _gate_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    include_promotion_failures: bool,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [
        row for row in source_rows if _selection_role(row) == "strict_reference"
    ]
    raw_rows = [row for row in source_rows if _selection_role(row) == "raw_reference"]
    v2_rows = [
        row
        for row in source_rows
        if _selection_role(row) == "schedule_value_learner_v2_reference"
    ]
    selected_rows = [
        row
        for row in source_rows
        if _selection_role(row) == "schedule_value_learner_v3"
    ]
    strict_anchors = v2._tenant_anchor_set(strict_rows)
    raw_anchors = v2._tenant_anchor_set(raw_rows)
    v2_anchors = v2._tenant_anchor_set(v2_rows)
    selected_anchors = v2._tenant_anchor_set(selected_rows)
    if (
        strict_anchors != raw_anchors
        or strict_anchors != v2_anchors
        or strict_anchors != selected_anchors
    ):
        failures.append(
            f"{source_model_name} strict/raw/v2/v3 rows must cover matching tenant-anchor sets"
        )
    tenant_count = len({tenant_id for tenant_id, _ in selected_anchors})
    validation_count = len(selected_anchors)
    if tenant_count < min_tenant_count:
        failures.append(
            f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}"
        )
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    failures.extend(
        _provenance_failures([*strict_rows, *raw_rows, *v2_rows, *selected_rows])
    )
    strict_mean = v2._mean_regret(strict_rows)
    raw_mean = v2._mean_regret(raw_rows)
    v2_mean = v2._mean_regret(v2_rows)
    selected_mean = v2._mean_regret(selected_rows)
    strict_median = v2._median_regret(strict_rows)
    selected_median = v2._median_regret(selected_rows)
    improvement_vs_raw = v2._improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = v2._improvement_ratio(strict_mean, selected_mean)
    improvement_vs_v2 = v2._improvement_ratio(v2_mean, selected_mean)
    development_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_raw > 0.0
    )
    production_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio
        and improvement_vs_v2 >= 0.0
        and selected_median <= strict_median
        and not failures
    )
    if include_promotion_failures:
        if (
            selected_rows
            and strict_rows
            and improvement_vs_strict < min_mean_regret_improvement_ratio
        ):
            failures.append(
                f"{source_model_name} mean regret improvement vs {CONTROL_MODEL_NAME} must be at least "
                f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
            )
        if selected_rows and v2_rows and improvement_vs_v2 < 0.0:
            failures.append(
                f"{source_model_name} v3 must not degrade frozen Schedule/Value Learner V2; "
                f"observed improvement_vs_v2={improvement_vs_v2:.1%}"
            )
        if selected_rows and strict_rows and selected_median > strict_median:
            failures.append(
                f"{source_model_name} median regret must not be worse than {CONTROL_MODEL_NAME}; "
                f"observed learner={selected_median:.2f}, strict={strict_median:.2f}"
            )
    return {
        "source_model_name": source_model_name,
        "learner_model_name": schedule_value_learner_v3_model_name(source_model_name),
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "v2_mean_regret_uah": v2_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_v2": improvement_vs_v2,
        "development_gate_passed": development_passed,
        "production_gate_passed": production_passed,
        "market_execution_enabled": False,
        "failures": failures,
    }, failures


def _provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    for row in rows:
        payload = v2._payload(row)
        if (
            str(row.get("data_quality_tier", payload.get("data_quality_tier")))
            != "thesis_grade"
        ):
            failures.append("schedule/value learner v3 rows must be thesis_grade")
            break
        if (
            float(
                row.get(
                    "observed_coverage_ratio",
                    payload.get("observed_coverage_ratio", 0.0),
                )
            )
            < 1.0
        ):
            failures.append(
                "schedule/value learner v3 rows must have observed coverage"
            )
            break
        if int(
            row.get("safety_violation_count", payload.get("safety_violation_count", 1))
        ):
            failures.append(
                "schedule/value learner v3 rows must have zero safety violations"
            )
            break
        if not bool(row.get("not_full_dfl", payload.get("not_full_dfl", False))):
            failures.append(
                "schedule/value learner v3 rows must keep not_full_dfl=true"
            )
            break
        if not bool(
            row.get("not_market_execution", payload.get("not_market_execution", False))
        ):
            failures.append(
                "schedule/value learner v3 rows must keep not_market_execution=true"
            )
            break
    return failures


def _source_model_name(row: dict[str, Any]) -> str:
    if "source_model_name" in row:
        return str(row["source_model_name"])
    payload = v2._payload(row)
    return str(payload.get("source_forecast_model_name", ""))


def _selection_role(row: dict[str, Any]) -> str:
    if "selection_role" in row:
        return str(row["selection_role"])
    payload = v2._payload(row)
    return str(payload.get("selection_role", ""))


__all__ = [
    "DEFAULT_V3_FEATURE_NAMES",
    "DFL_SCHEDULE_VALUE_LEARNER_V3_STRICT_LP_STRATEGY_KIND",
    "build_dfl_schedule_value_learner_v3_frame",
    "build_dfl_schedule_value_learner_v3_strict_lp_benchmark_frame",
    "evaluate_dfl_schedule_value_learner_v3_gate",
    "schedule_value_learner_v3_model_name",
    "validate_dfl_schedule_value_learner_v3_evidence",
]
