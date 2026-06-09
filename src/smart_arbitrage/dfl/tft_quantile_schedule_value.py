"""TFT quantile schedule/value gate against frozen official V2+ evidence."""

from __future__ import annotations

from statistics import mean, median
from typing import Any, Final, TypeAlias

import polars as pl

from smart_arbitrage.dfl.promotion_gate import PromotionGateResult
from smart_arbitrage.dfl.schedule_value_learner import (
    build_dfl_schedule_value_learner_v2_frame,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    build_dfl_schedule_candidate_library_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.trajectory_ranker import (
    build_dfl_schedule_candidate_library_from_strict_benchmark_frame,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

TFT_QUANTILE_SOURCE_MODELS: Final[tuple[str, ...]] = (
    "tft_official_global_panel_p10_v1",
    "tft_official_global_panel_v1",
    "tft_official_global_panel_p90_v1",
)
TFT_QUANTILE_CALIBRATED_SOURCE_MODELS: Final[tuple[str, ...]] = (
    "tft_official_global_panel_p10_v1_horizon_quantile_calibrated_v1",
    "tft_official_global_panel_v1_horizon_quantile_calibrated_v1",
    "tft_official_global_panel_p90_v1_horizon_quantile_calibrated_v1",
)
TFT_BASE_QUANTILE_MODEL_NAME: Final[str] = "tft_official_global_panel_v1"
FROZEN_V2_PLUS_BASELINE_MODEL_NAME: Final[str] = (
    "nbeatsx_official_global_panel_horizon_calibrated_v1"
)
DFL_TFT_QUANTILE_CANDIDATE_LIBRARY_CLAIM_SCOPE: Final[str] = (
    "dfl_tft_quantile_schedule_candidate_library_not_full_dfl"
)
DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_tft_augmented_v2_plus_strict_lp_benchmark"
)
DFL_TFT_AUGMENTED_V2_PLUS_CLAIM_SCOPE: Final[str] = (
    "dfl_tft_augmented_v2_plus_strict_lp_gate_not_full_dfl"
)
DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_tft_combined_v2_plus_strict_lp_benchmark"
)
DFL_TFT_COMBINED_V2_PLUS_CLAIM_SCOPE: Final[str] = (
    "dfl_tft_combined_v2_plus_strict_lp_gate_not_full_dfl"
)
COMBINED_NBEATSX_TFT_SOURCE_MODEL_NAME: Final[str] = (
    "nbeatsx_tft_official_global_panel_combined_v1"
)
COMBINED_NBEATSX_TFT_FORECAST_MODEL_NAME: Final[str] = (
    "dfl_tft_combined_v2_plus_selector"
)


def build_dfl_tft_quantile_schedule_candidate_library_frame(
    tft_quantile_strict_benchmark_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = TFT_QUANTILE_SOURCE_MODELS,
    final_validation_anchor_count_per_tenant: int = 18,
    perturb_spread_scale_grid: tuple[float, ...] = (0.9, 1.1),
    perturb_mean_shift_grid_uah_mwh: tuple[float, ...] = (-250.0, 250.0),
) -> pl.DataFrame:
    """Build TFT p10/p50/p90 schedule candidates from strict LP/oracle rows."""

    library = build_dfl_schedule_candidate_library_from_strict_benchmark_frame(
        tft_quantile_strict_benchmark_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        perturb_spread_scale_grid=perturb_spread_scale_grid,
        perturb_mean_shift_grid_uah_mwh=perturb_mean_shift_grid_uah_mwh,
    )
    if library.is_empty():
        return library
    return library.with_columns(
        [
            pl.col("source_model_name")
            .map_elements(_source_quantile, return_dtype=pl.String)
            .alias("source_quantile"),
            pl.lit(True).alias("quantile_candidate_lane"),
            pl.lit(False).alias("market_execution_enabled"),
            pl.lit(DFL_TFT_QUANTILE_CANDIDATE_LIBRARY_CLAIM_SCOPE).alias(
                "claim_scope"
            ),
        ]
    )


def build_dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame(
    dfl_tft_quantile_schedule_candidate_library_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = TFT_QUANTILE_SOURCE_MODELS,
    final_validation_anchor_count_per_tenant: int = 18,
    min_prior_mean_improvement_ratio_vs_v2: float = 0.01,
) -> pl.DataFrame:
    """Compare TFT quantile V2+ schedules against frozen NBEATSx V2+ rows."""

    try:
        v2_model = build_dfl_schedule_value_learner_v2_frame(
            dfl_tft_quantile_schedule_candidate_library_frame,
            tenant_ids=tenant_ids,
            forecast_model_names=forecast_model_names,
            final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        )
    except ValueError as error:
        if "schedule/value learner needs train rows" not in str(error):
            raise
        return _with_augmented_gate_metadata(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
            gate_blocker="missing_tft_train_rows",
        )
    v2_plus_library = build_dfl_schedule_candidate_library_v2_plus_frame(
        dfl_tft_quantile_schedule_candidate_library_frame
    )
    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        v2_plus_library,
        v2_model,
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        min_prior_mean_improvement_ratio_vs_v2=min_prior_mean_improvement_ratio_vs_v2,
    )
    tft_strict = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        v2_plus_library,
        v2_plus_model,
        v2_model,
    )
    combined = pl.concat(
        [
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
            tft_strict,
        ],
        how="diagonal_relaxed",
    )
    return _with_augmented_gate_metadata(combined)


def evaluate_dfl_tft_augmented_v2_plus_gate(
    strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    tft_source_model_names: tuple[str, ...] = TFT_QUANTILE_SOURCE_MODELS,
    min_validation_tenant_anchor_count: int = 90,
    min_mean_regret_improvement_ratio_vs_baseline: float = 0.0,
) -> PromotionGateResult:
    """Pass only when TFT-augmented V2+ beats frozen Ukrainian-only V2+."""

    required_columns = {
        "source_model_name",
        "selection_role",
        "regret_uah",
        "tenant_id",
        "anchor_timestamp",
    }
    missing = sorted(required_columns.difference(strict_frame.columns))
    if missing:
        return PromotionGateResult(
            False,
            "blocked",
            f"TFT augmented V2+ frame is missing required columns: {missing}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    baseline_rows = _selected_rows(
        rows,
        source_model_name=baseline_source_model_name,
        role="schedule_value_learner_v2_plus",
    )
    if len(baseline_rows) < min_validation_tenant_anchor_count:
        return PromotionGateResult(
            False,
            "blocked",
            "frozen V2+ baseline coverage is below the validation threshold",
            {
                "baseline_source_model_name": baseline_source_model_name,
                "baseline_validation_tenant_anchor_count": len(baseline_rows),
            },
        )
    baseline_mean = _mean_regret(baseline_rows)
    baseline_median = _median_regret(baseline_rows)
    summaries = [
        _source_summary(rows, source_model_name=source_model_name)
        for source_model_name in tft_source_model_names
    ]
    summaries = [summary for summary in summaries if summary["count"] > 0]
    if not summaries:
        return PromotionGateResult(
            False,
            "blocked",
            "no TFT quantile V2+ challenger rows are present",
            {"baseline_source_model_name": baseline_source_model_name},
        )
    best = min(summaries, key=lambda summary: float(summary["mean_regret_uah"]))
    improvement = (
        (baseline_mean - float(best["mean_regret_uah"])) / baseline_mean
        if baseline_mean > 0.0
        else 0.0
    )
    median_not_worse = float(best["median_regret_uah"]) <= baseline_median
    mean_passed = improvement >= min_mean_regret_improvement_ratio_vs_baseline
    market_execution_enabled = _market_execution_enabled(rows)
    passed = mean_passed and median_not_worse and not market_execution_enabled
    return PromotionGateResult(
        passed,
        "promote" if passed else "blocked",
        (
            "TFT augmented V2+ beats frozen Ukrainian-only V2+ under strict LP/oracle scoring."
            if passed
            else "TFT augmented V2+ does not beat frozen Ukrainian-only V2+ without median degradation."
        ),
        {
            "baseline_source_model_name": baseline_source_model_name,
            "best_tft_source_model_name": best["source_model_name"],
            "baseline_mean_regret_uah": baseline_mean,
            "baseline_median_regret_uah": baseline_median,
            "best_tft_mean_regret_uah": best["mean_regret_uah"],
            "best_tft_median_regret_uah": best["median_regret_uah"],
            "mean_regret_improvement_ratio_vs_frozen_v2_plus": improvement,
            "median_not_worse": median_not_worse,
            "market_execution_enabled": market_execution_enabled,
            "tft_summaries": summaries,
        },
    )


def validate_dfl_tft_augmented_v2_plus_evidence(
    strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    tft_source_model_names: tuple[str, ...] = TFT_QUANTILE_SOURCE_MODELS,
    min_validation_tenant_anchor_count: int = 90,
) -> EvidenceCheckOutcome:
    """Validate coverage and claim flags for TFT augmented V2+ evidence."""

    gate = evaluate_dfl_tft_augmented_v2_plus_gate(
        strict_frame,
        baseline_source_model_name=baseline_source_model_name,
        tft_source_model_names=tft_source_model_names,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    required_columns = {
        "source_model_name",
        "selection_role",
        "regret_uah",
        "tenant_id",
        "anchor_timestamp",
        "evaluation_payload",
    }
    missing = sorted(required_columns.difference(strict_frame.columns))
    if missing:
        return EvidenceCheckOutcome(
            False,
            f"TFT augmented V2+ evidence is missing required columns: {missing}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if _market_execution_enabled(rows):
        return EvidenceCheckOutcome(
            False,
            "TFT augmented V2+ evidence must keep market execution disabled.",
            {"row_count": strict_frame.height},
        )
    baseline_count = len(
        _selected_rows(
            rows,
            source_model_name=baseline_source_model_name,
            role="schedule_value_learner_v2_plus",
        )
    )
    tft_counts = {
        source_model_name: len(
            _selected_rows(
                rows,
                source_model_name=source_model_name,
                role="schedule_value_learner_v2_plus",
            )
        )
        for source_model_name in tft_source_model_names
    }
    failures: list[str] = []
    if baseline_count < min_validation_tenant_anchor_count:
        failures.append("baseline V2+ coverage is below threshold")
    missing_tft = [
        source_model_name
        for source_model_name, count in tft_counts.items()
        if count < min_validation_tenant_anchor_count
    ]
    if missing_tft:
        failures.append(f"TFT quantile coverage is below threshold: {missing_tft}")
    return EvidenceCheckOutcome(
        not failures,
        "TFT augmented V2+ evidence has valid coverage and claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": strict_frame.height,
            "baseline_source_model_name": baseline_source_model_name,
            "baseline_validation_tenant_anchor_count": baseline_count,
            "tft_validation_tenant_anchor_counts": tft_counts,
            "gate_decision": gate.decision,
            "gate_passed": gate.passed,
            "gate_metrics": gate.metrics,
        },
    )


def build_dfl_tft_combined_v2_plus_strict_lp_benchmark_frame(
    dfl_tft_quantile_schedule_candidate_library_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    tft_source_model_names: tuple[str, ...] = TFT_QUANTILE_SOURCE_MODELS,
    combined_source_model_name: str = COMBINED_NBEATSX_TFT_SOURCE_MODEL_NAME,
    final_validation_anchor_count_per_tenant: int = 18,
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.01,
) -> pl.DataFrame:
    """Select TFT schedules only when prior rows beat frozen NBEATSx V2+.

    The frozen V2+ rows remain the fallback and comparator. TFT rows are treated
    as complementary candidates, not as a replacement forecast family.
    """

    del final_validation_anchor_count_per_tenant
    _validate_tft_combined_inputs(
        dfl_tft_quantile_schedule_candidate_library_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
    )
    baseline_frame = (
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        .filter(pl.col("source_model_name") == baseline_source_model_name)
    )
    baseline_rows = list(baseline_frame.iter_rows(named=True))
    baseline_selected_rows = _selected_rows(
        baseline_rows,
        source_model_name=baseline_source_model_name,
        role="schedule_value_learner_v2_plus",
    )
    baseline_train_mean_by_tenant = _baseline_train_mean_by_tenant(
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        baseline_source_model_name=baseline_source_model_name,
    )
    tft_rows = [
        row
        for row in dfl_tft_quantile_schedule_candidate_library_frame.iter_rows(
            named=True
        )
        if str(row["tenant_id"]) in tenant_ids
        and str(row["source_model_name"]) in tft_source_model_names
    ]
    best_train_candidate_by_tenant = _best_tft_train_candidate_by_tenant(tft_rows)
    final_candidate_by_anchor = _final_tft_candidate_by_anchor(tft_rows)
    selected_rows: list[dict[str, Any]] = []
    for baseline_row in baseline_selected_rows:
        tenant_id = str(baseline_row["tenant_id"])
        anchor_timestamp = baseline_row["anchor_timestamp"]
        baseline_train_mean = baseline_train_mean_by_tenant.get(tenant_id)
        best_train = best_train_candidate_by_tenant.get(tenant_id)
        gate_blocker = "missing_frozen_v2_plus_prior_rows"
        prior_improvement = 0.0
        selected_tft_source_model_name: str | None = None
        selected_tft_candidate_family: str | None = None
        selected_tft_candidate_model_name: str | None = None
        candidate_row: dict[str, Any] | None = None
        if baseline_train_mean is not None and best_train is not None:
            best_key, selected_train_mean = best_train
            prior_improvement = (
                (baseline_train_mean - selected_train_mean) / baseline_train_mean
                if baseline_train_mean > 0.0
                else 0.0
            )
            selected_tft_source_model_name, selected_tft_candidate_family, (
                selected_tft_candidate_model_name
            ) = best_key
            if prior_improvement >= min_prior_mean_improvement_ratio_vs_v2_plus:
                candidate_row = final_candidate_by_anchor.get(
                    (tenant_id, anchor_timestamp, best_key)
                )
                gate_blocker = (
                    "missing_selected_tft_final_row"
                    if candidate_row is None
                    else "tft_prior_candidate_selected"
                )
            else:
                gate_blocker = "tft_prior_improvement_below_threshold"
        elif best_train is None:
            gate_blocker = "missing_tft_train_rows"
        if candidate_row is None:
            selected_rows.append(
                _copy_baseline_row_as_combined_selection(
                    baseline_row,
                    combined_source_model_name=combined_source_model_name,
                    baseline_train_mean_regret_uah=baseline_train_mean,
                    selected_train_mean_regret_uah=None,
                    selected_tft_source_model_name=selected_tft_source_model_name,
                    selected_tft_candidate_family=selected_tft_candidate_family,
                    selected_tft_candidate_model_name=selected_tft_candidate_model_name,
                    prior_mean_improvement_ratio_vs_v2_plus=prior_improvement,
                    gate_blocker=gate_blocker,
                )
            )
            continue
        selected_rows.append(
            _candidate_row_as_combined_selection(
                candidate_row,
                combined_source_model_name=combined_source_model_name,
                baseline_train_mean_regret_uah=baseline_train_mean,
                prior_mean_improvement_ratio_vs_v2_plus=prior_improvement,
            )
        )
    combined = pl.concat(
        [baseline_frame, pl.DataFrame(selected_rows)],
        how="diagonal_relaxed",
    )
    return _with_combined_gate_metadata(combined)


def evaluate_dfl_tft_combined_v2_plus_gate(
    strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    combined_source_model_name: str = COMBINED_NBEATSX_TFT_SOURCE_MODEL_NAME,
    min_validation_tenant_anchor_count: int = 90,
    min_mean_regret_improvement_ratio_vs_baseline: float = 0.0,
) -> PromotionGateResult:
    """Pass only when combined NBEATSx V2+ + TFT schedules beat frozen V2+."""

    required_columns = {
        "source_model_name",
        "selection_role",
        "regret_uah",
        "tenant_id",
        "anchor_timestamp",
    }
    missing = sorted(required_columns.difference(strict_frame.columns))
    if missing:
        return PromotionGateResult(
            False,
            "blocked",
            f"TFT combined V2+ frame is missing required columns: {missing}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    baseline_rows = _selected_rows(
        rows,
        source_model_name=baseline_source_model_name,
        role="schedule_value_learner_v2_plus",
    )
    combined_rows = _selected_rows(
        rows,
        source_model_name=combined_source_model_name,
        role="tft_combined_v2_plus",
    )
    if len(baseline_rows) < min_validation_tenant_anchor_count:
        return PromotionGateResult(
            False,
            "blocked",
            "frozen V2+ baseline coverage is below the validation threshold",
            {
                "baseline_source_model_name": baseline_source_model_name,
                "baseline_validation_tenant_anchor_count": len(baseline_rows),
            },
        )
    if len(combined_rows) < min_validation_tenant_anchor_count:
        return PromotionGateResult(
            False,
            "blocked",
            "combined NBEATSx+TFT coverage is below the validation threshold",
            {
                "combined_source_model_name": combined_source_model_name,
                "combined_validation_tenant_anchor_count": len(combined_rows),
            },
        )
    baseline_mean = _mean_regret(baseline_rows)
    baseline_median = _median_regret(baseline_rows)
    combined_mean = _mean_regret(combined_rows)
    combined_median = _median_regret(combined_rows)
    improvement = (
        (baseline_mean - combined_mean) / baseline_mean
        if baseline_mean > 0.0
        else 0.0
    )
    median_not_worse = combined_median <= baseline_median
    mean_passed = improvement > min_mean_regret_improvement_ratio_vs_baseline
    market_execution_enabled = _market_execution_enabled(rows)
    passed = mean_passed and median_not_worse and not market_execution_enabled
    fallback_count = sum(
        1 for row in combined_rows if row.get("fallback_to_v2_plus") is True
    )
    return PromotionGateResult(
        passed,
        "promote" if passed else "blocked",
        (
            "Combined NBEATSx V2+ + TFT schedules beat frozen V2+ under strict LP/oracle scoring."
            if passed
            else "Combined NBEATSx V2+ + TFT schedules do not beat frozen V2+ without median degradation."
        ),
        {
            "baseline_source_model_name": baseline_source_model_name,
            "combined_source_model_name": combined_source_model_name,
            "baseline_mean_regret_uah": baseline_mean,
            "baseline_median_regret_uah": baseline_median,
            "combined_mean_regret_uah": combined_mean,
            "combined_median_regret_uah": combined_median,
            "mean_regret_improvement_ratio_vs_frozen_v2_plus": improvement,
            "median_not_worse": median_not_worse,
            "fallback_to_v2_plus_count": fallback_count,
            "selected_tft_count": len(combined_rows) - fallback_count,
            "market_execution_enabled": market_execution_enabled,
        },
    )


def validate_dfl_tft_combined_v2_plus_evidence(
    strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    combined_source_model_name: str = COMBINED_NBEATSX_TFT_SOURCE_MODEL_NAME,
    min_validation_tenant_anchor_count: int = 90,
) -> EvidenceCheckOutcome:
    """Validate coverage and claim flags for combined NBEATSx+TFT evidence."""

    gate = evaluate_dfl_tft_combined_v2_plus_gate(
        strict_frame,
        baseline_source_model_name=baseline_source_model_name,
        combined_source_model_name=combined_source_model_name,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    required_columns = {
        "source_model_name",
        "selection_role",
        "regret_uah",
        "tenant_id",
        "anchor_timestamp",
        "evaluation_payload",
    }
    missing = sorted(required_columns.difference(strict_frame.columns))
    if missing:
        return EvidenceCheckOutcome(
            False,
            f"TFT combined V2+ evidence is missing required columns: {missing}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if _market_execution_enabled(rows):
        return EvidenceCheckOutcome(
            False,
            "TFT combined V2+ evidence must keep market execution disabled.",
            {"row_count": strict_frame.height},
        )
    baseline_count = len(
        _selected_rows(
            rows,
            source_model_name=baseline_source_model_name,
            role="schedule_value_learner_v2_plus",
        )
    )
    combined_count = len(
        _selected_rows(
            rows,
            source_model_name=combined_source_model_name,
            role="tft_combined_v2_plus",
        )
    )
    failures: list[str] = []
    if baseline_count < min_validation_tenant_anchor_count:
        failures.append("baseline V2+ coverage is below threshold")
    if combined_count < min_validation_tenant_anchor_count:
        failures.append("combined NBEATSx+TFT coverage is below threshold")
    return EvidenceCheckOutcome(
        not failures,
        "TFT combined V2+ evidence has valid coverage and claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": strict_frame.height,
            "baseline_source_model_name": baseline_source_model_name,
            "baseline_validation_tenant_anchor_count": baseline_count,
            "combined_source_model_name": combined_source_model_name,
            "combined_validation_tenant_anchor_count": combined_count,
            "gate_decision": gate.decision,
            "gate_passed": gate.passed,
            "gate_metrics": gate.metrics,
        },
    )


def _with_augmented_gate_metadata(
    frame: pl.DataFrame,
    *,
    gate_blocker: str | None = None,
) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    payloads: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        payload = dict(row["evaluation_payload"]) if isinstance(row["evaluation_payload"], dict) else {}
        payload.update(
            {
                "claim_scope": DFL_TFT_AUGMENTED_V2_PLUS_CLAIM_SCOPE,
                "benchmark_kind": DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        if gate_blocker is not None:
            payload["tft_gate_blocker"] = gate_blocker
        payloads.append(payload)
    expressions: list[pl.Expr | pl.Series] = [
            pl.lit(DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND).alias(
                "strategy_kind"
            ),
            pl.lit(False).alias("market_execution_enabled"),
            pl.Series("evaluation_payload", payloads),
    ]
    if gate_blocker is not None:
        expressions.append(pl.lit(gate_blocker).alias("tft_gate_blocker"))
    return frame.with_columns(expressions)


def _validate_tft_combined_inputs(
    tft_library: pl.DataFrame,
    frozen_strict_frame: pl.DataFrame,
    frozen_v2_plus_model_frame: pl.DataFrame,
) -> None:
    frame_columns = {
        "tft_library": (
            tft_library,
            {
                "tenant_id",
                "source_model_name",
                "candidate_family",
                "candidate_model_name",
                "anchor_timestamp",
                "split_name",
                "regret_uah",
                "evaluation_payload",
            },
        ),
        "frozen_strict_frame": (
            frozen_strict_frame,
            {
                "tenant_id",
                "source_model_name",
                "selection_role",
                "anchor_timestamp",
                "regret_uah",
                "evaluation_payload",
            },
        ),
        "frozen_v2_plus_model_frame": (
            frozen_v2_plus_model_frame,
            {
                "tenant_id",
                "source_model_name",
                "selected_train_mean_regret_uah",
            },
        ),
    }
    missing_by_frame: dict[str, list[str]] = {}
    for name, (frame, required_columns) in frame_columns.items():
        missing = sorted(required_columns.difference(frame.columns))
        if missing:
            missing_by_frame[name] = missing
    if missing_by_frame:
        raise ValueError(
            "TFT combined V2+ inputs are missing required columns: "
            f"{missing_by_frame}"
        )


def _baseline_train_mean_by_tenant(
    frozen_v2_plus_model_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str,
) -> dict[str, float]:
    train_means: dict[str, float] = {}
    for row in frozen_v2_plus_model_frame.iter_rows(named=True):
        if str(row["source_model_name"]) != baseline_source_model_name:
            continue
        train_means[str(row["tenant_id"])] = float(
            row["selected_train_mean_regret_uah"]
        )
    return train_means


_CandidateKey: TypeAlias = tuple[str, str, str]


def _best_tft_train_candidate_by_tenant(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[_CandidateKey, float]]:
    regrets_by_key: dict[tuple[str, _CandidateKey], list[float]] = {}
    for row in rows:
        if str(row.get("split_name")) == "final_holdout":
            continue
        if str(row.get("candidate_family")) == "strict_control":
            continue
        tenant_id = str(row["tenant_id"])
        key = _candidate_key(row)
        regrets_by_key.setdefault((tenant_id, key), []).append(float(row["regret_uah"]))
    best_by_tenant: dict[str, tuple[_CandidateKey, float]] = {}
    for (tenant_id, key), regrets in regrets_by_key.items():
        mean_regret = mean(regrets)
        current = best_by_tenant.get(tenant_id)
        if current is None or mean_regret < current[1]:
            best_by_tenant[tenant_id] = (key, mean_regret)
    return best_by_tenant


def _final_tft_candidate_by_anchor(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, Any, _CandidateKey], dict[str, Any]]:
    candidates: dict[tuple[str, Any, _CandidateKey], dict[str, Any]] = {}
    for row in rows:
        if str(row.get("split_name")) != "final_holdout":
            continue
        if str(row.get("candidate_family")) == "strict_control":
            continue
        key = (str(row["tenant_id"]), row["anchor_timestamp"], _candidate_key(row))
        candidates.setdefault(key, row)
    return candidates


def _candidate_key(row: dict[str, Any]) -> _CandidateKey:
    return (
        str(row["source_model_name"]),
        str(row["candidate_family"]),
        str(row["candidate_model_name"]),
    )


def _copy_baseline_row_as_combined_selection(
    row: dict[str, Any],
    *,
    combined_source_model_name: str,
    baseline_train_mean_regret_uah: float | None,
    selected_train_mean_regret_uah: float | None,
    selected_tft_source_model_name: str | None,
    selected_tft_candidate_family: str | None,
    selected_tft_candidate_model_name: str | None,
    prior_mean_improvement_ratio_vs_v2_plus: float,
    gate_blocker: str,
) -> dict[str, Any]:
    copied = dict(row)
    copied.update(
        {
            "evaluation_id": (
                f"{row['tenant_id']}:{COMBINED_NBEATSX_TFT_FORECAST_MODEL_NAME}:"
                f"{row['anchor_timestamp']}"
            ),
            "source_model_name": combined_source_model_name,
            "forecast_model_name": COMBINED_NBEATSX_TFT_FORECAST_MODEL_NAME,
            "strategy_kind": DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "selection_role": "tft_combined_v2_plus",
            "fallback_to_v2_plus": True,
            "selected_tft_source_model_name": selected_tft_source_model_name,
            "selected_tft_candidate_family": selected_tft_candidate_family,
            "selected_tft_candidate_model_name": selected_tft_candidate_model_name,
            "baseline_train_mean_regret_uah": baseline_train_mean_regret_uah,
            "selected_train_mean_regret_uah": selected_train_mean_regret_uah,
            "prior_mean_improvement_ratio_vs_v2_plus": (
                prior_mean_improvement_ratio_vs_v2_plus
            ),
            "tft_combined_gate_blocker": gate_blocker,
            "market_execution_enabled": False,
        }
    )
    payload = (
        dict(copied["evaluation_payload"])
        if isinstance(copied.get("evaluation_payload"), dict)
        else {}
    )
    payload.update(
        {
            "claim_scope": DFL_TFT_COMBINED_V2_PLUS_CLAIM_SCOPE,
            "benchmark_kind": DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "combined_source_model_name": combined_source_model_name,
            "selected_from": "frozen_v2_plus_fallback",
            "fallback_to_v2_plus": True,
            "tft_combined_gate_blocker": gate_blocker,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    copied["evaluation_payload"] = payload
    return copied


def _candidate_row_as_combined_selection(
    row: dict[str, Any],
    *,
    combined_source_model_name: str,
    baseline_train_mean_regret_uah: float | None,
    prior_mean_improvement_ratio_vs_v2_plus: float,
) -> dict[str, Any]:
    forecast_prices = [float(value) for value in row["forecast_price_uah_mwh_vector"]]
    actual_prices = [float(value) for value in row["actual_price_uah_mwh_vector"]]
    dispatch = [float(value) for value in row["dispatch_mw_vector"]]
    soc = [float(value) for value in row["soc_fraction_vector"]]
    payload = (
        dict(row["evaluation_payload"])
        if isinstance(row.get("evaluation_payload"), dict)
        else {}
    )
    payload.update(
        {
            "claim_scope": DFL_TFT_COMBINED_V2_PLUS_CLAIM_SCOPE,
            "benchmark_kind": DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "combined_source_model_name": combined_source_model_name,
            "selected_from": "tft_quantile_candidate",
            "selected_tft_source_model_name": row["source_model_name"],
            "selected_tft_candidate_family": row["candidate_family"],
            "selected_tft_candidate_model_name": row["candidate_model_name"],
            "fallback_to_v2_plus": False,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:{COMBINED_NBEATSX_TFT_FORECAST_MODEL_NAME}:"
            f"{row['anchor_timestamp']}"
        ),
        "tenant_id": row["tenant_id"],
        "source_model_name": combined_source_model_name,
        "forecast_model_name": COMBINED_NBEATSX_TFT_FORECAST_MODEL_NAME,
        "strategy_kind": DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": row["anchor_timestamp"],
        "generated_at": row["generated_at"],
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": soc[0] if soc else 0.5,
        "starting_soc_source": "candidate_library",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": "MIXED",
        "committed_power_mw": dispatch[0] if dispatch else 0.0,
        "rank_by_regret": None,
        "selection_role": "tft_combined_v2_plus",
        "fallback_to_v2_plus": False,
        "selected_tft_source_model_name": row["source_model_name"],
        "selected_tft_candidate_family": row["candidate_family"],
        "selected_tft_candidate_model_name": row["candidate_model_name"],
        "baseline_train_mean_regret_uah": baseline_train_mean_regret_uah,
        "selected_train_mean_regret_uah": float(row["prior_family_mean_regret_uah"]),
        "prior_mean_improvement_ratio_vs_v2_plus": (
            prior_mean_improvement_ratio_vs_v2_plus
        ),
        "tft_combined_gate_blocker": "tft_prior_candidate_selected",
        "market_execution_enabled": False,
        "not_market_execution": True,
        "evaluation_payload": payload,
        "forecast_price_uah_mwh_vector": forecast_prices,
        "actual_price_uah_mwh_vector": actual_prices,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": soc,
    }


def _with_combined_gate_metadata(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    payloads: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        payload = (
            dict(row["evaluation_payload"])
            if isinstance(row.get("evaluation_payload"), dict)
            else {}
        )
        payload.update(
            {
                "claim_scope": DFL_TFT_COMBINED_V2_PLUS_CLAIM_SCOPE,
                "benchmark_kind": DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        payloads.append(payload)
    return frame.with_columns(
        [
            pl.lit(DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND).alias(
                "strategy_kind"
            ),
            pl.lit(False).alias("market_execution_enabled"),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _source_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
) -> dict[str, Any]:
    selected = _selected_rows(
        rows,
        source_model_name=source_model_name,
        role="schedule_value_learner_v2_plus",
    )
    return {
        "source_model_name": source_model_name,
        "count": len(selected),
        "tenant_count": len({str(row["tenant_id"]) for row in selected}),
        "mean_regret_uah": _mean_regret(selected) if selected else float("inf"),
        "median_regret_uah": _median_regret(selected) if selected else float("inf"),
    }


def _selected_rows(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    role: str,
) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if str(row["source_model_name"]) == source_model_name
        and str(row["selection_role"]) == role
    ]


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    if not rows:
        raise ValueError("cannot compute mean regret for empty rows")
    return mean(float(row["regret_uah"]) for row in rows)


def _median_regret(rows: list[dict[str, Any]]) -> float:
    if not rows:
        raise ValueError("cannot compute median regret for empty rows")
    return median(float(row["regret_uah"]) for row in rows)


def _market_execution_enabled(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        if row.get("market_execution_enabled") is True:
            return True
        payload = row.get("evaluation_payload")
        if isinstance(payload, dict) and payload.get("market_execution_enabled") is True:
            return True
    return False


def _source_quantile(source_model_name: str) -> str:
    if source_model_name.endswith("_p10_v1") or "_p10_" in source_model_name:
        return "p10"
    if source_model_name.endswith("_p90_v1") or "_p90_" in source_model_name:
        return "p90"
    return "p50"


__all__ = [
    "DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND",
    "DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND",
    "TFT_QUANTILE_CALIBRATED_SOURCE_MODELS",
    "TFT_QUANTILE_SOURCE_MODELS",
    "build_dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame",
    "build_dfl_tft_combined_v2_plus_strict_lp_benchmark_frame",
    "build_dfl_tft_quantile_schedule_candidate_library_frame",
    "evaluate_dfl_tft_augmented_v2_plus_gate",
    "evaluate_dfl_tft_combined_v2_plus_gate",
    "validate_dfl_tft_augmented_v2_plus_evidence",
    "validate_dfl_tft_combined_v2_plus_evidence",
]
