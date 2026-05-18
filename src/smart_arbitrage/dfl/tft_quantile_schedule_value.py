"""TFT quantile schedule/value gate against frozen official V2+ evidence."""

from __future__ import annotations

from statistics import mean, median
from typing import Any, Final

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
    if source_model_name.endswith("_p10_v1"):
        return "p10"
    if source_model_name.endswith("_p90_v1"):
        return "p90"
    return "p50"


__all__ = [
    "DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND",
    "TFT_QUANTILE_SOURCE_MODELS",
    "build_dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame",
    "build_dfl_tft_quantile_schedule_candidate_library_frame",
    "evaluate_dfl_tft_augmented_v2_plus_gate",
    "validate_dfl_tft_augmented_v2_plus_evidence",
]
