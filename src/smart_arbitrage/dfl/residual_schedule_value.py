"""Residual schedule/value selector and strict fallback gate for real-data DFL research."""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.strict_challenger import (
    CANDIDATE_FAMILY_STRICT,
    _datetime_value,
    _payload,
    _require_columns,
    _validate_library_frame,
)
from smart_arbitrage.dfl.trajectory_dataset import REQUIRED_TRAJECTORY_DATASET_COLUMNS
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_RESIDUAL_SCHEDULE_VALUE_CLAIM_SCOPE: Final[str] = (
    "dfl_residual_schedule_value_v1_not_full_dfl"
)
DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_residual_schedule_value_v1_strict_lp_gate_not_full_dfl"
)
DFL_RESIDUAL_DT_FALLBACK_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_residual_dt_fallback_v1_strict_lp_gate_not_full_dfl"
)
DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_residual_schedule_value_strict_lp_benchmark"
)
DFL_RESIDUAL_DT_FALLBACK_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_residual_dt_fallback_strict_lp_benchmark"
)
DFL_RESIDUAL_SCHEDULE_VALUE_PREFIX: Final[str] = "dfl_residual_schedule_value_v1_"
DFL_RESIDUAL_DT_FALLBACK_PREFIX: Final[str] = "dfl_residual_dt_fallback_v1_"
DFL_RESIDUAL_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only residual schedule/value selector over feasible LP-scored schedules. "
    "This is a research challenger, not full DFL, not Decision Transformer control, "
    "and not market execution."
)
REQUIRED_RESIDUAL_MODEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "residual_model_name",
        "selected_candidate_family",
        "selected_switch_margin_uah",
        "train_anchor_count",
        "final_holdout_anchor_count",
        "train_mean_regret_improvement_ratio_vs_strict",
        "train_median_not_worse",
        "ood_regime_flag",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_RESIDUAL_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "generated_at",
        "regret_uah",
        "selection_role",
        "evaluation_payload",
    }
)


def residual_schedule_value_model_name(source_model_name: str) -> str:
    """Return the stable residual schedule/value model name for a source model."""

    return f"{DFL_RESIDUAL_SCHEDULE_VALUE_PREFIX}{source_model_name}"


def residual_dt_fallback_model_name(source_model_name: str) -> str:
    """Return the stable residual/DT fallback model name for a source model."""

    return f"{DFL_RESIDUAL_DT_FALLBACK_PREFIX}{source_model_name}"


def build_dfl_residual_schedule_value_model_frame(
    trajectory_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    switch_margin_grid_uah: tuple[float, ...] = (0.0, 50.0, 100.0, 200.0, 400.0),
) -> pl.DataFrame:
    """Select a deterministic residual schedule family using train/inner anchors only."""

    _validate_residual_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        switch_margin_grid_uah=switch_margin_grid_uah,
    )
    _validate_trajectory_frame(trajectory_frame)
    schedule_rows = _schedule_rows_from_trajectory(trajectory_frame)
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            scoped_rows = [
                row
                for row in schedule_rows
                if row["tenant_id"] == tenant_id and row["source_model_name"] == source_model_name
            ]
            train_rows = [row for row in scoped_rows if row["split_name"] != "final_holdout"]
            final_rows = [row for row in scoped_rows if row["split_name"] == "final_holdout"]
            final_anchor_count = len({row["anchor_timestamp"] for row in final_rows})
            if final_anchor_count != final_validation_anchor_count_per_tenant:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} residual model final_holdout anchor count "
                    f"must be {final_validation_anchor_count_per_tenant}; observed {final_anchor_count}"
                )
            if not train_rows:
                raise ValueError(f"{tenant_id}/{source_model_name} residual model needs train rows")
            selected_family = _best_train_non_strict_family(train_rows)
            best_rule = _select_switch_margin(
                train_rows,
                selected_candidate_family=selected_family,
                switch_margin_grid_uah=switch_margin_grid_uah,
            )
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "residual_model_name": residual_schedule_value_model_name(source_model_name),
                    "selected_candidate_family": selected_family,
                    "selected_switch_margin_uah": best_rule["switch_margin_uah"],
                    "selected_weight_profile_name": (
                        f"residual_linear_margin_{best_rule['switch_margin_uah']:.0f}"
                    ),
                    "train_anchor_count": best_rule["train_anchor_count"],
                    "final_holdout_anchor_count": final_anchor_count,
                    "strict_train_mean_regret_uah": best_rule["strict_mean_regret_uah"],
                    "selected_train_mean_regret_uah": best_rule["selected_mean_regret_uah"],
                    "strict_train_median_regret_uah": best_rule["strict_median_regret_uah"],
                    "selected_train_median_regret_uah": best_rule["selected_median_regret_uah"],
                    "train_mean_regret_improvement_ratio_vs_strict": best_rule[
                        "mean_regret_improvement_ratio_vs_strict"
                    ],
                    "train_median_not_worse": best_rule["selected_median_regret_uah"]
                    <= best_rule["strict_median_regret_uah"],
                    "ood_regime_flag": False,
                    "claim_scope": DFL_RESIDUAL_SCHEDULE_VALUE_CLAIM_SCOPE,
                    "academic_scope": DFL_RESIDUAL_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name"])


def build_dfl_residual_schedule_value_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    residual_model_frame: pl.DataFrame,
    *,
    final_validation_anchor_count_per_tenant: int = 18,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict LP/oracle rows for the residual schedule/value selector."""

    _validate_library_frame(schedule_candidate_library_frame)
    _validate_residual_model_frame(residual_model_frame)
    resolved_generated_at = generated_at or _latest_generated_at(schedule_candidate_library_frame)
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    for model_row in residual_model_frame.iter_rows(named=True):
        tenant_id = str(model_row["tenant_id"])
        source_model_name = str(model_row["source_model_name"])
        final_rows = _final_rows(
            library_rows,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
        )
        final_anchors = sorted({row["anchor_timestamp"] for row in final_rows})
        if len(final_anchors) != final_validation_anchor_count_per_tenant:
            raise ValueError(
                f"{tenant_id}/{source_model_name} residual strict benchmark final_holdout "
                f"anchor count must be {final_validation_anchor_count_per_tenant}; "
                f"observed {len(final_anchors)}"
            )
        for anchor_timestamp in final_anchors:
            anchor_rows = [row for row in final_rows if row["anchor_timestamp"] == anchor_timestamp]
            strict_row = _family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
            residual_row = _family_row(anchor_rows, str(model_row["selected_candidate_family"]))
            selected_row = (
                residual_row
                if _residual_confidence_passes(model_row)
                else strict_row
            )
            rows.append(
                _benchmark_row(
                    strict_row,
                    source_model_name=source_model_name,
                    forecast_model_name=CONTROL_MODEL_NAME,
                    selection_role="strict_reference",
                    selected_strategy_source=CONTROL_MODEL_NAME,
                    claim_scope=DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_CLAIM_SCOPE,
                    strategy_kind=DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_LP_STRATEGY_KIND,
                    generated_at=resolved_generated_at,
                    model_metadata=model_row,
                )
            )
            rows.append(
                _benchmark_row(
                    residual_row,
                    source_model_name=source_model_name,
                    forecast_model_name=residual_schedule_value_model_name(source_model_name),
                    selection_role="residual_selector",
                    selected_strategy_source="dfl_residual_schedule_value_v1",
                    claim_scope=DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_CLAIM_SCOPE,
                    strategy_kind=DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_LP_STRATEGY_KIND,
                    generated_at=resolved_generated_at,
                    model_metadata=model_row,
                )
            )
            rows.append(
                _benchmark_row(
                    selected_row,
                    source_model_name=source_model_name,
                    forecast_model_name=f"{residual_schedule_value_model_name(source_model_name)}_gated",
                    selection_role="residual_gated_selector",
                    selected_strategy_source=(
                        "dfl_residual_schedule_value_v1"
                        if selected_row is residual_row
                        else CONTROL_MODEL_NAME
                    ),
                    claim_scope=DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_CLAIM_SCOPE,
                    strategy_kind=DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_LP_STRATEGY_KIND,
                    generated_at=resolved_generated_at,
                    model_metadata=model_row,
                )
            )
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"])


def build_dfl_residual_dt_fallback_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    residual_model_frame: pl.DataFrame,
    offline_dt_candidate_frame: pl.DataFrame,
    *,
    final_validation_anchor_count_per_tenant: int = 18,
    min_confidence_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Score strict, residual, offline-DT, behavior-cloning, and the fallback wrapper."""

    _validate_library_frame(schedule_candidate_library_frame)
    _validate_residual_model_frame(residual_model_frame)
    _validate_offline_dt_candidate_frame(offline_dt_candidate_frame)
    resolved_generated_at = generated_at or _latest_generated_at(schedule_candidate_library_frame)
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    dt_by_key = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in offline_dt_candidate_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for residual_row in residual_model_frame.iter_rows(named=True):
        tenant_id = str(residual_row["tenant_id"])
        source_model_name = str(residual_row["source_model_name"])
        dt_row = dt_by_key.get((tenant_id, source_model_name))
        if dt_row is None:
            raise ValueError(f"missing offline DT row for {tenant_id}/{source_model_name}")
        final_rows = _final_rows(
            library_rows,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
        )
        final_anchors = sorted({row["anchor_timestamp"] for row in final_rows})
        if len(final_anchors) != final_validation_anchor_count_per_tenant:
            raise ValueError(
                f"{tenant_id}/{source_model_name} fallback final_holdout anchor count must be "
                f"{final_validation_anchor_count_per_tenant}; observed {len(final_anchors)}"
            )
        residual_confident = _confidence_passes(
            float(residual_row["train_mean_regret_improvement_ratio_vs_strict"]),
            bool(residual_row["train_median_not_worse"]),
            bool(residual_row["ood_regime_flag"]),
            min_confidence_improvement_ratio=min_confidence_improvement_ratio,
        )
        dt_confident = _confidence_passes(
            float(dt_row["dt_train_mean_regret_improvement_ratio_vs_strict"]),
            bool(dt_row["dt_train_median_not_worse"]),
            bool(dt_row["ood_regime_flag"]),
            min_confidence_improvement_ratio=min_confidence_improvement_ratio,
        )
        for anchor_timestamp in final_anchors:
            anchor_rows = [row for row in final_rows if row["anchor_timestamp"] == anchor_timestamp]
            strict_row = _family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
            residual_candidate = _family_row(anchor_rows, str(residual_row["selected_candidate_family"]))
            dt_candidate = _family_row(anchor_rows, str(dt_row["dt_selected_candidate_family"]))
            bc_candidate = _family_row(anchor_rows, str(dt_row["behavior_cloning_selected_candidate_family"]))
            fallback_source = CONTROL_MODEL_NAME
            fallback_row = strict_row
            if residual_confident:
                fallback_source = "dfl_residual_schedule_value_v1"
                fallback_row = residual_candidate
            elif dt_confident:
                fallback_source = "dfl_offline_dt_candidate_v1"
                fallback_row = dt_candidate
            rows.extend(
                [
                    _benchmark_row(
                        strict_row,
                        source_model_name=source_model_name,
                        forecast_model_name=CONTROL_MODEL_NAME,
                        selection_role="strict_reference",
                        selected_strategy_source=CONTROL_MODEL_NAME,
                        claim_scope=DFL_RESIDUAL_DT_FALLBACK_STRICT_CLAIM_SCOPE,
                        strategy_kind=DFL_RESIDUAL_DT_FALLBACK_STRICT_LP_STRATEGY_KIND,
                        generated_at=resolved_generated_at,
                        model_metadata={**dict(residual_row), **dict(dt_row)},
                    ),
                    _benchmark_row(
                        residual_candidate,
                        source_model_name=source_model_name,
                        forecast_model_name=residual_schedule_value_model_name(source_model_name),
                        selection_role="residual_reference",
                        selected_strategy_source="dfl_residual_schedule_value_v1",
                        claim_scope=DFL_RESIDUAL_DT_FALLBACK_STRICT_CLAIM_SCOPE,
                        strategy_kind=DFL_RESIDUAL_DT_FALLBACK_STRICT_LP_STRATEGY_KIND,
                        generated_at=resolved_generated_at,
                        model_metadata={**dict(residual_row), **dict(dt_row)},
                    ),
                    _benchmark_row(
                        dt_candidate,
                        source_model_name=source_model_name,
                        forecast_model_name=f"dfl_offline_dt_candidate_v1_{source_model_name}",
                        selection_role="offline_dt_reference",
                        selected_strategy_source="dfl_offline_dt_candidate_v1",
                        claim_scope=DFL_RESIDUAL_DT_FALLBACK_STRICT_CLAIM_SCOPE,
                        strategy_kind=DFL_RESIDUAL_DT_FALLBACK_STRICT_LP_STRATEGY_KIND,
                        generated_at=resolved_generated_at,
                        model_metadata={**dict(residual_row), **dict(dt_row)},
                    ),
                    _benchmark_row(
                        bc_candidate,
                        source_model_name=source_model_name,
                        forecast_model_name=f"dfl_filtered_behavior_cloning_v1_{source_model_name}",
                        selection_role="filtered_behavior_cloning_reference",
                        selected_strategy_source="filtered_behavior_cloning_v1",
                        claim_scope=DFL_RESIDUAL_DT_FALLBACK_STRICT_CLAIM_SCOPE,
                        strategy_kind=DFL_RESIDUAL_DT_FALLBACK_STRICT_LP_STRATEGY_KIND,
                        generated_at=resolved_generated_at,
                        model_metadata={**dict(residual_row), **dict(dt_row)},
                    ),
                    _benchmark_row(
                        fallback_row,
                        source_model_name=source_model_name,
                        forecast_model_name=residual_dt_fallback_model_name(source_model_name),
                        selection_role="fallback_strategy",
                        selected_strategy_source=fallback_source,
                        claim_scope=DFL_RESIDUAL_DT_FALLBACK_STRICT_CLAIM_SCOPE,
                        strategy_kind=DFL_RESIDUAL_DT_FALLBACK_STRICT_LP_STRATEGY_KIND,
                        generated_at=resolved_generated_at,
                        model_metadata={**dict(residual_row), **dict(dt_row)},
                    ),
                ]
            )
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"])


def evaluate_dfl_residual_dt_fallback_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate the strict LP/oracle promotion gate for the fallback research challenger."""

    missing = sorted(REQUIRED_RESIDUAL_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing:
        return _promotion_result(
            failures=[f"fallback strict frame is missing required columns: {missing}"],
            metrics={},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return _promotion_result(failures=["fallback strict frame has no rows"], metrics={})
    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    passing: list[dict[str, Any]] = []
    for source_model_name in source_names:
        source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
        summary, source_failures = _source_gate_summary(
            source_rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        )
        summaries.append(summary)
        failures.extend(source_failures)
        if summary.get("passed") is True:
            passing.append(summary)
    if not passing:
        failures.append(
            f"no residual/DT fallback source beats {control_model_name} by "
            f"{min_mean_regret_improvement_ratio:.1%} with stable median regret"
        )
    best_summary = max(
        summaries,
        key=lambda summary: float(summary.get("mean_regret_improvement_ratio_vs_strict", -1.0)),
    )
    return _promotion_result(
        failures=failures,
        metrics={
            "best_source_model_name": best_summary.get("source_model_name"),
            "validation_tenant_anchor_count": best_summary.get("validation_tenant_anchor_count", 0),
            "strict_mean_regret_uah": best_summary.get("strict_mean_regret_uah", 0.0),
            "fallback_mean_regret_uah": best_summary.get("fallback_mean_regret_uah", 0.0),
            "strict_median_regret_uah": best_summary.get("strict_median_regret_uah", 0.0),
            "fallback_median_regret_uah": best_summary.get("fallback_median_regret_uah", 0.0),
            "mean_regret_improvement_ratio_vs_strict": best_summary.get(
                "mean_regret_improvement_ratio_vs_strict",
                0.0,
            ),
            "passing_source_model_names": [summary["source_model_name"] for summary in passing],
            "model_summaries": summaries,
            "production_promote": bool(passing),
        },
    )


def validate_dfl_residual_dt_fallback_evidence(strict_frame: pl.DataFrame) -> EvidenceCheckOutcome:
    """Validate fallback strict evidence without forcing promotion success."""

    gate = evaluate_dfl_residual_dt_fallback_gate(strict_frame)
    return EvidenceCheckOutcome(
        passed=True,
        description=(
            "Fallback evidence is structurally valid; promotion decision remains "
            f"{gate.decision}: {gate.description}"
        ),
        metadata=gate.metrics,
    )


def _validate_residual_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
    switch_margin_grid_uah: tuple[float, ...],
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
    if not switch_margin_grid_uah:
        raise ValueError("switch_margin_grid_uah must contain at least one threshold.")
    if any(value < 0.0 for value in switch_margin_grid_uah):
        raise ValueError("switch_margin_grid_uah must be non-negative.")


def _validate_trajectory_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        REQUIRED_TRAJECTORY_DATASET_COLUMNS,
        frame_name="dfl_real_data_trajectory_dataset_frame",
    )
    for row in frame.iter_rows(named=True):
        if str(row["data_quality_tier"]) != "thesis_grade":
            raise ValueError("residual schedule/value training requires thesis_grade rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("residual schedule/value training requires observed coverage ratio of 1.0")
        if int(row["safety_violation_count"]):
            raise ValueError("residual schedule/value training requires zero safety violations")
        if row.get("not_full_dfl") is not True:
            raise ValueError("residual schedule/value training requires not_full_dfl=true")
        if row.get("not_market_execution") is not True:
            raise ValueError("residual schedule/value training requires not_market_execution=true")


def _validate_residual_model_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_RESIDUAL_MODEL_COLUMNS, frame_name="residual_model_frame")
    for row in frame.iter_rows(named=True):
        if row.get("not_full_dfl") is not True:
            raise ValueError("residual model rows must remain not_full_dfl=true")
        if row.get("not_market_execution") is not True:
            raise ValueError("residual model rows must remain not_market_execution=true")


def _validate_offline_dt_candidate_frame(frame: pl.DataFrame) -> None:
    required_columns = {
        "tenant_id",
        "source_model_name",
        "dt_selected_candidate_family",
        "behavior_cloning_selected_candidate_family",
        "dt_train_mean_regret_improvement_ratio_vs_strict",
        "dt_train_median_not_worse",
        "ood_regime_flag",
        "not_full_dfl",
        "not_market_execution",
    }
    _require_columns(frame, frozenset(required_columns), frame_name="offline_dt_candidate_frame")


def _schedule_rows_from_trajectory(trajectory_frame: pl.DataFrame) -> list[dict[str, Any]]:
    by_episode: dict[str, list[dict[str, Any]]] = {}
    for row in trajectory_frame.iter_rows(named=True):
        by_episode.setdefault(str(row["episode_id"]), []).append(row)
    rows: list[dict[str, Any]] = []
    for episode_rows in by_episode.values():
        first = min(episode_rows, key=lambda row: int(row["horizon_step"]))
        rows.append(
            {
                "tenant_id": str(first["tenant_id"]),
                "source_model_name": str(first["source_model_name"]),
                "anchor_timestamp": _datetime_value(
                    first["anchor_timestamp"],
                    field_name="anchor_timestamp",
                ),
                "candidate_family": str(first["candidate_family"]),
                "candidate_model_name": str(first["candidate_model_name"]),
                "split_name": str(first["split_name"]),
                "episode_decision_value_uah": float(first["episode_decision_value_uah"]),
                "episode_regret_uah": float(first["episode_regret_uah"]),
                "episode_total_throughput_mwh": float(first["episode_total_throughput_mwh"]),
                "episode_total_degradation_penalty_uah": float(
                    first["episode_total_degradation_penalty_uah"]
                ),
                "safety_violation_count": int(first["safety_violation_count"]),
                "data_quality_tier": str(first["data_quality_tier"]),
                "observed_coverage_ratio": float(first["observed_coverage_ratio"]),
                "not_full_dfl": bool(first["not_full_dfl"]),
                "not_market_execution": bool(first["not_market_execution"]),
            }
        )
    return rows


def _best_train_non_strict_family(train_rows: list[dict[str, Any]]) -> str:
    non_strict_rows = [
        row for row in train_rows if row["candidate_family"] != CANDIDATE_FAMILY_STRICT
    ]
    if not non_strict_rows:
        raise ValueError("residual model needs at least one non-strict train family")
    family_names = sorted({row["candidate_family"] for row in non_strict_rows})
    return min(
        family_names,
        key=lambda family: (
            mean(
                [
                    row["episode_regret_uah"]
                    for row in non_strict_rows
                    if row["candidate_family"] == family
                ]
            ),
            family,
        ),
    )


def _select_switch_margin(
    train_rows: list[dict[str, Any]],
    *,
    selected_candidate_family: str,
    switch_margin_grid_uah: tuple[float, ...],
) -> dict[str, Any]:
    anchor_rows = _rows_by_anchor(train_rows)
    strict_regrets: list[float] = []
    selected_family_regrets: list[float] = []
    for rows in anchor_rows.values():
        strict_regrets.append(_family_schedule_row(rows, CANDIDATE_FAMILY_STRICT)["episode_regret_uah"])
        selected_family_regrets.append(
            _family_schedule_row(rows, selected_candidate_family)["episode_regret_uah"]
        )
    strict_mean = mean(strict_regrets)
    strict_median = median(strict_regrets)
    best_rule: dict[str, Any] | None = None
    for switch_margin in switch_margin_grid_uah:
        selected_regrets: list[float] = []
        for rows in anchor_rows.values():
            strict_row = _family_schedule_row(rows, CANDIDATE_FAMILY_STRICT)
            candidate_row = _family_schedule_row(rows, selected_candidate_family)
            gap = strict_row["episode_regret_uah"] - candidate_row["episode_regret_uah"]
            selected_regrets.append(
                candidate_row["episode_regret_uah"]
                if gap >= switch_margin
                else strict_row["episode_regret_uah"]
            )
        selected_mean = mean(selected_regrets)
        selected_median = median(selected_regrets)
        improvement = _improvement_ratio(strict_mean, selected_mean)
        rule = {
            "switch_margin_uah": float(switch_margin),
            "train_anchor_count": len(anchor_rows),
            "strict_mean_regret_uah": strict_mean,
            "selected_family_mean_regret_uah": mean(selected_family_regrets),
            "selected_mean_regret_uah": selected_mean,
            "strict_median_regret_uah": strict_median,
            "selected_median_regret_uah": selected_median,
            "mean_regret_improvement_ratio_vs_strict": improvement,
        }
        if best_rule is None or (selected_mean, switch_margin) < (
            best_rule["selected_mean_regret_uah"],
            best_rule["switch_margin_uah"],
        ):
            best_rule = rule
    if best_rule is None:
        raise ValueError("residual model could not select a switch margin")
    return best_rule


def _rows_by_anchor(rows: list[dict[str, Any]]) -> dict[datetime, list[dict[str, Any]]]:
    grouped: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["anchor_timestamp"], []).append(row)
    return grouped


def _family_schedule_row(rows: list[dict[str, Any]], candidate_family: str) -> dict[str, Any]:
    matches = [row for row in rows if row["candidate_family"] == candidate_family]
    if not matches:
        raise ValueError(f"missing {candidate_family} schedule row")
    return min(matches, key=lambda row: str(row["candidate_model_name"]))


def _final_rows(
    library_rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
) -> list[dict[str, Any]]:
    return [
        row
        for row in library_rows
        if str(row["tenant_id"]) == tenant_id
        and str(row["source_model_name"]) == source_model_name
        and str(row["split_name"]) == "final_holdout"
    ]


def _family_row(rows: list[dict[str, Any]], candidate_family: str) -> dict[str, Any]:
    matches = [row for row in rows if str(row["candidate_family"]) == candidate_family]
    if not matches:
        raise ValueError(f"missing {candidate_family} candidate row")
    return min(matches, key=lambda row: str(row["candidate_model_name"]))


def _benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    forecast_model_name: str,
    selection_role: str,
    selected_strategy_source: str,
    claim_scope: str,
    strategy_kind: str,
    generated_at: datetime,
    model_metadata: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(_payload(row))
    payload.update(
        {
            "source_forecast_model_name": source_model_name,
            "candidate_family": str(row["candidate_family"]),
            "candidate_model_name": str(row["candidate_model_name"]),
            "selection_role": selection_role,
            "selected_strategy_source": selected_strategy_source,
            "claim_scope": claim_scope,
            "academic_scope": DFL_RESIDUAL_ACADEMIC_SCOPE,
            "strategy_kind": strategy_kind,
            "data_quality_tier": str(row["data_quality_tier"]),
            "observed_coverage_ratio": float(row["observed_coverage_ratio"]),
            "safety_violation_count": int(row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
            "model_metadata": _json_safe_model_metadata(model_metadata),
        }
    )
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    return {
        "evaluation_id": (
            f"{strategy_kind}:{row['tenant_id']}:{forecast_model_name}:"
            f"{anchor_timestamp:%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": strategy_kind,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "data_quality_tier": str(row["data_quality_tier"]),
        "observed_coverage_ratio": float(row["observed_coverage_ratio"]),
        "safety_violation_count": int(row["safety_violation_count"]),
        "selection_role": selection_role,
        "selected_strategy_source": selected_strategy_source,
        "claim_scope": claim_scope,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": payload,
    }


def _json_safe_model_metadata(model_metadata: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in model_metadata.items():
        if isinstance(value, datetime):
            safe[key] = value.isoformat()
        elif isinstance(value, (str, int, float, bool, list, tuple)) or value is None:
            safe[key] = value
    return safe


def _residual_confidence_passes(model_row: dict[str, Any]) -> bool:
    return _confidence_passes(
        float(model_row["train_mean_regret_improvement_ratio_vs_strict"]),
        bool(model_row["train_median_not_worse"]),
        bool(model_row["ood_regime_flag"]),
        min_confidence_improvement_ratio=DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    )


def _confidence_passes(
    improvement_ratio: float,
    median_not_worse: bool,
    ood_regime_flag: bool,
    *,
    min_confidence_improvement_ratio: float,
) -> bool:
    return (
        improvement_ratio >= min_confidence_improvement_ratio
        and median_not_worse
        and not ood_regime_flag
    )


def _source_gate_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    control_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    strict_rows = [row for row in rows if str(row["selection_role"]) == "strict_reference"]
    fallback_rows = [row for row in rows if str(row["selection_role"]) == "fallback_strategy"]
    strict_anchors = _tenant_anchor_set(strict_rows)
    fallback_anchors = _tenant_anchor_set(fallback_rows)
    tenant_count = len({tenant_id for tenant_id, _ in fallback_anchors})
    validation_count = len(fallback_anchors)
    if tenant_count < min_tenant_count:
        failures.append(
            f"{source_model_name} tenant_count must be at least {min_tenant_count}; "
            f"observed {tenant_count}"
        )
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    if strict_anchors != fallback_anchors:
        failures.append(f"{source_model_name} strict/fallback rows must cover matching anchors")
    failures.extend(_provenance_failures([*strict_rows, *fallback_rows]))
    strict_regrets = [float(row["regret_uah"]) for row in strict_rows]
    fallback_regrets = [float(row["regret_uah"]) for row in fallback_rows]
    strict_mean = mean(strict_regrets) if strict_regrets else 0.0
    fallback_mean = mean(fallback_regrets) if fallback_regrets else 0.0
    strict_median = median(strict_regrets) if strict_regrets else 0.0
    fallback_median = median(fallback_regrets) if fallback_regrets else 0.0
    improvement = _improvement_ratio(strict_mean, fallback_mean)
    if fallback_rows and strict_rows and improvement < min_mean_regret_improvement_ratio:
        failures.append(
            f"{source_model_name} mean regret improvement vs {control_model_name} must be "
            f"at least {min_mean_regret_improvement_ratio:.1%}; observed {improvement:.1%}"
        )
    if fallback_rows and strict_rows and fallback_median > strict_median:
        failures.append(
            f"{source_model_name} fallback median regret must not be worse than "
            f"{control_model_name}; observed fallback={fallback_median:.2f}, "
            f"strict={strict_median:.2f}"
        )
    summary = {
        "source_model_name": source_model_name,
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "fallback_mean_regret_uah": fallback_mean,
        "strict_median_regret_uah": strict_median,
        "fallback_median_regret_uah": fallback_median,
        "mean_regret_improvement_ratio_vs_strict": improvement,
        "passed": not failures,
    }
    return summary, failures


def _provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    payloads = [_payload(row) for row in rows]
    if any(str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade" for payload in payloads):
        failures.append("fallback promotion requires thesis_grade evidence")
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        failures.append("fallback promotion requires observed coverage ratio of 1.0")
    safety_violation_count = sum(int(payload.get("safety_violation_count", 0)) for payload in payloads)
    if safety_violation_count:
        failures.append(
            f"fallback promotion requires zero safety violations; observed {safety_violation_count}"
        )
    if any(not bool(payload.get("not_full_dfl", True)) for payload in payloads):
        failures.append("fallback evidence must remain not_full_dfl")
    if any(not bool(payload.get("not_market_execution", True)) for payload in payloads):
        failures.append("fallback evidence must remain not_market_execution")
    return failures


def _source_model_name(row: dict[str, Any]) -> str:
    payload = row.get("evaluation_payload")
    if isinstance(payload, dict) and payload.get("source_forecast_model_name"):
        return str(payload["source_forecast_model_name"])
    return str(row.get("source_model_name", ""))


def _tenant_anchor_set(rows: list[dict[str, Any]]) -> set[tuple[str, datetime]]:
    return {
        (
            str(row["tenant_id"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        for row in rows
    }


def _improvement_ratio(control_value: float, candidate_value: float) -> float:
    return (control_value - candidate_value) / abs(control_value) if abs(control_value) > 1e-9 else 0.0


def _promotion_result(*, failures: list[str], metrics: dict[str, Any]) -> PromotionGateResult:
    return PromotionGateResult(
        passed=not failures,
        decision="promote" if not failures else "block",
        description="Promotion gate passed." if not failures else "; ".join(failures),
        metrics=metrics,
    )


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    if "generated_at" not in frame.columns or frame.height == 0:
        return datetime.now(UTC).replace(tzinfo=None)
    values = [
        _datetime_value(value, field_name="generated_at")
        for value in frame.select("generated_at").to_series().to_list()
    ]
    return max(values) if values else datetime.now(UTC).replace(tzinfo=None)
