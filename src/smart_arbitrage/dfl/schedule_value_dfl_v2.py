"""Pairwise schedule-value DFL v2 challenger anchored to frozen V2+."""

from __future__ import annotations

from datetime import datetime
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

DFL_SCHEDULE_VALUE_DFL_V2_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_dfl_v2_not_full_dfl"
)
DFL_SCHEDULE_VALUE_DFL_V2_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_dfl_v2_strict_lp_gate_not_full_dfl"
)
DFL_SCHEDULE_VALUE_DFL_V2_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_schedule_value_dfl_v2_strict_lp_benchmark"
)
DFL_SCHEDULE_VALUE_DFL_V2_PREFIX: Final[str] = "dfl_schedule_value_dfl_v2_"
DFL_SCHEDULE_VALUE_DFL_V2_OBJECTIVE_NAME: Final[str] = (
    "pairwise_schedule_value_ranking_with_v2_plus_fallback"
)
DFL_SCHEDULE_VALUE_DFL_V2_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only pairwise schedule-family value ranking over feasible LP-scored "
    "schedules. This is additive DFL objective evidence, not full DFL, not "
    "deployed Decision Transformer control, and not market execution."
)

REQUIRED_MODEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "learner_model_name",
        "selected_objective_name",
        "selected_schedule_family",
        "selected_pairwise_family_scores",
        "selected_feature_names",
        "selected_feature_weights",
        "fallback_to_v2_plus",
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


def schedule_value_dfl_v2_model_name(source_model_name: str) -> str:
    """Return the stable DFL v2 schedule-value challenger model name."""

    return f"{DFL_SCHEDULE_VALUE_DFL_V2_PREFIX}{source_model_name}"


def build_dfl_schedule_value_dfl_v2_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    learner_v2_frame: pl.DataFrame,
    learner_v2_plus_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int = 18,
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.01,
) -> pl.DataFrame:
    """Select a prior-only schedule family with frozen V2+ non-degradation fallback."""

    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            min_prior_mean_improvement_ratio_vs_v2_plus
        ),
    )
    v2._validate_library_frame(schedule_candidate_library_frame)
    v2._validate_learner_frame(learner_v2_frame)
    _validate_v2_plus_model_frame(learner_v2_plus_frame)

    v2_plus_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_plus_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = v2._library_rows(
                schedule_candidate_library_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            train_rows = [
                row for row in source_rows if str(row["split_name"]) == "train_selection"
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
                    f"{tenant_id}/{source_model_name} schedule-value DFL v2 needs train rows"
                )
            v2_plus_row = v2_plus_rows.get((tenant_id, source_model_name))
            if v2_plus_row is None:
                raise ValueError(f"missing V2+ learner row for {tenant_id}/{source_model_name}")

            eligible_families = _eligible_candidate_families(
                train_rows,
                final_rows,
                required_final_anchor_count=final_anchor_count,
            )
            family_scores = _pairwise_family_scores(
                train_rows,
                candidate_families=eligible_families,
            )
            selected_family = _select_family_from_pairwise_scores(
                train_rows,
                family_scores=family_scores,
                candidate_families=eligible_families,
            )
            selected_train_rows = _select_family_rows(
                train_rows,
                selected_family,
                require_all_anchors=False,
            )
            selected_final_rows = _select_family_rows(final_rows, selected_family)
            v2_plus_train_mean = float(v2_plus_row["selected_train_mean_regret_uah"])
            v2_plus_final_mean = float(v2_plus_row["selected_final_mean_regret_uah"])
            selected_train_mean = v2._mean_regret(selected_train_rows)
            fallback_to_v2_plus = (
                v2._improvement_ratio(v2_plus_train_mean, selected_train_mean)
                < min_prior_mean_improvement_ratio_vs_v2_plus
            )
            effective_train_mean = (
                v2_plus_train_mean if fallback_to_v2_plus else selected_train_mean
            )
            effective_final_rows = [] if fallback_to_v2_plus else selected_final_rows
            effective_final_mean = (
                v2_plus_final_mean
                if fallback_to_v2_plus
                else v2._mean_regret(effective_final_rows)
            )
            strict_final_rows = v2._selected_family_rows(
                final_rows, v2.CANDIDATE_FAMILY_STRICT
            )
            raw_final_rows = v2._selected_family_rows(final_rows, v2.CANDIDATE_FAMILY_RAW)
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": schedule_value_dfl_v2_model_name(
                        source_model_name
                    ),
                    "selected_objective_name": DFL_SCHEDULE_VALUE_DFL_V2_OBJECTIVE_NAME,
                    "selected_schedule_family": selected_family,
                    "selected_pairwise_family_scores": family_scores,
                    "selected_feature_names": [
                        "prior_family_mean_regret_uah",
                        "forecast_spread_uah_mwh",
                        "total_degradation_penalty_uah",
                        "total_throughput_mwh",
                    ],
                    "selected_feature_weights": {
                        "selection_rule": "pairwise_family_value_score_then_prior_schedule_score",
                        "fallback_comparator": "schedule_value_learner_v2_plus",
                        "min_prior_mean_improvement_ratio_vs_v2_plus": (
                            min_prior_mean_improvement_ratio_vs_v2_plus
                        ),
                    },
                    "fallback_to_v2_plus": fallback_to_v2_plus,
                    "train_anchor_count": len(v2._anchor_set(train_rows)),
                    "final_holdout_anchor_count": final_anchor_count,
                    "final_holdout_tenant_anchor_count": final_anchor_count
                    * len(tenant_ids),
                    "strict_final_mean_regret_uah": v2._mean_regret(strict_final_rows),
                    "raw_final_mean_regret_uah": v2._mean_regret(raw_final_rows),
                    "v2_plus_train_mean_regret_uah": v2_plus_train_mean,
                    "v2_plus_final_mean_regret_uah": v2_plus_final_mean,
                    "candidate_train_mean_regret_uah": selected_train_mean,
                    "candidate_final_mean_regret_uah": v2._mean_regret(
                        selected_final_rows
                    ),
                    "selected_train_mean_regret_uah": effective_train_mean,
                    "selected_final_mean_regret_uah": effective_final_mean,
                    "selected_train_median_regret_uah": (
                        float(v2_plus_row["selected_train_median_regret_uah"])
                        if fallback_to_v2_plus
                        else v2._median_regret(selected_train_rows)
                    ),
                    "selected_final_median_regret_uah": (
                        float(v2_plus_row["selected_final_median_regret_uah"])
                        if fallback_to_v2_plus
                        else v2._median_regret(selected_final_rows)
                    ),
                    "selected_train_family_counts": (
                        dict(v2_plus_row["selected_train_family_counts"])
                        if fallback_to_v2_plus
                        else v2._family_counts(selected_train_rows)
                    ),
                    "selected_final_family_counts": (
                        dict(v2_plus_row["selected_final_family_counts"])
                        if fallback_to_v2_plus
                        else v2._family_counts(effective_final_rows)
                    ),
                    "train_mean_regret_improvement_ratio_vs_v2_plus": v2._improvement_ratio(
                        v2_plus_train_mean,
                        effective_train_mean,
                    ),
                    "final_mean_regret_improvement_ratio_vs_v2_plus": v2._improvement_ratio(
                        v2_plus_final_mean,
                        effective_final_mean,
                    ),
                    "final_mean_regret_improvement_ratio_vs_strict": v2._improvement_ratio(
                        v2._mean_regret(strict_final_rows),
                        effective_final_mean,
                    ),
                    "claim_scope": DFL_SCHEDULE_VALUE_DFL_V2_CLAIM_SCOPE,
                    "academic_scope": DFL_SCHEDULE_VALUE_DFL_V2_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_schedule_value_dfl_v2_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    dfl_v2_frame: pl.DataFrame,
    v2_plus_strict_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict/raw/V2+/DFL-v2 rows for the V2+-anchored strict gate."""

    v2._validate_library_frame(schedule_candidate_library_frame)
    _validate_dfl_v2_frame(dfl_v2_frame)
    _validate_v2_plus_strict_frame(v2_plus_strict_frame)
    resolved_generated_at = generated_at or v2._latest_generated_at(
        schedule_candidate_library_frame
    )
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    v2_plus_rows = list(v2_plus_strict_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    for learner_row in dfl_v2_frame.iter_rows(named=True):
        tenant_id = str(learner_row["tenant_id"])
        source_model_name = str(learner_row["source_model_name"])
        final_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and str(row["split_name"]) == "final_holdout"
        ]
        reference_rows = [
            row
            for row in v2_plus_rows
            if str(row["tenant_id"]) == tenant_id
            and _source_model_name(row) == source_model_name
        ]
        reference_by_role_anchor = {
            (
                _selection_role(row),
                v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
            ): row
            for row in reference_rows
        }
        selected_by_anchor = _selected_rows_for_learner(
            final_rows,
            learner_row=learner_row,
        )
        anchor_timestamps = sorted(
            timestamp
            for role, timestamp in reference_by_role_anchor
            if role == "schedule_value_learner_v2_plus"
        )
        for anchor_timestamp in anchor_timestamps:
            rows.extend(
                [
                    _clone_reference_row(
                        reference_by_role_anchor[("strict_reference", anchor_timestamp)],
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="strict_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _clone_reference_row(
                        reference_by_role_anchor[("raw_reference", anchor_timestamp)],
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="raw_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _clone_reference_row(
                        reference_by_role_anchor[
                            ("schedule_value_learner_v2_plus", anchor_timestamp)
                        ],
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v2_plus_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _selected_dfl_v2_row(
                        selected_by_anchor.get(anchor_timestamp),
                        fallback_row=reference_by_role_anchor[
                            ("schedule_value_learner_v2_plus", anchor_timestamp)
                        ],
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        generated_at=resolved_generated_at,
                    ),
                ]
            )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"]
    )


def validate_dfl_schedule_value_dfl_v2_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate structural DFL v2 evidence without requiring headline replacement."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"schedule-value DFL v2 evidence is missing required columns: {missing_columns}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False, "schedule-value DFL v2 evidence has no rows", {"row_count": 0}
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
            min_mean_regret_improvement_ratio_vs_strict=(
                DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
            ),
            min_mean_regret_improvement_ratio_vs_v2_plus=0.0,
            include_promotion_failures=False,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    return EvidenceCheckOutcome(
        not failures,
        "Schedule-value DFL v2 evidence has valid coverage and claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": strict_frame.height,
            "source_model_count": len(source_names),
            "source_model_names": list(source_names),
            "model_summaries": summaries,
        },
    )


def evaluate_dfl_schedule_value_dfl_v2_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0,
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Evaluate whether DFL v2 can replace frozen V2+ offline evidence."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return PromotionGateResult(
            False,
            "blocked",
            f"schedule-value DFL v2 strict frame is missing required columns: {missing_columns}",
            {},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(
            False, "blocked", "schedule-value DFL v2 strict frame has no rows", {}
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
            min_mean_regret_improvement_ratio_vs_strict=(
                min_mean_regret_improvement_ratio_vs_strict
            ),
            min_mean_regret_improvement_ratio_vs_v2_plus=(
                min_mean_regret_improvement_ratio_vs_v2_plus
            ),
            include_promotion_failures=True,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    production_passing = [
        summary for summary in summaries if summary["offline_strategy_replacement_passed"]
    ]
    development_passing = [
        summary for summary in summaries if summary["development_gate_passed"]
    ]
    best = max(
        summaries,
        key=lambda summary: float(summary["mean_regret_improvement_ratio_vs_v2_plus"]),
    )
    metrics = {
        "best_source_model_name": best["source_model_name"],
        "tenant_count": best["tenant_count"],
        "validation_tenant_anchor_count": best["validation_tenant_anchor_count"],
        "strict_mean_regret_uah": best["strict_mean_regret_uah"],
        "raw_mean_regret_uah": best["raw_mean_regret_uah"],
        "v2_plus_mean_regret_uah": best["v2_plus_mean_regret_uah"],
        "selected_mean_regret_uah": best["selected_mean_regret_uah"],
        "strict_median_regret_uah": best["strict_median_regret_uah"],
        "v2_plus_median_regret_uah": best["v2_plus_median_regret_uah"],
        "selected_median_regret_uah": best["selected_median_regret_uah"],
        "mean_regret_improvement_ratio_vs_v2_plus": best[
            "mean_regret_improvement_ratio_vs_v2_plus"
        ],
        "mean_regret_improvement_ratio_vs_strict": best[
            "mean_regret_improvement_ratio_vs_strict"
        ],
        "mean_regret_improvement_ratio_vs_raw": best[
            "mean_regret_improvement_ratio_vs_raw"
        ],
        "development_gate_passed": bool(development_passing),
        "offline_strategy_replacement_passed": bool(production_passing),
        "market_execution_enabled": False,
        "passing_source_model_names": [
            str(summary["source_model_name"]) for summary in production_passing
        ],
        "model_summaries": summaries,
    }
    if production_passing and not failures:
        return PromotionGateResult(
            True,
            "promote_offline_strategy_evidence",
            "schedule-value DFL v2 passes the V2+-anchored strict LP/oracle gate",
            metrics,
        )
    if development_passing:
        return PromotionGateResult(
            False,
            "diagnostic_pass_replacement_blocked",
            "schedule-value DFL v2 improves over raw neural schedules but remains "
            f"blocked versus V2+ or {CONTROL_MODEL_NAME}: " + "; ".join(failures),
            metrics,
        )
    return PromotionGateResult(
        False,
        "blocked",
        "; ".join(failures)
        if failures
        else "schedule-value DFL v2 has no development improvement",
        metrics,
    )


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
    min_prior_mean_improvement_ratio_vs_v2_plus: float,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
    if min_prior_mean_improvement_ratio_vs_v2_plus < 0.0:
        raise ValueError("min_prior_mean_improvement_ratio_vs_v2_plus must not be negative.")


def _validate_v2_plus_model_frame(frame: pl.DataFrame) -> None:
    required_columns = frozenset(
        {
            "tenant_id",
            "source_model_name",
            "selected_train_mean_regret_uah",
            "selected_final_mean_regret_uah",
            "selected_train_median_regret_uah",
            "selected_final_median_regret_uah",
            "selected_train_family_counts",
            "selected_final_family_counts",
            "claim_scope",
            "not_full_dfl",
            "not_market_execution",
        }
    )
    v2._require_columns(
        frame, required_columns, frame_name="schedule_value_learner_v2_plus_frame"
    )
    for row in frame.iter_rows(named=True):
        if not bool(row["not_full_dfl"]):
            raise ValueError("V2+ rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("V2+ rows must keep not_market_execution=true")


def _validate_dfl_v2_frame(frame: pl.DataFrame) -> None:
    v2._require_columns(
        frame, REQUIRED_MODEL_COLUMNS, frame_name="schedule_value_dfl_v2_frame"
    )
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != DFL_SCHEDULE_VALUE_DFL_V2_CLAIM_SCOPE:
            raise ValueError("schedule-value DFL v2 frame has an unexpected claim_scope")
        if not bool(row["not_full_dfl"]):
            raise ValueError("schedule-value DFL v2 rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError(
                "schedule-value DFL v2 rows must keep not_market_execution=true"
            )


def _validate_v2_plus_strict_frame(frame: pl.DataFrame) -> None:
    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"V2+ strict frame is missing required columns: {missing_columns}")
    for row in frame.iter_rows(named=True):
        if not bool(row["not_full_dfl"]):
            raise ValueError("V2+ strict rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("V2+ strict rows must keep not_market_execution=true")


def _pairwise_family_scores(
    rows: list[dict[str, Any]],
    *,
    candidate_families: tuple[str, ...],
) -> dict[str, dict[str, float]]:
    family_rows = {
        family: _select_family_rows(rows, family, require_all_anchors=False)
        for family in candidate_families
    }
    scores: dict[str, dict[str, float]] = {}
    for family, selected_rows in family_rows.items():
        scores[family] = {
            "pairwise_value_score_uah": 0.0,
            "pairwise_mean_value_score_uah": 0.0,
            "pairwise_comparison_count": 0.0,
            "pairwise_win_count": 0.0,
            "train_mean_regret_uah": v2._mean_regret(selected_rows),
        }
    for family, selected_rows in family_rows.items():
        by_anchor = {
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
            for row in selected_rows
        }
        for other_family, other_rows in family_rows.items():
            if family == other_family:
                continue
            other_by_anchor = {
                v2._datetime_value(
                    row["anchor_timestamp"], field_name="anchor_timestamp"
                ): row
                for row in other_rows
            }
            common_anchors = sorted(set(by_anchor).intersection(other_by_anchor))
            for anchor_timestamp in common_anchors:
                regret_delta = float(other_by_anchor[anchor_timestamp]["regret_uah"]) - float(
                    by_anchor[anchor_timestamp]["regret_uah"]
                )
                scores[family]["pairwise_value_score_uah"] += regret_delta
                scores[family]["pairwise_comparison_count"] += 1.0
                if regret_delta > 0.0:
                    scores[family]["pairwise_win_count"] += 1.0
    for family, score in scores.items():
        comparison_count = float(score["pairwise_comparison_count"])
        if comparison_count > 0.0:
            score["pairwise_mean_value_score_uah"] = (
                float(score["pairwise_value_score_uah"]) / comparison_count
            )
    return scores


def _eligible_candidate_families(
    train_rows: list[dict[str, Any]],
    final_rows: list[dict[str, Any]],
    *,
    required_final_anchor_count: int,
) -> tuple[str, ...]:
    train_families = set(_candidate_families(train_rows))
    final_by_anchor = v2._rows_by_anchor(final_rows)
    eligible: list[str] = []
    for family in sorted(train_families):
        final_anchor_count = sum(
            1
            for anchor_rows in final_by_anchor.values()
            if any(str(row["candidate_family"]) == family for row in anchor_rows)
        )
        if final_anchor_count == required_final_anchor_count:
            eligible.append(family)
    if not eligible:
        raise ValueError(
            "schedule-value DFL v2 needs at least one non-control family with full final coverage"
        )
    return tuple(eligible)


def _candidate_families(rows: list[dict[str, Any]]) -> tuple[str, ...]:
    families = {
        str(row["candidate_family"])
        for row in rows
        if str(row["candidate_family"])
        not in {v2.CANDIDATE_FAMILY_STRICT, v2.CANDIDATE_FAMILY_RAW}
    }
    if not families:
        raise ValueError("schedule-value DFL v2 needs at least one non-control family")
    return tuple(sorted(families))


def _select_family_from_pairwise_scores(
    rows: list[dict[str, Any]],
    *,
    family_scores: dict[str, dict[str, float]],
    candidate_families: tuple[str, ...],
) -> str:
    if not rows:
        raise ValueError("schedule-value DFL v2 cannot select from an empty row set")
    return min(
        candidate_families,
        key=lambda family: (
            float(family_scores[family]["train_mean_regret_uah"]),
            -float(family_scores[family]["pairwise_mean_value_score_uah"]),
            -float(family_scores[family]["pairwise_win_count"]),
            family,
        ),
    )


def _select_family_rows(
    rows: list[dict[str, Any]],
    family: str,
    *,
    require_all_anchors: bool = True,
) -> list[dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    for anchor_timestamp, anchor_rows in sorted(v2._rows_by_anchor(rows).items()):
        family_rows = [
            row for row in anchor_rows if str(row["candidate_family"]) == family
        ]
        if not family_rows:
            if not require_all_anchors:
                continue
            raise ValueError(
                f"missing {family} candidate for {anchor_timestamp.isoformat()}"
            )
        selected_rows.append(
            min(
                family_rows,
                key=lambda row: (
                    _prior_schedule_score(row),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _prior_schedule_score(row: dict[str, Any]) -> tuple[float, float, float, float]:
    return (
        float(row.get("prior_family_mean_regret_uah", 0.0)),
        -float(row.get("forecast_spread_uah_mwh", 0.0)),
        float(row.get("total_degradation_penalty_uah", 0.0)),
        float(row.get("total_throughput_mwh", 0.0)),
    )


def _selected_rows_for_learner(
    final_rows: list[dict[str, Any]],
    *,
    learner_row: dict[str, Any],
) -> dict[datetime, dict[str, Any]]:
    if bool(learner_row["fallback_to_v2_plus"]):
        return {}
    selected_family = str(learner_row["selected_schedule_family"])
    return {
        v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
        for row in _select_family_rows(final_rows, selected_family)
    }


def _selected_dfl_v2_row(
    selected_row: dict[str, Any] | None,
    *,
    fallback_row: dict[str, Any],
    source_model_name: str,
    learner_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    if selected_row is None:
        return _clone_reference_row(
            fallback_row,
            source_model_name=source_model_name,
            learner_row=learner_row,
            role="schedule_value_dfl_v2",
            generated_at=generated_at,
        )
    return _strict_benchmark_row(
        selected_row,
        source_model_name=source_model_name,
        learner_row=learner_row,
        role="schedule_value_dfl_v2",
        generated_at=generated_at,
    )


def _clone_reference_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    cloned = dict(row)
    anchor_timestamp = v2._datetime_value(
        cloned["anchor_timestamp"], field_name="anchor_timestamp"
    )
    payload = dict(v2._payload(cloned))
    payload.update(
        _payload_updates(
            row=cloned,
            source_model_name=source_model_name,
            learner_row=learner_row,
            role=role,
        )
    )
    cloned.update(
        {
            "evaluation_id": (
                f"{cloned['tenant_id']}:schedule-value-dfl-v2:{source_model_name}:"
                f"{role}:{anchor_timestamp:%Y%m%dT%H%M}"
            ),
            "strategy_kind": DFL_SCHEDULE_VALUE_DFL_V2_STRICT_LP_STRATEGY_KIND,
            "selection_role": role,
            "generated_at": generated_at,
            "claim_scope": DFL_SCHEDULE_VALUE_DFL_V2_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "evaluation_payload": payload,
        }
    )
    if role == "schedule_value_dfl_v2":
        cloned["forecast_model_name"] = schedule_value_dfl_v2_model_name(source_model_name)
    return cloned


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(v2._payload(row))
    payload.update(
        _payload_updates(
            row=row,
            source_model_name=source_model_name,
            learner_row=learner_row,
            role=role,
        )
    )
    anchor_timestamp = v2._datetime_value(
        row["anchor_timestamp"], field_name="anchor_timestamp"
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:schedule-value-dfl-v2:{source_model_name}:"
            f"{role}:{row['candidate_family']}:{anchor_timestamp:%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": schedule_value_dfl_v2_model_name(source_model_name),
        "strategy_kind": DFL_SCHEDULE_VALUE_DFL_V2_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": v2._first_or_default(
            row["soc_fraction_vector"], default=0.5
        ),
        "starting_soc_source": "schedule_candidate_library_v2_plus",
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
        "claim_scope": DFL_SCHEDULE_VALUE_DFL_V2_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": payload,
    }


def _payload_updates(
    *,
    row: dict[str, Any],
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
) -> dict[str, Any]:
    return {
        "strict_gate_kind": "dfl_schedule_value_dfl_v2_strict_lp",
        "source_forecast_model_name": source_model_name,
        "learner_model_name": schedule_value_dfl_v2_model_name(source_model_name),
        "selected_objective_name": str(learner_row["selected_objective_name"]),
        "selected_schedule_family": str(learner_row["selected_schedule_family"]),
        "selected_pairwise_family_scores": dict(
            learner_row["selected_pairwise_family_scores"]
        ),
        "selected_feature_names": list(learner_row["selected_feature_names"]),
        "selected_feature_weights": dict(learner_row["selected_feature_weights"]),
        "fallback_to_v2_plus": bool(learner_row["fallback_to_v2_plus"]),
        "selector_row_candidate_family": str(
            row.get("candidate_family", _payload(row).get("selector_row_candidate_family", ""))
        ),
        "selector_row_candidate_model_name": str(
            row.get(
                "candidate_model_name",
                _payload(row).get("selector_row_candidate_model_name", ""),
            )
        ),
        "selector_row_role": role,
        "selection_role": role,
        "claim_scope": DFL_SCHEDULE_VALUE_DFL_V2_STRICT_CLAIM_SCOPE,
        "academic_scope": DFL_SCHEDULE_VALUE_DFL_V2_ACADEMIC_SCOPE,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": int(row["safety_violation_count"]),
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _gate_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio_vs_strict: float,
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    include_promotion_failures: bool,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [
        row for row in source_rows if _selection_role(row) == "strict_reference"
    ]
    raw_rows = [row for row in source_rows if _selection_role(row) == "raw_reference"]
    v2_plus_rows = [
        row
        for row in source_rows
        if _selection_role(row) == "schedule_value_learner_v2_plus_reference"
    ]
    selected_rows = [
        row for row in source_rows if _selection_role(row) == "schedule_value_dfl_v2"
    ]
    strict_anchors = v2._tenant_anchor_set(strict_rows)
    raw_anchors = v2._tenant_anchor_set(raw_rows)
    v2_plus_anchors = v2._tenant_anchor_set(v2_plus_rows)
    selected_anchors = v2._tenant_anchor_set(selected_rows)
    if (
        strict_anchors != raw_anchors
        or strict_anchors != v2_plus_anchors
        or strict_anchors != selected_anchors
    ):
        failures.append(
            f"{source_model_name} strict/raw/V2+/DFL-v2 rows must cover matching tenant-anchor sets"
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
        v2._provenance_failures(
            [*strict_rows, *raw_rows, *v2_plus_rows, *selected_rows]
        )
    )
    strict_mean = v2._mean_regret(strict_rows)
    raw_mean = v2._mean_regret(raw_rows)
    v2_plus_mean = v2._mean_regret(v2_plus_rows)
    selected_mean = v2._mean_regret(selected_rows)
    strict_median = v2._median_regret(strict_rows)
    v2_plus_median = v2._median_regret(v2_plus_rows)
    selected_median = v2._median_regret(selected_rows)
    improvement_vs_raw = v2._improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = v2._improvement_ratio(strict_mean, selected_mean)
    improvement_vs_v2_plus = v2._improvement_ratio(v2_plus_mean, selected_mean)
    development_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_raw > 0.0
    )
    replacement_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio_vs_strict
        and improvement_vs_v2_plus > min_mean_regret_improvement_ratio_vs_v2_plus
        and selected_median <= v2_plus_median
        and not failures
    )
    if include_promotion_failures:
        if (
            selected_rows
            and v2_plus_rows
            and improvement_vs_v2_plus
            <= min_mean_regret_improvement_ratio_vs_v2_plus
        ):
            failures.append(
                f"{source_model_name} DFL v2 must improve over frozen V2+ by more than "
                f"{min_mean_regret_improvement_ratio_vs_v2_plus:.1%}; "
                f"observed {improvement_vs_v2_plus:.1%}"
            )
        if (
            selected_rows
            and strict_rows
            and improvement_vs_strict < min_mean_regret_improvement_ratio_vs_strict
        ):
            failures.append(
                f"{source_model_name} mean regret improvement vs {CONTROL_MODEL_NAME} must be at least "
                f"{min_mean_regret_improvement_ratio_vs_strict:.1%}; observed {improvement_vs_strict:.1%}"
            )
        if selected_rows and v2_plus_rows and selected_median > v2_plus_median:
            failures.append(
                f"{source_model_name} median regret must not be worse than frozen V2+; "
                f"observed learner={selected_median:.2f}, v2_plus={v2_plus_median:.2f}"
            )
    return {
        "source_model_name": source_model_name,
        "learner_model_name": schedule_value_dfl_v2_model_name(source_model_name),
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "v2_plus_mean_regret_uah": v2_plus_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "v2_plus_median_regret_uah": v2_plus_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2_plus,
        "development_gate_passed": development_passed,
        "offline_strategy_replacement_passed": replacement_passed,
        "market_execution_enabled": False,
        "failures": failures,
    }, failures


def _source_model_name(row: dict[str, Any]) -> str:
    if "source_model_name" in row:
        return str(row["source_model_name"])
    payload = v2._payload(row)
    return str(payload.get("source_forecast_model_name", ""))


def _selection_role(row: dict[str, Any]) -> str:
    if row.get("selection_role"):
        return str(row["selection_role"])
    payload = v2._payload(row)
    return str(payload.get("selection_role", payload.get("selector_row_role", "")))


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    return v2._payload(row)


__all__ = [
    "DFL_SCHEDULE_VALUE_DFL_V2_STRICT_LP_STRATEGY_KIND",
    "build_dfl_schedule_value_dfl_v2_frame",
    "build_dfl_schedule_value_dfl_v2_strict_lp_benchmark_frame",
    "evaluate_dfl_schedule_value_dfl_v2_gate",
    "schedule_value_dfl_v2_model_name",
    "validate_dfl_schedule_value_dfl_v2_evidence",
]
