"""Prior-only schedule/value learner for DFL v2 research evidence."""

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
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_SCHEDULE_VALUE_LEARNER_V2_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_learner_v2_not_full_dfl"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_learner_v2_strict_lp_gate_not_full_dfl"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_schedule_value_learner_v2_strict_lp_benchmark"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_PREFIX: Final[str] = "dfl_schedule_value_learner_v2_"
DFL_SCHEDULE_VALUE_LEARNER_V2_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only schedule/value learner over feasible LP-scored schedules. "
    "This is DFL research evidence, not full DFL, not Decision Transformer "
    "control, and not market execution."
)

CANDIDATE_FAMILY_STRICT: Final[str] = "strict_control"
CANDIDATE_FAMILY_RAW: Final[str] = "raw_source"

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
REQUIRED_MODEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "learner_model_name",
        "selected_weight_profile_name",
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

WEIGHT_PROFILES: Final[tuple[dict[str, float | str], ...]] = (
    {
        "name": "prior_regret_value",
        "prior_family_mean_regret_uah": 1.0,
        "forecast_spread_uah_mwh": 0.0,
        "forecast_objective_value_uah": 0.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.0,
        "soc_min_slack_fraction": 0.0,
        "non_strict_penalty_uah": 0.0,
    },
    {
        "name": "spread_value",
        "prior_family_mean_regret_uah": 0.6,
        "forecast_spread_uah_mwh": -0.02,
        "forecast_objective_value_uah": -0.001,
        "total_degradation_penalty_uah": 0.5,
        "total_throughput_mwh": 25.0,
        "soc_min_slack_fraction": -50.0,
        "non_strict_penalty_uah": 0.0,
    },
    {
        "name": "strict_guarded_prior_value",
        "prior_family_mean_regret_uah": 1.0,
        "forecast_spread_uah_mwh": -0.01,
        "forecast_objective_value_uah": -0.0005,
        "total_degradation_penalty_uah": 0.25,
        "total_throughput_mwh": 20.0,
        "soc_min_slack_fraction": -25.0,
        "non_strict_penalty_uah": 50.0,
    },
)


def schedule_value_learner_v2_model_name(source_model_name: str) -> str:
    """Return the stable DFL v2 schedule/value learner model name."""

    return f"{DFL_SCHEDULE_VALUE_LEARNER_V2_PREFIX}{source_model_name}"


def build_dfl_schedule_value_learner_v2_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    weight_profiles: tuple[dict[str, float | str], ...] = WEIGHT_PROFILES,
) -> pl.DataFrame:
    """Choose a schedule-scoring profile using train-selection anchors only."""

    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        weight_profiles=weight_profiles,
    )
    _validate_library_frame(schedule_candidate_library_frame)
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = _library_rows(
                schedule_candidate_library_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            train_rows = [row for row in source_rows if str(row["split_name"]) == "train_selection"]
            final_rows = [row for row in source_rows if str(row["split_name"]) == "final_holdout"]
            final_anchor_count = len(_anchor_set(final_rows))
            if final_anchor_count != final_validation_anchor_count_per_tenant:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} final-holdout tenant-anchor count must be "
                    f"{final_validation_anchor_count_per_tenant}; observed {final_anchor_count}"
                )
            if not train_rows:
                raise ValueError(f"{tenant_id}/{source_model_name} schedule/value learner needs train rows")
            selected_profile = _select_weight_profile(train_rows, weight_profiles)
            selected_train_rows = _select_rows_by_score(train_rows, profile=selected_profile)
            selected_final_rows = _select_rows_by_score(final_rows, profile=selected_profile)
            strict_train_rows = _selected_family_rows(train_rows, CANDIDATE_FAMILY_STRICT)
            raw_train_rows = _selected_family_rows(train_rows, CANDIDATE_FAMILY_RAW)
            strict_final_rows = _selected_family_rows(final_rows, CANDIDATE_FAMILY_STRICT)
            raw_final_rows = _selected_family_rows(final_rows, CANDIDATE_FAMILY_RAW)
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": schedule_value_learner_v2_model_name(source_model_name),
                    "selected_weight_profile_name": str(selected_profile["name"]),
                    "selected_weight_profile": _profile_payload(selected_profile),
                    "train_anchor_count": len(_anchor_set(train_rows)),
                    "final_holdout_anchor_count": final_anchor_count,
                    "final_holdout_tenant_anchor_count": final_anchor_count * len(tenant_ids),
                    "strict_train_mean_regret_uah": _mean_regret(strict_train_rows),
                    "raw_train_mean_regret_uah": _mean_regret(raw_train_rows),
                    "selected_train_mean_regret_uah": _mean_regret(selected_train_rows),
                    "strict_train_median_regret_uah": _median_regret(strict_train_rows),
                    "selected_train_median_regret_uah": _median_regret(selected_train_rows),
                    "strict_final_mean_regret_uah": _mean_regret(strict_final_rows),
                    "raw_final_mean_regret_uah": _mean_regret(raw_final_rows),
                    "selected_final_mean_regret_uah": _mean_regret(selected_final_rows),
                    "strict_final_median_regret_uah": _median_regret(strict_final_rows),
                    "selected_final_median_regret_uah": _median_regret(selected_final_rows),
                    "selected_train_family_counts": _family_counts(selected_train_rows),
                    "selected_final_family_counts": _family_counts(selected_final_rows),
                    "train_mean_regret_improvement_ratio_vs_strict": _improvement_ratio(
                        _mean_regret(strict_train_rows),
                        _mean_regret(selected_train_rows),
                    ),
                    "final_mean_regret_improvement_ratio_vs_strict": _improvement_ratio(
                        _mean_regret(strict_final_rows),
                        _mean_regret(selected_final_rows),
                    ),
                    "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_CLAIM_SCOPE,
                    "academic_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    learner_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict/raw/learner rows for the schedule-value DFL v2 gate."""

    _validate_library_frame(schedule_candidate_library_frame)
    _validate_learner_frame(learner_frame)
    resolved_generated_at = generated_at or _latest_generated_at(schedule_candidate_library_frame)
    rows: list[dict[str, Any]] = []
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    for learner_row in learner_frame.iter_rows(named=True):
        tenant_id = str(learner_row["tenant_id"])
        source_model_name = str(learner_row["source_model_name"])
        profile = _profile_by_name(str(learner_row["selected_weight_profile_name"]))
        final_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and str(row["split_name"]) == "final_holdout"
        ]
        selected_rows_by_anchor = {
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
            for row in _select_rows_by_score(final_rows, profile=profile)
        }
        for anchor_timestamp in sorted(selected_rows_by_anchor):
            anchor_rows = [
                row
                for row in final_rows
                if _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                == anchor_timestamp
            ]
            strict_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
            raw_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_RAW)
            selected_row = selected_rows_by_anchor[anchor_timestamp]
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
                        selected_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner",
                        generated_at=resolved_generated_at,
                    ),
                ]
            )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"])


def validate_dfl_schedule_value_learner_v2_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate structural schedule/value learner evidence without requiring promotion."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"schedule/value learner evidence is missing required columns: {missing_columns}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "schedule/value learner evidence has no rows", {"row_count": 0})
    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
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
        "Schedule/value learner evidence has valid coverage and claim boundaries."
        if not failures
        else "; ".join(failures),
        metadata,
    )


def evaluate_dfl_schedule_value_learner_v2_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate the schedule/value learner strict LP/oracle gate."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return PromotionGateResult(
            False,
            "blocked",
            f"schedule/value learner strict frame is missing required columns: {missing_columns}",
            {},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(False, "blocked", "schedule/value learner strict frame has no rows", {})
    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
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
        return PromotionGateResult(True, "promote", "schedule/value learner passes strict LP/oracle gate", metrics)
    if development_passing:
        return PromotionGateResult(
            False,
            "diagnostic_pass_production_blocked",
            "schedule/value learner improves over raw neural schedules but remains blocked versus "
            f"{CONTROL_MODEL_NAME}: " + "; ".join(failures),
            metrics,
        )
    return PromotionGateResult(
        False,
        "blocked",
        "; ".join(failures) if failures else "schedule/value learner has no development improvement",
        metrics,
    )


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
    weight_profiles: tuple[dict[str, float | str], ...],
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
    if not weight_profiles:
        raise ValueError("weight_profiles must contain at least one profile.")
    for profile in weight_profiles:
        if "name" not in profile:
            raise ValueError("every weight profile needs a name.")


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
            raise ValueError("schedule/value learner requires thesis_grade rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("schedule/value learner requires observed coverage ratio of 1.0")
        if int(row["safety_violation_count"]):
            raise ValueError("schedule/value learner requires zero safety violations")
        if not bool(row["not_full_dfl"]):
            raise ValueError("schedule/value learner requires not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("schedule/value learner requires not_market_execution=true")


def _validate_learner_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_MODEL_COLUMNS, frame_name="schedule_value_learner_frame")
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != DFL_SCHEDULE_VALUE_LEARNER_V2_CLAIM_SCOPE:
            raise ValueError("schedule/value learner frame has an unexpected claim_scope")
        if not bool(row["not_full_dfl"]):
            raise ValueError("schedule/value learner rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("schedule/value learner rows must keep not_market_execution=true")


def _select_weight_profile(
    train_rows: list[dict[str, Any]],
    weight_profiles: tuple[dict[str, float | str], ...],
) -> dict[str, float | str]:
    return min(
        weight_profiles,
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
    selected_rows: list[dict[str, Any]] = []
    for anchor_timestamp, anchor_rows in sorted(_rows_by_anchor(rows).items()):
        if not any(str(row["candidate_family"]) == CANDIDATE_FAMILY_STRICT for row in anchor_rows):
            raise ValueError(f"missing strict_control row for {anchor_timestamp.isoformat()}")
        selected_rows.append(
            min(
                anchor_rows,
                key=lambda row: (
                    _score_row(row, profile=profile),
                    _family_sort_index(str(row["candidate_family"])),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _score_row(row: dict[str, Any], *, profile: dict[str, float | str]) -> float:
    score = (
        _profile_weight(profile, "prior_family_mean_regret_uah") * float(row["prior_family_mean_regret_uah"])
        + _profile_weight(profile, "forecast_spread_uah_mwh") * float(row["forecast_spread_uah_mwh"])
        + _profile_weight(profile, "forecast_objective_value_uah") * float(row["forecast_objective_value_uah"])
        + _profile_weight(profile, "total_degradation_penalty_uah") * float(row["total_degradation_penalty_uah"])
        + _profile_weight(profile, "total_throughput_mwh") * float(row["total_throughput_mwh"])
        + _profile_weight(profile, "soc_min_slack_fraction") * float(row.get("soc_min_slack_fraction", 0.0))
    )
    if str(row["candidate_family"]) != CANDIDATE_FAMILY_STRICT:
        score += _profile_weight(profile, "non_strict_penalty_uah")
    return score


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(_payload(row))
    learner_model_name = schedule_value_learner_v2_model_name(source_model_name)
    forecast_model_name = learner_model_name if role == "schedule_value_learner" else str(row["candidate_model_name"])
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    payload.update(
        {
            "strict_gate_kind": "dfl_schedule_value_learner_v2_strict_lp",
            "source_forecast_model_name": source_model_name,
            "learner_model_name": learner_model_name,
            "selected_weight_profile_name": str(learner_row["selected_weight_profile_name"]),
            "selected_weight_profile": _profile_payload(
                _profile_by_name(str(learner_row["selected_weight_profile_name"]))
            ),
            "selector_row_candidate_family": str(row["candidate_family"]),
            "selector_row_candidate_model_name": str(row["candidate_model_name"]),
            "selector_row_role": role,
            "selection_role": role,
            "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_CLAIM_SCOPE,
            "academic_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": int(row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:schedule-value-learner-v2:{source_model_name}:"
            f"{role}:{row['candidate_family']}:{anchor_timestamp:%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _first_or_default(row["soc_fraction_vector"], default=0.5),
        "starting_soc_source": "schedule_candidate_library_v2",
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
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": int(row["safety_violation_count"]),
        "selection_role": role,
        "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": payload,
    }


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
    learner_model_name = schedule_value_learner_v2_model_name(source_model_name)
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [row for row in source_rows if _selection_role(row) == "strict_reference"]
    raw_rows = [row for row in source_rows if _selection_role(row) == "raw_reference"]
    selected_rows = [row for row in source_rows if str(row["forecast_model_name"]) == learner_model_name]
    strict_anchors = _tenant_anchor_set(strict_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    selected_anchors = _tenant_anchor_set(selected_rows)
    if strict_anchors != raw_anchors or strict_anchors != selected_anchors:
        failures.append(f"{source_model_name} strict/raw/learner rows must cover matching tenant-anchor sets")
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
    if include_promotion_failures:
        if selected_rows and strict_rows and improvement_vs_strict < min_mean_regret_improvement_ratio:
            failures.append(
                f"{source_model_name} mean regret improvement vs {CONTROL_MODEL_NAME} must be at least "
                f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
            )
        if selected_rows and strict_rows and selected_median > strict_median:
            failures.append(
                f"{source_model_name} median regret must not be worse than {CONTROL_MODEL_NAME}; "
                f"observed learner={selected_median:.2f}, strict={strict_median:.2f}"
            )
    return {
        "source_model_name": source_model_name,
        "learner_model_name": learner_model_name,
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


def _library_rows(frame: pl.DataFrame, *, tenant_id: str, source_model_name: str) -> list[dict[str, Any]]:
    rows = [
        row
        for row in frame.iter_rows(named=True)
        if str(row["tenant_id"]) == tenant_id and str(row["source_model_name"]) == source_model_name
    ]
    if not rows:
        raise ValueError(f"missing schedule/value learner rows for {tenant_id}/{source_model_name}")
    return rows


def _rows_by_anchor(rows: list[dict[str, Any]]) -> dict[datetime, list[dict[str, Any]]]:
    grouped: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"), []).append(row)
    return grouped


def _selected_family_rows(rows: list[dict[str, Any]], candidate_family: str) -> list[dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    for anchor_rows in _rows_by_anchor(rows).values():
        selected_rows.append(_single_family_row(anchor_rows, candidate_family))
    return selected_rows


def _single_family_row(rows: list[dict[str, Any]], candidate_family: str) -> dict[str, Any]:
    matches = [row for row in rows if str(row["candidate_family"]) == candidate_family]
    if not matches:
        raise ValueError(f"missing {candidate_family} row")
    return min(matches, key=lambda row: str(row["candidate_model_name"]))


def _profile_by_name(name: str) -> dict[str, float | str]:
    for profile in WEIGHT_PROFILES:
        if str(profile["name"]) == name:
            return profile
    raise ValueError(f"unknown schedule/value learner weight profile: {name}")


def _profile_weight(profile: dict[str, float | str], key: str) -> float:
    return float(profile.get(key, 0.0))


def _profile_payload(profile: dict[str, float | str]) -> dict[str, float | str]:
    return {key: value for key, value in profile.items()}


def _family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        family = str(row["candidate_family"])
        counts[family] = counts.get(family, 0) + 1
    return dict(sorted(counts.items()))


def _family_sort_index(candidate_family: str) -> int:
    order = (CANDIDATE_FAMILY_STRICT, CANDIDATE_FAMILY_RAW)
    return order.index(candidate_family) if candidate_family in order else len(order)


def _committed_action(row: dict[str, Any]) -> str:
    committed_power = _first_or_default(row["dispatch_mw_vector"], default=0.0)
    if committed_power > 0.0:
        return "DISCHARGE"
    if committed_power < 0.0:
        return "CHARGE"
    return "HOLD"


def _first_or_default(value: object, *, default: float) -> float:
    values = _float_list(value, field_name="vector")
    return values[0] if values else default


def _float_list(value: object, *, field_name: str) -> list[float]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list")
    return [float(item) for item in value]


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload", {})
    return payload if isinstance(payload, dict) else {}


def _selection_role(row: dict[str, Any]) -> str:
    if row.get("selection_role"):
        return str(row["selection_role"])
    payload = _payload(row)
    return str(payload.get("selection_role", payload.get("selector_row_role", "")))


def _source_model_name(row: dict[str, Any]) -> str:
    if row.get("source_model_name"):
        return str(row["source_model_name"])
    payload = _payload(row)
    return str(payload.get("source_forecast_model_name", ""))


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


def _improvement_ratio(control_value: float, candidate_value: float) -> float:
    return (control_value - candidate_value) / abs(control_value) if abs(control_value) > 1e-9 else 0.0


def _provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    payloads = [_payload(row) for row in rows]
    if not rows:
        return failures
    if any(str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade" for payload in payloads):
        failures.append("schedule/value learner promotion requires thesis_grade evidence")
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        failures.append("schedule/value learner promotion requires observed coverage ratio of 1.0")
    safety_violation_count = sum(int(payload.get("safety_violation_count", 0)) for payload in payloads)
    if safety_violation_count:
        failures.append(
            f"schedule/value learner promotion requires zero safety violations; observed {safety_violation_count}"
        )
    if any(not bool(payload.get("not_full_dfl", True)) for payload in payloads):
        failures.append("schedule/value learner evidence must remain not_full_dfl")
    if any(not bool(payload.get("not_market_execution", True)) for payload in payloads):
        failures.append("schedule/value learner evidence must remain not_market_execution")
    return failures


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    if "generated_at" not in frame.columns or frame.height == 0:
        return datetime.now(UTC).replace(tzinfo=None)
    values = [
        _datetime_value(value, field_name="generated_at")
        for value in frame.select("generated_at").to_series().to_list()
    ]
    return max(values) if values else datetime.now(UTC).replace(tzinfo=None)


def _datetime_value(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO datetime.") from exc
    raise ValueError(f"{field_name} must be a datetime.")


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {', '.join(missing)}")
