"""Trajectory/value selector evidence over already strict-LP-scored schedules."""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.action_targeting import action_target_model_name
from smart_arbitrage.dfl.decision_targeting import decision_target_model_name, panel_v2_model_name
from smart_arbitrage.dfl.promotion_gate import (
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    DEFAULT_MIN_ANCHOR_COUNT,
    CONTROL_MODEL_NAME,
    PromotionGateResult,
)

TRAJECTORY_VALUE_CANDIDATE_CLAIM_SCOPE: Final[str] = (
    "dfl_trajectory_value_candidate_panel_not_full_dfl"
)
TRAJECTORY_VALUE_SELECTOR_CLAIM_SCOPE: Final[str] = (
    "dfl_trajectory_value_selector_v1_not_full_dfl"
)
TRAJECTORY_VALUE_SELECTOR_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_trajectory_value_selector_v1_strict_lp_gate_not_full_dfl"
)
TRAJECTORY_VALUE_SELECTOR_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_trajectory_value_selector_strict_lp_benchmark"
)
TRAJECTORY_VALUE_SELECTOR_PREFIX: Final[str] = "dfl_trajectory_value_selector_v1_"
TRAJECTORY_VALUE_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only trajectory/value selector over feasible strict-LP-scored schedules. "
    "It is not full DFL, not Decision Transformer control, and not market execution."
)

CANDIDATE_FAMILY_STRICT: Final[str] = "strict_control"
CANDIDATE_FAMILY_RAW: Final[str] = "raw_source"
CANDIDATE_FAMILY_PANEL_V2: Final[str] = "panel_v2"
CANDIDATE_FAMILY_DECISION_V3: Final[str] = "decision_target_v3"
CANDIDATE_FAMILY_ACTION_V4: Final[str] = "action_target_v4"
CANDIDATE_FAMILY_ORDER: Final[tuple[str, ...]] = (
    CANDIDATE_FAMILY_STRICT,
    CANDIDATE_FAMILY_RAW,
    CANDIDATE_FAMILY_PANEL_V2,
    CANDIDATE_FAMILY_DECISION_V3,
    CANDIDATE_FAMILY_ACTION_V4,
)

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
REQUIRED_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "final_validation_anchor_count",
        "first_final_holdout_anchor_timestamp",
        "last_final_holdout_anchor_timestamp",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_CANDIDATE_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "split_name",
        "horizon_hours",
        "decision_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "prior_selection_mean_regret_uah",
        "data_quality_tier",
        "observed_coverage_ratio",
        "safety_violation_count",
        "not_full_dfl",
        "not_market_execution",
        "evaluation_payload",
    }
)
REQUIRED_SELECTOR_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "selected_candidate_family",
        "selected_candidate_model_name",
        "final_holdout_tenant_anchor_count",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)


def trajectory_value_selector_model_name(source_model_name: str) -> str:
    """Return the selector v1 model name for a raw source model."""

    return f"{TRAJECTORY_VALUE_SELECTOR_PREFIX}{source_model_name}"


def build_dfl_trajectory_value_candidate_panel_frame(
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    offline_dfl_panel_strict_lp_benchmark_frame: pl.DataFrame,
    offline_dfl_decision_target_strict_lp_benchmark_frame: pl.DataFrame,
    offline_dfl_action_target_strict_lp_benchmark_frame: pl.DataFrame,
    offline_dfl_panel_experiment_frame: pl.DataFrame,
    offline_dfl_decision_target_panel_frame: pl.DataFrame,
    offline_dfl_action_target_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    max_train_anchors_per_tenant: int = 72,
) -> pl.DataFrame:
    """Build final-holdout trajectory rows plus prior-only family selection metrics."""

    _require_columns(
        real_data_rolling_origin_benchmark_frame,
        REQUIRED_EVALUATION_COLUMNS,
        frame_name="real_data_rolling_origin_benchmark_frame",
    )
    _require_columns(
        offline_dfl_action_target_strict_lp_benchmark_frame,
        REQUIRED_EVALUATION_COLUMNS,
        frame_name="offline_dfl_action_target_strict_lp_benchmark_frame",
    )
    _require_columns(offline_dfl_panel_experiment_frame, REQUIRED_PANEL_COLUMNS, frame_name="offline_dfl_panel_experiment_frame")
    _require_columns(
        offline_dfl_decision_target_panel_frame,
        REQUIRED_PANEL_COLUMNS,
        frame_name="offline_dfl_decision_target_panel_frame",
    )
    _require_columns(
        offline_dfl_action_target_panel_frame,
        REQUIRED_PANEL_COLUMNS,
        frame_name="offline_dfl_action_target_panel_frame",
    )
    _validate_common_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
    )
    if max_train_anchors_per_tenant <= 0:
        raise ValueError("max_train_anchors_per_tenant must be positive.")

    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            panel_row = _single_panel_row(
                offline_dfl_panel_experiment_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                frame_name="offline_dfl_panel_experiment_frame",
            )
            decision_row = _single_panel_row(
                offline_dfl_decision_target_panel_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                frame_name="offline_dfl_decision_target_panel_frame",
            )
            action_row = _single_panel_row(
                offline_dfl_action_target_panel_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                frame_name="offline_dfl_action_target_panel_frame",
            )
            _validate_panel_rows_align(
                [panel_row, decision_row, action_row],
                final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
            )
            prior_scores = _prior_scores_by_family(
                real_data_rolling_origin_benchmark_frame,
                panel_row=panel_row,
                decision_row=decision_row,
                action_row=action_row,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                max_train_anchors_per_tenant=max_train_anchors_per_tenant,
            )
            _validate_strict_sidecar_coverage(
                offline_dfl_panel_strict_lp_benchmark_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                expected_families=(
                    CANDIDATE_FAMILY_STRICT,
                    CANDIDATE_FAMILY_RAW,
                    CANDIDATE_FAMILY_PANEL_V2,
                ),
                final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
                frame_name="offline_dfl_panel_strict_lp_benchmark_frame",
            )
            _validate_strict_sidecar_coverage(
                offline_dfl_decision_target_strict_lp_benchmark_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                expected_families=(
                    CANDIDATE_FAMILY_STRICT,
                    CANDIDATE_FAMILY_RAW,
                    CANDIDATE_FAMILY_PANEL_V2,
                    CANDIDATE_FAMILY_DECISION_V3,
                ),
                final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
                frame_name="offline_dfl_decision_target_strict_lp_benchmark_frame",
            )
            for family in CANDIDATE_FAMILY_ORDER:
                family_rows = _strict_family_rows(
                    offline_dfl_action_target_strict_lp_benchmark_frame,
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    candidate_family=family,
                    final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
                    frame_name="offline_dfl_action_target_strict_lp_benchmark_frame",
                )
                for row in family_rows:
                    rows.append(
                        _candidate_panel_row(
                            row,
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family=family,
                            prior_score=prior_scores[family],
                        )
                    )
    return pl.DataFrame(rows).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "candidate_family"]
    )


def build_dfl_trajectory_value_selector_frame(
    candidate_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    min_final_holdout_tenant_anchor_count_per_source_model: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> pl.DataFrame:
    """Select the schedule family with lowest prior/train-selection regret."""

    _require_columns(candidate_panel_frame, REQUIRED_CANDIDATE_PANEL_COLUMNS, frame_name="candidate_panel_frame")
    _validate_common_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=1,
    )
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = [
                row
                for row in candidate_panel_frame.iter_rows(named=True)
                if row["tenant_id"] == tenant_id and row["source_model_name"] == source_model_name
            ]
            if not source_rows:
                raise ValueError(f"missing trajectory/value candidate rows for {tenant_id}/{source_model_name}")
            family_scores = _family_prior_scores(source_rows)
            selected_family = min(
                CANDIDATE_FAMILY_ORDER,
                key=lambda family: (family_scores[family], CANDIDATE_FAMILY_ORDER.index(family)),
            )
            rows.append(
                _selector_row(
                    source_rows,
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    selected_family=selected_family,
                    family_scores=family_scores,
                    min_final_holdout_tenant_anchor_count_per_source_model=(
                        min_final_holdout_tenant_anchor_count_per_source_model
                    ),
                )
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_trajectory_value_selector_strict_lp_benchmark_frame(
    candidate_panel_frame: pl.DataFrame,
    selector_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict-LP benchmark rows for strict control, raw source, and selected family."""

    _require_columns(candidate_panel_frame, REQUIRED_CANDIDATE_PANEL_COLUMNS, frame_name="candidate_panel_frame")
    _require_columns(selector_frame, REQUIRED_SELECTOR_COLUMNS, frame_name="selector_frame")
    resolved_generated_at = generated_at or datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for selector_row in selector_frame.iter_rows(named=True):
        tenant_id = str(selector_row["tenant_id"])
        source_model_name = str(selector_row["source_model_name"])
        selected_family = str(selector_row["selected_candidate_family"])
        for family in (CANDIDATE_FAMILY_STRICT, CANDIDATE_FAMILY_RAW):
            family_rows = [
                row
                for row in candidate_panel_frame.iter_rows(named=True)
                if row["tenant_id"] == tenant_id
                and row["source_model_name"] == source_model_name
                and row["candidate_family"] == family
            ]
            if not family_rows:
                raise ValueError(f"missing selector strict rows for {tenant_id}/{source_model_name}/{family}")
            for row in family_rows:
                rows.append(
                    _strict_benchmark_row(
                        row,
                        selected_family=selected_family,
                        source_model_name=source_model_name,
                        generated_at=resolved_generated_at,
                        as_selector=False,
                    )
                )
        selected_rows = [
            row
            for row in candidate_panel_frame.iter_rows(named=True)
            if row["tenant_id"] == tenant_id
            and row["source_model_name"] == source_model_name
            and row["candidate_family"] == selected_family
        ]
        if not selected_rows:
            raise ValueError(f"missing selector strict rows for {tenant_id}/{source_model_name}/{selected_family}")
        for row in selected_rows:
            rows.append(
                _strict_benchmark_row(
                    row,
                    selected_family=selected_family,
                    source_model_name=source_model_name,
                    generated_at=resolved_generated_at,
                    as_selector=True,
                )
            )
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name", "anchor_timestamp", "forecast_model_name"])


def evaluate_dfl_trajectory_value_selector_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate selector development evidence and strict production promotion readiness."""

    _require_columns(strict_frame, REQUIRED_EVALUATION_COLUMNS, frame_name="strict_frame")
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(False, "blocked", "trajectory/value selector strict frame has no rows", {})
    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    for source_model_name in source_names:
        source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
        if not source_rows:
            failures.append(f"{source_model_name} has no trajectory/value selector rows")
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
        return PromotionGateResult(True, "promote", "trajectory/value selector passes strict LP/oracle gate", metrics)
    if development_passing:
        description = (
            "trajectory/value selector improves over raw neural schedules but remains blocked versus "
            f"{control_model_name}: " + "; ".join(failures)
        )
        return PromotionGateResult(False, "diagnostic_pass_production_blocked", description, metrics)
    description = "; ".join(failures) if failures else "trajectory/value selector has no development improvement"
    return PromotionGateResult(False, "blocked", description, metrics)


def _candidate_panel_row(
    row: dict[str, Any],
    *,
    tenant_id: str,
    source_model_name: str,
    candidate_family: str,
    prior_score: tuple[float, str],
) -> dict[str, Any]:
    payload = _payload(row)
    horizon = _horizon_rows(payload)
    horizon_hours = int(row["horizon_hours"])
    if len(horizon) != horizon_hours:
        raise ValueError(
            f"vector length must match horizon_hours for {tenant_id}/{source_model_name}/{candidate_family}; "
            f"observed {len(horizon)} vs {horizon_hours}"
        )
    data_quality_tier = str(payload.get("data_quality_tier", "demo_grade"))
    observed_coverage_ratio = float(payload.get("observed_coverage_ratio", 0.0))
    safety_violation_count = _safety_violation_count(payload)
    if data_quality_tier != "thesis_grade":
        raise ValueError("trajectory/value candidate panel requires thesis_grade strict rows")
    if observed_coverage_ratio < 1.0:
        raise ValueError("trajectory/value candidate panel requires observed coverage ratio of 1.0")
    if safety_violation_count:
        raise ValueError(f"trajectory/value candidate panel requires zero safety violations; observed {safety_violation_count}")
    if not bool(payload.get("not_full_dfl", False)):
        raise ValueError("trajectory/value candidate panel requires not_full_dfl=true")
    if not bool(payload.get("not_market_execution", False)):
        raise ValueError("trajectory/value candidate panel requires not_market_execution=true")
    prior_mean, metric_source = prior_score
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": str(row["forecast_model_name"]),
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        "split_name": "final_holdout",
        "horizon_hours": horizon_hours,
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": str(row["committed_action"]),
        "committed_power_mw": float(row["committed_power_mw"]),
        "rank_by_regret": int(row["rank_by_regret"]),
        "dispatch_mw_vector": _float_vector(horizon, keys=("net_power_mw", "signed_dispatch_mw")),
        "soc_fraction_vector": _float_vector(horizon, keys=("soc_fraction", "projected_soc_fraction"), default=0.0),
        "forecast_price_uah_mwh_vector": _float_vector(horizon, keys=("forecast_price_uah_mwh",)),
        "actual_price_uah_mwh_vector": _float_vector(horizon, keys=("actual_price_uah_mwh",)),
        "prior_selection_mean_regret_uah": prior_mean,
        "selection_metric_source": metric_source,
        "data_quality_tier": data_quality_tier,
        "observed_coverage_ratio": observed_coverage_ratio,
        "safety_violation_count": safety_violation_count,
        "claim_scope": TRAJECTORY_VALUE_CANDIDATE_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_venue": str(row["market_venue"]),
        "generated_at": row["generated_at"],
        "starting_soc_fraction": float(row["starting_soc_fraction"]),
        "starting_soc_source": str(row["starting_soc_source"]),
        "evaluation_id": str(row["evaluation_id"]),
        "evaluation_payload": payload,
    }


def _selector_row(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
    selected_family: str,
    family_scores: dict[str, float],
    min_final_holdout_tenant_anchor_count_per_source_model: int,
) -> dict[str, Any]:
    final_rows_by_family = {
        family: [row for row in rows if row["candidate_family"] == family and row["split_name"] == "final_holdout"]
        for family in CANDIDATE_FAMILY_ORDER
    }
    for family, family_rows in final_rows_by_family.items():
        if not family_rows:
            raise ValueError(f"missing final-holdout rows for {tenant_id}/{source_model_name}/{family}")
    selected_rows = final_rows_by_family[selected_family]
    strict_rows = final_rows_by_family[CANDIDATE_FAMILY_STRICT]
    raw_rows = final_rows_by_family[CANDIDATE_FAMILY_RAW]
    selected_mean = _mean_regret(selected_rows)
    strict_mean = _mean_regret(strict_rows)
    raw_mean = _mean_regret(raw_rows)
    selected_median = _median_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    raw_median = _median_regret(raw_rows)
    final_count = len(_tenant_anchor_set(selected_rows))
    development_improvement = _improvement_ratio(raw_mean, selected_mean)
    production_improvement = _improvement_ratio(strict_mean, selected_mean)
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "selected_candidate_family": selected_family,
        "selected_candidate_model_name": _candidate_model_name(source_model_name, selected_family),
        "selector_model_name": trajectory_value_selector_model_name(source_model_name),
        "selected_prior_selection_mean_regret_uah": family_scores[selected_family],
        "candidate_family_prior_scores": family_scores,
        "final_holdout_tenant_anchor_count": final_count,
        "min_required_final_holdout_tenant_anchor_count": (
            min_final_holdout_tenant_anchor_count_per_source_model
        ),
        "first_final_holdout_anchor_timestamp": min(
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") for row in selected_rows
        ),
        "last_final_holdout_anchor_timestamp": max(
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") for row in selected_rows
        ),
        "strict_control_final_mean_regret_uah": strict_mean,
        "strict_control_final_median_regret_uah": strict_median,
        "raw_source_final_mean_regret_uah": raw_mean,
        "raw_source_final_median_regret_uah": raw_median,
        "selected_final_mean_regret_uah": selected_mean,
        "selected_final_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": development_improvement,
        "mean_regret_improvement_ratio_vs_strict": production_improvement,
        "development_gate_passed": final_count >= min_final_holdout_tenant_anchor_count_per_source_model
        and development_improvement > 0.0,
        "production_promotion_passed": final_count >= min_final_holdout_tenant_anchor_count_per_source_model
        and production_improvement >= DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
        and selected_median <= strict_median,
        "claim_scope": TRAJECTORY_VALUE_SELECTOR_CLAIM_SCOPE,
        "academic_scope": TRAJECTORY_VALUE_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    selected_family: str,
    source_model_name: str,
    generated_at: datetime,
    as_selector: bool,
) -> dict[str, Any]:
    family = str(row["candidate_family"])
    forecast_model_name = (
        trajectory_value_selector_model_name(source_model_name)
        if as_selector
        else str(row["candidate_model_name"])
    )
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    payload = dict(_payload(row))
    payload.update(
        {
            "strict_gate_kind": "dfl_trajectory_value_selector_strict_lp",
            "source_forecast_model_name": source_model_name,
            "trajectory_value_selector_model_name": trajectory_value_selector_model_name(source_model_name),
            "trajectory_value_selected_candidate_family": selected_family,
            "trajectory_value_row_candidate_family": family,
            "trajectory_value_row_candidate_model_name": str(row["candidate_model_name"]),
            "trajectory_value_row_role": "selector" if as_selector else "reference",
            "trajectory_value_prior_selection_mean_regret_uah": float(
                row["prior_selection_mean_regret_uah"]
            ),
            "claim_scope": TRAJECTORY_VALUE_SELECTOR_STRICT_CLAIM_SCOPE,
            "academic_scope": TRAJECTORY_VALUE_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:trajectory-value-selector:{source_model_name}:"
            f"{'selector' if as_selector else 'reference'}:{family}:"
            f"{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": TRAJECTORY_VALUE_SELECTOR_STRICT_LP_STRATEGY_KIND,
        "market_venue": str(row["market_venue"]),
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": float(row["starting_soc_fraction"]),
        "starting_soc_source": str(row["starting_soc_source"]),
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": str(row["committed_action"]),
        "committed_power_mw": float(row["committed_power_mw"]),
        "rank_by_regret": int(row["rank_by_regret"]),
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
    selected_model_name = trajectory_value_selector_model_name(source_model_name)
    strict_rows = [row for row in rows if row["forecast_model_name"] == control_model_name]
    raw_rows = [row for row in rows if row["forecast_model_name"] == source_model_name]
    selected_rows = [row for row in rows if row["forecast_model_name"] == selected_model_name]
    strict_anchors = _tenant_anchor_set(strict_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    selected_anchors = _tenant_anchor_set(selected_rows)
    anchor_sets_match = strict_anchors == raw_anchors == selected_anchors
    tenant_count = len({tenant_id for tenant_id, _ in selected_anchors})
    validation_count = len(selected_anchors)
    if tenant_count < min_tenant_count:
        failures.append(f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    if not anchor_sets_match:
        failures.append(f"{source_model_name} strict/raw/selector rows must cover matching tenant-anchor sets")
    failures.extend(_provenance_failures([*strict_rows, *raw_rows, *selected_rows]))
    strict_mean = _mean_regret(strict_rows)
    raw_mean = _mean_regret(raw_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    raw_median = _median_regret(raw_rows)
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
            f"observed selector={selected_median:.2f}, strict={strict_median:.2f}"
        )
    summary = {
        "source_model_name": source_model_name,
        "selector_model_name": selected_model_name,
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "raw_median_regret_uah": raw_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "development_gate_passed": development_passed,
        "production_gate_passed": production_passed,
        "failures": failures,
    }
    return summary, failures


def _prior_scores_by_family(
    benchmark_frame: pl.DataFrame,
    *,
    panel_row: dict[str, Any],
    decision_row: dict[str, Any],
    action_row: dict[str, Any],
    tenant_id: str,
    source_model_name: str,
    max_train_anchors_per_tenant: int,
) -> dict[str, tuple[float, str]]:
    first_final_holdout_anchor = _datetime_value(
        panel_row["first_final_holdout_anchor_timestamp"],
        field_name="first_final_holdout_anchor_timestamp",
    )
    return {
        CANDIDATE_FAMILY_STRICT: (
            _prior_mean_regret(
                benchmark_frame,
                tenant_id=tenant_id,
                model_name=CONTROL_MODEL_NAME,
                before_anchor=first_final_holdout_anchor,
                max_train_anchors=max_train_anchors_per_tenant,
            ),
            "strict_control_train_selection_mean_regret",
        ),
        CANDIDATE_FAMILY_RAW: (
            _prior_mean_regret(
                benchmark_frame,
                tenant_id=tenant_id,
                model_name=source_model_name,
                before_anchor=first_final_holdout_anchor,
                max_train_anchors=max_train_anchors_per_tenant,
            ),
            "raw_source_train_selection_mean_regret",
        ),
        CANDIDATE_FAMILY_PANEL_V2: (
            float(panel_row.get("v2_inner_selection_relaxed_regret_uah", 0.0)),
            "panel_v2_inner_selection_relaxed_regret",
        ),
        CANDIDATE_FAMILY_DECISION_V3: (
            float(decision_row.get("inner_selection_mean_regret_uah", 0.0)),
            "decision_target_v3_inner_selection_mean_regret",
        ),
        CANDIDATE_FAMILY_ACTION_V4: (
            float(action_row.get("inner_selection_mean_regret_uah", 0.0)),
            "action_target_v4_inner_selection_mean_regret",
        ),
    }


def _prior_mean_regret(
    frame: pl.DataFrame,
    *,
    tenant_id: str,
    model_name: str,
    before_anchor: datetime,
    max_train_anchors: int,
) -> float:
    rows = (
        frame
        .filter(
            (pl.col("tenant_id") == tenant_id)
            & (pl.col("forecast_model_name") == model_name)
            & (pl.col("anchor_timestamp") < before_anchor)
        )
        .sort("anchor_timestamp")
        .tail(max_train_anchors)
        .iter_rows(named=True)
    )
    by_anchor: dict[datetime, float] = {}
    for row in rows:
        by_anchor.setdefault(_datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"), float(row["regret_uah"]))
    if not by_anchor:
        raise ValueError(f"missing prior train-selection rows for {tenant_id}/{model_name}")
    return mean(by_anchor.values())


def _validate_strict_sidecar_coverage(
    strict_frame: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    expected_families: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
    frame_name: str,
) -> None:
    _require_columns(strict_frame, REQUIRED_EVALUATION_COLUMNS, frame_name=frame_name)
    for family in expected_families:
        _strict_family_rows(
            strict_frame,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
            candidate_family=family,
            final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
            frame_name=frame_name,
        )


def _strict_family_rows(
    strict_frame: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    candidate_family: str,
    final_validation_anchor_count_per_tenant: int,
    frame_name: str,
) -> list[dict[str, Any]]:
    model_name = _candidate_model_name(source_model_name, candidate_family)
    rows = [
        row
        for row in strict_frame.iter_rows(named=True)
        if row["tenant_id"] == tenant_id
        and row["forecast_model_name"] == model_name
        and _source_model_name(row) == source_model_name
    ]
    if len(rows) != final_validation_anchor_count_per_tenant:
        raise ValueError(
            f"{frame_name} missing final-holdout rows for {tenant_id}/{source_model_name}/{model_name}; "
            f"observed {len(rows)}, expected {final_validation_anchor_count_per_tenant}"
        )
    if len(_anchor_set(rows)) != final_validation_anchor_count_per_tenant:
        raise ValueError(f"{frame_name} final-holdout rows must have unique anchors for {tenant_id}/{model_name}")
    return sorted(rows, key=lambda row: _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"))


def _single_panel_row(
    frame: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    frame_name: str,
) -> dict[str, Any]:
    rows = frame.filter((pl.col("tenant_id") == tenant_id) & (pl.col("forecast_model_name") == source_model_name))
    if rows.height == 0:
        raise ValueError(f"missing {frame_name} row for {tenant_id}/{source_model_name}")
    if rows.height > 1:
        raise ValueError(f"duplicate {frame_name} rows for {tenant_id}/{source_model_name}")
    return rows.row(0, named=True)


def _validate_panel_rows_align(
    rows: list[dict[str, Any]],
    *,
    final_validation_anchor_count_per_tenant: int,
) -> None:
    first_anchor = _datetime_value(
        rows[0]["first_final_holdout_anchor_timestamp"],
        field_name="first_final_holdout_anchor_timestamp",
    )
    last_anchor = _datetime_value(
        rows[0]["last_final_holdout_anchor_timestamp"],
        field_name="last_final_holdout_anchor_timestamp",
    )
    for row in rows:
        if int(row["final_validation_anchor_count"]) != final_validation_anchor_count_per_tenant:
            raise ValueError(
                "trajectory/value panel final_validation_anchor_count must match config; "
                f"observed {row['final_validation_anchor_count']}, expected {final_validation_anchor_count_per_tenant}"
            )
        if _datetime_value(
            row["first_final_holdout_anchor_timestamp"],
            field_name="first_final_holdout_anchor_timestamp",
        ) != first_anchor or _datetime_value(
            row["last_final_holdout_anchor_timestamp"],
            field_name="last_final_holdout_anchor_timestamp",
        ) != last_anchor:
            raise ValueError("trajectory/value panel rows must share the same final-holdout window")
        if str(row["data_quality_tier"]) != "thesis_grade":
            raise ValueError("trajectory/value panel requires thesis_grade panel rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("trajectory/value panel requires observed coverage ratio of 1.0")
        if not bool(row["not_full_dfl"]):
            raise ValueError("trajectory/value panel requires not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("trajectory/value panel requires not_market_execution=true")


def _family_prior_scores(rows: list[dict[str, Any]]) -> dict[str, float]:
    scores: dict[str, float] = {}
    for family in CANDIDATE_FAMILY_ORDER:
        family_scores = {
            float(row["prior_selection_mean_regret_uah"])
            for row in rows
            if row["candidate_family"] == family
        }
        if len(family_scores) != 1:
            raise ValueError(f"candidate family {family} must have exactly one prior score")
        scores[family] = family_scores.pop()
    return scores


def _candidate_model_name(source_model_name: str, candidate_family: str) -> str:
    if candidate_family == CANDIDATE_FAMILY_STRICT:
        return CONTROL_MODEL_NAME
    if candidate_family == CANDIDATE_FAMILY_RAW:
        return source_model_name
    if candidate_family == CANDIDATE_FAMILY_PANEL_V2:
        return panel_v2_model_name(source_model_name)
    if candidate_family == CANDIDATE_FAMILY_DECISION_V3:
        return decision_target_model_name(source_model_name)
    if candidate_family == CANDIDATE_FAMILY_ACTION_V4:
        return action_target_model_name(source_model_name)
    raise ValueError(f"unknown candidate family: {candidate_family}")


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("evaluation_payload")
    if not isinstance(value, dict):
        raise ValueError("evaluation_payload must be a dict")
    return value


def _horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list) or not horizon:
        raise ValueError("evaluation_payload.horizon must be a non-empty list")
    if not all(isinstance(point, dict) for point in horizon):
        raise ValueError("evaluation_payload.horizon must contain dict rows")
    return horizon


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


def _source_model_name(row: dict[str, Any]) -> str:
    if "source_model_name" in row and row["source_model_name"]:
        return str(row["source_model_name"])
    payload = _payload(row)
    value = payload.get("source_forecast_model_name")
    return str(value or "")


def _safety_violation_count(payload: dict[str, Any]) -> int:
    if "safety_violation_count" in payload:
        return int(payload["safety_violation_count"])
    safety_violations = payload.get("safety_violations")
    if isinstance(safety_violations, list):
        return len(safety_violations)
    return 0


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO datetime.") from exc
    raise ValueError(f"{field_name} must be a datetime.")


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
        failures.append("trajectory/value promotion requires thesis_grade evidence")
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        failures.append("trajectory/value promotion requires observed coverage ratio of 1.0")
    safety_violation_count = sum(_safety_violation_count(payload) for payload in payloads)
    if safety_violation_count:
        failures.append(f"trajectory/value promotion requires zero safety violations; observed {safety_violation_count}")
    if any(not bool(payload.get("not_full_dfl", False)) for payload in payloads):
        failures.append("trajectory/value promotion evidence must remain not_full_dfl")
    if any(not bool(payload.get("not_market_execution", False)) for payload in payloads):
        failures.append("trajectory/value promotion evidence must remain not_market_execution")
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
