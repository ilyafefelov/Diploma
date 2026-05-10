"""Feature-aware prior-only selector for strict-control failure opportunities."""

from __future__ import annotations

from datetime import datetime
from statistics import mean, median
from typing import Any, Final, NamedTuple

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.strict_failure_features import (
    REQUIRED_PRIOR_FEATURE_PANEL_COLUMNS,
    _datetime_value,
)
from smart_arbitrage.dfl.strict_failure_selector import (
    CANDIDATE_FAMILY_RAW,
    CANDIDATE_FAMILY_STRICT,
    REQUIRED_EVALUATION_COLUMNS,
    REQUIRED_LIBRARY_COLUMNS,
    _committed_action,
    _family_sort_index,
    _first_or_default,
    _float_list,
    _improvement_ratio,
    _mean_regret,
    _median_regret,
    _missing_column_failures,
    _payload,
    _provenance_failures,
    _require_columns,
    _tenant_anchor_set,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_CLAIM_SCOPE: Final[str] = (
    "dfl_feature_aware_strict_failure_selector_v2_not_full_dfl"
)
DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_feature_aware_strict_failure_selector_v2_strict_lp_gate_not_full_dfl"
)
DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_feature_aware_strict_failure_selector_strict_lp_benchmark"
)
DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_PREFIX: Final[str] = (
    "dfl_feature_aware_strict_failure_selector_v2_"
)
DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_ACADEMIC_SCOPE: Final[str] = (
    "Feature-aware prior-only selector over strict-failure audit features. "
    "It is not full DFL, not Decision Transformer control, and not market execution."
)

REQUIRED_FEATURE_SELECTOR_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "selector_model_name",
        "final_window_index",
        "training_window_indices",
        "selected_rule_name",
        "selected_switch_threshold_uah",
        "selected_rank_overlap_floor",
        "selected_price_regime_policy",
        "selected_volatility_policy",
        "train_window_count",
        "train_selection_anchor_count",
        "final_holdout_anchor_count",
        "final_holdout_tenant_anchor_count",
        "selected_train_mean_regret_uah",
        "strict_train_mean_regret_uah",
        "raw_train_mean_regret_uah",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)

DEFAULT_FEATURE_SWITCH_THRESHOLD_GRID_UAH: Final[tuple[float, ...]] = (
    0.0,
    50.0,
    100.0,
    200.0,
    400.0,
)
DEFAULT_RANK_OVERLAP_FLOOR_GRID: Final[tuple[float, ...]] = (0.0, 0.5, 0.75)
DEFAULT_PRICE_REGIME_POLICIES: Final[tuple[str, ...]] = ("all", "low_medium", "high_only")
DEFAULT_VOLATILITY_POLICIES: Final[tuple[str, ...]] = ("all", "non_volatile")


class _Rule(NamedTuple):
    switch_threshold_uah: float
    rank_overlap_floor: float
    price_regime_policy: str
    volatility_policy: str

    @property
    def name(self) -> str:
        rank_floor = f"{self.rank_overlap_floor:.2f}".replace(".", "p")
        threshold = f"{self.switch_threshold_uah:.0f}"
        return (
            f"threshold_{threshold}_rank_{rank_floor}_price_"
            f"{self.price_regime_policy}_vol_{self.volatility_policy}"
        )


def feature_aware_strict_failure_selector_model_name(source_model_name: str) -> str:
    """Return the stable feature-aware selector model name for a source model."""

    return f"{DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_PREFIX}{source_model_name}"


def build_dfl_feature_aware_strict_failure_selector_frame(
    prior_feature_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_window_index: int = 1,
    min_training_window_count: int = 3,
    switch_threshold_grid_uah: tuple[float, ...] = DEFAULT_FEATURE_SWITCH_THRESHOLD_GRID_UAH,
    rank_overlap_floor_grid: tuple[float, ...] = DEFAULT_RANK_OVERLAP_FLOOR_GRID,
    price_regime_policies: tuple[str, ...] = DEFAULT_PRICE_REGIME_POLICIES,
    volatility_policies: tuple[str, ...] = DEFAULT_VOLATILITY_POLICIES,
) -> pl.DataFrame:
    """Select a deterministic feature-aware rule from earlier rolling windows only."""

    _validate_selector_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_window_index=final_window_index,
        min_training_window_count=min_training_window_count,
        switch_threshold_grid_uah=switch_threshold_grid_uah,
        rank_overlap_floor_grid=rank_overlap_floor_grid,
        price_regime_policies=price_regime_policies,
        volatility_policies=volatility_policies,
    )
    _validate_feature_panel_frame(prior_feature_panel_frame)
    panel_rows = list(prior_feature_panel_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    rules = _candidate_rules(
        switch_threshold_grid_uah=switch_threshold_grid_uah,
        rank_overlap_floor_grid=rank_overlap_floor_grid,
        price_regime_policies=price_regime_policies,
        volatility_policies=volatility_policies,
    )
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            scoped_rows = _panel_rows(
                panel_rows,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            train_rows = [
                row
                for row in scoped_rows
                if int(row["window_index"]) > final_window_index
            ]
            final_rows = [
                row
                for row in scoped_rows
                if int(row["window_index"]) == final_window_index
            ]
            train_windows = sorted({int(row["window_index"]) for row in train_rows})
            if len(train_windows) < min_training_window_count:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} feature-aware selector needs at least "
                    f"{min_training_window_count} prior training windows"
                )
            if not final_rows:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} feature-aware selector is missing final window rows"
                )
            selected_rule = _select_rule(train_rows, rules)
            selected_train_regrets = [_selected_regret(row, selected_rule) for row in train_rows]
            selected_final_regrets = [_selected_regret(row, selected_rule) for row in final_rows]
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "selector_model_name": feature_aware_strict_failure_selector_model_name(
                        source_model_name
                    ),
                    "final_window_index": final_window_index,
                    "training_window_indices": tuple(train_windows),
                    "selected_rule_name": selected_rule.name,
                    "selected_switch_threshold_uah": selected_rule.switch_threshold_uah,
                    "selected_rank_overlap_floor": selected_rule.rank_overlap_floor,
                    "selected_price_regime_policy": selected_rule.price_regime_policy,
                    "selected_volatility_policy": selected_rule.volatility_policy,
                    "train_window_count": len(train_windows),
                    "train_selection_anchor_count": len(train_rows),
                    "final_holdout_anchor_count": len(final_rows),
                    "final_holdout_tenant_anchor_count": len(final_rows) * len(tenant_ids),
                    "train_switch_count": sum(1 for row in train_rows if _rule_fires(row, selected_rule)),
                    "final_switch_count": sum(1 for row in final_rows if _rule_fires(row, selected_rule)),
                    "strict_train_mean_regret_uah": _mean_field(
                        train_rows,
                        "analysis_only_strict_regret_uah",
                    ),
                    "raw_train_mean_regret_uah": _mean_field(
                        train_rows,
                        "analysis_only_raw_regret_uah",
                    ),
                    "best_non_strict_train_mean_regret_uah": _mean_field(
                        train_rows,
                        "analysis_only_best_non_strict_regret_uah",
                    ),
                    "selected_train_mean_regret_uah": mean(selected_train_regrets),
                    "selected_train_median_regret_uah": median(selected_train_regrets),
                    "selected_final_mean_regret_uah": mean(selected_final_regrets),
                    "selected_final_median_regret_uah": median(selected_final_regrets),
                    "claim_scope": DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_CLAIM_SCOPE,
                    "academic_scope": DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name"])


def build_dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    feature_aware_selector_frame: pl.DataFrame,
    prior_feature_panel_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict LP/oracle evidence rows for the feature-aware selector."""

    _validate_library_frame(schedule_candidate_library_frame)
    _validate_feature_panel_frame(prior_feature_panel_frame)
    _validate_feature_selector_frame(feature_aware_selector_frame)
    resolved_generated_at = generated_at or _latest_generated_at(schedule_candidate_library_frame)
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    panel_rows = list(prior_feature_panel_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    for selector_row in feature_aware_selector_frame.iter_rows(named=True):
        tenant_id = str(selector_row["tenant_id"])
        source_model_name = str(selector_row["source_model_name"])
        rule = _rule_from_selector_row(selector_row)
        source_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
        ]
        final_rows = [
            row
            for row in panel_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and int(row["window_index"]) == int(selector_row["final_window_index"])
        ]
        if not final_rows:
            raise ValueError(f"missing feature-aware final rows for {tenant_id}/{source_model_name}")
        for feature_row in sorted(
            final_rows,
            key=lambda row: _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        ):
            anchor_timestamp = _datetime_value(feature_row["anchor_timestamp"], field_name="anchor_timestamp")
            decision = _decision_for_feature_row(source_rows, feature_row)
            selected_row = decision["best_prior_non_strict_row"] if _rule_fires(feature_row, rule) else decision["strict_row"]
            rows.append(
                _strict_benchmark_row(
                    decision["strict_row"],
                    source_model_name=source_model_name,
                    selector_row=selector_row,
                    feature_row=feature_row,
                    role="strict_reference",
                    generated_at=resolved_generated_at,
                )
            )
            rows.append(
                _strict_benchmark_row(
                    decision["raw_row"],
                    source_model_name=source_model_name,
                    selector_row=selector_row,
                    feature_row=feature_row,
                    role="raw_reference",
                    generated_at=resolved_generated_at,
                )
            )
            rows.append(
                _strict_benchmark_row(
                    decision["best_prior_non_strict_row"],
                    source_model_name=source_model_name,
                    selector_row=selector_row,
                    feature_row=feature_row,
                    role="best_prior_non_strict_reference",
                    generated_at=resolved_generated_at,
                )
            )
            rows.append(
                _strict_benchmark_row(
                    selected_row,
                    source_model_name=source_model_name,
                    selector_row=selector_row,
                    feature_row=feature_row,
                    role="selector",
                    generated_at=resolved_generated_at,
                )
            )
            if anchor_timestamp != _datetime_value(selected_row["anchor_timestamp"], field_name="anchor_timestamp"):
                raise ValueError("feature-aware selector emitted mismatched anchor rows")
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name", "anchor_timestamp", "forecast_model_name"])


def validate_dfl_feature_aware_strict_failure_selector_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate coverage and claim boundaries for feature-aware selector evidence."""

    failures = _missing_column_failures(strict_frame, REQUIRED_EVALUATION_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": strict_frame.height})
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "feature-aware selector evidence has no rows", {"row_count": 0})
    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    summaries: list[dict[str, Any]] = []
    for source_model_name in source_names:
        summary, summary_failures = _selector_gate_summary(
            rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
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
        passed=not failures,
        description=(
            "Feature-aware strict-failure selector evidence has valid coverage and provenance."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def evaluate_dfl_feature_aware_strict_failure_selector_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate feature-aware selector evidence while keeping promotion blocked."""

    failures = _missing_column_failures(strict_frame, REQUIRED_EVALUATION_COLUMNS)
    if failures:
        return PromotionGateResult(False, "blocked", "; ".join(failures), {})
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(False, "blocked", "feature-aware selector strict frame has no rows", {})
    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    summaries: list[dict[str, Any]] = []
    all_failures: list[str] = []
    for source_model_name in source_names:
        summary, summary_failures = _selector_gate_summary(
            rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
            include_promotion_failures=True,
        )
        summaries.append(summary)
        all_failures.extend(summary_failures)
    development_passing = [summary for summary in summaries if summary["development_gate_passed"]]
    production_passing = [summary for summary in summaries if summary["production_gate_passed"]]
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
        "production_gate_passed": False,
        "strict_challenger_source_model_names": [
            str(summary["source_model_name"]) for summary in production_passing
        ],
        "model_summaries": summaries,
    }
    if production_passing and not all_failures:
        return PromotionGateResult(
            False,
            "research_challenger_production_blocked",
            "feature-aware selector clears the strict diagnostic threshold but remains research-only in this slice",
            metrics,
        )
    if development_passing:
        return PromotionGateResult(
            False,
            "diagnostic_pass_production_blocked",
            "feature-aware selector improves over raw neural schedules but remains blocked versus "
            f"{control_model_name}: " + "; ".join(all_failures),
            metrics,
        )
    description = "; ".join(all_failures) if all_failures else "feature-aware selector has no development improvement"
    return PromotionGateResult(False, "blocked", description, metrics)


def _validate_selector_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_window_index: int,
    min_training_window_count: int,
    switch_threshold_grid_uah: tuple[float, ...],
    rank_overlap_floor_grid: tuple[float, ...],
    price_regime_policies: tuple[str, ...],
    volatility_policies: tuple[str, ...],
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_window_index < 1:
        raise ValueError("final_window_index must be at least 1.")
    if min_training_window_count < 1:
        raise ValueError("min_training_window_count must be at least 1.")
    if not switch_threshold_grid_uah:
        raise ValueError("switch_threshold_grid_uah must contain at least one threshold.")
    if not rank_overlap_floor_grid:
        raise ValueError("rank_overlap_floor_grid must contain at least one threshold.")
    if not price_regime_policies:
        raise ValueError("price_regime_policies must contain at least one policy.")
    if not volatility_policies:
        raise ValueError("volatility_policies must contain at least one policy.")


def _validate_feature_panel_frame(frame: pl.DataFrame) -> None:
    required = REQUIRED_PRIOR_FEATURE_PANEL_COLUMNS.union(
        {
            "selector_feature_prior_strict_minus_best_non_strict_uah",
            "selector_feature_prior_top_rank_overlap_mean",
            "selector_feature_prior_bottom_rank_overlap_mean",
            "selector_feature_prior_price_regime",
            "selector_feature_prior_spread_volatility_regime",
            "analysis_only_best_non_strict_regret_uah",
        }
    )
    _require_columns(frame, required, frame_name="dfl_strict_failure_prior_feature_panel_frame")
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != "dfl_strict_failure_prior_feature_panel_not_full_dfl":
            raise ValueError("feature-aware selector requires strict-failure prior feature panel rows")
        if not bool(row["not_full_dfl"]):
            raise ValueError("feature-aware selector requires not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("feature-aware selector requires not_market_execution=true")


def _validate_feature_selector_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_FEATURE_SELECTOR_COLUMNS, frame_name="feature_aware_selector_frame")
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_CLAIM_SCOPE:
            raise ValueError("feature-aware selector frame has an unexpected claim_scope")
        if not bool(row["not_full_dfl"]):
            raise ValueError("feature-aware selector rows must remain not_full_dfl")
        if not bool(row["not_market_execution"]):
            raise ValueError("feature-aware selector rows must remain not_market_execution")


def _validate_library_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_LIBRARY_COLUMNS, frame_name="dfl_schedule_candidate_library_v2_frame")
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
            raise ValueError("feature-aware selector requires thesis_grade rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("feature-aware selector requires observed coverage ratio of 1.0")
        if int(row["safety_violation_count"]):
            raise ValueError("feature-aware selector requires zero safety violations")
        if not bool(row["not_full_dfl"]):
            raise ValueError("feature-aware selector requires not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("feature-aware selector requires not_market_execution=true")


def _candidate_rules(
    *,
    switch_threshold_grid_uah: tuple[float, ...],
    rank_overlap_floor_grid: tuple[float, ...],
    price_regime_policies: tuple[str, ...],
    volatility_policies: tuple[str, ...],
) -> list[_Rule]:
    return [
        _Rule(
            switch_threshold_uah=switch_threshold,
            rank_overlap_floor=rank_floor,
            price_regime_policy=price_policy,
            volatility_policy=vol_policy,
        )
        for switch_threshold in switch_threshold_grid_uah
        for rank_floor in rank_overlap_floor_grid
        for price_policy in price_regime_policies
        for vol_policy in volatility_policies
    ]


def _select_rule(rows: list[dict[str, Any]], rules: list[_Rule]) -> _Rule:
    return min(
        rules,
        key=lambda rule: (
            mean(_selected_regret(row, rule) for row in rows),
            -sum(1 for row in rows if _rule_fires(row, rule)),
            rule.switch_threshold_uah,
            rule.rank_overlap_floor,
            rule.price_regime_policy,
            rule.volatility_policy,
        ),
    )


def _rule_fires(row: dict[str, Any], rule: _Rule) -> bool:
    prior_advantage = float(row["selector_feature_prior_strict_minus_best_non_strict_uah"])
    top_overlap = float(row["selector_feature_prior_top_rank_overlap_mean"])
    bottom_overlap = float(row["selector_feature_prior_bottom_rank_overlap_mean"])
    return (
        prior_advantage >= rule.switch_threshold_uah
        and min(top_overlap, bottom_overlap) >= rule.rank_overlap_floor
        and _price_regime_allowed(str(row["selector_feature_prior_price_regime"]), rule.price_regime_policy)
        and _volatility_allowed(
            str(row["selector_feature_prior_spread_volatility_regime"]),
            rule.volatility_policy,
        )
    )


def _selected_regret(row: dict[str, Any], rule: _Rule) -> float:
    if _rule_fires(row, rule):
        return float(row["analysis_only_best_non_strict_regret_uah"])
    return float(row["analysis_only_strict_regret_uah"])


def _price_regime_allowed(price_regime: str, policy: str) -> bool:
    if policy == "all":
        return True
    if policy == "low_medium":
        return price_regime in {"low_spread", "medium_spread"}
    if policy == "high_only":
        return price_regime == "high_spread"
    raise ValueError(f"unknown price regime policy: {policy}")


def _volatility_allowed(volatility_regime: str, policy: str) -> bool:
    if policy == "all":
        return True
    if policy == "non_volatile":
        return volatility_regime != "volatile"
    raise ValueError(f"unknown volatility policy: {policy}")


def _rule_from_selector_row(selector_row: dict[str, Any]) -> _Rule:
    return _Rule(
        switch_threshold_uah=float(selector_row["selected_switch_threshold_uah"]),
        rank_overlap_floor=float(selector_row["selected_rank_overlap_floor"]),
        price_regime_policy=str(selector_row["selected_price_regime_policy"]),
        volatility_policy=str(selector_row["selected_volatility_policy"]),
    )


def _panel_rows(rows: list[dict[str, Any]], *, tenant_id: str, source_model_name: str) -> list[dict[str, Any]]:
    matches = [
        row
        for row in rows
        if str(row["tenant_id"]) == tenant_id and str(row["source_model_name"]) == source_model_name
    ]
    if not matches:
        raise ValueError(f"missing feature panel rows for {tenant_id}/{source_model_name}")
    return matches


def _decision_for_feature_row(
    source_rows: list[dict[str, Any]],
    feature_row: dict[str, Any],
) -> dict[str, Any]:
    anchor_timestamp = _datetime_value(feature_row["anchor_timestamp"], field_name="anchor_timestamp")
    prior_cutoff = _datetime_value(feature_row["prior_cutoff_timestamp"], field_name="prior_cutoff_timestamp")
    anchor_rows = [
        row
        for row in source_rows
        if _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") == anchor_timestamp
    ]
    strict_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
    raw_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_RAW)
    non_strict_candidates: list[dict[str, Any]] = []
    for row in anchor_rows:
        if str(row["candidate_family"]) == CANDIDATE_FAMILY_STRICT:
            continue
        prior_mean = _prior_mean_regret(
            source_rows,
            row,
            prior_cutoff=prior_cutoff,
        )
        if prior_mean is None:
            continue
        candidate = dict(row)
        candidate["selector_prior_mean_regret_uah"] = prior_mean
        non_strict_candidates.append(candidate)
    if not non_strict_candidates:
        raise ValueError("feature-aware selector is missing non-strict prior candidates")
    best_non_strict = min(
        non_strict_candidates,
        key=lambda row: (
            float(row["selector_prior_mean_regret_uah"]),
            _family_sort_index(str(row["candidate_family"])),
            str(row["candidate_model_name"]),
        ),
    )
    return {
        "strict_row": strict_row,
        "raw_row": raw_row,
        "best_prior_non_strict_row": best_non_strict,
    }


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    selector_row: dict[str, Any],
    feature_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(_payload(row))
    selector_model_name = feature_aware_strict_failure_selector_model_name(source_model_name)
    forecast_model_name = selector_model_name if role == "selector" else str(row["candidate_model_name"])
    candidate_family = str(row["candidate_family"])
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    payload.update(
        {
            "strict_gate_kind": "dfl_feature_aware_strict_failure_selector_strict_lp",
            "source_forecast_model_name": source_model_name,
            "selector_model_name": selector_model_name,
            "selected_rule_name": str(selector_row["selected_rule_name"]),
            "selected_switch_threshold_uah": float(selector_row["selected_switch_threshold_uah"]),
            "selected_rank_overlap_floor": float(selector_row["selected_rank_overlap_floor"]),
            "selected_price_regime_policy": str(selector_row["selected_price_regime_policy"]),
            "selected_volatility_policy": str(selector_row["selected_volatility_policy"]),
            "feature_prior_price_regime": str(feature_row["selector_feature_prior_price_regime"]),
            "feature_prior_volatility_regime": str(
                feature_row["selector_feature_prior_spread_volatility_regime"]
            ),
            "selector_row_candidate_family": candidate_family,
            "selector_row_candidate_model_name": str(row["candidate_model_name"]),
            "selector_row_role": role,
            "claim_scope": DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_STRICT_CLAIM_SCOPE,
            "academic_scope": DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": int(row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:feature-aware-strict-failure-selector:{source_model_name}:"
            f"{role}:{candidate_family}:{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND,
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
    include_promotion_failures: bool,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    selected_model_name = feature_aware_strict_failure_selector_model_name(source_model_name)
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [row for row in source_rows if row["forecast_model_name"] == control_model_name]
    raw_rows = [row for row in source_rows if row["forecast_model_name"] == source_model_name]
    selected_rows = [row for row in source_rows if row["forecast_model_name"] == selected_model_name]
    strict_anchors = _tenant_anchor_set(strict_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    selected_anchors = _tenant_anchor_set(selected_rows)
    if strict_anchors != raw_anchors or strict_anchors != selected_anchors:
        failures.append(f"{source_model_name} strict/raw/selector rows must cover matching tenant-anchor sets")
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
                f"{source_model_name} mean regret improvement vs {control_model_name} must be at least "
                f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
            )
        if selected_rows and strict_rows and selected_median > strict_median:
            failures.append(
                f"{source_model_name} median regret must not be worse than {control_model_name}; "
                f"observed selector={selected_median:.2f}, strict={strict_median:.2f}"
            )
    return {
        "source_model_name": source_model_name,
        "selector_model_name": selected_model_name,
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


def _single_family_row(rows: list[dict[str, Any]], candidate_family: str) -> dict[str, Any]:
    matches = [row for row in rows if str(row["candidate_family"]) == candidate_family]
    if not matches:
        raise ValueError(f"missing {candidate_family} row")
    return matches[0]


def _prior_mean_regret(
    rows: list[dict[str, Any]],
    row: dict[str, Any],
    *,
    prior_cutoff: datetime,
) -> float | None:
    prior_rows = [
        candidate
        for candidate in rows
        if str(candidate["split_name"]) == "train_selection"
        and str(candidate["candidate_family"]) == str(row["candidate_family"])
        and str(candidate["candidate_model_name"]) == str(row["candidate_model_name"])
        and _datetime_value(candidate["anchor_timestamp"], field_name="anchor_timestamp") < prior_cutoff
    ]
    return _mean_regret(prior_rows) if prior_rows else None


def _source_model_name(row: dict[str, Any]) -> str:
    if "source_model_name" in row and row["source_model_name"]:
        return str(row["source_model_name"])
    payload = _payload(row)
    return str(payload.get("source_forecast_model_name", ""))


def _mean_field(rows: list[dict[str, Any]], column_name: str) -> float:
    values = [float(row[column_name]) for row in rows]
    return mean(values) if values else 0.0


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    if "generated_at" not in frame.columns or frame.is_empty():
        return datetime.now()
    values = [
        _datetime_value(value, field_name="generated_at")
        for value in frame["generated_at"].to_list()
    ]
    return max(values) if values else datetime.now()
