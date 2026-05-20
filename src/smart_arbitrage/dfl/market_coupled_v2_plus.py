"""Experimental Poland-lagged market-coupled V2+ schedule/value selector."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import schedule_value_learner_v2_plus as v2_plus
from smart_arbitrage.dfl import schedule_value_learner_v2_plus_robustness as v2_plus_robust
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_ACADEMIC_SCOPE,
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE,
)
from smart_arbitrage.forecasting.market_coupling_features import (
    REQUIRED_MARKET_COUPLING_FEATURE_ROUTE_COLUMNS,
)

DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_PREFIX: Final[str] = (
    "dfl_market_coupled_schedule_value_learner_v2_plus_"
)
DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark"
)
DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_CLAIM_SCOPE: Final[str] = (
    "dfl_market_coupled_schedule_value_learner_v2_plus_not_full_dfl"
)
DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_gate_not_full_dfl"
)
DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_ACADEMIC_SCOPE: Final[str] = (
    "Experimental Ukrainian-plus-Poland lagged exogenous feature ablation over "
    "the frozen official global-panel Schedule/Value Learner V2+ candidate "
    "library. It selects schedule families from prior anchors only, keeps "
    "Ukrainian-only V2+ as fallback, and remains Offline Strategy Promotion "
    "evidence only: not full DFL and not market execution."
)
DEFAULT_POLAND_LAGGED_FEATURE_COLUMN: Final[str] = (
    "entsoe_pl_lag24_day_ahead_price_uah_mwh"
)
POLAND_LAG24_DELTA_1H_FEATURE_COLUMN: Final[str] = (
    "entsoe_pl_lag24_delta_1h_uah_mwh"
)
POLAND_LAG24_DELTA_24H_FEATURE_COLUMN: Final[str] = (
    "entsoe_pl_lag24_delta_24h_uah_mwh"
)
POLAND_LAG24_DAILY_SPREAD_FEATURE_COLUMN: Final[str] = (
    "entsoe_pl_lag24_daily_spread_uah_mwh"
)
POLAND_LAG24_DAILY_PRICE_RANK_FEATURE_COLUMN: Final[str] = (
    "entsoe_pl_lag24_daily_price_rank"
)
POLAND_LAG24_DAILY_PEAK_HOUR_FEATURE_COLUMN: Final[str] = (
    "entsoe_pl_lag24_daily_peak_hour_utc"
)
POLAND_LAG24_DAILY_TROUGH_HOUR_FEATURE_COLUMN: Final[str] = (
    "entsoe_pl_lag24_daily_trough_hour_utc"
)
RICH_POLAND_SELECTOR_PROFILE_NAME: Final[str] = (
    "poland_lag24_rich_regime_selector"
)


def market_coupled_schedule_value_learner_v2_plus_model_name(
    source_model_name: str,
) -> str:
    """Return the stable market-coupled V2+ experimental selector name."""

    return f"{DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_PREFIX}{source_model_name}"


def build_dfl_market_coupled_schedule_value_learner_v2_plus_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    learner_v2_frame: pl.DataFrame,
    learner_v2_plus_frame: pl.DataFrame,
    official_forecast_exogenous_feature_route_frame: pl.DataFrame,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int = 18,
    min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus: float = 0.01,
) -> pl.DataFrame:
    """Select schedule families using prior-only lagged Poland context."""

    _validate_selector_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        min_prior_mean_improvement_ratio=min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus,
    )
    v2._validate_library_frame(schedule_candidate_library_frame)
    v2._validate_learner_frame(learner_v2_frame)
    v2_plus._validate_learner_v2_plus_frame(learner_v2_plus_frame)
    approved_columns = _approved_experimental_feature_columns(
        official_forecast_exogenous_feature_route_frame
    )
    if not approved_columns:
        return _empty_model_frame()

    feature_by_timestamp = _feature_values_by_timestamp(
        entsoe_poland_lagged_feature_candidate_frame
    )
    v2_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_frame.iter_rows(named=True)
    }
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
                    f"{tenant_id}/{source_model_name} final-holdout tenant-anchor count "
                    f"must be {final_validation_anchor_count_per_tenant}; "
                    f"observed {final_anchor_count}"
                )
            if not train_rows:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} market-coupled selector needs train rows"
                )
            v2_learner_row = v2_rows.get((tenant_id, source_model_name))
            v2_plus_learner_row = v2_plus_rows.get((tenant_id, source_model_name))
            if v2_learner_row is None:
                raise ValueError(f"missing v2 learner row for {tenant_id}/{source_model_name}")
            if v2_plus_learner_row is None:
                raise ValueError(
                    f"missing Ukrainian-only v2+ learner row for {tenant_id}/{source_model_name}"
                )

            selected_v2_plus_train_rows = _selected_v2_plus_rows(
                train_rows,
                v2_learner_row=v2_learner_row,
                v2_plus_learner_row=v2_plus_learner_row,
            )
            selected_v2_plus_final_rows = _selected_v2_plus_rows(
                final_rows,
                v2_learner_row=v2_learner_row,
                v2_plus_learner_row=v2_plus_learner_row,
            )
            selector = _fit_poland_regime_selector(
                train_rows,
                feature_by_timestamp=feature_by_timestamp,
            )
            market_train_rows = _select_market_coupled_rows(
                train_rows,
                selector=selector,
                feature_by_timestamp=feature_by_timestamp,
            )
            market_final_rows = _select_market_coupled_rows(
                final_rows,
                selector=selector,
                feature_by_timestamp=feature_by_timestamp,
            )
            v2_plus_train_mean = v2._mean_regret(selected_v2_plus_train_rows)
            market_train_mean = v2._mean_regret(market_train_rows)
            v2_plus_train_median = v2._median_regret(selected_v2_plus_train_rows)
            market_train_median = v2._median_regret(market_train_rows)
            train_improvement = v2._improvement_ratio(
                v2_plus_train_mean,
                market_train_mean,
            )
            fallback_to_v2_plus = (
                train_improvement
                < min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus
                or market_train_median > v2_plus_train_median
            )
            selected_train_rows = (
                selected_v2_plus_train_rows if fallback_to_v2_plus else market_train_rows
            )
            selected_final_rows = (
                selected_v2_plus_final_rows if fallback_to_v2_plus else market_final_rows
            )
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": market_coupled_schedule_value_learner_v2_plus_model_name(
                        source_model_name
                    ),
                    "selected_weight_profile_name": RICH_POLAND_SELECTOR_PROFILE_NAME,
                    "approved_external_feature_columns_csv": ",".join(approved_columns),
                    "selected_feature_names": [
                        "poland_lag24_horizon_mean_uah_mwh",
                        "poland_lag24_horizon_spread_uah_mwh",
                        "entsoe_pl_lag24_delta_1h_uah_mwh",
                        "entsoe_pl_lag24_delta_24h_uah_mwh",
                        "entsoe_pl_lag24_daily_spread_uah_mwh",
                        "entsoe_pl_lag24_daily_price_rank",
                        "poland_lag24_peak_index",
                        "poland_lag24_trough_index",
                    ],
                    "selected_feature_weights": {
                        "selection_rule": (
                            "rich_poland_regime_family_mean_regret_with_v2_plus_fallback"
                        ),
                        "profile_name": selector["profile_name"],
                        "thresholds": selector["thresholds"],
                        "min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus": (
                            min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus
                        ),
                    },
                    "selected_regime_family_map": selector["family_by_regime"],
                    "poland_feature_median_threshold_uah_mwh": selector[
                        "median_feature_mean_uah_mwh"
                    ],
                    "fallback_to_ukrainian_v2_plus": fallback_to_v2_plus,
                    "train_anchor_count": len(v2._anchor_set(train_rows)),
                    "final_holdout_anchor_count": final_anchor_count,
                    "final_holdout_tenant_anchor_count": final_anchor_count * len(tenant_ids),
                    "ukrainian_v2_plus_train_mean_regret_uah": v2_plus_train_mean,
                    "market_coupled_candidate_train_mean_regret_uah": market_train_mean,
                    "selected_train_mean_regret_uah": v2._mean_regret(selected_train_rows),
                    "ukrainian_v2_plus_train_median_regret_uah": v2_plus_train_median,
                    "market_coupled_candidate_train_median_regret_uah": market_train_median,
                    "selected_train_median_regret_uah": v2._median_regret(selected_train_rows),
                    "ukrainian_v2_plus_final_mean_regret_uah": v2._mean_regret(
                        selected_v2_plus_final_rows
                    ),
                    "market_coupled_candidate_final_mean_regret_uah": v2._mean_regret(
                        market_final_rows
                    ),
                    "selected_final_mean_regret_uah": v2._mean_regret(selected_final_rows),
                    "ukrainian_v2_plus_final_median_regret_uah": v2._median_regret(
                        selected_v2_plus_final_rows
                    ),
                    "market_coupled_candidate_final_median_regret_uah": v2._median_regret(
                        market_final_rows
                    ),
                    "selected_final_median_regret_uah": v2._median_regret(
                        selected_final_rows
                    ),
                    "train_mean_regret_improvement_ratio_vs_ukrainian_v2_plus": (
                        train_improvement
                    ),
                    "final_mean_regret_improvement_ratio_vs_ukrainian_v2_plus": (
                        v2._improvement_ratio(
                            v2._mean_regret(selected_v2_plus_final_rows),
                            v2._mean_regret(selected_final_rows),
                        )
                    ),
                    "selected_train_family_counts": v2._family_counts(selected_train_rows),
                    "selected_final_family_counts": v2._family_counts(selected_final_rows),
                    "claim_scope": DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_CLAIM_SCOPE,
                    "academic_scope": DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    learner_v2_frame: pl.DataFrame,
    learner_v2_plus_frame: pl.DataFrame,
    market_coupled_learner_frame: pl.DataFrame,
    *,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame | None = None,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict LP/oracle rows for the materialized Poland B variant."""

    if market_coupled_learner_frame.is_empty():
        return _empty_strict_frame()
    v2._validate_library_frame(schedule_candidate_library_frame)
    v2._validate_learner_frame(learner_v2_frame)
    v2_plus._validate_learner_v2_plus_frame(learner_v2_plus_frame)
    _validate_market_coupled_learner_frame(market_coupled_learner_frame)
    resolved_generated_at = generated_at or v2._latest_generated_at(
        schedule_candidate_library_frame
    )
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    v2_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_frame.iter_rows(named=True)
    }
    v2_plus_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_plus_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for learner_row in market_coupled_learner_frame.iter_rows(named=True):
        tenant_id = str(learner_row["tenant_id"])
        source_model_name = str(learner_row["source_model_name"])
        v2_learner_row = v2_rows.get((tenant_id, source_model_name))
        v2_plus_learner_row = v2_plus_rows.get((tenant_id, source_model_name))
        if v2_learner_row is None or v2_plus_learner_row is None:
            raise ValueError(f"missing V2/V2+ learner rows for {tenant_id}/{source_model_name}")
        final_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and str(row["split_name"]) == "final_holdout"
        ]
        selected_v2_by_anchor = {
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
            for row in _selected_v2_rows(final_rows, v2_learner_row=v2_learner_row)
        }
        selected_v2_plus_by_anchor = {
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
            for row in _selected_v2_plus_rows(
                final_rows,
                v2_learner_row=v2_learner_row,
                v2_plus_learner_row=v2_plus_learner_row,
            )
        }
        if bool(learner_row["fallback_to_ukrainian_v2_plus"]):
            selected_market_by_anchor = selected_v2_plus_by_anchor
        else:
            if entsoe_poland_lagged_feature_candidate_frame is None:
                raise ValueError(
                    "entsoe_poland_lagged_feature_candidate_frame is required when "
                    "the market-coupled selector does not fall back to Ukrainian-only V2+"
                )
            selector = {
                "profile_name": dict(learner_row["selected_feature_weights"]).get(
                    "profile_name",
                    "lag24_level",
                ),
                "median_feature_mean_uah_mwh": float(
                    learner_row["poland_feature_median_threshold_uah_mwh"]
                ),
                "thresholds": dict(learner_row["selected_feature_weights"]).get(
                    "thresholds",
                    {
                        "mean": float(
                            learner_row["poland_feature_median_threshold_uah_mwh"]
                        )
                    },
                ),
                "family_by_regime": dict(learner_row["selected_regime_family_map"]),
            }
            selected_market_by_anchor = {
                v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
                for row in _select_market_coupled_rows(
                    final_rows,
                    selector=selector,
                    feature_by_timestamp=_feature_values_by_timestamp(
                        entsoe_poland_lagged_feature_candidate_frame
                    ),
                )
            }
        for anchor_timestamp in sorted(selected_market_by_anchor):
            anchor_rows = [
                row
                for row in final_rows
                if v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                == anchor_timestamp
            ]
            strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
            raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
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
                        selected_v2_by_anchor[anchor_timestamp],
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v2_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row(
                        selected_market_by_anchor[anchor_timestamp],
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v2_plus",
                        generated_at=resolved_generated_at,
                    ),
                ]
            )
    return pl.DataFrame(rows).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"]
    )


def build_dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    official_forecast_exogenous_feature_route_frame: pl.DataFrame,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    min_robust_passing_windows: int = 3,
    min_validation_tenant_anchor_count_per_source_model: int = 90,
    min_mean_regret_improvement_ratio: float = 0.05,
    min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus: float = 0.01,
) -> pl.DataFrame:
    """Replay the Poland B selector over latest-first rolling windows."""

    if not _approved_experimental_feature_columns(
        official_forecast_exogenous_feature_route_frame
    ):
        return _empty_robustness_frame()
    v2_plus_robust._validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        validation_window_count=validation_window_count,
        validation_anchor_count=validation_anchor_count,
        min_prior_anchors_before_window=min_prior_anchors_before_window,
        min_robust_passing_windows=min_robust_passing_windows,
    )
    v2._validate_library_frame(schedule_candidate_library_frame)
    rows: list[dict[str, Any]] = []
    for source_model_name in forecast_model_names:
        source_rows: list[dict[str, Any]] = []
        windows = v2_plus_robust._rolling_windows(
            schedule_candidate_library_frame,
            tenant_ids=tenant_ids,
            source_model_name=source_model_name,
            validation_window_count=validation_window_count,
            validation_anchor_count=validation_anchor_count,
            min_prior_anchors_before_window=min_prior_anchors_before_window,
        )
        for window in windows:
            source_rows.append(
                _robustness_window_summary_row(
                    schedule_candidate_library_frame,
                    official_forecast_exogenous_feature_route_frame,
                    entsoe_poland_lagged_feature_candidate_frame,
                    tenant_ids=tenant_ids,
                    source_model_name=source_model_name,
                    window=window,
                    validation_anchor_count=validation_anchor_count,
                    min_validation_tenant_anchor_count=(
                        min_validation_tenant_anchor_count_per_source_model
                    ),
                    min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
                    min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus=(
                        min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus
                    ),
                )
            )
        passing_count = sum(1 for row in source_rows if bool(row["v2_plus_window_passed"]))
        latest_passed = any(
            bool(row["v2_plus_window_passed"])
            for row in source_rows
            if int(row["window_index"]) == 1
        )
        robust = latest_passed and passing_count >= min_robust_passing_windows
        for row in source_rows:
            row["passing_window_count_for_source"] = passing_count
            row["robust_research_challenger"] = robust
            row["gate_label"] = v2_plus_robust._gate_label(row, robust=robust)
            row["production_promote"] = False
        rows.extend(source_rows)
    return pl.DataFrame(rows)


def _robustness_window_summary_row(
    frame: pl.DataFrame,
    route_frame: pl.DataFrame,
    lagged_feature_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    window: dict[str, Any],
    validation_anchor_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus: float,
) -> dict[str, Any]:
    validation_anchors = set(window["validation_anchors"])
    validation_start = v2._datetime_value(
        window["validation_start_anchor_timestamp"],
        field_name="validation_start_anchor_timestamp",
    )
    rolling_frame = v2_plus_robust._rolling_split_frame(
        frame,
        tenant_ids=tenant_ids,
        source_model_name=source_model_name,
        validation_anchors=validation_anchors,
        validation_start=validation_start,
    )
    base_frame = v2_plus_robust._base_library_frame(rolling_frame)
    v2_learner_frame = v2.build_dfl_schedule_value_learner_v2_frame(
        base_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=(source_model_name,),
        final_validation_anchor_count_per_tenant=validation_anchor_count,
    )
    v2_plus_learner_frame = v2_plus.build_dfl_schedule_value_learner_v2_plus_frame(
        rolling_frame,
        v2_learner_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=(source_model_name,),
        final_validation_anchor_count_per_tenant=validation_anchor_count,
        min_prior_mean_improvement_ratio_vs_v2=(
            min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus
        ),
    )
    market_learner_frame = build_dfl_market_coupled_schedule_value_learner_v2_plus_frame(
        rolling_frame,
        v2_learner_frame,
        v2_plus_learner_frame,
        route_frame,
        lagged_feature_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=(source_model_name,),
        final_validation_anchor_count_per_tenant=validation_anchor_count,
        min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus=(
            min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus
        ),
    )
    strict_frame = build_dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        rolling_frame,
        v2_learner_frame,
        v2_plus_learner_frame,
        market_learner_frame,
        entsoe_poland_lagged_feature_candidate_frame=lagged_feature_frame,
    )
    strict_rows = v2_plus_robust._role_rows(
        strict_frame, source_model_name, "strict_reference"
    )
    raw_rows = v2_plus_robust._role_rows(strict_frame, source_model_name, "raw_reference")
    v2_rows = v2_plus_robust._role_rows(
        strict_frame, source_model_name, "schedule_value_learner_v2_reference"
    )
    selected_rows = v2_plus_robust._role_rows(
        strict_frame, source_model_name, "schedule_value_learner_v2_plus"
    )
    strict_mean = v2_plus_robust._mean_regret(strict_rows)
    raw_mean = v2_plus_robust._mean_regret(raw_rows)
    v2_mean = v2_plus_robust._mean_regret(v2_rows)
    selected_mean = v2_plus_robust._mean_regret(selected_rows)
    strict_median = v2_plus_robust._median_regret(strict_rows)
    v2_median = v2_plus_robust._median_regret(v2_rows)
    selected_median = v2_plus_robust._median_regret(selected_rows)
    improvement_vs_raw = v2_plus_robust._improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = v2_plus_robust._improvement_ratio(strict_mean, selected_mean)
    improvement_vs_v2 = v2_plus_robust._improvement_ratio(v2_mean, selected_mean)
    validation_tenant_anchor_count = len(v2_plus_robust._tenant_anchor_set(selected_rows))
    development_passed = (
        validation_tenant_anchor_count >= min_validation_tenant_anchor_count
        and improvement_vs_raw > 0.0
    )
    strict_passed = (
        validation_tenant_anchor_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio
        and selected_median <= strict_median
    )
    v2_passed = improvement_vs_v2 > 0.0 and selected_median <= v2_median
    return {
        "source_model_name": source_model_name,
        "window_index": int(window["window_index"]),
        "validation_start_anchor_timestamp": window["validation_start_anchor_timestamp"],
        "validation_end_anchor_timestamp": window["validation_end_anchor_timestamp"],
        "tenant_count": len(tenant_ids),
        "validation_anchor_count_per_tenant": validation_anchor_count,
        "validation_tenant_anchor_count": validation_tenant_anchor_count,
        "minimum_prior_anchor_count_before_window": int(
            window["minimum_prior_anchor_count_before_window"]
        ),
        "fallback_to_v2_by_tenant": {
            str(row["tenant_id"]): bool(row["fallback_to_ukrainian_v2_plus"])
            for row in market_learner_frame.iter_rows(named=True)
        },
        "selected_family_counts": v2_plus_robust._selected_family_counts(selected_rows),
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "v2_mean_regret_uah": v2_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "v2_median_regret_uah": v2_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_v2": improvement_vs_v2,
        "development_passed": development_passed,
        "source_specific_strict_passed": strict_passed,
        "v2_non_degradation_passed": v2_passed,
        "v2_plus_window_passed": strict_passed and v2_passed,
        "passing_window_count_for_source": 0,
        "robust_research_challenger": False,
        "production_promote": False,
        "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE,
        "academic_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _selected_v2_rows(
    rows: list[dict[str, Any]],
    *,
    v2_learner_row: dict[str, Any],
) -> list[dict[str, Any]]:
    return v2._select_rows_by_score(
        v2_plus._base_candidate_rows(rows),
        profile=v2._profile_by_name(str(v2_learner_row["selected_weight_profile_name"])),
    )


def _selected_v2_plus_rows(
    rows: list[dict[str, Any]],
    *,
    v2_learner_row: dict[str, Any],
    v2_plus_learner_row: dict[str, Any],
) -> list[dict[str, Any]]:
    selected_v2_rows = _selected_v2_rows(rows, v2_learner_row=v2_learner_row)
    return v2_plus._selected_rows_from_learner_row(
        rows,
        learner_row=v2_plus_learner_row,
        selected_v2_rows=selected_v2_rows,
    )


def _fit_poland_regime_selector(
    train_rows: list[dict[str, Any]],
    *,
    feature_by_timestamp: dict[datetime, dict[str, float]],
) -> dict[str, Any]:
    anchor_stats = _anchor_feature_stats_for_rows(
        train_rows,
        feature_by_timestamp=feature_by_timestamp,
    )
    selectors = [
        _fit_regime_family_selector(
            train_rows,
            anchor_stats=anchor_stats,
            profile_name=profile_name,
        )
        for profile_name in (
            "lag24_level",
            "lag24_daily_spread",
            "lag24_delta_1h",
            "lag24_peak_timing",
            "lag24_level_spread",
        )
    ]
    scored_selectors = []
    for selector in selectors:
        selected_rows = _select_market_coupled_rows_from_embedded_features(
            train_rows,
            selector=selector,
            feature_by_anchor=anchor_stats,
        )
        scored_selectors.append(
            (
                v2._mean_regret(selected_rows),
                v2._median_regret(selected_rows),
                str(selector["profile_name"]),
                selector,
            )
        )
    return min(scored_selectors, key=lambda item: (item[0], item[1], item[2]))[3]


def _fit_regime_family_selector(
    train_rows: list[dict[str, Any]],
    *,
    anchor_stats: dict[datetime, dict[str, Any]],
    profile_name: str,
) -> dict[str, Any]:
    thresholds = _regime_thresholds(anchor_stats)
    regret_by_regime_family: dict[tuple[str, str], list[float]] = {}
    for row in train_rows:
        anchor = v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        regime = _regime(anchor_stats[anchor], profile_name=profile_name, thresholds=thresholds)
        key = (regime, str(row["candidate_family"]))
        regret_by_regime_family.setdefault(key, []).append(float(row["regret_uah"]))
    family_by_regime: dict[str, str] = {}
    for regime in sorted({key[0] for key in regret_by_regime_family}):
        candidates = [
            (mean(regrets), family)
            for (observed_regime, family), regrets in regret_by_regime_family.items()
            if observed_regime == regime
        ]
        if candidates:
            family_by_regime[regime] = min(candidates, key=lambda item: (item[0], item[1]))[1]
    if not family_by_regime:
        family_by_regime["default"] = v2.CANDIDATE_FAMILY_STRICT
    return {
        "profile_name": profile_name,
        "median_feature_mean_uah_mwh": thresholds["mean"],
        "thresholds": thresholds,
        "family_by_regime": dict(sorted(family_by_regime.items())),
    }


def _regime_thresholds(anchor_stats: dict[datetime, dict[str, Any]]) -> dict[str, float]:
    return {
        "mean": median([float(stats["mean"]) for stats in anchor_stats.values()]),
        "daily_spread": median(
            [float(stats["daily_spread_mean"]) for stats in anchor_stats.values()]
        ),
        "delta_1h": median(
            [float(stats["delta_1h_mean"]) for stats in anchor_stats.values()]
        ),
    }


def _select_market_coupled_rows(
    rows: list[dict[str, Any]],
    *,
    selector: dict[str, Any],
    feature_by_timestamp: dict[datetime, dict[str, float]],
) -> list[dict[str, Any]]:
    feature_by_anchor = _anchor_feature_stats_for_rows(
        rows,
        feature_by_timestamp=feature_by_timestamp,
    )
    return _select_market_coupled_rows_from_embedded_features(
        rows,
        selector=selector,
        feature_by_anchor=feature_by_anchor,
    )


def _select_market_coupled_rows_from_embedded_features(
    rows: list[dict[str, Any]],
    *,
    selector: dict[str, Any],
    feature_by_anchor: dict[datetime, dict[str, Any]],
) -> list[dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    profile_name = str(selector.get("profile_name", "lag24_level"))
    thresholds = {
        str(key): float(value)
        for key, value in dict(selector.get("thresholds", {})).items()
    }
    if not thresholds:
        thresholds = {"mean": float(selector["median_feature_mean_uah_mwh"])}
    family_by_regime = dict(selector["family_by_regime"])
    for anchor_timestamp, anchor_rows in sorted(v2._rows_by_anchor(rows).items()):
        stats = feature_by_anchor[anchor_timestamp]
        regime = _regime(stats, profile_name=profile_name, thresholds=thresholds)
        preferred_family = str(
            family_by_regime.get(regime, family_by_regime.get("default", ""))
        )
        family_rows = [
            row for row in anchor_rows if str(row["candidate_family"]) == preferred_family
        ]
        candidate_rows = family_rows or anchor_rows
        selected_rows.append(
            min(
                candidate_rows,
                key=lambda row: (
                    float(row["prior_family_mean_regret_uah"]),
                    _feature_alignment_error(row, stats=stats),
                    float(row["total_degradation_penalty_uah"]),
                    str(row["candidate_family"]),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(v2._payload(row))
    learner_model_name = market_coupled_schedule_value_learner_v2_plus_model_name(
        source_model_name
    )
    forecast_model_name = _forecast_model_name_for_role(
        row,
        source_model_name=source_model_name,
        role=role,
    )
    anchor_timestamp = v2._datetime_value(
        row["anchor_timestamp"], field_name="anchor_timestamp"
    )
    approved_columns = [
        value
        for value in str(learner_row["approved_external_feature_columns_csv"]).split(",")
        if value
    ]
    payload.update(
        {
            "strict_gate_kind": "dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp",
            "source_forecast_model_name": source_model_name,
            "learner_model_name": learner_model_name,
            "selected_weight_profile_name": str(learner_row["selected_weight_profile_name"]),
            "selected_feature_names": list(learner_row["selected_feature_names"]),
            "selected_feature_weights": dict(learner_row["selected_feature_weights"]),
            "selected_regime_family_map": dict(learner_row["selected_regime_family_map"]),
            "fallback_to_ukrainian_v2_plus": bool(
                learner_row["fallback_to_ukrainian_v2_plus"]
            ),
            "approved_external_feature_columns": approved_columns,
            "market_coupled_variant": True,
            "selector_row_candidate_family": str(row["candidate_family"]),
            "selector_row_candidate_model_name": str(row["candidate_model_name"]),
            "selector_row_role": role,
            "selection_role": role,
            "claim_scope": DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE,
            "academic_scope": DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": int(row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:market-coupled-v2-plus:{source_model_name}:"
            f"{role}:{row['candidate_family']}:{anchor_timestamp:%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": v2._first_or_default(
            row["soc_fraction_vector"], default=0.5
        ),
        "starting_soc_source": "schedule_candidate_library_v2_plus_market_coupled",
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
        "claim_scope": DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE,
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
    if role == "schedule_value_learner_v2_plus":
        return market_coupled_schedule_value_learner_v2_plus_model_name(
            source_model_name
        )
    if role == "schedule_value_learner_v2_reference":
        return v2.schedule_value_learner_v2_model_name(source_model_name)
    return str(row["candidate_model_name"])


def _anchor_feature_stats_for_rows(
    rows: list[dict[str, Any]],
    *,
    feature_by_timestamp: dict[datetime, dict[str, float]],
) -> dict[datetime, dict[str, Any]]:
    stats: dict[datetime, dict[str, Any]] = {}
    for anchor_rows in v2._rows_by_anchor(rows).values():
        reference = anchor_rows[0]
        anchor = v2._datetime_value(reference["anchor_timestamp"], field_name="anchor_timestamp")
        if anchor in stats:
            continue
        horizon_hours = int(reference["horizon_hours"])
        values: list[float] = []
        delta_1h_values: list[float] = []
        delta_24h_values: list[float] = []
        daily_spread_values: list[float] = []
        daily_rank_values: list[float] = []
        for offset in range(horizon_hours):
            timestamp = anchor + timedelta(hours=offset)
            try:
                feature_row = feature_by_timestamp[_timestamp_key(timestamp)]
            except KeyError as exc:
                raise ValueError(
                    "missing lagged Poland feature coverage for "
                    f"{timestamp.isoformat()}"
                ) from exc
            values.append(feature_row[DEFAULT_POLAND_LAGGED_FEATURE_COLUMN])
            delta_1h_values.append(feature_row[POLAND_LAG24_DELTA_1H_FEATURE_COLUMN])
            delta_24h_values.append(feature_row[POLAND_LAG24_DELTA_24H_FEATURE_COLUMN])
            daily_spread_values.append(
                feature_row[POLAND_LAG24_DAILY_SPREAD_FEATURE_COLUMN]
            )
            daily_rank_values.append(
                feature_row[POLAND_LAG24_DAILY_PRICE_RANK_FEATURE_COLUMN]
            )
        stats[anchor] = {
            "values": values,
            "mean": mean(values),
            "spread": max(values) - min(values),
            "delta_1h_mean": mean(delta_1h_values),
            "delta_24h_mean": mean(delta_24h_values),
            "daily_spread_mean": mean(daily_spread_values),
            "daily_price_rank_mean": mean(daily_rank_values),
            "peak_index": _arg_extreme(values, largest=True),
            "trough_index": _arg_extreme(values, largest=False),
        }
    return stats


def _feature_alignment_error(row: dict[str, Any], *, stats: dict[str, Any]) -> float:
    forecast = v2._float_list(
        row["forecast_price_uah_mwh_vector"],
        field_name="forecast_price_uah_mwh_vector",
    )
    if not forecast:
        return 0.0
    return float(
        abs(_arg_extreme(forecast, largest=True) - int(stats["peak_index"]))
        + abs(_arg_extreme(forecast, largest=False) - int(stats["trough_index"]))
    )


def _arg_extreme(values: list[float], *, largest: bool) -> int:
    return min(
        range(len(values)),
        key=lambda index: ((-values[index] if largest else values[index]), index),
    )


def _regime(
    stats: dict[str, Any],
    *,
    profile_name: str,
    thresholds: dict[str, float],
) -> str:
    if profile_name == "lag24_daily_spread":
        return (
            "high_spread_poland_lag24"
            if float(stats["daily_spread_mean"]) >= thresholds["daily_spread"]
            else "low_spread_poland_lag24"
        )
    if profile_name == "lag24_delta_1h":
        return (
            "rising_poland_lag24"
            if float(stats["delta_1h_mean"]) >= thresholds["delta_1h"]
            else "falling_poland_lag24"
        )
    if profile_name == "lag24_peak_timing":
        return (
            "late_peak_poland_lag24"
            if int(stats["peak_index"]) >= len(stats["values"]) // 2
            else "early_peak_poland_lag24"
        )
    if profile_name == "lag24_level_spread":
        level = "high" if float(stats["mean"]) >= thresholds["mean"] else "low"
        spread = (
            "high_spread"
            if float(stats["daily_spread_mean"]) >= thresholds["daily_spread"]
            else "low_spread"
        )
        return f"{level}_poland_lag24_{spread}"
    return (
        "high_poland_lag24"
        if float(stats["mean"]) >= thresholds["mean"]
        else "low_poland_lag24"
    )


def _approved_experimental_feature_columns(route_frame: pl.DataFrame) -> tuple[str, ...]:
    missing = sorted(REQUIRED_MARKET_COUPLING_FEATURE_ROUTE_COLUMNS.difference(route_frame.columns))
    if missing:
        raise ValueError(f"feature route frame is missing required columns: {missing}")
    return tuple(
        sorted(
            {
                str(row["approved_feature_column"])
                for row in route_frame.iter_rows(named=True)
                if str(row["approved_feature_column"]).strip()
                and (
                    bool(row["approved_for_official_training"])
                    or bool(row.get("approved_for_experimental_ablation", False))
                )
            }
        )
    )


def _feature_values_by_timestamp(frame: pl.DataFrame) -> dict[datetime, dict[str, float]]:
    if frame.is_empty():
        raise ValueError("lagged Poland feature frame has no rows")
    value_column = (
        "neighbor_market_price_uah_mwh"
        if "neighbor_market_price_uah_mwh" in frame.columns
        else DEFAULT_POLAND_LAGGED_FEATURE_COLUMN
    )
    missing = [
        column
        for column in ("delivery_timestamp_utc", value_column)
        if column not in frame.columns
    ]
    if missing:
        raise ValueError(f"lagged Poland feature frame is missing columns: {missing}")
    values: dict[datetime, dict[str, float]] = {}
    for row in frame.iter_rows(named=True):
        price = _row_float(
            row,
            DEFAULT_POLAND_LAGGED_FEATURE_COLUMN,
            fallback_column=value_column,
            default=0.0,
        )
        values[_timestamp_key(row["delivery_timestamp_utc"])] = {
            DEFAULT_POLAND_LAGGED_FEATURE_COLUMN: price,
            POLAND_LAG24_DELTA_1H_FEATURE_COLUMN: _row_float(
                row,
                POLAND_LAG24_DELTA_1H_FEATURE_COLUMN,
                default=0.0,
            ),
            POLAND_LAG24_DELTA_24H_FEATURE_COLUMN: _row_float(
                row,
                POLAND_LAG24_DELTA_24H_FEATURE_COLUMN,
                default=0.0,
            ),
            POLAND_LAG24_DAILY_SPREAD_FEATURE_COLUMN: _row_float(
                row,
                POLAND_LAG24_DAILY_SPREAD_FEATURE_COLUMN,
                default=0.0,
            ),
            POLAND_LAG24_DAILY_PRICE_RANK_FEATURE_COLUMN: _row_float(
                row,
                POLAND_LAG24_DAILY_PRICE_RANK_FEATURE_COLUMN,
                default=0.5,
            ),
            POLAND_LAG24_DAILY_PEAK_HOUR_FEATURE_COLUMN: _row_float(
                row,
                POLAND_LAG24_DAILY_PEAK_HOUR_FEATURE_COLUMN,
                default=0.0,
            ),
            POLAND_LAG24_DAILY_TROUGH_HOUR_FEATURE_COLUMN: _row_float(
                row,
                POLAND_LAG24_DAILY_TROUGH_HOUR_FEATURE_COLUMN,
                default=0.0,
            ),
        }
    return values


def _row_float(
    row: dict[str, Any],
    column: str,
    *,
    fallback_column: str | None = None,
    default: float,
) -> float:
    value = row.get(column)
    if value is None and fallback_column is not None:
        value = row.get(fallback_column)
    if value is None:
        return default
    return float(value)


def _timestamp_key(value: object) -> datetime:
    timestamp = v2._datetime_value(value, field_name="delivery_timestamp_utc")
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(UTC).replace(tzinfo=None)
    return timestamp


def _validate_selector_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
    min_prior_mean_improvement_ratio: float,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one source model.")
    if final_validation_anchor_count_per_tenant < 1:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
    if min_prior_mean_improvement_ratio < 0.0:
        raise ValueError("min_prior_mean_improvement_ratio must not be negative.")


def _validate_market_coupled_learner_frame(frame: pl.DataFrame) -> None:
    required = {
        "tenant_id",
        "source_model_name",
        "approved_external_feature_columns_csv",
        "selected_weight_profile_name",
        "selected_feature_names",
        "selected_feature_weights",
        "selected_regime_family_map",
        "poland_feature_median_threshold_uah_mwh",
        "fallback_to_ukrainian_v2_plus",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"market-coupled V2+ learner frame is missing columns: {missing}")
    for row in frame.iter_rows(named=True):
        if (
            str(row["claim_scope"])
            != DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_CLAIM_SCOPE
        ):
            raise ValueError("market-coupled V2+ rows have an unexpected claim_scope")
        if not bool(row["not_full_dfl"]) or not bool(row["not_market_execution"]):
            raise ValueError("market-coupled V2+ rows must keep research-only flags")


def _empty_model_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "tenant_id": pl.Utf8,
            "source_model_name": pl.Utf8,
            "learner_model_name": pl.Utf8,
            "selected_weight_profile_name": pl.Utf8,
            "approved_external_feature_columns_csv": pl.Utf8,
            "selected_feature_names": pl.List(pl.Utf8),
            "selected_feature_weights": pl.Object,
            "selected_regime_family_map": pl.Object,
            "poland_feature_median_threshold_uah_mwh": pl.Float64,
            "fallback_to_ukrainian_v2_plus": pl.Boolean,
            "claim_scope": pl.Utf8,
            "not_full_dfl": pl.Boolean,
            "not_market_execution": pl.Boolean,
        }
    )


def _empty_strict_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "evaluation_id": pl.Utf8,
            "tenant_id": pl.Utf8,
            "source_model_name": pl.Utf8,
            "forecast_model_name": pl.Utf8,
            "strategy_kind": pl.Utf8,
            "market_venue": pl.Utf8,
            "anchor_timestamp": pl.Datetime,
            "generated_at": pl.Datetime,
            "horizon_hours": pl.Int64,
            "starting_soc_fraction": pl.Float64,
            "starting_soc_source": pl.Utf8,
            "decision_value_uah": pl.Float64,
            "forecast_objective_value_uah": pl.Float64,
            "oracle_value_uah": pl.Float64,
            "regret_uah": pl.Float64,
            "regret_ratio": pl.Float64,
            "total_degradation_penalty_uah": pl.Float64,
            "total_throughput_mwh": pl.Float64,
            "committed_action": pl.Utf8,
            "committed_power_mw": pl.Float64,
            "rank_by_regret": pl.Int64,
            "data_quality_tier": pl.Utf8,
            "observed_coverage_ratio": pl.Float64,
            "safety_violation_count": pl.Int64,
            "selection_role": pl.Utf8,
            "claim_scope": pl.Utf8,
            "not_full_dfl": pl.Boolean,
            "not_market_execution": pl.Boolean,
            "evaluation_payload": pl.Object,
        }
    )


def _empty_robustness_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "source_model_name": pl.Utf8,
            "window_index": pl.Int64,
            "tenant_count": pl.Int64,
            "validation_anchor_count_per_tenant": pl.Int64,
            "validation_tenant_anchor_count": pl.Int64,
            "minimum_prior_anchor_count_before_window": pl.Int64,
            "strict_mean_regret_uah": pl.Float64,
            "raw_mean_regret_uah": pl.Float64,
            "v2_mean_regret_uah": pl.Float64,
            "selected_mean_regret_uah": pl.Float64,
            "strict_median_regret_uah": pl.Float64,
            "v2_median_regret_uah": pl.Float64,
            "selected_median_regret_uah": pl.Float64,
            "development_passed": pl.Boolean,
            "source_specific_strict_passed": pl.Boolean,
            "v2_non_degradation_passed": pl.Boolean,
            "v2_plus_window_passed": pl.Boolean,
            "robust_research_challenger": pl.Boolean,
            "production_promote": pl.Boolean,
            "claim_scope": pl.Utf8,
            "not_full_dfl": pl.Boolean,
            "not_market_execution": pl.Boolean,
        }
    )


__all__ = [
    "DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND",
    "build_dfl_market_coupled_schedule_value_learner_v2_plus_frame",
    "build_dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame",
    "build_dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    "market_coupled_schedule_value_learner_v2_plus_model_name",
]
