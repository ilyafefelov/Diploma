"""Official global-panel V2+-teacher trajectory and DFL/DT bridge helpers."""

from __future__ import annotations

from datetime import datetime
from statistics import mean, pstdev
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.offline_dt_candidate import build_dfl_offline_dt_candidate_frame
from smart_arbitrage.dfl.residual_schedule_value import (
    build_dfl_residual_dt_fallback_strict_lp_benchmark_frame,
    build_dfl_residual_schedule_value_model_frame,
)
from smart_arbitrage.dfl.strict_challenger import CANDIDATE_FAMILY_STRICT, _datetime_value
from smart_arbitrage.dfl.trajectory_dataset import (
    build_dfl_real_data_trajectory_dataset_frame,
)
from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import (
    V2_PLUS_HEADLINE_BASELINE_METRICS,
    build_dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame,
)

OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS: Final[tuple[str, ...]] = (
    "nbeatsx_official_global_panel_v1",
    "nbeatsx_official_global_panel_horizon_calibrated_v1",
)
DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE: Final[str] = (
    "dfl_official_global_panel_v2_plus_dfl_dt_bridge_not_full_dfl"
)
DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark"
)
DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_TRAJECTORY_CLAIM_SCOPE: Final[str] = (
    "dfl_official_global_panel_v2_plus_trajectory_dataset_not_full_dfl"
)
DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_ACADEMIC_SCOPE: Final[str] = (
    "Official global-panel V2+-teacher residual DFL/offline DT bridge. "
    "Teacher schedules are selected from train/prior anchors only; final holdout "
    "rows are scoring-only. This is Offline Strategy Promotion evidence only, "
    "not deployed DT control and not market execution."
)


def build_dfl_official_global_panel_v2_plus_trajectory_dataset_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    final_validation_anchor_count_per_tenant: int = 18,
) -> pl.DataFrame:
    """Build official V2+-teacher trajectory rows with prior-only features."""

    prior_feature_panel = build_official_global_panel_v2_plus_prior_feature_panel_frame(
        schedule_candidate_library_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
    )
    trajectory = build_dfl_real_data_trajectory_dataset_frame(
        schedule_candidate_library_frame,
        prior_feature_panel,
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
    )
    if trajectory.height == 0:
        return trajectory
    return trajectory.with_columns(
        pl.lit(DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_TRAJECTORY_CLAIM_SCOPE).alias(
            "claim_scope"
        ),
        pl.lit(DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_ACADEMIC_SCOPE).alias(
            "academic_scope"
        ),
    )


def build_official_global_panel_v2_plus_prior_feature_panel_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
) -> pl.DataFrame:
    """Create the minimal prior-feature panel required by trajectory training.

    Only train-selection anchors strictly before the target anchor are used for
    selector features. Final-holdout outcomes never contribute to feature values.
    """

    rows = [
        row
        for row in schedule_candidate_library_frame.iter_rows(named=True)
        if str(row["tenant_id"]) in tenant_ids
        and str(row["source_model_name"]) in forecast_model_names
    ]
    output_rows: list[dict[str, Any]] = []
    anchor_keys = sorted(
        {
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
            )
            for row in rows
        },
        key=lambda item: (item[1], item[0], item[2]),
    )
    for tenant_id, source_model_name, anchor_timestamp in anchor_keys:
        prior_rows = [
            row
            for row in rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and str(row["split_name"]) != "final_holdout"
            and _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
            < anchor_timestamp
        ]
        current_rows = [
            row
            for row in rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
            == anchor_timestamp
        ]
        strict_regrets = _family_regrets(prior_rows, CANDIDATE_FAMILY_STRICT)
        raw_regrets = _family_regrets(prior_rows, "raw_source")
        best_non_strict_regrets = _best_non_strict_regrets_by_anchor(prior_rows)
        spread_values = [
            float(row["forecast_spread_uah_mwh"])
            for row in prior_rows
            if str(row["candidate_family"]) != CANDIDATE_FAMILY_STRICT
        ]
        current_strict = _family_regret_or_zero(current_rows, CANDIDATE_FAMILY_STRICT)
        current_raw = _family_regret_or_zero(current_rows, "raw_source")
        current_selected = _current_best_non_strict(current_rows)
        output_rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "window_index": 0,
                "anchor_timestamp": anchor_timestamp,
                "prior_cutoff_timestamp": anchor_timestamp,
                "selector_feature_prior_anchor_count": len(
                    {
                        _datetime_value(
                            row["anchor_timestamp"], field_name="anchor_timestamp"
                        )
                        for row in prior_rows
                    }
                ),
                "selector_feature_prior_strict_mean_regret_uah": _mean_or_zero(
                    strict_regrets
                ),
                "selector_feature_prior_raw_mean_regret_uah": _mean_or_zero(
                    raw_regrets
                ),
                "selector_feature_prior_best_non_strict_mean_regret_uah": _mean_or_zero(
                    best_non_strict_regrets
                ),
                "selector_feature_prior_price_spread_std_uah_mwh": (
                    pstdev(spread_values) if len(spread_values) > 1 else 0.0
                ),
                "selector_feature_prior_net_load_mean_mw": 0.0,
                "analysis_only_strict_regret_uah": current_strict,
                "analysis_only_raw_regret_uah": current_raw,
                "analysis_only_selected_regret_uah": current_selected["regret_uah"],
                "analysis_only_selected_candidate_family": current_selected["family"],
                "analysis_only_selector_beats_strict": (
                    current_selected["regret_uah"] < current_strict
                    if current_strict > 0.0
                    else False
                ),
                "claim_scope": "dfl_strict_failure_prior_feature_panel_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(output_rows)


def build_dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    residual_model_frame: pl.DataFrame,
    offline_dt_candidate_frame: pl.DataFrame,
    schedule_value_v2_plus_strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] = OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    final_validation_anchor_count_per_tenant: int = 18,
    min_confidence_improvement_ratio: float = 0.05,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Build official V2+-anchored strict comparison rows."""

    residual_dt_fallback_frame = build_dfl_residual_dt_fallback_strict_lp_benchmark_frame(
        schedule_candidate_library_frame,
        residual_model_frame,
        offline_dt_candidate_frame,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        min_confidence_improvement_ratio=min_confidence_improvement_ratio,
        generated_at=generated_at,
    )
    return build_dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame(
        residual_dt_fallback_frame,
        schedule_value_v2_plus_strict_frame,
        source_model_names=source_model_names,
        generated_at=generated_at,
        strategy_kind=(
            DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND
        ),
        claim_scope=DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE,
        academic_scope=DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_ACADEMIC_SCOPE,
        baseline_metrics=V2_PLUS_HEADLINE_BASELINE_METRICS,
    )


def build_dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame(
    trajectory_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    final_validation_anchor_count_per_tenant: int = 18,
    switch_margin_grid_uah: tuple[float, ...] = (0.0, 50.0, 100.0, 200.0, 400.0),
) -> pl.DataFrame:
    """Build the official V2+-teacher residual selector model card."""

    return build_dfl_residual_schedule_value_model_frame(
        trajectory_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        switch_margin_grid_uah=switch_margin_grid_uah,
    )


def build_dfl_official_global_panel_v2_plus_offline_dt_candidate_frame(
    trajectory_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    final_validation_anchor_count_per_tenant: int = 18,
    high_value_quantile: float = 0.75,
    context_length: int = 24,
    hidden_dim: int = 32,
    num_layers: int = 1,
    num_heads: int = 2,
    max_epochs: int = 5,
    random_seed: int = 2026,
) -> pl.DataFrame:
    """Build the official V2+-teacher tiny offline DT candidate card."""

    return build_dfl_offline_dt_candidate_frame(
        trajectory_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        high_value_quantile=high_value_quantile,
        context_length=context_length,
        hidden_dim=hidden_dim,
        num_layers=num_layers,
        num_heads=num_heads,
        max_epochs=max_epochs,
        random_seed=random_seed,
    )


def _family_regrets(rows: list[dict[str, Any]], family: str) -> list[float]:
    return [
        float(row["regret_uah"])
        for row in rows
        if str(row["candidate_family"]) == family
    ]


def _best_non_strict_regrets_by_anchor(rows: list[dict[str, Any]]) -> list[float]:
    by_anchor: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row["candidate_family"]) == CANDIDATE_FAMILY_STRICT:
            continue
        anchor_timestamp = _datetime_value(
            row["anchor_timestamp"], field_name="anchor_timestamp"
        )
        by_anchor.setdefault(anchor_timestamp, []).append(row)
    return [min(float(row["regret_uah"]) for row in anchor_rows) for anchor_rows in by_anchor.values()]


def _current_best_non_strict(rows: list[dict[str, Any]]) -> dict[str, Any]:
    non_strict = [
        row for row in rows if str(row["candidate_family"]) != CANDIDATE_FAMILY_STRICT
    ]
    if not non_strict:
        return {"family": CANDIDATE_FAMILY_STRICT, "regret_uah": 0.0}
    best = min(
        non_strict,
        key=lambda row: (float(row["regret_uah"]), str(row["candidate_family"])),
    )
    return {
        "family": str(best["candidate_family"]),
        "regret_uah": float(best["regret_uah"]),
    }


def _family_regret_or_zero(rows: list[dict[str, Any]], family: str) -> float:
    regrets = _family_regrets(rows, family)
    return regrets[0] if regrets else 0.0


def _mean_or_zero(values: list[float]) -> float:
    return mean(values) if values else 0.0


__all__ = [
    "DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE",
    "DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND",
    "OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS",
    "build_dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame",
    "build_dfl_official_global_panel_v2_plus_offline_dt_candidate_frame",
    "build_dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame",
    "build_dfl_official_global_panel_v2_plus_trajectory_dataset_frame",
    "build_official_global_panel_v2_plus_prior_feature_panel_frame",
]
