"""Candidate-level NBEATSx V2+ plus TFT portfolio meta-selector.

This module keeps frozen Ukrainian-only NBEATSx V2+ as the default expert and
tests whether calibrated TFT quantile schedules add decision-diverse candidates.
The final evaluator remains the same strict LP/oracle regret table.
"""

from __future__ import annotations

from datetime import datetime
from statistics import mean, median
from typing import Any, Final, TypeAlias

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import schedule_value_learner_v2_plus as v2_plus
from smart_arbitrage.dfl.promotion_gate import PromotionGateResult
from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_NBEATSX_TFT_COMPLEMENTARITY_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_nbeatsx_tft_complementarity_audit_not_full_dfl"
)
DFL_NBEATSX_TFT_CANDIDATE_PORTFOLIO_CLAIM_SCOPE: Final[str] = (
    "dfl_nbeatsx_tft_candidate_portfolio_v1_not_full_dfl"
)
DFL_NBEATSX_TFT_META_SELECTOR_CLAIM_SCOPE: Final[str] = (
    "dfl_nbeatsx_tft_candidate_value_meta_selector_v1_not_full_dfl"
)
DFL_NBEATSX_TFT_META_SELECTOR_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_nbeatsx_tft_meta_selector_strict_lp_gate_not_full_dfl"
)
DFL_NBEATSX_TFT_META_SELECTOR_ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "dfl_nbeatsx_tft_meta_selector_robustness_not_full_dfl"
)
DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark"
)
DFL_NBEATSX_TFT_META_SELECTOR_ROLLING_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark"
)
DFL_NBEATSX_TFT_META_SELECTOR_MODEL_NAME: Final[str] = (
    "dfl_nbeatsx_tft_candidate_value_meta_selector_v1"
)
DEFAULT_COMBINED_SOURCE_MODEL_NAME: Final[str] = (
    "nbeatsx_tft_candidate_portfolio_meta_selector_v1"
)
FROZEN_V2_PLUS_FALLBACK_FAMILY: Final[str] = "frozen_v2_plus_fallback"
STRICT_CONTROL_FAMILY: Final[str] = "strict_control"
META_SELECTOR_ROLE: Final[str] = "nbeatsx_tft_meta_selector_v1"

_CandidateKey: TypeAlias = tuple[str, str, str]
_AnchorKey: TypeAlias = tuple[str, Any]

_REQUIRED_LIBRARY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "generated_at",
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
        "evaluation_payload",
    }
)
_REQUIRED_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "selection_role",
        "anchor_timestamp",
        "regret_uah",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "evaluation_payload",
    }
)
_REQUIRED_SELECTOR_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "combined_source_model_name",
        "selected_source_model_name",
        "selected_candidate_family",
        "selected_candidate_model_name",
        "fallback_to_v2_plus",
        "selected_feature_weights",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
        "market_execution_enabled",
    }
)


def build_dfl_nbeatsx_tft_complementarity_audit_frame(
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_tft_quantile_schedule_candidate_library_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    tft_source_model_names: tuple[str, ...] = TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
    final_validation_anchor_count_per_tenant: int = 18,
) -> pl.DataFrame:
    """Classify whether TFT has useful final-holdout candidates per anchor.

    The `selector_feature_*` columns are computed only from forecasts, schedule
    vectors, and prior schedule geometry. Realized regrets are stored as labels
    or diagnostics and can change without changing those feature columns.
    """

    del final_validation_anchor_count_per_tenant
    _validate_columns(
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="frozen V2+ strict frame",
    )
    _validate_columns(
        dfl_tft_quantile_schedule_candidate_library_frame,
        _REQUIRED_LIBRARY_COLUMNS,
        frame_name="TFT quantile candidate library",
    )
    baseline_rows = [
        row
        for row in dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame.iter_rows(
            named=True
        )
        if str(row["source_model_name"]) == baseline_source_model_name
        and str(row["selection_role"]) == "schedule_value_learner_v2_plus"
    ]
    tft_final_by_anchor = _final_tft_rows_by_anchor(
        dfl_tft_quantile_schedule_candidate_library_frame,
        tft_source_model_names=tft_source_model_names,
    )
    rows: list[dict[str, Any]] = []
    for baseline_row in baseline_rows:
        anchor_key = _anchor_key(baseline_row)
        candidate_rows = tft_final_by_anchor.get(anchor_key, [])
        best_tft = (
            min(candidate_rows, key=lambda row: float(row["regret_uah"]))
            if candidate_rows
            else None
        )
        baseline_regret = float(baseline_row["regret_uah"])
        best_tft_regret = float(best_tft["regret_uah"]) if best_tft else None
        if best_tft is None:
            complementarity_class = "missing_tft_candidate"
        elif best_tft_regret is not None and best_tft_regret < baseline_regret:
            complementarity_class = "candidate_available_but_not_selected"
        else:
            complementarity_class = "no_useful_tft_candidate"
        feature_row = _feature_row_against_baseline(best_tft, baseline_row)
        rows.append(
            {
                "tenant_id": baseline_row["tenant_id"],
                "anchor_timestamp": baseline_row["anchor_timestamp"],
                "baseline_source_model_name": baseline_source_model_name,
                "best_tft_source_model_name": best_tft["source_model_name"]
                if best_tft
                else None,
                "best_tft_candidate_family": best_tft["candidate_family"]
                if best_tft
                else None,
                "best_tft_candidate_model_name": best_tft["candidate_model_name"]
                if best_tft
                else None,
                "complementarity_class": complementarity_class,
                "baseline_v2_plus_regret_uah": baseline_regret,
                "best_tft_regret_uah": best_tft_regret,
                "label_tft_regret_delta_vs_v2_plus_uah": (
                    None
                    if best_tft_regret is None
                    else best_tft_regret - baseline_regret
                ),
                "diagnostic_oracle_gap_uah": best_tft_regret,
                "claim_scope": DFL_NBEATSX_TFT_COMPLEMENTARITY_AUDIT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
                **feature_row,
            }
        )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["tenant_id", "anchor_timestamp"])


def build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame(
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_tft_quantile_schedule_candidate_library_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_nbeatsx_tft_complementarity_audit_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    tft_source_model_names: tuple[str, ...] = TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
    final_validation_anchor_count_per_tenant: int = 18,
    max_tft_candidates_per_anchor_source_family: int = 3,
) -> pl.DataFrame:
    """Build a candidate-level NBEATSx+TFT schedule portfolio.

    Cross-model candidates are feasibility-preserving copies of existing LP
    schedules with additional selector metadata. This keeps the strict evaluator
    unchanged while letting the selector reason over portfolio diversity.
    """

    del (
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_nbeatsx_tft_complementarity_audit_frame,
        final_validation_anchor_count_per_tenant,
    )
    _validate_columns(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        _REQUIRED_LIBRARY_COLUMNS,
        frame_name="NBEATSx V2+ candidate library",
    )
    _validate_columns(
        dfl_tft_quantile_schedule_candidate_library_frame,
        _REQUIRED_LIBRARY_COLUMNS,
        frame_name="TFT quantile candidate library",
    )
    baseline_rows = [
        row
        for row in dfl_official_global_panel_schedule_candidate_library_v2_plus_frame.iter_rows(
            named=True
        )
        if str(row["source_model_name"]) == baseline_source_model_name
    ]
    baseline_by_anchor = {
        _anchor_key(row): row
        for row in baseline_rows
        if str(row["candidate_family"]) == FROZEN_V2_PLUS_FALLBACK_FAMILY
    }
    rows: list[dict[str, Any]] = []
    for row in baseline_rows:
        family = str(row["candidate_family"])
        portfolio_source = (
            "strict_fallback"
            if family == STRICT_CONTROL_FAMILY
            else "nbeatsx_v2_plus"
        )
        rows.append(
            _portfolio_row(
                row,
                baseline_row=baseline_by_anchor.get(_anchor_key(row)),
                portfolio_source=portfolio_source,
                candidate_family=family,
                candidate_model_name=str(row["candidate_model_name"]),
                source_model_name=str(row["source_model_name"]),
            )
        )
    bounded_tft_rows = _bounded_tft_candidate_rows(
        dfl_tft_quantile_schedule_candidate_library_frame,
        tft_source_model_names=tft_source_model_names,
        max_per_anchor_source_family=max_tft_candidates_per_anchor_source_family,
    )
    for row in bounded_tft_rows:
        baseline_row = baseline_by_anchor.get(_anchor_key(row))
        rows.append(
            _portfolio_row(
                row,
                baseline_row=baseline_row,
                portfolio_source="tft_quantile",
                candidate_family=str(row["candidate_family"]),
                candidate_model_name=str(row["candidate_model_name"]),
                source_model_name=str(row["source_model_name"]),
            )
        )
        for family in (
            "nbeatsx_tft_uncertainty_veto_v1",
            "nbeatsx_tft_peak_trough_alternative_v1",
            "nbeatsx_tft_disagreement_schedule_v1",
        ):
            rows.append(
                _portfolio_row(
                    row,
                    baseline_row=baseline_row,
                    portfolio_source="cross_model",
                    candidate_family=family,
                    candidate_model_name=f"{family}:{row['candidate_model_name']}",
                    source_model_name=str(row["source_model_name"]),
                )
            )
    if not rows:
        return pl.DataFrame()
    _attach_prior_mean_feature(rows)
    return pl.DataFrame(rows).sort(
        [
            "tenant_id",
            "anchor_timestamp",
            "split_name",
            "portfolio_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame(
    dfl_nbeatsx_tft_candidate_portfolio_v1_frame: pl.DataFrame,
    dfl_nbeatsx_tft_complementarity_audit_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    combined_source_model_name: str = DEFAULT_COMBINED_SOURCE_MODEL_NAME,
    final_validation_anchor_count_per_tenant: int = 18,
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.05,
) -> pl.DataFrame:
    """Select a candidate key from prior/train anchors with V2+ fallback."""

    del dfl_nbeatsx_tft_complementarity_audit_frame, final_validation_anchor_count_per_tenant
    _validate_columns(
        dfl_nbeatsx_tft_candidate_portfolio_v1_frame,
        _REQUIRED_LIBRARY_COLUMNS
        | frozenset(
            {
                "portfolio_source",
                "selector_feature_schedule_distance_from_v2_plus",
                "label_regret_delta_vs_v2_plus_uah",
            }
        ),
        frame_name="NBEATSx+TFT candidate portfolio",
    )
    rows = list(dfl_nbeatsx_tft_candidate_portfolio_v1_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        tenant_train_rows = [
            row
            for row in rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["split_name"]) != "final_holdout"
        ]
        baseline_train = [
            row
            for row in tenant_train_rows
            if str(row["source_model_name"]) == baseline_source_model_name
            and str(row["candidate_family"]) == FROZEN_V2_PLUS_FALLBACK_FAMILY
        ]
        baseline_mean = _mean_or_none(baseline_train)
        best_key: _CandidateKey | None = None
        best_source = "frozen_v2_plus_fallback"
        best_mean = baseline_mean
        candidate_means = _candidate_train_means(tenant_train_rows)
        for key, values in sorted(candidate_means.items(), key=lambda item: item[0]):
            source_model_name, family, _model_name = key
            if (
                source_model_name == baseline_source_model_name
                and family in {FROZEN_V2_PLUS_FALLBACK_FAMILY, STRICT_CONTROL_FAMILY}
            ):
                continue
            candidate_mean = mean(values)
            if best_mean is None or candidate_mean < best_mean:
                best_key = key
                best_mean = candidate_mean
                best_source = _portfolio_source_for_key(tenant_train_rows, key)
        prior_improvement = (
            0.0
            if baseline_mean is None or best_mean is None or baseline_mean <= 0.0
            else (baseline_mean - best_mean) / baseline_mean
        )
        fallback = (
            best_key is None
            or baseline_mean is None
            or prior_improvement < min_prior_mean_improvement_ratio_vs_v2_plus
        )
        selected_key = best_key or (
            baseline_source_model_name,
            FROZEN_V2_PLUS_FALLBACK_FAMILY,
            f"dfl_schedule_value_learner_v2_plus_{baseline_source_model_name}",
        )
        gate_blocker = (
            "candidate_prior_improvement_selected"
            if not fallback
            else "weak_or_missing_prior_improvement"
        )
        output_rows.append(
            {
                "tenant_id": tenant_id,
                "combined_source_model_name": combined_source_model_name,
                "baseline_source_model_name": baseline_source_model_name,
                "selected_source_model_name": selected_key[0],
                "selected_candidate_family": selected_key[1],
                "selected_candidate_model_name": selected_key[2],
                "selected_portfolio_source": best_source
                if not fallback
                else "frozen_v2_plus_fallback",
                "learner_model_name": DFL_NBEATSX_TFT_META_SELECTOR_MODEL_NAME,
                "selected_scorer_type": "prior_regret_weighted_candidate_value_ranker",
                "selected_feature_weights": {
                    "selector_feature_prior_mean_regret_uah": 1.0,
                    "selector_feature_schedule_distance_from_v2_plus": -0.05,
                    "selector_feature_quantile_spread_uah_mwh": 0.01,
                    "selector_feature_peak_hour_disagreement": -0.1,
                    "selector_feature_trough_hour_disagreement": -0.1,
                },
                "fallback_to_v2_plus": fallback,
                "selector_gate_blocker": gate_blocker,
                "baseline_train_mean_regret_uah": baseline_mean,
                "selected_train_mean_regret_uah": best_mean
                if not fallback
                else baseline_mean,
                "prior_mean_improvement_ratio_vs_v2_plus": prior_improvement,
                "train_anchor_count": len(
                    {row["anchor_timestamp"] for row in tenant_train_rows}
                ),
                "claim_scope": DFL_NBEATSX_TFT_META_SELECTOR_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(output_rows).sort(["tenant_id"])


def build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame(
    dfl_nbeatsx_tft_candidate_portfolio_v1_frame: pl.DataFrame,
    dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit frozen V2+ rows plus selected meta-selector rows."""

    _validate_columns(
        dfl_nbeatsx_tft_candidate_portfolio_v1_frame,
        _REQUIRED_LIBRARY_COLUMNS | frozenset({"portfolio_source"}),
        frame_name="NBEATSx+TFT candidate portfolio",
    )
    _validate_columns(
        dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame,
        _REQUIRED_SELECTOR_COLUMNS,
        frame_name="NBEATSx+TFT meta-selector",
    )
    _validate_columns(
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="frozen V2+ strict frame",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
    )
    baseline_frame = _with_strict_metadata(
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
    )
    portfolio_rows = list(dfl_nbeatsx_tft_candidate_portfolio_v1_frame.iter_rows(named=True))
    selector_rows = {
        str(row["tenant_id"]): row
        for row in dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame.iter_rows(
            named=True
        )
    }
    baseline_selected_rows = [
        row
        for row in baseline_frame.iter_rows(named=True)
        if str(row["selection_role"]) == "schedule_value_learner_v2_plus"
        and str(row["source_model_name"]) == baseline_source_model_name
    ]
    selected_rows: list[dict[str, Any]] = []
    for baseline_row in baseline_selected_rows:
        selector = selector_rows.get(str(baseline_row["tenant_id"]))
        if selector is None or bool(selector["fallback_to_v2_plus"]):
            selected_rows.append(
                _strict_row_from_baseline(
                    baseline_row,
                    selector=selector,
                    generated_at=resolved_generated_at,
                )
            )
            continue
        candidate = _final_candidate_for_selector(portfolio_rows, baseline_row, selector)
        if candidate is None:
            selected_rows.append(
                _strict_row_from_baseline(
                    baseline_row,
                    selector=selector,
                    generated_at=resolved_generated_at,
                    blocker="missing_selected_final_candidate",
                )
            )
            continue
        selected_rows.append(
            _strict_row_from_candidate(
                candidate,
                baseline_row=baseline_row,
                selector=selector,
                generated_at=resolved_generated_at,
            )
        )
    return pl.concat(
        [baseline_frame, pl.DataFrame(selected_rows)],
        how="diagonal_relaxed",
    ).sort(["tenant_id", "anchor_timestamp", "selection_role"])


def build_dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame(
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_tft_quantile_schedule_candidate_library_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    tft_source_model_names: tuple[str, ...] = TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
    combined_source_model_name: str = DEFAULT_COMBINED_SOURCE_MODEL_NAME,
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    min_prior_mean_improvement_ratio_vs_v2: float = 0.01,
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.05,
    max_tft_candidates_per_anchor_source_family: int = 3,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Replay the portfolio selector over true rolling strict windows.

    Unlike the latest-holdout strict frame, this rebuilds frozen V2+ inside
    each rolling window from anchors strictly before that window, synthesizes
    V2+ fallback candidate rows for those prior anchors, and then lets TFT
    candidates compete only through prior-window evidence.
    """

    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if validation_window_count < 1:
        raise ValueError("validation_window_count must be at least 1.")
    if validation_anchor_count < 1:
        raise ValueError("validation_anchor_count must be at least 1.")
    if min_prior_anchors_before_window < 1:
        raise ValueError("min_prior_anchors_before_window must be at least 1.")
    _validate_columns(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        _REQUIRED_LIBRARY_COLUMNS,
        frame_name="NBEATSx V2+ candidate library",
    )
    _validate_columns(
        dfl_tft_quantile_schedule_candidate_library_frame,
        _REQUIRED_LIBRARY_COLUMNS,
        frame_name="TFT quantile candidate library",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame
    )
    windows = _rolling_windows(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        tenant_ids=tenant_ids,
        source_model_name=baseline_source_model_name,
        validation_window_count=validation_window_count,
        validation_anchor_count=validation_anchor_count,
        min_prior_anchors_before_window=min_prior_anchors_before_window,
    )
    strict_frames: list[pl.DataFrame] = []
    for window in windows:
        rolling_nbeatsx_frame = _rolling_split_frame(
            dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
            tenant_ids=tenant_ids,
            source_model_names=(baseline_source_model_name,),
            validation_anchors=set(window["validation_anchors"]),
            prior_anchors=set(window["prior_anchors"]),
            validation_start=window["validation_start_anchor_timestamp"],
        )
        base_frame = rolling_nbeatsx_frame.filter(
            ~pl.col("candidate_family").is_in(sorted(v2_plus.V2_PLUS_CANDIDATE_FAMILIES))
        )
        v2_learner_frame = v2.build_dfl_schedule_value_learner_v2_frame(
            base_frame,
            tenant_ids=tenant_ids,
            forecast_model_names=(baseline_source_model_name,),
            final_validation_anchor_count_per_tenant=validation_anchor_count,
        )
        v2_plus_learner_frame = v2_plus.build_dfl_schedule_value_learner_v2_plus_frame(
            rolling_nbeatsx_frame,
            v2_learner_frame,
            tenant_ids=tenant_ids,
            forecast_model_names=(baseline_source_model_name,),
            final_validation_anchor_count_per_tenant=validation_anchor_count,
            min_prior_mean_improvement_ratio_vs_v2=(
                min_prior_mean_improvement_ratio_vs_v2
            ),
        )
        v2_plus_strict_frame = (
            v2_plus.build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
                rolling_nbeatsx_frame,
                v2_plus_learner_frame,
                v2_learner_frame,
                generated_at=resolved_generated_at,
            )
        )
        nbeatsx_with_fallback = pl.concat(
            [
                rolling_nbeatsx_frame,
                pl.DataFrame(
                    _v2_plus_fallback_candidate_rows(
                        rolling_nbeatsx_frame,
                        v2_learner_frame=v2_learner_frame,
                        v2_plus_learner_frame=v2_plus_learner_frame,
                    )
                ),
            ],
            how="diagonal_relaxed",
        )
        rolling_tft_frame = _rolling_split_frame(
            dfl_tft_quantile_schedule_candidate_library_frame,
            tenant_ids=tenant_ids,
            source_model_names=tft_source_model_names,
            validation_anchors=set(window["validation_anchors"]),
            prior_anchors=set(window["prior_anchors"]),
            validation_start=window["validation_start_anchor_timestamp"],
        )
        audit_frame = build_dfl_nbeatsx_tft_complementarity_audit_frame(
            v2_plus_strict_frame,
            rolling_tft_frame,
            baseline_source_model_name=baseline_source_model_name,
            tft_source_model_names=tft_source_model_names,
            final_validation_anchor_count_per_tenant=validation_anchor_count,
        )
        portfolio_frame = build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame(
            nbeatsx_with_fallback,
            rolling_tft_frame,
            v2_plus_strict_frame,
            audit_frame,
            baseline_source_model_name=baseline_source_model_name,
            tft_source_model_names=tft_source_model_names,
            final_validation_anchor_count_per_tenant=validation_anchor_count,
            max_tft_candidates_per_anchor_source_family=(
                max_tft_candidates_per_anchor_source_family
            ),
        )
        selector_frame = build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame(
            portfolio_frame,
            audit_frame,
            tenant_ids=tenant_ids,
            baseline_source_model_name=baseline_source_model_name,
            combined_source_model_name=combined_source_model_name,
            final_validation_anchor_count_per_tenant=validation_anchor_count,
            min_prior_mean_improvement_ratio_vs_v2_plus=(
                min_prior_mean_improvement_ratio_vs_v2_plus
            ),
        )
        strict_frame = build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame(
            portfolio_frame,
            selector_frame,
            v2_plus_strict_frame,
            baseline_source_model_name=baseline_source_model_name,
            generated_at=resolved_generated_at,
        )
        strict_frames.append(_with_rolling_strict_metadata(strict_frame, window=window))
    if not strict_frames:
        return pl.DataFrame()
    return pl.concat(strict_frames, how="diagonal_relaxed").sort(
        ["rolling_window_index", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def build_dfl_nbeatsx_tft_meta_selector_robustness_frame(
    dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    combined_source_model_name: str = DEFAULT_COMBINED_SOURCE_MODEL_NAME,
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.05,
) -> pl.DataFrame:
    """Evaluate latest-first rolling windows for the combined selector."""

    _validate_columns(
        dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="NBEATSx+TFT meta-selector strict frame",
    )
    anchors = sorted(
        {
            row["anchor_timestamp"]
            for row in dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame.iter_rows(
                named=True
            )
        },
        reverse=True,
    )
    rows = list(dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame.iter_rows(named=True))
    out: list[dict[str, Any]] = []
    for window_index in range(validation_window_count):
        start = window_index * validation_anchor_count
        window_anchors = set(anchors[start : start + validation_anchor_count])
        if not window_anchors:
            break
        baseline = [
            row
            for row in rows
            if row["anchor_timestamp"] in window_anchors
            and str(row["source_model_name"]) == baseline_source_model_name
            and str(row["selection_role"]) == "schedule_value_learner_v2_plus"
        ]
        selected = [
            row
            for row in rows
            if row["anchor_timestamp"] in window_anchors
            and str(row["source_model_name"]) == combined_source_model_name
            and str(row["selection_role"]) == META_SELECTOR_ROLE
        ]
        baseline_mean = _mean_regret(baseline) if baseline else float("inf")
        selected_mean = _mean_regret(selected) if selected else float("inf")
        baseline_median = _median_regret(baseline) if baseline else float("inf")
        selected_median = _median_regret(selected) if selected else float("inf")
        improvement = (
            (baseline_mean - selected_mean) / baseline_mean
            if baseline_mean not in {0.0, float("inf")}
            else 0.0
        )
        median_not_worse = selected_median <= baseline_median
        pass_window = (
            len(selected) == len(baseline)
            and bool(baseline)
            and improvement >= min_mean_regret_improvement_ratio_vs_v2_plus
            and median_not_worse
            and not _market_execution_enabled([*baseline, *selected])
        )
        out.append(
            {
                "window_index": window_index,
                "validation_anchor_count": len(window_anchors),
                "validation_tenant_anchor_count": len(selected),
                "baseline_source_model_name": baseline_source_model_name,
                "combined_source_model_name": combined_source_model_name,
                "baseline_mean_regret_uah": baseline_mean,
                "selected_mean_regret_uah": selected_mean,
                "baseline_median_regret_uah": baseline_median,
                "selected_median_regret_uah": selected_median,
                "mean_regret_improvement_ratio_vs_v2_plus": improvement,
                "median_not_worse": median_not_worse,
                "rolling_pass": pass_window,
                "claim_scope": DFL_NBEATSX_TFT_META_SELECTOR_ROBUSTNESS_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(out)


def evaluate_dfl_nbeatsx_tft_meta_selector_gate(
    strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    combined_source_model_name: str = DEFAULT_COMBINED_SOURCE_MODEL_NAME,
    min_validation_tenant_anchor_count: int = 90,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.05,
) -> PromotionGateResult:
    """Pass only when the combined portfolio beats frozen V2+."""

    missing = sorted(_REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing:
        return PromotionGateResult(
            False,
            "blocked",
            f"NBEATSx+TFT meta-selector strict frame is missing required columns: {missing}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    baseline = [
        row
        for row in rows
        if str(row["source_model_name"]) == baseline_source_model_name
        and str(row["selection_role"]) == "schedule_value_learner_v2_plus"
    ]
    selected = [
        row
        for row in rows
        if str(row["source_model_name"]) == combined_source_model_name
        and str(row["selection_role"]) == META_SELECTOR_ROLE
    ]
    if len(baseline) < min_validation_tenant_anchor_count:
        return PromotionGateResult(
            False,
            "blocked",
            "frozen V2+ baseline coverage is below the validation threshold",
            {"baseline_validation_tenant_anchor_count": len(baseline)},
        )
    if len(selected) < min_validation_tenant_anchor_count:
        return PromotionGateResult(
            False,
            "blocked",
            "combined NBEATSx+TFT portfolio coverage is below the validation threshold",
            {"selected_validation_tenant_anchor_count": len(selected)},
        )
    baseline_mean = _mean_regret(baseline)
    selected_mean = _mean_regret(selected)
    baseline_median = _median_regret(baseline)
    selected_median = _median_regret(selected)
    improvement = (
        (baseline_mean - selected_mean) / baseline_mean
        if baseline_mean > 0.0
        else 0.0
    )
    median_not_worse = selected_median <= baseline_median
    safety_violation_count = sum(
        int(row.get("safety_violation_count") or 0) for row in selected
    )
    market_execution_enabled = _market_execution_enabled(rows)
    passed = (
        improvement >= min_mean_regret_improvement_ratio_vs_v2_plus
        and median_not_worse
        and safety_violation_count == 0
        and not market_execution_enabled
    )
    fallback_count = sum(
        1 for row in selected if row.get("fallback_to_v2_plus") is True
    )
    return PromotionGateResult(
        passed,
        "promote" if passed else "blocked",
        (
            "NBEATSx+TFT candidate portfolio beats frozen V2+ under strict LP/oracle scoring."
            if passed
            else "NBEATSx+TFT candidate portfolio does not beat frozen V2+ under the unchanged gate."
        ),
        {
            "baseline_source_model_name": baseline_source_model_name,
            "combined_source_model_name": combined_source_model_name,
            "v2_plus_mean_regret_uah": baseline_mean,
            "selected_mean_regret_uah": selected_mean,
            "v2_plus_median_regret_uah": baseline_median,
            "selected_median_regret_uah": selected_median,
            "mean_regret_improvement_ratio_vs_v2_plus": improvement,
            "median_not_worse": median_not_worse,
            "fallback_to_v2_plus_count": fallback_count,
            "selected_candidate_count": len(selected) - fallback_count,
            "safety_violation_count": safety_violation_count,
            "market_execution_enabled": market_execution_enabled,
        },
    )


def validate_dfl_nbeatsx_tft_meta_selector_evidence(
    strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    combined_source_model_name: str = DEFAULT_COMBINED_SOURCE_MODEL_NAME,
    min_validation_tenant_anchor_count: int = 90,
) -> EvidenceCheckOutcome:
    """Validate structural evidence without requiring promotion success."""

    missing = sorted(_REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing:
        return EvidenceCheckOutcome(
            False,
            f"NBEATSx+TFT meta-selector evidence is missing required columns: {missing}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    market_execution_enabled = _market_execution_enabled(rows)
    baseline_count = len(
        [
            row
            for row in rows
            if str(row["source_model_name"]) == baseline_source_model_name
            and str(row["selection_role"]) == "schedule_value_learner_v2_plus"
        ]
    )
    selected_count = len(
        [
            row
            for row in rows
            if str(row["source_model_name"]) == combined_source_model_name
            and str(row["selection_role"]) == META_SELECTOR_ROLE
        ]
    )
    failures: list[str] = []
    if baseline_count < min_validation_tenant_anchor_count:
        failures.append("baseline V2+ coverage is below threshold")
    if selected_count < min_validation_tenant_anchor_count:
        failures.append("combined NBEATSx+TFT portfolio coverage is below threshold")
    if market_execution_enabled:
        failures.append("market execution must remain disabled")
    gate = evaluate_dfl_nbeatsx_tft_meta_selector_gate(
        strict_frame,
        baseline_source_model_name=baseline_source_model_name,
        combined_source_model_name=combined_source_model_name,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    return EvidenceCheckOutcome(
        not failures,
        "NBEATSx+TFT meta-selector evidence has valid coverage and claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": strict_frame.height,
            "baseline_validation_tenant_anchor_count": baseline_count,
            "selected_validation_tenant_anchor_count": selected_count,
            "gate_decision": gate.decision,
            "gate_passed": gate.passed,
            "gate_metrics": gate.metrics,
            "market_execution_enabled": market_execution_enabled,
        },
    )


def validate_dfl_nbeatsx_tft_meta_selector_robustness_evidence(
    robustness_frame: pl.DataFrame,
    *,
    validation_window_count: int = 4,
) -> EvidenceCheckOutcome:
    """Validate rolling-window evidence for the combined portfolio."""

    required = {
        "window_index",
        "rolling_pass",
        "market_execution_enabled",
        "claim_scope",
    }
    missing = sorted(required.difference(robustness_frame.columns))
    if missing:
        return EvidenceCheckOutcome(
            False,
            f"NBEATSx+TFT meta-selector robustness evidence is missing columns: {missing}",
            {"row_count": robustness_frame.height},
        )
    market_execution_enabled = any(
        bool(value) for value in robustness_frame["market_execution_enabled"].to_list()
    )
    failures: list[str] = []
    if robustness_frame.height < validation_window_count:
        failures.append("rolling window count is below threshold")
    if market_execution_enabled:
        failures.append("market execution must remain disabled")
    return EvidenceCheckOutcome(
        not failures,
        "NBEATSx+TFT meta-selector robustness evidence has valid claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": robustness_frame.height,
            "rolling_pass_count": int(robustness_frame.filter(pl.col("rolling_pass")).height),
            "market_execution_enabled": market_execution_enabled,
        },
    )


def _rolling_windows(
    frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    validation_window_count: int,
    validation_anchor_count: int,
    min_prior_anchors_before_window: int,
) -> list[dict[str, Any]]:
    common_anchors: list[datetime] | None = None
    for tenant_id in tenant_ids:
        anchors = sorted(
            {
                v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                for row in frame.iter_rows(named=True)
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            }
        )
        required_anchor_count = (
            validation_window_count * validation_anchor_count
            + min_prior_anchors_before_window
        )
        if len(anchors) < required_anchor_count:
            raise ValueError(
                "NBEATSx+TFT rolling strict frame requires at least "
                f"{required_anchor_count} anchors for {tenant_id}/{source_model_name}; "
                f"observed {len(anchors)}"
            )
        if common_anchors is None:
            common_anchors = anchors
        elif anchors != common_anchors:
            raise ValueError(f"coverage mismatch across tenants for {source_model_name}")
    if common_anchors is None:
        raise ValueError(f"coverage missing for {source_model_name}")

    windows: list[dict[str, Any]] = []
    for offset in range(validation_window_count):
        end = len(common_anchors) - (offset * validation_anchor_count)
        start = end - validation_anchor_count
        if start < min_prior_anchors_before_window:
            raise ValueError(
                "rolling validation window does not have enough prior anchors before validation start"
            )
        validation_anchors = common_anchors[start:end]
        prior_start = max(0, start - min_prior_anchors_before_window)
        prior_anchors = common_anchors[prior_start:start]
        windows.append(
            {
                "window_index": offset,
                "validation_anchors": validation_anchors,
                "prior_anchors": prior_anchors,
                "validation_start_anchor_timestamp": validation_anchors[0],
                "validation_end_anchor_timestamp": validation_anchors[-1],
                "available_prior_anchor_count_before_window": start,
                "used_prior_anchor_count": len(prior_anchors),
            }
        )
    return windows


def _rolling_split_frame(
    frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    validation_anchors: set[datetime],
    prior_anchors: set[datetime],
    validation_start: datetime,
) -> pl.DataFrame:
    tenant_set = set(tenant_ids)
    source_set = set(source_model_names)
    rows: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        if str(row["tenant_id"]) not in tenant_set:
            continue
        if str(row["source_model_name"]) not in source_set:
            continue
        anchor = v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        if anchor in validation_anchors:
            copied = dict(row)
            copied["split_name"] = "final_holdout"
            rows.append(copied)
        elif anchor in prior_anchors and anchor < validation_start:
            copied = dict(row)
            copied["split_name"] = "train_selection"
            rows.append(copied)
    return pl.DataFrame(rows)


def _v2_plus_fallback_candidate_rows(
    rolling_nbeatsx_frame: pl.DataFrame,
    *,
    v2_learner_frame: pl.DataFrame,
    v2_plus_learner_frame: pl.DataFrame,
) -> list[dict[str, Any]]:
    library_rows = list(rolling_nbeatsx_frame.iter_rows(named=True))
    v2_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in v2_learner_frame.iter_rows(named=True)
    }
    fallback_rows: list[dict[str, Any]] = []
    for learner_row in v2_plus_learner_frame.iter_rows(named=True):
        tenant_id = str(learner_row["tenant_id"])
        source_model_name = str(learner_row["source_model_name"])
        v2_learner_row = v2_rows[(tenant_id, source_model_name)]
        source_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
        ]
        for split_name in ("train_selection", "final_holdout"):
            split_rows = [
                row for row in source_rows if str(row["split_name"]) == split_name
            ]
            selected_v2_rows = v2._select_rows_by_score(
                v2_plus._base_candidate_rows(split_rows),
                profile=v2._profile_by_name(
                    str(v2_learner_row["selected_weight_profile_name"])
                ),
            )
            selected_plus_rows = v2_plus._selected_rows_from_learner_row(
                split_rows,
                learner_row=learner_row,
                selected_v2_rows=selected_v2_rows,
            )
            fallback_rows.extend(
                _v2_plus_fallback_candidate_row(
                    row,
                    source_model_name=source_model_name,
                    learner_row=learner_row,
                )
                for row in selected_plus_rows
            )
    return fallback_rows


def _v2_plus_fallback_candidate_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
) -> dict[str, Any]:
    copied = dict(row)
    copied.update(
        {
            "source_model_name": source_model_name,
            "candidate_family": FROZEN_V2_PLUS_FALLBACK_FAMILY,
            "candidate_model_name": f"dfl_schedule_value_learner_v2_plus_{source_model_name}",
            "claim_scope": DFL_NBEATSX_TFT_CANDIDATE_PORTFOLIO_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    payload = _payload(row)
    payload.update(
        {
            "claim_scope": DFL_NBEATSX_TFT_CANDIDATE_PORTFOLIO_CLAIM_SCOPE,
            "selected_from": FROZEN_V2_PLUS_FALLBACK_FAMILY,
            "selected_weight_profile_name": learner_row["selected_weight_profile_name"],
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    copied["evaluation_payload"] = payload
    return copied


def _with_rolling_strict_metadata(
    strict_frame: pl.DataFrame,
    *,
    window: dict[str, Any],
) -> pl.DataFrame:
    payloads: list[dict[str, Any]] = []
    for row in strict_frame.iter_rows(named=True):
        payload = _payload(row)
        payload.update(
            {
                "benchmark_kind": (
                    DFL_NBEATSX_TFT_META_SELECTOR_ROLLING_STRICT_LP_STRATEGY_KIND
                ),
                "rolling_window_index": int(window["window_index"]),
                "validation_start_anchor_timestamp": window[
                    "validation_start_anchor_timestamp"
                ].isoformat(),
                "validation_end_anchor_timestamp": window[
                    "validation_end_anchor_timestamp"
                ].isoformat(),
                "available_prior_anchor_count_before_window": int(
                    window["available_prior_anchor_count_before_window"]
                ),
                "used_prior_anchor_count": int(
                    window["used_prior_anchor_count"]
                ),
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        payloads.append(payload)
    return strict_frame.with_columns(
        [
            pl.lit(DFL_NBEATSX_TFT_META_SELECTOR_ROLLING_STRICT_LP_STRATEGY_KIND).alias(
                "strategy_kind"
            ),
            pl.lit(int(window["window_index"])).alias("rolling_window_index"),
            pl.lit(window["validation_start_anchor_timestamp"]).alias(
                "validation_start_anchor_timestamp"
            ),
            pl.lit(window["validation_end_anchor_timestamp"]).alias(
                "validation_end_anchor_timestamp"
            ),
            pl.lit(int(window["available_prior_anchor_count_before_window"])).alias(
                "available_prior_anchor_count_before_window"
            ),
            pl.lit(int(window["used_prior_anchor_count"])).alias(
                "used_prior_anchor_count"
            ),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _portfolio_row(
    row: dict[str, Any],
    *,
    baseline_row: dict[str, Any] | None,
    portfolio_source: str,
    candidate_family: str,
    candidate_model_name: str,
    source_model_name: str,
) -> dict[str, Any]:
    copied = dict(row)
    copied.update(
        {
            "source_model_name": source_model_name,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "portfolio_source": portfolio_source,
            "claim_scope": DFL_NBEATSX_TFT_CANDIDATE_PORTFOLIO_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    copied.update(_feature_row_against_baseline(row, baseline_row))
    baseline_regret = float(baseline_row["regret_uah"]) if baseline_row else None
    copied["label_regret_uah"] = float(row["regret_uah"])
    copied["label_regret_delta_vs_v2_plus_uah"] = (
        None if baseline_regret is None else float(row["regret_uah"]) - baseline_regret
    )
    copied["label_beats_v2_plus"] = (
        False if baseline_regret is None else float(row["regret_uah"]) < baseline_regret
    )
    payload = _payload(row)
    payload.update(
        {
            "claim_scope": DFL_NBEATSX_TFT_CANDIDATE_PORTFOLIO_CLAIM_SCOPE,
            "portfolio_source": portfolio_source,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    copied["evaluation_payload"] = payload
    return copied


def _feature_row_against_baseline(
    candidate_row: dict[str, Any] | None,
    baseline_row: dict[str, Any] | None,
) -> dict[str, float]:
    if candidate_row is None:
        return {
            "selector_feature_peak_hour_disagreement": 0.0,
            "selector_feature_trough_hour_disagreement": 0.0,
            "selector_feature_quantile_spread_uah_mwh": 0.0,
            "selector_feature_schedule_distance_from_v2_plus": 0.0,
            "selector_feature_charge_discharge_overlap": 0.0,
            "selector_feature_terminal_soc_delta_fraction": 0.0,
            "selector_feature_throughput_delta_mwh": 0.0,
            "selector_feature_spread_volatility_delta_uah_mwh": 0.0,
        }
    candidate_prices = _float_list(candidate_row.get("forecast_price_uah_mwh_vector"))
    baseline_prices = _float_list(
        baseline_row.get("forecast_price_uah_mwh_vector") if baseline_row else None
    )
    candidate_dispatch = _float_list(candidate_row.get("dispatch_mw_vector"))
    baseline_dispatch = _float_list(
        baseline_row.get("dispatch_mw_vector") if baseline_row else None
    )
    candidate_soc = _float_list(candidate_row.get("soc_fraction_vector"))
    baseline_soc = _float_list(
        baseline_row.get("soc_fraction_vector") if baseline_row else None
    )
    return {
        "selector_feature_peak_hour_disagreement": float(
            abs(_argmax(candidate_prices) - _argmax(baseline_prices))
        ),
        "selector_feature_trough_hour_disagreement": float(
            abs(_argmin(candidate_prices) - _argmin(baseline_prices))
        ),
        "selector_feature_quantile_spread_uah_mwh": _spread(candidate_prices),
        "selector_feature_schedule_distance_from_v2_plus": _mean_abs_delta(
            candidate_dispatch,
            baseline_dispatch,
        ),
        "selector_feature_charge_discharge_overlap": _same_sign_overlap(
            candidate_dispatch,
            baseline_dispatch,
        ),
        "selector_feature_terminal_soc_delta_fraction": (
            (candidate_soc[-1] if candidate_soc else 0.0)
            - (baseline_soc[-1] if baseline_soc else 0.0)
        ),
        "selector_feature_throughput_delta_mwh": float(
            candidate_row.get("total_throughput_mwh") or 0.0
        )
        - float(
            baseline_row.get("total_throughput_mwh") or 0.0
            if baseline_row
            else 0.0
        ),
        "selector_feature_spread_volatility_delta_uah_mwh": abs(
            _spread(candidate_prices) - _spread(baseline_prices)
        ),
    }


def _with_strict_metadata(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    payloads: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        payload = _payload(row)
        payload.update(
            {
                "claim_scope": DFL_NBEATSX_TFT_META_SELECTOR_STRICT_CLAIM_SCOPE,
                "benchmark_kind": DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        payloads.append(payload)
    return frame.with_columns(
        [
            pl.lit(DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND).alias(
                "strategy_kind"
            ),
            pl.lit(False).alias("market_execution_enabled"),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _strict_row_from_baseline(
    baseline_row: dict[str, Any],
    *,
    selector: dict[str, Any] | None,
    generated_at: datetime | None,
    blocker: str = "v2_plus_fallback_selected",
) -> dict[str, Any]:
    copied = dict(baseline_row)
    combined_source = (
        str(selector["combined_source_model_name"])
        if selector is not None
        else DEFAULT_COMBINED_SOURCE_MODEL_NAME
    )
    copied.update(
        {
            "evaluation_id": (
                f"{baseline_row['tenant_id']}:{DFL_NBEATSX_TFT_META_SELECTOR_MODEL_NAME}:"
                f"{baseline_row['source_model_name']}:{baseline_row['anchor_timestamp']}"
            ),
            "source_model_name": combined_source,
            "forecast_model_name": DFL_NBEATSX_TFT_META_SELECTOR_MODEL_NAME,
            "strategy_kind": DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND,
            "generated_at": generated_at or baseline_row.get("generated_at"),
            "selection_role": META_SELECTOR_ROLE,
            "fallback_to_v2_plus": True,
            "selected_source_model_name": baseline_row["source_model_name"],
            "selected_candidate_family": FROZEN_V2_PLUS_FALLBACK_FAMILY,
            "selected_candidate_model_name": baseline_row["forecast_model_name"],
            "selected_portfolio_source": "frozen_v2_plus_fallback",
            "selector_gate_blocker": blocker,
            "market_execution_enabled": False,
            "not_market_execution": True,
        }
    )
    copied["evaluation_payload"] = _strict_payload(copied, fallback=True)
    return copied


def _strict_row_from_candidate(
    candidate: dict[str, Any],
    *,
    baseline_row: dict[str, Any],
    selector: dict[str, Any],
    generated_at: datetime | None,
) -> dict[str, Any]:
    dispatch = _float_list(candidate["dispatch_mw_vector"])
    soc = _float_list(candidate["soc_fraction_vector"])
    row = {
        "evaluation_id": (
            f"{candidate['tenant_id']}:{DFL_NBEATSX_TFT_META_SELECTOR_MODEL_NAME}:"
            f"{baseline_row['source_model_name']}:{candidate['anchor_timestamp']}"
        ),
        "tenant_id": candidate["tenant_id"],
        "source_model_name": selector["combined_source_model_name"],
        "forecast_model_name": DFL_NBEATSX_TFT_META_SELECTOR_MODEL_NAME,
        "strategy_kind": DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": candidate["anchor_timestamp"],
        "generated_at": generated_at or candidate.get("generated_at"),
        "horizon_hours": int(candidate["horizon_hours"]),
        "starting_soc_fraction": soc[0] if soc else baseline_row.get("starting_soc_fraction", 0.5),
        "starting_soc_source": "candidate_portfolio",
        "decision_value_uah": float(candidate["decision_value_uah"]),
        "forecast_objective_value_uah": float(candidate["forecast_objective_value_uah"]),
        "oracle_value_uah": float(candidate["oracle_value_uah"]),
        "regret_uah": float(candidate["regret_uah"]),
        "regret_ratio": float(candidate["regret_ratio"]),
        "total_degradation_penalty_uah": float(candidate["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(candidate["total_throughput_mwh"]),
        "committed_action": "MIXED",
        "committed_power_mw": dispatch[0] if dispatch else 0.0,
        "rank_by_regret": int(candidate.get("rank_by_regret") or 1),
        "selection_role": META_SELECTOR_ROLE,
        "fallback_to_v2_plus": False,
        "selected_source_model_name": candidate["source_model_name"],
        "selected_candidate_family": candidate["candidate_family"],
        "selected_candidate_model_name": candidate["candidate_model_name"],
        "selected_portfolio_source": candidate["portfolio_source"],
        "selector_gate_blocker": selector["selector_gate_blocker"],
        "forecast_price_uah_mwh_vector": _float_list(
            candidate["forecast_price_uah_mwh_vector"]
        ),
        "actual_price_uah_mwh_vector": _float_list(
            candidate["actual_price_uah_mwh_vector"]
        ),
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": soc,
        "safety_violation_count": int(candidate.get("safety_violation_count") or 0),
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }
    row["evaluation_payload"] = _strict_payload(row, fallback=False)
    return row


def _strict_payload(row: dict[str, Any], *, fallback: bool) -> dict[str, Any]:
    payload = _payload(row)
    payload.update(
        {
            "claim_scope": DFL_NBEATSX_TFT_META_SELECTOR_STRICT_CLAIM_SCOPE,
            "benchmark_kind": DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND,
            "selected_from": "frozen_v2_plus_fallback"
            if fallback
            else "nbeatsx_tft_candidate_portfolio",
            "fallback_to_v2_plus": fallback,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return payload


def _final_tft_rows_by_anchor(
    frame: pl.DataFrame,
    *,
    tft_source_model_names: tuple[str, ...],
) -> dict[_AnchorKey, list[dict[str, Any]]]:
    grouped: dict[_AnchorKey, list[dict[str, Any]]] = {}
    for row in frame.iter_rows(named=True):
        if str(row["split_name"]) != "final_holdout":
            continue
        if str(row["source_model_name"]) not in tft_source_model_names:
            continue
        grouped.setdefault(_anchor_key(row), []).append(row)
    return grouped


def _bounded_tft_candidate_rows(
    frame: pl.DataFrame,
    *,
    tft_source_model_names: tuple[str, ...],
    max_per_anchor_source_family: int,
) -> list[dict[str, Any]]:
    if max_per_anchor_source_family <= 0:
        raise ValueError("max_per_anchor_source_family must be positive")
    groups: dict[tuple[str, Any, str, str, str], list[dict[str, Any]]] = {}
    for row in frame.iter_rows(named=True):
        if str(row["source_model_name"]) not in tft_source_model_names:
            continue
        if str(row["candidate_family"]) == STRICT_CONTROL_FAMILY:
            continue
        key = (
            str(row["tenant_id"]),
            row["anchor_timestamp"],
            str(row["split_name"]),
            str(row["source_model_name"]),
            str(row["candidate_family"]),
        )
        groups.setdefault(key, []).append(row)
    bounded: list[dict[str, Any]] = []
    for group_rows in groups.values():
        bounded.extend(
            sorted(group_rows, key=_prior_safe_tft_candidate_sort_key)[
                :max_per_anchor_source_family
            ]
        )
    return bounded


def _prior_safe_tft_candidate_sort_key(row: dict[str, Any]) -> tuple[float, float, str]:
    forecast_value = _safe_float(row.get("forecast_objective_value_uah"))
    forecast_spread = _safe_float(row.get("forecast_spread_uah_mwh"))
    return (
        -forecast_value,
        -forecast_spread,
        str(row.get("candidate_model_name") or ""),
    )


def _candidate_train_means(
    train_rows: list[dict[str, Any]],
) -> dict[_CandidateKey, list[float]]:
    regrets: dict[_CandidateKey, list[float]] = {}
    for row in train_rows:
        if str(row.get("candidate_family")) == STRICT_CONTROL_FAMILY:
            continue
        regrets.setdefault(_candidate_key(row), []).append(float(row["regret_uah"]))
    return regrets


def _portfolio_source_for_key(
    rows: list[dict[str, Any]],
    key: _CandidateKey,
) -> str:
    for row in rows:
        if _candidate_key(row) == key:
            return str(row.get("portfolio_source") or "unknown")
    return "unknown"


def _final_candidate_for_selector(
    portfolio_rows: list[dict[str, Any]],
    baseline_row: dict[str, Any],
    selector: dict[str, Any],
) -> dict[str, Any] | None:
    anchor_candidates = [
        row
        for row in portfolio_rows
        if str(row["split_name"]) == "final_holdout"
        and _anchor_key(row) == _anchor_key(baseline_row)
        and str(row["candidate_family"])
        not in {FROZEN_V2_PLUS_FALLBACK_FAMILY, STRICT_CONTROL_FAMILY}
    ]
    if anchor_candidates:
        weights = selector.get("selected_feature_weights")
        if isinstance(weights, dict):
            return min(
                anchor_candidates,
                key=lambda row: _candidate_score(row, weights=weights),
            )
    key = (
        str(selector["selected_source_model_name"]),
        str(selector["selected_candidate_family"]),
        str(selector["selected_candidate_model_name"]),
    )
    for row in portfolio_rows:
        if str(row["split_name"]) != "final_holdout":
            continue
        if _anchor_key(row) != _anchor_key(baseline_row):
            continue
        if _candidate_key(row) == key:
            return row
    return None


def _attach_prior_mean_feature(rows: list[dict[str, Any]]) -> None:
    regrets: dict[tuple[str, _CandidateKey], list[float]] = {}
    for row in rows:
        if str(row["split_name"]) == "final_holdout":
            continue
        if str(row["candidate_family"]) == STRICT_CONTROL_FAMILY:
            continue
        regrets.setdefault((str(row["tenant_id"]), _candidate_key(row)), []).append(
            float(row["regret_uah"])
        )
    train_means = {
        key: mean(values)
        for key, values in regrets.items()
        if values
    }
    for row in rows:
        train_mean = train_means.get((str(row["tenant_id"]), _candidate_key(row)))
        row["selector_feature_prior_mean_regret_uah"] = (
            float(train_mean) if train_mean is not None else float(row["regret_uah"])
        )


def _candidate_score(row: dict[str, Any], *, weights: dict[Any, Any]) -> float:
    score = 0.0
    for feature_name, raw_weight in weights.items():
        if not isinstance(feature_name, str):
            continue
        if not feature_name.startswith("selector_feature_"):
            continue
        try:
            weight = float(raw_weight)
            value = float(row.get(feature_name) or 0.0)
        except (TypeError, ValueError):
            continue
        score += weight * value
    return score


def _candidate_key(row: dict[str, Any]) -> _CandidateKey:
    return (
        str(row["source_model_name"]),
        str(row["candidate_family"]),
        str(row["candidate_model_name"]),
    )


def _anchor_key(row: dict[str, Any]) -> _AnchorKey:
    return (str(row["tenant_id"]), row["anchor_timestamp"])


def _mean_or_none(rows: list[dict[str, Any]]) -> float | None:
    return None if not rows else _mean_regret(rows)


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    return mean(float(row["regret_uah"]) for row in rows)


def _median_regret(rows: list[dict[str, Any]]) -> float:
    return median(float(row["regret_uah"]) for row in rows)


def _latest_generated_at(frame: pl.DataFrame) -> datetime | None:
    if frame.height == 0 or "generated_at" not in frame.columns:
        return None
    values = [
        value
        for value in frame.select("generated_at").to_series().to_list()
        if isinstance(value, datetime)
    ]
    return max(values) if values else None


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    return dict(payload) if isinstance(payload, dict) else {}


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _float_list(value: Any) -> list[float]:
    if value is None:
        return []
    return [float(item) for item in value]


def _argmax(values: list[float]) -> int:
    if not values:
        return 0
    return max(range(len(values)), key=values.__getitem__)


def _argmin(values: list[float]) -> int:
    if not values:
        return 0
    return min(range(len(values)), key=values.__getitem__)


def _spread(values: list[float]) -> float:
    return 0.0 if not values else max(values) - min(values)


def _mean_abs_delta(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    count = min(len(left), len(right))
    return mean(abs(left[index] - right[index]) for index in range(count))


def _same_sign_overlap(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    count = min(len(left), len(right))
    overlaps = 0
    active = 0
    for index in range(count):
        if abs(left[index]) <= 1e-9 and abs(right[index]) <= 1e-9:
            continue
        active += 1
        if (left[index] >= 0.0 and right[index] >= 0.0) or (
            left[index] <= 0.0 and right[index] <= 0.0
        ):
            overlaps += 1
    return 0.0 if active == 0 else overlaps / active


def _market_execution_enabled(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        if row.get("market_execution_enabled") is True:
            return True
        payload = row.get("evaluation_payload")
        if isinstance(payload, dict) and payload.get("market_execution_enabled") is True:
            return True
    return False


def _validate_columns(
    frame: pl.DataFrame,
    required_columns: frozenset[str],
    *,
    frame_name: str,
) -> None:
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")


__all__ = [
    "DEFAULT_COMBINED_SOURCE_MODEL_NAME",
    "DFL_NBEATSX_TFT_META_SELECTOR_ROLLING_STRICT_LP_STRATEGY_KIND",
    "DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND",
    "build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame",
    "build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame",
    "build_dfl_nbeatsx_tft_complementarity_audit_frame",
    "build_dfl_nbeatsx_tft_meta_selector_robustness_frame",
    "build_dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame",
    "build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame",
    "evaluate_dfl_nbeatsx_tft_meta_selector_gate",
    "validate_dfl_nbeatsx_tft_meta_selector_evidence",
    "validate_dfl_nbeatsx_tft_meta_selector_robustness_evidence",
]
