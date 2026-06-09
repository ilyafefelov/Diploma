"""Oracle-gap safe-switch layer before DT/LAVA.

This module turns the corrected V2+ oracle-gap audit into a conservative
candidate-index supervision layer. It is deliberately not raw hourly action
imitation: each candidate is an already feasible, strict-scored schedule, and
the selector falls back to corrected calibrated V2+ unless prior anchors show a
safe switch pattern.
"""

from __future__ import annotations

from datetime import datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import schedule_value_learner_v2_plus as v2_plus
from smart_arbitrage.dfl.lava_schedule_neighbor_bridge import (
    STRICT_REFERENCE_ROLE,
    V2_PLUS_REFERENCE_ROLE,
)
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
ORACLE_GAP_SAFE_SWITCH_LABEL_CLAIM_SCOPE: Final[str] = (
    "dfl_oracle_gap_safe_switch_labels_not_full_dfl"
)
ORACLE_GAP_SAFE_SWITCH_FEATURE_CLAIM_SCOPE: Final[str] = (
    "dfl_oracle_gap_safe_switch_feature_panel_not_full_dfl"
)
ORACLE_GAP_SAFE_SWITCH_SCORER_CLAIM_SCOPE: Final[str] = (
    "dfl_oracle_gap_safe_switch_scorer_not_full_dfl"
)
ORACLE_GAP_SAFE_SWITCH_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_oracle_gap_safe_switch_strict_lp_gate_not_full_dfl"
)
ORACLE_GAP_SAFE_SWITCH_ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "dfl_oracle_gap_safe_switch_rolling_robustness_not_full_dfl"
)
ORACLE_GAP_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_oracle_gap_safe_switch_strict_lp_benchmark"
)
ORACLE_GAP_SAFE_SWITCH_MODEL_NAME: Final[str] = (
    "dfl_oracle_gap_safe_switch_v1"
)
ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE: Final[str] = "oracle_gap_safe_switch"

ORACLE_GAP_SELECTOR_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "selector_feature_schedule_distance_from_v2_plus",
    "selector_feature_total_throughput_delta_mwh",
    "selector_feature_terminal_soc_delta_fraction",
    "selector_feature_forecast_spread_uah_mwh",
    "selector_feature_total_degradation_penalty_uah",
    "selector_feature_candidate_family_sort_index",
    "selector_feature_poland_shadow_candidate",
    "selector_feature_tft_shadow_candidate",
    "selector_feature_cross_model_disagreement",
)

_REQUIRED_LIBRARY_COLUMNS: Final[frozenset[str]] = v2.REQUIRED_LIBRARY_COLUMNS
_REQUIRED_V2_PLUS_STRICT_COLUMNS: Final[frozenset[str]] = (
    v2_plus.REQUIRED_STRICT_COLUMNS
)
_REQUIRED_LABEL_COLUMNS: Final[frozenset[str]] = _REQUIRED_LIBRARY_COLUMNS | frozenset(
    {
        "candidate_source",
        "eligible_for_final_selection",
        "is_train_or_prior_anchor",
        "v2_plus_baseline_regret_uah",
        "label_regret_delta_vs_v2_plus_uah",
        "label_safe_switch_win",
        "label_tail_risk_loss",
        "label_best_candidate_family",
        "label_best_candidate_model_name",
        "label_is_anchor_best_candidate",
        "oracle_gap_teacher_class",
        *ORACLE_GAP_SELECTOR_FEATURE_COLUMNS,
        "target_label_space",
        "raw_hourly_action_imitation",
        "market_execution_enabled",
    }
)


def build_dfl_oracle_gap_safe_switch_label_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    schedule_value_v2_plus_frame: pl.DataFrame,
    schedule_value_v2_frame: pl.DataFrame,
    schedule_value_v2_plus_strict_frame: pl.DataFrame,
    oracle_gap_audit_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    tail_risk_delta_uah: float = 150.0,
) -> pl.DataFrame:
    """Attach V2+-anchored safe-switch labels to every feasible candidate row."""

    v2._validate_library_frame(schedule_candidate_library_frame)
    v2_plus._validate_learner_v2_plus_frame(schedule_value_v2_plus_frame)
    v2._validate_learner_frame(schedule_value_v2_frame)
    _require_columns(
        schedule_value_v2_plus_strict_frame,
        _REQUIRED_V2_PLUS_STRICT_COLUMNS,
        frame_name="schedule_value_v2_plus_strict_frame",
    )
    _require_columns(
        oracle_gap_audit_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "oracle_gap_class",
                "best_candidate_family",
                "best_candidate_model_name",
            }
        ),
        frame_name="oracle_gap_audit_frame",
    )
    if tail_risk_delta_uah <= 0.0:
        raise ValueError("tail_risk_delta_uah must be positive.")

    source_filter = set(source_model_names or _source_model_names(schedule_value_v2_plus_frame))
    library_rows = [
        row
        for row in schedule_candidate_library_frame.iter_rows(named=True)
        if str(row["source_model_name"]) in source_filter
    ]
    baseline_by_anchor = _selected_v2_plus_rows_by_anchor(
        library_rows,
        schedule_value_v2_plus_frame,
        schedule_value_v2_frame,
        source_model_names=source_filter,
    )
    best_by_anchor = _best_candidate_rows_by_anchor(library_rows)
    audit_by_anchor = {
        _tenant_source_anchor_key(row): row
        for row in oracle_gap_audit_frame.iter_rows(named=True)
        if str(row["source_model_name"]) in source_filter
    }
    output_rows: list[dict[str, Any]] = []
    for row in library_rows:
        key = _tenant_source_anchor_key(row)
        baseline = baseline_by_anchor.get(key)
        best = best_by_anchor.get(key)
        if baseline is None or best is None:
            raise ValueError(
                "missing oracle-gap baseline/best candidate for "
                f"{key[0]}/{key[1]}/{key[2].isoformat()}"
            )
        output_rows.append(
            _label_row(
                row,
                baseline_row=baseline,
                best_row=best,
                audit_row=audit_by_anchor.get(key),
                tail_risk_delta_uah=tail_risk_delta_uah,
            )
        )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def build_dfl_oracle_gap_safe_switch_feature_panel_frame(
    oracle_gap_safe_switch_label_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Publish the prior-only feature panel used by the safe-switch scorer."""

    _validate_label_frame(oracle_gap_safe_switch_label_frame)
    null_features = [
        column
        for column in ORACLE_GAP_SELECTOR_FEATURE_COLUMNS
        if oracle_gap_safe_switch_label_frame.select(pl.col(column).is_null().any()).item()
    ]
    if null_features:
        raise ValueError(f"oracle-gap feature panel has null features: {null_features}")
    rows: list[dict[str, Any]] = []
    for row in oracle_gap_safe_switch_label_frame.iter_rows(named=True):
        copied = dict(row)
        copied.update(
            {
                "feature_panel_version": "oracle_gap_safe_switch_v1",
                "selected_feature_names": list(ORACLE_GAP_SELECTOR_FEATURE_COLUMNS),
                "claim_scope": ORACLE_GAP_SAFE_SWITCH_FEATURE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        rows.append(copied)
    return pl.DataFrame(rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def build_dfl_oracle_gap_safe_switch_scorer_frame(
    oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    min_prior_safe_win_count: int = 1,
    min_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_predicted_tail_risk_probability: float = 0.25,
    allowed_candidate_sources: tuple[str, ...] = (
        "oracle_gap_candidate",
        "poland_shadow_candidate",
        "tft_shadow_candidate",
    ),
    ridge_l2: float = 10.0,
) -> pl.DataFrame:
    """Train a prior-only profile scorer with V2+ fallback for weak/OOD rows."""

    _validate_feature_panel(oracle_gap_safe_switch_feature_panel_frame)
    _validate_scorer_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        min_prior_safe_win_count=min_prior_safe_win_count,
        min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
        allowed_candidate_sources=allowed_candidate_sources,
        ridge_l2=ridge_l2,
    )
    rows = list(oracle_gap_safe_switch_feature_panel_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = [
                row
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            ]
            scorer_row = _fit_safe_switch_for_scope(
                source_rows,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                min_prior_safe_win_count=min_prior_safe_win_count,
                min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_predicted_tail_risk_probability=(
                    max_predicted_tail_risk_probability
                ),
                allowed_candidate_sources=set(allowed_candidate_sources),
                ridge_l2=ridge_l2,
            )
            output_rows.append(scorer_row)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def build_dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame(
    oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
    oracle_gap_safe_switch_scorer_frame: pl.DataFrame,
    schedule_value_v2_plus_strict_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict, corrected V2+, and oracle-gap safe-switch rows."""

    _validate_feature_panel(oracle_gap_safe_switch_feature_panel_frame)
    _validate_scorer_frame(oracle_gap_safe_switch_scorer_frame)
    _require_columns(
        schedule_value_v2_plus_strict_frame,
        _REQUIRED_V2_PLUS_STRICT_COLUMNS,
        frame_name="schedule_value_v2_plus_strict_frame",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        oracle_gap_safe_switch_feature_panel_frame
    )
    candidate_rows = list(oracle_gap_safe_switch_feature_panel_frame.iter_rows(named=True))
    candidate_by_key = {_candidate_key(row): row for row in candidate_rows}
    v2_reference_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in schedule_value_v2_plus_strict_frame.iter_rows(named=True):
        if str(row["selection_role"]) not in {
            "strict_reference",
            "schedule_value_learner_v2_plus",
        }:
            continue
        role = (
            STRICT_REFERENCE_ROLE
            if str(row["selection_role"]) == "strict_reference"
            else V2_PLUS_REFERENCE_ROLE
        )
        if role == V2_PLUS_REFERENCE_ROLE:
            v2_reference_by_anchor[_strict_anchor_key(row)] = row
        output_rows.append(
            _reference_row(row, selection_role=role, generated_at=resolved_generated_at)
        )
    for scorer_row in oracle_gap_safe_switch_scorer_frame.iter_rows(named=True):
        for key in scorer_row["selected_final_candidate_keys"]:
            candidate = candidate_by_key[str(key)]
            output_rows.append(
                _candidate_benchmark_row(
                    candidate,
                    scorer_row=scorer_row,
                    generated_at=resolved_generated_at,
                )
            )
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            fallback = v2_reference_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"Missing corrected V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _fallback_benchmark_row(
                    fallback,
                    scorer_row=scorer_row,
                    generated_at=resolved_generated_at,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def build_dfl_oracle_gap_safe_switch_rolling_robustness_frame(
    oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    min_prior_safe_win_count: int = 1,
    min_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_predicted_tail_risk_probability: float = 0.25,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    allowed_candidate_sources: tuple[str, ...] = (
        "oracle_gap_candidate",
        "poland_shadow_candidate",
        "tft_shadow_candidate",
    ),
    ridge_l2: float = 10.0,
) -> pl.DataFrame:
    """Replay safe-switch selection over latest-first prior-only windows."""

    _validate_feature_panel(oracle_gap_safe_switch_feature_panel_frame)
    if validation_window_count <= 0:
        raise ValueError("validation_window_count must be positive.")
    if validation_anchor_count <= 0:
        raise ValueError("validation_anchor_count must be positive.")
    if min_prior_anchors_before_window < 0:
        raise ValueError("min_prior_anchors_before_window must not be negative.")

    rows = list(oracle_gap_safe_switch_feature_panel_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for source_model_name in forecast_model_names:
        windows = _rolling_windows(
            rows,
            tenant_ids=tenant_ids,
            source_model_name=source_model_name,
            validation_window_count=validation_window_count,
            validation_anchor_count=validation_anchor_count,
            min_prior_anchors_before_window=min_prior_anchors_before_window,
        )
        source_window_rows: list[dict[str, Any]] = []
        for window_index, validation_anchors, prior_anchors in windows:
            window_panel = _with_window_split(
                rows,
                tenant_ids=tenant_ids,
                source_model_name=source_model_name,
                validation_anchors=validation_anchors,
                prior_anchors=prior_anchors,
            )
            scorer = build_dfl_oracle_gap_safe_switch_scorer_frame(
                window_panel,
                tenant_ids=tenant_ids,
                forecast_model_names=(source_model_name,),
                min_prior_safe_win_count=min_prior_safe_win_count,
                min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
                min_predicted_improvement_uah=min_predicted_improvement_uah,
                max_predicted_tail_risk_probability=(
                    max_predicted_tail_risk_probability
                ),
                allowed_candidate_sources=allowed_candidate_sources,
                ridge_l2=ridge_l2,
            )
            source_window_rows.append(
                _rolling_summary_row(
                    window_panel,
                    scorer,
                    source_model_name=source_model_name,
                    window_index=window_index,
                    validation_anchors=validation_anchors,
                    prior_anchors=prior_anchors,
                    min_mean_regret_improvement_ratio_vs_v2_plus=(
                        min_mean_regret_improvement_ratio_vs_v2_plus
                    ),
                    min_mean_regret_improvement_ratio_vs_strict=(
                        min_mean_regret_improvement_ratio_vs_strict
                    ),
                )
            )
        pass_count = sum(
            1 for row in source_window_rows if bool(row["rolling_window_passed"])
        )
        diagnostic_count = sum(
            1 for row in source_window_rows if bool(row["diagnostic_window_passed"])
        )
        for row in source_window_rows:
            row["passing_window_count_for_source"] = pass_count
            row["diagnostic_window_count_for_source"] = diagnostic_count
            row["robust_safe_switch_challenger"] = pass_count >= validation_window_count
            row["diagnostic_signal_learnable"] = diagnostic_count >= min(
                validation_window_count, 3
            )
            row["production_promote"] = False
        output_rows.extend(source_window_rows)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "window_index"]
    )


def evaluate_dfl_oracle_gap_safe_switch_gate(
    strict_frame: pl.DataFrame,
    *,
    min_validation_tenant_anchor_count: int = 90,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Require the safe-switch scorer to beat corrected V2+ before promotion."""

    _require_columns(
        strict_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "selection_role",
                "anchor_timestamp",
                "regret_uah",
                "not_market_execution",
                "market_execution_enabled",
            }
        ),
        frame_name="oracle-gap safe-switch strict frame",
    )
    if strict_frame.select(pl.col("market_execution_enabled").any()).item():
        return PromotionGateResult(
            False,
            "blocked",
            "oracle-gap safe-switch refuses market execution claims",
            {"market_execution_enabled": False},
        )
    summaries = _role_summaries(strict_frame)
    selected = summaries.get(ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE)
    v2_reference = summaries.get(V2_PLUS_REFERENCE_ROLE)
    strict_reference = summaries.get(STRICT_REFERENCE_ROLE)
    validation_count = _tenant_anchor_count(
        strict_frame.filter(pl.col("selection_role") == ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE)
    )
    failures: list[str] = []
    if selected is None:
        failures.append(f"missing {ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE} rows")
    if v2_reference is None:
        failures.append("missing corrected V2+ reference rows")
    if strict_reference is None:
        failures.append("missing strict reference rows")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            "oracle-gap safe-switch validation tenant-anchor count below required "
            f"{min_validation_tenant_anchor_count}"
        )
    if failures or selected is None or v2_reference is None or strict_reference is None:
        return PromotionGateResult(
            False,
            "blocked",
            "; ".join(failures),
            {
                "role_summaries": summaries,
                "validation_tenant_anchor_count": validation_count,
                "market_execution_enabled": False,
            },
        )
    improvement_vs_v2 = _improvement_ratio(
        float(v2_reference["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    improvement_vs_strict = _improvement_ratio(
        float(strict_reference["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    median_degraded = float(selected["median_regret_uah"]) > float(
        v2_reference["median_regret_uah"]
    )
    if improvement_vs_v2 < min_mean_regret_improvement_ratio_vs_v2_plus:
        failures.append("mean_not_improved_vs_corrected_v2_plus")
    if improvement_vs_strict < min_mean_regret_improvement_ratio_vs_strict:
        failures.append(f"mean_not_improved_vs_{CONTROL_MODEL_NAME}")
    if median_degraded:
        failures.append("median_degraded_vs_corrected_v2_plus")
    metrics = {
        "selected_mean_regret_uah": selected["mean_regret_uah"],
        "v2_plus_mean_regret_uah": v2_reference["mean_regret_uah"],
        "strict_mean_regret_uah": strict_reference["mean_regret_uah"],
        "selected_median_regret_uah": selected["median_regret_uah"],
        "v2_plus_median_regret_uah": v2_reference["median_regret_uah"],
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "diagnostic_signal_passed": improvement_vs_v2 > 0.0 and not median_degraded,
        "validation_tenant_anchor_count": validation_count,
        "role_summaries": summaries,
        "market_execution_enabled": False,
        "offline_strategy_challenger_passed": not failures,
        "production_promote": False,
    }
    if failures:
        return PromotionGateResult(False, "blocked", "; ".join(failures), metrics)
    return PromotionGateResult(
        True,
        "offline_strategy_challenger",
        "Oracle-gap safe-switch scorer beats corrected V2+ under strict LP/oracle evidence",
        metrics,
    )


def _selected_v2_plus_rows_by_anchor(
    library_rows: list[dict[str, Any]],
    learner_v2_plus_frame: pl.DataFrame,
    learner_v2_frame: pl.DataFrame,
    *,
    source_model_names: set[str],
) -> dict[tuple[str, str, datetime], dict[str, Any]]:
    v2_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_frame.iter_rows(named=True)
    }
    result: dict[tuple[str, str, datetime], dict[str, Any]] = {}
    for learner_row in learner_v2_plus_frame.iter_rows(named=True):
        tenant_id = str(learner_row["tenant_id"])
        source_model_name = str(learner_row["source_model_name"])
        if source_model_name not in source_model_names:
            continue
        source_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
        ]
        v2_learner = v2_rows.get((tenant_id, source_model_name))
        if v2_learner is None:
            raise ValueError(f"missing V2 learner row for {tenant_id}/{source_model_name}")
        v2_profile = v2._profile_by_name(str(v2_learner["selected_weight_profile_name"]))
        selected_v2_rows = v2._select_rows_by_score(
            _base_candidate_rows(source_rows),
            profile=v2_profile,
        )
        selected_v2_plus_rows = _selected_rows_from_v2_plus_learner(
            source_rows,
            learner_row=learner_row,
            selected_v2_rows=selected_v2_rows,
        )
        for row in selected_v2_plus_rows:
            result[_tenant_source_anchor_key(row)] = row
    return result


def _selected_rows_from_v2_plus_learner(
    rows: list[dict[str, Any]],
    *,
    learner_row: dict[str, Any],
    selected_v2_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if bool(learner_row["fallback_to_v2"]):
        return selected_v2_rows
    v2_by_anchor = {
        _datetime_value(row["anchor_timestamp"]): row for row in selected_v2_rows
    }
    selected_rows: list[dict[str, Any]] = []
    for anchor, anchor_rows in sorted(_rows_by_anchor(rows).items()):
        plus_rows = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) in v2_plus.V2_PLUS_CANDIDATE_FAMILIES
        ]
        fallback = v2_by_anchor[anchor]
        selected_rows.append(
            min(
                [fallback, *plus_rows],
                key=lambda row: (
                    float(row["prior_family_mean_regret_uah"]),
                    str(row["candidate_family"]),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _label_row(
    row: dict[str, Any],
    *,
    baseline_row: dict[str, Any],
    best_row: dict[str, Any],
    audit_row: dict[str, Any] | None,
    tail_risk_delta_uah: float,
) -> dict[str, Any]:
    baseline_regret = float(baseline_row["regret_uah"])
    delta = float(row["regret_uah"]) - baseline_regret
    source = _candidate_source(row, baseline_row=baseline_row)
    best_family = str(best_row["candidate_family"])
    best_model = str(best_row["candidate_model_name"])
    is_best = _candidate_signature(row) == _candidate_signature(best_row)
    copied = dict(row)
    copied.update(
        {
            "candidate_source": source,
            "eligible_for_final_selection": _eligible_for_selection(row, source),
            "is_train_or_prior_anchor": str(row["split_name"]) != "final_holdout",
            "v2_plus_baseline_candidate_family": str(baseline_row["candidate_family"]),
            "v2_plus_baseline_candidate_model_name": str(
                baseline_row["candidate_model_name"]
            ),
            "v2_plus_baseline_regret_uah": baseline_regret,
            "label_regret_delta_vs_v2_plus_uah": delta,
            "label_safe_switch_win": delta < 0.0 and source != "v2_plus_default",
            "label_tail_risk_loss": delta >= tail_risk_delta_uah
            and source != "v2_plus_default",
            "label_best_candidate_family": best_family,
            "label_best_candidate_model_name": best_model,
            "label_is_anchor_best_candidate": is_best,
            "oracle_gap_class": str(
                (audit_row or {}).get("oracle_gap_class", "train_or_prior_label")
            ),
            "oracle_gap_teacher_class": _teacher_class(
                source=source,
                delta=delta,
                is_best=is_best,
                tail_risk_delta_uah=tail_risk_delta_uah,
            ),
            **_selector_features(row, baseline_row=baseline_row),
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "claim_scope": ORACLE_GAP_SAFE_SWITCH_LABEL_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _fit_safe_switch_for_scope(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
    min_prior_safe_win_count: int,
    min_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_predicted_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
    ridge_l2: float,
) -> dict[str, Any]:
    train_rows = [
        row
        for row in rows
        if str(row["split_name"]) != "final_holdout"
        and bool(row["eligible_for_final_selection"])
    ]
    final_rows = [
        row
        for row in rows
        if str(row["split_name"]) == "final_holdout"
        and bool(row["eligible_for_final_selection"])
    ]
    if not train_rows:
        raise ValueError(f"{tenant_id}/{source_model_name} safe switch needs train rows.")
    if not final_rows:
        raise ValueError(f"{tenant_id}/{source_model_name} safe switch needs final rows.")
    train_challengers = [
        row for row in train_rows if str(row["candidate_source"]) in allowed_candidate_sources
    ]
    if not train_challengers:
        raise ValueError(
            f"{tenant_id}/{source_model_name} safe switch needs challenger train rows."
        )
    profile_stats = _profile_stats(train_challengers, ridge_l2=ridge_l2)
    allowed_profiles = sorted(
        profile
        for profile, stats in profile_stats.items()
        if int(stats["safe_win_count"]) >= min_prior_safe_win_count
        and float(stats["mean_prior_delta_uah"]) <= -min_prior_mean_improvement_uah
        and float(stats["predicted_tail_risk_probability"])
        <= max_predicted_tail_risk_probability
    )
    selected_final: list[dict[str, Any]] = []
    fallback_anchor_keys: list[str] = []
    predicted_deltas: dict[str, float] = {}
    predicted_tail_risk: dict[str, float] = {}
    for anchor, anchor_rows in sorted(_rows_by_anchor(final_rows).items()):
        candidates: list[tuple[dict[str, Any], float, float]] = []
        for row in anchor_rows:
            profile = _profile_key(row)
            if (
                str(row["candidate_source"]) not in allowed_candidate_sources
                or profile not in allowed_profiles
            ):
                continue
            stats = profile_stats[profile]
            predicted_delta = float(stats["predicted_regret_delta_vs_v2_plus_uah"])
            tail_probability = float(stats["predicted_tail_risk_probability"])
            predicted_deltas[_candidate_key(row)] = predicted_delta
            predicted_tail_risk[_candidate_key(row)] = tail_probability
            if predicted_delta <= -min_predicted_improvement_uah:
                candidates.append((row, predicted_delta, tail_probability))
        if not candidates:
            fallback_anchor_keys.append(_anchor_key_from_parts(tenant_id, source_model_name, anchor))
            continue
        selected_final.append(
            min(
                candidates,
                key=lambda item: (
                    item[2],
                    item[1],
                    float(item[0]["selector_feature_schedule_distance_from_v2_plus"]),
                    str(item[0]["candidate_family"]),
                    str(item[0]["candidate_model_name"]),
                ),
            )[0]
        )
    selected_counts = _source_counts(selected_final)
    if fallback_anchor_keys:
        selected_counts["frozen_v2_plus_fallback"] = len(fallback_anchor_keys)
    selected_family_counts = _family_counts(selected_final)
    if fallback_anchor_keys:
        selected_family_counts["frozen_v2_plus_fallback"] = len(fallback_anchor_keys)
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "learner_model_name": ORACLE_GAP_SAFE_SWITCH_MODEL_NAME,
        "target_label_space": "schedule_candidate_index",
        "raw_hourly_action_imitation": False,
        "selected_scorer_type": "profile_shrunk_oracle_gap_safe_switch_v1",
        "selected_feature_names": list(ORACLE_GAP_SELECTOR_FEATURE_COLUMNS),
        "allowed_candidate_sources": sorted(allowed_candidate_sources),
        "allowed_risk_profiles": allowed_profiles,
        "risk_profile_prior_stats": profile_stats,
        "fallback_to_v2_plus": not selected_final,
        "uses_v2_plus_anchor_fallback": bool(fallback_anchor_keys),
        "selector_gate_blocker": (
            "oracle_gap_safe_switch_candidate_selected"
            if selected_final
            else "no_prior_safe_oracle_gap_profile"
        ),
        "min_predicted_improvement_uah": min_predicted_improvement_uah,
        "max_predicted_tail_risk_probability": max_predicted_tail_risk_probability,
        "train_anchor_count": _anchor_count(train_rows),
        "final_holdout_anchor_count": _anchor_count(final_rows),
        "fallback_final_anchor_keys": fallback_anchor_keys,
        "selected_final_candidate_keys": [_candidate_key(row) for row in selected_final],
        "selected_final_profile_keys": [_profile_key(row) for row in selected_final],
        "selected_final_family_counts": selected_family_counts,
        "selected_final_candidate_source_counts": selected_counts,
        "predicted_final_candidate_deltas": predicted_deltas,
        "predicted_final_tail_risk_probabilities": predicted_tail_risk,
        "claim_scope": ORACLE_GAP_SAFE_SWITCH_SCORER_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _rolling_summary_row(
    panel: pl.DataFrame,
    scorer: pl.DataFrame,
    *,
    source_model_name: str,
    window_index: int,
    validation_anchors: tuple[datetime, ...],
    prior_anchors: tuple[datetime, ...],
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    min_mean_regret_improvement_ratio_vs_strict: float,
) -> dict[str, Any]:
    rows = list(panel.iter_rows(named=True))
    scorer_rows = list(scorer.iter_rows(named=True))
    strict_rows = [
        row
        for row in rows
        if str(row["candidate_family"]) == v2.CANDIDATE_FAMILY_STRICT
        and str(row["split_name"]) == "final_holdout"
    ]
    v2_rows = [
        row
        for row in rows
        if str(row["candidate_source"]) == "v2_plus_default"
        and str(row["split_name"]) == "final_holdout"
    ]
    selected_rows = _selected_validation_rows(rows, scorer_rows)
    strict_mean = _mean_regret(strict_rows)
    v2_mean = _mean_regret(v2_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    v2_median = _median_regret(v2_rows)
    selected_median = _median_regret(selected_rows)
    improvement_vs_v2 = _improvement_ratio(v2_mean, selected_mean)
    improvement_vs_strict = _improvement_ratio(strict_mean, selected_mean)
    median_not_worse = selected_median <= v2_median
    window_passed = (
        improvement_vs_v2 >= min_mean_regret_improvement_ratio_vs_v2_plus
        and improvement_vs_strict >= min_mean_regret_improvement_ratio_vs_strict
        and median_not_worse
    )
    diagnostic_passed = improvement_vs_v2 > 0.0 and median_not_worse
    return {
        "source_model_name": source_model_name,
        "window_index": window_index,
        "tenant_count": len({str(row["tenant_id"]) for row in selected_rows}),
        "validation_anchor_count_per_tenant": len(validation_anchors),
        "validation_tenant_anchor_count": _anchor_count(selected_rows),
        "minimum_prior_anchor_count_before_window": len(prior_anchors),
        "strict_mean_regret_uah": strict_mean,
        "v2_plus_mean_regret_uah": v2_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "v2_plus_median_regret_uah": v2_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "median_not_worse_vs_v2_plus": median_not_worse,
        "rolling_window_passed": window_passed,
        "diagnostic_window_passed": diagnostic_passed,
        "fallback_row_count": sum(
            len(row["fallback_final_anchor_keys"]) for row in scorer_rows
        ),
        "selected_candidate_source_counts": _source_counts(selected_rows),
        "validation_window_anchor_start": min(validation_anchors).isoformat(),
        "validation_window_anchor_end": max(validation_anchors).isoformat(),
        "target_label_space": "schedule_candidate_index",
        "raw_hourly_action_imitation": False,
        "claim_scope": ORACLE_GAP_SAFE_SWITCH_ROBUSTNESS_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _selected_validation_rows(
    rows: list[dict[str, Any]],
    scorer_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key = {_candidate_key(row): row for row in rows}
    fallback_by_anchor = {
        _anchor_key(row): row
        for row in rows
        if str(row["candidate_source"]) == "v2_plus_default"
        and str(row["split_name"]) == "final_holdout"
    }
    selected: list[dict[str, Any]] = []
    for scorer_row in scorer_rows:
        for key in scorer_row["selected_final_candidate_keys"]:
            selected.append(by_key[str(key)])
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            selected.append(fallback_by_anchor[str(anchor_key)])
    return selected


def _reference_row(
    row: dict[str, Any],
    *,
    selection_role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    copied = dict(row)
    payload = dict(_payload(row))
    payload.update(
        {
            "oracle_gap_safe_switch_role": selection_role,
            "claim_scope": ORACLE_GAP_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    copied.update(
        {
            "selection_role": selection_role,
            "strategy_kind": ORACLE_GAP_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
            "generated_at": generated_at,
            "claim_scope": ORACLE_GAP_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "evaluation_payload": payload,
        }
    )
    return copied


def _candidate_benchmark_row(
    row: dict[str, Any],
    *,
    scorer_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(_payload(row))
    key = _candidate_key(row)
    payload.update(
        {
            "oracle_gap_safe_switch_selected": True,
            "selected_candidate_key": key,
            "predicted_regret_delta_vs_v2_plus_uah": dict(
                scorer_row["predicted_final_candidate_deltas"]
            ).get(key),
            "predicted_tail_risk_probability": dict(
                scorer_row["predicted_final_tail_risk_probabilities"]
            ).get(key),
            "claim_scope": ORACLE_GAP_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:oracle-gap-safe-switch:"
            f"{row['source_model_name']}:{row['candidate_family']}:"
            f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": str(row["source_model_name"]),
        "forecast_model_name": ORACLE_GAP_SAFE_SWITCH_MODEL_NAME,
        "strategy_kind": ORACLE_GAP_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _first_or_default(row["soc_fraction_vector"], 0.5),
        "starting_soc_source": "oracle_gap_safe_switch_feature_panel",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": v2._committed_action(row),
        "committed_power_mw": abs(_first_or_default(row["dispatch_mw_vector"], 0.0)),
        "rank_by_regret": 1,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": int(row["safety_violation_count"]),
        "selection_role": ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE,
        "selected_candidate_family": str(row["candidate_family"]),
        "selected_candidate_model_name": str(row["candidate_model_name"]),
        "fallback_to_v2_plus": False,
        "claim_scope": ORACLE_GAP_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "evaluation_payload": payload,
    }


def _fallback_benchmark_row(
    row: dict[str, Any],
    *,
    scorer_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    copied = _reference_row(
        row,
        selection_role=ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE,
        generated_at=generated_at,
    )
    payload = dict(copied["evaluation_payload"])
    payload.update(
        {
            "oracle_gap_safe_switch_selected": False,
            "fallback_to_corrected_v2_plus": True,
            "selector_gate_blocker": str(scorer_row["selector_gate_blocker"]),
        }
    )
    copied.update(
        {
            "forecast_model_name": ORACLE_GAP_SAFE_SWITCH_MODEL_NAME,
            "fallback_to_v2_plus": True,
            "evaluation_payload": payload,
        }
    )
    return copied


def _profile_stats(
    rows: list[dict[str, Any]],
    *,
    ridge_l2: float,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_profile_key(row), []).append(row)
    stats: dict[str, dict[str, Any]] = {}
    for profile, profile_rows in grouped.items():
        deltas = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in profile_rows]
        tail_losses = [bool(row["label_tail_risk_loss"]) for row in profile_rows]
        safe_wins = [bool(row["label_safe_switch_win"]) for row in profile_rows]
        count = len(profile_rows)
        raw_mean = mean(deltas)
        predicted_delta = sum(deltas) / (count + ridge_l2)
        predicted_tail = (sum(1 for value in tail_losses if value) + 1.0) / (
            count + 2.0
        )
        stats[profile] = {
            "candidate_source": str(profile_rows[0]["candidate_source"]),
            "candidate_family": str(profile_rows[0]["candidate_family"]),
            "candidate_model_name": str(profile_rows[0]["candidate_model_name"]),
            "train_row_count": count,
            "safe_win_count": sum(1 for value in safe_wins if value),
            "tail_loss_count": sum(1 for value in tail_losses if value),
            "safe_precision": (
                sum(1 for value in safe_wins if value) / count if count else 0.0
            ),
            "mean_prior_delta_uah": raw_mean,
            "predicted_regret_delta_vs_v2_plus_uah": predicted_delta,
            "predicted_tail_risk_probability": predicted_tail,
        }
    return stats


def _with_window_split(
    rows: list[dict[str, Any]],
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    validation_anchors: tuple[datetime, ...],
    prior_anchors: tuple[datetime, ...],
) -> pl.DataFrame:
    tenant_set = set(tenant_ids)
    validation_set = set(validation_anchors)
    prior_set = set(prior_anchors)
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        if str(row["tenant_id"]) not in tenant_set:
            continue
        if str(row["source_model_name"]) != source_model_name:
            continue
        anchor = _datetime_value(row["anchor_timestamp"])
        if anchor in validation_set:
            split_name = "final_holdout"
        elif anchor in prior_set:
            split_name = "train_selection"
        else:
            continue
        copied = dict(row)
        copied["split_name"] = split_name
        copied["is_train_or_prior_anchor"] = split_name != "final_holdout"
        output_rows.append(copied)
    return pl.DataFrame(output_rows, infer_schema_length=None)


def _rolling_windows(
    rows: list[dict[str, Any]],
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    validation_window_count: int,
    validation_anchor_count: int,
    min_prior_anchors_before_window: int,
) -> list[tuple[int, tuple[datetime, ...], tuple[datetime, ...]]]:
    anchors = sorted(
        {
            _datetime_value(row["anchor_timestamp"])
            for row in rows
            if str(row["source_model_name"]) == source_model_name
            and str(row["tenant_id"]) in set(tenant_ids)
        },
        reverse=True,
    )
    windows: list[tuple[int, tuple[datetime, ...], tuple[datetime, ...]]] = []
    for index in range(validation_window_count):
        start = index * validation_anchor_count
        end = start + validation_anchor_count
        validation = tuple(sorted(anchors[start:end]))
        if len(validation) != validation_anchor_count:
            raise ValueError(
                f"{source_model_name} lacks validation anchors for window {index + 1}; "
                f"expected {validation_anchor_count}, observed {len(validation)}"
            )
        prior = tuple(anchor for anchor in sorted(anchors) if anchor < min(validation))
        if len(prior) < min_prior_anchors_before_window:
            raise ValueError(
                f"{source_model_name} window {index + 1} needs at least "
                f"{min_prior_anchors_before_window} prior anchors; observed {len(prior)}"
            )
        windows.append((index + 1, validation, prior))
    return windows


def _validate_scorer_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    min_prior_safe_win_count: int,
    min_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_predicted_tail_risk_probability: float,
    allowed_candidate_sources: tuple[str, ...],
    ridge_l2: float,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must not be empty.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must not be empty.")
    if min_prior_safe_win_count < 1:
        raise ValueError("min_prior_safe_win_count must be at least 1.")
    if min_prior_mean_improvement_uah < 0.0:
        raise ValueError("min_prior_mean_improvement_uah must not be negative.")
    if min_predicted_improvement_uah < 0.0:
        raise ValueError("min_predicted_improvement_uah must not be negative.")
    if not 0.0 <= max_predicted_tail_risk_probability <= 1.0:
        raise ValueError(
            "max_predicted_tail_risk_probability must be between 0 and 1."
        )
    if not allowed_candidate_sources:
        raise ValueError("allowed_candidate_sources must not be empty.")
    if ridge_l2 < 0.0:
        raise ValueError("ridge_l2 must not be negative.")


def _validate_label_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, _REQUIRED_LABEL_COLUMNS, frame_name="oracle-gap label frame")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("oracle-gap label frame refuses market execution.")
    if frame.select(pl.col("raw_hourly_action_imitation").any()).item():
        raise ValueError("oracle-gap safe switch does not imitate raw hourly actions.")


def _validate_feature_panel(frame: pl.DataFrame) -> None:
    _validate_label_frame(frame)
    _require_columns(
        frame,
        frozenset({"feature_panel_version", "selected_feature_names"}),
        frame_name="oracle-gap feature panel",
    )


def _validate_scorer_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "selected_final_candidate_keys",
                "fallback_final_anchor_keys",
                "predicted_final_candidate_deltas",
                "predicted_final_tail_risk_probabilities",
                "raw_hourly_action_imitation",
                "market_execution_enabled",
            }
        ),
        frame_name="oracle-gap safe-switch scorer frame",
    )
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("oracle-gap scorer frame refuses market execution.")
    if frame.select(pl.col("raw_hourly_action_imitation").any()).item():
        raise ValueError("oracle-gap scorer does not imitate raw hourly actions.")


def _teacher_class(
    *,
    source: str,
    delta: float,
    is_best: bool,
    tail_risk_delta_uah: float,
) -> str:
    if source == "v2_plus_default":
        return "v2_plus_reference" if delta > 0.0 else "v2_plus_best"
    if delta >= tail_risk_delta_uah:
        return "tail_risk_loss"
    if delta < 0.0 and is_best:
        return "safe_switch_win"
    if delta < 0.0:
        return "safe_switch_candidate"
    return "neutral_or_weak_switch"


def _selector_features(
    row: dict[str, Any],
    *,
    baseline_row: dict[str, Any],
) -> dict[str, float]:
    dispatch = _float_list(row["dispatch_mw_vector"])
    baseline_dispatch = _float_list(baseline_row["dispatch_mw_vector"])
    soc = _float_list(row["soc_fraction_vector"])
    baseline_soc = _float_list(baseline_row["soc_fraction_vector"])
    candidate_source = _candidate_source(row, baseline_row=baseline_row)
    return {
        "selector_feature_schedule_distance_from_v2_plus": _schedule_distance(
            dispatch,
            baseline_dispatch,
        ),
        "selector_feature_total_throughput_delta_mwh": float(row["total_throughput_mwh"])
        - float(baseline_row["total_throughput_mwh"]),
        "selector_feature_terminal_soc_delta_fraction": (
            (soc[-1] if soc else 0.5) - (baseline_soc[-1] if baseline_soc else 0.5)
        ),
        "selector_feature_forecast_spread_uah_mwh": _spread(
            _float_list(row["forecast_price_uah_mwh_vector"])
        ),
        "selector_feature_total_degradation_penalty_uah": float(
            row["total_degradation_penalty_uah"]
        ),
        "selector_feature_candidate_family_sort_index": float(
            v2._family_sort_index(str(row["candidate_family"]))
        ),
        "selector_feature_poland_shadow_candidate": float(
            candidate_source == "poland_shadow_candidate"
        ),
        "selector_feature_tft_shadow_candidate": float(
            candidate_source == "tft_shadow_candidate"
        ),
        "selector_feature_cross_model_disagreement": float(
            candidate_source in {"poland_shadow_candidate", "tft_shadow_candidate"}
        ),
    }


def _candidate_source(row: dict[str, Any], *, baseline_row: dict[str, Any]) -> str:
    if _candidate_signature(row) == _candidate_signature(baseline_row):
        return "v2_plus_default"
    raw = " ".join(
        [
            str(row.get("source_model_name", "")),
            str(row.get("candidate_family", "")),
            str(row.get("candidate_model_name", "")),
        ]
    ).lower()
    if "poland" in raw or "lag24" in raw or "entsoe" in raw:
        return "poland_shadow_candidate"
    if "tft" in raw or "quantile" in raw:
        return "tft_shadow_candidate"
    if str(row["candidate_family"]) == v2.CANDIDATE_FAMILY_STRICT:
        return "strict_fallback"
    return "oracle_gap_candidate"


def _eligible_for_selection(row: dict[str, Any], source: str) -> bool:
    if source in {"strict_fallback", "v2_plus_default"}:
        return True
    family = str(row["candidate_family"]).lower()
    return "oracle_neighborhood" not in family and "diagnostic" not in family


def _source_model_names(frame: pl.DataFrame) -> tuple[str, ...]:
    return tuple(sorted(str(value) for value in frame["source_model_name"].unique()))


def _best_candidate_rows_by_anchor(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str, datetime], dict[str, Any]]:
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_tenant_source_anchor_key(row), []).append(row)
    return {
        key: min(
            anchor_rows,
            key=lambda row: (
                float(row["regret_uah"]),
                v2._family_sort_index(str(row["candidate_family"])),
                str(row["candidate_model_name"]),
            ),
        )
        for key, anchor_rows in grouped.items()
    }


def _base_candidate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if str(row["candidate_family"]) not in v2_plus.V2_PLUS_CANDIDATE_FAMILIES
    ]


def _rows_by_anchor(rows: list[dict[str, Any]]) -> dict[datetime, list[dict[str, Any]]]:
    grouped: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_datetime_value(row["anchor_timestamp"]), []).append(row)
    return grouped


def _tenant_source_anchor_key(row: dict[str, Any]) -> tuple[str, str, datetime]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _candidate_signature(row: dict[str, Any]) -> tuple[str, str, str, datetime]:
    return (
        str(row["source_model_name"]),
        str(row["candidate_family"]),
        str(row["candidate_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _candidate_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"]).isoformat(),
            str(row["candidate_family"]),
            str(row["candidate_model_name"]),
        ]
    )


def _anchor_key(row: dict[str, Any]) -> str:
    return _anchor_key_from_parts(
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _strict_anchor_key(row: dict[str, Any]) -> str:
    return _anchor_key_from_parts(
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _anchor_key_from_parts(tenant_id: str, source_model_name: str, anchor: datetime) -> str:
    return "|".join([tenant_id, source_model_name, anchor.isoformat()])


def _profile_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row["candidate_source"]),
            str(row["candidate_family"]),
            str(row["candidate_model_name"]),
        ]
    )


def _role_summaries(frame: pl.DataFrame) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for role in sorted(str(value) for value in frame["selection_role"].unique()):
        role_rows = frame.filter(pl.col("selection_role") == role).to_dicts()
        summaries[role] = {
            "row_count": len(role_rows),
            "tenant_anchor_count": len(
                {
                    (
                        str(row["tenant_id"]),
                        str(row["source_model_name"]),
                        _datetime_value(row["anchor_timestamp"]),
                    )
                    for row in role_rows
                }
            ),
            "mean_regret_uah": _mean_regret(role_rows),
            "median_regret_uah": _median_regret(role_rows),
        }
    return summaries


def _tenant_anchor_count(frame: pl.DataFrame) -> int:
    return len(
        {
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                _datetime_value(row["anchor_timestamp"]),
            )
            for row in frame.iter_rows(named=True)
        }
    )


def _anchor_count(rows: list[dict[str, Any]]) -> int:
    return len(
        {
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                _datetime_value(row["anchor_timestamp"]),
            )
            for row in rows
        }
    )


def _source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("candidate_source", "unknown_source"))
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["candidate_family"])
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    return mean(float(row["regret_uah"]) for row in rows) if rows else 0.0


def _median_regret(rows: list[dict[str, Any]]) -> float:
    return median(float(row["regret_uah"]) for row in rows) if rows else 0.0


def _improvement_ratio(baseline: float, challenger: float) -> float:
    return (baseline - challenger) / abs(baseline) if abs(baseline) > 1e-9 else 0.0


def _schedule_distance(left: list[float], right: list[float]) -> float:
    width = max(len(left), len(right), 1)
    padded_left = left + [0.0] * (width - len(left))
    padded_right = right + [0.0] * (width - len(right))
    return sum(abs(a - b) for a, b in zip(padded_left, padded_right)) / width


def _spread(values: list[float]) -> float:
    return max(values) - min(values) if values else 0.0


def _first_or_default(values: Any, default: float) -> float:
    parsed = _float_list(values)
    return parsed[0] if parsed else default


def _float_list(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, list | tuple):
        return [float(item) for item in value]
    if isinstance(value, pl.Series):
        return [float(item) for item in value.to_list()]
    return [float(value)]


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload", {})
    return dict(payload) if isinstance(payload, dict) else {}


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    return max(_datetime_value(row["generated_at"]) for row in frame.iter_rows(named=True))


def _require_columns(frame: pl.DataFrame, columns: frozenset[str], *, frame_name: str) -> None:
    missing = sorted(columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")


__all__ = [
    "ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE",
    "ORACLE_GAP_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND",
    "build_dfl_oracle_gap_safe_switch_feature_panel_frame",
    "build_dfl_oracle_gap_safe_switch_label_frame",
    "build_dfl_oracle_gap_safe_switch_rolling_robustness_frame",
    "build_dfl_oracle_gap_safe_switch_scorer_frame",
    "build_dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame",
    "evaluate_dfl_oracle_gap_safe_switch_gate",
]
