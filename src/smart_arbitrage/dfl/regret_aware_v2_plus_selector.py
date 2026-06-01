"""Regret-aware V2+ fallback selector for DT-shadow candidate rows.

The selector is intentionally small and research-only. It trains on
candidate-level teacher rows using regret delta versus V2+ as the target, then
selects a non-V2+ candidate only when the predicted improvement clears an
explicit threshold. Otherwise it abstains back to the frozen V2+ comparator.
"""

from __future__ import annotations

import ast
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
import json
import math
from pathlib import Path
from typing import Any, Final, cast

import numpy as np
import polars as pl

REGRET_AWARE_V2_PLUS_SELECTOR_CLAIM_SCOPE: Final[str] = (
    "regret_aware_v2_plus_selector_shadow_not_promotable_not_market_execution"
)
REGRET_AWARE_V2_PLUS_SELECTOR_ARTIFACT_PREFIX: Final[str] = (
    "regret_aware_v2_plus_selector"
)
SELECTED_ROWS_CSV_NAME: Final[str] = (
    f"{REGRET_AWARE_V2_PLUS_SELECTOR_ARTIFACT_PREFIX}_selected_rows.csv"
)
SUMMARY_JSON_NAME: Final[str] = (
    f"{REGRET_AWARE_V2_PLUS_SELECTOR_ARTIFACT_PREFIX}_summary.json"
)
SUMMARY_MD_NAME: Final[str] = (
    f"{REGRET_AWARE_V2_PLUS_SELECTOR_ARTIFACT_PREFIX}_summary.md"
)
SAMPLE_WEIGHT_SCALE_UAH: Final[float] = 100.0
LOSS_FUNCTION_NAME: Final[str] = "weighted_ridge_regret_delta_vs_v2_plus"
LOSS_FUNCTION_HIST_GRADIENT_BOOSTING: Final[str] = (
    "hist_gradient_boosting_regret_delta_vs_v2_plus"
)
LOSS_FUNCTION_RANDOM_FOREST: Final[str] = (
    "random_forest_regret_delta_vs_v2_plus"
)
SAMPLE_WEIGHT_FORMULA: Final[str] = "1 + abs(regret_delta_vs_v2_plus_uah) / 100"
MODEL_KIND_WEIGHTED_RIDGE: Final[str] = "weighted_ridge"
MODEL_KIND_HIST_GRADIENT_BOOSTING: Final[str] = "hist_gradient_boosting"
MODEL_KIND_RANDOM_FOREST: Final[str] = "random_forest"
FEATURE_SET_BASE: Final[str] = "base_prior_context"
FEATURE_SET_EXPANDED: Final[str] = "expanded_prior_context_v1"
V2_PLUS_FAMILY_ALIASES: Final[frozenset[str]] = frozenset(
    {
        "schedule_value_learner_v2_plus",
        "schedule_value_learner_v2_plus_reference",
        "frozen_v2_plus_fallback",
    }
)
BASE_NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    "selector_feature_forecast_spread_uah_mwh",
    "selector_feature_terminal_soc_delta_fraction",
    "selector_feature_total_throughput_delta_mwh",
    "selector_feature_total_degradation_penalty_uah",
    "selector_feature_soc_min_slack_fraction",
    "selector_feature_candidate_index",
    "selector_feature_candidate_count",
    "selector_feature_anchor_hour",
)
FAMILY_INTERACTION_FEATURES: Final[tuple[str, ...]] = (
    "selector_feature_forecast_spread_uah_mwh",
    "selector_feature_terminal_soc_delta_fraction",
    "selector_feature_total_throughput_delta_mwh",
    "selector_feature_total_degradation_penalty_uah",
)
EXPANDED_NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    "selector_feature_anchor_day_of_year",
    "selector_feature_anchor_month",
    "selector_feature_anchor_day_of_month",
    "selector_feature_forecast_mean_uah_mwh",
    "selector_feature_forecast_std_uah_mwh",
    "selector_feature_forecast_range_uah_mwh",
    "selector_feature_forecast_top3_mean_uah_mwh",
    "selector_feature_forecast_bottom3_mean_uah_mwh",
    "selector_feature_forecast_peak_hour",
    "selector_feature_forecast_trough_hour",
    "selector_feature_forecast_peak_trough_gap_hours",
    "selector_feature_forecast_objective_uah",
    "selector_feature_dispatch_dot_centered_forecast",
    "selector_feature_charge_hour_count",
    "selector_feature_discharge_hour_count",
    "selector_feature_first_charge_hour",
    "selector_feature_first_discharge_hour",
)
FORBIDDEN_FEATURE_TOKENS: Final[tuple[str, ...]] = (
    "actual",
    "decision_value",
    "label",
    "oracle",
    "regret",
    "return_to_go",
    "schedule_value",
)
REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "dt_candidate_id_target",
        "dt_candidate_index_target",
        "dt_schedule_family_target",
        "regret_uah",
        "regret_delta_vs_v2_plus_uah",
        "schedule_value_uah",
        "not_full_dfl",
        "not_market_execution",
        "market_execution_enabled",
        "promotion_gate_passed",
        "market_execution_gate_passed",
        "raw_hourly_action_imitation",
    }
)


def build_regret_aware_v2_plus_selector_packet(
    teacher_rows_frame: pl.DataFrame,
    *,
    run_slug: str,
    min_predicted_improvement_uah: float = 150.0,
    tail_risk_loss_threshold_uah: float = 150.0,
    max_family_tail_risk_probability: float = 0.5,
    ridge_l2: float = 10.0,
    model_kind: str = MODEL_KIND_WEIGHTED_RIDGE,
    feature_set: str = FEATURE_SET_BASE,
    random_seed: int = 1,
) -> dict[str, Any]:
    """Train/evaluate a regret-aware selector with explicit V2+ abstention."""

    _validate_config(
        run_slug=run_slug,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
        ridge_l2=ridge_l2,
        model_kind=model_kind,
        feature_set=feature_set,
        random_seed=random_seed,
    )
    frame = _normalized_teacher_rows(teacher_rows_frame)
    train_rows = [
        row for row in frame.iter_rows(named=True) if str(row["split_name"]) == "train_selection"
    ]
    final_rows = [
        row for row in frame.iter_rows(named=True) if str(row["split_name"]) == "final_holdout"
    ]
    if not train_rows:
        raise ValueError("regret-aware V2+ selector requires train_selection rows.")
    if not final_rows:
        raise ValueError("regret-aware V2+ selector requires final_holdout rows.")

    family_names = tuple(
        sorted({str(row["dt_schedule_family_target"]) for row in [*train_rows, *final_rows]})
    )
    feature_names = _feature_names(
        family_names,
        feature_set=feature_set,
        model_kind=model_kind,
    )
    leakage = _feature_leakage_guard(feature_names)
    if leakage["uses_realized_regret_as_feature"]:
        raise ValueError("regret-aware V2+ selector feature set contains label columns.")

    model = _fit_model(
        rows=train_rows,
        feature_names=feature_names,
        family_names=family_names,
        ridge_l2=ridge_l2,
        model_kind=model_kind,
        random_seed=random_seed,
    )
    family_tail_risk = _family_tail_risk(
        train_rows,
        tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
    )
    selected_rows = _select_final_rows(
        final_rows,
        model=model,
        feature_names=feature_names,
        family_names=family_names,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
        family_tail_risk=family_tail_risk,
    )
    selected_frame = pl.DataFrame(selected_rows, infer_schema_length=None).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp"]
    )
    summary = _summary(
        run_slug=run_slug,
        frame=frame,
        train_rows=train_rows,
        final_rows=final_rows,
        selected_rows=selected_rows,
        model=model,
        feature_names=feature_names,
        leakage=leakage,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
        ridge_l2=ridge_l2,
        family_tail_risk=family_tail_risk,
        model_kind=model_kind,
        feature_set=feature_set,
        random_seed=random_seed,
    )
    return {"selected_rows": selected_frame, "summary": summary, "model": model}


def write_regret_aware_v2_plus_selector_packet(
    *,
    output_dir: Path,
    result: Mapping[str, Any],
) -> dict[str, Path]:
    """Write selected rows, JSON summary, and markdown summary."""

    output_dir.mkdir(parents=True, exist_ok=True)
    selected_rows = result["selected_rows"]
    summary = dict(result["summary"])
    if not isinstance(selected_rows, pl.DataFrame):
        raise TypeError("result['selected_rows'] must be a Polars DataFrame.")
    selected_csv_path = output_dir / SELECTED_ROWS_CSV_NAME
    summary_json_path = output_dir / SUMMARY_JSON_NAME
    summary_md_path = output_dir / SUMMARY_MD_NAME
    selected_rows.write_csv(selected_csv_path)
    summary_json_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_md_path.write_text(_summary_markdown(summary), encoding="utf-8")
    return {
        "selected_rows_csv": selected_csv_path,
        "summary_json": summary_json_path,
        "summary_markdown": summary_md_path,
    }


def _validate_config(
    *,
    run_slug: str,
    min_predicted_improvement_uah: float,
    tail_risk_loss_threshold_uah: float,
    max_family_tail_risk_probability: float,
    ridge_l2: float,
    model_kind: str,
    feature_set: str,
    random_seed: int,
) -> None:
    if not run_slug:
        raise ValueError("run_slug must be non-empty.")
    if min_predicted_improvement_uah < 0.0:
        raise ValueError("min_predicted_improvement_uah must not be negative.")
    if tail_risk_loss_threshold_uah <= 0.0:
        raise ValueError("tail_risk_loss_threshold_uah must be positive.")
    if not 0.0 <= max_family_tail_risk_probability <= 1.0:
        raise ValueError("max_family_tail_risk_probability must be in [0, 1].")
    if ridge_l2 < 0.0:
        raise ValueError("ridge_l2 must not be negative.")
    if model_kind not in {
        MODEL_KIND_WEIGHTED_RIDGE,
        MODEL_KIND_HIST_GRADIENT_BOOSTING,
        MODEL_KIND_RANDOM_FOREST,
    }:
        raise ValueError(f"unsupported regret-aware selector model_kind: {model_kind}")
    if feature_set not in {FEATURE_SET_BASE, FEATURE_SET_EXPANDED}:
        raise ValueError(f"unsupported regret-aware selector feature_set: {feature_set}")
    if isinstance(random_seed, bool) or not isinstance(random_seed, int):
        raise ValueError("random_seed must be an integer.")
    if random_seed < 0:
        raise ValueError("random_seed must be non-negative.")


def _normalized_teacher_rows(frame: pl.DataFrame) -> pl.DataFrame:
    missing = sorted(REQUIRED_COLUMNS.difference(frame.columns))
    if missing:
        raise ValueError(f"regret-aware V2+ selector rows missing columns: {missing}")
    if frame.height == 0:
        raise ValueError("regret-aware V2+ selector rows cannot be empty.")
    if _frame_has_true(frame, "market_execution_enabled") or _frame_has_true(
        frame,
        "market_execution_gate_passed",
    ):
        raise ValueError("regret-aware V2+ selector refuses market execution rows.")
    if _frame_has_true(frame, "promotion_gate_passed"):
        raise ValueError("regret-aware V2+ selector refuses promoted rows.")
    if _frame_has_true(frame, "raw_hourly_action_imitation"):
        raise ValueError("regret-aware V2+ selector refuses raw hourly action rows.")
    if not _frame_all_true(frame, "not_full_dfl"):
        raise ValueError("regret-aware V2+ selector requires not_full_dfl=true.")
    if not _frame_all_true(frame, "not_market_execution"):
        raise ValueError("regret-aware V2+ selector requires not_market_execution=true.")
    return frame.with_columns(
        pl.col("anchor_timestamp").cast(pl.Datetime, strict=False),
        pl.lit(False).alias("market_execution_enabled"),
        pl.lit(False).alias("dt_lava_ready"),
        pl.lit(False).alias("permits_model_training"),
        pl.lit(True).alias("research_shadow_not_promotable"),
    ).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "dt_candidate_index_target",
        ]
    )


def _feature_names(
    family_names: Sequence[str],
    *,
    feature_set: str,
    model_kind: str,
) -> list[str]:
    names = list(BASE_NUMERIC_FEATURES)
    if feature_set == FEATURE_SET_EXPANDED:
        names.extend(EXPANDED_NUMERIC_FEATURES)
    for family in family_names:
        names.append(f"candidate_family={family}")
    if model_kind == MODEL_KIND_WEIGHTED_RIDGE:
        for family in family_names:
            for feature in FAMILY_INTERACTION_FEATURES:
                names.append(f"{feature}*candidate_family={family}")
    return names


def _feature_values(
    row: Mapping[str, Any],
    *,
    feature_names: Sequence[str],
    family_names: Sequence[str],
) -> list[float]:
    base = _base_feature_values(row)
    family = str(row["dt_schedule_family_target"])
    values: list[float] = []
    for name in feature_names:
        if name in base:
            values.append(base[name])
        elif name.startswith("candidate_family="):
            values.append(1.0 if name.removeprefix("candidate_family=") == family else 0.0)
        elif "*candidate_family=" in name:
            feature_name, family_name = name.split("*candidate_family=", 1)
            values.append(base.get(feature_name, 0.0) if family_name == family else 0.0)
        else:  # pragma: no cover - protects future feature additions.
            raise KeyError(f"unknown regret-aware selector feature: {name}")
    del family_names
    return values


def _base_feature_values(row: Mapping[str, Any]) -> dict[str, float]:
    anchor = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    forecast_values = _vector(row.get("forecast_price_uah_mwh_vector"))
    dispatch_values = _vector(row.get("dispatch_mw_vector"))
    forecast_summary = _forecast_summary(forecast_values)
    dispatch_summary = _dispatch_summary(
        forecast_values=forecast_values,
        dispatch_values=dispatch_values,
    )
    values = {
        "selector_feature_forecast_spread_uah_mwh": _feature_float(
            row,
            "selector_feature_forecast_spread_uah_mwh",
            fallback=_feature_float(row, "forecast_spread_uah_mwh"),
        ),
        "selector_feature_terminal_soc_delta_fraction": _feature_float(
            row,
            "selector_feature_terminal_soc_delta_fraction",
            fallback=_terminal_soc_delta(row.get("soc_fraction_vector")),
        ),
        "selector_feature_total_throughput_delta_mwh": _feature_float(
            row,
            "selector_feature_total_throughput_delta_mwh",
            fallback=_feature_float(row, "total_throughput_mwh"),
        ),
        "selector_feature_total_degradation_penalty_uah": _feature_float(
            row,
            "selector_feature_total_degradation_penalty_uah",
            fallback=_feature_float(row, "total_degradation_penalty_uah"),
        ),
        "selector_feature_soc_min_slack_fraction": _feature_float(
            row,
            "soc_min_slack_fraction",
        ),
        "selector_feature_candidate_index": _feature_float(
            row,
            "dt_candidate_index_target",
        ),
        "selector_feature_candidate_count": _feature_float(
            row,
            "teacher_anchor_candidate_count",
            fallback=1.0,
        ),
        "selector_feature_anchor_hour": float(anchor.hour),
        "selector_feature_anchor_day_of_year": float(anchor.timetuple().tm_yday),
        "selector_feature_anchor_month": float(anchor.month),
        "selector_feature_anchor_day_of_month": float(anchor.day),
        "selector_feature_forecast_mean_uah_mwh": forecast_summary["mean"],
        "selector_feature_forecast_std_uah_mwh": forecast_summary["std"],
        "selector_feature_forecast_range_uah_mwh": forecast_summary["range"],
        "selector_feature_forecast_top3_mean_uah_mwh": forecast_summary["top3_mean"],
        "selector_feature_forecast_bottom3_mean_uah_mwh": forecast_summary[
            "bottom3_mean"
        ],
        "selector_feature_forecast_peak_hour": forecast_summary["peak_hour"],
        "selector_feature_forecast_trough_hour": forecast_summary["trough_hour"],
        "selector_feature_forecast_peak_trough_gap_hours": (
            forecast_summary["peak_hour"] - forecast_summary["trough_hour"]
        ),
        "selector_feature_forecast_objective_uah": _feature_float(
            row,
            "forecast_objective_value_uah",
        ),
        "selector_feature_dispatch_dot_centered_forecast": dispatch_summary[
            "dispatch_dot_centered_forecast"
        ],
        "selector_feature_charge_hour_count": dispatch_summary["charge_hour_count"],
        "selector_feature_discharge_hour_count": dispatch_summary[
            "discharge_hour_count"
        ],
        "selector_feature_first_charge_hour": dispatch_summary["first_charge_hour"],
        "selector_feature_first_discharge_hour": dispatch_summary[
            "first_discharge_hour"
        ],
    }
    return values


def _forecast_summary(values: Sequence[float]) -> dict[str, float]:
    if not values:
        return {
            "mean": 0.0,
            "std": 0.0,
            "range": 0.0,
            "top3_mean": 0.0,
            "bottom3_mean": 0.0,
            "peak_hour": 0.0,
            "trough_hour": 0.0,
        }
    array = np.asarray(values, dtype=float)
    ordered = np.sort(array)
    top = ordered[-3:] if ordered.size >= 3 else ordered
    bottom = ordered[:3] if ordered.size >= 3 else ordered
    return {
        "mean": float(array.mean()),
        "std": float(array.std()),
        "range": float(array.max() - array.min()),
        "top3_mean": float(top.mean()),
        "bottom3_mean": float(bottom.mean()),
        "peak_hour": float(array.argmax()),
        "trough_hour": float(array.argmin()),
    }


def _dispatch_summary(
    *,
    forecast_values: Sequence[float],
    dispatch_values: Sequence[float],
) -> dict[str, float]:
    if not dispatch_values:
        return {
            "dispatch_dot_centered_forecast": 0.0,
            "charge_hour_count": 0.0,
            "discharge_hour_count": 0.0,
            "first_charge_hour": -1.0,
            "first_discharge_hour": -1.0,
        }
    dispatch = np.asarray(dispatch_values, dtype=float)
    forecast = np.asarray(forecast_values, dtype=float)
    if forecast.size:
        centered = forecast - float(forecast.mean())
        usable = min(centered.size, dispatch.size)
        dispatch_dot = float((centered[:usable] * dispatch[:usable]).sum())
    else:
        dispatch_dot = 0.0
    charge_hours = [index for index, value in enumerate(dispatch) if value < -1e-6]
    discharge_hours = [index for index, value in enumerate(dispatch) if value > 1e-6]
    return {
        "dispatch_dot_centered_forecast": dispatch_dot,
        "charge_hour_count": float(len(charge_hours)),
        "discharge_hour_count": float(len(discharge_hours)),
        "first_charge_hour": float(min(charge_hours)) if charge_hours else -1.0,
        "first_discharge_hour": (
            float(min(discharge_hours)) if discharge_hours else -1.0
        ),
    }


def _fit_model(
    *,
    rows: Sequence[Mapping[str, Any]],
    feature_names: Sequence[str],
    family_names: Sequence[str],
    ridge_l2: float,
    model_kind: str,
    random_seed: int,
) -> dict[str, Any]:
    if model_kind == MODEL_KIND_WEIGHTED_RIDGE:
        return _fit_weighted_ridge(
            rows=rows,
            feature_names=feature_names,
            family_names=family_names,
            ridge_l2=ridge_l2,
        )
    if model_kind == MODEL_KIND_HIST_GRADIENT_BOOSTING:
        return _fit_hist_gradient_boosting(
            rows=rows,
            feature_names=feature_names,
            family_names=family_names,
            ridge_l2=ridge_l2,
            random_seed=random_seed,
        )
    return _fit_random_forest(
        rows=rows,
        feature_names=feature_names,
        family_names=family_names,
        random_seed=random_seed,
    )


def _fit_weighted_ridge(
    *,
    rows: Sequence[Mapping[str, Any]],
    feature_names: Sequence[str],
    family_names: Sequence[str],
    ridge_l2: float,
) -> dict[str, Any]:
    x = np.asarray(
        [
            _feature_values(row, feature_names=feature_names, family_names=family_names)
            for row in rows
        ],
        dtype=float,
    )
    y = np.asarray([_target(row) for row in rows], dtype=float)
    weights = np.asarray([_sample_weight(row) for row in rows], dtype=float)
    means = x.mean(axis=0)
    scales = x.std(axis=0)
    scales = np.where(scales == 0.0, 1.0, scales)
    x_scaled = (x - means) / scales
    x_augmented = np.column_stack([np.ones(x_scaled.shape[0]), x_scaled])
    sqrt_weights = np.sqrt(weights)
    weighted_x = x_augmented * sqrt_weights[:, None]
    weighted_y = y * sqrt_weights
    penalty = np.eye(x_augmented.shape[1]) * ridge_l2
    penalty[0, 0] = 0.0
    coefficients = np.linalg.pinv(weighted_x.T @ weighted_x + penalty) @ (
        weighted_x.T @ weighted_y
    )
    predictions = x_augmented @ coefficients
    residuals = y - predictions
    weighted_mse = float(np.average(residuals * residuals, weights=weights))
    return {
        "model_type": "weighted_ridge_regression",
        "target": "regret_delta_vs_v2_plus_uah",
        "feature_names": list(feature_names),
        "feature_means": means.tolist(),
        "feature_scales": scales.tolist(),
        "coefficients": coefficients.tolist(),
        "ridge_l2": ridge_l2,
        "train_weighted_mse": weighted_mse,
        "train_weighted_rmse_uah": math.sqrt(weighted_mse),
        "train_row_count": len(rows),
        "min_sample_weight": float(weights.min()),
        "max_sample_weight": float(weights.max()),
        "mean_sample_weight": float(weights.mean()),
    }


def _fit_hist_gradient_boosting(
    *,
    rows: Sequence[Mapping[str, Any]],
    feature_names: Sequence[str],
    family_names: Sequence[str],
    ridge_l2: float,
    random_seed: int,
) -> dict[str, Any]:
    try:
        from sklearn.ensemble import HistGradientBoostingRegressor
    except ImportError as exc:  # pragma: no cover - environment guard.
        raise RuntimeError(
            "hist_gradient_boosting selector requires scikit-learn."
        ) from exc
    x = np.asarray(
        [
            _feature_values(row, feature_names=feature_names, family_names=family_names)
            for row in rows
        ],
        dtype=float,
    )
    y = np.asarray([_target(row) for row in rows], dtype=float)
    weights = np.asarray([_sample_weight(row) for row in rows], dtype=float)
    estimator = HistGradientBoostingRegressor(
        max_leaf_nodes=8,
        learning_rate=0.05,
        max_iter=200,
        l2_regularization=ridge_l2,
        random_state=random_seed,
    )
    estimator.fit(x, y, sample_weight=weights)
    predictions = np.asarray(estimator.predict(x), dtype=float)
    residuals = y - predictions
    weighted_mse = float(np.average(residuals * residuals, weights=weights))
    return {
        "model_type": "hist_gradient_boosting_regression",
        "target": "regret_delta_vs_v2_plus_uah",
        "feature_names": list(feature_names),
        "estimator": estimator,
        "ridge_l2": ridge_l2,
        "train_weighted_mse": weighted_mse,
        "train_weighted_rmse_uah": math.sqrt(weighted_mse),
        "train_row_count": len(rows),
        "min_sample_weight": float(weights.min()),
        "max_sample_weight": float(weights.max()),
        "mean_sample_weight": float(weights.mean()),
    }


def _fit_random_forest(
    *,
    rows: Sequence[Mapping[str, Any]],
    feature_names: Sequence[str],
    family_names: Sequence[str],
    random_seed: int,
) -> dict[str, Any]:
    try:
        from sklearn.ensemble import RandomForestRegressor
    except ImportError as exc:  # pragma: no cover - environment guard.
        raise RuntimeError("random_forest selector requires scikit-learn.") from exc
    x = np.asarray(
        [
            _feature_values(row, feature_names=feature_names, family_names=family_names)
            for row in rows
        ],
        dtype=float,
    )
    y = np.asarray([_target(row) for row in rows], dtype=float)
    weights = np.asarray([_sample_weight(row) for row in rows], dtype=float)
    estimator = RandomForestRegressor(
        n_estimators=500,
        max_depth=6,
        min_samples_leaf=1,
        random_state=random_seed,
    )
    estimator.fit(x, y, sample_weight=weights)
    predictions = np.asarray(estimator.predict(x), dtype=float)
    residuals = y - predictions
    weighted_mse = float(np.average(residuals * residuals, weights=weights))
    return {
        "model_type": "random_forest_regression",
        "target": "regret_delta_vs_v2_plus_uah",
        "feature_names": list(feature_names),
        "estimator": estimator,
        "ridge_l2": 0.0,
        "train_weighted_mse": weighted_mse,
        "train_weighted_rmse_uah": math.sqrt(weighted_mse),
        "train_row_count": len(rows),
        "min_sample_weight": float(weights.min()),
        "max_sample_weight": float(weights.max()),
        "mean_sample_weight": float(weights.mean()),
    }


def _predict_delta(
    row: Mapping[str, Any],
    *,
    model: Mapping[str, Any],
    feature_names: Sequence[str],
    family_names: Sequence[str],
) -> float:
    values = np.asarray(
        _feature_values(row, feature_names=feature_names, family_names=family_names),
        dtype=float,
    )
    if model.get("model_type") in {
        "hist_gradient_boosting_regression",
        "random_forest_regression",
    }:
        estimator = model["estimator"]
        return float(cast(Any, estimator).predict(values.reshape(1, -1))[0])
    means = np.asarray(model["feature_means"], dtype=float)
    scales = np.asarray(model["feature_scales"], dtype=float)
    coefficients = np.asarray(model["coefficients"], dtype=float)
    scaled = (values - means) / scales
    return float(np.concatenate([[1.0], scaled]) @ coefficients)


def _select_final_rows(
    final_rows: Sequence[Mapping[str, Any]],
    *,
    model: Mapping[str, Any],
    feature_names: Sequence[str],
    family_names: Sequence[str],
    min_predicted_improvement_uah: float,
    max_family_tail_risk_probability: float,
    family_tail_risk: Mapping[str, Mapping[str, float]],
) -> list[dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    for _, anchor_rows in _group_anchor_rows(final_rows):
        v2_row = _v2_plus_row(anchor_rows)
        if v2_row is None:
            raise ValueError("regret-aware V2+ selector requires a V2+ fallback row per final anchor.")
        scored = []
        for row in anchor_rows:
            predicted_delta = _predict_delta(
                row,
                model=model,
                feature_names=feature_names,
                family_names=family_names,
            )
            scored.append((row, predicted_delta))
        non_v2_scored = [
            (row, predicted_delta)
            for row, predicted_delta in scored
            if not _is_v2_plus_family(str(row["dt_schedule_family_target"]))
            and int(_feature_float(row, "safety_violation_count")) == 0
        ]
        if not non_v2_scored:
            selected_rows.append(
                _selected_row(
                    selected=v2_row,
                    v2_row=v2_row,
                    predicted_delta=0.0,
                    abstained=True,
                    abstention_reason="no_safe_non_v2_plus_candidate",
                    family_tail_risk_probability=0.0,
                    tail_risk_guard_passed=True,
                )
            )
            continue
        tail_safe_scored = [
            (row, predicted_delta)
            for row, predicted_delta in non_v2_scored
            if float(
                family_tail_risk.get(
                    str(row["dt_schedule_family_target"]),
                    {},
                ).get("tail_risk_probability", 0.0)
            )
            <= max_family_tail_risk_probability
        ]
        if not tail_safe_scored:
            worst_family_tail = max(
                float(
                    family_tail_risk.get(
                        str(row["dt_schedule_family_target"]),
                        {},
                    ).get("tail_risk_probability", 0.0)
                )
                for row, _ in non_v2_scored
            )
            selected_rows.append(
                _selected_row(
                    selected=v2_row,
                    v2_row=v2_row,
                    predicted_delta=0.0,
                    abstained=True,
                    abstention_reason="family_tail_risk_above_threshold",
                    family_tail_risk_probability=worst_family_tail,
                    tail_risk_guard_passed=False,
                )
            )
            continue
        candidate, predicted_delta = min(tail_safe_scored, key=lambda item: item[1])
        predicted_improvement = -predicted_delta
        family = str(candidate["dt_schedule_family_target"])
        family_tail = float(
            family_tail_risk.get(family, {}).get("tail_risk_probability", 0.0)
        )
        if predicted_improvement < min_predicted_improvement_uah:
            selected_rows.append(
                _selected_row(
                    selected=v2_row,
                    v2_row=v2_row,
                    predicted_delta=0.0,
                    abstained=True,
                    abstention_reason="predicted_improvement_below_threshold",
                    family_tail_risk_probability=family_tail,
                    tail_risk_guard_passed=True,
                )
            )
            continue
        selected_rows.append(
            _selected_row(
                selected=candidate,
                v2_row=v2_row,
                predicted_delta=predicted_delta,
                abstained=False,
                abstention_reason="selected_predicted_regret_improvement",
                family_tail_risk_probability=family_tail,
                tail_risk_guard_passed=True,
            )
        )
    return selected_rows


def _selected_row(
    *,
    selected: Mapping[str, Any],
    v2_row: Mapping[str, Any],
    predicted_delta: float,
    abstained: bool,
    abstention_reason: str,
    family_tail_risk_probability: float,
    tail_risk_guard_passed: bool,
) -> dict[str, Any]:
    selected_regret = _float(selected["regret_uah"])
    v2_regret = _float(v2_row["regret_uah"])
    selected_value = _float(selected["schedule_value_uah"])
    v2_value = _float(v2_row["schedule_value_uah"])
    family = str(selected["dt_schedule_family_target"])
    return {
        "tenant_id": str(selected["tenant_id"]),
        "source_model_name": str(selected["source_model_name"]),
        "anchor_timestamp": _datetime_value(
            selected["anchor_timestamp"],
            field_name="anchor_timestamp",
        ),
        "selected_candidate_id": str(selected["dt_candidate_id_target"]),
        "selected_candidate_index": int(_feature_float(selected, "dt_candidate_index_target")),
        "selected_schedule_family": family,
        "selected_regret_uah": selected_regret,
        "selected_value_uah": selected_value,
        "v2_plus_candidate_id": str(v2_row["dt_candidate_id_target"]),
        "v2_plus_regret_uah": v2_regret,
        "v2_plus_value_uah": v2_value,
        "selected_minus_v2_plus_regret_uah": selected_regret - v2_regret,
        "selected_minus_v2_plus_value_uah": selected_value - v2_value,
        "predicted_regret_delta_vs_v2_plus_uah": predicted_delta,
        "predicted_improvement_vs_v2_plus_uah": -predicted_delta,
        "abstained_to_v2_plus": abstained,
        "abstention_reason": abstention_reason,
        "family_tail_risk_probability": family_tail_risk_probability,
        "tail_risk_guard_passed": tail_risk_guard_passed,
        "research_shadow_not_promotable": True,
        "dt_lava_ready": False,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
        "not_market_execution": True,
    }


def _summary(
    *,
    run_slug: str,
    frame: pl.DataFrame,
    train_rows: Sequence[Mapping[str, Any]],
    final_rows: Sequence[Mapping[str, Any]],
    selected_rows: Sequence[Mapping[str, Any]],
    model: Mapping[str, Any],
    feature_names: Sequence[str],
    leakage: Mapping[str, Any],
    min_predicted_improvement_uah: float,
    tail_risk_loss_threshold_uah: float,
    max_family_tail_risk_probability: float,
    ridge_l2: float,
    family_tail_risk: Mapping[str, Mapping[str, float]],
    model_kind: str,
    feature_set: str,
    random_seed: int,
) -> dict[str, Any]:
    selected_regrets = [_float(row["selected_regret_uah"]) for row in selected_rows]
    v2_regrets = [_float(row["v2_plus_regret_uah"]) for row in selected_rows]
    selected_values = [_float(row["selected_value_uah"]) for row in selected_rows]
    v2_values = [_float(row["v2_plus_value_uah"]) for row in selected_rows]
    selected_counts = Counter(str(row["selected_schedule_family"]) for row in selected_rows)
    control_summary = _control_summary(final_rows)
    non_v2_switch_count = sum(
        1
        for row in selected_rows
        if not _is_v2_plus_family(str(row["selected_schedule_family"]))
    )
    abstention_count = sum(1 for row in selected_rows if bool(row["abstained_to_v2_plus"]))
    final_anchor_count = len({_anchor_key(row) for row in final_rows})
    return {
        "run_slug": run_slug,
        "claim_scope": REGRET_AWARE_V2_PLUS_SELECTOR_CLAIM_SCOPE,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "method": _method_description(model_kind),
        "loss_function": _loss_function_name(model_kind),
        "sample_weight_formula": SAMPLE_WEIGHT_FORMULA,
        "target_column": "regret_delta_vs_v2_plus_uah",
        "model_kind": model_kind,
        "feature_set": feature_set,
        "random_seed": random_seed,
        "feature_names": list(feature_names),
        "feature_leakage_guard": dict(leakage),
        "training": {
            "train_row_count": len(train_rows),
            "train_anchor_count": len({_anchor_key(row) for row in train_rows}),
            "ridge_l2": ridge_l2,
            "train_weighted_rmse_uah": model["train_weighted_rmse_uah"],
            "model_type": model["model_type"],
            "min_sample_weight": model["min_sample_weight"],
            "max_sample_weight": model["max_sample_weight"],
            "mean_sample_weight": model["mean_sample_weight"],
            "family_tail_risk": dict(family_tail_risk),
        },
        "evaluation": {
            "final_holdout_row_count": len(final_rows),
            "final_holdout_anchor_count": final_anchor_count,
            "selector_mean_regret_uah": _mean(selected_regrets),
            "selector_median_regret_uah": _median(selected_regrets),
            "selector_mean_value_uah": _mean(selected_values),
            "v2_plus_mean_regret_uah": _mean(v2_regrets),
            "v2_plus_median_regret_uah": _median(v2_regrets),
            "v2_plus_mean_value_uah": _mean(v2_values),
            "selector_minus_v2_plus_mean_regret_uah": (
                _mean(selected_regrets) - _mean(v2_regrets)
            ),
            "selector_minus_v2_plus_mean_value_uah": (
                _mean(selected_values) - _mean(v2_values)
            ),
            "non_v2_plus_switch_count": non_v2_switch_count,
            "abstention_count": abstention_count,
            "selected_family_counts": dict(sorted(selected_counts.items())),
            "control_summary": control_summary,
        },
        "selector_config": {
            "min_predicted_improvement_uah": min_predicted_improvement_uah,
            "tail_risk_loss_threshold_uah": tail_risk_loss_threshold_uah,
            "max_family_tail_risk_probability": max_family_tail_risk_probability,
        },
        "input_summary": {
            "teacher_row_count": frame.height,
            "source_model_names": sorted(
                {str(value) for value in frame["source_model_name"].unique().to_list()}
            ),
            "candidate_families": sorted(
                {str(value) for value in frame["dt_schedule_family_target"].unique().to_list()}
            ),
        },
        "boundary": {
            "research_shadow_not_promotable": True,
            "mirrored_training_rows_possible": True,
            "out_of_sample_generalization_claim": False,
            "abstains_to_v2_plus_when_signal_is_weak": True,
            "raw_hourly_buy_sell_hold_action_target": False,
            "dt_lava_ready": False,
            "dt_promotion_gate_passed": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
            "not_market_execution": True,
            "no_dashboard_api_default_switch": True,
        },
        "attached_artifacts": {
            "selected_rows_csv": SELECTED_ROWS_CSV_NAME,
            "summary_json": SUMMARY_JSON_NAME,
            "summary_markdown": SUMMARY_MD_NAME,
        },
    }


def _summary_markdown(summary: Mapping[str, Any]) -> str:
    evaluation = _mapping(summary["evaluation"])
    training = _mapping(summary["training"])
    config = _mapping(summary["selector_config"])
    lines = [
        "# Regret-Aware V2+ Selector Shadow",
        "",
        f"Run slug: `{summary['run_slug']}`",
        "",
        str(summary["method"]),
        "",
        "## Result",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Selector mean regret | `{_float(evaluation['selector_mean_regret_uah']):.2f}` UAH |",
        f"| V2+ mean regret | `{_float(evaluation['v2_plus_mean_regret_uah']):.2f}` UAH |",
        "| Selector minus V2+ regret | "
        f"`{_float(evaluation['selector_minus_v2_plus_mean_regret_uah']):.2f}` UAH |",
        f"| Non-V2+ switches | `{int(evaluation['non_v2_plus_switch_count'])}` |",
        f"| V2+ abstentions | `{int(evaluation['abstention_count'])}` |",
        "",
        "## Training",
        "",
        f"- Loss: `{summary['loss_function']}`.",
        f"- Model kind: `{summary['model_kind']}`.",
        f"- Feature set: `{summary['feature_set']}`.",
        f"- Sample weights: `{summary['sample_weight_formula']}`.",
        f"- Train weighted RMSE: `{_float(training['train_weighted_rmse_uah']):.2f}` UAH.",
        "- Minimum predicted improvement for a switch: "
        f"`{_float(config['min_predicted_improvement_uah']):.2f}` UAH.",
        "",
        "## Boundary",
        "",
        "- Research-shadow only; no out-of-sample promotion claim.",
        "- Explicit abstention falls back to V2+ when the signal is weak.",
        "- `market_execution_enabled=false`; no DT/LAVA promotion and no market-submittable bid.",
    ]
    return "\n".join(lines) + "\n"


def _loss_function_name(model_kind: str) -> str:
    if model_kind == MODEL_KIND_HIST_GRADIENT_BOOSTING:
        return LOSS_FUNCTION_HIST_GRADIENT_BOOSTING
    if model_kind == MODEL_KIND_RANDOM_FOREST:
        return LOSS_FUNCTION_RANDOM_FOREST
    return LOSS_FUNCTION_NAME


def _method_description(model_kind: str) -> str:
    if model_kind == MODEL_KIND_RANDOM_FOREST:
        return (
            "Candidate-level random-forest residual selector over point-in-time "
            "DT-shadow teacher-row features with explicit V2+ abstention."
        )
    if model_kind == MODEL_KIND_HIST_GRADIENT_BOOSTING:
        return (
            "Candidate-level histogram-gradient residual selector over "
            "point-in-time DT-shadow teacher-row features with explicit V2+ "
            "abstention."
        )
    return (
        "Candidate-level weighted ridge ranker over point-in-time DT-shadow "
        "teacher-row features with explicit V2+ abstention."
    )


def _control_summary(final_rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, float | int]]:
    rows_by_family: dict[str, list[Mapping[str, Any]]] = {}
    for row in final_rows:
        rows_by_family.setdefault(str(row["dt_schedule_family_target"]), []).append(row)
    return {
        family: {
            "row_count": len(rows),
            "mean_regret_uah": _mean([_float(row["regret_uah"]) for row in rows]),
            "median_regret_uah": _median([_float(row["regret_uah"]) for row in rows]),
            "mean_value_uah": _mean([_float(row["schedule_value_uah"]) for row in rows]),
        }
        for family, rows in sorted(rows_by_family.items())
    }


def _family_tail_risk(
    train_rows: Sequence[Mapping[str, Any]],
    *,
    tail_risk_loss_threshold_uah: float,
) -> dict[str, dict[str, float]]:
    grouped: dict[str, list[float]] = {}
    for row in train_rows:
        family = str(row["dt_schedule_family_target"])
        if _is_v2_plus_family(family):
            continue
        grouped.setdefault(family, []).append(_target(row))
    return {
        family: {
            "train_row_count": float(len(targets)),
            "tail_loss_count": float(
                sum(1 for target in targets if target >= tail_risk_loss_threshold_uah)
            ),
            "tail_risk_probability": (
                sum(1 for target in targets if target >= tail_risk_loss_threshold_uah)
                / len(targets)
            ),
            "mean_regret_delta_vs_v2_plus_uah": _mean(targets),
        }
        for family, targets in grouped.items()
        if targets
    }


def _feature_leakage_guard(feature_names: Sequence[str]) -> dict[str, Any]:
    forbidden_features = [
        feature
        for feature in feature_names
        if any(token in _feature_leakage_subject(feature) for token in FORBIDDEN_FEATURE_TOKENS)
    ]
    return {
        "uses_realized_regret_as_feature": bool(forbidden_features),
        "forbidden_feature_tokens": list(FORBIDDEN_FEATURE_TOKENS),
        "forbidden_features": forbidden_features,
        "selector_features_prior_context_only": not forbidden_features,
    }


def _feature_leakage_subject(feature_name: str) -> str:
    if feature_name.startswith("candidate_family="):
        return "candidate_family_indicator"
    if "*candidate_family=" in feature_name:
        return feature_name.split("*candidate_family=", 1)[0]
    return feature_name


def _group_anchor_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[tuple[tuple[str, str, datetime], list[Mapping[str, Any]]]]:
    grouped: dict[tuple[str, str, datetime], list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_anchor_key(row), []).append(row)
    return sorted(grouped.items(), key=lambda item: item[0])


def _v2_plus_row(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for row in rows:
        if _is_v2_plus_family(str(row["dt_schedule_family_target"])):
            return row
    return None


def _is_v2_plus_family(family: str) -> bool:
    normalized = family.casefold()
    return normalized in V2_PLUS_FAMILY_ALIASES or "v2_plus" in normalized


def _anchor_key(row: Mapping[str, Any]) -> tuple[str, str, datetime]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
    )


def _target(row: Mapping[str, Any]) -> float:
    return _float(row["regret_delta_vs_v2_plus_uah"])


def _sample_weight(row: Mapping[str, Any]) -> float:
    return 1.0 + abs(_target(row)) / SAMPLE_WEIGHT_SCALE_UAH


def _terminal_soc_delta(value: object) -> float:
    values = _vector(value)
    return values[-1] - values[0] if len(values) >= 2 else 0.0


def parse_selector_vector(value: object) -> list[float]:
    """Parse selector vector fields, including CSV double-encoded JSON lists."""

    if isinstance(value, pl.Series):
        return [_float(item) for item in value.to_list()]
    if isinstance(value, np.ndarray):
        return [_float(item) for item in value.tolist()]
    if isinstance(value, list | tuple):
        return [_float(item) for item in value]
    if isinstance(value, str):
        parsed: object = value.strip()
        for _ in range(3):
            if not isinstance(parsed, str):
                break
            if not parsed:
                return []
            try:
                parsed = json.loads(parsed)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(cast(str, parsed))
                except (SyntaxError, ValueError):
                    return []
        if isinstance(parsed, list | tuple):
            return [_float(item) for item in parsed]
    return []


def _vector(value: object) -> list[float]:
    return parse_selector_vector(value)


def _feature_float(
    row: Mapping[str, Any],
    name: str,
    *,
    fallback: float = 0.0,
) -> float:
    if name not in row or row[name] is None:
        return fallback
    return _float(row[name], fallback=fallback)


def _float(value: object, *, fallback: float = 0.0) -> float:
    if value is None:
        return fallback
    try:
        result = float(cast(Any, value))
    except (TypeError, ValueError):
        return fallback
    if math.isnan(result) or math.isinf(result):
        return fallback
    return result


def _datetime_value(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    raise TypeError(f"{field_name} must be a datetime or ISO datetime string.")


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected mapping value")
    return value


def _mean(values: Sequence[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[midpoint])
    return float((ordered[midpoint - 1] + ordered[midpoint]) / 2.0)


def _frame_has_true(frame: pl.DataFrame, column: str) -> bool:
    return column in frame.columns and bool(frame.select(pl.col(column).any()).item())


def _frame_all_true(frame: pl.DataFrame, column: str) -> bool:
    return column in frame.columns and bool(frame.select(pl.col(column).all()).item())


__all__ = [
    "FEATURE_SET_BASE",
    "FEATURE_SET_EXPANDED",
    "MODEL_KIND_HIST_GRADIENT_BOOSTING",
    "MODEL_KIND_RANDOM_FOREST",
    "MODEL_KIND_WEIGHTED_RIDGE",
    "REGRET_AWARE_V2_PLUS_SELECTOR_CLAIM_SCOPE",
    "SUMMARY_JSON_NAME",
    "SUMMARY_MD_NAME",
    "SELECTED_ROWS_CSV_NAME",
    "build_regret_aware_v2_plus_selector_packet",
    "parse_selector_vector",
    "write_regret_aware_v2_plus_selector_packet",
]
