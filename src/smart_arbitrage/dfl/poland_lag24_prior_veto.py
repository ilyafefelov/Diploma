"""Prior-only tail-risk veto for Poland lag-24 schedules."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any, Final

import numpy as np
import polars as pl

SUMMARY_JSON_ARTIFACT_NAME: Final[str] = "poland_lag24_prior_veto_summary.json"
SUMMARY_MARKDOWN_ARTIFACT_NAME: Final[str] = "poland_lag24_prior_veto_summary.md"
VETO_ROWS_CSV_ARTIFACT_NAME: Final[str] = "poland_lag24_prior_veto_rows.csv"
VETO_BY_TENANT_CSV_ARTIFACT_NAME: Final[str] = "poland_lag24_prior_veto_by_tenant.csv"

PRIOR_SAFE_NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    "challenger_quantile_spread_scale",
    "action_changed",
    "baseline_committed_power_mw",
    "challenger_committed_power_mw",
    "throughput_delta_mwh",
    "degradation_delta_uah",
    "baseline_forecast_peak_step",
    "challenger_forecast_peak_step",
    "baseline_forecast_trough_step",
    "challenger_forecast_trough_step",
    "baseline_forecast_spread_uah_mwh",
    "challenger_forecast_spread_uah_mwh",
    "forecast_spread_delta_uah_mwh",
    "baseline_absolute_dispatch_mwh",
    "challenger_absolute_dispatch_mwh",
)
PRIOR_SAFE_CATEGORICAL_FEATURES: Final[tuple[str, ...]] = (
    "tenant_id",
    "challenger_candidate_family",
    "challenger_weight_profile",
    "challenger_source_quantile",
    "baseline_committed_action",
    "challenger_committed_action",
)
LABEL_COLUMNS: Final[tuple[str, ...]] = (
    "baseline_regret_uah",
    "challenger_regret_uah",
    "delta_regret_uah",
)
REQUIRED_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "anchor_timestamp",
        *LABEL_COLUMNS,
        *PRIOR_SAFE_NUMERIC_FEATURES,
        *PRIOR_SAFE_CATEGORICAL_FEATURES,
    }
)


@dataclass(frozen=True)
class PolandLag24PriorVetoConfig:
    """Configuration for the prior-only Poland tail-risk veto."""

    min_prior_rows: int = 20
    ridge_alpha: float = 100.0
    threshold_candidates: tuple[float, ...] = (
        -500.0,
        -300.0,
        -200.0,
        -150.0,
        -100.0,
        -75.0,
        -50.0,
        -30.0,
        -20.0,
        -10.0,
        0.0,
        10.0,
        20.0,
        30.0,
        50.0,
        75.0,
        100.0,
    )
    min_prior_selected_rows: int = 1
    max_prior_selected_loss_delta_uah: float = 250.0
    require_prior_mean_non_degradation: bool = True
    require_prior_median_non_degradation: bool = True
    promotion_min_improvement_ratio: float = 0.05


def build_poland_lag24_prior_veto_frame(
    audit_frame: pl.DataFrame,
    *,
    config: PolandLag24PriorVetoConfig | None = None,
) -> pl.DataFrame:
    """Build row-level strict-score evidence for a prior-only Poland veto."""

    active_config = config or PolandLag24PriorVetoConfig()
    _validate_inputs(audit_frame, active_config)
    rows = _audit_rows(audit_frame)
    anchors = sorted({row["anchor_dt"] for row in rows})
    prior_rows: list[dict[str, Any]] = []
    output_rows: list[dict[str, Any]] = []
    for anchor in anchors:
        current_rows = [row for row in rows if row["anchor_dt"] == anchor]
        if len(prior_rows) < active_config.min_prior_rows:
            output_rows.extend(
                _fallback_output_row(
                    row,
                    reason="fallback_insufficient_prior",
                    prior_rows_available=len(prior_rows),
                    config=active_config,
                )
                for row in current_rows
            )
            prior_rows.extend(current_rows)
            continue
        model = _fit_prior_delta_model(prior_rows, active_config)
        threshold_selection = _select_threshold(prior_rows, model, active_config)
        if threshold_selection is None:
            output_rows.extend(
                _fallback_output_row(
                    row,
                    reason="fallback_no_prior_safe_threshold",
                    prior_rows_available=len(prior_rows),
                    config=active_config,
                    model=model,
                )
                for row in current_rows
            )
            prior_rows.extend(current_rows)
            continue
        for row in current_rows:
            prediction = _predict_delta(model, row)
            use_challenger = prediction < threshold_selection["threshold"]
            output_rows.append(
                _selected_output_row(
                    row,
                    use_challenger=use_challenger,
                    predicted_delta=prediction,
                    threshold_selection=threshold_selection,
                    prior_rows_available=len(prior_rows),
                    config=active_config,
                )
            )
        prior_rows.extend(current_rows)
    return pl.DataFrame(output_rows).sort(["anchor_timestamp", "tenant_id"])


def build_poland_lag24_prior_veto_packet(
    *,
    run_slug: str,
    veto_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    config: PolandLag24PriorVetoConfig | None = None,
) -> dict[str, Any]:
    """Build local evidence packet for the prior-only veto result."""

    if veto_frame.is_empty():
        raise ValueError("prior-veto packet requires non-empty rows.")
    active_config = config or PolandLag24PriorVetoConfig()
    summary = _summary(veto_frame, active_config)
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "config": asdict(active_config),
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "no_dashboard_or_api_default_switch": True,
            "strict_fallback": "strict_similar_day",
            "default_fallback": "frozen_ukrainian_v2_plus",
            "external_feature_role": (
                "point_in_time_poland_lag24_exogenous_columns_only"
            ),
            "no_european_training_rows": True,
            "selector_is_prior_only": True,
        },
        "summary": summary,
        "gate": _gate(summary, active_config),
        "by_tenant": _frame_rows(_by_tenant(veto_frame)),
        "interpretation": _interpretation(summary, active_config),
        "attached_artifacts": {
            "summary_json": SUMMARY_JSON_ARTIFACT_NAME,
            "summary_markdown": SUMMARY_MARKDOWN_ARTIFACT_NAME,
            "veto_rows_csv": VETO_ROWS_CSV_ARTIFACT_NAME,
            "by_tenant_csv": VETO_BY_TENANT_CSV_ARTIFACT_NAME,
        },
    }


def write_poland_lag24_prior_veto_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    veto_frame: pl.DataFrame,
) -> Path:
    """Write the prior-only veto evidence packet."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    veto_frame.write_csv(export_dir / VETO_ROWS_CSV_ARTIFACT_NAME)
    _by_tenant(veto_frame).write_csv(export_dir / VETO_BY_TENANT_CSV_ARTIFACT_NAME)
    (export_dir / SUMMARY_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / SUMMARY_MARKDOWN_ARTIFACT_NAME).write_text(
        _markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _validate_inputs(
    audit_frame: pl.DataFrame,
    config: PolandLag24PriorVetoConfig,
) -> None:
    if audit_frame.is_empty():
        raise ValueError("audit_frame must not be empty.")
    missing_columns = sorted(REQUIRED_AUDIT_COLUMNS.difference(audit_frame.columns))
    if missing_columns:
        raise ValueError(f"audit_frame is missing columns: {missing_columns}")
    if config.min_prior_rows <= 0:
        raise ValueError("min_prior_rows must be positive.")
    if config.ridge_alpha < 0.0:
        raise ValueError("ridge_alpha must be non-negative.")
    if not config.threshold_candidates:
        raise ValueError("threshold_candidates must not be empty.")
    if config.min_prior_selected_rows <= 0:
        raise ValueError("min_prior_selected_rows must be positive.")
    if "market_execution_enabled" in audit_frame.columns and audit_frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError("prior-veto frame refuses market execution claims.")


def _audit_rows(audit_frame: pl.DataFrame) -> list[dict[str, Any]]:
    normalized = audit_frame.with_columns(
        pl.col("anchor_timestamp")
        .cast(pl.Utf8)
        .str.replace("T", " ")
        .str.replace(r"\+00:00$", "")
        .str.strptime(pl.Datetime, strict=False)
        .alias("anchor_dt")
    ).sort(["anchor_dt", "tenant_id"])
    return [dict(row) for row in normalized.iter_rows(named=True)]


@dataclass(frozen=True)
class _DeltaModel:
    weights: np.ndarray
    means: np.ndarray
    scales: np.ndarray
    categorical_levels: dict[str, tuple[str, ...]]


def _fit_prior_delta_model(
    prior_rows: list[dict[str, Any]],
    config: PolandLag24PriorVetoConfig,
) -> _DeltaModel:
    categorical_levels = {
        column: tuple(sorted({_as_text(row.get(column)) for row in prior_rows}))
        for column in PRIOR_SAFE_CATEGORICAL_FEATURES
    }
    feature_matrix = np.vstack(
        [_feature_vector(row, categorical_levels) for row in prior_rows]
    )
    labels = np.array([float(row["delta_regret_uah"]) for row in prior_rows])
    means = feature_matrix.mean(axis=0)
    scales = feature_matrix.std(axis=0)
    scales[scales < 1e-6] = 1.0
    standardized = (feature_matrix - means) / scales
    design = np.c_[np.ones(len(prior_rows)), standardized]
    regularizer = config.ridge_alpha * np.eye(design.shape[1])
    regularizer[0, 0] = 0.0
    matrix = design.T @ design + regularizer
    rhs = design.T @ labels
    try:
        weights = np.linalg.solve(matrix, rhs)
    except np.linalg.LinAlgError:
        weights = np.linalg.pinv(matrix) @ rhs
    return _DeltaModel(
        weights=weights,
        means=means,
        scales=scales,
        categorical_levels=categorical_levels,
    )


def _predict_delta(model: _DeltaModel, row: dict[str, Any]) -> float:
    features = (_feature_vector(row, model.categorical_levels) - model.means) / model.scales
    return float(np.r_[1.0, features] @ model.weights)


def _feature_vector(
    row: dict[str, Any],
    categorical_levels: dict[str, tuple[str, ...]],
) -> np.ndarray:
    values: list[float] = []
    for column in PRIOR_SAFE_NUMERIC_FEATURES:
        values.append(_as_float(row.get(column)))
    for column in PRIOR_SAFE_CATEGORICAL_FEATURES:
        current_value = _as_text(row.get(column))
        values.extend(
            1.0 if current_value == level else 0.0
            for level in categorical_levels[column]
        )
    return np.array(values, dtype=float)


def _select_threshold(
    prior_rows: list[dict[str, Any]],
    model: _DeltaModel,
    config: PolandLag24PriorVetoConfig,
) -> dict[str, Any] | None:
    predictions = [_predict_delta(model, row) for row in prior_rows]
    baseline_regrets = [float(row["baseline_regret_uah"]) for row in prior_rows]
    baseline_mean = _mean(baseline_regrets)
    baseline_median = _median(baseline_regrets)
    best_selection: dict[str, Any] | None = None
    for threshold in config.threshold_candidates:
        selected_flags = [prediction < threshold for prediction in predictions]
        selected_count = sum(selected_flags)
        if selected_count < config.min_prior_selected_rows:
            continue
        selected_regrets = [
            float(row["challenger_regret_uah"])
            if selected
            else float(row["baseline_regret_uah"])
            for row, selected in zip(prior_rows, selected_flags, strict=True)
        ]
        selected_deltas = [
            float(row["delta_regret_uah"])
            for row, selected in zip(prior_rows, selected_flags, strict=True)
            if selected
        ]
        selected_mean = _mean(selected_regrets)
        selected_median = _median(selected_regrets)
        max_selected_loss_delta = max(
            [delta for delta in selected_deltas if delta > 0.0],
            default=0.0,
        )
        if (
            config.require_prior_mean_non_degradation
            and selected_mean > baseline_mean
        ):
            continue
        if (
            config.require_prior_median_non_degradation
            and selected_median > baseline_median
        ):
            continue
        if max_selected_loss_delta > config.max_prior_selected_loss_delta_uah:
            continue
        selection = {
            "threshold": float(threshold),
            "prior_selected_rows": selected_count,
            "prior_rows_available": len(prior_rows),
            "prior_baseline_mean_regret_uah": baseline_mean,
            "prior_selected_mean_regret_uah": selected_mean,
            "prior_baseline_median_regret_uah": baseline_median,
            "prior_selected_median_regret_uah": selected_median,
            "prior_max_selected_delta_regret_uah": max_selected_loss_delta,
        }
        if (
            best_selection is None
            or selected_mean < best_selection["prior_selected_mean_regret_uah"]
        ):
            best_selection = selection
    return best_selection


def _fallback_output_row(
    row: dict[str, Any],
    *,
    reason: str,
    prior_rows_available: int,
    config: PolandLag24PriorVetoConfig,
    model: _DeltaModel | None = None,
) -> dict[str, Any]:
    predicted_delta = _predict_delta(model, row) if model is not None else None
    return _base_output_row(
        row,
        use_challenger=False,
        selected_regret=float(row["baseline_regret_uah"]),
        predicted_delta=predicted_delta,
        selected_threshold=None,
        selection_reason=reason,
        prior_rows_available=prior_rows_available,
        prior_selected_rows=0,
        prior_baseline_mean_regret_uah=None,
        prior_selected_mean_regret_uah=None,
        prior_baseline_median_regret_uah=None,
        prior_selected_median_regret_uah=None,
        prior_max_selected_delta_regret_uah=None,
        config=config,
    )


def _selected_output_row(
    row: dict[str, Any],
    *,
    use_challenger: bool,
    predicted_delta: float,
    threshold_selection: dict[str, Any],
    prior_rows_available: int,
    config: PolandLag24PriorVetoConfig,
) -> dict[str, Any]:
    selected_regret = float(
        row["challenger_regret_uah"] if use_challenger else row["baseline_regret_uah"]
    )
    return _base_output_row(
        row,
        use_challenger=use_challenger,
        selected_regret=selected_regret,
        predicted_delta=predicted_delta,
        selected_threshold=float(threshold_selection["threshold"]),
        selection_reason=(
            "selected_poland_prior_safe"
            if use_challenger
            else "fallback_prior_veto"
        ),
        prior_rows_available=prior_rows_available,
        prior_selected_rows=int(threshold_selection["prior_selected_rows"]),
        prior_baseline_mean_regret_uah=float(
            threshold_selection["prior_baseline_mean_regret_uah"]
        ),
        prior_selected_mean_regret_uah=float(
            threshold_selection["prior_selected_mean_regret_uah"]
        ),
        prior_baseline_median_regret_uah=float(
            threshold_selection["prior_baseline_median_regret_uah"]
        ),
        prior_selected_median_regret_uah=float(
            threshold_selection["prior_selected_median_regret_uah"]
        ),
        prior_max_selected_delta_regret_uah=float(
            threshold_selection["prior_max_selected_delta_regret_uah"]
        ),
        config=config,
    )


def _base_output_row(
    row: dict[str, Any],
    *,
    use_challenger: bool,
    selected_regret: float,
    predicted_delta: float | None,
    selected_threshold: float | None,
    selection_reason: str,
    prior_rows_available: int,
    prior_selected_rows: int,
    prior_baseline_mean_regret_uah: float | None,
    prior_selected_mean_regret_uah: float | None,
    prior_baseline_median_regret_uah: float | None,
    prior_selected_median_regret_uah: float | None,
    prior_max_selected_delta_regret_uah: float | None,
    config: PolandLag24PriorVetoConfig,
) -> dict[str, Any]:
    selected_prefix = "challenger" if use_challenger else "baseline"
    return {
        "tenant_id": row["tenant_id"],
        "anchor_timestamp": row["anchor_timestamp"],
        "strategy_kind": "poland_lag24_prior_tail_risk_veto_strict_lp_benchmark",
        "selected_strategy_name": (
            "poland_lag24_prior_tail_risk_veto"
            if use_challenger
            else "frozen_ukrainian_v2_plus_fallback"
        ),
        "selected_uses_challenger": use_challenger,
        "selection_reason": selection_reason,
        "selected_regret_uah": selected_regret,
        "selected_decision_value_uah": float(row[f"{selected_prefix}_decision_value_uah"])
        if f"{selected_prefix}_decision_value_uah" in row
        else 0.0,
        "selected_committed_action": row.get(f"{selected_prefix}_committed_action"),
        "selected_committed_power_mw": _as_float(
            row.get(f"{selected_prefix}_committed_power_mw")
        ),
        "predicted_delta_regret_uah": predicted_delta,
        "selected_threshold_uah": selected_threshold,
        "baseline_regret_uah": float(row["baseline_regret_uah"]),
        "challenger_regret_uah": float(row["challenger_regret_uah"]),
        "delta_regret_uah": float(row["delta_regret_uah"]),
        "baseline_model_name": row.get("baseline_model_name"),
        "challenger_model_name": row.get("challenger_model_name"),
        "challenger_candidate_family": row.get("challenger_candidate_family"),
        "challenger_weight_profile": row.get("challenger_weight_profile"),
        "challenger_source_quantile": row.get("challenger_source_quantile"),
        "challenger_quantile_spread_scale": _as_float(
            row.get("challenger_quantile_spread_scale")
        ),
        "action_changed": bool(row.get("action_changed")),
        "throughput_delta_mwh": _as_float(row.get("throughput_delta_mwh")),
        "degradation_delta_uah": _as_float(row.get("degradation_delta_uah")),
        "forecast_spread_delta_uah_mwh": _as_float(
            row.get("forecast_spread_delta_uah_mwh")
        ),
        "prior_rows_available": prior_rows_available,
        "prior_selected_rows": prior_selected_rows,
        "prior_baseline_mean_regret_uah": prior_baseline_mean_regret_uah,
        "prior_selected_mean_regret_uah": prior_selected_mean_regret_uah,
        "prior_baseline_median_regret_uah": prior_baseline_median_regret_uah,
        "prior_selected_median_regret_uah": prior_selected_median_regret_uah,
        "prior_max_selected_delta_regret_uah": prior_max_selected_delta_regret_uah,
        "min_prior_rows": config.min_prior_rows,
        "ridge_alpha": config.ridge_alpha,
        "market_execution_enabled": False,
        "not_market_execution": True,
        "not_full_dfl": True,
        "selector_is_prior_only": True,
    }


def _summary(
    veto_frame: pl.DataFrame,
    config: PolandLag24PriorVetoConfig,
) -> dict[str, Any]:
    baseline_regrets = _float_column(veto_frame, "baseline_regret_uah")
    challenger_regrets = _float_column(veto_frame, "challenger_regret_uah")
    selected_regrets = _float_column(veto_frame, "selected_regret_uah")
    selected_challenger_rows = int(
        veto_frame.select(pl.col("selected_uses_challenger").sum()).item()
    )
    baseline_mean = _mean(baseline_regrets)
    selected_mean = _mean(selected_regrets)
    baseline_median = _median(baseline_regrets)
    selected_median = _median(selected_regrets)
    improvement_ratio = _safe_ratio(baseline_mean - selected_mean, baseline_mean)
    return {
        "row_count": veto_frame.height,
        "tenant_count": int(veto_frame.select(pl.col("tenant_id").n_unique()).item()),
        "anchor_count": int(
            veto_frame.select(pl.col("anchor_timestamp").n_unique()).item()
        ),
        "selected_challenger_rows": selected_challenger_rows,
        "fallback_rows": veto_frame.height - selected_challenger_rows,
        "baseline_mean_regret_uah": baseline_mean,
        "challenger_all_rows_mean_regret_uah": _mean(challenger_regrets),
        "selected_mean_regret_uah": selected_mean,
        "baseline_median_regret_uah": baseline_median,
        "challenger_all_rows_median_regret_uah": _median(challenger_regrets),
        "selected_median_regret_uah": selected_median,
        "mean_regret_delta_vs_baseline_uah": selected_mean - baseline_mean,
        "mean_regret_improvement_ratio_vs_baseline": improvement_ratio,
        "median_regret_delta_vs_baseline_uah": selected_median - baseline_median,
        "promotion_min_improvement_ratio": config.promotion_min_improvement_ratio,
    }


def _gate(
    summary: dict[str, Any],
    config: PolandLag24PriorVetoConfig,
) -> dict[str, Any]:
    beats_mean = summary["selected_mean_regret_uah"] < summary["baseline_mean_regret_uah"]
    no_median_degradation = (
        summary["selected_median_regret_uah"]
        <= summary["baseline_median_regret_uah"]
    )
    passes_min_improvement = (
        summary["mean_regret_improvement_ratio_vs_baseline"]
        >= config.promotion_min_improvement_ratio
    )
    promotes = bool(beats_mean and no_median_degradation and passes_min_improvement)
    blocker = None
    if not beats_mean:
        blocker = "mean_not_improved_vs_frozen_v2_plus"
    elif not no_median_degradation:
        blocker = "median_degraded_vs_frozen_v2_plus"
    elif not passes_min_improvement:
        blocker = "improvement_below_5_percent"
    return {
        "beats_frozen_v2_plus_mean": beats_mean,
        "no_median_degradation": no_median_degradation,
        "passes_min_improvement": passes_min_improvement,
        "promotes_over_frozen_v2_plus": promotes,
        "blocker": blocker,
        "negative_evidence": not promotes,
        "market_execution_enabled": False,
    }


def _by_tenant(veto_frame: pl.DataFrame) -> pl.DataFrame:
    return (
        veto_frame.group_by("tenant_id")
        .agg(
            [
                pl.len().alias("row_count"),
                pl.sum("selected_uses_challenger").alias("selected_challenger_rows"),
                pl.mean("baseline_regret_uah").alias("baseline_mean_regret_uah"),
                pl.mean("selected_regret_uah").alias("selected_mean_regret_uah"),
                (
                    pl.mean("selected_regret_uah") - pl.mean("baseline_regret_uah")
                ).alias("mean_delta_vs_baseline_uah"),
                pl.median("baseline_regret_uah").alias("baseline_median_regret_uah"),
                pl.median("selected_regret_uah").alias("selected_median_regret_uah"),
            ]
        )
        .sort("mean_delta_vs_baseline_uah")
    )


def _interpretation(
    summary: dict[str, Any],
    config: PolandLag24PriorVetoConfig,
) -> dict[str, str]:
    improvement_pct = 100.0 * summary["mean_regret_improvement_ratio_vs_baseline"]
    if improvement_pct >= 100.0 * config.promotion_min_improvement_ratio:
        headline = "The prior-only veto clears the configured mean-improvement gate."
    elif summary["selected_mean_regret_uah"] < summary["baseline_mean_regret_uah"]:
        headline = (
            "The prior-only veto improves over frozen V2+, but the improvement is "
            "below the conservative 5% replacement threshold."
        )
    else:
        headline = (
            "The prior-only veto does not improve over frozen V2+ and should remain "
            "diagnostic."
        )
    return {
        "headline": headline,
        "why_this_is_prior_only": (
            "The ridge model trains only on earlier anchors and uses only "
            "pre-anchor forecast, schedule, tenant, and candidate-family columns "
            "for the current anchor."
        ),
        "why_nbeatsx_did_not_automatically_win": (
            "The Poland columns can improve typical schedule choices, but the "
            "forecast model still optimizes prediction structure rather than final "
            "LP/oracle regret. Extra exogenous data can therefore reduce median "
            "errors while introducing a few high-regret dispatch tails."
        ),
    }


def _markdown(packet: dict[str, Any]) -> str:
    summary = packet["summary"]
    gate = packet["gate"]
    interpretation = packet["interpretation"]
    lines = [
        "# Poland Lag-24 Prior-Only Tail-Risk Veto",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Dagster run: `{packet.get('dagster_run_id')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet is Offline Strategy Promotion evidence only: "
        "`market_execution_enabled=false`, no dashboard/API default switch, "
        "no live dispatch, and no European rows in Ukrainian training.",
        "",
        "## Result",
        "",
        f"- Frozen V2+ mean regret: {summary['baseline_mean_regret_uah']:.2f} UAH",
        f"- Prior-veto mean regret: {summary['selected_mean_regret_uah']:.2f} UAH",
        (
            "- Improvement versus V2+: "
            f"{summary['mean_regret_improvement_ratio_vs_baseline']:.2%}"
        ),
        (
            f"- Frozen V2+ median regret: {summary['baseline_median_regret_uah']:.2f} UAH"
        ),
        f"- Prior-veto median regret: {summary['selected_median_regret_uah']:.2f} UAH",
        f"- Poland rows selected: {summary['selected_challenger_rows']} / {summary['row_count']}",
        f"- Promotion blocker: `{gate['blocker']}`",
        "",
        "## Interpretation",
        "",
        interpretation["headline"],
        "",
        interpretation["why_this_is_prior_only"],
        "",
        interpretation["why_nbeatsx_did_not_automatically_win"],
        "",
        "## Tenant Summary",
        "",
        "| Tenant | Rows | Poland selected | Mean delta | Median selected |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in packet["by_tenant"]:
        lines.append(
            "| {tenant_id} | {row_count} | {selected_challenger_rows} | "
            "{mean_delta_vs_baseline_uah:.2f} | "
            "{selected_median_regret_uah:.2f} |".format(**row)
        )
    lines.append("")
    return "\n".join(lines)


def _float_column(frame: pl.DataFrame, column: str) -> list[float]:
    return [float(value) for value in frame[column].to_list()]


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, float) and math.isnan(value):
        return 0.0
    return float(value)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return [dict(row) for row in frame.iter_rows(named=True)]


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    return value
