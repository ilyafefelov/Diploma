"""Poland lag-24 feature-consumption and rolling-gate diagnostics."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final, cast

import polars as pl

from smart_arbitrage.forecasting.sota_training import (
    POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS,
)

FEATURE_CONSUMPTION_CLAIM_BOUNDARY: Final[str] = (
    "poland_lag24_feature_consumption_audit_not_market_execution"
)
ROLLING_GATE_CLAIM_BOUNDARY: Final[str] = "positive_shadow_evidence_not_promoted"


def build_poland_lag24_feature_consumption_audit_frame(
    lagged_feature_frame: pl.DataFrame,
    *,
    strict_benchmark_frame: pl.DataFrame | None = None,
    feature_columns: tuple[str, ...] = POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS,
) -> pl.DataFrame:
    """Audit whether Poland lag-24 features are usable by official training.

    The audit intentionally checks the feature contract, not model quality. It
    proves that source-backed prior-safe feature columns have full coverage,
    non-constant values, 24h lag alignment, and explicit scaler metadata before
    they are interpreted as a NeuralForecast/NBEATSx/TFT input.
    """

    _validate_lagged_feature_frame(lagged_feature_frame, feature_columns)
    strict_metadata = _strict_metadata(strict_benchmark_frame)
    lag_status = _lag_alignment_status(lagged_feature_frame)
    rows: list[dict[str, Any]] = []
    for feature_column in feature_columns:
        series = lagged_feature_frame.get_column(feature_column).cast(pl.Float64)
        non_null = series.drop_nulls()
        null_count = int(series.null_count())
        unique_count = int(non_null.n_unique()) if len(non_null) else 0
        variance = _variance(non_null)
        source_backed_all = bool(
            lagged_feature_frame.select(pl.col("source_backed").all()).item()
        )
        coverage_statuses = sorted(
            str(value)
            for value in lagged_feature_frame.select("coverage_status")
            .to_series()
            .unique()
            .to_list()
        )
        blockers = _feature_blockers(
            feature_column=feature_column,
            null_count=null_count,
            variance=variance,
            source_backed_all=source_backed_all,
            coverage_statuses=coverage_statuses,
            lag_status=lag_status,
            feature_columns=feature_columns,
        )
        rows.append(
            {
                "feature_column": feature_column,
                "in_neuralforecast_training_contract": (
                    feature_column in POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS
                ),
                "feature_scaler_fit_scope": "train_rows_only_per_unique_id",
                "known_future_covariate_route": True,
                "row_count": lagged_feature_frame.height,
                "non_null_count": len(non_null),
                "null_count": null_count,
                "unique_value_count": unique_count,
                "variance": variance,
                "has_variance": variance > 0.0,
                "min_value": _optional_float(non_null.min()),
                "max_value": _optional_float(non_null.max()),
                "source_backed_all": source_backed_all,
                "coverage_statuses_csv": ",".join(coverage_statuses),
                "timestamp_alignment_status": lag_status,
                "scaler_retention_status": (
                    "retained_by_contract"
                    if variance > 0.0
                    else "at_risk_constant_feature"
                ),
                "consumption_status": (
                    "passes_training_consumption_audit"
                    if not blockers
                    else "blocked_" + "_".join(blockers)
                ),
                "blockers_csv": ",".join(blockers),
                "claim_boundary": FEATURE_CONSUMPTION_CLAIM_BOUNDARY,
                "market_execution_enabled": False,
                "not_market_execution": True,
                **strict_metadata,
            }
        )
    return pl.DataFrame(rows).sort("feature_column")


def build_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame(
    poland_robustness_frame: pl.DataFrame,
    frozen_v2_plus_robustness_frame: pl.DataFrame,
    *,
    min_mean_regret_improvement_ratio_vs_frozen_v2_plus: float = 0.05,
    min_passing_windows: int = 3,
) -> pl.DataFrame:
    """Compare Poland-enhanced rolling windows against frozen Ukrainian V2+."""

    _validate_robustness_frame(
        poland_robustness_frame,
        frame_name="poland_robustness_frame",
    )
    _validate_robustness_frame(
        frozen_v2_plus_robustness_frame,
        frame_name="frozen_v2_plus_robustness_frame",
    )
    if min_passing_windows <= 0:
        raise ValueError("min_passing_windows must be positive.")
    if min_mean_regret_improvement_ratio_vs_frozen_v2_plus < 0.0:
        raise ValueError("minimum improvement ratio must be non-negative.")

    frozen_best = _best_frozen_rows(frozen_v2_plus_robustness_frame)
    rows: list[dict[str, Any]] = []
    for row in poland_robustness_frame.sort(
        ["source_model_name", "window_index"]
    ).iter_rows(named=True):
        window_index = int(row["window_index"])
        frozen = frozen_best.get(window_index)
        if frozen is None:
            raise ValueError(f"missing frozen V2+ robustness row for window {window_index}.")
        poland_mean = float(row["selected_mean_regret_uah"])
        poland_median = float(row["selected_median_regret_uah"])
        frozen_mean = float(frozen["selected_mean_regret_uah"])
        frozen_median = float(frozen["selected_median_regret_uah"])
        improvement_ratio = (
            0.0 if frozen_mean == 0.0 else (frozen_mean - poland_mean) / frozen_mean
        )
        median_delta = poland_median - frozen_median
        poland_window_passed = (
            bool(row["v2_plus_window_passed"])
            and improvement_ratio >= min_mean_regret_improvement_ratio_vs_frozen_v2_plus
            and median_delta <= 0.0
        )
        rows.append(
            {
                "source_model_name": str(row["source_model_name"]),
                "window_index": window_index,
                "validation_tenant_anchor_count": int(
                    row["validation_tenant_anchor_count"]
                ),
                "poland_mean_regret_uah": poland_mean,
                "poland_median_regret_uah": poland_median,
                "frozen_v2_plus_source_model_name": str(frozen["source_model_name"]),
                "frozen_v2_plus_mean_regret_uah": frozen_mean,
                "frozen_v2_plus_median_regret_uah": frozen_median,
                "mean_regret_delta_vs_frozen_v2_plus_uah": poland_mean - frozen_mean,
                "mean_regret_improvement_ratio_vs_frozen_v2_plus": improvement_ratio,
                "median_regret_delta_vs_frozen_v2_plus_uah": median_delta,
                "beats_frozen_mean_gate": (
                    improvement_ratio
                    >= min_mean_regret_improvement_ratio_vs_frozen_v2_plus
                ),
                "median_non_degradation_vs_frozen": median_delta <= 0.0,
                "poland_internal_window_passed": bool(row["v2_plus_window_passed"]),
                "poland_window_passed": poland_window_passed,
                "claim_boundary": ROLLING_GATE_CLAIM_BOUNDARY,
                "market_execution_enabled": False,
                "not_market_execution": True,
            }
        )
    gate_rows = _with_source_gate_status(rows, min_passing_windows=min_passing_windows)
    return pl.DataFrame(gate_rows).sort(["source_model_name", "window_index"])


def _validate_lagged_feature_frame(
    lagged_feature_frame: pl.DataFrame,
    feature_columns: tuple[str, ...],
) -> None:
    if lagged_feature_frame.is_empty():
        raise ValueError("lagged_feature_frame must not be empty.")
    required_columns = {
        "delivery_timestamp_utc",
        "source_delivery_timestamp_utc",
        "source_backed",
        "coverage_status",
        *feature_columns,
    }
    missing_columns = sorted(required_columns.difference(lagged_feature_frame.columns))
    if missing_columns:
        raise ValueError(f"lagged_feature_frame is missing columns: {missing_columns}")
    if "market_execution_enabled" in lagged_feature_frame.columns and lagged_feature_frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError("feature-consumption audit refuses market execution claims.")


def _strict_metadata(strict_benchmark_frame: pl.DataFrame | None) -> dict[str, Any]:
    if strict_benchmark_frame is None or strict_benchmark_frame.is_empty():
        return {
            "strict_evidence_row_count": 0,
            "strict_evidence_anchor_count": 0,
            "strict_evidence_source_model_count": 0,
        }
    required_columns = {"anchor_timestamp", "forecast_model_name"}
    missing_columns = sorted(required_columns.difference(strict_benchmark_frame.columns))
    if missing_columns:
        raise ValueError(f"strict_benchmark_frame is missing columns: {missing_columns}")
    return {
        "strict_evidence_row_count": strict_benchmark_frame.height,
        "strict_evidence_anchor_count": strict_benchmark_frame.select(
            pl.col("anchor_timestamp").n_unique()
        ).item(),
        "strict_evidence_source_model_count": strict_benchmark_frame.select(
            pl.col("forecast_model_name").n_unique()
        ).item(),
    }


def _lag_alignment_status(lagged_feature_frame: pl.DataFrame) -> str:
    lag_hours: list[float] = []
    for row in lagged_feature_frame.select(
        ["delivery_timestamp_utc", "source_delivery_timestamp_utc"]
    ).iter_rows(named=True):
        delivery = _parse_datetime(str(row["delivery_timestamp_utc"]))
        source = _parse_datetime(str(row["source_delivery_timestamp_utc"]))
        lag_hours.append((delivery - source).total_seconds() / 3600.0)
    if not lag_hours:
        return "missing_timestamp_alignment"
    min_lag = min(lag_hours)
    max_lag = max(lag_hours)
    if min_lag >= 24.0 and max_lag < 24.0001:
        return "lagged_24h_prior_safe"
    if min_lag >= 24.0:
        return "lagged_prior_safe_variable"
    return "blocked_not_prior_safe"


def _parse_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def _variance(series: pl.Series) -> float:
    if len(series) <= 1:
        return 0.0
    value = cast(float | None, series.var(ddof=0))
    return 0.0 if value is None else float(value)


def _optional_float(value: object) -> float | None:
    return None if value is None else float(cast(float, value))


def _feature_blockers(
    *,
    feature_column: str,
    null_count: int,
    variance: float,
    source_backed_all: bool,
    coverage_statuses: list[str],
    lag_status: str,
    feature_columns: tuple[str, ...],
) -> list[str]:
    blockers: list[str] = []
    if feature_column not in feature_columns:
        blockers.append("not_in_feature_set")
    if null_count:
        blockers.append("null_values")
    if variance <= 0.0:
        blockers.append("no_variance")
    if not source_backed_all:
        blockers.append("source_unbacked")
    if coverage_statuses != ["full_lagged_feature_coverage"]:
        blockers.append("incomplete_coverage")
    if not lag_status.startswith("lagged_"):
        blockers.append("timestamp_alignment")
    return blockers


def _validate_robustness_frame(frame: pl.DataFrame, *, frame_name: str) -> None:
    if frame.is_empty():
        raise ValueError(f"{frame_name} must not be empty.")
    required_columns = {
        "source_model_name",
        "window_index",
        "validation_tenant_anchor_count",
        "selected_mean_regret_uah",
        "selected_median_regret_uah",
        "v2_plus_window_passed",
        "not_market_execution",
    }
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing columns: {missing_columns}")
    if not frame.select(pl.col("not_market_execution").all()).item():
        raise ValueError(f"{frame_name} refuses non-research rows.")
    if "market_execution_enabled" in frame.columns and frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError(f"{frame_name} refuses market execution claims.")


def _best_frozen_rows(frame: pl.DataFrame) -> dict[int, dict[str, Any]]:
    best_rows: dict[int, dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        window_index = int(row["window_index"])
        current = best_rows.get(window_index)
        if current is None or float(row["selected_mean_regret_uah"]) < float(
            current["selected_mean_regret_uah"]
        ):
            best_rows[window_index] = dict(row)
    return best_rows


def _with_source_gate_status(
    rows: list[dict[str, Any]],
    *,
    min_passing_windows: int,
) -> list[dict[str, Any]]:
    passing_by_source: dict[str, int] = {}
    total_by_source: dict[str, int] = {}
    latest_pass_by_source: dict[str, bool] = {}
    for row in rows:
        source = str(row["source_model_name"])
        total_by_source[source] = total_by_source.get(source, 0) + 1
        if bool(row["poland_window_passed"]):
            passing_by_source[source] = passing_by_source.get(source, 0) + 1
            if int(row["window_index"]) == 1:
                latest_pass_by_source[source] = True
    output: list[dict[str, Any]] = []
    for row in rows:
        source = str(row["source_model_name"])
        passing_count = passing_by_source.get(source, 0)
        total_count = total_by_source[source]
        promotes = passing_count >= min_passing_windows and latest_pass_by_source.get(
            source,
            False,
        )
        row = dict(row)
        row["passing_window_count_for_source"] = passing_count
        row["rolling_window_count_for_source"] = total_count
        row["promotes_over_frozen_v2_plus"] = False
        if promotes:
            row["rolling_gate_status"] = "candidate_ready_for_review_not_default"
        elif passing_count > 0:
            row["rolling_gate_status"] = "positive_not_promoted"
        else:
            row["rolling_gate_status"] = "blocked_by_rolling_gate"
        output.append(row)
    return output
