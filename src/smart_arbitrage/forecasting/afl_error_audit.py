"""AFL forecast-error diagnostics for strict LP/oracle readiness."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

AFL_FORECAST_ERROR_AUDIT_CLAIM_SCOPE: Final[str] = (
    "afl_forecast_error_audit_not_full_dfl"
)

_STRICT_MODEL_NAME: Final[str] = "strict_similar_day"
_FAILURE_PRIORITY: Final[tuple[str, ...]] = (
    "lp_value_failure",
    "spread_shape_failure",
    "rank_extrema_failure",
    "weather_load_regime_failure",
)


def build_afl_forecast_error_audit_frame(
    forecast_candidate_forensics_frame: pl.DataFrame,
    afl_training_panel_frame: pl.DataFrame,
    *,
    spread_shape_failure_threshold_ratio: float = 0.25,
    rank_extrema_failure_threshold: float = 0.5,
    lp_value_failure_margin_uah: float = 0.0,
) -> pl.DataFrame:
    """Classify compact/official forecast failures in AFL evidence rows."""

    _validate_source_frames(
        forecast_candidate_forensics_frame,
        afl_training_panel_frame,
    )
    candidate_names = _candidate_model_names(forecast_candidate_forensics_frame)
    strict_by_key = _strict_regrets_by_key(afl_training_panel_frame)
    strict_high_threshold_by_tenant_split = _strict_high_thresholds(
        afl_training_panel_frame
    )
    selector_feature_columns = _feature_columns(afl_training_panel_frame)
    label_columns = _label_columns(afl_training_panel_frame)
    weather_load_columns = [
        column
        for column in selector_feature_columns
        if "weather" in column.lower() or "load" in column.lower()
    ]
    forensics_by_model = {
        str(row["forecast_model_name"]): row
        for row in forecast_candidate_forensics_frame.iter_rows(named=True)
    }

    grouped_rows: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in afl_training_panel_frame.iter_rows(named=True):
        model_name = str(row["forecast_model_name"])
        if model_name not in candidate_names:
            continue
        grouped_rows[
            (
                str(row["tenant_id"]),
                model_name,
                str(row["split"]),
            )
        ].append(row)

    output_rows: list[dict[str, Any]] = []
    for (tenant_id, model_name, split_name), rows in grouped_rows.items():
        row_count = len(rows)
        lp_value_failures = 0
        spread_failures = 0
        rank_failures = 0
        strict_high_regret_overlaps = 0
        regret_values: list[float] = []
        strict_regret_values: list[float] = []
        spread_error_ratios: list[float] = []
        rank_scores: list[float] = []
        weather_load_failures = 0

        for row in rows:
            anchor_timestamp = _datetime(row["anchor_timestamp"])
            strict_regret = strict_by_key.get((tenant_id, str(row["split"]), anchor_timestamp))
            if strict_regret is None:
                raise ValueError(
                    "AFL forecast error audit requires strict_similar_day rows "
                    f"for tenant={tenant_id}, split={row['split']}, anchor={anchor_timestamp}."
                )
            regret = float(row["label_regret_uah"])
            actual_spread = float(row["label_actual_price_spread_uah_mwh"])
            forecast_spread = float(row["feature_forecast_price_spread_uah_mwh"])
            spread_error_ratio = abs(forecast_spread - actual_spread) / max(
                abs(actual_spread),
                1.0,
            )
            rank_score = _rank_score(row)
            strict_threshold = strict_high_threshold_by_tenant_split[
                (tenant_id, str(row["split"]))
            ]

            regret_values.append(regret)
            strict_regret_values.append(strict_regret)
            spread_error_ratios.append(spread_error_ratio)
            rank_scores.append(rank_score)
            if regret > strict_regret + lp_value_failure_margin_uah:
                lp_value_failures += 1
            if spread_error_ratio >= spread_shape_failure_threshold_ratio:
                spread_failures += 1
            if rank_score < rank_extrema_failure_threshold:
                rank_failures += 1
            if strict_regret >= strict_threshold:
                strict_high_regret_overlaps += 1
            if weather_load_columns and _missing_weather_load_context(row):
                weather_load_failures += 1

        failure_rates = {
            "lp_value_failure": lp_value_failures / row_count if row_count else 0.0,
            "spread_shape_failure": spread_failures / row_count if row_count else 0.0,
            "rank_extrema_failure": rank_failures / row_count if row_count else 0.0,
            "weather_load_regime_failure": weather_load_failures / row_count
            if row_count
            else 0.0,
        }
        forensics = forensics_by_model[model_name]
        output_rows.append(
            {
                "tenant_id": tenant_id,
                "forecast_model_name": model_name,
                "model_family": str(forensics["model_family"]),
                "candidate_kind": str(forensics["candidate_kind"]),
                "split": split_name,
                "row_count": row_count,
                "anchor_count": len({_datetime(row["anchor_timestamp"]) for row in rows}),
                "mean_regret_uah": _mean(regret_values),
                "mean_strict_regret_uah": _mean(strict_regret_values),
                "mean_regret_delta_vs_strict_uah": _mean(
                    [
                        regret - strict_regret
                        for regret, strict_regret in zip(
                            regret_values,
                            strict_regret_values,
                            strict=True,
                        )
                    ]
                ),
                "mean_spread_error_ratio": _mean(spread_error_ratios),
                "mean_rank_extrema_score": _mean(rank_scores),
                "spread_shape_failure_rate": failure_rates["spread_shape_failure"],
                "rank_extrema_failure_rate": failure_rates["rank_extrema_failure"],
                "lp_value_failure_rate": failure_rates["lp_value_failure"],
                "weather_load_regime_failure_rate": failure_rates[
                    "weather_load_regime_failure"
                ],
                "strict_control_high_regret_overlap_rate": (
                    strict_high_regret_overlaps / row_count if row_count else 0.0
                ),
                "weather_load_regime_status": (
                    "feature_context_present"
                    if weather_load_columns
                    else "context_unavailable"
                ),
                "dominant_failure_mode": _dominant_failure_mode(failure_rates),
                "selector_feature_columns_csv": ",".join(selector_feature_columns),
                "label_columns_used_csv": ",".join(label_columns),
                "weather_load_feature_columns_csv": ",".join(weather_load_columns),
                "claim_scope": AFL_FORECAST_ERROR_AUDIT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )

    if not output_rows:
        raise ValueError("AFL forecast error audit found no forecast candidate rows.")
    return pl.DataFrame(output_rows).sort(["forecast_model_name", "tenant_id", "split"])


def validate_afl_forecast_error_audit_evidence(
    frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate AFL audit rows as research-only evidence."""

    required_columns = {
        "tenant_id",
        "forecast_model_name",
        "split",
        "row_count",
        "anchor_count",
        "dominant_failure_mode",
        "selector_feature_columns_csv",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
    failures: list[str] = []
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        failures.append(f"missing required columns: {missing}")
        return _outcome(
            failures=failures,
            metadata={"missing_columns": missing},
        )
    if frame.height == 0:
        failures.append("AFL forecast error audit frame must not be empty.")

    claim_flag_failure_rows = _claim_flag_failure_rows(frame)
    selector_label_column_rows = sum(
        1
        for row in frame.iter_rows(named=True)
        if "label_" in str(row["selector_feature_columns_csv"])
    )
    invalid_scope_rows = frame.filter(
        pl.col("claim_scope") != AFL_FORECAST_ERROR_AUDIT_CLAIM_SCOPE
    ).height
    if claim_flag_failure_rows:
        failures.append("AFL audit rows must be not_full_dfl and not_market_execution.")
    if selector_label_column_rows:
        failures.append("selector feature columns must not include label columns.")
    if invalid_scope_rows:
        failures.append("AFL audit rows must use the expected claim scope.")

    metadata = {
        "row_count": frame.height,
        "tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
        "model_count": frame.select("forecast_model_name").n_unique()
        if frame.height
        else 0,
        "split_count": frame.select("split").n_unique() if frame.height else 0,
        "claim_flag_failure_rows": claim_flag_failure_rows,
        "selector_label_column_rows": selector_label_column_rows,
        "invalid_scope_rows": invalid_scope_rows,
    }
    return _outcome(failures=failures, metadata=metadata)


def _validate_source_frames(forensics_frame: pl.DataFrame, panel_frame: pl.DataFrame) -> None:
    forensics_required = {
        "forecast_model_name",
        "model_family",
        "candidate_kind",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
    panel_required = {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "split",
        "feature_forecast_price_spread_uah_mwh",
        "diagnostic_forecast_top3_bottom3_rank_overlap",
        "label_regret_uah",
        "label_actual_price_spread_uah_mwh",
        "diagnostic_top_k_price_recall",
        "diagnostic_spread_ranking_quality",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
    missing_forensics = sorted(forensics_required.difference(forensics_frame.columns))
    missing_panel = sorted(panel_required.difference(panel_frame.columns))
    if missing_forensics:
        raise ValueError(f"forensics frame is missing required columns: {missing_forensics}")
    if missing_panel:
        raise ValueError(f"AFL panel is missing required columns: {missing_panel}")
    if forensics_frame.is_empty() or panel_frame.is_empty():
        raise ValueError("AFL audit inputs must not be empty.")
    if _claim_flag_failure_rows(forensics_frame) or _claim_flag_failure_rows(panel_frame):
        raise ValueError("AFL audit inputs must remain research-only claim evidence.")
    if _STRICT_MODEL_NAME not in panel_frame.select("forecast_model_name").to_series().to_list():
        raise ValueError("AFL audit requires strict_similar_day panel rows.")


def _candidate_model_names(forensics_frame: pl.DataFrame) -> set[str]:
    names: set[str] = set()
    for row in forensics_frame.iter_rows(named=True):
        model_name = str(row["forecast_model_name"])
        candidate_kind = str(row["candidate_kind"])
        if model_name == _STRICT_MODEL_NAME:
            continue
        if candidate_kind == "frozen_control_comparator":
            continue
        names.add(model_name)
    if not names:
        raise ValueError("AFL audit requires non-strict forecast candidate rows.")
    return names


def _strict_regrets_by_key(frame: pl.DataFrame) -> dict[tuple[str, str, datetime], float]:
    values: dict[tuple[str, str, datetime], float] = {}
    strict_rows = frame.filter(pl.col("forecast_model_name") == _STRICT_MODEL_NAME)
    for row in strict_rows.iter_rows(named=True):
        values[
            (
                str(row["tenant_id"]),
                str(row["split"]),
                _datetime(row["anchor_timestamp"]),
            )
        ] = float(row["label_regret_uah"])
    return values


def _strict_high_thresholds(frame: pl.DataFrame) -> dict[tuple[str, str], float]:
    regrets: dict[tuple[str, str], list[float]] = defaultdict(list)
    strict_rows = frame.filter(pl.col("forecast_model_name") == _STRICT_MODEL_NAME)
    for row in strict_rows.iter_rows(named=True):
        regrets[(str(row["tenant_id"]), str(row["split"]))].append(
            float(row["label_regret_uah"])
        )
    return {key: _median(values) for key, values in regrets.items()}


def _feature_columns(frame: pl.DataFrame) -> list[str]:
    return sorted(column for column in frame.columns if column.startswith("feature_"))


def _label_columns(frame: pl.DataFrame) -> list[str]:
    return sorted(column for column in frame.columns if column.startswith("label_"))


def _rank_score(row: dict[str, Any]) -> float:
    return min(
        float(row["diagnostic_forecast_top3_bottom3_rank_overlap"]),
        float(row["diagnostic_top_k_price_recall"]),
        float(row["diagnostic_spread_ranking_quality"]),
    )


def _missing_weather_load_context(row: dict[str, Any]) -> bool:
    weather_count = float(row.get("feature_prior_weather_context_row_count", 0.0))
    net_load_count = float(row.get("feature_prior_net_load_context_row_count", 0.0))
    return weather_count <= 0.0 or net_load_count <= 0.0


def _dominant_failure_mode(failure_rates: dict[str, float]) -> str:
    best_mode = _FAILURE_PRIORITY[0]
    best_rate = -1.0
    for mode in _FAILURE_PRIORITY:
        rate = failure_rates.get(mode, 0.0)
        if rate > best_rate:
            best_mode = mode
            best_rate = rate
    return best_mode if best_rate > 0.0 else "no_dominant_failure"


def _claim_flag_failure_rows(frame: pl.DataFrame) -> int:
    if "not_full_dfl" not in frame.columns or "not_market_execution" not in frame.columns:
        return frame.height
    return frame.filter(
        (~pl.col("not_full_dfl").cast(pl.Boolean))
        | (~pl.col("not_market_execution").cast(pl.Boolean))
    ).height


def _outcome(
    *,
    failures: list[str],
    metadata: dict[str, Any],
) -> EvidenceCheckOutcome:
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "AFL forecast error audit evidence is research-only and selector-safe."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def _datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("anchor_timestamp must contain datetime values.")
    return value


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2.0
