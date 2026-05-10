"""Strict LP/oracle evaluation for the all-tenant offline DFL panel."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

OFFLINE_DFL_PANEL_STRICT_LP_STRATEGY_KIND: Final[str] = "offline_dfl_panel_strict_lp_benchmark"
OFFLINE_DFL_PANEL_STRICT_CLAIM_SCOPE: Final[str] = "offline_dfl_panel_strict_lp_gate_not_full_dfl"
STRICT_PANEL_ACADEMIC_SCOPE: Final[str] = (
    "Strict LP/oracle promotion test for the offline DFL panel. It reuses the frozen Level 1 DAM LP, "
    "oracle regret, UAH economics, and strict_similar_day control comparator. It is not full DFL and "
    "not market execution."
)
REQUIRED_BENCHMARK_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "starting_soc_fraction",
        "starting_soc_source",
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
        "v2_checkpoint_horizon_biases_uah_mwh",
        "v2_checkpoint_epoch",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_offline_dfl_panel_strict_lp_benchmark_frame(
    evaluation_frame: pl.DataFrame,
    panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Score selected panel v2 horizon biases with the canonical strict LP/oracle evaluator."""

    _require_columns(evaluation_frame, REQUIRED_BENCHMARK_COLUMNS, frame_name="evaluation_frame")
    _require_columns(panel_frame, REQUIRED_PANEL_COLUMNS, frame_name="panel_frame")
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")

    resolved_generated_at = generated_at or datetime.now(UTC)
    frames: list[pl.DataFrame] = []
    for source_model_name in forecast_model_names:
        for tenant_id in tenant_ids:
            panel_row = _single_panel_row(
                panel_frame,
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
            )
            _validate_panel_row(
                panel_row,
                final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
            )
            source_rows = _final_holdout_rows(
                evaluation_frame,
                panel_row=panel_row,
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
                final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
                row_kind="source",
            )
            control_rows = _final_holdout_rows(
                evaluation_frame,
                panel_row=panel_row,
                tenant_id=tenant_id,
                forecast_model_name="strict_similar_day",
                final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
                row_kind="strict control",
            )
            control_by_anchor = {
                _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
                for row in control_rows.iter_rows(named=True)
            }
            tenant_defaults = tenant_battery_defaults_from_registry(tenant_id)
            horizon_biases = _float_list(
                panel_row["v2_checkpoint_horizon_biases_uah_mwh"],
                field_name="v2_checkpoint_horizon_biases_uah_mwh",
            )
            for source_row in source_rows.iter_rows(named=True):
                anchor_timestamp = _datetime_value(source_row["anchor_timestamp"], field_name="anchor_timestamp")
                control_row = control_by_anchor.get(anchor_timestamp)
                if control_row is None:
                    raise ValueError(
                        "missing strict_similar_day row for final-holdout anchor "
                        f"{tenant_id}/{source_model_name}/{anchor_timestamp.isoformat()}"
                    )
                source_payload = _payload(source_row)
                control_payload = _payload(control_row)
                _require_thesis_grade_observed(
                    [source_payload, control_payload],
                    tenant_id=tenant_id,
                    forecast_model_name=source_model_name,
                    anchor_timestamp=anchor_timestamp,
                )
                evaluation = evaluate_forecast_candidates_against_oracle(
                    price_history=_price_history_from_payload(source_payload, anchor_timestamp=anchor_timestamp),
                    tenant_id=tenant_id,
                    battery_metrics=tenant_defaults.metrics,
                    starting_soc_fraction=float(source_row["starting_soc_fraction"]),
                    starting_soc_source=str(source_row["starting_soc_source"]),
                    anchor_timestamp=anchor_timestamp,
                    candidates=[
                        ForecastCandidate(
                            model_name="strict_similar_day",
                            forecast_frame=_forecast_frame_from_payload(
                                control_payload,
                                anchor_timestamp=anchor_timestamp,
                            ),
                            point_prediction_column="predicted_price_uah_mwh",
                        ),
                        ForecastCandidate(
                            model_name=source_model_name,
                            forecast_frame=_forecast_frame_from_payload(
                                source_payload,
                                anchor_timestamp=anchor_timestamp,
                            ),
                            point_prediction_column="predicted_price_uah_mwh",
                        ),
                        ForecastCandidate(
                            model_name=f"offline_dfl_panel_v2_{source_model_name}",
                            forecast_frame=_forecast_frame_from_payload(
                                source_payload,
                                anchor_timestamp=anchor_timestamp,
                                horizon_biases=horizon_biases,
                            ),
                            point_prediction_column="predicted_price_uah_mwh",
                        ),
                    ],
                    evaluation_id=(
                        f"{tenant_id}:offline-dfl-panel-strict:{source_model_name}:"
                        f"{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
                    ),
                    generated_at=resolved_generated_at,
                )
                frames.append(
                    _with_strict_panel_metadata(
                        evaluation,
                        source_model_name=source_model_name,
                        panel_row=panel_row,
                        horizon_biases=horizon_biases,
                    )
                )
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed").sort(
        ["tenant_id", "anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


def _single_panel_row(
    panel_frame: pl.DataFrame,
    *,
    tenant_id: str,
    forecast_model_name: str,
) -> dict[str, Any]:
    rows = panel_frame.filter(
        (pl.col("tenant_id") == tenant_id) & (pl.col("forecast_model_name") == forecast_model_name)
    )
    if rows.height == 0:
        raise ValueError(f"missing offline DFL panel row for {tenant_id}/{forecast_model_name}")
    if rows.height > 1:
        raise ValueError(f"duplicate offline DFL panel rows for {tenant_id}/{forecast_model_name}")
    return rows.row(0, named=True)


def _validate_panel_row(
    panel_row: dict[str, Any],
    *,
    final_validation_anchor_count_per_tenant: int,
) -> None:
    observed_final_count = int(panel_row["final_validation_anchor_count"])
    if observed_final_count != final_validation_anchor_count_per_tenant:
        raise ValueError(
            "offline DFL panel final_validation_anchor_count must match strict evaluation config; "
            f"observed {observed_final_count}, expected {final_validation_anchor_count_per_tenant}"
        )
    if str(panel_row["data_quality_tier"]) != "thesis_grade":
        raise ValueError("offline DFL panel strict benchmark requires thesis_grade panel rows")
    if float(panel_row["observed_coverage_ratio"]) < 1.0:
        raise ValueError("offline DFL panel strict benchmark requires observed coverage ratio of 1.0")
    if not bool(panel_row["not_full_dfl"]):
        raise ValueError("offline DFL panel strict benchmark requires not_full_dfl=true")
    if not bool(panel_row["not_market_execution"]):
        raise ValueError("offline DFL panel strict benchmark requires not_market_execution=true")


def _final_holdout_rows(
    evaluation_frame: pl.DataFrame,
    *,
    panel_row: dict[str, Any],
    tenant_id: str,
    forecast_model_name: str,
    final_validation_anchor_count_per_tenant: int,
    row_kind: str,
) -> pl.DataFrame:
    first_anchor = _datetime_value(
        panel_row["first_final_holdout_anchor_timestamp"],
        field_name="first_final_holdout_anchor_timestamp",
    )
    last_anchor = _datetime_value(
        panel_row["last_final_holdout_anchor_timestamp"],
        field_name="last_final_holdout_anchor_timestamp",
    )
    rows = (
        evaluation_frame
        .filter(
            (pl.col("tenant_id") == tenant_id)
            & (pl.col("forecast_model_name") == forecast_model_name)
            & (pl.col("anchor_timestamp") >= first_anchor)
            & (pl.col("anchor_timestamp") <= last_anchor)
        )
        .sort("anchor_timestamp")
    )
    if rows.height != final_validation_anchor_count_per_tenant:
        raise ValueError(
            f"missing final-holdout {row_kind} rows for {tenant_id}/{forecast_model_name}; "
            f"observed {rows.height}, expected {final_validation_anchor_count_per_tenant}"
        )
    anchor_count = rows.select("anchor_timestamp").n_unique()
    if anchor_count != final_validation_anchor_count_per_tenant:
        raise ValueError(
            f"final-holdout {row_kind} rows must have unique anchors for {tenant_id}/{forecast_model_name}"
        )
    return rows


def _forecast_frame_from_payload(
    payload: dict[str, Any],
    *,
    anchor_timestamp: datetime,
    horizon_biases: list[float] | None = None,
) -> pl.DataFrame:
    horizon = _horizon_rows(payload)
    if horizon_biases is not None and len(horizon_biases) != len(horizon):
        raise ValueError(f"bias length must match horizon length; observed {len(horizon_biases)} vs {len(horizon)}")
    forecast_prices = [
        _float_value(point["forecast_price_uah_mwh"], field_name="forecast_price_uah_mwh")
        + (horizon_biases[step_index] if horizon_biases is not None else 0.0)
        for step_index, point in enumerate(horizon)
    ]
    forecast_timestamps = [
        _future_interval_start(point, anchor_timestamp=anchor_timestamp)
        for point in horizon
    ]
    return pl.DataFrame(
        {
            "forecast_timestamp": forecast_timestamps,
            "predicted_price_uah_mwh": forecast_prices,
        }
    )


def _price_history_from_payload(payload: dict[str, Any], *, anchor_timestamp: datetime) -> pl.DataFrame:
    horizon = _horizon_rows(payload)
    return pl.DataFrame(
        {
            DEFAULT_TIMESTAMP_COLUMN: [
                _future_interval_start(point, anchor_timestamp=anchor_timestamp)
                for point in horizon
            ],
            DEFAULT_PRICE_COLUMN: [
                _float_value(point["actual_price_uah_mwh"], field_name="actual_price_uah_mwh")
                for point in horizon
            ],
        }
    )


def _with_strict_panel_metadata(
    evaluation: pl.DataFrame,
    *,
    source_model_name: str,
    panel_row: dict[str, Any],
    horizon_biases: list[float],
) -> pl.DataFrame:
    payloads: list[dict[str, Any]] = []
    for row in evaluation.iter_rows(named=True):
        payload = dict(row["evaluation_payload"])
        payload.update(
            {
                "strict_gate_kind": "offline_dfl_panel_strict_lp",
                "source_forecast_model_name": source_model_name,
                "v2_forecast_model_name": f"offline_dfl_panel_v2_{source_model_name}",
                "v2_checkpoint_epoch": int(panel_row["v2_checkpoint_epoch"]),
                "v2_checkpoint_horizon_biases_uah_mwh": horizon_biases,
                "final_validation_anchor_count": int(panel_row["final_validation_anchor_count"]),
                "claim_scope": OFFLINE_DFL_PANEL_STRICT_CLAIM_SCOPE,
                "academic_scope": STRICT_PANEL_ACADEMIC_SCOPE,
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
        payloads.append(payload)
    return evaluation.with_columns(
        [
            pl.lit(OFFLINE_DFL_PANEL_STRICT_LP_STRATEGY_KIND).alias("strategy_kind"),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _require_thesis_grade_observed(
    payloads: list[dict[str, Any]],
    *,
    tenant_id: str,
    forecast_model_name: str,
    anchor_timestamp: datetime,
) -> None:
    if any(str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade" for payload in payloads):
        raise ValueError(
            "strict LP panel benchmark requires thesis_grade benchmark rows for "
            f"{tenant_id}/{forecast_model_name}/{anchor_timestamp.isoformat()}"
        )
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        raise ValueError(
            "strict LP panel benchmark requires observed coverage ratio of 1.0 for "
            f"{tenant_id}/{forecast_model_name}/{anchor_timestamp.isoformat()}"
        )


def _horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list) or not horizon:
        raise ValueError("evaluation_payload must contain a non-empty horizon list")
    rows: list[dict[str, Any]] = []
    for point in horizon:
        if not isinstance(point, dict):
            raise ValueError("evaluation_payload horizon entries must be objects")
        rows.append(point)
    return rows


def _future_interval_start(point: dict[str, Any], *, anchor_timestamp: datetime) -> datetime:
    interval_start = _datetime_value(point.get("interval_start"), field_name="interval_start")
    if interval_start <= anchor_timestamp:
        raise ValueError("forecast interval_start must be after anchor_timestamp")
    return interval_start


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row["evaluation_payload"]
    if not isinstance(payload, dict):
        raise TypeError("evaluation_payload must be a dict")
    return payload


def _float_list(value: Any, *, field_name: str) -> list[float]:
    if not isinstance(value, list):
        raise TypeError(f"{field_name} must be a list")
    return [_float_value(item, field_name=field_name) for item in value]


def _float_value(value: Any, *, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field_name} must be numeric") from exc


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"{field_name} must be a datetime value")


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing required columns: {missing_columns}")
