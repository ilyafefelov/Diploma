"""Decision-targeted strict LP DFL candidate construction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from math import ceil
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

DECISION_TARGET_EXPERIMENT_NAME: Final[str] = "offline_dfl_decision_target_v3"
DECISION_TARGET_CLAIM_SCOPE: Final[str] = "offline_dfl_decision_target_v3_not_full_dfl"
DECISION_TARGET_STRICT_LP_STRATEGY_KIND: Final[str] = "offline_dfl_decision_target_strict_lp_benchmark"
DECISION_TARGET_STRICT_CLAIM_SCOPE: Final[str] = "offline_dfl_decision_target_v3_strict_lp_gate_not_full_dfl"
DECISION_TARGET_V3_PREFIX: Final[str] = "offline_dfl_decision_target_v3_"
PANEL_V2_PREFIX: Final[str] = "offline_dfl_panel_v2_"
DEFAULT_SPREAD_SCALE_GRID: Final[tuple[float, ...]] = (0.75, 1.0, 1.25, 1.5)
DEFAULT_MEAN_SHIFT_GRID_UAH_MWH: Final[tuple[float, ...]] = (-500.0, 0.0, 500.0)
DEFAULT_INCLUDE_PANEL_V2_BIAS_OPTIONS: Final[tuple[bool, ...]] = (False, True)
DECISION_TARGET_ACADEMIC_SCOPE: Final[str] = (
    "Decision-targeted forecast correction selected on prior strict LP/oracle regret only. "
    "It is not full DFL, not Decision Transformer control, and not market execution."
)
REQUIRED_BENCHMARK_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "generated_at",
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
REQUIRED_DECISION_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "final_validation_anchor_count",
        "first_final_holdout_anchor_timestamp",
        "last_final_holdout_anchor_timestamp",
        "spread_scale",
        "mean_shift_uah_mwh",
        "include_panel_v2_bias",
        "panel_v2_horizon_biases_uah_mwh",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
    }
)


@dataclass(frozen=True, slots=True)
class DecisionTargetParameters:
    """Selected affine/spread correction parameters."""

    spread_scale: float
    mean_shift_uah_mwh: float
    include_panel_v2_bias: bool


@dataclass(frozen=True, slots=True)
class _DecisionTargetSelection:
    parameters: DecisionTargetParameters
    inner_selection_mean_regret_uah: float


def decision_target_model_name(source_model_name: str) -> str:
    """Return the v3 candidate name for a raw source model."""

    return f"{DECISION_TARGET_V3_PREFIX}{source_model_name}"


def panel_v2_model_name(source_model_name: str) -> str:
    """Return the prior all-tenant panel v2 candidate name for a raw source model."""

    return f"{PANEL_V2_PREFIX}{source_model_name}"


def corrected_decision_target_prices(
    raw_forecast_prices: list[float],
    *,
    spread_scale: float,
    mean_shift_uah_mwh: float,
    include_panel_v2_bias: bool,
    panel_v2_horizon_biases_uah_mwh: list[float],
) -> list[float]:
    """Apply the v3 mean/spread decision-target correction rule."""

    if not raw_forecast_prices:
        raise ValueError("raw_forecast_prices must contain at least one value.")
    if len(panel_v2_horizon_biases_uah_mwh) != len(raw_forecast_prices):
        raise ValueError(
            "bias length must match horizon length; "
            f"observed {len(panel_v2_horizon_biases_uah_mwh)} vs {len(raw_forecast_prices)}"
        )
    raw_mean = mean(raw_forecast_prices)
    return [
        raw_mean
        + spread_scale * (raw_value - raw_mean)
        + mean_shift_uah_mwh
        + (panel_v2_horizon_biases_uah_mwh[index] if include_panel_v2_bias else 0.0)
        for index, raw_value in enumerate(raw_forecast_prices)
    ]


def build_offline_dfl_decision_target_panel_frame(
    evaluation_frame: pl.DataFrame,
    panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    max_train_anchors_per_tenant: int = 72,
    inner_validation_fraction: float = 0.2,
    spread_scale_grid: tuple[float, ...] = DEFAULT_SPREAD_SCALE_GRID,
    mean_shift_grid_uah_mwh: tuple[float, ...] = DEFAULT_MEAN_SHIFT_GRID_UAH_MWH,
    include_panel_v2_bias_options: tuple[bool, ...] = DEFAULT_INCLUDE_PANEL_V2_BIAS_OPTIONS,
) -> pl.DataFrame:
    """Select v3 correction parameters using only prior/inner-validation anchors."""

    _require_columns(evaluation_frame, REQUIRED_BENCHMARK_COLUMNS, frame_name="evaluation_frame")
    _require_columns(panel_frame, REQUIRED_PANEL_COLUMNS, frame_name="panel_frame")
    _validate_common_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
    )
    if max_train_anchors_per_tenant < 2:
        raise ValueError("max_train_anchors_per_tenant must be at least 2.")
    if not 0.0 < inner_validation_fraction < 1.0:
        raise ValueError("inner_validation_fraction must be between 0 and 1.")
    if not spread_scale_grid:
        raise ValueError("spread_scale_grid must contain at least one value.")
    if not mean_shift_grid_uah_mwh:
        raise ValueError("mean_shift_grid_uah_mwh must contain at least one value.")
    if not include_panel_v2_bias_options:
        raise ValueError("include_panel_v2_bias_options must contain at least one value.")

    rows: list[dict[str, Any]] = []
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
            source_rows = _source_rows_before_final_holdout(
                evaluation_frame,
                panel_row=panel_row,
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
                max_train_anchors_per_tenant=max_train_anchors_per_tenant,
            )
            _require_source_rows_thesis_grade_observed(
                source_rows,
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
            )
            horizon_biases = _float_list(
                panel_row["v2_checkpoint_horizon_biases_uah_mwh"],
                field_name="v2_checkpoint_horizon_biases_uah_mwh",
            )
            _validate_bias_length_against_rows(source_rows, horizon_biases)
            fit_rows, inner_rows = _split_prior_rows(
                source_rows,
                inner_validation_fraction=inner_validation_fraction,
            )
            selection = _select_decision_target_parameters(
                inner_rows,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                horizon_biases=horizon_biases,
                spread_scale_grid=spread_scale_grid,
                mean_shift_grid_uah_mwh=mean_shift_grid_uah_mwh,
                include_panel_v2_bias_options=include_panel_v2_bias_options,
            )
            rows.append(
                _decision_target_panel_row(
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    panel_row=panel_row,
                    fit_rows=fit_rows,
                    inner_rows=inner_rows,
                    horizon_biases=horizon_biases,
                    selection=selection,
                    spread_scale_grid=spread_scale_grid,
                    mean_shift_grid_uah_mwh=mean_shift_grid_uah_mwh,
                    include_panel_v2_bias_options=include_panel_v2_bias_options,
                )
            )
    return pl.DataFrame(rows)


def build_offline_dfl_decision_target_strict_lp_benchmark_frame(
    evaluation_frame: pl.DataFrame,
    panel_frame: pl.DataFrame,
    decision_target_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Score raw, panel v2, and decision-target v3 candidates with the strict LP/oracle evaluator."""

    _require_columns(evaluation_frame, REQUIRED_BENCHMARK_COLUMNS, frame_name="evaluation_frame")
    _require_columns(panel_frame, REQUIRED_PANEL_COLUMNS, frame_name="panel_frame")
    _require_columns(
        decision_target_panel_frame,
        REQUIRED_DECISION_PANEL_COLUMNS,
        frame_name="decision_target_panel_frame",
    )
    _validate_common_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
    )

    resolved_generated_at = generated_at or datetime.now(UTC)
    frames: list[pl.DataFrame] = []
    for source_model_name in forecast_model_names:
        for tenant_id in tenant_ids:
            panel_row = _single_panel_row(
                panel_frame,
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
            )
            decision_row = _single_decision_panel_row(
                decision_target_panel_frame,
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
            )
            _validate_panel_row(
                panel_row,
                final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
            )
            _validate_decision_panel_row(
                decision_row,
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
                decision_row["panel_v2_horizon_biases_uah_mwh"],
                field_name="panel_v2_horizon_biases_uah_mwh",
            )
            parameters = DecisionTargetParameters(
                spread_scale=float(decision_row["spread_scale"]),
                mean_shift_uah_mwh=float(decision_row["mean_shift_uah_mwh"]),
                include_panel_v2_bias=bool(decision_row["include_panel_v2_bias"]),
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
                            model_name=panel_v2_model_name(source_model_name),
                            forecast_frame=_forecast_frame_from_payload(
                                source_payload,
                                anchor_timestamp=anchor_timestamp,
                                horizon_biases=horizon_biases,
                            ),
                            point_prediction_column="predicted_price_uah_mwh",
                        ),
                        ForecastCandidate(
                            model_name=decision_target_model_name(source_model_name),
                            forecast_frame=_decision_target_forecast_frame_from_payload(
                                source_payload,
                                anchor_timestamp=anchor_timestamp,
                                horizon_biases=horizon_biases,
                                parameters=parameters,
                            ),
                            point_prediction_column="predicted_price_uah_mwh",
                        ),
                    ],
                    evaluation_id=(
                        f"{tenant_id}:offline-dfl-decision-target-strict:{source_model_name}:"
                        f"{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
                    ),
                    generated_at=resolved_generated_at,
                )
                frames.append(
                    _with_decision_target_metadata(
                        evaluation,
                        source_model_name=source_model_name,
                        decision_row=decision_row,
                        parameters=parameters,
                        horizon_biases=horizon_biases,
                    )
                )
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed").sort(
        ["tenant_id", "anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


def _select_decision_target_parameters(
    inner_rows: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    horizon_biases: list[float],
    spread_scale_grid: tuple[float, ...],
    mean_shift_grid_uah_mwh: tuple[float, ...],
    include_panel_v2_bias_options: tuple[bool, ...],
) -> _DecisionTargetSelection:
    best_selection: _DecisionTargetSelection | None = None
    for spread_scale in spread_scale_grid:
        for mean_shift in mean_shift_grid_uah_mwh:
            for include_bias in include_panel_v2_bias_options:
                parameters = DecisionTargetParameters(
                    spread_scale=float(spread_scale),
                    mean_shift_uah_mwh=float(mean_shift),
                    include_panel_v2_bias=bool(include_bias),
                )
                inner_mean_regret = _mean_strict_regret_for_rows(
                    inner_rows,
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    parameters=parameters,
                    horizon_biases=horizon_biases,
                )
                if best_selection is None or inner_mean_regret < best_selection.inner_selection_mean_regret_uah - 1e-9:
                    best_selection = _DecisionTargetSelection(
                        parameters=parameters,
                        inner_selection_mean_regret_uah=inner_mean_regret,
                    )
    if best_selection is None:
        raise ValueError("decision-target parameter grid produced no candidates.")
    return best_selection


def _mean_strict_regret_for_rows(
    rows: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    parameters: DecisionTargetParameters,
    horizon_biases: list[float],
) -> float:
    tenant_defaults = tenant_battery_defaults_from_registry(tenant_id)
    regrets: list[float] = []
    for row in rows.iter_rows(named=True):
        anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        payload = _payload(row)
        evaluation = evaluate_forecast_candidates_against_oracle(
            price_history=_price_history_from_payload(payload, anchor_timestamp=anchor_timestamp),
            tenant_id=tenant_id,
            battery_metrics=tenant_defaults.metrics,
            starting_soc_fraction=float(row["starting_soc_fraction"]),
            starting_soc_source=str(row["starting_soc_source"]),
            anchor_timestamp=anchor_timestamp,
            candidates=[
                ForecastCandidate(
                    model_name=decision_target_model_name(source_model_name),
                    forecast_frame=_decision_target_forecast_frame_from_payload(
                        payload,
                        anchor_timestamp=anchor_timestamp,
                        horizon_biases=horizon_biases,
                        parameters=parameters,
                    ),
                    point_prediction_column="predicted_price_uah_mwh",
                )
            ],
            evaluation_id=(
                f"{tenant_id}:offline-dfl-decision-target-selection:{source_model_name}:"
                f"{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
            ),
        )
        regrets.append(float(evaluation.row(0, named=True)["regret_uah"]))
    if not regrets:
        raise ValueError("inner selection rows must contain at least one row.")
    return mean(regrets)


def _decision_target_panel_row(
    *,
    tenant_id: str,
    source_model_name: str,
    panel_row: dict[str, Any],
    fit_rows: pl.DataFrame,
    inner_rows: pl.DataFrame,
    horizon_biases: list[float],
    selection: _DecisionTargetSelection,
    spread_scale_grid: tuple[float, ...],
    mean_shift_grid_uah_mwh: tuple[float, ...],
    include_panel_v2_bias_options: tuple[bool, ...],
) -> dict[str, Any]:
    return {
        "experiment_name": DECISION_TARGET_EXPERIMENT_NAME,
        "tenant_id": tenant_id,
        "forecast_model_name": source_model_name,
        "decision_target_v3_model_name": decision_target_model_name(source_model_name),
        "panel_v2_model_name": panel_v2_model_name(source_model_name),
        "fit_anchor_count": fit_rows.height,
        "inner_selection_anchor_count": inner_rows.height,
        "final_validation_anchor_count": int(panel_row["final_validation_anchor_count"]),
        "horizon_hours": len(horizon_biases),
        "spread_scale": selection.parameters.spread_scale,
        "mean_shift_uah_mwh": selection.parameters.mean_shift_uah_mwh,
        "include_panel_v2_bias": selection.parameters.include_panel_v2_bias,
        "panel_v2_checkpoint_epoch": int(panel_row["v2_checkpoint_epoch"]),
        "panel_v2_horizon_biases_uah_mwh": horizon_biases,
        "inner_selection_mean_regret_uah": selection.inner_selection_mean_regret_uah,
        "spread_scale_grid": [float(value) for value in spread_scale_grid],
        "mean_shift_grid_uah_mwh": [float(value) for value in mean_shift_grid_uah_mwh],
        "include_panel_v2_bias_options": [bool(value) for value in include_panel_v2_bias_options],
        "last_fit_anchor_timestamp": _last_anchor_timestamp(fit_rows),
        "first_inner_selection_anchor_timestamp": _first_anchor_timestamp(inner_rows),
        "last_inner_selection_anchor_timestamp": _last_anchor_timestamp(inner_rows),
        "first_final_holdout_anchor_timestamp": _datetime_value(
            panel_row["first_final_holdout_anchor_timestamp"],
            field_name="first_final_holdout_anchor_timestamp",
        ),
        "last_final_holdout_anchor_timestamp": _datetime_value(
            panel_row["last_final_holdout_anchor_timestamp"],
            field_name="last_final_holdout_anchor_timestamp",
        ),
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "claim_scope": DECISION_TARGET_CLAIM_SCOPE,
        "academic_scope": DECISION_TARGET_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _with_decision_target_metadata(
    evaluation: pl.DataFrame,
    *,
    source_model_name: str,
    decision_row: dict[str, Any],
    parameters: DecisionTargetParameters,
    horizon_biases: list[float],
) -> pl.DataFrame:
    payloads: list[dict[str, Any]] = []
    for row in evaluation.iter_rows(named=True):
        payload = dict(row["evaluation_payload"])
        payload.update(
            {
                "strict_gate_kind": "offline_dfl_decision_target_strict_lp",
                "source_forecast_model_name": source_model_name,
                "panel_v2_forecast_model_name": panel_v2_model_name(source_model_name),
                "decision_target_v3_forecast_model_name": decision_target_model_name(source_model_name),
                "decision_target_v3_spread_scale": parameters.spread_scale,
                "decision_target_v3_mean_shift_uah_mwh": parameters.mean_shift_uah_mwh,
                "decision_target_v3_include_panel_v2_bias": parameters.include_panel_v2_bias,
                "decision_target_v3_inner_selection_mean_regret_uah": float(
                    decision_row["inner_selection_mean_regret_uah"]
                ),
                "panel_v2_checkpoint_epoch": int(decision_row["panel_v2_checkpoint_epoch"]),
                "panel_v2_horizon_biases_uah_mwh": horizon_biases,
                "final_validation_anchor_count": int(decision_row["final_validation_anchor_count"]),
                "claim_scope": DECISION_TARGET_STRICT_CLAIM_SCOPE,
                "academic_scope": DECISION_TARGET_ACADEMIC_SCOPE,
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
        payloads.append(payload)
    return evaluation.with_columns(
        [
            pl.lit(DECISION_TARGET_STRICT_LP_STRATEGY_KIND).alias("strategy_kind"),
            pl.Series("evaluation_payload", payloads),
        ]
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


def _single_decision_panel_row(
    decision_target_panel_frame: pl.DataFrame,
    *,
    tenant_id: str,
    forecast_model_name: str,
) -> dict[str, Any]:
    rows = decision_target_panel_frame.filter(
        (pl.col("tenant_id") == tenant_id) & (pl.col("forecast_model_name") == forecast_model_name)
    )
    if rows.height == 0:
        raise ValueError(f"missing decision-target panel row for {tenant_id}/{forecast_model_name}")
    if rows.height > 1:
        raise ValueError(f"duplicate decision-target panel rows for {tenant_id}/{forecast_model_name}")
    return rows.row(0, named=True)


def _validate_panel_row(
    panel_row: dict[str, Any],
    *,
    final_validation_anchor_count_per_tenant: int,
) -> None:
    observed_final_count = int(panel_row["final_validation_anchor_count"])
    if observed_final_count != final_validation_anchor_count_per_tenant:
        raise ValueError(
            "offline DFL panel final_validation_anchor_count must match decision-target config; "
            f"observed {observed_final_count}, expected {final_validation_anchor_count_per_tenant}"
        )
    if str(panel_row["data_quality_tier"]) != "thesis_grade":
        raise ValueError("decision-target DFL requires thesis_grade panel rows")
    if float(panel_row["observed_coverage_ratio"]) < 1.0:
        raise ValueError("decision-target DFL requires observed coverage ratio of 1.0")
    if not bool(panel_row["not_full_dfl"]):
        raise ValueError("decision-target DFL requires not_full_dfl=true")
    if not bool(panel_row["not_market_execution"]):
        raise ValueError("decision-target DFL requires not_market_execution=true")


def _validate_decision_panel_row(
    decision_row: dict[str, Any],
    *,
    final_validation_anchor_count_per_tenant: int,
) -> None:
    observed_final_count = int(decision_row["final_validation_anchor_count"])
    if observed_final_count != final_validation_anchor_count_per_tenant:
        raise ValueError(
            "decision-target final_validation_anchor_count must match strict evaluation config; "
            f"observed {observed_final_count}, expected {final_validation_anchor_count_per_tenant}"
        )
    if str(decision_row["data_quality_tier"]) != "thesis_grade":
        raise ValueError("decision-target strict benchmark requires thesis_grade panel rows")
    if float(decision_row["observed_coverage_ratio"]) < 1.0:
        raise ValueError("decision-target strict benchmark requires observed coverage ratio of 1.0")
    if not bool(decision_row["not_full_dfl"]):
        raise ValueError("decision-target strict benchmark requires not_full_dfl=true")
    if not bool(decision_row["not_market_execution"]):
        raise ValueError("decision-target strict benchmark requires not_market_execution=true")


def _source_rows_before_final_holdout(
    evaluation_frame: pl.DataFrame,
    *,
    panel_row: dict[str, Any],
    tenant_id: str,
    forecast_model_name: str,
    max_train_anchors_per_tenant: int,
) -> pl.DataFrame:
    first_final_anchor = _datetime_value(
        panel_row["first_final_holdout_anchor_timestamp"],
        field_name="first_final_holdout_anchor_timestamp",
    )
    rows = (
        evaluation_frame
        .filter(
            (pl.col("tenant_id") == tenant_id)
            & (pl.col("forecast_model_name") == forecast_model_name)
            & (pl.col("anchor_timestamp") < first_final_anchor)
        )
        .sort("anchor_timestamp")
        .tail(max_train_anchors_per_tenant)
    )
    if rows.height < 2:
        raise ValueError(
            f"decision-target selection needs at least two prior rows for {tenant_id}/{forecast_model_name}; "
            f"observed {rows.height}"
        )
    anchor_count = rows.select("anchor_timestamp").n_unique()
    if anchor_count != rows.height:
        raise ValueError(f"prior rows must have unique anchors for {tenant_id}/{forecast_model_name}")
    return rows


def _split_prior_rows(
    rows: pl.DataFrame,
    *,
    inner_validation_fraction: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    inner_count = ceil(rows.height * inner_validation_fraction)
    inner_count = min(max(1, inner_count), rows.height - 1)
    fit_count = rows.height - inner_count
    return rows.head(fit_count), rows.tail(inner_count)


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
    forecast_timestamps = [_future_interval_start(point, anchor_timestamp=anchor_timestamp) for point in horizon]
    return pl.DataFrame(
        {
            "forecast_timestamp": forecast_timestamps,
            "predicted_price_uah_mwh": forecast_prices,
        }
    )


def _decision_target_forecast_frame_from_payload(
    payload: dict[str, Any],
    *,
    anchor_timestamp: datetime,
    horizon_biases: list[float],
    parameters: DecisionTargetParameters,
) -> pl.DataFrame:
    horizon = _horizon_rows(payload)
    forecast_prices = corrected_decision_target_prices(
        [
            _float_value(point["forecast_price_uah_mwh"], field_name="forecast_price_uah_mwh")
            for point in horizon
        ],
        spread_scale=parameters.spread_scale,
        mean_shift_uah_mwh=parameters.mean_shift_uah_mwh,
        include_panel_v2_bias=parameters.include_panel_v2_bias,
        panel_v2_horizon_biases_uah_mwh=horizon_biases,
    )
    forecast_timestamps = [_future_interval_start(point, anchor_timestamp=anchor_timestamp) for point in horizon]
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


def _require_source_rows_thesis_grade_observed(
    rows: pl.DataFrame,
    *,
    tenant_id: str,
    forecast_model_name: str,
) -> None:
    for row in rows.iter_rows(named=True):
        anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        _require_thesis_grade_observed(
            [_payload(row)],
            tenant_id=tenant_id,
            forecast_model_name=forecast_model_name,
            anchor_timestamp=anchor_timestamp,
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
            "decision-target DFL requires thesis_grade benchmark rows for "
            f"{tenant_id}/{forecast_model_name}/{anchor_timestamp.isoformat()}"
        )
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        raise ValueError(
            "decision-target DFL requires observed coverage ratio of 1.0 for "
            f"{tenant_id}/{forecast_model_name}/{anchor_timestamp.isoformat()}"
        )


def _validate_bias_length_against_rows(rows: pl.DataFrame, horizon_biases: list[float]) -> None:
    for row in rows.iter_rows(named=True):
        payload = _payload(row)
        horizon = _horizon_rows(payload)
        if len(horizon_biases) != len(horizon):
            raise ValueError(
                f"bias length must match horizon length; observed {len(horizon_biases)} vs {len(horizon)}"
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


def _first_anchor_timestamp(rows: pl.DataFrame) -> datetime:
    return _datetime_value(rows.row(0, named=True)["anchor_timestamp"], field_name="anchor_timestamp")


def _last_anchor_timestamp(rows: pl.DataFrame) -> datetime:
    return _datetime_value(rows.row(rows.height - 1, named=True)["anchor_timestamp"], field_name="anchor_timestamp")


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing required columns: {missing_columns}")


def _validate_common_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
