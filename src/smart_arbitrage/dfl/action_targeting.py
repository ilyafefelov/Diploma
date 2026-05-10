"""Action-targeted strict LP DFL candidate construction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.decision_targeting import (
    REQUIRED_BENCHMARK_COLUMNS,
    REQUIRED_DECISION_PANEL_COLUMNS,
    REQUIRED_PANEL_COLUMNS,
    DecisionTargetParameters,
    _datetime_value,
    _final_holdout_rows,
    _first_anchor_timestamp,
    _float_list,
    _forecast_frame_from_payload,
    _future_interval_start,
    _horizon_rows,
    _last_anchor_timestamp,
    _payload,
    _price_history_from_payload,
    _require_columns,
    _require_source_rows_thesis_grade_observed,
    _require_thesis_grade_observed,
    _single_decision_panel_row,
    _single_panel_row,
    _source_rows_before_final_holdout,
    _split_prior_rows,
    _validate_bias_length_against_rows,
    _validate_common_config,
    _validate_decision_panel_row,
    _validate_panel_row,
    corrected_decision_target_prices,
    decision_target_model_name,
    panel_v2_model_name,
)
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

ACTION_TARGET_EXPERIMENT_NAME: Final[str] = "offline_dfl_action_target_v4"
ACTION_TARGET_CLAIM_SCOPE: Final[str] = "offline_dfl_action_target_v4_not_full_dfl"
ACTION_TARGET_STRICT_LP_STRATEGY_KIND: Final[str] = "offline_dfl_action_target_strict_lp_benchmark"
ACTION_TARGET_STRICT_CLAIM_SCOPE: Final[str] = "offline_dfl_action_target_v4_strict_lp_gate_not_full_dfl"
ACTION_TARGET_V4_PREFIX: Final[str] = "offline_dfl_action_target_v4_"
DEFAULT_CHARGE_HOUR_COUNT_GRID: Final[tuple[int, ...]] = (2, 3)
DEFAULT_DISCHARGE_HOUR_COUNT_GRID: Final[tuple[int, ...]] = (2, 3)
DEFAULT_ACTION_SPREAD_GRID_UAH_MWH: Final[tuple[float, ...]] = (500.0, 1000.0, 1500.0)
DEFAULT_INCLUDE_PANEL_V2_BIAS_OPTIONS: Final[tuple[bool, ...]] = (False, True)
DEFAULT_INCLUDE_DECISION_V3_CORRECTION_OPTIONS: Final[tuple[bool, ...]] = (False, True)
ACTION_TARGET_ACADEMIC_SCOPE: Final[str] = (
    "Action-targeted forecast correction selected on prior strict LP/oracle regret only. "
    "It emphasizes raw forecast charge/discharge ranks and is not full DFL, not Decision "
    "Transformer control, and not market execution."
)
REQUIRED_ACTION_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "final_validation_anchor_count",
        "first_final_holdout_anchor_timestamp",
        "last_final_holdout_anchor_timestamp",
        "charge_hour_count",
        "discharge_hour_count",
        "action_spread_uah_mwh",
        "include_panel_v2_bias",
        "include_decision_v3_correction",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
    }
)


@dataclass(frozen=True, slots=True)
class ActionTargetParameters:
    """Selected raw-rank charge/discharge emphasis parameters."""

    charge_hour_count: int
    discharge_hour_count: int
    action_spread_uah_mwh: float
    include_panel_v2_bias: bool
    include_decision_v3_correction: bool


@dataclass(frozen=True, slots=True)
class _ActionTargetSelection:
    parameters: ActionTargetParameters
    inner_selection_mean_regret_uah: float


def action_target_model_name(source_model_name: str) -> str:
    """Return the v4 candidate name for a raw source model."""

    return f"{ACTION_TARGET_V4_PREFIX}{source_model_name}"


def corrected_action_target_prices(
    raw_forecast_prices: list[float],
    *,
    parameters: ActionTargetParameters,
    panel_v2_horizon_biases_uah_mwh: list[float],
    decision_target_parameters: DecisionTargetParameters,
) -> list[float]:
    """Apply the v4 raw-rank action-target correction rule."""

    if not raw_forecast_prices:
        raise ValueError("raw_forecast_prices must contain at least one value.")
    if len(panel_v2_horizon_biases_uah_mwh) != len(raw_forecast_prices):
        raise ValueError(
            "bias length must match horizon length; "
            f"observed {len(panel_v2_horizon_biases_uah_mwh)} vs {len(raw_forecast_prices)}"
        )
    _validate_action_parameters(parameters)

    if parameters.include_decision_v3_correction:
        corrected = corrected_decision_target_prices(
            raw_forecast_prices,
            spread_scale=decision_target_parameters.spread_scale,
            mean_shift_uah_mwh=decision_target_parameters.mean_shift_uah_mwh,
            include_panel_v2_bias=decision_target_parameters.include_panel_v2_bias,
            panel_v2_horizon_biases_uah_mwh=panel_v2_horizon_biases_uah_mwh,
        )
    else:
        corrected = list(raw_forecast_prices)

    if parameters.include_panel_v2_bias:
        corrected = [
            value + panel_v2_horizon_biases_uah_mwh[index]
            for index, value in enumerate(corrected)
        ]

    half_spread = parameters.action_spread_uah_mwh / 2.0
    charge_indices = _lowest_rank_indices(raw_forecast_prices, parameters.charge_hour_count)
    discharge_indices = _highest_rank_indices(
        raw_forecast_prices,
        parameters.discharge_hour_count,
        excluded_indices=charge_indices,
    )
    for index in charge_indices:
        corrected[index] -= half_spread
    for index in discharge_indices:
        corrected[index] += half_spread
    return corrected


def build_offline_dfl_action_target_panel_frame(
    evaluation_frame: pl.DataFrame,
    panel_frame: pl.DataFrame,
    decision_target_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    max_train_anchors_per_tenant: int = 72,
    inner_validation_fraction: float = 0.2,
    charge_hour_count_grid: tuple[int, ...] = DEFAULT_CHARGE_HOUR_COUNT_GRID,
    discharge_hour_count_grid: tuple[int, ...] = DEFAULT_DISCHARGE_HOUR_COUNT_GRID,
    action_spread_grid_uah_mwh: tuple[float, ...] = DEFAULT_ACTION_SPREAD_GRID_UAH_MWH,
    include_panel_v2_bias_options: tuple[bool, ...] = DEFAULT_INCLUDE_PANEL_V2_BIAS_OPTIONS,
    include_decision_v3_correction_options: tuple[bool, ...] = DEFAULT_INCLUDE_DECISION_V3_CORRECTION_OPTIONS,
) -> pl.DataFrame:
    """Select v4 action-rank parameters using only prior/inner-validation anchors."""

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
    if max_train_anchors_per_tenant < 2:
        raise ValueError("max_train_anchors_per_tenant must be at least 2.")
    if not 0.0 < inner_validation_fraction < 1.0:
        raise ValueError("inner_validation_fraction must be between 0 and 1.")
    _validate_grid(
        charge_hour_count_grid=charge_hour_count_grid,
        discharge_hour_count_grid=discharge_hour_count_grid,
        action_spread_grid_uah_mwh=action_spread_grid_uah_mwh,
        include_panel_v2_bias_options=include_panel_v2_bias_options,
        include_decision_v3_correction_options=include_decision_v3_correction_options,
    )

    rows: list[dict[str, Any]] = []
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
                decision_row["panel_v2_horizon_biases_uah_mwh"],
                field_name="panel_v2_horizon_biases_uah_mwh",
            )
            _validate_bias_length_against_rows(source_rows, horizon_biases)
            fit_rows, inner_rows = _split_prior_rows(
                source_rows,
                inner_validation_fraction=inner_validation_fraction,
            )
            decision_parameters = _decision_parameters_from_row(decision_row)
            selection = _select_action_target_parameters(
                inner_rows,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                horizon_biases=horizon_biases,
                decision_parameters=decision_parameters,
                charge_hour_count_grid=charge_hour_count_grid,
                discharge_hour_count_grid=discharge_hour_count_grid,
                action_spread_grid_uah_mwh=action_spread_grid_uah_mwh,
                include_panel_v2_bias_options=include_panel_v2_bias_options,
                include_decision_v3_correction_options=include_decision_v3_correction_options,
            )
            rows.append(
                _action_target_panel_row(
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    panel_row=panel_row,
                    decision_row=decision_row,
                    fit_rows=fit_rows,
                    inner_rows=inner_rows,
                    horizon_biases=horizon_biases,
                    decision_parameters=decision_parameters,
                    selection=selection,
                    charge_hour_count_grid=charge_hour_count_grid,
                    discharge_hour_count_grid=discharge_hour_count_grid,
                    action_spread_grid_uah_mwh=action_spread_grid_uah_mwh,
                    include_panel_v2_bias_options=include_panel_v2_bias_options,
                    include_decision_v3_correction_options=include_decision_v3_correction_options,
                )
            )
    return pl.DataFrame(rows)


def build_offline_dfl_action_target_strict_lp_benchmark_frame(
    evaluation_frame: pl.DataFrame,
    panel_frame: pl.DataFrame,
    decision_target_panel_frame: pl.DataFrame,
    action_target_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Score raw, panel v2, decision-target v3, and action-target v4 candidates."""

    _require_columns(evaluation_frame, REQUIRED_BENCHMARK_COLUMNS, frame_name="evaluation_frame")
    _require_columns(panel_frame, REQUIRED_PANEL_COLUMNS, frame_name="panel_frame")
    _require_columns(
        decision_target_panel_frame,
        REQUIRED_DECISION_PANEL_COLUMNS,
        frame_name="decision_target_panel_frame",
    )
    _require_columns(
        action_target_panel_frame,
        REQUIRED_ACTION_PANEL_COLUMNS,
        frame_name="action_target_panel_frame",
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
            action_row = _single_action_panel_row(
                action_target_panel_frame,
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
            _validate_action_panel_row(
                action_row,
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
                action_row["panel_v2_horizon_biases_uah_mwh"],
                field_name="panel_v2_horizon_biases_uah_mwh",
            )
            decision_parameters = _decision_parameters_from_row(decision_row)
            action_parameters = _action_parameters_from_row(action_row)
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
                                parameters=decision_parameters,
                            ),
                            point_prediction_column="predicted_price_uah_mwh",
                        ),
                        ForecastCandidate(
                            model_name=action_target_model_name(source_model_name),
                            forecast_frame=_action_target_forecast_frame_from_payload(
                                source_payload,
                                anchor_timestamp=anchor_timestamp,
                                horizon_biases=horizon_biases,
                                action_parameters=action_parameters,
                                decision_parameters=decision_parameters,
                            ),
                            point_prediction_column="predicted_price_uah_mwh",
                        ),
                    ],
                    evaluation_id=(
                        f"{tenant_id}:offline-dfl-action-target-strict:{source_model_name}:"
                        f"{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
                    ),
                    generated_at=resolved_generated_at,
                )
                frames.append(
                    _with_action_target_metadata(
                        evaluation,
                        source_model_name=source_model_name,
                        decision_row=decision_row,
                        action_row=action_row,
                        decision_parameters=decision_parameters,
                        action_parameters=action_parameters,
                        horizon_biases=horizon_biases,
                    )
                )
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed").sort(
        ["tenant_id", "anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


def _select_action_target_parameters(
    inner_rows: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    horizon_biases: list[float],
    decision_parameters: DecisionTargetParameters,
    charge_hour_count_grid: tuple[int, ...],
    discharge_hour_count_grid: tuple[int, ...],
    action_spread_grid_uah_mwh: tuple[float, ...],
    include_panel_v2_bias_options: tuple[bool, ...],
    include_decision_v3_correction_options: tuple[bool, ...],
) -> _ActionTargetSelection:
    best_selection: _ActionTargetSelection | None = None
    for charge_hour_count in charge_hour_count_grid:
        for discharge_hour_count in discharge_hour_count_grid:
            for action_spread in action_spread_grid_uah_mwh:
                for include_panel_bias in include_panel_v2_bias_options:
                    for include_decision_correction in include_decision_v3_correction_options:
                        parameters = ActionTargetParameters(
                            charge_hour_count=int(charge_hour_count),
                            discharge_hour_count=int(discharge_hour_count),
                            action_spread_uah_mwh=float(action_spread),
                            include_panel_v2_bias=bool(include_panel_bias),
                            include_decision_v3_correction=bool(include_decision_correction),
                        )
                        inner_mean_regret = _mean_strict_regret_for_rows(
                            inner_rows,
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            action_parameters=parameters,
                            decision_parameters=decision_parameters,
                            horizon_biases=horizon_biases,
                        )
                        if (
                            best_selection is None
                            or inner_mean_regret < best_selection.inner_selection_mean_regret_uah - 1e-9
                        ):
                            best_selection = _ActionTargetSelection(
                                parameters=parameters,
                                inner_selection_mean_regret_uah=inner_mean_regret,
                            )
    if best_selection is None:
        raise ValueError("action-target parameter grid produced no candidates.")
    return best_selection


def _mean_strict_regret_for_rows(
    rows: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    action_parameters: ActionTargetParameters,
    decision_parameters: DecisionTargetParameters,
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
                    model_name=action_target_model_name(source_model_name),
                    forecast_frame=_action_target_forecast_frame_from_payload(
                        payload,
                        anchor_timestamp=anchor_timestamp,
                        horizon_biases=horizon_biases,
                        action_parameters=action_parameters,
                        decision_parameters=decision_parameters,
                    ),
                    point_prediction_column="predicted_price_uah_mwh",
                )
            ],
            evaluation_id=(
                f"{tenant_id}:offline-dfl-action-target-selection:{source_model_name}:"
                f"{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
            ),
        )
        regrets.append(float(evaluation.row(0, named=True)["regret_uah"]))
    if not regrets:
        raise ValueError("inner selection rows must contain at least one row.")
    return mean(regrets)


def _action_target_panel_row(
    *,
    tenant_id: str,
    source_model_name: str,
    panel_row: dict[str, Any],
    decision_row: dict[str, Any],
    fit_rows: pl.DataFrame,
    inner_rows: pl.DataFrame,
    horizon_biases: list[float],
    decision_parameters: DecisionTargetParameters,
    selection: _ActionTargetSelection,
    charge_hour_count_grid: tuple[int, ...],
    discharge_hour_count_grid: tuple[int, ...],
    action_spread_grid_uah_mwh: tuple[float, ...],
    include_panel_v2_bias_options: tuple[bool, ...],
    include_decision_v3_correction_options: tuple[bool, ...],
) -> dict[str, Any]:
    return {
        "experiment_name": ACTION_TARGET_EXPERIMENT_NAME,
        "tenant_id": tenant_id,
        "forecast_model_name": source_model_name,
        "action_target_v4_model_name": action_target_model_name(source_model_name),
        "decision_target_v3_model_name": decision_target_model_name(source_model_name),
        "panel_v2_model_name": panel_v2_model_name(source_model_name),
        "fit_anchor_count": fit_rows.height,
        "inner_selection_anchor_count": inner_rows.height,
        "final_validation_anchor_count": int(panel_row["final_validation_anchor_count"]),
        "horizon_hours": len(horizon_biases),
        "charge_hour_count": selection.parameters.charge_hour_count,
        "discharge_hour_count": selection.parameters.discharge_hour_count,
        "action_spread_uah_mwh": selection.parameters.action_spread_uah_mwh,
        "include_panel_v2_bias": selection.parameters.include_panel_v2_bias,
        "include_decision_v3_correction": selection.parameters.include_decision_v3_correction,
        "decision_target_v3_spread_scale": decision_parameters.spread_scale,
        "decision_target_v3_mean_shift_uah_mwh": decision_parameters.mean_shift_uah_mwh,
        "decision_target_v3_include_panel_v2_bias": decision_parameters.include_panel_v2_bias,
        "decision_target_v3_inner_selection_mean_regret_uah": float(
            decision_row["inner_selection_mean_regret_uah"]
        ),
        "panel_v2_checkpoint_epoch": int(decision_row["panel_v2_checkpoint_epoch"]),
        "panel_v2_horizon_biases_uah_mwh": horizon_biases,
        "inner_selection_mean_regret_uah": selection.inner_selection_mean_regret_uah,
        "charge_hour_count_grid": [int(value) for value in charge_hour_count_grid],
        "discharge_hour_count_grid": [int(value) for value in discharge_hour_count_grid],
        "action_spread_grid_uah_mwh": [float(value) for value in action_spread_grid_uah_mwh],
        "include_panel_v2_bias_options": [bool(value) for value in include_panel_v2_bias_options],
        "include_decision_v3_correction_options": [
            bool(value) for value in include_decision_v3_correction_options
        ],
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
        "claim_scope": ACTION_TARGET_CLAIM_SCOPE,
        "academic_scope": ACTION_TARGET_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _with_action_target_metadata(
    evaluation: pl.DataFrame,
    *,
    source_model_name: str,
    decision_row: dict[str, Any],
    action_row: dict[str, Any],
    decision_parameters: DecisionTargetParameters,
    action_parameters: ActionTargetParameters,
    horizon_biases: list[float],
) -> pl.DataFrame:
    payloads: list[dict[str, Any]] = []
    for row in evaluation.iter_rows(named=True):
        payload = dict(row["evaluation_payload"])
        payload.update(
            {
                "strict_gate_kind": "offline_dfl_action_target_strict_lp",
                "source_forecast_model_name": source_model_name,
                "panel_v2_forecast_model_name": panel_v2_model_name(source_model_name),
                "decision_target_v3_forecast_model_name": decision_target_model_name(source_model_name),
                "action_target_v4_forecast_model_name": action_target_model_name(source_model_name),
                "decision_target_v3_spread_scale": decision_parameters.spread_scale,
                "decision_target_v3_mean_shift_uah_mwh": decision_parameters.mean_shift_uah_mwh,
                "decision_target_v3_include_panel_v2_bias": decision_parameters.include_panel_v2_bias,
                "decision_target_v3_inner_selection_mean_regret_uah": float(
                    decision_row["inner_selection_mean_regret_uah"]
                ),
                "action_target_v4_charge_hour_count": action_parameters.charge_hour_count,
                "action_target_v4_discharge_hour_count": action_parameters.discharge_hour_count,
                "action_target_v4_action_spread_uah_mwh": action_parameters.action_spread_uah_mwh,
                "action_target_v4_include_panel_v2_bias": action_parameters.include_panel_v2_bias,
                "action_target_v4_include_decision_v3_correction": (
                    action_parameters.include_decision_v3_correction
                ),
                "action_target_v4_inner_selection_mean_regret_uah": float(
                    action_row["inner_selection_mean_regret_uah"]
                ),
                "panel_v2_checkpoint_epoch": int(action_row["panel_v2_checkpoint_epoch"]),
                "panel_v2_horizon_biases_uah_mwh": horizon_biases,
                "final_validation_anchor_count": int(action_row["final_validation_anchor_count"]),
                "claim_scope": ACTION_TARGET_STRICT_CLAIM_SCOPE,
                "academic_scope": ACTION_TARGET_ACADEMIC_SCOPE,
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
        payloads.append(payload)
    return evaluation.with_columns(
        [
            pl.lit(ACTION_TARGET_STRICT_LP_STRATEGY_KIND).alias("strategy_kind"),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _action_target_forecast_frame_from_payload(
    payload: dict[str, Any],
    *,
    anchor_timestamp: datetime,
    horizon_biases: list[float],
    action_parameters: ActionTargetParameters,
    decision_parameters: DecisionTargetParameters,
) -> pl.DataFrame:
    horizon = _horizon_rows(payload)
    forecast_prices = corrected_action_target_prices(
        [
            float(point["forecast_price_uah_mwh"])
            for point in horizon
        ],
        parameters=action_parameters,
        panel_v2_horizon_biases_uah_mwh=horizon_biases,
        decision_target_parameters=decision_parameters,
    )
    return pl.DataFrame(
        {
            "forecast_timestamp": [
                _future_interval_start(point, anchor_timestamp=anchor_timestamp)
                for point in horizon
            ],
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
            float(point["forecast_price_uah_mwh"])
            for point in horizon
        ],
        spread_scale=parameters.spread_scale,
        mean_shift_uah_mwh=parameters.mean_shift_uah_mwh,
        include_panel_v2_bias=parameters.include_panel_v2_bias,
        panel_v2_horizon_biases_uah_mwh=horizon_biases,
    )
    return pl.DataFrame(
        {
            "forecast_timestamp": [
                _future_interval_start(point, anchor_timestamp=anchor_timestamp)
                for point in horizon
            ],
            "predicted_price_uah_mwh": forecast_prices,
        }
    )


def _single_action_panel_row(
    action_target_panel_frame: pl.DataFrame,
    *,
    tenant_id: str,
    forecast_model_name: str,
) -> dict[str, Any]:
    rows = action_target_panel_frame.filter(
        (pl.col("tenant_id") == tenant_id) & (pl.col("forecast_model_name") == forecast_model_name)
    )
    if rows.height == 0:
        raise ValueError(f"missing action-target panel row for {tenant_id}/{forecast_model_name}")
    if rows.height > 1:
        raise ValueError(f"duplicate action-target panel rows for {tenant_id}/{forecast_model_name}")
    return rows.row(0, named=True)


def _validate_action_panel_row(
    action_row: dict[str, Any],
    *,
    final_validation_anchor_count_per_tenant: int,
) -> None:
    observed_final_count = int(action_row["final_validation_anchor_count"])
    if observed_final_count != final_validation_anchor_count_per_tenant:
        raise ValueError(
            "action-target final_validation_anchor_count must match strict evaluation config; "
            f"observed {observed_final_count}, expected {final_validation_anchor_count_per_tenant}"
        )
    if str(action_row["data_quality_tier"]) != "thesis_grade":
        raise ValueError("action-target strict benchmark requires thesis_grade panel rows")
    if float(action_row["observed_coverage_ratio"]) < 1.0:
        raise ValueError("action-target strict benchmark requires observed coverage ratio of 1.0")
    if not bool(action_row["not_full_dfl"]):
        raise ValueError("action-target strict benchmark requires not_full_dfl=true")
    if not bool(action_row["not_market_execution"]):
        raise ValueError("action-target strict benchmark requires not_market_execution=true")


def _decision_parameters_from_row(row: dict[str, Any]) -> DecisionTargetParameters:
    return DecisionTargetParameters(
        spread_scale=float(row["spread_scale"]),
        mean_shift_uah_mwh=float(row["mean_shift_uah_mwh"]),
        include_panel_v2_bias=bool(row["include_panel_v2_bias"]),
    )


def _action_parameters_from_row(row: dict[str, Any]) -> ActionTargetParameters:
    return ActionTargetParameters(
        charge_hour_count=int(row["charge_hour_count"]),
        discharge_hour_count=int(row["discharge_hour_count"]),
        action_spread_uah_mwh=float(row["action_spread_uah_mwh"]),
        include_panel_v2_bias=bool(row["include_panel_v2_bias"]),
        include_decision_v3_correction=bool(row["include_decision_v3_correction"]),
    )


def _validate_grid(
    *,
    charge_hour_count_grid: tuple[int, ...],
    discharge_hour_count_grid: tuple[int, ...],
    action_spread_grid_uah_mwh: tuple[float, ...],
    include_panel_v2_bias_options: tuple[bool, ...],
    include_decision_v3_correction_options: tuple[bool, ...],
) -> None:
    if not charge_hour_count_grid:
        raise ValueError("charge_hour_count_grid must contain at least one value.")
    if not discharge_hour_count_grid:
        raise ValueError("discharge_hour_count_grid must contain at least one value.")
    if not action_spread_grid_uah_mwh:
        raise ValueError("action_spread_grid_uah_mwh must contain at least one value.")
    if not include_panel_v2_bias_options:
        raise ValueError("include_panel_v2_bias_options must contain at least one value.")
    if not include_decision_v3_correction_options:
        raise ValueError("include_decision_v3_correction_options must contain at least one value.")
    for charge_count in charge_hour_count_grid:
        if int(charge_count) <= 0:
            raise ValueError("charge_hour_count_grid values must be positive.")
    for discharge_count in discharge_hour_count_grid:
        if int(discharge_count) <= 0:
            raise ValueError("discharge_hour_count_grid values must be positive.")
    for action_spread in action_spread_grid_uah_mwh:
        if float(action_spread) < 0.0:
            raise ValueError("action_spread_grid_uah_mwh values must be non-negative.")


def _validate_action_parameters(parameters: ActionTargetParameters) -> None:
    if parameters.charge_hour_count <= 0:
        raise ValueError("charge_hour_count must be positive.")
    if parameters.discharge_hour_count <= 0:
        raise ValueError("discharge_hour_count must be positive.")
    if parameters.action_spread_uah_mwh < 0.0:
        raise ValueError("action_spread_uah_mwh must be non-negative.")


def _lowest_rank_indices(values: list[float], count: int) -> set[int]:
    return set(sorted(range(len(values)), key=lambda index: (values[index], index))[: min(count, len(values))])


def _highest_rank_indices(values: list[float], count: int, *, excluded_indices: set[int]) -> set[int]:
    ordered_indices = sorted(range(len(values)), key=lambda index: (-values[index], index))
    selected: list[int] = []
    for index in ordered_indices:
        if index in excluded_indices:
            continue
        selected.append(index)
        if len(selected) >= count:
            break
    return set(selected)
