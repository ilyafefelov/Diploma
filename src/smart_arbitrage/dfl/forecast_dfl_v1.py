"""Tiny decision-loss forecast correction candidate for DFL v1 evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from math import ceil, isfinite
from typing import Any, Final

import polars as pl
import torch

from smart_arbitrage.dfl.decision_loss import (
    DecisionLossWeights,
    compute_decision_loss_v1,
)
from smart_arbitrage.dfl.decision_targeting import (
    _datetime_value,
    _final_holdout_rows,
    _forecast_frame_from_payload,
    _payload,
    _price_history_from_payload,
    _require_thesis_grade_observed,
)
from smart_arbitrage.dfl.offline_experiment import (
    _ExperimentExample,
    _Score,
    _examples_from_frame,
    _price_tensor,
    _realized_values,
    _single_starting_soc,
)
from smart_arbitrage.dfl.relaxed_dispatch import solve_relaxed_dispatch_tensor
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

DFL_FORECAST_DFL_V1_EXPERIMENT_NAME: Final[str] = "dfl_forecast_decision_loss_v1"
DFL_FORECAST_DFL_V1_CLAIM_SCOPE: Final[str] = "dfl_forecast_decision_loss_v1_not_full_dfl"
DFL_FORECAST_DFL_V1_STRICT_LP_STRATEGY_KIND: Final[str] = "dfl_forecast_decision_loss_v1_strict_lp_benchmark"
DFL_FORECAST_DFL_V1_STRICT_CLAIM_SCOPE: Final[str] = "dfl_forecast_decision_loss_v1_strict_lp_gate_not_full_dfl"
DFL_FORECAST_DFL_V1_PREFIX: Final[str] = "dfl_forecast_dfl_v1_"
DFL_FORECAST_DFL_V1_ACADEMIC_SCOPE: Final[str] = (
    "Tiny prior-only horizon-bias correction trained with a relaxed decision loss. "
    "It is not full DFL, not Decision Transformer control, and not market execution."
)
RELAXED_SOLVER_FALLBACK_REGRET_UAH: Final[float] = 1_000_000_000_000.0
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
REQUIRED_DFL_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "final_validation_anchor_count",
        "first_final_holdout_anchor_timestamp",
        "last_final_holdout_anchor_timestamp",
        "dfl_v1_checkpoint_horizon_biases_uah_mwh",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
    }
)


def dfl_forecast_dfl_v1_model_name(source_model_name: str) -> str:
    """Return the strict-score candidate name for DFL v1 correction."""

    return f"{DFL_FORECAST_DFL_V1_PREFIX}{source_model_name}"


def build_dfl_forecast_dfl_v1_panel_frame(
    evaluation_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    max_train_anchors_per_tenant: int = 72,
    inner_validation_fraction: float = 0.2,
    epoch_count: int = 8,
    learning_rate: float = 10.0,
    capacity_mwh: float = 1.0,
    max_power_mw: float = 0.25,
    soc_min_fraction: float = 0.05,
    soc_max_fraction: float = 0.95,
    degradation_cost_per_mwh: float = 0.0,
    decision_loss_weights: DecisionLossWeights | None = None,
) -> pl.DataFrame:
    """Train a tiny horizon-bias correction using prior anchors only."""

    _require_columns(evaluation_frame, REQUIRED_BENCHMARK_COLUMNS, frame_name="evaluation_frame")
    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        max_train_anchors_per_tenant=max_train_anchors_per_tenant,
        inner_validation_fraction=inner_validation_fraction,
        epoch_count=epoch_count,
        learning_rate=learning_rate,
        capacity_mwh=capacity_mwh,
        max_power_mw=max_power_mw,
        soc_min_fraction=soc_min_fraction,
        soc_max_fraction=soc_max_fraction,
        degradation_cost_per_mwh=degradation_cost_per_mwh,
    )
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = _source_rows(
                evaluation_frame,
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
            )
            if source_rows.height <= final_validation_anchor_count_per_tenant + 1:
                raise ValueError(
                    "DFL v1 panel needs prior and final-holdout anchors for "
                    f"{tenant_id}/{source_model_name}."
                )
            final_holdout_frame = source_rows.tail(final_validation_anchor_count_per_tenant)
            prior_frame = source_rows.head(source_rows.height - final_validation_anchor_count_per_tenant)
            train_selection_frame = prior_frame.tail(max_train_anchors_per_tenant)
            _require_source_rows_thesis_grade_observed(
                pl.concat([train_selection_frame, final_holdout_frame], how="vertical"),
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
            )
            fit_frame, inner_frame = _split_prior_rows(
                train_selection_frame,
                inner_validation_fraction=inner_validation_fraction,
            )
            fit_examples = _examples_from_frame(fit_frame)
            inner_examples = _examples_from_frame(inner_frame)
            final_examples = _examples_from_frame(final_holdout_frame)
            if not fit_examples or not inner_examples or not final_examples:
                raise ValueError(f"DFL v1 examples are empty for {tenant_id}/{source_model_name}.")
            horizon_hours = len(fit_examples[0].forecast_prices)
            if any(
                len(example.forecast_prices) != horizon_hours
                for example in [*fit_examples, *inner_examples, *final_examples]
            ):
                raise ValueError("DFL v1 requires consistent horizon lengths.")

            (
                final_biases,
                final_loss,
                checkpoint_biases,
                checkpoint_epoch,
                checkpoint_inner_score,
                training_solver_status,
            ) = (
                _train_decision_loss_horizon_biases_with_checkpoints(
                    training_examples=fit_examples,
                    inner_selection_examples=inner_examples,
                    horizon_hours=horizon_hours,
                    epoch_count=epoch_count,
                    learning_rate=learning_rate,
                    capacity_mwh=capacity_mwh,
                    max_power_mw=max_power_mw,
                    soc_min_fraction=soc_min_fraction,
                    soc_max_fraction=soc_max_fraction,
                    degradation_cost_per_mwh=degradation_cost_per_mwh,
                    decision_loss_weights=decision_loss_weights,
                )
            )
            raw_final, raw_final_solver_status = _safe_score_examples(
                examples=final_examples,
                horizon_biases=[0.0] * horizon_hours,
                capacity_mwh=capacity_mwh,
                max_power_mw=max_power_mw,
                soc_min_fraction=soc_min_fraction,
                soc_max_fraction=soc_max_fraction,
                degradation_cost_per_mwh=degradation_cost_per_mwh,
            )
            dfl_final, dfl_final_solver_status = _safe_score_examples(
                examples=final_examples,
                horizon_biases=checkpoint_biases,
                capacity_mwh=capacity_mwh,
                max_power_mw=max_power_mw,
                soc_min_fraction=soc_min_fraction,
                soc_max_fraction=soc_max_fraction,
                degradation_cost_per_mwh=degradation_cost_per_mwh,
            )
            solver_status = _merge_solver_statuses(
                training_solver_status,
                raw_final_solver_status,
                dfl_final_solver_status,
            )
            rows.append(
                {
                    "experiment_name": DFL_FORECAST_DFL_V1_EXPERIMENT_NAME,
                    "tenant_id": tenant_id,
                    "forecast_model_name": source_model_name,
                    "dfl_v1_model_name": dfl_forecast_dfl_v1_model_name(source_model_name),
                    "fit_anchor_count": fit_frame.height,
                    "inner_selection_anchor_count": inner_frame.height,
                    "final_validation_anchor_count": final_holdout_frame.height,
                    "horizon_hours": horizon_hours,
                    "epoch_count": epoch_count,
                    "learning_rate": learning_rate,
                    "last_fit_anchor_timestamp": _last_anchor_timestamp(fit_frame),
                    "first_inner_selection_anchor_timestamp": _first_anchor_timestamp(inner_frame),
                    "last_inner_selection_anchor_timestamp": _last_anchor_timestamp(inner_frame),
                    "first_final_holdout_anchor_timestamp": _first_anchor_timestamp(final_holdout_frame),
                    "last_final_holdout_anchor_timestamp": _last_anchor_timestamp(final_holdout_frame),
                    "dfl_v1_final_horizon_biases_uah_mwh": final_biases,
                    "dfl_v1_checkpoint_horizon_biases_uah_mwh": checkpoint_biases,
                    "dfl_v1_checkpoint_epoch": checkpoint_epoch,
                    "dfl_v1_final_training_loss": final_loss,
                    "dfl_v1_inner_selection_relaxed_regret_uah": checkpoint_inner_score.mean_regret_uah,
                    "raw_final_holdout_relaxed_regret_uah": raw_final.mean_regret_uah,
                    "dfl_v1_final_holdout_relaxed_regret_uah": dfl_final.mean_regret_uah,
                    "relaxed_regret_delta_uah": raw_final.mean_regret_uah - dfl_final.mean_regret_uah,
                    "relaxed_solver_status": solver_status,
                    "data_quality_tier": "thesis_grade",
                    "observed_coverage_ratio": 1.0,
                    "claim_scope": DFL_FORECAST_DFL_V1_CLAIM_SCOPE,
                    "academic_scope": DFL_FORECAST_DFL_V1_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["tenant_id", "forecast_model_name"])


def build_dfl_forecast_dfl_v1_strict_lp_benchmark_frame(
    evaluation_frame: pl.DataFrame,
    dfl_v1_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict LP/oracle score raw source forecasts and DFL v1 corrected forecasts."""

    _require_columns(evaluation_frame, REQUIRED_BENCHMARK_COLUMNS, frame_name="evaluation_frame")
    _require_columns(dfl_v1_panel_frame, REQUIRED_DFL_PANEL_COLUMNS, frame_name="dfl_v1_panel_frame")
    resolved_generated_at = generated_at or datetime.now(UTC)
    frames: list[pl.DataFrame] = []
    for tenant_id in tenant_ids:
        tenant_defaults = tenant_battery_defaults_from_registry(tenant_id)
        for source_model_name in forecast_model_names:
            panel_row = _single_panel_row(
                dfl_v1_panel_frame,
                tenant_id=tenant_id,
                forecast_model_name=source_model_name,
            )
            _validate_panel_row(
                panel_row,
                final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
            )
            horizon_biases = _float_list(
                panel_row["dfl_v1_checkpoint_horizon_biases_uah_mwh"],
                field_name="dfl_v1_checkpoint_horizon_biases_uah_mwh",
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
                row_kind="strict_similar_day",
            )
            control_by_anchor = {
                _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
                for row in control_rows.iter_rows(named=True)
            }
            for source_row in source_rows.iter_rows(named=True):
                anchor_timestamp = _datetime_value(source_row["anchor_timestamp"], field_name="anchor_timestamp")
                control_row = control_by_anchor[anchor_timestamp]
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
                            model_name=dfl_forecast_dfl_v1_model_name(source_model_name),
                            forecast_frame=_forecast_frame_from_payload(
                                source_payload,
                                anchor_timestamp=anchor_timestamp,
                                horizon_biases=horizon_biases,
                            ),
                            point_prediction_column="predicted_price_uah_mwh",
                        ),
                    ],
                    evaluation_id=(
                        f"{tenant_id}:dfl-forecast-v1-strict:{source_model_name}:"
                        f"{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
                    ),
                    generated_at=resolved_generated_at,
                )
                frames.append(
                    _with_dfl_v1_metadata(
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


def _train_decision_loss_horizon_biases_with_checkpoints(
    *,
    training_examples: list[_ExperimentExample],
    inner_selection_examples: list[_ExperimentExample],
    horizon_hours: int,
    epoch_count: int,
    learning_rate: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    degradation_cost_per_mwh: float,
    decision_loss_weights: DecisionLossWeights | None,
) -> tuple[list[float], float, list[float], int, Any, str]:
    starting_soc_fraction = _single_starting_soc([*training_examples, *inner_selection_examples])
    best_inner_score, best_inner_status = _safe_score_examples(
        examples=inner_selection_examples,
        horizon_biases=[0.0] * horizon_hours,
        capacity_mwh=capacity_mwh,
        max_power_mw=max_power_mw,
        soc_min_fraction=soc_min_fraction,
        soc_max_fraction=soc_max_fraction,
        degradation_cost_per_mwh=degradation_cost_per_mwh,
    )
    solver_statuses = [best_inner_status]
    forecast_prices = _price_tensor([example.forecast_prices for example in training_examples])
    actual_prices = _price_tensor([example.actual_prices for example in training_examples])
    horizon_biases = torch.zeros(horizon_hours, dtype=torch.float64, requires_grad=True)
    optimizer = torch.optim.Adam([horizon_biases], lr=learning_rate)
    best_biases = [0.0] * horizon_hours
    best_epoch = 0
    final_loss = 0.0
    for epoch_index in range(1, epoch_count + 1):
        optimizer.zero_grad()
        corrected_prices = forecast_prices + horizon_biases
        try:
            corrected_dispatch = solve_relaxed_dispatch_tensor(
                prices_uah_mwh=corrected_prices,
                starting_soc_fraction=starting_soc_fraction,
                capacity_mwh=capacity_mwh,
                max_power_mw=max_power_mw,
                soc_min_fraction=soc_min_fraction,
                soc_max_fraction=soc_max_fraction,
                degradation_cost_per_mwh=degradation_cost_per_mwh,
                solver_args={"eps": 1e-6, "max_iters": 5000},
            )
            oracle_dispatch = solve_relaxed_dispatch_tensor(
                prices_uah_mwh=actual_prices,
                starting_soc_fraction=starting_soc_fraction,
                capacity_mwh=capacity_mwh,
                max_power_mw=max_power_mw,
                soc_min_fraction=soc_min_fraction,
                soc_max_fraction=soc_max_fraction,
                degradation_cost_per_mwh=degradation_cost_per_mwh,
                solver_args={"eps": 1e-6, "max_iters": 5000},
            )
        except Exception as exc:
            solver_statuses.append(_fallback_status("training_epoch", exc))
            break
        solver_statuses.append(
            _merge_solver_statuses(
                corrected_dispatch.solver_status,
                oracle_dispatch.solver_status,
            )
        )
        oracle_values = _realized_values(
            actual_prices=actual_prices,
            charge=oracle_dispatch.charge_mw,
            discharge=oracle_dispatch.discharge_mw,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        loss_result = compute_decision_loss_v1(
            predicted_prices=corrected_prices,
            actual_prices=actual_prices,
            charge_mw=corrected_dispatch.charge_mw,
            discharge_mw=corrected_dispatch.discharge_mw,
            oracle_value_uah=oracle_values.detach(),
            degradation_cost_per_mwh=degradation_cost_per_mwh,
            weights=decision_loss_weights,
        )
        bias_regularization = torch.mean(torch.square(horizon_biases / 1000.0))
        loss = loss_result.total_loss + bias_regularization
        loss.backward()
        optimizer.step()
        with torch.no_grad():
            horizon_biases.clamp_(min=-5000.0, max=5000.0)
        final_loss = float(loss.detach().cpu())
        candidate_biases = [round(float(value), 6) for value in horizon_biases.detach().cpu().tolist()]
        candidate_inner_score, candidate_inner_status = _safe_score_examples(
            examples=inner_selection_examples,
            horizon_biases=candidate_biases,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        solver_statuses.append(candidate_inner_status)
        if (
            _is_relaxed_score_usable(candidate_inner_status)
            and candidate_inner_score.mean_regret_uah <= best_inner_score.mean_regret_uah
        ):
            best_biases = candidate_biases
            best_epoch = epoch_index
            best_inner_score = candidate_inner_score
    final_biases = [round(float(value), 6) for value in horizon_biases.detach().cpu().tolist()]
    return final_biases, final_loss, best_biases, best_epoch, best_inner_score, _merge_solver_statuses(*solver_statuses)


def _safe_score_examples(
    *,
    examples: list[_ExperimentExample],
    horizon_biases: list[float],
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    degradation_cost_per_mwh: float,
) -> tuple[_Score, str]:
    try:
        score, solver_status = _score_examples(
            examples=examples,
            horizon_biases=horizon_biases,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
    except Exception as exc:
        return _fallback_score(), _fallback_status("score", exc)
    if not all(
        isfinite(value)
        for value in (
            score.mean_realized_value_uah,
            score.mean_oracle_value_uah,
            score.mean_regret_uah,
        )
    ):
        return _fallback_score(), "fallback:score:non_finite"
    return score, solver_status


def _score_examples(
    *,
    examples: list[_ExperimentExample],
    horizon_biases: list[float],
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    degradation_cost_per_mwh: float,
) -> tuple[_Score, str]:
    starting_soc_fraction = _single_starting_soc(examples)
    forecast_prices = _price_tensor([example.forecast_prices for example in examples])
    actual_prices = _price_tensor([example.actual_prices for example in examples])
    bias_tensor = torch.tensor(horizon_biases, dtype=torch.float64)
    with torch.no_grad():
        corrected_dispatch = solve_relaxed_dispatch_tensor(
            prices_uah_mwh=forecast_prices + bias_tensor,
            starting_soc_fraction=starting_soc_fraction,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
            solver_args={"eps": 1e-6, "max_iters": 5000},
        )
        oracle_dispatch = solve_relaxed_dispatch_tensor(
            prices_uah_mwh=actual_prices,
            starting_soc_fraction=starting_soc_fraction,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
            solver_args={"eps": 1e-6, "max_iters": 5000},
        )
        realized_values = _realized_values(
            actual_prices=actual_prices,
            charge=corrected_dispatch.charge_mw,
            discharge=corrected_dispatch.discharge_mw,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        oracle_values = _realized_values(
            actual_prices=actual_prices,
            charge=oracle_dispatch.charge_mw,
            discharge=oracle_dispatch.discharge_mw,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        regrets = torch.clamp(oracle_values - realized_values, min=0.0)
    return (
        _Score(
            mean_realized_value_uah=float(torch.mean(realized_values).cpu()),
            mean_oracle_value_uah=float(torch.mean(oracle_values).cpu()),
            mean_regret_uah=float(torch.mean(regrets).cpu()),
        ),
        _merge_solver_statuses(
            corrected_dispatch.solver_status,
            oracle_dispatch.solver_status,
        ),
    )


def _fallback_score() -> _Score:
    return _Score(
        mean_realized_value_uah=0.0,
        mean_oracle_value_uah=0.0,
        mean_regret_uah=RELAXED_SOLVER_FALLBACK_REGRET_UAH,
    )


def _fallback_status(stage: str, exc: Exception) -> str:
    return f"fallback:{stage}:{exc.__class__.__name__}"


def _is_relaxed_score_usable(status: str) -> bool:
    return bool(status) and not status.startswith("fallback")


def _merge_solver_statuses(*statuses: str) -> str:
    non_ok_statuses = [status for status in statuses if status and status != "ok"]
    if not non_ok_statuses:
        return "ok"
    return ";".join(dict.fromkeys(non_ok_statuses))


def _source_rows(
    evaluation_frame: pl.DataFrame,
    *,
    tenant_id: str,
    forecast_model_name: str,
) -> pl.DataFrame:
    rows = (
        evaluation_frame
        .filter((pl.col("tenant_id") == tenant_id) & (pl.col("forecast_model_name") == forecast_model_name))
        .sort("anchor_timestamp")
    )
    if rows.height == 0:
        raise ValueError(f"missing DFL v1 source rows for {tenant_id}/{forecast_model_name}")
    if rows.select("anchor_timestamp").n_unique() != rows.height:
        raise ValueError(f"DFL v1 source rows must have unique anchors for {tenant_id}/{forecast_model_name}")
    return rows


def _split_prior_rows(rows: pl.DataFrame, *, inner_validation_fraction: float) -> tuple[pl.DataFrame, pl.DataFrame]:
    if rows.height < 2:
        raise ValueError("DFL v1 needs at least two prior rows for fit and inner selection.")
    inner_count = ceil(rows.height * inner_validation_fraction)
    inner_count = min(max(1, inner_count), rows.height - 1)
    return rows.head(rows.height - inner_count), rows.tail(inner_count)


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
        raise ValueError(f"missing DFL v1 panel row for {tenant_id}/{forecast_model_name}")
    if rows.height > 1:
        raise ValueError(f"duplicate DFL v1 panel rows for {tenant_id}/{forecast_model_name}")
    return rows.row(0, named=True)


def _validate_panel_row(
    panel_row: dict[str, Any],
    *,
    final_validation_anchor_count_per_tenant: int,
) -> None:
    if int(panel_row["final_validation_anchor_count"]) != final_validation_anchor_count_per_tenant:
        raise ValueError("DFL v1 panel final_validation_anchor_count must match strict scoring config.")
    if str(panel_row["data_quality_tier"]) != "thesis_grade":
        raise ValueError("DFL v1 strict benchmark requires thesis_grade panel rows.")
    if float(panel_row["observed_coverage_ratio"]) < 1.0:
        raise ValueError("DFL v1 strict benchmark requires observed coverage ratio of 1.0.")
    if not bool(panel_row["not_full_dfl"]):
        raise ValueError("DFL v1 strict benchmark requires not_full_dfl=true.")
    if not bool(panel_row["not_market_execution"]):
        raise ValueError("DFL v1 strict benchmark requires not_market_execution=true.")


def _with_dfl_v1_metadata(
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
                "strict_gate_kind": "dfl_forecast_decision_loss_v1_strict_lp",
                "source_forecast_model_name": source_model_name,
                "dfl_v1_forecast_model_name": dfl_forecast_dfl_v1_model_name(source_model_name),
                "dfl_v1_checkpoint_epoch": int(panel_row["dfl_v1_checkpoint_epoch"]),
                "dfl_v1_checkpoint_horizon_biases_uah_mwh": horizon_biases,
                "dfl_v1_inner_selection_relaxed_regret_uah": float(
                    panel_row["dfl_v1_inner_selection_relaxed_regret_uah"]
                ),
                "dfl_v1_relaxed_solver_status": str(panel_row.get("relaxed_solver_status", "ok")),
                "final_validation_anchor_count": int(panel_row["final_validation_anchor_count"]),
                "claim_scope": DFL_FORECAST_DFL_V1_STRICT_CLAIM_SCOPE,
                "academic_scope": DFL_FORECAST_DFL_V1_ACADEMIC_SCOPE,
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
        payloads.append(payload)
    return evaluation.with_columns(
        [
            pl.lit(DFL_FORECAST_DFL_V1_STRICT_LP_STRATEGY_KIND).alias("strategy_kind"),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
    max_train_anchors_per_tenant: int,
    inner_validation_fraction: float,
    epoch_count: int,
    learning_rate: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    degradation_cost_per_mwh: float,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
    if max_train_anchors_per_tenant < 2:
        raise ValueError("max_train_anchors_per_tenant must be at least 2.")
    if not 0.0 < inner_validation_fraction < 1.0:
        raise ValueError("inner_validation_fraction must be between 0 and 1.")
    if epoch_count <= 0:
        raise ValueError("epoch_count must be positive.")
    if learning_rate <= 0.0:
        raise ValueError("learning_rate must be positive.")
    if capacity_mwh <= 0.0 or max_power_mw <= 0.0:
        raise ValueError("capacity_mwh and max_power_mw must be positive.")
    if not 0.0 <= soc_min_fraction <= soc_max_fraction <= 1.0:
        raise ValueError("SOC bounds must stay within [0, 1].")
    if degradation_cost_per_mwh < 0.0:
        raise ValueError("degradation_cost_per_mwh cannot be negative.")


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing = required_columns.difference(frame.columns)
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {', '.join(sorted(missing))}")


def _float_list(value: Any, *, field_name: str) -> list[float]:
    if not isinstance(value, list):
        raise TypeError(f"{field_name} must be a list")
    return [float(item) for item in value]


def _first_anchor_timestamp(rows: pl.DataFrame) -> datetime:
    return _datetime_value(rows.row(0, named=True)["anchor_timestamp"], field_name="anchor_timestamp")


def _last_anchor_timestamp(rows: pl.DataFrame) -> datetime:
    return _datetime_value(rows.row(rows.height - 1, named=True)["anchor_timestamp"], field_name="anchor_timestamp")
