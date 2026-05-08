"""Dependency-free supervised action-label baseline for DFL readiness."""

from __future__ import annotations

from collections.abc import Iterable
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import LEVEL1_INTERVAL_MINUTES, LEVEL1_MARKET_VENUE
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics, DispatchCommand
from smart_arbitrage.strategy.forecast_strategy_evaluation import tenant_battery_defaults_from_registry


ALL_SOURCE_MODELS: Final = "all_source_models"
DEFAULT_BASELINE_NAME: Final = "dfl_action_classifier_v0"
CLAIM_SCOPE: Final = "dfl_action_classifier_baseline_not_full_dfl"
PROMOTION_STATUS: Final = "blocked_classification_only_no_strict_lp_value"
DFL_ACTION_CLASSIFIER_STRICT_LP_STRATEGY_KIND: Final = (
    "dfl_action_classifier_strict_lp_projection"
)
DFL_ACTION_CLASSIFIER_STRICT_CLAIM_SCOPE: Final = (
    "dfl_action_classifier_strict_lp_projection_not_full_dfl"
)

ACTION_LABELS: Final[tuple[str, ...]] = ("hold", "charge", "discharge")
REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "split_name",
        "horizon_hours",
        "forecast_price_vector_uah_mwh",
        "target_charge_mask",
        "target_discharge_mask",
        "target_hold_mask",
        "candidate_regret_uah",
        "strict_baseline_regret_uah",
        "candidate_net_value_uah",
        "strict_baseline_net_value_uah",
        "oracle_net_value_uah",
        "candidate_safety_violation_count",
        "strict_baseline_safety_violation_count",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_BENCHMARK_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "evaluation_id",
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "generated_at",
        "horizon_hours",
        "starting_soc_fraction",
        "starting_soc_source",
    }
)


@dataclass(frozen=True)
class ActionExample:
    tenant_id: str
    forecast_model_name: str
    split_name: str
    anchor_timestamp: datetime
    horizon_step_index: int
    rank_bin: str
    true_label: str


@dataclass(frozen=True)
class FittedActionClassifier:
    rules: dict[tuple[str, int, str], str]
    model_fallbacks: dict[str, str]
    global_fallback: str

    def predict(self, example: ActionExample) -> str:
        key = (
            example.forecast_model_name,
            example.horizon_step_index,
            example.rank_bin,
        )
        if key in self.rules:
            return self.rules[key]
        return self.model_fallbacks.get(example.forecast_model_name, self.global_fallback)


@dataclass(frozen=True)
class ActionProjectionResult:
    predicted_action_labels: list[str]
    charge_mw_vector: list[float]
    discharge_mw_vector: list[float]
    signed_dispatch_vector_mw: list[float]
    soc_before_mwh_vector: list[float]
    soc_after_mwh_vector: list[float]
    degradation_penalty_vector_uah: list[float]
    net_value_uah: float
    total_throughput_mwh: float
    total_degradation_penalty_uah: float
    committed_action: str
    committed_power_mw: float


def action_classifier_model_name(source_model_name: str, *, baseline_name: str = DEFAULT_BASELINE_NAME) -> str:
    """Return the strict projection candidate name for a source forecast model."""

    return f"{baseline_name}_{source_model_name}"


def build_dfl_action_classifier_baseline_frame(
    action_label_frame: pl.DataFrame,
    *,
    baseline_name: str = DEFAULT_BASELINE_NAME,
) -> pl.DataFrame:
    """Fit a majority-rule action classifier on train rows and score both splits.

    The baseline intentionally has no neural training stack. It is a transparent
    supervised classification probe over the existing strict LP/oracle action
    labels, and it never uses final-holdout labels during fitting.
    """

    _validate_action_label_frame(action_label_frame)
    examples = _expand_action_examples(action_label_frame)
    training_examples = [example for example in examples if example.split_name == "train_selection"]
    if not training_examples:
        raise ValueError("dfl_action_classifier_v0 requires train_selection rows")

    fitted = _fit_majority_classifier(training_examples)
    training_row_frame = action_label_frame.filter(pl.col("split_name") == "train_selection")
    summaries: list[dict[str, object]] = []
    model_names = sorted({example.forecast_model_name for example in examples})
    for split_name in ("train_selection", "final_holdout"):
        split_examples = [example for example in examples if example.split_name == split_name]
        split_rows = action_label_frame.filter(pl.col("split_name") == split_name)
        for model_name in [*model_names, ALL_SOURCE_MODELS]:
            if model_name == ALL_SOURCE_MODELS:
                scoped_examples = split_examples
                scoped_rows = split_rows
            else:
                scoped_examples = [
                    example for example in split_examples if example.forecast_model_name == model_name
                ]
                scoped_rows = split_rows.filter(pl.col("forecast_model_name") == model_name)
            if not scoped_examples:
                continue
            summaries.append(
                _build_summary_row(
                    baseline_name=baseline_name,
                    forecast_model_name=model_name,
                    split_name=split_name,
                    examples=scoped_examples,
                    row_frame=scoped_rows,
                    fitted=fitted,
                    training_row_frame=training_row_frame,
                    training_label_hour_count=len(training_examples),
                )
            )

    return pl.DataFrame(summaries).sort(["split_name", "forecast_model_name"])


def build_dfl_action_classifier_strict_lp_benchmark_frame(
    action_label_frame: pl.DataFrame,
    benchmark_frame: pl.DataFrame,
    *,
    baseline_name: str = DEFAULT_BASELINE_NAME,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Project classifier action labels to feasible dispatch and score final holdout.

    The classifier is fit only on train-selection labels. Final-holdout action
    labels are used only as evaluation labels; realized prices are used only for
    scoring the projected dispatch value.
    """

    _validate_action_label_frame(action_label_frame)
    _require_benchmark_columns(benchmark_frame)
    training_examples = [
        example
        for example in _expand_action_examples(action_label_frame)
        if example.split_name == "train_selection"
    ]
    if not training_examples:
        raise ValueError("dfl_action_classifier_v0 strict projection requires train_selection rows")
    fitted = _fit_majority_classifier(training_examples)
    benchmark_rows = _benchmark_rows_by_key(benchmark_frame)
    resolved_generated_at = generated_at or _latest_generated_at(benchmark_frame) or datetime.now(UTC)

    rows: list[dict[str, object]] = []
    final_holdout_rows = action_label_frame.filter(pl.col("split_name") == "final_holdout")
    for action_row in final_holdout_rows.iter_rows(named=True):
        tenant_id = str(action_row["tenant_id"])
        source_model_name = str(action_row["forecast_model_name"])
        anchor_timestamp = _datetime_value(action_row["anchor_timestamp"])
        source_benchmark_row = _required_benchmark_row(
            benchmark_rows,
            tenant_id=tenant_id,
            forecast_model_name=source_model_name,
            anchor_timestamp=anchor_timestamp,
        )
        _required_benchmark_row(
            benchmark_rows,
            tenant_id=tenant_id,
            forecast_model_name="strict_similar_day",
            anchor_timestamp=anchor_timestamp,
        )
        tenant_defaults = tenant_battery_defaults_from_registry(tenant_id)
        candidate_row = _candidate_projection_row(
            action_row=action_row,
            source_benchmark_row=source_benchmark_row,
            fitted=fitted,
            baseline_name=baseline_name,
            generated_at=resolved_generated_at,
            battery_metrics=tenant_defaults.metrics,
        )
        strict_row = _strict_control_row(
            action_row=action_row,
            source_benchmark_row=source_benchmark_row,
            generated_at=resolved_generated_at,
        )
        ranked_rows = sorted(
            [candidate_row, strict_row],
            key=lambda item: (_numeric_row_value(item, "regret_uah"), str(item["forecast_model_name"])),
        )
        for rank, ranked_row in enumerate(ranked_rows, start=1):
            ranked_row["rank_by_regret"] = rank
            rows.append(ranked_row)
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(
        ["tenant_id", "anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


def _validate_action_label_frame(frame: pl.DataFrame) -> None:
    if frame.is_empty():
        raise ValueError("dfl_action_classifier_v0 requires non-empty action-label rows")
    missing_columns = sorted(REQUIRED_COLUMNS.difference(frame.columns))
    if missing_columns:
        raise ValueError(
            "dfl_action_classifier_v0 input is missing required columns: "
            + ", ".join(missing_columns)
        )
    split_names = set(frame.select("split_name").to_series().to_list())
    if "train_selection" not in split_names:
        raise ValueError("dfl_action_classifier_v0 requires train_selection rows")
    if "final_holdout" not in split_names:
        raise ValueError("dfl_action_classifier_v0 requires final_holdout rows")
    non_thesis_count = frame.filter(pl.col("data_quality_tier") != "thesis_grade").height
    if non_thesis_count:
        raise ValueError("dfl_action_classifier_v0 requires thesis_grade action-label rows")
    non_observed_count = frame.filter(pl.col("observed_coverage_ratio") < 1.0).height
    if non_observed_count:
        raise ValueError("dfl_action_classifier_v0 requires observed coverage ratio of 1.0")
    if frame.filter(~pl.col("not_full_dfl")).height:
        raise ValueError("dfl_action_classifier_v0 rows must remain not_full_dfl")
    if frame.filter(~pl.col("not_market_execution")).height:
        raise ValueError("dfl_action_classifier_v0 rows must remain not_market_execution")
    if frame.filter(
        (pl.col("candidate_safety_violation_count") != 0)
        | (pl.col("strict_baseline_safety_violation_count") != 0)
    ).height:
        raise ValueError("dfl_action_classifier_v0 requires zero safety violations")


def _require_benchmark_columns(frame: pl.DataFrame) -> None:
    if frame.is_empty():
        raise ValueError("benchmark_frame must contain benchmark rows")
    missing_columns = sorted(REQUIRED_BENCHMARK_COLUMNS.difference(frame.columns))
    if missing_columns:
        raise ValueError("benchmark_frame is missing required columns: " + ", ".join(missing_columns))


def _expand_action_examples(frame: pl.DataFrame) -> list[ActionExample]:
    examples: list[ActionExample] = []
    for row in frame.iter_rows(named=True):
        horizon_hours = _positive_int(row["horizon_hours"], field_name="horizon_hours")
        forecast_prices = _float_vector(
            row["forecast_price_vector_uah_mwh"],
            expected_length=horizon_hours,
            field_name="forecast_price_vector_uah_mwh",
        )
        charge_mask = _int_mask(
            row["target_charge_mask"],
            expected_length=horizon_hours,
            field_name="target_charge_mask",
        )
        discharge_mask = _int_mask(
            row["target_discharge_mask"],
            expected_length=horizon_hours,
            field_name="target_discharge_mask",
        )
        hold_mask = _int_mask(
            row["target_hold_mask"],
            expected_length=horizon_hours,
            field_name="target_hold_mask",
        )
        rank_bins = _rank_bins(forecast_prices)
        for step_index, (rank_bin, charge, discharge, hold) in enumerate(
            zip(rank_bins, charge_mask, discharge_mask, hold_mask, strict=True)
        ):
            if charge + discharge + hold != 1:
                raise ValueError("dfl_action_classifier_v0 target action masks must be one-hot")
            true_label = _label_from_masks(charge=charge, discharge=discharge, hold=hold)
            examples.append(
                ActionExample(
                    tenant_id=str(row["tenant_id"]),
                    forecast_model_name=str(row["forecast_model_name"]),
                    split_name=str(row["split_name"]),
                    anchor_timestamp=_datetime_value(row["anchor_timestamp"]),
                    horizon_step_index=step_index,
                    rank_bin=rank_bin,
                    true_label=true_label,
                )
            )
    return examples


def _action_examples_from_row(row: dict[str, Any]) -> list[ActionExample]:
    horizon_hours = _positive_int(row["horizon_hours"], field_name="horizon_hours")
    forecast_prices = _float_vector(
        row["forecast_price_vector_uah_mwh"],
        expected_length=horizon_hours,
        field_name="forecast_price_vector_uah_mwh",
    )
    rank_bins = _rank_bins(forecast_prices)
    return [
        ActionExample(
            tenant_id=str(row["tenant_id"]),
            forecast_model_name=str(row["forecast_model_name"]),
            split_name=str(row["split_name"]),
            anchor_timestamp=_datetime_value(row["anchor_timestamp"]),
            horizon_step_index=step_index,
            rank_bin=rank_bin,
            true_label="hold",
        )
        for step_index, rank_bin in enumerate(rank_bins)
    ]


def _fit_majority_classifier(examples: list[ActionExample]) -> FittedActionClassifier:
    rule_counts: dict[tuple[str, int, str], Counter[str]] = defaultdict(Counter)
    model_counts: dict[str, Counter[str]] = defaultdict(Counter)
    global_counts: Counter[str] = Counter()
    for example in examples:
        key = (
            example.forecast_model_name,
            example.horizon_step_index,
            example.rank_bin,
        )
        rule_counts[key][example.true_label] += 1
        model_counts[example.forecast_model_name][example.true_label] += 1
        global_counts[example.true_label] += 1
    rules = {key: _majority_label(counts) for key, counts in rule_counts.items()}
    model_fallbacks = {model: _majority_label(counts) for model, counts in model_counts.items()}
    return FittedActionClassifier(
        rules=rules,
        model_fallbacks=model_fallbacks,
        global_fallback=_majority_label(global_counts),
    )


def _candidate_projection_row(
    *,
    action_row: dict[str, Any],
    source_benchmark_row: dict[str, Any],
    fitted: FittedActionClassifier,
    baseline_name: str,
    generated_at: datetime,
    battery_metrics: BatteryPhysicalMetrics,
) -> dict[str, object]:
    tenant_id = str(action_row["tenant_id"])
    source_model_name = str(action_row["forecast_model_name"])
    anchor_timestamp = _datetime_value(action_row["anchor_timestamp"])
    horizon_hours = _positive_int(action_row["horizon_hours"], field_name="horizon_hours")
    actual_prices = _float_vector(
        action_row["actual_price_vector_uah_mwh"],
        expected_length=horizon_hours,
        field_name="actual_price_vector_uah_mwh",
    )
    predicted_labels = [
        fitted.predict(example) for example in _action_examples_from_row(action_row)
    ]
    projection = _project_action_labels_to_dispatch(
        predicted_labels,
        actual_prices=actual_prices,
        battery_metrics=battery_metrics,
        starting_soc_fraction=float(source_benchmark_row["starting_soc_fraction"]),
        anchor_timestamp=anchor_timestamp,
    )
    oracle_value = float(action_row["oracle_net_value_uah"])
    regret_uah = max(0.0, oracle_value - projection.net_value_uah)
    classifier_model_name = action_classifier_model_name(
        source_model_name,
        baseline_name=baseline_name,
    )
    return {
        "evaluation_id": _projection_evaluation_id(
            tenant_id=tenant_id,
            source_model_name=source_model_name,
            anchor_timestamp=anchor_timestamp,
        ),
        "tenant_id": tenant_id,
        "forecast_model_name": classifier_model_name,
        "strategy_kind": DFL_ACTION_CLASSIFIER_STRICT_LP_STRATEGY_KIND,
        "market_venue": LEVEL1_MARKET_VENUE,
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": horizon_hours,
        "starting_soc_fraction": float(source_benchmark_row["starting_soc_fraction"]),
        "starting_soc_source": str(source_benchmark_row["starting_soc_source"]),
        "decision_value_uah": projection.net_value_uah,
        "forecast_objective_value_uah": projection.net_value_uah,
        "oracle_value_uah": oracle_value,
        "regret_uah": regret_uah,
        "regret_ratio": _regret_ratio(regret_uah, oracle_value),
        "total_degradation_penalty_uah": projection.total_degradation_penalty_uah,
        "total_throughput_mwh": projection.total_throughput_mwh,
        "committed_action": projection.committed_action,
        "committed_power_mw": projection.committed_power_mw,
        "rank_by_regret": 0,
        "evaluation_payload": _candidate_projection_payload(
            action_row=action_row,
            source_model_name=source_model_name,
            classifier_model_name=classifier_model_name,
            actual_prices=actual_prices,
            projection=projection,
        ),
    }


def _strict_control_row(
    *,
    action_row: dict[str, Any],
    source_benchmark_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, object]:
    tenant_id = str(action_row["tenant_id"])
    source_model_name = str(action_row["forecast_model_name"])
    anchor_timestamp = _datetime_value(action_row["anchor_timestamp"])
    horizon_hours = _positive_int(action_row["horizon_hours"], field_name="horizon_hours")
    signed_dispatch = _float_vector(
        action_row["strict_baseline_signed_dispatch_vector_mw"],
        expected_length=horizon_hours,
        field_name="strict_baseline_signed_dispatch_vector_mw",
    )
    oracle_value = float(action_row["oracle_net_value_uah"])
    regret_uah = float(action_row["strict_baseline_regret_uah"])
    committed_dispatch = _dispatch_command_from_net_power(
        interval_start=anchor_timestamp + timedelta(hours=1),
        net_power_mw=signed_dispatch[0],
        reason="dfl_action_classifier_strict_projection:strict_control",
    )
    return {
        "evaluation_id": _projection_evaluation_id(
            tenant_id=tenant_id,
            source_model_name=source_model_name,
            anchor_timestamp=anchor_timestamp,
        ),
        "tenant_id": tenant_id,
        "forecast_model_name": "strict_similar_day",
        "strategy_kind": DFL_ACTION_CLASSIFIER_STRICT_LP_STRATEGY_KIND,
        "market_venue": LEVEL1_MARKET_VENUE,
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": horizon_hours,
        "starting_soc_fraction": float(source_benchmark_row["starting_soc_fraction"]),
        "starting_soc_source": str(source_benchmark_row["starting_soc_source"]),
        "decision_value_uah": float(action_row["strict_baseline_net_value_uah"]),
        "forecast_objective_value_uah": float(action_row["strict_baseline_net_value_uah"]),
        "oracle_value_uah": oracle_value,
        "regret_uah": regret_uah,
        "regret_ratio": _regret_ratio(regret_uah, oracle_value),
        "total_degradation_penalty_uah": _optional_float(
            action_row,
            "strict_baseline_total_degradation_penalty_uah",
            default=0.0,
        ),
        "total_throughput_mwh": _optional_float(
            action_row,
            "strict_baseline_total_throughput_mwh",
            default=sum(abs(value) for value in signed_dispatch),
        ),
        "committed_action": committed_dispatch.action,
        "committed_power_mw": committed_dispatch.power_mw,
        "rank_by_regret": 0,
        "evaluation_payload": _strict_control_payload(
            action_row=action_row,
            source_model_name=source_model_name,
            signed_dispatch=signed_dispatch,
        ),
    }


def _project_action_labels_to_dispatch(
    predicted_action_labels: list[str],
    *,
    actual_prices: list[float],
    battery_metrics: BatteryPhysicalMetrics,
    starting_soc_fraction: float,
    anchor_timestamp: datetime,
) -> ActionProjectionResult:
    if len(predicted_action_labels) != len(actual_prices):
        raise ValueError("predicted action labels and actual prices must have the same length")
    if any(label not in ACTION_LABELS for label in predicted_action_labels):
        raise ValueError("predicted action labels must be charge, discharge, or hold")
    if not 0.0 <= starting_soc_fraction <= 1.0:
        raise ValueError("starting_soc_fraction must be between 0.0 and 1.0")

    cvxpy = _require_cvxpy()
    horizon = len(predicted_action_labels)
    dt_hours = LEVEL1_INTERVAL_MINUTES / 60.0
    charge_efficiency = battery_metrics.round_trip_efficiency ** 0.5
    discharge_efficiency = battery_metrics.round_trip_efficiency ** 0.5
    initial_soc_mwh = starting_soc_fraction * battery_metrics.capacity_mwh

    charge_mw = cvxpy.Variable(horizon, nonneg=True)
    discharge_mw = cvxpy.Variable(horizon, nonneg=True)
    soc_mwh = cvxpy.Variable(horizon + 1)
    constraints = [
        soc_mwh[0] == initial_soc_mwh,
        soc_mwh[1:]
        == soc_mwh[:-1]
        + (charge_mw * charge_efficiency * dt_hours)
        - (discharge_mw * dt_hours / discharge_efficiency),
        soc_mwh >= battery_metrics.soc_min_fraction * battery_metrics.capacity_mwh,
        soc_mwh <= battery_metrics.soc_max_fraction * battery_metrics.capacity_mwh,
        charge_mw <= battery_metrics.max_power_mw,
        discharge_mw <= battery_metrics.max_power_mw,
    ]
    objective_terms: list[Any] = []
    for step_index, label in enumerate(predicted_action_labels):
        if label == "charge":
            constraints.append(discharge_mw[step_index] == 0.0)
            objective_terms.append(charge_mw[step_index])
        elif label == "discharge":
            constraints.append(charge_mw[step_index] == 0.0)
            objective_terms.append(discharge_mw[step_index])
        else:
            constraints.append(charge_mw[step_index] == 0.0)
            constraints.append(discharge_mw[step_index] == 0.0)
    objective = cvxpy.Maximize(cvxpy.sum(objective_terms)) if objective_terms else cvxpy.Maximize(0)
    problem = cvxpy.Problem(objective, constraints)
    problem.solve()
    if problem.status not in {cvxpy.OPTIMAL, cvxpy.OPTIMAL_INACCURATE}:
        raise RuntimeError(f"Action-mask LP projection did not converge: status={problem.status}")

    charge_values = _solver_float_list(charge_mw.value, horizon)
    discharge_values = _solver_float_list(discharge_mw.value, horizon)
    soc_values = _solver_float_list(soc_mwh.value, horizon + 1)
    signed_dispatch: list[float] = []
    degradation_penalties: list[float] = []
    total_value = 0.0
    total_throughput = 0.0
    for step_index, actual_price in enumerate(actual_prices):
        charge_value = max(charge_values[step_index], 0.0)
        discharge_value = max(discharge_values[step_index], 0.0)
        net_power = discharge_value - charge_value
        throughput = (charge_value + discharge_value) * dt_hours
        degradation_penalty = battery_metrics.degradation_cost_per_mwh_throughput_uah * throughput
        total_value += actual_price * net_power * dt_hours - degradation_penalty
        total_throughput += throughput
        signed_dispatch.append(net_power)
        degradation_penalties.append(degradation_penalty)
    committed_dispatch = _dispatch_command_from_net_power(
        interval_start=anchor_timestamp + timedelta(hours=1),
        net_power_mw=signed_dispatch[0],
        reason="dfl_action_classifier_strict_projection",
    )
    return ActionProjectionResult(
        predicted_action_labels=list(predicted_action_labels),
        charge_mw_vector=charge_values,
        discharge_mw_vector=discharge_values,
        signed_dispatch_vector_mw=signed_dispatch,
        soc_before_mwh_vector=soc_values[:-1],
        soc_after_mwh_vector=soc_values[1:],
        degradation_penalty_vector_uah=degradation_penalties,
        net_value_uah=total_value,
        total_throughput_mwh=total_throughput,
        total_degradation_penalty_uah=sum(degradation_penalties),
        committed_action=committed_dispatch.action,
        committed_power_mw=committed_dispatch.power_mw,
    )


def _candidate_projection_payload(
    *,
    action_row: dict[str, Any],
    source_model_name: str,
    classifier_model_name: str,
    actual_prices: list[float],
    projection: ActionProjectionResult,
) -> dict[str, object]:
    anchor_timestamp = _datetime_value(action_row["anchor_timestamp"])
    return {
        "data_quality_tier": str(action_row["data_quality_tier"]),
        "observed_coverage_ratio": float(action_row["observed_coverage_ratio"]),
        "source_forecast_model_name": source_model_name,
        "action_classifier_model_name": classifier_model_name,
        "projection_method": "action_mask_lp_projection",
        "predicted_action_labels": projection.predicted_action_labels,
        "projected_signed_dispatch_vector_mw": projection.signed_dispatch_vector_mw,
        "uses_final_holdout_for_training": False,
        "split_name": "final_holdout",
        "claim_scope": DFL_ACTION_CLASSIFIER_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "horizon": [
            {
                "step_index": step_index,
                "interval_start": anchor_timestamp + timedelta(hours=step_index + 1),
                "actual_price_uah_mwh": actual_price,
                "predicted_action_label": projection.predicted_action_labels[step_index],
                "charge_mw": projection.charge_mw_vector[step_index],
                "discharge_mw": projection.discharge_mw_vector[step_index],
                "net_power_mw": projection.signed_dispatch_vector_mw[step_index],
                "soc_before_mwh": projection.soc_before_mwh_vector[step_index],
                "soc_after_mwh": projection.soc_after_mwh_vector[step_index],
                "degradation_penalty_uah": projection.degradation_penalty_vector_uah[step_index],
            }
            for step_index, actual_price in enumerate(actual_prices)
        ],
    }


def _strict_control_payload(
    *,
    action_row: dict[str, Any],
    source_model_name: str,
    signed_dispatch: list[float],
) -> dict[str, object]:
    actual_prices = _float_vector(
        action_row["actual_price_vector_uah_mwh"],
        expected_length=len(signed_dispatch),
        field_name="actual_price_vector_uah_mwh",
    )
    anchor_timestamp = _datetime_value(action_row["anchor_timestamp"])
    return {
        "data_quality_tier": str(action_row["data_quality_tier"]),
        "observed_coverage_ratio": float(action_row["observed_coverage_ratio"]),
        "source_forecast_model_name": source_model_name,
        "projection_method": "strict_similar_day_lp_from_benchmark",
        "split_name": "final_holdout",
        "claim_scope": DFL_ACTION_CLASSIFIER_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "horizon": [
            {
                "step_index": step_index,
                "interval_start": anchor_timestamp + timedelta(hours=step_index + 1),
                "actual_price_uah_mwh": actual_price,
                "net_power_mw": signed_dispatch[step_index],
            }
            for step_index, actual_price in enumerate(actual_prices)
        ],
    }


def _build_summary_row(
    *,
    baseline_name: str,
    forecast_model_name: str,
    split_name: str,
    examples: list[ActionExample],
    row_frame: pl.DataFrame,
    fitted: FittedActionClassifier,
    training_row_frame: pl.DataFrame,
    training_label_hour_count: int,
) -> dict[str, object]:
    predictions = [fitted.predict(example) for example in examples]
    true_labels = [example.true_label for example in examples]
    metrics = _classification_metrics(true_labels, predictions)
    return {
        "baseline_name": baseline_name,
        "summary_kind": "action_classifier_baseline",
        "forecast_model_name": forecast_model_name,
        "split_name": split_name,
        "action_label_row_count": row_frame.height,
        "label_hour_count": len(examples),
        "tenant_count": row_frame.select("tenant_id").n_unique() if row_frame.height else 0,
        "anchor_count": row_frame.select("anchor_timestamp").n_unique() if row_frame.height else 0,
        "accuracy": metrics["accuracy"],
        "macro_f1": metrics["macro_f1"],
        "true_charge_count": metrics["true_charge_count"],
        "true_discharge_count": metrics["true_discharge_count"],
        "true_hold_count": metrics["true_hold_count"],
        "pred_charge_count": metrics["pred_charge_count"],
        "pred_discharge_count": metrics["pred_discharge_count"],
        "pred_hold_count": metrics["pred_hold_count"],
        "trained_rule_count": len(fitted.rules),
        "fallback_label": fitted.global_fallback,
        "training_action_label_rows": training_row_frame.height,
        "training_label_hours": training_label_hour_count,
        "uses_final_holdout_for_training": False,
        "mean_candidate_regret_uah": _mean_or_none(row_frame, "candidate_regret_uah"),
        "mean_strict_baseline_regret_uah": _mean_or_none(row_frame, "strict_baseline_regret_uah"),
        "mean_candidate_net_value_uah": _mean_or_none(row_frame, "candidate_net_value_uah"),
        "mean_strict_baseline_net_value_uah": _mean_or_none(
            row_frame,
            "strict_baseline_net_value_uah",
        ),
        "mean_oracle_net_value_uah": _mean_or_none(row_frame, "oracle_net_value_uah"),
        "promotion_status": PROMOTION_STATUS,
        "claim_scope": CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _classification_metrics(true_labels: list[str], predictions: list[str]) -> dict[str, float | int]:
    if not true_labels:
        raise ValueError("dfl_action_classifier_v0 requires at least one label for scoring")
    correct_count = sum(
        int(true_label == predicted_label)
        for true_label, predicted_label in zip(true_labels, predictions, strict=True)
    )
    true_counts = Counter(true_labels)
    pred_counts = Counter(predictions)
    f1_values: list[float] = []
    for label in ACTION_LABELS:
        true_positive = sum(
            int(true_label == label and predicted_label == label)
            for true_label, predicted_label in zip(true_labels, predictions, strict=True)
        )
        false_positive = pred_counts[label] - true_positive
        false_negative = true_counts[label] - true_positive
        precision = (
            true_positive / (true_positive + false_positive)
            if true_positive + false_positive > 0
            else 0.0
        )
        recall = (
            true_positive / (true_positive + false_negative)
            if true_positive + false_negative > 0
            else 0.0
        )
        f1_values.append(
            2 * precision * recall / (precision + recall) if precision + recall > 0 else 0.0
        )
    return {
        "accuracy": correct_count / len(true_labels),
        "macro_f1": sum(f1_values) / len(f1_values),
        "true_charge_count": true_counts["charge"],
        "true_discharge_count": true_counts["discharge"],
        "true_hold_count": true_counts["hold"],
        "pred_charge_count": pred_counts["charge"],
        "pred_discharge_count": pred_counts["discharge"],
        "pred_hold_count": pred_counts["hold"],
    }


def _majority_label(counts: Counter[str]) -> str:
    if not counts:
        return "hold"
    return max(ACTION_LABELS, key=lambda label: (counts[label], -ACTION_LABELS.index(label)))


def _rank_bins(values: list[float]) -> list[str]:
    minimum = min(values)
    maximum = max(values)
    if maximum == minimum:
        return ["flat" for _ in values]
    bins: list[str] = []
    for value in values:
        position = (value - minimum) / (maximum - minimum)
        if position <= 1 / 3:
            bins.append("low")
        elif position <= 2 / 3:
            bins.append("mid")
        else:
            bins.append("high")
    return bins


def _label_from_masks(*, charge: int, discharge: int, hold: int) -> str:
    if charge:
        return "charge"
    if discharge:
        return "discharge"
    if hold:
        return "hold"
    raise ValueError("dfl_action_classifier_v0 target action masks must be one-hot")


def _float_vector(value: Any, *, expected_length: int, field_name: str) -> list[float]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    vector = [float(item) for item in value]
    if len(vector) != expected_length:
        raise ValueError(f"{field_name} length must match horizon_hours")
    return vector


def _int_mask(value: Any, *, expected_length: int, field_name: str) -> list[int]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    mask = [int(item) for item in value]
    if len(mask) != expected_length:
        raise ValueError(f"{field_name} length must match horizon_hours")
    if any(item not in {0, 1} for item in mask):
        raise ValueError(f"{field_name} must contain only 0/1 values")
    return mask


def _positive_int(value: Any, *, field_name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise ValueError("anchor_timestamp must be a datetime or ISO datetime string")


def _mean_or_none(frame: pl.DataFrame, column_name: str) -> float | None:
    if frame.is_empty() or column_name not in frame.columns:
        return None
    value = frame.select(pl.col(column_name).mean()).item()
    return None if value is None else float(value)


def _benchmark_rows_by_key(frame: pl.DataFrame) -> dict[tuple[str, str, datetime], dict[str, Any]]:
    rows: dict[tuple[str, str, datetime], dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        key = (
            str(row["tenant_id"]),
            str(row["forecast_model_name"]),
            _datetime_value(row["anchor_timestamp"]),
        )
        rows[key] = row
    return rows


def _required_benchmark_row(
    rows: dict[tuple[str, str, datetime], dict[str, Any]],
    *,
    tenant_id: str,
    forecast_model_name: str,
    anchor_timestamp: datetime,
) -> dict[str, Any]:
    key = (tenant_id, forecast_model_name, anchor_timestamp)
    if key not in rows:
        raise ValueError(
            "missing benchmark row for "
            f"{tenant_id}/{forecast_model_name}/{anchor_timestamp.isoformat()}"
        )
    return rows[key]


def _projection_evaluation_id(
    *,
    tenant_id: str,
    source_model_name: str,
    anchor_timestamp: datetime,
) -> str:
    return (
        f"{tenant_id}:dfl-action-classifier-strict:{source_model_name}:"
        f"{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
    )


def _dispatch_command_from_net_power(
    *,
    interval_start: datetime,
    net_power_mw: float,
    reason: str,
) -> DispatchCommand:
    return DispatchCommand.from_net_power(
        interval_start=interval_start,
        duration_minutes=LEVEL1_INTERVAL_MINUTES,
        net_power_mw=net_power_mw,
        reason=reason,
    )


def _optional_float(row: dict[str, Any], column_name: str, *, default: float) -> float:
    value = row.get(column_name)
    if value is None:
        return default
    return float(value)


def _numeric_row_value(row: dict[str, object], column_name: str) -> float:
    value = row[column_name]
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{column_name} must be numeric")
    return float(value)


def _regret_ratio(regret_uah: float, oracle_value_uah: float) -> float:
    return regret_uah / abs(oracle_value_uah) if abs(oracle_value_uah) > 1e-9 else 0.0


def _latest_generated_at(frame: pl.DataFrame) -> datetime | None:
    if frame.is_empty() or "generated_at" not in frame.columns:
        return None
    values = [
        value
        for value in frame.select("generated_at").to_series().to_list()
        if isinstance(value, datetime)
    ]
    return max(values) if values else None


def _require_cvxpy() -> Any:
    try:
        import cvxpy
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "cvxpy is required to project action labels through the strict LP constraints."
        ) from error
    return cvxpy


def _solver_float_list(values: object, expected_length: int) -> list[float]:
    if values is None:
        raise RuntimeError("Expected solver values, received None.")
    if hasattr(values, "tolist"):
        raw_values = values.tolist()
    elif isinstance(values, Iterable):
        raw_values = list(values)
    else:
        raise RuntimeError("Expected iterable solver values.")
    flattened = [float(item) for item in raw_values]
    if len(flattened) != expected_length:
        raise RuntimeError("Unexpected solver output length.")
    return flattened
