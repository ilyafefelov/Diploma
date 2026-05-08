"""Dependency-free supervised action-label baseline for DFL readiness."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

import polars as pl


ALL_SOURCE_MODELS: Final = "all_source_models"
DEFAULT_BASELINE_NAME: Final = "dfl_action_classifier_v0"
CLAIM_SCOPE: Final = "dfl_action_classifier_baseline_not_full_dfl"
PROMOTION_STATUS: Final = "blocked_classification_only_no_strict_lp_value"

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
