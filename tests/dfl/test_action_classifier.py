from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.action_classifier import (
    DFL_ACTION_CLASSIFIER_STRICT_LP_STRATEGY_KIND,
    action_classifier_model_name,
    build_dfl_action_classifier_baseline_frame,
    build_dfl_action_classifier_strict_lp_benchmark_frame,
)


TENANTS: tuple[str, ...] = ("client_003_dnipro_factory", "client_004_kharkiv_hospital")
MODELS: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_ANCHOR = datetime(2026, 1, 8, 23)
GENERATED_AT = datetime(2026, 5, 8, 8)


def test_action_classifier_trains_on_train_selection_and_scores_holdout() -> None:
    frame = _action_label_frame(anchor_count=6)

    result = build_dfl_action_classifier_baseline_frame(frame)

    holdout = _summary_row(result, split_name="final_holdout", forecast_model_name="all_source_models")
    train = _summary_row(result, split_name="train_selection", forecast_model_name="all_source_models")

    assert result.height == 6
    assert train["action_label_row_count"] == 16
    assert train["label_hour_count"] == 48
    assert holdout["action_label_row_count"] == 8
    assert holdout["label_hour_count"] == 24
    assert holdout["uses_final_holdout_for_training"] is False
    assert holdout["training_action_label_rows"] == 16
    assert holdout["trained_rule_count"] > 0
    assert 0.0 <= holdout["accuracy"] <= 1.0
    assert 0.0 <= holdout["macro_f1"] <= 1.0
    assert holdout["promotion_status"] == "blocked_classification_only_no_strict_lp_value"
    assert holdout["not_full_dfl"] is True
    assert holdout["not_market_execution"] is True


def test_action_classifier_final_holdout_mutation_does_not_change_training_summary() -> None:
    frame = _action_label_frame(anchor_count=6)
    mutated_holdout = frame.with_columns(
        pl.when(pl.col("split_name") == "final_holdout")
        .then(pl.lit([1, 0, 0]))
        .otherwise(pl.col("target_charge_mask"))
        .alias("target_charge_mask"),
        pl.when(pl.col("split_name") == "final_holdout")
        .then(pl.lit([0, 0, 0]))
        .otherwise(pl.col("target_discharge_mask"))
        .alias("target_discharge_mask"),
        pl.when(pl.col("split_name") == "final_holdout")
        .then(pl.lit([0, 1, 1]))
        .otherwise(pl.col("target_hold_mask"))
        .alias("target_hold_mask"),
    )

    original = build_dfl_action_classifier_baseline_frame(frame)
    mutated = build_dfl_action_classifier_baseline_frame(mutated_holdout)

    original_train = _summary_row(original, split_name="train_selection", forecast_model_name="all_source_models")
    mutated_train = _summary_row(mutated, split_name="train_selection", forecast_model_name="all_source_models")
    original_holdout = _summary_row(original, split_name="final_holdout", forecast_model_name="all_source_models")
    mutated_holdout_row = _summary_row(mutated, split_name="final_holdout", forecast_model_name="all_source_models")

    assert mutated_train["accuracy"] == original_train["accuracy"]
    assert mutated_train["trained_rule_count"] == original_train["trained_rule_count"]
    assert mutated_train["training_action_label_rows"] == original_train["training_action_label_rows"]
    assert mutated_holdout_row["accuracy"] != original_holdout["accuracy"]


def test_action_classifier_rejects_invalid_training_inputs() -> None:
    frame = _action_label_frame(anchor_count=6)
    no_train = frame.filter(pl.col("split_name") == "final_holdout")
    non_thesis = _replace_first_row(frame, data_quality_tier="demo_grade")
    false_claim = _replace_first_row(frame, not_market_execution=False)

    with pytest.raises(ValueError, match="train_selection"):
        build_dfl_action_classifier_baseline_frame(no_train)
    with pytest.raises(ValueError, match="thesis_grade"):
        build_dfl_action_classifier_baseline_frame(non_thesis)
    with pytest.raises(ValueError, match="not_market_execution"):
        build_dfl_action_classifier_baseline_frame(false_claim)


def test_action_classifier_strict_lp_projection_scores_final_holdout_only() -> None:
    frame = _action_label_frame(anchor_count=6)
    benchmark = _benchmark_frame_for_action_labels(frame)

    result = build_dfl_action_classifier_strict_lp_benchmark_frame(
        frame,
        benchmark,
        generated_at=GENERATED_AT,
    )

    model_names = set(result.select("forecast_model_name").to_series().to_list())
    candidate_name = action_classifier_model_name(MODELS[0])
    candidate_row = result.filter(pl.col("forecast_model_name") == candidate_name).row(0, named=True)
    payload = candidate_row["evaluation_payload"]

    assert result.height == 16
    assert result.filter(pl.col("forecast_model_name") == "strict_similar_day").height == 8
    assert result.filter(pl.col("forecast_model_name").str.starts_with("dfl_action_classifier_v0_")).height == 8
    assert model_names == {
        "strict_similar_day",
        action_classifier_model_name(MODELS[0]),
        action_classifier_model_name(MODELS[1]),
    }
    assert result.select("strategy_kind").to_series().unique().to_list() == [
        DFL_ACTION_CLASSIFIER_STRICT_LP_STRATEGY_KIND
    ]
    assert payload["source_forecast_model_name"] == MODELS[0]
    assert payload["projection_method"] == "action_mask_lp_projection"
    assert payload["uses_final_holdout_for_training"] is False
    assert len(payload["predicted_action_labels"]) == candidate_row["horizon_hours"]
    assert len(payload["projected_signed_dispatch_vector_mw"]) == candidate_row["horizon_hours"]
    assert candidate_row["regret_uah"] == max(
        0.0,
        candidate_row["oracle_value_uah"] - candidate_row["decision_value_uah"],
    )
    assert candidate_row["evaluation_payload"]["not_full_dfl"] is True
    assert candidate_row["evaluation_payload"]["not_market_execution"] is True


def test_action_classifier_strict_lp_projection_does_not_train_on_final_holdout_actuals() -> None:
    frame = _action_label_frame(anchor_count=6)
    mutated = _replace_final_holdout_actuals(
        frame,
        actual_price_vector_uah_mwh=[5000.0, 1500.0, 500.0],
        oracle_net_value_uah=2500.0,
    )
    benchmark = _benchmark_frame_for_action_labels(frame)

    base = build_dfl_action_classifier_strict_lp_benchmark_frame(
        frame,
        benchmark,
        generated_at=GENERATED_AT,
    )
    changed = build_dfl_action_classifier_strict_lp_benchmark_frame(
        mutated,
        benchmark,
        generated_at=GENERATED_AT,
    )

    candidate_name = action_classifier_model_name(MODELS[0])
    base_row = base.filter(pl.col("forecast_model_name") == candidate_name).row(0, named=True)
    changed_row = changed.filter(pl.col("forecast_model_name") == candidate_name).row(0, named=True)

    assert (
        base_row["evaluation_payload"]["predicted_action_labels"]
        == changed_row["evaluation_payload"]["predicted_action_labels"]
    )
    assert (
        base_row["evaluation_payload"]["projected_signed_dispatch_vector_mw"]
        == changed_row["evaluation_payload"]["projected_signed_dispatch_vector_mw"]
    )
    assert base_row["decision_value_uah"] != changed_row["decision_value_uah"]


def test_action_classifier_strict_lp_projection_rejects_missing_benchmark_rows() -> None:
    frame = _action_label_frame(anchor_count=6)
    benchmark = _benchmark_frame_for_action_labels(frame).filter(
        pl.col("forecast_model_name") != MODELS[0]
    )

    with pytest.raises(ValueError, match="missing benchmark row"):
        build_dfl_action_classifier_strict_lp_benchmark_frame(frame, benchmark)


def _summary_row(result: pl.DataFrame, *, split_name: str, forecast_model_name: str) -> dict[str, object]:
    return result.filter(
        (pl.col("split_name") == split_name)
        & (pl.col("forecast_model_name") == forecast_model_name)
    ).row(0, named=True)


def _action_label_frame(*, anchor_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for model_name in MODELS:
            for anchor_index in range(anchor_count):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                is_final_holdout = anchor_index >= anchor_count - 2
                masks = _masks_for_anchor(anchor_index)
                rows.append(
                    {
                        "action_label_id": f"{tenant_id}:{model_name}:{anchor:%Y%m%dT%H%M}",
                        "tenant_id": tenant_id,
                        "forecast_model_name": model_name,
                        "anchor_timestamp": anchor,
                        "split_name": "final_holdout" if is_final_holdout else "train_selection",
                        "is_final_holdout": is_final_holdout,
                        "horizon_hours": 3,
                        "forecast_price_vector_uah_mwh": [
                            900.0 + anchor_index,
                            1200.0 + anchor_index,
                            1500.0 + anchor_index,
                        ],
                        "actual_price_vector_uah_mwh": [
                            950.0 + anchor_index,
                            1250.0 + anchor_index,
                            1550.0 + anchor_index,
                        ],
                        "candidate_signed_dispatch_vector_mw": [0.0, 0.0, 0.0],
                        "strict_baseline_signed_dispatch_vector_mw": [0.0, 0.0, 0.0],
                        "oracle_signed_dispatch_vector_mw": _oracle_dispatch_for_masks(masks),
                        "target_charge_mask": masks["charge"],
                        "target_discharge_mask": masks["discharge"],
                        "target_hold_mask": masks["hold"],
                        "candidate_regret_uah": 120.0 + anchor_index,
                        "strict_baseline_regret_uah": 80.0,
                        "candidate_net_value_uah": 900.0,
                        "strict_baseline_net_value_uah": 950.0,
                        "oracle_net_value_uah": 1000.0,
                        "candidate_safety_violation_count": 0,
                        "strict_baseline_safety_violation_count": 0,
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                        "generated_at": GENERATED_AT,
                    }
                )
    return pl.DataFrame(rows)


def _masks_for_anchor(anchor_index: int) -> dict[str, list[int]]:
    if anchor_index % 2 == 0:
        return {
            "charge": [1, 0, 0],
            "discharge": [0, 0, 1],
            "hold": [0, 1, 0],
        }
    return {
        "charge": [0, 0, 0],
        "discharge": [0, 1, 1],
        "hold": [1, 0, 0],
    }


def _oracle_dispatch_for_masks(masks: dict[str, list[int]]) -> list[float]:
    dispatch: list[float] = []
    for charge, discharge in zip(masks["charge"], masks["discharge"], strict=True):
        if charge:
            dispatch.append(-0.25)
        elif discharge:
            dispatch.append(0.25)
        else:
            dispatch.append(0.0)
    return dispatch


def _replace_first_row(frame: pl.DataFrame, **updates: object) -> pl.DataFrame:
    rows = frame.iter_rows(named=True)
    updated_rows: list[dict[str, object]] = []
    for row_index, row in enumerate(rows):
        updated_rows.append({**row, **updates} if row_index == 0 else row)
    return pl.DataFrame(updated_rows)


def _replace_final_holdout_actuals(frame: pl.DataFrame, **updates: object) -> pl.DataFrame:
    updated_rows: list[dict[str, object]] = []
    for row in frame.iter_rows(named=True):
        updated_rows.append({**row, **updates} if row["split_name"] == "final_holdout" else row)
    return pl.DataFrame(updated_rows)


def _benchmark_frame_for_action_labels(action_label_frame: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    strict_seen: set[tuple[str, datetime]] = set()
    for row in action_label_frame.iter_rows(named=True):
        tenant_id = str(row["tenant_id"])
        model_name = str(row["forecast_model_name"])
        anchor = row["anchor_timestamp"]
        if not isinstance(anchor, datetime):
            raise TypeError("anchor_timestamp must be a datetime in test data")
        strict_key = (tenant_id, anchor)
        if strict_key not in strict_seen:
            strict_seen.add(strict_key)
            rows.append(
                _benchmark_row(
                    tenant_id=tenant_id,
                    forecast_model_name="strict_similar_day",
                    anchor_timestamp=anchor,
                    decision_value_uah=float(row["strict_baseline_net_value_uah"]),
                    oracle_value_uah=float(row["oracle_net_value_uah"]),
                    regret_uah=float(row["strict_baseline_regret_uah"]),
                )
            )
        rows.append(
            _benchmark_row(
                tenant_id=tenant_id,
                forecast_model_name=model_name,
                anchor_timestamp=anchor,
                decision_value_uah=float(row["candidate_net_value_uah"]),
                oracle_value_uah=float(row["oracle_net_value_uah"]),
                regret_uah=float(row["candidate_regret_uah"]),
            )
        )
    return pl.DataFrame(rows)


def _benchmark_row(
    *,
    tenant_id: str,
    forecast_model_name: str,
    anchor_timestamp: datetime,
    decision_value_uah: float,
    oracle_value_uah: float,
    regret_uah: float,
) -> dict[str, object]:
    return {
        "evaluation_id": f"{tenant_id}:{forecast_model_name}:{anchor_timestamp:%Y%m%dT%H%M}",
        "tenant_id": tenant_id,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": "real_data_rolling_origin_benchmark",
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": GENERATED_AT,
        "horizon_hours": 3,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "test_fixture",
        "decision_value_uah": decision_value_uah,
        "forecast_objective_value_uah": decision_value_uah,
        "oracle_value_uah": oracle_value_uah,
        "regret_uah": regret_uah,
        "regret_ratio": regret_uah / abs(oracle_value_uah),
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.0,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "horizon": [],
        },
    }
