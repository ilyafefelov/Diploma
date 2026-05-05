from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.strategy.ensemble_gate import (
    CALIBRATED_VALUE_AWARE_ENSEMBLE_MODEL_NAME,
    VALUE_AWARE_ENSEMBLE_MODEL_NAME,
    build_calibrated_value_aware_ensemble_frame,
    build_value_aware_ensemble_frame,
)


def test_value_aware_ensemble_defaults_to_strict_then_uses_only_prior_anchor_regret() -> None:
    first_anchor = datetime(2026, 5, 1, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(3):
        anchor = first_anchor + timedelta(days=anchor_index)
        for model_name, regret in [
            ("strict_similar_day", 100.0 + anchor_index),
            ("nbeatsx_silver_v0", 10.0 if anchor_index == 0 else 900.0),
            ("tft_silver_v0", 500.0),
        ]:
            rows.append(
                {
                    "evaluation_id": f"{anchor_index}:{model_name}",
                    "tenant_id": "client_003_dnipro_factory",
                    "forecast_model_name": model_name,
                    "strategy_kind": "real_data_rolling_origin_benchmark",
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "generated_at": datetime(2026, 5, 5),
                    "horizon_hours": 24,
                    "starting_soc_fraction": 0.5,
                    "starting_soc_source": "tenant_default",
                    "decision_value_uah": 1000.0 - regret,
                    "forecast_objective_value_uah": 1000.0,
                    "oracle_value_uah": 1000.0,
                    "regret_uah": regret,
                    "regret_ratio": regret / 1000.0,
                    "total_degradation_penalty_uah": 1.0,
                    "total_throughput_mwh": 0.1,
                    "committed_action": "HOLD",
                    "committed_power_mw": 0.0,
                    "rank_by_regret": 1,
                    "evaluation_payload": {"data_quality_tier": "thesis_grade"},
                }
            )

    ensemble = build_value_aware_ensemble_frame(pl.DataFrame(rows), validation_window_anchors=2)

    selected_models = [
        row["evaluation_payload"]["selected_model_name"]
        for row in ensemble.sort("anchor_timestamp").iter_rows(named=True)
    ]
    assert ensemble.select("forecast_model_name").to_series().unique().to_list() == [
        VALUE_AWARE_ENSEMBLE_MODEL_NAME
    ]
    assert selected_models == [
        "strict_similar_day",
        "nbeatsx_silver_v0",
        "strict_similar_day",
    ]


def test_value_aware_ensemble_selection_is_unchanged_by_future_regret_mutation() -> None:
    first_anchor = datetime(2026, 5, 1, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(4):
        anchor = first_anchor + timedelta(days=anchor_index)
        for model_name, regret in [
            ("strict_similar_day", 20.0),
            ("nbeatsx_silver_v0", 100.0),
            ("tft_silver_v0", 200.0),
        ]:
            rows.append(
                {
                    "evaluation_id": f"{anchor_index}:{model_name}",
                    "tenant_id": "client_003_dnipro_factory",
                    "forecast_model_name": model_name,
                    "strategy_kind": "real_data_rolling_origin_benchmark",
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "generated_at": datetime(2026, 5, 5),
                    "horizon_hours": 24,
                    "starting_soc_fraction": 0.5,
                    "starting_soc_source": "tenant_default",
                    "decision_value_uah": 1000.0 - regret,
                    "forecast_objective_value_uah": 1000.0,
                    "oracle_value_uah": 1000.0,
                    "regret_uah": regret,
                    "regret_ratio": regret / 1000.0,
                    "total_degradation_penalty_uah": 1.0,
                    "total_throughput_mwh": 0.1,
                    "committed_action": "HOLD",
                    "committed_power_mw": 0.0,
                    "rank_by_regret": 1,
                    "evaluation_payload": {"data_quality_tier": "thesis_grade"},
                }
            )
    control = pl.DataFrame(rows)
    mutated_future = control.with_columns(
        pl.when(
            (pl.col("anchor_timestamp") > first_anchor + timedelta(days=1))
            & (pl.col("forecast_model_name") == "tft_silver_v0")
        )
        .then(0.0)
        .otherwise(pl.col("regret_uah"))
        .alias("regret_uah")
    )

    control_selected = build_value_aware_ensemble_frame(control).filter(
        pl.col("anchor_timestamp") <= first_anchor + timedelta(days=1)
    )
    mutated_selected = build_value_aware_ensemble_frame(mutated_future).filter(
        pl.col("anchor_timestamp") <= first_anchor + timedelta(days=1)
    )

    assert control_selected.select("evaluation_payload").to_series().to_list() == mutated_selected.select(
        "evaluation_payload"
    ).to_series().to_list()


def test_calibrated_value_aware_ensemble_only_selects_horizon_candidates() -> None:
    first_anchor = datetime(2026, 5, 1, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(3):
        anchor = first_anchor + timedelta(days=anchor_index)
        for model_name, regret in [
            ("strict_similar_day", 120.0),
            ("tft_silver_v0", 1.0),
            ("nbeatsx_silver_v0", 2.0),
            ("tft_horizon_regret_weighted_calibrated_v0", 20.0 if anchor_index == 0 else 900.0),
            ("nbeatsx_horizon_regret_weighted_calibrated_v0", 300.0),
        ]:
            rows.append(_evaluation_row(anchor_index=anchor_index, anchor=anchor, model_name=model_name, regret=regret))

    ensemble = build_calibrated_value_aware_ensemble_frame(pl.DataFrame(rows), validation_window_anchors=2)

    selected_models = [
        row["evaluation_payload"]["selected_model_name"]
        for row in ensemble.sort("anchor_timestamp").iter_rows(named=True)
    ]
    assert ensemble.select("forecast_model_name").to_series().unique().to_list() == [
        CALIBRATED_VALUE_AWARE_ENSEMBLE_MODEL_NAME
    ]
    assert selected_models == [
        "strict_similar_day",
        "tft_horizon_regret_weighted_calibrated_v0",
        "strict_similar_day",
    ]
    assert all(
        "silver_v0" not in row["evaluation_payload"]["selected_model_name"]
        or row["evaluation_payload"]["selected_model_name"] == "strict_similar_day"
        for row in ensemble.iter_rows(named=True)
    )


def test_calibrated_value_aware_ensemble_selection_is_unchanged_by_future_regret_mutation() -> None:
    first_anchor = datetime(2026, 5, 1, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(4):
        anchor = first_anchor + timedelta(days=anchor_index)
        for model_name, regret in [
            ("strict_similar_day", 30.0),
            ("tft_horizon_regret_weighted_calibrated_v0", 100.0),
            ("nbeatsx_horizon_regret_weighted_calibrated_v0", 200.0),
        ]:
            rows.append(_evaluation_row(anchor_index=anchor_index, anchor=anchor, model_name=model_name, regret=regret))
    control = pl.DataFrame(rows)
    mutated_future = control.with_columns(
        pl.when(
            (pl.col("anchor_timestamp") > first_anchor + timedelta(days=1))
            & (pl.col("forecast_model_name") == "tft_horizon_regret_weighted_calibrated_v0")
        )
        .then(0.0)
        .otherwise(pl.col("regret_uah"))
        .alias("regret_uah")
    )

    control_selected = build_calibrated_value_aware_ensemble_frame(control).filter(
        pl.col("anchor_timestamp") <= first_anchor + timedelta(days=1)
    )
    mutated_selected = build_calibrated_value_aware_ensemble_frame(mutated_future).filter(
        pl.col("anchor_timestamp") <= first_anchor + timedelta(days=1)
    )

    assert control_selected.select("evaluation_payload").to_series().to_list() == mutated_selected.select(
        "evaluation_payload"
    ).to_series().to_list()


def _evaluation_row(
    *,
    anchor_index: int,
    anchor: datetime,
    model_name: str,
    regret: float,
) -> dict[str, object]:
    return {
        "evaluation_id": f"{anchor_index}:{model_name}",
        "tenant_id": "client_003_dnipro_factory",
        "forecast_model_name": model_name,
        "strategy_kind": "horizon_regret_weighted_forecast_calibration_benchmark",
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": datetime(2026, 5, 5),
        "horizon_hours": 24,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 1000.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 1.0,
        "total_throughput_mwh": 0.1,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "evaluation_payload": {"data_quality_tier": "thesis_grade"},
    }
