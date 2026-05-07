from datetime import datetime, timedelta
import math

import polars as pl

from smart_arbitrage.dfl.offline_experiment import build_offline_dfl_experiment_frame


def test_offline_dfl_experiment_trains_on_prior_anchors_and_scores_holdout() -> None:
    frame = _evaluation_frame(anchor_count=6)

    result = build_offline_dfl_experiment_frame(
        frame,
        tenant_id="client_003_dnipro_factory",
        forecast_model_names=("tft_silver_v0",),
        validation_fraction=0.33,
        max_train_anchors=4,
        max_validation_anchors=2,
        epoch_count=2,
        learning_rate=10.0,
    )

    assert result.height == 1
    row = result.row(0, named=True)
    assert row["experiment_name"] == "offline_horizon_bias_dfl_v0"
    assert row["tenant_id"] == "client_003_dnipro_factory"
    assert row["forecast_model_name"] == "tft_silver_v0"
    assert row["train_anchor_count"] == 4
    assert row["validation_anchor_count"] == 2
    assert row["last_training_anchor_timestamp"] < row["first_validation_anchor_timestamp"]
    assert row["claim_scope"] == "offline_dfl_experiment_not_full_dfl"
    assert row["not_full_dfl"] is True
    assert row["not_market_execution"] is True
    assert len(row["learned_horizon_biases_uah_mwh"]) == 2
    assert math.isfinite(row["baseline_validation_relaxed_regret_uah"])
    assert math.isfinite(row["dfl_validation_relaxed_regret_uah"])


def test_offline_dfl_experiment_does_not_learn_from_validation_actuals() -> None:
    base = _evaluation_frame(anchor_count=6)
    mutated_future = _evaluation_frame(anchor_count=6, validation_actual_offset=5000.0)

    base_result = build_offline_dfl_experiment_frame(
        base,
        tenant_id="client_003_dnipro_factory",
        forecast_model_names=("tft_silver_v0",),
        validation_fraction=0.33,
        max_train_anchors=4,
        max_validation_anchors=2,
        epoch_count=3,
        learning_rate=10.0,
    )
    mutated_result = build_offline_dfl_experiment_frame(
        mutated_future,
        tenant_id="client_003_dnipro_factory",
        forecast_model_names=("tft_silver_v0",),
        validation_fraction=0.33,
        max_train_anchors=4,
        max_validation_anchors=2,
        epoch_count=3,
        learning_rate=10.0,
    )

    base_biases = base_result.row(0, named=True)["learned_horizon_biases_uah_mwh"]
    mutated_biases = mutated_result.row(0, named=True)["learned_horizon_biases_uah_mwh"]
    assert mutated_biases == base_biases


def _evaluation_frame(*, anchor_count: int, validation_actual_offset: float = 0.0) -> pl.DataFrame:
    first_anchor = datetime(2026, 1, 1, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(anchor_count):
        anchor = first_anchor + timedelta(days=anchor_index)
        in_validation_window = anchor_index >= anchor_count - 2
        actual_offset = validation_actual_offset if in_validation_window else 0.0
        rows.append(
            {
                "evaluation_id": f"{anchor_index}:tft_silver_v0",
                "tenant_id": "client_003_dnipro_factory",
                "forecast_model_name": "tft_silver_v0",
                "strategy_kind": "real_data_rolling_origin_benchmark",
                "market_venue": "DAM",
                "anchor_timestamp": anchor,
                "generated_at": datetime(2026, 5, 7),
                "horizon_hours": 2,
                "starting_soc_fraction": 0.5,
                "decision_value_uah": 100.0,
                "oracle_value_uah": 300.0,
                "regret_uah": 200.0,
                "evaluation_payload": {
                    "data_quality_tier": "thesis_grade",
                    "observed_coverage_ratio": 1.0,
                    "horizon": [
                        {
                            "step_index": 0,
                            "interval_start": (anchor + timedelta(hours=1)).isoformat(),
                            "forecast_price_uah_mwh": 1100.0,
                            "actual_price_uah_mwh": 900.0 + actual_offset,
                        },
                        {
                            "step_index": 1,
                            "interval_start": (anchor + timedelta(hours=2)).isoformat(),
                            "forecast_price_uah_mwh": 1000.0,
                            "actual_price_uah_mwh": 1500.0 + actual_offset,
                        },
                    ],
                },
            }
        )
    return pl.DataFrame(rows)
