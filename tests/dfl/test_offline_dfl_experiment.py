from datetime import datetime, timedelta
import math

import polars as pl
import pytest

from smart_arbitrage.dfl.offline_experiment import (
    build_offline_dfl_experiment_frame,
    build_offline_dfl_panel_experiment_frame,
)
from smart_arbitrage.dfl.promotion_gate import evaluate_offline_dfl_panel_development_gate


CANONICAL_TENANTS = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
STARTING_SOC_BY_TENANT = {
    "client_001_kyiv_mall": 0.52,
    "client_002_lviv_office": 0.55,
    "client_003_dnipro_factory": 0.50,
    "client_004_kharkiv_hospital": 0.70,
    "client_005_odesa_hotel": 0.48,
}


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


def test_offline_dfl_panel_builds_five_tenant_final_holdout() -> None:
    frame = _panel_evaluation_frame(anchor_count=90)

    result = build_offline_dfl_panel_experiment_frame(
        frame,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=("tft_silver_v0",),
        final_validation_anchor_count_per_tenant=18,
        max_train_anchors_per_tenant=72,
        inner_validation_fraction=0.2,
        epoch_count=2,
        learning_rate=10.0,
    )

    assert result.height == 5
    assert result.select("tenant_id").n_unique() == 5
    assert result.select(pl.sum("final_validation_anchor_count")).item() == 90
    assert result.select(pl.sum("fit_anchor_count")).item() > 0
    assert result.select(pl.sum("inner_selection_anchor_count")).item() > 0
    assert result.select("forecast_model_name").unique().to_series().to_list() == ["tft_silver_v0"]
    for row in result.iter_rows(named=True):
        assert row["experiment_name"] == "offline_horizon_bias_panel_dfl_v2"
        assert row["last_inner_selection_anchor_timestamp"] < row["first_final_holdout_anchor_timestamp"]
        assert row["data_quality_tier"] == "thesis_grade"
        assert row["observed_coverage_ratio"] == 1.0
        assert row["not_full_dfl"] is True
        assert row["not_market_execution"] is True
        assert len(row["v0_horizon_biases_uah_mwh"]) == 2
        assert len(row["v2_checkpoint_horizon_biases_uah_mwh"]) == 2


def test_offline_dfl_panel_does_not_select_checkpoint_from_final_holdout_actuals() -> None:
    base = _panel_evaluation_frame(anchor_count=12, final_holdout_actual_offset=0.0)
    mutated_future = _panel_evaluation_frame(anchor_count=12, final_holdout_actual_offset=5000.0)

    base_result = build_offline_dfl_panel_experiment_frame(
        base,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=("tft_silver_v0",),
        final_validation_anchor_count_per_tenant=2,
        max_train_anchors_per_tenant=10,
        inner_validation_fraction=0.25,
        epoch_count=3,
        learning_rate=10.0,
    )
    mutated_result = build_offline_dfl_panel_experiment_frame(
        mutated_future,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=("tft_silver_v0",),
        final_validation_anchor_count_per_tenant=2,
        max_train_anchors_per_tenant=10,
        inner_validation_fraction=0.25,
        epoch_count=3,
        learning_rate=10.0,
    )

    base_rows = base_result.sort(["tenant_id", "forecast_model_name"]).iter_rows(named=True)
    mutated_rows = mutated_result.sort(["tenant_id", "forecast_model_name"]).iter_rows(named=True)
    for base_row, mutated_row in zip(base_rows, mutated_rows, strict=True):
        assert mutated_row["v2_checkpoint_epoch"] == base_row["v2_checkpoint_epoch"]
        assert mutated_row["v2_checkpoint_horizon_biases_uah_mwh"] == base_row[
            "v2_checkpoint_horizon_biases_uah_mwh"
        ]


def test_offline_dfl_panel_rejects_non_thesis_or_non_observed_rows() -> None:
    frame = _panel_evaluation_frame(anchor_count=12, invalid_first_row=True)

    with pytest.raises(ValueError, match="thesis_grade observed"):
        build_offline_dfl_panel_experiment_frame(
            frame,
            tenant_ids=CANONICAL_TENANTS,
            forecast_model_names=("tft_silver_v0",),
            final_validation_anchor_count_per_tenant=2,
            max_train_anchors_per_tenant=10,
            inner_validation_fraction=0.25,
            epoch_count=2,
            learning_rate=10.0,
        )


def test_offline_dfl_panel_gate_blocks_under_90_validation_coverage() -> None:
    result = evaluate_offline_dfl_panel_development_gate(_panel_result_frame(validation_per_tenant=17))

    assert result.passed is False
    assert result.decision == "block"
    assert "validation tenant-anchor count" in result.description


def test_offline_dfl_panel_gate_blocks_negative_regret_improvement() -> None:
    result = evaluate_offline_dfl_panel_development_gate(
        _panel_result_frame(validation_per_tenant=18, baseline_regret=100.0, v2_regret=105.0)
    )

    assert result.passed is False
    assert result.decision == "block"
    assert "relaxed regret must improve" in result.description


def test_offline_dfl_panel_gate_passes_only_for_full_observed_improving_panel() -> None:
    result = evaluate_offline_dfl_panel_development_gate(
        _panel_result_frame(validation_per_tenant=18, baseline_regret=100.0, v2_regret=90.0)
    )

    assert result.passed is True
    assert result.decision == "promote"
    assert result.metrics["validation_tenant_anchor_count"] == 90
    assert result.metrics["mean_relaxed_regret_improvement_ratio"] == pytest.approx(0.1)


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


def _panel_evaluation_frame(
    *,
    anchor_count: int,
    final_holdout_actual_offset: float = 0.0,
    invalid_first_row: bool = False,
) -> pl.DataFrame:
    first_anchor = datetime(2026, 1, 1, 23)
    rows: list[dict[str, object]] = []
    for tenant_index, tenant_id in enumerate(CANONICAL_TENANTS):
        for anchor_index in range(anchor_count):
            anchor = first_anchor + timedelta(days=anchor_index)
            in_final_holdout = anchor_index >= anchor_count - 2
            actual_offset = final_holdout_actual_offset if in_final_holdout else 0.0
            row_is_invalid = invalid_first_row and tenant_index == 0 and anchor_index == 0
            rows.append(
                {
                    "evaluation_id": f"{tenant_id}:{anchor_index}:tft_silver_v0",
                    "tenant_id": tenant_id,
                    "forecast_model_name": "tft_silver_v0",
                    "strategy_kind": "real_data_rolling_origin_benchmark",
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "generated_at": datetime(2026, 5, 7),
                    "horizon_hours": 2,
                    "starting_soc_fraction": STARTING_SOC_BY_TENANT[tenant_id],
                    "decision_value_uah": 100.0,
                    "oracle_value_uah": 300.0,
                    "regret_uah": 200.0,
                    "evaluation_payload": {
                        "data_quality_tier": "demo_grade" if row_is_invalid else "thesis_grade",
                        "observed_coverage_ratio": 0.5 if row_is_invalid else 1.0,
                        "horizon": [
                            {
                                "step_index": 0,
                                "interval_start": (anchor + timedelta(hours=1)).isoformat(),
                                "forecast_price_uah_mwh": 1100.0 + tenant_index,
                                "actual_price_uah_mwh": 900.0 + tenant_index + actual_offset,
                            },
                            {
                                "step_index": 1,
                                "interval_start": (anchor + timedelta(hours=2)).isoformat(),
                                "forecast_price_uah_mwh": 1000.0 + tenant_index,
                                "actual_price_uah_mwh": 1500.0 + tenant_index + actual_offset,
                            },
                        ],
                    },
                }
            )
    return pl.DataFrame(rows)


def _panel_result_frame(
    *,
    validation_per_tenant: int,
    baseline_regret: float = 100.0,
    v2_regret: float = 90.0,
) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": tenant_id,
                "forecast_model_name": "tft_silver_v0",
                "final_validation_anchor_count": validation_per_tenant,
                "baseline_final_holdout_relaxed_regret_uah": baseline_regret,
                "v2_final_holdout_relaxed_regret_uah": v2_regret,
                "v2_improved_over_baseline": v2_regret <= baseline_regret,
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
            for tenant_id in CANONICAL_TENANTS
        ]
    )
