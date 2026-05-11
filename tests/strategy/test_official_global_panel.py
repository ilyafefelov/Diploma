from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.strategy.official_global_panel import (
    OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME,
    OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATION_STRATEGY_KIND,
    OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND,
    build_official_global_panel_nbeatsx_horizon_calibration_frame,
    build_official_global_panel_nbeatsx_horizon_calibrated_strict_lp_benchmark_frame,
    build_official_global_panel_nbeatsx_strict_lp_benchmark_frame,
)


TENANT_ID = "client_003_dnipro_factory"
GENERATED_AT = datetime(2026, 5, 11, 18, tzinfo=UTC)


def test_global_panel_nbeatsx_strict_lp_benchmark_scores_against_strict_control() -> None:
    silver_frame = _silver_frame()
    forecast_frame = _global_panel_forecast_frame(anchor_timestamp=datetime(2026, 1, 10, 23))

    result = build_official_global_panel_nbeatsx_strict_lp_benchmark_frame(
        silver_frame,
        forecast_frame,
        tenant_ids=(TENANT_ID,),
        generated_at=GENERATED_AT,
    )

    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        "nbeatsx_official_global_panel_v1",
    }
    assert result.select("strategy_kind").to_series().unique().to_list() == [
        OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND
    ]
    assert result.select("generated_at").to_series().unique().to_list() == [
        GENERATED_AT
    ]
    payload = result.filter(pl.col("forecast_model_name") == "nbeatsx_official_global_panel_v1").row(0, named=True)[
        "evaluation_payload"
    ]
    assert payload["claim_scope"] == "official_global_panel_nbeatsx_strict_lp_not_full_dfl"
    assert payload["data_quality_tier"] == "thesis_grade"
    assert payload["not_full_dfl"] is True
    assert payload["not_market_execution"] is True


def test_global_panel_nbeatsx_strict_lp_benchmark_rejects_missing_tenant_forecast() -> None:
    try:
        build_official_global_panel_nbeatsx_strict_lp_benchmark_frame(
            _silver_frame(),
            _global_panel_forecast_frame(anchor_timestamp=datetime(2026, 1, 10, 23)),
            tenant_ids=("missing_tenant",),
            generated_at=GENERATED_AT,
        )
    except ValueError as error:
        assert "Missing global-panel NBEATSx forecast rows" in str(error)
    else:
        raise AssertionError("missing tenant forecast rows should fail clearly")


def test_global_panel_nbeatsx_horizon_calibration_uses_prior_anchors_only() -> None:
    first_anchor = datetime(2026, 1, 10, 23)
    source_rows = [
        _evaluation_row(
            first_anchor + timedelta(days=index),
            model_name="nbeatsx_official_global_panel_v1",
            forecast_prices=[1000.0, 1000.0],
            actual_prices=[1200.0 if index < 5 else 5000.0, 900.0 if index < 5 else 5000.0],
        )
        for index in range(6)
    ]

    calibration = build_official_global_panel_nbeatsx_horizon_calibration_frame(
        pl.DataFrame(source_rows),
        min_prior_anchors=2,
        rolling_calibration_window_anchors=3,
    )

    fifth_anchor = first_anchor + timedelta(days=4)
    fifth_row = calibration.filter(pl.col("anchor_timestamp") == fifth_anchor).row(0, named=True)
    assert fifth_row["corrected_forecast_model_name"] == OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME
    assert fifth_row["horizon_biases_uah_mwh"] == pytest.approx([200.0, -100.0])
    assert fifth_row["prior_anchor_count"] == 4
    assert fifth_row["calibration_status"] == "calibrated"

    first_row = calibration.filter(pl.col("anchor_timestamp") == first_anchor).row(0, named=True)
    assert first_row["calibration_status"] == "insufficient_prior_history"
    assert first_row["horizon_biases_uah_mwh"] == [0.0, 0.0]


def test_global_panel_nbeatsx_horizon_calibrated_gate_routes_corrected_forecast_through_lp() -> None:
    anchor = datetime(2026, 1, 10, 23)
    evaluation_frame = pl.DataFrame(
        [
            _evaluation_row(anchor, model_name="strict_similar_day", forecast_prices=[1000.0, 1400.0]),
            _evaluation_row(
                anchor,
                model_name="nbeatsx_official_global_panel_v1",
                forecast_prices=[900.0, 1200.0],
            ),
        ]
    )
    calibration_frame = pl.DataFrame(
        [
            {
                "tenant_id": TENANT_ID,
                "anchor_timestamp": anchor,
                "source_forecast_model_name": "nbeatsx_official_global_panel_v1",
                "corrected_forecast_model_name": OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME,
                "horizon_biases_uah_mwh": [100.0, 300.0],
                "mean_horizon_bias_uah_mwh": 200.0,
                "max_abs_horizon_bias_uah_mwh": 300.0,
                "prior_anchor_count": 14,
                "calibration_window_anchor_count": 14,
                "calibration_status": "calibrated",
                "data_quality_tier": "thesis_grade",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        ]
    )

    result = build_official_global_panel_nbeatsx_horizon_calibrated_strict_lp_benchmark_frame(
        evaluation_frame,
        calibration_frame,
    )

    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        "nbeatsx_official_global_panel_v1",
        OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME,
    }
    assert set(result["strategy_kind"].to_list()) == {
        OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATION_STRATEGY_KIND
    }
    corrected_payload = result.filter(
        pl.col("forecast_model_name") == OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME
    ).row(0, named=True)["evaluation_payload"]
    assert corrected_payload["source_forecast_model_name"] == "nbeatsx_official_global_panel_v1"
    assert corrected_payload["horizon_biases_uah_mwh"] == [100.0, 300.0]
    assert corrected_payload["not_full_dfl"] is True
    assert corrected_payload["not_market_execution"] is True


def _silver_frame() -> pl.DataFrame:
    start = datetime(2026, 1, 1)
    rows: list[dict[str, object]] = []
    for index in range(12 * 24):
        timestamp = start + timedelta(hours=index)
        rows.append(
            {
                "tenant_id": TENANT_ID,
                "timestamp": timestamp,
                "price_uah_mwh": 1000.0 + 300.0 * (index % 24 in {8, 9, 18, 19}),
                "source_kind": "observed",
            }
        )
    return pl.DataFrame(rows)


def _global_panel_forecast_frame(*, anchor_timestamp: datetime) -> pl.DataFrame:
    timestamps = [anchor_timestamp + timedelta(hours=index + 1) for index in range(24)]
    return pl.DataFrame(
        {
            "model_name": ["nbeatsx_official_global_panel_v1"] * 24,
            "model_family": ["NBEATSx"] * 24,
            "backend_name": ["neuralforecast"] * 24,
            "backend_status": ["trained"] * 24,
            "unique_id": [f"{TENANT_ID}:DAM"] * 24,
            "forecast_timestamp": timestamps,
            "predicted_price_uah_mwh": [1100.0 + float(index % 5) * 25.0 for index in range(24)],
            "predicted_price_p10_uah_mwh": [None] * 24,
            "predicted_price_p50_uah_mwh": [1100.0 + float(index % 5) * 25.0 for index in range(24)],
            "predicted_price_p90_uah_mwh": [None] * 24,
            "prediction_interval_kind": ["point"] * 24,
            "training_rows": [200] * 24,
            "horizon_rows": [24] * 24,
            "adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * 24,
        }
    )


def _evaluation_row(
    anchor: datetime,
    *,
    model_name: str,
    forecast_prices: list[float],
    actual_prices: list[float] | None = None,
) -> dict[str, object]:
    resolved_actual_prices = actual_prices or [1000.0, 1500.0]
    return {
        "evaluation_id": f"{TENANT_ID}:{model_name}:{anchor:%Y%m%dT%H%M}",
        "tenant_id": TENANT_ID,
        "forecast_model_name": model_name,
        "strategy_kind": OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": len(forecast_prices),
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 100.0,
        "forecast_objective_value_uah": 90.0,
        "oracle_value_uah": 120.0,
        "regret_uah": 20.0,
        "regret_ratio": 0.1,
        "total_degradation_penalty_uah": 1.0,
        "total_throughput_mwh": 0.1,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "claim_scope": "official_global_panel_nbeatsx_strict_lp_not_full_dfl",
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "horizon": [
                {
                    "step_index": index,
                    "interval_start": (anchor + timedelta(hours=index + 1)).isoformat(),
                    "forecast_price_uah_mwh": forecast_prices[index],
                    "actual_price_uah_mwh": resolved_actual_prices[index],
                    "net_power_mw": 0.0,
                    "degradation_penalty_uah": 0.0,
                }
                for index in range(len(forecast_prices))
            ],
        },
    }
