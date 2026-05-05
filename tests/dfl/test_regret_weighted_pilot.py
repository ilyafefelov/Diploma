from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.regret_weighted import run_regret_weighted_dfl_pilot


def test_regret_weighted_dfl_pilot_learns_bias_correction_from_prior_rows() -> None:
    first_anchor = datetime(2026, 5, 1, 23)
    rows = [
        {
            "tenant_id": "client_003_dnipro_factory",
            "anchor_timestamp": first_anchor + timedelta(days=index),
            "forecast_model_name": "tft_silver_v0",
            "mean_forecast_price_uah_mwh": 1000.0,
            "mean_actual_price_uah_mwh": 1200.0,
            "training_weight": 1.0 + index,
            "regret_uah": 100.0 + index,
            "regret_ratio": 0.1,
            "data_quality_tier": "thesis_grade",
        }
        for index in range(8)
    ]

    result = run_regret_weighted_dfl_pilot(
        pl.DataFrame(rows),
        tenant_id="client_003_dnipro_factory",
        forecast_model_name="tft_silver_v0",
        validation_fraction=0.25,
    )

    row = result.row(0, named=True)
    assert row["pilot_name"] == "regret_weighted_bias_correction_v0"
    assert row["scope"] == "pilot_not_full_dfl"
    assert row["regret_weighted_bias_uah_mwh"] == 200.0
    assert row["validation_weighted_mae_after"] < row["validation_weighted_mae_before"]
    assert row["expanded_to_all_tenants_ready"] is True
