from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from smart_arbitrage.strategy.official_global_panel import (
    OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND,
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
