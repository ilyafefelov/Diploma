from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.poland_lag24_feature_audit import (
    build_poland_lag24_feature_consumption_audit_frame,
    build_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame,
)
from smart_arbitrage.forecasting.sota_training import (
    POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS,
)


def test_poland_feature_consumption_audit_proves_training_contract_and_variance() -> None:
    lagged = _lagged_feature_frame()
    strict = _strict_frame()

    audit = build_poland_lag24_feature_consumption_audit_frame(
        lagged,
        strict_benchmark_frame=strict,
    )

    assert audit.height == len(POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS)
    assert set(audit["feature_column"]) == set(POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS)
    assert audit.select(pl.col("in_neuralforecast_training_contract").all()).item() is True
    assert audit.select(pl.col("has_variance").all()).item() is True
    assert audit.select(pl.col("timestamp_alignment_status").unique()).to_series().to_list() == [
        "lagged_24h_prior_safe"
    ]
    assert audit.select(pl.col("consumption_status").unique()).to_series().to_list() == [
        "passes_training_consumption_audit"
    ]
    first = audit.row(0, named=True)
    assert first["feature_scaler_fit_scope"] == "train_rows_only_per_unique_id"
    assert first["strict_evidence_anchor_count"] == 2
    assert first["strict_evidence_source_model_count"] == 1


def test_poland_feature_consumption_audit_flags_zero_variance_feature() -> None:
    lagged = _lagged_feature_frame().with_columns(
        pl.lit(42.0).alias("entsoe_pl_lag24_delta_1h_uah_mwh")
    )

    audit = build_poland_lag24_feature_consumption_audit_frame(lagged)
    flagged = audit.filter(pl.col("feature_column") == "entsoe_pl_lag24_delta_1h_uah_mwh").row(
        0,
        named=True,
    )

    assert flagged["has_variance"] is False
    assert flagged["scaler_retention_status"] == "at_risk_constant_feature"
    assert flagged["consumption_status"] == "blocked_no_variance"


def test_poland_rolling_gate_requires_beating_frozen_v2_plus_windows() -> None:
    poland = _robustness_frame(
        source_model_name="tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1",
        means=[160.0, 170.0, 190.0, 165.0],
        medians=[50.0, 55.0, 90.0, 60.0],
    )
    frozen = _robustness_frame(
        source_model_name="nbeatsx_official_global_panel_horizon_calibrated_v1",
        means=[174.77, 174.77, 174.77, 174.77],
        medians=[67.30, 67.30, 67.30, 67.30],
    )

    gate = build_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame(
        poland,
        frozen,
        min_mean_regret_improvement_ratio_vs_frozen_v2_plus=0.05,
        min_passing_windows=3,
    )

    assert gate.height == 4
    assert gate["poland_window_passed"].to_list() == [True, False, False, True]
    assert set(gate["rolling_gate_status"]) == {"positive_not_promoted"}
    assert set(gate["claim_boundary"]) == {"positive_shadow_evidence_not_promoted"}
    assert gate.select(pl.col("market_execution_enabled").any()).item() is False


def _lagged_feature_frame() -> pl.DataFrame:
    start = datetime(2026, 4, 1)
    rows: list[dict[str, object]] = []
    for hour_index in range(48):
        delivery = start + timedelta(hours=hour_index)
        source = delivery - timedelta(hours=24)
        row: dict[str, object] = {
            "delivery_timestamp_utc": delivery.isoformat() + "+00:00",
            "source_delivery_timestamp_utc": source.isoformat() + "+00:00",
            "source_backed": True,
            "coverage_status": "full_lagged_feature_coverage",
        }
        for feature_index, feature_column in enumerate(
            POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS,
            start=1,
        ):
            row[feature_column] = float(hour_index + feature_index)
        rows.append(row)
    return pl.DataFrame(rows)


def _strict_frame() -> pl.DataFrame:
    start = datetime(2026, 4, 1, 23)
    return pl.DataFrame(
        [
            {
                "tenant_id": "client_001_kyiv_mall",
                "anchor_timestamp": start + timedelta(days=anchor_index),
                "forecast_model_name": (
                    "tft_official_global_panel_poland_lag24_experimental_v1"
                ),
                "regret_uah": 100.0,
            }
            for anchor_index in range(2)
        ]
    )


def _robustness_frame(
    *,
    source_model_name: str,
    means: list[float],
    medians: list[float],
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for index, (mean, median) in enumerate(zip(means, medians, strict=True), start=1):
        rows.append(
            {
                "source_model_name": source_model_name,
                "window_index": index,
                "validation_tenant_anchor_count": 90,
                "selected_mean_regret_uah": mean,
                "selected_median_regret_uah": median,
                "v2_plus_window_passed": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows)
