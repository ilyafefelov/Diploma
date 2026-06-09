from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.forecast_pipeline_truth import (
    DFL_FORECAST_PIPELINE_TRUTH_AUDIT_CLAIM_SCOPE,
    build_forecast_pipeline_truth_audit_frame,
    validate_forecast_pipeline_truth_audit_evidence,
)


TENANT_ID = "client_003_dnipro_factory"


def test_forecast_pipeline_truth_passes_perfect_forecast_sanity() -> None:
    frame = _benchmark_frame(
        "perfect_forecast_sanity",
        forecast_prices=[1000.0, 1500.0, 900.0],
        actual_prices=[1000.0, 1500.0, 900.0],
    )

    audit = build_forecast_pipeline_truth_audit_frame(frame)
    outcome = validate_forecast_pipeline_truth_audit_evidence(audit)

    row = audit.row(0, named=True)
    assert row["forecast_model_name"] == "perfect_forecast_sanity"
    assert row["perfect_forecast_anchor_count"] == 1
    assert row["perfect_forecast_sanity_passed"] is True
    assert row["zero_shift_best_anchor_count"] == 1
    assert row["shifted_better_anchor_count"] == 0
    assert row["blocking_failure_count"] == 0
    assert row["claim_scope"] == DFL_FORECAST_PIPELINE_TRUTH_AUDIT_CLAIM_SCOPE
    assert row["not_full_dfl"] is True
    assert row["not_market_execution"] is True
    assert outcome.passed is True


def test_forecast_pipeline_truth_reports_horizon_shift_without_blocking() -> None:
    frame = _benchmark_frame(
        "shifted_forecast",
        forecast_prices=[20.0, 30.0, 40.0, 50.0],
        actual_prices=[10.0, 20.0, 30.0, 40.0],
    )

    audit = build_forecast_pipeline_truth_audit_frame(frame)
    outcome = validate_forecast_pipeline_truth_audit_evidence(audit)

    row = audit.row(0, named=True)
    assert row["best_shift_offset_counts"] == [{"shift_offset_hours": 1, "anchor_count": 1}]
    assert row["zero_shift_best_anchor_count"] == 0
    assert row["shifted_better_anchor_count"] == 1
    assert row["diagnostic_warning_count"] == 1
    assert row["blocking_failure_count"] == 0
    assert outcome.passed is True


def test_forecast_pipeline_truth_blocks_bad_provenance_units_and_leaky_horizon() -> None:
    anchor = datetime(2026, 3, 29, 23, tzinfo=UTC)
    frame = _benchmark_frame(
        "bad_forecast",
        forecast_prices=[17_000.0, 1000.0],
        actual_prices=[1000.0, 1100.0],
        anchor_timestamp=anchor,
        first_interval_start=anchor,
        data_quality_tier="demo_grade",
        observed_coverage_ratio=0.5,
    )

    audit = build_forecast_pipeline_truth_audit_frame(frame, price_cap_uah_mwh=16_000.0)
    outcome = validate_forecast_pipeline_truth_audit_evidence(audit)

    row = audit.row(0, named=True)
    assert row["non_thesis_grade_rows"] == 1
    assert row["non_observed_rows"] == 1
    assert row["unit_sanity_failure_count"] == 1
    assert row["leaky_horizon_row_count"] == 1
    assert row["blocking_failure_count"] >= 4
    assert outcome.passed is False
    assert "forecast pipeline truth audit has blocking failures" in outcome.description


def test_forecast_pipeline_truth_detects_vector_round_trip_mutation() -> None:
    frame = _benchmark_frame(
        "bad_vector",
        forecast_prices=[1000.0, float("nan")],
        actual_prices=[1000.0, 1100.0],
    )

    audit = build_forecast_pipeline_truth_audit_frame(frame)
    outcome = validate_forecast_pipeline_truth_audit_evidence(audit)

    row = audit.row(0, named=True)
    assert row["vector_round_trip_failure_count"] == 1
    assert outcome.passed is False


def _benchmark_frame(
    model_name: str,
    *,
    forecast_prices: list[float],
    actual_prices: list[float],
    anchor_timestamp: datetime | None = None,
    first_interval_start: datetime | None = None,
    data_quality_tier: str = "thesis_grade",
    observed_coverage_ratio: float = 1.0,
) -> pl.DataFrame:
    anchor = anchor_timestamp or datetime(2026, 4, 1, 23, tzinfo=UTC)
    first_interval = first_interval_start or anchor + timedelta(hours=1)
    horizon = [
        {
            "step_index": index,
            "interval_start": (first_interval + timedelta(hours=index)).isoformat(),
            "forecast_price_uah_mwh": forecast_price,
            "actual_price_uah_mwh": actual_price,
        }
        for index, (forecast_price, actual_price) in enumerate(
            zip(forecast_prices, actual_prices, strict=True)
        )
    ]
    return pl.DataFrame(
        [
            {
                "evaluation_id": f"{model_name}:0",
                "tenant_id": TENANT_ID,
                "forecast_model_name": model_name,
                "strategy_kind": "real_data_rolling_origin_benchmark",
                "market_venue": "DAM",
                "anchor_timestamp": anchor,
                "generated_at": datetime(2026, 5, 11, tzinfo=UTC),
                "horizon_hours": len(horizon),
                "decision_value_uah": 1000.0,
                "oracle_value_uah": 1000.0,
                "regret_uah": 0.0,
                "evaluation_payload": {
                    "data_quality_tier": data_quality_tier,
                    "observed_coverage_ratio": observed_coverage_ratio,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "horizon": horizon,
                },
            }
        ]
    )
