from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.coverage_repair import (
    build_dfl_ua_coverage_repair_audit_frame,
)
from smart_arbitrage.dfl.data_expansion import build_dfl_data_coverage_audit_frame

TENANT_ID = "client_003_dnipro_factory"
FIRST_TIMESTAMP = datetime(2026, 1, 1)


def test_coverage_repair_reports_exact_unrecoverable_gap_timestamp() -> None:
    missing_timestamp = FIRST_TIMESTAMP + timedelta(hours=3)
    feature_frame = _feature_frame(hour_count=8, skip_timestamps={missing_timestamp})
    coverage_audit = build_dfl_data_coverage_audit_frame(
        feature_frame,
        tenant_ids=(TENANT_ID,),
        target_anchor_count_per_tenant=180,
        required_past_hours=2,
        horizon_hours=2,
    )

    repair_audit = build_dfl_ua_coverage_repair_audit_frame(
        feature_frame,
        coverage_audit,
        tenant_ids=(TENANT_ID,),
        target_anchor_count_per_tenant=180,
    )

    row = repair_audit.row(0, named=True)
    assert repair_audit.height == 1
    assert row["tenant_id"] == TENANT_ID
    assert row["missing_timestamp"] == missing_timestamp
    assert row["gap_kind"] == "price_and_weather_gap"
    assert row["repair_status"] == "not_recoverable_from_current_feature_frame"
    assert row["coverage_ceiling_anchor_count"] == coverage_audit["eligible_anchor_count"][0]
    assert row["coverage_ceiling_documented"] is True
    assert row["not_market_execution"] is True


def test_coverage_repair_blocks_non_observed_candidate_rows() -> None:
    synthetic_timestamp = FIRST_TIMESTAMP + timedelta(hours=3)
    feature_frame = _feature_frame(
        hour_count=8,
        synthetic_timestamps={synthetic_timestamp},
    )
    coverage_audit = pl.DataFrame(
        [
            {
                "tenant_id": TENANT_ID,
                "first_timestamp": FIRST_TIMESTAMP,
                "last_timestamp": FIRST_TIMESTAMP + timedelta(hours=7),
                "eligible_anchor_count": 0,
                "target_anchor_count_per_tenant": 180,
                "data_quality_tier": "coverage_gap",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        ]
    )

    repair_audit = build_dfl_ua_coverage_repair_audit_frame(
        feature_frame,
        coverage_audit,
        tenant_ids=(TENANT_ID,),
        target_anchor_count_per_tenant=180,
    )

    row = repair_audit.row(0, named=True)
    assert row["missing_timestamp"] == synthetic_timestamp
    assert row["gap_kind"] == "non_observed_price_and_weather"
    assert row["repair_status"] == "blocked_non_observed_source"
    assert row["repair_candidate_is_observed"] is False


def _feature_frame(
    *,
    hour_count: int,
    skip_timestamps: set[datetime] | None = None,
    synthetic_timestamps: set[datetime] | None = None,
) -> pl.DataFrame:
    skipped = skip_timestamps or set()
    synthetic = synthetic_timestamps or set()
    rows: list[dict[str, object]] = []
    for hour_index in range(hour_count):
        timestamp = FIRST_TIMESTAMP + timedelta(hours=hour_index)
        if timestamp in skipped:
            continue
        source_kind = "synthetic" if timestamp in synthetic else "observed"
        rows.append(
            {
                "tenant_id": TENANT_ID,
                "timestamp": timestamp,
                "price_uah_mwh": 1000.0 + hour_index,
                "source_kind": source_kind,
                "weather_source_kind": source_kind,
            }
        )
    return pl.DataFrame(rows)
