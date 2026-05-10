from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.semantic_event_failure_audit import (
    DFL_SEMANTIC_EVENT_STRICT_FAILURE_AUDIT_CLAIM_SCOPE,
    build_dfl_semantic_event_strict_failure_audit_frame,
    validate_dfl_semantic_event_strict_failure_audit_evidence,
)
from smart_arbitrage.forecasting.afe import build_forecast_afe_feature_catalog_frame
from smart_arbitrage.forecasting.grid_event_signals import GRID_EVENT_FEATURE_COLUMNS


TENANT_ID = "client_003_dnipro_factory"
SOURCE_MODEL = "tft_silver_v0"


def test_semantic_event_failure_audit_summarizes_official_grid_event_context() -> None:
    audit = build_dfl_semantic_event_strict_failure_audit_frame(
        _strict_selector_frame(),
        _grid_event_signal_frame(),
        build_forecast_afe_feature_catalog_frame(),
    )

    assert audit.height == 1
    row = audit.row(0, named=True)
    assert row["tenant_id"] == TENANT_ID
    assert row["source_model_name"] == SOURCE_MODEL
    assert row["semantic_source_name"] == "UKRENERGO_TELEGRAM"
    assert row["semantic_source_kind"] == "official_telegram"
    assert row["validation_anchor_count"] == 2
    assert row["event_anchor_count"] == 1
    assert row["strict_failure_count"] == 1
    assert row["strict_failure_with_event_count"] == 1
    assert row["selector_gain_with_events_uah"] == 120.0
    assert row["selector_gain_without_events_uah"] == -20.0
    assert row["claim_scope"] == DFL_SEMANTIC_EVENT_STRICT_FAILURE_AUDIT_CLAIM_SCOPE
    assert row["not_full_dfl"] is True
    assert row["not_market_execution"] is True

    outcome = validate_dfl_semantic_event_strict_failure_audit_evidence(
        audit,
        min_tenant_count=1,
        min_source_model_count=1,
        min_validation_anchor_count=2,
    )

    assert outcome.passed is True
    assert outcome.metadata["tenant_count"] == 1
    assert outcome.metadata["source_model_count"] == 1


def test_semantic_event_failure_audit_rejects_future_event_freshness() -> None:
    grid_rows = []
    for row_index, row in enumerate(_grid_event_signal_frame().iter_rows(named=True)):
        grid_rows.append(
            {**row, "event_source_freshness_hours": -1.0}
            if row_index == 0
            else row
        )

    with pytest.raises(ValueError, match="future"):
        build_dfl_semantic_event_strict_failure_audit_frame(
            _strict_selector_frame(),
            pl.DataFrame(grid_rows),
            build_forecast_afe_feature_catalog_frame(),
        )


def test_semantic_event_failure_audit_requires_official_telegram_semantic_catalog() -> None:
    catalog_rows = []
    for row in build_forecast_afe_feature_catalog_frame().iter_rows(named=True):
        catalog_rows.append(
            {**row, "semantic_source_kind": "scraped_news"}
            if row["feature_group"] == "semantic_grid_event"
            else row
        )

    with pytest.raises(ValueError, match="official Telegram"):
        build_dfl_semantic_event_strict_failure_audit_frame(
            _strict_selector_frame(),
            _grid_event_signal_frame(),
            pl.DataFrame(catalog_rows),
        )


def test_semantic_event_failure_audit_evidence_blocks_bad_claim_flags() -> None:
    audit = build_dfl_semantic_event_strict_failure_audit_frame(
        _strict_selector_frame(),
        _grid_event_signal_frame(),
        build_forecast_afe_feature_catalog_frame(),
    )
    bad_audit = audit.with_columns(pl.lit(False).alias("not_market_execution"))

    outcome = validate_dfl_semantic_event_strict_failure_audit_evidence(
        bad_audit,
        min_tenant_count=1,
        min_source_model_count=1,
        min_validation_anchor_count=2,
    )

    assert outcome.passed is False
    assert outcome.metadata["claim_flag_failure_rows"] == 1


def _strict_selector_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for anchor_index, anchor in enumerate(_anchors()):
        strict_regret = 200.0 if anchor_index == 0 else 100.0
        selector_regret = 80.0 if anchor_index == 0 else 120.0
        for role, forecast_model_name, regret in [
            ("strict_reference", "strict_similar_day", strict_regret),
            (
                "selector",
                f"dfl_feature_aware_strict_failure_selector_v2_{SOURCE_MODEL}",
                selector_regret,
            ),
        ]:
            rows.append(
                {
                    "evaluation_id": f"{TENANT_ID}:{SOURCE_MODEL}:{role}:{anchor.isoformat()}",
                    "tenant_id": TENANT_ID,
                    "source_model_name": SOURCE_MODEL,
                    "forecast_model_name": forecast_model_name,
                    "strategy_kind": "dfl_feature_aware_strict_failure_selector_strict_lp_benchmark",
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "generated_at": datetime(2026, 5, 8, tzinfo=UTC),
                    "horizon_hours": 24,
                    "decision_value_uah": 1000.0 - regret,
                    "oracle_value_uah": 1000.0,
                    "regret_uah": regret,
                    "regret_ratio": regret / 1000.0,
                    "total_degradation_penalty_uah": 1.0,
                    "total_throughput_mwh": 0.2,
                    "committed_action": "HOLD",
                    "committed_power_mw": 0.0,
                    "evaluation_payload": {
                        "source_forecast_model_name": SOURCE_MODEL,
                        "selector_row_role": role,
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "safety_violation_count": 0,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                    },
                }
            )
    return pl.DataFrame(rows)


def _grid_event_signal_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for anchor_index, anchor in enumerate(_anchors()):
        active = anchor_index == 0
        feature_values = {column_name: 0.0 for column_name in GRID_EVENT_FEATURE_COLUMNS}
        if active:
            feature_values.update(
                {
                    "grid_event_count_24h": 1.0,
                    "tenant_region_affected": 1.0,
                    "national_grid_risk_score": 0.85,
                    "days_since_grid_event": 0.08,
                    "outage_flag": 1.0,
                    "event_source_freshness_hours": 2.0,
                }
            )
        else:
            feature_values["days_since_grid_event"] = 999.0
            feature_values["event_source_freshness_hours"] = 999.0
        rows.append({"tenant_id": TENANT_ID, "timestamp": anchor, **feature_values})
    return pl.DataFrame(rows)


def _anchors() -> list[datetime]:
    start = datetime(2026, 4, 28, 23, tzinfo=UTC)
    return [start + timedelta(days=index) for index in range(2)]

