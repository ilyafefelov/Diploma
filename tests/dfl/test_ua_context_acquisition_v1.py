from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import subprocess
import sys

import polars as pl

from smart_arbitrage.dfl.ua_context_acquisition_v1 import (
    build_dfl_ua_calendar_block_context_backfill_frame,
    build_dfl_ua_context_backfill_coverage_gate_frame,
    build_dfl_ua_context_source_inventory_frame,
    build_dfl_ua_dam_publication_backfill_frame,
    build_dfl_ua_dam_publication_receipts_overlay_frame,
    build_dfl_ua_grid_event_backfill_frame,
    build_dfl_ua_weather_load_pv_proxy_backfill_frame,
    normalize_dfl_ua_dam_publication_receipts_frame,
)

TENANT = "client_003_dnipro_factory"
SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"
ANCHOR = datetime(2026, 4, 29, 23)


def test_ua_context_source_inventory_lists_source_backed_context_families() -> None:
    inventory = build_dfl_ua_context_source_inventory_frame(
        _requirements(),
        price_context_frame=_price_context(with_publication=True),
        weather_context_frame=_weather_context(),
        tenant_load_frame=_load_context(),
        grid_event_signal_frame=_grid_context(),
        source_window_start="2025-01-01",
        source_window_end="2026-04-30",
    )

    assert set(inventory["source_family"].to_list()) == {
        "oree_dam_publication",
        "open_meteo_archive_weather",
        "tenant_load_pv_proxy",
        "ukrenergo_grid_event_history",
        "ua_calendar_block_context",
    }
    assert set(inventory["no_eu_rows_as_ukrainian_targets"].to_list()) == {True}
    assert set(inventory["market_execution_enabled"].to_list()) == {False}


def test_dam_publication_backfill_blocks_missing_publication_metadata() -> None:
    dam = build_dfl_ua_dam_publication_backfill_frame(
        _requirements(),
        _price_context(with_publication=False),
    )

    assert dam["dam_publication_backfill_status"].to_list() == [
        "missing_publication_time"
    ]
    assert dam["prior_available"].to_list() == [False]
    assert dam["market_execution_enabled"].to_list() == [False]


def test_dam_publication_backfill_uses_explicit_prior_publication_metadata() -> None:
    dam = build_dfl_ua_dam_publication_backfill_frame(
        _requirements(),
        _price_context(with_publication=True),
    )

    assert dam["dam_publication_backfill_status"].to_list() == ["context_ready"]
    assert dam["prior_available"].to_list() == [True]
    assert dam["source_publication_timestamp"].to_list() == [ANCHOR - timedelta(hours=26)]


def test_dam_publication_receipts_overlay_adds_explicit_metadata() -> None:
    overlay = build_dfl_ua_dam_publication_receipts_overlay_frame(
        _price_context(with_publication=False),
        _receipt_context(),
    )

    assert overlay["source_publication_timestamp"].to_list() == [
        ANCHOR - timedelta(hours=26)
    ]
    assert overlay["source_url"].to_list() == [
        "https://www.oree.com.ua/index.php/receipt/test"
    ]
    assert overlay["source_title"].to_list() == ["OREE row publication receipt"]
    assert overlay["publication_receipt_id"].to_list() == ["receipt-001"]
    assert overlay["market_execution_enabled"].to_list() == [False]


def test_dam_publication_receipts_normalizer_emits_safe_csv_schema() -> None:
    normalized = normalize_dfl_ua_dam_publication_receipts_frame(
        _receipt_context().with_columns(
            pl.col("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%S"),
            pl.col("source_publication_timestamp").dt.strftime("%Y-%m-%dT%H:%M:%S"),
        )
    )

    assert normalized.columns == [
        "timestamp",
        "source_publication_timestamp",
        "source_url",
        "source_title",
        "receipt_id",
        "market_execution_enabled",
    ]
    assert normalized["timestamp"].to_list() == [ANCHOR - timedelta(hours=1)]
    assert normalized["source_publication_timestamp"].to_list() == [
        ANCHOR - timedelta(hours=26)
    ]
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_dam_publication_receipts_normalizer_rejects_non_prior_publication_time() -> None:
    try:
        normalize_dfl_ua_dam_publication_receipts_frame(
            pl.DataFrame(
                [
                    {
                        "timestamp": "2026-01-01T00:00:00",
                        "source_publication_timestamp": "2026-01-01T00:00:00",
                    }
                ]
            )
        )
    except ValueError as exc:
        assert "prior to delivery timestamp" in str(exc)
    else:
        raise AssertionError("non-prior DAM publication receipt was accepted")


def test_dam_publication_receipts_normalizer_rejects_observation_metadata_columns() -> None:
    try:
        normalize_dfl_ua_dam_publication_receipts_frame(
            pl.DataFrame(
                [
                    {
                        "timestamp": "2026-01-01T00:00:00",
                        "source_publication_timestamp": "2025-12-31T14:00:00",
                        "source_observed_at_utc": "2025-12-31T14:01:00+00:00",
                    }
                ]
            )
        )
    except ValueError as exc:
        assert "observation/download metadata" in str(exc)
    else:
        raise AssertionError("observation metadata was accepted as receipt input")


def test_dam_publication_receipts_cli_writes_normalized_csv(tmp_path: Path) -> None:
    input_path = tmp_path / "receipts.csv"
    output_path = tmp_path / "normalized_receipts.csv"
    _receipt_context().write_csv(input_path)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/validate_oree_dam_publication_receipts.py",
            "--input",
            str(input_path),
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    summary = json.loads(result.stdout)
    normalized = pl.read_csv(output_path, try_parse_dates=True)

    assert summary["receipt_rows"] == 1
    assert summary["market_execution_enabled"] is False
    assert summary["claim_boundary"] == "v13_source_readiness_only_not_market_execution"
    assert normalized["source_publication_timestamp"].to_list() == [
        ANCHOR - timedelta(hours=26)
    ]
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_dam_publication_receipts_overlay_rejects_missing_timestamp() -> None:
    receipts = _receipt_context().drop("source_publication_timestamp")

    try:
        build_dfl_ua_dam_publication_receipts_overlay_frame(
            _price_context(with_publication=False),
            receipts,
        )
    except ValueError as exc:
        assert "source_publication_timestamp" in str(exc)
    else:
        raise AssertionError("missing receipt publication timestamp was accepted")


def test_dam_publication_receipts_overlay_rejects_duplicate_timestamps() -> None:
    receipts = pl.concat([_receipt_context(), _receipt_context()])

    try:
        build_dfl_ua_dam_publication_receipts_overlay_frame(
            _price_context(with_publication=False),
            receipts,
        )
    except ValueError as exc:
        assert "duplicate" in str(exc)
    else:
        raise AssertionError("duplicate receipt timestamp was accepted")


def test_dam_publication_receipts_overlay_empty_frame_preserves_backfill_behavior() -> None:
    overlay = build_dfl_ua_dam_publication_receipts_overlay_frame(
        _price_context(with_publication=False),
        pl.DataFrame(),
    )
    dam = build_dfl_ua_dam_publication_backfill_frame(_requirements(), overlay)

    assert dam["dam_publication_backfill_status"].to_list() == [
        "missing_publication_time"
    ]


def test_dam_publication_backfill_uses_source_backed_market_rule_deadline() -> None:
    dam = build_dfl_ua_dam_publication_backfill_frame(
        _requirements(),
        _price_context(with_publication=False),
        publication_rule_hour_kyiv=14,
        publication_rule_source_url="https://www.oree.com.ua/index.php/web/13245784",
        publication_rule_source_title="OREE DAM results publication rule",
    )

    assert dam["dam_publication_backfill_status"].to_list() == ["context_ready"]
    assert dam["prior_available"].to_list() == [True]
    assert dam["source_publication_timestamp"].to_list() == [
        datetime(2026, 4, 28, 14)
    ]
    assert dam["publication_evidence_mode"].to_list() == ["market_rule_deadline"]
    assert dam["publication_evidence_source_url"].to_list() == [
        "https://www.oree.com.ua/index.php/web/13245784"
    ]


def test_weather_load_pv_backfill_requires_prior_source_backed_rows() -> None:
    future_weather = _weather_context().with_columns(
        pl.lit(ANCHOR + timedelta(hours=1)).alias("timestamp")
    )

    weather = build_dfl_ua_weather_load_pv_proxy_backfill_frame(
        _requirements(),
        future_weather,
        _load_context(),
    )

    assert weather["weather_load_pv_backfill_status"].to_list() == [
        "missing_prior_weather_history"
    ]
    assert weather["prior_available"].to_list() == [False]


def test_grid_event_backfill_blocks_missing_historical_grid_coverage() -> None:
    grid = build_dfl_ua_grid_event_backfill_frame(
        _requirements(),
        pl.DataFrame(),
    )

    assert grid["grid_event_backfill_status"].to_list() == [
        "missing_grid_event_history"
    ]
    assert grid["prior_available"].to_list() == [False]


def test_grid_event_backfill_requires_source_coverage_for_no_event_rows() -> None:
    grid_signal = _grid_context(include_source_window=False)

    grid = build_dfl_ua_grid_event_backfill_frame(_requirements(), grid_signal)

    assert grid["grid_event_backfill_status"].to_list() == [
        "missing_grid_event_history_source_window"
    ]
    assert grid["prior_available"].to_list() == [False]


def test_grid_event_backfill_accepts_source_backed_no_event_coverage() -> None:
    grid = build_dfl_ua_grid_event_backfill_frame(_requirements(), _grid_context())

    assert grid["grid_event_backfill_status"].to_list() == ["context_ready"]
    assert grid["prior_available"].to_list() == [True]
    assert grid["context_source"].to_list() == ["ukrenergo_grid_event_signal_history"]


def test_calendar_block_context_marks_dst_gap_without_synthesizing() -> None:
    requirements = _requirements(anchor=datetime(2026, 3, 29, 23))

    calendar = build_dfl_ua_calendar_block_context_backfill_frame(requirements)

    assert calendar["calendar_block_backfill_status"].to_list() == [
        "dst_calendar_gap_excluded"
    ]
    assert calendar["prior_available"].to_list() == [False]


def test_coverage_gate_allows_v11_only_when_all_required_context_is_ready() -> None:
    requirements = _requirements()
    dam = build_dfl_ua_dam_publication_backfill_frame(
        requirements,
        _price_context(with_publication=True),
    )
    weather = build_dfl_ua_weather_load_pv_proxy_backfill_frame(
        requirements,
        _weather_context(),
        _load_context(),
    )
    grid = build_dfl_ua_grid_event_backfill_frame(requirements, _grid_context())
    calendar = build_dfl_ua_calendar_block_context_backfill_frame(requirements)

    gate = build_dfl_ua_context_backfill_coverage_gate_frame(
        requirements,
        dam,
        weather,
        grid,
        calendar,
    )

    assert gate["v11_candidate_generation_ready"].to_list() == [True]
    assert gate["context_backfill_gate_decision"].to_list() == ["context_backfill_ready"]
    assert gate["market_execution_enabled"].to_list() == [False]


def test_coverage_gate_is_unchanged_by_final_holdout_label_mutation() -> None:
    requirements = _requirements().with_columns(
        pl.lit(123.0).alias("label_regret_delta_vs_v2_plus_uah")
    )
    changed_requirements = requirements.with_columns(
        pl.lit(9999.0).alias("label_regret_delta_vs_v2_plus_uah")
    )

    def _gate(frame: pl.DataFrame) -> pl.DataFrame:
        return build_dfl_ua_context_backfill_coverage_gate_frame(
            frame,
            build_dfl_ua_dam_publication_backfill_frame(
                frame,
                _price_context(with_publication=True),
            ),
            build_dfl_ua_weather_load_pv_proxy_backfill_frame(
                frame,
                _weather_context(),
                _load_context(),
            ),
            build_dfl_ua_grid_event_backfill_frame(frame, _grid_context()),
            build_dfl_ua_calendar_block_context_backfill_frame(frame),
        )

    base = _gate(requirements)
    changed = _gate(changed_requirements)

    assert base.select(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "v11_candidate_generation_ready",
            "context_backfill_gate_decision",
            "blocking_context_families",
        ]
    ).equals(
        changed.select(
            [
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "v11_candidate_generation_ready",
                "context_backfill_gate_decision",
                "blocking_context_families",
            ]
        )
    )


def _requirements(anchor: datetime = ANCHOR) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": TENANT,
                "source_model_name": SOURCE,
                "anchor_timestamp": anchor,
                "split_name": "final_holdout",
                "anchor_key": f"{TENANT}|{SOURCE}|{anchor.isoformat()}",
                "context_backfill_decision": "data_acquisition_needed",
                "dam_publication_timing_needed": True,
                "weather_load_pv_proxy_needed": True,
                "grid_outage_event_context_needed": True,
                "calendar_holiday_block_context_needed": True,
                "forecast_extrema_stability_needed": True,
                "dt_lava_ready": False,
                "requires_new_ukrainian_context_rows": True,
                "requires_new_ukrainian_target_rows": False,
                "no_eu_rows_as_ukrainian_targets": True,
                "market_execution_enabled": False,
            }
        ]
    )


def _price_context(*, with_publication: bool) -> pl.DataFrame:
    row = {
        "timestamp": ANCHOR - timedelta(hours=1),
        "price_uah_mwh": 4200.0,
        "source_kind": "observed_oree",
        "market_execution_enabled": False,
    }
    if with_publication:
        row["source_publication_timestamp"] = ANCHOR - timedelta(hours=26)
    return pl.DataFrame([row])


def _receipt_context() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "timestamp": ANCHOR - timedelta(hours=1),
                "source_publication_timestamp": ANCHOR - timedelta(hours=26),
                "source_url": "https://www.oree.com.ua/index.php/receipt/test",
                "source_title": "OREE row publication receipt",
                "receipt_id": "receipt-001",
            }
        ]
    )


def _weather_context() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": TENANT,
                "timestamp": ANCHOR - timedelta(hours=1),
                "weather_temperature": 12.0,
                "weather_wind_speed": 4.5,
                "weather_effective_solar": 80.0,
                "weather_precipitation": 0.0,
                "weather_source_kind": "historical_open_meteo",
            }
        ]
    )


def _load_context() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": TENANT,
                "timestamp": ANCHOR - timedelta(hours=1),
                "net_load_mw": 0.8,
                "pv_estimate_mw": 0.1,
                "source_kind": "configured_proxy",
            }
        ]
    )


def _grid_context(*, include_source_window: bool = True) -> pl.DataFrame:
    row = {
        "tenant_id": TENANT,
        "timestamp": ANCHOR - timedelta(hours=1),
        "grid_event_count_24h": 0.0,
        "tenant_region_affected": 0.0,
        "national_grid_risk_score": 0.0,
        "days_since_grid_event": 999.0,
        "outage_flag": 0.0,
        "saving_request_flag": 0.0,
        "solar_shift_hint": 0.0,
        "event_source_freshness_hours": 999.0,
    }
    if include_source_window:
        row["source_coverage_start_timestamp"] = ANCHOR - timedelta(days=14)
        row["source_coverage_end_timestamp"] = ANCHOR
        row["source_coverage_status"] = "source_covered"
    return pl.DataFrame([row])
