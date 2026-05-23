from __future__ import annotations

from datetime import datetime

import polars as pl

from smart_arbitrage.dfl.ua_context_acquisition_export import (
    build_dfl_ua_context_backfill_readiness_packet,
    write_dfl_ua_context_backfill_readiness_packet,
)


def test_ua_context_backfill_readiness_packet_exports_blocked_gate(tmp_path) -> None:
    source_inventory = pl.DataFrame(
        [
            {
                "source_family": "oree_dam_publication",
                "source_rows": 1,
                "required_anchor_rows": 1,
                "publication_metadata_supported": False,
                "prior_timestamp_supported": True,
                "market_execution_enabled": False,
            }
        ]
    )
    gate = pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "anchor_timestamp": datetime(2026, 4, 29, 23),
                "v11_candidate_generation_ready": False,
                "context_backfill_gate_decision": "data_acquisition_needed",
                "blocking_context_families": "dam_publication:missing_publication_time",
                "market_execution_enabled": False,
            }
        ]
    )
    empty_family = pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "anchor_timestamp": datetime(2026, 4, 29, 23),
                "prior_available": False,
                "market_execution_enabled": False,
            }
        ]
    )

    packet = build_dfl_ua_context_backfill_readiness_packet(
        run_slug="test_ua_context_acquisition",
        source_inventory_frame=source_inventory,
        dam_publication_frame=empty_family,
        weather_load_pv_frame=empty_family,
        grid_event_frame=empty_family,
        calendar_block_frame=empty_family,
        coverage_gate_frame=gate,
        asset_check_status="passed",
    )
    export_dir = write_dfl_ua_context_backfill_readiness_packet(
        packet,
        output_root=tmp_path,
        source_inventory_frame=source_inventory,
        dam_publication_frame=empty_family,
        weather_load_pv_frame=empty_family,
        grid_event_frame=empty_family,
        calendar_block_frame=empty_family,
        coverage_gate_frame=gate,
    )

    assert packet["v11_candidate_generation_ready"] is False
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert (export_dir / "dfl_ua_context_backfill_readiness_summary.json").exists()
    assert (export_dir / "dfl_ua_context_backfill_readiness_summary.md").exists()
    assert (export_dir / "dfl_ua_context_source_inventory_rows.csv").exists()
    assert (export_dir / "dfl_ua_context_family_coverage_rows.csv").exists()
    assert (export_dir / "dfl_ua_context_v11_gate_decision_rows.csv").exists()
