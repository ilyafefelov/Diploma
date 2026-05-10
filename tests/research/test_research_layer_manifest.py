from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import polars as pl

from smart_arbitrage.research.real_data_research_layer import (
    build_research_layer_outputs,
    select_latest_real_data_benchmark_frame,
    write_research_layer_exports,
)


def test_research_layer_manifest_separates_latest_tenant_batches(tmp_path) -> None:
    stale_dnipro_generated_at = datetime(2026, 5, 4, 12, tzinfo=UTC)
    latest_dnipro_generated_at = datetime(2026, 5, 5, 12, tzinfo=UTC)
    lviv_generated_at = datetime(2026, 5, 5, 13, tzinfo=UTC)
    raw_frame = pl.concat(
        [
            _benchmark_frame(
                tenant_id="client_003_dnipro_factory",
                generated_at=stale_dnipro_generated_at,
                start_regret=900.0,
            ),
            _benchmark_frame(
                tenant_id="client_003_dnipro_factory",
                generated_at=latest_dnipro_generated_at,
                start_regret=100.0,
            ),
            _benchmark_frame(
                tenant_id="client_002_lviv_office",
                generated_at=lviv_generated_at,
                start_regret=200.0,
            ),
        ]
    )

    latest_frame = select_latest_real_data_benchmark_frame(raw_frame)
    outputs = build_research_layer_outputs(
        latest_frame,
        pilot_tenant_id="client_003_dnipro_factory",
        pilot_model_name="tft_silver_v0",
    )
    export_dir = write_research_layer_exports(
        outputs,
        output_root=tmp_path,
        run_slug="manifest-test",
    )

    manifest = json.loads((export_dir / "research_layer_manifest.json").read_text(encoding="utf-8"))

    assert manifest["run_slug"] == "manifest-test"
    assert manifest["claim_scope"] == "calibration_selector_evidence_not_full_dfl"
    assert manifest["not_full_dfl"] is True
    assert manifest["not_market_execution"] is True
    assert manifest["tenant_ids"] == [
        "client_002_lviv_office",
        "client_003_dnipro_factory",
    ]
    assert manifest["latest_generated_at_by_tenant_strategy"][
        "client_003_dnipro_factory|real_data_rolling_origin_benchmark"
    ] == latest_dnipro_generated_at.isoformat()
    assert stale_dnipro_generated_at.isoformat() not in (
        manifest["latest_generated_at_by_tenant_strategy"].values()
    )
    assert manifest["anchor_count_by_tenant_strategy"][
        "client_003_dnipro_factory|real_data_rolling_origin_benchmark"
    ] == 5
    assert manifest["row_count_by_tenant_strategy"][
        "client_003_dnipro_factory|real_data_rolling_origin_benchmark"
    ] == 15
    assert manifest["data_quality_tiers"] == ["thesis_grade"]
    assert "https://huggingface.co/papers/2510.13654" in manifest["source_links"]

    summary = json.loads((export_dir / "research_layer_summary.json").read_text(encoding="utf-8"))
    assert summary["research_layer_manifest"] == "research_layer_manifest.json"


def _benchmark_frame(
    *,
    tenant_id: str,
    generated_at: datetime,
    start_regret: float,
) -> pl.DataFrame:
    first_anchor = datetime(2026, 5, 1, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(5):
        anchor = first_anchor + timedelta(days=anchor_index)
        for model_name, regret_offset in [
            ("strict_similar_day", 0.0),
            ("nbeatsx_silver_v0", 40.0),
            ("tft_silver_v0", 20.0),
        ]:
            regret = start_regret + regret_offset + anchor_index
            rows.append(
                {
                    "evaluation_id": f"{tenant_id}:{generated_at.isoformat()}:{anchor_index}:{model_name}",
                    "tenant_id": tenant_id,
                    "forecast_model_name": model_name,
                    "strategy_kind": "real_data_rolling_origin_benchmark",
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "generated_at": generated_at,
                    "horizon_hours": 2,
                    "starting_soc_fraction": 0.5,
                    "starting_soc_source": "tenant_default",
                    "decision_value_uah": 1000.0 - regret,
                    "forecast_objective_value_uah": 950.0,
                    "oracle_value_uah": 1000.0,
                    "regret_uah": regret,
                    "regret_ratio": regret / 1000.0,
                    "total_degradation_penalty_uah": 10.0,
                    "total_throughput_mwh": 0.1,
                    "committed_action": "HOLD",
                    "committed_power_mw": 0.0,
                    "rank_by_regret": 1,
                    "evaluation_payload": {
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "horizon": [
                            {
                                "step_index": 0,
                                "interval_start": (anchor + timedelta(hours=1)).isoformat(),
                                "forecast_price_uah_mwh": 1000.0 - regret_offset,
                                "actual_price_uah_mwh": 1100.0,
                                "net_power_mw": 0.0,
                                "degradation_penalty_uah": 0.0,
                            },
                            {
                                "step_index": 1,
                                "interval_start": (anchor + timedelta(hours=2)).isoformat(),
                                "forecast_price_uah_mwh": 1050.0 - regret_offset,
                                "actual_price_uah_mwh": 1150.0,
                                "net_power_mw": 0.0,
                                "degradation_penalty_uah": 0.0,
                            },
                        ],
                    },
                }
            )
    return pl.DataFrame(rows)
