from __future__ import annotations

import json
from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.research.real_data_research_layer import (
    build_research_layer_outputs,
    select_latest_real_data_benchmark_frame,
    write_research_layer_exports,
)


def test_research_layer_builds_outputs_from_latest_tenant_batches(tmp_path) -> None:
    raw_frame = pl.concat(
        [
            _benchmark_frame(
                tenant_id="client_003_dnipro_factory",
                generated_at=datetime(2026, 5, 4, 12),
                start_regret=900.0,
            ),
            _benchmark_frame(
                tenant_id="client_003_dnipro_factory",
                generated_at=datetime(2026, 5, 5, 12),
                start_regret=100.0,
            ),
        ]
    )

    latest_frame = select_latest_real_data_benchmark_frame(raw_frame)
    outputs = build_research_layer_outputs(
        latest_frame,
        pilot_tenant_id="client_003_dnipro_factory",
        pilot_model_name="tft_silver_v0",
    )
    export_dir = write_research_layer_exports(outputs, output_root=tmp_path, run_slug="test-run")

    assert latest_frame.height == 15
    assert outputs.ensemble_frame.height == 5
    assert outputs.dfl_training_frame.height == 20
    assert outputs.pilot_frame.height == 1
    assert outputs.regret_weighted_calibration_frame.height == 10
    assert outputs.regret_weighted_benchmark_frame.height == 25
    assert outputs.horizon_regret_weighted_calibration_frame.height == 10
    assert outputs.horizon_regret_weighted_benchmark_frame.height == 25
    assert outputs.calibrated_ensemble_frame.height == 5
    assert set(outputs.model_summary.select("forecast_model_name").to_series().to_list()) == {
        "strict_similar_day",
        "nbeatsx_silver_v0",
        "tft_silver_v0",
        "value_aware_ensemble_v0",
    }
    assert (export_dir / "research_layer_model_summary.csv").exists()
    assert (export_dir / "dfl_training_summary.csv").exists()
    assert (export_dir / "regret_weighted_dfl_pilot_summary.json").exists()
    assert (export_dir / "regret_weighted_calibration_summary.csv").exists()
    assert (export_dir / "regret_weighted_benchmark_summary.csv").exists()
    assert (export_dir / "horizon_regret_weighted_calibration_summary.csv").exists()
    assert (export_dir / "horizon_regret_weighted_benchmark_summary.csv").exists()
    assert (export_dir / "calibrated_ensemble_summary.csv").exists()
    summary = json.loads((export_dir / "research_layer_summary.json").read_text(encoding="utf-8"))
    assert summary["benchmark_rows"] == 15
    assert summary["dfl_training_rows"] == 20
    assert summary["dfl_pilot_scope"] == "pilot_not_full_dfl"
    assert summary["regret_weighted_benchmark_rows"] == 25
    assert summary["horizon_regret_weighted_benchmark_rows"] == 25
    assert summary["calibrated_ensemble_rows"] == 5


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
