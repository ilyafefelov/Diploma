from datetime import UTC, datetime
import json
from pathlib import Path

import polars as pl

from smart_arbitrage.research.official_forecast_smoke import (
    build_official_forecast_smoke_summary,
    detect_runtime_acceleration,
    persist_official_forecast_runs,
    write_official_forecast_smoke_exports,
)
from smart_arbitrage.resources.forecast_store import InMemoryForecastStore


def test_official_forecast_smoke_summary_reports_rows_window_and_runtime() -> None:
    start = datetime(2026, 5, 5, 18, tzinfo=UTC)
    forecast_frame = pl.DataFrame(
        {
            "model_name": ["nbeatsx_official_v0", "nbeatsx_official_v0"],
            "forecast_timestamp": [start, start.replace(hour=19)],
            "predicted_price_uah_mwh": [4200.0, 4300.0],
            "prediction_interval_kind": ["point", "point"],
        }
    )

    summary = build_official_forecast_smoke_summary(
        forecast_frames={"nbeatsx_official_v0": forecast_frame, "tft_official_v0": pl.DataFrame()},
        runtime_acceleration={
            "backend": "torch 2.11.0+cpu",
            "device_type": "cpu",
            "device_name": "CPU only",
            "gpu_available": False,
            "recommended_scope": "keep smoke runs small",
        },
    )

    assert summary["model_rows"] == {
        "nbeatsx_official_v0": 2,
        "tft_official_v0": 0,
    }
    assert summary["forecast_window_start"] == "2026-05-05T18:00:00+00:00"
    assert summary["forecast_window_end"] == "2026-05-05T19:00:00+00:00"
    assert summary["forecast_quality"]["nbeatsx_official_v0"]["out_of_dam_cap_rows"] == 0
    assert summary["runtime_acceleration"]["device_type"] == "cpu"
    assert summary["claim_boundary"] == "official_adapter_smoke_not_full_sota_benchmark"


def test_official_forecast_smoke_summary_flags_out_of_cap_smoke_prices() -> None:
    forecast_frame = pl.DataFrame(
        {
            "model_name": ["nbeatsx_official_v0", "nbeatsx_official_v0"],
            "forecast_timestamp": [
                datetime(2026, 5, 5, 18, tzinfo=UTC),
                datetime(2026, 5, 5, 19, tzinfo=UTC),
            ],
            "predicted_price_uah_mwh": [-25.0, 52000.0],
            "prediction_interval_kind": ["point", "point"],
        }
    )

    summary = build_official_forecast_smoke_summary(
        forecast_frames={"nbeatsx_official_v0": forecast_frame},
        runtime_acceleration={"device_type": "cpu"},
    )

    assert summary["forecast_quality"]["nbeatsx_official_v0"] == {
        "min_predicted_price_uah_mwh": -25.0,
        "max_predicted_price_uah_mwh": 52000.0,
        "out_of_dam_cap_rows": 2,
        "quality_boundary": "needs_calibration_before_value_claim",
    }


def test_write_official_forecast_smoke_exports_creates_csv_and_json(tmp_path: Path) -> None:
    forecast_frame = pl.DataFrame(
        {
            "model_name": ["nbeatsx_official_v0"],
            "forecast_timestamp": [datetime(2026, 5, 5, 18, tzinfo=UTC)],
            "predicted_price_uah_mwh": [4200.0],
            "prediction_interval_kind": ["point"],
        }
    )
    summary = build_official_forecast_smoke_summary(
        forecast_frames={"nbeatsx_official_v0": forecast_frame},
        runtime_acceleration={"device_type": "cpu"},
    )

    written = write_official_forecast_smoke_exports(
        forecast_frames={"nbeatsx_official_v0": forecast_frame},
        summary=summary,
        output_dir=tmp_path,
        run_id="smoke-test",
    )

    assert written.summary_json.exists()
    assert written.forecasts_csv.exists()
    assert json.loads(written.summary_json.read_text(encoding="utf-8"))["model_rows"] == {
        "nbeatsx_official_v0": 1
    }
    assert "nbeatsx_official_v0" in written.forecasts_csv.read_text(encoding="utf-8")


def test_detect_runtime_acceleration_reports_backend_and_scope() -> None:
    runtime = detect_runtime_acceleration()

    assert "backend" in runtime
    assert runtime["device_type"] in {"cpu", "cuda", "mps", "unknown"}
    assert runtime["recommended_scope"]


def test_persist_official_forecast_runs_uses_quantile_p50_when_available() -> None:
    store = InMemoryForecastStore()
    forecast_frames = {
        "nbeatsx_official_v0": pl.DataFrame(
            {
                "forecast_timestamp": [datetime(2026, 5, 6, 0, tzinfo=UTC)],
                "predicted_price_uah_mwh": [4200.0],
            }
        ),
        "tft_official_v0": pl.DataFrame(
            {
                "forecast_timestamp": [datetime(2026, 5, 6, 0, tzinfo=UTC)],
                "predicted_price_uah_mwh": [4100.0],
                "predicted_price_p50_uah_mwh": [4150.0],
            }
        ),
        "empty_model": pl.DataFrame(),
    }

    run_ids = persist_official_forecast_runs(
        forecast_frames=forecast_frames,
        forecast_store=store,
    )

    assert set(run_ids) == {"nbeatsx_official_v0", "tft_official_v0"}
    latest = store.latest_forecast_observation_frame(
        model_names=["nbeatsx_official_v0", "tft_official_v0"],
        limit_per_model=1,
    )
    tft_row = latest.filter(pl.col("model_name") == "tft_official_v0").row(0, named=True)
    assert tft_row["predicted_price_uah_mwh"] == 4150.0
