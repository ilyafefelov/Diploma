"""Research utilities for official NBEATSx/TFT smoke evidence."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Final

import polars as pl


OFFICIAL_FORECAST_SMOKE_CLAIM_BOUNDARY: Final[str] = (
    "official_adapter_smoke_not_full_sota_benchmark"
)
SMOKE_DAM_PRICE_CAP_MIN_UAH_MWH: Final[float] = 10.0
SMOKE_DAM_PRICE_CAP_MAX_UAH_MWH: Final[float] = 15_000.0


@dataclass(frozen=True)
class OfficialForecastSmokeExports:
    summary_json: Path
    forecasts_csv: Path


def build_official_forecast_smoke_summary(
    *,
    forecast_frames: Mapping[str, pl.DataFrame],
    runtime_acceleration: Mapping[str, Any],
) -> dict[str, Any]:
    """Summarize official forecast adapter smoke outputs for reports and CI checks."""

    model_rows = {
        model_name: forecast_frame.height
        for model_name, forecast_frame in forecast_frames.items()
    }
    timestamp_values = _forecast_timestamp_values(forecast_frames.values())
    mean_prices = {
        model_name: _mean_price(forecast_frame)
        for model_name, forecast_frame in forecast_frames.items()
    }
    forecast_quality = {
        model_name: _forecast_quality(forecast_frame)
        for model_name, forecast_frame in forecast_frames.items()
    }

    return {
        "model_rows": model_rows,
        "model_mean_price_uah_mwh": mean_prices,
        "forecast_quality": forecast_quality,
        "forecast_window_start": _iso_or_none(min(timestamp_values)) if timestamp_values else None,
        "forecast_window_end": _iso_or_none(max(timestamp_values)) if timestamp_values else None,
        "runtime_acceleration": dict(runtime_acceleration),
        "claim_boundary": OFFICIAL_FORECAST_SMOKE_CLAIM_BOUNDARY,
    }


def write_official_forecast_smoke_exports(
    *,
    forecast_frames: Mapping[str, pl.DataFrame],
    summary: Mapping[str, Any],
    output_dir: Path,
    run_id: str,
) -> OfficialForecastSmokeExports:
    """Write compact official forecast smoke artifacts to disk."""

    output_dir.mkdir(parents=True, exist_ok=True)
    summary_json = output_dir / f"{run_id}_summary.json"
    forecasts_csv = output_dir / f"{run_id}_forecasts.csv"

    summary_json.write_text(
        json.dumps(dict(summary), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _concat_forecasts(forecast_frames).write_csv(forecasts_csv)
    return OfficialForecastSmokeExports(
        summary_json=summary_json,
        forecasts_csv=forecasts_csv,
    )


def detect_runtime_acceleration() -> dict[str, Any]:
    """Return torch runtime information relevant to official forecast/DT runs."""

    try:
        import torch
    except ModuleNotFoundError:
        return {
            "backend": "torch unavailable",
            "device_type": "unknown",
            "device_name": "torch unavailable",
            "gpu_available": False,
            "recommended_scope": "install torch before official SOTA forecast/DT runs",
        }

    torch_version = str(getattr(torch, "__version__", "unknown"))
    cuda_available = bool(torch.cuda.is_available())
    if cuda_available:
        return {
            "backend": f"torch {torch_version}",
            "device_type": "cuda",
            "device_name": str(torch.cuda.get_device_name(0)),
            "gpu_available": True,
            "cuda_version": str(getattr(torch.version, "cuda", None) or "") or None,
            "recommended_scope": "use GPU for official NBEATSx/TFT training and DT sweeps",
        }
    mps_backend = getattr(getattr(torch, "backends", None), "mps", None)
    mps_available = bool(mps_backend is not None and mps_backend.is_available())
    if mps_available:
        return {
            "backend": f"torch {torch_version}",
            "device_type": "mps",
            "device_name": "Apple Metal Performance Shaders",
            "gpu_available": True,
            "recommended_scope": "use MPS for smoke-sized official forecasts; verify numerical parity on CPU",
        }
    return {
        "backend": f"torch {torch_version}",
        "device_type": "cpu",
        "device_name": "CPU only",
        "gpu_available": False,
        "cuda_version": str(getattr(torch.version, "cuda", None) or "") or None,
        "recommended_scope": "keep official NBEATSx/TFT and DT runs small; install CUDA torch before sweeps",
    }


def _concat_forecasts(forecast_frames: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
    frames = [frame for frame in forecast_frames.values() if not frame.is_empty()]
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical_relaxed")


def _forecast_timestamp_values(frames: Iterable[pl.DataFrame]) -> list[Any]:
    timestamps: list[Any] = []
    for frame in frames:
        if (
            isinstance(frame, pl.DataFrame)
            and not frame.is_empty()
            and "forecast_timestamp" in frame.columns
        ):
            timestamps.extend(frame.select("forecast_timestamp").to_series().drop_nulls().to_list())
    return timestamps


def _mean_price(frame: pl.DataFrame) -> float | None:
    if frame.is_empty() or "predicted_price_uah_mwh" not in frame.columns:
        return None
    value = frame.select(pl.col("predicted_price_uah_mwh").mean()).item()
    return None if value is None else float(value)


def _forecast_quality(frame: pl.DataFrame) -> dict[str, Any]:
    if frame.is_empty() or "predicted_price_uah_mwh" not in frame.columns:
        return {
            "min_predicted_price_uah_mwh": None,
            "max_predicted_price_uah_mwh": None,
            "out_of_dam_cap_rows": 0,
            "quality_boundary": "not_materialized",
        }
    quality_row = frame.select(
        [
            pl.col("predicted_price_uah_mwh").min().alias("min_price"),
            pl.col("predicted_price_uah_mwh").max().alias("max_price"),
            (
                (pl.col("predicted_price_uah_mwh") < SMOKE_DAM_PRICE_CAP_MIN_UAH_MWH)
                | (pl.col("predicted_price_uah_mwh") > SMOKE_DAM_PRICE_CAP_MAX_UAH_MWH)
            )
            .sum()
            .alias("out_of_cap_rows"),
        ]
    ).row(0, named=True)
    out_of_cap_rows = int(quality_row["out_of_cap_rows"])
    return {
        "min_predicted_price_uah_mwh": float(quality_row["min_price"]),
        "max_predicted_price_uah_mwh": float(quality_row["max_price"]),
        "out_of_dam_cap_rows": out_of_cap_rows,
        "quality_boundary": (
            "smoke_values_inside_dam_cap_not_value_claim"
            if out_of_cap_rows == 0
            else "needs_calibration_before_value_claim"
        ),
    }


def _iso_or_none(value: Any) -> str | None:
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return None
