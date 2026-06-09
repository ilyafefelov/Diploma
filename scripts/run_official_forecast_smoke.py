"""Run a smoke-sized official NBEATSx/TFT forecast experiment."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import pickle
from typing import Any
import warnings

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.forecasting.neural_features import build_neural_forecast_feature_frame
from smart_arbitrage.forecasting.official_adapters import (
    OfficialForecastAdapterError,
    build_official_nbeatsx_forecast,
    build_official_tft_forecast,
)
from smart_arbitrage.forecasting.sota_training import build_sota_forecast_training_frame
from smart_arbitrage.research.official_forecast_smoke import (
    build_official_forecast_smoke_summary,
    detect_runtime_acceleration,
    persist_official_forecast_runs,
    write_official_forecast_smoke_exports,
)
from smart_arbitrage.resources.forecast_store import get_forecast_store


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MARKET_STORAGE_PATH = REPO_ROOT / "data" / "dagster_home" / "storage" / "dam_price_history"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "official_forecast_smoke"


def main() -> None:
    warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")
    warnings.filterwarnings("ignore", category=UserWarning, module="lightning")
    warnings.filterwarnings("ignore", category=UserWarning, module="pytorch_lightning")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant-id", default="client_003_dnipro_factory")
    parser.add_argument("--horizon-hours", type=int, default=6)
    parser.add_argument("--nbeatsx-max-steps", type=int, default=1)
    parser.add_argument("--tft-max-epochs", type=int, default=1)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--market-storage-path", type=Path, default=DEFAULT_MARKET_STORAGE_PATH)
    parser.add_argument("--synthetic-only", action="store_true")
    parser.add_argument("--persist-forecast-store", action="store_true")
    args = parser.parse_args()

    market_history, market_source = _load_market_history(
        storage_path=args.market_storage_path,
        synthetic_only=bool(args.synthetic_only),
        horizon_hours=int(args.horizon_hours),
    )
    feature_frame = build_neural_forecast_feature_frame(
        market_history,
        horizon_hours=int(args.horizon_hours),
        future_weather_mode="forecast_only",
    )
    training_frame = build_sota_forecast_training_frame(
        feature_frame,
        tenant_id=str(args.tenant_id),
    )

    forecast_frames: dict[str, pl.DataFrame] = {}
    adapter_errors: dict[str, str] = {}
    try:
        forecast_frames["nbeatsx_official_v0"] = build_official_nbeatsx_forecast(
            training_frame,
            horizon_hours=int(args.horizon_hours),
            max_steps=int(args.nbeatsx_max_steps),
        )
    except OfficialForecastAdapterError as exc:
        adapter_errors["nbeatsx_official_v0"] = str(exc)
        forecast_frames["nbeatsx_official_v0"] = pl.DataFrame()

    try:
        forecast_frames["tft_official_v0"] = build_official_tft_forecast(
            training_frame,
            horizon_hours=int(args.horizon_hours),
            max_epochs=int(args.tft_max_epochs),
            batch_size=32,
        )
    except OfficialForecastAdapterError as exc:
        adapter_errors["tft_official_v0"] = str(exc)
        forecast_frames["tft_official_v0"] = pl.DataFrame()

    summary = build_official_forecast_smoke_summary(
        forecast_frames=forecast_frames,
        runtime_acceleration=detect_runtime_acceleration(),
    )
    summary.update(
        {
            "tenant_id": str(args.tenant_id),
            "horizon_hours": int(args.horizon_hours),
            "market_source": market_source,
            "training_rows": training_frame.filter(pl.col("is_train")).height,
            "forecast_rows": training_frame.filter(pl.col("is_forecast")).height,
            "adapter_errors": adapter_errors,
        }
    )
    if bool(args.persist_forecast_store):
        summary["forecast_store_run_ids"] = persist_official_forecast_runs(
            forecast_frames=forecast_frames,
            forecast_store=get_forecast_store(),
        )
    else:
        summary["forecast_store_run_ids"] = {}
    run_id = f"official_forecast_smoke_{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}"
    exports = write_official_forecast_smoke_exports(
        forecast_frames=forecast_frames,
        summary=summary,
        output_dir=args.output_dir,
        run_id=run_id,
    )
    print(f"summary_json={exports.summary_json}")
    print(f"forecasts_csv={exports.forecasts_csv}")
    print(summary)


def _load_market_history(
    *,
    storage_path: Path,
    synthetic_only: bool,
    horizon_hours: int,
) -> tuple[pl.DataFrame, str]:
    if not synthetic_only and storage_path.exists():
        loaded = _load_polars_pickle(storage_path)
        if loaded is not None and loaded.height >= 168 + horizon_hours:
            return loaded, f"dagster_storage:{storage_path}"
    return (
        build_synthetic_market_price_history(
            history_hours=max(240, 168 + horizon_hours + 24),
            forecast_hours=horizon_hours,
        ),
        "synthetic_smoke_fallback",
    )


def _load_polars_pickle(path: Path) -> pl.DataFrame | None:
    try:
        with path.open("rb") as file:
            loaded: Any = pickle.load(file)
    except (OSError, pickle.PickleError, EOFError):
        return None
    if isinstance(loaded, pl.DataFrame):
        return loaded
    return None


if __name__ == "__main__":
    main()
