"""Materialize source-backed DAM/IDM operator preview forecast-store rows."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from typing import Any

from smart_arbitrage.research.operator_preview_forecast import (
    materialize_operator_preview_forecast_runs,
    resolve_next_operator_preview_forecast_start,
)
from smart_arbitrage.resources.forecast_store import get_forecast_store
from smart_arbitrage.resources.market_data_store import get_market_data_store


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant-id", default="client_003_dnipro_factory")
    parser.add_argument("--market-venue", choices=["DAM", "IDM"], required=True)
    parser.add_argument("--forecast-start", default=None, help="YYYY-MM-DD or ISO datetime. Defaults to next hour after latest complete official day.")
    parser.add_argument("--horizon-hours", type=int, default=72)
    parser.add_argument("--nbeatsx-max-steps", type=int, default=1)
    parser.add_argument("--tft-max-epochs", type=int, default=1)
    args = parser.parse_args()

    market_data_store = get_market_data_store()
    forecast_start = (
        _parse_forecast_start(str(args.forecast_start))
        if args.forecast_start
        else resolve_next_operator_preview_forecast_start(
            market_data_store=market_data_store,
            market_venue=str(args.market_venue),
        )
    )
    result = materialize_operator_preview_forecast_runs(
        market_data_store=market_data_store,
        forecast_store=get_forecast_store(),
        tenant_id=str(args.tenant_id),
        market_venue=str(args.market_venue),
        forecast_start=forecast_start,
        horizon_hours=int(args.horizon_hours),
        nbeatsx_max_steps=int(args.nbeatsx_max_steps),
        tft_max_epochs=int(args.tft_max_epochs),
    )
    print(json.dumps(_result_payload(result), indent=2, sort_keys=True))


def _parse_forecast_start(value: str) -> datetime:
    if len(value) == 10:
        return datetime.fromisoformat(f"{value}T00:00:00")
    return datetime.fromisoformat(value)


def _result_payload(result: Any) -> dict[str, Any]:
    return {
        "market_venue": result.market_venue,
        "forecast_start": result.forecast_start.isoformat(),
        "forecast_end": result.forecast_end.isoformat(),
        "horizon_hours": result.horizon_hours,
        "source_history_rows": result.source_history_rows,
        "run_ids": result.run_ids,
        "claim_boundary": result.claim_boundary,
        "market_execution_enabled": result.market_execution_enabled,
    }


if __name__ == "__main__":
    main()

