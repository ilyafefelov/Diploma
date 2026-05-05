"""Silver tenant net-load feature assets."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.tenant_load import build_tenant_net_load_hourly_frame


@dg.asset(group_name="silver", tags={"medallion": "silver", "domain": "tenant_load"})
def tenant_net_load_hourly_silver(
    context,
    tenant_consumption_schedule_bronze: pl.DataFrame,
    weather_forecast_bronze: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Hourly tenant load/PV/net-load features from configured schedules and weather forecasts."""

    net_load_frame = build_tenant_net_load_hourly_frame(
        tenant_consumption_schedule_bronze,
        weather_frame=weather_forecast_bronze,
        anchor_timestamp=datetime.now(tz=UTC),
        horizon_hours=24,
    )
    _add_metadata(
        context,
        {
            "rows": net_load_frame.height,
            "tenant_count": net_load_frame.select("tenant_id").n_unique() if net_load_frame.height else 0,
            "source_kind": "configured",
            "weather_mode": "forecast_or_schedule_estimate",
        },
    )
    return net_load_frame


TENANT_LOAD_SILVER_ASSETS = [tenant_net_load_hourly_silver]


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
    if context is not None:
        context.add_output_metadata(metadata)
