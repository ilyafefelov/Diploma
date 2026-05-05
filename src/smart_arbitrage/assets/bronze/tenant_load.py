"""Bronze tenant consumption schedule assets."""

from __future__ import annotations

from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.tenant_load import build_tenant_consumption_schedule_frame


@dg.asset(group_name="bronze", tags={"medallion": "bronze", "domain": "tenant_load"})
def tenant_consumption_schedule_bronze(context) -> pl.DataFrame:
    """Configured tenant consumption/open-hour schedules from the canonical tenant registry."""

    schedule_frame = build_tenant_consumption_schedule_frame()
    _add_metadata(
        context,
        {
            "rows": schedule_frame.height,
            "tenant_count": schedule_frame.select("tenant_id").n_unique() if schedule_frame.height else 0,
            "source_kind": "configured",
        },
    )
    return schedule_frame


TENANT_LOAD_BRONZE_ASSETS = [tenant_consumption_schedule_bronze]


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
    if context is not None:
        context.add_output_metadata(metadata)
