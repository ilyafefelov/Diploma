"""Silver grid-event feature assets."""

from __future__ import annotations

from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.forecasting.grid_event_signals import build_grid_event_signal_frame


@dg.asset(
    group_name=taxonomy.SILVER_GRID_EVENTS,
    tags=taxonomy.asset_tags(
        medallion="silver",
        domain="grid_events",
        elt_stage="transform",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
    ),
)
def grid_event_signal_silver(
    context,
    dam_price_history: pl.DataFrame,
    ukrenergo_grid_events_bronze: pl.DataFrame,
) -> pl.DataFrame:
    """Tenant-hour Silver features from public Ukrenergo grid-event messages."""

    signal_frame = build_grid_event_signal_frame(
        price_history=dam_price_history,
        grid_events=ukrenergo_grid_events_bronze,
    )
    _add_metadata(
        context,
        {
            "rows": signal_frame.height,
            "tenant_count": signal_frame.select("tenant_id").n_unique() if signal_frame.height else 0,
            "max_national_grid_risk_score": (
                signal_frame.select("national_grid_risk_score").max().item() if signal_frame.height else 0.0
            ),
        },
    )
    return signal_frame


GRID_EVENT_SILVER_ASSETS = [grid_event_signal_silver]


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
    if context is not None:
        context.add_output_metadata(metadata)


__all__ = ["GRID_EVENT_SILVER_ASSETS", "grid_event_signal_silver"]
