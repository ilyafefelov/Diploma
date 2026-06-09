"""Bronze source snapshots for no-token Poland neighbor-market features."""

from pathlib import Path

import dagster as dg
import polars as pl

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.forecasting.poland_neighbor_snapshot import (
    build_poland_neighbor_market_snapshot_bronze_frame,
)


class PolandNeighborMarketSnapshotConfig(dg.Config):
    """Local/public Poland neighbor-market snapshot metadata."""

    snapshot_csv_path: str = ""
    source_url: str = ""
    source_access_method: str = "manual_export_csv"
    source_retrieved_at_utc: str = ""
    source_publication_timestamp_utc: str = ""
    source_license_status: str = "review_required"
    snapshot_kind: str = "day_ahead_price_eur_mwh"


@dg.asset(
    group_name=taxonomy.BRONZE_MARKET_DATA,
    tags=taxonomy.asset_tags(
        medallion="bronze",
        domain="market_coupling",
        elt_stage="extract_load",
        ml_stage="source_data",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def poland_neighbor_market_snapshot_bronze(
    context,
    config: PolandNeighborMarketSnapshotConfig,
) -> pl.DataFrame:
    """Source-backed no-token Poland snapshot rows for governed feature routing."""

    frame = build_poland_neighbor_market_snapshot_bronze_frame(
        snapshot_csv_path=Path(config.snapshot_csv_path)
        if config.snapshot_csv_path.strip()
        else None,
        source_url=config.source_url,
        source_access_method=config.source_access_method,
        source_retrieved_at_utc=config.source_retrieved_at_utc,
        source_publication_timestamp_utc=config.source_publication_timestamp_utc,
        source_license_status=config.source_license_status,
        snapshot_kind=config.snapshot_kind,
    )
    context.add_output_metadata(
        {
            "rows": frame.height,
            "source_backed_rows": frame.filter(pl.col("source_backed")).height
            if frame.height
            else 0,
            "source_access_methods": sorted(
                frame["source_access_method"].unique().to_list()
            )
            if frame.height
            else [],
            "market_execution_enabled": False,
            "scope": "poland_neighbor_market_snapshot_bronze_research_gate",
        }
    )
    return frame


POLAND_NEIGHBOR_MARKET_SNAPSHOT_BRONZE_ASSETS = [poland_neighbor_market_snapshot_bronze]

__all__ = [
    "POLAND_NEIGHBOR_MARKET_SNAPSHOT_BRONZE_ASSETS",
    "PolandNeighborMarketSnapshotConfig",
    "poland_neighbor_market_snapshot_bronze",
]
