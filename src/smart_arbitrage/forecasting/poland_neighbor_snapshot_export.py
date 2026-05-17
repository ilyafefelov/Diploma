"""Local evidence packet export for Poland neighbor-market snapshots."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    validate_entsoe_neighbor_market_feature_candidate_evidence,
)
from smart_arbitrage.forecasting.poland_neighbor_snapshot import (
    validate_poland_neighbor_market_snapshot_evidence,
)

SUMMARY_JSON_ARTIFACT_NAME: Final[str] = "poland_neighbor_market_snapshot_summary.json"
SUMMARY_MD_ARTIFACT_NAME: Final[str] = "poland_neighbor_market_snapshot_summary.md"
SNAPSHOT_ROWS_CSV_ARTIFACT_NAME: Final[str] = "poland_neighbor_market_snapshot_rows.csv"
FEATURE_CANDIDATE_ROWS_CSV_ARTIFACT_NAME: Final[str] = (
    "poland_neighbor_market_feature_candidate_rows.csv"
)


def build_poland_neighbor_market_snapshot_packet(
    *,
    snapshot_frame: pl.DataFrame,
    feature_candidate_frame: pl.DataFrame,
) -> dict[str, Any]:
    """Build a packet only after snapshot and candidate evidence validate."""

    snapshot_outcome = validate_poland_neighbor_market_snapshot_evidence(snapshot_frame)
    if not snapshot_outcome.passed:
        raise ValueError(
            "Poland neighbor snapshot evidence check failed; refusing export: "
            f"{snapshot_outcome.description}"
        )
    candidate_outcome = validate_entsoe_neighbor_market_feature_candidate_evidence(
        feature_candidate_frame
    )
    if not candidate_outcome.passed:
        raise ValueError(
            "Poland neighbor feature candidate evidence check failed; refusing export: "
            f"{candidate_outcome.description}"
        )

    snapshot_rows = _frame_rows(snapshot_frame)
    candidate_rows = _frame_rows(feature_candidate_frame)
    return {
        "schema_version": 1,
        "packet_kind": "poland_neighbor_market_snapshot_no_token_route",
        "snapshot_summary": {
            **snapshot_outcome.metadata,
            "source_names": sorted({str(row["source_name"]) for row in snapshot_rows}),
            "source_urls": sorted({str(row["source_url"]) for row in snapshot_rows}),
            "source_access_methods": sorted(
                {str(row["source_access_method"]) for row in snapshot_rows}
            ),
            "source_license_statuses": sorted(
                {str(row["source_license_status"]) for row in snapshot_rows}
            ),
        },
        "candidate_summary": {
            **candidate_outcome.metadata,
            "feature_columns": sorted(
                {str(row["feature_column"]) for row in candidate_rows}
            ),
            "fetch_statuses": sorted({str(row["fetch_status"]) for row in candidate_rows}),
        },
        "claim_boundary": {
            "offline_strategy_promotion_scope": True,
            "market_execution_enabled": False,
            "no_live_execution": True,
            "no_dashboard_api_default_switch": True,
            "no_eu_training_rows": True,
            "external_rows_are_point_in_time_feature_candidates_only": True,
        },
        "artifacts": {
            "summary_json": SUMMARY_JSON_ARTIFACT_NAME,
            "summary_markdown": SUMMARY_MD_ARTIFACT_NAME,
            "snapshot_rows_csv": SNAPSHOT_ROWS_CSV_ARTIFACT_NAME,
            "feature_candidate_rows_csv": FEATURE_CANDIDATE_ROWS_CSV_ARTIFACT_NAME,
        },
    }


def write_poland_neighbor_market_snapshot_packet(
    *,
    output_root: Path,
    run_slug: str,
    snapshot_frame: pl.DataFrame,
    feature_candidate_frame: pl.DataFrame,
) -> Path:
    """Write JSON, Markdown, and row-level CSV packet artifacts."""

    packet = build_poland_neighbor_market_snapshot_packet(
        snapshot_frame=snapshot_frame,
        feature_candidate_frame=feature_candidate_frame,
    )
    export_dir = output_root / run_slug
    export_dir.mkdir(parents=True, exist_ok=True)
    (export_dir / SUMMARY_JSON_ARTIFACT_NAME).write_text(
        json.dumps(packet, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (export_dir / SUMMARY_MD_ARTIFACT_NAME).write_text(
        _packet_markdown(packet),
        encoding="utf-8",
    )
    snapshot_frame.write_csv(export_dir / SNAPSHOT_ROWS_CSV_ARTIFACT_NAME)
    feature_candidate_frame.write_csv(export_dir / FEATURE_CANDIDATE_ROWS_CSV_ARTIFACT_NAME)
    return export_dir


def _packet_markdown(packet: dict[str, Any]) -> str:
    snapshot = packet["snapshot_summary"]
    candidate = packet["candidate_summary"]
    return "\n".join(
        [
            "# Poland Neighbor-Market Snapshot Evidence",
            "",
            "This packet records a no-token external feature route based on a local/public "
            "snapshot. It is source evidence only until governance approves training use.",
            "",
            "## Snapshot",
            "",
            f"- Source-backed rows: {snapshot['source_backed_rows']}",
            f"- Source names: `{', '.join(snapshot['source_names'])}`",
            f"- Access methods: `{', '.join(snapshot['source_access_methods'])}`",
            f"- License statuses: `{', '.join(snapshot['source_license_statuses'])}`",
            "",
            "## Feature Candidates",
            "",
            f"- Candidate rows: {candidate['row_count']}",
            f"- Training rows: {candidate['training_allowed_rows']}",
            f"- Feature rows: {candidate['feature_allowed_rows']}",
            f"- Token bypass rows: {candidate['token_bypass_rows']}",
            f"- Feature columns: `{', '.join(candidate['feature_columns'])}`",
            "",
            "## Claim Boundary",
            "",
            "- Offline Strategy Promotion evidence only.",
            "- `market_execution_enabled=false`.",
            "- No live market execution.",
            "- No dashboard/API default switch.",
            "- No European rows enter Ukrainian training; only governed exogenous columns may be routed.",
            "",
        ]
    )


def _frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in frame.to_dicts():
        rows.append(
            {
                key: value.isoformat() if hasattr(value, "isoformat") else value
                for key, value in row.items()
            }
        )
    return rows


__all__ = [
    "build_poland_neighbor_market_snapshot_packet",
    "write_poland_neighbor_market_snapshot_packet",
]
