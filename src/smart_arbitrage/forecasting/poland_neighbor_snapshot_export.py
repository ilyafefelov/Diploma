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
    validate_entsoe_poland_governance_closure_evidence,
    validate_poland_neighbor_market_hourly_feature_evidence,
    validate_poland_neighbor_market_snapshot_evidence,
)

SUMMARY_JSON_ARTIFACT_NAME: Final[str] = "poland_neighbor_market_snapshot_summary.json"
SUMMARY_MD_ARTIFACT_NAME: Final[str] = "poland_neighbor_market_snapshot_summary.md"
SNAPSHOT_ROWS_CSV_ARTIFACT_NAME: Final[str] = "poland_neighbor_market_snapshot_rows.csv"
FEATURE_CANDIDATE_ROWS_CSV_ARTIFACT_NAME: Final[str] = (
    "poland_neighbor_market_feature_candidate_rows.csv"
)
GOVERNANCE_CLOSURE_SUMMARY_JSON_ARTIFACT_NAME: Final[str] = (
    "entsoe_poland_governance_closure_summary.json"
)
GOVERNANCE_CLOSURE_SUMMARY_MD_ARTIFACT_NAME: Final[str] = (
    "entsoe_poland_governance_closure_summary.md"
)
HOURLY_FEATURE_ROWS_CSV_ARTIFACT_NAME: Final[str] = (
    "poland_neighbor_market_hourly_feature_rows.csv"
)
GOVERNANCE_CLOSURE_ROWS_CSV_ARTIFACT_NAME: Final[str] = (
    "entsoe_poland_governance_closure_rows.csv"
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


def build_entsoe_poland_governance_closure_packet(
    *,
    snapshot_frame: pl.DataFrame,
    hourly_feature_frame: pl.DataFrame,
    governance_closure_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
) -> dict[str, Any]:
    """Build a Poland hourly governance-closure packet after checks pass."""

    snapshot_outcome = validate_poland_neighbor_market_snapshot_evidence(snapshot_frame)
    if not snapshot_outcome.passed:
        raise ValueError(
            "Poland neighbor snapshot evidence check failed; refusing export: "
            f"{snapshot_outcome.description}"
        )
    hourly_outcome = validate_poland_neighbor_market_hourly_feature_evidence(
        hourly_feature_frame
    )
    if not hourly_outcome.passed:
        raise ValueError(
            "Poland hourly feature evidence check failed; refusing export: "
            f"{hourly_outcome.description}"
        )
    closure_outcome = validate_entsoe_poland_governance_closure_evidence(
        governance_closure_frame
    )
    if not closure_outcome.passed:
        raise ValueError(
            "ENTSO-E Poland governance closure evidence check failed; refusing export: "
            f"{closure_outcome.description}"
        )

    snapshot_rows = _frame_rows(snapshot_frame)
    hourly_rows = _frame_rows(hourly_feature_frame)
    closure_rows = _frame_rows(governance_closure_frame)
    closure_row = closure_rows[0] if closure_rows else {}
    blockers = _csv_values(str(closure_row.get("training_blockers_csv", "")))
    return {
        "schema_version": 1,
        "packet_kind": "entsoe_poland_governance_closure",
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
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
        "hourly_feature_summary": {
            **hourly_outcome.metadata,
            "feature_columns": sorted(
                {str(row["feature_column"]) for row in hourly_rows}
            ),
            "source_resolution_minutes": sorted(
                {int(row["source_resolution_minutes"]) for row in hourly_rows}
            ),
        },
        "governance_summary": {
            **closure_outcome.metadata,
            "readiness_status": str(closure_row.get("readiness_status", "")),
            "approved_feature_column": str(
                closure_row.get("approved_feature_column", "")
            ),
            "publication_time_status": str(
                closure_row.get("publication_time_status", "")
            ),
            "currency_status": str(closure_row.get("currency_status", "")),
            "timezone_status": str(closure_row.get("timezone_status", "")),
            "licensing_status": str(closure_row.get("licensing_status", "")),
            "market_rules_status": str(closure_row.get("market_rules_status", "")),
            "domain_shift_status": str(closure_row.get("domain_shift_status", "")),
            "temporal_availability_status": str(
                closure_row.get("temporal_availability_status", "")
            ),
            "blockers": blockers,
        },
        "claim_boundary": {
            "offline_strategy_promotion_scope": True,
            "market_execution_enabled": False,
            "no_live_execution": True,
            "no_dashboard_api_default_switch": True,
            "no_eu_training_rows": True,
            "external_rows_are_point_in_time_feature_candidates_only": True,
            "blocked_governance_row_is_valid_evidence": True,
        },
        "artifacts": {
            "summary_json": GOVERNANCE_CLOSURE_SUMMARY_JSON_ARTIFACT_NAME,
            "summary_markdown": GOVERNANCE_CLOSURE_SUMMARY_MD_ARTIFACT_NAME,
            "snapshot_rows_csv": SNAPSHOT_ROWS_CSV_ARTIFACT_NAME,
            "hourly_feature_rows_csv": HOURLY_FEATURE_ROWS_CSV_ARTIFACT_NAME,
            "governance_closure_rows_csv": GOVERNANCE_CLOSURE_ROWS_CSV_ARTIFACT_NAME,
        },
    }


def write_entsoe_poland_governance_closure_packet(
    *,
    output_root: Path,
    run_slug: str,
    snapshot_frame: pl.DataFrame,
    hourly_feature_frame: pl.DataFrame,
    governance_closure_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
) -> Path:
    """Write JSON, Markdown, and row-level CSV artifacts for Poland closure."""

    packet = build_entsoe_poland_governance_closure_packet(
        snapshot_frame=snapshot_frame,
        hourly_feature_frame=hourly_feature_frame,
        governance_closure_frame=governance_closure_frame,
        dagster_run_id=dagster_run_id,
        materialization_command=materialization_command,
    )
    export_dir = output_root / run_slug
    export_dir.mkdir(parents=True, exist_ok=True)
    (export_dir / GOVERNANCE_CLOSURE_SUMMARY_JSON_ARTIFACT_NAME).write_text(
        json.dumps(packet, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (export_dir / GOVERNANCE_CLOSURE_SUMMARY_MD_ARTIFACT_NAME).write_text(
        _governance_closure_packet_markdown(packet),
        encoding="utf-8",
    )
    snapshot_frame.write_csv(export_dir / SNAPSHOT_ROWS_CSV_ARTIFACT_NAME)
    hourly_feature_frame.write_csv(export_dir / HOURLY_FEATURE_ROWS_CSV_ARTIFACT_NAME)
    governance_closure_frame.write_csv(export_dir / GOVERNANCE_CLOSURE_ROWS_CSV_ARTIFACT_NAME)
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


def _governance_closure_packet_markdown(packet: dict[str, Any]) -> str:
    snapshot = packet["snapshot_summary"]
    hourly = packet["hourly_feature_summary"]
    governance = packet["governance_summary"]
    blockers = governance["blockers"]
    return "\n".join(
        [
            "# ENTSO-E Poland Governance Closure Evidence",
            "",
            "This packet records source-backed Poland neighbor-market evidence after "
            "hourly alignment and governance closure. It does not approve external "
            "features for training unless every point-in-time governance gate passes.",
            "",
            "## Source Snapshot",
            "",
            f"- Source-backed rows: {snapshot['source_backed_rows']}",
            f"- Source names: `{', '.join(snapshot['source_names'])}`",
            f"- Access methods: `{', '.join(snapshot['source_access_methods'])}`",
            f"- License statuses: `{', '.join(snapshot['source_license_statuses'])}`",
            "",
            "## Hourly Feature Evidence",
            "",
            f"- Hourly rows: {hourly['source_backed_hour_count']}",
            f"- Feature columns: `{', '.join(hourly['feature_columns'])}`",
            f"- Source resolutions, minutes: `{', '.join(str(v) for v in hourly['source_resolution_minutes'])}`",
            "",
            "## Governance Closure",
            "",
            f"- Readiness status: `{governance['readiness_status']}`",
            f"- Approved feature count: {governance['approved_feature_count']}",
            f"- Training rows: {governance['training_allowed_rows']}",
            f"- Blockers: `{', '.join(blockers) if blockers else 'none'}`",
            f"- Publication-time status: `{governance['publication_time_status']}`",
            f"- Currency status: `{governance['currency_status']}`",
            f"- Timezone status: `{governance['timezone_status']}`",
            f"- Licensing status: `{governance['licensing_status']}`",
            f"- Market-rule status: `{governance['market_rules_status']}`",
            f"- Domain-shift status: `{governance['domain_shift_status']}`",
            f"- Temporal availability status: `{governance['temporal_availability_status']}`",
            "",
            "## Claim Boundary",
            "",
            "- Offline Strategy Promotion evidence only.",
            "- `market_execution_enabled=false`.",
            "- No live market execution.",
            "- No dashboard/API default switch.",
            "- No European rows enter Ukrainian training; only governed exogenous columns may be routed.",
            "- A blocked governance row is valid evidence; it is not feature admission.",
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


def _csv_values(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


__all__ = [
    "build_entsoe_poland_governance_closure_packet",
    "build_poland_neighbor_market_snapshot_packet",
    "write_entsoe_poland_governance_closure_packet",
    "write_poland_neighbor_market_snapshot_packet",
]
