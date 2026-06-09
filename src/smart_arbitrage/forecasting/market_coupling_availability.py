"""Market-coupling source availability gate for future AFE/DFL features."""

from __future__ import annotations

from typing import Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.forecasting.afe import REQUIRED_AFE_FEATURE_CATALOG_COLUMNS

MARKET_COUPLING_TEMPORAL_AVAILABILITY_CLAIM_SCOPE: Final[str] = (
    "market_coupling_temporal_availability_research_gate"
)
EXTERNAL_TRAINING_BLOCKERS: Final[str] = (
    "licensing,timezone,currency,market_rules,temporal_availability,domain_shift"
)
REQUIRED_MARKET_COUPLING_AVAILABILITY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "feature_name",
        "source_name",
        "source_url",
        "source_status",
        "source_kind",
        "temporal_resolution",
        "regions",
        "candidate_neighbor_zones",
        "external_validation_role",
        "dataset_viewer_status",
        "source_observation_count",
        "source_column_count",
        "training_blockers_csv",
        "training_use_allowed",
        "licensing_status",
        "timezone_status",
        "currency_status",
        "market_rules_status",
        "temporal_availability_status",
        "domain_shift_status",
        "publication_time_policy",
        "decision_cutoff_policy",
        "readiness_status",
        "next_action",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_market_coupling_temporal_availability_frame(
    forecast_afe_feature_catalog_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build a source-governance gate before EU rows can become training features."""

    missing = sorted(
        REQUIRED_AFE_FEATURE_CATALOG_COLUMNS.difference(forecast_afe_feature_catalog_frame.columns)
    )
    if missing:
        raise ValueError(f"forecast_afe_feature_catalog_frame missing columns: {missing}")

    external_rows = forecast_afe_feature_catalog_frame.filter(
        pl.col("feature_group") == "external_market_context"
    )
    rows = [_availability_row(row) for row in external_rows.iter_rows(named=True)]
    return pl.DataFrame(rows).sort(["source_name", "feature_name"])


def validate_market_coupling_temporal_availability_evidence(
    frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate that external market-coupling sources remain no-leakage blocked."""

    failures = _missing_column_failures(frame, REQUIRED_MARKET_COUPLING_AVAILABILITY_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False,
            "market-coupling availability frame has no rows",
            {"row_count": 0},
        )

    training_allowed_rows = [row for row in rows if bool(row["training_use_allowed"])]
    missing_blocker_rows = [
        row
        for row in rows
        if str(row["training_blockers_csv"]) != EXTERNAL_TRAINING_BLOCKERS
    ]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != MARKET_COUPLING_TEMPORAL_AVAILABILITY_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
    ]
    training_ready_rows = [
        row for row in rows if str(row["readiness_status"]) == "training_ready"
    ]
    missing_temporal_policy_rows = [
        row for row in rows if not str(row["publication_time_policy"]).strip()
    ]
    if training_allowed_rows:
        failures.append("external market-coupling rows must remain blocked from training")
    if missing_blocker_rows:
        failures.append("market-coupling rows must list every external training blocker")
    if bad_claim_rows:
        failures.append("market-coupling rows must keep research-only claim flags")
    if training_ready_rows:
        failures.append("market-coupling rows cannot be training_ready in this slice")
    if missing_temporal_policy_rows:
        failures.append("market-coupling rows must define publication_time_policy")

    metadata = {
        "row_count": len(rows),
        "source_count": len({str(row["source_name"]) for row in rows}),
        "training_allowed_rows": len(training_allowed_rows),
        "missing_blocker_rows": len(missing_blocker_rows),
        "bad_claim_rows": len(bad_claim_rows),
        "training_ready_rows": len(training_ready_rows),
        "missing_temporal_policy_rows": len(missing_temporal_policy_rows),
        "pricefm_observation_count": _pricefm_observation_count(rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Market-coupling availability gate keeps external sources blocked until mapped."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def _availability_row(row: dict[str, object]) -> dict[str, object]:
    source_name = str(row["source_name"])
    overrides = _source_overrides(source_name)
    return {
        "feature_name": str(row["feature_name"]),
        "source_name": source_name,
        "source_url": str(row["source_url"]),
        "source_status": str(row["source_status"]),
        "source_kind": str(row["source_kind"]),
        "temporal_resolution": str(row["temporal_resolution"]),
        "regions": str(row["regions"]),
        "candidate_neighbor_zones": overrides["candidate_neighbor_zones"],
        "external_validation_role": str(row["external_validation_role"]),
        "dataset_viewer_status": overrides["dataset_viewer_status"],
        "source_observation_count": overrides["source_observation_count"],
        "source_column_count": overrides["source_column_count"],
        "training_blockers_csv": EXTERNAL_TRAINING_BLOCKERS,
        "training_use_allowed": False,
        "licensing_status": str(row["license_status"]),
        "timezone_status": "blocked_until_zone_and_dst_alignment",
        "currency_status": "blocked_until_uah_normalization",
        "market_rules_status": "blocked_until_dam_gate_closure_and_price_cap_mapping",
        "temporal_availability_status": "blocked_until_publication_timestamp_mapping",
        "domain_shift_status": "blocked_until_ukrainian_holdout_validation",
        "publication_time_policy": overrides["publication_time_policy"],
        "decision_cutoff_policy": str(row["decision_cutoff_policy"]),
        "readiness_status": overrides["readiness_status"],
        "next_action": overrides["next_action"],
        "claim_scope": MARKET_COUPLING_TEMPORAL_AVAILABILITY_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _source_overrides(source_name: str) -> dict[str, object]:
    if source_name == "ENTSO_E":
        return {
            "candidate_neighbor_zones": "PL,SK,HU,RO,MD",
            "dataset_viewer_status": "not_hugging_face_dataset",
            "source_observation_count": 0,
            "source_column_count": 0,
            "publication_time_policy": "requires_document_publication_time_before_ua_anchor",
            "readiness_status": "blocked_until_mapping",
            "next_action": "map_entsoe_document_types_bidding_zones_publication_times_and_terms",
        }
    if source_name == "PRICEFM_HF":
        return {
            "candidate_neighbor_zones": "european_price_graph_context",
            "dataset_viewer_status": "viewer_available",
            "source_observation_count": 140257,
            "source_column_count": 191,
            "publication_time_policy": "offline_dataset_no_operational_publication_time",
            "readiness_status": "blocked_external_validation_only",
            "next_action": "keep_as_external_validation_until_domain_shift_and_license_review",
        }
    if source_name == "NORD_POOL":
        return {
            "candidate_neighbor_zones": "nordic_baltic_bidding_zones",
            "dataset_viewer_status": "not_hugging_face_dataset",
            "source_observation_count": 0,
            "source_column_count": 0,
            "publication_time_policy": "commercial_portal_terms_required",
            "readiness_status": "blocked_restricted_access",
            "next_action": "keep_watch_only_unless_access_terms_are_approved",
        }
    return {
        "candidate_neighbor_zones": "external_validation_context",
        "dataset_viewer_status": "not_hugging_face_dataset",
        "source_observation_count": 0,
        "source_column_count": 0,
        "publication_time_policy": "not_operationally_mapped",
        "readiness_status": "blocked_external_validation_only",
        "next_action": "keep_research_only_until_availability_and_domain_shift_mapping",
    }


def _pricefm_observation_count(rows: list[dict[str, object]]) -> int:
    for row in rows:
        if str(row["source_name"]) == "PRICEFM_HF":
            value = row["source_observation_count"]
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            if isinstance(value, str) and value.strip():
                return int(value)
    return 0


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []
