from pathlib import Path

import polars as pl

from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    build_entsoe_poland_feature_governance_frame,
    validate_entsoe_neighbor_market_feature_candidate_evidence,
    validate_entsoe_poland_feature_governance_evidence,
)
from smart_arbitrage.forecasting.poland_neighbor_snapshot import (
    build_poland_neighbor_market_snapshot_bronze_frame,
    build_poland_neighbor_market_snapshot_feature_candidate_frame,
    validate_poland_neighbor_market_snapshot_evidence,
)
from smart_arbitrage.forecasting.poland_neighbor_snapshot_export import (
    build_poland_neighbor_market_snapshot_packet,
    write_poland_neighbor_market_snapshot_packet,
)


ENTSOE_PHYSICAL_FLOWS_PAGE = (
    "https://transparency.entsoe.eu/transmission/physicalFlows?"
    "permalink=6a099e4745ff0f278409f9b9"
)


def _write_snapshot_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "delivery_timestamp_utc,price_eur_mwh",
                "2026-01-01T00:00:00+00:00,102.50",
                "2026-01-01T01:00:00+00:00,111.00",
            ]
        ),
        encoding="utf-8",
    )


def _snapshot_frame(tmp_path: Path) -> pl.DataFrame:
    snapshot_path = tmp_path / "poland-dam-price-export.csv"
    _write_snapshot_csv(snapshot_path)
    return build_poland_neighbor_market_snapshot_bronze_frame(
        snapshot_csv_path=snapshot_path,
        source_url=ENTSOE_PHYSICAL_FLOWS_PAGE,
        source_access_method="manual_export_csv",
        source_retrieved_at_utc="2026-05-17T10:00:00+00:00",
        source_publication_timestamp_utc="2025-12-31T11:00:00+00:00",
        source_license_status="research_non_commercial_review_required",
    )


def test_poland_neighbor_snapshot_parses_manual_export_without_token(
    tmp_path: Path,
) -> None:
    frame = _snapshot_frame(tmp_path)

    assert frame.height == 2
    assert frame["source_backed"].unique().to_list() == [True]
    assert frame["source_access_method"].unique().to_list() == ["manual_export_csv"]
    assert frame["security_token_required"].unique().to_list() == [False]
    assert frame["source_url"].unique().to_list() == [ENTSOE_PHYSICAL_FLOWS_PAGE]
    assert frame["source_sha256"].null_count() == 0
    assert frame["training_use_allowed"].unique().to_list() == [False]
    assert frame["feature_use_allowed"].unique().to_list() == [False]
    assert frame["market_execution_enabled"].unique().to_list() == [False]

    outcome = validate_poland_neighbor_market_snapshot_evidence(frame)
    assert outcome.passed is True
    assert outcome.metadata["source_backed_rows"] == 2


def test_poland_snapshot_candidate_feeds_existing_governance_without_token(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot_frame(tmp_path)
    candidate = build_poland_neighbor_market_snapshot_feature_candidate_frame(
        snapshot,
        ua_decision_anchor_timestamp_utc="2025-12-31T12:00:00+00:00",
        prior_eur_uah_fx_rate=45.0,
        prior_eur_uah_fx_timestamp_utc="2025-12-31T11:30:00+00:00",
        fx_rate_source="fixture_prior_fx",
    )

    assert candidate.height == 2
    assert candidate["feature_name"].unique().to_list() == [
        "entsoe_neighbor_day_ahead_price_context"
    ]
    assert candidate["feature_column"].unique().to_list() == [
        "entsoe_pl_day_ahead_price_eur_mwh"
    ]
    assert candidate["source_backed"].unique().to_list() == [True]
    assert candidate["security_token_required"].unique().to_list() == [False]
    assert candidate["security_token_available"].unique().to_list() == [False]
    assert candidate["fetch_status"].unique().to_list() == [
        "source_backed_manual_snapshot_not_training"
    ]
    assert candidate["neighbor_market_price_uah_mwh"].to_list() == [4612.5, 4995.0]
    assert candidate["training_use_allowed"].unique().to_list() == [False]
    assert candidate["feature_use_allowed"].unique().to_list() == [False]

    candidate_outcome = validate_entsoe_neighbor_market_feature_candidate_evidence(
        candidate
    )
    assert candidate_outcome.passed is True
    assert candidate_outcome.metadata["token_bypass_rows"] == 0

    governance = build_entsoe_poland_feature_governance_frame(
        candidate,
        entsoe_security_token=None,
        publication_timestamp_utc="2025-12-31T11:00:00+00:00",
        ua_decision_anchor_timestamp_utc="2025-12-31T12:00:00+00:00",
        prior_eur_uah_fx_rate=45.0,
        prior_eur_uah_fx_timestamp_utc="2025-12-31T11:30:00+00:00",
        fx_rate_source="fixture_prior_fx",
        timezone_dst_mapping_ready=True,
        licensing_approved=True,
        market_rules_mapped=True,
        domain_shift_validated=True,
    )
    row = governance.row(0, named=True)
    assert row["approved_for_official_training"] is True
    assert row["training_blockers_csv"] == ""
    assert row["source_backed_row_count"] == 2

    governance_outcome = validate_entsoe_poland_feature_governance_evidence(governance)
    assert governance_outcome.passed is True
    assert governance_outcome.metadata["approved_feature_count"] == 1


def test_poland_snapshot_missing_required_columns_fails(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "bad-export.csv"
    snapshot_path.write_text("timestamp,value\n2026-01-01T00:00:00+00:00,\n", encoding="utf-8")

    try:
        build_poland_neighbor_market_snapshot_bronze_frame(
            snapshot_csv_path=snapshot_path,
            source_url=ENTSOE_PHYSICAL_FLOWS_PAGE,
            source_retrieved_at_utc="2026-05-17T10:00:00+00:00",
        )
    except ValueError as exc:
        assert "price column" in str(exc)
    else:
        raise AssertionError("missing price column should fail")


def test_poland_snapshot_evidence_packet_exports_local_artifacts(tmp_path: Path) -> None:
    snapshot = _snapshot_frame(tmp_path)
    candidate = build_poland_neighbor_market_snapshot_feature_candidate_frame(
        snapshot,
        ua_decision_anchor_timestamp_utc="2025-12-31T12:00:00+00:00",
        prior_eur_uah_fx_rate=0.0,
        prior_eur_uah_fx_timestamp_utc="",
        fx_rate_source="",
    )

    packet = build_poland_neighbor_market_snapshot_packet(
        snapshot_frame=snapshot,
        feature_candidate_frame=candidate,
    )
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["snapshot_summary"]["source_backed_rows"] == 2
    assert packet["candidate_summary"]["training_allowed_rows"] == 0

    export_dir = write_poland_neighbor_market_snapshot_packet(
        output_root=tmp_path,
        run_slug="snapshot-packet",
        snapshot_frame=snapshot,
        feature_candidate_frame=candidate,
    )

    assert (export_dir / "poland_neighbor_market_snapshot_summary.json").exists()
    assert (export_dir / "poland_neighbor_market_snapshot_summary.md").exists()
    assert (export_dir / "poland_neighbor_market_snapshot_rows.csv").exists()
    assert (export_dir / "poland_neighbor_market_feature_candidate_rows.csv").exists()
