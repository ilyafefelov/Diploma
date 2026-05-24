from __future__ import annotations

from datetime import UTC, datetime

from smart_arbitrage.dfl.oree_dam_publication_receipts import (
    build_oree_dam_publication_receipt_source_audit,
    build_oree_dam_publication_receipt_probe,
)


def test_oree_dam_receipt_probe_marks_http_retrieval_metadata_insufficient() -> None:
    probe = build_oree_dam_publication_receipt_probe(
        requested_month="04.2026",
        source_url="https://www.oree.com.ua/index.php/pricectr/data_view",
        response_headers={
            "Date": "Sun, 24 May 2026 00:00:20 GMT",
            "Cache-Control": "no-store, must-revalidate, no-cache",
        },
        response_text='<table id="price_table"><tr><th>Дата</th><th>1</th></tr></table>',
        retrieved_at=datetime(2026, 5, 24, 0, 0, 21, tzinfo=UTC),
    )

    assert probe["receipt_status"] == "not_sufficient_for_v13_receipts"
    assert probe["row_level_publication_metadata_found"] is False
    assert probe["http_date_header_is_retrieval_metadata_only"] is True
    assert probe["http_last_modified_present"] is False
    assert probe["market_execution_enabled"] is False
    assert probe["not_market_execution"] is True


def test_oree_dam_receipt_probe_accepts_explicit_row_level_publication_metadata() -> None:
    probe = build_oree_dam_publication_receipt_probe(
        requested_month="04.2026",
        source_url="https://example.test/oree-receipts.csv",
        response_headers={"Last-Modified": "Tue, 28 Apr 2026 11:00:00 GMT"},
        response_text="timestamp,source_publication_timestamp\n2026-04-29T23:00:00,2026-04-28T14:00:00\n",
        retrieved_at=datetime(2026, 5, 24, 0, 0, 21, tzinfo=UTC),
    )

    assert probe["receipt_status"] == "candidate_receipt_metadata_present"
    assert probe["row_level_publication_metadata_found"] is True
    assert probe["http_last_modified_present"] is True
    assert probe["market_execution_enabled"] is False


def test_oree_dam_receipt_source_audit_keeps_negative_months_as_blocker() -> None:
    audit = build_oree_dam_publication_receipt_source_audit(
        [
            build_oree_dam_publication_receipt_probe(
                requested_month="03.2026",
                source_url="https://www.oree.com.ua/index.php/pricectr/data_view",
                response_headers={"Date": "Sun, 24 May 2026 00:00:20 GMT"},
                response_text="<table><tr><th>date</th></tr></table>",
                retrieved_at=datetime(2026, 5, 24, 0, 0, 21, tzinfo=UTC),
            ),
            build_oree_dam_publication_receipt_probe(
                requested_month="04.2026",
                source_url="https://www.oree.com.ua/index.php/pricectr/data_view",
                response_headers={"Date": "Sun, 24 May 2026 00:00:22 GMT"},
                response_text="<table><tr><th>date</th></tr></table>",
                retrieved_at=datetime(2026, 5, 24, 0, 0, 23, tzinfo=UTC),
            ),
        ]
    )

    assert audit["claim_scope"] == "oree_dam_publication_receipt_source_audit"
    assert audit["months_probed"] == ["03.2026", "04.2026"]
    assert audit["probe_count"] == 2
    assert audit["candidate_receipt_source_found"] is False
    assert audit["all_probes_insufficient_for_v13_receipts"] is True
    assert audit["insufficient_months"] == ["03.2026", "04.2026"]
    assert audit["candidate_receipt_months"] == []
    assert audit["market_execution_enabled"] is False
    assert audit["not_market_execution"] is True


def test_oree_dam_receipt_source_audit_surfaces_candidate_months_without_promotion() -> None:
    audit = build_oree_dam_publication_receipt_source_audit(
        [
            build_oree_dam_publication_receipt_probe(
                requested_month="04.2026",
                source_url="https://example.test/oree-receipts.csv",
                response_headers={"Last-Modified": "Tue, 28 Apr 2026 11:00:00 GMT"},
                response_text=(
                    "timestamp,source_publication_timestamp\n"
                    "2026-04-29T23:00:00,2026-04-28T14:00:00\n"
                ),
                retrieved_at=datetime(2026, 5, 24, 0, 0, 21, tzinfo=UTC),
            )
        ]
    )

    assert audit["candidate_receipt_source_found"] is True
    assert audit["candidate_receipt_months"] == ["04.2026"]
    assert audit["all_probes_insufficient_for_v13_receipts"] is False
    assert audit["market_execution_enabled"] is False
    assert audit["not_full_dfl"] is True
