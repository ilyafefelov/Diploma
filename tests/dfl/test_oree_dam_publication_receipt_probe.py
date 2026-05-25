from __future__ import annotations

from datetime import UTC, datetime
import json

import polars as pl

from smart_arbitrage.dfl.oree_dam_publication_observations import (
    build_oree_dam_publication_observation_frame,
    empty_oree_dam_publication_observation_frame,
    summarize_oree_dam_publication_observation_frame,
)
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


def test_oree_pxs_observation_capture_is_not_v13_receipt_metadata() -> None:
    frame = build_oree_dam_publication_observation_frame(
        delivery_date="25.05.2026",
        hdata_payload={
            "labels": [1, 2],
            "pricesData": [8000.0, 6482.0],
            "amountsData": [3189.6, 2832.2],
        },
        retrieved_at=datetime(2026, 5, 24, 18, 52, 38, tzinfo=UTC),
        source_url="https://www.oree.com.ua/index.php/control/results_mo/DAM",
    )

    assert frame["timestamp"].to_list() == [
        datetime(2026, 5, 25, 0, 0),
        datetime(2026, 5, 25, 1, 0),
    ]
    assert "source_publication_timestamp" not in frame.columns
    assert frame["source_observed_at_utc"].to_list() == [
        "2026-05-24T18:52:38+00:00",
        "2026-05-24T18:52:38+00:00",
    ]
    assert frame["publication_observation_status"].unique().to_list() == [
        "observed_without_source_publication_timestamp"
    ]
    assert frame["validated_receipt_csv_ready"].to_list() == [False, False]
    assert frame["permits_model_training"].to_list() == [False, False]
    assert frame["market_execution_enabled"].to_list() == [False, False]


def test_oree_pxs_observation_summary_preserves_blocker_boundary() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 5, 25, 0, 0)],
            "source_observed_at_utc": ["2026-05-24T18:52:38+00:00"],
            "publication_observation_status": [
                "observed_without_source_publication_timestamp"
            ],
            "validated_receipt_csv_ready": [False],
            "permits_model_training": [False],
            "market_execution_enabled": [False],
        }
    )

    summary = summarize_oree_dam_publication_observation_frame(frame)

    assert summary["claim_scope"] == "oree_dam_publication_observation_not_v13_receipt"
    assert summary["observation_rows"] == 1
    assert summary["can_satisfy_v13_explicit_receipts"] is False
    assert summary["receipt_csv_generated"] is False
    assert summary["validated_receipt_csv_ready"] is False
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False


def test_oree_pxs_empty_first_seen_summary_preserves_blocker_boundary() -> None:
    frame = empty_oree_dam_publication_observation_frame()

    summary = summarize_oree_dam_publication_observation_frame(frame)

    assert frame.columns
    assert summary["observation_rows"] == 0
    assert summary["validated_receipt_csv_ready"] is False
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False


def test_oree_pxs_observation_cli_writes_empty_polling_blocker(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts.capture_oree_dam_publication_observations import main
    import scripts.capture_oree_dam_publication_observations as cli

    output_csv = tmp_path / "observations.csv"
    summary_json = tmp_path / "summary.json"
    attempt_log_json = tmp_path / "attempts.json"

    def fake_fetch(
        delivery_date,
        *,
        max_attempts: int,
        sleep_seconds: float,
    ):
        assert max_attempts == 2
        assert sleep_seconds == 0
        return None, [
            {
                "attempt_index": 1,
                "retrieved_at": "2026-05-24T10:00:00+00:00",
                "delivery_date": delivery_date.isoformat(),
                "hdata_link_found": False,
                "source_probe_status": "hdata_not_found",
                "market_execution_enabled": False,
                "permits_model_training": False,
                "validated_receipt_csv_ready": False,
            },
            {
                "attempt_index": 2,
                "retrieved_at": "2026-05-24T10:00:30+00:00",
                "delivery_date": delivery_date.isoformat(),
                "hdata_link_found": False,
                "source_probe_status": "hdata_not_found",
                "market_execution_enabled": False,
                "permits_model_training": False,
                "validated_receipt_csv_ready": False,
            },
        ]

    monkeypatch.setattr(cli, "_fetch_oree_pxs_hdata_payload_with_attempts", fake_fetch)

    exit_code = main(
        [
            "--delivery-date",
            "2026-05-26",
            "--max-attempts",
            "2",
            "--sleep-seconds",
            "0",
            "--output-csv",
            str(output_csv),
            "--summary-json",
            str(summary_json),
            "--attempt-log-json",
            str(attempt_log_json),
        ]
    )

    assert exit_code == 0
    summary = json.loads(summary_json.read_text(encoding="utf-8"))
    attempts = json.loads(attempt_log_json.read_text(encoding="utf-8"))
    assert summary["source_probe_status"] == "hdata_not_found"
    assert summary["attempt_count"] == 2
    assert summary["first_seen_attempt_index"] is None
    assert summary["observation_rows"] == 0
    assert summary["receipt_csv_generated"] is False
    assert summary["validated_receipt_csv_ready"] is False
    assert summary["market_execution_enabled"] is False
    assert attempts[0]["hdata_link_found"] is False
    assert pl.read_csv(output_csv).height == 0
