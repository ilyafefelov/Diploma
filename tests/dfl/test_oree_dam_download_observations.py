from __future__ import annotations

from datetime import UTC, datetime

from smart_arbitrage.dfl.oree_dam_download_observations import (
    build_oree_dam_download_observation_frame,
    summarize_oree_dam_download_observation_frame,
)


def test_oree_dam_download_observation_keeps_missing_publication_header_blocked() -> None:
    frame = build_oree_dam_download_observation_frame(
        delivery_date="2026-05-25",
        hdata_link="25.05.2026/DAM/2",
        hdata_payload={
            "labels": [1, 2],
            "pricesData": [8000, 6482],
            "amountsData": [3189.6, 2832.2],
        },
        hdata_headers={"date": "Sun, 24 May 2026 20:09:34 GMT"},
        download_headers={
            "date": "Sun, 24 May 2026 20:09:34 GMT",
            "content-disposition": 'attachment;filename="DAM_25.05.2026.xls"',
            "content-type": "application/vnd.ms-excel",
        },
        download_content=b"fake-xls",
        retrieved_at=datetime(2026, 5, 24, 20, 10, tzinfo=UTC),
        source_url="https://www.oree.com.ua/index.php/control/results_mo/DAM",
    )

    assert frame.height == 2
    assert frame["timestamp"].to_list() == [
        datetime(2026, 5, 25, 0),
        datetime(2026, 5, 25, 1),
    ]
    assert frame["download_filename"].to_list() == [
        "DAM_25.05.2026.xls",
        "DAM_25.05.2026.xls",
    ]
    assert frame["receipt_candidate_status"].to_list() == [
        "download_observed_without_source_publication_timestamp",
        "download_observed_without_source_publication_timestamp",
    ]
    assert frame["validated_receipt_csv_ready"].to_list() == [False, False]
    assert frame["market_execution_enabled"].to_list() == [False, False]

    summary = summarize_oree_dam_download_observation_frame(frame)
    assert summary["observation_rows"] == 2
    assert summary["download_file_count"] == 1
    assert summary["last_modified_header_rows"] == 0
    assert summary["candidate_receipt_source_found"] is False
    assert summary["receipt_csv_generated"] is False
    assert summary["market_execution_enabled"] is False
