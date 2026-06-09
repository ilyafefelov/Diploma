from __future__ import annotations

from datetime import UTC, datetime
import json

from smart_arbitrage.dfl.oree_v13_receipt_candidate_audit import (
    build_oree_v13_receipt_candidate_artifact,
    summarize_oree_v13_receipt_candidate_audit,
)


def test_oree_price_table_is_price_only_not_v13_receipt() -> None:
    artifact = build_oree_v13_receipt_candidate_artifact(
        artifact_kind="pricectr_data_view",
        source_url="https://www.oree.com.ua/index.php/pricectr/data_view",
        market="DAM",
        month="05.2026",
        delivery_date=None,
        status_code=200,
        response_headers={
            "Date": "Sun, 31 May 2026 20:41:08 GMT",
            "Content-Type": "text/html; charset=windows-1251",
        },
        response_content=(
            '{"caption":"Погодинні ціни на РДН за 05.2026",'
            '"content":"<table><tr><th>Дата</th><th>1</th></tr>'
            '<tr><td>01.05.2026</td><td>5574.92</td></tr></table>"}'
        ).encode(),
        retrieved_at=datetime(2026, 5, 31, 20, 41, 8, tzinfo=UTC),
    )

    assert artifact["delivery_timestamps_found"] is True
    assert artifact["explicit_publication_timestamps_found"] is False
    assert artifact["http_last_modified_present"] is False
    assert artifact["download_filename"] == ""
    assert artifact["v13_verdict"] == "price_only"
    assert artifact["validated_receipt_csv_ready"] is False
    assert artifact["market_execution_enabled"] is False


def test_oree_pxs_hdata_is_observation_only_not_v13_receipt() -> None:
    artifact = build_oree_v13_receipt_candidate_artifact(
        artifact_kind="pxs_hdata",
        source_url=(
            "https://www.oree.com.ua/index.php/PXS/get_pxs_hdata/25.05.2026/IDM/2"
        ),
        market="IDM",
        month="05.2026",
        delivery_date="2026-05-25",
        status_code=200,
        response_headers={
            "Date": "Sun, 31 May 2026 20:43:41 GMT",
            "Content-Type": "text/html; charset=windows-1251",
        },
        response_content=json.dumps(
            {
                "html": (
                    "<table><tr><th>Година</th><th>Ціна, грн/МВт.год</th></tr>"
                    "<tr><td>1</td><td>7163.31</td></tr></table>"
                )
            }
        ).encode(),
        retrieved_at=datetime(2026, 5, 31, 20, 43, 41, tzinfo=UTC),
    )

    assert artifact["delivery_timestamps_found"] is True
    assert artifact["hourly_result_rows_found"] is True
    assert artifact["explicit_publication_timestamps_found"] is False
    assert artifact["v13_verdict"] == "observation_only"
    assert artifact["verdict_reason"] == "row_level_result_observed_without_publication_timestamp"
    assert artifact["permits_model_training"] is False
    assert artifact["market_execution_enabled"] is False


def test_explicit_source_publication_timestamp_is_valid_candidate_without_promotion() -> None:
    artifact = build_oree_v13_receipt_candidate_artifact(
        artifact_kind="manual_receipt_export",
        source_url="https://example.test/oree-dam-receipts.csv",
        market="DAM",
        month="05.2026",
        delivery_date=None,
        status_code=200,
        response_headers={
            "Last-Modified": "Thu, 30 Apr 2026 11:00:00 GMT",
            "Content-Type": "text/csv",
        },
        response_content=(
            "timestamp,source_publication_timestamp\n"
            "2026-05-01T00:00:00,2026-04-30T11:00:00\n"
        ).encode(),
        retrieved_at=datetime(2026, 5, 31, 20, 43, 41, tzinfo=UTC),
    )

    assert artifact["delivery_timestamps_found"] is True
    assert artifact["explicit_publication_timestamps_found"] is True
    assert artifact["http_last_modified_present"] is True
    assert artifact["v13_verdict"] == "valid_receipt"
    assert artifact["receipt_csv_generated"] is False
    assert artifact["validated_receipt_csv_ready"] is False
    assert artifact["market_execution_enabled"] is False


def test_oree_v13_receipt_candidate_audit_cli_writes_non_promotional_outputs(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts.audit_oree_v13_receipt_candidates import main
    import scripts.audit_oree_v13_receipt_candidates as cli

    output_json = tmp_path / "audit.json"
    output_csv = tmp_path / "audit.csv"
    artifact = build_oree_v13_receipt_candidate_artifact(
        artifact_kind="pricectr_get_file",
        source_url="https://www.oree.com.ua/index.php/pricectr/get_file",
        market="DAM",
        month="05.2026",
        delivery_date=None,
        status_code=200,
        response_headers={
            "Content-Disposition": 'attachment;filename="price_DAM_IDM_05.2026.xls"',
            "Content-Type": "application/vnd.ms-excel",
        },
        response_content=b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1fake workbook",
        retrieved_at=datetime(2026, 5, 31, 20, 41, 8, tzinfo=UTC),
    )

    def fake_fetch(*, month: str, delivery_date: str, zone: str, timeout_seconds: float):
        assert month == "05.2026"
        assert delivery_date == "2026-05-25"
        assert zone == "IPS"
        assert timeout_seconds == 45.0
        return [artifact]

    monkeypatch.setattr(cli, "_fetch_oree_v13_receipt_candidate_artifacts", fake_fetch)

    exit_code = main(
        [
            "--month",
            "05.2026",
            "--delivery-date",
            "2026-05-25",
            "--output-json",
            str(output_json),
            "--output-csv",
            str(output_csv),
        ]
    )

    assert exit_code == 0
    audit = json.loads(output_json.read_text(encoding="utf-8"))
    csv_text = output_csv.read_text(encoding="utf-8")
    assert audit["claim_scope"] == "oree_v13_receipt_candidate_audit_not_receipt"
    assert audit["artifact_count"] == 1
    assert audit["verdict_counts"] == {"price_only": 1}
    assert audit["candidate_receipt_source_found"] is False
    assert audit["receipt_csv_generated"] is False
    assert audit["validated_receipt_csv_ready"] is False
    assert audit["permits_model_training"] is False
    assert audit["market_execution_enabled"] is False
    assert "price_DAM_IDM_05.2026.xls" in csv_text
    assert "price_only" in csv_text


def test_oree_v13_receipt_candidate_audit_summary_surfaces_valid_candidates_only() -> None:
    price_only = build_oree_v13_receipt_candidate_artifact(
        artifact_kind="indexes_downloadfile",
        source_url="https://www.oree.com.ua/index.php/indexes/downloadfile",
        market="INDEXES",
        month="05.2026",
        delivery_date=None,
        status_code=200,
        response_headers={"Content-Type": "application/vnd.ms-excel"},
        response_content=b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1fake indexes workbook",
        retrieved_at=datetime(2026, 5, 31, 20, 41, 8, tzinfo=UTC),
    )
    valid_candidate = build_oree_v13_receipt_candidate_artifact(
        artifact_kind="manual_receipt_export",
        source_url="https://example.test/oree-dam-receipts.csv",
        market="DAM",
        month="05.2026",
        delivery_date=None,
        status_code=200,
        response_headers={"Content-Type": "text/csv"},
        response_content=(
            "timestamp,source_publication_timestamp\n"
            "2026-05-01T00:00:00,2026-04-30T11:00:00\n"
        ).encode(),
        retrieved_at=datetime(2026, 5, 31, 20, 43, 41, tzinfo=UTC),
    )

    audit = summarize_oree_v13_receipt_candidate_audit(
        [price_only, valid_candidate],
        requested_month="05.2026",
        requested_delivery_date="2026-05-25",
    )

    assert audit["artifact_count"] == 2
    assert audit["candidate_receipt_source_found"] is True
    assert audit["valid_receipt_artifact_count"] == 1
    assert audit["verdict_counts"] == {"price_only": 1, "valid_receipt": 1}
    assert audit["receipt_csv_generated"] is False
    assert audit["validated_receipt_csv_ready"] is False
    assert audit["market_execution_enabled"] is False
