from __future__ import annotations

from datetime import UTC, datetime
import json

from smart_arbitrage.dfl.oree_policy_publication_deadline_evidence import (
    build_oree_policy_publication_deadline_evidence_frame,
    summarize_oree_policy_publication_deadline_evidence_frame,
)


def _artifact_row(
    *,
    artifact_kind: str,
    market: str,
    delivery_date: str,
    retrieved_at: str,
    verdict: str,
) -> dict[str, object]:
    return {
        "artifact_kind": artifact_kind,
        "source_url": (
            "https://www.oree.com.ua/index.php/PXS/get_pxs_hdata/"
            f"{delivery_date.replace('-', '.')}/{market}/2"
        ),
        "market": market,
        "month": "05.2026",
        "delivery_date": delivery_date,
        "status": "http_ok",
        "status_code": 200,
        "content_type": "text/html; charset=windows-1251",
        "download_filename": "",
        "http_last_modified_present": False,
        "delivery_timestamps_found": True,
        "explicit_publication_timestamps_found": False,
        "hourly_result_rows_found": True,
        "v13_verdict": verdict,
        "verdict_reason": "row_level_result_observed_without_publication_timestamp",
        "retrieved_at": retrieved_at,
        "validated_receipt_csv_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def test_policy_deadline_evidence_derives_dam_and_idm_deadlines_without_receipts() -> None:
    frame = build_oree_policy_publication_deadline_evidence_frame(
        {
            "requested_month": "05.2026",
            "requested_delivery_date": "2026-05-25",
            "artifact_rows": [
                _artifact_row(
                    artifact_kind="pxs_hdata",
                    market="DAM",
                    delivery_date="2026-05-25",
                    retrieved_at="2026-05-31T20:43:41+00:00",
                    verdict="observation_only",
                ),
                _artifact_row(
                    artifact_kind="pxs_downloadxlsx",
                    market="IDM",
                    delivery_date="2026-05-25",
                    retrieved_at="2026-05-31T20:43:42+00:00",
                    verdict="observation_only",
                ),
            ],
        },
        generated_at=datetime(2026, 6, 1, 9, 0, tzinfo=UTC),
    )

    assert frame.height == 2
    assert "source_publication_timestamp" not in frame.columns
    rows = {
        row["market"]: row
        for row in frame.sort("market").iter_rows(named=True)
    }
    assert rows["DAM"]["policy_publication_deadline_kyiv"] == "2026-05-24T14:00:00"
    assert rows["IDM"]["policy_publication_deadline_kyiv"] == "2026-05-26T14:00:00"
    assert rows["DAM"]["publication_evidence_kind"] == (
        "policy_deadline_plus_observed_public_presence"
    )
    assert rows["IDM"]["publication_evidence_kind"] == (
        "policy_deadline_plus_observed_public_presence"
    )
    assert rows["DAM"]["policy_deadline_is_publication_timestamp"] is False
    assert rows["IDM"]["explicit_publication_timestamp_available"] is False
    assert rows["DAM"]["can_satisfy_v13_explicit_receipts"] is False
    assert rows["IDM"]["validated_receipt_csv_ready"] is False
    assert rows["DAM"]["market_execution_enabled"] is False


def test_policy_deadline_evidence_summary_is_non_promotional() -> None:
    frame = build_oree_policy_publication_deadline_evidence_frame(
        {
            "requested_month": "05.2026",
            "requested_delivery_date": "2026-05-25",
            "artifact_rows": [
                _artifact_row(
                    artifact_kind="pxs_hdata",
                    market="DAM",
                    delivery_date="2026-05-25",
                    retrieved_at="2026-05-31T20:43:41+00:00",
                    verdict="observation_only",
                )
            ],
        },
        generated_at=datetime(2026, 6, 1, 9, 0, tzinfo=UTC),
    )

    summary = summarize_oree_policy_publication_deadline_evidence_frame(frame)

    assert summary["claim_scope"] == (
        "oree_policy_publication_deadline_evidence_not_v13_receipt"
    )
    assert summary["policy_evidence_row_count"] == 1
    assert summary["observed_market_count"] == 1
    assert summary["markets_observed"] == ["DAM"]
    assert summary["can_satisfy_v13_explicit_receipts"] is False
    assert summary["source_publication_timestamp_available"] is False
    assert summary["receipt_csv_generated"] is False
    assert summary["validated_receipt_csv_ready"] is False
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False


def test_policy_deadline_evidence_cli_writes_csv_and_summary(tmp_path) -> None:
    from scripts.materialize_oree_policy_publication_deadline_evidence import main

    candidate_audit_json = tmp_path / "candidate_audit.json"
    output_csv = tmp_path / "policy_deadlines.csv"
    summary_json = tmp_path / "policy_deadlines_summary.json"
    candidate_audit_json.write_text(
        json.dumps(
            {
                "requested_month": "05.2026",
                "requested_delivery_date": "2026-05-25",
                "artifact_rows": [
                    _artifact_row(
                        artifact_kind="pxs_hdata",
                        market="DAM",
                        delivery_date="2026-05-25",
                        retrieved_at="2026-05-31T20:43:41+00:00",
                        verdict="observation_only",
                    )
                ],
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--candidate-audit-json",
            str(candidate_audit_json),
            "--output-csv",
            str(output_csv),
            "--summary-json",
            str(summary_json),
            "--generated-at",
            "2026-06-01T09:00:00+00:00",
        ]
    )

    assert exit_code == 0
    summary = json.loads(summary_json.read_text(encoding="utf-8"))
    csv_text = output_csv.read_text(encoding="utf-8")
    assert summary["policy_evidence_row_count"] == 1
    assert summary["can_satisfy_v13_explicit_receipts"] is False
    assert "policy_publication_deadline_kyiv" in csv_text
    assert "2026-05-24T14:00:00" in csv_text
    assert "source_publication_timestamp" not in csv_text
