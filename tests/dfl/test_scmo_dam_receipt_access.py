from __future__ import annotations

from datetime import datetime, timezone
import json

from smart_arbitrage.dfl.scmo_dam_receipt_access import (
    build_scmo_dam_publication_receipt_access_probe,
)


def test_scmo_probe_classifies_login_redirect_as_auth_blocked_lead() -> None:
    probe = build_scmo_dam_publication_receipt_access_probe(
        source_url="https://scmo.oree.com.ua/",
        final_url="https://login-scmo.oree.com.ua/login?signin=abc",
        status_code=200,
        content_type="text/html; charset=utf-8",
        response_text="<title>Single Sign On - XMtrade|PXS - Login to the system</title>",
        retrieved_at=datetime(2026, 5, 24, 21, 0, tzinfo=timezone.utc),
    )

    assert probe["claim_scope"] == "scmo_dam_receipt_access_probe_not_receipt"
    assert probe["auth_required"] is True
    assert probe["source_probe_status"] == "auth_required_sso_login"
    assert probe["candidate_receipt_source_found"] is False
    assert probe["receipt_csv_generated"] is False
    assert probe["validated_receipt_csv_ready"] is False
    assert probe["market_execution_enabled"] is False
    assert probe["lead_row"] == {
        "lead_id": "scmo_published_information_of_dam_export",
        "source_url": "https://scmo.oree.com.ua/",
        "source_title": "SCMO XMtrade/PXS Published information of DAM export",
        "lead_kind": "official_credentialed_portal",
        "metadata_scope": "row_level",
        "has_timestamp_column": True,
        "has_source_publication_timestamp_column": False,
        "download_auth_required": True,
        "source_probe_status": "auth_required_sso_login",
        "market_execution_enabled": False,
    }


def test_scmo_probe_surfaces_candidate_when_public_response_has_receipt_columns() -> None:
    probe = build_scmo_dam_publication_receipt_access_probe(
        source_url="https://scmo.oree.com.ua/export",
        final_url="https://scmo.oree.com.ua/export",
        status_code=200,
        content_type="text/csv",
        response_text="timestamp,source_publication_timestamp,price\n2026-01-01T00:00:00,2025-12-31T14:00:00,1",
        retrieved_at=datetime(2026, 5, 24, 21, 0, tzinfo=timezone.utc),
    )

    assert probe["auth_required"] is False
    assert probe["source_probe_status"] == "candidate_receipt_metadata_present"
    assert probe["candidate_receipt_source_found"] is True
    assert probe["lead_row"]["has_source_publication_timestamp_column"] is True
    assert probe["lead_row"]["download_auth_required"] is False
    assert probe["market_execution_enabled"] is False


def test_scmo_probe_cli_writes_probe_and_lead(tmp_path) -> None:
    from scripts.probe_scmo_dam_publication_receipt_access import main

    probe_json = tmp_path / "probe.json"
    lead_csv = tmp_path / "lead.csv"

    exit_code = main(
        [
            "--url",
            "https://scmo.oree.com.ua/",
            "--response-text-override",
            "<title>Login to the system</title>",
            "--final-url-override",
            "https://login-scmo.oree.com.ua/login?signin=test",
            "--probe-output-json",
            str(probe_json),
            "--lead-output-csv",
            str(lead_csv),
        ]
    )

    assert exit_code == 0
    probe = json.loads(probe_json.read_text(encoding="utf-8"))
    assert probe["auth_required"] is True
    assert "scmo_published_information_of_dam_export" in lead_csv.read_text(
        encoding="utf-8"
    )
