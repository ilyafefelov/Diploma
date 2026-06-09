from __future__ import annotations

from io import BytesIO
import json
from zipfile import ZipFile

import pytest

from smart_arbitrage.dfl.scmo_dam_receipt_fetch import (
    ScmoExportResponse,
    fetch_result_from_scmo_export_response,
)


def test_scmo_export_fetch_rejects_login_redirect() -> None:
    with pytest.raises(ValueError, match="auth_required_sso_login"):
        fetch_result_from_scmo_export_response(
            ScmoExportResponse(
                source_url="https://scmo.oree.com.ua/export",
                final_url="https://login-scmo.oree.com.ua/login?signin=abc",
                status_code=200,
                content_type="text/html; charset=utf-8",
                body=b"<title>Single Sign On - XMtrade|PXS - Login to the system</title>",
            )
        )


def test_scmo_export_fetch_accepts_csv_with_publication_timestamp(tmp_path) -> None:
    result = fetch_result_from_scmo_export_response(
        ScmoExportResponse(
            source_url="https://scmo.oree.com.ua/export",
            final_url="https://scmo.oree.com.ua/export",
            status_code=200,
            content_type="text/csv; charset=utf-8",
            body=(
                "delivery_hour,published_at\n"
                "2026-01-01T00:00:00,2025-12-31T14:00:00\n"
            ).encode("utf-8"),
        ),
        raw_output_path=tmp_path / "scmo_export.csv",
        normalized_output_path=tmp_path / "dam_receipts_v13.csv",
        timestamp_column="delivery_hour",
        source_publication_timestamp_column="published_at",
    )

    assert result["claim_scope"] == "scmo_dam_receipt_export_fetch_not_market_execution"
    assert result["raw_export_written"] is True
    assert result["raw_export_path"].endswith("scmo_export.csv")
    assert result["normalized_receipts_written"] is True
    assert result["validated_receipt_csv_ready"] is True
    assert result["receipt_rows"] == 1
    assert result["market_execution_enabled"] is False
    assert (tmp_path / "scmo_export.csv").exists()
    assert (tmp_path / "dam_receipts_v13.csv").exists()


def test_scmo_export_fetch_infers_common_csv_columns(tmp_path) -> None:
    result = fetch_result_from_scmo_export_response(
        ScmoExportResponse(
            source_url="https://scmo.oree.com.ua/export",
            final_url="https://scmo.oree.com.ua/export",
            status_code=200,
            content_type="text/csv; charset=utf-8",
            body=(
                "delivery_hour,published_at\n"
                "2026-01-01T00:00:00,2025-12-31T14:00:00\n"
            ).encode("utf-8"),
        ),
        raw_output_path=tmp_path / "scmo_export.csv",
        normalized_output_path=tmp_path / "dam_receipts_v13.csv",
    )

    assert result["validated_receipt_csv_ready"] is True
    assert result["receipt_rows"] == 1
    assert result["market_execution_enabled"] is False


def test_scmo_export_fetch_accepts_zip_container_with_publication_timestamp(
    tmp_path,
) -> None:
    archive_body = _zip_bytes(
        "published_information_of_dam.csv",
        "delivery_hour,published_at\n"
        "2026-01-01T00:00:00,2025-12-31T14:00:00\n",
    )

    result = fetch_result_from_scmo_export_response(
        ScmoExportResponse(
            source_url="https://scmo.oree.com.ua/export.zip",
            final_url="https://scmo.oree.com.ua/export.zip",
            status_code=200,
            content_type="application/zip",
            body=archive_body,
        ),
        raw_output_path=tmp_path / "scmo_export.zip",
        normalized_output_path=tmp_path / "dam_receipts_v13.csv",
    )

    assert result["raw_export_path"].endswith("scmo_export.zip")
    assert result["validated_receipt_csv_ready"] is True
    assert result["receipt_rows"] == 1
    assert result["market_execution_enabled"] is False


def test_scmo_export_fetch_accepts_xml_with_publication_timestamp(tmp_path) -> None:
    result = fetch_result_from_scmo_export_response(
        ScmoExportResponse(
            source_url="https://scmo.oree.com.ua/export.xml",
            final_url="https://scmo.oree.com.ua/export.xml",
            status_code=200,
            content_type="application/xml",
            body=(
                b'<?xml version="1.0" encoding="UTF-8"?>'
                b"<export><row>"
                b"<delivery_hour>2026-01-01T00:00:00</delivery_hour>"
                b"<published_at>2025-12-31T14:00:00</published_at>"
                b"</row></export>"
            ),
        ),
        raw_output_path=tmp_path / "scmo_export.xml",
        normalized_output_path=tmp_path / "dam_receipts_v13.csv",
        timestamp_column="delivery_hour",
        source_publication_timestamp_column="published_at",
    )

    assert result["normalized_receipts_written"] is True
    assert result["validated_receipt_csv_ready"] is True
    assert result["receipt_rows"] == 1
    assert result["market_execution_enabled"] is False


def test_scmo_export_fetch_accepts_html_table_with_publication_timestamp(
    tmp_path,
) -> None:
    result = fetch_result_from_scmo_export_response(
        ScmoExportResponse(
            source_url="https://scmo.oree.com.ua/export.html",
            final_url="https://scmo.oree.com.ua/export.html",
            status_code=200,
            content_type="text/html; charset=utf-8",
            body=(
                b"<!doctype html><html><body>"
                b"<table>"
                b"<tr><th>delivery_hour</th><th>published_at</th></tr>"
                b"<tr>"
                b"<td>2026-01-01T00:00:00</td>"
                b"<td>2025-12-31T14:00:00</td>"
                b"</tr>"
                b"</table>"
                b"</body></html>"
            ),
        ),
        raw_output_path=tmp_path / "scmo_export.html",
        normalized_output_path=tmp_path / "dam_receipts_v13.csv",
    )

    assert result["normalized_receipts_written"] is True
    assert result["validated_receipt_csv_ready"] is True
    assert result["receipt_rows"] == 1
    assert result["market_execution_enabled"] is False


def test_scmo_export_fetch_cli_uses_cookie_env_and_writes_summary(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts.fetch_scmo_dam_publication_receipt_export import main
    import scripts.fetch_scmo_dam_publication_receipt_export as cli

    raw_output = tmp_path / "scmo_export.zip"
    normalized_output = tmp_path / "dam_receipts_v13.csv"
    summary_output = tmp_path / "fetch_summary.json"

    monkeypatch.setenv("SCMO_COOKIE", "session=test-cookie")

    def fake_fetch(url: str, *, cookie_header: str, extra_headers: dict[str, str]):
        assert url == "https://scmo.oree.com.ua/export"
        assert cookie_header == "session=test-cookie"
        assert extra_headers == {"X-Test": "1"}
        return cli.ScmoExportResponse(
            source_url=url,
            final_url=url,
            status_code=200,
            content_type="application/zip",
            body=_zip_bytes(
                "published_information_of_dam.csv",
                "delivery_hour,published_at\n"
                "2026-01-01T00:00:00,2025-12-31T14:00:00\n",
            ),
        )

    monkeypatch.setattr(cli, "_fetch", fake_fetch)

    exit_code = main(
        [
            "--url",
            "https://scmo.oree.com.ua/export",
            "--cookie-env-var",
            "SCMO_COOKIE",
            "--header",
            "X-Test: 1",
            "--raw-output",
            str(raw_output),
            "--normalized-output",
            str(normalized_output),
            "--summary-json",
            str(summary_output),
            "--input-format",
            "zip",
            "--timestamp-column",
            "delivery_hour",
            "--source-publication-timestamp-column",
            "published_at",
        ]
    )

    assert exit_code == 0
    summary = json.loads(summary_output.read_text(encoding="utf-8"))
    assert summary["validated_receipt_csv_ready"] is True
    assert summary["market_execution_enabled"] is False


def test_scmo_export_fetch_cli_can_write_v13_input_config_and_preflight(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts.fetch_scmo_dam_publication_receipt_export import main
    import scripts.fetch_scmo_dam_publication_receipt_export as cli

    raw_output = tmp_path / "scmo_export.zip"
    normalized_output = tmp_path / "dam_receipts_v13.csv"
    summary_output = tmp_path / "fetch_summary.json"
    safe_switch_path = tmp_path / "safe_switch_v13.csv"
    base_config_path = tmp_path / "base_v13.yaml"
    output_config_path = tmp_path / "v13_inputs.yaml"
    preflight_path = tmp_path / "v13_preflight.json"

    safe_switch_path.write_text(
        (
            "tenant_id,source_model_name,anchor_timestamp,split_name,"
            "source_evidence_timestamp,label_v13_material_safe_switch,"
            "label_v13_tail_risk_loss\n"
            "client_004_kharkiv_hospital,nbeatsx_silver_v0,"
            "2026-01-02T00:00:00,train_selection,2025-12-31T14:00:00,"
            "true,false\n"
        ),
        encoding="utf-8",
    )
    base_config_path.write_text(
        (
            "ops:\n"
            "  dfl_ua_dam_publication_receipts_overlay_frame:\n"
            "    config:\n"
            "      oree_dam_publication_receipts_csv_path: \"\"\n"
            "  dfl_ua_context_safe_switch_examples_v13_frame:\n"
            "    config:\n"
            "      ua_context_safe_switch_examples_csv_path: \"\"\n"
        ),
        encoding="utf-8",
    )

    def fake_fetch(url: str, *, cookie_header: str, extra_headers: dict[str, str]):
        assert url == "https://scmo.oree.com.ua/export.zip"
        return cli.ScmoExportResponse(
            source_url=url,
            final_url=url,
            status_code=200,
            content_type="application/zip",
            body=_zip_bytes(
                "published_information_of_dam.csv",
                "delivery_hour,published_at\n"
                "2026-01-01T00:00:00,2025-12-31T14:00:00\n",
            ),
        )

    monkeypatch.setattr(cli, "_fetch", fake_fetch)

    exit_code = main(
        [
            "--url",
            "https://scmo.oree.com.ua/export.zip",
            "--raw-output",
            str(raw_output),
            "--normalized-output",
            str(normalized_output),
            "--summary-json",
            str(summary_output),
            "--input-format",
            "auto",
            "--v13-base-config",
            str(base_config_path),
            "--v13-safe-switch-csv",
            str(safe_switch_path),
            "--v13-output-config",
            str(output_config_path),
            "--v13-preflight-output",
            str(preflight_path),
        ]
    )

    assert exit_code == 0
    summary = json.loads(summary_output.read_text(encoding="utf-8"))
    assert output_config_path.exists()
    assert preflight_path.exists()
    assert summary["v13_input_config_summary"]["input_config_validated"] is True
    assert summary["v13_input_config_summary"]["data_acquisition_needed"] is False
    assert summary["v13_input_config_summary"]["full_v13_gate_evaluated"] is False
    assert summary["v13_input_config_summary"]["market_execution_enabled"] is False


def _zip_bytes(filename: str, content: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr(filename, content)
    return buffer.getvalue()
