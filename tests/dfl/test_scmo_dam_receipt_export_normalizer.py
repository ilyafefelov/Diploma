from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import polars as pl
import pytest

from smart_arbitrage.dfl.scmo_dam_receipt_export import (
    read_scmo_dam_receipt_export_bytes,
    normalize_scmo_dam_publication_receipt_export_frame,
)


def test_scmo_receipt_export_normalizer_maps_authenticated_export_columns() -> None:
    normalized = normalize_scmo_dam_publication_receipt_export_frame(
        pl.DataFrame(
            [
                {
                    "delivery_hour": "2026-01-01T00:00:00",
                    "published_at": "2025-12-31T14:00:00",
                    "source_export_row_id": "scmo-row-001",
                }
            ]
        ),
        timestamp_column="delivery_hour",
        source_publication_timestamp_column="published_at",
        receipt_id_column="source_export_row_id",
    )

    assert normalized.columns == [
        "timestamp",
        "source_publication_timestamp",
        "source_url",
        "source_title",
        "receipt_id",
        "market_execution_enabled",
    ]
    assert normalized["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").to_list() == [
        "2026-01-01T00:00:00"
    ]
    assert normalized["source_publication_timestamp"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    ).to_list() == ["2025-12-31T14:00:00"]
    assert normalized["source_url"].to_list() == ["https://scmo.oree.com.ua/"]
    assert normalized["receipt_id"].to_list() == ["scmo-row-001"]
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_scmo_receipt_export_normalizer_infers_common_export_columns() -> None:
    normalized = normalize_scmo_dam_publication_receipt_export_frame(
        pl.DataFrame(
            [
                {
                    "Delivery Hour": "2026-01-01T00:00:00",
                    "Published At": "2025-12-31T14:00:00",
                }
            ]
        )
    )

    assert normalized["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").to_list() == [
        "2026-01-01T00:00:00"
    ]
    assert normalized["source_publication_timestamp"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    ).to_list() == ["2025-12-31T14:00:00"]
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_scmo_receipt_export_normalizer_rejects_ambiguous_publication_columns() -> None:
    with pytest.raises(ValueError, match="ambiguous SCMO source publication"):
        normalize_scmo_dam_publication_receipt_export_frame(
            pl.DataFrame(
                [
                    {
                        "delivery_hour": "2026-01-01T00:00:00",
                        "published_at": "2025-12-31T14:00:00",
                        "publication_timestamp": "2025-12-31T14:05:00",
                    }
                ]
            )
        )


def test_scmo_receipt_export_normalizer_rejects_observation_timestamp_substitution() -> None:
    with pytest.raises(ValueError, match="source_observed_at_utc"):
        normalize_scmo_dam_publication_receipt_export_frame(
            pl.DataFrame(
                [
                    {
                        "timestamp": "2026-01-01T00:00:00",
                        "source_observed_at_utc": "2026-05-24T20:30:00+00:00",
                    }
                ]
            ),
            source_publication_timestamp_column="source_observed_at_utc",
        )


def test_scmo_receipt_export_normalizer_rejects_non_prior_publication_time() -> None:
    with pytest.raises(ValueError, match="prior to delivery timestamp"):
        normalize_scmo_dam_publication_receipt_export_frame(
            pl.DataFrame(
                [
                    {
                        "timestamp": "2026-01-01T00:00:00",
                        "source_publication_timestamp": "2026-01-01T00:00:00",
                    }
                ]
            )
        )


def test_scmo_receipt_export_cli_writes_validated_v13_receipts(tmp_path) -> None:
    from scripts.normalize_scmo_dam_publication_receipt_export import main

    input_path = tmp_path / "scmo_export.csv"
    output_path = tmp_path / "dam_receipts_v13.csv"
    pl.DataFrame(
        [
            {
                "delivery_hour": "2026-01-01T00:00:00",
                "published_at": "2025-12-31T14:00:00",
            }
        ]
    ).write_csv(input_path)

    exit_code = main(
        [
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--timestamp-column",
            "delivery_hour",
            "--source-publication-timestamp-column",
            "published_at",
        ]
    )

    assert exit_code == 0
    normalized = pl.read_csv(output_path, try_parse_dates=True)
    assert normalized.height == 1
    assert normalized["market_execution_enabled"].to_list() == [False]
    summary = json.loads((tmp_path / "dam_receipts_v13.summary.json").read_text())
    assert summary["validated_receipt_csv_ready"] is True
    assert summary["market_execution_enabled"] is False


def test_scmo_receipt_export_cli_can_write_v13_input_config_and_preflight(
    tmp_path: Path,
) -> None:
    from scripts.normalize_scmo_dam_publication_receipt_export import main

    input_path = tmp_path / "scmo_export.csv"
    receipt_output_path = tmp_path / "dam_receipts_v13.csv"
    safe_switch_path = tmp_path / "safe_switch_v13.csv"
    base_config_path = tmp_path / "base_v13.yaml"
    output_config_path = tmp_path / "v13_inputs.yaml"
    preflight_path = tmp_path / "v13_preflight.json"
    pl.DataFrame(
        [
            {
                "delivery_hour": "2026-01-01T00:00:00",
                "published_at": "2025-12-31T14:00:00",
            }
        ]
    ).write_csv(input_path)
    pl.DataFrame(
        [
            {
                "tenant_id": "client_004_kharkiv_hospital",
                "source_model_name": "nbeatsx_silver_v0",
                "anchor_timestamp": "2026-01-02T00:00:00",
                "split_name": "train_selection",
                "source_evidence_timestamp": "2025-12-31T14:00:00",
                "label_v13_material_safe_switch": True,
                "label_v13_tail_risk_loss": False,
            }
        ]
    ).write_csv(safe_switch_path)
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

    exit_code = main(
        [
            "--input",
            str(input_path),
            "--output",
            str(receipt_output_path),
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
    summary = json.loads(
        receipt_output_path.with_suffix(".summary.json").read_text(encoding="utf-8")
    )
    assert output_config_path.exists()
    assert preflight_path.exists()
    assert summary["v13_input_config_summary"]["input_config_validated"] is True
    assert summary["v13_input_config_summary"]["data_acquisition_needed"] is False
    assert summary["v13_input_config_summary"]["full_v13_gate_evaluated"] is False
    assert summary["v13_input_config_summary"]["market_execution_enabled"] is False
    assert summary["v13_input_config_summary"]["preflight_summary"][
        "missing_required_inputs"
    ] == []


def test_scmo_receipt_export_cli_accepts_xml_input_format(tmp_path) -> None:
    from scripts.normalize_scmo_dam_publication_receipt_export import main

    input_path = tmp_path / "scmo_export.xml"
    output_path = tmp_path / "dam_receipts_v13.csv"
    input_path.write_text(
        (
            '<?xml version="1.0" encoding="UTF-8"?>'
            "<export><row>"
            "<delivery_hour>2026-01-01T00:00:00</delivery_hour>"
            "<published_at>2025-12-31T14:00:00</published_at>"
            "</row></export>"
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--input",
            str(input_path),
            "--input-format",
            "xml",
            "--output",
            str(output_path),
            "--timestamp-column",
            "delivery_hour",
            "--source-publication-timestamp-column",
            "published_at",
        ]
    )

    assert exit_code == 0
    normalized = pl.read_csv(output_path, try_parse_dates=True)
    assert normalized.height == 1
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_scmo_receipt_export_cli_accepts_html_input_format(tmp_path) -> None:
    from scripts.normalize_scmo_dam_publication_receipt_export import main

    input_path = tmp_path / "scmo_export.html"
    output_path = tmp_path / "dam_receipts_v13.csv"
    input_path.write_text(
        (
            "<!doctype html><html><body>"
            "<table>"
            "<tr><th>delivery_hour</th><th>published_at</th></tr>"
            "<tr>"
            "<td>2026-01-01T00:00:00</td>"
            "<td>2025-12-31T14:00:00</td>"
            "</tr>"
            "</table>"
            "</body></html>"
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--input",
            str(input_path),
            "--input-format",
            "html",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    normalized = pl.read_csv(output_path, try_parse_dates=True)
    assert normalized.height == 1
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_scmo_receipt_export_cli_infers_common_export_columns(tmp_path) -> None:
    from scripts.normalize_scmo_dam_publication_receipt_export import main

    input_path = tmp_path / "scmo_export.csv"
    output_path = tmp_path / "dam_receipts_v13.csv"
    pl.DataFrame(
        [
            {
                "delivery_hour": "2026-01-01T00:00:00",
                "published_at": "2025-12-31T14:00:00",
            }
        ]
    ).write_csv(input_path)

    exit_code = main(
        [
            "--input",
            str(input_path),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    normalized = pl.read_csv(output_path, try_parse_dates=True)
    assert normalized.height == 1
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_scmo_receipt_export_reader_accepts_xml_rows() -> None:
    raw = read_scmo_dam_receipt_export_bytes(
        (
            b'<?xml version="1.0" encoding="UTF-8"?>'
            b"<export><row>"
            b"<delivery_hour>2026-01-01T00:00:00</delivery_hour>"
            b"<published_at>2025-12-31T14:00:00</published_at>"
            b"</row></export>"
        ),
        input_format="xml",
    )

    normalized = normalize_scmo_dam_publication_receipt_export_frame(
        raw,
        timestamp_column="delivery_hour",
        source_publication_timestamp_column="published_at",
    )

    assert normalized.height == 1
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_scmo_receipt_export_reader_accepts_html_table_rows() -> None:
    raw = read_scmo_dam_receipt_export_bytes(
        (
            b"<!doctype html><html><body>"
            b"<table>"
            b"<thead><tr><th>delivery_hour</th><th>published_at</th></tr></thead>"
            b"<tbody><tr>"
            b"<td>2026-01-01T00:00:00</td>"
            b"<td>2025-12-31T14:00:00</td>"
            b"</tr></tbody>"
            b"</table>"
            b"</body></html>"
        ),
        input_format="html",
    )

    normalized = normalize_scmo_dam_publication_receipt_export_frame(raw)

    assert normalized.height == 1
    assert normalized["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").to_list() == [
        "2026-01-01T00:00:00"
    ]
    assert normalized["source_publication_timestamp"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    ).to_list() == ["2025-12-31T14:00:00"]
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_scmo_receipt_export_reader_accepts_isotedata_soap_response() -> None:
    raw = read_scmo_dam_receipt_export_bytes(
        (
            b'<?xml version="1.0" encoding="UTF-8"?>'
            b'<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">'
            b"<s:Body>"
            b'<DownloadResponse xmlns="http://sfera.sk/ws/xmtrade/isot/interfaces/evaluations/services/2009/04/01">'
            b'<ISOTEDATA xmlns="http://sfera.sk/ws/xmtrade/isot/interfaces/evaluations/types/2009/04/01" '
            b'message-code="943" date-time="2025-12-31T13:27:00">'
            b'<Trade trade-day="2026-01-01" market-area="UA_IPS">'
            b'<ProfileData profile-role="SP02">'
            b'<Data period="1" value="1000.00" unit="UAH" />'
            b'<Data period="2" value="1100.00" unit="UAH" />'
            b"</ProfileData>"
            b'<ProfileData profile-role="SC02">'
            b'<Data period="1" value="20.0" unit="MWH" />'
            b'<Data period="2" value="21.0" unit="MWH" />'
            b"</ProfileData>"
            b"</Trade>"
            b"</ISOTEDATA>"
            b"</DownloadResponse>"
            b"</s:Body>"
            b"</s:Envelope>"
        ),
        input_format="xml",
    )

    normalized = normalize_scmo_dam_publication_receipt_export_frame(raw)

    assert normalized["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").to_list() == [
        "2026-01-01T00:00:00",
        "2026-01-01T01:00:00",
    ]
    assert normalized["source_publication_timestamp"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    ).to_list() == ["2025-12-31T13:27:00", "2025-12-31T13:27:00"]
    assert normalized["receipt_id"].to_list() == [
        "scmo-isotedata:943:2026-01-01:UA_IPS:1",
        "scmo-isotedata:943:2026-01-01:UA_IPS:2",
    ]
    assert normalized["market_execution_enabled"].to_list() == [False, False]


def test_scmo_receipt_export_reader_accepts_minimal_xlsx_rows(tmp_path) -> None:
    workbook_path = tmp_path / "scmo_export.xlsx"
    _write_minimal_xlsx(
        workbook_path,
        rows=[
            ["delivery_hour", "published_at"],
            ["2026-01-01T00:00:00", "2025-12-31T14:00:00"],
        ],
    )

    raw = read_scmo_dam_receipt_export_bytes(
        workbook_path.read_bytes(),
        input_format="xlsx",
    )
    normalized = normalize_scmo_dam_publication_receipt_export_frame(
        raw,
        timestamp_column="delivery_hour",
        source_publication_timestamp_column="published_at",
    )

    assert normalized.height == 1
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_scmo_receipt_export_reader_accepts_xlsx_excel_serial_timestamps(
    tmp_path,
) -> None:
    workbook_path = tmp_path / "scmo_export_serial_dates.xlsx"
    _write_minimal_xlsx(
        workbook_path,
        rows=[
            ["delivery_hour", "published_at"],
            [
                _excel_serial(datetime(2026, 1, 1, 0, 0, 0)),
                _excel_serial(datetime(2025, 12, 31, 14, 0, 0)),
            ],
        ],
    )

    raw = read_scmo_dam_receipt_export_bytes(
        workbook_path.read_bytes(),
        input_format="xlsx",
    )
    normalized = normalize_scmo_dam_publication_receipt_export_frame(raw)

    assert normalized["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").to_list() == [
        "2026-01-01T00:00:00"
    ]
    assert normalized["source_publication_timestamp"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    ).to_list() == ["2025-12-31T14:00:00"]
    assert normalized["market_execution_enabled"].to_list() == [False]


def test_scmo_receipt_export_reader_accepts_zip_container_with_single_csv(
    tmp_path,
) -> None:
    archive_path = tmp_path / "scmo_export.zip"
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(
            "published_information_of_dam.csv",
            "delivery_hour,published_at\n"
            "2026-01-01T00:00:00,2025-12-31T14:00:00\n",
        )

    raw = read_scmo_dam_receipt_export_bytes(
        archive_path.read_bytes(),
        input_format="auto",
        source_name=archive_path.name,
    )
    normalized = normalize_scmo_dam_publication_receipt_export_frame(raw)

    assert normalized.height == 1
    assert normalized["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").to_list() == [
        "2026-01-01T00:00:00"
    ]
    assert normalized["source_publication_timestamp"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    ).to_list() == ["2025-12-31T14:00:00"]
    assert normalized["market_execution_enabled"].to_list() == [False]


def _write_minimal_xlsx(path: Path, *, rows: list[list[str | int | float]]) -> None:
    shared_strings = [str(cell) for row in rows for cell in row]
    sheet_rows: list[str] = []
    shared_string_index = 0
    for row_index, row in enumerate(rows, start=1):
        cells: list[str] = []
        for column_index, cell in enumerate(row, start=1):
            cell_ref = f"{_xlsx_column_name(column_index)}{row_index}"
            if isinstance(cell, int | float) and not isinstance(cell, bool):
                cells.append(f'<c r="{cell_ref}"><v>{cell}</v></c>')
            else:
                cells.append(
                    f'<c r="{cell_ref}" t="s"><v>{shared_string_index}</v></c>'
                )
                shared_string_index += 1
        sheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    with ZipFile(path, "w", compression=ZIP_DEFLATED) as workbook:
        workbook.writestr(
            "[Content_Types].xml",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                '<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
                "</Types>"
            ),
        )
        workbook.writestr(
            "_rels/.rels",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
                "</Relationships>"
            ),
        )
        workbook.writestr(
            "xl/workbook.xml",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                "<sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets>"
                "</workbook>"
            ),
        )
        workbook.writestr(
            "xl/_rels/workbook.xml.rels",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
                '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>'
                "</Relationships>"
            ),
        )
        workbook.writestr(
            "xl/sharedStrings.xml",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                + "".join(f"<si><t>{value}</t></si>" for value in shared_strings)
                + "</sst>"
            ),
        )
        workbook.writestr(
            "xl/worksheets/sheet1.xml",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                f'<sheetData>{"".join(sheet_rows)}</sheetData></worksheet>'
            ),
        )


def _xlsx_column_name(column_index: int) -> str:
    name = ""
    while column_index:
        column_index, remainder = divmod(column_index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _excel_serial(value: datetime) -> float:
    return (value - datetime(1899, 12, 30)).total_seconds() / 86400.0
