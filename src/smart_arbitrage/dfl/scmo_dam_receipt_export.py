"""Normalize authenticated SCMO DAM publication exports into V13 receipts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
import re
from typing import Final, Literal
from zipfile import ZipFile
import xml.etree.ElementTree as ET

import polars as pl

from smart_arbitrage.dfl.ua_context_acquisition_v1 import (
    normalize_dfl_ua_dam_publication_receipts_frame,
)

SCMO_DAM_RECEIPT_EXPORT_CLAIM_SCOPE: Final[str] = (
    "scmo_dam_receipt_export_normalizer_not_market_execution"
)
DEFAULT_SCMO_SOURCE_URL: Final[str] = "https://scmo.oree.com.ua/"
DEFAULT_SCMO_SOURCE_TITLE: Final[str] = (
    "SCMO XMtrade/PXS Published information of DAM export"
)
_FORBIDDEN_PUBLICATION_TIMESTAMP_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_observed_at_utc",
        "download_http_date_utc",
        "hdata_http_date_utc",
        "retrieved_at",
        "downloaded_at",
    }
)
ScmoReceiptExportFormat = Literal["auto", "csv", "xml", "xlsx", "zip", "html"]
SCMO_AUTO_COLUMN: Final[str] = "auto"
_XLSX_COLUMN_RE: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)")


def _column_key(column_name: str) -> str:
    return re.sub(r"[^\w]+", "_", column_name.casefold()).strip("_")


_TIMESTAMP_COLUMN_ALIASES: Final[frozenset[str]] = frozenset(
    {
        "timestamp",
        "delivery_hour",
        "delivery_timestamp",
        "delivery_datetime",
        "delivery_date_time",
        "period_start",
        "hour_start",
        "date_time",
        "datetime",
        "дата_та_час_поставки",
        "дата_час_поставки",
        "дата_поставки",
        "час_поставки",
    }
)
_SOURCE_PUBLICATION_TIMESTAMP_ALIASES: Final[frozenset[str]] = frozenset(
    {
        "source_publication_timestamp",
        "published_at",
        "publication_timestamp",
        "publication_datetime",
        "publication_date_time",
        "publication_time",
        "publish_time",
        "published_datetime",
        "published_date_time",
        "published_on",
        "date_published",
        "date_publication",
        "source_published_at",
        "source_published_datetime",
        "дата_публікації",
        "час_публікації",
        "дата_час_публікації",
        "дата_публикации",
        "время_публикации",
        "дата_время_публикации",
    }
)
_FORBIDDEN_PUBLICATION_TIMESTAMP_KEYS: Final[frozenset[str]] = frozenset(
    _column_key(column_name)
    for column_name in _FORBIDDEN_PUBLICATION_TIMESTAMP_COLUMNS
)
_PERIOD_NUMBER_RE: Final[re.Pattern[str]] = re.compile(r"\d+")
_EXCEL_SERIAL_DATE_EPOCH: Final[datetime] = datetime(1899, 12, 30)
_MIN_EXCEL_SERIAL_DATE: Final[float] = 20000.0
_MAX_EXCEL_SERIAL_DATE: Final[float] = 80000.0


def normalize_scmo_dam_publication_receipt_export_frame(
    export_frame: pl.DataFrame,
    *,
    timestamp_column: str = SCMO_AUTO_COLUMN,
    source_publication_timestamp_column: str = SCMO_AUTO_COLUMN,
    receipt_id_column: str | None = None,
    source_url: str = DEFAULT_SCMO_SOURCE_URL,
    source_title: str = DEFAULT_SCMO_SOURCE_TITLE,
) -> pl.DataFrame:
    """Validate a credentialed/manual SCMO export as explicit V13 receipt rows."""

    resolved_timestamp_column = _resolve_timestamp_column(
        export_frame,
        timestamp_column,
    )
    resolved_source_publication_timestamp_column = (
        _resolve_source_publication_timestamp_column(
            export_frame,
            source_publication_timestamp_column,
        )
    )
    resolved_receipt_id_column = _resolve_receipt_id_column(
        export_frame,
        receipt_id_column,
    )
    _validate_mapping_columns(
        export_frame,
        timestamp_column=resolved_timestamp_column,
        source_publication_timestamp_column=(
            resolved_source_publication_timestamp_column
        ),
        receipt_id_column=resolved_receipt_id_column,
    )
    _refuse_forbidden_publication_column(
        resolved_source_publication_timestamp_column
    )
    timestamp_expr = _timestamp_value_expr(resolved_timestamp_column)
    source_publication_timestamp_expr = _timestamp_value_expr(
        resolved_source_publication_timestamp_column
    )
    mapped = export_frame.select(
        [
            timestamp_expr.alias("timestamp"),
            source_publication_timestamp_expr.alias("source_publication_timestamp"),
            pl.lit(source_url).alias("source_url"),
            pl.lit(source_title).alias("source_title"),
            _receipt_id_expr(
                timestamp_column=resolved_timestamp_column,
                source_publication_timestamp_column=(
                    resolved_source_publication_timestamp_column
                ),
                receipt_id_column=resolved_receipt_id_column,
            ).alias("receipt_id"),
            pl.lit(False).alias("market_execution_enabled"),
        ]
    )
    normalized = normalize_dfl_ua_dam_publication_receipts_frame(mapped)
    _validate_prior_publication(normalized)
    return normalized


def read_scmo_dam_receipt_export_path(
    input_path: str | Path,
    *,
    input_format: ScmoReceiptExportFormat = "auto",
) -> pl.DataFrame:
    """Read a manual/authenticated SCMO export before V13 receipt normalization."""

    path = Path(input_path)
    return read_scmo_dam_receipt_export_bytes(
        path.read_bytes(),
        input_format=input_format,
        source_name=path.name,
    )


def read_scmo_dam_receipt_export_bytes(
    body: bytes,
    *,
    input_format: ScmoReceiptExportFormat = "auto",
    content_type: str = "",
    source_name: str = "",
) -> pl.DataFrame:
    """Read SCMO CSV/XML/XLSX/HTML export bytes into a raw Polars frame."""

    resolved_format = _resolve_input_format(
        body,
        input_format=input_format,
        content_type=content_type,
        source_name=source_name,
    )
    if resolved_format == "csv":
        return pl.read_csv(BytesIO(body), try_parse_dates=True)
    if resolved_format == "xml":
        return _read_xml_export_bytes(body)
    if resolved_format == "xlsx":
        return _read_xlsx_export_bytes(body)
    if resolved_format == "zip":
        return _read_zip_export_bytes(body)
    if resolved_format == "html":
        return _read_html_export_bytes(body)
    raise ValueError(f"Unsupported SCMO receipt export format: {input_format}")


def _validate_mapping_columns(
    frame: pl.DataFrame,
    *,
    timestamp_column: str,
    source_publication_timestamp_column: str,
    receipt_id_column: str | None,
) -> None:
    required = [timestamp_column, source_publication_timestamp_column]
    if receipt_id_column:
        required.append(receipt_id_column)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"SCMO receipt export missing required columns: {missing}")
    if "market_execution_enabled" in frame.columns and frame.filter(
        pl.col("market_execution_enabled")
    ).height:
        raise ValueError("SCMO receipt export contains market execution rows.")


def _resolve_timestamp_column(frame: pl.DataFrame, requested_column: str) -> str:
    if requested_column != SCMO_AUTO_COLUMN:
        return _resolve_explicit_column(
            frame,
            requested_column,
            role="SCMO delivery timestamp",
        )
    return _infer_single_column(
        frame,
        aliases=_TIMESTAMP_COLUMN_ALIASES,
        role="SCMO delivery timestamp",
    )


def _resolve_source_publication_timestamp_column(
    frame: pl.DataFrame,
    requested_column: str,
) -> str:
    if requested_column != SCMO_AUTO_COLUMN:
        resolved = _resolve_explicit_column(
            frame,
            requested_column,
            role="SCMO source publication timestamp",
        )
        _refuse_forbidden_publication_column(resolved)
        return resolved
    matches = [
        column_name
        for column_name in frame.columns
        if _column_key(column_name) in _SOURCE_PUBLICATION_TIMESTAMP_ALIASES
    ]
    if not matches:
        forbidden_matches = [
            column_name
            for column_name in frame.columns
            if _column_key(column_name) in _FORBIDDEN_PUBLICATION_TIMESTAMP_KEYS
        ]
        if forbidden_matches:
            raise ValueError(
                "SCMO receipt export requires an explicit source publication "
                "timestamp column; retrieval/observation columns are not receipt "
                f"evidence: {forbidden_matches}"
            )
        raise ValueError(
            "SCMO receipt export requires an explicit source publication "
            "timestamp column."
        )
    if len(matches) > 1:
        raise ValueError(
            "ambiguous SCMO source publication timestamp columns: "
            f"{sorted(matches)}"
        )
    _refuse_forbidden_publication_column(matches[0])
    return matches[0]


def _resolve_explicit_column(
    frame: pl.DataFrame,
    requested_column: str,
    *,
    role: str,
) -> str:
    if requested_column in frame.columns:
        return requested_column
    requested_key = _column_key(requested_column)
    matches = [
        column_name for column_name in frame.columns if _column_key(column_name) == requested_key
    ]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"ambiguous {role} columns: {sorted(matches)}")
    raise ValueError(f"SCMO receipt export missing required column: {requested_column}")


def _resolve_receipt_id_column(
    frame: pl.DataFrame,
    requested_column: str | None,
) -> str | None:
    if requested_column is not None:
        return _resolve_explicit_column(
            frame,
            requested_column,
            role="SCMO receipt id",
        )
    return "receipt_id" if "receipt_id" in frame.columns else None


def _infer_single_column(
    frame: pl.DataFrame,
    *,
    aliases: frozenset[str],
    role: str,
) -> str:
    matches = [column_name for column_name in frame.columns if _column_key(column_name) in aliases]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"ambiguous {role} columns: {sorted(matches)}")
    raise ValueError(f"SCMO receipt export missing required {role} column.")


def _refuse_forbidden_publication_column(column_name: str) -> None:
    if _column_key(column_name) in _FORBIDDEN_PUBLICATION_TIMESTAMP_KEYS:
        raise ValueError(
            "SCMO receipt export cannot use source_observed_at_utc or retrieval "
            "metadata as source_publication_timestamp."
        )


def _timestamp_value_expr(column_name: str) -> pl.Expr:
    return pl.col(column_name).map_elements(
        _timestamp_text_or_excel_serial,
        return_dtype=pl.Utf8,
    )


def _timestamp_text_or_excel_serial(value: object) -> str | None:
    if value is None:
        return None
    serial = _excel_serial_number(value)
    if serial is not None:
        return (_EXCEL_SERIAL_DATE_EPOCH + timedelta(days=serial)).isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _excel_serial_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        serial = float(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if not re.fullmatch(r"\d+(?:\.\d+)?", stripped):
            return None
        serial = float(stripped)
    else:
        return None
    if _MIN_EXCEL_SERIAL_DATE <= serial <= _MAX_EXCEL_SERIAL_DATE:
        return serial
    return None


def _resolve_input_format(
    body: bytes,
    *,
    input_format: ScmoReceiptExportFormat,
    content_type: str,
    source_name: str,
) -> Literal["csv", "xml", "xlsx", "zip", "html"]:
    if input_format != "auto":
        return input_format

    normalized_content_type = content_type.casefold()
    normalized_source_name = source_name.casefold()
    if "zip" in normalized_content_type:
        return "zip"
    if "spreadsheetml" in normalized_content_type or "excel" in normalized_content_type:
        return "xlsx"
    if "html" in normalized_content_type:
        return "html"
    if "xml" in normalized_content_type:
        return "xml"
    if "csv" in normalized_content_type or "text/plain" in normalized_content_type:
        return "csv"
    if normalized_source_name.endswith(".xlsx"):
        return "xlsx"
    if normalized_source_name.endswith(".xml"):
        return "xml"
    if normalized_source_name.endswith(".csv"):
        return "csv"
    if normalized_source_name.endswith(".zip"):
        return "zip"
    if normalized_source_name.endswith((".html", ".htm")):
        return "html"
    if body.startswith(b"PK\x03\x04"):
        return _zip_payload_format(body)
    stripped = body.lstrip()
    stripped_lower = stripped[:256].lower()
    if stripped_lower.startswith((b"<!doctype html", b"<html", b"<table")):
        return "html"
    if stripped.startswith(b"<?xml") or stripped.startswith(b"<"):
        return "xml"
    return "csv"


def _zip_payload_format(body: bytes) -> Literal["xlsx", "zip"]:
    with ZipFile(BytesIO(body)) as archive:
        names = set(archive.namelist())
    return "xlsx" if "xl/workbook.xml" in names else "zip"


def _read_xml_export_bytes(body: bytes) -> pl.DataFrame:
    root = ET.fromstring(body)
    isotedata_rows = _read_isotedata_receipt_rows(root)
    if isotedata_rows:
        return pl.DataFrame(isotedata_rows)
    rows = [_xml_row_to_dict(element) for element in _xml_row_elements(root)]
    rows = [row for row in rows if row]
    if not rows:
        root_row = _xml_row_to_dict(root)
        if root_row:
            rows = [root_row]
    if not rows:
        raise ValueError("SCMO XML receipt export did not contain tabular rows.")
    return pl.DataFrame(rows)


def _read_isotedata_receipt_rows(root: ET.Element) -> list[dict[str, str]]:
    rows_by_key: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for isotedata in root.iter():
        if _local_name(isotedata.tag) != "ISOTEDATA":
            continue
        publication_timestamp = isotedata.attrib.get("date-time", "").strip()
        message_code = isotedata.attrib.get("message-code", "").strip()
        if not publication_timestamp:
            raise ValueError("SCMO ISOTEDATA response missing date-time attribute.")
        if not message_code:
            raise ValueError("SCMO ISOTEDATA response missing message-code attribute.")
        for trade in _direct_children(isotedata, "Trade"):
            trade_day = trade.attrib.get("trade-day", "").strip()
            market_area = trade.attrib.get("market-area", "").strip()
            if not trade_day:
                raise ValueError("SCMO ISOTEDATA Trade missing trade-day attribute.")
            for period in _trade_periods(trade):
                timestamp = _delivery_timestamp_from_period(
                    trade_day=trade_day,
                    period=period,
                )
                key = (message_code, trade_day, market_area, period)
                rows_by_key[key] = {
                    "delivery_hour": timestamp,
                    "source_publication_timestamp": publication_timestamp,
                    "trade_day": trade_day,
                    "period": period,
                    "market_area": market_area,
                    "message_code": message_code,
                    "receipt_id": (
                        "scmo-isotedata:"
                        f"{message_code}:{trade_day}:{market_area}:{period}"
                    ),
                }
    return [
        rows_by_key[key]
        for key in sorted(
            rows_by_key,
            key=lambda item: (
                item[1],
                item[2],
                _period_start_hour(item[3]),
                item[0],
            ),
        )
    ]


def _direct_children(element: ET.Element, local_name: str) -> list[ET.Element]:
    return [child for child in list(element) if _local_name(child.tag) == local_name]


def _trade_periods(trade: ET.Element) -> list[str]:
    periods: set[str] = set()
    for profile_data in _direct_children(trade, "ProfileData"):
        for data in _direct_children(profile_data, "Data"):
            period = data.attrib.get("period", "").strip()
            if period:
                periods.add(period)
    return sorted(periods, key=_period_start_hour)


def _delivery_timestamp_from_period(*, trade_day: str, period: str) -> str:
    start_hour = _period_start_hour(period)
    return f"{trade_day}T{start_hour:02d}:00:00"


def _period_start_hour(period: str) -> int:
    numbers = [int(value) for value in _PERIOD_NUMBER_RE.findall(period)]
    if not numbers:
        raise ValueError(f"SCMO ISOTEDATA period is not numeric: {period!r}")
    first_number = numbers[0]
    if "-" in period or "\u2013" in period or "\u2014" in period:
        if not 0 <= first_number <= 23:
            raise ValueError(f"SCMO ISOTEDATA period start is out of range: {period!r}")
        return first_number
    if not 1 <= first_number <= 24:
        raise ValueError(f"SCMO ISOTEDATA period is out of range: {period!r}")
    return first_number - 1


def _xml_row_elements(root: ET.Element) -> list[ET.Element]:
    preferred_rows = [
        element
        for element in root.iter()
        if element is not root and _local_name(element.tag) in {"row", "record", "item"}
    ]
    if preferred_rows:
        return preferred_rows

    children = list(root)
    if children and len({_local_name(child.tag) for child in children}) == 1:
        return children
    return [element for element in root.iter() if element is not root]


def _xml_row_to_dict(element: ET.Element) -> dict[str, str]:
    row = {_local_name(name): value for name, value in element.attrib.items()}
    for child in list(element):
        child_text = "".join(child.itertext()).strip()
        if child_text:
            row[_local_name(child.tag)] = child_text
    return row


def _read_xlsx_export_bytes(body: bytes) -> pl.DataFrame:
    with ZipFile(BytesIO(body)) as workbook:
        shared_strings = _read_xlsx_shared_strings(workbook)
        worksheet_name = _first_xlsx_worksheet_name(workbook)
        worksheet_root = ET.fromstring(workbook.read(worksheet_name))

    rows = [
        _read_xlsx_row(row_element, shared_strings=shared_strings)
        for row_element in worksheet_root.iter()
        if _local_name(row_element.tag) == "row"
    ]
    rows = [row for row in rows if row]
    if len(rows) < 2:
        raise ValueError("SCMO XLSX receipt export must contain a header and data rows.")
    header = [str(value) for value in rows[0]]
    data_rows = [
        {header[index]: value for index, value in enumerate(row) if index < len(header)}
        for row in rows[1:]
    ]
    data_rows = [row for row in data_rows if any(str(value).strip() for value in row.values())]
    if not data_rows:
        raise ValueError("SCMO XLSX receipt export did not contain data rows.")
    return pl.DataFrame(data_rows)


def _read_zip_export_bytes(body: bytes) -> pl.DataFrame:
    with ZipFile(BytesIO(body)) as archive:
        candidate_names = [
            name
            for name in sorted(archive.namelist())
            if not name.endswith("/")
            and not name.startswith("__MACOSX/")
            and name.casefold().endswith((".csv", ".xml", ".xlsx", ".html", ".htm"))
        ]
        if not candidate_names:
            raise ValueError(
                "SCMO ZIP receipt export did not contain a CSV, XML, XLSX, or HTML "
                "file."
            )
        if len(candidate_names) > 1:
            raise ValueError(
                "SCMO ZIP receipt export is ambiguous; expected one data file, "
                f"found {candidate_names}."
            )
        member_name = candidate_names[0]
        member_body = archive.read(member_name)
    return read_scmo_dam_receipt_export_bytes(
        member_body,
        input_format="auto",
        source_name=member_name,
    )


def _read_html_export_bytes(body: bytes) -> pl.DataFrame:
    parser = _FirstHtmlTableParser()
    parser.feed(body.decode("utf-8", errors="replace"))
    rows = parser.rows
    if not rows:
        raise ValueError("SCMO HTML receipt export did not contain a table.")
    if len(rows) < 2:
        raise ValueError("SCMO HTML receipt export must contain a header and data rows.")
    header = [value.strip() for value in rows[0]]
    if not any(header):
        raise ValueError("SCMO HTML receipt export table header is empty.")
    data_rows = [
        {header[index]: value for index, value in enumerate(row) if index < len(header)}
        for row in rows[1:]
    ]
    data_rows = [row for row in data_rows if any(value.strip() for value in row.values())]
    if not data_rows:
        raise ValueError("SCMO HTML receipt export did not contain data rows.")
    return pl.DataFrame(data_rows)


class _FirstHtmlTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._table_depth = 0
        self._in_row = False
        self._in_cell = False
        self._captured_first_table = False
        self._current_row: list[str] = []
        self._current_cell_parts: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        del attrs
        if self._captured_first_table:
            return
        normalized_tag = tag.casefold()
        if normalized_tag == "table":
            self._table_depth += 1
        if self._table_depth <= 0:
            return
        if normalized_tag == "tr":
            self._in_row = True
            self._current_row = []
        if self._in_row and normalized_tag in {"td", "th"}:
            self._in_cell = True
            self._current_cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._captured_first_table:
            return
        normalized_tag = tag.casefold()
        if self._table_depth > 0 and self._in_cell and normalized_tag in {"td", "th"}:
            self._current_row.append(" ".join(self._current_cell_parts).strip())
            self._current_cell_parts = []
            self._in_cell = False
            return
        if self._table_depth > 0 and self._in_row and normalized_tag == "tr":
            if any(value.strip() for value in self._current_row):
                self.rows.append(self._current_row)
            self._current_row = []
            self._in_row = False
            return
        if normalized_tag == "table" and self._table_depth > 0:
            self._table_depth -= 1
            if self._table_depth == 0:
                self._captured_first_table = True


def _read_xlsx_shared_strings(workbook: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in workbook.namelist():
        return []
    root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    return [
        "".join(shared_string.itertext())
        for shared_string in root.iter()
        if _local_name(shared_string.tag) == "si"
    ]


def _first_xlsx_worksheet_name(workbook: ZipFile) -> str:
    worksheet_names = sorted(
        name
        for name in workbook.namelist()
        if name.startswith("xl/worksheets/") and name.endswith(".xml")
    )
    if not worksheet_names:
        raise ValueError("SCMO XLSX receipt export did not contain worksheets.")
    return worksheet_names[0]


def _read_xlsx_row(
    row_element: ET.Element,
    *,
    shared_strings: list[str],
) -> list[str]:
    values_by_column: dict[int, str] = {}
    for cell in row_element:
        if _local_name(cell.tag) != "c":
            continue
        column_index = _xlsx_cell_column_index(cell.attrib.get("r", ""))
        values_by_column[column_index] = _read_xlsx_cell(cell, shared_strings)
    if not values_by_column:
        return []
    return [
        values_by_column.get(column_index, "")
        for column_index in range(1, max(values_by_column) + 1)
    ]


def _read_xlsx_cell(cell: ET.Element, shared_strings: list[str]) -> str:
    value_text = ""
    for child in cell:
        if _local_name(child.tag) == "v" and child.text is not None:
            value_text = child.text
            break
        if _local_name(child.tag) == "is":
            return "".join(child.itertext())
    if cell.attrib.get("t") == "s" and value_text:
        shared_string_index = int(value_text)
        if shared_string_index >= len(shared_strings):
            raise ValueError("SCMO XLSX shared string index is out of range.")
        return shared_strings[shared_string_index]
    return value_text


def _xlsx_cell_column_index(cell_ref: str) -> int:
    match = _XLSX_COLUMN_RE.match(cell_ref)
    if match is None:
        return 1
    column_index = 0
    for char in match.group(1):
        column_index = column_index * 26 + ord(char) - 64
    return column_index


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _receipt_id_expr(
    *,
    timestamp_column: str,
    source_publication_timestamp_column: str,
    receipt_id_column: str | None,
) -> pl.Expr:
    if receipt_id_column:
        return pl.col(receipt_id_column).cast(pl.Utf8)
    return (
        pl.lit("scmo-dam-publication:")
        + pl.col(timestamp_column).cast(pl.Utf8)
        + pl.lit(":")
        + pl.col(source_publication_timestamp_column).cast(pl.Utf8)
    )


def _validate_prior_publication(frame: pl.DataFrame) -> None:
    checked = frame.with_columns(
        pl.col("timestamp")
        .map_elements(_naive_datetime, return_dtype=pl.Datetime)
        .alias("_timestamp"),
        pl.col("source_publication_timestamp")
        .map_elements(_naive_datetime, return_dtype=pl.Datetime)
        .alias("_source_publication_timestamp"),
    )
    bad = checked.filter(pl.col("_source_publication_timestamp") >= pl.col("_timestamp"))
    if bad.height:
        raise ValueError(
            "SCMO source_publication_timestamp must be prior to delivery timestamp."
        )


def _naive_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


__all__ = [
    "DEFAULT_SCMO_SOURCE_TITLE",
    "DEFAULT_SCMO_SOURCE_URL",
    "SCMO_AUTO_COLUMN",
    "SCMO_DAM_RECEIPT_EXPORT_CLAIM_SCOPE",
    "ScmoReceiptExportFormat",
    "read_scmo_dam_receipt_export_bytes",
    "read_scmo_dam_receipt_export_path",
    "normalize_scmo_dam_publication_receipt_export_frame",
]
