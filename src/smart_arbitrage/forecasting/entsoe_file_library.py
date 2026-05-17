"""ENTSO-E File Library helpers for research-only market-coupling snapshots.

The helpers in this module intentionally keep credentials and bearer tokens out
of metadata, receipts, and repr output. They support fetching source-backed
File Library extracts that still pass through the existing governance gate
before any official training route may consume them.
"""

from __future__ import annotations

import csv
import io
import json
import os
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl

ENTSOE_FMS_TOKEN_URL: Final[str] = (
    "https://keycloak.tp.entsoe.eu/realms/tp/protocol/openid-connect/token"
)
ENTSOE_FMS_SERVICE_URL: Final[str] = "https://fms.tp.entsoe.eu"
ENTSOE_FMS_CLIENT_ID: Final[str] = "tp-fms-public"
ENTSOE_ENERGY_PRICES_FOLDER: Final[str] = "/TP_export/EnergyPrices_12.1.D_r3/"
ENTSOE_EXPORT_LOG_PATH: Final[str] = "/TP_export/Export_log_r3.csv"
ENTSOE_FILE_LIBRARY_GUIDE_URL: Final[str] = (
    "https://transparencyplatform.zendesk.com/hc/en-us/articles/"
    "35960137882129-File-Library-Guide"
)

_USERNAME_KEYS: Final[tuple[str, ...]] = (
    "ENTSOE_EMAIL",
    "ENTSOE_USERNAME",
    "ENTSOE_TP_EMAIL",
    "entsoe_email",
    "entsoe_username",
)
_PASSWORD_KEYS: Final[tuple[str, ...]] = (
    "ENTSOE_PASSWORD",
    "ENTSOE_TP_PASSWORD",
    "entsoe_password",
)
_ENERGY_PRICE_TIMESTAMP_COLUMNS: Final[tuple[str, ...]] = (
    "delivery_timestamp_utc",
    "DateTime(UTC)",
    "datetime_utc",
    "timestamp_utc",
    "timestamp",
)
_ENERGY_PRICE_COLUMNS: Final[tuple[str, ...]] = (
    "price_eur_mwh",
    "Price[Currency/MWh]",
    "Price",
    "price",
    "neighbor_market_price_eur_mwh",
)

PostForm = Callable[[str, dict[str, str], dict[str, str]], Mapping[str, object]]
PostJson = Callable[[str, Mapping[str, object], dict[str, str]], Mapping[str, object] | bytes]


@dataclass(frozen=True)
class EntsoeFileLibraryCredentials:
    """Runtime-only ENTSO-E File Library credentials."""

    username: str = field(repr=False)
    password: str = field(repr=False)
    credential_source: str = "environment"

    def safe_metadata(self) -> dict[str, object]:
        return {
            "credential_source": self.credential_source,
            "username_present": bool(self.username),
            "password_present": bool(self.password),
        }


@dataclass(frozen=True)
class EntsoeFmsToken:
    """Bearer token with secret value hidden from repr and metadata."""

    access_token: str = field(repr=False)
    expires_in: int
    token_type: str = "Bearer"

    def safe_metadata(self) -> dict[str, object]:
        return {
            "token_available": bool(self.access_token),
            "expires_in": self.expires_in,
            "token_type": self.token_type,
        }


@dataclass(frozen=True)
class EntsoeFmsFileMetadata:
    """Minimal safe File Library metadata for a downloadable file."""

    filename: str
    folder: str
    last_updated_timestamp: str
    created_timestamp: str = ""
    period_from: str = ""
    period_to: str = ""
    content_id: str = ""

    @property
    def source_publication_timestamp_utc(self) -> str:
        return _normalize_optional_iso_utc(self.last_updated_timestamp)


def load_entsoe_file_library_credentials(
    *,
    env: Mapping[str, str] | None = None,
    env_file: Path | str | None = Path(".env"),
) -> EntsoeFileLibraryCredentials:
    """Load ENTSO-E File Library credentials from env vars or a local .env file."""

    file_values = _read_env_file(Path(env_file)) if env_file is not None else {}
    environment = dict(os.environ if env is None else env)
    merged = {**file_values, **environment}
    username = _first_present(merged, _USERNAME_KEYS)
    password = _first_present(merged, _PASSWORD_KEYS)
    if not username or not password:
        raise RuntimeError(
            "ENTSO-E File Library credentials are required. Set entsoe_email and "
            "entsoe_password in .env, or use ENTSOE_EMAIL/ENTSOE_PASSWORD."
        )
    source = "env_file" if username == file_values.get("entsoe_email") else "environment"
    return EntsoeFileLibraryCredentials(
        username=username,
        password=password,
        credential_source=source,
    )


def request_entsoe_fms_token(
    *,
    username: str,
    password: str,
    post_form: PostForm | None = None,
) -> EntsoeFmsToken:
    """Request a Keycloak bearer token for ENTSO-E FMS."""

    payload = {
        "client_id": ENTSOE_FMS_CLIENT_ID,
        "grant_type": "password",
        "username": username,
        "password": password,
    }
    response = (
        post_form(ENTSOE_FMS_TOKEN_URL, payload, _form_headers())
        if post_form is not None
        else _post_form_json(ENTSOE_FMS_TOKEN_URL, payload, _form_headers())
    )
    token = str(response.get("access_token", "")).strip()
    if not token:
        raise RuntimeError("ENTSO-E token response did not include access_token")
    expires_in_value = response.get("expires_in", 0)
    return EntsoeFmsToken(
        access_token=token,
        expires_in=int(str(expires_in_value or 0)),
        token_type=str(response.get("token_type", "Bearer") or "Bearer"),
    )


def list_entsoe_energy_price_files(
    token: EntsoeFmsToken,
    *,
    folder: str = ENTSOE_ENERGY_PRICES_FOLDER,
    post_json: PostJson | None = None,
) -> list[EntsoeFmsFileMetadata]:
    """List ENTSO-E EnergyPrices files from File Library."""

    response = _post_fms_json(
        "/listFileMetadata",
        token,
        {
            "topLevelFolder": "TP_export",
            "typeSpecificAttributeMap": {"path": folder},
            "sorterList": [{"key": "periodCovered.from", "ascending": True}],
            "pageInfo": {"pageIndex": 0, "pageSize": 5000},
        },
        post_json=post_json,
    )
    if not isinstance(response, Mapping):
        raise RuntimeError("ENTSO-E listFolder response must be JSON")
    items = response.get("itemList") or response.get("items") or []
    if not isinstance(items, list):
        raise RuntimeError("ENTSO-E listFolder response itemList must be a list")
    files = [_file_metadata_from_item(item, folder=folder) for item in items]
    return sorted(files, key=lambda item: (item.period_from, item.filename))


def select_entsoe_energy_price_file(
    files: list[EntsoeFmsFileMetadata],
    *,
    month: str,
) -> EntsoeFmsFileMetadata:
    """Select one monthly EnergyPrices file by YYYY-MM or YYYY_MM match."""

    normalized_month = month.strip()
    if not normalized_month:
        raise ValueError("month is required")
    filename_marks = {
        normalized_month,
        normalized_month.replace("-", "_"),
        normalized_month.replace("-", ""),
    }
    for file in files:
        if file.period_from.startswith(normalized_month):
            return file
        if any(mark in file.filename for mark in filename_marks):
            return file
    available = ", ".join(file.filename for file in files[:5])
    raise ValueError(
        f"No ENTSO-E EnergyPrices file matched month {month!r}. "
        f"First available files: {available}"
    )


def download_entsoe_fms_file(
    token: EntsoeFmsToken,
    file: EntsoeFmsFileMetadata,
    *,
    post_json: PostJson | None = None,
) -> bytes:
    """Download one File Library file as bytes."""

    response = _post_fms_json(
        "/downloadFileContent",
        token,
        {
            "folder": file.folder,
            "filename": file.filename,
            "lastUpdateTimestamp": file.last_updated_timestamp,
            "topLevelFolder": "TP_export",
            "downloadAsZip": False,
        },
        post_json=post_json,
    )
    if isinstance(response, bytes):
        return response
    if isinstance(response, Mapping) and isinstance(response.get("content"), str):
        return str(response["content"]).encode("utf-8")
    raise RuntimeError("ENTSO-E downloadFileContent response was not file bytes")


def decode_entsoe_fms_file_content(content: bytes) -> str:
    """Decode ENTSO-E FMS bytes, including zipped CSV payloads."""

    stream = io.BytesIO(content)
    if zipfile.is_zipfile(stream):
        stream.seek(0)
        with zipfile.ZipFile(stream) as archive:
            csv_names = [
                name
                for name in archive.namelist()
                if name.lower().endswith((".csv", ".txt"))
            ]
            if not csv_names:
                raise ValueError("ENTSO-E FMS ZIP payload did not contain CSV content")
            with archive.open(csv_names[0]) as fh:
                return fh.read().decode("utf-8-sig")
    return content.decode("utf-8-sig")


def normalize_energy_prices_csv_to_poland_snapshot_frame(
    csv_text: str,
    *,
    country_code: str = "PL",
) -> pl.DataFrame:
    """Normalize ENTSO-E EnergyPrices CSV content to snapshot parser columns."""

    rows = list(csv.DictReader(io.StringIO(csv_text), delimiter=_detect_delimiter(csv_text)))
    if not rows:
        return pl.DataFrame({"delivery_timestamp_utc": [], "price_eur_mwh": []})
    timestamp_column = _find_column(rows[0], _ENERGY_PRICE_TIMESTAMP_COLUMNS, "timestamp")
    price_column = _find_column(rows[0], _ENERGY_PRICE_COLUMNS, "price")
    normalized_rows: list[dict[str, object]] = []
    for row in rows:
        if not _row_matches_country(row, country_code):
            continue
        if not _row_matches_day_ahead(row):
            continue
        if not _row_matches_eur(row):
            continue
        normalized_rows.append(
            {
                "delivery_timestamp_utc": _parse_entsoe_datetime_utc(
                    row[timestamp_column]
                ).isoformat(),
                "price_eur_mwh": _to_float(row[price_column]),
            }
        )
    return pl.DataFrame(normalized_rows).sort("delivery_timestamp_utc")


def write_poland_snapshot_csv(frame: pl.DataFrame, output_path: Path) -> Path:
    """Write normalized Poland snapshot rows to CSV for the existing asset route."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(output_path)
    return output_path


def safe_entsoe_fms_smoke_receipt(
    *,
    token_metadata: Mapping[str, object],
    selected_file: EntsoeFmsFileMetadata,
    output_csv_path: Path,
    row_count: int,
) -> dict[str, object]:
    """Build a local receipt that cannot include token/password values."""

    return {
        "receipt_kind": "entsoe_file_library_poland_snapshot_smoke",
        "source": "ENTSO-E File Library",
        "file_library_guide_url": ENTSOE_FILE_LIBRARY_GUIDE_URL,
        "energy_prices_folder": ENTSOE_ENERGY_PRICES_FOLDER,
        "token_metadata": dict(token_metadata),
        "selected_file": {
            "filename": selected_file.filename,
            "folder": selected_file.folder,
            "last_updated_timestamp": selected_file.last_updated_timestamp,
            "period_from": selected_file.period_from,
            "period_to": selected_file.period_to,
        },
        "output_csv_path": str(output_csv_path),
        "normalized_row_count": row_count,
        "training_use_allowed": False,
        "feature_use_allowed": False,
        "market_execution_enabled": False,
        "claim_boundary": (
            "Source-backed Poland exogenous feature evidence only; no EU rows enter "
            "Ukrainian training unless governance later approves the route."
        ),
    }


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _first_present(values: Mapping[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = values.get(key)
        if value and value.strip():
            return value.strip()
    return ""


def _form_headers() -> dict[str, str]:
    return {"Content-Type": "application/x-www-form-urlencoded"}


def _post_form_json(
    url: str,
    payload: Mapping[str, str],
    headers: Mapping[str, str],
) -> Mapping[str, object]:
    data = urlencode(payload).encode("utf-8")
    request = Request(url, data=data, headers=dict(headers), method="POST")
    with urlopen(request, timeout=60) as response:  # noqa: S310 - ENTSO-E endpoint.
        return json.loads(response.read().decode("utf-8"))


def _post_fms_json(
    endpoint: str,
    token: EntsoeFmsToken,
    payload: Mapping[str, object],
    *,
    post_json: PostJson | None,
) -> Mapping[str, object] | bytes:
    url = f"{ENTSOE_FMS_SERVICE_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token.access_token}",
        "Content-Type": "application/json",
    }
    if post_json is not None:
        return post_json(url, payload, headers)
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urlopen(request, timeout=120) as response:  # noqa: S310 - ENTSO-E endpoint.
        content = response.read()
        content_type = response.headers.get("Content-Type", "")
    if "application/json" in content_type:
        return json.loads(content.decode("utf-8"))
    return content


def _file_metadata_from_item(
    item: object,
    *,
    folder: str,
) -> EntsoeFmsFileMetadata:
    if not isinstance(item, Mapping):
        raise RuntimeError("ENTSO-E file metadata item must be an object")
    content = item.get("content") if isinstance(item.get("content"), Mapping) else {}
    content_map = content if isinstance(content, Mapping) else {}
    filename = str(
        content_map.get("filename")
        or item.get("filename")
        or item.get("name")
        or ""
    ).strip()
    if not filename:
        raise RuntimeError("ENTSO-E file metadata did not include filename")
    last_updated = str(
        content_map.get("lastUpdatedTimestamp")
        or item.get("lastUpdatedTimestamp")
        or item.get("lastUpdateTimestamp")
        or ""
    ).strip()
    created = str(content_map.get("createdTimestamp") or item.get("createdTimestamp") or "").strip()
    period = content_map.get("periodCovered") or item.get("periodCovered") or {}
    period_map = period if isinstance(period, Mapping) else {}
    return EntsoeFmsFileMetadata(
        filename=filename,
        folder=str(item.get("path") or folder),
        last_updated_timestamp=last_updated,
        created_timestamp=created,
        period_from=str(period_map.get("from") or period_map.get("start") or "").strip(),
        period_to=str(period_map.get("to") or period_map.get("end") or "").strip(),
        content_id=str(content_map.get("id") or item.get("contentId") or "").strip(),
    )


def _detect_delimiter(csv_text: str) -> str:
    header = csv_text.splitlines()[0] if csv_text.splitlines() else ""
    if "\t" in header:
        return "\t"
    if ";" in header:
        return ";"
    return ","


def _find_column(row: Mapping[str, str], aliases: tuple[str, ...], label: str) -> str:
    for alias in aliases:
        if alias in row:
            return alias
    lowered = {key.lower(): key for key in row}
    for alias in aliases:
        match = lowered.get(alias.lower())
        if match:
            return match
    raise ValueError(f"ENTSO-E EnergyPrices CSV missing {label} column")


def _row_matches_country(row: Mapping[str, str], country_code: str) -> bool:
    country = country_code.upper()
    for key in ("MapCode", "map_code", "country_code", "CountryCode"):
        if str(row.get(key, "")).strip().upper() == country:
            return True
    for key in ("AreaDisplayName", "AreaName", "area_name"):
        value = str(row.get(key, "")).upper()
        if value == country or value.endswith(f"|{country}") or f" {country}" in value:
            return True
    return False


def _row_matches_day_ahead(row: Mapping[str, str]) -> bool:
    for key in ("ContractType", "contract_type", "contractType"):
        value = str(row.get(key, "")).strip().lower()
        if not value:
            continue
        if "intraday" in value or "intra-day" in value:
            return False
        if "day-ahead" not in value and "day ahead" not in value:
            return False
    return True


def _row_matches_eur(row: Mapping[str, str]) -> bool:
    currency = str(row.get("Currency", row.get("currency", ""))).strip().upper()
    return not currency or currency == "EUR"


def _parse_entsoe_datetime_utc(value: str) -> datetime:
    text = str(value).strip().replace("Z", "+00:00")
    if not text:
        raise ValueError("ENTSO-E timestamp is empty")
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        pass
    for pattern in ("%d/%m/%Y %H:%M:%S", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, pattern).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(f"Unsupported ENTSO-E timestamp format: {value!r}")


def _normalize_optional_iso_utc(value: str) -> str:
    if not value.strip():
        return ""
    return _parse_entsoe_datetime_utc(value).isoformat()


def _to_float(value: str) -> float:
    return float(str(value).strip().replace(",", "."))


__all__ = [
    "ENTSOE_ENERGY_PRICES_FOLDER",
    "ENTSOE_EXPORT_LOG_PATH",
    "ENTSOE_FILE_LIBRARY_GUIDE_URL",
    "ENTSOE_FMS_CLIENT_ID",
    "ENTSOE_FMS_SERVICE_URL",
    "ENTSOE_FMS_TOKEN_URL",
    "EntsoeFileLibraryCredentials",
    "EntsoeFmsFileMetadata",
    "EntsoeFmsToken",
    "decode_entsoe_fms_file_content",
    "download_entsoe_fms_file",
    "list_entsoe_energy_price_files",
    "load_entsoe_file_library_credentials",
    "normalize_energy_prices_csv_to_poland_snapshot_frame",
    "request_entsoe_fms_token",
    "safe_entsoe_fms_smoke_receipt",
    "select_entsoe_energy_price_file",
    "write_poland_snapshot_csv",
]
