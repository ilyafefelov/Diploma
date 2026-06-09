"""Build V13 DAM receipt source leads from Energy Map dataset metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final

import polars as pl

CLAIM_SCOPE: Final[str] = "energy_map_dam_receipt_metadata_not_v13_receipt"
ENERGY_MAP_DATASET_BASE_URL: Final[str] = "https://energy-map.info/en/datasets"
ENERGY_MAP_DATASET_API_BASE_URL: Final[str] = "https://energy-map.info/apis/v1/datasets"

_PUBLICATION_TOKENS: Final[tuple[str, ...]] = (
    "source_publication_timestamp",
    "publication_timestamp",
    "published_at",
    "receipt_id",
)
_TIMESTAMP_TOKENS: Final[tuple[str, ...]] = (
    "timestamp",
    "date",
    "day",
    "hour",
)


def build_energy_map_dam_receipt_metadata_leads_v13_frame(
    dataset_payloads: Sequence[Mapping[str, Any]],
    *,
    locale: str = "en",
) -> pl.DataFrame:
    """Convert Energy Map dataset metadata into non-promotional receipt leads.

    Energy Map exposes dataset/file update timestamps through its metadata API,
    but the current DAM metadata does not expose a row-level
    `source_publication_timestamp` field. The returned rows are therefore
    source-acquisition leads only and are intentionally classified as
    dataset-level metadata.
    """

    rows: list[dict[str, object]] = []
    for payload in dataset_payloads:
        dataset_uuid = _required_string(payload, "uuid")
        title = _required_string(payload, "title")
        field_names = _field_names(payload.get("fieldsDesc", []))
        has_timestamp_column = _contains_any(field_names, _TIMESTAMP_TOKENS)
        has_source_publication_timestamp_column = _contains_any(
            field_names,
            _PUBLICATION_TOKENS,
        )
        files = payload.get("files", [])
        if not isinstance(files, Sequence) or isinstance(files, (str, bytes)):
            raise TypeError("Energy Map dataset payload field 'files' must be a sequence.")
        for file_row in files:
            if not isinstance(file_row, Mapping):
                raise TypeError("Energy Map file metadata rows must be mappings.")
            filename = _required_string(file_row, "filename")
            updated_at = _file_updated_at(file_row)
            rows.append(
                {
                    "lead_id": f"energy_map_file_metadata:{dataset_uuid}:{filename}",
                    "source_url": f"{ENERGY_MAP_DATASET_BASE_URL}/{dataset_uuid}",
                    "source_title": f"Energy Map {title} file metadata: {filename}",
                    "lead_kind": "dataset_file_metadata",
                    "metadata_scope": "dataset_level",
                    "has_timestamp_column": has_timestamp_column,
                    "has_source_publication_timestamp_column": (
                        has_source_publication_timestamp_column
                    ),
                    "download_auth_required": False,
                    "source_probe_status": "file_level_publication_metadata_only",
                    "dataset_uuid": dataset_uuid,
                    "dataset_locale": locale,
                    "dataset_file_name": filename,
                    "dataset_file_format": ",".join(_file_format(file_row)),
                    "dataset_file_rows": _optional_int(file_row.get("rows")),
                    "dataset_file_size_bytes": _file_size(file_row),
                    "dataset_last_updated_timestamp": updated_at,
                    "receipt_csv_generated": False,
                    "validated_receipt_csv_ready": False,
                    "dt_lava_ready": False,
                    "permits_model_training": False,
                    "market_execution_enabled": False,
                    "claim_scope": CLAIM_SCOPE,
                }
            )
    if not rows:
        return _empty_frame()
    return pl.DataFrame(rows, infer_schema_length=None).sort(
        ["dataset_uuid", "dataset_file_name"]
    )


def _empty_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "lead_id": pl.Utf8,
            "source_url": pl.Utf8,
            "source_title": pl.Utf8,
            "lead_kind": pl.Utf8,
            "metadata_scope": pl.Utf8,
            "has_timestamp_column": pl.Boolean,
            "has_source_publication_timestamp_column": pl.Boolean,
            "download_auth_required": pl.Boolean,
            "source_probe_status": pl.Utf8,
            "dataset_last_updated_timestamp": pl.Utf8,
            "market_execution_enabled": pl.Boolean,
        }
    )


def _required_string(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if value is None or not str(value).strip():
        raise ValueError(f"Energy Map dataset metadata is missing {key!r}.")
    return str(value).strip()


def _field_names(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("Energy Map dataset payload field 'fieldsDesc' must be a sequence.")
    names: list[str] = []
    for field in value:
        if isinstance(field, Sequence) and not isinstance(field, (str, bytes)):
            names.extend(str(item).casefold() for item in field if item is not None)
        elif isinstance(field, Mapping):
            names.extend(str(item).casefold() for item in field.values())
    return names


def _contains_any(values: Sequence[str], tokens: Sequence[str]) -> bool:
    return any(token.casefold() in value for value in values for token in tokens)


def _file_updated_at(file_row: Mapping[str, Any]) -> str:
    updated = file_row.get("updated")
    if isinstance(updated, Mapping):
        time_value = updated.get("time")
        if time_value is not None and str(time_value).strip():
            return str(time_value).strip()
    return ""


def _file_format(file_row: Mapping[str, Any]) -> list[str]:
    value = file_row.get("format", [])
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [str(item) for item in value]


def _file_size(file_row: Mapping[str, Any]) -> int | None:
    value = file_row.get("size")
    if isinstance(value, Mapping):
        return _optional_int(value.get("size"))
    return _optional_int(value)


def _optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)


__all__ = [
    "CLAIM_SCOPE",
    "ENERGY_MAP_DATASET_API_BASE_URL",
    "build_energy_map_dam_receipt_metadata_leads_v13_frame",
]
