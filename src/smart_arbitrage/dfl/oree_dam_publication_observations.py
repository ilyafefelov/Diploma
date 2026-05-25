"""Capture OREE DAM first-seen observations without treating them as receipts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any, Final

import polars as pl

CLAIM_SCOPE: Final[str] = "oree_dam_publication_observation_not_v13_receipt"
DEFAULT_SOURCE_TITLE: Final[str] = "OREE PXS DAM trading results endpoint"
OBSERVATION_STATUS: Final[str] = "observed_without_source_publication_timestamp"
RECEIPT_KIND: Final[str] = "first_seen_observation_not_source_publication_receipt"

_REQUIRED_SUMMARY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "timestamp",
        "source_observed_at_utc",
        "publication_observation_status",
        "validated_receipt_csv_ready",
        "permits_model_training",
        "market_execution_enabled",
    }
)
_OBSERVATION_SCHEMA: Final[dict[str, Any]] = {
    "timestamp": pl.Datetime,
    "delivery_date": pl.Utf8,
    "delivery_hour": pl.Int64,
    "price_uah_mwh": pl.Float64,
    "volume_mwh": pl.Float64,
    "source_observed_at_utc": pl.Utf8,
    "source_url": pl.Utf8,
    "source_title": pl.Utf8,
    "observation_id": pl.Utf8,
    "publication_observation_status": pl.Utf8,
    "receipt_kind": pl.Utf8,
    "can_satisfy_v13_explicit_receipts": pl.Boolean,
    "receipt_csv_generated": pl.Boolean,
    "validated_receipt_csv_ready": pl.Boolean,
    "dt_lava_ready": pl.Boolean,
    "permits_model_training": pl.Boolean,
    "market_execution_enabled": pl.Boolean,
    "claim_scope": pl.Utf8,
    "not_full_dfl": pl.Boolean,
    "not_market_execution": pl.Boolean,
}


def build_oree_dam_publication_observation_frame(
    *,
    delivery_date: str | date,
    hdata_payload: Mapping[str, Any],
    retrieved_at: datetime,
    source_url: str,
    source_title: str = DEFAULT_SOURCE_TITLE,
) -> pl.DataFrame:
    """Build row-level OREE PXS observation evidence.

    The OREE PXS endpoint exposes hourly DAM results but does not expose the
    original row-level source publication timestamp. This frame records when
    the results were observed by the acquisition process and intentionally does
    not include the V13 receipt column `source_publication_timestamp`.
    """

    parsed_delivery_date = _delivery_date(delivery_date)
    observed_at = _utc_iso(retrieved_at)
    prices = _sequence(payload=hdata_payload, key="pricesData")
    amounts = _sequence(payload=hdata_payload, key="amountsData")
    labels = _labels(hdata_payload, row_count=len(prices))
    if len(labels) != len(prices):
        raise ValueError("OREE PXS labels and pricesData lengths differ.")

    rows: list[dict[str, object]] = []
    for row_index, hour_label in enumerate(labels):
        hour_index = _hour_index(hour_label)
        timestamp = datetime.combine(
            parsed_delivery_date,
            datetime.min.time().replace(hour=hour_index - 1),
        )
        rows.append(
            {
                "timestamp": timestamp,
                "delivery_date": parsed_delivery_date.isoformat(),
                "delivery_hour": hour_index,
                "price_uah_mwh": _optional_float(prices[row_index]),
                "volume_mwh": _optional_float(amounts[row_index])
                if row_index < len(amounts)
                else None,
                "source_observed_at_utc": observed_at,
                "source_url": source_url,
                "source_title": source_title,
                "observation_id": f"oree_pxs_dam:{parsed_delivery_date.isoformat()}:{hour_index:02d}:{observed_at}",
                "publication_observation_status": OBSERVATION_STATUS,
                "receipt_kind": RECEIPT_KIND,
                "can_satisfy_v13_explicit_receipts": False,
                "receipt_csv_generated": False,
                "validated_receipt_csv_ready": False,
                "dt_lava_ready": False,
                "permits_model_training": False,
                "market_execution_enabled": False,
                "claim_scope": CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(
        rows,
        schema=_OBSERVATION_SCHEMA,
        infer_schema_length=None,
    ).sort("timestamp")


def empty_oree_dam_publication_observation_frame() -> pl.DataFrame:
    """Return an empty observation frame with the stable CSV schema."""

    return pl.DataFrame(schema=_OBSERVATION_SCHEMA)


def summarize_oree_dam_publication_observation_frame(
    frame: pl.DataFrame,
) -> dict[str, Any]:
    """Summarize PXS observation evidence while preserving V13 blockers."""

    if frame.height == 0:
        return {
            "claim_scope": CLAIM_SCOPE,
            "observation_rows": 0,
            "first_timestamp": None,
            "last_timestamp": None,
            "first_observed_at_utc": None,
            "last_observed_at_utc": None,
            "publication_observation_statuses": [],
            "can_satisfy_v13_explicit_receipts": False,
            "receipt_csv_generated": False,
            "validated_receipt_csv_ready": False,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
            "not_full_dfl": True,
            "not_market_execution": True,
        }

    _require_columns(frame, _REQUIRED_SUMMARY_COLUMNS)
    _refuse_true(frame, "market_execution_enabled")
    _refuse_true(frame, "permits_model_training")
    _refuse_true(frame, "validated_receipt_csv_ready")
    statuses = (
        sorted(str(value) for value in frame["publication_observation_status"].unique())
        if frame.height
        else []
    )
    return {
        "claim_scope": CLAIM_SCOPE,
        "observation_rows": frame.height,
        "first_timestamp": _iso_at(frame, "timestamp", 0),
        "last_timestamp": _iso_at(frame, "timestamp", -1),
        "first_observed_at_utc": _string_at(frame, "source_observed_at_utc", 0),
        "last_observed_at_utc": _string_at(frame, "source_observed_at_utc", -1),
        "publication_observation_statuses": statuses,
        "can_satisfy_v13_explicit_receipts": False,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _sequence(*, payload: Mapping[str, Any], key: str) -> Sequence[Any]:
    value = payload.get(key, [])
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"OREE PXS payload field {key!r} must be a sequence.")
    return value


def _labels(payload: Mapping[str, Any], *, row_count: int) -> Sequence[Any]:
    labels = payload.get("labels")
    if labels is None:
        return list(range(1, row_count + 1))
    if not isinstance(labels, Sequence) or isinstance(labels, (str, bytes)):
        raise TypeError("OREE PXS payload field 'labels' must be a sequence.")
    return labels


def _delivery_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    stripped = value.strip()
    if not stripped:
        raise ValueError("delivery_date must not be blank.")
    for date_format in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(stripped, date_format).date()
        except ValueError:
            continue
    raise ValueError("delivery_date must use YYYY-MM-DD or DD.MM.YYYY format.")


def _hour_index(value: Any) -> int:
    if isinstance(value, int):
        hour = value
    elif isinstance(value, float) and value.is_integer():
        hour = int(value)
    elif isinstance(value, str):
        hour = int(value.strip())
    else:
        raise TypeError(f"Cannot convert {type(value).__name__} to delivery hour.")
    if not 1 <= hour <= 24:
        raise ValueError("OREE PXS delivery hour must be in 1..24.")
    return hour


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, int, str)):
        stripped = str(value).strip()
        if not stripped:
            return None
        return float(stripped.replace(",", "."))
    raise TypeError(f"Cannot convert {type(value).__name__} to float.")


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _require_columns(frame: pl.DataFrame, columns: frozenset[str]) -> None:
    missing = columns.difference(frame.columns)
    if missing:
        raise ValueError(
            f"OREE DAM publication observation frame is missing columns: {sorted(missing)}"
        )


def _refuse_true(frame: pl.DataFrame, column_name: str) -> None:
    if frame.is_empty():
        return
    if frame.with_columns(pl.col(column_name).cast(pl.Boolean)).filter(
        pl.col(column_name)
    ).height:
        raise ValueError(
            f"OREE DAM publication observation frame contains true {column_name}."
        )


def _iso_at(frame: pl.DataFrame, column_name: str, index: int) -> str | None:
    if frame.is_empty():
        return None
    value = frame[column_name].item(index)
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _string_at(frame: pl.DataFrame, column_name: str, index: int) -> str | None:
    if frame.is_empty():
        return None
    return str(frame[column_name].item(index))


__all__ = [
    "CLAIM_SCOPE",
    "build_oree_dam_publication_observation_frame",
    "empty_oree_dam_publication_observation_frame",
    "summarize_oree_dam_publication_observation_frame",
]
