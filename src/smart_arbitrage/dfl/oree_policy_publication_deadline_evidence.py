from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import polars as pl

CLAIM_SCOPE: Final[str] = "oree_policy_publication_deadline_evidence_not_v13_receipt"
DEFAULT_POLICY_SOURCE_URL: Final[str] = (
    "https://www.oree.com.ua/index.php/web/13245784?lang=english"
)
DEFAULT_POLICY_SOURCE_TITLE: Final[str] = (
    "OREE Notice of the time and trading procedure"
)
DEFAULT_POLICY_TIMEZONE: Final[str] = "Europe/Kyiv"

_OUTPUT_SCHEMA: Final[dict[str, Any]] = {
    "market": pl.Utf8,
    "delivery_date": pl.Utf8,
    "policy_publication_deadline_kyiv": pl.Utf8,
    "policy_deadline_rule_id": pl.Utf8,
    "policy_timezone": pl.Utf8,
    "policy_source_url": pl.Utf8,
    "policy_source_title": pl.Utf8,
    "observed_public_artifact_count": pl.Int64,
    "observed_artifact_kinds": pl.Utf8,
    "observed_source_urls": pl.Utf8,
    "first_observed_at_utc": pl.Utf8,
    "last_observed_at_utc": pl.Utf8,
    "publication_evidence_kind": pl.Utf8,
    "policy_deadline_is_publication_timestamp": pl.Boolean,
    "explicit_publication_timestamp_available": pl.Boolean,
    "can_satisfy_v13_explicit_receipts": pl.Boolean,
    "receipt_csv_generated": pl.Boolean,
    "validated_receipt_csv_ready": pl.Boolean,
    "dt_lava_ready": pl.Boolean,
    "permits_model_training": pl.Boolean,
    "market_execution_enabled": pl.Boolean,
    "claim_scope": pl.Utf8,
    "generated_at": pl.Utf8,
}


def build_oree_policy_publication_deadline_evidence_frame(
    candidate_audit: Mapping[str, Any],
    *,
    generated_at: datetime,
    policy_source_url: str = DEFAULT_POLICY_SOURCE_URL,
    policy_source_title: str = DEFAULT_POLICY_SOURCE_TITLE,
) -> pl.DataFrame:
    """Build weak OREE policy-deadline evidence from public observation artifacts."""

    generated_at_utc = _utc_datetime(generated_at)
    requested_delivery_date = str(candidate_audit.get("requested_delivery_date", ""))
    artifact_rows = _artifact_rows(candidate_audit)
    rows: list[dict[str, Any]] = []
    for market in ("DAM", "IDM"):
        observed = _observed_public_artifacts(
            artifact_rows,
            market=market,
            requested_delivery_date=requested_delivery_date,
        )
        if not observed:
            continue
        delivery_date = _delivery_date_for_market(
            observed,
            requested_delivery_date=requested_delivery_date,
        )
        first_observed_at, last_observed_at = _observed_bounds(observed)
        rows.append(
            {
                "market": market,
                "delivery_date": delivery_date,
                "policy_publication_deadline_kyiv": _policy_deadline(
                    market,
                    delivery_date,
                ).isoformat(),
                "policy_deadline_rule_id": _policy_rule_id(market),
                "policy_timezone": DEFAULT_POLICY_TIMEZONE,
                "policy_source_url": policy_source_url,
                "policy_source_title": policy_source_title,
                "observed_public_artifact_count": len(observed),
                "observed_artifact_kinds": ",".join(
                    sorted({str(row.get("artifact_kind", "")) for row in observed})
                ),
                "observed_source_urls": ",".join(
                    sorted({str(row.get("source_url", "")) for row in observed})
                ),
                "first_observed_at_utc": first_observed_at,
                "last_observed_at_utc": last_observed_at,
                "publication_evidence_kind": (
                    "policy_deadline_plus_observed_public_presence"
                ),
                "policy_deadline_is_publication_timestamp": False,
                "explicit_publication_timestamp_available": False,
                "can_satisfy_v13_explicit_receipts": False,
                "receipt_csv_generated": False,
                "validated_receipt_csv_ready": False,
                "dt_lava_ready": False,
                "permits_model_training": False,
                "market_execution_enabled": False,
                "claim_scope": CLAIM_SCOPE,
                "generated_at": generated_at_utc.isoformat(),
            }
        )
    if not rows:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)
    return pl.DataFrame(rows, schema=_OUTPUT_SCHEMA).sort(["market", "delivery_date"])


def summarize_oree_policy_publication_deadline_evidence_frame(
    frame: pl.DataFrame,
) -> dict[str, Any]:
    """Summarize weak OREE policy evidence without treating it as receipts."""

    _require_policy_evidence_columns(frame)
    _refuse_true(frame, "can_satisfy_v13_explicit_receipts")
    _refuse_true(frame, "explicit_publication_timestamp_available")
    _refuse_true(frame, "validated_receipt_csv_ready")
    _refuse_true(frame, "permits_model_training")
    _refuse_true(frame, "market_execution_enabled")
    if frame.height == 0:
        return {
            "claim_scope": CLAIM_SCOPE,
            "policy_evidence_row_count": 0,
            "observed_market_count": 0,
            "markets_observed": [],
            "all_policy_deadlines_have_observed_public_artifact": False,
            "can_satisfy_v13_explicit_receipts": False,
            "source_publication_timestamp_available": False,
            "receipt_csv_generated": False,
            "validated_receipt_csv_ready": False,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        }
    markets = sorted(str(value) for value in frame["market"].unique())
    return {
        "claim_scope": CLAIM_SCOPE,
        "policy_evidence_row_count": frame.height,
        "observed_market_count": len(markets),
        "markets_observed": markets,
        "all_policy_deadlines_have_observed_public_artifact": frame.filter(
            pl.col("observed_public_artifact_count") <= 0
        ).height
        == 0,
        "policy_source_urls": sorted(
            str(value) for value in frame["policy_source_url"].unique()
        ),
        "publication_evidence_kinds": sorted(
            str(value) for value in frame["publication_evidence_kind"].unique()
        ),
        "can_satisfy_v13_explicit_receipts": False,
        "source_publication_timestamp_available": False,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _artifact_rows(candidate_audit: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw_rows = candidate_audit.get("artifact_rows", [])
    if not isinstance(raw_rows, list):
        return []
    return [row for row in raw_rows if isinstance(row, Mapping)]


def _observed_public_artifacts(
    artifact_rows: Sequence[Mapping[str, Any]],
    *,
    market: str,
    requested_delivery_date: str,
) -> list[Mapping[str, Any]]:
    observed: list[Mapping[str, Any]] = []
    for row in artifact_rows:
        if str(row.get("market", "")) != market:
            continue
        if str(row.get("status", "")) != "http_ok":
            continue
        if bool(row.get("explicit_publication_timestamps_found", False)):
            continue
        if str(row.get("v13_verdict", "")) not in {"price_only", "observation_only"}:
            continue
        if not bool(row.get("delivery_timestamps_found", False)):
            continue
        if str(row.get("delivery_date", "")) not in {"", requested_delivery_date}:
            continue
        observed.append(row)
    return observed


def _delivery_date_for_market(
    observed_rows: Sequence[Mapping[str, Any]],
    *,
    requested_delivery_date: str,
) -> str:
    for row in observed_rows:
        raw_delivery_date = str(row.get("delivery_date", ""))
        if raw_delivery_date:
            return raw_delivery_date
    if requested_delivery_date:
        return requested_delivery_date
    raise ValueError("A requested delivery date or observed delivery date is required.")


def _policy_deadline(market: str, delivery_date: str) -> datetime:
    parsed_delivery_date = datetime.strptime(delivery_date, "%Y-%m-%d").date()
    if market == "DAM":
        deadline_date = parsed_delivery_date - timedelta(days=1)
    elif market == "IDM":
        deadline_date = parsed_delivery_date + timedelta(days=1)
    else:
        raise ValueError(f"Unsupported market for OREE policy deadline: {market}")
    return datetime.combine(deadline_date, datetime.min.time().replace(hour=14))


def _policy_rule_id(market: str) -> str:
    if market == "DAM":
        return "oree_dam_results_no_later_than_14_00_d_minus_1_kyiv"
    if market == "IDM":
        return "oree_idm_results_no_later_than_14_00_d_plus_1_kyiv"
    raise ValueError(f"Unsupported market for OREE policy deadline: {market}")


def _observed_bounds(observed_rows: Sequence[Mapping[str, Any]]) -> tuple[str, str]:
    timestamps = sorted(
        {
            _utc_datetime(
                datetime.fromisoformat(str(row["retrieved_at"]).replace("Z", "+00:00"))
            ).isoformat()
            for row in observed_rows
            if str(row.get("retrieved_at", "")).strip()
        }
    )
    if not timestamps:
        return "", ""
    return timestamps[0], timestamps[-1]


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _require_policy_evidence_columns(frame: pl.DataFrame) -> None:
    missing = set(_OUTPUT_SCHEMA).difference(frame.columns)
    if missing:
        raise ValueError(
            "OREE policy publication deadline evidence frame is missing "
            f"required columns: {sorted(missing)}"
        )


def _refuse_true(frame: pl.DataFrame, column_name: str) -> None:
    if frame.height and frame.select(pl.col(column_name).any()).item():
        raise ValueError(
            "OREE policy publication deadline evidence refuses "
            f"{column_name}=true."
        )


__all__ = [
    "CLAIM_SCOPE",
    "build_oree_policy_publication_deadline_evidence_frame",
    "summarize_oree_policy_publication_deadline_evidence_frame",
]
