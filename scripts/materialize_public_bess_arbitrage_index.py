"""Materialize static JSON for the public Ukraine BESS Arbitrage Index."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from smart_arbitrage.publication.bess_arbitrage_index import (
    OREE_DAM_RESULTS_URL,
    PUBLIC_BESS_INDEX_CLAIM_BOUNDARY,
    build_public_bess_arbitrage_history_payload,
    build_public_bess_arbitrage_index_payload,
)
from smart_arbitrage.publication.oree_pxs import fetch_oree_dam_price_rows

DEFAULT_PUBLIC_OUTPUT_DIR: Final[Path] = Path("dashboard/public/data/bess-arbitrage-index")
PUBLICATION_STATUS_FILENAME: Final[str] = "publication_status.json"


def _kyiv_timezone():
    try:
        return ZoneInfo("Europe/Kyiv")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=3), name="Europe/Kyiv")


KYIV_TZ = _kyiv_timezone()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery-date", default=None, help="Delivery date as YYYY-MM-DD or DD.MM.YYYY. Defaults to yesterday in Europe/Kyiv.")
    parser.add_argument("--input-json", type=Path, default=None, help="Optional offline price rows JSON for deterministic local/CI tests.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_PUBLIC_OUTPUT_DIR)
    parser.add_argument("--history-days", type=int, default=90)
    parser.add_argument("--fail-on-fetch-error", action="store_true")
    args = parser.parse_args(argv)

    generated_at = datetime.now(UTC)
    expected_delivery_day = datetime.now(KYIV_TZ).date() - timedelta(days=1)
    delivery_day = _delivery_date(args.delivery_date) if args.delivery_date else expected_delivery_day
    output_dir = args.output_dir
    latest_path = output_dir / "latest.json"
    history_path = output_dir / "history.json"
    status_path = output_dir / PUBLICATION_STATUS_FILENAME
    output_dir.mkdir(parents=True, exist_ok=True)
    run_status = "published"
    error_payload: dict[str, Any] | None = None

    try:
        price_rows = (
            _read_input_price_rows(args.input_json)
            if args.input_json is not None
            else fetch_oree_dam_price_rows(delivery_day)
        )
        latest_payload = build_public_bess_arbitrage_index_payload(
            price_rows,
            generated_at=generated_at,
        )
    except Exception as error:
        if args.fail_on_fetch_error:
            raise
        run_status = "preserved_previous_latest" if latest_path.exists() else "blocked"
        error_payload = {
            "error_type": type(error).__name__,
            "error": str(error),
        }
        if latest_path.exists():
            latest_payload = _read_json(latest_path)
            if not isinstance(latest_payload, Mapping):
                raise TypeError(f"{latest_path} must contain a JSON object")
            _write_publication_status(
                status_path,
                _publication_status_payload(
                    generated_at=generated_at,
                    status=run_status,
                    expected_delivery_day=expected_delivery_day,
                    attempted_delivery_day=delivery_day,
                    latest_payload=latest_payload,
                    latest_path=latest_path,
                    history_path=history_path,
                    forecast_path=None,
                    scoreboard_path=None,
                    error_payload=error_payload,
                ),
            )
            print(json.dumps(_read_json(status_path), indent=2, sort_keys=True))
            return 0
        latest_payload = _blocked_latest_payload(
            delivery_day=delivery_day,
            generated_at=generated_at,
            error=error,
        )

    previous_history = _read_json(history_path) if history_path.exists() else None
    history_payload = build_public_bess_arbitrage_history_payload(
        latest_payload=latest_payload,
        previous_history=previous_history if isinstance(previous_history, Mapping) else None,
        max_days=args.history_days,
    )
    _write_json(latest_path, latest_payload)
    _write_json(history_path, history_payload)
    _write_publication_status(
        status_path,
        _publication_status_payload(
            generated_at=generated_at,
            status=run_status,
            expected_delivery_day=expected_delivery_day,
            attempted_delivery_day=delivery_day,
            latest_payload=latest_payload,
            latest_path=latest_path,
            history_path=history_path,
            forecast_path=None,
            scoreboard_path=None,
            error_payload=error_payload,
        ),
    )
    print(
        json.dumps(
            {
                "status": "published",
                "latest_json": str(latest_path),
                "history_json": str(history_path),
                "delivery_date": (latest_payload.get("source") or {}).get("delivery_date"),
                "history_rows": history_payload["row_count"],
                "publication_status_json": str(status_path),
                "claim_boundary": latest_payload["claim_boundary"],
                "market_execution_enabled": latest_payload["market_execution_enabled"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _blocked_latest_payload(
    *,
    delivery_day: date,
    generated_at: datetime,
    error: Exception,
) -> dict[str, Any]:
    return {
        "schema_version": "ukraine_bess_arbitrage_index.v1",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "market_venue": "DAM",
        "market_zone": "OES Ukraine",
        "market_timezone": "Europe/Kyiv",
        "source": {
            "source_name": "OREE DAM hourly prices",
            "source_url": OREE_DAM_RESULTS_URL,
            "delivery_date": delivery_day.isoformat(),
            "row_count": 0,
            "source_scope": "official_observed_hourly_prices_only",
            "source_status": "blocked_no_complete_oree_delivery_day",
            "blocker_type": type(error).__name__,
            "blocker": str(error),
        },
        "methodology": {
            "index_kind": "realized_perfect_hindsight_daily_dispatch",
            "objective": "not_computed_without_complete_official_rows",
            "not_market_execution": True,
        },
        "presets": [],
        "summary": {
            "headline_preset_id": None,
            "headline_net_value_uah": 0.0,
            "headline_normalized_uah_per_mwh_capacity": 0.0,
            "preset_count": 0,
        },
        "claim_boundary": PUBLIC_BESS_INDEX_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "proposed_bid_status": "not_emitted",
    }


def _read_input_price_rows(path: Path) -> list[dict[str, Any]]:
    payload = _read_json(path)
    rows = payload.get("rows") if isinstance(payload, Mapping) else payload
    if not isinstance(rows, list):
        raise TypeError("--input-json must contain a JSON array or an object with rows")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_publication_status(path: Path, payload: Mapping[str, Any]) -> None:
    previous_status = _read_json(path) if path.exists() else None
    if isinstance(previous_status, Mapping):
        merged_payload = {
            **previous_status,
            **payload,
            "artifacts": {
                **dict(previous_status.get("artifacts") or {}),
                **dict(payload.get("artifacts") or {}),
            },
        }
    else:
        merged_payload = dict(payload)
    _write_json(path, merged_payload)


def _publication_status_payload(
    *,
    generated_at: datetime,
    status: str,
    expected_delivery_day: date,
    attempted_delivery_day: date,
    latest_payload: Mapping[str, Any],
    latest_path: Path,
    history_path: Path,
    forecast_path: Path | None,
    scoreboard_path: Path | None,
    error_payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    source = latest_payload.get("source") if isinstance(latest_payload.get("source"), Mapping) else {}
    actual_delivery_date = str(source.get("delivery_date") or "")
    is_current = actual_delivery_date == expected_delivery_day.isoformat()
    payload: dict[str, Any] = {
        "schema_version": "ukraine_bess_publication_status.v1",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "publisher_timezone": "Europe/Kyiv",
        "realized": {
            "status": status,
            "expected_delivery_date": expected_delivery_day.isoformat(),
            "attempted_delivery_date": attempted_delivery_day.isoformat(),
            "actual_delivery_date": actual_delivery_date,
            "is_current_for_kyiv_schedule": is_current,
            "source_status": str(source.get("source_status") or ""),
            "row_count": int(source.get("row_count") or 0),
        },
        "artifacts": {
            "latest_json": _posix_path(latest_path),
            "history_json": _posix_path(history_path),
        },
        "autonomy": {
            "compute_layer": "github_actions_scheduled_static_json",
            "host_layer": "github_pages_workflow_static_host",
            "schedule_cron_utc": "35 5 * * *",
            "schedule_timezone_note": "05:35 UTC is 08:35 Europe/Kyiv during summer time",
            "market_execution_enabled": False,
            "proposed_bid_status": "not_emitted",
        },
    }
    if forecast_path is not None:
        payload["artifacts"]["forecast_latest_json"] = _posix_path(forecast_path)
    if scoreboard_path is not None:
        payload["artifacts"]["forecast_scoreboard_json"] = _posix_path(scoreboard_path)
    if error_payload:
        payload["realized"]["last_error"] = dict(error_payload)
    return payload


def _posix_path(path: Path) -> str:
    return path.as_posix()


def _delivery_date(value: str | None) -> date:
    if value is None:
        raise ValueError("delivery date is required")
    stripped = value.strip()
    for date_format in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(stripped, date_format).date()
        except ValueError:
            continue
    raise ValueError("delivery date must use YYYY-MM-DD or DD.MM.YYYY format")


if __name__ == "__main__":
    raise SystemExit(main())
