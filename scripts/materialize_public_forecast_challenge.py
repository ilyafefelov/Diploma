"""Materialize static JSON for the public BESS Forecast Challenge."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from smart_arbitrage.publication.bess_arbitrage_index import (
    PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY,
)
from smart_arbitrage.publication.forecast_challenge import (
    build_empty_forecast_scoreboard_payload,
    build_public_forecast_challenge_payload,
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
    parser.add_argument("--target-delivery-date", default=None, help="Target date as YYYY-MM-DD. Defaults to tomorrow in Europe/Kyiv.")
    parser.add_argument("--history-days", type=int, default=10)
    parser.add_argument("--input-json", type=Path, default=None, help="Optional offline history rows JSON.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_PUBLIC_OUTPUT_DIR)
    parser.add_argument("--fail-on-fetch-error", action="store_true")
    args = parser.parse_args(argv)

    generated_at = datetime.now(UTC)
    expected_target_delivery_date = datetime.now(KYIV_TZ).date() + timedelta(days=1)
    target_delivery_date = _delivery_date(args.target_delivery_date) if args.target_delivery_date else expected_target_delivery_date
    output_dir = args.output_dir
    forecast_dir = output_dir / "forecast"
    forecast_archive_dir = forecast_dir / "forecasts"
    forecast_dir.mkdir(parents=True, exist_ok=True)
    forecast_archive_dir.mkdir(parents=True, exist_ok=True)
    latest_path = forecast_dir / "latest.json"
    scoreboard_path = output_dir / "forecast_scoreboard.json"
    status_path = output_dir / PUBLICATION_STATUS_FILENAME
    run_status = "published"
    error_payload: dict[str, Any] | None = None

    try:
        history_rows = (
            _read_input_history_rows(args.input_json)
            if args.input_json is not None
            else _fetch_history_rows(target_delivery_date=target_delivery_date, history_days=args.history_days)
        )
        latest_payload = build_public_forecast_challenge_payload(
            history_rows,
            target_delivery_date=target_delivery_date,
            generated_at=generated_at,
        )
    except Exception as error:
        if args.fail_on_fetch_error:
            raise
        run_status = "preserved_previous_forecast_latest" if latest_path.exists() else "blocked"
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
                    expected_target_delivery_date=expected_target_delivery_date,
                    attempted_target_delivery_date=target_delivery_date,
                    latest_payload=latest_payload,
                    latest_path=latest_path,
                    scoreboard_path=scoreboard_path,
                    error_payload=error_payload,
                ),
            )
            print(json.dumps(_read_json(status_path), indent=2, sort_keys=True))
            return 0
        latest_payload = _blocked_forecast_payload(
            target_delivery_date=target_delivery_date,
            generated_at=generated_at,
            error=error,
        )

    archive_path = forecast_archive_dir / _forecast_archive_filename(latest_payload)
    _write_json(latest_path, latest_payload)
    _write_json(archive_path, latest_payload)
    if not scoreboard_path.exists():
        _write_json(scoreboard_path, build_empty_forecast_scoreboard_payload(generated_at=generated_at))
    _write_publication_status(
        status_path,
        _publication_status_payload(
            generated_at=generated_at,
            status=run_status,
            expected_target_delivery_date=expected_target_delivery_date,
            attempted_target_delivery_date=target_delivery_date,
            latest_payload=latest_payload,
            latest_path=latest_path,
            scoreboard_path=scoreboard_path,
            error_payload=error_payload,
        ),
    )
    print(
        json.dumps(
            {
                "status": "published",
                "latest_json": str(latest_path),
                "archive_json": str(archive_path),
                "scoreboard_json": str(scoreboard_path),
                "publication_status_json": str(status_path),
                "target_delivery_date": latest_payload["target_delivery_date"],
                "model_count": len(latest_payload["models"]),
                "claim_boundary": latest_payload["claim_boundary"],
                "market_execution_enabled": latest_payload["market_execution_enabled"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _fetch_history_rows(
    *,
    target_delivery_date: date,
    history_days: int,
) -> list[dict[str, Any]]:
    if history_days < 8:
        raise ValueError("--history-days must be at least 8 for strict similar-day coverage")
    rows: list[dict[str, Any]] = []
    fetch_errors: list[str] = []
    start_day = target_delivery_date - timedelta(days=history_days)
    end_day = target_delivery_date - timedelta(days=1)
    current_day = start_day
    while current_day <= end_day:
        try:
            rows.extend(fetch_oree_dam_price_rows(current_day))
        except Exception as error:
            fetch_errors.append(f"{current_day.isoformat()}: {type(error).__name__}: {error}")
        current_day += timedelta(days=1)
    if not rows:
        raise ValueError("no source-backed OREE history rows fetched; " + "; ".join(fetch_errors))
    return rows


def _blocked_forecast_payload(
    *,
    target_delivery_date: date,
    generated_at: datetime,
    error: Exception,
) -> dict[str, Any]:
    return {
        "schema_version": "ukraine_bess_forecast_challenge.v1",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "target_delivery_date": target_delivery_date.isoformat(),
        "source": {
            "history_row_count": 0,
            "training_cutoff": None,
            "source_scope": "source_backed_observed_oree_history_only",
            "source_status": "blocked_no_complete_history",
            "blocker_type": type(error).__name__,
            "blocker": str(error),
        },
        "models": [],
        "claim_boundary": PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "proposed_bid_status": "not_emitted",
    }


def _forecast_archive_filename(payload: Mapping[str, Any]) -> str:
    target = str(payload.get("target_delivery_date") or "unknown")
    generated = str(payload.get("generated_at") or datetime.now(UTC).isoformat())
    safe_generated = (
        generated
        .replace(":", "")
        .replace("-", "")
        .replace("+0000", "Z")
        .replace("+00:00", "Z")
    )
    return f"forecast_{target}_generated_{safe_generated}.json"


def _read_input_history_rows(path: Path) -> list[dict[str, Any]]:
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
    expected_target_delivery_date: date,
    attempted_target_delivery_date: date,
    latest_payload: Mapping[str, Any],
    latest_path: Path,
    scoreboard_path: Path,
    error_payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    source = latest_payload.get("source") if isinstance(latest_payload.get("source"), Mapping) else {}
    actual_target_date = str(latest_payload.get("target_delivery_date") or "")
    model_statuses = [
        {
            "model_name": str(model.get("model_name") or ""),
            "backend_status": str(model.get("backend_status") or ""),
            "quality_boundary": str(model.get("quality_boundary") or ""),
            "point_count": int(model.get("point_count") or 0),
        }
        for model in latest_payload.get("models", [])
        if isinstance(model, Mapping)
    ]
    payload: dict[str, Any] = {
        "schema_version": "ukraine_bess_publication_status.v1",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "publisher_timezone": "Europe/Kyiv",
        "forecast": {
            "status": status,
            "expected_target_delivery_date": expected_target_delivery_date.isoformat(),
            "attempted_target_delivery_date": attempted_target_delivery_date.isoformat(),
            "actual_target_delivery_date": actual_target_date,
            "is_current_for_kyiv_schedule": actual_target_date == expected_target_delivery_date.isoformat(),
            "history_row_count": int(source.get("history_row_count") or 0),
            "training_cutoff": source.get("training_cutoff"),
            "model_statuses": model_statuses,
        },
        "artifacts": {
            "forecast_latest_json": _posix_path(latest_path),
            "forecast_scoreboard_json": _posix_path(scoreboard_path),
        },
        "autonomy": {
            "compute_layer": "github_actions_scheduled_static_json",
            "host_layer": "vercel_git_auto_deploy",
            "schedule_cron_utc": "35 5 * * *",
            "schedule_timezone_note": "05:35 UTC is 08:35 Europe/Kyiv during summer time",
            "market_execution_enabled": False,
            "proposed_bid_status": "not_emitted",
        },
    }
    if error_payload:
        payload["forecast"]["last_error"] = dict(error_payload)
    return payload


def _posix_path(path: Path) -> str:
    return path.as_posix()


def _delivery_date(value: str | None) -> date:
    if value is None:
        raise ValueError("delivery date is required")
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


if __name__ == "__main__":
    raise SystemExit(main())
