"""NBU EUR/UAH metadata for prior-only market-coupling features."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
import json
from typing import Final
from urllib.request import urlopen
from zoneinfo import ZoneInfo

import polars as pl

NBU_EUR_UAH_FX_METADATA_CLAIM_SCOPE: Final[str] = (
    "nbu_eur_uah_fx_metadata_research_gate"
)
NBU_EUR_UAH_RANGE_URL_TEMPLATE: Final[str] = (
    "https://bank.gov.ua/NBU_Exchange/exchange_site?"
    "start={start}&end={end}&valcode=eur&sort=exchangedate&order=asc&json"
)
NBU_EUR_UAH_SOURCE_LABEL: Final[str] = "NBU official exchange_site EUR/UAH"
REQUIRED_NBU_EUR_UAH_FX_METADATA_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "fx_rate_effective_date",
        "fx_rate_calc_date",
        "fx_rate_timestamp_utc",
        "fx_rate_eur_uah",
        "currency_pair",
        "fx_rate_source",
        "source_url",
        "source_backed",
        "currency_normalization_status",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)

FetchJsonByUrl = Callable[[str], str]


def build_nbu_eur_uah_fx_metadata_frame(
    benchmark_frame: pl.DataFrame,
    *,
    lag_hours: int = 24,
    fetch_enabled: bool = True,
    fetch_json_by_url: FetchJsonByUrl | None = None,
) -> pl.DataFrame:
    """Fetch NBU EUR/UAH rates for lagged Poland source dates.

    The effective FX date is derived from the external source timestamp
    ``benchmark_timestamp - lag_hours``. Publication metadata is represented by
    the NBU ``calcdate`` at 15:30 Europe/Kyiv, which is conservative for the
    project because the lagged feature only consumes the value on later
    Ukrainian timestamps.
    """

    missing = sorted({"timestamp"}.difference(benchmark_frame.columns))
    if missing:
        raise ValueError(f"benchmark_frame missing columns: {missing}")
    if lag_hours <= 0:
        raise ValueError("lag_hours must be positive.")
    source_dates = _required_source_dates(benchmark_frame, lag_hours=lag_hours)
    if not source_dates:
        raise ValueError("benchmark_frame must contain at least one timestamp.")

    start_date = source_dates[0]
    end_date = source_dates[-1]
    source_url = _nbu_eur_uah_range_url(start_date, end_date)
    rates_by_date = (
        _fetch_nbu_rates_by_date(source_url, fetch_json_by_url=fetch_json_by_url)
        if fetch_enabled
        else {}
    )
    rows = [
        _fx_metadata_row(
            effective_date=source_date,
            raw_rate=rates_by_date.get(source_date),
            source_url=source_url,
            fetch_enabled=fetch_enabled,
        )
        for source_date in source_dates
    ]
    return pl.DataFrame(rows, infer_schema_length=None).sort("fx_rate_effective_date")


def _required_source_dates(
    benchmark_frame: pl.DataFrame,
    *,
    lag_hours: int,
) -> list[date]:
    timestamps = (
        benchmark_frame.select("timestamp").unique().sort("timestamp").to_series().to_list()
    )
    return sorted(
        {
            (_timestamp_naive_utc(timestamp) - timedelta(hours=lag_hours)).date()
            for timestamp in timestamps
        }
    )


def _fetch_nbu_rates_by_date(
    source_url: str,
    *,
    fetch_json_by_url: FetchJsonByUrl | None,
) -> dict[date, dict[str, object]]:
    raw_json = (
        fetch_json_by_url(source_url)
        if fetch_json_by_url is not None
        else _fetch_text(source_url)
    )
    parsed = json.loads(raw_json)
    rows = parsed if isinstance(parsed, list) else [parsed]
    rates_by_date: dict[date, dict[str, object]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("cc", "")).upper() != "EUR":
            continue
        exchangedate = str(row.get("exchangedate", "")).strip()
        if not exchangedate:
            continue
        rates_by_date[_parse_ddmmyyyy_date(exchangedate)] = row
    return rates_by_date


def _fx_metadata_row(
    *,
    effective_date: date,
    raw_rate: dict[str, object] | None,
    source_url: str,
    fetch_enabled: bool,
) -> dict[str, object]:
    if raw_rate is None:
        return {
            "fx_rate_effective_date": effective_date.isoformat(),
            "fx_rate_calc_date": "",
            "fx_rate_timestamp_utc": "",
            "fx_rate_eur_uah": None,
            "currency_pair": "EUR/UAH",
            "fx_rate_source": "",
            "source_url": source_url,
            "source_backed": False,
            "fetch_enabled": fetch_enabled,
            "currency_normalization_status": "blocked_missing_nbu_eur_uah_rate",
            "claim_scope": NBU_EUR_UAH_FX_METADATA_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
        }

    calc_date_text = str(raw_rate.get("calcdate", "")).strip()
    calc_date = (
        _parse_ddmmyyyy_date(calc_date_text)
        if calc_date_text
        else effective_date - timedelta(days=1)
    )
    publication_timestamp = _nbu_publication_timestamp_utc(calc_date)
    rate = raw_rate.get("rate_per_unit", raw_rate.get("rate"))
    return {
        "fx_rate_effective_date": effective_date.isoformat(),
        "fx_rate_calc_date": calc_date.isoformat(),
        "fx_rate_timestamp_utc": publication_timestamp.isoformat(),
        "fx_rate_eur_uah": float(str(rate)),
        "currency_pair": "EUR/UAH",
        "fx_rate_source": NBU_EUR_UAH_SOURCE_LABEL,
        "source_url": source_url,
        "source_backed": True,
        "fetch_enabled": fetch_enabled,
        "currency_normalization_status": "prior_eur_uah_normalized",
        "claim_scope": NBU_EUR_UAH_FX_METADATA_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _nbu_publication_timestamp_utc(calc_date: date) -> datetime:
    local_timestamp = datetime.combine(
        calc_date,
        time(hour=15, minute=30),
        ZoneInfo("Europe/Kyiv"),
    )
    return local_timestamp.astimezone(UTC)


def _nbu_eur_uah_range_url(start_date: date, end_date: date) -> str:
    return NBU_EUR_UAH_RANGE_URL_TEMPLATE.format(
        start=start_date.strftime("%Y%m%d"),
        end=end_date.strftime("%Y%m%d"),
    )


def _fetch_text(url: str) -> str:
    with urlopen(url, timeout=60) as response:  # noqa: S310 - URL is NBU API.
        return response.read().decode("utf-8")


def _parse_ddmmyyyy_date(value: str) -> date:
    return datetime.strptime(value, "%d.%m.%Y").date()


def _timestamp_naive_utc(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
    parsed = datetime.fromisoformat(str(value))
    return parsed.astimezone(UTC).replace(tzinfo=None) if parsed.tzinfo else parsed


__all__ = [
    "NBU_EUR_UAH_FX_METADATA_CLAIM_SCOPE",
    "REQUIRED_NBU_EUR_UAH_FX_METADATA_COLUMNS",
    "build_nbu_eur_uah_fx_metadata_frame",
]
