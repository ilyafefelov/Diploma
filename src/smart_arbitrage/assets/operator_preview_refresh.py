"""Dagster automation for warming source-backed operator preview rows."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
from dagster import OpExecutionContext

from smart_arbitrage.assets.bronze.market_weather import list_available_weather_tenants
from smart_arbitrage.research.operator_preview_refresh import (
    OPERATOR_PREVIEW_FORECAST_MAX_HORIZON_HOURS,
    ensure_operator_preview_window,
)
from smart_arbitrage.resources.forecast_store import get_forecast_store
from smart_arbitrage.resources.market_data_store import get_market_data_store


OPERATOR_PREVIEW_REFRESH_TIMEZONE = ZoneInfo("Europe/Kyiv")


@dg.op(
    config_schema={
        "tenant_ids_csv": dg.Field(str, default_value=""),
        "market_venues_csv": dg.Field(str, default_value="DAM,IDM"),
        "target_days_ahead": dg.Field(int, default_value=1),
        "target_delivery_date": dg.Field(dg.Noneable(str), default_value=None),
    }
)
def operator_preview_cache_warm_op(
    context: OpExecutionContext,
) -> list[dict[str, object]]:
    config = context.op_config
    target_delivery_date = (
        datetime.fromisoformat(str(config["target_delivery_date"])).date()
        if config["target_delivery_date"]
        else (datetime.now(OPERATOR_PREVIEW_REFRESH_TIMEZONE).date() + timedelta(days=int(config["target_days_ahead"])))
    )
    rows: list[dict[str, object]] = []
    for tenant_id in _tenant_ids_from_csv(str(config["tenant_ids_csv"])):
        for market_venue in _market_venues_from_csv(str(config["market_venues_csv"])):
            result = ensure_operator_preview_window(
                market_data_store=get_market_data_store(),
                forecast_store=get_forecast_store(),
                tenant_id=tenant_id,
                market_venue=market_venue,
                target_delivery_date=target_delivery_date,
                cache_horizon_hours=OPERATOR_PREVIEW_FORECAST_MAX_HORIZON_HOURS,
            )
            row = {
                "tenant_id": result.tenant_id,
                "market_venue": result.market_venue,
                "target_delivery_date": result.target_delivery_date.isoformat(),
                "status": result.status,
                "stage": result.stage,
                "source_refresh_rows": result.source_refresh_rows,
                "forecast_rows": result.forecast_rows,
                "market_execution_enabled": result.market_execution_enabled,
                "message": result.message,
            }
            rows.append(row)
            if result.status not in {"ready", "materialized"}:
                context.log.warning(
                    "Operator preview cache warm blocked for %s %s: %s",
                    result.tenant_id,
                    result.market_venue,
                    result.message,
                )
    context.add_output_metadata(
        {
            "target_delivery_date": target_delivery_date.isoformat(),
            "rows": len(rows),
            "materialized_or_ready": sum(1 for row in rows if row["status"] in {"ready", "materialized"}),
            "market_execution_enabled": False,
            "read_model_boundary": "operator_preview_no_market_submission",
        }
    )
    return rows


@dg.job(name="operator_preview_cache_warm")
def operator_preview_cache_warm_job() -> None:
    operator_preview_cache_warm_op()


operator_preview_cache_warm_schedule = dg.ScheduleDefinition(
    name="operator_preview_cache_warm_schedule",
    job=operator_preview_cache_warm_job,
    cron_schedule="10 14 * * *",
    execution_timezone="Europe/Kyiv",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description=(
        "Stopped-by-default daily warm job that refreshes source-backed OREE rows when available "
        "and caches a bounded 168-hour operator-preview forecast horizon."
    ),
)


OPERATOR_PREVIEW_REFRESH_JOBS = [operator_preview_cache_warm_job]
OPERATOR_PREVIEW_REFRESH_SCHEDULES = [operator_preview_cache_warm_schedule]


def _tenant_ids_from_csv(value: str) -> list[str]:
    tenant_ids = [item.strip() for item in value.split(",") if item.strip()]
    if tenant_ids:
        return tenant_ids
    return [
        str(tenant["tenant_id"])
        for tenant in list_available_weather_tenants()
        if tenant.get("tenant_id") is not None
    ]


def _market_venues_from_csv(value: str) -> list[str]:
    venues = [item.strip().upper() for item in value.split(",") if item.strip()]
    return [venue for venue in venues if venue in {"DAM", "IDM"}] or ["DAM", "IDM"]


__all__ = [
    "OPERATOR_PREVIEW_REFRESH_JOBS",
    "OPERATOR_PREVIEW_REFRESH_SCHEDULES",
    "operator_preview_cache_warm_job",
    "operator_preview_cache_warm_op",
    "operator_preview_cache_warm_schedule",
]
