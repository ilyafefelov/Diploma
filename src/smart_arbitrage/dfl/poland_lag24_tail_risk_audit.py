"""Tail-risk audit for Poland lag-24 schedule/value near misses."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any, Final

import polars as pl

TAIL_RISK_SUMMARY_JSON: Final[str] = "poland_lag24_tail_risk_summary.json"
TAIL_RISK_SUMMARY_MARKDOWN: Final[str] = "poland_lag24_tail_risk_summary.md"
TAIL_RISK_ROWS_CSV: Final[str] = "poland_lag24_tail_risk_rows.csv"
TAIL_RISK_BY_TENANT_CSV: Final[str] = "poland_lag24_tail_risk_by_tenant.csv"
TAIL_RISK_TOP_FAILURES_CSV: Final[str] = "poland_lag24_tail_risk_top_failures.csv"

REQUIRED_STRICT_ROW_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "anchor_timestamp",
        "forecast_model_name",
        "regret_uah",
        "decision_value_uah",
        "oracle_value_uah",
        "committed_action",
        "committed_power_mw",
        "total_throughput_mwh",
        "total_degradation_penalty_uah",
        "evaluation_payload",
    }
)


def build_poland_lag24_tail_risk_audit_frame(
    *,
    baseline_frame: pl.DataFrame,
    challenger_frame: pl.DataFrame,
    baseline_model_name: str,
    challenger_model_name: str,
    tail_loss_quantile: float = 0.8,
) -> pl.DataFrame:
    """Compare frozen V2+ against the best Poland-enhanced challenger row by row."""

    if not 0.0 <= tail_loss_quantile <= 1.0:
        raise ValueError("tail_loss_quantile must be in [0.0, 1.0].")
    _validate_research_rows(baseline_frame, frame_name="baseline_frame")
    _validate_research_rows(challenger_frame, frame_name="challenger_frame")
    baseline_rows = _model_rows(
        baseline_frame,
        model_name=baseline_model_name,
        frame_name="baseline_frame",
    )
    challenger_rows = _model_rows(
        challenger_frame,
        model_name=challenger_model_name,
        frame_name="challenger_frame",
    )
    joined = _joined_rows(
        baseline_rows,
        challenger_rows,
        baseline_model_name=baseline_model_name,
        challenger_model_name=challenger_model_name,
    )
    if not joined:
        raise ValueError("tail-risk audit found no overlapping tenant/anchor rows.")
    positive_deltas = sorted(
        row["delta_regret_uah"] for row in joined if row["delta_regret_uah"] > 1e-6
    )
    tail_loss_threshold = (
        _linear_quantile(positive_deltas, tail_loss_quantile)
        if positive_deltas
        else math.inf
    )
    for row in joined:
        row["tail_loss_threshold_uah"] = (
            None if math.isinf(tail_loss_threshold) else tail_loss_threshold
        )
        row["tail_risk_class"] = _tail_risk_class(
            row["delta_regret_uah"],
            threshold=tail_loss_threshold,
        )
    return pl.DataFrame(joined).sort(["delta_regret_uah", "tenant_id"])


def build_poland_lag24_tail_risk_packet(
    *,
    run_slug: str,
    audit_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
) -> dict[str, Any]:
    """Build a local evidence packet for the Poland lag-24 tail-risk audit."""

    if audit_frame.is_empty():
        raise ValueError("tail-risk packet requires non-empty audit rows.")
    summary = _summary(audit_frame)
    by_tenant = _by_tenant(audit_frame)
    top_failures = _top_failures(audit_frame)
    top_wins = _top_wins(audit_frame)
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "no_dashboard_or_api_default_switch": True,
            "strict_fallback": "strict_similar_day",
            "external_feature_role": (
                "point_in_time_poland_lag24_exogenous_columns_only"
            ),
            "no_european_training_rows": True,
            "oracle_loss_avoidance_is_diagnostic_only": True,
        },
        "summary": summary,
        "by_tenant": _frame_rows(by_tenant),
        "top_tail_failures": _frame_rows(top_failures),
        "top_wins": _frame_rows(top_wins),
        "interpretation": {
            "why_the_gate_failed": (
                "The Poland-enhanced calibrated TFT V2+ challenger improved the "
                "median row but a small set of high-regret losses outweighed "
                "those local wins in the mean regret gate."
            ),
            "why_not_use_all_poland_schedules": (
                "Selecting every Poland-enhanced schedule would import those "
                "tail losses. The only lower-regret blend here is an oracle-only "
                "diagnostic that uses final outcomes to avoid bad rows, so it is "
                "not admissible for promotion."
            ),
            "next_research_step": (
                "Train or validate a prior-only tail-risk veto over the Poland "
                "feature route, then rerun the unchanged strict LP/oracle gate."
            ),
        },
        "attached_artifacts": {
            "summary_json": TAIL_RISK_SUMMARY_JSON,
            "summary_markdown": TAIL_RISK_SUMMARY_MARKDOWN,
            "audit_rows_csv": TAIL_RISK_ROWS_CSV,
            "by_tenant_csv": TAIL_RISK_BY_TENANT_CSV,
            "top_failures_csv": TAIL_RISK_TOP_FAILURES_CSV,
        },
    }


def write_poland_lag24_tail_risk_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    audit_frame: pl.DataFrame,
) -> Path:
    """Write JSON, Markdown, and CSV artifacts for local evidence review."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    audit_frame.write_csv(export_dir / TAIL_RISK_ROWS_CSV)
    _by_tenant(audit_frame).write_csv(export_dir / TAIL_RISK_BY_TENANT_CSV)
    _top_failures(audit_frame).write_csv(export_dir / TAIL_RISK_TOP_FAILURES_CSV)
    (export_dir / TAIL_RISK_SUMMARY_JSON).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / TAIL_RISK_SUMMARY_MARKDOWN).write_text(
        _markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _validate_research_rows(frame: pl.DataFrame, *, frame_name: str) -> None:
    if frame.is_empty():
        raise ValueError(f"{frame_name} must not be empty.")
    missing_columns = sorted(REQUIRED_STRICT_ROW_COLUMNS.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing columns: {missing_columns}")
    if "market_execution_enabled" in frame.columns and frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError(f"{frame_name} refuses market execution claims.")
    if "not_market_execution" in frame.columns and not frame.select(
        pl.col("not_market_execution").all()
    ).item():
        raise ValueError(f"{frame_name} refuses non-research rows.")


def _model_rows(
    frame: pl.DataFrame,
    *,
    model_name: str,
    frame_name: str,
) -> pl.DataFrame:
    rows = (
        frame.filter(pl.col("forecast_model_name") == model_name)
        .with_columns(
            _anchor_datetime_expr().alias("anchor_dt"),
            pl.col("tenant_id").cast(pl.Utf8),
        )
        .sort(["tenant_id", "anchor_dt"])
    )
    if rows.is_empty():
        raise ValueError(f"{frame_name} has no rows for model `{model_name}`.")
    duplicates = rows.group_by(["tenant_id", "anchor_dt"]).len().filter(
        pl.col("len") > 1
    )
    if not duplicates.is_empty():
        raise ValueError(f"{frame_name} has duplicate tenant/anchor model rows.")
    return rows


def _anchor_datetime_expr() -> pl.Expr:
    return (
        pl.col("anchor_timestamp")
        .cast(pl.Utf8)
        .str.replace("T", " ")
        .str.replace(r"\+00:00$", "")
        .str.strptime(pl.Datetime, strict=False)
    )


def _joined_rows(
    baseline_rows: pl.DataFrame,
    challenger_rows: pl.DataFrame,
    *,
    baseline_model_name: str,
    challenger_model_name: str,
) -> list[dict[str, Any]]:
    baseline_by_key = {
        (str(row["tenant_id"]), row["anchor_dt"]): row
        for row in baseline_rows.iter_rows(named=True)
    }
    challenger_by_key = {
        (str(row["tenant_id"]), row["anchor_dt"]): row
        for row in challenger_rows.iter_rows(named=True)
    }
    shared_keys = sorted(set(baseline_by_key).intersection(challenger_by_key))
    rows: list[dict[str, Any]] = []
    for tenant_id, anchor_dt in shared_keys:
        baseline = baseline_by_key[(tenant_id, anchor_dt)]
        challenger = challenger_by_key[(tenant_id, anchor_dt)]
        baseline_payload = _payload_dict(baseline.get("evaluation_payload"))
        challenger_payload = _payload_dict(challenger.get("evaluation_payload"))
        baseline_horizon = _horizon_metrics(baseline_payload)
        challenger_horizon = _horizon_metrics(challenger_payload)
        baseline_regret = _safe_float(baseline["regret_uah"])
        challenger_regret = _safe_float(challenger["regret_uah"])
        delta_regret = challenger_regret - baseline_regret
        rows.append(
            {
                "tenant_id": tenant_id,
                "anchor_timestamp": anchor_dt,
                "baseline_model_name": baseline_model_name,
                "challenger_model_name": challenger_model_name,
                "baseline_regret_uah": baseline_regret,
                "challenger_regret_uah": challenger_regret,
                "delta_regret_uah": delta_regret,
                "outcome_class": _outcome_class(delta_regret),
                "baseline_decision_value_uah": _safe_float(
                    baseline["decision_value_uah"]
                ),
                "challenger_decision_value_uah": _safe_float(
                    challenger["decision_value_uah"]
                ),
                "oracle_value_uah": _safe_float(challenger["oracle_value_uah"]),
                "baseline_committed_action": _safe_str(baseline["committed_action"]),
                "challenger_committed_action": _safe_str(
                    challenger["committed_action"]
                ),
                "baseline_committed_power_mw": _safe_float(
                    baseline["committed_power_mw"]
                ),
                "challenger_committed_power_mw": _safe_float(
                    challenger["committed_power_mw"]
                ),
                "action_changed": _action_changed(baseline, challenger),
                "baseline_total_throughput_mwh": _safe_float(
                    baseline["total_throughput_mwh"]
                ),
                "challenger_total_throughput_mwh": _safe_float(
                    challenger["total_throughput_mwh"]
                ),
                "throughput_delta_mwh": _safe_float(
                    challenger["total_throughput_mwh"]
                )
                - _safe_float(baseline["total_throughput_mwh"]),
                "baseline_total_degradation_penalty_uah": _safe_float(
                    baseline["total_degradation_penalty_uah"]
                ),
                "challenger_total_degradation_penalty_uah": _safe_float(
                    challenger["total_degradation_penalty_uah"]
                ),
                "degradation_delta_uah": _safe_float(
                    challenger["total_degradation_penalty_uah"]
                )
                - _safe_float(baseline["total_degradation_penalty_uah"]),
                "baseline_candidate_family": _payload_text(
                    baseline_payload,
                    "candidate_family",
                ),
                "challenger_candidate_family": _payload_text(
                    challenger_payload,
                    "candidate_family",
                ),
                "baseline_weight_profile": _payload_text(
                    baseline_payload,
                    "selected_weight_profile_name",
                ),
                "challenger_weight_profile": _payload_text(
                    challenger_payload,
                    "selected_weight_profile_name",
                ),
                "challenger_source_quantile": _payload_text(
                    challenger_payload,
                    "source_quantile",
                ),
                "challenger_quantile_spread_scale": _payload_float(
                    challenger_payload,
                    "quantile_spread_scale",
                ),
                **_horizon_delta_columns(
                    baseline_horizon=baseline_horizon,
                    challenger_horizon=challenger_horizon,
                ),
            }
        )
    return rows


def _payload_dict(raw_payload: Any) -> dict[str, Any]:
    if isinstance(raw_payload, dict):
        return raw_payload
    if isinstance(raw_payload, str):
        try:
            parsed = json.loads(raw_payload)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _horizon_metrics(payload: dict[str, Any]) -> dict[str, float | int | None]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list) or not horizon:
        return _empty_horizon_metrics()
    actual_prices: list[float] = []
    forecast_prices: list[float] = []
    powers: list[float] = []
    for item in horizon:
        if not isinstance(item, dict):
            continue
        actual_prices.append(_safe_float(item.get("actual_price_uah_mwh")))
        forecast_prices.append(_safe_float(item.get("forecast_price_uah_mwh")))
        powers.append(_safe_float(item.get("net_power_mw")))
    if not actual_prices or not forecast_prices:
        return _empty_horizon_metrics()
    actual_peak_step = _argmax(actual_prices)
    forecast_peak_step = _argmax(forecast_prices)
    actual_trough_step = _argmin(actual_prices)
    forecast_trough_step = _argmin(forecast_prices)
    return {
        "actual_peak_step": actual_peak_step,
        "forecast_peak_step": forecast_peak_step,
        "actual_trough_step": actual_trough_step,
        "forecast_trough_step": forecast_trough_step,
        "peak_step_error": abs(forecast_peak_step - actual_peak_step),
        "trough_step_error": abs(forecast_trough_step - actual_trough_step),
        "actual_spread_uah_mwh": max(actual_prices) - min(actual_prices),
        "forecast_spread_uah_mwh": max(forecast_prices) - min(forecast_prices),
        "absolute_dispatch_mwh": sum(abs(power) for power in powers),
    }


def _empty_horizon_metrics() -> dict[str, float | int | None]:
    return {
        "actual_peak_step": None,
        "forecast_peak_step": None,
        "actual_trough_step": None,
        "forecast_trough_step": None,
        "peak_step_error": None,
        "trough_step_error": None,
        "actual_spread_uah_mwh": None,
        "forecast_spread_uah_mwh": None,
        "absolute_dispatch_mwh": None,
    }


def _horizon_delta_columns(
    *,
    baseline_horizon: dict[str, float | int | None],
    challenger_horizon: dict[str, float | int | None],
) -> dict[str, float | int | None]:
    baseline_peak_error = baseline_horizon["peak_step_error"]
    challenger_peak_error = challenger_horizon["peak_step_error"]
    baseline_trough_error = baseline_horizon["trough_step_error"]
    challenger_trough_error = challenger_horizon["trough_step_error"]
    baseline_forecast_spread = baseline_horizon["forecast_spread_uah_mwh"]
    challenger_forecast_spread = challenger_horizon["forecast_spread_uah_mwh"]
    return {
        "actual_peak_step": challenger_horizon["actual_peak_step"],
        "actual_trough_step": challenger_horizon["actual_trough_step"],
        "baseline_forecast_peak_step": baseline_horizon["forecast_peak_step"],
        "challenger_forecast_peak_step": challenger_horizon["forecast_peak_step"],
        "baseline_forecast_trough_step": baseline_horizon["forecast_trough_step"],
        "challenger_forecast_trough_step": challenger_horizon["forecast_trough_step"],
        "baseline_peak_step_error": baseline_peak_error,
        "challenger_peak_step_error": challenger_peak_error,
        "peak_step_error_delta": _optional_delta(
            challenger_peak_error,
            baseline_peak_error,
        ),
        "baseline_trough_step_error": baseline_trough_error,
        "challenger_trough_step_error": challenger_trough_error,
        "trough_step_error_delta": _optional_delta(
            challenger_trough_error,
            baseline_trough_error,
        ),
        "actual_spread_uah_mwh": challenger_horizon["actual_spread_uah_mwh"],
        "baseline_forecast_spread_uah_mwh": baseline_forecast_spread,
        "challenger_forecast_spread_uah_mwh": challenger_forecast_spread,
        "forecast_spread_delta_uah_mwh": _optional_delta(
            challenger_forecast_spread,
            baseline_forecast_spread,
        ),
        "baseline_absolute_dispatch_mwh": baseline_horizon["absolute_dispatch_mwh"],
        "challenger_absolute_dispatch_mwh": challenger_horizon[
            "absolute_dispatch_mwh"
        ],
    }


def _summary(audit_frame: pl.DataFrame) -> dict[str, Any]:
    rows = _frame_rows(audit_frame)
    baseline_regrets = [float(row["baseline_regret_uah"]) for row in rows]
    challenger_regrets = [float(row["challenger_regret_uah"]) for row in rows]
    deltas = [float(row["delta_regret_uah"]) for row in rows]
    oracle_regrets = [
        min(float(row["baseline_regret_uah"]), float(row["challenger_regret_uah"]))
        for row in rows
    ]
    tail_deltas = [
        float(row["delta_regret_uah"])
        for row in rows
        if row["tail_risk_class"] == "tail_loss"
    ]
    return {
        "row_count": len(rows),
        "tenant_count": audit_frame.select(pl.col("tenant_id").n_unique()).item(),
        "anchor_count": audit_frame.select(pl.col("anchor_timestamp").n_unique()).item(),
        "wins": sum(1 for delta in deltas if delta < -1e-6),
        "losses": sum(1 for delta in deltas if delta > 1e-6),
        "ties": sum(1 for delta in deltas if abs(delta) <= 1e-6),
        "baseline_mean_regret_uah": _mean(baseline_regrets),
        "challenger_mean_regret_uah": _mean(challenger_regrets),
        "baseline_median_regret_uah": _median(baseline_regrets),
        "challenger_median_regret_uah": _median(challenger_regrets),
        "mean_delta_regret_uah": _mean(deltas),
        "median_delta_regret_uah": _median(deltas),
        "total_delta_regret_uah": sum(deltas),
        "tail_loss_count": len(tail_deltas),
        "tail_loss_total_delta_uah": sum(tail_deltas),
        "tail_loss_share_of_positive_loss_delta": _safe_ratio(
            sum(tail_deltas),
            sum(delta for delta in deltas if delta > 0.0),
        ),
        "oracle_loss_avoidance_mean_regret_uah": _mean(oracle_regrets),
        "oracle_loss_avoidance_delta_vs_baseline_mean_uah": _mean(oracle_regrets)
        - _mean(baseline_regrets),
        "oracle_loss_avoidance_delta_vs_challenger_mean_uah": _mean(oracle_regrets)
        - _mean(challenger_regrets),
        "oracle_loss_avoidance_is_diagnostic_only": True,
    }


def _by_tenant(audit_frame: pl.DataFrame) -> pl.DataFrame:
    return (
        audit_frame.group_by("tenant_id")
        .agg(
            [
                pl.len().alias("row_count"),
                (pl.col("delta_regret_uah") < -1e-6).sum().alias("wins"),
                (pl.col("delta_regret_uah") > 1e-6).sum().alias("losses"),
                (pl.col("tail_risk_class") == "tail_loss").sum().alias(
                    "tail_loss_count"
                ),
                pl.mean("delta_regret_uah").alias("mean_delta_regret_uah"),
                pl.sum("delta_regret_uah").alias("total_delta_regret_uah"),
                pl.max("delta_regret_uah").alias("max_loss_delta_uah"),
                pl.median("challenger_regret_uah").alias(
                    "challenger_median_regret_uah"
                ),
                pl.median("baseline_regret_uah").alias("baseline_median_regret_uah"),
            ]
        )
        .sort("total_delta_regret_uah", descending=True)
    )


def _top_failures(audit_frame: pl.DataFrame, *, limit: int = 12) -> pl.DataFrame:
    return (
        audit_frame.filter(pl.col("delta_regret_uah") > 1e-6)
        .sort("delta_regret_uah", descending=True)
        .head(limit)
    )


def _top_wins(audit_frame: pl.DataFrame, *, limit: int = 12) -> pl.DataFrame:
    return (
        audit_frame.filter(pl.col("delta_regret_uah") < -1e-6)
        .sort("delta_regret_uah")
        .head(limit)
    )


def _markdown(packet: dict[str, Any]) -> str:
    summary = packet["summary"]
    lines = [
        "# Poland Lag-24 Tail-Risk Audit",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Dagster run: `{packet.get('dagster_run_id')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet is Offline Strategy Promotion evidence only: "
        "`market_execution_enabled=false`, no dashboard/API default switch, "
        "no live dispatch, and no European rows in Ukrainian training.",
        "",
        "## Result",
        "",
        (
            "- Challenger mean regret: "
            f"{summary['challenger_mean_regret_uah']:.2f} UAH"
        ),
        (
            "- Frozen V2+ mean regret on matched rows: "
            f"{summary['baseline_mean_regret_uah']:.2f} UAH"
        ),
        (
            "- Mean delta versus frozen V2+: "
            f"{summary['mean_delta_regret_uah']:.2f} UAH"
        ),
        (
            "- Median delta versus frozen V2+: "
            f"{summary['median_delta_regret_uah']:.2f} UAH"
        ),
        f"- Wins/losses/ties: {summary['wins']} / {summary['losses']} / {summary['ties']}",
        (
            "- Tail-loss rows: "
            f"{summary['tail_loss_count']} rows, "
            f"{summary['tail_loss_total_delta_uah']:.2f} UAH total delta"
        ),
        (
            "- Oracle-only diagnostic mean regret if bad Poland rows fell back to "
            f"V2+: {summary['oracle_loss_avoidance_mean_regret_uah']:.2f} UAH"
        ),
        "",
        "The oracle-only diagnostic is not promotion evidence because it uses "
        "final outcomes to decide when to avoid Poland-enhanced schedules.",
        "",
        "## By Tenant",
        "",
        "| Tenant | Rows | Wins | Losses | Tail losses | Mean delta | Max loss |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in packet["by_tenant"]:
        lines.append(
            "| {tenant_id} | {row_count} | {wins} | {losses} | "
            "{tail_loss_count} | {mean_delta_regret_uah:.2f} | "
            "{max_loss_delta_uah:.2f} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Top Tail Failures",
            "",
            "| Tenant | Anchor | V2+ regret | Poland regret | Delta | Candidate family |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for row in packet["top_tail_failures"]:
        lines.append(
            "| {tenant_id} | {anchor_timestamp} | {baseline_regret_uah:.2f} | "
            "{challenger_regret_uah:.2f} | {delta_regret_uah:.2f} | "
            "{challenger_candidate_family} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            packet["interpretation"]["why_the_gate_failed"],
            "",
            packet["interpretation"]["why_not_use_all_poland_schedules"],
            "",
            packet["interpretation"]["next_research_step"],
            "",
        ]
    )
    return "\n".join(lines)


def _outcome_class(delta_regret_uah: float) -> str:
    if delta_regret_uah < -1e-6:
        return "poland_improved"
    if delta_regret_uah > 1e-6:
        return "poland_worse"
    return "tie"


def _tail_risk_class(delta_regret_uah: float, *, threshold: float) -> str:
    if delta_regret_uah <= 1e-6:
        return "not_loss"
    if delta_regret_uah >= threshold:
        return "tail_loss"
    return "ordinary_loss"


def _linear_quantile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("cannot calculate quantile for an empty list.")
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def _action_changed(baseline: dict[str, Any], challenger: dict[str, Any]) -> bool:
    if _safe_str(baseline["committed_action"]) != _safe_str(
        challenger["committed_action"]
    ):
        return True
    return (
        abs(
            _safe_float(challenger["committed_power_mw"])
            - _safe_float(baseline["committed_power_mw"])
        )
        > 1e-3
    )


def _payload_text(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    return str(value)


def _payload_float(payload: dict[str, Any], key: str) -> float | None:
    value = payload.get(key)
    if value is None:
        return None
    return _safe_float(value)


def _argmax(values: list[float]) -> int:
    return max(range(len(values)), key=values.__getitem__)


def _argmin(values: list[float]) -> int:
    return min(range(len(values)), key=values.__getitem__)


def _optional_delta(
    challenger_value: float | int | None,
    baseline_value: float | int | None,
) -> float | None:
    if challenger_value is None or baseline_value is None:
        return None
    return float(challenger_value) - float(baseline_value)


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


def _frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return [dict(row) for row in frame.iter_rows(named=True)]


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value
