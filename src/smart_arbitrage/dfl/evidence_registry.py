"""Evidence registry helpers for DFL vector materialization runs."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    DNIPRO_TENANT_ID,
    PromotionGateResult,
    evaluate_offline_dfl_promotion_gate,
    evaluate_strategy_promotion_gate,
)
from smart_arbitrage.dfl.regret_weighted import HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND
from smart_arbitrage.strategy.ensemble_gate import (
    CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
    RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
)
from smart_arbitrage.strategy.forecast_strategy_evaluation import REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND

VECTOR_COLUMNS: Final[tuple[str, ...]] = (
    "forecast_price_vector_uah_mwh",
    "actual_price_vector_uah_mwh",
    "candidate_dispatch_vector_mw",
    "baseline_dispatch_vector_mw",
    "candidate_degradation_penalty_vector_uah",
    "baseline_degradation_penalty_vector_uah",
)
REQUIRED_TRAINING_VECTOR_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "training_example_id",
        "tenant_id",
        "anchor_timestamp",
        "horizon_hours",
        "forecast_model_name",
        "baseline_forecast_model_name",
        "data_quality_tier",
        "observed_coverage_ratio",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
        "generated_at",
        *VECTOR_COLUMNS,
    }
)
DEFAULT_CANDIDATE_MODEL_NAMES_BY_STRATEGY_KIND: Final[dict[str, tuple[str, ...]]] = {
    REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND: (
        "tft_silver_v0",
        "nbeatsx_silver_v0",
    ),
    HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND: (
        "tft_horizon_regret_weighted_calibrated_v0",
        "nbeatsx_horizon_regret_weighted_calibrated_v0",
    ),
}
DEFAULT_SELECTOR_STRATEGY_KINDS: Final[tuple[str, ...]] = (
    CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
    RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
)


def build_dfl_vector_evidence_registry(
    *,
    run_slug: str,
    training_example_frame: pl.DataFrame,
    evaluation_frames_by_strategy_kind: dict[str, pl.DataFrame],
    selector_frames_by_strategy_kind: dict[str, pl.DataFrame] | None = None,
    offline_dfl_experiment_frame: pl.DataFrame | None = None,
    candidate_model_names_by_strategy_kind: dict[str, tuple[str, ...]] | None = None,
    tenant_id: str = DNIPRO_TENANT_ID,
    min_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> dict[str, Any]:
    """Build a concise registry from persisted DFL vectors and latest strategy rows."""

    candidate_names = candidate_model_names_by_strategy_kind or DEFAULT_CANDIDATE_MODEL_NAMES_BY_STRATEGY_KIND
    promotion_gate_results = [
        _promotion_result_dict(
            strategy_kind=strategy_kind,
            candidate_model_name=candidate_model_name,
            result=_evaluate_candidate(
                evaluation_frames_by_strategy_kind.get(strategy_kind, pl.DataFrame()),
                candidate_model_name=candidate_model_name,
                tenant_id=tenant_id,
                min_anchor_count=min_anchor_count,
                min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
            ),
        )
        for strategy_kind, model_names in candidate_names.items()
        for candidate_model_name in model_names
    ]
    offline_gate_result = None
    if offline_dfl_experiment_frame is not None and offline_dfl_experiment_frame.height > 0:
        offline_gate_result = _promotion_result_dict(
            strategy_kind="offline_dfl_experiment",
            candidate_model_name="offline_dfl_experiment",
            result=evaluate_offline_dfl_promotion_gate(
                offline_dfl_experiment_frame,
                tenant_id=tenant_id,
                min_validation_anchor_count=min_anchor_count,
                min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
            ),
        )

    return {
        "run_slug": run_slug,
        "tenant_id": tenant_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "claim_boundary": {
            "claim_scope": "dfl_vector_evidence_and_promotion_gate_not_full_dfl",
            "not_full_dfl": True,
            "not_market_execution": True,
            "strict_baseline": "strict_similar_day",
        },
        "training_vector_summary": summarize_dfl_training_example_vectors(
            training_example_frame,
            tenant_id=tenant_id,
            min_anchor_count=min_anchor_count,
        ),
        "promotion_gate_results": promotion_gate_results,
        "selector_summaries": _selector_summaries(
            selector_frames_by_strategy_kind or {},
            tenant_id=tenant_id,
        ),
        "offline_dfl_gate_result": offline_gate_result,
        "overall_promotion_decision": _overall_promotion_decision(promotion_gate_results),
    }


def summarize_dfl_training_example_vectors(
    training_example_frame: pl.DataFrame,
    *,
    tenant_id: str = DNIPRO_TENANT_ID,
    min_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> dict[str, Any]:
    """Summarize and validate the latest DFL training-example vector batch."""

    failures = _missing_column_failures(training_example_frame, REQUIRED_TRAINING_VECTOR_COLUMNS)
    if failures:
        return _vector_summary_result(failures=failures, metrics={"tenant_id": tenant_id})
    latest_frame = _latest_tenant_frame(training_example_frame, tenant_id=tenant_id)
    if latest_frame.height == 0:
        return _vector_summary_result(
            failures=[f"tenant_id={tenant_id} has no DFL training-example vector rows"],
            metrics={"tenant_id": tenant_id},
        )

    vector_length_failures = _vector_length_failures(latest_frame)
    data_quality_tiers = _sorted_strings(latest_frame["data_quality_tier"].to_list())
    claim_scopes = _sorted_strings(latest_frame["claim_scope"].to_list())
    observed_coverage_min = float(latest_frame.select(pl.min("observed_coverage_ratio")).item())
    anchor_count = latest_frame.select("anchor_timestamp").n_unique()
    forecast_model_count = latest_frame.select("forecast_model_name").n_unique()
    latest_generated_at = _datetime_value(latest_frame.select(pl.max("generated_at")).item(), field_name="generated_at")

    failures.extend(vector_length_failures)
    if anchor_count < min_anchor_count:
        failures.append(f"anchor_count must be at least {min_anchor_count}; observed {anchor_count}")
    if data_quality_tiers != ["thesis_grade"]:
        failures.append(f"DFL vectors require thesis_grade rows; observed {data_quality_tiers}")
    if observed_coverage_min < 1.0:
        failures.append(f"DFL vectors require observed coverage ratio of 1.0; observed {observed_coverage_min:.3f}")
    if not all(bool(value) for value in latest_frame["not_full_dfl"].to_list()):
        failures.append("DFL vectors must remain not_full_dfl")
    if not all(bool(value) for value in latest_frame["not_market_execution"].to_list()):
        failures.append("DFL vectors must remain not_market_execution")
    if "strict_similar_day" not in set(str(value) for value in latest_frame["forecast_model_name"].to_list()):
        failures.append("DFL vectors must include strict_similar_day control examples")
    if set(str(value) for value in latest_frame["baseline_forecast_model_name"].to_list()) != {"strict_similar_day"}:
        failures.append("DFL vectors must join strict_similar_day as the baseline for every row")

    return _vector_summary_result(
        failures=failures,
        metrics={
            "tenant_id": tenant_id,
            "latest_generated_at": latest_generated_at.isoformat(),
            "training_example_row_count": latest_frame.height,
            "anchor_count": anchor_count,
            "forecast_model_count": forecast_model_count,
            "data_quality_tiers": data_quality_tiers,
            "claim_scopes": claim_scopes,
            "observed_coverage_min": observed_coverage_min,
            "not_full_dfl": all(bool(value) for value in latest_frame["not_full_dfl"].to_list()),
            "not_market_execution": all(bool(value) for value in latest_frame["not_market_execution"].to_list()),
            "vector_lengths": _vector_length_summary(latest_frame),
        },
    )


def load_dfl_training_example_vectors_from_postgres(
    dsn: str,
    *,
    tenant_id: str = DNIPRO_TENANT_ID,
) -> pl.DataFrame:
    """Load the latest persisted DFL vector batch for one tenant."""

    from psycopg import connect
    from psycopg.rows import dict_row

    with connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM dfl_training_example_vectors
                WHERE tenant_id = %s
                  AND generated_at = (
                      SELECT max(generated_at)
                      FROM dfl_training_example_vectors
                      WHERE tenant_id = %s
                  )
                ORDER BY anchor_timestamp, forecast_model_name
                """,
                (tenant_id, tenant_id),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    return pl.DataFrame([_normalize_vector_row(row) for row in rows])


def write_dfl_vector_evidence_registry(
    registry: dict[str, Any],
    *,
    output_root: Path,
    run_slug: str,
) -> Path:
    """Write concise local registry artifacts for supervisor evidence review."""

    export_dir = output_root / run_slug
    export_dir.mkdir(parents=True, exist_ok=True)
    (export_dir / "dfl_vector_evidence_registry.json").write_text(
        json.dumps(_jsonable(registry), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / "dfl_vector_evidence_registry.md").write_text(
        _markdown_registry(registry),
        encoding="utf-8",
    )
    return export_dir


def _evaluate_candidate(
    evaluation_frame: pl.DataFrame,
    *,
    candidate_model_name: str,
    tenant_id: str,
    min_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> PromotionGateResult:
    if evaluation_frame.height == 0:
        return PromotionGateResult(
            passed=False,
            decision="block",
            description="strategy evidence frame has no latest-batch rows",
            metrics={
                "tenant_id": tenant_id,
                "candidate_model_name": candidate_model_name,
                "candidate_anchor_count": 0,
            },
        )
    return evaluate_strategy_promotion_gate(
        evaluation_frame,
        candidate_model_name=candidate_model_name,
        tenant_id=tenant_id,
        min_anchor_count=min_anchor_count,
        min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
    )


def _promotion_result_dict(
    *,
    strategy_kind: str,
    candidate_model_name: str,
    result: PromotionGateResult,
) -> dict[str, Any]:
    return {
        "strategy_kind": strategy_kind,
        "candidate_model_name": candidate_model_name,
        "passed": result.passed,
        "decision": result.decision,
        "description": result.description,
        "metrics": _jsonable(result.metrics),
    }


def _selector_summaries(
    selector_frames_by_strategy_kind: dict[str, pl.DataFrame],
    *,
    tenant_id: str,
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for strategy_kind in DEFAULT_SELECTOR_STRATEGY_KINDS:
        frame = selector_frames_by_strategy_kind.get(strategy_kind, pl.DataFrame())
        latest_frame = _latest_tenant_frame(frame, tenant_id=tenant_id)
        if latest_frame.height == 0:
            summaries.append(
                {
                    "strategy_kind": strategy_kind,
                    "row_count": 0,
                    "anchor_count": 0,
                    "forecast_model_names": [],
                    "latest_generated_at": None,
                }
            )
            continue
        latest_generated_at = _datetime_value(
            latest_frame.select(pl.max("generated_at")).item(),
            field_name="generated_at",
        )
        summaries.append(
            {
                "strategy_kind": strategy_kind,
                "row_count": latest_frame.height,
                "anchor_count": latest_frame.select("anchor_timestamp").n_unique(),
                "forecast_model_names": _sorted_strings(latest_frame["forecast_model_name"].to_list()),
                "latest_generated_at": latest_generated_at.isoformat(),
            }
        )
    return summaries


def _latest_tenant_frame(frame: pl.DataFrame, *, tenant_id: str) -> pl.DataFrame:
    if frame.height == 0 or "tenant_id" not in frame.columns or "generated_at" not in frame.columns:
        return pl.DataFrame()
    tenant_frame = frame.filter(pl.col("tenant_id") == tenant_id)
    if tenant_frame.height == 0:
        return pl.DataFrame()
    latest_generated_at = tenant_frame.select(pl.max("generated_at")).item()
    return tenant_frame.filter(pl.col("generated_at") == latest_generated_at)


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing_columns = sorted(required_columns.difference(frame.columns))
    return [f"frame is missing required columns: {missing_columns}"] if missing_columns else []


def _vector_length_failures(frame: pl.DataFrame) -> list[str]:
    failures: list[str] = []
    for row in frame.iter_rows(named=True):
        horizon_hours = int(row["horizon_hours"])
        for column_name in VECTOR_COLUMNS:
            vector_length = _sequence_length(row[column_name])
            if vector_length != horizon_hours:
                failures.append(
                    f"{column_name} length must match horizon_hours for {row['training_example_id']}; "
                    f"observed {vector_length} != {horizon_hours}"
                )
    return failures


def _vector_length_summary(frame: pl.DataFrame) -> dict[str, list[int]]:
    return {
        column_name: sorted({_sequence_length(row[column_name]) for row in frame.iter_rows(named=True)})
        for column_name in VECTOR_COLUMNS
    }


def _sequence_length(value: Any) -> int:
    if isinstance(value, list | tuple):
        return len(value)
    if isinstance(value, str):
        loaded = json.loads(value)
        if isinstance(loaded, list):
            return len(loaded)
    return -1


def _vector_summary_result(*, failures: list[str], metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "passed": not failures,
        "decision": "pass" if not failures else "fail",
        "description": "DFL vector evidence passed." if not failures else "; ".join(failures),
        **_jsonable(metrics),
    }


def _overall_promotion_decision(results: list[dict[str, Any]]) -> str:
    if any(bool(result["passed"]) for result in results):
        return "candidate_promoted_for_research_review"
    return "no_candidate_promoted"


def _normalize_vector_row(row: dict[str, Any]) -> dict[str, Any]:
    for column_name in VECTOR_COLUMNS:
        value = row.get(column_name)
        if isinstance(value, str):
            loaded = json.loads(value)
            row[column_name] = loaded if isinstance(loaded, list) else value
    return row


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"{field_name} must be a datetime value.")


def _sorted_strings(values: list[Any]) -> list[str]:
    return sorted({str(value) for value in values})


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value


def _markdown_registry(registry: dict[str, Any]) -> str:
    vector_summary = registry["training_vector_summary"]
    gate_rows = "\n".join(
        (
            f"| {row['strategy_kind']} | {row['candidate_model_name']} | "
            f"{row['decision']} | {row['description']} |"
        )
        for row in registry["promotion_gate_results"]
    )
    if not gate_rows:
        gate_rows = "| none | none | block | No candidate rows were evaluated. |"
    selector_rows = "\n".join(
        (
            f"| {row['strategy_kind']} | {row['row_count']} | "
            f"{row['anchor_count']} | {', '.join(row['forecast_model_names']) or 'none'} |"
        )
        for row in registry["selector_summaries"]
    )
    if not selector_rows:
        selector_rows = "| none | 0 | 0 | none |"
    return (
        "# DFL Vector Evidence Registry\n\n"
        f"- Run slug: `{registry['run_slug']}`\n"
        f"- Tenant: `{registry['tenant_id']}`\n"
        "- Claim boundary: DFL vector evidence and promotion gate only; not full DFL and not market execution.\n"
        "- Frozen control comparator: `strict_similar_day`\n\n"
        "## Training Vectors\n\n"
        f"- Decision: `{vector_summary['decision']}`\n"
        f"- Rows: `{vector_summary.get('training_example_row_count', 0)}`\n"
        f"- Anchors: `{vector_summary.get('anchor_count', 0)}`\n"
        f"- Forecast models: `{vector_summary.get('forecast_model_count', 0)}`\n"
        f"- Description: {vector_summary['description']}\n\n"
        "## Promotion Gate\n\n"
        "| Strategy kind | Candidate | Decision | Description |\n"
        "| --- | --- | --- | --- |\n"
        f"{gate_rows}\n\n"
        "## Selector Summaries\n\n"
        "| Strategy kind | Rows | Anchors | Models |\n"
        "| --- | ---: | ---: | --- |\n"
        f"{selector_rows}\n"
    )
