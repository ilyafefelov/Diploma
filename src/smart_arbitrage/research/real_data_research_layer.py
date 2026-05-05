from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import polars as pl

from smart_arbitrage.dfl.regret_weighted import (
    build_horizon_regret_weighted_forecast_calibration_frame,
    build_horizon_regret_weighted_forecast_strategy_benchmark_frame,
    build_regret_weighted_forecast_calibration_frame,
    build_regret_weighted_forecast_strategy_benchmark_frame,
    run_regret_weighted_dfl_pilot,
)
from smart_arbitrage.resources.dfl_training_store import (
    DflTrainingStore,
    get_dfl_training_store,
)
from smart_arbitrage.resources.strategy_evaluation_store import (
    StrategyEvaluationStore,
    get_strategy_evaluation_store,
)
from smart_arbitrage.strategy.ensemble_gate import build_value_aware_ensemble_frame
from smart_arbitrage.training.dfl_training import build_dfl_training_frame

REAL_DATA_BENCHMARK_STRATEGY_KIND = "real_data_rolling_origin_benchmark"


@dataclass(frozen=True, slots=True)
class ResearchLayerOutputs:
    benchmark_frame: pl.DataFrame
    ensemble_frame: pl.DataFrame
    dfl_training_frame: pl.DataFrame
    pilot_frame: pl.DataFrame
    regret_weighted_calibration_frame: pl.DataFrame
    regret_weighted_benchmark_frame: pl.DataFrame
    horizon_regret_weighted_calibration_frame: pl.DataFrame
    horizon_regret_weighted_benchmark_frame: pl.DataFrame
    model_summary: pl.DataFrame
    regret_weighted_model_summary: pl.DataFrame
    horizon_regret_weighted_model_summary: pl.DataFrame
    dfl_training_summary: pl.DataFrame


def select_latest_real_data_benchmark_frame(raw_frame: pl.DataFrame) -> pl.DataFrame:
    """Keep the latest persisted real-data benchmark batch for each tenant."""

    required_columns = {"tenant_id", "strategy_kind", "generated_at", "anchor_timestamp", "forecast_model_name"}
    missing_columns = required_columns.difference(raw_frame.columns)
    if missing_columns:
        raise ValueError(f"raw_frame is missing required columns: {sorted(missing_columns)}")
    benchmark_frame = raw_frame.filter(pl.col("strategy_kind") == REAL_DATA_BENCHMARK_STRATEGY_KIND)
    if benchmark_frame.height == 0:
        return benchmark_frame
    latest_by_tenant = benchmark_frame.group_by("tenant_id").agg(
        pl.max("generated_at").alias("_latest_generated_at")
    )
    return (
        benchmark_frame
        .join(latest_by_tenant, on="tenant_id")
        .filter(pl.col("generated_at") == pl.col("_latest_generated_at"))
        .drop("_latest_generated_at")
        .sort(["tenant_id", "anchor_timestamp", "forecast_model_name"])
    )


def build_research_layer_outputs(
    benchmark_frame: pl.DataFrame,
    *,
    pilot_tenant_id: str,
    pilot_model_name: str,
    validation_fraction: float = 0.2,
    calibration_min_prior_anchors: int = 14,
    calibration_window_anchors: int = 28,
) -> ResearchLayerOutputs:
    """Build ensemble, DFL-ready examples, pilot result, and summaries."""

    if benchmark_frame.height == 0:
        raise ValueError("benchmark_frame must contain the latest real-data benchmark rows.")
    ensemble_frame = build_value_aware_ensemble_frame(benchmark_frame)
    combined_frame = pl.concat([benchmark_frame, ensemble_frame], how="diagonal_relaxed")
    training_frame = build_dfl_training_frame(combined_frame, require_thesis_grade=True)
    pilot_frame = run_regret_weighted_dfl_pilot(
        training_frame,
        tenant_id=pilot_tenant_id,
        forecast_model_name=pilot_model_name,
        validation_fraction=validation_fraction,
    )
    calibration_frame = build_regret_weighted_forecast_calibration_frame(
        training_frame,
        min_prior_anchors=calibration_min_prior_anchors,
        rolling_calibration_window_anchors=calibration_window_anchors,
    )
    regret_weighted_benchmark_frame = build_regret_weighted_forecast_strategy_benchmark_frame(
        benchmark_frame,
        calibration_frame,
    )
    horizon_calibration_frame = build_horizon_regret_weighted_forecast_calibration_frame(
        benchmark_frame,
        min_prior_anchors=calibration_min_prior_anchors,
        rolling_calibration_window_anchors=calibration_window_anchors,
    )
    horizon_regret_weighted_benchmark_frame = (
        build_horizon_regret_weighted_forecast_strategy_benchmark_frame(
            benchmark_frame,
            horizon_calibration_frame,
        )
    )
    return ResearchLayerOutputs(
        benchmark_frame=benchmark_frame,
        ensemble_frame=ensemble_frame,
        dfl_training_frame=training_frame,
        pilot_frame=pilot_frame,
        regret_weighted_calibration_frame=calibration_frame,
        regret_weighted_benchmark_frame=regret_weighted_benchmark_frame,
        horizon_regret_weighted_calibration_frame=horizon_calibration_frame,
        horizon_regret_weighted_benchmark_frame=horizon_regret_weighted_benchmark_frame,
        model_summary=_model_summary(combined_frame, training_frame),
        regret_weighted_model_summary=_strategy_model_summary(regret_weighted_benchmark_frame),
        horizon_regret_weighted_model_summary=_strategy_model_summary(
            horizon_regret_weighted_benchmark_frame
        ),
        dfl_training_summary=_dfl_training_summary(training_frame),
    )


def load_latest_real_data_benchmark_frame_from_postgres(dsn: str) -> pl.DataFrame:
    """Read latest persisted real-data benchmark rows per tenant from Postgres."""

    from psycopg import connect
    from psycopg.rows import dict_row

    with connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH latest_tenant_batches AS (
                    SELECT tenant_id, max(generated_at) AS latest_generated_at
                    FROM forecast_strategy_evaluations
                    WHERE strategy_kind = %s
                    GROUP BY tenant_id
                )
                SELECT evaluations.*
                FROM forecast_strategy_evaluations AS evaluations
                JOIN latest_tenant_batches AS latest
                  ON evaluations.tenant_id = latest.tenant_id
                 AND evaluations.generated_at = latest.latest_generated_at
                WHERE evaluations.strategy_kind = %s
                ORDER BY evaluations.tenant_id, evaluations.anchor_timestamp, evaluations.forecast_model_name
                """,
                (REAL_DATA_BENCHMARK_STRATEGY_KIND, REAL_DATA_BENCHMARK_STRATEGY_KIND),
            )
            rows = cursor.fetchall()
    return pl.DataFrame([_normalize_postgres_row(dict(row)) for row in rows])


def persist_research_layer_outputs(
    outputs: ResearchLayerOutputs,
    *,
    strategy_store: StrategyEvaluationStore | None = None,
    dfl_store: DflTrainingStore | None = None,
) -> None:
    """Persist ensemble rows and DFL research outputs."""

    (strategy_store or get_strategy_evaluation_store()).upsert_evaluation_frame(outputs.ensemble_frame)
    (strategy_store or get_strategy_evaluation_store()).upsert_evaluation_frame(
        outputs.regret_weighted_benchmark_frame
    )
    (strategy_store or get_strategy_evaluation_store()).upsert_evaluation_frame(
        outputs.horizon_regret_weighted_benchmark_frame
    )
    resolved_dfl_store = dfl_store or get_dfl_training_store()
    resolved_dfl_store.upsert_training_frame(outputs.dfl_training_frame)
    resolved_dfl_store.upsert_pilot_frame(outputs.pilot_frame)


def write_research_layer_exports(
    outputs: ResearchLayerOutputs,
    *,
    output_root: Path,
    run_slug: str,
) -> Path:
    """Write compact CSV/JSON exports for the research report."""

    output_dir = output_root / run_slug
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs.model_summary.write_csv(output_dir / "research_layer_model_summary.csv")
    outputs.regret_weighted_model_summary.write_csv(
        output_dir / "regret_weighted_benchmark_summary.csv"
    )
    outputs.horizon_regret_weighted_model_summary.write_csv(
        output_dir / "horizon_regret_weighted_benchmark_summary.csv"
    )
    _calibration_summary(outputs.regret_weighted_calibration_frame).write_csv(
        output_dir / "regret_weighted_calibration_summary.csv"
    )
    _horizon_calibration_summary(outputs.horizon_regret_weighted_calibration_frame).write_csv(
        output_dir / "horizon_regret_weighted_calibration_summary.csv"
    )
    outputs.dfl_training_summary.write_csv(output_dir / "dfl_training_summary.csv")
    outputs.pilot_frame.write_csv(output_dir / "regret_weighted_dfl_pilot_summary.csv")
    (output_dir / "regret_weighted_dfl_pilot_summary.json").write_text(
        json.dumps(_first_row(outputs.pilot_frame), indent=2, default=str),
        encoding="utf-8",
    )
    (output_dir / "research_layer_summary.json").write_text(
        json.dumps(_research_layer_summary(outputs), indent=2, default=str),
        encoding="utf-8",
    )
    return output_dir


def _model_summary(combined_frame: pl.DataFrame, training_frame: pl.DataFrame) -> pl.DataFrame:
    win_summary = _min_regret_win_summary(combined_frame)
    base_summary = (
        training_frame
        .group_by(["forecast_model_name", "strategy_kind"])
        .agg(
            [
                pl.len().alias("rows"),
                pl.col("tenant_id").n_unique().alias("tenant_count"),
                pl.col("anchor_timestamp").n_unique().alias("anchor_count"),
                pl.mean("regret_uah").alias("mean_regret_uah"),
                pl.median("regret_uah").alias("median_regret_uah"),
                pl.mean("regret_ratio").alias("mean_regret_ratio"),
                pl.mean("decision_value_uah").alias("mean_decision_value_uah"),
                pl.mean("oracle_value_uah").alias("mean_oracle_value_uah"),
                pl.mean("forecast_mae_uah_mwh").alias("mean_forecast_mae_uah_mwh"),
                pl.mean("forecast_rmse_uah_mwh").alias("mean_forecast_rmse_uah_mwh"),
                pl.mean("directional_accuracy").alias("mean_directional_accuracy"),
                pl.mean("spread_ranking_quality").alias("mean_spread_ranking_quality"),
                pl.mean("top_k_price_recall").alias("mean_top_k_price_recall"),
            ]
        )
    )
    return (
        base_summary
        .join(win_summary, on="forecast_model_name", how="left")
        .with_columns(
            [
                pl.col("wins").fill_null(0),
                pl.col("win_rate").fill_null(0.0),
            ]
        )
        .sort("mean_regret_uah")
    )


def _strategy_model_summary(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return pl.DataFrame()
    win_summary = _rank_win_summary(frame)
    return (
        frame
        .group_by(["forecast_model_name", "strategy_kind"])
        .agg(
            [
                pl.len().alias("rows"),
                pl.col("tenant_id").n_unique().alias("tenant_count"),
                pl.col("anchor_timestamp").n_unique().alias("anchor_count"),
                pl.mean("regret_uah").alias("mean_regret_uah"),
                pl.median("regret_uah").alias("median_regret_uah"),
                pl.mean("regret_ratio").alias("mean_regret_ratio"),
                pl.mean("decision_value_uah").alias("mean_decision_value_uah"),
                pl.mean("oracle_value_uah").alias("mean_oracle_value_uah"),
                pl.mean("total_degradation_penalty_uah").alias("mean_degradation_penalty_uah"),
                pl.mean("total_throughput_mwh").alias("mean_throughput_mwh"),
            ]
        )
        .join(win_summary, on="forecast_model_name", how="left")
        .with_columns(
            [
                pl.col("wins").fill_null(0),
                pl.col("win_rate").fill_null(0.0),
            ]
        )
        .sort("mean_regret_uah")
    )


def _calibration_summary(calibration_frame: pl.DataFrame) -> pl.DataFrame:
    if calibration_frame.height == 0:
        return pl.DataFrame()
    return (
        calibration_frame
        .group_by(["source_forecast_model_name", "corrected_forecast_model_name", "calibration_status"])
        .agg(
            [
                pl.len().alias("rows"),
                pl.col("tenant_id").n_unique().alias("tenant_count"),
                pl.mean("regret_weighted_bias_uah_mwh").alias("mean_bias_uah_mwh"),
                pl.median("regret_weighted_bias_uah_mwh").alias("median_bias_uah_mwh"),
                pl.max("prior_anchor_count").alias("max_prior_anchor_count"),
            ]
        )
        .sort(["source_forecast_model_name", "calibration_status"])
    )


def _horizon_calibration_summary(calibration_frame: pl.DataFrame) -> pl.DataFrame:
    if calibration_frame.height == 0:
        return pl.DataFrame()
    return (
        calibration_frame
        .group_by(["source_forecast_model_name", "corrected_forecast_model_name", "calibration_status"])
        .agg(
            [
                pl.len().alias("rows"),
                pl.col("tenant_id").n_unique().alias("tenant_count"),
                pl.mean("mean_horizon_bias_uah_mwh").alias("mean_bias_uah_mwh"),
                pl.median("mean_horizon_bias_uah_mwh").alias("median_bias_uah_mwh"),
                pl.mean("max_abs_horizon_bias_uah_mwh").alias("mean_max_abs_bias_uah_mwh"),
                pl.max("prior_anchor_count").alias("max_prior_anchor_count"),
            ]
        )
        .sort(["source_forecast_model_name", "calibration_status"])
    )


def _min_regret_win_summary(combined_frame: pl.DataFrame) -> pl.DataFrame:
    minimums = combined_frame.group_by(["tenant_id", "anchor_timestamp"]).agg(
        pl.min("regret_uah").alias("_min_anchor_regret_uah")
    )
    return (
        combined_frame
        .join(minimums, on=["tenant_id", "anchor_timestamp"])
        .with_columns(
            (pl.col("regret_uah") == pl.col("_min_anchor_regret_uah"))
            .cast(pl.Int64)
            .alias("_is_win")
        )
        .group_by("forecast_model_name")
        .agg(
            [
                pl.sum("_is_win").alias("wins"),
                pl.mean("_is_win").alias("win_rate"),
            ]
        )
    )


def _rank_win_summary(combined_frame: pl.DataFrame) -> pl.DataFrame:
    return (
        combined_frame
        .with_columns(
            (pl.col("rank_by_regret") == 1)
            .cast(pl.Int64)
            .alias("_is_win")
        )
        .group_by("forecast_model_name")
        .agg(
            [
                pl.sum("_is_win").alias("wins"),
                pl.mean("_is_win").alias("win_rate"),
            ]
        )
    )


def _dfl_training_summary(training_frame: pl.DataFrame) -> pl.DataFrame:
    return (
        training_frame
        .group_by(["forecast_model_name", "strategy_kind", "data_quality_tier"])
        .agg(
            [
                pl.len().alias("rows"),
                pl.col("tenant_id").n_unique().alias("tenant_count"),
                pl.col("anchor_timestamp").n_unique().alias("anchor_count"),
                pl.mean("regret_uah").alias("mean_regret_uah"),
                pl.mean("training_weight").alias("mean_training_weight"),
                pl.mean("total_degradation_penalty_uah").alias("mean_degradation_penalty_uah"),
                pl.mean("total_throughput_mwh").alias("mean_throughput_mwh"),
            ]
        )
        .sort("forecast_model_name")
    )


def _research_layer_summary(outputs: ResearchLayerOutputs) -> dict[str, Any]:
    pilot_row = _first_row(outputs.pilot_frame)
    expanded_to_all_tenants_ready = bool(pilot_row.get("expanded_to_all_tenants_ready", False))
    return {
        "benchmark_rows": outputs.benchmark_frame.height,
        "ensemble_rows": outputs.ensemble_frame.height,
        "dfl_training_rows": outputs.dfl_training_frame.height,
        "dfl_pilot_rows": outputs.pilot_frame.height,
        "dfl_pilot_scope": str(pilot_row.get("scope", "")),
        "regret_weighted_calibration_rows": outputs.regret_weighted_calibration_frame.height,
        "regret_weighted_benchmark_rows": outputs.regret_weighted_benchmark_frame.height,
        "horizon_regret_weighted_calibration_rows": (
            outputs.horizon_regret_weighted_calibration_frame.height
        ),
        "horizon_regret_weighted_benchmark_rows": (
            outputs.horizon_regret_weighted_benchmark_frame.height
        ),
        "dfl_expansion_recommendation": (
            "expand_to_all_tenants"
            if expanded_to_all_tenants_ready
            else "keep_as_negative_or_neutral_pilot"
        ),
        "model_summary": outputs.model_summary.to_dicts(),
        "regret_weighted_model_summary": outputs.regret_weighted_model_summary.to_dicts(),
        "horizon_regret_weighted_model_summary": (
            outputs.horizon_regret_weighted_model_summary.to_dicts()
        ),
    }


def _first_row(frame: pl.DataFrame) -> dict[str, Any]:
    if frame.height == 0:
        return {}
    return dict(frame.row(0, named=True))


def _normalize_postgres_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    if isinstance(payload, str):
        row["evaluation_payload"] = json.loads(payload)
    return row
