from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

import polars as pl

from smart_arbitrage.dfl.offline_experiment import build_offline_dfl_panel_experiment_frame
from smart_arbitrage.dfl.promotion_gate import evaluate_offline_dfl_panel_development_gate
from smart_arbitrage.resources.strategy_evaluation_store import PostgresStrategyEvaluationStore

DEFAULT_LOCAL_DSN = "postgresql://smart:arbitrage@localhost:5432/smart_arbitrage"
DEFAULT_RUN_SLUG = "week3_offline_dfl_panel_v2_90"
DEFAULT_TENANT_IDS = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
DEFAULT_FORECAST_MODEL_NAMES = ("tft_silver_v0", "nbeatsx_silver_v0")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export concise offline DFL panel evidence from latest Postgres rows.")
    parser.add_argument("--dsn", default=DEFAULT_LOCAL_DSN)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--tenant-ids-csv", default=",".join(DEFAULT_TENANT_IDS))
    parser.add_argument("--forecast-model-names-csv", default=",".join(DEFAULT_FORECAST_MODEL_NAMES))
    parser.add_argument("--final-validation-anchor-count-per-tenant", type=int, default=18)
    parser.add_argument("--max-train-anchors-per-tenant", type=int, default=72)
    parser.add_argument("--inner-validation-fraction", type=float, default=0.2)
    parser.add_argument("--epoch-count", type=int, default=8)
    parser.add_argument("--learning-rate", type=float, default=10.0)
    args = parser.parse_args()

    tenant_ids = _csv_values(args.tenant_ids_csv, field_name="tenant_ids_csv")
    forecast_model_names = _csv_values(args.forecast_model_names_csv, field_name="forecast_model_names_csv")
    store = PostgresStrategyEvaluationStore(args.dsn)
    source_frame = _load_latest_benchmark_panel(store, tenant_ids=tenant_ids)
    panel_frame = build_offline_dfl_panel_experiment_frame(
        source_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=args.final_validation_anchor_count_per_tenant,
        max_train_anchors_per_tenant=args.max_train_anchors_per_tenant,
        inner_validation_fraction=args.inner_validation_fraction,
        epoch_count=args.epoch_count,
        learning_rate=args.learning_rate,
    )
    gate = evaluate_offline_dfl_panel_development_gate(panel_frame)
    export_dir = args.output_root / args.run_slug
    export_dir.mkdir(parents=True, exist_ok=True)
    panel_path = export_dir / "offline_dfl_panel_experiment_frame.json"
    registry_path = export_dir / "offline_dfl_panel_evidence_registry.json"
    markdown_path = export_dir / "offline_dfl_panel_evidence_registry.md"
    panel_frame.write_json(panel_path)
    registry = _build_registry(
        run_slug=args.run_slug,
        source_frame=source_frame,
        panel_frame=panel_frame,
        gate_metrics=gate.metrics,
        gate_decision=gate.decision,
        gate_description=gate.description,
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
    )
    registry_path.write_text(json.dumps(registry, indent=2, default=str), encoding="utf-8")
    markdown_path.write_text(_registry_markdown(registry), encoding="utf-8")
    json.dump(
        {
            "export_dir": str(export_dir),
            "panel_json": str(panel_path),
            "registry_json": str(registry_path),
            "registry_markdown": str(markdown_path),
            "development_gate_decision": gate.decision,
            "validation_tenant_anchor_count": gate.metrics.get("validation_tenant_anchor_count", 0),
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")


def _load_latest_benchmark_panel(
    store: PostgresStrategyEvaluationStore,
    *,
    tenant_ids: tuple[str, ...],
) -> pl.DataFrame:
    frames = [store.latest_real_data_benchmark_frame(tenant_id=tenant_id) for tenant_id in tenant_ids]
    frames = [frame for frame in frames if frame.height > 0]
    if not frames:
        raise ValueError("No latest real-data benchmark rows found for the requested tenants.")
    return pl.concat(frames, how="diagonal_relaxed")


def _build_registry(
    *,
    run_slug: str,
    source_frame: pl.DataFrame,
    panel_frame: pl.DataFrame,
    gate_metrics: dict[str, Any],
    gate_decision: str,
    gate_description: str,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
) -> dict[str, Any]:
    generated_batches = (
        source_frame
        .group_by("tenant_id")
        .agg(
            [
                pl.len().alias("rows"),
                pl.n_unique("anchor_timestamp").alias("anchor_count"),
                pl.max("generated_at").alias("latest_generated_at"),
            ]
        )
        .sort("tenant_id")
    )
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "claim_boundary": {
            "claim_scope": "offline_dfl_panel_experiment_not_full_dfl",
            "not_full_dfl": True,
            "not_market_execution": True,
            "production_promotion_blocked_until_strict_lp_gate_passes": True,
        },
        "requested_scope": {
            "tenant_ids": list(tenant_ids),
            "forecast_model_names": list(forecast_model_names),
        },
        "source_benchmark_batches": generated_batches.to_dicts(),
        "panel_summary": {
            "row_count": panel_frame.height,
            "tenant_count": panel_frame.select("tenant_id").n_unique() if panel_frame.height else 0,
            "model_count": panel_frame.select("forecast_model_name").n_unique() if panel_frame.height else 0,
            "development_gate_decision": gate_decision,
            "development_gate_description": gate_description,
            **gate_metrics,
        },
    }


def _registry_markdown(registry: dict[str, Any]) -> str:
    model_summaries = registry["panel_summary"].get("model_summaries", [])
    lines = [
        "# Offline DFL Panel Evidence Registry",
        "",
        f"Run slug: `{registry['run_slug']}`",
        "",
        "## Claim Boundary",
        "",
        "- `not_full_dfl=true`",
        "- `not_market_execution=true`",
        "- Production promotion remains blocked until the strict-LP/oracle promotion gate passes.",
        "",
        "## Panel Summary",
        "",
        f"- Gate decision: `{registry['panel_summary']['development_gate_decision']}`",
        f"- Gate description: {registry['panel_summary']['development_gate_description']}",
        f"- Tenant count: {registry['panel_summary']['tenant_count']}",
        f"- Model count: {registry['panel_summary']['model_count']}",
        f"- Validation tenant-anchor count: {registry['panel_summary'].get('validation_tenant_anchor_count', 0)}",
        "",
        "| Model | Validation tenant-anchors | Raw relaxed regret | v2 relaxed regret | Improvement |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in model_summaries:
        lines.append(
            "| {model} | {anchors} | {baseline:.2f} | {v2:.2f} | {improvement:.2%} |".format(
                model=row["forecast_model_name"],
                anchors=row["validation_tenant_anchor_count"],
                baseline=row["baseline_mean_relaxed_regret_uah"],
                v2=row["v2_mean_relaxed_regret_uah"],
                improvement=row["mean_relaxed_regret_improvement_ratio"],
            )
        )
    lines.extend(["", "## Source Batches", "", "| Tenant | Rows | Anchors | Latest generated_at |", "|---|---:|---:|---|"])
    for row in registry["source_benchmark_batches"]:
        lines.append(
            f"| `{row['tenant_id']}` | {row['rows']} | {row['anchor_count']} | `{row['latest_generated_at']}` |"
        )
    lines.append("")
    return "\n".join(lines)


def _csv_values(raw_value: str, *, field_name: str) -> tuple[str, ...]:
    values = tuple(value.strip() for value in raw_value.split(",") if value.strip())
    if not values:
        raise ValueError(f"{field_name} must contain at least one value.")
    return values


if __name__ == "__main__":
    main()
