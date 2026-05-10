from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from smart_arbitrage.dfl.evidence_registry import (
    CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
    DEFAULT_SELECTOR_STRATEGY_KINDS,
    DNIPRO_TENANT_ID,
    HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
    REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND,
    RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
    build_dfl_vector_evidence_registry,
    load_dfl_training_example_vectors_from_postgres,
    write_dfl_vector_evidence_registry,
)
from smart_arbitrage.resources.strategy_evaluation_store import PostgresStrategyEvaluationStore

DEFAULT_LOCAL_DSN = "postgresql://smart:arbitrage@localhost:5432/smart_arbitrage"
DEFAULT_RUN_SLUG = "week3_dfl_vector_evidence_dnipro_90"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Materialize a concise DFL vector evidence and promotion-gate registry."
    )
    parser.add_argument("--dsn", default=DEFAULT_LOCAL_DSN)
    parser.add_argument("--tenant-id", default=DNIPRO_TENANT_ID)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--min-anchor-count", type=int, default=90)
    parser.add_argument("--min-mean-regret-improvement-ratio", type=float, default=0.05)
    args = parser.parse_args()

    strategy_store = PostgresStrategyEvaluationStore(args.dsn)
    training_example_frame = load_dfl_training_example_vectors_from_postgres(
        args.dsn,
        tenant_id=args.tenant_id,
    )
    evaluation_frames = {
        REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND: strategy_store.latest_real_data_benchmark_frame(
            tenant_id=args.tenant_id
        ),
        HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND: strategy_store.latest_strategy_kind_frame(
            tenant_id=args.tenant_id,
            strategy_kind=HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
        ),
    }
    selector_frames = {
        CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND: strategy_store.latest_strategy_kind_frame(
            tenant_id=args.tenant_id,
            strategy_kind=CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
        ),
        RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND: strategy_store.latest_strategy_kind_frame(
            tenant_id=args.tenant_id,
            strategy_kind=RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
        ),
    }

    registry = build_dfl_vector_evidence_registry(
        run_slug=args.run_slug,
        tenant_id=args.tenant_id,
        training_example_frame=training_example_frame,
        evaluation_frames_by_strategy_kind=evaluation_frames,
        selector_frames_by_strategy_kind=selector_frames,
        min_anchor_count=args.min_anchor_count,
        min_mean_regret_improvement_ratio=args.min_mean_regret_improvement_ratio,
    )
    export_dir = write_dfl_vector_evidence_registry(
        registry,
        output_root=args.output_root,
        run_slug=args.run_slug,
    )
    summary = {
        "export_dir": str(export_dir),
        "registry_json": str(export_dir / "dfl_vector_evidence_registry.json"),
        "registry_markdown": str(export_dir / "dfl_vector_evidence_registry.md"),
        "overall_promotion_decision": registry["overall_promotion_decision"],
        "training_vector_decision": registry["training_vector_summary"]["decision"],
        "training_vector_rows": registry["training_vector_summary"].get("training_example_row_count", 0),
        "training_vector_anchors": registry["training_vector_summary"].get("anchor_count", 0),
        "promotion_gate_decisions": [
            {
                "strategy_kind": row["strategy_kind"],
                "candidate_model_name": row["candidate_model_name"],
                "decision": row["decision"],
            }
            for row in registry["promotion_gate_results"]
        ],
        "selector_strategy_kinds": list(DEFAULT_SELECTOR_STRATEGY_KINDS),
    }
    json.dump(summary, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
