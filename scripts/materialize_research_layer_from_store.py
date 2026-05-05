from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

from smart_arbitrage.research.real_data_research_layer import (
    build_research_layer_outputs,
    load_latest_real_data_benchmark_frame_from_postgres,
    persist_research_layer_outputs,
    write_research_layer_exports,
)
from smart_arbitrage.resources.dfl_training_store import PostgresDflTrainingStore
from smart_arbitrage.resources.strategy_evaluation_store import PostgresStrategyEvaluationStore

DEFAULT_LOCAL_DSN = "postgresql://smart:arbitrage@localhost:5432/smart_arbitrage"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Materialize downstream real-data research outputs from persisted benchmark rows."
    )
    parser.add_argument("--dsn", default=DEFAULT_LOCAL_DSN)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=f"research_layer_{datetime.now().strftime('%Y%m%dT%H%M%S')}")
    parser.add_argument("--pilot-tenant-id", default="client_003_dnipro_factory")
    parser.add_argument("--pilot-model-name", default="tft_silver_v0")
    parser.add_argument("--validation-fraction", type=float, default=0.2)
    args = parser.parse_args()

    benchmark_frame = load_latest_real_data_benchmark_frame_from_postgres(args.dsn)
    if benchmark_frame.height == 0:
        raise SystemExit("No persisted real-data benchmark rows found. Run the benchmark before this script.")

    outputs = build_research_layer_outputs(
        benchmark_frame,
        pilot_tenant_id=args.pilot_tenant_id,
        pilot_model_name=args.pilot_model_name,
        validation_fraction=args.validation_fraction,
    )
    persist_research_layer_outputs(
        outputs,
        strategy_store=PostgresStrategyEvaluationStore(args.dsn),
        dfl_store=PostgresDflTrainingStore(args.dsn),
    )
    export_dir = write_research_layer_exports(
        outputs,
        output_root=args.output_root,
        run_slug=args.run_slug,
    )
    summary = {
        "export_dir": str(export_dir),
        "benchmark_rows": outputs.benchmark_frame.height,
        "ensemble_rows": outputs.ensemble_frame.height,
        "dfl_training_rows": outputs.dfl_training_frame.height,
        "pilot_rows": outputs.pilot_frame.height,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
