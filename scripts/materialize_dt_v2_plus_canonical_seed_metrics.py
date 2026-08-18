"""Create canonical DT/V2+ seed metrics from regret-aware selector rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from typing import Any, Sequence

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.canonical_experiment_metrics import (  # noqa: E402
    CANONICAL_EXPERIMENT_CLAIM_SCOPE,
    validate_canonical_experiment_metrics_payload,
)
from smart_arbitrage.dfl.regret_aware_v2_plus_selector import (  # noqa: E402
    FEATURE_SET_EXPANDED,
    MODEL_KIND_HIST_GRADIENT_BOOSTING,
    MODEL_KIND_RANDOM_FOREST,
    MODEL_KIND_WEIGHTED_RIDGE,
    V2_PLUS_FAMILY_ALIASES,
    build_regret_aware_v2_plus_selector_packet,
    parse_selector_vector,
    write_regret_aware_v2_plus_selector_packet,
)

DEFAULT_TEACHER_ROWS_CSV = (
    PROJECT_ROOT
    / "data"
    / "research_runs"
    / "week3_dt_v2_plus_safe_switch_selector_current"
    / "regret_aware_v2_plus_selector_teacher_rows.csv"
)
DEFAULT_SOURCE_MODEL_NAME = "nbeatsx_official_global_panel_horizon_calibrated_v1"
VECTOR_COLUMNS = (
    "forecast_price_uah_mwh_vector",
    "actual_price_uah_mwh_vector",
    "dispatch_mw_vector",
    "soc_fraction_vector",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize runs/dt_v2_plus/seed_*/metrics.json and "
            "runs/v2plus/baseline_seed_means.json from source-backed "
            "DT/V2+ teacher rows. This writes offline evidence only."
        )
    )
    parser.add_argument("--teacher-rows-csv", type=Path, default=DEFAULT_TEACHER_ROWS_CSV)
    parser.add_argument("--model-dir", type=Path, default=Path("runs/dt_v2_plus"))
    parser.add_argument(
        "--baseline-seed-means-json",
        type=Path,
        default=Path("runs/v2plus/baseline_seed_means.json"),
    )
    parser.add_argument("--seeds", nargs="+", type=int, default=[42, 2026, 7])
    parser.add_argument("--source-model-name", default=DEFAULT_SOURCE_MODEL_NAME)
    parser.add_argument("--run-slug-prefix", default="dt_v2_plus_canonical")
    parser.add_argument("--model-kind", default=MODEL_KIND_RANDOM_FOREST)
    parser.add_argument("--feature-set", default=FEATURE_SET_EXPANDED)
    parser.add_argument("--min-predicted-improvement-uah", type=float, default=20.0)
    parser.add_argument("--tail-risk-loss-threshold-uah", type=float, default=150.0)
    parser.add_argument("--max-family-tail-risk-probability", type=float, default=0.5)
    parser.add_argument("--ridge-l2", type=float, default=10.0)
    parser.add_argument("--git-commit", default=None)
    args = parser.parse_args(argv)

    seeds = _unique_seeds(args.seeds)
    teacher_rows = _read_teacher_rows(
        args.teacher_rows_csv,
        source_model_name=args.source_model_name,
    )
    vector_parse_summary = _vector_parse_summary(teacher_rows)
    baseline_mean = _v2_plus_final_holdout_mean_regret(teacher_rows)
    args.baseline_seed_means_json.parent.mkdir(parents=True, exist_ok=True)
    args.baseline_seed_means_json.write_text(
        json.dumps([baseline_mean for _ in seeds], indent=2) + "\n",
        encoding="utf-8",
    )

    git_commit = args.git_commit or _git_commit()
    seed_metrics_paths: list[Path] = []
    evaluation_independence: dict[str, Any] | None = None
    for seed in seeds:
        metrics_path, seed_independence = _write_seed_metrics(
            teacher_rows=teacher_rows,
            seed=seed,
            model_dir=args.model_dir,
            run_slug=f"{args.run_slug_prefix}_seed_{seed}",
            git_commit=git_commit,
            cmd=_cmd_text(argv),
            model_kind=args.model_kind,
            feature_set=args.feature_set,
            min_predicted_improvement_uah=args.min_predicted_improvement_uah,
            tail_risk_loss_threshold_uah=args.tail_risk_loss_threshold_uah,
            max_family_tail_risk_probability=args.max_family_tail_risk_probability,
            ridge_l2=args.ridge_l2,
        )
        seed_metrics_paths.append(metrics_path)
        if evaluation_independence is None:
            evaluation_independence = seed_independence
        elif evaluation_independence != seed_independence:
            raise ValueError("seed runs produced inconsistent evaluation independence audits.")
    if evaluation_independence is None:
        raise ValueError("canonical seed metrics require at least one seed result.")

    manifest = {
        "claim_scope": CANONICAL_EXPERIMENT_CLAIM_SCOPE,
        "model_dir": str(args.model_dir),
        "baseline_seed_means_json": str(args.baseline_seed_means_json),
        "baseline_mean_regret": baseline_mean,
        "seeds": seeds,
        "seed_metrics": [str(path) for path in seed_metrics_paths],
        "teacher_rows_csv": str(args.teacher_rows_csv),
        "source_model_name": args.source_model_name,
        "vector_parse_summary": vector_parse_summary,
        "estimator_class": args.model_kind,
        "model": _model_name(args.model_kind),
        "artifact_identifier": "dt_v2_plus",
        "evaluation_independence": evaluation_independence,
        "inference_valid": bool(evaluation_independence["independent_holdout"]),
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
    }
    manifest_path = args.model_dir / "canonical_seed_metrics_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    json.dump({**manifest, "manifest_json": str(manifest_path)}, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


def _read_teacher_rows(path: Path, *, source_model_name: str) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pl.read_csv(path, infer_schema_length=1000)
    if source_model_name:
        frame = frame.filter(pl.col("source_model_name") == source_model_name)
    if frame.is_empty():
        raise ValueError(f"No teacher rows found for source model: {source_model_name}")
    return frame


def _write_seed_metrics(
    *,
    teacher_rows: pl.DataFrame,
    seed: int,
    model_dir: Path,
    run_slug: str,
    git_commit: str,
    cmd: str,
    model_kind: str,
    feature_set: str,
    min_predicted_improvement_uah: float,
    tail_risk_loss_threshold_uah: float,
    max_family_tail_risk_probability: float,
    ridge_l2: float,
) -> tuple[Path, dict[str, Any]]:
    started = time.perf_counter()
    result = build_regret_aware_v2_plus_selector_packet(
        teacher_rows,
        run_slug=run_slug,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
        ridge_l2=ridge_l2,
        model_kind=model_kind,
        feature_set=feature_set,
        random_seed=seed,
    )
    seed_dir = model_dir / f"seed_{seed}"
    artifact_dir = seed_dir / "artifacts"
    write_regret_aware_v2_plus_selector_packet(output_dir=artifact_dir, result=result)
    selected_rows = result["selected_rows"]
    if not isinstance(selected_rows, pl.DataFrame):
        raise TypeError("selector result selected_rows must be a Polars DataFrame.")
    regrets = _float_column(selected_rows, "selected_regret_uah")
    if not regrets:
        raise ValueError("selector result did not produce selected final-holdout rows.")
    regret_mean = statistics.fmean(regrets)
    regret_std = statistics.pstdev(regrets) if len(regrets) > 1 else 0.0
    elapsed_s = time.perf_counter() - started
    independence = dict(result["summary"]["evaluation_independence"])
    payload = validate_canonical_experiment_metrics_payload(
        {
            "run_id": run_slug,
            "model": _model_name(model_kind),
            "estimator_class": model_kind,
            "artifact_identifier": "dt_v2_plus",
            "independent_holdout": bool(independence["independent_holdout"]),
            "evaluation_content_overlap_ratio": float(
                independence["content_overlap_ratio"]
            ),
            "seed": seed,
            "epoch": 0,
            "val_regret_mean": regret_mean,
            "val_regret_std": regret_std,
            "test_regret_mean": regret_mean,
            "test_regret_std": regret_std,
            "early_stop_epoch": 0,
            "wall_time_s": elapsed_s,
            "gpu_hours": 0.0,
            "max_mem_gb": 0.0,
            "git_commit": git_commit,
            "cmd": cmd,
            "claim_scope": CANONICAL_EXPERIMENT_CLAIM_SCOPE,
        }
    )
    metrics_path = seed_dir / "metrics.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return metrics_path, independence


def _model_name(model_kind: str) -> str:
    model_names = {
        MODEL_KIND_WEIGHTED_RIDGE: "weighted_ridge_v2_plus_safe_switch",
        MODEL_KIND_HIST_GRADIENT_BOOSTING: (
            "hist_gradient_boosting_v2_plus_safe_switch"
        ),
        MODEL_KIND_RANDOM_FOREST: "random_forest_v2_plus_safe_switch",
    }
    if model_kind not in model_names:
        raise ValueError(f"unsupported selector model kind: {model_kind}")
    return model_names[model_kind]


def _v2_plus_final_holdout_mean_regret(teacher_rows: pl.DataFrame) -> float:
    baseline_rows = teacher_rows.filter(
        (pl.col("split_name") == "final_holdout")
        & pl.col("dt_schedule_family_target").is_in(sorted(V2_PLUS_FAMILY_ALIASES))
    )
    if baseline_rows.is_empty():
        raise ValueError("No final_holdout V2+ baseline rows found in teacher rows.")
    return statistics.fmean(_float_column(baseline_rows, "regret_uah"))


def _vector_parse_summary(frame: pl.DataFrame) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for column in VECTOR_COLUMNS:
        if column not in frame.columns:
            continue
        values = frame.get_column(column).to_list()
        parsed_lengths = [len(parse_selector_vector(value)) for value in values]
        summary[column] = {
            "row_count": len(parsed_lengths),
            "non_empty_vector_count": sum(1 for length in parsed_lengths if length > 0),
            "max_vector_length": max(parsed_lengths, default=0),
        }
    return summary


def _float_column(frame: pl.DataFrame, column: str) -> list[float]:
    if column not in frame.columns:
        raise ValueError(f"missing required numeric column: {column}")
    values: list[float] = []
    for value in frame.get_column(column).to_list():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{column} must contain numeric values.")
        values.append(float(value))
    return values


def _unique_seeds(seeds: Sequence[int]) -> list[int]:
    normalized: list[int] = []
    for seed in seeds:
        if isinstance(seed, bool) or seed < 0:
            raise ValueError("seeds must be non-negative integers.")
        if seed in normalized:
            raise ValueError("seeds must be unique.")
        normalized.append(int(seed))
    if not normalized:
        raise ValueError("at least one seed is required.")
    return normalized


def _git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    commit = result.stdout.strip()
    return commit or "unknown"


def _cmd_text(argv: Sequence[str] | None) -> str:
    if argv is None:
        parts = ["python", "scripts/materialize_dt_v2_plus_canonical_seed_metrics.py"]
        parts.extend(sys.argv[1:])
        return " ".join(parts)
    parts = ["python", "scripts/materialize_dt_v2_plus_canonical_seed_metrics.py"]
    parts.extend(str(part) for part in argv)
    return " ".join(parts)


if __name__ == "__main__":
    raise SystemExit(main())
