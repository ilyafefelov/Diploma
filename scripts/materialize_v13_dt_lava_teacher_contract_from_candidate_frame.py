"""Materialize a V13-gated DT/LAVA teacher contract from candidate rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
from typing import Any, Sequence

import polars as pl

from smart_arbitrage.dfl.ua_context_lava_dt import (
    build_dfl_ua_context_lava_sequence_training_frame,
    build_dfl_ua_context_lava_teacher_frame,
)
from smart_arbitrage.dfl.v13_dt_lava_teacher_contract import (
    DT_ACTION_TARGET_CONTRACT,
    V2_PLUS_ROLE,
    build_dfl_v13_gated_dt_lava_teacher_contract_frame,
)

CLAIM_SCOPE = "v13_dt_lava_teacher_contract_materializer_not_market_execution"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a V13-gated DT/LAVA teacher contract pickle from an existing "
            "LAVA schedule-neighbor candidate frame and V13 readiness CSV. This "
            "does not train DT/LAVA or enable market execution."
        )
    )
    parser.add_argument("--candidate-frame-pickle", type=Path, required=True)
    parser.add_argument("--readiness-csv", type=Path, required=True)
    parser.add_argument("--output-pickle", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--tail-risk-delta-uah", type=float, default=150.0)
    parser.add_argument(
        "--include-non-v13-readiness-pairs",
        action="store_true",
        help=(
            "Keep candidate rows whose tenant/source pair is absent from the "
            "V13 readiness CSV. By default those rows are dropped so the "
            "contract only describes V13-tracked sources."
        ),
    )
    args = parser.parse_args(argv)

    raw_candidate_frame = _load_polars_frame(args.candidate_frame_pickle)
    readiness_frame = pl.read_csv(args.readiness_csv, try_parse_dates=True)
    candidate_frame = (
        raw_candidate_frame
        if args.include_non_v13_readiness_pairs
        else _filter_to_v13_readiness_pairs(raw_candidate_frame, readiness_frame)
    )
    teacher_input = _ensure_teacher_label_columns(
        candidate_frame,
        tail_risk_delta_uah=args.tail_risk_delta_uah,
    )
    teacher_frame = build_dfl_ua_context_lava_teacher_frame(
        teacher_input,
        tail_risk_delta_uah=args.tail_risk_delta_uah,
    )
    sequence_frame = build_dfl_ua_context_lava_sequence_training_frame(teacher_frame)
    contract_frame = build_dfl_v13_gated_dt_lava_teacher_contract_frame(
        sequence_frame,
        readiness_frame,
    )

    args.output_pickle.parent.mkdir(parents=True, exist_ok=True)
    with args.output_pickle.open("wb") as file:
        pickle.dump(contract_frame, file)
    summary = _summary(
        contract_frame,
        args,
        candidate_rows=raw_candidate_frame.height,
        filtered_candidate_rows=candidate_frame.height,
    )
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote V13 DT/LAVA teacher contract pickle: {args.output_pickle}")
    print(f"Wrote V13 DT/LAVA teacher contract summary: {args.summary_json}")
    return 0


def _ensure_teacher_label_columns(
    frame: pl.DataFrame,
    *,
    tail_risk_delta_uah: float,
) -> pl.DataFrame:
    if tail_risk_delta_uah <= 0.0:
        raise ValueError("tail-risk-delta-uah must be positive.")
    if "market_execution_enabled" in frame.columns and frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError("Candidate frame must not contain market execution rows.")

    expressions: list[pl.Expr] = []
    if "label_safe_switch_win" not in frame.columns:
        expressions.append(
            (
                (pl.col("label_regret_delta_vs_v2_plus_uah") < 0.0)
                & (pl.col("safety_violation_count") == 0)
                & pl.col("eligible_for_final_selection")
            ).alias("label_safe_switch_win")
        )
    if "label_tail_risk_loss" not in frame.columns:
        expressions.append(
            (
                (pl.col("label_regret_delta_vs_v2_plus_uah") >= tail_risk_delta_uah)
                | (pl.col("safety_violation_count") > 0)
            ).alias("label_tail_risk_loss")
        )
    if "raw_hourly_action_imitation" not in frame.columns:
        expressions.append(pl.lit(False).alias("raw_hourly_action_imitation"))
    if "not_market_execution" not in frame.columns:
        expressions.append(pl.lit(True).alias("not_market_execution"))
    if "market_execution_enabled" not in frame.columns:
        expressions.append(pl.lit(False).alias("market_execution_enabled"))
    return frame.with_columns(expressions) if expressions else frame


def _filter_to_v13_readiness_pairs(
    candidate_frame: pl.DataFrame,
    readiness_frame: pl.DataFrame,
) -> pl.DataFrame:
    readiness_pairs = readiness_frame.select(["tenant_id", "source_model_name"]).unique()
    return candidate_frame.join(
        readiness_pairs,
        on=["tenant_id", "source_model_name"],
        how="inner",
    )


def _summary(
    contract_frame: pl.DataFrame,
    args: argparse.Namespace,
    *,
    candidate_rows: int,
    filtered_candidate_rows: int,
) -> dict[str, Any]:
    blocker_counts = (
        contract_frame.group_by("training_blocker")
        .len(name="row_count")
        .sort("training_blocker")
    )
    dt_action_target_contract = _single_value(
        contract_frame,
        "dt_action_target_contract",
    )
    v2_plus_role = _single_value(contract_frame, "v2_plus_role")
    if (
        contract_frame.height > 0
        and dt_action_target_contract != DT_ACTION_TARGET_CONTRACT
    ):
        raise ValueError(
            "Unexpected DT action target contract: "
            f"{dt_action_target_contract!r}."
        )
    if contract_frame.height > 0 and v2_plus_role != V2_PLUS_ROLE:
        raise ValueError(f"Unexpected V2+ role: {v2_plus_role!r}.")
    return {
        "claim_scope": CLAIM_SCOPE,
        "candidate_frame_pickle": str(args.candidate_frame_pickle),
        "readiness_csv": str(args.readiness_csv),
        "output_pickle": str(args.output_pickle),
        "candidate_rows": candidate_rows,
        "filtered_candidate_rows": filtered_candidate_rows,
        "filtered_to_v13_readiness_pairs": (
            not bool(args.include_non_v13_readiness_pairs)
        ),
        "dropped_non_v13_readiness_pair_rows": candidate_rows - filtered_candidate_rows,
        "contract_rows": contract_frame.height,
        "tenant_count": contract_frame["tenant_id"].n_unique(),
        "source_model_count": contract_frame["source_model_name"].n_unique(),
        "train_selection_rows": contract_frame.filter(
            pl.col("split_name") == "train_selection"
        ).height,
        "permitted_model_training_rows": contract_frame.filter(
            pl.col("permitted_model_training_row")
        ).height,
        "training_blocker_counts": {
            str(row["training_blocker"]): int(row["row_count"])
            for row in blocker_counts.iter_rows(named=True)
        },
        "dt_action_target_contract": dt_action_target_contract
        or DT_ACTION_TARGET_CONTRACT,
        "v2_plus_role": v2_plus_role or V2_PLUS_ROLE,
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "permits_model_training": bool(
            contract_frame.select(pl.col("permits_model_training").any()).item()
        ),
        "market_execution_enabled": False,
    }


def _single_value(frame: pl.DataFrame, column: str) -> str:
    values = sorted({str(value) for value in frame[column].drop_nulls().to_list()})
    return values[0] if len(values) == 1 else "|".join(values)


def _load_polars_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
