from __future__ import annotations

import argparse
import csv
import json
import math
import random
from pathlib import Path
from typing import Iterable


ROLES = (
    "raw_reference",
    "strict_reference",
    "schedule_value_learner_v2_reference",
    "schedule_value_learner_v2_plus",
)
SOURCES = (
    "nbeatsx_official_global_panel_horizon_calibrated_v1",
    "nbeatsx_official_global_panel_v1",
)
CAPACITY_MWH_BY_TENANT = {
    "client_001_kyiv_mall": 0.280,
    "client_002_lviv_office": 0.150,
    "client_003_dnipro_factory": 0.500,
    "client_004_kharkiv_hospital": 0.400,
    "client_005_odesa_hotel": 0.200,
}
BOOTSTRAP_BLOCK_LENGTHS = (2, 3, 4, 6)
EXPORT_COLUMNS = (
    "source_model_name",
    "selection_role",
    "tenant_id",
    "anchor_timestamp",
    "decision_value_uah",
    "oracle_value_uah",
    "regret_uah",
    "safety_violation_count",
)


def percentile(values: Iterable[float], quantile: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def moving_block_interval(
    values: list[float],
    *,
    replicates: int,
    block_length: int,
    seed: int,
) -> list[float]:
    rng = random.Random(seed)
    sample_size = len(values)
    means: list[float] = []
    for _ in range(replicates):
        indexes: list[int] = []
        while len(indexes) < sample_size:
            start = rng.randrange(sample_size)
            indexes.extend((start + offset) % sample_size for offset in range(block_length))
        selected = indexes[:sample_size]
        means.append(sum(values[index] for index in selected) / sample_size)
    return [percentile(means, 0.025), percentile(means, 0.975)]


def reconstruct(
    input_path: Path,
    output_dir: Path,
    *,
    bootstrap_replicates: int,
    block_length: int,
    seed: int,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, str]] = []
    with input_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row["source_model_name"] in SOURCES and row["selection_role"] in ROLES:
                rows.append({column: row[column] for column in EXPORT_COLUMNS})

    expected_rows = len(SOURCES) * len(ROLES) * 5 * 18
    if len(rows) != expected_rows:
        raise ValueError(f"Expected {expected_rows} paired rows, found {len(rows)}")

    with (output_dir / "headline_rows.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    keyed: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (
            row["source_model_name"],
            row["selection_role"],
            row["tenant_id"],
            row["anchor_timestamp"],
        )
        if key in keyed:
            raise ValueError(f"Duplicate paired row: {key}")
        keyed[key] = row

    summary: dict[str, object] = {}
    date_rows: list[dict[str, object]] = []
    tenant_rows: list[dict[str, object]] = []
    for source in SOURCES:
        dates = sorted({key[3] for key in keyed if key[0] == source})
        tenants = sorted({key[2] for key in keyed if key[0] == source})
        role_metrics: dict[str, object] = {}
        for role in ROLES:
            role_rows = [
                keyed[(source, role, tenant, date)] for date in dates for tenant in tenants
            ]
            regrets = [float(row["regret_uah"]) for row in role_rows]
            normalized_regrets = [
                float(row["regret_uah"]) / CAPACITY_MWH_BY_TENANT[row["tenant_id"]]
                for row in role_rows
            ]
            role_metrics[role] = {
                "mean_regret_uah": sum(regrets) / len(regrets),
                "median_regret_uah": percentile(regrets, 0.5),
                "mean_regret_uah_per_mwh_capacity": (
                    sum(normalized_regrets) / len(normalized_regrets)
                ),
                "mean_decision_value_uah": sum(
                    float(row["decision_value_uah"]) for row in role_rows
                )
                / len(role_rows),
                "mean_oracle_value_uah": sum(
                    float(row["oracle_value_uah"]) for row in role_rows
                )
                / len(role_rows),
                "safety_violation_count": sum(
                    int(row["safety_violation_count"]) for row in role_rows
                ),
            }

        date_means: dict[str, dict[str, float]] = {}
        for date in dates:
            date_means[date] = {}
            for role in ROLES:
                mean_regret = sum(
                    float(keyed[(source, role, tenant, date)]["regret_uah"])
                    for tenant in tenants
                ) / len(tenants)
                date_means[date][role] = mean_regret
                date_rows.append(
                    {
                        "source_model_name": source,
                        "anchor_timestamp": date,
                        "selection_role": role,
                        "tenant_count": len(tenants),
                        "mean_regret_uah": mean_regret,
                    }
                )

        for tenant in tenants:
            for role in ROLES:
                tenant_rows.append(
                    {
                        "source_model_name": source,
                        "tenant_id": tenant,
                        "selection_role": role,
                        "anchor_count": len(dates),
                        "capacity_mwh": CAPACITY_MWH_BY_TENANT[tenant],
                        "mean_regret_uah": sum(
                            float(keyed[(source, role, tenant, date)]["regret_uah"])
                            for date in dates
                        )
                        / len(dates),
                        "mean_regret_uah_per_mwh_capacity": sum(
                            float(keyed[(source, role, tenant, date)]["regret_uah"])
                            / CAPACITY_MWH_BY_TENANT[tenant]
                            for date in dates
                        )
                        / len(dates),
                    }
                )

        paired: dict[str, object] = {}
        for name, comparator in (
            ("strict_minus_v2_plus", "strict_reference"),
            ("v2_minus_v2_plus", "schedule_value_learner_v2_reference"),
        ):
            differences = [
                date_means[date][comparator]
                - date_means[date]["schedule_value_learner_v2_plus"]
                for date in dates
            ]
            paired[name] = {
                "mean_uah": sum(differences) / len(differences),
                "moving_block_bootstrap_95_ci_uah": moving_block_interval(
                    differences,
                    replicates=bootstrap_replicates,
                    block_length=block_length,
                    seed=seed,
                ),
                "block_length_sensitivity_95_ci_uah": {
                    str(sensitivity_block_length): moving_block_interval(
                        differences,
                        replicates=bootstrap_replicates,
                        block_length=sensitivity_block_length,
                        seed=seed,
                    )
                    for sensitivity_block_length in BOOTSTRAP_BLOCK_LENGTHS
                },
                "positive_date_count": sum(value > 0 for value in differences),
                "date_count": len(differences),
                "bootstrap_replicates": bootstrap_replicates,
                "block_length_dates": block_length,
                "seed": seed,
            }

        summary[source] = {
            "row_count": len(ROLES) * len(dates) * len(tenants),
            "date_count": len(dates),
            "tenant_count": len(tenants),
            "roles": role_metrics,
            "paired_date_effects": paired,
        }

    for filename, fieldnames, output_rows in (
        (
            "date_role_summary.csv",
            ("source_model_name", "anchor_timestamp", "selection_role", "tenant_count", "mean_regret_uah"),
            date_rows,
        ),
        (
            "tenant_role_summary.csv",
            (
                "source_model_name",
                "tenant_id",
                "selection_role",
                "anchor_count",
                "capacity_mwh",
                "mean_regret_uah",
                "mean_regret_uah_per_mwh_capacity",
            ),
            tenant_rows,
        ),
    ):
        with (output_dir / filename).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_rows)

    (output_dir / "headline_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconstruct the article's frozen headline tables.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--bootstrap-replicates", type=int, default=20_000)
    parser.add_argument("--block-length", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20_260_712)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    reconstruct(
        args.input,
        args.output_dir,
        bootstrap_replicates=args.bootstrap_replicates,
        block_length=args.block_length,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
