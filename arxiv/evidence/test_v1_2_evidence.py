from __future__ import annotations

import csv
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent
LINEAGE = ROOT / "lineage"


class Version12EvidenceTest(unittest.TestCase):
    def test_dt_temporal_suite_is_independent_and_not_beneficial(self) -> None:
        summary = _json("dt_temporal_v2_plus_suite_summary.json")
        self.assertEqual(summary["protocol_run_count"], 36)
        self.assertTrue(summary["all_protocols_independent"])
        self.assertEqual(summary["beneficial_run_count"], 0)
        self.assertEqual(summary["tie_run_count"], 33)
        self.assertEqual(summary["harmful_run_count"], 3)

    def test_differentiable_suite_preserves_primary_and_secondary_counts(self) -> None:
        summary = _json("v1_2_differentiable_dfl_suite_summary.json")
        rows = _csv("v1_2_differentiable_dfl_suite_rows.csv")
        self.assertEqual(len(rows), 72)
        self.assertEqual(summary["beneficial_vs_v2_plus_run_count"], 0)
        self.assertEqual(summary["harmful_vs_v2_plus_run_count"], 72)
        self.assertEqual(summary["transformer_comparison_count"], 36)
        self.assertEqual(summary["transformer_better_regret_count"], 28)
        self.assertEqual(summary["beneficial_vs_raw_run_count"], 32)
        self.assertTrue(summary["all_content_overlap_counts_zero"])
        self.assertTrue(summary["all_safety_violation_counts_zero"])
        self.assertTrue(summary["profile_aware_decision_loss"])
        self.assertEqual(summary["decision_focused_profile_count"], 5)
        self.assertTrue(all(int(row["content_overlap_count"]) == 0 for row in rows))
        self.assertTrue(all(int(row["safety_violation_count"]) == 0 for row in rows))
        self.assertTrue(
            all(float(row["mean_dfl_minus_v2_plus_regret_uah"]) > 0.0 for row in rows)
        )

    def test_public_oree_probe_does_not_fake_a_receipt(self) -> None:
        summary = _json("v1_2_oree_public_probe_summary.json")
        self.assertFalse(summary["public_path_gate_closed"])
        self.assertEqual(summary["promotable_v13_permitted_training_rows"], 0)
        self.assertFalse(summary["market_execution_enabled"])
        self.assertFalse(summary["dam_observation"]["can_satisfy_v13_explicit_receipts"])


def _json(name: str) -> dict[str, object]:
    value = json.loads((LINEAGE / name).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{name} must contain a JSON object")
    return value


def _csv(name: str) -> list[dict[str, str]]:
    with (LINEAGE / name).open(encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


if __name__ == "__main__":
    unittest.main()
