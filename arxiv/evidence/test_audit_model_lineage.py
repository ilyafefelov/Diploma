from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


EVIDENCE_DIR = Path(__file__).resolve().parent


class ModelLineageAuditCliTest(unittest.TestCase):
    def test_detects_rf_identity_exact_mirroring_and_single_switch_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "model_lineage_audit.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(EVIDENCE_DIR / "audit_model_lineage.py"),
                    "--lineage-dir",
                    str(EVIDENCE_DIR / "lineage"),
                    "--output",
                    str(output_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            audit = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(audit["canonical_artifact_identifier"], "dt_v2_plus")
            self.assertEqual(audit["canonical_estimator_class"], "random_forest")
            self.assertTrue(audit["exact_mirror"]["all_model_inputs_and_targets_equal"])
            self.assertEqual(audit["exact_mirror"]["paired_candidate_row_count"], 360)
            self.assertEqual(audit["rf_selection"]["switch_count"], 4)
            self.assertEqual(audit["rf_selection"]["abstention_count"], 86)
            self.assertEqual(audit["rf_selection"]["distinct_switch_dates"], ["2026-04-15"])
            self.assertEqual(audit["hf_diagnostics"]["frozen_mean_regret_uah"], 158.71213017569647)
            self.assertEqual(audit["hf_diagnostics"]["read_model_audit_day_count"], 32)
            temporal_replay = audit["temporal_replay"]
            self.assertEqual(temporal_replay["training_candidate_row_count"], 1080)
            self.assertEqual(temporal_replay["evaluation_candidate_row_count"], 360)
            self.assertEqual(temporal_replay["content_overlap_candidate_row_count"], 0)
            self.assertTrue(temporal_replay["independent_holdout"])
            self.assertEqual(temporal_replay["profile_date_row_count"], 90)
            self.assertEqual(temporal_replay["distinct_market_date_count"], 18)
            self.assertEqual(temporal_replay["switch_count"], 0)
            self.assertEqual(temporal_replay["abstention_count"], 90)
            self.assertEqual(
                temporal_replay["selector_minus_v2_plus_mean_regret_uah"], 0.0
            )
            self.assertFalse(temporal_replay["promotion_gate_passed"])
            self.assertFalse(temporal_replay["market_execution_enabled"])
            temporal_suite = audit["temporal_suite"]
            self.assertEqual(temporal_suite["run_count"], 14)
            self.assertEqual(temporal_suite["source_model_count"], 2)
            self.assertEqual(temporal_suite["evaluation_window_indices"], [1, 2, 3])
            self.assertEqual(
                temporal_suite["thresholds_uah"], [0.0, 5.0, 10.0, 20.0, 50.0]
            )
            self.assertTrue(temporal_suite["all_independent_holdouts"])
            self.assertEqual(temporal_suite["maximum_content_overlap_ratio"], 0.0)
            self.assertEqual(temporal_suite["beneficial_protocol_count"], 0)
            self.assertEqual(temporal_suite["harmful_protocol_count"], 3)
            self.assertAlmostEqual(
                temporal_suite["maximum_primary_seed_harm_uah"],
                123.08140686958836,
            )
            self.assertFalse(temporal_suite["promotion_gate_passed"])
            self.assertFalse(temporal_suite["market_execution_enabled"])


if __name__ == "__main__":
    unittest.main()
