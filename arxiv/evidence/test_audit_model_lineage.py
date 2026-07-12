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


if __name__ == "__main__":
    unittest.main()
