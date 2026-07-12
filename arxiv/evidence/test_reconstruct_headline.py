from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


EVIDENCE_DIR = Path(__file__).resolve().parent
SOURCE_CSV = EVIDENCE_DIR / "frozen" / "headline_rows.csv"


class ReconstructionCliTest(unittest.TestCase):
    def test_reconstructs_frozen_headline_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            completed = subprocess.run(
                [
                    sys.executable,
                    str(EVIDENCE_DIR / "reconstruct_headline.py"),
                    "--input",
                    str(SOURCE_CSV),
                    "--output-dir",
                    str(output_dir),
                    "--bootstrap-replicates",
                    "20000",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            summary = json.loads((output_dir / "headline_summary.json").read_text())
            calibrated = summary["nbeatsx_official_global_panel_horizon_calibrated_v1"]
            raw = summary["nbeatsx_official_global_panel_v1"]

            self.assertEqual(calibrated["row_count"], 360)
            self.assertEqual(calibrated["date_count"], 18)
            self.assertEqual(calibrated["tenant_count"], 5)
            self.assertAlmostEqual(
                calibrated["roles"]["schedule_value_learner_v2_plus"]["mean_regret_uah"],
                174.7683983151615,
            )
            self.assertAlmostEqual(
                calibrated["roles"]["schedule_value_learner_v2_plus"]
                ["mean_regret_uah_per_mwh_capacity"],
                584.1807692582956,
            )
            self.assertAlmostEqual(
                calibrated["paired_date_effects"]["strict_minus_v2_plus"]["mean_uah"],
                135.8144098251736,
            )
            self.assertEqual(
                calibrated["paired_date_effects"]["strict_minus_v2_plus"]["positive_date_count"],
                14,
            )
            self.assertEqual(
                calibrated["paired_date_effects"]["strict_minus_v2_plus"]
                ["moving_block_bootstrap_95_ci_uah"],
                [64.2706117474575, 217.1037774557274],
            )
            self.assertAlmostEqual(
                raw["roles"]["schedule_value_learner_v2_plus"]["mean_regret_uah"],
                193.35903850044008,
            )
            self.assertEqual(
                raw["paired_date_effects"]["v2_minus_v2_plus"]
                ["moving_block_bootstrap_95_ci_uah"],
                [13.481855956965626, 53.18305586145166],
            )
            self.assertEqual(
                sorted(
                    raw["paired_date_effects"]["v2_minus_v2_plus"]
                    ["block_length_sensitivity_95_ci_uah"]
                ),
                ["2", "3", "4", "6"],
            )


if __name__ == "__main__":
    unittest.main()
