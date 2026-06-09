from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_dagster_definitions_do_not_eagerly_import_torch() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    pythonpath_entries = [str(repo_root), str(repo_root / "src")]
    existing_pythonpath = os.environ.get("PYTHONPATH")
    if existing_pythonpath:
        pythonpath_entries.append(existing_pythonpath)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "import smart_arbitrage.defs\n"
                "print('torch' in sys.modules)\n"
            ),
        ],
        check=True,
        capture_output=True,
        cwd=repo_root,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(pythonpath_entries)},
        text=True,
        timeout=60,
    )

    assert result.stdout.strip() == "False"
