"""Submit or dry-run a generated Hugging Face Jobs official-evidence payload."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from smart_arbitrage.cloud.hf_official_jobs import (
    submit_hf_official_schedule_value_job_payload,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Write a guarded HF Jobs receipt for official schedule/value evidence; "
            "the default is a dry-run receipt and does not submit paid compute."
        )
    )
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Submit the job to Hugging Face Jobs; without this flag only a dry-run receipt is written.",
    )
    args = parser.parse_args(argv)

    receipt = submit_hf_official_schedule_value_job_payload(
        args.payload,
        output_path=args.output,
        submit=args.submit,
    )
    print(args.output)
    if receipt["submitted"]:
        print(f"submitted job: {receipt['job_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
