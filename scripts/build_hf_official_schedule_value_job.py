"""Build a Hugging Face Jobs payload for official forecast evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from smart_arbitrage.cloud.hf_official_jobs import (
    HfOfficialScheduleValueJobConfig,
    build_hf_official_schedule_value_job_payload,
)

DEFAULT_CONFIG = HfOfficialScheduleValueJobConfig()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a Hugging Face Jobs payload JSON for official schedule/value evidence."
    )
    parser.add_argument("--repo-url", default=DEFAULT_CONFIG.repo_url)
    parser.add_argument("--git-ref", default=DEFAULT_CONFIG.git_ref)
    parser.add_argument(
        "--total-anchors-per-tenant",
        type=int,
        default=DEFAULT_CONFIG.total_anchors_per_tenant,
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_CONFIG.batch_size)
    parser.add_argument(
        "--anchor-batch-order",
        choices=("chronological", "latest_first"),
        default=DEFAULT_CONFIG.anchor_batch_order,
    )
    parser.add_argument(
        "--enabled-official-models-csv",
        default=DEFAULT_CONFIG.enabled_official_models_csv,
    )
    parser.add_argument(
        "--nbeatsx-max-steps",
        type=int,
        default=DEFAULT_CONFIG.nbeatsx_max_steps,
    )
    parser.add_argument(
        "--tft-max-epochs",
        type=int,
        default=DEFAULT_CONFIG.tft_max_epochs,
    )
    parser.add_argument("--flavor", default=DEFAULT_CONFIG.flavor)
    parser.add_argument("--timeout", default=DEFAULT_CONFIG.timeout)
    parser.add_argument("--run-slug", default=DEFAULT_CONFIG.run_slug)
    parser.add_argument("--artifact-repo-id", default="")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    config = HfOfficialScheduleValueJobConfig(
        repo_url=args.repo_url,
        git_ref=args.git_ref,
        total_anchors_per_tenant=args.total_anchors_per_tenant,
        batch_size=args.batch_size,
        anchor_batch_order=args.anchor_batch_order,
        enabled_official_models_csv=args.enabled_official_models_csv,
        nbeatsx_max_steps=args.nbeatsx_max_steps,
        tft_max_epochs=args.tft_max_epochs,
        flavor=args.flavor,
        timeout=args.timeout,
        run_slug=args.run_slug,
        artifact_repo_id=args.artifact_repo_id,
    )
    payload = build_hf_official_schedule_value_job_payload(config)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
