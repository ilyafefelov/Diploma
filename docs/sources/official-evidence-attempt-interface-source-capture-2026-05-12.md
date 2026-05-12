# Official Evidence Attempt Interface Source Capture

Date: 2026-05-12

Purpose: record the operational sources used while deepening the official
forecast evidence-attempt interface.

## Engineering Sources

| Source | URL | Use in this slice |
|---|---|---|
| Hugging Face Jobs configuration | https://huggingface.co/docs/hub/jobs-configuration | Confirms UV jobs can use custom timeouts, secrets, hardware flavors, and mounted artifacts. |
| Hugging Face Hub Jobs guide | https://huggingface.co/docs/huggingface_hub/main/guides/jobs | Confirms `run_uv_job`/Jobs support environment, secrets, timeout, and managed execution metadata. |
| Repo official schedule/value runner | `scripts/run-official-schedule-value-batches.ps1` | Local source of resumable 104-anchor schedule/value evidence. |
| Repo official global-panel runner | `scripts/run-official-global-panel-batches.ps1` | Local source of resumable 365-anchor global-panel evidence. |

## Repo Decision

Official evidence attempts now write a manifest before expensive materialization
starts. The manifest is intentionally small and stable:

- fixed generated timestamp;
- planned anchor batches;
- exact asset selection;
- downstream gate selection;
- resume rule;
- Offline Strategy Promotion claim boundary.

This is not a new forecast model and not live market execution. It is the
execution-governance layer needed before reliable Hugging Face Jobs offload.
