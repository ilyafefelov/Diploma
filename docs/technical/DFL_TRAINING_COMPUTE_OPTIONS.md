# DFL Training Compute Options

Date: 2026-05-11

This note records the compute decision after the serious official NBEATSx/TFT
local CPU run proved too slow for blind 104-anchor retries.

## Current Position

The local official run persisted 20 chronological anchors per source before it
was stopped. The partial result is not promotion-grade, but it is enough to show
that a full local CPU run should not be the first diagnostic:

| Model | Anchors | Mean regret | Median regret |
|---|---:|---:|---:|
| `strict_similar_day` | 20 | 341.84 | 187.99 |
| `nbeatsx_official_v0` | 20 | 807.70 | 450.08 |
| `tft_official_v0` | 20 | 1354.23 | 987.03 |

The result supports a stronger engineering rule: screen recent anchors and one
source model at a time before spending compute on the full promotion gate.

## Local GTX 1050 Ti

The GTX 1050 Ti can run small PyTorch CUDA jobs, but it is a weak training
device for this repo:

- 4 GB VRAM limits batch size and model size;
- Pascal compute capability means no Tensor Cores;
- Windows/Docker GPU passthrough adds setup friction;
- current workload spends meaningful time in repeated rolling-origin data prep
  and orchestration, not only tensor math.

Use it for CUDA sanity checks and tiny screens, not as the main DFL/DT training
platform.

## Best Near-Term Cloud Path

For the thesis workload, the most practical offload path is not Databricks first.
It is a single packaged training command that can run on a cheap GPU box and
return persisted evidence artifacts.

Recommended provider order:

1. **Modal**: best for script-like jobs and free monthly starter credits; good
   target once the training command is stateless and artifact-based.
2. **Hugging Face Jobs**: strong fit for reproducible ML jobs with Docker/UV,
   but requires HF Pro or a Team/Enterprise organization.
3. **RunPod or Vast.ai**: usually cheapest for RTX 3090/4090 class GPUs, but
   reliability and image hygiene require more manual checking.
4. **Lambda or Paperspace**: easier stable VM/notebook experience; somewhat
   more expensive, but less marketplace risk.
5. **Colab/Kaggle**: useful free experiments, weak for unattended/reproducible
   evidence because availability and runtime limits fluctuate.
6. **Databricks**: good for large Spark/MLflow organizations, overkill for the
   current single-repo rolling-origin experiment.

## Cost Envelope

Expected small thesis runs should be cheap if they are screened first:

- latest 18/36-anchor official screen: likely under a few dollars on T4/L4/A10
  class hardware if packaged cleanly;
- full 104-anchor source-specific run: likely a few dollars to low tens of
  dollars depending on provider, retry rate, and whether both source models are
  trained;
- broad hyperparameter sweeps or DT experiments: only run after evidence shows
  the source is close to `strict_similar_day`.

## Implementation Rule

Before paid compute, the repo needs one clean cloud/offload entrypoint:

```powershell
.\scripts\run-official-schedule-value-batches.ps1 `
  -TotalAnchorsPerTenant 18 `
  -BatchSize 4 `
  -AnchorBatchOrder latest_first `
  -EnabledOfficialModelsCsv tft_official_v0 `
  -NbeatsxMaxSteps 25 `
  -TftMaxEpochs 5 `
  -SkipDownstreamGate
```

The same command should later be wrapped for HF Jobs, Modal, or a GPU VM. The
training output should be artifact-first: write compact Parquet/JSON summaries
and only merge back into Postgres after the evidence run is validated.

## Claim Boundary

Cloud training does not change the academic claim by itself. Any official
NBEATSx/TFT, DFL, or offline DT candidate must still pass the same strict
LP/oracle promotion gate against frozen `strict_similar_day`.
